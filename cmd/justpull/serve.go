package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/distribution/reference"

	"github.com/cryguy/justpull"
)

// The answers to a request that cannot be served. They are fixed so that
// they never quote the request, which may hold a login.
const (
	msgMalformed  = "malformed request: want one JSON object with ref, and optionally allTags, credential and stallLimit"
	msgRef        = "the request's ref is not a valid image reference"
	msgAllTags    = "allTags needs a ref that names no tag or digest"
	msgStallLimit = "the request's stallLimit is not a Go duration such as 10m"
)

// request is the first line on stdin.
type request struct {
	Ref        string `json:"ref"`
	AllTags    bool   `json:"allTags"`
	Credential struct {
		Username      string `json:"username"`
		Password      string `json:"password"`
		IdentityToken string `json:"identityToken"`
	} `json:"credential"`
	StallLimit string `json:"stallLimit"`
}

// command is a later line on stdin.
type command struct {
	Cmd     string `json:"cmd"`
	Success bool   `json:"success"`
}

// serve runs one Session for the CLI on the host, which makes the Docker
// calls: the request is the first line on stdin, the events go to stdout as
// one JSON object per line, warnings go to stderr. ctx ending (on SIGINT or
// SIGTERM) ends the session as failed, as stdin ending does. It returns the
// exit code.
func serve(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() { fmt.Fprint(stderr, usage) }
	dir := flags.String("cache", "/cache", "")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() > 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	events := &eventWriter{enc: json.NewEncoder(stdout)}
	lines := bufio.NewScanner(stdin)

	req, opts, msg := readRequest(lines)
	if msg != "" {
		events.failed(msg, false)
		return 1
	}
	logger := slog.New(retryHandler{slog.NewJSONHandler(stderr, nil), events})
	opts.Logger = logger
	cache, err := justpull.New(*dir, opts)
	if err != nil {
		events.failed(err.Error(), false)
		return 1
	}
	s, err := cache.Serve(ctx, req.Ref, justpull.SessionOptions{AllTags: req.AllTags})
	if err != nil {
		events.failed(err.Error(), false)
		return 1
	}
	events.send(struct {
		Event    string `json:"event"`
		LocalRef string `json:"localRef"`
	}{"ready", s.LocalRef()})

	finish := make(chan bool, 1)
	go func() { finish <- awaitFinish(lines, logger) }()
	var success bool
	select {
	case <-s.Context().Done():
		sendCause(s.Context(), events)
		select {
		case success = <-finish:
		case <-ctx.Done():
		}
	case success = <-finish:
		// A layer can fail while the host's finish is on its way.
		sendCause(s.Context(), events)
	}
	if err := s.Finish(success); err != nil {
		logger.Warn("could not delete cached files", "err", err)
	}
	events.send(struct {
		Event   string `json:"event"`
		Success bool   `json:"success"`
	}{"finished", success})
	return 0
}

// eventWriter writes the events, one JSON object per line. Downloads send
// retry events from their own goroutines.
type eventWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (w *eventWriter) send(event any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.enc.Encode(event)
}

// failed sends the failed event. refused tells the host that a login, or
// another one, may help.
func (w *eventWriter) failed(msg string, refused bool) {
	w.send(struct {
		Event   string `json:"event"`
		Error   string `json:"error"`
		Refused bool   `json:"refused,omitempty"`
	}{"failed", msg, refused})
}

// retryHandler also sends a retry event for every warning that carries a
// layer's short digest and the offset its download resumes from.
type retryHandler struct {
	slog.Handler
	events *eventWriter
}

func (h retryHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		var id string
		var offset int64
		var hasID, hasOffset bool
		r.Attrs(func(a slog.Attr) bool {
			switch {
			case a.Key == "digest" && a.Value.Kind() == slog.KindString:
				id, hasID = a.Value.String(), true
			case a.Key == "offset" && a.Value.Kind() == slog.KindInt64:
				offset, hasOffset = a.Value.Int64(), true
			}
			return true
		})
		if hasID && hasOffset {
			h.events.send(struct {
				Event  string `json:"event"`
				ID     string `json:"id"`
				Offset int64  `json:"offset"`
			}{"retry", id, offset})
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h retryHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return retryHandler{h.Handler.WithAttrs(attrs), h.events}
}

func (h retryHandler) WithGroup(name string) slog.Handler {
	return retryHandler{h.Handler.WithGroup(name), h.events}
}

// readRequest reads the request from the first line. msg is one of the
// fixed answers when the request cannot be served.
func readRequest(lines *bufio.Scanner) (req request, opts justpull.Options, msg string) {
	if !lines.Scan() {
		return req, opts, msgMalformed
	}
	if err := json.Unmarshal(lines.Bytes(), &req); err != nil || req.Ref == "" {
		return req, opts, msgMalformed
	}
	named, err := reference.ParseNormalizedNamed(req.Ref)
	if err != nil {
		return req, opts, msgRef
	}
	if req.AllTags && !reference.IsNameOnly(named) {
		return req, opts, msgAllTags
	}
	if req.StallLimit != "" {
		if opts.StallLimit, err = time.ParseDuration(req.StallLimit); err != nil {
			return req, opts, msgStallLimit
		}
	}
	registry := reference.Domain(named)
	cred := justpull.Credential{
		Username:      req.Credential.Username,
		Password:      req.Credential.Password,
		IdentityToken: req.Credential.IdentityToken,
	}
	opts.Credentials = func(_ context.Context, name string) (justpull.Credential, error) {
		if name != registry {
			return justpull.Credential{}, nil
		}
		return cred, nil
	}
	return req, opts, ""
}

// awaitFinish reads stdin up to the finish command and returns its success.
// Stdin ending without one means the host is gone: the pull failed.
func awaitFinish(lines *bufio.Scanner, logger *slog.Logger) bool {
	for lines.Scan() {
		var cmd command
		if err := json.Unmarshal(lines.Bytes(), &cmd); err != nil || cmd.Cmd != "finish" {
			logger.Warn("ignoring a line on stdin that is not a finish command")
			continue
		}
		return cmd.Success
	}
	return false
}

// sendCause sends the failed event when the session ended with a failure
// rather than a cancel. The host then cancels Docker's pull.
func sendCause(ctx context.Context, events *eventWriter) {
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		events.failed(cause.Error(), errors.Is(cause, justpull.ErrRefused))
	}
}
