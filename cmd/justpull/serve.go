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
// one JSON object per line, warnings go to stderr. It returns the exit code.
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
	events := json.NewEncoder(stdout)
	lines := bufio.NewScanner(stdin)

	req, opts, msg := readRequest(lines)
	if msg != "" {
		sendFailed(events, msg)
		return 1
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	opts.Logger = logger
	cache, err := justpull.New(*dir, opts)
	if err != nil {
		sendFailed(events, err.Error())
		return 1
	}
	s, err := cache.Serve(ctx, req.Ref, justpull.SessionOptions{AllTags: req.AllTags})
	if err != nil {
		sendFailed(events, err.Error())
		return 1
	}
	_ = events.Encode(struct {
		Event    string `json:"event"`
		LocalRef string `json:"localRef"`
	}{"ready", s.LocalRef()})

	finish := make(chan bool, 1)
	go func() { finish <- awaitFinish(lines, logger) }()
	var success bool
	select {
	case <-s.Context().Done():
		sendCause(s.Context(), events)
		success = <-finish
	case success = <-finish:
		// A layer can fail while the host's finish is on its way.
		sendCause(s.Context(), events)
	}
	if err := s.Finish(success); err != nil {
		logger.Warn("could not delete cached files", "err", err)
	}
	_ = events.Encode(struct {
		Event   string `json:"event"`
		Success bool   `json:"success"`
	}{"finished", success})
	return 0
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
func sendCause(ctx context.Context, events *json.Encoder) {
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		sendFailed(events, cause.Error())
	}
}

func sendFailed(events *json.Encoder, msg string) {
	_ = events.Encode(struct {
		Event string `json:"event"`
		Error string `json:"error"`
	}{"failed", msg})
}
