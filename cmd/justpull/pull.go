package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/distribution/reference"

	"github.com/cryguy/justpull"
	"github.com/cryguy/justpull/internal/dockerconfig"
	"github.com/cryguy/justpull/internal/engine"
)

// exitInterrupted is docker pull's exit code when SIGINT or SIGTERM cancels
// it: its error is then context.Canceled, which the docker CLI neither prints
// nor maps to a code (cmd/docker/docker.go, main).
const exitInterrupted = 0

// finishTimeout bounds the wait for the proxy to end after the finish
// command; then docker run is killed, and the container ends on the end of
// its stdin.
const finishTimeout = 10 * time.Second

// env is what a pull reads and writes besides Docker.
type env struct {
	stdout, stderr io.Writer
	tty            bool // stdout is a terminal
	width          int  // its width, 0 if unknown
	getenv         func(string) string
	config         func() (loginConfig, error)
	docker         func() (dockerAPI, error)
}

// loginConfig is the docker CLI's login config, dockerconfig.Config.
type loginConfig interface {
	Credential(ctx context.Context, registry string) (justpull.Credential, error)
	Warnings() []string
}

func loadConfig() (loginConfig, error) {
	cfg, err := dockerconfig.Load()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// pullCommand is docker pull through the proxy. ctx ends on an interrupt.
// It returns the exit code.
func pullCommand(ctx context.Context, args []string, e *env) int {
	opts, cerr := parseArgs(args, e.getenv("DOCKER_DEFAULT_PLATFORM"))
	switch {
	case cerr != nil:
		fmt.Fprintln(e.stderr, cerr.msg)
		return cerr.code
	case opts.help:
		_, _ = io.WriteString(e.stdout, helpText(e.getenv("DOCKER_DEFAULT_PLATFORM")))
		return 0
	case opts.version:
		fmt.Fprintf(e.stdout, "justpull version %s\n", version)
		return 0
	}

	named, err := reference.ParseNormalizedNamed(opts.remote)
	switch {
	case err != nil:
		fmt.Fprintln(e.stderr, err)
		return 1
	case opts.all && !reference.IsNameOnly(named):
		fmt.Fprintln(e.stderr, "tag can't be used with --all-tags/-a")
		return 1
	case !opts.all && reference.IsNameOnly(named):
		named = reference.TagNameOnly(named)
		if tagged, ok := named.(reference.Tagged); ok && !opts.quiet {
			fmt.Fprintln(e.stdout, "Using default tag:", tagged.Tag())
		}
	}
	p := &puller{e: e, opts: opts, named: named, registry: reference.Domain(named)}
	return p.run(ctx)
}

type puller struct {
	e        *env
	opts     pullOptions
	named    reference.Named // what the user asked for, normalised
	registry string
}

func (p *puller) run(ctx context.Context) int {
	cred, code, ok := p.login(ctx)
	if !ok {
		return code
	}
	d, err := p.e.docker()
	if err != nil {
		return p.failed(ctx, err)
	}
	var before map[string]string
	if !p.opts.quiet {
		if before, err = p.before(ctx, d); err != nil {
			return p.failed(ctx, err)
		}
	}
	progress := p.e.stderr
	if p.opts.quiet {
		progress = io.Discard
	}
	img, err := d.EnsureProxy(ctx, progress)
	if err != nil {
		return p.failed(ctx, err)
	}
	proc, err := d.StartProxy(img)
	if err != nil {
		return p.failed(ctx, err)
	}
	s := newSession(proc)
	return p.session(ctx, d, s, cred, before)
}

// before records the image IDs of the user's names before the pull, by
// nameOf the name. A name Docker does not have is left out. With all tags the
// names are the repository's tags Docker has.
func (p *puller) before(ctx context.Context, d dockerAPI) (map[string]string, error) {
	ids := map[string]string{}
	if p.opts.all {
		tags, err := d.TagIDs(ctx, reference.FamiliarName(p.named))
		if err != nil {
			return nil, err
		}
		for tag, id := range tags {
			ids[p.named.Name()+":"+tag] = id
		}
		return ids, nil
	}
	key := nameOf(p.named.Name(), p.named)
	id, ok, err := d.ImageID(ctx, key)
	if err != nil {
		return nil, err
	}
	if ok {
		ids[key] = id
	}
	return ids, nil
}

// nameOf is ref's tag or digest under the repository name: by its digest
// alone when it has one, as the proxy's local name drops the tag of a tag and
// digest; name alone when ref has neither.
func nameOf(name string, ref reference.Named) string {
	if d, ok := ref.(reference.Digested); ok {
		return name + "@" + d.Digest().String()
	}
	if t, ok := ref.(reference.Tagged); ok {
		return name + ":" + t.Tag()
	}
	return name
}

// upToDate is the renderer's upToDate. dockerd reports a pull up to date when
// the pulled name's image ID is the one the name had before. That holds on
// both image stores: the containerd store's ID is the manifest or index
// digest the name points to, the graphdriver store's the config digest. At
// the status line Docker has the pulled image under the local name. A status
// for the repository alone ends the graphdriver store's all-tags pull: it is
// up to date when every tag pulled has the ID its user's name had.
func (p *puller) upToDate(ctx context.Context, d dockerAPI, local reference.Named, before map[string]string) func(string) bool {
	return func(ref string) bool {
		named, err := reference.ParseNormalizedNamed(ref)
		if err != nil {
			return false
		}
		if reference.IsNameOnly(named) {
			if len(before) == 0 {
				return false
			}
			after, err := d.TagIDs(ctx, local.Name())
			if err != nil || len(after) == 0 {
				return false
			}
			for tag, id := range after {
				if before[named.Name()+":"+tag] != id {
					return false
				}
			}
			return true
		}
		id, ok := before[nameOf(named.Name(), named)]
		if !ok {
			return false
		}
		now, ok, err := d.ImageID(ctx, nameOf(local.Name(), named))
		return err == nil && ok && now == id
	}
}

// login finds the login docker would use. A config docker cannot read is a
// warning, and the pull goes on without a login, as docker's does.
func (p *puller) login(ctx context.Context) (cred justpull.Credential, code int, ok bool) {
	cfg, err := p.e.config()
	if err != nil {
		fmt.Fprintln(p.e.stderr, "WARNING: Error", err)
		return cred, 0, true
	}
	for _, w := range cfg.Warnings() {
		fmt.Fprintln(p.e.stderr, w)
	}
	cred, err = cfg.Credential(ctx, p.registry)
	if err != nil {
		if ctx.Err() != nil {
			return cred, exitInterrupted, false
		}
		fmt.Fprintln(p.e.stderr, err)
		return cred, 1, false
	}
	return cred, 0, true
}

// failed reports a failure of justpull's own steps, which docker pull does
// not have.
func (p *puller) failed(ctx context.Context, err error) int {
	if ctx.Err() != nil {
		return exitInterrupted
	}
	fmt.Fprintln(p.e.stderr, "justpull:", err)
	return 1
}

func (p *puller) session(ctx context.Context, d dockerAPI, s *session, cred justpull.Credential, before map[string]string) int {
	req := request{Ref: p.named.String(), AllTags: p.opts.all}
	req.Credential.Username = cred.Username
	req.Credential.Password = cred.Password
	req.Credential.IdentityToken = cred.IdentityToken
	line, err := json.Marshal(req)
	if err != nil {
		s.end(false)
		return p.failed(ctx, err)
	}
	// A failed write means the container is gone; its stdout ends too.
	_, _ = s.proc.stdin.Write(append(line, '\n'))

	var first proxyEvent
	var ok bool
	select {
	case first, ok = <-s.events:
	case <-ctx.Done():
		s.end(false)
		return exitInterrupted
	}
	switch {
	case !ok:
		werr := s.end(false)
		if ctx.Err() != nil {
			return exitInterrupted
		}
		msg := strings.TrimSpace(s.proc.stderr())
		if msg == "" && werr != nil {
			msg = werr.Error()
		}
		fmt.Fprintln(p.e.stderr, "justpull: the proxy container did not start:", msg)
		return 1
	case first.Event == "failed":
		s.end(false)
		return p.proxyFailed(first)
	case first.Event != "ready":
		s.end(false)
		return p.failed(ctx, fmt.Errorf("the proxy sent %q before ready", first.Event))
	}

	local, err := reference.ParseNormalizedNamed(first.LocalRef)
	if err != nil {
		s.end(false)
		return p.failed(ctx, fmt.Errorf("the proxy's reference %q: %w", first.LocalRef, err))
	}
	rename := renamer(local, p.named)
	r := newRenderer(p.e.stdout, p.e.tty, p.e.width, p.opts.quiet, rename)
	r.renameID = idRenamer(local, p.named)
	pullCtx, cancelPull := context.WithCancel(ctx)
	defer cancelPull()
	if !p.opts.quiet {
		r.upToDate = p.upToDate(pullCtx, d, local, before)
	}
	s.dispatch(r, cancelPull)

	if err := p.pull(pullCtx, d, local, r); err != nil {
		s.end(false)
		if ctx.Err() != nil {
			return exitInterrupted
		}
		if cause, ok := s.cause(); ok {
			return p.proxyFailed(cause)
		}
		fmt.Fprintln(p.e.stderr, dockerMessage(err, rename))
		return 1
	}
	if err := p.name(ctx, d, local, cred); err != nil {
		s.end(false)
		return p.failed(ctx, err)
	}
	s.end(true)
	fmt.Fprintln(p.e.stdout, p.named.String())
	return 0
}

// pull pulls the local reference through the proxy and prints its progress.
func (p *puller) pull(ctx context.Context, d dockerAPI, local reference.Named, r *renderer) error {
	rc, err := d.Pull(ctx, local.Name(), tagOrDigest(local), p.opts.platform, p.opts.all, nil)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return r.render(rc)
}

// name gives the pulled image the name the user asked for and removes the
// local name. On a failure the local name stays, holding the image, as the
// library's Pull leaves it.
func (p *puller) name(ctx context.Context, d dockerAPI, local reference.Named, cred justpull.Credential) error {
	var locals []string
	switch {
	case p.opts.all:
		refs, err := d.ListTags(ctx, local.Name())
		if err != nil {
			return err
		}
		for _, ref := range refs {
			tag, ok := strings.CutPrefix(ref, local.Name()+":")
			if !ok {
				continue
			}
			if err := d.Tag(ctx, ref, p.named.Name()+":"+tag); err != nil {
				return err
			}
			locals = append(locals, ref)
		}
	case isDigested(local):
		// A digest reference cannot be created with a tag. Pulling it by its
		// real name costs one manifest request: every layer is in Docker.
		same := func(s string) string { return s }
		rc, err := d.Pull(ctx, p.named.Name(), tagOrDigest(local), p.opts.platform, false, registryAuth(cred, p.registry))
		if err != nil {
			return fmt.Errorf("pull %s: %s", p.named.String(), dockerMessage(err, same))
		}
		err = newRenderer(io.Discard, false, 0, true, same).render(rc)
		_ = rc.Close()
		if err != nil {
			return fmt.Errorf("pull %s: %w", p.named.String(), err)
		}
		locals = append(locals, local.String())
	default:
		if err := d.Tag(ctx, local.String(), p.named.String()); err != nil {
			return err
		}
		locals = append(locals, local.String())
	}
	for _, ref := range locals {
		if err := d.Untag(ctx, ref); err != nil {
			fmt.Fprintf(p.e.stderr, "WARNING: could not remove the image name %s: %v\n", ref, err)
		}
	}
	return nil
}

// proxyFailed prints the proxy's cause, and how to log in when the registry
// refused the login or the image.
func (p *puller) proxyFailed(e proxyEvent) int {
	fmt.Fprintln(p.e.stderr, "justpull:", e.Error)
	if e.Refused {
		fmt.Fprintln(p.e.stderr, loginHint(p.registry))
	}
	return 1
}

func loginHint(registry string) string {
	if registry == "docker.io" {
		return "log in with: docker login"
	}
	return "log in with: docker login " + registry
}

// registryAuth is the login as the docker CLI sends it with a pull.
func registryAuth(cred justpull.Credential, registry string) *engine.RegistryAuth {
	return &engine.RegistryAuth{
		Username:      cred.Username,
		Password:      cred.Password,
		IdentityToken: cred.IdentityToken,
		ServerAddress: dockerconfig.ServerAddress(registry),
	}
}

// dockerMessage is what the docker CLI prints for a failed pull, renamed.
func dockerMessage(err error, rename func(string) string) string {
	if apiErr, ok := errors.AsType[*engine.APIError](err); ok {
		return "Error response from daemon: " + rename(strings.TrimSpace(apiErr.Message))
	}
	return rename(err.Error())
}

// renamer maps the proxy's names to what the user pulled: the local
// repository to the familiar name, and the proxy's address alone, as in a
// URL, to the registry's.
func renamer(local, user reference.Named) func(string) string {
	host := reference.Domain(user)
	if host == "docker.io" {
		host = "registry-1.docker.io"
	}
	return strings.NewReplacer(local.Name(), reference.FamiliarName(user), reference.Domain(local), host).Replace
}

// idRenamer maps a message id that names the local reference to the user's
// reference in full, as dockerd writes it: a digest pull's "Pulling from"
// message has the pulled reference as its id.
func idRenamer(local, user reference.Named) func(string) string {
	return strings.NewReplacer(local.Name(), user.Name()).Replace
}

func tagOrDigest(ref reference.Named) string {
	if d, ok := ref.(reference.Digested); ok {
		return d.Digest().String()
	}
	if t, ok := ref.(reference.Tagged); ok {
		return t.Tag()
	}
	return ""
}

func isDigested(ref reference.Named) bool {
	_, ok := ref.(reference.Digested)
	return ok
}

// proxyEvent is a line of the proxy's stdout.
type proxyEvent struct {
	Event    string `json:"event"`
	LocalRef string `json:"localRef"`
	Error    string `json:"error"`
	Refused  bool   `json:"refused"`
	Success  bool   `json:"success"`
	ID       string `json:"id"`
	Offset   int64  `json:"offset"`
}

// session is the conversation with a running proxy. Its events have one
// reader: dispatch after ready, end otherwise.
type session struct {
	proc       *proxyProc
	events     chan proxyEvent
	dispatched bool
	finished   chan struct{} // closed on the finished event or the end of stdout
	once       sync.Once
	mu         sync.Mutex
	failure    *proxyEvent
}

func newSession(proc *proxyProc) *session {
	s := &session{proc: proc, events: make(chan proxyEvent), finished: make(chan struct{})}
	go func() {
		defer close(s.events)
		lines := bufio.NewScanner(proc.stdout)
		for lines.Scan() {
			var e proxyEvent
			if json.Unmarshal(lines.Bytes(), &e) != nil {
				continue
			}
			s.events <- e
		}
	}()
	return s
}

// dispatch handles the events after ready: a failure is remembered and
// cancels the pull, a retry shows on the layer's line.
func (s *session) dispatch(r *renderer, cancelPull context.CancelFunc) {
	s.dispatched = true
	go func() {
		defer s.once.Do(func() { close(s.finished) })
		for e := range s.events {
			switch e.Event {
			case "failed":
				s.mu.Lock()
				s.failure = &e
				s.mu.Unlock()
				cancelPull()
			case "retry":
				r.resumed(e.ID, e.Offset)
			case "finished":
				s.once.Do(func() { close(s.finished) })
			}
		}
	}()
}

// cause is the proxy's failed event, if it sent one. After end, every event
// the proxy sent has been read.
func (s *session) cause() (proxyEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure == nil {
		return proxyEvent{}, false
	}
	return *s.failure, true
}

// end sends the finish command, waits for the proxy's finished event and for
// docker run to exit, and kills it after finishTimeout. It returns docker
// run's exit error.
func (s *session) end(success bool) error {
	deadline := time.NewTimer(finishTimeout)
	defer deadline.Stop()
	cmd := `{"cmd":"finish","success":false}`
	if success {
		cmd = `{"cmd":"finish","success":true}`
	}
	// The write blocks while docker run does not read; closing stdin below
	// ends it.
	go func() { _, _ = io.WriteString(s.proc.stdin, cmd+"\n") }()
	if !s.dispatched {
		go func() {
			for e := range s.events {
				if e.Event == "finished" {
					s.once.Do(func() { close(s.finished) })
				}
			}
			s.once.Do(func() { close(s.finished) })
		}()
	}
	expired := false
	select {
	case <-s.finished:
	case <-deadline.C:
		expired = true
	}
	_ = s.proc.stdin.Close()
	exited := make(chan error, 1)
	go func() { exited <- s.proc.wait() }()
	if expired {
		s.proc.kill()
		return <-exited
	}
	select {
	case err := <-exited:
		return err
	case <-deadline.C:
		s.proc.kill()
		return <-exited
	}
}
