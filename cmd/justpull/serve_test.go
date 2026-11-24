package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cryguy/justpull/internal/fakedocker"
	"github.com/cryguy/justpull/internal/fakereg"
)

const secret = "s3cret-pw"

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type event struct {
	Event    string `json:"event"`
	LocalRef string `json:"localRef"`
	Error    string `json:"error"`
	Refused  bool   `json:"refused"`
	Success  *bool  `json:"success"`
	ID       string `json:"id"`
	Offset   int64  `json:"offset"`
}

// served is a serve run on pipes, driven as the host CLI drives it. Retry
// events, which come whenever a download resumes, are kept apart.
type served struct {
	t       *testing.T
	stdin   *io.PipeWriter
	events  chan event
	stdout  syncBuffer
	stderr  syncBuffer
	done    chan struct{}
	code    int
	mu      sync.Mutex
	retries []event
}

func startServe(t *testing.T, dir string) *served {
	t.Helper()
	return startServeCtx(t, context.Background(), dir)
}

func startServeCtx(t *testing.T, ctx context.Context, dir string) *served {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	s := &served{t: t, stdin: inW, events: make(chan event, 16), done: make(chan struct{})}
	go func() {
		s.code = serve(ctx, []string{"--cache", dir}, inR, outW, &s.stderr)
		close(s.done)
		_ = outW.Close()
	}()
	go func() {
		lines := bufio.NewScanner(outR)
		for lines.Scan() {
			_, _ = s.stdout.Write(append(lines.Bytes(), '\n'))
			var e event
			if err := json.Unmarshal(lines.Bytes(), &e); err != nil {
				e = event{Event: "not JSON: " + lines.Text()}
			}
			if e.Event == "retry" {
				s.mu.Lock()
				s.retries = append(s.retries, e)
				s.mu.Unlock()
				continue
			}
			s.events <- e
		}
		close(s.events)
	}()
	t.Cleanup(func() {
		_ = inR.CloseWithError(errors.New("test over"))
		select {
		case <-s.done:
		case <-time.After(10 * time.Second):
		}
	})
	return s
}

func (s *served) send(line string) {
	s.t.Helper()
	sent := make(chan error, 1)
	go func() {
		_, err := io.WriteString(s.stdin, line+"\n")
		sent <- err
	}()
	select {
	case err := <-sent:
		if err != nil {
			s.t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		s.t.Fatal("serve did not read stdin")
	}
}

func (s *served) next(want string) event {
	s.t.Helper()
	select {
	case e, ok := <-s.events:
		if !ok {
			s.t.Fatalf("stdout closed; want a %s event", want)
		}
		if e.Event != want {
			s.t.Fatalf("event %+v, want %s", e, want)
		}
		return e
	case <-time.After(20 * time.Second):
		s.t.Fatalf("no %s event", want)
	}
	return event{}
}

// exit waits for serve to return, checks that it wrote nothing more, and
// returns its exit code.
func (s *served) exit() int {
	s.t.Helper()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		s.t.Fatal("serve did not return")
	}
	if e, ok := <-s.events; ok {
		s.t.Fatalf("event %+v after the last one", e)
	}
	return s.code
}

func (s *served) finish(success bool) {
	s.t.Helper()
	if success {
		s.send(`{"cmd":"finish","success":true}`)
	} else {
		s.send(`{"cmd":"finish","success":false}`)
	}
	if e := s.next("finished"); e.Success == nil || *e.Success != success {
		s.t.Fatalf("finished event %+v, want success %v", e, success)
	}
	if code := s.exit(); code != 0 {
		s.t.Fatalf("exit code %d, want 0", code)
	}
}

// dockerPull pulls ref with d and returns the error its stream reports.
func dockerPull(ctx context.Context, d *fakedocker.Docker, ref string) error {
	rc, err := d.Pull(ctx, ref)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	dec := json.NewDecoder(rc)
	for {
		var m struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if m.Error != "" {
			return errors.New(m.Error)
		}
	}
}

// cacheFiles lists the cache's blob, manifest and lock files.
func cacheFiles(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, kind := range []string{"blobs", "manifests", "locks"} {
		entries, err := os.ReadDir(filepath.Join(dir, kind, "sha256"))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				t.Fatal(err)
			}
			out[kind+"/"+e.Name()] = info.Size()
		}
	}
	return out
}

func TestServePullsAndFinishEmptiesTheCache(t *testing.T) {
	reg := fakereg.New(t)
	reg.Redirect = true
	img := reg.AddImage("team/app", "v1", 200_000, 100_000)
	dir := t.TempDir()
	s := startServe(t, dir)

	s.send(`{"ref":"` + reg.Host + `/team/app:v1"}`)
	ready := s.next("ready")
	if host, path, _ := strings.Cut(ready.LocalRef, "/"); !strings.HasPrefix(host, "127.0.0.1:") || path != "team/app:v1" {
		t.Fatalf("localRef = %q, want 127.0.0.1:<port>/team/app:v1", ready.LocalRef)
	}
	if err := dockerPull(context.Background(), fakedocker.New(), ready.LocalRef); err != nil {
		t.Fatal(err)
	}
	if _, ok := cacheFiles(t, dir)["blobs/"+strings.TrimPrefix(img.Layers[0].Digest, "sha256:")]; !ok {
		t.Fatal("the layer was not cached")
	}
	s.finish(true)
	if files := cacheFiles(t, dir); len(files) != 0 {
		t.Fatalf("cache not emptied: %v", files)
	}
}

func TestServeReportsAFailedLayerAndKeepsNothingCorrupt(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 300_000)
	img.Layers[0].Faults(fakereg.Corrupt)
	dir := t.TempDir()
	s := startServe(t, dir)

	s.send(`{"ref":"` + reg.Host + `/team/app:v1","stallLimit":"30s"}`)
	ready := s.next("ready")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pulled := make(chan error, 1)
	go func() { pulled <- dockerPull(ctx, fakedocker.New(), ready.LocalRef) }()

	if e := s.next("failed"); !strings.Contains(e.Error, "digest mismatch") || e.Refused {
		t.Fatalf("failed event %+v, want the digest mismatch, not refused", e)
	}
	cancel() // as the host does on a failed event
	if err := <-pulled; err == nil {
		t.Fatal("docker's pull succeeded")
	}
	s.finish(false)
	hexDigest := strings.TrimPrefix(img.Layers[0].Digest, "sha256:")
	files := cacheFiles(t, dir)
	for _, name := range []string{hexDigest, hexDigest + ".partial"} {
		if _, ok := files["blobs/"+name]; ok {
			t.Fatalf("corrupt layer kept as %s", name)
		}
	}
}

func TestServeEndsAsFailedWhenStdinEnds(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 400_000)
	layer := img.Layers[0]
	layer.Faults(fakereg.Cut)
	layer.SetBroken(true)
	dir := t.TempDir()
	s := startServe(t, dir)

	s.send(`{"ref":"` + reg.Host + `/team/app:v1"}`)
	ready := s.next("ready")
	pulled := make(chan error, 1)
	go func() { pulled <- dockerPull(context.Background(), fakedocker.New(), ready.LocalRef) }()

	partial := filepath.Join(dir, "blobs", "sha256", strings.TrimPrefix(layer.Digest, "sha256:")+".partial")
	var kept int64
	for deadline := time.Now().Add(10 * time.Second); kept == 0; time.Sleep(10 * time.Millisecond) {
		if info, err := os.Stat(partial); err == nil {
			kept = info.Size()
		}
		if time.Now().After(deadline) {
			t.Fatal("no bytes of the layer on disk")
		}
	}
	_ = s.stdin.Close()

	if e := s.next("finished"); e.Success == nil || *e.Success {
		t.Fatalf("finished event %+v, want success false", e)
	}
	if code := s.exit(); code != 0 {
		t.Fatalf("exit code %d, want 0", code)
	}
	if info, err := os.Stat(partial); err != nil || info.Size() < kept {
		t.Fatalf("partial after the end of stdin: %v; want the %d bytes kept", err, kept)
	}
	select {
	case err := <-pulled:
		if err == nil {
			t.Fatal("docker's pull succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("docker's pull did not end with the session")
	}
}

func TestServeSendsARetryEventWhenADownloadResumes(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 300_000)
	layer := img.Layers[0]
	layer.Faults(fakereg.Cut)
	dir := t.TempDir()
	s := startServe(t, dir)

	s.send(`{"ref":"` + reg.Host + `/team/app:v1"}`)
	ready := s.next("ready")
	if err := dockerPull(context.Background(), fakedocker.New(), ready.LocalRef); err != nil {
		t.Fatal(err)
	}
	s.finish(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	want := event{Event: "retry", ID: strings.TrimPrefix(layer.Digest, "sha256:")[:12], Offset: int64(len(layer.Data) / 2)}
	if len(s.retries) != 1 || s.retries[0] != want {
		t.Fatalf("retry events %+v, want one %+v", s.retries, want)
	}
	if !strings.Contains(s.stderr.String(), `"msg":"layer download broke, retrying"`) {
		t.Fatalf("the warning is no longer on stderr: %s", s.stderr.String())
	}
}

func TestServeMarksARefusalInTheFailedEvent(t *testing.T) {
	reg := fakereg.New(t)
	reg.TokenTTL = time.Hour
	reg.Username, reg.Password = "alice", secret
	reg.AddImage("team/app", "v1", 100_000)
	s := startServe(t, t.TempDir())

	s.send(`{"ref":"` + reg.Host + `/team/app:v1","credential":{"username":"alice","password":"wrong"}}`)
	ready := s.next("ready")
	pulled := make(chan error, 1)
	go func() { pulled <- dockerPull(context.Background(), fakedocker.New(), ready.LocalRef) }()
	if e := s.next("failed"); !e.Refused || !strings.Contains(e.Error, "refused the login for alice") {
		t.Fatalf("failed event %+v, want a refused login", e)
	}
	if err := <-pulled; err == nil {
		t.Fatal("docker's pull succeeded")
	}
	s.finish(false)
}

func TestServeEndsAsFailedWhenItsContextEnds(t *testing.T) {
	reg := fakereg.New(t)
	reg.AddImage("team/app", "v1", 400_000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := startServeCtx(t, ctx, t.TempDir())

	s.send(`{"ref":"` + reg.Host + `/team/app:v1"}`)
	s.next("ready")
	cancel() // as SIGINT or SIGTERM do
	if e := s.next("finished"); e.Success == nil || *e.Success {
		t.Fatalf("finished event %+v, want success false", e)
	}
	if code := s.exit(); code != 0 {
		t.Fatalf("exit code %d, want 0", code)
	}
}

func TestServeLogsInWithTheRequestsCredentialAndPrintsNoSecret(t *testing.T) {
	reg := fakereg.New(t)
	reg.TokenTTL = time.Hour
	reg.Redirect = true
	reg.Username, reg.Password = "alice", secret
	reg.AddImage("team/app", "v1", 200_000)
	dir := t.TempDir()
	s := startServe(t, dir)

	s.send(`{"ref":"` + reg.Host + `/team/app:v1","credential":{"username":"alice","password":"` + secret + `"},"stallLimit":"30s"}`)
	ready := s.next("ready")
	if err := dockerPull(context.Background(), fakedocker.New(), ready.LocalRef); err != nil {
		t.Fatal(err)
	}
	s.finish(true)

	loggedIn := false
	for _, r := range reg.Log() {
		if r.Host == "token" && r.Status == 200 && strings.HasPrefix(r.Auth, "Basic ") {
			loggedIn = true
		}
	}
	if !loggedIn {
		t.Fatal("the registry never got the login")
	}
	for name, out := range map[string]string{"stdout": s.stdout.String(), "stderr": s.stderr.String()} {
		if strings.Contains(out, secret) {
			t.Fatalf("%s carries the password", name)
		}
	}
}

func TestServeAllTagsListsThroughTheProxyAndPullsEveryTag(t *testing.T) {
	reg := fakereg.New(t)
	reg.AbsoluteLinks = true
	reg.TagsPerPage = 1
	v1 := reg.AddImage("team/app", "v1", 100_000)
	v2 := reg.AddImage("team/app", "v2", 150_000)
	dir := t.TempDir()
	s := startServe(t, dir)

	s.send(`{"ref":"` + reg.Host + `/team/app","allTags":true}`)
	ready := s.next("ready")
	proxy, path, _ := strings.Cut(ready.LocalRef, "/")
	if !strings.HasPrefix(proxy, "127.0.0.1:") || path != "team/app" {
		t.Fatalf("localRef = %q, want 127.0.0.1:<port>/team/app", ready.LocalRef)
	}
	d := fakedocker.New()
	if err := dockerPull(context.Background(), d, ready.LocalRef); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"http://" + proxy + "/v2/team/app/tags/list",
		"http://" + proxy + "/v2/team/app/tags/list?last=v1&n=1",
	}
	if !slices.Equal(d.TagLists, want) {
		t.Fatalf("tag list pages = %q, want %q", d.TagLists, want)
	}
	for _, img := range []*fakereg.Image{v1, v2} {
		if n := d.Gets(img.Layers[0].Digest); n != 1 {
			t.Fatalf("%s: %d GETs of its layer, want 1", img.Tag, n)
		}
	}
	s.finish(true)
	if files := cacheFiles(t, dir); len(files) != 0 {
		t.Fatalf("cache not emptied: %v", files)
	}
}

func TestServeRefusesABadRequest(t *testing.T) {
	cred := `"credential":{"username":"alice","password":"` + secret + `"}`
	digest := "sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name, line, msg string
	}{
		{"not JSON", `{"ref":"team/app:v1",` + cred, msgMalformed},
		{"no ref", `{` + cred + `}`, msgMalformed},
		{"invalid ref", `{"ref":"team/App:v1",` + cred + `}`, msgRef},
		{"all tags of a tag", `{"ref":"team/app:v1","allTags":true,` + cred + `}`, msgAllTags},
		{"all tags of a digest", `{"ref":"team/app@` + digest + `","allTags":true,` + cred + `}`, msgAllTags},
		{"stall limit", `{"ref":"team/app:v1","stallLimit":"` + secret + `",` + cred + `}`, msgStallLimit},
		{"empty stdin", "", msgMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := startServe(t, t.TempDir())
			if tc.line == "" {
				_ = s.stdin.Close()
			} else {
				s.send(tc.line)
			}
			if e := s.next("failed"); e.Error != tc.msg {
				t.Fatalf("failed event %+v, want the message %q", e, tc.msg)
			}
			if code := s.exit(); code != 1 {
				t.Fatalf("exit code %d, want 1", code)
			}
			for name, out := range map[string]string{"stdout": s.stdout.String(), "stderr": s.stderr.String()} {
				if strings.Contains(out, secret) {
					t.Fatalf("%s carries the password", name)
				}
			}
		})
	}
}

func TestServeWithArgumentsPrintsItsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve", "extra"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("exit code %d, want 2", code)
	}
	if !strings.HasPrefix(stderr.String(), "usage: justpull serve") || stdout.Len() != 0 {
		t.Errorf("stdout %q, stderr %q; want the usage on stderr", stdout.String(), stderr.String())
	}
}
