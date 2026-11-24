package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"

	"github.com/cryguy/justpull"
	"github.com/cryguy/justpull/internal/engine"
	"github.com/cryguy/justpull/internal/fakedocker"
	"github.com/cryguy/justpull/internal/fakereg"
)

// fakeDocker runs serve in-process as the proxy container and pulls with
// fakedocker, which writes dockerd's stream.
type fakeDocker struct {
	dir string
	d   *fakedocker.Docker
	// tags is ListTags' answer, as tags of the repository asked for.
	tags []string
	// ids are the image IDs of the user's names before the pull, by
	// reference; pulled are those Docker gives the proxy's local names, by
	// tag or digest.
	ids, pulled map[string]string
	// pullErr, if set, may fail a pull as the daemon's API does.
	pullErr func(image string) error
	// failStart makes the container end at once with this on stderr.
	failStart string

	mu       sync.Mutex
	pulls    []pullCall
	tagged   [][2]string
	untagged []string
	stdin    syncBuffer // every line sent to the proxy
	serveErr syncBuffer
}

type pullCall struct {
	image, tag, platform string
	allTags              bool
	auth                 *engine.RegistryAuth
}

func newFake(t *testing.T) *fakeDocker {
	return &fakeDocker{dir: t.TempDir(), d: fakedocker.New()}
}

func (f *fakeDocker) EnsureProxy(_ context.Context, progress io.Writer) (string, error) {
	fmt.Fprintln(progress, "proxy image progress")
	return "ghcr.io/cryguy/justpull:test", nil
}

type recordingWriter struct {
	io.WriteCloser
	log *syncBuffer
}

func (w recordingWriter) Write(p []byte) (int, error) {
	_, _ = w.log.Write(p)
	return w.WriteCloser.Write(p)
}

func (f *fakeDocker) StartProxy(string) (*proxyProc, error) {
	if f.failStart != "" {
		return &proxyProc{
			stdin:  recordingWriter{nopCloser{io.Discard}, &f.stdin},
			stdout: strings.NewReader(""),
			wait:   func() error { return errors.New("exit status 125") },
			kill:   func() {},
			stderr: func() string { return f.failStart + "\n" },
		}, nil
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		serve(ctx, []string{"--cache", f.dir}, inR, outW, &f.serveErr)
		_ = outW.Close()
		_ = inR.CloseWithError(io.ErrClosedPipe)
		close(done)
	}()
	return &proxyProc{
		stdin:  recordingWriter{inW, &f.stdin},
		stdout: outR,
		wait:   func() error { <-done; return nil },
		kill:   cancel,
		stderr: f.serveErr.String,
	}, nil
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func (f *fakeDocker) Pull(ctx context.Context, image, tag, platform string, allTags bool, auth *engine.RegistryAuth) (io.ReadCloser, error) {
	f.mu.Lock()
	f.pulls = append(f.pulls, pullCall{image, tag, platform, allTags, auth})
	f.mu.Unlock()
	if f.pullErr != nil {
		if err := f.pullErr(image); err != nil {
			return nil, err
		}
	}
	ref := image
	switch {
	case allTags:
	case strings.HasPrefix(tag, "sha256:"):
		ref += "@" + tag
	default:
		ref += ":" + tag
	}
	return f.d.Pull(ctx, ref)
}

func (f *fakeDocker) Tag(_ context.Context, source, target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tagged = append(f.tagged, [2]string{source, target})
	return nil
}

func (f *fakeDocker) Untag(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.untagged = append(f.untagged, ref)
	return nil
}

func (f *fakeDocker) ListTags(_ context.Context, repo string) ([]string, error) {
	var refs []string
	for _, tag := range f.tags {
		refs = append(refs, repo+":"+tag)
	}
	return refs, nil
}

// local reports whether repo is the proxy's local repository: the first
// pull's.
func (f *fakeDocker) local(repo string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pulls) > 0 && f.pulls[0].image == repo
}

func (f *fakeDocker) ImageID(_ context.Context, ref string) (string, bool, error) {
	named, err := reference.ParseNormalizedNamed(ref)
	if err != nil {
		return "", false, err
	}
	if f.local(named.Name()) {
		id, ok := f.pulled[tagOrDigest(named)]
		return id, ok, nil
	}
	id, ok := f.ids[ref]
	return id, ok, nil
}

func (f *fakeDocker) TagIDs(_ context.Context, repo string) (map[string]string, error) {
	ids := map[string]string{}
	if f.local(repo) {
		for tag, id := range f.pulled {
			if !strings.HasPrefix(tag, "sha256:") {
				ids[tag] = id
			}
		}
		return ids, nil
	}
	for ref, id := range f.ids {
		if tag, ok := strings.CutPrefix(ref, repo+":"); ok {
			ids[tag] = id
		}
	}
	return ids, nil
}

// proxyRepo is the local repository of the first pull: the proxy's.
func (f *fakeDocker) proxyRepo(t *testing.T) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pulls) == 0 || !strings.HasPrefix(f.pulls[0].image, "127.0.0.1:") {
		t.Fatalf("pulls %+v, want a first pull from the proxy", f.pulls)
	}
	return f.pulls[0].image
}

func (f *fakeDocker) finishSent(t *testing.T, success bool) {
	t.Helper()
	want := fmt.Sprintf(`{"cmd":"finish","success":%v}`+"\n", success)
	if !strings.HasSuffix(f.stdin.String(), want) {
		t.Fatalf("lines sent to the proxy %q, want the last %q", f.stdin.String(), want)
	}
}

type fakeConfig struct {
	cred     justpull.Credential
	warnings []string
}

func (c *fakeConfig) Credential(context.Context, string) (justpull.Credential, error) {
	return c.cred, nil
}

func (c *fakeConfig) Warnings() []string { return c.warnings }

type result struct {
	code           int
	stdout, stderr string
}

// runPull runs the command with f as Docker; f nil fails the test if Docker
// is used.
func runPull(t *testing.T, ctx context.Context, f *fakeDocker, cfg *fakeConfig, environ map[string]string, args ...string) result {
	t.Helper()
	var stdout, stderr syncBuffer
	if cfg == nil {
		cfg = &fakeConfig{}
	}
	e := &env{
		stdout: &stdout,
		stderr: &stderr,
		getenv: func(k string) string { return environ[k] },
		config: func() (loginConfig, error) { return cfg, nil },
		docker: func() (dockerAPI, error) {
			if f == nil {
				t.Error("Docker was used")
				return nil, errors.New("no Docker here")
			}
			return f, nil
		},
	}
	code := pullCommand(ctx, args, e)
	return result{code, stdout.String(), stderr.String()}
}

func shortID(b *fakereg.Blob) string { return strings.TrimPrefix(b.Digest, "sha256:")[:12] }

func TestPullTagsTheImageAndPrintsAsDocker(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "latest", 200_000, 100_000)
	f := newFake(t)
	cfg := &fakeConfig{warnings: []string{"Failed to create credential store from DOCKER_AUTH_CONFIG: bad"}}

	res := runPull(t, context.Background(), f, cfg, nil, reg.Host+"/team/app")
	if res.code != 0 {
		t.Fatalf("exit code %d, stderr %q", res.code, res.stderr)
	}
	proxy := f.proxyRepo(t)
	l1, l2 := shortID(img.Layers[0]), shortID(img.Layers[1])
	want := "Using default tag: latest\n" +
		"latest: Pulling from team/app\n" +
		l1 + ": Pulling fs layer\n" + l2 + ": Pulling fs layer\n" +
		l1 + ": Download complete\n" + l1 + ": Pull complete\n" +
		l2 + ": Download complete\n" + l2 + ": Pull complete\n" +
		"Digest: " + img.Digest + "\n" +
		"Status: Downloaded newer image for " + reg.Host + "/team/app:latest\n" +
		reg.Host + "/team/app:latest\n"
	if res.stdout != want {
		t.Fatalf("stdout\n%s\nwant\n%s", res.stdout, want)
	}
	if want := cfg.warnings[0] + "\nproxy image progress\n"; res.stderr != want {
		t.Fatalf("stderr %q, want %q", res.stderr, want)
	}
	if !slices.Equal(f.tagged, [][2]string{{proxy + ":latest", reg.Host + "/team/app:latest"}}) || !slices.Equal(f.untagged, []string{proxy + ":latest"}) {
		t.Fatalf("tagged %q, untagged %q", f.tagged, f.untagged)
	}
	if p := f.pulls[0]; len(f.pulls) != 1 || p.tag != "latest" || p.platform != "" || p.allTags || p.auth != nil {
		t.Fatalf("pulls %+v, want one of the proxy's latest without a login", f.pulls)
	}
	if !strings.HasPrefix(f.stdin.String(), `{"ref":"`+reg.Host+`/team/app:latest","allTags":false,`) {
		t.Fatalf("request %q", f.stdin.String())
	}
	f.finishSent(t, true)
	if files := cacheFiles(t, f.dir); len(files) != 0 {
		t.Fatalf("cache not emptied: %v", files)
	}
}

func TestPullQuietPrintsTheReferenceOnly(t *testing.T) {
	reg := fakereg.New(t)
	reg.AddImage("team/app", "v1", 100_000)
	f := newFake(t)

	res := runPull(t, context.Background(), f, nil, nil, "-q", reg.Host+"/team/app:v1")
	if res.code != 0 || res.stdout != reg.Host+"/team/app:v1\n" || res.stderr != "" {
		t.Fatalf("exit code %d, stdout %q, stderr %q; want only the reference", res.code, res.stdout, res.stderr)
	}
	f.finishSent(t, true)
}

func TestPullDigestPullsTheRealNameWithTheLogin(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000)
	f := newFake(t)
	cfg := &fakeConfig{cred: justpull.Credential{Username: "alice", Password: secret}}
	ref := reg.Host + "/team/app@" + img.Digest

	res := runPull(t, context.Background(), f, cfg, nil, ref)
	if res.code != 0 || !strings.HasSuffix(res.stdout, "\n"+ref+"\n") {
		t.Fatalf("exit code %d, stdout %q, stderr %q", res.code, res.stdout, res.stderr)
	}
	proxy := f.proxyRepo(t)
	wantAuth := engine.RegistryAuth{Username: "alice", Password: secret, ServerAddress: reg.Host}
	if len(f.pulls) != 2 || f.pulls[1].image != reg.Host+"/team/app" || f.pulls[1].tag != img.Digest || f.pulls[1].auth == nil || *f.pulls[1].auth != wantAuth {
		t.Fatalf("pulls %+v, want the real name by digest with %+v", f.pulls, wantAuth)
	}
	if len(f.tagged) != 0 || !slices.Equal(f.untagged, []string{proxy + "@" + img.Digest}) {
		t.Fatalf("tagged %q, untagged %q", f.tagged, f.untagged)
	}
	if strings.Contains(res.stdout+res.stderr, secret) {
		t.Fatal("the password was printed")
	}
	f.finishSent(t, true)
}

func TestRegistryAuthNamesDockerHubAsDockerDoes(t *testing.T) {
	cred := justpull.Credential{IdentityToken: "refresh"}
	want := engine.RegistryAuth{IdentityToken: "refresh", ServerAddress: "https://index.docker.io/v1/"}
	if got := registryAuth(cred, "docker.io"); *got != want {
		t.Fatalf("registryAuth = %+v, want %+v", *got, want)
	}
}

func TestPullAllTagsRetagsEveryTag(t *testing.T) {
	reg := fakereg.New(t)
	v1 := reg.AddImage("team/app", "v1", 100_000)
	v2 := reg.AddImage("team/app", "v2", 120_000)
	f := newFake(t)
	f.tags = []string{"v1", "v2"}
	name := reg.Host + "/team/app"
	f.ids = map[string]string{name + ":v1": v1.Digest}
	f.pulled = map[string]string{"v1": v1.Digest, "v2": v2.Digest}

	res := runPull(t, context.Background(), f, nil, nil, "-a", name)
	if res.code != 0 || strings.Contains(res.stdout, "Using default tag") ||
		!strings.Contains(res.stdout, "\nDigest: "+v1.Digest+"\nStatus: Image is up to date for "+name+":v1\n") ||
		!strings.HasSuffix(res.stdout, "\nDigest: "+v2.Digest+"\nStatus: Downloaded newer image for "+name+":v2\n"+name+"\n") {
		t.Fatalf("exit code %d, stdout %q, stderr %q", res.code, res.stdout, res.stderr)
	}
	proxy := f.proxyRepo(t)
	if p := f.pulls[0]; !p.allTags || p.tag != "" {
		t.Fatalf("pull %+v, want every tag", p)
	}
	wantTags := [][2]string{{proxy + ":v1", reg.Host + "/team/app:v1"}, {proxy + ":v2", reg.Host + "/team/app:v2"}}
	if !slices.Equal(f.tagged, wantTags) || !slices.Equal(f.untagged, []string{proxy + ":v1", proxy + ":v2"}) {
		t.Fatalf("tagged %q, untagged %q", f.tagged, f.untagged)
	}
	f.finishSent(t, true)
}

func TestPullSaysUpToDateAsDocker(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000)
	name := reg.Host + "/team/app"
	other := "sha256:" + strings.Repeat("0", 64)
	upToDate, newer := "Status: Image is up to date for ", "Status: Downloaded newer image for "
	// The containerd store's image ID is the manifest's digest, the
	// graphdriver store's the config's. The stream's Digest line is the
	// manifest's either way.
	tests := []struct {
		name        string
		ref         string
		ids, pulled map[string]string
		want        string
	}{
		{"containerd: the same image", name + ":v1", map[string]string{name + ":v1": img.Digest}, map[string]string{"v1": img.Digest}, upToDate + name + ":v1"},
		{"containerd: another image", name + ":v1", map[string]string{name + ":v1": other}, map[string]string{"v1": img.Digest}, newer + name + ":v1"},
		// The name never held the manifest's digest: the index moved on with
		// this platform's config unchanged, or docker load gave the image.
		{"graphdriver: the same config under a manifest new to the name", name + ":v1", map[string]string{name + ":v1": img.Config.Digest}, map[string]string{"v1": img.Config.Digest}, upToDate + name + ":v1"},
		// As when the same index is pulled for another platform.
		{"graphdriver: another config", name + ":v1", map[string]string{name + ":v1": other}, map[string]string{"v1": img.Config.Digest}, newer + name + ":v1"},
		{"absent", name + ":v1", nil, map[string]string{"v1": img.Digest}, newer + name + ":v1"},
		{"a digest reference present", name + "@" + img.Digest, map[string]string{name + "@" + img.Digest: img.Digest}, map[string]string{img.Digest: img.Digest}, upToDate + name + "@" + img.Digest},
		{"a digest reference absent", name + "@" + img.Digest, nil, map[string]string{img.Digest: img.Digest}, newer + name + "@" + img.Digest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.ids, f.pulled = tt.ids, tt.pulled
			res := runPull(t, context.Background(), f, nil, nil, tt.ref)
			if want := "\nDigest: " + img.Digest + "\n" + tt.want + "\n" + tt.ref + "\n"; res.code != 0 || !strings.HasSuffix(res.stdout, want) {
				t.Fatalf("exit code %d, stdout %q, stderr %q; want the end %q", res.code, res.stdout, res.stderr, want)
			}
		})
	}
}

func TestUpToDateReadsDockersNames(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	local, _ := reference.ParseNormalizedNamed("127.0.0.1:41234/library/alpine")
	for _, tt := range []struct{ user, inspected, status, pulled string }{
		{"alpine:3.20", "docker.io/library/alpine:3.20", "alpine:3.20", "3.20"},
		{"alpine:3.20@" + digest, "docker.io/library/alpine@" + digest, "alpine@" + digest, digest},
	} {
		named, _ := reference.ParseNormalizedNamed(tt.user)
		f := newFake(t)
		f.ids = map[string]string{tt.inspected: "sha256:abc"}
		f.pulled = map[string]string{tt.pulled: "sha256:abc"}
		p := &puller{named: named}
		before, err := p.before(context.Background(), f)
		f.pulls = []pullCall{{image: local.Name()}} // the status line comes from the local name's pull
		if err != nil || !p.upToDate(context.Background(), f, local, before)(tt.status) {
			t.Errorf("%s: before %v, %v; want %s up to date", tt.user, before, err, tt.status)
		}
	}
}

// The graphdriver store ends a pull of every tag with one status for the
// repository alone; docker says up to date when no tag got another image.
func TestUpToDateAllTagsOnGraphdriver(t *testing.T) {
	named, _ := reference.ParseNormalizedNamed("alpine")
	local, _ := reference.ParseNormalizedNamed("127.0.0.1:41234/library/alpine")
	p := &puller{named: named, opts: pullOptions{all: true}}
	f := newFake(t)
	f.ids = map[string]string{"alpine:3.19": "sha256:a", "alpine:3.20": "sha256:b"}
	before, err := p.before(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	f.pulls = []pullCall{{image: local.Name(), allTags: true}}
	for _, tt := range []struct {
		name   string
		pulled map[string]string
		want   bool
	}{
		{"every tag unchanged", map[string]string{"3.19": "sha256:a", "3.20": "sha256:b"}, true},
		{"a tag changed", map[string]string{"3.19": "sha256:a", "3.20": "sha256:c"}, false},
		{"a tag new", map[string]string{"3.19": "sha256:a", "3.20": "sha256:b", "3.21": "sha256:d"}, false},
	} {
		f.pulled = tt.pulled
		if got := p.upToDate(context.Background(), f, local, before)("alpine"); got != tt.want {
			t.Errorf("%s: up to date %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestPullPrintsTheProxysCauseAndFinishesAsFailed(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 300_000)
	img.Layers[0].Faults(fakereg.Corrupt)
	f := newFake(t)

	res := runPull(t, context.Background(), f, nil, nil, reg.Host+"/team/app:v1")
	prefix := "proxy image progress\njustpull: layer " + strings.TrimSuffix(img.Layers[0].Digest, img.Layers[0].Digest[19:]) + ": digest mismatch: "
	if res.code != 1 || !strings.HasPrefix(res.stderr, prefix) || strings.Count(res.stderr, "\n") != 2 {
		t.Fatalf("exit code %d, stderr %q; want the proxy's cause and no hint", res.code, res.stderr)
	}
	if len(f.tagged) != 0 || len(f.untagged) != 0 {
		t.Fatalf("tagged %q, untagged %q after a failure", f.tagged, f.untagged)
	}
	f.finishSent(t, false)
}

func TestPullHintsALoginWhenTheRegistryRefusesIt(t *testing.T) {
	reg := fakereg.New(t)
	reg.TokenTTL = time.Hour
	reg.Username, reg.Password = "alice", secret
	reg.AddImage("team/app", "v1", 100_000)
	f := newFake(t)
	cfg := &fakeConfig{cred: justpull.Credential{Username: "alice", Password: "wrong-pw"}}

	res := runPull(t, context.Background(), f, cfg, nil, reg.Host+"/team/app:v1")
	want := "proxy image progress\njustpull: registry " + reg.Host + " refused the login for alice: 401 Unauthorized\nlog in with: docker login " + reg.Host + "\n"
	if res.code != 1 || res.stderr != want {
		t.Fatalf("exit code %d, stderr %q; want %q", res.code, res.stderr, want)
	}
	if strings.Contains(res.stdout+res.stderr, "wrong-pw") {
		t.Fatal("the password was printed")
	}
	f.finishSent(t, false)
	if got := loginHint("docker.io"); got != "log in with: docker login" {
		t.Fatalf("Docker Hub's hint %q", got)
	}
}

func TestPullPrintsDockersErrorRenamed(t *testing.T) {
	reg := fakereg.New(t)
	reg.AddImage("team/app", "v1", 100_000)
	f := newFake(t)
	f.pullErr = func(image string) error {
		return &engine.APIError{Call: "docker pull " + image, Message: "manifest for " + image + ":v1 not found: manifest unknown\n"}
	}

	res := runPull(t, context.Background(), f, nil, nil, reg.Host+"/team/app:v1")
	want := "proxy image progress\nError response from daemon: manifest for " + reg.Host + "/team/app:v1 not found: manifest unknown\n"
	if res.code != 1 || res.stderr != want {
		t.Fatalf("exit code %d, stderr %q; want %q", res.code, res.stderr, want)
	}
	f.finishSent(t, false)
}

func TestPullInterruptedFinishesAsFailed(t *testing.T) {
	reg := fakereg.New(t)
	reg.AddImage("team/app", "v1", 300_000)
	f := newFake(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.d.AfterManifest = cancel // as SIGINT does

	res := runPull(t, ctx, f, nil, nil, reg.Host+"/team/app:v1")
	if res.code != exitInterrupted || res.stderr != "proxy image progress\n" {
		t.Fatalf("exit code %d, stderr %q; want %d and nothing printed", res.code, res.stderr, exitInterrupted)
	}
	if len(f.tagged) != 0 {
		t.Fatalf("tagged %q after an interrupt", f.tagged)
	}
	f.finishSent(t, false)
}

func TestPullShowsAResumedLayer(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 1_000_000)
	layer := img.Layers[0]
	layer.Faults(fakereg.Cut)
	f := newFake(t)

	res := runPull(t, context.Background(), f, nil, nil, reg.Host+"/team/app:v1")
	if res.code != 0 {
		t.Fatalf("exit code %d, stderr %q", res.code, res.stderr)
	}
	want := shortID(layer) + ": Resuming at " + humanSize(float64(len(layer.Data)/2)) + "\n"
	if !strings.Contains(res.stdout, want) {
		t.Fatalf("stdout %q, want the line %q", res.stdout, want)
	}
}

func TestPullReportsAProxyThatDidNotStart(t *testing.T) {
	f := newFake(t)
	f.failStart = "docker: Error response from daemon: no such image"

	res := runPull(t, context.Background(), f, nil, nil, "alpine:3.20")
	want := "proxy image progress\njustpull: the proxy container did not start: docker: Error response from daemon: no such image\n"
	if res.code != 1 || res.stderr != want {
		t.Fatalf("exit code %d, stderr %q; want %q", res.code, res.stderr, want)
	}
}

func TestPullUsesDockerDefaultPlatform(t *testing.T) {
	reg := fakereg.New(t)
	reg.AddImage("team/app", "v1", 100_000)
	f := newFake(t)

	res := runPull(t, context.Background(), f, nil, map[string]string{"DOCKER_DEFAULT_PLATFORM": "linux/arm64"}, reg.Host+"/team/app:v1")
	if res.code != 0 || f.pulls[0].platform != "linux/arm64" {
		t.Fatalf("exit code %d, pulls %+v; want the platform linux/arm64", res.code, f.pulls)
	}
	opts, err := parseArgs([]string{"--platform", "linux/amd64", "alpine"}, "linux/arm64")
	if err != nil || opts.platform != "linux/amd64" {
		t.Fatalf("--platform over the default: %+v, %v", opts, err)
	}
	if help := helpText("linux/arm64"); !strings.Contains(help, `capable (default "linux/arm64")`) {
		t.Fatalf("help without the default platform:\n%s", help)
	}
}

const helpWant = `Usage:  justpull [OPTIONS] NAME[:TAG|@DIGEST]

Download an image from a registry, resuming broken downloads

Options:
  -a, --all-tags          Download all tagged images in the repository
      --platform string   Set platform if server is multi-platform capable
  -q, --quiet             Suppress verbose output
      --version           Print version information and quit
`

func TestPullArguments(t *testing.T) {
	argsErr := "justpull: 'justpull' requires 1 argument\n\nUsage:  justpull [OPTIONS] NAME[:TAG|@DIGEST]\n\nRun 'justpull --help' for more information\n"
	flagErr := func(msg string) string {
		return msg + "\n\nUsage:  justpull [OPTIONS] NAME[:TAG|@DIGEST]\n\nRun 'justpull --help' for more information\n"
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		args           []string
		code           int
		stdout, stderr string
	}{
		{nil, 1, "", argsErr},
		{[]string{"pull", "alpine"}, 1, "", argsErr},
		{[]string{"-q"}, 1, "", argsErr},
		{[]string{"-a", "alpine:3.20"}, 1, "", "tag can't be used with --all-tags/-a\n"},
		{[]string{"alpine@" + digest, "--all-tags"}, 1, "", "tag can't be used with --all-tags/-a\n"},
		{[]string{"Alpine"}, 1, "", "invalid reference format: repository name (library/Alpine) must be lowercase\n"},
		{[]string{"--", "-q"}, 1, "", "invalid reference format\n"},
		{[]string{"--bogus", "alpine"}, 125, "", flagErr("unknown flag: --bogus")},
		{[]string{"alpine", "-x"}, 125, "", flagErr("unknown shorthand flag: 'x' in -x")},
		{[]string{"-aqx", "alpine"}, 125, "", flagErr("unknown shorthand flag: 'x' in -x")},
		{[]string{"alpine", "--platform"}, 125, "", flagErr("flag needs an argument: --platform")},
		{[]string{"--quiet=maybe", "alpine"}, 125, "", flagErr(`invalid argument "maybe" for "-q, --quiet" flag: strconv.ParseBool: parsing "maybe": invalid syntax`)},
		{[]string{"---x", "alpine"}, 125, "", flagErr("bad flag syntax: ---x")},
		{[]string{"--help"}, 0, helpWant, ""},
		{[]string{"alpine", "extra", "-h"}, 0, helpWant, ""},
		{[]string{"--version"}, 0, "justpull version " + version + "\n", ""},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			res := runPull(t, context.Background(), nil, nil, nil, tt.args...)
			if res.code != tt.code || res.stdout != tt.stdout || res.stderr != tt.stderr {
				t.Fatalf("exit code %d, stdout %q, stderr %q\nwant %d, %q, %q", res.code, res.stdout, res.stderr, tt.code, tt.stdout, tt.stderr)
			}
		})
	}
}

func TestParseArgsAsDocker(t *testing.T) {
	tests := []struct {
		args []string
		want pullOptions
	}{
		{[]string{"-aq", "alpine"}, pullOptions{remote: "alpine", all: true, quiet: true}},
		{[]string{"alpine", "-q"}, pullOptions{remote: "alpine", quiet: true}},
		{[]string{"--platform=linux/arm64", "alpine"}, pullOptions{remote: "alpine", platform: "linux/arm64"}},
		{[]string{"alpine", "--platform", "linux/arm64"}, pullOptions{remote: "alpine", platform: "linux/arm64"}},
		{[]string{"-q", "--", "alpine"}, pullOptions{remote: "alpine", quiet: true}},
		{[]string{"-a=false", "--quiet=true", "alpine"}, pullOptions{remote: "alpine", quiet: true}},
		{[]string{"--disable-content-trust", "alpine"}, pullOptions{remote: "alpine"}},
	}
	for _, tt := range tests {
		got, err := parseArgs(tt.args, "")
		if err != nil || got != tt.want {
			t.Errorf("%q: %+v, %v; want %+v", tt.args, got, err, tt.want)
		}
	}
}

func TestRenamerHidesTheProxy(t *testing.T) {
	local, _ := reference.ParseNormalizedNamed("127.0.0.1:41234/library/alpine:latest")
	user, _ := reference.ParseNormalizedNamed("alpine")
	rename := renamer(local, user)
	for in, want := range map[string]string{
		"Status: Downloaded newer image for 127.0.0.1:41234/library/alpine:latest":       "Status: Downloaded newer image for alpine:latest",
		`Get "http://127.0.0.1:41234/v2/library/alpine/manifests/latest": EOF`:           `Get "http://registry-1.docker.io/v2/library/alpine/manifests/latest": EOF`,
		"manifest for 127.0.0.1:41234/library/alpine@sha256:abc not found: unknown name": "manifest for alpine@sha256:abc not found: unknown name",
	} {
		if got := rename(in); got != want {
			t.Errorf("rename(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIDRenamerNamesTheReferenceAsDockerd(t *testing.T) {
	const dgst = "sha256:beefdbd8a1da6d2915566fde36db9db0b524eb737fc57cd1367effd16dc0d06d"
	tests := []struct{ local, user, id, want string }{
		{"127.0.0.1:41234/library/alpine@" + dgst, "alpine@" + dgst, "127.0.0.1:41234/library/alpine@" + dgst, "docker.io/library/alpine@" + dgst},
		{"127.0.0.1:39869/e2e/pinned@" + dgst, "127.0.0.1:44627/e2e/pinned@" + dgst, "127.0.0.1:39869/e2e/pinned@" + dgst, "127.0.0.1:44627/e2e/pinned@" + dgst},
		{"127.0.0.1:41234/library/alpine:3.20", "alpine:3.20", "3.20", "3.20"},
		{"127.0.0.1:41234/library/alpine:latest", "alpine:latest", "latest", "latest"},
		{"127.0.0.1:41234/library/alpine@" + dgst, "alpine@" + dgst, "0a9a5dfd008f", "0a9a5dfd008f"},
	}
	for _, tt := range tests {
		local, _ := reference.ParseNormalizedNamed(tt.local)
		user, _ := reference.ParseNormalizedNamed(tt.user)
		if got := idRenamer(local, user)(tt.id); got != tt.want {
			t.Errorf("idRenamer(%s, %s)(%q) = %q, want %q", tt.local, tt.user, tt.id, got, tt.want)
		}
	}
}

func TestProxyArgs(t *testing.T) {
	want := "run -i --rm --pull never --name justpull-0a1b2c3d --network host --user 65534:65534 --cap-drop ALL --security-opt no-new-privileges --read-only -v justpull-cache:/cache ghcr.io/cryguy/justpull:v1 --serve --cache /cache"
	if got := strings.Join(proxyArgs("ghcr.io/cryguy/justpull:v1", "justpull-0a1b2c3d"), " "); got != want {
		t.Fatalf("docker %s\nwant   %s", got, want)
	}
}

func TestBuildVersion(t *testing.T) {
	for _, tt := range []struct{ linked, module, want string }{
		{"v0.2.0", "v0.1.0", "v0.2.0"},
		{"dev", "v1.2.3", "v1.2.3"},
		{"dev", "(devel)", "dev"},
		{"dev", "", "dev"},
		{"dev", "v0.0.0-20261123192653-4dc4fb521066", "dev"},
		{"dev", "v1.2.4-0.20261123192653-4dc4fb521066", "dev"},
		{"dev", "v1.2.3+dirty", "dev"},
	} {
		if got := buildVersion(tt.linked, tt.module); got != tt.want {
			t.Errorf("buildVersion(%q, %q) = %q, want %q", tt.linked, tt.module, got, tt.want)
		}
	}
}
