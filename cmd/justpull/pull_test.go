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
	reg.AddImage("team/app", "v1", 100_000)
	reg.AddImage("team/app", "v2", 120_000)
	f := newFake(t)
	f.tags = []string{"v1", "v2"}

	res := runPull(t, context.Background(), f, nil, nil, "-a", reg.Host+"/team/app")
	if res.code != 0 || strings.Contains(res.stdout, "Using default tag") || !strings.HasSuffix(res.stdout, "Status: Downloaded newer image for "+reg.Host+"/team/app:v2\n"+reg.Host+"/team/app\n") {
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

func TestProxyArgs(t *testing.T) {
	want := "run -i --rm --pull never --name justpull-0a1b2c3d --network host --user 65534:65534 --cap-drop ALL --security-opt no-new-privileges --read-only -v justpull-cache:/cache ghcr.io/cryguy/justpull:v1 serve --cache /cache"
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
