package justpull

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cryguy/justpull/internal/fakedocker"
	"github.com/cryguy/justpull/internal/fakereg"
)

func newPullCache(t *testing.T, stall time.Duration) *Cache {
	t.Helper()
	return newPullCacheOpts(t, Options{StallLimit: stall})
}

func newPullCacheOpts(t *testing.T, opts Options) *Cache {
	t.Helper()
	return newPullCacheIn(t, t.TempDir(), opts)
}

func newPullCacheIn(t *testing.T, dir string, opts Options) *Cache {
	t.Helper()
	c, err := New(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	c.idleTimeout = 200 * time.Millisecond
	c.retryBase = 10 * time.Millisecond
	c.retryMax = 50 * time.Millisecond
	c.followPoll = 10 * time.Millisecond
	return c
}

// cacheFiles lists the blob files by name, the saved manifests as
// "m/<name>" and the lock files as "l/<name>".
func cacheFiles(t *testing.T, c *Cache) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for prefix, dir := range map[string]string{"": c.dir, "m/": c.mdir, "l/": c.ldir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			info, _ := e.Info()
			out[prefix+e.Name()] = info.Size()
		}
	}
	return out
}

func registryBlobRequests(reg *fakereg.Registry, digest, host string) []fakereg.Request {
	var out []fakereg.Request
	for _, r := range reg.Log() {
		if r.Host == host && strings.HasSuffix(r.Path, strings.TrimPrefix(digest, "sha256:")) {
			out = append(out, r)
		}
	}
	return out
}

func TestPullTagsUntagsAndCleansUp(t *testing.T) {
	reg := fakereg.New(t)
	reg.TokenTTL = time.Hour
	reg.Redirect = true
	img := reg.AddImage("team/app", "v1", 300_000, 200_000)
	c := newPullCache(t, 5*time.Second)
	d := fakedocker.New()
	var progress []Progress

	err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", func(p Progress) { progress = append(progress, p) })
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Pulls) != 1 || !strings.HasPrefix(d.Pulls[0], "127.0.0.1:") || !strings.HasSuffix(d.Pulls[0], "/team/app:v1") {
		t.Fatalf("docker pulled %v, want the localhost proxy name", d.Pulls)
	}
	if len(d.Tags) != 1 || d.Tags[0] != [2]string{d.Pulls[0], reg.Host + "/team/app:v1"} {
		t.Fatalf("tags = %v", d.Tags)
	}
	if len(d.Untags) != 1 || d.Untags[0] != d.Pulls[0] {
		t.Fatalf("untags = %v", d.Untags)
	}
	if files := cacheFiles(t, c); len(files) != 0 {
		t.Fatalf("cache not emptied: %v", files)
	}
	var want int64
	for _, l := range img.Layers {
		want += int64(len(l.Data))
	}
	if last := progress[len(progress)-1]; last.Done != want || last.Total != want {
		t.Fatalf("last progress %+v, want %d/%d", last, want, want)
	}
	for _, p := range progress {
		if p.Done > 0 && p.Total != want {
			t.Fatalf("progress %+v: the total must be the full %d once bytes flow", p, want)
		}
	}
	for _, r := range reg.Log() {
		if r.Host == "storage" && r.Auth != "" {
			t.Fatalf("storage host got Authorization %q", r.Auth)
		}
	}
}

func TestUpstreamBreaksNeverReachDocker(t *testing.T) {
	faults := []fakereg.Fault{fakereg.Cut, fakereg.Reset, fakereg.Stall, fakereg.IgnoreRange, fakereg.WrongStart}
	for _, f := range faults {
		t.Run(fmt.Sprint(f), func(t *testing.T) {
			reg := fakereg.New(t)
			img := reg.AddImage("team/app", "v1", 400_000)
			layer := img.Layers[0]
			layer.Faults(fakereg.Cut, f) // the first break makes the second GET a resume
			c := newPullCache(t, 5*time.Second)
			d := fakedocker.New()

			if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil); err != nil {
				t.Fatal(err)
			}
			if n := d.Gets(layer.Digest); n != 1 {
				t.Fatalf("docker made %d GETs for the layer, want 1 unbroken GET", n)
			}
			reqs := registryBlobRequests(reg, layer.Digest, "registry")
			if len(reqs) < 2 || reqs[1].Range == "" {
				t.Fatalf("registry requests %+v, want resumes with Range", reqs)
			}
		})
	}
}

func TestExpiredStorageLinkIsReplacedFromTheRegistry(t *testing.T) {
	reg := fakereg.New(t)
	reg.Redirect = true
	reg.LinkTTL = 100 * time.Millisecond
	img := reg.AddImage("team/app", "v1", 400_000)
	layer := img.Layers[0]
	layer.Faults(fakereg.Stall) // quiet past the link's life; the idle timeout reconnects
	c := newPullCache(t, 5*time.Second)

	if err := c.Pull(context.Background(), fakedocker.New(), reg.Host+"/team/app:v1", nil); err != nil {
		t.Fatal(err)
	}
	for _, r := range registryBlobRequests(reg, layer.Digest, "storage") {
		if r.Status == http.StatusForbidden {
			t.Fatalf("an expired storage link was reused: %+v", r)
		}
	}
	if n := len(registryBlobRequests(reg, layer.Digest, "registry")); n != 2 {
		t.Fatalf("registry blob GETs = %d, want 2 (one per attempt)", n)
	}
}

func TestExpiredTokenMidPullIsRenewed(t *testing.T) {
	reg := fakereg.New(t)
	reg.TokenTTL = 100 * time.Millisecond
	img := reg.AddImage("team/app", "v1", 400_000)
	img.Layers[0].Faults(fakereg.Stall)
	c := newPullCache(t, 5*time.Second)

	if err := c.Pull(context.Background(), fakedocker.New(), reg.Host+"/team/app:v1", nil); err != nil {
		t.Fatal(err)
	}
	tokens := 0
	for _, r := range reg.Log() {
		if r.Host == "token" {
			tokens++
		}
	}
	if tokens < 2 {
		t.Fatalf("token requests = %d, want a renewal", tokens)
	}
}

func TestCorruptLayerFailsThePullAndLeavesNoFile(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 300_000)
	img.Layers[0].Faults(fakereg.Corrupt)
	c := newPullCache(t, 5*time.Second)
	d := fakedocker.New()

	err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil)
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("err = %v, want a digest mismatch", err)
	}
	if len(d.Tags) != 0 {
		t.Fatal("a failed pull must not tag")
	}
	hexDigest := strings.TrimPrefix(img.Layers[0].Digest, "sha256:")
	files := cacheFiles(t, c)
	if _, ok := files[hexDigest]; ok {
		t.Fatal("corrupt layer kept as verified")
	}
	if _, ok := files[hexDigest+".partial"]; ok {
		t.Fatal("corrupt layer kept as partial")
	}
}

func TestFailedPullIsResumedByTheNextPull(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000, 400_000)
	small, big := img.Layers[0], img.Layers[1]
	big.Faults(fakereg.Cut)
	big.SetBroken(true)
	c := newPullCache(t, 400*time.Millisecond)

	start := time.Now()
	err := c.Pull(context.Background(), fakedocker.New(), reg.Host+"/team/app:v1", nil)
	if err == nil || !strings.Contains(err.Error(), "no new bytes for 400ms") {
		t.Fatalf("err = %v, want the stall limit", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("failing took %s", took)
	}
	files := cacheFiles(t, c)
	kept := files[strings.TrimPrefix(big.Digest, "sha256:")+".partial"]
	if kept == 0 || kept >= int64(len(big.Data)) {
		t.Fatalf("partial kept %d bytes of %d: %v", kept, len(big.Data), files)
	}
	if _, ok := files[strings.TrimPrefix(small.Digest, "sha256:")]; !ok {
		t.Fatalf("finished layer not kept: %v", files)
	}

	big.SetBroken(false)
	before := len(reg.Log())
	if err := c.Pull(context.Background(), fakedocker.New(), reg.Host+"/team/app:v1", nil); err != nil {
		t.Fatal(err)
	}
	var resumed, refetchedSmall bool
	for _, r := range reg.Log()[before:] {
		if strings.HasSuffix(r.Path, strings.TrimPrefix(big.Digest, "sha256:")) && r.Range == fmt.Sprintf("bytes=%d-", kept) {
			resumed = true
		}
		if strings.HasSuffix(r.Path, strings.TrimPrefix(small.Digest, "sha256:")) {
			refetchedSmall = true
		}
	}
	if !resumed || refetchedSmall {
		t.Fatalf("resumed at %d: %v; refetched the finished layer: %v", kept, resumed, refetchedSmall)
	}
	if files := cacheFiles(t, c); len(files) != 0 {
		t.Fatalf("cache not emptied after success: %v", files)
	}
}

// The containerd store can still hold a failed attempt's finished layers
// when the next attempt starts, and then never asks for them.
func TestSuccessRemovesCachedLayersDockerSkipped(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000, 400_000)
	small, big := img.Layers[0], img.Layers[1]
	big.Faults(fakereg.Cut)
	big.SetBroken(true)
	c := newPullCache(t, 400*time.Millisecond)
	d := fakedocker.New()
	d.ReuseManifests = true
	if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil); err == nil {
		t.Fatal("first pull should fail")
	}

	big.SetBroken(false)
	d.Have[small.Digest] = true
	before := d.Gets(small.Digest)
	if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil); err != nil {
		t.Fatal(err)
	}
	if d.Gets(small.Digest) != before {
		t.Fatal("docker asked for a layer it has")
	}
	if files := cacheFiles(t, c); len(files) != 0 {
		t.Fatalf("cache not emptied after success: %v", files)
	}
}

func TestCancelKeepsThePartial(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 400_000)
	img.Layers[0].Faults(fakereg.Cut)
	img.Layers[0].SetBroken(true)
	c := newPullCache(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := c.Pull(ctx, fakedocker.New(), reg.Host+"/team/app:v1", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context's error", err)
	}
	if kept := cacheFiles(t, c)[strings.TrimPrefix(img.Layers[0].Digest, "sha256:")+".partial"]; kept == 0 {
		t.Fatal("cancel lost the partial download")
	}
}

func TestDeniedFailsAtOnce(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "latest", 100_000)
	img.Layers[0].Faults(fakereg.Deny)
	c := newPullCache(t, 10*time.Second)

	start := time.Now()
	err := c.Pull(context.Background(), fakedocker.New(), reg.Host+"/team/app", nil)
	if err == nil || !strings.Contains(err.Error(), "DENIED") {
		t.Fatalf("err = %v, want DENIED", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s; a refusal must not wait for the stall limit", took)
	}
}

// privateRegistry is a fakereg that wants alice's login: for its token
// when tokenTTL is set, as Basic auth on every request otherwise.
func privateRegistry(t *testing.T, tokenTTL time.Duration) *fakereg.Registry {
	reg := fakereg.New(t)
	reg.TokenTTL = tokenTTL
	reg.Redirect = true
	reg.Username, reg.Password = "alice", "s3cret-pw"
	reg.RefreshToken = "s3cret-refresh"
	reg.AddImage("team/app", "v1", 200_000, 100_000)
	return reg
}

// credentialLog answers every Credentials call with cred (or err) and keeps
// the registry names it was asked for.
type credentialLog struct {
	cred Credential
	err  error

	mu    sync.Mutex
	asked []string
}

func (l *credentialLog) credentials(_ context.Context, registry string) (Credential, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked = append(l.asked, registry)
	return l.cred, l.err
}

func (l *credentialLog) names() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.asked...)
}

func pullWith(t *testing.T, reg *fakereg.Registry, credentials func(context.Context, string) (Credential, error)) error {
	t.Helper()
	c := newPullCacheOpts(t, Options{StallLimit: 10 * time.Second, Credentials: credentials})
	return c.Pull(context.Background(), fakedocker.New(), reg.Host+"/team/app:v1", nil)
}

func TestPullLogsInToAPrivateRegistry(t *testing.T) {
	cases := []struct {
		name     string
		tokenTTL time.Duration
		cred     Credential
		// login reports whether the log shows the login used the way
		// this mode must.
		login func(log []fakereg.Request) bool
	}{
		{"bearer with password", time.Hour, Credential{Username: "alice", Password: "s3cret-pw"}, func(log []fakereg.Request) bool {
			for _, r := range log {
				if r.Host == "token" && r.Status == http.StatusOK && r.Method == http.MethodGet && strings.HasPrefix(r.Auth, "Basic ") {
					return true
				}
			}
			return false
		}},
		{"bearer with identity token", time.Hour, Credential{IdentityToken: "s3cret-refresh"}, func(log []fakereg.Request) bool {
			posts := 0
			for _, r := range log {
				if r.Host == "token" && r.Method != http.MethodPost {
					return false
				}
				if r.Host == "token" && r.Status == http.StatusOK {
					posts++
				}
			}
			return posts > 0
		}},
		{"basic", 0, Credential{Username: "alice", Password: "s3cret-pw"}, func(log []fakereg.Request) bool {
			basic := 0
			for _, r := range log {
				if r.Host == "token" {
					return false
				}
				if r.Host == "registry" && r.Status == http.StatusOK && strings.HasPrefix(r.Auth, "Basic ") {
					basic++
				}
			}
			return basic > 0
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := privateRegistry(t, tc.tokenTTL)
			creds := &credentialLog{cred: tc.cred}

			if err := pullWith(t, reg, creds.credentials); err != nil {
				t.Fatal(err)
			}
			log := reg.Log()
			if !tc.login(log) {
				t.Fatal("the log does not show the login used as this mode must")
			}
			storage := 0
			for _, r := range log {
				if r.Host != "storage" {
					continue
				}
				storage++
				if r.Auth != "" {
					t.Fatalf("storage request %s carried an Authorization header", r.Path)
				}
			}
			if storage == 0 {
				t.Fatal("no blob came from the storage host")
			}
			names := creds.names()
			if len(names) == 0 {
				t.Fatal("Credentials was never called")
			}
			for _, n := range names {
				if n != reg.Host {
					t.Fatalf("Credentials asked for %q, want %q", n, reg.Host)
				}
			}
		})
	}
}

func TestWrongLoginFailsAtOnce(t *testing.T) {
	cases := []struct {
		name     string
		tokenTTL time.Duration
		cred     Credential
		secret   string
	}{
		{"bearer with password", time.Hour, Credential{Username: "alice", Password: "wrong-pw"}, "wrong-pw"},
		{"bearer with identity token", time.Hour, Credential{IdentityToken: "wrong-refresh"}, "wrong-refresh"},
		{"basic", 0, Credential{Username: "alice", Password: "wrong-pw"}, "wrong-pw"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := privateRegistry(t, tc.tokenTTL)
			creds := &credentialLog{cred: tc.cred}

			start := time.Now()
			err := pullWith(t, reg, creds.credentials)
			if err == nil || !isPermanent(err) || !strings.Contains(err.Error(), "refused the login") || !errors.Is(err, ErrRefused) {
				t.Fatalf("err = %v, want a permanent refused login", err)
			}
			if strings.Contains(err.Error(), tc.secret) {
				t.Fatal("the error carries the secret")
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("took %s; a refused login must not be retried", took)
			}
		})
	}
}

func TestPrivateRegistryWithoutCredentialsIsRefused(t *testing.T) {
	for _, tokenTTL := range []time.Duration{time.Hour, 0} {
		t.Run(fmt.Sprintf("token TTL %s", tokenTTL), func(t *testing.T) {
			reg := privateRegistry(t, tokenTTL)

			err := pullWith(t, reg, nil)
			if err == nil || !isPermanent(err) || !strings.Contains(err.Error(), "refused access to team/app") || !errors.Is(err, ErrRefused) {
				t.Fatalf("err = %v, want the registry's refusal", err)
			}
		})
	}
}

func TestCredentialsAreAskedOnlyOnAChallenge(t *testing.T) {
	reg := fakereg.New(t)
	reg.AddImage("team/app", "v1", 100_000)
	creds := &credentialLog{err: errors.New("must not be asked")}

	if err := pullWith(t, reg, creds.credentials); err != nil {
		t.Fatal(err)
	}
	if n := len(creds.names()); n != 0 {
		t.Fatalf("Credentials called %d times by a registry that never challenged", n)
	}
}

func TestCredentialsErrorFailsThePull(t *testing.T) {
	reg := privateRegistry(t, time.Hour)
	errHelper := errors.New("credential helper exited with status 1")
	creds := &credentialLog{err: errHelper}

	err := pullWith(t, reg, creds.credentials)
	if !errors.Is(err, errHelper) || !strings.Contains(err.Error(), "credentials for "+reg.Host) {
		t.Fatalf("err = %v, want the helper's error for %s", err, reg.Host)
	}
}

func TestLayersDockerHasAreNotDownloaded(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000, 200_000)
	c := newPullCache(t, 5*time.Second)
	d := fakedocker.New()
	d.Have[img.Layers[0].Digest] = true
	var last Progress

	if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", func(p Progress) { last = p }); err != nil {
		t.Fatal(err)
	}
	if n := len(registryBlobRequests(reg, img.Layers[0].Digest, "registry")); n != 0 {
		t.Fatalf("registry served the layer Docker had %d times", n)
	}
	if want := int64(len(img.Layers[1].Data)); last.Total != want || last.Done != want {
		t.Fatalf("progress %+v, want %d/%d", last, want, want)
	}
}

func TestDigestReferenceIsPulledAgainInsteadOfTagged(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000)
	c := newPullCache(t, 5*time.Second)
	d := fakedocker.New()
	ref := reg.Host + "/team/app@" + img.Digest

	if err := c.Pull(context.Background(), d, ref, nil); err != nil {
		t.Fatal(err)
	}
	if len(d.Pulls) != 2 || !strings.HasSuffix(d.Pulls[0], "/team/app@"+img.Digest) || d.Pulls[1] != ref {
		t.Fatalf("pulls = %v", d.Pulls)
	}
	if len(d.Tags) != 0 || len(d.Untags) != 1 {
		t.Fatalf("tags %v untags %v", d.Tags, d.Untags)
	}
}

func TestProxyServesOnlyItsRepository(t *testing.T) {
	c := newPullCache(t, time.Second)
	tgt, err := parseRef("127.0.0.1:1/team/app:v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	srv := httptest.NewServer(newProxy(ctx, c, tgt, cancel))
	defer srv.Close()

	for path, want := range map[string]int{
		"/v2/":                         200,
		"/v2/other/app/manifests/v1":   404,
		"/v2/other/app/blobs/sha256:x": 404,
		"/v2/other/app/tags/list":      404,
		"/v2/team/app/blobs/sha512:ab": 404,
		"/v2/team/app/nope":            404,
		"/nope":                        404,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
	resp, err := http.Post(srv.URL+"/v2/team/app/blobs/uploads/", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d, want 405", resp.StatusCode)
	}
}

func TestPullRepositoryWithARouteSegment(t *testing.T) {
	for _, repo := range []string{"team/manifests/app", "team/blobs/app", "manifests/blobs"} {
		reg := fakereg.New(t)
		reg.AddImage(repo, "v1", 300_000, 200_000)
		c := newPullCache(t, 5*time.Second)
		d := fakedocker.New()
		if err := c.Pull(context.Background(), d, reg.Host+"/"+repo+":v1", nil); err != nil {
			t.Fatalf("%s: %v", repo, err)
		}
		if len(d.Tags) != 1 || d.Tags[0][1] != reg.Host+"/"+repo+":v1" {
			t.Fatalf("%s: tags = %v", repo, d.Tags)
		}
	}
}

func TestParseRef(t *testing.T) {
	cases := []struct {
		in, host, repo, scheme, tag, digest, original, registry string
	}{
		{"ubuntu", "registry-1.docker.io", "library/ubuntu", "https", "latest", "", "docker.io/library/ubuntu:latest", "docker.io"},
		{"example/app:1.2.3", "registry-1.docker.io", "example/app", "https", "1.2.3", "", "docker.io/example/app:1.2.3", "docker.io"},
		{"registry.example.com/team/app", "registry.example.com", "team/app", "https", "latest", "", "registry.example.com/team/app:latest", "registry.example.com"},
		{"localhost:5000/a/b:c", "localhost:5000", "a/b", "http", "c", "", "localhost:5000/a/b:c", "localhost:5000"},
		{"127.0.0.1:5000/a:b", "127.0.0.1:5000", "a", "http", "b", "", "127.0.0.1:5000/a:b", "127.0.0.1:5000"},
		{
			"alpine:3.20@sha256:" + strings.Repeat("a", 64), "registry-1.docker.io", "library/alpine", "https", "",
			"sha256:" + strings.Repeat("a", 64), "docker.io/library/alpine@sha256:" + strings.Repeat("a", 64), "docker.io",
		},
	}
	for _, tc := range cases {
		got, err := parseRef(tc.in)
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if got.remote.host != tc.host || got.remote.repo != tc.repo || got.scheme != tc.scheme ||
			got.tag != tc.tag || got.digest != tc.digest || got.original != tc.original || got.remote.registry != tc.registry {
			t.Errorf("%s = %+v", tc.in, got)
		}
	}
	if _, err := parseRef("team/App"); err == nil {
		t.Error("an invalid reference parsed")
	}
}

func TestReadStreamReturnsTheErrorMessage(t *testing.T) {
	stream := `{"status":"Pulling from x","id":"v1"}` + "\n" + `{"errorDetail":{"message":"no space left on device"},"error":"no space left on device"}` + "\n"
	err := readStream(strings.NewReader(stream), nil, nil)
	if err == nil || err.Error() != "no space left on device" {
		t.Fatalf("err = %v", err)
	}
}
