package justpull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cryguy/justpull/internal/fakereg"
)

// fakeDocker pulls like dockerd: manifest, config, then each layer it does
// not have, resuming a blob with Range when its body ends early. It writes
// dockerd's progress stream.
type fakeDocker struct {
	client *http.Client
	// reuseManifests makes it skip the GET for a manifest it already holds
	// (a HEAD resolves the tag), as the containerd store does.
	reuseManifests bool

	mu        sync.Mutex
	have      map[string]bool
	manifests map[string][]byte
	blobGets  map[string]int
	pulls     []string
	tags      [][2]string
	untags    []string
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{client: &http.Client{}, have: map[string]bool{}, manifests: map[string][]byte{}, blobGets: map[string]int{}}
}

func (f *fakeDocker) manifest(ctx context.Context, url string) ([]byte, error) {
	if f.reuseManifests {
		req, _ := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")
		resp, err := f.client.Do(req)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		f.mu.Lock()
		raw, ok := f.manifests[resp.Header.Get("Docker-Content-Digest")]
		f.mu.Unlock()
		if ok {
			return raw, nil
		}
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest: %s: %s", resp.Status, raw)
	}
	sum := sha256.Sum256(raw)
	f.mu.Lock()
	f.manifests["sha256:"+hex.EncodeToString(sum[:])] = raw
	f.mu.Unlock()
	return raw, nil
}

func (f *fakeDocker) Pull(ctx context.Context, ref string) (io.ReadCloser, error) {
	f.mu.Lock()
	f.pulls = append(f.pulls, ref)
	f.mu.Unlock()
	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw)
		if err := f.pull(ctx, ref, enc); err != nil {
			_ = enc.Encode(map[string]string{"error": err.Error()})
		}
		_ = pw.Close()
	}()
	return pr, nil
}

func (f *fakeDocker) Tag(_ context.Context, source, target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tags = append(f.tags, [2]string{source, target})
	return nil
}

func (f *fakeDocker) Untag(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.untags = append(f.untags, ref)
	return nil
}

type streamMsg struct {
	ID             string         `json:"id,omitempty"`
	Status         string         `json:"status"`
	ProgressDetail map[string]any `json:"progressDetail,omitempty"`
}

func (f *fakeDocker) pull(ctx context.Context, ref string, enc *json.Encoder) error {
	host, rest, _ := strings.Cut(ref, "/")
	name, mref := rest, ""
	if i := strings.Index(rest, "@"); i >= 0 {
		name, mref = rest[:i], rest[i+1:]
	} else if i := strings.LastIndex(rest, ":"); i >= 0 {
		name, mref = rest[:i], rest[i+1:]
	}
	base := "http://" + host + "/v2/" + name

	raw, err := f.manifest(ctx, base+"/manifests/"+mref)
	if err != nil {
		return err
	}
	var m struct {
		Config struct{ Digest string } `json:"config"`
		Layers []struct{ Digest string } `json:"layers"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	short := func(d string) string { return strings.TrimPrefix(d, "sha256:")[:12] }
	_ = enc.Encode(streamMsg{ID: mref, Status: "Pulling from " + name})
	for _, l := range m.Layers {
		_ = enc.Encode(streamMsg{ID: short(l.Digest), Status: "Pulling fs layer"})
	}
	if err := f.fetchBlob(ctx, base, m.Config.Digest, nil); err != nil {
		return err
	}
	for _, l := range m.Layers {
		id := short(l.Digest)
		f.mu.Lock()
		have := f.have[l.Digest]
		f.mu.Unlock()
		if have {
			_ = enc.Encode(streamMsg{ID: id, Status: "Already exists"})
			continue
		}
		err := f.fetchBlob(ctx, base, l.Digest, func(cur, total int64) {
			_ = enc.Encode(streamMsg{ID: id, Status: "Downloading", ProgressDetail: map[string]any{"current": cur, "total": total}})
		})
		if err != nil {
			return err
		}
		_ = enc.Encode(streamMsg{ID: id, Status: "Download complete"})
		_ = enc.Encode(streamMsg{ID: id, Status: "Pull complete"})
	}
	_ = enc.Encode(streamMsg{Status: "Status: Downloaded newer image for " + ref})
	return nil
}

type progressWriter struct {
	n  int64
	fn func(int64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	p.n += int64(len(b))
	if p.fn != nil {
		p.fn(p.n)
	}
	return len(b), nil
}

func (f *fakeDocker) fetchBlob(ctx context.Context, base, digest string, onProgress func(cur, total int64)) error {
	h := sha256.New()
	var got, total int64 = 0, -1
	for attempt := 0; attempt < 4; attempt++ {
		f.mu.Lock()
		f.blobGets[digest]++
		f.mu.Unlock()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/blobs/"+digest, nil)
		if got > 0 {
			req.Header.Set("Range", "bytes="+strconv.FormatInt(got, 10)+"-")
		}
		resp, err := f.client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("blob %s: %s: %s", digest[:19], resp.Status, raw)
		}
		if total < 0 {
			total = resp.ContentLength + got
		}
		pw := &progressWriter{n: got}
		if onProgress != nil {
			pw.fn = func(n int64) { onProgress(n, total) }
		}
		n, err := io.Copy(io.MultiWriter(h, pw), resp.Body)
		_ = resp.Body.Close()
		got += n
		if err == nil && got == total {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
	}
	if sum := "sha256:" + hex.EncodeToString(h.Sum(nil)); sum != digest {
		return fmt.Errorf("blob %s: digest mismatch", digest[:19])
	}
	return nil
}

func (f *fakeDocker) gets(digest string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blobGets[digest]
}

func newPullCache(t *testing.T, stall time.Duration) *Cache {
	t.Helper()
	c, err := New(t.TempDir(), Options{StallLimit: stall})
	if err != nil {
		t.Fatal(err)
	}
	c.idleTimeout = 200 * time.Millisecond
	c.retryBase = 10 * time.Millisecond
	c.retryMax = 50 * time.Millisecond
	return c
}

// cacheFiles lists the blob files by name and the saved manifests as
// "m/<name>".
func cacheFiles(t *testing.T, c *Cache) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for prefix, dir := range map[string]string{"": c.dir, "m/": c.mdir} {
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
	d := newFakeDocker()
	var progress []Progress

	err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", func(p Progress) { progress = append(progress, p) })
	if err != nil {
		t.Fatal(err)
	}
	if len(d.pulls) != 1 || !strings.HasPrefix(d.pulls[0], "127.0.0.1:") || !strings.HasSuffix(d.pulls[0], "/team/app:v1") {
		t.Fatalf("docker pulled %v, want the localhost proxy name", d.pulls)
	}
	if len(d.tags) != 1 || d.tags[0] != [2]string{d.pulls[0], reg.Host + "/team/app:v1"} {
		t.Fatalf("tags = %v", d.tags)
	}
	if len(d.untags) != 1 || d.untags[0] != d.pulls[0] {
		t.Fatalf("untags = %v", d.untags)
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
			d := newFakeDocker()

			if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil); err != nil {
				t.Fatal(err)
			}
			if n := d.gets(layer.Digest); n != 1 {
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

	if err := c.Pull(context.Background(), newFakeDocker(), reg.Host+"/team/app:v1", nil); err != nil {
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

	if err := c.Pull(context.Background(), newFakeDocker(), reg.Host+"/team/app:v1", nil); err != nil {
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
	d := newFakeDocker()

	err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil)
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("err = %v, want a digest mismatch", err)
	}
	if len(d.tags) != 0 {
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
	err := c.Pull(context.Background(), newFakeDocker(), reg.Host+"/team/app:v1", nil)
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
	if err := c.Pull(context.Background(), newFakeDocker(), reg.Host+"/team/app:v1", nil); err != nil {
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
	d := newFakeDocker()
	d.reuseManifests = true
	if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil); err == nil {
		t.Fatal("first pull should fail")
	}

	big.SetBroken(false)
	d.have[small.Digest] = true
	before := d.gets(small.Digest)
	if err := c.Pull(context.Background(), d, reg.Host+"/team/app:v1", nil); err != nil {
		t.Fatal(err)
	}
	if d.gets(small.Digest) != before {
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

	err := c.Pull(ctx, newFakeDocker(), reg.Host+"/team/app:v1", nil)
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
	err := c.Pull(context.Background(), newFakeDocker(), reg.Host+"/team/app", nil)
	if err == nil || !strings.Contains(err.Error(), "DENIED") {
		t.Fatalf("err = %v, want DENIED", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s; a refusal must not wait for the stall limit", took)
	}
}

func TestLayersDockerHasAreNotDownloaded(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 100_000, 200_000)
	c := newPullCache(t, 5*time.Second)
	d := newFakeDocker()
	d.have[img.Layers[0].Digest] = true
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
	d := newFakeDocker()
	ref := reg.Host + "/team/app@" + img.Digest

	if err := c.Pull(context.Background(), d, ref, nil); err != nil {
		t.Fatal(err)
	}
	if len(d.pulls) != 2 || !strings.HasSuffix(d.pulls[0], "/team/app@"+img.Digest) || d.pulls[1] != ref {
		t.Fatalf("pulls = %v", d.pulls)
	}
	if len(d.tags) != 0 || len(d.untags) != 1 {
		t.Fatalf("tags %v untags %v", d.tags, d.untags)
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
		"/v2/team/app/blobs/sha512:ab": 404,
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

func TestParseRef(t *testing.T) {
	cases := []struct {
		in, host, repo, scheme, tag, digest, original string
	}{
		{"ubuntu", "registry-1.docker.io", "library/ubuntu", "https", "latest", "", "docker.io/library/ubuntu:latest"},
		{"example/app:1.2.3", "registry-1.docker.io", "example/app", "https", "1.2.3", "", "docker.io/example/app:1.2.3"},
		{"registry.example.com/team/app", "registry.example.com", "team/app", "https", "latest", "", "registry.example.com/team/app:latest"},
		{"localhost:5000/a/b:c", "localhost:5000", "a/b", "http", "c", "", "localhost:5000/a/b:c"},
		{"127.0.0.1:5000/a:b", "127.0.0.1:5000", "a", "http", "b", "", "127.0.0.1:5000/a:b"},
		{
			"alpine:3.20@sha256:" + strings.Repeat("a", 64), "registry-1.docker.io", "library/alpine", "https", "",
			"sha256:" + strings.Repeat("a", 64), "docker.io/library/alpine@sha256:" + strings.Repeat("a", 64),
		},
	}
	for _, tc := range cases {
		got, err := parseRef(tc.in)
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if got.remote.host != tc.host || got.remote.repo != tc.repo || got.scheme != tc.scheme ||
			got.tag != tc.tag || got.digest != tc.digest || got.original != tc.original {
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
