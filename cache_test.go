package dockercachepull

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestCache(t *testing.T, stall time.Duration) *Cache {
	t.Helper()
	c, err := New(t.TempDir(), Options{StallLimit: stall, MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	c.idleTimeout = 200 * time.Millisecond
	c.retryBase = 10 * time.Millisecond
	c.retryMax = 50 * time.Millisecond
	return c
}

func randomBlob(t *testing.T, size int) ([]byte, string) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

// fakeFetch serves data from the asked offset, letting a test script each
// attempt. It records the offsets asked for.
type fakeFetch struct {
	mu      sync.Mutex
	offsets []int64
	attempt func(n int, offset int64) (io.ReadCloser, int64, int64, error)
}

func (f *fakeFetch) fetch(_ context.Context, offset int64) (io.ReadCloser, int64, int64, error) {
	f.mu.Lock()
	n := len(f.offsets)
	f.offsets = append(f.offsets, offset)
	f.mu.Unlock()
	return f.attempt(n, offset)
}

func (f *fakeFetch) asked() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.offsets...)
}

func whole(data []byte, offset int64) (io.ReadCloser, int64, int64, error) {
	return io.NopCloser(bytes.NewReader(data[offset:])), offset, int64(len(data)), nil
}

// breaking yields data[offset:upto] and then fails like a dropped connection.
func breaking(data []byte, offset, upto int64) (io.ReadCloser, int64, int64, error) {
	r := io.MultiReader(bytes.NewReader(data[offset:upto]), errReader{errors.New("connection reset by peer")})
	return io.NopCloser(r), offset, int64(len(data)), nil
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// stallingBody yields its bytes, then blocks until closed.
type stallingBody struct {
	r      io.Reader
	closed chan struct{}
	once   sync.Once
}

func (s *stallingBody) Read(p []byte) (int, error) {
	if n, err := s.r.Read(p); n > 0 || err == nil {
		return n, nil
	}
	<-s.closed
	return 0, errors.New("use of closed network connection")
}

func (s *stallingBody) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

type result struct {
	got     []byte
	err     error
	failErr error
}

// runDownload downloads the blob through the cache and serves it to a
// reader the way a proxy request would.
func runDownload(t *testing.T, c *Cache, digest string, size int64, fetch fetchFunc) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failErr error
	b := c.acquire(digest, size)
	defer c.release(b)
	c.ensureDownload(ctx, b, fetch, func(err error) {
		mu.Lock()
		failErr = err
		mu.Unlock()
	}, &wg)

	n, err := b.waitSize(ctx)
	var buf bytes.Buffer
	if err == nil {
		err = b.serve(ctx, &buf, func() {}, 0, n-1)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return result{got: buf.Bytes(), err: err, failErr: failErr}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestDownloadServesAndStoresVerifiedBlob(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 1<<20+123)
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err != nil || r.failErr != nil {
		t.Fatalf("serve err %v, fail %v", r.err, r.failErr)
	}
	if !bytes.Equal(r.got, data) {
		t.Fatal("served bytes differ from the blob")
	}
	if !fileExists(filepath.Join(c.dir, digest)) || fileExists(filepath.Join(c.dir, digest+".partial")) {
		t.Fatal("expected only the verified file on disk")
	}
}

func TestBrokenDownloadResumesFromItsOffset(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 1<<20)
	f := &fakeFetch{attempt: func(n int, off int64) (io.ReadCloser, int64, int64, error) {
		if n == 0 {
			return breaking(data, off, 400_000)
		}
		return whole(data, off)
	}}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
	if got := f.asked(); len(got) != 2 || got[0] != 0 || got[1] != 400_000 {
		t.Fatalf("offsets asked = %v, want [0 400000]", got)
	}
}

func TestServerIgnoringRangeIsSkippedNotRewritten(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 1<<20)
	f := &fakeFetch{attempt: func(n int, off int64) (io.ReadCloser, int64, int64, error) {
		if n == 0 {
			return breaking(data, off, 300_000)
		}
		return whole(data, 0) // 200 with the whole blob
	}}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
}

func TestWrongStartIsRetried(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 1<<20)
	f := &fakeFetch{attempt: func(n int, off int64) (io.ReadCloser, int64, int64, error) {
		switch n {
		case 0:
			return breaking(data, off, 200_000)
		case 1:
			return whole(data, off+10)
		}
		return whole(data, off)
	}}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
	if got := f.asked(); len(got) != 3 || got[2] != 200_000 {
		t.Fatalf("offsets asked = %v, want a third attempt at 200000", got)
	}
}

func TestDigestMismatchDeletesFileAndNeverServesTheLastByte(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 600_000)
	bad := append([]byte(nil), data...)
	bad[1000] ^= 0xff
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(bad, off) }}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err == nil || r.failErr == nil || !strings.Contains(r.failErr.Error(), "digest mismatch") {
		t.Fatalf("serve err %v, fail %v; want a digest mismatch", r.err, r.failErr)
	}
	if len(r.got) >= len(data) {
		t.Fatalf("served %d of %d bytes; the last byte must be held back", len(r.got), len(data))
	}
	if fileExists(filepath.Join(c.dir, digest+".partial")) || fileExists(filepath.Join(c.dir, digest)) {
		t.Fatal("a corrupt download must leave no file")
	}
}

func TestStallLimitFailsTheLayer(t *testing.T) {
	c := newTestCache(t, 300*time.Millisecond)
	_, digest := randomBlob(t, 1000)
	f := &fakeFetch{attempt: func(int, int64) (io.ReadCloser, int64, int64, error) {
		return nil, 0, 0, errors.New("dial tcp: connection refused")
	}}

	start := time.Now()
	r := runDownload(t, c, digest, -1, f.fetch)
	if r.failErr == nil || !strings.Contains(r.failErr.Error(), "no new bytes for 300ms") {
		t.Fatalf("fail = %v, want the stall error", r.failErr)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s, want about the stall limit", took)
	}
	if len(f.asked()) < 3 {
		t.Fatalf("only %d attempts; transient errors must be retried", len(f.asked()))
	}
}

func TestQuietConnectionIsClosedAndResumed(t *testing.T) {
	c := newTestCache(t, 5*time.Second)
	data, digest := randomBlob(t, 1<<20)
	f := &fakeFetch{attempt: func(n int, off int64) (io.ReadCloser, int64, int64, error) {
		if n == 0 {
			return &stallingBody{r: bytes.NewReader(data[:100_000]), closed: make(chan struct{})}, 0, int64(len(data)), nil
		}
		return whole(data, off)
	}}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
	if got := f.asked(); len(got) != 2 || got[1] != 100_000 {
		t.Fatalf("offsets asked = %v, want [0 100000]", got)
	}
}

func TestPermanentErrorFailsAtOnce(t *testing.T) {
	c := newTestCache(t, 5*time.Second)
	_, digest := randomBlob(t, 1000)
	f := &fakeFetch{attempt: func(int, int64) (io.ReadCloser, int64, int64, error) {
		return nil, 0, 0, permanent(errors.New("registry answered 401 Unauthorized: DENIED"))
	}}

	start := time.Now()
	r := runDownload(t, c, digest, -1, f.fetch)
	if r.failErr == nil || !strings.Contains(r.failErr.Error(), "DENIED") {
		t.Fatalf("fail = %v, want the DENIED error", r.failErr)
	}
	if took := time.Since(start); took > time.Second || len(f.asked()) != 1 {
		t.Fatalf("took %s over %d attempts; a permanent error must not be retried", took, len(f.asked()))
	}
}

func TestPartialOnDiskIsResumedByANewCache(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 1<<20)
	if err := os.WriteFile(filepath.Join(c.dir, digest+".partial"), data[:700_000], 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	r := runDownload(t, c, digest, int64(len(data)), f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
	if got := f.asked(); len(got) != 1 || got[0] != 700_000 {
		t.Fatalf("offsets asked = %v, want [700000]", got)
	}
}

func TestCompletePartialIsVerifiedWithoutFetching(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 50_000)
	if err := os.WriteFile(filepath.Join(c.dir, digest+".partial"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	r := runDownload(t, c, digest, int64(len(data)), f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) || len(f.asked()) != 0 {
		t.Fatalf("serve err %v, equal %v, fetches %d", r.err, bytes.Equal(r.got, data), len(f.asked()))
	}
}

func TestOversizedPartialStartsOver(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 50_000)
	if err := os.WriteFile(filepath.Join(c.dir, digest+".partial"), append(data, 1, 2, 3), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	r := runDownload(t, c, digest, int64(len(data)), f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
	if got := f.asked(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("offsets asked = %v, want [0]", got)
	}
}

func TestRefusedResumeOffsetStartsOver(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 50_000)
	if err := os.WriteFile(filepath.Join(c.dir, digest+".partial"), data[:20_000], 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) {
		if off > 0 {
			return nil, 0, 0, errRangeNotSatisfiable
		}
		return whole(data, 0)
	}}

	r := runDownload(t, c, digest, -1, f.fetch)
	if r.err != nil || !bytes.Equal(r.got, data) {
		t.Fatalf("serve err %v, bytes equal %v", r.err, bytes.Equal(r.got, data))
	}
}

func TestServeHonoursARange(t *testing.T) {
	c := newTestCache(t, 2*time.Second)
	data, digest := randomBlob(t, 300_000)
	f := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}
	runDownload(t, c, digest, -1, f.fetch)

	b := c.acquire(digest, -1)
	defer c.release(b)
	var buf bytes.Buffer
	if err := b.serve(context.Background(), &buf, func() {}, 1000, 1999); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data[1000:2000]) {
		t.Fatal("range bytes differ")
	}
}

func TestSweepDeletesOnlyOldUnusedFiles(t *testing.T) {
	c := newTestCache(t, time.Second)
	old := time.Now().Add(-2 * time.Hour)
	name := func(b byte) string { return strings.Repeat(string(b), 64) }
	files := map[string]bool{ // file -> should survive
		name('a'):              false,
		name('b') + ".partial": false,
		name('c') + ".partial": true, // fresh
		name('d'):              true, // old but in use
		"notes.txt":            true, // not ours
	}
	for f := range files {
		p := filepath.Join(c.dir, f)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if f != name('c')+".partial" {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	b := c.acquire(name('d'), -1)
	defer c.release(b)

	if err := c.Sweep(); err != nil {
		t.Fatal(err)
	}
	for f, keep := range files {
		if got := fileExists(filepath.Join(c.dir, f)); got != keep {
			t.Errorf("%s exists = %v, want %v", f, got, keep)
		}
	}
}

func TestSavedManifestsLoadVerifyAndAreSwept(t *testing.T) {
	c := newTestCache(t, time.Second)
	body := []byte(`{"schemaVersion":2}`)
	h := c.saveManifest(body)
	if got, ok := c.loadManifest(h); !ok || string(got) != string(body) {
		t.Fatalf("load = %q, %v", got, ok)
	}
	if err := os.WriteFile(filepath.Join(c.mdir, h), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.loadManifest(h); ok {
		t.Fatal("a manifest that no longer matches its digest loaded")
	}

	old := time.Now().Add(-2 * time.Hour)
	stale, fresh := strings.Repeat("1", 64), strings.Repeat("2", 64)
	for _, f := range []string{stale, fresh, ".tmp-123"} {
		p := filepath.Join(c.mdir, f)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if f != fresh {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.Sweep(); err != nil {
		t.Fatal(err)
	}
	if fileExists(filepath.Join(c.mdir, stale)) || fileExists(filepath.Join(c.mdir, ".tmp-123")) || !fileExists(filepath.Join(c.mdir, fresh)) {
		t.Fatal("sweep of saved manifests kept or dropped the wrong files")
	}
}

func TestRemoveBlobsSkipsBlobsInUse(t *testing.T) {
	c := newTestCache(t, time.Second)
	free, busy := strings.Repeat("e", 64), strings.Repeat("f", 64)
	for _, f := range []string{free, free + ".partial", busy} {
		if err := os.WriteFile(filepath.Join(c.dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	b := c.acquire(busy, -1)
	defer c.release(b)

	if err := c.removeBlobs([]string{free, busy}); err != nil {
		t.Fatal(err)
	}
	if fileExists(filepath.Join(c.dir, free)) || fileExists(filepath.Join(c.dir, free+".partial")) {
		t.Error("free blob still on disk")
	}
	if !fileExists(filepath.Join(c.dir, busy)) {
		t.Error("busy blob was removed")
	}
}
