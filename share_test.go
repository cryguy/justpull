package justpull

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cryguy/justpull/internal/fakereg"
)

// Two Caches on one directory stand for two processes sharing it: their
// flocks exclude each other as two processes' do.

// watchWriter keeps what a reader got and closes first on its first byte.
type watchWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	first chan struct{}
}

func newWatchWriter() *watchWriter { return &watchWriter{first: make(chan struct{})} }

func (w *watchWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() == 0 && len(p) > 0 {
		close(w.first)
	}
	return w.buf.Write(p)
}

func (w *watchWriter) got() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buf.Bytes()...)
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// gatedFetch hands its first caller a body the test writes to, so the test
// decides when each byte reaches the downloader. The body fails once ctx
// ends, as an HTTP body does. Later calls fail like a refused dial.
type gatedFetch struct {
	fakeFetch
	pw      *io.PipeWriter
	fetched chan struct{}
}

func newGatedFetch(ctx context.Context, size int) *gatedFetch {
	pr, pw := io.Pipe()
	context.AfterFunc(ctx, func() { _ = pw.CloseWithError(ctx.Err()) })
	g := &gatedFetch{pw: pw, fetched: make(chan struct{})}
	g.attempt = func(n int, _ int64) (io.ReadCloser, int64, int64, error) {
		if n > 0 {
			return nil, 0, 0, errors.New("dial tcp: connection refused")
		}
		close(g.fetched)
		return pr, 0, int64(size), nil
	}
	return g
}

func (g *gatedFetch) send(t *testing.T, data []byte) {
	t.Helper()
	if _, err := g.pw.Write(data); err != nil {
		t.Fatal(err)
	}
}

func TestSecondCacheStreamsTheFirstCachesDownload(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, 5*time.Second), newTestCacheIn(t, dir, 5*time.Second)
	a.idleTimeout = 10 * time.Second // the gate is not a quiet connection
	data, digest := randomBlob(t, 1<<20)
	size := int64(len(data))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newGatedFetch(ctx, len(data))

	toA, toB := newWatchWriter(), newWatchWriter()
	resA, resB := make(chan result, 1), make(chan result, 1)
	go func() { resA <- serveBlob(ctx, a, digest, size, f.fetch, toA) }()
	waitFor(t, f.fetched, "the first cache's fetch")
	go func() { resB <- serveBlob(ctx, b, digest, size, f.fetch, toB) }()

	f.send(t, data[:len(data)/2])
	waitFor(t, toB.first, "the second cache's first byte while the download is half done")
	f.send(t, data[len(data)/2:])
	_ = f.pw.Close()

	ra, rb := <-resA, <-resB
	if ra.err != nil || ra.failErr != nil || rb.err != nil || rb.failErr != nil {
		t.Fatalf("first: serve %v fail %v; second: serve %v fail %v", ra.err, ra.failErr, rb.err, rb.failErr)
	}
	if !bytes.Equal(toA.got(), data) || !bytes.Equal(toB.got(), data) {
		t.Fatal("a reader got bytes that differ from the blob")
	}
	if got := f.asked(); len(got) != 1 {
		t.Fatalf("fetches = %v, want the one download", got)
	}
	if !fileExists(filepath.Join(dir, "blobs", "sha256", digest)) || fileExists(filepath.Join(dir, "blobs", "sha256", digest+".partial")) {
		t.Fatal("expected only the verified file on disk")
	}
}

func TestFollowerTakesOverACancelledDownload(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, 5*time.Second), newTestCacheIn(t, dir, 5*time.Second)
	a.idleTimeout = 10 * time.Second
	data, digest := randomBlob(t, 1<<20)
	size := int64(len(data))
	const kept = 400_000
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fa := newGatedFetch(ctxA, len(data))
	fb := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	toB := newWatchWriter()
	resA, resB := make(chan result, 1), make(chan result, 1)
	go func() { resA <- serveBlob(ctxA, a, digest, size, fa.fetch, io.Discard) }()
	waitFor(t, fa.fetched, "the first cache's fetch")
	fa.send(t, data[:kept])
	go func() { resB <- serveBlob(ctx, b, digest, size, fb.fetch, toB) }()
	waitFor(t, toB.first, "the second cache's first byte")
	if got := fb.asked(); len(got) != 0 {
		t.Fatalf("follower fetched %v while the first cache was downloading", got)
	}
	cancelA()

	if ra := <-resA; !errors.Is(ra.err, context.Canceled) {
		t.Fatalf("first cache's serve = %v, want its cancel", ra.err)
	}
	rb := <-resB
	if rb.err != nil || rb.failErr != nil || !bytes.Equal(toB.got(), data) {
		t.Fatalf("serve %v, fail %v, bytes equal %v", rb.err, rb.failErr, bytes.Equal(toB.got(), data))
	}
	if got := fb.asked(); len(got) != 1 || got[0] != kept {
		t.Fatalf("follower's fetches = %v, want one resume from the %d bytes on disk", got, kept)
	}
}

func TestFollowerFailsWhenTheDownloadFailsItsDigest(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, 5*time.Second), newTestCacheIn(t, dir, 5*time.Second)
	a.idleTimeout = 10 * time.Second
	data, digest := randomBlob(t, 600_000)
	bad := append([]byte(nil), data...)
	bad[1000] ^= 0xff
	size := int64(len(data))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fa := newGatedFetch(ctx, len(bad))
	// Taking over would fetch good bytes and hide the corrupt ones served.
	fb := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	toB := newWatchWriter()
	resA, resB := make(chan result, 1), make(chan result, 1)
	go func() { resA <- serveBlob(ctx, a, digest, size, fa.fetch, io.Discard) }()
	waitFor(t, fa.fetched, "the first cache's fetch")
	fa.send(t, bad[:len(bad)/2])
	go func() { resB <- serveBlob(ctx, b, digest, size, fb.fetch, toB) }()
	waitFor(t, toB.first, "the second cache's first byte")
	fa.send(t, bad[len(bad)/2:])
	_ = fa.pw.Close()

	ra, rb := <-resA, <-resB
	if ra.failErr == nil || !strings.Contains(ra.failErr.Error(), "digest mismatch") {
		t.Fatalf("downloader fail = %v, want its digest mismatch", ra.failErr)
	}
	if rb.err == nil || rb.failErr == nil || !isPermanent(rb.failErr) || !strings.Contains(rb.failErr.Error(), "digest mismatch in another process") {
		t.Fatalf("follower: serve %v, fail %v; want the permanent mismatch error", rb.err, rb.failErr)
	}
	if n := len(toB.got()); n >= len(data) {
		t.Fatalf("follower served %d of %d bytes; the last byte must be held back", n, len(data))
	}
	if got := fb.asked(); len(got) != 0 {
		t.Fatalf("follower fetched %v; it must not take over a failed digest", got)
	}
	for _, name := range []string{digest, digest + ".partial"} {
		if fileExists(filepath.Join(dir, "blobs", "sha256", name)) {
			t.Fatalf("%s left on disk", name)
		}
	}
}

func TestFollowerGivesUpOnAStalledDownload(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, 10*time.Second), newTestCacheIn(t, dir, 300*time.Millisecond)
	data, digest := randomBlob(t, 1<<20)
	size := int64(len(data))
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	fetched := make(chan struct{})
	fa := &fakeFetch{attempt: func(n int, _ int64) (io.ReadCloser, int64, int64, error) {
		if n == 0 {
			close(fetched)
			return &stallingBody{r: bytes.NewReader(data[:100_000]), closed: make(chan struct{})}, 0, size, nil
		}
		return nil, 0, 0, errors.New("dial tcp: connection refused")
	}}
	fb := &fakeFetch{attempt: func(_ int, off int64) (io.ReadCloser, int64, int64, error) { return whole(data, off) }}

	resA := make(chan result, 1)
	go func() { resA <- serveBlob(ctxA, a, digest, size, fa.fetch, io.Discard) }()
	waitFor(t, fetched, "the first cache's fetch")

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rb := serveBlob(ctx, b, digest, size, fb.fetch, io.Discard)
	if rb.failErr == nil || !strings.Contains(rb.failErr.Error(), "no new bytes for 300ms") {
		t.Fatalf("follower fail = %v, want its stall error", rb.failErr)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s, want about the follower's stall limit", took)
	}
	if got := fb.asked(); len(got) != 0 {
		t.Fatalf("follower fetched %v while the download lock was held", got)
	}
	cancelA()
	<-resA
}

func TestLockWonOnAnUnlinkedFileIsTakenAgain(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, time.Second), newTestCacheIn(t, dir, time.Second)
	h := strings.Repeat("c", 64)
	path := a.lockPath(h, ".use")
	deleter, err := tryLockExclusive(path)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan error, 1)
	go func() {
		bl, err := b.acquire(h, -1)
		if err == nil {
			t.Cleanup(func() { b.release(bl) })
		}
		acquired <- err
	}()
	// Long enough for the acquire to open the file and wait on its lock,
	// which is when the check matters; any order passes with the check.
	time.Sleep(100 * time.Millisecond)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	_ = deleter.Close()
	if err := <-acquired; err != nil {
		t.Fatal(err)
	}

	if f, err := tryLockExclusive(path); !errors.Is(err, errLocked) {
		if f != nil {
			_ = f.Close()
		}
		t.Fatalf("a deleter locked the blob another cache holds: err = %v", err)
	}
}

func writeOld(t *testing.T, path string, old time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupSkipsABlobAnotherCachePins(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, time.Second), newTestCacheIn(t, dir, time.Second)
	pinned, free := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for _, h := range []string{pinned, free} {
		if err := os.WriteFile(filepath.Join(a.dir, h), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	p := newProxy(ctx, b, target{}, cancel)
	p.recordManifest("", []byte(fmt.Sprintf(`{"layers":[{"digest":"sha256:%s","size":1}]}`, pinned)), 0)

	if err := a.removeBlobs([]string{pinned, free}); err != nil {
		t.Fatal(err)
	}
	if fileExists(filepath.Join(a.dir, free)) {
		t.Error("the free blob is still on disk")
	}
	if !fileExists(filepath.Join(a.dir, pinned)) {
		t.Fatal("the blob the other cache pins was removed")
	}

	p.unpin()
	if err := b.removeBlobs([]string{pinned}); err != nil {
		t.Fatal(err)
	}
	if files := cacheFiles(t, a); len(files) != 0 {
		t.Fatalf("cache not emptied once the pin is gone: %v", files)
	}
}

func TestSweepSkipsBlobsAnotherCacheUsesAndRemovesOldLocks(t *testing.T) {
	dir := t.TempDir()
	a, b := newTestCacheIn(t, dir, time.Second), newTestCacheIn(t, dir, time.Second)
	old := time.Now().Add(-2 * time.Hour)
	inUse, unused, orphan, fresh := strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64), strings.Repeat("4", 64)
	writeOld(t, filepath.Join(a.dir, inUse), old)
	writeOld(t, filepath.Join(a.dir, unused+".partial"), old)
	writeOld(t, a.lockPath(orphan, ".dl"), old)
	writeOld(t, a.lockPath(orphan, ".use"), old)
	if err := os.WriteFile(a.lockPath(fresh, ".use"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	held := mustAcquire(t, b, inUse, -1)

	if err := a.Sweep(); err != nil {
		t.Fatal(err)
	}
	files := cacheFiles(t, a)
	want := map[string]bool{inUse: true, "l/" + inUse + ".use": true, "l/" + fresh + ".use": true}
	for name := range files {
		if !want[name] {
			t.Errorf("%s survived the sweep", name)
		}
	}
	for name := range want {
		if _, ok := files[name]; !ok {
			t.Errorf("%s was swept", name)
		}
	}

	b.release(held)
	if err := a.Sweep(); err != nil {
		t.Fatal(err)
	}
	files = cacheFiles(t, a)
	if _, ok := files["l/"+fresh+".use"]; len(files) != 1 || !ok {
		t.Fatalf("after the release, files = %v; want only the fresh lock", files)
	}
}

func TestConcurrentPullsOfOneImageDownloadEachLayerOnce(t *testing.T) {
	reg := fakereg.New(t)
	reg.Redirect = true
	img := reg.AddImage("team/app", "v1", 300_000, 200_000)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Both pulls hold their manifests before either asks for a blob, so
	// neither can finish and clean up before the other has started.
	var arrived atomic.Int32
	both := make(chan struct{})
	barrier := func() {
		if arrived.Add(1) == 2 {
			close(both)
		}
		select {
		case <-both:
		case <-ctx.Done():
		}
	}
	errs := make(chan error, 2)
	for range 2 {
		c := newPullCacheIn(t, dir, Options{StallLimit: 5 * time.Second})
		d := newFakeDocker()
		d.afterManifest = barrier
		go func() { errs <- c.Pull(ctx, d, reg.Host+"/team/app:v1", nil) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	for _, blob := range append([]*fakereg.Blob{img.Config}, img.Layers...) {
		for _, host := range []string{"registry", "storage"} {
			if n := len(registryBlobRequests(reg, blob.Digest, host)); n != 1 {
				t.Errorf("%s: %d blob GETs at the %s host, want 1", blob.Digest[:19], n, host)
			}
		}
	}
	c := newPullCacheIn(t, dir, Options{})
	if files := cacheFiles(t, c); len(files) != 0 {
		t.Fatalf("cache not emptied after both pulls: %v", files)
	}
}
