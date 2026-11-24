package justpull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultStallLimit is how long a layer may go without a new byte from
	// the registry, reconnects included, before the pull fails. A slow link
	// that keeps moving is never cut off.
	DefaultStallLimit = 10 * time.Minute
	// DefaultMaxAge is how long Sweep keeps a cached file after its last
	// write.
	DefaultMaxAge = 24 * time.Hour

	defaultIdleTimeout = 30 * time.Second
	defaultRetryBase   = time.Second
	defaultRetryMax    = 30 * time.Second
	defaultFollowPoll  = 200 * time.Millisecond

	chunkSize = 256 << 10
)

// Options tunes a Cache. The zero value uses the defaults.
type Options struct {
	// StallLimit fails a pull when a layer has received no new bytes for
	// this long. Zero means DefaultStallLimit.
	StallLimit time.Duration
	// MaxAge is how long Sweep keeps a cached file after its last write.
	// Zero means DefaultMaxAge.
	MaxAge time.Duration
	// Logger receives warnings that do not fail a pull. Nil means
	// slog.Default().
	Logger *slog.Logger
	// Credentials returns the login for a registry, named as in image
	// references: docker.io for Docker Hub, ghcr.io, localhost:5000,
	// 127.0.0.1:5000. It is called only when a registry answers 401 with a
	// challenge, and may be called concurrently. A zero Credential means
	// anonymous; nil means every pull is anonymous.
	Credentials func(ctx context.Context, registry string) (Credential, error)
}

// Cache stores blobs on disk by digest and pulls images through them, so a
// download broken by the network, a failed pull or a restart continues where
// it stopped. Layout: <dir>/blobs/sha256/<hex> once verified,
// <hex>.partial while downloading; <dir>/manifests/sha256/<hex> for the
// manifests relayed, which name an image's blobs for cleanup;
// <dir>/locks/sha256/<hex>.dl and <hex>.use, the flock(2) files that
// coordinate the processes sharing the directory.
//
// Several processes, or several Caches, may share a directory. A blob is
// downloaded by one of them at a time; the others stream it from the cache
// file as it arrives, and one of them takes the download over from what is on
// disk if its downloader dies or gives up. Cleanup and Sweep skip blobs
// another process uses. Unix only (Linux, macOS, the BSDs): New fails
// elsewhere. A Cache's methods are safe for concurrent use.
type Cache struct {
	dir  string // blobs
	mdir string // manifests
	ldir string // locks
	opts Options

	// Fixed by New; tests shorten them.
	idleTimeout time.Duration
	retryBase   time.Duration
	retryMax    time.Duration
	followPoll  time.Duration

	up *upstream

	mu    sync.Mutex
	blobs map[string]*blob // blobs held by a request, a pin or a running download
}

// New opens (creating if needed) a cache rooted at dir.
func New(dir string, opts Options) (*Cache, error) {
	if !flockSupported {
		return nil, errors.New("justpull: the cache shares its directory through flock(2), which this system lacks; it runs on Linux, macOS and the BSDs")
	}
	if opts.StallLimit <= 0 {
		opts.StallLimit = DefaultStallLimit
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = DefaultMaxAge
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	bdir := filepath.Join(dir, "blobs", "sha256")
	mdir := filepath.Join(dir, "manifests", "sha256")
	ldir := filepath.Join(dir, "locks", "sha256")
	for _, d := range []string{bdir, mdir, ldir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("justpull: create cache dir: %w", err)
		}
	}
	return &Cache{
		dir:         bdir,
		mdir:        mdir,
		ldir:        ldir,
		opts:        opts,
		idleTimeout: defaultIdleTimeout,
		retryBase:   defaultRetryBase,
		retryMax:    defaultRetryMax,
		followPoll:  defaultFollowPoll,
		up:          newUpstream(opts.Credentials),
		blobs:       map[string]*blob{},
	}, nil
}

func (c *Cache) lockPath(hexDigest, kind string) string {
	return filepath.Join(c.ldir, hexDigest+kind)
}

// fetchFunc opens a blob's bytes from offset on. start is where the body
// really begins (a server may ignore Range); total is the blob's size, or -1
// if the server did not say.
type fetchFunc func(ctx context.Context, offset int64) (body io.ReadCloser, start, total int64, err error)

type blob struct {
	hex string
	dir string
	use *os.File // shared lock on <hex>.use while the Cache holds the blob

	mu       sync.Mutex
	changed  chan struct{} // closed and replaced on every state change
	size     int64         // -1 until known
	written  int64
	verified bool
	err      error
	refs     int
	running  bool
}

func (b *blob) partialPath() string  { return filepath.Join(b.dir, b.hex+".partial") }
func (b *blob) completePath() string { return filepath.Join(b.dir, b.hex) }

func (b *blob) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *blob) update(f func()) {
	b.mu.Lock()
	f()
	b.notifyLocked()
	b.mu.Unlock()
}

// acquire returns the blob with one more reference. size, if positive, is
// the size a manifest gave for it.
func (c *Cache) acquire(hexDigest string, size int64) (*blob, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.blobs[hexDigest]
	if b == nil {
		// Taken before the files are looked at: from then on no other
		// process deletes them.
		use, err := lockShared(c.lockPath(hexDigest, ".use"))
		if err != nil {
			return nil, fmt.Errorf("lock layer sha256:%s: %w", hexDigest[:12], err)
		}
		b = &blob{hex: hexDigest, dir: c.dir, use: use, size: -1, changed: make(chan struct{})}
		if fi, err := os.Stat(b.completePath()); err == nil {
			b.size, b.written, b.verified = fi.Size(), fi.Size(), true
		}
		c.blobs[hexDigest] = b
	}
	b.mu.Lock()
	b.refs++
	if b.size < 0 && size > 0 {
		b.size = size
	}
	b.mu.Unlock()
	return b, nil
}

func (c *Cache) release(b *blob) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refs--
	if b.refs == 0 && !b.running {
		c.dropLocked(b)
	}
}

// dropLocked forgets a blob nothing holds any more, which lets other
// processes delete it. The caller holds c.mu and b.mu.
func (c *Cache) dropLocked(b *blob) {
	delete(c.blobs, b.hex)
	_ = b.use.Close()
}

// ensureDownload starts the blob's download unless it is verified, failed in
// this pull, or already running. onFail hears every failure except ctx ending.
func (c *Cache) ensureDownload(ctx context.Context, b *blob, fetch fetchFunc, onFail func(error), wg *sync.WaitGroup) {
	b.mu.Lock()
	if b.verified || b.running || b.err != nil {
		b.mu.Unlock()
		return
	}
	b.running = true
	b.mu.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c.fill(ctx, b, fetch)
		c.mu.Lock()
		b.mu.Lock()
		b.running = false
		if err != nil {
			b.err = err
		}
		b.notifyLocked()
		if b.refs == 0 {
			c.dropLocked(b)
		}
		b.mu.Unlock()
		c.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			onFail(fmt.Errorf("layer sha256:%s: %w", b.hex[:12], err))
		}
	}()
}

// errFollowedDeleted means the partial file streamed from another process's
// download was deleted, which only a digest mismatch does.
var errFollowedDeleted = permanent(errors.New("digest mismatch in another process's download of the layer; the bytes already served are bad"))

// fill ends with the blob's complete file on disk. Only the holder of the
// blob's download lock fetches it. Every other process follows the holder's
// partial file, so its readers get the bytes as they land, and takes the
// lock over when the holder stops without finishing; download then resumes
// from the bytes on disk.
func (c *Cache) fill(ctx context.Context, b *blob, fetch fetchFunc) error {
	// followed is the partial file this blob has been served from. Those
	// bytes are good only if that same file is finished or taken over; a
	// rename keeps the file, a deletion and a fresh download do not.
	var followed os.FileInfo
	most, lastProgress := int64(-1), time.Now()
	for {
		if done, err := finished(b, followed); done || err != nil {
			return err
		}
		dl, err := tryLockExclusive(c.lockPath(b.hex, ".dl"))
		if err == nil {
			err = c.downloadLocked(ctx, b, fetch, followed)
			_ = dl.Close()
			return err
		}
		if !errors.Is(err, errLocked) {
			return permanent(fmt.Errorf("lock layer download: %w", err))
		}

		info, err := os.Stat(b.partialPath())
		switch {
		case err == nil && followed != nil && !os.SameFile(followed, info):
			return errFollowedDeleted
		case err == nil:
			followed = info
			n := info.Size()
			b.mu.Lock()
			if b.written != n {
				b.written = n
				b.notifyLocked()
			}
			b.mu.Unlock()
			if n > most {
				most, lastProgress = n, time.Now()
			}
		case followed != nil && errors.Is(err, os.ErrNotExist):
			// Renamed to the complete name since that was looked at, or
			// deleted.
			if done, err := finished(b, followed); done || err != nil {
				return err
			}
			return errFollowedDeleted
		}
		if time.Since(lastProgress) >= c.opts.StallLimit {
			return fmt.Errorf("no new bytes for %s", c.opts.StallLimit)
		}
		t := time.NewTimer(c.followPoll)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// downloadLocked runs with the blob's download lock held, so no other
// process changes its files meanwhile.
func (c *Cache) downloadLocked(ctx context.Context, b *blob, fetch fetchFunc, followed os.FileInfo) error {
	if done, err := finished(b, followed); done || err != nil {
		return err
	}
	if followed != nil {
		info, err := os.Stat(b.partialPath())
		if errors.Is(err, os.ErrNotExist) || (err == nil && !os.SameFile(followed, info)) {
			return errFollowedDeleted
		}
		if err != nil {
			return permanent(fmt.Errorf("partial file: %w", err))
		}
	}
	return c.download(ctx, b, fetch)
}

// finished marks the blob verified if its complete file exists. A complete
// file other than the partial file followed is a download that started over
// after that one was deleted.
func finished(b *blob, followed os.FileInfo) (bool, error) {
	info, err := os.Stat(b.completePath())
	if err != nil {
		return false, nil
	}
	if followed != nil && !os.SameFile(followed, info) {
		return false, errFollowedDeleted
	}
	b.update(func() { b.size, b.written, b.verified = info.Size(), info.Size(), true })
	return true, nil
}

// download fills the blob's partial file from the registry, resuming from
// what is already on disk, and renames it once its sha256 matches.
func (c *Cache) download(ctx context.Context, b *blob, fetch fetchFunc) error {
	f, err := os.OpenFile(b.partialPath(), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return permanent(fmt.Errorf("open partial file: %w", err))
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	offset, err := io.Copy(h, f)
	if err != nil {
		return permanent(fmt.Errorf("read partial file: %w", err))
	}
	restart := func() error {
		if err := f.Truncate(0); err != nil {
			return permanent(fmt.Errorf("truncate partial file: %w", err))
		}
		h.Reset()
		offset = 0
		b.update(func() { b.written = 0 })
		return nil
	}
	b.mu.Lock()
	size := b.size
	b.mu.Unlock()
	if size >= 0 && offset > size {
		if err := restart(); err != nil {
			return err
		}
	}
	b.update(func() { b.written = offset })

	lastProgress := time.Now()
	wait := c.retryBase
	for size < 0 || offset < size {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Since(lastProgress) >= c.opts.StallLimit {
			return fmt.Errorf("no new bytes for %s", c.opts.StallLimit)
		}

		body, start, total, err := fetch(ctx, offset)
		if errors.Is(err, errRangeNotSatisfiable) {
			if err := restart(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if isPermanent(err) {
				return err
			}
			c.opts.Logger.Warn("layer download failed, retrying", "digest", b.hex[:12], "offset", offset, "err", err)
			c.sleep(ctx, &wait, lastProgress)
			continue
		}

		if total < 0 && size < 0 {
			_ = body.Close()
			return permanent(errors.New("registry did not send the layer size"))
		}
		if total >= 0 {
			if size >= 0 && total != size {
				_ = body.Close()
				return permanent(fmt.Errorf("registry says the layer is %d bytes, the manifest says %d", total, size))
			}
			size = total
			b.update(func() { b.size = total })
		}
		if offset > size {
			_ = body.Close()
			if err := restart(); err != nil {
				return err
			}
			continue
		}
		if start > offset {
			_ = body.Close()
			c.opts.Logger.Warn("registry answered from the wrong offset, retrying", "digest", b.hex[:12], "asked", offset, "got", start)
			c.sleep(ctx, &wait, lastProgress)
			continue
		}

		n, err := c.copyBody(b, f, h, body, start, &offset, size, &lastProgress)
		_ = body.Close()
		if err != nil && isPermanent(err) {
			return err
		}
		if offset == size {
			break
		}
		if n > 0 {
			wait = c.retryBase
			continue
		}
		c.opts.Logger.Warn("layer download broke, retrying", "digest", b.hex[:12], "offset", offset, "err", err)
		c.sleep(ctx, &wait, lastProgress)
	}

	sum := hex.EncodeToString(h.Sum(nil))
	if sum != b.hex {
		_ = f.Close()
		_ = os.Remove(b.partialPath())
		return permanent(fmt.Errorf("digest mismatch: the downloaded bytes hash to sha256:%s", sum))
	}
	if err := f.Close(); err != nil {
		return permanent(fmt.Errorf("close partial file: %w", err))
	}
	if err := os.Rename(b.partialPath(), b.completePath()); err != nil {
		return permanent(fmt.Errorf("finish layer file: %w", err))
	}
	b.update(func() { b.verified = true })
	return nil
}

// copyBody appends the body to the partial file from *offset. A body that
// starts before *offset (the server ignored Range) is read past first. A
// body that goes quiet for idleTimeout is closed so the caller reconnects.
func (c *Cache) copyBody(b *blob, f *os.File, h hash.Hash, body io.ReadCloser, start int64, offset *int64, size int64, lastProgress *time.Time) (int64, error) {
	timer := time.AfterFunc(c.idleTimeout, func() { _ = body.Close() })
	defer timer.Stop()

	skip := *offset - start
	buf := make([]byte, chunkSize)
	var added int64
	for {
		n, err := body.Read(buf)
		if n > 0 {
			timer.Reset(c.idleTimeout)
			data := buf[:n]
			if skip > 0 {
				drop := min(skip, int64(len(data)))
				data = data[drop:]
				skip -= drop
			}
			if room := size - *offset; int64(len(data)) > room {
				data = data[:room]
			}
			if len(data) > 0 {
				if _, werr := f.WriteAt(data, *offset); werr != nil {
					return added, permanent(fmt.Errorf("write partial file: %w", werr))
				}
				h.Write(data)
				*offset += int64(len(data))
				added += int64(len(data))
				*lastProgress = time.Now()
				written := *offset
				b.update(func() { b.written = written })
			}
			if *offset == size {
				return added, nil
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return added, io.ErrUnexpectedEOF
			}
			return added, err
		}
	}
}

// sleep waits before the next attempt: exponential from retryBase to
// retryMax, never past the stall limit.
func (c *Cache) sleep(ctx context.Context, wait *time.Duration, lastProgress time.Time) {
	d := min(*wait, time.Until(lastProgress.Add(c.opts.StallLimit)))
	*wait = min(*wait*2, c.retryMax)
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// waitSize blocks until the blob's size is known.
func (b *blob) waitSize(ctx context.Context) (int64, error) {
	for {
		b.mu.Lock()
		size, err, ch := b.size, b.err, b.changed
		b.mu.Unlock()
		if size >= 0 {
			return size, nil
		}
		if err != nil {
			return 0, err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// serve writes bytes [off, end] to w as they arrive. The last byte is held
// back until the digest is verified, so the client never receives a whole
// blob that turns out to be corrupt.
func (b *blob) serve(ctx context.Context, w io.Writer, flush func(), off, end int64) error {
	var f *os.File
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()
	buf := make([]byte, chunkSize)
	for off <= end {
		b.mu.Lock()
		readable := b.written
		if !b.verified && b.size > 0 {
			readable = min(readable, b.size-1)
		}
		verified, err, ch := b.verified, b.err, b.changed
		b.mu.Unlock()

		if off < readable {
			if f == nil {
				var oerr error
				f, oerr = openBlobFile(b, verified)
				if oerr != nil {
					return oerr
				}
			}
			n := min(readable-off, end+1-off, int64(len(buf)))
			if _, rerr := f.ReadAt(buf[:n], off); rerr != nil {
				return fmt.Errorf("read layer file: %w", rerr)
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			flush()
			off += n
			continue
		}
		if err != nil {
			return err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// openBlobFile opens whichever file holds the blob's bytes. On Linux an open
// partial file stays readable after it is renamed to the complete name.
func openBlobFile(b *blob, verified bool) (*os.File, error) {
	first, second := b.partialPath(), b.completePath()
	if verified {
		first, second = second, first
	}
	f, err := os.Open(first)
	if err == nil {
		return f, nil
	}
	f, err = os.Open(second)
	if err != nil {
		return nil, fmt.Errorf("open layer file: %w", err)
	}
	return f, nil
}

// Sweep deletes cached files that have not been written for MaxAge and are
// not in use by this or another process, and the lock files of blobs whose
// files are gone. Pull runs it first; a long-running caller should also run
// it periodically so a cancelled pull's files go even when no new pull comes.
func (c *Cache) Sweep() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-c.opts.MaxAge)
	stale := map[string]bool{}
	errs := []error{
		staleDigests(c.dir, []string{"", ".partial"}, cutoff, stale),
		staleDigests(c.ldir, []string{".dl", ".use"}, cutoff, stale),
	}
	old := func(path string) bool {
		info, err := os.Stat(path)
		return err == nil && info.ModTime().Before(cutoff)
	}
	for hexDigest := range stale {
		if c.blobs[hexDigest] == nil {
			errs = append(errs, c.removeBlobLocked(hexDigest, old))
		}
	}
	errs = append(errs, sweepDir(c.mdir, cutoff, func(name string) bool {
		return isHexDigest(name) || strings.HasPrefix(name, ".tmp-")
	}))
	return errors.Join(errs...)
}

// staleDigests adds to out the digest of every file in dir named <hex> and
// one of suffixes that was last written before cutoff.
func staleDigests(dir string, suffixes []string, cutoff time.Time, out map[string]bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("justpull: sweep: %w", err)
	}
	for _, e := range entries {
		for _, suffix := range suffixes {
			hexDigest, ok := strings.CutSuffix(e.Name(), suffix)
			if !ok || !isHexDigest(hexDigest) {
				continue
			}
			if info, err := e.Info(); err == nil && info.ModTime().Before(cutoff) {
				out[hexDigest] = true
			}
		}
	}
	return nil
}

// sweepDir deletes the files in dir that ours accepts and that were last
// written before cutoff.
func sweepDir(dir string, cutoff time.Time, ours func(name string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("justpull: sweep: %w", err)
	}
	var errs []error
	for _, e := range entries {
		if !ours(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// saveManifest keeps a copy of a relayed manifest under its sha256 and
// returns that digest. Failing to save only costs cleanup precision, so it
// is logged, not returned.
func (c *Cache) saveManifest(body []byte) string {
	sum := sha256.Sum256(body)
	hexDigest := hex.EncodeToString(sum[:])
	path := filepath.Join(c.mdir, hexDigest)
	if _, err := os.Stat(path); err == nil {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
		return hexDigest
	}
	f, err := os.CreateTemp(c.mdir, ".tmp-")
	if err != nil {
		c.opts.Logger.Warn("could not save manifest", "err", err)
		return hexDigest
	}
	_, werr := f.Write(body)
	cerr := f.Close()
	if err := errors.Join(werr, cerr); err != nil {
		_ = os.Remove(f.Name())
		c.opts.Logger.Warn("could not save manifest", "err", err)
		return hexDigest
	}
	if err := os.Rename(f.Name(), path); err != nil {
		_ = os.Remove(f.Name())
		c.opts.Logger.Warn("could not save manifest", "err", err)
	}
	return hexDigest
}

// loadManifest returns a saved manifest if its bytes still match its digest.
func (c *Cache) loadManifest(hexDigest string) ([]byte, bool) {
	if !isHexDigest(hexDigest) {
		return nil, false
	}
	body, err := os.ReadFile(filepath.Join(c.mdir, hexDigest))
	if err != nil {
		return nil, false
	}
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:]) == hexDigest
}

// removeManifests deletes saved manifests. They are not locked, so another
// process pulling the same image may lose its copy; that pull then knows
// fewer of its blobs for sizes, pins and cleanup, and still completes.
func (c *Cache) removeManifests(hexDigests []string) error {
	var errs []error
	for _, hexDigest := range hexDigests {
		if err := os.Remove(filepath.Join(c.mdir, hexDigest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// removeBlobs deletes the files of the given blobs that no request, pin or
// download of this or another process is using.
func (c *Cache) removeBlobs(hexDigests []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, hexDigest := range hexDigests {
		if c.blobs[hexDigest] != nil {
			continue
		}
		errs = append(errs, c.removeBlobLocked(hexDigest, func(string) bool { return true }))
	}
	return errors.Join(errs...)
}

// removeBlobLocked deletes the blob's files that del accepts, and its lock
// files once no blob file is left, unless another process holds the blob.
// The caller holds c.mu and has checked that this Cache does not.
func (c *Cache) removeBlobLocked(hexDigest string, del func(path string) bool) error {
	use, err := tryLockExclusive(c.lockPath(hexDigest, ".use"))
	if errors.Is(err, errLocked) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock layer sha256:%s: %w", hexDigest[:12], err)
	}
	defer func() { _ = use.Close() }()

	var errs []error
	left := false
	for _, name := range []string{hexDigest, hexDigest + ".partial"} {
		path := filepath.Join(c.dir, name)
		if !del(path) {
			if _, err := os.Lstat(path); err == nil {
				left = true
			}
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
			left = true
		}
	}
	if left {
		return errors.Join(errs...)
	}
	// Unlinked while held, so anyone who opened them earlier finds them gone
	// once it gets the lock. The download lock goes first: once .use is
	// unlinked, a newcomer locks a new one and must not find the old .dl.
	for _, kind := range []string{".dl", ".use"} {
		if err := os.Remove(c.lockPath(hexDigest, kind)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func isHexDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
