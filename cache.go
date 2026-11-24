package dockercachepull

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
}

// Cache stores blobs on disk by digest and pulls images through them, so a
// download broken by the network, a failed pull or a restart continues where
// it stopped. Layout: <dir>/blobs/sha256/<hex> once verified,
// <hex>.partial while downloading; <dir>/manifests/sha256/<hex> for the
// manifests relayed, which name an image's blobs for cleanup.
//
// One Cache should own a directory; its methods are safe for concurrent use.
type Cache struct {
	dir  string // blobs
	mdir string // manifests
	opts Options

	// Fixed by New; tests shorten them.
	idleTimeout time.Duration
	retryBase   time.Duration
	retryMax    time.Duration

	up *upstream

	mu    sync.Mutex
	blobs map[string]*blob // blobs held by a request or a running download
}

// New opens (creating if needed) a cache rooted at dir.
func New(dir string, opts Options) (*Cache, error) {
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
	for _, d := range []string{bdir, mdir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("dockercachepull: create cache dir: %w", err)
		}
	}
	return &Cache{
		dir:         bdir,
		mdir:        mdir,
		opts:        opts,
		idleTimeout: defaultIdleTimeout,
		retryBase:   defaultRetryBase,
		retryMax:    defaultRetryMax,
		up:          newUpstream(),
		blobs:       map[string]*blob{},
	}, nil
}

// fetchFunc opens a blob's bytes from offset on. start is where the body
// really begins (a server may ignore Range); total is the blob's size, or -1
// if the server did not say.
type fetchFunc func(ctx context.Context, offset int64) (body io.ReadCloser, start, total int64, err error)

type blob struct {
	hex string
	dir string

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
func (c *Cache) acquire(hexDigest string, size int64) *blob {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.blobs[hexDigest]
	if b == nil {
		b = &blob{hex: hexDigest, dir: c.dir, size: -1, changed: make(chan struct{})}
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
	return b
}

func (c *Cache) release(b *blob) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refs--
	if b.refs == 0 && !b.running {
		delete(c.blobs, b.hex)
	}
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
		err := c.download(ctx, b, fetch)
		c.mu.Lock()
		b.mu.Lock()
		b.running = false
		if err != nil {
			b.err = err
		}
		b.notifyLocked()
		if b.refs == 0 {
			delete(c.blobs, b.hex)
		}
		b.mu.Unlock()
		c.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			onFail(fmt.Errorf("layer sha256:%s: %w", b.hex[:12], err))
		}
	}()
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
// not in use. Pull runs it first; a long-running caller should also run it
// periodically so a cancelled pull's files go even when no new pull comes.
func (c *Cache) Sweep() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-c.opts.MaxAge)
	return errors.Join(
		sweepDir(c.dir, cutoff, func(name string) bool {
			hexDigest := strings.TrimSuffix(name, ".partial")
			return isHexDigest(hexDigest) && c.blobs[hexDigest] == nil
		}),
		sweepDir(c.mdir, cutoff, func(name string) bool {
			return isHexDigest(name) || strings.HasPrefix(name, ".tmp-")
		}),
	)
}

// sweepDir deletes the files in dir that ours accepts and that were last
// written before cutoff.
func sweepDir(dir string, cutoff time.Time, ours func(name string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("dockercachepull: sweep: %w", err)
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

func (c *Cache) removeManifests(hexDigests []string) error {
	var errs []error
	for _, hexDigest := range hexDigests {
		if err := os.Remove(filepath.Join(c.mdir, hexDigest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// removeBlobs deletes the files of the given blobs that no request or
// download is using.
func (c *Cache) removeBlobs(hexDigests []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, hexDigest := range hexDigests {
		if c.blobs[hexDigest] != nil {
			continue
		}
		for _, name := range []string{hexDigest, hexDigest + ".partial"} {
			if err := os.Remove(filepath.Join(c.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, err)
			}
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
