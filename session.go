package justpull

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// SessionOptions tunes a Session. The zero value serves the image the
// reference names.
type SessionOptions struct{}

// Session is the proxy of one pull without the Docker calls, for a caller
// that drives Docker itself: Docker pulls LocalRef, the caller tags the
// image, and Finish ends the session. Pull is built on it.
type Session struct {
	c      *Cache
	t      target
	p      *proxy
	srv    *http.Server
	ctx    context.Context
	cancel context.CancelCauseFunc
	local  string
	stop   sync.Once
}

// Serve sweeps the cache, then serves ref on 127.0.0.1 until Stop or Finish.
func (c *Cache) Serve(ctx context.Context, ref string, opts SessionOptions) (*Session, error) {
	t, err := parseRef(ref)
	if err != nil {
		return nil, fmt.Errorf("justpull: %w", err)
	}
	if err := c.Sweep(); err != nil {
		c.opts.Logger.Warn("cache sweep failed", "err", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("justpull: listen: %w", err)
	}
	sctx, cancel := context.WithCancelCause(ctx)
	p := newProxy(sctx, c, t, cancel)
	s := &Session{
		c:      c,
		t:      t,
		p:      p,
		ctx:    sctx,
		cancel: cancel,
		local:  t.localRef(ln.Addr().String()),
		srv: &http.Server{
			Handler:           p,
			ReadHeaderTimeout: 30 * time.Second,
			BaseContext:       func(net.Listener) context.Context { return sctx },
		},
	}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

// LocalRef is the reference Docker pulls: 127.0.0.1:<port>/<repository>
// with the tag or digest of the reference served.
func (s *Session) LocalRef() string { return s.local }

// Context ends when a layer fails for good, with that failure as its cause,
// or when Stop runs. The caller then cancels Docker's pull.
func (s *Session) Context() context.Context { return s.ctx }

// LayerSize returns the size the image's manifests give for the layer that
// Docker's progress stream names by its 12-character ID, or 0 if unknown.
func (s *Session) LayerSize(shortID string) int64 { return s.p.layerSize(shortID) }

// Stop stops serving, waits for running requests and downloads, and lets
// other processes clean up the image's blobs again. The cached files stay.
// Calling it again does nothing.
func (s *Session) Stop() {
	s.stop.Do(func() {
		s.cancel(nil)
		// Handlers return quickly once the context ends; waiting for them means
		// no request still holds a blob when the files are removed.
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		if err := s.srv.Shutdown(sctx); err != nil {
			_ = s.srv.Close()
		}
		s.p.wg.Wait()
		s.p.unpin()
	})
}

// Finish stops the session. After a successful pull it deletes the image's
// cached blobs, except those another process is using, and its saved
// manifests; after a failed one they stay, so the next pull of an image
// sharing those layers continues from them. The error is about deleting.
func (s *Session) Finish(success bool) error {
	s.Stop()
	if !success {
		return nil
	}
	blobs, manifests := s.p.imageFiles()
	var errs []error
	if err := s.c.removeBlobs(blobs); err != nil {
		errs = append(errs, fmt.Errorf("justpull: delete cached layers: %w", err))
	}
	if err := s.c.removeManifests(manifests); err != nil {
		errs = append(errs, fmt.Errorf("justpull: delete cached manifests: %w", err))
	}
	return errors.Join(errs...)
}
