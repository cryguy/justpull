// Package justpull pulls container images through a local
// pull-through proxy that keeps every downloaded byte on disk, so a pull
// broken by the network, a failed attempt or a restart continues where it
// stopped instead of starting over.
//
// Docker is pointed at 127.0.0.1:<port>/<repository>, which dockerd treats
// as an insecure registry by default. It asks the proxy only for the blobs
// it does not have, on either image store; the proxy fetches them from the
// real registry, resuming after any break, and serves them while they
// download, so Docker's own connection never breaks. After a successful
// pull the image gets its real name, the localhost name is removed and the
// cached files are deleted.
package justpull

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/distribution/reference"
)

// Docker is the part of the Docker API a pull needs. References are plain
// image references. An implementation over the Docker client:
//
//	Pull:  ImagePull(ref) with no registry auth, returning the progress stream
//	Tag:   ImageTag(source, target)
//	Untag: ImageRemove(ref) with Force and PruneChildren off
type Docker interface {
	Pull(ctx context.Context, ref string) (io.ReadCloser, error)
	Tag(ctx context.Context, source, target string) error
	Untag(ctx context.Context, ref string) error
}

// Progress is how far a pull's downloads are, in compressed bytes. Layers
// Docker already has are not counted.
type Progress struct {
	Done  int64
	Total int64
}

// target is the image a pull asked for.
type target struct {
	remote   remote
	scheme   string
	path     string // repository path, e.g. library/ubuntu
	tag      string
	digest   string
	original string // the image's real name, given back after the pull
}

func parseRef(ref string) (target, error) {
	named, err := reference.ParseNormalizedNamed(ref)
	if err != nil {
		return target{}, fmt.Errorf("parse image reference %q: %w", ref, err)
	}
	domain, path := reference.Domain(named), reference.Path(named)
	host := domain
	if domain == "docker.io" {
		host = "registry-1.docker.io"
	}
	t := target{remote: remote{host: host, repo: path, registry: domain}, scheme: schemeFor(host), path: path}
	if d, ok := named.(reference.Digested); ok {
		t.digest = d.Digest().String()
		t.original = reference.TrimNamed(named).String() + "@" + t.digest
		return t, nil
	}
	tagged := reference.TagNameOnly(named).(reference.Tagged)
	t.tag = tagged.Tag()
	t.original = tagged.String()
	return t, nil
}

// schemeFor follows dockerd's default: registries on loopback are plain
// HTTP, everything else HTTPS.
func schemeFor(host string) string {
	name := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	}
	if isLoopback(name) {
		return "http"
	}
	return "https"
}

// isLoopback reports whether a host name without port is localhost or a
// loopback address.
func isLoopback(name string) bool {
	if name == "localhost" {
		return true
	}
	ip := net.ParseIP(name)
	return ip != nil && ip.IsLoopback()
}

func (t target) localRef(addr string) string {
	if t.digest != "" {
		return addr + "/" + t.path + "@" + t.digest
	}
	return addr + "/" + t.path + ":" + t.tag
}

// Pull pulls ref into Docker through the cache. On success the image is
// known by ref, as after a plain docker pull, and this pull's cached files
// are gone. On failure or cancel they stay, so the next Pull of an image
// sharing those layers continues where this one stopped. progress may be
// nil; it is called from Pull's goroutine.
func (c *Cache) Pull(ctx context.Context, d Docker, ref string, progress func(Progress)) error {
	t, err := parseRef(ref)
	if err != nil {
		return fmt.Errorf("justpull: %w", err)
	}
	if err := c.Sweep(); err != nil {
		c.opts.Logger.Warn("cache sweep failed", "err", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("justpull: listen: %w", err)
	}
	pctx, cancel := context.WithCancelCause(ctx)
	p := newProxy(pctx, c, t, cancel)
	srv := &http.Server{
		Handler:           p,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return pctx },
	}
	go func() { _ = srv.Serve(ln) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel(nil)
			// Handlers return quickly once pctx ends; waiting for them means
			// no request still holds a blob when the files are removed.
			sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer scancel()
			if err := srv.Shutdown(sctx); err != nil {
				_ = srv.Close()
			}
			p.wg.Wait()
			p.unpin()
		})
	}
	defer stop()

	local := t.localRef(ln.Addr().String())
	if err := dockerPull(pctx, d, local, p.layerSize, progress); err != nil {
		switch cause := context.Cause(pctx); {
		case ctx.Err() != nil:
			err = ctx.Err()
		case cause != nil && !errors.Is(cause, context.Canceled):
			// A layer failure cancelled Docker's pull; it says why.
			err = cause
		}
		return fmt.Errorf("justpull: pull %s: %w", ref, err)
	}
	stop()

	if t.digest == "" {
		if err := d.Tag(ctx, local, t.original); err != nil {
			return fmt.Errorf("justpull: tag %s: %w", t.original, err)
		}
	} else if err := dockerPull(ctx, d, t.original, nil, nil); err != nil {
		// A digest reference cannot be created with a tag. Pulling it costs
		// one manifest request: every layer is already in Docker.
		return fmt.Errorf("justpull: pull %s: %w", t.original, err)
	}
	if err := d.Untag(ctx, local); err != nil {
		c.opts.Logger.Warn("could not remove the localhost image name", "ref", local, "err", err)
	}
	blobs, manifests := p.imageFiles()
	if err := c.removeBlobs(blobs); err != nil {
		c.opts.Logger.Warn("could not delete cached layers", "err", err)
	}
	if err := c.removeManifests(manifests); err != nil {
		c.opts.Logger.Warn("could not delete cached manifests", "err", err)
	}
	return nil
}

func dockerPull(ctx context.Context, d Docker, ref string, layerSize func(shortID string) int64, progress func(Progress)) error {
	rc, err := d.Pull(ctx, ref)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return readStream(rc, layerSize, progress)
}

// readStream consumes Docker's pull stream (newline-delimited JSON). The
// outcome is inside the stream: a failure is an {"error":...} message, not
// an error from the call. Progress counts each layer at the size its
// manifest gives (from layerSize, by the stream's 12-character layer ID),
// falling back to the size Docker reports while downloading.
func readStream(r io.Reader, layerSize func(shortID string) int64, progress func(Progress)) error {
	type layer struct {
		size, done int64
		skip       bool
	}
	layers := map[string]*layer{}
	var last Progress
	dec := json.NewDecoder(r)
	for {
		var m struct {
			ID             string `json:"id"`
			Status         string `json:"status"`
			Error          string `json:"error"`
			ProgressDetail struct {
				Current int64 `json:"current"`
				Total   int64 `json:"total"`
			} `json:"progressDetail"`
		}
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read docker pull stream: %w", err)
		}
		if m.Error != "" {
			return errors.New(m.Error)
		}
		if progress == nil || m.ID == "" {
			continue
		}
		switch m.Status {
		case "Pulling fs layer", "Waiting", "Downloading", "Verifying Checksum",
			"Download complete", "Extracting", "Pull complete", "Already exists":
		default:
			continue
		}
		l := layers[m.ID]
		if l == nil {
			l = &layer{}
			if layerSize != nil {
				l.size = layerSize(m.ID)
			}
			layers[m.ID] = l
		}
		switch m.Status {
		case "Already exists":
			l.skip = true
		case "Downloading":
			if l.size <= 0 {
				l.size = m.ProgressDetail.Total
			}
			l.done = min(m.ProgressDetail.Current, l.size)
		case "Verifying Checksum", "Download complete", "Extracting", "Pull complete":
			l.done = l.size
		}
		var p Progress
		for _, l := range layers {
			if !l.skip && l.size > 0 {
				p.Done += l.done
				p.Total += l.size
			}
		}
		if p != last {
			last = p
			progress(p)
		}
	}
}

// shortIDMatch reports whether a stream layer ID names this hex digest.
func shortIDMatch(shortID, hexDigest string) bool {
	return len(shortID) >= 12 && strings.HasPrefix(hexDigest, shortID)
}
