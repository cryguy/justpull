package justpull

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// maxManifestBytes caps a relayed manifest, matching the registry
// distribution's own limit.
const maxManifestBytes = 4 << 20

// proxy serves the pull subset of the registry API for one repository:
// manifests relayed from the real registry, blobs from the cache. Any other
// repository is unknown, so the listener is no use to anyone else on the
// machine.
type proxy struct {
	ctx  context.Context
	c    *Cache
	t    target
	fail func(error)
	wg   sync.WaitGroup // running downloads

	mu        sync.Mutex
	sizes     map[string]int64 // hex digest -> size, from this image's manifests
	touched   map[string]bool  // blobs Docker asked for
	manifests map[string]bool  // this image's manifests
}

func newProxy(ctx context.Context, c *Cache, t target, cancel context.CancelCauseFunc) *proxy {
	return &proxy{
		ctx:       ctx,
		c:         c,
		t:         t,
		fail:      func(err error) { cancel(err) },
		sizes:     map[string]int64{},
		touched:   map[string]bool{},
		manifests: map[string]bool{},
	}
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "pull only")
		return
	}
	if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}")
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v2/")
	if !ok {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "not found")
		return
	}
	if i := strings.LastIndex(rest, "/manifests/"); i > 0 {
		if rest[:i] != p.t.path {
			writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "repository not served here")
			return
		}
		p.manifest(w, r, rest[i+len("/manifests/"):])
		return
	}
	if i := strings.LastIndex(rest, "/blobs/"); i > 0 {
		if rest[:i] != p.t.path {
			writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "repository not served here")
			return
		}
		p.blob(w, r, rest[i+len("/blobs/"):])
		return
	}
	writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "not found")
}

// manifest relays a manifest byte for byte, with the headers dockerd
// verifies and dispatches on. A registry challenge is not passed on, so
// dockerd never tries to log in to the proxy.
func (p *proxy) manifest(w http.ResponseWriter, r *http.Request, ref string) {
	resp, err := p.c.up.manifest(r.Context(), p.t.scheme, r.Method, p.t.remote, ref, r.Header.Values("Accept"))
	if err != nil {
		writeError(w, http.StatusBadGateway, "UNKNOWN", "registry unreachable: "+err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for _, k := range []string{"Content-Type", "Docker-Content-Digest", "Etag"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	if r.Method == http.MethodHead {
		if v := resp.Header.Get("Content-Length"); v != "" {
			w.Header().Set("Content-Length", v)
		}
		// The containerd store skips the GET for a manifest it still holds
		// from an earlier attempt; a copy saved then still names the blobs.
		if h, ok := strings.CutPrefix(resp.Header.Get("Docker-Content-Digest"), "sha256:"); ok && resp.StatusCode == http.StatusOK {
			if body, found := p.c.loadManifest(h); found {
				p.recordManifest(h, body, 0)
			}
		}
		w.WriteHeader(resp.StatusCode)
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestBytes+1))
	if err != nil {
		writeError(w, http.StatusBadGateway, "UNKNOWN", "reading manifest: "+err.Error())
		return
	}
	if len(body) > maxManifestBytes {
		writeError(w, http.StatusBadGateway, "UNKNOWN", "manifest larger than 4 MiB")
		return
	}
	if resp.StatusCode == http.StatusOK {
		p.recordManifest(p.c.saveManifest(body), body, 0)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// recordManifest notes a manifest as part of this pull's image along with
// the blob sizes it lists, so downloads can be checked against them and the
// image's files found for cleanup. An index's children are followed if a
// copy of them was saved.
func (p *proxy) recordManifest(hexDigest string, manifest []byte, depth int) {
	type descriptor struct {
		Digest string `json:"digest"`
		Size   int64  `json:"size"`
	}
	var m struct {
		Config    *descriptor  `json:"config"`
		Layers    []descriptor `json:"layers"`
		Manifests []descriptor `json:"manifests"`
	}
	if json.Unmarshal(manifest, &m) != nil {
		return
	}
	descs := m.Layers
	if m.Config != nil {
		descs = append(descs, *m.Config)
	}
	p.mu.Lock()
	if hexDigest != "" {
		p.manifests[hexDigest] = true
	}
	for _, d := range descs {
		if h, ok := strings.CutPrefix(d.Digest, "sha256:"); ok && isHexDigest(h) && d.Size > 0 {
			p.sizes[h] = d.Size
		}
	}
	p.mu.Unlock()
	if depth > 0 {
		return
	}
	for _, child := range m.Manifests {
		h, ok := strings.CutPrefix(child.Digest, "sha256:")
		if !ok {
			continue
		}
		if body, found := p.c.loadManifest(h); found {
			p.recordManifest(h, body, depth+1)
		}
	}
}

func (p *proxy) layerSize(shortID string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	for h, size := range p.sizes {
		if shortIDMatch(shortID, h) {
			return size
		}
	}
	return 0
}

// imageFiles lists the cached files of this pull's image: every blob its
// manifests name or Docker asked for, and the saved manifests. Docker skips
// blobs it still has from an earlier attempt; their cached copies belong to
// the image all the same.
func (p *proxy) imageFiles() (blobs, manifests []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for h := range p.sizes {
		blobs = append(blobs, h)
	}
	for h := range p.touched {
		if _, ok := p.sizes[h]; !ok {
			blobs = append(blobs, h)
		}
	}
	for h := range p.manifests {
		manifests = append(manifests, h)
	}
	return blobs, manifests
}

// blob serves a blob from the cache, starting its download if needed, and
// honours dockerd's Range requests. If the download fails, the connection
// is aborted so dockerd cannot take a short body for a whole one.
func (p *proxy) blob(w http.ResponseWriter, r *http.Request, digest string) {
	hexDigest, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || !isHexDigest(hexDigest) {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "only sha256 blobs are served")
		return
	}
	p.mu.Lock()
	p.touched[hexDigest] = true
	size := p.sizes[hexDigest]
	p.mu.Unlock()

	b := p.c.acquire(hexDigest, size)
	defer p.c.release(b)
	fetch := func(ctx context.Context, offset int64) (io.ReadCloser, int64, int64, error) {
		return p.c.up.openBlob(ctx, p.t.scheme, p.t.remote, digest, offset)
	}
	p.c.ensureDownload(p.ctx, b, fetch, p.fail, &p.wg)

	total, err := b.waitSize(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "UNKNOWN", "layer download failed: "+err.Error())
		return
	}
	h := w.Header()
	h.Set("Docker-Content-Digest", digest)
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Accept-Ranges", "bytes")
	if r.Method == http.MethodHead {
		h.Set("Content-Length", strconv.FormatInt(total, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	off, end, ranged, ok := parseRange(r.Header.Get("Range"), total)
	if !ok {
		h.Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "BLOB_UNKNOWN", "range not satisfiable")
		return
	}
	h.Set("Content-Length", strconv.FormatInt(end-off+1, 10))
	status := http.StatusOK
	if ranged {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, total))
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)
	rc := http.NewResponseController(w)
	if err := b.serve(r.Context(), w, func() { _ = rc.Flush() }, off, end); err != nil {
		panic(http.ErrAbortHandler)
	}
}

// parseRange reads a single "bytes=" range against a blob of size bytes.
func parseRange(h string, size int64) (off, end int64, ranged, ok bool) {
	if h == "" {
		return 0, size - 1, false, true
	}
	spec, found := strings.CutPrefix(h, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false, false
	}
	if first == "" {
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 || size == 0 {
			return 0, 0, false, false
		}
		return max(size-n, 0), size - 1, true, true
	}
	off, err := strconv.ParseInt(first, 10, 64)
	if err != nil || off < 0 || off >= size {
		return 0, 0, false, false
	}
	end = size - 1
	if last != "" {
		e, err := strconv.ParseInt(last, 10, 64)
		if err != nil || e < off {
			return 0, 0, false, false
		}
		end = min(e, size-1)
	}
	return off, end, true, true
}

// writeError writes a registry-protocol error body.
func writeError(w http.ResponseWriter, status int, code, message string) {
	type entry struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	body, _ := json.Marshal(struct {
		Errors []entry `json:"errors"`
	}{[]entry{{code, message}}})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
