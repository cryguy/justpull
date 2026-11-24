package justpull

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/distribution/reference"
)

// maxManifestBytes caps a relayed manifest or tag list page, matching the
// registry distribution's own limit for manifests.
const maxManifestBytes = 4 << 20

// proxy serves the pull subset of the registry API for one repository:
// manifests and the tag list relayed from the real registry, blobs from the
// cache. Any other repository is unknown, so the listener is no use to
// anyone else on the machine.
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
	pins      map[string]*blob // this image's blobs, held until the pull ends; nil while being acquired
	unpinned  bool
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
		pins:      map[string]*blob{},
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
	// A repository path may itself have a segment named manifests or blobs,
	// so the route is what follows the served repository.
	route, ok := strings.CutPrefix(rest, p.t.path+"/")
	if !ok {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "repository not served here")
		return
	}
	if route == "tags/list" {
		p.tags(w, r)
		return
	}
	if ref, ok := strings.CutPrefix(route, "manifests/"); ok {
		// The reference goes into the registry URL, with the login.
		if !manifestRef.MatchString(ref) {
			writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "not a tag or digest")
			return
		}
		p.manifest(w, r, ref)
		return
	}
	if digest, ok := strings.CutPrefix(route, "blobs/"); ok {
		p.blob(w, r, digest)
		return
	}
	writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "not found")
}

// manifestRef is a tag or a digest, which is all a manifest request names.
var manifestRef = regexp.MustCompile(`^(?:` + reference.TagRegexp.String() + `|` + reference.DigestRegexp.String() + `)$`)

// manifest relays a manifest byte for byte, with the headers dockerd
// verifies and dispatches on. A registry challenge is not passed on, so
// dockerd never tries to log in to the proxy.
func (p *proxy) manifest(w http.ResponseWriter, r *http.Request, ref string) {
	resp, err := p.c.up.manifest(r.Context(), p.t.scheme, r.Method, p.t.remote, ref, r.Header.Values("Accept"))
	if err != nil {
		if isPermanent(err) {
			p.fail(err)
		}
		writeError(w, http.StatusBadGateway, "UNKNOWN", "registry unreachable: "+err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for _, k := range []string{"Content-Type", "Docker-Content-Digest", "Etag"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	refused := resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden
	if r.Method == http.MethodHead {
		if refused {
			p.refuse(resp.Status, nil)
		}
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
	if refused {
		p.refuse(resp.Status, body)
	}
	if resp.StatusCode == http.StatusOK {
		p.recordManifest(p.c.saveManifest(body), body, 0)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// tags relays a page of the repository's tag list, which docker pull
// --all-tags reads, with the paging query Docker sent.
func (p *proxy) tags(w http.ResponseWriter, r *http.Request) {
	resp, err := p.c.up.tags(r.Context(), p.t.scheme, r.Method, p.t.remote, r.URL.RawQuery)
	if err != nil {
		if isPermanent(err) {
			p.fail(err)
		}
		writeError(w, http.StatusBadGateway, "UNKNOWN", "registry unreachable: "+err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()
	link := resp.Header.Get("Link")
	if link != "" {
		if link, err = p.tagListLink(link); err != nil {
			writeError(w, http.StatusBadGateway, "UNKNOWN", err.Error())
			return
		}
	}
	var body []byte
	if r.Method == http.MethodGet {
		body, err = io.ReadAll(io.LimitReader(resp.Body, maxManifestBytes+1))
		if err != nil {
			writeError(w, http.StatusBadGateway, "UNKNOWN", "reading tag list: "+err.Error())
			return
		}
		if len(body) > maxManifestBytes {
			writeError(w, http.StatusBadGateway, "UNKNOWN", "tag list larger than 4 MiB")
			return
		}
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		p.refuse(resp.Status, body)
	}
	if v := resp.Header.Get("Content-Type"); v != "" {
		w.Header().Set("Content-Type", v)
	}
	if link != "" {
		w.Header().Set("Link", link)
	}
	if r.Method == http.MethodHead {
		if v := resp.Header.Get("Content-Length"); v != "" {
			w.Header().Set("Content-Length", v)
		}
		w.WriteHeader(resp.StatusCode)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// tagListLink points the registry's Link to the next page of the tag list
// at the proxy, keeping its query and parameters. dockerd resolves the URL
// between < and > against the page it asked for, so an absolute URL, which
// some registries send, would take it to the registry itself.
func (p *proxy) tagListLink(v string) (string, error) {
	target, params, hasParams := strings.Cut(v, ";")
	next, err := url.Parse(strings.Trim(strings.TrimSpace(target), "<>"))
	if err != nil {
		return "", fmt.Errorf("registry sent an unusable tag list Link: %w", err)
	}
	link := "</v2/" + p.t.path + "/tags/list"
	if next.RawQuery != "" {
		link += "?" + next.RawQuery
	}
	link += ">"
	if hasParams {
		link += ";" + params
	}
	return link, nil
}

// refuse fails the pull with the registry's refusal of a manifest or the tag
// list, which registryDo could not answer with a login. It must run before
// the status is relayed: Pull reports this cause only if it is set when
// Docker gives up.
func (p *proxy) refuse(status string, body []byte) {
	msg := strings.TrimSpace(string(body[:min(len(body), 512)]))
	if msg != "" {
		msg = ": " + msg
	}
	p.fail(permanent(refused(fmt.Errorf("registry %s refused access to %s: %s%s", p.t.remote.registry, p.t.path, status, msg))))
}

// recordManifest notes a manifest as part of this pull's image along with
// the blob sizes it lists, so downloads can be checked against them and the
// image's files found for cleanup, and pins those blobs. An index's children
// are followed if a copy of them was saved.
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
	pin := map[string]int64{}
	p.mu.Lock()
	if hexDigest != "" {
		p.manifests[hexDigest] = true
	}
	for _, d := range descs {
		if h, ok := strings.CutPrefix(d.Digest, "sha256:"); ok && isHexDigest(h) && d.Size > 0 {
			p.sizes[h] = d.Size
			if _, held := p.pins[h]; !held && !p.unpinned {
				p.pins[h] = nil
				pin[h] = d.Size
			}
		}
	}
	p.mu.Unlock()
	for h, size := range pin {
		p.pin(h, size)
	}
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

// pin holds a blob of the image in the cache until the pull ends, so another
// process's cleanup cannot delete it between Docker's requests.
func (p *proxy) pin(hexDigest string, size int64) {
	b, err := p.c.acquire(hexDigest, size)
	if err != nil {
		p.fail(err)
		return
	}
	p.mu.Lock()
	if p.unpinned {
		// The pull ended while this handler ran on: a server that does not
		// shut down in time is closed without waiting for its handlers.
		p.mu.Unlock()
		p.c.release(b)
		return
	}
	p.pins[hexDigest] = b
	p.mu.Unlock()
}

// unpin releases every pin; pins asked for later are released at once.
func (p *proxy) unpin() {
	p.mu.Lock()
	pins := p.pins
	p.pins, p.unpinned = nil, true
	p.mu.Unlock()
	for _, b := range pins {
		if b != nil {
			p.c.release(b)
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

	b, err := p.c.acquire(hexDigest, size)
	if err != nil {
		p.fail(err)
		writeError(w, http.StatusInternalServerError, "UNKNOWN", err.Error())
		return
	}
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
