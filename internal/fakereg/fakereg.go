// Package fakereg is an in-process OCI registry for tests: images with
// random layers, optional bearer auth, optional redirects to a separate
// storage host whose links expire, and scripted faults on blob GETs.
package fakereg

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fault is what one blob GET does wrong.
type Fault int

const (
	None        Fault = iota
	Cut               // send half the body, then close the connection
	Reset             // send half the body, then reset the connection
	Stall             // send half the body, then nothing until the client leaves
	Corrupt           // send the body with one byte flipped
	IgnoreRange       // answer a Range request with 200 and the whole blob
	WrongStart        // answer a Range request from one byte later
	Deny              // 401 with no challenge, like a proxy's DENIED
)

// Blob is one blob the registry serves.
type Blob struct {
	Digest string // sha256:<hex>
	Data   []byte

	reg    *Registry
	faults []Fault
	broken bool
}

// Faults queues faults, one per GET of this blob.
func (b *Blob) Faults(fs ...Fault) {
	b.reg.mu.Lock()
	defer b.reg.mu.Unlock()
	b.faults = append(b.faults, fs...)
}

// SetBroken makes every GET (after queued faults) reset before any byte.
func (b *Blob) SetBroken(v bool) {
	b.reg.mu.Lock()
	defer b.reg.mu.Unlock()
	b.broken = v
}

// Image is one image the registry serves under repo:tag and repo@digest.
type Image struct {
	Repo     string
	Tag      string
	Digest   string
	Manifest []byte
	Config   *Blob
	Layers   []*Blob
}

// Request is one request the registry or its storage host received.
type Request struct {
	Host   string // "registry", "storage" or "token"
	Method string
	Path   string
	Range  string
	Auth   string
	Status int
}

// Registry is the fake. Configure it before the first request.
type Registry struct {
	Host string // 127.0.0.1:<port>

	// TokenTTL, when set, requires bearer tokens that live this long.
	TokenTTL time.Duration
	// Redirect sends blob GETs to a storage host with links that live
	// LinkTTL (forever when zero).
	Redirect bool
	LinkTTL  time.Duration

	t       testing.TB
	srv     *httptest.Server
	storage *httptest.Server

	mu     sync.Mutex
	images map[string]*Image // "repo:tag" and "repo@digest"
	blobs  map[string]*Blob
	tokens map[string]time.Time
	log    []Request
}

// New starts a registry that stops when the test ends.
func New(t testing.TB) *Registry {
	r := &Registry{t: t, images: map[string]*Image{}, blobs: map[string]*Blob{}, tokens: map[string]time.Time{}}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serveRegistry))
	r.storage = httptest.NewServer(http.HandlerFunc(r.serveStorage))
	r.Host = strings.TrimPrefix(r.srv.URL, "http://")
	t.Cleanup(func() { r.srv.Close(); r.storage.Close() })
	return r
}

// AddImage adds repo:tag with one random layer per size.
func (r *Registry) AddImage(repo, tag string, layerSizes ...int) *Image {
	r.t.Helper()
	img := &Image{Repo: repo, Tag: tag}
	var diffIDs []string
	type desc struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Size      int    `json:"size"`
	}
	var layers []desc
	for _, size := range layerSizes {
		data, diffID := r.layer(size)
		b := r.addBlob(data)
		img.Layers = append(img.Layers, b)
		diffIDs = append(diffIDs, diffID)
		layers = append(layers, desc{"application/vnd.oci.image.layer.v1.tar+gzip", b.Digest, len(data)})
	}
	cfg, _ := json.Marshal(map[string]any{
		"architecture": "amd64",
		"os":           "linux",
		"config":       map[string]any{},
		"rootfs":       map[string]any{"type": "layers", "diff_ids": diffIDs},
	})
	img.Config = r.addBlob(cfg)
	img.Manifest, _ = json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     "application/vnd.oci.image.manifest.v1+json",
		"config":        desc{"application/vnd.oci.image.config.v1+json", img.Config.Digest, len(cfg)},
		"layers":        layers,
	})
	img.Digest = digestOf(img.Manifest)
	r.mu.Lock()
	r.images[repo+":"+tag] = img
	r.images[repo+"@"+img.Digest] = img
	r.mu.Unlock()
	return img
}

func (r *Registry) layer(size int) ([]byte, string) {
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		r.t.Fatal(err)
	}
	var tb bytes.Buffer
	tw := tar.NewWriter(&tb)
	if err := tw.WriteHeader(&tar.Header{Name: "data", Mode: 0o644, Size: int64(size), Typeflag: tar.TypeReg}); err != nil {
		r.t.Fatal(err)
	}
	_, _ = tw.Write(payload)
	_ = tw.Close()
	diffID := digestOf(tb.Bytes())
	var gb bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gb, gzip.BestSpeed)
	_, _ = zw.Write(tb.Bytes())
	_ = zw.Close()
	return gb.Bytes(), diffID
}

func (r *Registry) addBlob(data []byte) *Blob {
	b := &Blob{Digest: digestOf(data), Data: data, reg: r}
	r.mu.Lock()
	r.blobs[b.Digest] = b
	r.mu.Unlock()
	return b
}

// Log returns every request so far.
func (r *Registry) Log() []Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Request(nil), r.log...)
}

func (r *Registry) record(req Request) {
	r.mu.Lock()
	r.log = append(r.log, req)
	r.mu.Unlock()
}

func (r *Registry) serveRegistry(w http.ResponseWriter, req *http.Request) {
	entry := Request{Host: "registry", Method: req.Method, Path: req.URL.Path, Range: req.Header.Get("Range"), Auth: req.Header.Get("Authorization")}
	sw := &statusWriter{ResponseWriter: w}
	defer func() { entry.Status = sw.status; r.record(entry) }()
	w = sw

	if req.URL.Path == "/token" {
		entry.Host = "token"
		tok := fmt.Sprintf("tok-%d", time.Now().UnixNano())
		r.mu.Lock()
		r.tokens[tok] = time.Now().Add(r.TokenTTL)
		r.mu.Unlock()
		fmt.Fprintf(w, `{"token":%q}`, tok)
		return
	}
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if r.TokenTTL > 0 {
		r.mu.Lock()
		exp, ok := r.tokens[strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")]
		r.mu.Unlock()
		if !ok || time.Now().After(exp) {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="fakereg"`, r.srv.URL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	if req.URL.Path == "/v2/" {
		_, _ = w.Write([]byte("{}"))
		return
	}
	rest := strings.TrimPrefix(req.URL.Path, "/v2/")
	if i := strings.LastIndex(rest, "/manifests/"); i > 0 {
		repo, ref := rest[:i], rest[i+len("/manifests/"):]
		sep := ":"
		if strings.HasPrefix(ref, "sha256:") {
			sep = "@"
		}
		r.mu.Lock()
		img := r.images[repo+sep+ref]
		r.mu.Unlock()
		if img == nil {
			http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.Header().Set("Docker-Content-Digest", img.Digest)
		w.Header().Set("Content-Length", strconv.Itoa(len(img.Manifest)))
		if req.Method != http.MethodHead {
			_, _ = w.Write(img.Manifest)
		}
		return
	}
	if i := strings.LastIndex(rest, "/blobs/"); i > 0 {
		r.mu.Lock()
		b := r.blobs[rest[i+len("/blobs/"):]]
		r.mu.Unlock()
		if b == nil {
			http.Error(w, `{"errors":[{"code":"BLOB_UNKNOWN"}]}`, http.StatusNotFound)
			return
		}
		if r.Redirect {
			if r.peekFault(b) == Deny {
				r.serveBytes(w, req, b)
				return
			}
			exp := "0"
			if r.LinkTTL > 0 {
				exp = strconv.FormatInt(time.Now().Add(r.LinkTTL).UnixNano(), 10)
			}
			http.Redirect(w, req, r.storage.URL+"/blob/"+strings.TrimPrefix(b.Digest, "sha256:")+"?exp="+exp, http.StatusTemporaryRedirect)
			return
		}
		r.serveBytes(w, req, b)
		return
	}
	http.NotFound(w, req)
}

func (r *Registry) serveStorage(w http.ResponseWriter, req *http.Request) {
	entry := Request{Host: "storage", Method: req.Method, Path: req.URL.Path, Range: req.Header.Get("Range"), Auth: req.Header.Get("Authorization")}
	sw := &statusWriter{ResponseWriter: w}
	defer func() { entry.Status = sw.status; r.record(entry) }()
	w = sw

	if exp, _ := strconv.ParseInt(req.URL.Query().Get("exp"), 10, 64); exp != 0 && time.Now().UnixNano() > exp {
		http.Error(w, "<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>", http.StatusForbidden)
		return
	}
	r.mu.Lock()
	b := r.blobs["sha256:"+strings.TrimPrefix(req.URL.Path, "/blob/")]
	r.mu.Unlock()
	if b == nil {
		http.NotFound(w, req)
		return
	}
	r.serveBytes(w, req, b)
}

func (r *Registry) peekFault(b *Blob) Fault {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(b.faults) > 0 {
		return b.faults[0]
	}
	return None
}

func (r *Registry) serveBytes(w http.ResponseWriter, req *http.Request, b *Blob) {
	r.mu.Lock()
	fault := None
	switch {
	case len(b.faults) > 0:
		fault = b.faults[0]
		b.faults = b.faults[1:]
	case b.broken:
		fault = Reset
	}
	broken := b.broken && fault == Reset
	r.mu.Unlock()

	if fault == Deny {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errors":[{"code":"DENIED","message":"upstream registry denied access"}]}`))
		return
	}

	size := int64(len(b.Data))
	var off int64
	ranged := false
	if v, ok := strings.CutPrefix(req.Header.Get("Range"), "bytes="); ok {
		first, _, _ := strings.Cut(v, "-")
		if n, err := strconv.ParseInt(first, 10, 64); err == nil {
			if n >= size {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			off, ranged = n, true
		}
	}
	if fault == IgnoreRange {
		off, ranged = 0, false
	}
	start := off
	if fault == WrongStart && ranged {
		start = min(off+1, size-1)
	}
	body := b.Data[start:]
	if fault == Corrupt {
		body = append([]byte(nil), body...)
		body[len(body)/2] ^= 0xff
	}

	header := http.Header{}
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Docker-Content-Digest", b.Digest)
	header.Set("Content-Length", strconv.Itoa(len(body)))
	status := http.StatusOK
	if ranged {
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, size-1, size))
		status = http.StatusPartialContent
	}

	half := len(body) / 2
	if broken {
		half = 0
	}
	switch fault {
	case Reset:
		resetAfter(w, status, header, body[:half])
		return
	case Cut, Stall:
		for k, v := range header {
			w.Header()[k] = v
		}
		w.WriteHeader(status)
		_, _ = w.Write(body[:half])
		_ = http.NewResponseController(w).Flush()
		if fault == Cut {
			panic(http.ErrAbortHandler)
		}
		<-req.Context().Done()
		return
	}
	for k, v := range header {
		w.Header()[k] = v
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// resetAfter writes the response head and some body by hand, then closes
// the connection with SO_LINGER 0 so the client sees a TCP reset.
func resetAfter(w http.ResponseWriter, status int, header http.Header, body []byte) {
	conn, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	fmt.Fprintf(buf, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	_ = header.Write(buf)
	_, _ = buf.WriteString("\r\n")
	_, _ = buf.Write(body)
	_ = buf.Flush()
	time.Sleep(20 * time.Millisecond)
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(p)
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func digestOf(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}
