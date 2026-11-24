package justpull

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// testRegistry is a minimal registry with bearer auth and a separate
// storage host that blob GETs redirect to.
type testRegistry struct {
	t        *testing.T
	reg      *httptest.Server
	storage  *httptest.Server
	tokenTTL time.Duration

	mu            sync.Mutex
	tokens        map[string]time.Time
	tokenCalls    int
	registryGets  int
	storageAuth   []string
	storageRanges []string
	blobStatus    int // registry answer for blobs when non-zero
	storageStatus int // storage answer when non-zero
	manifest5xx   int // manifest requests to fail with 503 first
	accepts       [][]string
}

func newTestRegistry(t *testing.T) *testRegistry {
	r := &testRegistry{t: t, tokenTTL: time.Hour, tokens: map[string]time.Time{}}
	r.storage = httptest.NewServer(http.HandlerFunc(r.serveStorage))
	r.reg = httptest.NewServer(http.HandlerFunc(r.serveRegistry))
	t.Cleanup(func() { r.reg.Close(); r.storage.Close() })
	return r
}

func (r *testRegistry) remote() remote {
	host := strings.TrimPrefix(r.reg.URL, "http://")
	return remote{host: host, repo: "team/app", registry: host}
}

func (r *testRegistry) serveRegistry(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/token" {
		r.mu.Lock()
		r.tokenCalls++
		tok := fmt.Sprintf("tok%d", r.tokenCalls)
		r.tokens[tok] = time.Now().Add(r.tokenTTL)
		r.mu.Unlock()
		if req.URL.Query().Get("scope") != "repository:team/app:pull" {
			http.Error(w, "bad scope", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, `{"token":%q}`, tok)
		return
	}
	r.mu.Lock()
	exp, ok := r.tokens[strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")]
	r.mu.Unlock()
	if !ok || time.Now().After(exp) {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="test",scope="repository:team/app:pull"`, r.reg.URL))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch {
	case strings.Contains(req.URL.Path, "/manifests/"):
		r.mu.Lock()
		r.accepts = append(r.accepts, req.Header.Values("Accept"))
		fail := r.manifest5xx > 0
		if fail {
			r.manifest5xx--
		}
		r.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = io.WriteString(w, `{}`)
	case strings.Contains(req.URL.Path, "/blobs/"):
		r.mu.Lock()
		r.registryGets++
		status := r.blobStatus
		r.mu.Unlock()
		if status != 0 {
			http.Error(w, `{"errors":[{"code":"X"}]}`, status)
			return
		}
		http.Redirect(w, req, r.storage.URL+"/data?sig=abc", http.StatusTemporaryRedirect)
	default:
		http.NotFound(w, req)
	}
}

const storageBody = "0123456789abcdefghij"

func (r *testRegistry) serveStorage(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.storageAuth = append(r.storageAuth, req.Header.Get("Authorization"))
	r.storageRanges = append(r.storageRanges, req.Header.Get("Range"))
	status := r.storageStatus
	r.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	http.ServeContent(w, req, "data", time.Time{}, strings.NewReader(storageBody))
}

func testUpstream() *upstream { return newUpstream(nil) }

type registryCounts struct {
	storageAuth, storageRanges []string
	registryGets, tokenCalls   int
	accepts                    [][]string
}

func (r *testRegistry) counts() registryCounts {
	r.mu.Lock()
	defer r.mu.Unlock()
	return registryCounts{
		storageAuth:   append([]string(nil), r.storageAuth...),
		storageRanges: append([]string(nil), r.storageRanges...),
		registryGets:  r.registryGets,
		tokenCalls:    r.tokenCalls,
		accepts:       append([][]string(nil), r.accepts...),
	}
}

func TestOpenBlobAuthenticatesFollowsRedirectAndResumes(t *testing.T) {
	r := newTestRegistry(t)
	u := testUpstream()

	body, start, total, err := u.openBlob(context.Background(), "http", r.remote(), "sha256:x", 5)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(body)
	_ = body.Close()
	if start != 5 || total != int64(len(storageBody)) || string(got) != storageBody[5:] {
		t.Fatalf("start %d total %d body %q", start, total, got)
	}
	c := r.counts()
	if c.storageAuth[0] != "" {
		t.Fatalf("storage host received Authorization %q", c.storageAuth[0])
	}
	if c.storageRanges[0] != "bytes=5-" {
		t.Fatalf("storage Range = %q", c.storageRanges[0])
	}

	// A second open goes back to the registry and reuses the token.
	body, _, _, err = u.openBlob(context.Background(), "http", r.remote(), "sha256:x", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	c = r.counts()
	if c.registryGets != 2 || c.tokenCalls != 1 {
		t.Fatalf("registry blob GETs %d, token calls %d; want 2 and 1", c.registryGets, c.tokenCalls)
	}
	if c.storageRanges[1] != "" {
		t.Fatalf("offset 0 sent Range %q", c.storageRanges[1])
	}
}

func TestExpiredTokenIsRenewed(t *testing.T) {
	r := newTestRegistry(t)
	r.tokenTTL = 50 * time.Millisecond
	u := testUpstream()

	for i := 0; i < 2; i++ {
		body, _, _, err := u.openBlob(context.Background(), "http", r.remote(), "sha256:x", 0)
		if err != nil {
			t.Fatal(err)
		}
		_ = body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	if c := r.counts(); c.tokenCalls != 2 {
		t.Fatalf("token calls = %d, want 2", c.tokenCalls)
	}
}

func TestOpenBlobClassifiesFailures(t *testing.T) {
	cases := []struct {
		name          string
		blobStatus    int
		storageStatus int
		permanent     bool
		rangeRefused  bool
	}{
		{name: "registry 404", blobStatus: 404, permanent: true},
		{name: "registry 403", blobStatus: 403, permanent: true},
		{name: "registry 503", blobStatus: 503},
		{name: "registry 429", blobStatus: 429},
		{name: "storage 403 (expired link)", storageStatus: 403},
		{name: "storage 404", storageStatus: 404},
		{name: "storage 410", storageStatus: 410},
		{name: "storage 500", storageStatus: 500},
		{name: "range refused", storageStatus: 416, rangeRefused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRegistry(t)
			r.blobStatus, r.storageStatus = tc.blobStatus, tc.storageStatus
			_, _, _, err := testUpstream().openBlob(context.Background(), "http", r.remote(), "sha256:x", 3)
			if err == nil {
				t.Fatal("want an error")
			}
			if got := isPermanent(err); got != tc.permanent {
				t.Fatalf("permanent = %v, want %v (%v)", got, tc.permanent, err)
			}
			if got := errors.Is(err, errRangeNotSatisfiable); got != tc.rangeRefused {
				t.Fatalf("range refused = %v, want %v (%v)", got, tc.rangeRefused, err)
			}
		})
	}
}

func TestUnauthorizedWithoutChallengeIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"errors":[{"code":"DENIED","message":"upstream registry denied access"}]}`)
	}))
	defer srv.Close()
	rm := remote{host: strings.TrimPrefix(srv.URL, "http://"), repo: "team/app"}

	_, _, _, err := testUpstream().openBlob(context.Background(), "http", rm, "sha256:x", 0)
	if !isPermanent(err) || !strings.Contains(err.Error(), "DENIED") {
		t.Fatalf("err = %v, want a permanent DENIED", err)
	}
}

func TestManifestRetries5xxAndForwardsAccept(t *testing.T) {
	r := newTestRegistry(t)
	r.manifest5xx = 2
	accept := []string{"application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json"}

	resp, err := testUpstream().manifest(context.Background(), "http", http.MethodGet, r.remote(), "v1", accept)
	if err != nil {
		t.Fatal(err)
	}
	drain(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	accepts := r.counts().accepts
	last := accepts[len(accepts)-1]
	if strings.Join(last, ",") != strings.Join(accept, ",") {
		t.Fatalf("Accept = %v", last)
	}
}

func TestParseChallenge(t *testing.T) {
	scheme, p := parseChallenge(`Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:a/b:pull,push"`)
	if scheme != "bearer" || p["realm"] != "https://auth.docker.io/token" || p["service"] != "registry.docker.io" || p["scope"] != "repository:a/b:pull,push" {
		t.Fatalf("%q %v", scheme, p)
	}
	if scheme, p := parseChallenge(`Basic realm="fakereg"`); scheme != "basic" || p["realm"] != "fakereg" {
		t.Fatalf("basic = %q %v", scheme, p)
	}
	if scheme, p := parseChallenge(""); scheme != "" || len(p) != 0 {
		t.Fatalf("empty = %q %v", scheme, p)
	}
	if _, p := parseChallenge(`Bearer service="x"`); p["realm"] != "" {
		t.Fatalf("realm %q read from a challenge without one", p["realm"])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func stubResponse(req *http.Request, status int, header http.Header, body string) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req,
	}
}

func TestLoginIsNeverSentOverPlainHTTP(t *testing.T) {
	bearer := `Bearer realm="http://example.invalid/token",service="test"`
	password := Credential{Username: "alice", Password: "pw"}
	cases := []struct {
		name      string
		challenge string
		cred      Credential
		refused   bool
	}{
		{"bearer with password", bearer, password, true},
		{"bearer with identity token", bearer, Credential{IdentityToken: "rt"}, true},
		{"basic", `Basic realm="x"`, password, true},
		{"anonymous bearer", bearer, Credential{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var sent []*http.Request
			u := newUpstream(func(context.Context, string) (Credential, error) { return tc.cred, nil })
			stub := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				sent = append(sent, req)
				mu.Unlock()
				if req.URL.Path == "/token" {
					return stubResponse(req, http.StatusOK, nil, `{"token":"anon"}`), nil
				}
				if req.Header.Get("Authorization") == "Bearer anon" {
					return stubResponse(req, http.StatusOK, nil, "{}"), nil
				}
				return stubResponse(req, http.StatusUnauthorized, http.Header{"Www-Authenticate": {tc.challenge}}, ""), nil
			})
			u.client.Transport, u.tokenClient.Transport = stub, stub
			rm := remote{host: "example.invalid", repo: "team/app", registry: "example.invalid"}

			resp, err := u.registryDo(context.Background(), http.MethodGet, rm.url("http", "manifests", "v1"), nil, rm)
			if !tc.refused {
				if err != nil {
					t.Fatal(err)
				}
				drain(resp)
				if resp.StatusCode != http.StatusOK || len(sent) != 3 || sent[1].URL.Path != "/token" {
					t.Fatalf("status %d after %d requests; want the anonymous token fetched", resp.StatusCode, len(sent))
				}
				return
			}
			if err == nil {
				drain(resp)
				t.Fatalf("status %d, want the login refused", resp.StatusCode)
			}
			if !isPermanent(err) || !strings.Contains(err.Error(), "plain HTTP") || !strings.Contains(err.Error(), "example.invalid") {
				t.Fatalf("err = %v, want a permanent refusal naming the registry", err)
			}
			for _, req := range sent {
				if req.URL.Path == "/token" || req.Header.Get("Authorization") != "" {
					t.Fatalf("%s %s was sent with the login", req.Method, req.URL.Path)
				}
			}
		})
	}
}

func TestIdentityTokenIsExchangedWithAPost(t *testing.T) {
	var mu sync.Mutex
	var form map[string]string
	var method, contentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/token" {
			mu.Lock()
			method, contentType = req.Method, req.Header.Get("Content-Type")
			form = map[string]string{}
			for _, k := range []string{"grant_type", "refresh_token", "service", "scope", "client_id"} {
				form[k] = req.PostFormValue(k)
			}
			mu.Unlock()
			if req.PostFormValue("refresh_token") != "rt-good" {
				http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(w, `{"access_token":"acc","expires_in":300}`)
			return
		}
		if req.Header.Get("Authorization") != "Bearer acc" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="http://`+req.Host+`/token",service="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	rm := remote{host: host, repo: "team/app", registry: host}
	login := func(token string) (*http.Response, error) {
		u := newUpstream(func(context.Context, string) (Credential, error) {
			return Credential{Username: "ignored", Password: "ignored", IdentityToken: token}, nil
		})
		return u.registryDo(context.Background(), http.MethodGet, rm.url("http", "manifests", "v1"), nil, rm)
	}

	resp, err := login("rt-good")
	if err != nil {
		t.Fatal(err)
	}
	drain(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	mu.Lock()
	want := map[string]string{"grant_type": "refresh_token", "refresh_token": "rt-good", "service": "test", "scope": "repository:team/app:pull", "client_id": "justpull"}
	for k, v := range want {
		if form[k] != v {
			t.Errorf("form %s = %q, want %q", k, form[k], v)
		}
	}
	if method != http.MethodPost || contentType != "application/x-www-form-urlencoded" {
		t.Errorf("token request %s %q, want a form POST", method, contentType)
	}
	mu.Unlock()

	_, err = login("rt-revoked")
	if !isPermanent(err) || !strings.Contains(err.Error(), "refused the login") {
		t.Fatalf("err = %v, want a permanent refused login", err)
	}
	if strings.Contains(err.Error(), "rt-revoked") {
		t.Fatal("the error carries the identity token")
	}
}

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in           string
		start, total int64
		ok           bool
	}{
		{"bytes 100-199/1000", 100, 1000, true},
		{"bytes 0-0/1", 0, 1, true},
		{"bytes 5-9/*", 5, -1, true},
		{"bytes */1000", 0, 0, false},
		{"items 0-1/2", 0, 0, false},
		{"bytes 10-20/5", 0, 0, false},
	}
	for _, tc := range cases {
		start, total, ok := parseContentRange(tc.in)
		if ok != tc.ok || (ok && (start != tc.start || total != tc.total)) {
			t.Errorf("%q = %d %d %v", tc.in, start, total, ok)
		}
	}
}
