package justpull

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxRedirects = 5

// remote is one repository on a registry, addressed by its API host
// (registry-1.docker.io for Docker Hub).
type remote struct {
	host string
	repo string
}

// upstream talks to registries the way dockerd does for an anonymous pull:
// bearer tokens on challenge, redirects to presigned storage URLs.
type upstream struct {
	client      *http.Client // never follows redirects; openBlob does
	tokenClient *http.Client

	mu     sync.Mutex
	tokens map[string]string // "<host>/<repo>" -> Authorization value
}

// newUpstream speaks HTTP/1.1 only, as dockerd does to registries. Each
// layer then has its own connection, so closing a stuck body drops that
// connection and the retry dials a fresh one; over HTTP/2 the retry would
// share the stuck connection with every other layer.
func newUpstream() *upstream {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   8,
	}
	return &upstream{
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		tokenClient: &http.Client{Transport: transport, Timeout: 30 * time.Second},
		tokens:      map[string]string{},
	}
}

func (r remote) url(scheme, kind, ref string) string {
	return scheme + "://" + r.host + "/v2/" + r.repo + "/" + kind + "/" + ref
}

// registryDo sends one request to the registry, fetching a token and
// retrying once when the registry answers 401 with a bearer challenge. A 401
// without one (an upstream that refused credentials, such as a proxy's
// DENIED) comes back as is.
func (u *upstream) registryDo(ctx context.Context, method, rawURL string, header http.Header, r remote) (*http.Response, error) {
	key := r.host + "/" + r.repo
	send := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
		if err != nil {
			return nil, permanent(err)
		}
		for k, vs := range header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		u.mu.Lock()
		auth := u.tokens[key]
		u.mu.Unlock()
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		return u.client.Do(req)
	}

	resp, err := send()
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	ch, ok := parseBearerChallenge(resp.Header.Get("WWW-Authenticate"))
	if !ok {
		return resp, nil
	}
	drain(resp)
	token, err := u.token(ctx, ch, r.repo)
	if err != nil {
		return nil, err
	}
	u.mu.Lock()
	u.tokens[key] = "Bearer " + token
	u.mu.Unlock()
	return send()
}

// token runs the anonymous token exchange named by a bearer challenge.
func (u *upstream) token(ctx context.Context, ch challenge, repo string) (string, error) {
	endpoint, err := url.Parse(ch.realm)
	if err != nil {
		return "", permanent(fmt.Errorf("token realm %q: %w", ch.realm, err))
	}
	q := endpoint.Query()
	if ch.service != "" {
		q.Set("service", ch.service)
	}
	scope := ch.scope
	if scope == "" {
		scope = "repository:" + repo + ":pull"
	}
	q.Set("scope", scope)
	endpoint.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", permanent(err)
	}
	resp, err := u.tokenClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return "", permanent(fmt.Errorf("token request: %s", resp.Status))
	case resp.StatusCode != http.StatusOK:
		return "", fmt.Errorf("token request: %s", resp.Status)
	}
	var body struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return "", fmt.Errorf("token response: %w", err)
	}
	if body.Token != "" {
		return body.Token, nil
	}
	if body.AccessToken != "" {
		return body.AccessToken, nil
	}
	return "", fmt.Errorf("token response has no token")
}

// manifest fetches a manifest, retrying network errors and 5xx a few times.
// The caller relays the response as is.
func (u *upstream) manifest(ctx context.Context, scheme, method string, r remote, ref string, accept []string) (*http.Response, error) {
	header := http.Header{}
	for _, a := range accept {
		header.Add("Accept", a)
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			t := time.NewTimer(time.Duration(attempt) * time.Second)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, ctx.Err()
			case <-t.C:
			}
		}
		resp, err := u.registryDo(ctx, method, r.url(scheme, "manifests", ref), header, r)
		if err != nil {
			if isPermanent(err) || ctx.Err() != nil {
				return nil, err
			}
			lastErr = err
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("registry answered %s", resp.Status)
			drain(resp)
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// openBlob GETs a blob from offset on. Every call starts at the registry, so
// a presigned storage URL is never reused after it may have expired, and the
// registry's token is never sent to the storage host.
func (u *upstream) openBlob(ctx context.Context, scheme string, r remote, digest string, offset int64) (io.ReadCloser, int64, int64, error) {
	header := http.Header{}
	if offset > 0 {
		header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	resp, err := u.registryDo(ctx, http.MethodGet, r.url(scheme, "blobs", digest), header, r)
	if err != nil {
		return nil, 0, 0, err
	}

	fromRegistry := true
	for hops := 0; isRedirect(resp.StatusCode); hops++ {
		loc, err := resp.Location()
		drain(resp)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("redirect without a usable Location: %w", err)
		}
		if hops == maxRedirects {
			return nil, 0, 0, fmt.Errorf("more than %d redirects", maxRedirects)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, loc.String(), nil)
		if err != nil {
			return nil, 0, 0, err
		}
		if offset > 0 {
			req.Header.Set("Range", header.Get("Range"))
		}
		resp, err = u.client.Do(req)
		if err != nil {
			return nil, 0, 0, err
		}
		fromRegistry = false
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, 0, resp.ContentLength, nil
	case http.StatusPartialContent:
		start, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok {
			drain(resp)
			return nil, 0, 0, fmt.Errorf("206 with unusable Content-Range %q", resp.Header.Get("Content-Range"))
		}
		return resp.Body, start, total, nil
	case http.StatusRequestedRangeNotSatisfiable:
		drain(resp)
		return nil, 0, 0, errRangeNotSatisfiable
	}

	status := resp.Status
	msg := readErrorBody(resp)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return nil, 0, 0, fmt.Errorf("registry answered %s%s", status, msg)
	case !fromRegistry:
		// The storage host refused a link the registry just gave out: it
		// expired or was rotated. The next attempt asks the registry again.
		return nil, 0, 0, fmt.Errorf("storage host answered %s%s", status, msg)
	default:
		return nil, 0, 0, permanent(fmt.Errorf("registry answered %s%s", status, msg))
	}
}

func isRedirect(code int) bool {
	switch code {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// parseContentRange reads "bytes <start>-<end>/<total>"; total is -1 for "*".
func parseContentRange(v string) (start, total int64, ok bool) {
	rest, found := strings.CutPrefix(v, "bytes ")
	if !found {
		return 0, 0, false
	}
	span, size, found := strings.Cut(rest, "/")
	if !found {
		return 0, 0, false
	}
	first, _, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if size == "*" {
		return start, -1, true
	}
	total, err = strconv.ParseInt(size, 10, 64)
	if err != nil || total <= start {
		return 0, 0, false
	}
	return start, total, true
}

type challenge struct {
	realm, service, scope string
}

// parseBearerChallenge reads `Bearer realm="...",service="...",scope="..."`.
func parseBearerChallenge(h string) (challenge, bool) {
	scheme, rest, _ := strings.Cut(strings.TrimSpace(h), " ")
	if !strings.EqualFold(scheme, "bearer") {
		return challenge{}, false
	}
	params := parseAuthParams(rest)
	if params["realm"] == "" {
		return challenge{}, false
	}
	return challenge{realm: params["realm"], service: params["service"], scope: params["scope"]}, true
}

func parseAuthParams(s string) map[string]string {
	out := map[string]string{}
	for {
		s = strings.TrimLeft(s, " ,")
		key, rest, found := strings.Cut(s, "=")
		if !found {
			return out
		}
		key = strings.ToLower(strings.TrimSpace(key))
		rest = strings.TrimLeft(rest, " ")
		if !strings.HasPrefix(rest, `"`) {
			val, next, _ := strings.Cut(rest, ",")
			out[key] = strings.TrimSpace(val)
			s = next
			continue
		}
		var b strings.Builder
		i := 1
		for ; i < len(rest) && rest[i] != '"'; i++ {
			if rest[i] == '\\' && i+1 < len(rest) {
				i++
			}
			b.WriteByte(rest[i])
		}
		out[key] = b.String()
		if i >= len(rest) {
			return out
		}
		s = rest[i+1:]
	}
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}

// readErrorBody returns ": <first bytes of the body>" for an error message.
func readErrorBody(resp *http.Response) string {
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	return ": " + s
}
