package justpull

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// Credential is a registry login, as `docker login` stores it.
type Credential struct {
	Username string
	Password string
	// IdentityToken is an OAuth2 refresh token. When set it is used
	// instead of Username and Password.
	IdentityToken string
}

// ErrRefused is wrapped by the error of a registry that refused the login,
// or refused access to the image: a login, or another one, may help.
var ErrRefused = errors.New("access refused")

// refusedError marks a registry's refusal without changing its message.
type refusedError struct{ err error }

func (e refusedError) Error() string        { return e.err.Error() }
func (e refusedError) Unwrap() error        { return e.err }
func (e refusedError) Is(target error) bool { return target == ErrRefused }

func refused(err error) error { return refusedError{err} }

// credential asks Options.Credentials for the login of r's registry. No
// callback means anonymous.
func (u *upstream) credential(ctx context.Context, r remote) (Credential, error) {
	if u.credentials == nil {
		return Credential{}, nil
	}
	cred, err := u.credentials(ctx, r.registry)
	if err != nil {
		return Credential{}, permanent(fmt.Errorf("credentials for %s: %w", r.registry, err))
	}
	return cred, nil
}

// checkLoginURL refuses to send a login to dest over plain HTTP, where
// anyone on the path could read it. Loopback is exempt: dockerd talks
// plain HTTP to registries there.
func checkLoginURL(registry string, dest *url.URL) error {
	if dest.Scheme != "http" || isLoopback(dest.Hostname()) {
		return nil
	}
	return permanent(fmt.Errorf("registry %s: refusing to send the login over plain HTTP to %s", registry, dest.Host))
}

// checkTokenRedirect keeps a token request's login off plain HTTP when the
// token endpoint redirects: Go resends a POST body on 307 and 308, and an
// Authorization header to the same host whatever the scheme.
func checkTokenRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if req.Method != http.MethodPost && req.Header.Get("Authorization") == "" {
		return nil
	}
	if req.URL.Scheme != "http" || isLoopback(req.URL.Hostname()) {
		return nil
	}
	return permanent(fmt.Errorf("refusing to follow a redirect that sends the login over plain HTTP to %s", req.URL.Host))
}

func basicAuth(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}
