package justpull

import (
	"context"
	"encoding/base64"
	"fmt"
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

func basicAuth(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}
