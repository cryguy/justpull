// Package dockerconfig finds the registry login the docker CLI would use, from
// DOCKER_AUTH_CONFIG, its config file and its credential helpers.
package dockerconfig

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cryguy/justpull"
)

// hubKey is where the docker CLI keeps the Docker Hub login.
const hubKey = "https://index.docker.io/v1/"

const (
	helperPrefix = "docker-credential-"
	// tokenUsername is the username a helper returns when its secret is an
	// identity token.
	tokenUsername = "<token>"
	// notFoundMessage is what docker-credential-helpers print on stdout when
	// they hold no login for the address.
	notFoundMessage = "credentials not found in native keychain"
)

// envKey names the variable whose logins the docker CLI puts in front of the
// config file's: {"auths":{"<address>":{"auth":"<base64 user:password>"}}}.
const envKey = "DOCKER_AUTH_CONFIG"

// Config is the login part of the docker CLI's config.json, and of
// DOCKER_AUTH_CONFIG.
type Config struct {
	auths       map[string]authEntry
	credsStore  string
	credHelpers map[string]string
	env         map[string]justpull.Credential
	warnings    []string
}

type envFile struct {
	Auths map[string]envEntry `json:"auths"`
}

type envEntry struct {
	Auth string `json:"auth"`
}

type configFile struct {
	Auths       map[string]authEntry `json:"auths"`
	CredsStore  string               `json:"credsStore"`
	CredHelpers map[string]string    `json:"credHelpers"`
}

type authEntry struct {
	Auth          string `json:"auth"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	IdentityToken string `json:"identitytoken"`
}

type helperOutput struct {
	Username string
	Secret   string
}

// Load reads the docker CLI's config: $DOCKER_CONFIG/config.json, else
// ~/.docker/config.json. A missing file is an empty config. A malformed
// DOCKER_AUTH_CONFIG is ignored, as the docker CLI ignores it, and reported
// by Warnings.
func Load() (*Config, error) {
	dir := os.Getenv("DOCKER_CONFIG")
	if dir == "" {
		home, err := homeDir()
		if err != nil {
			return nil, fmt.Errorf("docker config: %w", err)
		}
		dir = filepath.Join(home, ".docker")
	}
	c, err := load(filepath.Join(dir, "config.json"))
	if err != nil {
		return nil, err
	}
	if v := os.Getenv(envKey); v != "" {
		env, err := parseEnv(v)
		if err != nil {
			c.warnings = append(c.warnings, "Failed to create credential store from "+envKey+": "+err.Error())
		}
		c.env = env
	}
	return c, nil
}

// Warnings returns the lines the docker CLI prints on stderr for this
// config. They never quote a login.
func (c *Config) Warnings() []string {
	return slices.Clone(c.warnings)
}

// parseEnv reads DOCKER_AUTH_CONFIG. Its errors never quote the value.
func parseEnv(v string) (map[string]justpull.Credential, error) {
	var f envFile
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.New(`the value is not JSON of the form {"auths":{"<registry>":{"auth":"<base64 user:password>"}}}`)
	}
	if dec.More() {
		return nil, errors.New(envKey + " does not support more than one JSON object")
	}
	creds := make(map[string]justpull.Credential, len(f.Auths))
	for _, addr := range slices.Sorted(maps.Keys(f.Auths)) {
		auth := f.Auths[addr].Auth
		if auth == "" {
			return nil, fmt.Errorf("%s environment variable is missing key `auth` for %s", envKey, addr)
		}
		username, password, err := decodeAuth(auth)
		if err != nil {
			return nil, fmt.Errorf("the auth for %s %w", addr, err)
		}
		creds[addr] = justpull.Credential{Username: username, Password: password}
	}
	return creds, nil
}

func homeDir() (string, error) {
	home, err := os.UserHomeDir()
	if err == nil || runtime.GOOS == "windows" {
		return home, err
	}
	u, uerr := user.Current()
	if uerr != nil {
		return "", err
	}
	return u.HomeDir, nil
}

func load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return withDefaultStore(&Config{}), nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading docker config: %w", err)
	}
	var f configFile
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parsing docker config %s: %w", path, err)
	}
	return withDefaultStore(&Config{auths: f.Auths, credsStore: f.CredsStore, credHelpers: f.CredHelpers}), nil
}

// withDefaultStore picks the platform's credential helper, if it is
// installed, for a config that names no login at all, as the docker CLI does.
func withDefaultStore(c *Config) *Config {
	if c.credsStore != "" || len(c.credHelpers) > 0 || len(c.auths) > 0 {
		return c
	}
	name := platformStore()
	if name == "" {
		return c
	}
	if _, err := exec.LookPath(helperPrefix + name); err != nil {
		return c
	}
	c.credsStore = name
	return c
}

func platformStore() string {
	switch runtime.GOOS {
	case "darwin":
		return "osxkeychain"
	case "windows":
		return "wincred"
	case "linux":
		if _, err := exec.LookPath("pass"); err == nil {
			return "pass"
		}
		return "secretservice"
	}
	return ""
}

// Credential returns the login docker would use for registry, named as image
// references name it (docker.io, ghcr.io, localhost:5000). No login is a zero
// Credential and a nil error.
func (c *Config) Credential(ctx context.Context, registry string) (justpull.Credential, error) {
	addr := serverAddress(registry)
	if cred, ok := c.env[addr]; ok {
		return cred, nil
	}
	helper, ok := c.credHelpers[addr]
	if !ok {
		helper = c.credsStore
	}
	if helper != "" {
		return runHelper(ctx, helperPrefix+helper, addr, registry)
	}
	return c.fromAuths(addr, registry)
}

// serverAddress is the key the docker CLI files registry's login under.
// registry-1.docker.io is not Docker Hub here: the CLI keys it by its own
// name.
func serverAddress(registry string) string {
	if registry == "docker.io" || registry == "index.docker.io" {
		return hubKey
	}
	return registry
}

func (c *Config) fromAuths(addr, registry string) (justpull.Credential, error) {
	e, ok := c.auths[addr]
	if !ok {
		for _, key := range slices.Sorted(maps.Keys(c.auths)) {
			if hostname(key) == addr {
				e, ok = c.auths[key], true
				break
			}
		}
	}
	if !ok {
		return justpull.Credential{}, nil
	}
	cred := justpull.Credential{Username: e.Username, Password: e.Password, IdentityToken: e.IdentityToken}
	if e.Auth == "" {
		return cred, nil
	}
	username, password, err := decodeAuth(e.Auth)
	if err != nil {
		return justpull.Credential{}, fmt.Errorf("docker config: the auth of the %s login %w", registry, err)
	}
	cred.Username = username
	cred.Password = password
	return cred, nil
}

// decodeAuth splits an auth field, base64 of user:password. Its errors never
// quote the field.
func decodeAuth(auth string) (username, password string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return "", "", errors.New("is not base64")
	}
	username, password, found := strings.Cut(string(decoded), ":")
	if !found || username == "" {
		return "", "", errors.New("is not user:password")
	}
	return username, strings.Trim(password, "\x00"), nil
}

// hostname turns an auths key written as a URL, such as https://ghcr.io or
// http://localhost:5000/v2/, into the host[:port] it names.
func hostname(key string) string {
	if strings.Contains(key, "://") {
		if u, err := url.Parse(key); err == nil && u.Hostname() != "" {
			if u.Port() == "" {
				return u.Hostname()
			}
			return net.JoinHostPort(u.Hostname(), u.Port())
		}
		if host := hostFromURL(key); host != "" {
			return host
		}
	}
	host, _, _ := strings.Cut(key, "/")
	return host
}

// hostFromURL takes the host from a URL that net/url rejects, such as one
// with an IPv6 address written without brackets.
func hostFromURL(key string) string {
	_, rest, ok := strings.Cut(key, "://")
	if !ok {
		return ""
	}
	host, _, _ := strings.Cut(rest, "/")
	if host == "" {
		return ""
	}
	if strings.Count(host, ":") > 1 && !strings.HasPrefix(host, "[") {
		i := strings.LastIndex(host, ":")
		addr, port := host[:i], host[i+1:]
		if addr != "" && isPort(port) {
			return net.JoinHostPort(addr, port)
		}
	}
	return host
}

func isPort(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// runHelper asks a docker credential helper for addr's login. Its errors
// never quote the helper's output, which may hold the secret.
func runHelper(ctx context.Context, name, addr, registry string) (justpull.Credential, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return justpull.Credential{}, fmt.Errorf("credential helper %s for %s: %w", name, registry, err)
	}
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, path, "get")
	cmd.Stdin = strings.NewReader(addr)
	cmd.Stdout = &stdout
	// A child of the helper can keep stdout open after the helper is killed.
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return justpull.Credential{}, fmt.Errorf("credential helper %s for %s: %w", name, registry, context.Cause(ctx))
		}
		if strings.TrimSpace(stdout.String()) == notFoundMessage {
			return justpull.Credential{}, nil
		}
		return justpull.Credential{}, fmt.Errorf("credential helper %s for %s: %w", name, registry, err)
	}
	var out helperOutput
	if err := json.NewDecoder(&stdout).Decode(&out); err != nil {
		return justpull.Credential{}, fmt.Errorf("credential helper %s for %s: its answer is not JSON", name, registry)
	}
	if out.Username == tokenUsername {
		return justpull.Credential{IdentityToken: out.Secret}, nil
	}
	return justpull.Credential{Username: out.Username, Password: out.Secret}, nil
}
