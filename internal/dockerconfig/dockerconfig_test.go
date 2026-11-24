package dockerconfig

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cryguy/justpull"
)

func TestMain(m *testing.M) {
	_ = os.Unsetenv("DOCKER_AUTH_CONFIG")
	os.Exit(m.Run())
}

// echoHelper answers with a login whose username names the helper and the
// address it was asked for.
func echoHelper(tag string) string {
	return `[ "$1" = get ] || exit 3
read -r addr || true
printf '{"ServerURL":"%s","Username":"` + tag + `@%s","Secret":"pw-` + tag + `"}' "$addr" "$addr"
`
}

// writeConfig points DOCKER_CONFIG at a new directory holding config.
func writeConfig(t *testing.T, config string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", dir)
}

// helperDir puts a new directory first on PATH, or alone on it, and returns
// it.
func helperDir(t *testing.T, alone bool) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fake credential helpers are shell scripts")
	}
	dir := t.TempDir()
	path := dir
	if !alone {
		path += string(os.PathListSeparator) + os.Getenv("PATH")
	}
	t.Setenv("PATH", path)
	return dir
}

func writeHelper(t *testing.T, dir, name, script string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "docker-credential-"+name), []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func credential(t *testing.T, registry string) (justpull.Credential, error) {
	t.Helper()
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	return c.Credential(context.Background(), registry)
}

func mustCredential(t *testing.T, registry string) justpull.Credential {
	t.Helper()
	cred, err := credential(t, registry)
	if err != nil {
		t.Fatalf("Credential(%q): %v", registry, err)
	}
	return cred
}

func basic(user, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
}

func TestLookupOrder(t *testing.T) {
	dir := helperDir(t, false)
	writeHelper(t, dir, "one", echoHelper("one"))
	writeHelper(t, dir, "store", echoHelper("store"))
	fromFile := justpull.Credential{Username: "file", Password: "pw-file"}

	cases := []struct {
		name     string
		config   string
		registry string
		want     justpull.Credential
	}{
		{
			name:     "credHelpers beats credsStore and auths",
			config:   `{"credHelpers":{"ghcr.io":"one"},"credsStore":"store","auths":{"ghcr.io":{"auth":"` + basic("file", "pw-file") + `"}}}`,
			registry: "ghcr.io",
			want:     justpull.Credential{Username: "one@ghcr.io", Password: "pw-one"},
		},
		{
			name:     "credsStore beats auths",
			config:   `{"credHelpers":{"ghcr.io":"one"},"credsStore":"store","auths":{"quay.io":{"auth":"` + basic("file", "pw-file") + `"}}}`,
			registry: "quay.io",
			want:     justpull.Credential{Username: "store@quay.io", Password: "pw-store"},
		},
		{
			name:     "auths when no helper is named",
			config:   `{"credHelpers":{"ghcr.io":"one"},"auths":{"quay.io":{"auth":"` + basic("file", "pw-file") + `"}}}`,
			registry: "quay.io",
			want:     fromFile,
		},
		{
			name:     "an empty credHelpers entry means the file",
			config:   `{"credHelpers":{"quay.io":""},"credsStore":"store","auths":{"quay.io":{"auth":"` + basic("file", "pw-file") + `"}}}`,
			registry: "quay.io",
			want:     fromFile,
		},
		{
			name:     "localhost with a port",
			config:   `{"credHelpers":{"localhost:5000":"one"}}`,
			registry: "localhost:5000",
			want:     justpull.Credential{Username: "one@localhost:5000", Password: "pw-one"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeConfig(t, tc.config)
			if got := mustCredential(t, tc.registry); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDockerHubKey(t *testing.T) {
	writeConfig(t, `{"auths":{"https://index.docker.io/v1/":{"auth":"`+basic("hubuser", "hubpw")+`"}}}`)
	want := justpull.Credential{Username: "hubuser", Password: "hubpw"}
	for _, registry := range []string{"docker.io", "index.docker.io"} {
		if got := mustCredential(t, registry); got != want {
			t.Errorf("%s: got %+v, want %+v", registry, got, want)
		}
	}
	// The docker CLI keys registry-1.docker.io by its own name.
	if got := mustCredential(t, "registry-1.docker.io"); got != (justpull.Credential{}) {
		t.Errorf("registry-1.docker.io: got %+v, want no login", got)
	}

	dir := helperDir(t, false)
	writeHelper(t, dir, "store", echoHelper("store"))
	writeConfig(t, `{"credsStore":"store"}`)
	if got := mustCredential(t, "docker.io"); got.Username != "store@https://index.docker.io/v1/" {
		t.Errorf("the helper was asked for %q, want the Hub key", strings.TrimPrefix(got.Username, "store@"))
	}

	writeConfig(t, `{"credHelpers":{"https://index.docker.io/v1/":"one"}}`)
	writeHelper(t, dir, "one", echoHelper("one"))
	if got := mustCredential(t, "docker.io"); got.Username != "one@https://index.docker.io/v1/" {
		t.Errorf("credHelpers under the Hub key: got %+v", got)
	}
}

func TestAuthsEntries(t *testing.T) {
	cases := []struct {
		name  string
		entry string
		want  justpull.Credential
	}{
		{"auth", `{"auth":"` + basic("user", "pa:ss") + `"}`, justpull.Credential{Username: "user", Password: "pa:ss"}},
		{"auth beats legacy fields", `{"auth":"` + basic("user", "pw") + `","username":"old","password":"oldpw"}`, justpull.Credential{Username: "user", Password: "pw"}},
		{"legacy username and password", `{"username":"user","password":"pw"}`, justpull.Credential{Username: "user", Password: "pw"}},
		{"identitytoken", `{"auth":"` + basic("user", "") + `","identitytoken":"refresh"}`, justpull.Credential{Username: "user", IdentityToken: "refresh"}},
		{"empty entry", `{}`, justpull.Credential{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeConfig(t, `{"auths":{"ghcr.io":`+tc.entry+`}}`)
			if got := mustCredential(t, "ghcr.io"); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAuthsKeysWithSchemeAndPath(t *testing.T) {
	cases := []struct {
		key, registry string
	}{
		{"https://ghcr.io", "ghcr.io"},
		{"https://ghcr.io/v2/", "ghcr.io"},
		{"http://localhost:5000/v2/", "localhost:5000"},
		{"registry.example.com/v1/", "registry.example.com"},
		{"https://[::1]:5000/", "[::1]:5000"},
		{"http://::1:5000/v2/", "[::1]:5000"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			writeConfig(t, `{"auths":{"`+tc.key+`":{"auth":"`+basic("user", "pw")+`"}}}`)
			want := justpull.Credential{Username: "user", Password: "pw"}
			if got := mustCredential(t, tc.registry); got != want {
				t.Fatalf("%s: got %+v, want %+v", tc.registry, got, want)
			}
		})
	}
	writeConfig(t, `{"auths":{"https://ghcr.io":{"auth":"`+basic("user", "pw")+`"}}}`)
	if got := mustCredential(t, "quay.io"); got != (justpull.Credential{}) {
		t.Fatalf("another registry got %+v", got)
	}
}

func TestBadAuthIsAnErrorWithoutTheValue(t *testing.T) {
	for _, auth := range []string{"not*base64secret", base64.StdEncoding.EncodeToString([]byte("nocolonsecret"))} {
		writeConfig(t, `{"auths":{"ghcr.io":{"auth":"`+auth+`"}}}`)
		_, err := credential(t, "ghcr.io")
		if err == nil {
			t.Fatalf("auth %q: no error", auth)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), auth) {
			t.Fatalf("the error quotes the auth: %v", err)
		}
	}
}

func TestHelperNotFoundIsNoLogin(t *testing.T) {
	dir := helperDir(t, false)
	writeHelper(t, dir, "store", `echo "credentials not found in native keychain"; exit 1`)
	writeConfig(t, `{"credsStore":"store","auths":{"ghcr.io":{"auth":"`+basic("file", "pw")+`"}}}`)
	if got := mustCredential(t, "ghcr.io"); got != (justpull.Credential{}) {
		t.Fatalf("got %+v, want no login", got)
	}
}

func TestHelperTokenIsIdentityToken(t *testing.T) {
	dir := helperDir(t, false)
	writeHelper(t, dir, "store", `printf '{"ServerURL":"ghcr.io","Username":"<token>","Secret":"refresh"}'`)
	writeConfig(t, `{"credsStore":"store"}`)
	want := justpull.Credential{IdentityToken: "refresh"}
	if got := mustCredential(t, "ghcr.io"); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestHelperFailureNeverQuotesTheSecret(t *testing.T) {
	dir := helperDir(t, false)
	writeHelper(t, dir, "fails", `printf '{"Username":"u","Secret":"hunter2"}'; echo hunter2 >&2; exit 1`)
	writeHelper(t, dir, "garbled", `printf 'hunter2'`)
	for _, helper := range []string{"fails", "garbled"} {
		writeConfig(t, `{"credsStore":"`+helper+`"}`)
		_, err := credential(t, "ghcr.io")
		if err == nil {
			t.Fatalf("%s: no error", helper)
		}
		msg := err.Error()
		if strings.Contains(msg, "hunter2") || strings.Contains(msg, "'h'") {
			t.Fatalf("%s: the error quotes the helper's output: %v", helper, msg)
		}
		if !strings.Contains(msg, "docker-credential-"+helper) || !strings.Contains(msg, "ghcr.io") {
			t.Fatalf("%s: the error does not name the helper and the registry: %v", helper, msg)
		}
	}
}

func TestMissingHelperIsDockersError(t *testing.T) {
	helperDir(t, true)
	writeConfig(t, `{"credHelpers":{"ghcr.io":"absent"}}`)
	_, lookErr := exec.LookPath("docker-credential-absent")
	if lookErr == nil {
		t.Fatal("docker-credential-absent is on PATH")
	}
	_, err := credential(t, "ghcr.io")
	// The words docker pull prints for a helper it cannot find.
	want := "error getting credentials - err: " + lookErr.Error() + ", out: ``"
	if err == nil || err.Error() != want {
		t.Fatalf("got %v\nwant %s", err, want)
	}
}

func TestCancelStopsAHangingHelper(t *testing.T) {
	dir := helperDir(t, false)
	// sleep runs as the shell's child, so it keeps stdout open after the
	// shell is killed.
	writeHelper(t, dir, "hangs", `sleep 30`)
	writeConfig(t, `{"credsStore":"hangs"}`)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	_, err = c.Credential(ctx, "ghcr.io")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Credential returned %s after the cancel", took)
	}
}

func TestNoConfigIsNoLogin(t *testing.T) {
	helperDir(t, true)
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	if got := mustCredential(t, "ghcr.io"); got != (justpull.Credential{}) {
		t.Fatalf("missing file: got %+v", got)
	}
	writeConfig(t, ``)
	if got := mustCredential(t, "ghcr.io"); got != (justpull.Credential{}) {
		t.Fatalf("empty file: got %+v", got)
	}
}

func TestMalformedConfigIsAnError(t *testing.T) {
	writeConfig(t, `{"auths":`)
	if _, err := Load(); err == nil {
		t.Fatal("no error")
	}
}

func TestPlatformStoreWhenTheConfigNamesNoLogin(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the fake helper stands in for Linux's secretservice")
	}
	dir := helperDir(t, true)
	writeHelper(t, dir, "secretservice", echoHelper("secretservice"))
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	if got := mustCredential(t, "ghcr.io"); got.Username != "secretservice@ghcr.io" {
		t.Fatalf("missing file: got %+v, want the platform store's login", got)
	}
	writeConfig(t, `{"auths":{"quay.io":{}}}`)
	if got := mustCredential(t, "ghcr.io"); got != (justpull.Credential{}) {
		t.Fatalf("a config with auths: got %+v, want no login", got)
	}
}

func envConfig(auths map[string]string) string {
	var b strings.Builder
	b.WriteString(`{"auths":{`)
	first := true
	for addr, auth := range auths {
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(`"` + addr + `":{"auth":"` + auth + `"}`)
	}
	b.WriteString(`}}`)
	return b.String()
}

func TestAuthConfigEnvWins(t *testing.T) {
	dir := helperDir(t, false)
	writeHelper(t, dir, "one", echoHelper("one"))
	writeHelper(t, dir, "store", echoHelper("store"))
	writeConfig(t, `{"credHelpers":{"ghcr.io":"one"},"auths":{"quay.io":{"auth":"`+basic("file", "pw-file")+`"}}}`)
	t.Setenv("DOCKER_AUTH_CONFIG", envConfig(map[string]string{
		"ghcr.io":                     basic("env-ghcr", "pw:1"),
		"quay.io":                     basic("env-quay", "pw2"),
		"https://index.docker.io/v1/": basic("env-hub", "pw3"),
	}))
	cases := map[string]justpull.Credential{
		"ghcr.io":   {Username: "env-ghcr", Password: "pw:1"},
		"quay.io":   {Username: "env-quay", Password: "pw2"},
		"docker.io": {Username: "env-hub", Password: "pw3"},
	}
	for registry, want := range cases {
		if got := mustCredential(t, registry); got != want {
			t.Errorf("%s: got %+v, want %+v", registry, got, want)
		}
	}

	writeConfig(t, `{"credsStore":"store"}`)
	if got := mustCredential(t, "ghcr.io"); got.Username != "env-ghcr" {
		t.Errorf("credsStore: got %+v, want the env's login", got)
	}
}

func TestAuthConfigEnvFallsBack(t *testing.T) {
	dir := helperDir(t, false)
	writeHelper(t, dir, "one", echoHelper("one"))
	writeConfig(t, `{"credHelpers":{"ghcr.io":"one"},"auths":{"quay.io":{"auth":"`+basic("file", "pw-file")+`"}}}`)
	// The env's keys match exactly: https://ghcr.io is not ghcr.io there.
	t.Setenv("DOCKER_AUTH_CONFIG", envConfig(map[string]string{
		"https://ghcr.io":  basic("env-ghcr", "pw"),
		"registry.example": basic("env-example", "pw"),
	}))
	if got := mustCredential(t, "ghcr.io"); got.Username != "one@ghcr.io" {
		t.Errorf("ghcr.io: got %+v, want the helper's login", got)
	}
	if got, want := mustCredential(t, "quay.io"), (justpull.Credential{Username: "file", Password: "pw-file"}); got != want {
		t.Errorf("quay.io: got %+v, want %+v", got, want)
	}

	for _, env := range []string{`{"auths":{}}`, `{}`, "  "} {
		t.Setenv("DOCKER_AUTH_CONFIG", env)
		if got := mustCredential(t, "ghcr.io"); got.Username != "one@ghcr.io" {
			t.Errorf("env %q: got %+v, want the helper's login", env, got)
		}
	}
}

func TestAuthConfigEnvWarnsOnlyWhenMalformed(t *testing.T) {
	writeConfig(t, `{}`)
	for _, env := range []string{"", `{"auths":{"ghcr.io":{"auth":"` + basic("u", "p") + `"}}}`, "  "} {
		t.Setenv("DOCKER_AUTH_CONFIG", env)
		c, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if w := c.Warnings(); len(w) != 0 {
			t.Errorf("env %q: warnings %q", env, w)
		}
	}
}

func TestMalformedAuthConfigEnvIsIgnoredWithAWarning(t *testing.T) {
	writeConfig(t, `{"auths":{"ghcr.io":{"auth":"`+basic("file", "pw-file")+`"}}}`)
	fromFile := justpull.Credential{Username: "file", Password: "pw-file"}
	secret := basic("user", "hunter2")
	cases := map[string]string{
		"not JSON":           `{"auths":{"ghcr.io":{"auth":"` + secret + `"`,
		"syntax error":       `{"auths":{"ghcr.io":{"auth":"` + secret + `" hunter2}}}`,
		"wrong type":         `{"auths":{"ghcr.io":{"auth":12345}}}`,
		"unknown field":      `{"auths":{"ghcr.io":{"auth":"` + secret + `","password":"hunter2"}}}`,
		"two objects":        `{"auths":{"ghcr.io":{"auth":"` + secret + `"}}} {"hunter2":1}`,
		"no auth":            `{"auths":{"ghcr.io":{}}}`,
		"auth not base64":    `{"auths":{"ghcr.io":{"auth":"hunter2!!"}}}`,
		"auth without colon": `{"auths":{"ghcr.io":{"auth":"` + base64.StdEncoding.EncodeToString([]byte("hunter2")) + `"}}}`,
		"another registry":   `{"auths":{"ghcr.io":{"auth":"` + secret + `"},"quay.io":{"auth":"hunter2!!"}}}`,
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("DOCKER_AUTH_CONFIG", env)
			c, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Credential(context.Background(), "ghcr.io")
			if err != nil || got != fromFile {
				t.Fatalf("got %+v, %v; want the config file's login", got, err)
			}
			warnings := c.Warnings()
			if len(warnings) != 1 {
				t.Fatalf("warnings %q, want one", warnings)
			}
			msg := warnings[0]
			if !strings.HasPrefix(msg, "Failed to create credential store from DOCKER_AUTH_CONFIG: ") {
				t.Fatalf("the warning is not docker's: %q", msg)
			}
			for _, leak := range []string{"hunter2", secret, "12345", "aHVudGVyMg", "'h'", "'u'"} {
				if strings.Contains(msg, leak) {
					t.Fatalf("the warning quotes %q: %q", leak, msg)
				}
			}
			t.Logf("%s", msg)
		})
	}
}

func TestDockerConfigDefaultsToHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DOCKER_CONFIG", "")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if err := os.Mkdir(filepath.Join(home, ".docker"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := `{"auths":{"ghcr.io":{"auth":"` + basic("home", "pw") + `"}}}`
	if err := os.WriteFile(filepath.Join(home, ".docker", "config.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	want := justpull.Credential{Username: "home", Password: "pw"}
	if got := mustCredential(t, "ghcr.io"); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
