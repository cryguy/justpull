package engine

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeEnv names the directory of the fake docker's script and record. When
// it is set, the test binary is the fake docker.
const fakeEnv = "ENGINE_TEST_FAKE_DOCKER"

func TestMain(m *testing.M) {
	if dir := os.Getenv(fakeEnv); dir != "" {
		os.Exit(fakeDocker(dir, os.Args[1:]))
	}
	os.Exit(m.Run())
}

// fake is what the fake docker does.
type fake struct {
	Stdout string
	Stderr string
	Exit   int
	// For dial-stdio: the answer's status and body. Status 0 answers nothing
	// and exits with Stderr, as dial-stdio does when the daemon is not
	// running. Hang keeps the answer open after Body until killed.
	Status int
	Body   string
	Hang   bool
}

// call is what the fake docker received.
type call struct {
	Args  []string
	Stdin string
	// The HTTP request on dial-stdio.
	Method  string
	URI     string
	Auth    string
	HasAuth bool
	Close   bool
}

// newFake returns a Client whose docker is the fake, and a function that
// returns the fake's last call.
func newFake(t *testing.T, f fake) (*Client, func() call) {
	t.Helper()
	dir := t.TempDir()
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fake.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(fakeEnv, dir)
	// A race-enabled fake would otherwise sleep a second before exiting.
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return &Client{docker: exe}, func() call {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, "call.json"))
		if err != nil {
			t.Fatal(err)
		}
		var c call
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		return c
	}
}

func fakeDocker(dir string, args []string) int {
	raw, err := os.ReadFile(filepath.Join(dir, "fake.json"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 99
	}
	var f fake
	if err := json.Unmarshal(raw, &f); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 99
	}
	rec := call{Args: args}
	save := func() {
		raw, _ := json.Marshal(rec)
		_ = os.WriteFile(filepath.Join(dir, "call.json"), raw, 0o644)
	}
	if reflect.DeepEqual(args, []string{"system", "dial-stdio"}) {
		return f.dialStdio(&rec, save)
	}
	in, _ := io.ReadAll(os.Stdin)
	rec.Stdin = string(in)
	save()
	_, _ = os.Stdout.WriteString(f.Stdout)
	_, _ = os.Stderr.WriteString(f.Stderr)
	return f.Exit
}

func (f fake) dialStdio(rec *call, save func()) int {
	in := bufio.NewReader(os.Stdin)
	req, err := http.ReadRequest(in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 99
	}
	rec.Method, rec.URI, rec.Close = req.Method, req.RequestURI, req.Close
	_, rec.HasAuth = req.Header["X-Registry-Auth"]
	rec.Auth = req.Header.Get("X-Registry-Auth")
	save()
	if f.Status == 0 {
		_, _ = os.Stderr.WriteString(f.Stderr)
		return f.Exit
	}
	// Docker Desktop answers 500 without a body when stdin closes before the
	// answer has been read.
	eof := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, in)
		close(eof)
	}()
	select {
	case <-eof:
		_, _ = io.WriteString(os.Stdout, "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
		return 0
	case <-time.After(100 * time.Millisecond):
	}
	_, _ = fmt.Fprintf(os.Stdout, "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", f.Status, http.StatusText(f.Status))
	chunks := httputil.NewChunkedWriter(os.Stdout)
	_, _ = io.WriteString(chunks, f.Body)
	if f.Hang {
		time.Sleep(time.Hour)
	}
	_ = chunks.Close()
	_, _ = io.WriteString(os.Stdout, "\r\n")
	return 0
}

const stream = `{"status":"Pulling from cryguy/justpull","id":"v1"}` + "\n"

func TestPullRequest(t *testing.T) {
	const image = "ghcr.io/cryguy/justpull"
	tests := []struct {
		name     string
		tag      string
		platform string
		allTags  bool
		want     url.Values
	}{
		{"tag and platform", "v1.2.3", "linux/arm64", false, url.Values{"fromImage": {image}, "tag": {"v1.2.3"}, "platform": {"linux/arm64"}}},
		{"daemon's platform", "v1.2.3", "", false, url.Values{"fromImage": {image}, "tag": {"v1.2.3"}}},
		{"no tag is latest", "", "", false, url.Values{"fromImage": {image}, "tag": {"latest"}}},
		{"digest", "sha256:" + strings.Repeat("ab", 32), "", false, url.Values{"fromImage": {image}, "tag": {"sha256:" + strings.Repeat("ab", 32)}}},
		{"all tags", "", "", true, url.Values{"fromImage": {image}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, last := newFake(t, fake{Status: http.StatusOK, Body: stream})
			rc, err := c.Pull(t.Context(), image, tt.tag, tt.platform, tt.allTags, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}
			if string(got) != stream {
				t.Errorf("stream = %q, want %q", got, stream)
			}
			rec := last()
			u, err := url.Parse(rec.URI)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Method != http.MethodPost || u.Path != "/images/create" {
				t.Errorf("request = %s %s, want POST /images/create", rec.Method, u.Path)
			}
			if !reflect.DeepEqual(u.Query(), tt.want) {
				t.Errorf("query = %v, want %v", u.Query(), tt.want)
			}
			if rec.HasAuth {
				t.Errorf("X-Registry-Auth sent without a login: %q", rec.Auth)
			}
			if !rec.Close {
				t.Error("request does not ask to close the connection")
			}
		})
	}
}

func TestPullAuth(t *testing.T) {
	tests := []struct {
		name string
		auth RegistryAuth
		want string
	}{
		{"password", RegistryAuth{Username: "me", Password: "secret", ServerAddress: "ghcr.io"}, `{"username":"me","password":"secret","serveraddress":"ghcr.io"}`},
		{"identity token", RegistryAuth{IdentityToken: "refresh", ServerAddress: "registry.example"}, `{"identitytoken":"refresh","serveraddress":"registry.example"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, last := newFake(t, fake{Status: http.StatusOK, Body: stream})
			rc, err := c.Pull(t.Context(), "ghcr.io/cryguy/private", "v1", "", false, &tt.auth)
			if err != nil {
				t.Fatal(err)
			}
			_ = rc.Close()
			raw, err := base64.URLEncoding.DecodeString(last().Auth)
			if err != nil {
				t.Fatalf("X-Registry-Auth is not base64url: %v", err)
			}
			if string(raw) != tt.want {
				t.Errorf("X-Registry-Auth = %s, want %s", raw, tt.want)
			}
		})
	}
}

func TestRegistryAuthURLAlphabet(t *testing.T) {
	a := RegistryAuth{Password: "??>>~~"}
	raw, _ := json.Marshal(a)
	if !strings.ContainsAny(base64.StdEncoding.EncodeToString(raw), "+/") {
		t.Fatal("the password does not reach the characters where the alphabets differ")
	}
	if got := a.encode(); strings.ContainsAny(got, "+/") {
		t.Errorf("encode() = %q, uses the standard alphabet", got)
	}
}

func TestPullDaemonError(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"message", http.StatusNotFound, `{"message":"pull access denied for nothing, repository does not exist"}`, "docker pull nothing: pull access denied for nothing, repository does not exist"},
		{"empty body", http.StatusInternalServerError, "", "docker pull nothing: 500 Internal Server Error"},
		{"text body", http.StatusBadGateway, "upstream gone\n", "docker pull nothing: 502 Bad Gateway: upstream gone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newFake(t, fake{Status: tt.status, Body: tt.body})
			_, err := c.Pull(t.Context(), "nothing", "v1", "", false, nil)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestPullDaemonUnreachable(t *testing.T) {
	const msg = "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"
	c, _ := newFake(t, fake{Stderr: msg + "\n", Exit: 1})
	_, err := c.Pull(t.Context(), "nothing", "v1", "", false, nil)
	if err == nil || !strings.Contains(err.Error(), msg) {
		t.Fatalf("err = %v, want dial-stdio's message", err)
	}
}

// TestPullCancel cancels a pull whose stream is open: the read must end with
// the context's error and the subprocess must be gone.
func TestPullCancel(t *testing.T) {
	c, _ := newFake(t, fake{Status: http.StatusOK, Body: stream, Hang: true})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rc, err := c.Pull(ctx, "ghcr.io/cryguy/justpull", "v1", "", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	lines := bufio.NewReader(rc)
	if line, err := lines.ReadString('\n'); err != nil || line != stream {
		t.Fatalf("first line = %q, %v", line, err)
	}
	read := make(chan error, 1)
	go func() {
		_, err := lines.ReadString('\n')
		read <- err
	}()
	cancel()
	select {
	case err := <-read:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read after cancel: %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stream is still open 10s after cancel")
	}
	_ = rc.Close()
	if rc.(body).conn.cmd.ProcessState == nil {
		t.Fatal("dial-stdio was not waited for")
	}
}

func TestArch(t *testing.T) {
	tests := []struct{ info, want string }{
		{"x86_64\n", "amd64"},
		{"aarch64\n", "arm64"},
		{"arm64\n", "arm64"},
	}
	for _, tt := range tests {
		c, last := newFake(t, fake{Stdout: tt.info})
		got, err := c.Arch(t.Context())
		if err != nil || got != tt.want {
			t.Errorf("Arch() with %q = %q, %v; want %q", tt.info, got, err, tt.want)
		}
		if args := last().Args; !reflect.DeepEqual(args, []string{"info", "--format", "{{.Architecture}}"}) {
			t.Errorf("args = %q", args)
		}
	}
	c, _ := newFake(t, fake{Stdout: "s390x\n"})
	if _, err := c.Arch(t.Context()); err == nil || !strings.Contains(err.Error(), `"s390x"`) {
		t.Errorf("Arch() on s390x: err = %v, want one naming s390x", err)
	}
}

func TestListTags(t *testing.T) {
	const repo = "ghcr.io/cryguy/justpull"
	c, last := newFake(t, fake{Stdout: repo + "\tv1\n" + repo + "\t<none>\n" + repo + "\tv2\n"})
	got, err := c.ListTags(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{repo + ":v1", repo + ":v2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListTags = %q, want %q", got, want)
	}
	if args := last().Args; !reflect.DeepEqual(args, []string{"image", "ls", "--format", "{{.Repository}}\t{{.Tag}}", repo}) {
		t.Errorf("args = %q", args)
	}
}

func TestImageExists(t *testing.T) {
	tests := []struct {
		name    string
		f       fake
		want    bool
		wantErr string
	}{
		{"present", fake{Stdout: "sha256:abc\n"}, true, ""},
		{"absent", fake{Stderr: "Error response from daemon: No such image: x:v1\n", Exit: 1}, false, ""},
		{"daemon down", fake{Stderr: "Cannot connect to the Docker daemon\n", Exit: 1}, false, "docker image inspect --format {{.Id}} x:v1: Cannot connect to the Docker daemon"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newFake(t, tt.f)
			got, err := c.ImageExists(t.Context(), "x:v1")
			if got != tt.want {
				t.Errorf("ImageExists = %v, want %v", got, tt.want)
			}
			if tt.wantErr == "" && err != nil || tt.wantErr != "" && (err == nil || err.Error() != tt.wantErr) {
				t.Errorf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestImageID(t *testing.T) {
	c, last := newFake(t, fake{Stdout: "sha256:abc\n"})
	if id, ok, err := c.ImageID(t.Context(), "x:v1"); id != "sha256:abc" || !ok || err != nil {
		t.Errorf("ImageID = %q, %v, %v; want sha256:abc", id, ok, err)
	}
	if args := last().Args; !reflect.DeepEqual(args, []string{"image", "inspect", "--format", "{{.Id}}", "x:v1"}) {
		t.Errorf("args = %q", args)
	}
	c, _ = newFake(t, fake{Stderr: "Error response from daemon: No such image: x:v1\n", Exit: 1})
	if id, ok, err := c.ImageID(t.Context(), "x:v1"); id != "" || ok || err != nil {
		t.Errorf("ImageID of a missing image = %q, %v, %v; want not ok", id, ok, err)
	}
}

func TestRepoDigests(t *testing.T) {
	c, last := newFake(t, fake{Stdout: `["alpine@sha256:aa","127.0.0.1:5000/library/alpine@sha256:aa"]` + "\n"})
	got, err := c.RepoDigests(t.Context(), "alpine:3.20")
	if want := []string{"alpine@sha256:aa", "127.0.0.1:5000/library/alpine@sha256:aa"}; err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("RepoDigests = %q, %v; want %q", got, err, want)
	}
	if args := last().Args; !reflect.DeepEqual(args, []string{"image", "inspect", "--format", "{{json .RepoDigests}}", "alpine:3.20"}) {
		t.Errorf("args = %q", args)
	}
	for _, out := range []string{"[]\n", "null\n"} {
		c, _ = newFake(t, fake{Stdout: out})
		if got, err := c.RepoDigests(t.Context(), "alpine:3.20"); err != nil || len(got) != 0 {
			t.Errorf("RepoDigests of %q = %q, %v; want none", out, got, err)
		}
	}
	c, _ = newFake(t, fake{Stderr: "Error response from daemon: No such image: alpine:3.20\n", Exit: 1})
	if _, err := c.RepoDigests(t.Context(), "alpine:3.20"); err == nil {
		t.Error("RepoDigests of a missing image: no error")
	}
}

func TestTagIDs(t *testing.T) {
	const repo = "127.0.0.1:41234/library/alpine"
	c, last := newFake(t, fake{Stdout: "3.20\tsha256:aa\n<none>\tsha256:bb\n3.19\tsha256:cc\n"})
	got, err := c.TagIDs(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"3.20": "sha256:aa", "3.19": "sha256:cc"}; !reflect.DeepEqual(got, want) {
		t.Errorf("TagIDs = %q, want %q", got, want)
	}
	if args := last().Args; !reflect.DeepEqual(args, []string{"image", "ls", "--no-trunc", "--format", "{{.Tag}}\t{{.ID}}", repo}) {
		t.Errorf("args = %q", args)
	}
}

func TestTagAndUntag(t *testing.T) {
	c, last := newFake(t, fake{})
	if err := c.Tag(t.Context(), "127.0.0.1:5000/a:v1", "a:v1"); err != nil {
		t.Fatal(err)
	}
	if args := last().Args; !reflect.DeepEqual(args, []string{"image", "tag", "127.0.0.1:5000/a:v1", "a:v1"}) {
		t.Errorf("tag args = %q", args)
	}
	if err := c.Untag(t.Context(), "127.0.0.1:5000/a:v1"); err != nil {
		t.Fatal(err)
	}
	if args := last().Args; !reflect.DeepEqual(args, []string{"image", "rm", "127.0.0.1:5000/a:v1"}) {
		t.Errorf("untag args = %q", args)
	}
}

func TestCommandErrorCarriesStderr(t *testing.T) {
	c, _ := newFake(t, fake{Stderr: "\nError response from daemon: No such image: a:v1\n\n", Exit: 1})
	err := c.Tag(t.Context(), "a:v1", "b:v1")
	if want := "docker image tag a:v1 b:v1: Error response from daemon: No such image: a:v1"; err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	c, _ = newFake(t, fake{Exit: 3})
	if err := c.Tag(t.Context(), "a:v1", "b:v1"); err == nil || !strings.Contains(err.Error(), "exit status 3") {
		t.Fatalf("err without stderr = %v, want the exit status", err)
	}
}

func TestImport(t *testing.T) {
	c, last := newFake(t, fake{Stdout: "sha256:abc\n"})
	changes := []string{`ENTRYPOINT ["/justpull"]`, "USER 65534:65534"}
	if err := c.Import(t.Context(), strings.NewReader("tarball bytes"), "ghcr.io/cryguy/justpull:v1", "linux/amd64", changes); err != nil {
		t.Fatal(err)
	}
	rec := last()
	want := []string{"image", "import", "--platform", "linux/amd64", "--change", `ENTRYPOINT ["/justpull"]`, "--change", "USER 65534:65534", "-", "ghcr.io/cryguy/justpull:v1"}
	if !reflect.DeepEqual(rec.Args, want) {
		t.Errorf("args = %q, want %q", rec.Args, want)
	}
	if rec.Stdin != "tarball bytes" {
		t.Errorf("stdin = %q", rec.Stdin)
	}
}

// TestRealPing asks the daemon the docker command reaches for /_ping through
// docker system dial-stdio.
func TestRealPing(t *testing.T) {
	if os.Getenv("JUSTPULL_REAL_DOCKER") != "1" {
		t.Skip("set JUSTPULL_REAL_DOCKER=1 to ping the local Docker")
	}
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://api.moby.localhost/_ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.roundTrip(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s, API %s: %q", resp.Status, resp.Header.Get("Api-Version"), raw)
	if resp.StatusCode != http.StatusOK || string(raw) != "OK" {
		t.Fatalf("ping = %s %q, want 200 OK", resp.Status, raw)
	}
}
