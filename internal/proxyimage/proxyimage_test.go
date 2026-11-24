package proxyimage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/cryguy/justpull/internal/engine"
)

// fakeDocker records the calls Ensure makes, in order.
type fakeDocker struct {
	exists   bool
	arch     string
	stream   string
	tags     []string
	listErr  error
	untagErr map[string]error

	calls    []string
	imported []byte
	changes  []string
}

func (f *fakeDocker) ImageExists(_ context.Context, ref string) (bool, error) {
	f.calls = append(f.calls, "exists "+ref)
	return f.exists, nil
}

func (f *fakeDocker) Arch(context.Context) (string, error) {
	f.calls = append(f.calls, "arch")
	return f.arch, nil
}

func (f *fakeDocker) Import(_ context.Context, tarball io.Reader, ref, platform string, changes []string) error {
	f.calls = append(f.calls, "import "+ref+" "+platform)
	f.imported, _ = io.ReadAll(tarball)
	f.changes = changes
	return nil
}

func (f *fakeDocker) Pull(_ context.Context, image, tag, platform string, allTags bool, auth *engine.RegistryAuth) (io.ReadCloser, error) {
	f.calls = append(f.calls, fmt.Sprintf("pull %s %s platform=%q allTags=%v auth=%v", image, tag, platform, allTags, auth != nil))
	return io.NopCloser(strings.NewReader(f.stream)), nil
}

func (f *fakeDocker) ListTags(_ context.Context, repo string) ([]string, error) {
	f.calls = append(f.calls, "list "+repo)
	return f.tags, f.listErr
}

func (f *fakeDocker) Untag(_ context.Context, ref string) error {
	f.calls = append(f.calls, "untag "+ref)
	return f.untagErr[ref]
}

// embeds returns an embedded func that has binary for arch only.
func embeds(t *testing.T, arch string, binary []byte) func(string) ([]byte, bool) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(binary); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return func(a string) ([]byte, bool) {
		if a != arch {
			return nil, false
		}
		return buf.Bytes(), true
	}
}

func embedsNone(string) ([]byte, bool) { return nil, false }

const pulled = `{"status":"Pulling from cryguy/justpull","id":"v1"}
{"status":"Pull complete","progressDetail":{},"id":"0123456789ab"}
`

func TestEnsureDecides(t *testing.T) {
	binary := []byte("\x7fELF proxy")
	tests := []struct {
		name      string
		version   string
		exists    bool
		embedded  func(string) ([]byte, bool)
		wantCalls []string
		wantErr   string
	}{
		{
			name: "present", version: "v1", exists: true, embedded: embeds(t, "arm64", binary),
			wantCalls: []string{"exists " + Repo + ":v1"},
		},
		{
			name: "embedded", version: "v1", embedded: embeds(t, "arm64", binary),
			wantCalls: []string{"exists " + Repo + ":v1", "arch", "import " + Repo + ":v1 linux/arm64", "list " + Repo},
		},
		{
			name: "embedded dev", version: "dev", embedded: embeds(t, "arm64", binary),
			wantCalls: []string{"exists " + Repo + ":dev", "arch", "import " + Repo + ":dev linux/arm64", "list " + Repo},
		},
		{
			name: "none", version: "v1", embedded: embedsNone,
			wantCalls: []string{"exists " + Repo + ":v1", "arch", `pull ` + Repo + ` v1 platform="" allTags=false auth=false`, "list " + Repo},
		},
		{
			name: "only another arch", version: "v1", embedded: embeds(t, "amd64", binary),
			wantCalls: []string{"exists " + Repo + ":v1", "arch", `pull ` + Repo + ` v1 platform="" allTags=false auth=false`, "list " + Repo},
		},
		{
			name: "none dev", version: "dev", embedded: embedsNone,
			wantCalls: []string{"exists " + Repo + ":dev", "arch"},
			wantErr:   "use a release build",
		},
		{
			name: "present dev", version: "dev", exists: true, embedded: embedsNone,
			wantCalls: []string{"exists " + Repo + ":dev"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &fakeDocker{exists: tt.exists, arch: "arm64", stream: pulled}
			ref, err := ensure(t.Context(), d, tt.version, tt.embedded, io.Discard)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
				}
			} else if err != nil || ref != Repo+":"+tt.version {
				t.Fatalf("Ensure = %q, %v; want %s:%s", ref, err, Repo, tt.version)
			}
			if !reflect.DeepEqual(d.calls, tt.wantCalls) {
				t.Errorf("calls:\n%s\nwant:\n%s", strings.Join(d.calls, "\n"), strings.Join(tt.wantCalls, "\n"))
			}
			if d.imported == nil {
				return
			}
			if !reflect.DeepEqual(d.changes, []string{`ENTRYPOINT ["/justpull"]`, "USER 65534:65534"}) {
				t.Errorf("changes = %q", d.changes)
			}
			if got := tarFiles(t, d.imported)["justpull"]; !bytes.Equal(got, binary) {
				t.Errorf("imported /justpull = %q, want the unzipped binary %q", got, binary)
			}
		})
	}
}

func TestEnsurePullProgress(t *testing.T) {
	stream := `{"status":"Pulling from cryguy/justpull","id":"v1"}
{"status":"Pulling fs layer","progressDetail":{},"id":"0123456789ab"}
{"status":"Downloading","progressDetail":{"current":512,"total":1024},"progress":"[=====>     ]","id":"0123456789ab"}
{"status":"Verifying Checksum","progressDetail":{},"id":"0123456789ab"}
{"status":"Download complete","progressDetail":{},"id":"0123456789ab"}
{"status":"Extracting","progressDetail":{"current":1024,"total":1024},"progress":"[==========>]","id":"0123456789ab"}
{"status":"Pull complete","progressDetail":{},"id":"0123456789ab"}
{"status":"Digest: sha256:` + strings.Repeat("cd", 32) + `"}
{"status":"Status: Downloaded newer image for ghcr.io/cryguy/justpull:v1"}
`
	want := `v1: Pulling from cryguy/justpull
0123456789ab: Pulling fs layer
0123456789ab: Verifying Checksum
0123456789ab: Download complete
0123456789ab: Pull complete
Digest: sha256:` + strings.Repeat("cd", 32) + `
Status: Downloaded newer image for ghcr.io/cryguy/justpull:v1
`
	d := &fakeDocker{arch: "amd64", stream: stream}
	var progress strings.Builder
	if _, err := ensure(t.Context(), d, "v1", embedsNone, &progress); err != nil {
		t.Fatal(err)
	}
	if progress.String() != want {
		t.Errorf("progress:\n%s\nwant:\n%s", progress.String(), want)
	}
}

func TestEnsurePullError(t *testing.T) {
	d := &fakeDocker{arch: "amd64", stream: `{"status":"Pulling from cryguy/justpull","id":"v9"}
{"errorDetail":{"message":"manifest unknown"},"error":"manifest unknown"}
`}
	_, err := ensure(t.Context(), d, "v9", embedsNone, io.Discard)
	if want := "pull " + Repo + ":v9: manifest unknown"; err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	if last := d.calls[len(d.calls)-1]; strings.HasPrefix(last, "list") {
		t.Error("old tags were cleaned up after a failed pull")
	}
}

func TestEnsureRemovesOtherTags(t *testing.T) {
	d := &fakeDocker{
		arch:     "amd64",
		stream:   pulled,
		tags:     []string{Repo + ":v0.9", Repo + ":v1", Repo + ":v0.8"},
		untagErr: map[string]error{Repo + ":v0.9": errors.New("conflict: unable to remove repository reference (must force) - container abc is using its referenced image")},
	}
	if _, err := ensure(t.Context(), d, "v1", embedsNone, io.Discard); err != nil {
		t.Fatalf("Ensure: %v, want the untag error ignored", err)
	}
	var untagged []string
	for _, c := range d.calls {
		if ref, ok := strings.CutPrefix(c, "untag "); ok {
			untagged = append(untagged, ref)
		}
	}
	if want := []string{Repo + ":v0.9", Repo + ":v0.8"}; !reflect.DeepEqual(untagged, want) {
		t.Errorf("untagged %q, want %q", untagged, want)
	}

	d = &fakeDocker{arch: "amd64", stream: pulled, listErr: errors.New("docker image ls: daemon gone")}
	if _, err := ensure(t.Context(), d, "v1", embedsNone, io.Discard); err != nil {
		t.Fatalf("Ensure: %v, want the list error ignored", err)
	}
}

// tarFiles returns the regular files of a tar by name.
func tarFiles(t *testing.T, raw []byte) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			files[h.Name], _ = io.ReadAll(tr)
		}
	}
}

func TestRootfs(t *testing.T) {
	binary := []byte("\x7fELF the proxy")
	raw, err := rootfs(binary)
	if err != nil {
		t.Fatal(err)
	}
	type entry struct {
		name     string
		typ      byte
		mode     int64
		uid, gid int
		body     string
	}
	var got []entry
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tr)
		got = append(got, entry{h.Name, h.Typeflag, h.Mode, h.Uid, h.Gid, string(body)})
	}
	want := []entry{
		{"cache/", tar.TypeDir, 0o755, 65534, 65534, ""},
		{"justpull", tar.TypeReg, 0o755, 0, 0, string(binary)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tar entries = %+v, want %+v", got, want)
	}
}

// TestRealImport imports a stand-in proxy image into the local Docker as
// justpull-test/proxy:test, checks its config, and removes it.
func TestRealImport(t *testing.T) {
	if os.Getenv("JUSTPULL_REAL_DOCKER") != "1" {
		t.Skip("set JUSTPULL_REAL_DOCKER=1 to import into the local Docker")
	}
	const repo = "justpull-test/proxy"
	const ref = repo + ":test"
	cli, err := engine.New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	arch, err := cli.Arch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	layer, err := rootfs([]byte("not a binary\n"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Untag(context.Background(), ref) })
	if err := cli.Import(ctx, bytes.NewReader(layer), ref, "linux/"+arch, changes); err != nil {
		t.Fatal(err)
	}
	out, err := cli.Command(ctx, "image", "inspect", "--format", "{{json .Config.Entrypoint}} {{.Config.User}} {{.Os}}/{{.Architecture}}", ref).Output()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("inspect: %s", bytes.TrimSpace(out))
	if got, want := string(bytes.TrimSpace(out)), `["/justpull"] 65534:65534 linux/`+arch; got != want {
		t.Errorf("config = %s, want %s", got, want)
	}
	if tags, err := cli.ListTags(ctx, repo); err != nil || !reflect.DeepEqual(tags, []string{ref}) {
		t.Errorf("ListTags = %q, %v; want [%s]", tags, err, ref)
	}
	if err := cli.Untag(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if exists, err := cli.ImageExists(ctx, ref); err != nil || exists {
		t.Errorf("after removal: ImageExists = %v, %v", exists, err)
	}
}
