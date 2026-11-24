package justpull

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cryguy/justpull/internal/fakereg"
)

func TestSessionFinishKeepsTheFilesOnFailureAndDeletesThemOnSuccess(t *testing.T) {
	reg := fakereg.New(t)
	img := reg.AddImage("team/app", "v1", 200_000, 100_000)
	c := newPullCache(t, 5*time.Second)
	ref := reg.Host + "/team/app:v1"

	s, err := c.Serve(context.Background(), ref, SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dockerPull(s.Context(), newFakeDocker(), s.LocalRef(), nil, nil); err != nil {
		t.Fatal(err)
	}
	layer := img.Layers[0]
	if got := s.LayerSize(strings.TrimPrefix(layer.Digest, "sha256:")[:12]); got != int64(len(layer.Data)) {
		t.Fatalf("LayerSize = %d, want %d", got, len(layer.Data))
	}
	if err := s.Finish(false); err != nil {
		t.Fatal(err)
	}
	if cause := context.Cause(s.Context()); !errors.Is(cause, context.Canceled) {
		t.Fatalf("context cause after Finish = %v, want a plain cancel", cause)
	}
	host, _, _ := strings.Cut(s.LocalRef(), "/")
	if resp, err := http.Get("http://" + host + "/v2/"); err == nil {
		_ = resp.Body.Close()
		t.Fatal("the proxy still answers after Finish")
	}
	files := cacheFiles(t, c)
	for _, b := range append([]*fakereg.Blob{img.Config}, img.Layers...) {
		if _, ok := files[strings.TrimPrefix(b.Digest, "sha256:")]; !ok {
			t.Fatalf("%s not kept after a failed pull: %v", b.Digest[:19], files)
		}
	}
	if _, ok := files["m/"+strings.TrimPrefix(img.Digest, "sha256:")]; !ok {
		t.Fatalf("manifest not kept after a failed pull: %v", files)
	}

	before := len(reg.Log())
	s, err = c.Serve(context.Background(), ref, SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dockerPull(s.Context(), newFakeDocker(), s.LocalRef(), nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, r := range reg.Log()[before:] {
		if strings.Contains(r.Path, "/blobs/") {
			t.Fatalf("%s fetched again; the cache had it", r.Path)
		}
	}
	if err := s.Finish(true); err != nil {
		t.Fatal(err)
	}
	if files := cacheFiles(t, c); len(files) != 0 {
		t.Fatalf("cache not emptied after a successful pull: %v", files)
	}
}

func TestSessionLocalRef(t *testing.T) {
	c := newPullCache(t, time.Second)
	digest := "sha256:" + strings.Repeat("a", 64)
	for ref, want := range map[string]string{
		"alpine:3.20":                             "library/alpine:3.20",
		"registry.example.com/team/app":           "team/app:latest",
		"registry.example.com/team/app@" + digest: "team/app@" + digest,
	} {
		s, err := c.Serve(context.Background(), ref, SessionOptions{})
		if err != nil {
			t.Fatal(err)
		}
		local := s.LocalRef()
		_ = s.Finish(false)
		host, path, _ := strings.Cut(local, "/")
		if !strings.HasPrefix(host, "127.0.0.1:") || path != want {
			t.Errorf("%s: LocalRef = %s, want 127.0.0.1:<port>/%s", ref, local, want)
		}
	}
}
