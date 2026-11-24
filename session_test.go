package justpull

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cryguy/justpull/internal/fakedocker"
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
	if err := dockerPull(s.Context(), fakedocker.New(), s.LocalRef(), nil, nil); err != nil {
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
	if err := dockerPull(s.Context(), fakedocker.New(), s.LocalRef(), nil, nil); err != nil {
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
	cases := []struct {
		ref     string
		allTags bool
		want    string
	}{
		{"alpine:3.20", false, "library/alpine:3.20"},
		{"registry.example.com/team/app", false, "team/app:latest"},
		{"registry.example.com/team/app@" + digest, false, "team/app@" + digest},
		{"alpine", true, "library/alpine"},
		{"registry.example.com/team/app", true, "team/app"},
	}
	for _, tc := range cases {
		s, err := c.Serve(context.Background(), tc.ref, SessionOptions{AllTags: tc.allTags})
		if err != nil {
			t.Fatal(err)
		}
		local := s.LocalRef()
		_ = s.Finish(false)
		host, path, _ := strings.Cut(local, "/")
		if !strings.HasPrefix(host, "127.0.0.1:") || path != tc.want {
			t.Errorf("%s (all tags %v): LocalRef = %s, want 127.0.0.1:<port>/%s", tc.ref, tc.allTags, local, tc.want)
		}
	}
}

func TestAllTagsRefusesATagOrDigest(t *testing.T) {
	c := newPullCache(t, time.Second)
	for _, ref := range []string{"team/app:latest", "team/app@sha256:" + strings.Repeat("a", 64)} {
		s, err := c.Serve(context.Background(), ref, SessionOptions{AllTags: true})
		if err == nil {
			_ = s.Finish(false)
			t.Errorf("%s: served all tags of a reference that names one image", ref)
		}
	}
}

func TestTagListIsRelayedPageByPageWithLinksToTheProxy(t *testing.T) {
	reg := fakereg.New(t)
	reg.TokenTTL = time.Hour
	reg.AbsoluteLinks = true
	for _, tag := range []string{"v2", "v3", "v1"} {
		reg.AddImage("team/app", tag, 1000)
	}
	c := newPullCache(t, 5*time.Second)
	s, err := c.Serve(context.Background(), reg.Host+"/team/app", SessionOptions{AllTags: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Finish(false) }()
	host, _, _ := strings.Cut(s.LocalRef(), "/")
	page, err := url.Parse("http://" + host + "/v2/team/app/tags/list?n=2")
	if err != nil {
		t.Fatal(err)
	}

	var tags, links []string
	for {
		resp, err := http.Get(page.String())
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Tags []string `json:"tags"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || err != nil {
			t.Fatalf("GET %s = %s, decode %v", page, resp.Status, err)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type = %q", ct)
		}
		tags = append(tags, body.Tags...)
		link := resp.Header.Get("Link")
		if link == "" {
			break
		}
		links = append(links, link)
		// As dockerd follows it: the URL between < and >, resolved against
		// the page it asked for.
		next, err := url.Parse(strings.Trim(strings.Split(link, ";")[0], "<>"))
		if err != nil {
			t.Fatal(err)
		}
		page = page.ResolveReference(next)
	}
	if want := []string{"v1", "v2", "v3"}; !slices.Equal(tags, want) {
		t.Fatalf("tags = %v, want %v", tags, want)
	}
	if want := []string{`</v2/team/app/tags/list?last=v2&n=2>; rel="next"`}; !slices.Equal(links, want) {
		t.Fatalf("links = %q, want %q", links, want)
	}

	req, _ := http.NewRequest(http.MethodHead, "http://"+host+"/v2/team/app/tags/list?n=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if want := `</v2/team/app/tags/list?last=v1&n=1>; rel="next"`; resp.StatusCode != http.StatusOK || resp.Header.Get("Link") != want {
		t.Fatalf("HEAD = %s with Link %q, want 200 with %q", resp.Status, resp.Header.Get("Link"), want)
	}
}

func TestRefusedTagListFailsTheSession(t *testing.T) {
	reg := privateRegistry(t, time.Hour)
	c := newPullCache(t, 5*time.Second)
	s, err := c.Serve(context.Background(), reg.Host+"/team/app", SessionOptions{AllTags: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Finish(false) }()
	host, _, _ := strings.Cut(s.LocalRef(), "/")

	resp, err := http.Get("http://" + host + "/v2/team/app/tags/list")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("the proxy served a tag list the registry refused")
	}
	if cause := context.Cause(s.Context()); !isPermanent(cause) || !strings.Contains(cause.Error(), "refused access to team/app") {
		t.Fatalf("session cause = %v, want the registry's refusal", cause)
	}
}
