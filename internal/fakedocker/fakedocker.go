// Package fakedocker is a Docker for tests. It pulls like dockerd:
// manifest, config, then each layer it does not have, resuming a blob with
// Range when its body ends early; a repository with no tag is pulled with
// every tag from its tag list. It writes dockerd's progress stream.
package fakedocker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// Docker is the fake. Set its fields before the first pull; read the
// recorded calls once the pull is over.
type Docker struct {
	// ReuseManifests makes it skip the GET for a manifest it already holds
	// (a HEAD resolves the tag), as the containerd store does.
	ReuseManifests bool
	// AfterManifest, if set, runs between the manifest and the first blob.
	AfterManifest func()
	// Have holds the digests of the layers it already has.
	Have map[string]bool

	// Pulls, Tags and Untags record the calls, in order.
	Pulls  []string
	Tags   [][2]string
	Untags []string
	// TagLists records the URL of every tag list page it asked for.
	TagLists []string

	client    *http.Client
	mu        sync.Mutex
	manifests map[string][]byte
	blobGets  map[string]int
}

func New() *Docker {
	return &Docker{client: &http.Client{}, Have: map[string]bool{}, manifests: map[string][]byte{}, blobGets: map[string]int{}}
}

func (f *Docker) manifest(ctx context.Context, url string) ([]byte, error) {
	if f.ReuseManifests {
		req, _ := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")
		resp, err := f.client.Do(req)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		f.mu.Lock()
		raw, ok := f.manifests[resp.Header.Get("Docker-Content-Digest")]
		f.mu.Unlock()
		if ok {
			return raw, nil
		}
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest: %s: %s", resp.Status, raw)
	}
	sum := sha256.Sum256(raw)
	f.mu.Lock()
	f.manifests["sha256:"+hex.EncodeToString(sum[:])] = raw
	f.mu.Unlock()
	return raw, nil
}

func (f *Docker) Pull(ctx context.Context, ref string) (io.ReadCloser, error) {
	f.mu.Lock()
	f.Pulls = append(f.Pulls, ref)
	f.mu.Unlock()
	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw)
		if err := f.pull(ctx, ref, enc); err != nil {
			// dockerd's failure message: the docker CLI reads errorDetail.
			_ = enc.Encode(map[string]any{"errorDetail": map[string]string{"message": err.Error()}, "error": err.Error()})
		}
		_ = pw.Close()
	}()
	return pr, nil
}

func (f *Docker) Tag(_ context.Context, source, target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Tags = append(f.Tags, [2]string{source, target})
	return nil
}

func (f *Docker) Untag(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Untags = append(f.Untags, ref)
	return nil
}

type streamMsg struct {
	ID             string         `json:"id,omitempty"`
	Status         string         `json:"status"`
	ProgressDetail map[string]any `json:"progressDetail,omitempty"`
}

func (f *Docker) pull(ctx context.Context, ref string, enc *json.Encoder) error {
	host, rest, _ := strings.Cut(ref, "/")
	name, mref := rest, ""
	if i := strings.Index(rest, "@"); i >= 0 {
		name, mref = rest[:i], rest[i+1:]
	} else if i := strings.LastIndex(rest, ":"); i >= 0 {
		name, mref = rest[:i], rest[i+1:]
	}
	base := "http://" + host + "/v2/" + name
	if mref != "" {
		return f.pullImage(ctx, ref, base, name, mref, enc)
	}

	// A repository with no tag is pulled with every tag, as by
	// docker pull --all-tags.
	tags, err := f.tagList(ctx, base+"/tags/list")
	if err != nil {
		return err
	}
	for _, tag := range tags {
		if err := f.pullImage(ctx, ref+":"+tag, base, name, tag, enc); err != nil {
			return err
		}
	}
	return nil
}

// tagList reads every page of the tag list as dockerd does: the next page
// is the Link's URL between < and >, resolved against the page asked for.
func (f *Docker) tagList(ctx context.Context, rawURL string) ([]string, error) {
	page, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	var tags []string
	for {
		f.mu.Lock()
		f.TagLists = append(f.TagLists, page.String())
		f.mu.Unlock()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, page.String(), nil)
		resp, err := f.client.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("tag list: %s: %s", resp.Status, raw)
		}
		var body struct {
			Tags []string `json:"tags"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, err
		}
		tags = append(tags, body.Tags...)
		link := resp.Header.Get("Link")
		if link == "" {
			return tags, nil
		}
		next, err := url.Parse(strings.Trim(strings.Split(link, ";")[0], "<>"))
		if err != nil {
			return nil, err
		}
		page = page.ResolveReference(next)
	}
}

func (f *Docker) pullImage(ctx context.Context, ref, base, name, mref string, enc *json.Encoder) error {
	raw, err := f.manifest(ctx, base+"/manifests/"+mref)
	if err != nil {
		return err
	}
	var m struct {
		Config struct{ Digest string }   `json:"config"`
		Layers []struct{ Digest string } `json:"layers"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	if f.AfterManifest != nil {
		f.AfterManifest()
	}
	short := func(d string) string { return strings.TrimPrefix(d, "sha256:")[:12] }
	_ = enc.Encode(streamMsg{ID: mref, Status: "Pulling from " + name})
	for _, l := range m.Layers {
		_ = enc.Encode(streamMsg{ID: short(l.Digest), Status: "Pulling fs layer"})
	}
	if err := f.fetchBlob(ctx, base, m.Config.Digest, nil); err != nil {
		return err
	}
	for _, l := range m.Layers {
		id := short(l.Digest)
		f.mu.Lock()
		have := f.Have[l.Digest]
		f.mu.Unlock()
		if have {
			_ = enc.Encode(streamMsg{ID: id, Status: "Already exists"})
			continue
		}
		err := f.fetchBlob(ctx, base, l.Digest, func(cur, total int64) {
			_ = enc.Encode(streamMsg{ID: id, Status: "Downloading", ProgressDetail: map[string]any{"current": cur, "total": total}})
		})
		if err != nil {
			return err
		}
		_ = enc.Encode(streamMsg{ID: id, Status: "Download complete"})
		_ = enc.Encode(streamMsg{ID: id, Status: "Pull complete"})
	}
	_ = enc.Encode(streamMsg{Status: "Status: Downloaded newer image for " + ref})
	return nil
}

type progressWriter struct {
	n  int64
	fn func(int64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	p.n += int64(len(b))
	if p.fn != nil {
		p.fn(p.n)
	}
	return len(b), nil
}

func (f *Docker) fetchBlob(ctx context.Context, base, digest string, onProgress func(cur, total int64)) error {
	h := sha256.New()
	var got, total int64 = 0, -1
	for attempt := 0; attempt < 4; attempt++ {
		f.mu.Lock()
		f.blobGets[digest]++
		f.mu.Unlock()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/blobs/"+digest, nil)
		if got > 0 {
			req.Header.Set("Range", "bytes="+strconv.FormatInt(got, 10)+"-")
		}
		resp, err := f.client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("blob %s: %s: %s", digest[:19], resp.Status, raw)
		}
		if total < 0 {
			total = resp.ContentLength + got
		}
		pw := &progressWriter{n: got}
		if onProgress != nil {
			pw.fn = func(n int64) { onProgress(n, total) }
		}
		n, err := io.Copy(io.MultiWriter(h, pw), resp.Body)
		_ = resp.Body.Close()
		got += n
		if err == nil && got == total {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
	}
	if sum := "sha256:" + hex.EncodeToString(h.Sum(nil)); sum != digest {
		return fmt.Errorf("blob %s: digest mismatch", digest[:19])
	}
	return nil
}

// Gets returns how many GETs it sent for the blob.
func (f *Docker) Gets(digest string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blobGets[digest]
}
