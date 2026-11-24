// Package proxyimage makes the image that runs the justpull proxy present in
// Docker: imported from the Linux binary a release build embeds, or pulled.
package proxyimage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cryguy/justpull/internal/engine"
)

// Repo is the proxy image's repository; its tag is the justpull version.
const Repo = "ghcr.io/cryguy/justpull"

// changes are the config of an imported image. The proxy runs as nobody
// (65534), which owns /cache.
var changes = []string{`ENTRYPOINT ["/justpull"]`, "USER 65534:65534"}

// docker is the part of engine.Client that Ensure uses.
type docker interface {
	ImageExists(ctx context.Context, ref string) (bool, error)
	Arch(ctx context.Context) (string, error)
	Import(ctx context.Context, tarball io.Reader, ref, platform string, changes []string) error
	Pull(ctx context.Context, image, tag, platform string, allTags bool, auth *engine.RegistryAuth) (io.ReadCloser, error)
	ListTags(ctx context.Context, repo string) ([]string, error)
	Untag(ctx context.Context, ref string) error
}

// Ensure makes Repo:version present in Docker and returns that reference.
// If it is missing, it imports the binary embedded returns for the daemon's
// architecture (gzipped; ok false when this build has none), or else pulls
// Repo:version anonymously, writing Docker's progress to progress as plain
// lines. A development build ("dev") has nothing to pull. After adding the
// image it removes every other tag of Repo; a tag whose image a container
// uses stays.
func Ensure(ctx context.Context, cli *engine.Client, version string, embedded func(arch string) (gzipped []byte, ok bool), progress io.Writer) (string, error) {
	return ensure(ctx, cli, version, embedded, progress)
}

func ensure(ctx context.Context, d docker, version string, embedded func(arch string) ([]byte, bool), progress io.Writer) (string, error) {
	ref := Repo + ":" + version
	present, err := d.ImageExists(ctx, ref)
	if err != nil {
		return "", err
	}
	if present {
		return ref, nil
	}
	arch, err := d.Arch(ctx)
	if err != nil {
		return "", err
	}
	gzipped, ok := embedded(arch)
	switch {
	case ok:
		err = importBinary(ctx, d, ref, arch, gzipped)
	case version == "dev":
		return "", fmt.Errorf("the proxy image %s is not in Docker, and this development build of justpull carries no proxy for linux/%s to import: use a release build", ref, arch)
	default:
		err = pull(ctx, d, version, progress)
	}
	if err != nil {
		return "", err
	}
	removeOthers(ctx, d, ref)
	return ref, nil
}

func importBinary(ctx context.Context, d docker, ref, arch string, gzipped []byte) error {
	zr, err := gzip.NewReader(bytes.NewReader(gzipped))
	if err != nil {
		return fmt.Errorf("unpack the embedded proxy for linux/%s: %w", arch, err)
	}
	binary, err := io.ReadAll(zr)
	if err != nil {
		return fmt.Errorf("unpack the embedded proxy for linux/%s: %w", arch, err)
	}
	layer, err := rootfs(binary)
	if err != nil {
		return err
	}
	return d.Import(ctx, bytes.NewReader(layer), ref, "linux/"+arch, changes)
}

// rootfs returns the image's filesystem as a tar: the proxy at /justpull and
// an empty /cache owned by nobody. A fresh named volume mounted on /cache
// takes the directory's owner, so the proxy, running as nobody, can write
// its cache there. Dockerfile.proxy builds the published image with the same
// filesystem and changes; keep the two alike.
func rootfs(binary []byte) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	epoch := time.Unix(0, 0)
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "cache/", Mode: 0o755, Uid: 65534, Gid: 65534, ModTime: epoch}); err != nil {
		return nil, fmt.Errorf("build the proxy image: %w", err)
	}
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "justpull", Mode: 0o755, Size: int64(len(binary)), ModTime: epoch}); err != nil {
		return nil, fmt.Errorf("build the proxy image: %w", err)
	}
	if _, err := tw.Write(binary); err != nil {
		return nil, fmt.Errorf("build the proxy image: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("build the proxy image: %w", err)
	}
	return buf.Bytes(), nil
}

// pull pulls Repo:version for the daemon's platform. As with docker pull,
// a failure during the pull is an {"error":...} message in the stream.
func pull(ctx context.Context, d docker, version string, progress io.Writer) error {
	rc, err := d.Pull(ctx, Repo, version, "", false, nil)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	dec := json.NewDecoder(rc)
	for {
		var m struct {
			ID             string `json:"id"`
			Status         string `json:"status"`
			Error          string `json:"error"`
			ProgressDetail struct {
				Current int64 `json:"current"`
				Total   int64 `json:"total"`
			} `json:"progressDetail"`
		}
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read docker pull stream: %w", err)
		}
		if m.Error != "" {
			return fmt.Errorf("pull %s:%s: %s", Repo, version, m.Error)
		}
		// Like docker pull without a terminal: the byte counts of a download
		// or an extraction are not lines.
		if m.ProgressDetail.Current > 0 || m.ProgressDetail.Total > 0 {
			continue
		}
		line := m.Status
		if m.ID != "" {
			line = m.ID + ": " + line
		}
		_, _ = fmt.Fprintln(progress, line)
	}
}

// removeOthers untags every tag of Repo but ref. Errors are ignored: an
// image a container uses cannot go, and the next new version tries again.
func removeOthers(ctx context.Context, d docker, ref string) {
	tags, err := d.ListTags(ctx, Repo)
	if err != nil {
		return
	}
	for _, tag := range tags {
		if tag != ref {
			_ = d.Untag(ctx, tag)
		}
	}
}
