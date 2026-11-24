package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/cryguy/justpull/internal/engine"
	"github.com/cryguy/justpull/internal/proxyimage"
)

// dockerAPI is what a pull needs from Docker: engine.Client, the proxy image
// and the proxy container.
type dockerAPI interface {
	// EnsureProxy makes the proxy image present and returns its reference.
	EnsureProxy(ctx context.Context, progress io.Writer) (string, error)
	// StartProxy runs justpull --serve from image.
	StartProxy(image string) (*proxyProc, error)
	Pull(ctx context.Context, image, tag, platform string, allTags bool, auth *engine.RegistryAuth) (io.ReadCloser, error)
	Tag(ctx context.Context, source, target string) error
	Untag(ctx context.Context, ref string) error
	ListTags(ctx context.Context, repo string) ([]string, error)
	ImageID(ctx context.Context, ref string) (string, bool, error)
	TagIDs(ctx context.Context, repo string) (map[string]string, error)
}

// proxyProc is a running justpull --serve.
type proxyProc struct {
	stdin  io.WriteCloser
	stdout io.Reader
	// wait waits for it to exit. Call it once.
	wait func() error
	// kill ends it at once.
	kill func()
	// stderr is what it printed on stderr; complete once wait returned.
	stderr func() string
}

type engineDocker struct{ *engine.Client }

func newDocker() (dockerAPI, error) {
	cli, err := engine.New()
	if err != nil {
		return nil, err
	}
	return engineDocker{cli}, nil
}

func (d engineDocker) EnsureProxy(ctx context.Context, progress io.Writer) (string, error) {
	return proxyimage.Ensure(ctx, d.Client, version, embeddedProxy, progress)
}

// StartProxy runs the proxy with docker run. The process is tied to no
// context: an interrupted pull still tells the proxy to finish.
func (d engineDocker) StartProxy(image string) (*proxyProc, error) {
	id := make([]byte, 4)
	_, _ = rand.Read(id)
	cmd := d.Command(context.Background(), proxyArgs(image, "justpull-"+hex.EncodeToString(id))...)
	var stderr lockedWriter
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	return &proxyProc{
		stdin:  stdin,
		stdout: stdout,
		wait:   cmd.Wait,
		kill:   func() { _ = cmd.Process.Kill() },
		stderr: stderr.String,
	}, nil
}

// proxyArgs runs the proxy on the host's network, where dockerd's loopback
// is the container's; Docker Desktop's VM cannot reach the host's loopback.
// On the host's network Docker's default capabilities would let a hijacked
// proxy sniff the host's traffic, so it gets none, as nobody, read-only.
// --pull never keeps a newer CLI's cleanup from making an older one fetch
// its image from ghcr.io. The login reaches it on stdin, never here.
func proxyArgs(image, name string) []string {
	return []string{
		"run", "-i", "--rm", "--pull", "never", "--name", name,
		"--network", "host",
		"--user", "65534:65534", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
		"-v", "justpull-cache:/cache",
		image, "--serve", "--cache", "/cache",
	}
}

type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
