// Package engine talks to the Docker daemon that the user's docker command
// reaches, whatever its context, DOCKER_HOST or transport, without the Docker
// SDK. Engine API calls go over `docker system dial-stdio`; everything else
// runs the docker command and reads its output.
package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// Client runs the docker command.
type Client struct {
	docker string // path of the docker command
}

// New finds the docker command on PATH.
func New() (*Client, error) {
	path, err := exec.LookPath("docker")
	if err != nil {
		return nil, fmt.Errorf("find the docker command: %w", err)
	}
	return &Client{docker: path}, nil
}

// Command returns a docker subprocess with the given arguments, for callers
// that need its pipes. Cancelling ctx kills it.
func (c *Client) Command(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, c.docker, args...)
}

// Tag gives the image source also the name target.
func (c *Client) Tag(ctx context.Context, source, target string) error {
	_, err := c.run(ctx, nil, "image", "tag", source, target)
	return err
}

// Untag runs docker image rm on ref. It only removes the name while another
// name holds the image; on the last name it removes the image, which fails
// while a container uses it.
func (c *Client) Untag(ctx context.Context, ref string) error {
	_, err := c.run(ctx, nil, "image", "rm", ref)
	return err
}

// ImageExists reports whether Docker has an image named ref.
func (c *Client) ImageExists(ctx context.Context, ref string) (bool, error) {
	_, ok, err := c.ImageID(ctx, ref)
	return ok, err
}

// ImageID returns the ID of the image Docker has under ref; ok is false when
// it has none.
func (c *Client) ImageID(ctx context.Context, ref string) (id string, ok bool, err error) {
	out, err := c.run(ctx, nil, "image", "inspect", "--format", "{{.Id}}", ref)
	if err == nil {
		return strings.TrimSpace(out), true, nil
	}
	if re, isRun := errors.AsType[*runError](err); isRun && strings.Contains(re.stderr, "No such image") {
		return "", false, nil
	}
	return "", false, err
}

// TagIDs returns the image ID of each tag Docker has in repo, by tag.
func (c *Client) TagIDs(ctx context.Context, repo string) (map[string]string, error) {
	out, err := c.run(ctx, nil, "image", "ls", "--no-trunc", "--format", "{{.Tag}}\t{{.ID}}", repo)
	if err != nil {
		return nil, err
	}
	ids := map[string]string{}
	for line := range strings.Lines(out) {
		tag, id, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || tag == "<none>" {
			continue
		}
		ids[tag] = id
	}
	return ids, nil
}

// ListTags returns the names docker images lists for repo, as repo:tag.
// Images without a tag are left out.
func (c *Client) ListTags(ctx context.Context, repo string) ([]string, error) {
	out, err := c.run(ctx, nil, "image", "ls", "--format", "{{.Repository}}\t{{.Tag}}", repo)
	if err != nil {
		return nil, err
	}
	var refs []string
	for line := range strings.Lines(out) {
		name, tag, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || tag == "<none>" {
			continue
		}
		refs = append(refs, name+":"+tag)
	}
	return refs, nil
}

// Arch returns the daemon's architecture as a GOARCH name.
func (c *Client) Arch(ctx context.Context) (string, error) {
	out, err := c.run(ctx, nil, "info", "--format", "{{.Architecture}}")
	if err != nil {
		return "", err
	}
	switch arch := strings.TrimSpace(out); arch {
	case "x86_64":
		return "amd64", nil
	case "aarch64", "arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("docker runs on an architecture justpull does not support: %q", arch)
	}
}

// Import creates the image ref from a root filesystem tarball. platform ""
// means the daemon's; changes are Dockerfile instructions such as
// ENTRYPOINT or USER applied to the image's config.
func (c *Client) Import(ctx context.Context, tarball io.Reader, ref, platform string, changes []string) error {
	args := []string{"image", "import"}
	if platform != "" {
		args = append(args, "--platform", platform)
	}
	for _, change := range changes {
		args = append(args, "--change", change)
	}
	args = append(args, "-", ref)
	_, err := c.run(ctx, tarball, args...)
	return err
}

// run runs docker with args and stdin and returns its stdout.
func (c *Client) run(ctx context.Context, stdin io.Reader, args ...string) (string, error) {
	cmd := c.Command(ctx, args...)
	cmd.Stdin = stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return "", &runError{args: args, stderr: strings.TrimSpace(stderr.String()), err: err}
	}
	return stdout.String(), nil
}

// runError is a docker command that failed. Its message is docker's stderr.
type runError struct {
	args   []string
	stderr string
	err    error
}

func (e *runError) Error() string {
	msg := e.stderr
	if msg == "" {
		msg = e.err.Error()
	}
	return "docker " + strings.Join(e.args, " ") + ": " + msg
}

func (e *runError) Unwrap() error { return e.err }
