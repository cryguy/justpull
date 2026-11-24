package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
)

// RegistryAuth is a registry login as the daemon takes it for a pull.
type RegistryAuth struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	ServerAddress string `json:"serveraddress,omitempty"`
}

// encode returns the X-Registry-Auth value the docker CLI sends: the JSON,
// base64url-encoded.
func (a RegistryAuth) encode() string {
	raw, _ := json.Marshal(a)
	return base64.URLEncoding.EncodeToString(raw)
}

// Pull starts pulling image and returns Docker's progress stream, one JSON
// message per line. The outcome is inside the stream: a failure during the
// pull is an {"error":...} message. tag may be a tag or a digest; "" means
// latest, unless allTags pulls every tag of image. platform "" means the
// daemon's. auth, when non-nil, is the registry login. Cancelling ctx, or
// closing the stream, ends the connection, and dockerd stops the pull.
func (c *Client) Pull(ctx context.Context, image, tag, platform string, allTags bool, auth *RegistryAuth) (io.ReadCloser, error) {
	q := url.Values{"fromImage": {image}}
	if !allTags {
		if tag == "" {
			tag = "latest"
		}
		q.Set("tag", tag)
	}
	if platform != "" {
		q.Set("platform", platform)
	}
	req, err := http.NewRequest(http.MethodPost, "http://api.moby.localhost/images/create?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("docker pull %s: %w", image, err)
	}
	if auth != nil {
		req.Header.Set("X-Registry-Auth", auth.encode())
	}
	resp, err := c.roundTrip(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("docker pull %s: %w", image, err)
	}
	if resp.StatusCode/100 != 2 {
		defer func() { _ = resp.Body.Close() }()
		return nil, &APIError{Call: "docker pull " + image, Message: apiMessage(resp)}
	}
	return resp.Body, nil
}

// APIError is the daemon's error answer to an API call. The docker CLI
// prints it as "Error response from daemon: " and Message.
type APIError struct {
	Call    string
	Message string
}

func (e *APIError) Error() string { return e.Call + ": " + e.Message }

// apiMessage is the message of the daemon's error answer {"message":...}.
func apiMessage(resp *http.Response) string {
	raw, _ := io.ReadAll(resp.Body)
	var e struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &e) == nil && e.Message != "" {
		return e.Message
	}
	if text := strings.TrimSpace(string(raw)); text != "" {
		return resp.Status + ": " + text
	}
	return resp.Status
}

// roundTrip sends req to the daemon on a connection of its own and reads the
// answer's header. Closing the body ends the connection, and so does
// cancelling ctx.
func (c *Client) roundTrip(ctx context.Context, req *http.Request) (*http.Response, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	req.Close = true
	if err := req.Write(conn); err != nil {
		return nil, conn.fail(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return nil, conn.fail(err)
	}
	resp.Body = body{Reader: resp.Body, conn: conn}
	return resp, nil
}

// body is an answer's body whose Close ends its connection at once. The
// body http.ReadResponse returns would first read a chunked body to its end,
// which for a pull stream is the end of the pull.
type body struct {
	io.Reader
	conn *stdioConn
}

func (b body) Close() error { return b.conn.Close() }

// stdioConn is a connection to the daemon's API through a `docker system
// dial-stdio` subprocess: reads come from its stdout, writes go to its stdin.
// Its stdin stays open until Close: Docker Desktop answers a request whose
// stdin closed before the answer was read with 500 and no body.
type stdioConn struct {
	ctx    context.Context
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr bytes.Buffer
	stop   func() bool
	once   sync.Once
}

// dial starts dial-stdio. Cancelling ctx closes the connection.
func (c *Client) dial(ctx context.Context) (*stdioConn, error) {
	cmd := exec.Command(c.docker, "system", "dial-stdio")
	conn := &stdioConn{ctx: ctx, cmd: cmd}
	cmd.Stderr = &conn.stderr
	var err error
	if conn.stdin, err = cmd.StdinPipe(); err != nil {
		return nil, fmt.Errorf("docker system dial-stdio: %w", err)
	}
	if conn.stdout, err = cmd.StdoutPipe(); err != nil {
		return nil, fmt.Errorf("docker system dial-stdio: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("docker system dial-stdio: %w", err)
	}
	conn.stop = context.AfterFunc(ctx, conn.shutdown)
	return conn, nil
}

func (c *stdioConn) Read(p []byte) (int, error) {
	n, err := c.stdout.Read(p)
	if err != nil && c.ctx.Err() != nil {
		err = c.ctx.Err()
	}
	return n, err
}

func (c *stdioConn) Write(p []byte) (int, error) {
	n, err := c.stdin.Write(p)
	if err != nil && c.ctx.Err() != nil {
		err = c.ctx.Err()
	}
	return n, err
}

// Close closes stdin, then kills the subprocess and waits for it.
func (c *stdioConn) Close() error {
	c.stop()
	c.shutdown()
	return nil
}

func (c *stdioConn) shutdown() {
	c.once.Do(func() {
		_ = c.stdin.Close()
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
	})
}

// fail closes the connection after err and returns why the exchange failed:
// dial-stdio's own message when it printed one, such as a daemon that is not
// running.
func (c *stdioConn) fail(err error) error {
	_ = c.Close()
	if c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	if msg := strings.TrimSpace(c.stderr.String()); msg != "" {
		return fmt.Errorf("docker system dial-stdio: %s", msg)
	}
	return fmt.Errorf("docker system dial-stdio: %w", err)
}
