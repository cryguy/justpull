package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The fixtures pull through 127.0.0.1:41234. The goldens are what the docker
// CLI 29 renderer (moby client v0.6.0, pkg/jsonmessage) prints for the same
// streams naming the real image, non-TTY and in a pty of the width in the
// file name. hello.json is a real pull of hello-world on the containerd
// store, whose non-TTY golden matches the docker CLI's own output byte for
// byte.
var rename = strings.NewReplacer("127.0.0.1:41234/library/", "").Replace

func fixture(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestRenderGolden(t *testing.T) {
	const pullErr = "failed to register layer of alpine:3.20: unexpected EOF"
	tests := []struct {
		stream  string
		tty     bool
		width   int
		golden  string
		wantErr string
	}{
		{"hello.json", false, 0, "hello.notty.golden", ""},
		{"hello.json", false, 120, "hello.notty.golden", ""},
		{"hello.json", true, 200, "hello.tty200.golden", ""},
		{"hello.json", true, 0, "hello.tty200.golden", ""},
		{"layers.json", false, 0, "layers.notty.golden", ""},
		{"layers.json", true, 200, "layers.tty200.golden", ""},
		{"layers.json", true, 120, "layers.tty120.golden", ""},
		{"layers.json", true, 80, "layers.tty80.golden", ""},
		{"layers.json", true, 40, "layers.tty40.golden", ""},
		{"error.json", false, 0, "error.notty.golden", pullErr},
		{"error.json", true, 200, "error.tty200.golden", pullErr},
		{"alltags.json", false, 0, "alltags.notty.golden", ""},
		{"alltags.json", true, 200, "alltags.tty200.golden", ""},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/tty=%v/width=%d", tt.stream, tt.tty, tt.width), func(t *testing.T) {
			var out bytes.Buffer
			err := newRenderer(&out, tt.tty, tt.width, false, rename).render(strings.NewReader(fixture(t, tt.stream)))
			if got := errText(err); got != tt.wantErr {
				t.Fatalf("render error = %q, want %q", got, tt.wantErr)
			}
			if want := fixture(t, tt.golden); out.String() != want {
				t.Fatalf("output differs from docker's\n got %q\nwant %q", out.String(), want)
			}
		})
	}
}

func TestRenderWidthBoundary(t *testing.T) {
	// Docker never truncates a line: below 111 columns it drops the bar.
	tty120, tty80 := fixture(t, "layers.tty120.golden"), fixture(t, "layers.tty80.golden")
	for width, want := range map[int]string{111: tty120, 110: tty80} {
		var out bytes.Buffer
		if err := newRenderer(&out, true, width, false, rename).render(strings.NewReader(fixture(t, "layers.json"))); err != nil {
			t.Fatal(err)
		}
		if out.String() != want {
			t.Errorf("width %d: got %q\nwant %q", width, out.String(), want)
		}
	}
}

func TestRenderErrorStopsReading(t *testing.T) {
	var out bytes.Buffer
	err := newRenderer(&out, false, 0, false, rename).render(strings.NewReader(fixture(t, "error.json")))
	if err == nil || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("render error = %v, want the renamed stream error", err)
	}
	if strings.Contains(out.String(), "Status:") || strings.Contains(out.String(), "failed") {
		t.Fatalf("printed past or printed the error:\n%s", out.String())
	}
}

func TestRenderReadError(t *testing.T) {
	err := newRenderer(io.Discard, false, 0, false, rename).render(strings.NewReader(`{"status":"Pulling fs`))
	if err == nil || err.Error() != "read docker pull stream: unexpected EOF" {
		t.Fatalf("render error = %v", err)
	}
}

func TestRenderQuiet(t *testing.T) {
	var out bytes.Buffer
	r := newRenderer(&out, true, 200, true, rename)
	if err := r.render(strings.NewReader(fixture(t, "layers.json"))); err != nil {
		t.Fatal(err)
	}
	err := newRenderer(&out, true, 200, true, rename).render(strings.NewReader(fixture(t, "error.json")))
	if errText(err) != "failed to register layer of alpine:3.20: unexpected EOF" {
		t.Fatalf("quiet render error = %v", err)
	}
	q := newRenderer(&out, true, 200, true, rename)
	q.resumed("8a49fdb3b6a5", 1<<20)
	if out.Len() != 0 {
		t.Fatalf("quiet printed %q", out.String())
	}
}

// hello.json split after the layer's first message.
func helloSteps(t *testing.T, r *renderer, offset int64) *stepReader {
	hello := fixture(t, "hello.json")
	cut := strings.Index(hello, `{"status":"Download complete"`)
	return &stepReader{items: []any{hello[:cut], func() { r.resumed("4f55086f7dd0", offset) }, hello[cut:]}}
}

func TestResumedNoTTY(t *testing.T) {
	var out bytes.Buffer
	r := newRenderer(&out, false, 0, false, rename)
	if err := r.render(helloSteps(t, r, 5_242_880)); err != nil {
		t.Fatal(err)
	}
	golden := fixture(t, "hello.notty.golden")
	want := strings.Replace(golden, "4f55086f7dd0: Pulling fs layer\n", "4f55086f7dd0: Pulling fs layer\n4f55086f7dd0: Resuming at 5.243MB\n", 1)
	if out.String() != want {
		t.Fatalf("got %q\nwant %q", out.String(), want)
	}
	r.resumed("4f55086f7dd0", 1)
	if out.String() != want {
		t.Fatalf("resumed after the stream ended printed %q", strings.TrimPrefix(out.String(), want))
	}
}

func TestResumedTTY(t *testing.T) {
	var out bytes.Buffer
	r := newRenderer(&out, true, 200, false, rename)
	steps := helloSteps(t, r, 5_242_880)
	var atResume []string
	steps.items[1] = func() {
		r.resumed("4f55086f7dd0", 5_242_880)
		atResume = screen(out.String())
	}
	if err := r.render(steps); err != nil {
		t.Fatal(err)
	}
	wantAtResume := []string{"latest: Pulling from library/hello-world", "4f55086f7dd0: Resuming at 5.243MB ", ""}
	if !equal(atResume, wantAtResume) {
		t.Fatalf("screen after resumed = %q, want %q", atResume, wantAtResume)
	}
	golden := fixture(t, "hello.tty200.golden")
	at := strings.Index(golden, "\x1b[1A\x1b[2K\r4f55086f7dd0: Download complete")
	want := golden[:at] + "\x1b[1A\x1b[2K\r4f55086f7dd0: Resuming at 5.243MB \r\x1b[1B" + golden[at:]
	if out.String() != want {
		t.Fatalf("got %q\nwant %q", out.String(), want)
	}
	if got, want := screen(out.String()), screen(golden); !equal(got, want) {
		t.Fatalf("Docker's next message did not replace the line: screen %q, want %q", got, want)
	}
}

func TestResumedConcurrent(t *testing.T) {
	var out lockedBuffer
	r := newRenderer(&out, false, 0, false, rename)
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- r.render(pr) }()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := range 4 {
		wg.Go(func() {
			for n := range int64(2000) {
				select {
				case <-stop:
					return
				default:
					r.resumed(fmt.Sprintf("%012x", i), n)
				}
			}
		})
	}
	for line := range strings.Lines(fixture(t, "layers.json")) {
		_, _ = io.WriteString(pw, line)
	}
	_ = pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	golden := fixture(t, "layers.notty.golden")
	var rest []string
	for line := range strings.Lines(out.String()) {
		if strings.Contains(line, ": Resuming at ") {
			continue
		}
		rest = append(rest, line)
	}
	if strings.Join(rest, "") != golden {
		t.Fatalf("docker's lines were mixed up:\n%s", strings.Join(rest, ""))
	}
}

func TestProgressBarTimeLeft(t *testing.T) {
	now := time.Unix(1_700_000_010, 0)
	p := progress{Current: 1_000_000, Total: 3_000_000, Start: 1_700_000_000}
	tests := []struct {
		width int
		want  string
	}{
		{120, "[================>                                  ]      1MB/3MB 20s"},
		{110, "     1MB/3MB 20s"},
		{50, "     1MB/3MB"},
	}
	for _, tt := range tests {
		if got := progressBar(p, tt.width, now); got != tt.want {
			t.Errorf("width %d: %q, want %q", tt.width, got, tt.want)
		}
	}
}

func TestHumanSize(t *testing.T) {
	for size, want := range map[float64]string{0: "0B", 999: "999B", 1000: "1kB", 10_070: "10.07kB", 999_950: "1000kB", 3_623_807: "3.624MB", 2e12: "2TB"} {
		if got := humanSize(size); got != want {
			t.Errorf("humanSize(%v) = %q, want %q", size, got, want)
		}
	}
}

func TestTerminalNotATerminal(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if tty, width := terminal(f); tty || width != 0 {
		t.Fatalf("terminal(file) = %v, %d", tty, width)
	}
}

// screen plays the escape sequences the renderer uses (line feed, carriage
// return, cursor up and down, erase line) and returns the lines a terminal
// shows, the cursor's empty last line included.
func screen(s string) []string {
	lines := []string{""}
	row, col := 0, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\n':
			row++
			if row == len(lines) {
				lines = append(lines, "")
			}
		case c == '\r':
			col = 0
		case c == 0x1b:
			end := i + 2
			for end < len(s) && (s[end] < 'A' || s[end] > 'Z') {
				end++
			}
			n, _ := strconv.Atoi(s[i+2 : end])
			switch s[end] {
			case 'A':
				row = max(row-n, 0)
			case 'B':
				row = min(row+n, len(lines)-1)
			case 'K':
				lines[row] = ""
			}
			i = end
		default:
			line := []byte(lines[row])
			for len(line) < col {
				line = append(line, ' ')
			}
			lines[row] = string(append(line[:col], c)) + string(line[min(col+1, len(line)):])
			col++
		}
	}
	return lines
}

// stepReader serves its strings in order and runs each func when the reader
// reaches it, which json.Decoder does only after decoding everything before.
type stepReader struct{ items []any }

func (s *stepReader) Read(p []byte) (int, error) {
	for len(s.items) > 0 {
		switch v := s.items[0].(type) {
		case func():
			s.items = s.items[1:]
			v()
		case string:
			n := copy(p, v)
			if n < len(v) {
				s.items[0] = v[n:]
			} else {
				s.items = s.items[1:]
			}
			return n, nil
		}
	}
	return 0, io.EOF
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func equal(a, b []string) bool {
	return strings.Join(a, "\x00") == strings.Join(b, "\x00") && len(a) == len(b)
}
