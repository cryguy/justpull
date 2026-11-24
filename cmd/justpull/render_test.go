package main

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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
// byte. dockerd writes a digest pull's reference in full as its first
// message's id, so renameID maps the address to docker.io.
var (
	rename   = strings.NewReplacer("127.0.0.1:41234/library/", "").Replace
	renameID = strings.NewReplacer("127.0.0.1:41234/", "docker.io/").Replace
)

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
		{"digest.json", false, 0, "digest.notty.golden", ""},
		{"digest.json", true, 200, "digest.tty200.golden", ""},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/tty=%v/width=%d", tt.stream, tt.tty, tt.width), func(t *testing.T) {
			var out bytes.Buffer
			r := newRenderer(&out, tt.tty, tt.width, false, rename)
			r.renameID = renameID
			err := r.render(strings.NewReader(fixture(t, tt.stream)))
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

// A line without an id during a hold first draws the held line in place:
// lines above it are never rewritten again.
func TestResumedTTYHoldEndsAtALineWithoutID(t *testing.T) {
	var out bytes.Buffer
	r, clk := heldRenderer(&out)
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
	digest := strings.Index(golden, "Digest: ")
	want := golden[:at] + "\x1b[1A\x1b[2K\r4f55086f7dd0: Resuming at 5.243MB \r\x1b[1B" +
		"\n\x1b[1A\x1b[2K\rd5e71e642bf5: Download complete \r\x1b[1B" +
		"\x1b[2A\x1b[2K\r4f55086f7dd0: Pull complete \r\x1b[2B" + golden[digest:]
	if out.String() != want {
		t.Fatalf("got %q\nwant %q", out.String(), want)
	}
	if got, want := screen(out.String()), screen(golden); !equal(got, want) {
		t.Fatalf("screen %q, want %q", got, want)
	}
	if n := clk.live(); n != 0 {
		t.Fatalf("%d hold timers still running", n)
	}
}

func TestResumedTTYHoldShowsNewest(t *testing.T) {
	stream := fixtureLines(t, "layers.json")
	var out bytes.Buffer
	r, clk := heldRenderer(&out)
	var atResume, during, justBefore, after []string
	steps := &stepReader{items: []any{
		strings.Join(stream[:3], ""),
		func() {
			r.resumed("8a49fdb3b6a5", 5_000_000)
			atResume = screen(out.String())
		},
		// c3bf32f7e4e2's lines, and 8a49fdb3b6a5 at 1MB, then at 3.6MB.
		strings.Join(stream[3:7], ""),
		func() {
			during = screen(out.String())
			clk.advance(resumeHold - time.Millisecond)
			justBefore = screen(out.String())
			clk.advance(time.Millisecond)
			after = screen(out.String())
		},
		strings.Join(stream[7:], ""),
	}}
	if err := r.render(steps); err != nil {
		t.Fatal(err)
	}
	const resuming = "8a49fdb3b6a5: Resuming at 5MB "
	if got := atResume[2]; got != resuming {
		t.Fatalf("line at resume = %q, want %q", got, resuming)
	}
	if got := during[2]; got != resuming {
		t.Fatalf("Docker's messages replaced the line during the hold: %q", got)
	}
	if got, want := during[3], "c3bf32f7e4e2: Waiting "; got != want {
		t.Fatalf("another layer's line = %q, want %q", got, want)
	}
	if !equal(justBefore, during) {
		t.Fatalf("the screen changed before the hold ended: %q", justBefore)
	}
	if want := screen(plain(t, stream[:7])); !equal(after, want) {
		t.Fatalf("screen when the hold ended = %q, want Docker's newest message %q", after, want)
	}
	if strings.Contains(out.String(), "1.049MB/3.624MB") {
		t.Fatal("drew a held message that a newer one replaced")
	}
	if got, want := screen(out.String()), screen(fixture(t, "layers.tty200.golden")); !equal(got, want) {
		t.Fatalf("screen %q, want %q", got, want)
	}
}

func TestResumedTTYSecondResumeExtendsTheHold(t *testing.T) {
	stream := fixtureLines(t, "layers.json")
	var out bytes.Buffer
	r, clk := heldRenderer(&out)
	var second, atFirstEnd, justBefore, after []string
	steps := &stepReader{items: []any{
		strings.Join(stream[:3], ""),
		func() { r.resumed("8a49fdb3b6a5", 1_000_000) },
		strings.Join(stream[3:6], ""),
		func() {
			clk.advance(1500 * time.Millisecond)
			r.resumed("8a49fdb3b6a5", 2_000_000)
			second = screen(out.String())
		},
		stream[6],
		func() {
			clk.advance(500 * time.Millisecond)
			atFirstEnd = screen(out.String())
			clk.advance(resumeHold - 500*time.Millisecond - time.Millisecond)
			justBefore = screen(out.String())
			clk.advance(time.Millisecond)
			after = screen(out.String())
		},
		strings.Join(stream[7:], ""),
	}}
	if err := r.render(steps); err != nil {
		t.Fatal(err)
	}
	const resuming = "8a49fdb3b6a5: Resuming at 2MB "
	if got := second[2]; got != resuming {
		t.Fatalf("line at the second resume = %q, want %q", got, resuming)
	}
	if got := atFirstEnd[2]; got != resuming {
		t.Fatalf("the first hold's end replaced the line: %q", got)
	}
	if got := justBefore[2]; got != resuming {
		t.Fatalf("the line changed before the second hold ended: %q", got)
	}
	if want := screen(plain(t, stream[:7])); !equal(after, want) {
		t.Fatalf("screen when the hold ended = %q, want Docker's newest message %q", after, want)
	}
	if got, want := screen(out.String()), screen(fixture(t, "layers.tty200.golden")); !equal(got, want) {
		t.Fatalf("screen %q, want %q", got, want)
	}
}

func TestResumedTTYStreamEndDrawsHeldLines(t *testing.T) {
	// layers.json up to its last layer message: the stream ends during the
	// hold, with no line without an id after it.
	stream := fixtureLines(t, "layers.json")[:16]
	var out bytes.Buffer
	r, clk := heldRenderer(&out)
	steps := &stepReader{items: []any{
		strings.Join(stream[:3], ""),
		func() { r.resumed("8a49fdb3b6a5", 5_000_000) },
		strings.Join(stream[3:], ""),
	}}
	if err := r.render(steps); err != nil {
		t.Fatal(err)
	}
	if got, want := screen(out.String()), screen(plain(t, stream)); !equal(got, want) {
		t.Fatalf("screen at the end = %q, want %q", got, want)
	}
	if n := clk.live(); n != 0 {
		t.Fatalf("%d hold timers still running", n)
	}
	r.resumed("8a49fdb3b6a5", 1)
	clk.advance(resumeHold)
	if got, want := screen(out.String()), screen(plain(t, stream)); !equal(got, want) {
		t.Fatalf("drew after the stream ended: %q", got)
	}
}

func TestResumedTTYUnknownIDOpensALine(t *testing.T) {
	var out bytes.Buffer
	r, _ := heldRenderer(&out)
	steps := helloSteps(t, r, 0)
	var atResume string
	steps.items[1] = func() {
		before := out.Len()
		r.resumed("0123456789ab", 1000)
		atResume = out.String()[before:]
	}
	if err := r.render(steps); err != nil {
		t.Fatal(err)
	}
	if want := "\n\x1b[1A\x1b[2K\r0123456789ab: Resuming at 1kB \r\x1b[1B"; atResume != want {
		t.Fatalf("resumed wrote %q, want %q", atResume, want)
	}
	golden := screen(fixture(t, "hello.tty200.golden"))
	want := slices.Insert(golden, 2, "0123456789ab: Resuming at 1kB ")
	if got := screen(out.String()); !equal(got, want) {
		t.Fatalf("screen %q, want %q", got, want)
	}
}

func TestResumedTTYConcurrent(t *testing.T) {
	var out lockedBuffer
	r, clk := heldRenderer(&out)
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- r.render(pr) }()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	ids := []string{"8a49fdb3b6a5", "c3bf32f7e4e2", "000000000000", "000000000001"}
	for _, id := range ids {
		wg.Go(func() {
			for n := range int64(500) {
				select {
				case <-stop:
					return
				default:
					r.resumed(id, n)
				}
			}
		})
	}
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				clk.advance(time.Second)
			}
		}
	})
	for line := range strings.Lines(fixture(t, "layers.json")) {
		_, _ = io.WriteString(pw, line)
	}
	_ = pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	// A resume can open a line after Docker's last one, until render returns.
	got := slices.DeleteFunc(screen(out.String()), func(line string) bool { return strings.Contains(line, ": Resuming at ") })
	golden := screen(fixture(t, "layers.tty200.golden"))
	if tail, want := got[len(got)-3:], golden[len(golden)-3:]; !equal(tail, want) {
		t.Fatalf("the screen ends %q, want %q", tail, want)
	}
}

// heldRenderer is a terminal renderer whose hold timers run on a fake clock.
func heldRenderer(out io.Writer) (*renderer, *fakeClock) {
	r := newRenderer(out, true, 200, false, rename)
	clk := &fakeClock{}
	r.after = clk.after
	return r, clk
}

// plain renders lines of a stream in a terminal, without a resume.
func plain(t *testing.T, lines []string) string {
	t.Helper()
	var out bytes.Buffer
	if err := newRenderer(&out, true, 200, false, rename).render(strings.NewReader(strings.Join(lines, ""))); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func fixtureLines(t *testing.T, name string) []string {
	return slices.Collect(strings.Lines(fixture(t, name)))
}

// fakeClock runs the timers a renderer starts when the test advances it.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Duration
	timers []*fakeTimer
}

type fakeTimer struct {
	at   time.Duration
	f    func()
	done bool // fired or stopped
}

func (c *fakeClock) after(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now + d, f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		stopped := !t.done
		t.done = true
		return stopped
	}
}

// advance moves the clock by d and runs the timers that are due, earliest
// first, in the caller's goroutine.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	var due []*fakeTimer
	live := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.done:
		case t.at <= c.now:
			t.done = true
			due = append(due, t)
		default:
			live = append(live, t)
		}
	}
	c.timers = live
	c.mu.Unlock()
	slices.SortStableFunc(due, func(a, b *fakeTimer) int { return cmp.Compare(a.at, b.at) })
	for _, t := range due {
		t.f()
	}
}

// live counts the timers neither fired nor stopped.
func (c *fakeClock) live() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.done {
			n++
		}
	}
	return n
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
