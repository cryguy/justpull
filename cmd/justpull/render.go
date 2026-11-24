package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// renderer prints a docker pull's progress the way the docker CLI does. It
// follows the CLI's jsonmessage display: in a terminal each layer keeps one
// line that its later messages rewrite in place; otherwise the messages
// carrying byte counts are dropped and every other one is a line of its own.
type renderer struct {
	out    io.Writer
	tty    bool
	width  int
	rename func(string) string
	now    func() time.Time
	// after runs f after d, as time.AfterFunc does, and returns the timer's
	// Stop.
	after func(d time.Duration, f func()) (stop func() bool)
	// renameID, if set, maps a message's id to what the user pulled. A
	// digest pull's first message has the local reference as its id. Set it
	// before render.
	renameID func(string) string
	// upToDate, if set, reports whether docker pull would find the user's
	// name ref (as the status line names it, renamed) up to date. It may
	// call Docker. Set it before render.
	upToDate func(ref string) bool

	mu    sync.Mutex
	lines map[string]int   // a layer's line, counted from the first since the last line without progress
	holds map[string]*hold // the layers whose "Resuming at" line is held on screen
	ended bool
}

// resumeHold is how long a layer's "Resuming at" line stays in a terminal.
// Docker's next message for the layer comes within milliseconds and would
// replace it before anyone could read it.
const resumeHold = 2 * time.Second

// hold keeps a layer's "Resuming at" line on screen. Docker's messages for
// the layer wait; when the hold ends, the newest one is drawn.
type hold struct {
	stop func() bool
	next *message // Docker's newest message for the layer, not drawn yet
}

// message is one message of Docker's JSON stream, as the docker CLI reads
// it. The CLI ignores the "error" string and the pre-rendered "progress"
// bar; it reads errorDetail and progressDetail.
type message struct {
	Stream   string           `json:"stream"`
	Status   string           `json:"status"`
	Progress *progress        `json:"progressDetail"`
	ID       string           `json:"id"`
	Error    *streamError     `json:"errorDetail"`
	Aux      *json.RawMessage `json:"aux"`
}

type progress struct {
	Current    int64  `json:"current"`
	Total      int64  `json:"total"`
	Start      int64  `json:"start"`
	HideCounts bool   `json:"hidecounts"`
	Units      string `json:"units"`
}

type streamError struct {
	Message string `json:"message"`
}

const (
	eraseLine  = "\x1b[2K"
	cursorUp   = "\x1b[%dA"
	cursorDown = "\x1b[%dB"
)

// newRenderer: out is where docker would print (stdout). tty selects the
// terminal mode (cursor movement and progress bars) and width is the
// terminal width (0 if unknown). quiet prints nothing. rename maps text that
// may hold the local name to what the user pulled.
func newRenderer(out io.Writer, tty bool, width int, quiet bool, rename func(string) string) *renderer {
	if quiet {
		out, tty = io.Discard, false
	}
	return &renderer{
		out: out, tty: tty, width: width, rename: rename, now: time.Now, after: afterFunc,
		lines: map[string]int{}, holds: map[string]*hold{},
	}
}

func afterFunc(d time.Duration, f func()) func() bool {
	return time.AfterFunc(d, f).Stop
}

// render reads Docker's JSON message stream to its end and prints it. It
// returns the stream's error message (renamed) when the pull failed inside
// the stream, like readStream does, or a read error. It draws every held
// line before it returns.
func (r *renderer) render(stream io.Reader) error {
	defer func() {
		r.mu.Lock()
		r.ended = true
		r.release()
		r.mu.Unlock()
	}()
	dec := json.NewDecoder(stream)
	for {
		var m message
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read docker pull stream: %w", err)
		}
		m.Status, m.Stream = r.rename(m.Status), r.rename(m.Stream)
		if r.renameID != nil {
			m.ID = r.renameID(m.ID)
		}
		r.status(&m)
		r.mu.Lock()
		err := r.show(m)
		r.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

// resumed shows that the proxy resumed a layer's download: in a terminal the
// layer's line reads "<id>: Resuming at <size>" for resumeHold, or until a
// line without progress or the stream's end, and then shows Docker's newest
// message for the layer; otherwise one such line is printed. A second resume
// during the hold restarts it. id is the 12-hex layer ID docker uses. size
// uses the same units docker prints. Safe to call from another goroutine
// while render runs.
func (r *renderer) resumed(id string, offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended {
		return
	}
	var next *message
	if h, ok := r.holds[id]; ok {
		h.stop()
		next = h.next
		delete(r.holds, id)
	}
	_ = r.show(message{ID: id, Status: "Resuming at " + humanSize(float64(offset)), Progress: &progress{}})
	if !r.tty {
		return
	}
	h := &hold{next: next}
	h.stop = r.after(resumeHold, func() { r.unhold(id, h) })
	r.holds[id] = h
}

// unhold ends id's hold h, unless it already ended, and draws the message it
// kept.
func (r *renderer) unhold(id string, h *hold) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.holds[id] != h {
		return
	}
	delete(r.holds, id)
	if h.next != nil {
		_ = r.show(*h.next)
	}
}

// release ends every hold and draws the messages they kept, top line first.
// The caller holds mu.
func (r *renderer) release() {
	if len(r.holds) == 0 {
		return
	}
	ids := slices.SortedFunc(maps.Keys(r.holds), func(a, b string) int { return cmp.Compare(r.lines[a], r.lines[b]) })
	for _, id := range ids {
		h := r.holds[id]
		delete(r.holds, id)
		h.stop()
		if h.next != nil {
			_ = r.show(*h.next)
		}
	}
}

const (
	newerStatus    = "Status: Downloaded newer image for "
	upToDateStatus = "Status: Image is up to date for "
)

// status turns dockerd's "Downloaded newer image" into "Image is up to date"
// when upToDate says so. dockerd compares the pulled name's image before and
// after the pull, but it pulled the local name, which is always new to it.
func (r *renderer) status(m *message) {
	if r.upToDate == nil || m.ID != "" {
		return
	}
	if ref, ok := strings.CutPrefix(m.Status, newerStatus); ok && r.upToDate(ref) {
		m.Status = upToDateStatus + ref
	}
}

// show prints m as one write. The caller holds mu.
func (r *renderer) show(m message) error {
	if m.Aux != nil {
		return nil
	}
	if m.ID != "" && m.Progress != nil {
		if h, ok := r.holds[m.ID]; ok {
			h.next = &m
			return nil
		}
	} else {
		// The held lines are drawn while they can still be rewritten.
		r.release()
	}
	var b strings.Builder
	var diff int
	if m.ID != "" && m.Progress != nil {
		line, ok := r.lines[m.ID]
		if !ok {
			line = len(r.lines)
			r.lines[m.ID] = line
			if r.tty {
				b.WriteString("\n")
			}
		}
		diff = len(r.lines) - line
		if r.tty && diff > 0 {
			fmt.Fprintf(&b, cursorUp, diff)
		}
	} else {
		// Lines above a line without progress are never rewritten again.
		clear(r.lines)
	}
	err := r.display(&b, m)
	if m.ID != "" && r.tty && diff > 0 {
		fmt.Fprintf(&b, cursorDown, diff)
	}
	_, _ = io.WriteString(r.out, b.String())
	return err
}

// display writes m's text, as jsonmessage.Display does.
func (r *renderer) display(b *strings.Builder, m message) error {
	if m.Error != nil {
		return errors.New(r.rename(m.Error.Message))
	}
	var endl string
	if r.tty && m.Stream == "" && m.Progress != nil {
		b.WriteString(eraseLine + "\r")
		endl = "\r"
	} else if m.Progress != nil && (m.Progress.Current > 0 || m.Progress.Total > 0) {
		return nil
	}
	if m.ID != "" {
		b.WriteString(m.ID + ": ")
	}
	switch {
	case m.Progress != nil && r.tty:
		width := r.width
		if width == 0 {
			width = 200
		}
		b.WriteString(m.Status + " " + progressBar(*m.Progress, width, r.now()) + endl)
	case m.Stream != "":
		b.WriteString(m.Stream + endl)
	default:
		b.WriteString(m.Status + endl + "\n")
	}
	return nil
}

// progressBar is jsonmessage.RenderTUIProgress: the bar needs more than 110
// columns and the time left more than 50.
func progressBar(p progress, width int, now time.Time) string {
	if p.Current <= 0 && p.Total <= 0 {
		return ""
	}
	if p.Total <= 0 {
		if p.Units == "" {
			return fmt.Sprintf("%8v", humanSize(float64(p.Current)))
		}
		return fmt.Sprintf("%d %s", p.Current, p.Units)
	}
	percentage := max(min(int(float64(p.Current)/float64(p.Total)*100)/2, 50), 0)
	var bar string
	if width > 110 {
		bar = "[" + strings.Repeat("=", percentage) + ">" + strings.Repeat(" ", 50-percentage) + "] "
	}
	var numbers string
	switch {
	case p.HideCounts:
	case p.Units == "" && p.Current > p.Total:
		numbers = fmt.Sprintf("%8v", humanSize(float64(p.Current)))
	case p.Units == "":
		numbers = fmt.Sprintf("%8v/%v", humanSize(float64(p.Current)), humanSize(float64(p.Total)))
	case p.Current > p.Total:
		numbers = fmt.Sprintf("%d %s", p.Current, p.Units)
	default:
		numbers = fmt.Sprintf("%d/%d %s", p.Current, p.Total, p.Units)
	}
	var left string
	if width > 50 && p.Current > 0 && p.Start > 0 && percentage < 50 {
		perEntry := now.UTC().Sub(time.Unix(p.Start, 0)) / time.Duration(p.Current)
		left = " " + (time.Duration(p.Total-p.Current) * perEntry).Round(time.Second).String()
	}
	return bar + numbers + left
}

// humanSize is go-units' HumanSize: decimal units, four significant digits.
func humanSize(size float64) string {
	units := []string{"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"}
	i := 0
	for size >= 1000 && i < len(units)-1 {
		size /= 1000
		i++
	}
	return fmt.Sprintf("%.4g%s", size, units[i])
}
