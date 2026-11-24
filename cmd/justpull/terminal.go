package main

import (
	"os"

	"golang.org/x/term"
)

// terminal reports whether f is a terminal and its width, enabling ANSI
// escape processing on Windows consoles (fall back to non-TTY if that fails).
func terminal(f *os.File) (tty bool, width int) {
	fd := int(f.Fd())
	if !term.IsTerminal(fd) || enableANSI(f) != nil {
		return false, 0
	}
	width, _, err := term.GetSize(fd)
	if err != nil {
		return true, 0
	}
	return true, width
}
