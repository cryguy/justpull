package main

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableANSI turns on escape sequence processing for a console, which the
// docker CLI also leaves on.
func enableANSI(f *os.File) error {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		return err
	}
	return windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
}
