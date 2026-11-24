//go:build !windows

package main

import "os"

// enableANSI does nothing: Unix terminals process escape sequences.
func enableANSI(*os.File) error { return nil }
