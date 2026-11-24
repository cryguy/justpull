//go:build !windows

package main

import (
	"os"
	"syscall"
)

// terminationSignals are the signals that interrupt docker.
var terminationSignals = []os.Signal{syscall.SIGTERM, syscall.SIGINT}
