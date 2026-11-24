//go:build !(darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd)

package justpull

import (
	"errors"
	"os"
)

// The cache coordinates the processes sharing its directory with flock(2),
// which the syscall package offers only on the systems lock_unix.go names,
// so New refuses to open one here.
const flockSupported = false

var errNoFlock = errors.New("flock(2) is not available")

func lockShared(string) (*os.File, error) { return nil, errNoFlock }

func tryLockExclusive(string) (*os.File, error) { return nil, errNoFlock }
