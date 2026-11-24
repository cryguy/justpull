//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package justpull

import (
	"errors"
	"os"
	"syscall"
)

const flockSupported = true

// lockShared takes a shared lock on the file at path, creating it, waiting
// while another holder has it exclusively.
func lockShared(path string) (*os.File, error) {
	return lockFile(path, syscall.LOCK_SH)
}

// tryLockExclusive takes an exclusive lock on the file at path, creating it,
// or returns errLocked if anyone else holds it.
func tryLockExclusive(path string) (*os.File, error) {
	return lockFile(path, syscall.LOCK_EX|syscall.LOCK_NB)
}

// lockFile opens the file at path and flocks it. A flock belongs to the open
// file description: it ends when the file is closed or the process dies, and
// two opens in one process exclude each other as two processes do.
func lockFile(path string, how int) (*os.File, error) {
	for {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, err
		}
		if err := flock(f, how); err != nil {
			_ = f.Close()
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return nil, errLocked
			}
			return nil, err
		}
		// A deleter unlinks lock files while it holds them. A lock won on a
		// file no longer at path excludes nobody, so it is dropped and taken
		// again on the file there now.
		held, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		now, err := os.Stat(path)
		if err == nil && os.SameFile(held, now) {
			return f, nil
		}
		_ = f.Close()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
}

func flock(f *os.File, how int) error {
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
