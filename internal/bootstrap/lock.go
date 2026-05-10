package bootstrap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// fileLock is an advisory exclusive lock on a sentinel file inside
// the data directory. It exists so two hoardarr processes can't
// concurrently mutate the same DB / incomplete-tree.
//
// Unix only (uses syscall.Flock). Containers are the deployment
// target so this is fine; if Windows support is ever needed, a build
// tag split with golang.org/x/sys/windows.LockFileEx covers it.
type fileLock struct {
	f *os.File
}

// acquireDataDirLock takes an exclusive lock on dataDir/hoardarr.lock.
//
// Returns a clear error if another process holds the lock so the
// operator gets useful guidance instead of a cryptic SQLite error
// when two daemons end up writing the same DB.
func acquireDataDirLock(dataDir string) (*fileLock, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	p := filepath.Join(dataDir, "hoardarr.lock")
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock %q: %w", p, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another hoardarr instance is already using %s (lockfile %q)", dataDir, p)
		}
		return nil, fmt.Errorf("flock %q: %w", p, err)
	}
	return &fileLock{f: f}, nil
}

// Release drops the lock and closes the file. Idempotent.
func (l *fileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}
