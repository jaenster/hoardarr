// Package fs implements deliver.Filesystem for the local filesystem.
//
// The interesting bit is Move: it tries os.Rename first (atomic on
// the same filesystem) and falls back to copy + fsync + rename + unlink
// on EXDEV (cross-device link error). Cross-FS deliveries happen when
// users keep incomplete/ on a fast SSD and complete/ on a NAS mount.
package fs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Default is a Filesystem backed by the OS, with no extra knobs.
var Default = &osFS{}

type osFS struct{}

// Move src → dst. Creates the parent directory of dst if missing.
func (osFS) Move(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir parent of %s: %w", dst, err)
	}

	// Fast path: same filesystem → atomic rename.
	if err := os.Rename(src, dst); err == nil {
		return nil
	} else if !isCrossDevice(err) {
		return fmt.Errorf("rename %s → %s: %w", src, dst, err)
	}

	// Slow path: cross-filesystem. Copy + fsync + rename-into-place +
	// unlink the source. We rename the destination via a .partial
	// sibling so a crash mid-copy doesn't leave a partial file at the
	// final name.
	tmp := dst + ".partial"
	if err := copyFile(src, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s → %s: %w", tmp, dst, err)
	}
	if err := os.Remove(src); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unlink source %s: %w", src, err)
	}
	return nil
}

func (osFS) MkdirAll(path string) error {
	return os.MkdirAll(path, 0o755)
}

func (osFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// copyFile streams src → dst with a final fsync so the destination is
// durable before the caller renames it into place.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open src: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open dst: %w", err)
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return fmt.Errorf("fsync: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close dst: %w", err)
	}
	return nil
}

// isCrossDevice unwraps os.PathError / os.LinkError to spot EXDEV.
func isCrossDevice(err error) bool {
	var le *os.LinkError
	if errors.As(err, &le) {
		return errors.Is(le.Err, syscall.EXDEV)
	}
	return errors.Is(err, syscall.EXDEV)
}
