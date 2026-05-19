// Package logfile is a small daily-rotating file writer for slog
// output. Sonarr keeps `sonarr.txt` (info) + `sonarr.debug.txt`
// (verbose), capped at ~1 MB per file, with rotation. We do the same
// in a simpler shape — one active file `hoardarr.log`, rotated when
// either the date crosses midnight or the file passes maxBytes.
// Rotated files are renamed to `hoardarr-YYYY-MM-DD-N.log`.
//
// This is intentionally minimal: no compression, no async I/O, no
// remote shipping. The loghub already serves the live tail use case
// over SSE; this exists so an operator can grab a fresh log file
// after the fact to attach to a bug report.
package logfile

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// activeName is the current open file. Always opened in append mode.
	activeName = "hoardarr.log"
	// maxBytes caps each rotated file to keep total disk usage bounded
	// and per-file download size reasonable. SAB's default is 1 MB; we
	// pick 8 MB as a compromise — fewer rotation events on a chatty
	// debug day without ballooning if the operator forgets to clean.
	maxBytes = 8 << 20 // 8 MiB
	// retain is the upper bound on how many rotated files we keep.
	// Older ones are deleted at rotation time.
	retain = 14
)

// Writer is an io.Writer that appends to <dir>/hoardarr.log and
// rotates when the date crosses or the current file exceeds maxBytes.
// Concurrent Write calls are serialised; rotation is opportunistic
// at write time so we don't need a background goroutine.
type Writer struct {
	dir string

	mu       sync.Mutex
	f        *os.File
	written  int64
	openedAt time.Time
}

// Open creates the dir if needed and opens the active log file.
// Subsequent Writes append. Caller must call Close on shutdown.
func Open(dir string) (*Writer, error) {
	if dir == "" {
		return nil, fmt.Errorf("logfile: dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("logfile: mkdir %q: %w", dir, err)
	}
	w := &Writer{dir: dir}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) openActive() error {
	path := filepath.Join(w.dir, activeName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("logfile: open %q: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("logfile: stat: %w", err)
	}
	w.f = f
	w.written = info.Size()
	w.openedAt = time.Now()
	return nil
}

// Write implements io.Writer. Rotates the current file before
// appending if either size or date threshold is crossed.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.shouldRotate() {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := w.f.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *Writer) shouldRotate() bool {
	if w.written >= maxBytes {
		return true
	}
	if !sameDay(w.openedAt, time.Now()) {
		return true
	}
	return false
}

func sameDay(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd
}

// rotate closes the current file, renames it to a timestamped name
// with a uniquifier suffix if needed, then opens a fresh active
// file. Old rotations beyond `retain` are deleted.
func (w *Writer) rotate() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("logfile: close active: %w", err)
	}
	stamp := w.openedAt.Format("2006-01-02")
	base := fmt.Sprintf("hoardarr-%s", stamp)
	target := filepath.Join(w.dir, base+".log")
	for i := 1; ; i++ {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			break
		}
		target = filepath.Join(w.dir, fmt.Sprintf("%s-%d.log", base, i))
	}
	if err := os.Rename(filepath.Join(w.dir, activeName), target); err != nil {
		// Don't fail the write; just complain and re-open. Worst
		// case the file keeps growing past maxBytes for one write.
		return fmt.Errorf("logfile: rename: %w", err)
	}
	w.pruneOld()
	return w.openActive()
}

func (w *Writer) pruneOld() {
	files := w.listRotated()
	if len(files) <= retain {
		return
	}
	// listRotated returns newest-first; remove the tail past retain.
	for _, f := range files[retain:] {
		_ = os.Remove(filepath.Join(w.dir, f))
	}
}

// listRotated returns rotated filenames (excluding the active one),
// newest-first. Filenames sort lexicographically because of the
// YYYY-MM-DD date stamp.
func (w *Writer) listRotated() []string {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if n == activeName {
			continue
		}
		if !strings.HasPrefix(n, "hoardarr-") || !strings.HasSuffix(n, ".log") {
			continue
		}
		names = append(names, n)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return names
}

// Close releases the active file handle.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// FileInfo is one entry in the list-files response.
type FileInfo struct {
	Name      string    `json:"name"`
	Size      int64     `json:"size_bytes"`
	UpdatedAt time.Time `json:"updated_at"`
	Active    bool      `json:"active"`
}

// List returns metadata for every log file in dir (active +
// rotated), newest-first. Active first, then rotated in date-desc.
func List(dir string) []FileInfo {
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]FileInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if n != activeName && (!strings.HasPrefix(n, "hoardarr-") || !strings.HasSuffix(n, ".log")) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, FileInfo{
			Name:      n,
			Size:      info.Size(),
			UpdatedAt: info.ModTime(),
			Active:    n == activeName,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		// Active first, then newest mtime.
		if out[i].Active != out[j].Active {
			return out[i].Active
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

// SafePath returns the absolute path inside dir for `name`, or an
// error if `name` looks like a traversal attempt. Use this to
// validate the path-parameter on the download endpoint.
func SafePath(dir, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("logfile: name required")
	}
	if name != activeName && (!strings.HasPrefix(name, "hoardarr-") || !strings.HasSuffix(name, ".log")) {
		return "", fmt.Errorf("logfile: %q is not a hoardarr log file", name)
	}
	clean := filepath.Clean(name)
	if clean != name || strings.ContainsAny(clean, `/\`) {
		return "", fmt.Errorf("logfile: traversal in %q", name)
	}
	return filepath.Join(dir, clean), nil
}
