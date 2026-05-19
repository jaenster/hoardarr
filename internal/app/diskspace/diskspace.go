// Package diskspace surfaces filesystem free/total counts for the
// configured hoardarr paths. Sonarr-style "/diskspace" surface so the
// UI can render a per-path usage bar and the health checker can
// raise a warning when free space goes critical.
//
// Implementation is intentionally tiny — no aggregate, no events, no
// persistence. Caller asks for a snapshot; we run statfs(2) on each
// path and return. Snapshot is cheap (microseconds per path) so
// there's no caching layer.
package diskspace

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// Entry is one (path, free, total) tuple. Label is a human-facing
// name ("Incomplete", "Complete") for the UI so the operator
// doesn't have to translate the raw filesystem path themselves.
type Entry struct {
	Label      string `json:"label"`
	Path       string `json:"path"`
	FreeBytes  int64  `json:"free_bytes"`
	TotalBytes int64  `json:"total_bytes"`
	UsedBytes  int64  `json:"used_bytes"`
	Reachable  bool   `json:"reachable"`
	Error      string `json:"error,omitempty"`
}

// Source is the input set the snapshot iterates over. Construct it
// at bootstrap time from the resolved config paths; pass to Snapshot
// each tick / on each request.
type Source struct {
	Label string
	Path  string
}

// Snapshot returns one Entry per source. Unreachable paths return
// Reachable=false with the underlying error string — the UI renders
// these distinctly from "healthy but full".
func Snapshot(sources []Source) []Entry {
	out := make([]Entry, 0, len(sources))
	for _, s := range sources {
		if s.Path == "" {
			continue
		}
		out = append(out, statfs(s))
	}
	return out
}

func statfs(s Source) Entry {
	e := Entry{Label: s.Label, Path: s.Path}

	// Make sure the path even exists before reaching for syscalls — a
	// dangling bind-mount on the host shows up as ENOENT and confuses
	// the raw Statfs error message.
	if _, err := os.Stat(s.Path); err != nil {
		e.Error = err.Error()
		return e
	}

	var st syscall.Statfs_t
	if err := syscall.Statfs(s.Path, &st); err != nil {
		e.Error = fmt.Errorf("statfs %q: %w", s.Path, err).Error()
		return e
	}
	// Bsize is signed/unsigned varies by GOOS; cast through int64 to
	// keep the math + JSON shape platform-portable.
	bsize := int64(st.Bsize)
	if bsize <= 0 {
		e.Error = "statfs reports nonsensical block size"
		return e
	}
	total := int64(st.Blocks) * bsize
	free := int64(st.Bavail) * bsize
	if total < 0 || free < 0 {
		e.Error = errors.New("statfs overflow").Error()
		return e
	}
	e.Reachable = true
	e.TotalBytes = total
	e.FreeBytes = free
	if free <= total {
		e.UsedBytes = total - free
	}
	return e
}
