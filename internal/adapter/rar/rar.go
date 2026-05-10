// Package rar implements extract.Extractor over nwaples/rardecode/v2.
//
// rardecode handles both RAR3 (LZSS-like + filter VM) and RAR5
// (PPMd / LZSS / BLAKE2) and follows volume continuations
// transparently when given the first .rar file.
//
// Path-traversal defence: every entry name is sanitised with
// filepath.Clean and rejected if it escapes targetDir. RAR archives
// from untrusted sources occasionally contain "../" in entries, which
// without this check would let a hostile NZB write outside complete/.
package rar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nwaples/rardecode/v2"

	"github.com/jaenster/hoardarr/internal/domain/extract"
)

// Extractor is the rardecode-backed implementation of
// extract.Extractor. Stateless; safe to share.
type Extractor struct{}

// Compile-time port check.
var _ extract.Extractor = Extractor{}

// Extract reads archives at archivePaths (the first being the
// entry-point volume — `.rar` or `.part1.rar`) and writes contents
// into targetDir. Returns the relative paths of files written.
//
// rardecode's OpenReader uses the entry path as a base and finds
// sibling volumes itself, so we only need to pass the first one.
// We accept a slice for symmetry with future ZIP/7z extractors that
// may want explicit lists.
func (Extractor) Extract(ctx context.Context, archivePaths []string, targetDir string) ([]string, error) {
	if len(archivePaths) == 0 {
		return nil, errors.New("rar: no archive paths supplied")
	}
	entry := pickFirstVolume(archivePaths)

	rc, err := rardecode.OpenReader(entry)
	if err != nil {
		return nil, fmt.Errorf("rar: open %s: %w", entry, err)
	}
	defer rc.Close()

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, fmt.Errorf("rar: mkdir target: %w", err)
	}

	var written []string
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		hdr, err := rc.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return written, fmt.Errorf("rar: next entry: %w", err)
		}
		// Reject path traversal: clean → check it stays under targetDir.
		clean := filepath.Clean(hdr.Name)
		if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
			return written, fmt.Errorf("rar: entry %q escapes target", hdr.Name)
		}
		dst := filepath.Join(targetDir, clean)
		if !strings.HasPrefix(dst+string(os.PathSeparator), targetDir+string(os.PathSeparator)) &&
			dst != targetDir {
			return written, fmt.Errorf("rar: resolved path %q escapes target %q", dst, targetDir)
		}

		if hdr.IsDir {
			if err := os.MkdirAll(dst, 0o755); err != nil {
				return written, fmt.Errorf("rar: mkdir %s: %w", dst, err)
			}
			continue
		}

		// Make sure the parent dir exists for nested files.
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return written, fmt.Errorf("rar: mkdir parent %s: %w", filepath.Dir(dst), err)
		}

		f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return written, fmt.Errorf("rar: create %s: %w", dst, err)
		}
		// Stream from the archive reader. rardecode's Reader.Read returns
		// the current entry's contents until io.EOF (then Next() advances).
		if _, err := io.Copy(f, &rc.Reader); err != nil {
			_ = f.Close()
			return written, fmt.Errorf("rar: copy %s: %w", dst, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return written, fmt.Errorf("rar: fsync %s: %w", dst, err)
		}
		if err := f.Close(); err != nil {
			return written, fmt.Errorf("rar: close %s: %w", dst, err)
		}
		written = append(written, clean)
	}
	return written, nil
}

// pickFirstVolume picks the volume rardecode should open from a list
// of paths in arbitrary order. The picking rules:
//
//   - if a "*.part1.rar" exists, use it (RAR5 multi-part style)
//   - else if a "*.r00" sibling of any "*.rar" exists, prefer "*.rar"
//   - else lexicographically smallest "*.rar"
//   - else the first path
func pickFirstVolume(paths []string) string {
	if len(paths) == 1 {
		return paths[0]
	}
	cp := append([]string(nil), paths...)
	sort.Strings(cp)
	// Look for explicit part1.
	for _, p := range cp {
		low := strings.ToLower(filepath.Base(p))
		if strings.HasSuffix(low, ".part1.rar") || strings.HasSuffix(low, ".part01.rar") ||
			strings.HasSuffix(low, ".part001.rar") {
			return p
		}
	}
	// Otherwise the first .rar.
	for _, p := range cp {
		if strings.HasSuffix(strings.ToLower(p), ".rar") {
			return p
		}
	}
	return cp[0]
}
