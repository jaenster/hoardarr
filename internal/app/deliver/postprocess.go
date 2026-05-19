package deliver

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// minRenameSize is the floor on a "largest data file" for the
	// deobfuscation fallback rename. Smaller files (sidecars, metadata,
	// even short clips) aren't load-bearing enough to risk renaming.
	minRenameSize = 10 * 1024 * 1024 // 10 MiB

	// renameRatio: the largest file must be at least 3x the second
	// largest. If a release has multiple roughly-equal data files
	// (episode pack), renaming the biggest to the job name would
	// either collide with siblings or destroy episode-level naming.
	renameRatio = 3.0
)

// sampleRe matches sample/proof clips that SABnzbd's remove_samples
// strips out at the end of post-processing. Anchored to a leading
// non-word or start-of-string boundary so "examplefoo.mkv" doesn't trip
// it but "sample.mkv", "movie-sample.mkv", and "proof.mkv" do.
var sampleRe = regexp.MustCompile(`(?i)((^|[\W_])(sample|proof))`)

// dirEntry pairs a filesystem path with its size, used by the
// deobfuscation rename to find the dominant data file.
type dirEntry struct {
	path string
	size int64
}

// listDataFiles walks dir and returns all regular files that aren't
// excluded extensions. The largest-file logic only cares about real
// content, not parity/disc-metadata/subtitles.
func listDataFiles(dir string) ([]dirEntry, error) {
	var out []dirEntry
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if isExcludedExt(d.Name()) {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		out = append(out, dirEntry{path: path, size: fi.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// par2SetName extracts the release-name prefix that a PAR2 set's
// filenames share. Given e.g. ["Chicago.Med.S11E21.XviD-AFG.par2",
// "Chicago.Med.S11E21.XviD-AFG.vol-01.par2", ...] this returns
// "Chicago.Med.S11E21.XviD-AFG".
//
// Obfuscated releases routinely have an obfuscated NZB-level name
// (the bot that posted them strips meaningful text from the file
// names of the binary parts) BUT keep the canonical release name in
// the PAR2 set filenames — the latter encode the recovery-set name
// the producer used at par2create time, which is the human label.
// SAB exploits this; we do too. Falls back to "" if the input list
// has no .par2 entries or their prefixes don't agree.
func par2SetName(par2Filenames []string) string {
	var names []string
	for _, n := range par2Filenames {
		s := strings.TrimSuffix(strings.ToLower(filepath.Base(n)), ".par2")
		// Strip the SAB-style .vol-NN[-MM]/.volNN+MM suffixes.
		s = trimVolSuffix(s)
		if s == "" {
			continue
		}
		names = append(names, s)
	}
	if len(names) == 0 {
		return ""
	}
	// All entries must agree, else we don't trust the inference.
	prefix := names[0]
	for _, n := range names[1:] {
		if n != prefix {
			return ""
		}
	}
	// Return with original case if possible — find a par2Filename
	// whose lowered+trimmed form matches prefix and return its
	// pre-lower base name minus the suffix.
	for _, n := range par2Filenames {
		base := strings.TrimSuffix(filepath.Base(n), ".par2")
		baseTrim := trimVolSuffix(base)
		if strings.ToLower(baseTrim) == prefix {
			return baseTrim
		}
	}
	return prefix
}

// volSuffixRe matches every PAR2 recovery-vol naming convention we've
// seen in the wild: "foo.vol000+01", "foo.vol000-001", "foo.vol-01",
// "foo.vol01". Strips them so the leading file basename surfaces.
var volSuffixRe = regexp.MustCompile(`\.vol\d+([+-]\d+)?$|\.vol-?\d+$`)

func trimVolSuffix(s string) string {
	return volSuffixRe.ReplaceAllString(s, "")
}

// deobfuscateRename is the safety net for releases that land with an
// obfuscated largest file (e.g. 32-hex name). Picks the best
// human-readable target name and renames the largest data file to
// it, guarded by:
//
//   - excluded extensions (sample / proof / archive parts skipped)
//   - disc-structure detection (VIDEO_TS dirs left alone)
//   - 10 MiB minimum size (sidecars never get renamed)
//   - 3x ratio over second-largest (multi-data-file releases pass through)
//   - obfuscation heuristic on the current name (hand-named files pass)
//
// Name preference, in order: the PAR2 set name (if not itself
// obfuscated), then the NZB-derived job name. Obfuscated releases
// frequently have an obfuscated NZB-level name AND obfuscated data
// filenames, but keep the canonical release name in their
// "<release>.vol-NN.par2" filenames — that's the most reliable
// label when present. SAB exploits this; we do too.
//
// Returns the new path if a rename happened (for logging), or "" if
// no rename was warranted.
func deobfuscateRename(dir, jobName, parSetName string, logger *slog.Logger) (string, error) {
	if isDiscStructure(dir) {
		logger.Debug("deobfuscate: disc structure detected, skipping", "dir", dir)
		return "", nil
	}
	entries, err := listDataFiles(dir)
	if err != nil {
		return "", fmt.Errorf("list data files: %w", err)
	}
	if len(entries) == 0 {
		return "", nil
	}

	largest, secondLargest := entries[0], dirEntry{}
	for _, e := range entries[1:] {
		switch {
		case e.size > largest.size:
			secondLargest = largest
			largest = e
		case e.size > secondLargest.size:
			secondLargest = e
		}
	}

	if largest.size < minRenameSize {
		logger.Debug("deobfuscate: largest under min size, skipping",
			"dir", dir, "largest", filepath.Base(largest.path), "size", largest.size)
		return "", nil
	}
	if isExcludedExt(largest.path) {
		logger.Debug("deobfuscate: largest has excluded ext, skipping",
			"dir", dir, "largest", filepath.Base(largest.path))
		return "", nil
	}
	if secondLargest.size > 0 {
		ratio := float64(largest.size) / float64(secondLargest.size)
		if ratio < renameRatio {
			logger.Debug("deobfuscate: multiple comparable files, skipping",
				"dir", dir, "largest", filepath.Base(largest.path),
				"second", filepath.Base(secondLargest.path), "ratio", ratio)
			return "", nil
		}
	}
	if !isProbablyObfuscated(filepath.Base(largest.path)) {
		logger.Debug("deobfuscate: largest looks hand-named, skipping",
			"dir", dir, "largest", filepath.Base(largest.path))
		return "", nil
	}

	// Pick the rename target: PAR2 set name (more reliable on
	// obfuscated releases) wins over the NZB-derived job name as
	// long as it's a recognisable hand-shaped name itself.
	targetName := jobName
	if parSetName != "" && !isProbablyObfuscated(parSetName) {
		targetName = parSetName
	}

	ext := filepath.Ext(largest.path)
	target := filepath.Join(filepath.Dir(largest.path), sanitizeFilename(targetName)+ext)
	if target == largest.path {
		return "", nil
	}
	if _, err := os.Stat(target); err == nil {
		logger.Warn("deobfuscate: target name already exists, skipping",
			"dir", dir, "target", filepath.Base(target))
		return "", nil
	}
	if err := os.Rename(largest.path, target); err != nil {
		return "", fmt.Errorf("rename %s → %s: %w",
			filepath.Base(largest.path), filepath.Base(target), err)
	}
	logger.Info("deobfuscate: renamed obfuscated file",
		"dir", dir, "from", filepath.Base(largest.path), "to", filepath.Base(target))
	return target, nil
}

// removeSamples walks dir and deletes anything matching SABnzbd's
// sample/proof regex. The safety check is the SAB-borrowed
// false-positive guard: if 100% of files match (e.g. a release
// genuinely named "sample-pack"), skip removal entirely.
//
// Sub-directory walking is intentional — sample clips sometimes live
// in their own folder (Sample/sample.mkv).
func removeSamples(dir string, logger *slog.Logger) error {
	var all []string
	var matches []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		all = append(all, path)
		// Match against the path relative to dir so files inside a
		// folder named "Sample" get caught even when the file itself
		// has a neutral name (Sample/preview.mkv).
		rel, _ := filepath.Rel(dir, path)
		if sampleRe.MatchString(rel) {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk for samples: %w", err)
	}
	if len(matches) == 0 {
		return nil
	}
	if len(matches) == len(all) {
		logger.Info("samples: all files matched sample regex, refusing to nuke release",
			"dir", dir, "count", len(matches))
		return nil
	}
	for _, p := range matches {
		if err := os.Remove(p); err != nil {
			logger.Warn("samples: remove failed", "file", p, "err", err)
			continue
		}
		logger.Info("samples: removed", "file", filepath.Base(p))
	}
	// Sweep any sub-directories that became empty by the removal
	// (common for the Sample/ pattern).
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == dir {
			return nil
		}
		entries, err := os.ReadDir(path)
		if err == nil && len(entries) == 0 {
			_ = os.Remove(path)
		}
		return nil
	})
	return nil
}

// collapseSingleFolder lifts dir/inner/... up to dir/... when dir
// contains exactly one sub-directory and no files at its top level.
// Cleans up the redundant outer wrap that some releases ship inside
// the NZB. Skips silently when the structure doesn't match.
//
// PAR2 leftover files are not present at this stage (the deliver loop
// already skipped them). The check here is just "single subdir, no
// top-level files".
func collapseSingleFolder(dir string, logger *slog.Logger) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read top: %w", err)
	}
	var subdir string
	for _, e := range entries {
		if e.IsDir() {
			if subdir != "" {
				return nil
			}
			subdir = e.Name()
			continue
		}
		// Any top-level file disqualifies the collapse.
		return nil
	}
	if subdir == "" {
		return nil
	}
	innerPath := filepath.Join(dir, subdir)
	inner, err := os.ReadDir(innerPath)
	if err != nil {
		return fmt.Errorf("read inner: %w", err)
	}
	for _, e := range inner {
		src := filepath.Join(innerPath, e.Name())
		dst := filepath.Join(dir, e.Name())
		if _, err := os.Stat(dst); err == nil {
			// Name collision (the inner has a file that matches the
			// subdir name itself). Bail rather than overwrite.
			logger.Warn("collapse: target name collision, skipping",
				"dir", dir, "name", e.Name())
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat target: %w", err)
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("collapse rename %s: %w", e.Name(), err)
		}
	}
	if err := os.Remove(innerPath); err != nil {
		logger.Warn("collapse: remove inner failed", "dir", innerPath, "err", err)
		return nil
	}
	logger.Info("collapse: flattened single-folder release",
		"dir", dir, "inner", subdir, "files", len(inner))
	return nil
}
