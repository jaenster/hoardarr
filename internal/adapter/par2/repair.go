package par2

// Repair: given parsed PAR2 metadata plus the on-disk data files,
// locate damaged slices, solve the Reed-Solomon system, and write
// reconstructed bytes back into the right files at the right offsets.
//
// The function is best-effort: files that already match their IFSC
// checksums are left alone; files with damage that can be repaired
// are corrected in place; files with damage that exceeds the available
// recovery slices are reported as failed.

import (
	"bufio"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
)

// RepairInput is what Repair needs to do its job.
type RepairInput struct {
	// Par2Paths are paths to .par2 / .vol*.par2 files for the
	// recovery set. Repair re-parses them so that fresh fixtures
	// from M3a's earlier verify pass don't need to be threaded
	// through here.
	Par2Paths []string

	// DataPaths maps logical filename (as declared in FileDesc.Name)
	// to its on-disk path. Files declared in PAR2 but absent here are
	// treated as entirely missing.
	DataPaths map[string]string
}

// RepairResult is the outcome of a Repair pass.
type RepairResult struct {
	Repaired  []RepairedFile
	AlreadyOK []string
	Failed    []FailedFile
}

// RepairedFile names a file and the slice indices reconstructed.
type RepairedFile struct {
	Filename       string
	SlicesRepaired []int
}

// FailedFile names a file and explains why repair failed.
type FailedFile struct {
	Filename string
	Reason   string
}

// ErrUnrecoverableSet is returned when the total number of damaged
// slices across all files exceeds the available recovery slice count.
var ErrUnrecoverableSet = errors.New("par2: not enough recovery slices for the set")

// fileState is the per-file repair workspace.
type fileState struct {
	f        *ParFile
	path     string
	onDisk   bool
	slices   [][]byte // sliceSize each; nil entries are placeholders for missing slices
	missing  []int    // local slice indices that failed IFSC or are absent
}

// Repair runs the repair flow.
func Repair(_ context.Context, in RepairInput) (RepairResult, error) {
	set, err := parseFiles(in.Par2Paths)
	if err != nil {
		return RepairResult{}, fmt.Errorf("parse par2: %w", err)
	}
	if set.SliceSize == 0 {
		return RepairResult{}, errors.New("par2: missing Main packet (no slice size)")
	}
	if set.SliceSize%2 != 0 {
		return RepairResult{}, fmt.Errorf("par2: odd slice size %d", set.SliceSize)
	}
	sliceSize := int(set.SliceSize)

	// Order files by the Main packet's RecoveryFiles list. Recovery-set
	// global slice indices count up from 0 in that order, so the RS
	// math depends on it.
	ordered := make([]*ParFile, 0, len(set.RecoveryFiles))
	for _, id := range set.RecoveryFiles {
		f := set.fileByID(id)
		if f == nil || f.Name == "" {
			continue
		}
		ordered = append(ordered, f)
	}

	// Build an MD5-of-first-16KB index over DataPaths so we can match
	// obfuscated releases where the NZB filename and the PAR2-recorded
	// filename are different (two independent obfuscation layers).
	// PAR2's FileDesc stores MD516k for exactly this purpose.
	md5Index := buildMD516kIndex(in.DataPaths)

	// Diagnostic: emit the PAR2 set vs DataPaths mapping so we can see
	// which way the filenames diverge when repair fails. One line per
	// repair invocation; not in the hot path.
	logRepairFilenames(set, ordered, in.DataPaths)

	// Per-file analysis.
	states := make([]*fileState, 0, len(ordered))
	totalSlices := 0
	for _, f := range ordered {
		st := &fileState{f: f}
		path, ok := in.DataPaths[f.Name]
		if !ok {
			// Filename miss — try matching by MD5 of the first 16 KB
			// (PAR2 records this as FileDesc.MD516k specifically so
			// repair can survive renames). Obfuscated releases routinely
			// ship with NZB-level filenames different from the PAR2-
			// recorded ones (two independent obfuscation layers), and
			// SABnzbd handles them the same way.
			if p, ok2 := md5Index[f.MD516k]; ok2 {
				slog.Info("par2: filename mismatch resolved by MD5",
					"par2_name", f.Name,
					"matched_path", p,
				)
				path = p
				ok = true
			}
		}
		if !ok {
			// File completely absent on disk — every slice missing.
			slog.Warn("par2: file in recovery set not in DataPaths; treating all slices as missing",
				"par2_name", f.Name,
				"par2_name_bytes", []byte(f.Name),
				"slices", len(f.Slices),
				"datapath_keys", sortedKeys(in.DataPaths),
			)
			st.slices = make([][]byte, len(f.Slices))
			for i := range f.Slices {
				st.missing = append(st.missing, i)
			}
			states = append(states, st)
			totalSlices += len(f.Slices)
			continue
		}
		st.path = path
		st.onDisk = true
		slicesData, missing, err := analyseFileSlices(path, f, sliceSize)
		if err != nil {
			// I/O error: treat the whole file as missing.
			slog.Warn("par2: file slice analysis failed; treating all slices as missing",
				"par2_name", f.Name,
				"path", path,
				"err", err.Error(),
			)
			st.onDisk = false
			st.slices = make([][]byte, len(f.Slices))
			for i := range f.Slices {
				st.missing = append(st.missing, i)
			}
			states = append(states, st)
			totalSlices += len(f.Slices)
			continue
		}
		st.slices = slicesData
		st.missing = missing
		states = append(states, st)
		totalSlices += len(f.Slices)
	}

	// Compose global present + missing arrays in canonical order.
	globalPresent := make([][]byte, totalSlices)
	var globalMissing []int
	offset := 0
	for _, st := range states {
		for i, s := range st.slices {
			gIdx := offset + i
			if isLocalMissing(st.missing, i) {
				globalMissing = append(globalMissing, gIdx)
			} else {
				globalPresent[gIdx] = s
			}
		}
		offset += len(st.slices)
	}

	result := RepairResult{}

	if len(globalMissing) == 0 {
		for _, st := range states {
			result.AlreadyOK = append(result.AlreadyOK, st.f.Name)
		}
		return result, nil
	}

	damaged := damagedSet(states)

	if len(set.RecoverySlices) < len(globalMissing) {
		for _, st := range states {
			if damaged[st.f.Name] {
				result.Failed = append(result.Failed, FailedFile{
					Filename: st.f.Name,
					Reason:   "insufficient recovery slices",
				})
			} else {
				result.AlreadyOK = append(result.AlreadyOK, st.f.Name)
			}
		}
		return result, ErrUnrecoverableSet
	}

	reconstructed, err := Reconstruct(ReconstructInput{
		N:          totalSlices,
		SliceSize:  sliceSize,
		Present:    globalPresent,
		MissingIdx: globalMissing,
		Recovery:   set.RecoverySlices,
	})
	if err != nil {
		for _, st := range states {
			if damaged[st.f.Name] {
				result.Failed = append(result.Failed, FailedFile{
					Filename: st.f.Name,
					Reason:   err.Error(),
				})
			} else {
				result.AlreadyOK = append(result.AlreadyOK, st.f.Name)
			}
		}
		return result, err
	}

	if err := applyReconstructed(states, globalMissing, reconstructed, sliceSize, in.DataPaths); err != nil {
		return result, fmt.Errorf("apply: %w", err)
	}

	// Final whole-file MD5 verification post-repair.
	for _, st := range states {
		if !damaged[st.f.Name] {
			result.AlreadyOK = append(result.AlreadyOK, st.f.Name)
			continue
		}
		path := st.path
		if path == "" {
			path = in.DataPaths[st.f.Name]
		}
		if path == "" {
			result.Failed = append(result.Failed, FailedFile{
				Filename: st.f.Name,
				Reason:   "no destination path for file",
			})
			continue
		}
		ok2, reason := verifyFile(path, st.f.MD5, st.f.Size)
		if !ok2 {
			result.Failed = append(result.Failed, FailedFile{
				Filename: st.f.Name,
				Reason:   "post-repair md5: " + reason,
			})
			continue
		}
		result.Repaired = append(result.Repaired, RepairedFile{
			Filename:       st.f.Name,
			SlicesRepaired: st.missing,
		})
	}
	return result, nil
}

// analyseFileSlices reads the file at path slice-by-slice, checks each
// against the IFSC MD5, and returns (slice bytes, missing local indices).
// On short read of the final slice we zero-pad to sliceSize because
// that's what IFSC was computed against.
func analyseFileSlices(path string, pf *ParFile, sliceSize int) ([][]byte, []int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	out := make([][]byte, len(pf.Slices))
	var missing []int
	for i, sc := range pf.Slices {
		buf := make([]byte, sliceSize)
		n, err := io.ReadFull(br, buf)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			if n < sliceSize {
				for j := n; j < sliceSize; j++ {
					buf[j] = 0
				}
			}
		} else if err != nil {
			return nil, nil, err
		}
		sum := md5.Sum(buf)
		if sum != sc.MD5 {
			missing = append(missing, i)
		}
		out[i] = buf
	}
	return out, missing, nil
}

// applyReconstructed maps each reconstructed slice back to its (file,
// local-index) location and writes the file contents to disk.
//
// Writing strategy:
//   - For each damaged file, splice the reconstructed slice bytes
//     into the in-memory `slices` array, then concatenate and write
//     the result truncated to the original file size.
//   - If the file didn't exist on disk we create its parent directory
//     before opening for write — typical when a whole file was lost.
func applyReconstructed(states []*fileState, missingGlobal []int, recon [][]byte, sliceSize int, paths map[string]string) error {
	// Build (globalIdx → file state, localIdx) inverse map.
	type loc struct {
		st  *fileState
		idx int
	}
	locByGlobal := make(map[int]loc)
	offset := 0
	for _, st := range states {
		for i := range st.slices {
			locByGlobal[offset+i] = loc{st: st, idx: i}
		}
		offset += len(st.slices)
	}

	// Slot reconstructed slices back into their file state.
	for k, g := range missingGlobal {
		l := locByGlobal[g]
		l.st.slices[l.idx] = recon[k]
	}

	// Write each damaged file. The IFSC pass populated st.missing.
	for _, st := range states {
		if len(st.missing) == 0 {
			continue
		}
		// Prefer st.path (resolved during analysis — may have come
		// from the MD5-fallback when filenames don't match) over the
		// raw paths[f.Name] lookup that only knows literal names.
		path := st.path
		if path == "" {
			path = paths[st.f.Name]
		}
		if path == "" {
			return fmt.Errorf("no path for damaged file %q", st.f.Name)
		}
		// Assemble file bytes; truncate to the declared file size so
		// zero-padding from the last slice doesn't bloat the output.
		total := int(st.f.Size)
		if total == 0 {
			total = sliceSize * len(st.slices)
		}
		assembled := make([]byte, 0, total)
		for _, s := range st.slices {
			assembled = append(assembled, s...)
		}
		if total < len(assembled) {
			assembled = assembled[:total]
		}
		// Ensure parent dir exists (file may have been totally absent).
		if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
			return fmt.Errorf("mkdir for %s: %w", path, err)
		}
		if err := os.WriteFile(path, assembled, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return nil
}

func damagedSet(states []*fileState) map[string]bool {
	out := make(map[string]bool, len(states))
	for _, st := range states {
		if len(st.missing) > 0 {
			out[st.f.Name] = true
		}
	}
	return out
}

func isLocalMissing(missing []int, i int) bool {
	for _, m := range missing {
		if m == i {
			return true
		}
	}
	return false
}

// dirOf returns filepath.Dir(p) without importing path/filepath at
// package scope. We have filepath imported from par2.go.
func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[:i]
		}
	}
	return "."
}

// logRepairFilenames emits a single INFO line summarising the PAR2
// recovery set and the on-disk DataPaths so operators can spot
// filename divergence at a glance. Skipped FileDesc rows (id with no
// name) are reported separately — they usually mean a .vol* file we
// didn't fully parse.
func logRepairFilenames(set *RecoverySet, ordered []*ParFile, dataPaths map[string]string) {
	names := make([]string, 0, len(ordered))
	for _, f := range ordered {
		names = append(names, f.Name)
	}
	sort.Strings(names)

	missingFromData := make([]string, 0)
	for _, n := range names {
		if _, ok := dataPaths[n]; !ok {
			missingFromData = append(missingFromData, n)
		}
	}

	slog.Info("par2: repair set vs disk",
		"recovery_files", len(set.RecoveryFiles),
		"par_names", names,
		"datapath_keys", sortedKeys(dataPaths),
		"par_names_missing_from_disk", missingFromData,
		"slice_size", set.SliceSize,
		"recovery_slices", len(set.RecoverySlices),
	)
}

// sortedKeys returns the map keys in sorted order so log output is
// reproducible.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// buildMD516kIndex computes MD5 of the first 16 KB of every file in
// dataPaths and returns a map keyed by that digest. Used as a
// content-addressed fallback when PAR2 FileDesc names don't match
// our NZB-derived filenames (obfuscated releases — see logs in repair.go
// for the canonical "filename mismatch resolved by MD5" line).
//
// Files that fail to read or are smaller than 16 KB use the available
// bytes (matching the PAR2 spec — MD516k = MD5 of first 16384 bytes OR
// the entire file if smaller). Unreadable files are skipped silently:
// they'll just stay unmatched and get flagged elsewhere.
//
// Hashing every data file up-front sounds expensive but 16 KB per file
// is trivial next to PAR2 verification + Reed-Solomon reconstruction.
func buildMD516kIndex(dataPaths map[string]string) map[[16]byte]string {
	out := make(map[[16]byte]string, len(dataPaths))
	for _, p := range dataPaths {
		h, err := md5First16k(p)
		if err != nil {
			continue
		}
		// First-wins on collisions: vanishingly unlikely for unrelated
		// files, possible for byte-identical duplicates. Either way one
		// path is enough.
		if _, exists := out[h]; !exists {
			out[h] = p
		}
	}
	return out
}

// md5First16k computes MD5 of the first 16 KB of path (or the entire
// file if smaller).
func md5First16k(path string) ([16]byte, error) {
	var zero [16]byte
	f, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	defer f.Close()
	buf := make([]byte, 16384)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return zero, err
	}
	return md5.Sum(buf[:n]), nil
}
