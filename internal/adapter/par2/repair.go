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
	"os"
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

	// Per-file analysis.
	states := make([]*fileState, 0, len(ordered))
	totalSlices := 0
	for _, f := range ordered {
		st := &fileState{f: f}
		path, ok := in.DataPaths[f.Name]
		if !ok {
			// File completely absent on disk — every slice missing.
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
		path, ok := in.DataPaths[st.f.Name]
		if !ok {
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
		path := paths[st.f.Name]
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
