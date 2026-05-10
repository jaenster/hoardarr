package verify

import (
	"context"
	"errors"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// Repository persists VerifySet aggregates.
type Repository interface {
	Save(ctx context.Context, v *VerifySet) error
	ByID(ctx context.Context, id VerifySetID) (*VerifySet, error)
	ByJobID(ctx context.Context, jobID download.JobID) (*VerifySet, error)
}

// ErrNotFound is returned by Repository methods when no row exists.
var ErrNotFound = errors.New("verify: not found")

// FileResult is the per-file outcome of a verify pass.
type FileResult struct {
	Filename string
	OK       bool
	// Reason is populated when OK is false.
	Reason string
}

// Result aggregates per-file outcomes.
type Result struct {
	Files []FileResult
}

// AllOK reports whether every file verified successfully.
func (r Result) AllOK() bool {
	for _, f := range r.Files {
		if !f.OK {
			return false
		}
	}
	return true
}

// FailedNames returns filenames that didn't verify, in iteration order.
func (r Result) FailedNames() []string {
	var out []string
	for _, f := range r.Files {
		if !f.OK {
			out = append(out, f.Filename)
		}
	}
	return out
}

// Verifier runs PAR2 verification.
//
// par2Paths are absolute paths to the .par2 metadata files (FileDesc
// and IFSC packets — the integrity reference data).
//
// dataPaths maps each data file's PAR2-declared filename to its on-
// disk path. The orchestrator names .tmp files by file ID, not by
// name, so the caller does the name → path mapping using the Job
// aggregate's Files() (matching by Filename()).
//
// Implementations live in adapter packages (internal/adapter/par2).
type Verifier interface {
	Verify(ctx context.Context, par2Paths []string, dataPaths map[string]string) (Result, error)
}
