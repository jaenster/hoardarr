// Package verify is the bounded context that owns post-download
// integrity checking via PAR2 (Parchive Volume 2).
//
// Aggregate root: VerifySet — ties one download.JobID to its parsed
// PAR2 metadata and the running verification state.
//
// State machine:
//
//	pending ─▶ verifying ─▶ ok
//	                  └────▶ repair_needed   (M3b picks this up)
//	                  └────▶ failed
//
// "ok" means every file's MD5 matched. "repair_needed" means the
// download is intact but at least one file's checksum disagrees;
// the M3b repair worker fetches more recovery slices and reconstructs.
// "failed" means we couldn't even read the par2 metadata or files —
// terminal.
package verify

// VerifyState is the lifecycle of a verification attempt for one Job.
type VerifyState string

const (
	VerifyStatePending      VerifyState = "pending"
	VerifyStateVerifying    VerifyState = "verifying"
	VerifyStateOK           VerifyState = "ok"
	VerifyStateRepairNeeded VerifyState = "repair_needed"
	VerifyStateFailed       VerifyState = "failed"
)

// IsTerminal reports whether the state will not change further.
func (s VerifyState) IsTerminal() bool {
	switch s {
	case VerifyStateOK, VerifyStateRepairNeeded, VerifyStateFailed:
		return true
	default:
		return false
	}
}
