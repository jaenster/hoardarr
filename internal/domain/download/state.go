package download

// JobState is the lifecycle of a download job.
//
// Forward edges (M1 scope, no verify/repair yet):
//
//	queued ─▶ downloading ─▶ download_complete ─▶ completed
//	   │           │                    │
//	   │           ▼                    ▼
//	   ▼        paused              failed
//	aborted
//
// Verify/repair/unpack states are reserved for M3-M4. The aggregate
// already accepts them so orchestrator code lands without churn.
type JobState string

const (
	JobStateQueued           JobState = "queued"
	JobStateDownloading      JobState = "downloading"
	JobStatePaused           JobState = "paused"
	JobStateDownloadComplete JobState = "download_complete"
	JobStateVerifying        JobState = "verifying"
	JobStateRepairing        JobState = "repairing"
	JobStateUnpacking        JobState = "unpacking"
	JobStateCompleted        JobState = "completed"
	JobStateFailed           JobState = "failed"
	JobStateAborted          JobState = "aborted"
)

// IsActive returns true for states where downloading or post-processing
// is in progress.
func (s JobState) IsActive() bool {
	switch s {
	case JobStateQueued, JobStateDownloading, JobStateVerifying, JobStateRepairing, JobStateUnpacking:
		return true
	default:
		return false
	}
}

// IsTerminal returns true for states from which no further transitions
// are expected.
func (s JobState) IsTerminal() bool {
	switch s {
	case JobStateCompleted, JobStateFailed, JobStateAborted:
		return true
	default:
		return false
	}
}

// FileState is the lifecycle of a single file within a Job.
type FileState string

const (
	FileStatePending     FileState = "pending"
	FileStateDownloading FileState = "downloading"
	FileStateComplete    FileState = "complete"
	FileStateFailed      FileState = "failed"
)

// SegmentState is the lifecycle of a single segment (one Usenet article).
type SegmentState string

const (
	// SegmentStatePending — not yet dispatched. Default state.
	SegmentStatePending SegmentState = "pending"

	// SegmentStateInflight — handed to a worker; awaiting fetch result.
	// On orchestrator restart, in-flight segments are reset to pending.
	SegmentStateInflight SegmentState = "inflight"

	// SegmentStateDone — fetched, decoded, written to disk.
	SegmentStateDone SegmentState = "done"

	// SegmentStateMissing — every configured server returned 430. The
	// segment is unrecoverable from Usenet; PAR2 may rescue the file.
	SegmentStateMissing SegmentState = "missing"

	// SegmentStateFailed — non-430 errors exhausted the retry budget
	// (network, parse, CRC mismatch, ...).
	SegmentStateFailed SegmentState = "failed"
)

// IsTerminal reports whether the segment state will not change again.
func (s SegmentState) IsTerminal() bool {
	switch s {
	case SegmentStateDone, SegmentStateMissing, SegmentStateFailed:
		return true
	default:
		return false
	}
}
