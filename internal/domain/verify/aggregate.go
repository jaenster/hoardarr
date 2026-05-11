package verify

import (
	"errors"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// VerifySetID identifies a VerifySet aggregate.
type VerifySetID int64

// VerifySet is the aggregate root: one verification run for one Job.
//
// The aggregate holds a coarse summary (state + failed-file list);
// the detailed PAR2 metadata stays in the adapter layer (par2.RecoverySet)
// and isn't serialised through the aggregate. The aggregate's job is
// to gate state transitions and emit events; the heavy lifting is in
// the application service.
type VerifySet struct {
	id          VerifySetID
	jobID       download.JobID
	state       VerifyState
	startedAt   time.Time
	finishedAt  time.Time
	errorMsg    string
	failedFiles []string // filenames that didn't match (RepairNeeded set)

	events []event.Event
}

// NewVerifySetParams gathers inputs to construct a fresh VerifySet.
type NewVerifySetParams struct {
	JobID download.JobID
}

// NewVerifySet creates a VerifySet in pending state. No event is
// emitted at construction time; VerifyStarted fires when the worker
// transitions to verifying via MarkStarted.
func NewVerifySet(p NewVerifySetParams) *VerifySet {
	if p.JobID == 0 {
		// Caller error — use a sentinel so it's obvious in tests.
		panic("verify: NewVerifySet requires JobID")
	}
	return &VerifySet{
		jobID: p.JobID,
		state: VerifyStatePending,
	}
}

// HydrateParams is the snapshot the repository hands back when loading.
type HydrateParams struct {
	ID          VerifySetID
	JobID       download.JobID
	State       VerifyState
	StartedAt   time.Time
	FinishedAt  time.Time
	ErrorMsg    string
	FailedFiles []string
}

// Hydrate reconstructs a VerifySet from persistence with no events.
func Hydrate(p HydrateParams) *VerifySet {
	return &VerifySet{
		id:          p.ID,
		jobID:       p.JobID,
		state:       p.State,
		startedAt:   p.StartedAt,
		finishedAt:  p.FinishedAt,
		errorMsg:    p.ErrorMsg,
		failedFiles: append([]string(nil), p.FailedFiles...),
	}
}

// Accessors.
func (v *VerifySet) ID() VerifySetID         { return v.id }
func (v *VerifySet) JobID() download.JobID   { return v.jobID }
func (v *VerifySet) State() VerifyState      { return v.state }
func (v *VerifySet) StartedAt() time.Time    { return v.startedAt }
func (v *VerifySet) FinishedAt() time.Time   { return v.finishedAt }
func (v *VerifySet) ErrorMsg() string        { return v.errorMsg }
func (v *VerifySet) FailedFiles() []string   { return append([]string(nil), v.failedFiles...) }

// SetID assigns a database id after a successful insert.
func (v *VerifySet) SetID(id VerifySetID) { v.id = id }

// MarkStarted transitions pending → verifying. No-op for any other
// state (idempotent on retry).
func (v *VerifySet) MarkStarted(now time.Time) {
	if v.state != VerifyStatePending {
		return
	}
	v.state = VerifyStateVerifying
	v.startedAt = now
	v.events = append(v.events, VerifyStarted{ID: v.id, JobID: v.jobID, At: now})
}

// MarkOK transitions verifying → ok. Emits VerifyOK.
func (v *VerifySet) MarkOK(now time.Time) error {
	if v.state.IsTerminal() {
		return errors.New("verify: already terminal")
	}
	v.state = VerifyStateOK
	v.finishedAt = now
	v.events = append(v.events, VerifyOK{ID: v.id, JobID: v.jobID, At: now})
	return nil
}

// MarkRepairNeeded transitions verifying → repair_needed. failedFiles
// lists the names whose checksums failed. M3b's repair worker
// subscribes to the emitted RepairNeeded event.
func (v *VerifySet) MarkRepairNeeded(failedFiles []string, now time.Time) error {
	if v.state.IsTerminal() {
		return errors.New("verify: already terminal")
	}
	v.state = VerifyStateRepairNeeded
	v.finishedAt = now
	v.failedFiles = append([]string(nil), failedFiles...)
	v.events = append(v.events, RepairNeeded{
		ID: v.id, JobID: v.jobID, FailedFiles: append([]string(nil), failedFiles...), At: now,
	})
	return nil
}

// MarkFailed transitions any state → failed (terminal). Used when
// PAR2 metadata is itself unreadable (e.g. all .par2 files missing
// or corrupt).
func (v *VerifySet) MarkFailed(reason string, now time.Time) error {
	if v.state == VerifyStateFailed {
		return nil
	}
	v.state = VerifyStateFailed
	v.errorMsg = reason
	v.finishedAt = now
	v.events = append(v.events, VerifyFailed{ID: v.id, JobID: v.jobID, Err: reason, At: now})
	return nil
}

// Reset transitions repair_needed → pending so a subsequent verify
// pass can run after the M3b repair worker has reconstructed damaged
// files. Returns an error from any other state to keep callers honest;
// nobody should reset an OK or Failed verify. No event is emitted —
// the verify service will emit VerifyStarted when it picks up.
func (v *VerifySet) Reset() error {
	if v.state != VerifyStateRepairNeeded {
		return errors.New("verify: Reset requires repair_needed state")
	}
	v.state = VerifyStatePending
	v.failedFiles = nil
	v.errorMsg = ""
	return nil
}

// PullEvents drains and returns the pending event list.
func (v *VerifySet) PullEvents() []event.Event {
	out := v.events
	v.events = nil
	return out
}
