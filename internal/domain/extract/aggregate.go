// Package extract owns the bounded context responsible for unpacking
// archive jobs (RAR today; ZIP/7z later if anyone asks). The aggregate
// is durable so a crash mid-extract resumes cleanly: an existing row
// in `extracting` state on startup tells us to retry rather than
// re-deliver.
package extract

import (
	"errors"
	"strconv"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// ExtractID identifies an Extract aggregate (one per Job).
type ExtractID int64

// State is the lifecycle of an Extract.
type State string

const (
	StatePending    State = "pending"
	StateExtracting State = "extracting"
	StateComplete   State = "complete"
	StateFailed     State = "failed"
)

// Extract is the aggregate root.
type Extract struct {
	id        ExtractID
	jobID     download.JobID
	state     State
	targetDir string
	errMsg    string

	createdAt  time.Time
	startedAt  time.Time
	finishedAt time.Time

	events []event.Event
}

// NewParams is the input to New.
type NewParams struct {
	JobID     download.JobID
	TargetDir string
}

// New builds a fresh pending Extract and emits ExtractQueued.
func New(p NewParams, now time.Time) *Extract {
	x := &Extract{
		jobID:     p.JobID,
		state:     StatePending,
		targetDir: p.TargetDir,
		createdAt: now,
	}
	x.events = append(x.events, ExtractQueued{
		JobID: p.JobID, TargetDir: p.TargetDir, At: now,
	})
	return x
}

// HydrateParams is what the repository hands back when loading a row.
type HydrateParams struct {
	ID         ExtractID
	JobID      download.JobID
	State      State
	TargetDir  string
	ErrMsg     string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

// Hydrate reconstructs without emitting events.
func Hydrate(p HydrateParams) *Extract {
	return &Extract{
		id:         p.ID,
		jobID:      p.JobID,
		state:      p.State,
		targetDir:  p.TargetDir,
		errMsg:     p.ErrMsg,
		createdAt:  p.CreatedAt,
		startedAt:  p.StartedAt,
		finishedAt: p.FinishedAt,
	}
}

// Accessors.
func (x *Extract) ID() ExtractID         { return x.id }
func (x *Extract) JobID() download.JobID { return x.jobID }
func (x *Extract) State() State          { return x.state }
func (x *Extract) TargetDir() string     { return x.targetDir }
func (x *Extract) ErrMsg() string        { return x.errMsg }
func (x *Extract) CreatedAt() time.Time  { return x.createdAt }
func (x *Extract) StartedAt() time.Time  { return x.startedAt }
func (x *Extract) FinishedAt() time.Time { return x.finishedAt }

// SetID is called by the repository after INSERT.
func (x *Extract) SetID(id ExtractID) {
	x.id = id
	for i := range x.events {
		if q, ok := x.events[i].(ExtractQueued); ok && q.ID == 0 {
			q.ID = id
			x.events[i] = q
		}
	}
}

// PullEvents drains the buffered events.
func (x *Extract) PullEvents() []event.Event {
	out := x.events
	x.events = nil
	return out
}

// Start: pending → extracting.
func (x *Extract) Start(now time.Time) error {
	if x.state != StatePending {
		return errors.New("extract: Start requires pending state")
	}
	x.state = StateExtracting
	x.startedAt = now
	x.events = append(x.events, ExtractStarted{
		ID: x.id, JobID: x.jobID, At: now,
	})
	return nil
}

// Complete: extracting → complete.
func (x *Extract) Complete(now time.Time) error {
	if x.state != StateExtracting {
		return errors.New("extract: Complete requires extracting state")
	}
	x.state = StateComplete
	x.finishedAt = now
	x.events = append(x.events, ExtractComplete{
		ID: x.id, JobID: x.jobID, TargetDir: x.targetDir, At: now,
	})
	return nil
}

// Fail: pending|extracting → failed.
func (x *Extract) Fail(reason string, now time.Time) error {
	if x.state != StatePending && x.state != StateExtracting {
		return errors.New("extract: Fail requires pending or extracting state")
	}
	x.state = StateFailed
	x.errMsg = reason
	x.finishedAt = now
	x.events = append(x.events, ExtractFailed{
		ID: x.id, JobID: x.jobID, Err: reason, At: now,
	})
	return nil
}

func aggID(id ExtractID) string {
	return strconv.FormatInt(int64(id), 10)
}
