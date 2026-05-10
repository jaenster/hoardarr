// Package deliver owns the bounded context responsible for moving
// verified files from incomplete/ to complete/<category>/<release>/.
//
// Deliveries are durable aggregates so a crashed move can be retried
// cleanly: the row tells us "this Job already started moving but didn't
// finish." A pure stateless mover would either leave half-renamed
// files or risk delivering twice across a restart.
package deliver

import (
	"errors"
	"strconv"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// DeliveryID identifies a Delivery aggregate (one per Job, today).
type DeliveryID int64

// State is the lifecycle of a Delivery.
type State string

const (
	StatePending  State = "pending"  // queued; mover hasn't started
	StateMoving   State = "moving"   // files are being moved
	StateComplete State = "complete" // every file landed in target dir
	StateFailed   State = "failed"   // mover errored; see errMsg
	StateSkipped  State = "skipped"  // job is an archive; extract owns it
)

// Delivery is the aggregate root.
type Delivery struct {
	id        DeliveryID
	jobID     download.JobID
	state     State
	targetDir string // resolved on creation; "" until then
	errMsg    string

	createdAt  time.Time
	startedAt  time.Time
	finishedAt time.Time

	events []event.Event
}

// NewParams is the input to New (for fresh inserts; ID is assigned by
// the repository on Save).
type NewParams struct {
	JobID     download.JobID
	TargetDir string
}

// New constructs a fresh pending Delivery and emits DeliveryQueued.
func New(p NewParams, now time.Time) *Delivery {
	d := &Delivery{
		jobID:     p.JobID,
		state:     StatePending,
		targetDir: p.TargetDir,
		createdAt: now,
	}
	d.events = append(d.events, DeliveryQueued{
		JobID:     p.JobID,
		TargetDir: p.TargetDir,
		At:        now,
	})
	return d
}

// HydrateParams is what the repository hands back when loading a row.
type HydrateParams struct {
	ID         DeliveryID
	JobID      download.JobID
	State      State
	TargetDir  string
	ErrMsg     string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

// Hydrate reconstructs without emitting events.
func Hydrate(p HydrateParams) *Delivery {
	return &Delivery{
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
func (d *Delivery) ID() DeliveryID         { return d.id }
func (d *Delivery) JobID() download.JobID  { return d.jobID }
func (d *Delivery) State() State           { return d.state }
func (d *Delivery) TargetDir() string      { return d.targetDir }
func (d *Delivery) ErrMsg() string         { return d.errMsg }
func (d *Delivery) CreatedAt() time.Time   { return d.createdAt }
func (d *Delivery) StartedAt() time.Time   { return d.startedAt }
func (d *Delivery) FinishedAt() time.Time  { return d.finishedAt }

// SetID is called by the repository after INSERT.
func (d *Delivery) SetID(id DeliveryID) {
	d.id = id
	// Patch the queued event in flight (if still pending dispatch) so
	// subscribers see the canonical id rather than a placeholder zero.
	for i := range d.events {
		if q, ok := d.events[i].(DeliveryQueued); ok && q.ID == 0 {
			q.ID = id
			d.events[i] = q
		}
	}
}

// PullEvents drains the buffered events. Application services call
// this just before publishing inside the transaction.
func (d *Delivery) PullEvents() []event.Event {
	out := d.events
	d.events = nil
	return out
}

// Start transitions pending → moving.
func (d *Delivery) Start(now time.Time) error {
	if d.state != StatePending {
		return errors.New("deliver: Start requires pending state")
	}
	d.state = StateMoving
	d.startedAt = now
	d.events = append(d.events, DeliveryStarted{
		ID: d.id, JobID: d.jobID, At: now,
	})
	return nil
}

// Complete transitions moving → complete.
func (d *Delivery) Complete(now time.Time) error {
	if d.state != StateMoving {
		return errors.New("deliver: Complete requires moving state")
	}
	d.state = StateComplete
	d.finishedAt = now
	d.events = append(d.events, DeliveryComplete{
		ID: d.id, JobID: d.jobID, TargetDir: d.targetDir, At: now,
	})
	return nil
}

// Fail transitions moving → failed.
func (d *Delivery) Fail(reason string, now time.Time) error {
	if d.state != StateMoving && d.state != StatePending {
		return errors.New("deliver: Fail requires pending or moving state")
	}
	d.state = StateFailed
	d.errMsg = reason
	d.finishedAt = now
	d.events = append(d.events, DeliveryFailed{
		ID: d.id, JobID: d.jobID, Err: reason, At: now,
	})
	return nil
}

// Skip transitions pending → skipped (used when the job needs extract
// rather than direct delivery).
func (d *Delivery) Skip(now time.Time) error {
	if d.state != StatePending {
		return errors.New("deliver: Skip requires pending state")
	}
	d.state = StateSkipped
	d.finishedAt = now
	d.events = append(d.events, DeliverySkipped{
		ID: d.id, JobID: d.jobID, At: now,
	})
	return nil
}

func aggID(id DeliveryID) string {
	return strconv.FormatInt(int64(id), 10)
}
