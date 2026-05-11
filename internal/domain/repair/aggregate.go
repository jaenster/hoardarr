// Package repair owns the bounded context that uses PAR2 recovery
// slices to reconstruct damaged files. The aggregate is durable so a
// crash mid-repair resumes cleanly.
package repair

import (
	"errors"
	"strconv"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// RepairID identifies a Repair aggregate (one per Job).
type RepairID int64

// State is the lifecycle of a Repair.
type State string

const (
	StatePending   State = "pending"
	StateRepairing State = "repairing"
	StateOK        State = "ok"        // reconstruction succeeded
	StateFailed    State = "failed"    // not enough RS or post-repair md5 still wrong
)

// Repair is the aggregate root.
type Repair struct {
	id    RepairID
	jobID download.JobID
	state State
	err   string

	createdAt  time.Time
	startedAt  time.Time
	finishedAt time.Time

	events []event.Event
}

// NewParams is the input to New (fresh inserts).
type NewParams struct {
	JobID download.JobID
}

// New builds a fresh pending Repair and emits RepairQueued.
func New(p NewParams, now time.Time) *Repair {
	r := &Repair{
		jobID:     p.JobID,
		state:     StatePending,
		createdAt: now,
	}
	r.events = append(r.events, RepairQueued{
		JobID: p.JobID, At: now,
	})
	return r
}

// HydrateParams reloads an existing row.
type HydrateParams struct {
	ID         RepairID
	JobID      download.JobID
	State      State
	Err        string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

// Hydrate reconstructs without emitting events.
func Hydrate(p HydrateParams) *Repair {
	return &Repair{
		id:         p.ID,
		jobID:      p.JobID,
		state:      p.State,
		err:        p.Err,
		createdAt:  p.CreatedAt,
		startedAt:  p.StartedAt,
		finishedAt: p.FinishedAt,
	}
}

func (r *Repair) ID() RepairID         { return r.id }
func (r *Repair) JobID() download.JobID { return r.jobID }
func (r *Repair) State() State          { return r.state }
func (r *Repair) Err() string           { return r.err }
func (r *Repair) CreatedAt() time.Time  { return r.createdAt }
func (r *Repair) StartedAt() time.Time  { return r.startedAt }
func (r *Repair) FinishedAt() time.Time { return r.finishedAt }

func (r *Repair) SetID(id RepairID) {
	r.id = id
	for i := range r.events {
		if q, ok := r.events[i].(RepairQueued); ok && q.ID == 0 {
			q.ID = id
			r.events[i] = q
		}
	}
}

func (r *Repair) PullEvents() []event.Event {
	out := r.events
	r.events = nil
	return out
}

func (r *Repair) Start(now time.Time) error {
	if r.state != StatePending {
		return errors.New("repair: Start requires pending state")
	}
	r.state = StateRepairing
	r.startedAt = now
	r.events = append(r.events, RepairStarted{ID: r.id, JobID: r.jobID, At: now})
	return nil
}

func (r *Repair) MarkOK(now time.Time) error {
	if r.state != StateRepairing {
		return errors.New("repair: MarkOK requires repairing state")
	}
	r.state = StateOK
	r.finishedAt = now
	r.events = append(r.events, RepairOK{ID: r.id, JobID: r.jobID, At: now})
	return nil
}

func (r *Repair) MarkFailed(reason string, now time.Time) error {
	if r.state != StateRepairing && r.state != StatePending {
		return errors.New("repair: MarkFailed requires pending or repairing state")
	}
	r.state = StateFailed
	r.err = reason
	r.finishedAt = now
	r.events = append(r.events, RepairFailed{ID: r.id, JobID: r.jobID, Err: reason, At: now})
	return nil
}

func aggID(id RepairID) string {
	return strconv.FormatInt(int64(id), 10)
}
