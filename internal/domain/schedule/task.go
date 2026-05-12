// Package schedule owns the durable scheduled-tasks bounded context.
// A Task is one unit of work that must fire at a specific time
// (oneshot) or on a cadence (recurring), and must survive process
// restart. Persistence lives in adapter/sqlite; the application loop
// in app/schedule reads due tasks, claims them, and invokes
// registered handlers.
//
// Why durable: tickers in goroutines die on restart and never run their
// missed slot. The scheduled_tasks table captures next_run_at so a
// task that should have fired during downtime fires immediately when
// the process comes back up.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// TaskID identifies a Task aggregate.
type TaskID int64

// Kind discriminates recurring (fires forever on cadence) from oneshot
// (fires once at next_run_at then disables itself).
type Kind string

const (
	KindRecurring Kind = "recurring"
	KindOneshot   Kind = "oneshot"
)

// Status reflects the worker-claim state.
type Status string

const (
	StatusIdle    Status = "idle"
	StatusRunning Status = "running"
)

// Task is the aggregate root.
type Task struct {
	id                  TaskID
	name                string
	kind                Kind
	cadence             time.Duration // 0 for oneshot
	payload             []byte
	nextRunAt           time.Time
	lastRunAt           time.Time
	lastError           string
	consecutiveFailures int
	enabled             bool
	status              Status
	claimedAt           time.Time
	createdAt           time.Time
	updatedAt           time.Time
}

// NewParams gathers Task constructor inputs.
type NewParams struct {
	Name      string
	Kind      Kind
	Cadence   time.Duration // required for recurring; ignored for oneshot
	Payload   []byte
	FirstRun  time.Time // first time the task should fire
}

// New constructs a Task with validation.
func New(p NewParams, now time.Time) (*Task, error) {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return nil, errors.New("schedule: name required")
	}
	switch p.Kind {
	case KindRecurring:
		if p.Cadence <= 0 {
			return nil, errors.New("schedule: recurring task needs positive cadence")
		}
	case KindOneshot:
		// cadence ignored
	default:
		return nil, fmt.Errorf("schedule: unknown kind %q", p.Kind)
	}
	if p.FirstRun.IsZero() {
		return nil, errors.New("schedule: first-run time required")
	}
	return &Task{
		name:      name,
		kind:      p.Kind,
		cadence:   p.Cadence,
		payload:   p.Payload,
		nextRunAt: p.FirstRun,
		enabled:   true,
		status:    StatusIdle,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// HydrateParams is the snapshot the repository hands back.
type HydrateParams struct {
	ID                  TaskID
	Name                string
	Kind                Kind
	Cadence             time.Duration
	Payload             []byte
	NextRunAt           time.Time
	LastRunAt           time.Time
	LastError           string
	ConsecutiveFailures int
	Enabled             bool
	Status              Status
	ClaimedAt           time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Hydrate reconstructs without emitting events.
func Hydrate(p HydrateParams) *Task {
	return &Task{
		id:                  p.ID,
		name:                p.Name,
		kind:                p.Kind,
		cadence:             p.Cadence,
		payload:             p.Payload,
		nextRunAt:           p.NextRunAt,
		lastRunAt:           p.LastRunAt,
		lastError:           p.LastError,
		consecutiveFailures: p.ConsecutiveFailures,
		enabled:             p.Enabled,
		status:              p.Status,
		claimedAt:           p.ClaimedAt,
		createdAt:           p.CreatedAt,
		updatedAt:           p.UpdatedAt,
	}
}

// Accessors.
func (t *Task) ID() TaskID                     { return t.id }
func (t *Task) Name() string                   { return t.name }
func (t *Task) Kind() Kind                     { return t.kind }
func (t *Task) Cadence() time.Duration         { return t.cadence }
func (t *Task) Payload() []byte                { return t.payload }
func (t *Task) NextRunAt() time.Time           { return t.nextRunAt }
func (t *Task) LastRunAt() time.Time           { return t.lastRunAt }
func (t *Task) LastError() string              { return t.lastError }
func (t *Task) ConsecutiveFailures() int       { return t.consecutiveFailures }
func (t *Task) Enabled() bool                  { return t.enabled }
func (t *Task) Status() Status                 { return t.status }
func (t *Task) ClaimedAt() time.Time           { return t.claimedAt }
func (t *Task) CreatedAt() time.Time           { return t.createdAt }
func (t *Task) UpdatedAt() time.Time           { return t.updatedAt }

// SetID is called by the repository after INSERT.
func (t *Task) SetID(id TaskID) { t.id = id }

// MarkClaimed transitions to running. The repo enforces optimistic
// concurrency: the UPDATE includes a status='idle' predicate so a
// second worker can't double-claim.
func (t *Task) MarkClaimed(now time.Time) {
	t.status = StatusRunning
	t.claimedAt = now
	t.updatedAt = now
}

// MarkSucceeded records a successful run and recomputes next_run_at.
// Oneshot tasks disable themselves; recurring tasks add cadence.
func (t *Task) MarkSucceeded(now time.Time) {
	t.lastRunAt = now
	t.lastError = ""
	t.consecutiveFailures = 0
	t.status = StatusIdle
	t.claimedAt = time.Time{}
	t.updatedAt = now
	switch t.kind {
	case KindRecurring:
		t.nextRunAt = now.Add(t.cadence)
	case KindOneshot:
		t.enabled = false
	}
}

// MarkFailed records a failure. Recurring tasks back off with
// exponential delay (cadence × 2^failures, capped at 1 hour) so a
// poison task doesn't burn CPU. Oneshot tasks remain enabled — the
// admin must intervene or the task will retry next sweep with a
// short backoff.
func (t *Task) MarkFailed(reason string, now time.Time) {
	t.lastRunAt = now
	t.lastError = reason
	t.consecutiveFailures++
	t.status = StatusIdle
	t.claimedAt = time.Time{}
	t.updatedAt = now
	backoff := t.computeBackoff()
	t.nextRunAt = now.Add(backoff)
}

func (t *Task) computeBackoff() time.Duration {
	base := t.cadence
	if base <= 0 {
		base = 30 * time.Second
	}
	mult := 1 << t.consecutiveFailures
	if mult > 64 {
		mult = 64
	}
	backoff := time.Duration(mult) * base
	const cap = time.Hour
	if backoff > cap {
		backoff = cap
	}
	return backoff
}

// SetEnabled toggles the enabled flag.
func (t *Task) SetEnabled(enabled bool, now time.Time) {
	if t.enabled == enabled {
		return
	}
	t.enabled = enabled
	t.updatedAt = now
}

// Reschedule moves next_run_at. Used for admin "run now" + cadence
// edits.
func (t *Task) Reschedule(at time.Time, now time.Time) {
	t.nextRunAt = at
	t.updatedAt = now
}

// SetCadence is only valid for recurring tasks. Idempotent if same.
func (t *Task) SetCadence(d time.Duration, now time.Time) error {
	if t.kind != KindRecurring {
		return errors.New("schedule: cadence only applies to recurring tasks")
	}
	if d <= 0 {
		return errors.New("schedule: cadence must be positive")
	}
	if t.cadence == d {
		return nil
	}
	t.cadence = d
	t.updatedAt = now
	return nil
}

// ResetStaleClaim is used by startup recovery: any task that the
// repo finds in status='running' must have come from a previous
// process that died mid-run. Flip it back to idle so the scheduler
// can pick it up again.
func (t *Task) ResetStaleClaim(now time.Time) {
	if t.status != StatusRunning {
		return
	}
	t.status = StatusIdle
	t.claimedAt = time.Time{}
	t.updatedAt = now
}
