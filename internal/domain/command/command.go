// Package command is the bounded context for one-off async
// operations that the operator (or other code) triggers, distinct
// from recurring scheduled tasks. Sonarr calls them Commands; we
// adopt the name so the mental model travels.
//
// A Command is a JSON-bodied unit of work claimed off a queue by a
// background worker. State progresses queued → running → completed,
// with `result` capturing success/failure. The wire shape matches
// Sonarr's so future SAB / *arr-compat work has fewer translations.
//
// Concrete handlers live in app/command/handlers/* and register
// against the service by Name(). The domain knows nothing about
// what the work actually does.
package command

import (
	"errors"
	"strings"
	"time"
)

// CommandID identifies a Command aggregate.
type CommandID int64

// Status is the lifecycle state.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
)

// Result is populated when Status transitions to completed.
type Result string

const (
	ResultSuccessful Result = "successful"
	ResultFailed     Result = "failed"
)

// Trigger discriminates how the command came into existence —
// useful for filtering "show me only the things I started" in the UI.
type Trigger string

const (
	TriggerManual    Trigger = "manual"
	TriggerAPI       Trigger = "api"
	TriggerScheduled Trigger = "scheduled"
)

// Command is the aggregate root.
type Command struct {
	id        CommandID
	name      string  // handler name registered with the service
	body      []byte  // JSON payload, handler-specific
	trigger   Trigger
	status    Status
	result    Result
	err       string
	queuedAt  time.Time
	startedAt time.Time
	endedAt   time.Time
}

// NewParams gathers Submit() inputs.
type NewParams struct {
	Name    string
	Body    []byte
	Trigger Trigger
}

// New constructs a queued command.
func New(p NewParams, now time.Time) (*Command, error) {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return nil, errors.New("command: name required")
	}
	trig := p.Trigger
	if trig == "" {
		trig = TriggerAPI
	}
	return &Command{
		name:     name,
		body:     append([]byte(nil), p.Body...),
		trigger:  trig,
		status:   StatusQueued,
		queuedAt: now.UTC(),
	}, nil
}

func (c *Command) ID() CommandID      { return c.id }
func (c *Command) Name() string       { return c.name }
func (c *Command) Body() []byte       { return c.body }
func (c *Command) Trigger() Trigger   { return c.trigger }
func (c *Command) Status() Status     { return c.status }
func (c *Command) Result() Result     { return c.result }
func (c *Command) Error() string      { return c.err }
func (c *Command) QueuedAt() time.Time  { return c.queuedAt }
func (c *Command) StartedAt() time.Time { return c.startedAt }
func (c *Command) EndedAt() time.Time   { return c.endedAt }

// Duration returns the wall-clock the handler spent. Zero before
// completion or when the command never started (e.g. cancelled).
func (c *Command) Duration() time.Duration {
	if c.startedAt.IsZero() {
		return 0
	}
	end := c.endedAt
	if end.IsZero() {
		end = time.Now().UTC()
	}
	return end.Sub(c.startedAt)
}

// SetID is for persistence to populate the id after insert.
func (c *Command) SetID(id CommandID) { c.id = id }

// MarkRunning transitions queued → running. Worker calls this after
// claiming and before invoking the handler.
func (c *Command) MarkRunning(now time.Time) error {
	if c.status != StatusQueued {
		return errors.New("command: can only start a queued command")
	}
	c.status = StatusRunning
	c.startedAt = now.UTC()
	return nil
}

// MarkCompleted transitions running → completed with a successful
// result. handlerErr is the handler's return value; if non-nil,
// records as failed instead.
func (c *Command) MarkCompleted(handlerErr error, now time.Time) {
	c.status = StatusCompleted
	c.endedAt = now.UTC()
	if handlerErr != nil {
		c.result = ResultFailed
		c.err = handlerErr.Error()
		return
	}
	c.result = ResultSuccessful
}

// Rehydrate is the persistence escape hatch — restores a Command
// from a stored row without going through New's validation.
type Rehydrate struct {
	ID        CommandID
	Name      string
	Body      []byte
	Trigger   Trigger
	Status    Status
	Result    Result
	Error     string
	QueuedAt  time.Time
	StartedAt time.Time
	EndedAt   time.Time
}

func From(r Rehydrate) *Command {
	return &Command{
		id:        r.ID,
		name:      r.Name,
		body:      r.Body,
		trigger:   r.Trigger,
		status:    r.Status,
		result:    r.Result,
		err:       r.Error,
		queuedAt:  r.QueuedAt,
		startedAt: r.StartedAt,
		endedAt:   r.EndedAt,
	}
}
