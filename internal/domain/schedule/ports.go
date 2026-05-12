package schedule

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound — no row matches the lookup.
var ErrNotFound = errors.New("schedule: task not found")

// ErrClaimLost — another worker claimed the task between our SELECT
// and our UPDATE. The caller should skip and continue.
var ErrClaimLost = errors.New("schedule: claim lost")

// Repository persists Tasks. All time fields are stored as unix-ms.
type Repository interface {
	// Save inserts (if id==0) or updates the task.
	Save(ctx context.Context, t *Task) error
	// ByID fetches a single task.
	ByID(ctx context.Context, id TaskID) (*Task, error)
	// ByName fetches by unique name (used for upsert on register).
	ByName(ctx context.Context, name string) (*Task, error)
	// List returns every task — used by the admin UI.
	List(ctx context.Context) ([]*Task, error)
	// Delete removes a task.
	Delete(ctx context.Context, id TaskID) error

	// ClaimDue tries to atomically claim up to n tasks whose
	// next_run_at <= now and that are enabled+idle. Returns the
	// claimed tasks in NextRunAt order. Implementation uses an
	// UPDATE...RETURNING (or SELECT + conditional UPDATE) so
	// concurrent schedulers can't double-claim.
	ClaimDue(ctx context.Context, now time.Time, n int) ([]*Task, error)

	// ResetStaleClaims flips any rows in status='running' (left over
	// from a previous process that crashed) back to idle. Called once
	// at scheduler startup.
	ResetStaleClaims(ctx context.Context, now time.Time) (int, error)
}

// Handler runs a single task. The payload is the task-specific blob
// stored on the row; the handler decodes it. Returning an error puts
// the task into MarkFailed with the error string preserved.
type Handler func(ctx context.Context, payload []byte) error
