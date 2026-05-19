package command

import "context"

// Repository is the persistence port for Command aggregates.
//
// ClaimNext is the worker's interaction: atomically pick the oldest
// queued command and flip it to running. The atomicity is critical
// because a future "scale the command worker" change must not
// double-run a command.
type Repository interface {
	Save(ctx context.Context, c *Command) error
	ByID(ctx context.Context, id CommandID) (*Command, error)
	// List returns the n most recent commands, newest-first.
	List(ctx context.Context, limit int) ([]*Command, error)
	// ClaimNext atomically picks the oldest queued command, marks it
	// running, and returns it. Returns (nil, nil) when nothing is due.
	ClaimNext(ctx context.Context) (*Command, error)
	// ResetStaleClaims rescues commands stuck in `running` after a
	// crash. Mark anything running with started_at older than `older`
	// back to queued. Returns count affected.
	ResetStaleClaims(ctx context.Context, older int64) (int, error)
}
