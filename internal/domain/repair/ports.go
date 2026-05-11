package repair

import (
	"context"
	"errors"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// Repository persists Repair aggregates.
type Repository interface {
	Save(ctx context.Context, r *Repair) error
	ByID(ctx context.Context, id RepairID) (*Repair, error)
	ByJobID(ctx context.Context, jobID download.JobID) (*Repair, error)
}

// ErrNotFound — no row matches the lookup.
var ErrNotFound = errors.New("repair: not found")
