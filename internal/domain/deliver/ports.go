package deliver

import (
	"context"
	"errors"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// DeliveryRepository persists Delivery aggregates.
//
// Save inserts when the aggregate has ID() == 0, otherwise updates.
// ByJobID returns the (at-most-one) Delivery row for a Job; ErrNotFound
// when none exists yet — the typical state at the moment a verify
// completes for the first time.
type DeliveryRepository interface {
	Save(ctx context.Context, d *Delivery) error
	ByID(ctx context.Context, id DeliveryID) (*Delivery, error)
	ByJobID(ctx context.Context, jobID download.JobID) (*Delivery, error)
}

// ErrNotFound is returned when no row matches the lookup.
var ErrNotFound = errors.New("deliver: not found")

// Filesystem is the port the deliver service uses to actually move
// files. Implementations live in adapter/fs.
//
// Move semantics:
//   - same filesystem: rename(2) (atomic).
//   - cross filesystem: copy + fsync + rename + unlink-source. Not
//     atomic by syscall, but resumable: on failure mid-copy the
//     destination is partially written; rerunning Move overwrites it.
//
// MkdirAll is os.MkdirAll equivalent. RemoveAll prunes a directory
// tree (used for cleaning incomplete/<jobid>/ after delivery).
type Filesystem interface {
	Move(src, dst string) error
	MkdirAll(path string) error
	RemoveAll(path string) error
}
