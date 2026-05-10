package extract

import (
	"context"
	"errors"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// Repository persists Extract aggregates. UNIQUE(job_id) at the DB
// level means at most one Extract row exists per Job.
type Repository interface {
	Save(ctx context.Context, x *Extract) error
	ByID(ctx context.Context, id ExtractID) (*Extract, error)
	ByJobID(ctx context.Context, jobID download.JobID) (*Extract, error)
}

// ErrNotFound is returned when no row matches the lookup.
var ErrNotFound = errors.New("extract: not found")

// Extractor is the port the app/extract worker uses to actually unpack
// archives. Implementations live in adapter/rar (RAR3 + RAR5 via
// nwaples/rardecode/v2).
//
// Extract reads from one or more source archive files (multi-part
// volumes) and writes the contained entries into targetDir. Returns
// the list of extracted relative paths so the caller can verify the
// expected layout. Implementations must reject any entry whose
// resolved path escapes targetDir (path traversal).
type Extractor interface {
	Extract(ctx context.Context, archivePaths []string, targetDir string) ([]string, error)
}
