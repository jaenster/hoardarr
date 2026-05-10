package download

import (
	"context"
	"errors"
	"io"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// JobRepository persists Job aggregates. Implementations live in adapter
// packages (e.g. internal/adapter/sqlite/repo_download.go).
//
// Save inserts a new row tree when the Job's ID is 0, or updates the
// existing rows otherwise. Children (files, segments) are persisted in
// the same tx.
type JobRepository interface {
	Save(ctx context.Context, j *Job) error
	ByID(ctx context.Context, id JobID) (*Job, error)
	ByNZBHash(ctx context.Context, hash string) (*Job, error)
	List(ctx context.Context) ([]*Job, error)
	Active(ctx context.Context) ([]*Job, error)
	Delete(ctx context.Context, id JobID) error

	// UpdateSegmentBatch applies many small segment-completion updates
	// in a single tx. Used by the orchestrator's 100ms drainer.
	UpdateSegmentBatch(ctx context.Context, updates []SegmentUpdate) error
}

// SegmentUpdate is a single mutation pushed by the orchestrator's
// completion drainer. The repo applies all updates in one tx so they
// commit atomically with the events the orchestrator publishes.
type SegmentUpdate struct {
	SegmentID  SegmentID
	State      SegmentState
	Attempts   int
	LastError  string
	FileOffset int64
}

// ErrJobNotFound is returned by repo lookups when the id has no row.
var ErrJobNotFound = errors.New("download: job not found")

// ArticleFetcher fetches one article body from a configured Usenet
// server. Implementations: internal/adapter/nntp.Pool.
//
// On 430 (article missing), implementations return an error wrapping
// nntp.ErrArticleMissing so the orchestrator can decide whether to try
// another server.
//
// The returned ReadCloser yields the de-dot-stuffed article body.
// Callers MUST drain to EOF or call Close.
type ArticleFetcher interface {
	Fetch(ctx context.Context, srv server.ServerID, messageID string) (io.ReadCloser, error)
}
