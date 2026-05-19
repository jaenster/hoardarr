package download

import (
	"context"
	"errors"
	"io"
	"time"

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
	// ListShallow / ActiveShallow / HistoryShallow return jobs with
	// file metadata only — no per-file segment hydration. Use these
	// for UI lists where the cost of loading every segment dominates
	// response time.
	ListShallow(ctx context.Context) ([]*Job, error)
	ActiveShallow(ctx context.Context) ([]*Job, error)
	HistoryShallow(ctx context.Context, q HistoryQuery) ([]*Job, error)
	// *JobsOnly variants return Jobs with NO files attached — even
	// cheaper than Shallow. Use for the queue + history list endpoints
	// where callers only consume job-level summary fields (state,
	// totals, names). Saves N file queries per list call.
	ListJobsOnly(ctx context.Context) ([]*Job, error)
	ActiveJobsOnly(ctx context.Context) ([]*Job, error)
	HistoryJobsOnly(ctx context.Context, q HistoryQuery) ([]*Job, error)
	// CountAll / CountActive return scalar counts — for endpoints that
	// only display queue depth (e.g. /api/v1/system/status). Doesn't
	// materialise any rows.
	CountAll(ctx context.Context) (int, error)
	CountActive(ctx context.Context) (int, error)
	History(ctx context.Context, q HistoryQuery) ([]*Job, error)
	Delete(ctx context.Context, id JobID) error

	// UpdateSegmentBatch applies many small segment-completion updates
	// in a single tx. Used by the orchestrator's 100ms drainer.
	UpdateSegmentBatch(ctx context.Context, updates []SegmentUpdate) error
}

// HistoryQuery filters terminal-state jobs returned by JobRepository.History.
// All fields are optional; empty filters mean "no constraint".
//
// Limit is clamped by the repo to a sane upper bound (500 today) so a
// runaway client can't drag the whole history into memory.
type HistoryQuery struct {
	Since    *time.Time // finished_at > since
	Category string     // exact match
	State    JobState   // optional restriction; zero-value = any terminal state
	Limit    int        // 0 → repo default (100); negative treated as default
}

// SegmentUpdate is a single mutation pushed by the orchestrator's
// completion drainer. The repo applies all updates in one tx so they
// commit atomically with the events the orchestrator publishes.
type SegmentUpdate struct {
	SegmentID   SegmentID
	State       SegmentState
	Attempts    int
	LastError   string
	FileOffset  int64
	NextRetryAt time.Time
}

// ErrJobNotFound is returned by repo lookups when the id has no row.
var ErrJobNotFound = errors.New("download: job not found")

// ErrDuplicateNZBHash is returned by JobRepository.Save when an
// insert violates the UNIQUE(nzb_hash) constraint. The application
// service catches this and converts to its public ErrDuplicateNZB
// (looking up the existing job's ID for the response).
//
// Race scenario: two concurrent uploads of identical NZB bytes both
// pass a ByNZBHash pre-check before either commits. The first INSERT
// wins; the second hits this error.
var ErrDuplicateNZBHash = errors.New("download: nzb hash already exists")

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
