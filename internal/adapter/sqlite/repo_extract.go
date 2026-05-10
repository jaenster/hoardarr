package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/extract"
)

// ExtractRepo persists extract.Extract aggregates against `extracts`.
type ExtractRepo struct {
	db *DB
}

func NewExtractRepo(db *DB) *ExtractRepo { return &ExtractRepo{db: db} }

func (r *ExtractRepo) Save(ctx context.Context, x *extract.Extract) error {
	if x.ID() == 0 {
		return r.insert(ctx, x)
	}
	return r.update(ctx, x)
}

func (r *ExtractRepo) insert(ctx context.Context, x *extract.Extract) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO extracts(job_id, state, target_dir, err_msg,
			created_at, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		int64(x.JobID()),
		string(x.State()),
		x.TargetDir(),
		nullableString(x.ErrMsg()),
		x.CreatedAt().UnixMilli(),
		nullableMillis(x.StartedAt()),
		nullableMillis(x.FinishedAt()),
	)
	if err != nil {
		return fmt.Errorf("insert extract: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}
	x.SetID(extract.ExtractID(id))
	return nil
}

func (r *ExtractRepo) update(ctx context.Context, x *extract.Extract) error {
	_, err := r.db.ExecCtx(ctx, `
		UPDATE extracts
		SET state = ?, target_dir = ?, err_msg = ?,
			started_at = ?, finished_at = ?
		WHERE id = ?
	`,
		string(x.State()),
		x.TargetDir(),
		nullableString(x.ErrMsg()),
		nullableMillis(x.StartedAt()),
		nullableMillis(x.FinishedAt()),
		int64(x.ID()),
	)
	if err != nil {
		return fmt.Errorf("update extract: %w", err)
	}
	return nil
}

func (r *ExtractRepo) ByID(ctx context.Context, id extract.ExtractID) (*extract.Extract, error) {
	row := r.db.QueryRowCtx(ctx, selectExtractByID, int64(id))
	return scanExtract(row)
}

func (r *ExtractRepo) ByJobID(ctx context.Context, jobID download.JobID) (*extract.Extract, error) {
	row := r.db.QueryRowCtx(ctx, selectExtractByJob, int64(jobID))
	return scanExtract(row)
}

func scanExtract(row *sql.Row) (*extract.Extract, error) {
	var (
		id         int64
		jobID      int64
		state      string
		targetDir  string
		errMsg     sql.NullString
		createdAt  int64
		startedAt  sql.NullInt64
		finishedAt sql.NullInt64
	)
	err := row.Scan(&id, &jobID, &state, &targetDir, &errMsg,
		&createdAt, &startedAt, &finishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, extract.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan extract: %w", err)
	}
	return extract.Hydrate(extract.HydrateParams{
		ID:         extract.ExtractID(id),
		JobID:      download.JobID(jobID),
		State:      extract.State(state),
		TargetDir:  targetDir,
		ErrMsg:     errMsg.String,
		CreatedAt:  time.UnixMilli(createdAt).UTC(),
		StartedAt:  nullableTime(startedAt),
		FinishedAt: nullableTime(finishedAt),
	}), nil
}

const extractColumns = `id, job_id, state, target_dir, err_msg,
	created_at, started_at, finished_at`

const selectExtractByID = `SELECT ` + extractColumns + ` FROM extracts WHERE id = ?`
const selectExtractByJob = `SELECT ` + extractColumns + ` FROM extracts WHERE job_id = ?`
