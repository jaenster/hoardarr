package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/verify"
)

// VerifyRepo implements verify.Repository against SQLite.
type VerifyRepo struct {
	db *DB
}

// Compile-time check.
var _ verify.Repository = (*VerifyRepo)(nil)

// NewVerifyRepo wires the repo over db.
func NewVerifyRepo(db *DB) *VerifyRepo {
	return &VerifyRepo{db: db}
}

// Save inserts a new row when ID == 0, else updates the existing row.
// Tx-aware via TxFromContext.
func (r *VerifyRepo) Save(ctx context.Context, v *verify.VerifySet) error {
	if v.ID() == 0 {
		return r.insert(ctx, v)
	}
	return r.update(ctx, v)
}

func (r *VerifyRepo) insert(ctx context.Context, v *verify.VerifySet) error {
	failedJSON, err := json.Marshal(v.FailedFiles())
	if err != nil {
		return err
	}
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO par2_sets(job_id, state, started_at, finished_at, error_msg, failed_files)
		VALUES (?, ?, ?, ?, ?, ?)
	`,
		int64(v.JobID()), string(v.State()),
		nullableMillis(v.StartedAt()), nullableMillis(v.FinishedAt()),
		nullableString(v.ErrorMsg()),
		string(failedJSON),
	)
	if err != nil {
		return fmt.Errorf("insert par2_sets: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last_id: %w", err)
	}
	v.SetID(verify.VerifySetID(id))
	return nil
}

func (r *VerifyRepo) update(ctx context.Context, v *verify.VerifySet) error {
	failedJSON, err := json.Marshal(v.FailedFiles())
	if err != nil {
		return err
	}
	_, err = r.db.ExecCtx(ctx, `
		UPDATE par2_sets SET
			state = ?, started_at = ?, finished_at = ?,
			error_msg = ?, failed_files = ?
		WHERE id = ?
	`,
		string(v.State()),
		nullableMillis(v.StartedAt()), nullableMillis(v.FinishedAt()),
		nullableString(v.ErrorMsg()),
		string(failedJSON),
		int64(v.ID()),
	)
	if err != nil {
		return fmt.Errorf("update par2_sets: %w", err)
	}
	return nil
}

// ByID loads by primary key.
func (r *VerifyRepo) ByID(ctx context.Context, id verify.VerifySetID) (*verify.VerifySet, error) {
	row := r.db.QueryRowCtx(ctx, selectVerifyByID, int64(id))
	return scanVerify(row)
}

// ByJobID loads by job id (UNIQUE constraint guarantees one row).
func (r *VerifyRepo) ByJobID(ctx context.Context, jobID download.JobID) (*verify.VerifySet, error) {
	row := r.db.QueryRowCtx(ctx, selectVerifyByJobID, int64(jobID))
	return scanVerify(row)
}

const verifyColumns = `id, job_id, state, started_at, finished_at, error_msg, failed_files`

const selectVerifyByID = `SELECT ` + verifyColumns + ` FROM par2_sets WHERE id = ?`
const selectVerifyByJobID = `SELECT ` + verifyColumns + ` FROM par2_sets WHERE job_id = ?`

func scanVerify(row *sql.Row) (*verify.VerifySet, error) {
	var (
		id          int64
		jobID       int64
		state       string
		startedAt   sql.NullInt64
		finishedAt  sql.NullInt64
		errorMsg    sql.NullString
		failedJSON  string
	)
	if err := row.Scan(&id, &jobID, &state, &startedAt, &finishedAt, &errorMsg, &failedJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, verify.ErrNotFound
		}
		return nil, err
	}
	var failed []string
	if failedJSON != "" {
		_ = json.Unmarshal([]byte(failedJSON), &failed)
	}
	return verify.Hydrate(verify.HydrateParams{
		ID:          verify.VerifySetID(id),
		JobID:       download.JobID(jobID),
		State:       verify.VerifyState(state),
		StartedAt:   nullableTime(startedAt),
		FinishedAt:  nullableTime(finishedAt),
		ErrorMsg:    errorMsg.String,
		FailedFiles: failed,
	}), nil
}

// silence — these helpers are referenced from peer files.
var _ = time.Time{}
