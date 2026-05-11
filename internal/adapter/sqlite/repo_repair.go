package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/repair"
)

// RepairRepo persists repair.Repair aggregates against `repairs`.
type RepairRepo struct {
	db *DB
}

func NewRepairRepo(db *DB) *RepairRepo { return &RepairRepo{db: db} }

func (r *RepairRepo) Save(ctx context.Context, x *repair.Repair) error {
	if x.ID() == 0 {
		return r.insert(ctx, x)
	}
	return r.update(ctx, x)
}

func (r *RepairRepo) insert(ctx context.Context, x *repair.Repair) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO repairs(job_id, state, err_msg, created_at, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`,
		int64(x.JobID()),
		string(x.State()),
		nullableString(x.Err()),
		x.CreatedAt().UnixMilli(),
		nullableMillis(x.StartedAt()),
		nullableMillis(x.FinishedAt()),
	)
	if err != nil {
		return fmt.Errorf("insert repair: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}
	x.SetID(repair.RepairID(id))
	return nil
}

func (r *RepairRepo) update(ctx context.Context, x *repair.Repair) error {
	_, err := r.db.ExecCtx(ctx, `
		UPDATE repairs
		SET state = ?, err_msg = ?, started_at = ?, finished_at = ?
		WHERE id = ?
	`,
		string(x.State()),
		nullableString(x.Err()),
		nullableMillis(x.StartedAt()),
		nullableMillis(x.FinishedAt()),
		int64(x.ID()),
	)
	if err != nil {
		return fmt.Errorf("update repair: %w", err)
	}
	return nil
}

func (r *RepairRepo) ByID(ctx context.Context, id repair.RepairID) (*repair.Repair, error) {
	row := r.db.QueryRowCtx(ctx, selectRepairByID, int64(id))
	return scanRepair(row)
}

func (r *RepairRepo) ByJobID(ctx context.Context, jobID download.JobID) (*repair.Repair, error) {
	row := r.db.QueryRowCtx(ctx, selectRepairByJob, int64(jobID))
	return scanRepair(row)
}

func scanRepair(row *sql.Row) (*repair.Repair, error) {
	var (
		id         int64
		jobID      int64
		state      string
		errMsg     sql.NullString
		createdAt  int64
		startedAt  sql.NullInt64
		finishedAt sql.NullInt64
	)
	err := row.Scan(&id, &jobID, &state, &errMsg, &createdAt, &startedAt, &finishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repair.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan repair: %w", err)
	}
	return repair.Hydrate(repair.HydrateParams{
		ID:         repair.RepairID(id),
		JobID:      download.JobID(jobID),
		State:      repair.State(state),
		Err:        errMsg.String,
		CreatedAt:  time.UnixMilli(createdAt).UTC(),
		StartedAt:  nullableTime(startedAt),
		FinishedAt: nullableTime(finishedAt),
	}), nil
}

const repairColumns = `id, job_id, state, err_msg, created_at, started_at, finished_at`

const selectRepairByID = `SELECT ` + repairColumns + ` FROM repairs WHERE id = ?`
const selectRepairByJob = `SELECT ` + repairColumns + ` FROM repairs WHERE job_id = ?`
