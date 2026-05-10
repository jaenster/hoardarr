package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/deliver"
	"github.com/jaenster/hoardarr/internal/domain/download"
)

// DeliveryRepo persists deliver.Delivery aggregates against the
// `deliveries` table.
type DeliveryRepo struct {
	db *DB
}

// NewDeliveryRepo wires the repo over db.
func NewDeliveryRepo(db *DB) *DeliveryRepo {
	return &DeliveryRepo{db: db}
}

// Save inserts when ID()==0, otherwise updates by id.
func (r *DeliveryRepo) Save(ctx context.Context, d *deliver.Delivery) error {
	if d.ID() == 0 {
		return r.insert(ctx, d)
	}
	return r.update(ctx, d)
}

func (r *DeliveryRepo) insert(ctx context.Context, d *deliver.Delivery) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO deliveries(job_id, state, target_dir, err_msg,
			created_at, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		int64(d.JobID()),
		string(d.State()),
		d.TargetDir(),
		nullableString(d.ErrMsg()),
		d.CreatedAt().UnixMilli(),
		nullableMillis(d.StartedAt()),
		nullableMillis(d.FinishedAt()),
	)
	if err != nil {
		return fmt.Errorf("insert delivery: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}
	d.SetID(deliver.DeliveryID(id))
	return nil
}

func (r *DeliveryRepo) update(ctx context.Context, d *deliver.Delivery) error {
	_, err := r.db.ExecCtx(ctx, `
		UPDATE deliveries
		SET state = ?, target_dir = ?, err_msg = ?,
			started_at = ?, finished_at = ?
		WHERE id = ?
	`,
		string(d.State()),
		d.TargetDir(),
		nullableString(d.ErrMsg()),
		nullableMillis(d.StartedAt()),
		nullableMillis(d.FinishedAt()),
		int64(d.ID()),
	)
	if err != nil {
		return fmt.Errorf("update delivery: %w", err)
	}
	return nil
}

// ByID looks up a delivery by primary key.
func (r *DeliveryRepo) ByID(ctx context.Context, id deliver.DeliveryID) (*deliver.Delivery, error) {
	row := r.db.QueryRowCtx(ctx, selectDeliveryByID, int64(id))
	return scanDelivery(row)
}

// ByJobID returns the (at-most-one) delivery for a job. UNIQUE(job_id)
// enforces "at most one" at the schema level.
func (r *DeliveryRepo) ByJobID(ctx context.Context, jobID download.JobID) (*deliver.Delivery, error) {
	row := r.db.QueryRowCtx(ctx, selectDeliveryByJob, int64(jobID))
	return scanDelivery(row)
}

func scanDelivery(row *sql.Row) (*deliver.Delivery, error) {
	var (
		id        int64
		jobID     int64
		state     string
		targetDir string
		errMsg    sql.NullString
		createdAt int64
		startedAt sql.NullInt64
		finishedAt sql.NullInt64
	)
	err := row.Scan(&id, &jobID, &state, &targetDir, &errMsg,
		&createdAt, &startedAt, &finishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, deliver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan delivery: %w", err)
	}
	return deliver.Hydrate(deliver.HydrateParams{
		ID:         deliver.DeliveryID(id),
		JobID:      download.JobID(jobID),
		State:      deliver.State(state),
		TargetDir:  targetDir,
		ErrMsg:     errMsg.String,
		CreatedAt:  time.UnixMilli(createdAt).UTC(),
		StartedAt:  nullableTime(startedAt),
		FinishedAt: nullableTime(finishedAt),
	}), nil
}

const deliveryColumns = `id, job_id, state, target_dir, err_msg,
	created_at, started_at, finished_at`

const selectDeliveryByID = `SELECT ` + deliveryColumns + ` FROM deliveries WHERE id = ?`
const selectDeliveryByJob = `SELECT ` + deliveryColumns + ` FROM deliveries WHERE job_id = ?`
