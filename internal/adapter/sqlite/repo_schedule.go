package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/schedule"
)

// ScheduleRepo persists schedule.Task aggregates against the
// `scheduled_tasks` table.
type ScheduleRepo struct {
	db *DB
}

// Compile-time port check.
var _ schedule.Repository = (*ScheduleRepo)(nil)

// NewScheduleRepo wires the repo over db.
func NewScheduleRepo(db *DB) *ScheduleRepo {
	return &ScheduleRepo{db: db}
}

const scheduleColumns = `id, name, kind, cadence, payload,
	next_run_at, last_run_at, last_error, consecutive_failures,
	enabled, status, claimed_at, created_at, updated_at`

func (r *ScheduleRepo) Save(ctx context.Context, t *schedule.Task) error {
	if t.ID() == 0 {
		return r.insert(ctx, t)
	}
	return r.update(ctx, t)
}

func (r *ScheduleRepo) insert(ctx context.Context, t *schedule.Task) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO scheduled_tasks(
			name, kind, cadence, payload,
			next_run_at, last_run_at, last_error, consecutive_failures,
			enabled, status, claimed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.Name(),
		string(t.Kind()),
		nullableString(durationString(t.Cadence())),
		nullableBlob(t.Payload()),
		t.NextRunAt().UnixMilli(),
		nullableMillis(t.LastRunAt()),
		nullableString(t.LastError()),
		t.ConsecutiveFailures(),
		boolToInt(t.Enabled()),
		string(t.Status()),
		nullableMillis(t.ClaimedAt()),
		t.CreatedAt().UnixMilli(),
		t.UpdatedAt().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("insert scheduled_task: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}
	t.SetID(schedule.TaskID(id))
	return nil
}

func (r *ScheduleRepo) update(ctx context.Context, t *schedule.Task) error {
	res, err := r.db.ExecCtx(ctx, `
		UPDATE scheduled_tasks
		SET name = ?, kind = ?, cadence = ?, payload = ?,
			next_run_at = ?, last_run_at = ?, last_error = ?, consecutive_failures = ?,
			enabled = ?, status = ?, claimed_at = ?, updated_at = ?
		WHERE id = ?`,
		t.Name(),
		string(t.Kind()),
		nullableString(durationString(t.Cadence())),
		nullableBlob(t.Payload()),
		t.NextRunAt().UnixMilli(),
		nullableMillis(t.LastRunAt()),
		nullableString(t.LastError()),
		t.ConsecutiveFailures(),
		boolToInt(t.Enabled()),
		string(t.Status()),
		nullableMillis(t.ClaimedAt()),
		t.UpdatedAt().UnixMilli(),
		int64(t.ID()),
	)
	if err != nil {
		return fmt.Errorf("update scheduled_task: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return schedule.ErrNotFound
	}
	return nil
}

func (r *ScheduleRepo) ByID(ctx context.Context, id schedule.TaskID) (*schedule.Task, error) {
	row := r.db.QueryRowCtx(ctx,
		`SELECT `+scheduleColumns+` FROM scheduled_tasks WHERE id = ?`,
		int64(id))
	return scanTask(row)
}

func (r *ScheduleRepo) ByName(ctx context.Context, name string) (*schedule.Task, error) {
	row := r.db.QueryRowCtx(ctx,
		`SELECT `+scheduleColumns+` FROM scheduled_tasks WHERE name = ?`,
		name)
	return scanTask(row)
}

func (r *ScheduleRepo) List(ctx context.Context) ([]*schedule.Task, error) {
	rows, err := r.db.QueryCtx(ctx,
		`SELECT `+scheduleColumns+` FROM scheduled_tasks ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("query scheduled_tasks: %w", err)
	}
	defer rows.Close()
	var out []*schedule.Task
	for rows.Next() {
		t, err := scanTaskFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *ScheduleRepo) Delete(ctx context.Context, id schedule.TaskID) error {
	res, err := r.db.ExecCtx(ctx, `DELETE FROM scheduled_tasks WHERE id = ?`, int64(id))
	if err != nil {
		return fmt.Errorf("delete scheduled_task: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return schedule.ErrNotFound
	}
	return nil
}

// ClaimDue atomically claims up to n due tasks via a per-row
// conditional UPDATE. Each UPDATE includes WHERE status='idle' so
// a concurrent claimer can't double-claim — RowsAffected==1 means we
// own that row. After all UPDATEs, we re-fetch only the IDs we
// actually won, so the caller never sees a task it doesn't own.
func (r *ScheduleRepo) ClaimDue(ctx context.Context, now time.Time, n int) ([]*schedule.Task, error) {
	if n <= 0 {
		return nil, nil
	}
	nowMillis := now.UnixMilli()
	// 1) Find candidate IDs.
	rows, err := r.db.QueryCtx(ctx, `
		SELECT id FROM scheduled_tasks
		WHERE enabled = 1 AND status = 'idle' AND next_run_at <= ?
		ORDER BY next_run_at ASC, id ASC
		LIMIT ?`,
		nowMillis, n)
	if err != nil {
		return nil, fmt.Errorf("select due: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, nil
	}
	// 2) Conditionally flip each to running; only collect IDs we won.
	var won []int64
	for _, id := range ids {
		res, err := r.db.ExecCtx(ctx, `
			UPDATE scheduled_tasks
			SET status = 'running', claimed_at = ?, updated_at = ?
			WHERE id = ? AND status = 'idle'`,
			nowMillis, nowMillis, id)
		if err != nil {
			return nil, fmt.Errorf("claim id=%d: %w", id, err)
		}
		if affected, _ := res.RowsAffected(); affected == 1 {
			won = append(won, id)
		}
	}
	if len(won) == 0 {
		return nil, nil
	}
	// 3) Re-fetch the rows we actually claimed.
	fetchRows, err := r.db.QueryCtx(ctx, `
		SELECT `+scheduleColumns+` FROM scheduled_tasks
		WHERE id IN (`+placeholders(len(won))+`)
		ORDER BY next_run_at ASC, id ASC`,
		int64Args(won)...)
	if err != nil {
		return nil, fmt.Errorf("refetch claimed: %w", err)
	}
	defer fetchRows.Close()
	var claimed []*schedule.Task
	for fetchRows.Next() {
		t, err := scanTaskFromRows(fetchRows)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, t)
	}
	return claimed, fetchRows.Err()
}

// ResetStaleClaims flips any rows in status='running' back to idle.
// Called once at scheduler startup so tasks from a crashed previous
// process get re-tried.
func (r *ScheduleRepo) ResetStaleClaims(ctx context.Context, now time.Time) (int, error) {
	res, err := r.db.ExecCtx(ctx, `
		UPDATE scheduled_tasks
		SET status = 'idle', claimed_at = NULL, updated_at = ?
		WHERE status = 'running'`,
		now.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("reset stale claims: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

type taskScanner interface {
	Scan(dest ...any) error
}

func scanTask(row *sql.Row) (*schedule.Task, error) {
	t, err := scanTaskFromRows(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, schedule.ErrNotFound
	}
	return t, err
}

func scanTaskFromRows(s taskScanner) (*schedule.Task, error) {
	var (
		id           int64
		name         string
		kind         string
		cadence      sql.NullString
		payload      []byte
		nextRunAt    int64
		lastRunAt    sql.NullInt64
		lastError    sql.NullString
		consecFails  int
		enabled      int
		status       string
		claimedAt    sql.NullInt64
		createdAt    int64
		updatedAt    int64
	)
	if err := s.Scan(&id, &name, &kind, &cadence, &payload,
		&nextRunAt, &lastRunAt, &lastError, &consecFails,
		&enabled, &status, &claimedAt, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	var d time.Duration
	if cadence.Valid && cadence.String != "" {
		parsed, err := time.ParseDuration(cadence.String)
		if err != nil {
			return nil, fmt.Errorf("parse cadence %q: %w", cadence.String, err)
		}
		d = parsed
	}
	return schedule.Hydrate(schedule.HydrateParams{
		ID:                  schedule.TaskID(id),
		Name:                name,
		Kind:                schedule.Kind(kind),
		Cadence:             d,
		Payload:             payload,
		NextRunAt:           time.UnixMilli(nextRunAt).UTC(),
		LastRunAt:           nullableTime(lastRunAt),
		LastError:           lastError.String,
		ConsecutiveFailures: consecFails,
		Enabled:             enabled != 0,
		Status:              schedule.Status(status),
		ClaimedAt:           nullableTime(claimedAt),
		CreatedAt:           time.UnixMilli(createdAt).UTC(),
		UpdatedAt:           time.UnixMilli(updatedAt).UTC(),
	}), nil
}

func durationString(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

func nullableBlob(b []byte) any {
	if b == nil {
		return nil
	}
	return b
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '?')
	}
	return string(buf)
}

func int64Args(xs []int64) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
