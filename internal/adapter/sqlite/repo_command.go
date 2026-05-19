package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/command"
)

// CommandRepo persists command.Command aggregates against the
// `commands` table.
type CommandRepo struct {
	db *DB
}

var _ command.Repository = (*CommandRepo)(nil)

func NewCommandRepo(db *DB) *CommandRepo {
	return &CommandRepo{db: db}
}

func (r *CommandRepo) Save(ctx context.Context, c *command.Command) error {
	if c.ID() == 0 {
		return r.insert(ctx, c)
	}
	return r.update(ctx, c)
}

func (r *CommandRepo) insert(ctx context.Context, c *command.Command) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO commands(name, body, trigger, status, result, error,
			queued_at, started_at, ended_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.Name(),
		nullableBlob(c.Body()),
		string(c.Trigger()),
		string(c.Status()),
		string(c.Result()),
		c.Error(),
		c.QueuedAt().UnixMilli(),
		nullableMillis(c.StartedAt()),
		nullableMillis(c.EndedAt()),
	)
	if err != nil {
		return fmt.Errorf("insert command: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	c.SetID(command.CommandID(id))
	return nil
}

func (r *CommandRepo) update(ctx context.Context, c *command.Command) error {
	_, err := r.db.ExecCtx(ctx, `
		UPDATE commands SET
			status = ?, result = ?, error = ?,
			started_at = ?, ended_at = ?
		WHERE id = ?`,
		string(c.Status()),
		string(c.Result()),
		c.Error(),
		nullableMillis(c.StartedAt()),
		nullableMillis(c.EndedAt()),
		int64(c.ID()),
	)
	return err
}

const commandColumns = `id, name, body, trigger, status, result, error,
	queued_at, started_at, ended_at`

func (r *CommandRepo) ByID(ctx context.Context, id command.CommandID) (*command.Command, error) {
	row := r.db.QueryRowCtx(ctx, `SELECT `+commandColumns+` FROM commands WHERE id = ?`, int64(id))
	return scanCommand(row)
}

func (r *CommandRepo) List(ctx context.Context, limit int) ([]*command.Command, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.QueryCtx(ctx, `SELECT `+commandColumns+` FROM commands ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*command.Command, 0, limit)
	for rows.Next() {
		c, err := scanCommandFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ClaimNext: oldest queued row → flip to running atomically.
// SQLite doesn't support RETURNING the new state across all versions
// we target, so we do this in a tx: SELECT id, UPDATE, then re-load.
func (r *CommandRepo) ClaimNext(ctx context.Context) (*command.Command, error) {
	now := time.Now().UTC()
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var id int64
	row := tx.QueryRowContext(ctx, `
		SELECT id FROM commands
		WHERE status = 'queued'
		ORDER BY queued_at ASC LIMIT 1`)
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE commands SET status = 'running', started_at = ?
		WHERE id = ? AND status = 'queued'`,
		now.UnixMilli(), id,
	)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Race with another claimer (shouldn't happen with our single
		// worker, but defend anyway).
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.ByID(ctx, command.CommandID(id))
}

// ResetStaleClaims flips any 'running' row whose started_at is older
// than `olderMillis` back to 'queued'. Called once at startup so a
// crash mid-handler doesn't park a command in 'running' forever.
func (r *CommandRepo) ResetStaleClaims(ctx context.Context, olderMillis int64) (int, error) {
	res, err := r.db.ExecCtx(ctx, `
		UPDATE commands SET status = 'queued', started_at = NULL
		WHERE status = 'running' AND (started_at IS NULL OR started_at < ?)`,
		olderMillis,
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func scanCommand(row *sql.Row) (*command.Command, error) {
	return scanCommandFromRows(row)
}

type cmdScanner interface {
	Scan(dest ...any) error
}

func scanCommandFromRows(s cmdScanner) (*command.Command, error) {
	var (
		id         int64
		name       string
		body       []byte
		trigger    string
		status     string
		result     string
		errMsg     string
		queuedMs   int64
		startedMs  sql.NullInt64
		endedMs    sql.NullInt64
	)
	if err := s.Scan(
		&id, &name, &body, &trigger, &status, &result, &errMsg,
		&queuedMs, &startedMs, &endedMs,
	); err != nil {
		return nil, err
	}
	r := command.Rehydrate{
		ID:       command.CommandID(id),
		Name:     name,
		Body:     body,
		Trigger:  command.Trigger(trigger),
		Status:   command.Status(status),
		Result:   command.Result(result),
		Error:    errMsg,
		QueuedAt: time.UnixMilli(queuedMs).UTC(),
	}
	if startedMs.Valid {
		r.StartedAt = time.UnixMilli(startedMs.Int64).UTC()
	}
	if endedMs.Valid {
		r.EndedAt = time.UnixMilli(endedMs.Int64).UTC()
	}
	return command.From(r), nil
}
