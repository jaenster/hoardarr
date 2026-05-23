package sqlite

import (
	"context"
	"fmt"
	"time"

	appsystem "github.com/jaenster/hoardarr/internal/app/system"
)

// SpeedHistoryRepo persists downsampled throughput samples for the
// speed-history endpoint. One row per minute; bucket_at is unix-seconds
// aligned to the minute boundary by the caller.
type SpeedHistoryRepo struct {
	db *DB
}

// Compile-time check.
var _ appsystem.SpeedHistoryStore = (*SpeedHistoryRepo)(nil)

// NewSpeedHistoryRepo wires the repo over db.
func NewSpeedHistoryRepo(db *DB) *SpeedHistoryRepo {
	return &SpeedHistoryRepo{db: db}
}

// Append inserts or overwrites the row at s.At. Idempotent — a flusher
// that fires twice for the same minute (e.g. across a restart) leaves
// only the latest value.
func (r *SpeedHistoryRepo) Append(ctx context.Context, s appsystem.SpeedSample) error {
	bucket := s.At.UTC().Unix()
	bucket -= bucket % 60
	_, err := r.db.ExecCtx(ctx, `
		INSERT INTO speed_history(bucket_at, bytes_per_sec) VALUES (?, ?)
		ON CONFLICT(bucket_at) DO UPDATE SET bytes_per_sec = excluded.bytes_per_sec`,
		bucket, s.BytesPerSec)
	if err != nil {
		return fmt.Errorf("append speed_history: %w", err)
	}
	return nil
}

// Range loads samples in [from, to] inclusive, oldest first.
func (r *SpeedHistoryRepo) Range(ctx context.Context, from, to time.Time) ([]appsystem.SpeedSample, error) {
	rows, err := r.db.QueryCtx(ctx, `
		SELECT bucket_at, bytes_per_sec
		FROM speed_history
		WHERE bucket_at >= ? AND bucket_at <= ?
		ORDER BY bucket_at ASC`,
		from.UTC().Unix(), to.UTC().Unix())
	if err != nil {
		return nil, fmt.Errorf("query speed_history: %w", err)
	}
	defer rows.Close()
	out := make([]appsystem.SpeedSample, 0, 128)
	for rows.Next() {
		var sec int64
		var bps int64
		if err := rows.Scan(&sec, &bps); err != nil {
			return nil, fmt.Errorf("scan speed_history: %w", err)
		}
		out = append(out, appsystem.SpeedSample{
			At:          time.Unix(sec, 0).UTC(),
			BytesPerSec: bps,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter speed_history: %w", err)
	}
	return out, nil
}

// Purge deletes rows older than the cutoff. Returns the number removed.
func (r *SpeedHistoryRepo) Purge(ctx context.Context, before time.Time) (int64, error) {
	res, err := r.db.ExecCtx(ctx, `DELETE FROM speed_history WHERE bucket_at < ?`, before.UTC().Unix())
	if err != nil {
		return 0, fmt.Errorf("purge speed_history: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("purge rows affected: %w", err)
	}
	return n, nil
}
