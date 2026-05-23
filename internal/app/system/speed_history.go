package system

import (
	"context"
	"time"
)

// SpeedSample is one downsampled throughput point: the average rate
// observed during a one-minute window starting at At.
type SpeedSample struct {
	At          time.Time `json:"at"`
	BytesPerSec int64     `json:"bytes_per_sec"`
}

// SpeedHistoryStore persists downsampled throughput samples for
// long-range queries beyond the in-memory ring. Implementations live
// in the persistence adapters (SQLite today, others tomorrow).
//
// Resolution is fixed at one row per minute; callers align At to the
// minute boundary themselves so the store can use the timestamp as the
// primary key.
type SpeedHistoryStore interface {
	Append(ctx context.Context, s SpeedSample) error
	Range(ctx context.Context, from, to time.Time) ([]SpeedSample, error)
	Purge(ctx context.Context, before time.Time) (int64, error)
}
