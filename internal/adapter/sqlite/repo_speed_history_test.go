package sqlite

import (
	"context"
	"testing"
	"time"

	appsystem "github.com/jaenster/hoardarr/internal/app/system"
)

func TestSpeedHistoryRepo_AppendReadRange(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	r := NewSpeedHistoryRepo(db)

	base := time.Unix(1_700_000_000, 0).UTC()
	base = base.Truncate(time.Minute)

	// Write a handful of minute samples.
	for i := 0; i < 5; i++ {
		s := appsystem.SpeedSample{
			At:          base.Add(time.Duration(i) * time.Minute),
			BytesPerSec: int64((i + 1) * 1_000_000),
		}
		if err := r.Append(ctx, s); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Read the middle 3.
	got, err := r.Range(ctx, base.Add(time.Minute), base.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d; want 3", len(got))
	}
	for i, s := range got {
		wantBps := int64((i + 2) * 1_000_000)
		if s.BytesPerSec != wantBps {
			t.Errorf("got[%d].BytesPerSec = %d; want %d", i, s.BytesPerSec, wantBps)
		}
	}
}

func TestSpeedHistoryRepo_AppendUpsertsOnMinute(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	r := NewSpeedHistoryRepo(db)

	// Two appends inside the same minute: the second overwrites the first.
	// Start at a minute boundary so both writes land in the same bucket.
	at := time.Unix(1_700_000_040, 0).UTC() // :00 of a minute
	if err := r.Append(ctx, appsystem.SpeedSample{At: at.Add(5 * time.Second), BytesPerSec: 100}); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := r.Append(ctx, appsystem.SpeedSample{At: at.Add(45 * time.Second), BytesPerSec: 200}); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	got, err := r.Range(ctx, at.Add(-time.Minute), at.Add(time.Minute))
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d; want 1 (same-minute upsert)", len(got))
	}
	if got[0].BytesPerSec != 200 {
		t.Errorf("BytesPerSec = %d; want 200 (latest write wins)", got[0].BytesPerSec)
	}
}

func TestSpeedHistoryRepo_Purge(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	r := NewSpeedHistoryRepo(db)

	base := time.Unix(1_700_000_000, 0).UTC().Truncate(time.Minute)
	for i := 0; i < 10; i++ {
		_ = r.Append(ctx, appsystem.SpeedSample{
			At:          base.Add(time.Duration(i) * time.Minute),
			BytesPerSec: 1,
		})
	}
	n, err := r.Purge(ctx, base.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 5 {
		t.Errorf("purged = %d; want 5", n)
	}
	left, err := r.Range(ctx, base, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(left) != 5 {
		t.Errorf("remaining = %d; want 5", len(left))
	}
}
