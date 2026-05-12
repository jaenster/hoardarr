package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/schedule"
)

func TestScheduleRepo_SaveByNameLoad(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := NewScheduleRepo(db)

	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	task, err := schedule.New(schedule.NewParams{
		Name:     "test.recurring",
		Kind:     schedule.KindRecurring,
		Cadence:  5 * time.Minute,
		FirstRun: now.Add(5 * time.Minute),
	}, now)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := repo.Save(ctx, task); err != nil {
		t.Fatalf("save: %v", err)
	}
	if task.ID() == 0 {
		t.Fatal("save did not assign id")
	}

	got, err := repo.ByName(ctx, "test.recurring")
	if err != nil {
		t.Fatalf("byname: %v", err)
	}
	if got.Cadence() != 5*time.Minute {
		t.Errorf("cadence = %v; want 5m", got.Cadence())
	}
	if got.Kind() != schedule.KindRecurring {
		t.Errorf("kind = %v; want recurring", got.Kind())
	}
}

func TestScheduleRepo_ClaimDueAtomic(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := NewScheduleRepo(db)

	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	// Insert one due task.
	t1, _ := schedule.New(schedule.NewParams{
		Name:     "due.task",
		Kind:     schedule.KindRecurring,
		Cadence:  time.Minute,
		FirstRun: now.Add(-10 * time.Second),
	}, now)
	if err := repo.Save(ctx, t1); err != nil {
		t.Fatalf("save: %v", err)
	}
	// And one not-due task.
	t2, _ := schedule.New(schedule.NewParams{
		Name:     "future.task",
		Kind:     schedule.KindRecurring,
		Cadence:  time.Minute,
		FirstRun: now.Add(time.Hour),
	}, now)
	if err := repo.Save(ctx, t2); err != nil {
		t.Fatalf("save: %v", err)
	}

	claimed, err := repo.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d; want 1", len(claimed))
	}
	if claimed[0].Name() != "due.task" {
		t.Errorf("claimed wrong task: %s", claimed[0].Name())
	}
	if claimed[0].Status() != schedule.StatusRunning {
		t.Errorf("status = %s; want running", claimed[0].Status())
	}

	// Second claim should return nothing — the task is now 'running'.
	again, err := repo.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("claim2: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second claim returned %d tasks; want 0", len(again))
	}
}

func TestScheduleRepo_ResetStaleClaims(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := NewScheduleRepo(db)

	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	task, _ := schedule.New(schedule.NewParams{
		Name:     "stuck.task",
		Kind:     schedule.KindRecurring,
		Cadence:  time.Minute,
		FirstRun: now.Add(-10 * time.Second),
	}, now)
	if err := repo.Save(ctx, task); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Simulate a crash: claim it, but never release.
	claimed, _ := repo.ClaimDue(ctx, now, 1)
	if len(claimed) != 1 {
		t.Fatalf("setup: claimed %d", len(claimed))
	}

	// Reset stale claims.
	n, err := repo.ResetStaleClaims(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if n != 1 {
		t.Errorf("reset count = %d; want 1", n)
	}

	// Now it should be claimable again.
	again, err := repo.ClaimDue(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if len(again) != 1 {
		t.Errorf("re-claim count = %d; want 1", len(again))
	}
}

func TestScheduleRepo_OneshotDisablesAfterSuccess(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := NewScheduleRepo(db)

	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	task, _ := schedule.New(schedule.NewParams{
		Name:     "oneshot.task",
		Kind:     schedule.KindOneshot,
		FirstRun: now.Add(-10 * time.Second),
	}, now)
	if err := repo.Save(ctx, task); err != nil {
		t.Fatalf("save: %v", err)
	}

	claimed, _ := repo.ClaimDue(ctx, now, 1)
	if len(claimed) != 1 {
		t.Fatalf("claimed %d", len(claimed))
	}
	claimed[0].MarkSucceeded(now)
	if err := repo.Save(ctx, claimed[0]); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := repo.ByName(ctx, "oneshot.task")
	if err != nil {
		t.Fatalf("byname: %v", err)
	}
	if got.Enabled() {
		t.Error("oneshot still enabled after success; want disabled")
	}
}
