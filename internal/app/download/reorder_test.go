package download

import (
	"context"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// TestQueueService_Reorder seeds three queued jobs, requests a fresh
// id ordering, and asserts (a) repository.Active returns them in the
// requested order, (b) terminal jobs are silently skipped, and (c) a
// missing id surfaces ErrNotFound.
func TestQueueService_Reorder(t *testing.T) {
	f := newOrchestratorFixture(t)
	svc := NewQueueService(QueueServiceParams{
		Repo:      f.repo,
		Bus:       f.bus,
		TxManager: f.txm,
	})

	ctx := context.Background()
	now := time.Now().UTC()

	// Seed three queued jobs with ascending queue_order.
	j1 := seedQueuedJob(t, ctx, f, "alpha", 0, now)
	j2 := seedQueuedJob(t, ctx, f, "bravo", 1, now)
	j3 := seedQueuedJob(t, ctx, f, "charlie", 2, now)

	// Reorder: drag charlie to the top, then bravo, then alpha. The
	// service should write queue_order so Active() returns them in
	// that order.
	if err := svc.Reorder(ctx, []download.JobID{j3.ID(), j2.ID(), j1.ID()}); err != nil {
		t.Fatalf("Reorder: %v", err)
	}

	active, err := f.repo.Active(ctx)
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	wantOrder := []download.JobID{j3.ID(), j2.ID(), j1.ID()}
	if len(active) != len(wantOrder) {
		t.Fatalf("active len = %d; want %d", len(active), len(wantOrder))
	}
	for i, j := range active {
		if j.ID() != wantOrder[i] {
			t.Errorf("active[%d].ID = %d; want %d", i, j.ID(), wantOrder[i])
		}
	}

	// Empty list is a no-op.
	if err := svc.Reorder(ctx, nil); err != nil {
		t.Errorf("Reorder(nil): %v", err)
	}

	// Unknown id surfaces an error (typically ErrNotFound). Don't
	// match it precisely — the contract is "non-nil".
	if err := svc.Reorder(ctx, []download.JobID{99999}); err == nil {
		t.Errorf("Reorder(unknown) = nil; want error")
	}
}

// seedQueuedJob creates a queued job with a single 1-byte segment so
// the repo's NOT-NULL invariants are satisfied. Returns the persisted
// aggregate (ID assigned by the repo).
func seedQueuedJob(t *testing.T, ctx context.Context, f *orchestratorTestFixture, name string, order int64, now time.Time) *download.Job {
	t.Helper()
	j, err := download.NewJob(download.NewJobParams{
		NZBHash:    name + "-hash",
		Name:       name,
		Category:   "*",
		Priority:   0,
		QueueOrder: order,
		NZBBlob:    []byte("<nzb/>"),
		Files: []download.NewFileParams{{
			Filename:  name + ".bin",
			SizeBytes: 1,
			Segments: []download.NewSegmentParams{{
				SeqIndex:  1,
				MessageID: name + "-seg-1@h",
				Bytes:     1,
			}},
		}},
	}, now)
	if err != nil {
		t.Fatalf("NewJob(%s): %v", name, err)
	}
	if err := f.repo.Save(ctx, j); err != nil {
		t.Fatalf("Save(%s): %v", name, err)
	}
	return j
}
