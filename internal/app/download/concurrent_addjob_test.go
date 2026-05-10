package download

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/eventbus/memory"
	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/domain/download"
)

// TestAddJob_ConcurrentSameNZB hammers AddJob with N parallel goroutines
// uploading the same NZB bytes. Exactly one should win the INSERT;
// every other call must surface ErrDuplicateNZB without leaking a
// half-committed job, and all callers should converge on the same
// JobID.
func TestAddJob_ConcurrentSameNZB(t *testing.T) {
	f := newOrchestratorFixture(t)
	svc := NewAddJobService(f.repo, f.bus, f.txm, nil)

	const nzbBody = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head><meta type="title">Concurrent</meta></head>
  <file poster="x" date="0" subject='[1/1] - "concurrent.bin" yEnc'>
    <groups><group>g</group></groups>
    <segments>
      <segment bytes="100" number="1">m1@h</segment>
    </segments>
  </file>
</nzb>`

	const N = 12
	var (
		wg        sync.WaitGroup
		successes atomic.Int32
		dupes     atomic.Int32
		ids       sync.Map // download.JobID → struct{}
	)

	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, err := svc.AddJob(context.Background(), AddJobCmd{
				NZB: bytes.NewReader([]byte(nzbBody)),
			})
			if errors.Is(err, ErrDuplicateNZB) {
				dupes.Add(1)
				ids.Store(id, struct{}{})
				return
			}
			if err != nil {
				t.Errorf("AddJob: %v", err)
				return
			}
			successes.Add(1)
			ids.Store(id, struct{}{})
		}()
	}
	close(start)

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent AddJob did not finish within 10s")
	}

	if successes.Load() != 1 {
		t.Errorf("successes = %d; want exactly 1", successes.Load())
	}
	if dupes.Load() != N-1 {
		t.Errorf("dupes = %d; want %d", dupes.Load(), N-1)
	}
	// Every caller should have observed the same JobID.
	count := 0
	ids.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Errorf("distinct JobIDs returned = %d; want 1", count)
	}

	// Verify exactly one row in the DB.
	all, err := f.repo.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("DB rows = %d; want 1", len(all))
	}
}

// silence unused — these come from the parent file in the same pkg.
var _ = memory.New
var _ = sqlite.NewServerRepo
var _ download.JobID = 0
