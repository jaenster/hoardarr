package download

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/eventbus/memory"
	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// flakyFetcher returns ErrFlaky for the first failuresBefore-N calls
// per messageID, then returns the canned body. Used to test that the
// orchestrator retries transient failures up to MaxAttempts.
type flakyFetcher struct {
	bodies          map[string][]byte
	failuresBefore  int
	calls           atomic.Int64
	perMsgFailCount map[string]*atomic.Int32
}

func newFlakyFetcher(failuresBefore int, bodies map[string][]byte) *flakyFetcher {
	f := &flakyFetcher{
		bodies:          bodies,
		failuresBefore:  failuresBefore,
		perMsgFailCount: make(map[string]*atomic.Int32, len(bodies)),
	}
	for k := range bodies {
		f.perMsgFailCount[k] = &atomic.Int32{}
	}
	return f
}

func (f *flakyFetcher) Fetch(_ context.Context, _ domainserver.ServerID, messageID string) (io.ReadCloser, error) {
	f.calls.Add(1)
	c := f.perMsgFailCount[messageID]
	if c == nil {
		return nil, errors.New("unknown message-id")
	}
	if int(c.Add(1)) <= f.failuresBefore {
		return nil, errors.New("simulated transient")
	}
	body, ok := f.bodies[messageID]
	if !ok {
		return nil, errors.New("no body")
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

// helper to build a valid yEnc body for a single-part article.
func yencSinglePart(payload []byte, name string) []byte {
	var buf bytes.Buffer
	buf.WriteString("=ybegin line=128 size=" + strconv.Itoa(len(payload)) + " name=" + name + "\r\n")
	col := 0
	for _, b := range payload {
		out := byte(b + 42)
		critical := out == 0x00 || out == 0x0A || out == 0x0D || out == '='
		if critical || (col == 0 && (out == '\t' || out == ' ' || out == '.')) {
			buf.WriteByte('=')
			buf.WriteByte(out + 64)
			col += 2
		} else {
			buf.WriteByte(out)
			col++
		}
		if col >= 128 {
			buf.WriteString("\r\n")
			col = 0
		}
	}
	if col > 0 {
		buf.WriteString("\r\n")
	}
	crc := crc32.ChecksumIEEE(payload)
	buf.WriteString("=yend size=" + strconv.Itoa(len(payload)) + " crc32=" +
		toHex8(crc) + "\r\n")
	return buf.Bytes()
}

func toHex8(v uint32) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		out[i] = hexdigits[v&0xf]
		v >>= 4
	}
	return string(out)
}

// orchestratorTestFixture wires the minimal stack needed to drive the
// orchestrator: in-memory bus, real SQLite, real txm, real repos.
type orchestratorTestFixture struct {
	t       *testing.T
	bus     event.Bus
	repo    *sqlite.JobRepo
	txm     *sqlite.TxManager
	jobDir  string
}

func newOrchestratorFixture(t *testing.T) *orchestratorTestFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := sqlite.Open(ctx, dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return &orchestratorTestFixture{
		t:      t,
		bus:    memory.New(),
		repo:   sqlite.NewJobRepo(db),
		txm:    sqlite.NewTxManager(db),
		jobDir: filepath.Join(dir, "incomplete"),
	}
}

func TestOrchestrator_RetriesTransientFailures(t *testing.T) {
	f := newOrchestratorFixture(t)
	ctx := context.Background()
	now := time.UnixMilli(1).UTC()

	payload := []byte("hello hoardarr retry test, twelve dozen bytes here ok")
	bodies := map[string][]byte{
		"flake@host": yencSinglePart(payload, "f.bin"),
	}
	const failures = 2
	fetcher := newFlakyFetcher(failures, bodies)

	job, err := download.NewJob(download.NewJobParams{
		NZBHash: "retry-test",
		Name:    "retry",
		NZBBlob: []byte("<nzb/>"),
		Files: []download.NewFileParams{
			{
				Filename:  "f.bin",
				SizeBytes: int64(len(payload)),
				Segments: []download.NewSegmentParams{
					{SeqIndex: 1, MessageID: "flake@host", Bytes: int64(len(payload))},
				},
			},
		},
	}, now)
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	if err := f.repo.Save(ctx, job); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = job.PullEvents()

	orch := NewOrchestrator(f.repo, fetcher, f.bus, f.txm,
		1, 1, f.jobDir,
		OrchestratorOptions{
			MaxAttempts: 3,
			BaseBackoff: 1 * time.Millisecond, // fast tests
		},
	)
	if err := orch.Run(ctx, job.ID()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	final, err := f.repo.ByID(ctx, job.ID())
	if err != nil {
		t.Fatalf("ByID: %v", err)
	}
	segs := final.Files()[0].Segments()
	if got := segs[0].State(); got != download.SegmentStateDone {
		t.Errorf("segment state = %s; want done", got)
	}
	// fetcher.calls counts every Fetch invocation across attempts.
	if got := fetcher.calls.Load(); got != int64(failures+1) {
		t.Errorf("fetch calls = %d; want %d (= %d failures + 1 success)",
			got, failures+1, failures)
	}
	// Verify file on disk matches.
	tmpPath := filepath.Join(f.jobDir, strconv.FormatInt(int64(final.ID()), 10),
		strconv.FormatInt(int64(segs[0].FileID()), 10)+".tmp")
	got := mustReadAll(t, tmpPath)
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch")
	}
}

func TestOrchestrator_GivesUpAfterMaxAttempts(t *testing.T) {
	f := newOrchestratorFixture(t)
	ctx := context.Background()
	now := time.UnixMilli(1).UTC()

	bodies := map[string][]byte{"dead@host": []byte("unused")}
	const maxAttempts = 3
	// failuresBefore set to maxAttempts + 1 so we never succeed.
	fetcher := newFlakyFetcher(maxAttempts+1, bodies)

	job, _ := download.NewJob(download.NewJobParams{
		NZBHash: "fail-test",
		Name:    "fail",
		NZBBlob: []byte("<nzb/>"),
		Files: []download.NewFileParams{
			{
				Filename:  "x.bin",
				SizeBytes: 6,
				Segments: []download.NewSegmentParams{
					{SeqIndex: 1, MessageID: "dead@host", Bytes: 6},
				},
			},
		},
	}, now)
	if err := f.repo.Save(ctx, job); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = job.PullEvents()

	// MaxDurableAttempts: 1 gives the segment one durable-retry pass
	// after the in-process retry budget exhausts, then escalates to
	// terminal failure. Total fetch calls = maxAttempts * (1 + 1
	// durable retry) = 6. DurableBackoff is squeezed to ~zero so the
	// outer Run loop doesn't sleep between batches.
	orch := NewOrchestrator(f.repo, fetcher, f.bus, f.txm,
		1, 1, f.jobDir,
		OrchestratorOptions{
			MaxAttempts:        maxAttempts,
			BaseBackoff:        1 * time.Millisecond,
			MaxDurableAttempts: 1,
			DurableBackoffBase: 1 * time.Millisecond,
			DurableBackoffMax:  1 * time.Millisecond,
			MaxPollGap:         1 * time.Millisecond,
		},
	)
	if err := orch.Run(ctx, job.ID()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	final, _ := f.repo.ByID(ctx, job.ID())
	segs := final.Files()[0].Segments()
	if got := segs[0].State(); got != download.SegmentStateFailed {
		t.Errorf("segment state = %s; want failed", got)
	}
	if got, want := fetcher.calls.Load(), int64(maxAttempts*2); got != want {
		t.Errorf("fetch calls = %d; want %d (max attempts × (1 + durable retry))", got, want)
	}
}

// TestOrchestrator_DurableRetry_TransientThenSuccess proves the
// happy-path of durable retry: a segment fails enough times to
// exhaust the in-process retry budget, gets deferred via
// MarkSegmentForRetry with next_retry_at = now+backoff, and on the
// outer Run loop's next poll iteration is dispatched again and
// succeeds.
func TestOrchestrator_DurableRetry_TransientThenSuccess(t *testing.T) {
	f := newOrchestratorFixture(t)
	ctx := context.Background()
	now := time.UnixMilli(1).UTC()

	payload := []byte("hello!")
	bodies := map[string][]byte{"flaky@host": yencSinglePart(payload, "f.bin")}
	// 5 failures: in-process attempts run 3 (fail) → durable retry → 2 more (fail) → durable retry → succeeds on 6th call.
	const maxAttempts = 3
	fetcher := newFlakyFetcher(5, bodies)

	job, _ := download.NewJob(download.NewJobParams{
		NZBHash: "durable-happy",
		Name:    "release",
		NZBBlob: []byte("<nzb/>"),
		Files: []download.NewFileParams{
			{
				Filename:  "f.bin",
				SizeBytes: int64(len(payload)),
				Segments:  []download.NewSegmentParams{{SeqIndex: 1, MessageID: "flaky@host", Bytes: int64(len(payload))}},
			},
		},
	}, now)
	if err := f.repo.Save(ctx, job); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = job.PullEvents()

	orch := NewOrchestrator(f.repo, fetcher, f.bus, f.txm,
		1, 1, f.jobDir,
		OrchestratorOptions{
			MaxAttempts:        maxAttempts,
			BaseBackoff:        1 * time.Millisecond,
			MaxDurableAttempts: 5,
			DurableBackoffBase: 1 * time.Millisecond,
			DurableBackoffMax:  1 * time.Millisecond,
			MaxPollGap:         1 * time.Millisecond,
		},
	)
	if err := orch.Run(ctx, job.ID()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	final, _ := f.repo.ByID(ctx, job.ID())
	segs := final.Files()[0].Segments()
	if got := segs[0].State(); got != download.SegmentStateDone {
		t.Errorf("segment state = %s; want done (durable retry should have caught the success on a later batch)", got)
	}
	// In-process retries don't bump segment.Attempts; only MarkPendingRetry does.
	// Dispatch 1 (3 in-process attempts: all fail, calls 1-3) → durable retry #1
	// (Attempts → 1). Dispatch 2 (in-process attempts 1-2 fail = calls 4-5,
	// attempt 3 succeeds = call 6) → done. Net: exactly one durable retry.
	if got := segs[0].Attempts(); got != 1 {
		t.Errorf("seg.Attempts = %d; want 1 (one durable retry before success)", got)
	}
}

// TestOrchestrator_DurableRetry_RespectsNextRetryAt validates that the
// outer Run loop honours next_retry_at: a segment scheduled for a
// future time must not be picked up until that time has passed.
func TestOrchestrator_DurableRetry_RespectsNextRetryAt(t *testing.T) {
	f := newOrchestratorFixture(t)
	ctx := context.Background()
	now := time.UnixMilli(1).UTC()

	payload := []byte("ok-body")
	bodies := map[string][]byte{"slow@host": yencSinglePart(payload, "s.bin")}
	// Permanent failure: ensures the orchestrator decides "transient"
	// and schedules a durable retry. We fix the backoff small so the
	// test runs quickly; we still assert that *some* wall-clock time
	// elapsed between dispatches.
	fetcher := newFlakyFetcher(99, bodies)

	job, _ := download.NewJob(download.NewJobParams{
		NZBHash: "durable-defer",
		Name:    "release",
		NZBBlob: []byte("<nzb/>"),
		Files: []download.NewFileParams{
			{
				Filename:  "s.bin",
				SizeBytes: int64(len(payload)),
				Segments:  []download.NewSegmentParams{{SeqIndex: 1, MessageID: "slow@host", Bytes: int64(len(payload))}},
			},
		},
	}, now)
	if err := f.repo.Save(ctx, job); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = job.PullEvents()

	orch := NewOrchestrator(f.repo, fetcher, f.bus, f.txm,
		1, 1, f.jobDir,
		OrchestratorOptions{
			MaxAttempts:        1, // one in-process attempt per dispatch — fail fast
			BaseBackoff:        1 * time.Millisecond,
			MaxDurableAttempts: 2,
			DurableBackoffBase: 50 * time.Millisecond,
			DurableBackoffMax:  50 * time.Millisecond,
			MaxPollGap:         50 * time.Millisecond,
		},
	)
	start := time.Now()
	if err := orch.Run(ctx, job.ID()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)

	// 3 dispatches total: initial + 2 durable retries (then terminal).
	// Between each dispatch the orchestrator sleeps DurableBackoffBase
	// (50ms). Floor: 2 * 50ms = 100ms. We assert at least 80ms to leave
	// room for scheduler jitter on busy CI.
	if elapsed < 80*time.Millisecond {
		t.Errorf("elapsed = %v; want ≥80ms (must wait for next_retry_at)", elapsed)
	}
	if got := fetcher.calls.Load(); got != 3 {
		t.Errorf("fetch calls = %d; want 3 (initial + 2 durable retries)", got)
	}

	final, _ := f.repo.ByID(ctx, job.ID())
	segs := final.Files()[0].Segments()
	if got := segs[0].State(); got != download.SegmentStateFailed {
		t.Errorf("final state = %s; want failed (exhausted durable retries)", got)
	}
}

func mustReadAll(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}
