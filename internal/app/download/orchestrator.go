package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/adapter/yenc"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/server"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// Orchestrator drives one Job from "queued" to "download_complete" by
// dispatching segment fetches to a worker pool, decoding yEnc, writing
// to the per-file tmp file at known offsets, and feeding completion
// results to a 100ms-batched drainer that updates the aggregate and
// publishes events atomically.
//
// M1 scope: single server, single job at a time. The CLI invokes Run
// synchronously and exits when it returns. M2 wraps this in a long-
// lived orchestrator that subscribes to JobCreated for multi-job
// concurrency.
type Orchestrator struct {
	repo    download.JobRepository
	fetcher download.ArticleFetcher
	bus     event.Bus
	tx      tx.TransactionManager
	logger  *slog.Logger
	now     func() time.Time

	srv           server.ServerID
	workers       int
	incompleteDir string

	// drainer cadence
	flushInterval time.Duration
	flushBatchMax int

	// segment retry
	maxAttempts int
	baseBackoff time.Duration
	// poolWait is the wait between re-checks when the fetcher reports
	// ErrNoPoolsAvailable. It is intentionally larger than baseBackoff
	// — pool availability changes on operator action (adding a server,
	// quota reset), not on the ms timescale.
	poolWait time.Duration
}

// OrchestratorOptions tunes runtime behaviour. Zero values are sensible.
type OrchestratorOptions struct {
	// Workers caps concurrent in-flight fetches. Defaults to the
	// server's MaxConns(). The fetcher's pool is the actual bound;
	// this number is the upper bound on goroutines spawned.
	Workers int

	// FlushInterval is how often the drainer flushes a batch of
	// completion results to the DB and bus. Default 100ms.
	FlushInterval time.Duration

	// FlushBatchMax forces a flush when batch reaches this size.
	// Default 256.
	FlushBatchMax int

	// MaxAttempts caps the number of fetch attempts before a
	// segment moves to terminal "failed". 1 = no retry (the only
	// attempt is the first). Default 3.
	//
	// ErrArticleMissing (430) bypasses retry — that's a definitive
	// "no such article", and re-asking the same server won't help.
	// Multi-server failover (M7) will fan 430s out to the next
	// server before terminal-missing.
	MaxAttempts int

	// BaseBackoff is the first-retry delay; each subsequent retry
	// doubles. Default 200ms (so 200 / 400 / 800 ms for 3 attempts).
	BaseBackoff time.Duration

	// PoolWait is how long the runner sleeps between re-checks when
	// no pool is available (ErrNoPoolsAvailable). Does not consume
	// the retry budget — see processSegment. Default 5s.
	PoolWait time.Duration

	Logger *slog.Logger
	Now    func() time.Time
}

// NewOrchestrator constructs the orchestrator.
//
// incompleteDir is the absolute path under which per-job tmp files
// land (incompleteDir/<jobid>/<fileid>.tmp). srv is the single server
// used for fetches in M1.
func NewOrchestrator(
	repo download.JobRepository,
	fetcher download.ArticleFetcher,
	bus event.Bus,
	txm tx.TransactionManager,
	srv server.ServerID,
	workers int,
	incompleteDir string,
	opts OrchestratorOptions,
) *Orchestrator {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = 100 * time.Millisecond
	}
	if opts.FlushBatchMax == 0 {
		opts.FlushBatchMax = 256
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.BaseBackoff == 0 {
		opts.BaseBackoff = 200 * time.Millisecond
	}
	if opts.PoolWait == 0 {
		opts.PoolWait = 5 * time.Second
	}
	if workers <= 0 {
		workers = 1
	}
	if opts.Workers > 0 {
		workers = opts.Workers
	}
	return &Orchestrator{
		repo:          repo,
		fetcher:       fetcher,
		bus:           bus,
		tx:            txm,
		logger:        opts.Logger,
		now:           opts.Now,
		srv:           srv,
		workers:       workers,
		incompleteDir: incompleteDir,
		flushInterval: opts.FlushInterval,
		flushBatchMax: opts.FlushBatchMax,
		maxAttempts:   opts.MaxAttempts,
		baseBackoff:   opts.BaseBackoff,
		poolWait:      opts.PoolWait,
	}
}

// Run drives jobID to download_complete. Returns when every segment
// has reached a terminal state (done, missing, or failed) or ctx is
// cancelled.
//
// The job is loaded fresh from the repo; any inflight rows from a
// prior crash are reset to pending before dispatch. This is M1's
// crash-recovery strategy: restart the orchestrator, re-fetch in
// progress segments. Idempotent WriteAt at known offsets makes it
// safe.
func (o *Orchestrator) Run(ctx context.Context, jobID download.JobID) error {
	job, err := o.repo.ByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load job %d: %w", jobID, err)
	}

	if reset := job.ResetInflightToPending(); reset > 0 {
		o.logger.Info("orchestrator: reset inflight segments", "job", jobID, "count", reset)
		if err := o.flushAggregate(ctx, job, nil); err != nil {
			return fmt.Errorf("persist reset: %w", err)
		}
	}

	if err := o.markStarted(ctx, job); err != nil {
		return fmt.Errorf("mark started: %w", err)
	}

	pending := job.PendingSegments()
	if len(pending) == 0 {
		o.logger.Info("orchestrator: no pending segments", "job", jobID)
		return nil
	}

	jobDir := filepath.Join(o.incompleteDir, fmt.Sprintf("%d", int64(job.ID())))
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return fmt.Errorf("create job dir: %w", err)
	}

	workCh := make(chan *download.Segment, o.workers*2)
	resultCh := make(chan segmentResult, o.workers*2)

	// Workers.
	var workerWG sync.WaitGroup
	for i := 0; i < o.workers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			o.workerLoop(ctx, job, jobDir, workCh, resultCh)
		}()
	}

	// Drainer.
	drainerDone := make(chan error, 1)
	go func() {
		drainerDone <- o.drainerLoop(ctx, job, resultCh)
	}()

	// Producer.
	go func() {
		defer close(workCh)
		for _, s := range pending {
			select {
			case workCh <- s:
			case <-ctx.Done():
				return
			}
		}
	}()

	workerWG.Wait()
	close(resultCh)
	if err := <-drainerDone; err != nil {
		return err
	}

	return nil
}

func (o *Orchestrator) markStarted(ctx context.Context, job *download.Job) error {
	return o.tx.InTx(ctx, func(ctx context.Context) error {
		job.MarkStarted(o.now())
		if err := o.repo.Save(ctx, job); err != nil {
			return err
		}
		return o.bus.Publish(ctx, job.PullEvents()...)
	})
}

// workerLoop pulls segments from in, fetches+decodes+writes each one,
// and pushes a segmentResult to out.
func (o *Orchestrator) workerLoop(
	ctx context.Context,
	job *download.Job,
	jobDir string,
	in <-chan *download.Segment,
	out chan<- segmentResult,
) {
	for seg := range in {
		res := o.processSegment(ctx, job, jobDir, seg)
		select {
		case out <- res:
		case <-ctx.Done():
			return
		}
	}
}

// processSegment runs one segment to completion, retrying transient
// failures up to maxAttempts. ErrArticleMissing (430) is terminal on
// the first attempt — re-asking the same server won't help.
//
// ErrNoPoolsAvailable is special: it means there is currently no enabled
// server to ask. That's expected if the operator queued NZBs before
// configuring any server, or temporarily disabled them all. We do NOT
// burn the retry budget on it — we just wait for pools to come back and
// re-attempt. The runner's ctx cancels if the job is paused/removed or
// the service stops, so this isn't an infinite loop.
func (o *Orchestrator) processSegment(
	ctx context.Context,
	job *download.Job,
	jobDir string,
	seg *download.Segment,
) segmentResult {
	var res segmentResult
	for attempt := 1; attempt <= o.maxAttempts; attempt++ {
		res = o.attemptSegment(ctx, job, jobDir, seg)
		if res.done || res.missing {
			return res
		}
		if ctx.Err() != nil {
			return segmentResult{seg: seg, cancelled: true}
		}
		// No pools available — wait for a server to appear, don't
		// burn the retry budget. Re-checks every poolWait window.
		if errors.Is(res.err, ErrNoPoolsAvailable) {
			o.logger.Info("orchestrator: no pools available; waiting",
				"job_id", int64(job.ID()),
				"segment_id", int64(seg.ID()),
			)
			select {
			case <-time.After(o.poolWait):
			case <-ctx.Done():
				return segmentResult{seg: seg, cancelled: true}
			}
			attempt-- // do not consume the budget
			continue
		}
		if attempt < o.maxAttempts {
			o.logger.Debug("orchestrator: retrying segment",
				"job_id", int64(job.ID()),
				"segment_id", int64(seg.ID()),
				"attempt", attempt,
				"err", res.err,
			)
			delay := o.baseBackoff << (attempt - 1)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return segmentResult{seg: seg, cancelled: true}
			}
		}
	}
	if res.err != nil {
		res.err = fmt.Errorf("after %d attempts: %w", o.maxAttempts, res.err)
	}
	return res
}

// attemptSegment performs a single fetch + decode + write. The outer
// processSegment retries on non-missing failures.
func (o *Orchestrator) attemptSegment(
	ctx context.Context,
	job *download.Job,
	jobDir string,
	seg *download.Segment,
) segmentResult {
	res := segmentResult{seg: seg}

	body, err := o.fetcher.Fetch(ctx, o.srv, seg.MessageID())
	if err != nil {
		if errors.Is(err, nntp.ErrArticleMissing) {
			res.missing = true
			return res
		}
		res.err = err
		return res
	}

	decoded, hdr, _, err := yenc.Decode(body)
	_ = body.Close()
	if err != nil {
		res.err = fmt.Errorf("yenc decode: %w", err)
		return res
	}

	file, _ := job.SegmentByID(seg.ID())
	if file == nil {
		res.err = fmt.Errorf("segment %d not in job aggregate", seg.ID())
		return res
	}

	// Compute write offset. yEnc multi-part carries =ypart begin
	// (1-based); single-part articles fall through with begin=1.
	offset := int64(0)
	if hdr.Begin > 0 {
		offset = hdr.Begin - 1
	}

	tmpPath := filepath.Join(jobDir, fmt.Sprintf("%d.tmp", int64(file.ID())))
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		res.err = fmt.Errorf("open tmp: %w", err)
		return res
	}
	defer f.Close()

	// Pre-allocate to declared size on first write. Truncate is
	// idempotent — subsequent calls are cheap no-ops on most file
	// systems (APFS does CoW; that's fine for our access pattern).
	if hdr.Size > 0 {
		if err := f.Truncate(hdr.Size); err != nil {
			res.err = fmt.Errorf("truncate: %w", err)
			return res
		}
	}

	if _, err := f.WriteAt(decoded, offset); err != nil {
		res.err = fmt.Errorf("writeat: %w", err)
		return res
	}

	res.done = true
	res.bytesOnDisk = int64(len(decoded))
	res.fileOffset = offset
	return res
}

// drainerLoop batches segmentResult into ~100ms windows and flushes
// them via flushBatch (which mutates the aggregate, persists, publishes).
func (o *Orchestrator) drainerLoop(ctx context.Context, job *download.Job, in <-chan segmentResult) error {
	ticker := time.NewTicker(o.flushInterval)
	defer ticker.Stop()

	var batch []segmentResult
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := o.flushBatch(ctx, job, batch)
		batch = batch[:0]
		return err
	}

	for {
		select {
		case r, ok := <-in:
			if !ok {
				return flush()
			}
			batch = append(batch, r)
			if len(batch) >= o.flushBatchMax {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			_ = flush()
			return ctx.Err()
		}
	}
}

// flushBatch applies a batch of results to the aggregate and persists
// in one transaction (segment row updates + job state save + outbox
// events).
//
// Cancelled segments are skipped (no state mutation, no DB update);
// they remain pending and will be re-dispatched on restart.
func (o *Orchestrator) flushBatch(ctx context.Context, job *download.Job, batch []segmentResult) error {
	now := o.now()
	persisted := batch[:0]
	for _, r := range batch {
		switch {
		case r.cancelled:
			// Skip — leave segment pending for the next runner.
			continue
		case r.done:
			if err := job.MarkSegmentDone(download.SegmentResult{
				SegmentID:   r.seg.ID(),
				BytesOnDisk: r.bytesOnDisk,
				FileOffset:  r.fileOffset,
			}, now); err != nil {
				o.logger.Error("MarkSegmentDone", "err", err)
			}
		case r.missing:
			if err := job.MarkSegmentMissing(r.seg.ID(), now); err != nil {
				o.logger.Error("MarkSegmentMissing", "err", err)
			}
		default:
			msg := "unknown failure"
			if r.err != nil {
				msg = r.err.Error()
			}
			if err := job.MarkSegmentFailed(r.seg.ID(), msg, now); err != nil {
				o.logger.Error("MarkSegmentFailed", "err", err)
			}
		}
		persisted = append(persisted, r)
	}
	return o.flushAggregate(ctx, job, persisted)
}

// flushAggregate persists the current job state and publishes pending
// events, plus any segment-level updates implied by batch.
func (o *Orchestrator) flushAggregate(ctx context.Context, job *download.Job, batch []segmentResult) error {
	updates := buildSegmentUpdates(job, batch)
	return o.tx.InTx(ctx, func(ctx context.Context) error {
		if len(updates) > 0 {
			if err := o.repo.UpdateSegmentBatch(ctx, updates); err != nil {
				return fmt.Errorf("update segments: %w", err)
			}
		}
		if err := o.repo.Save(ctx, job); err != nil {
			return fmt.Errorf("save job: %w", err)
		}
		return o.bus.Publish(ctx, job.PullEvents()...)
	})
}

// buildSegmentUpdates extracts SegmentUpdate rows for each segment
// touched by the batch. Reads current state from the aggregate after
// it's been mutated.
func buildSegmentUpdates(job *download.Job, batch []segmentResult) []download.SegmentUpdate {
	if len(batch) == 0 {
		return nil
	}
	out := make([]download.SegmentUpdate, 0, len(batch))
	seen := map[download.SegmentID]struct{}{}
	for _, r := range batch {
		if _, ok := seen[r.seg.ID()]; ok {
			continue
		}
		seen[r.seg.ID()] = struct{}{}
		_, s := job.SegmentByID(r.seg.ID())
		if s == nil {
			continue
		}
		out = append(out, download.SegmentUpdate{
			SegmentID:  s.ID(),
			State:      s.State(),
			Attempts:   s.Attempts(),
			LastError:  s.LastError(),
			FileOffset: s.FileOffset(),
		})
	}
	return out
}

// segmentResult is the orchestrator's worker → drainer message.
//
// Exactly one of done/missing/cancelled may be true at a time. When
// none are set and err != nil, the result is a terminal failure
// (retry budget exhausted).
type segmentResult struct {
	seg         *download.Segment
	done        bool
	missing     bool
	// cancelled means the worker exited due to ctx cancellation
	// before resolving the segment. The drainer leaves these
	// segments in the pending state so a restart picks them up
	// cleanly (this is the crash-recovery path).
	cancelled   bool
	bytesOnDisk int64
	fileOffset  int64
	err         error
}

// Ensure imports are referenced (some are used only in error paths).
var _ = bytes.Buffer{}
