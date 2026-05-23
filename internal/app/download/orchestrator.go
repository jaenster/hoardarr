package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	// segment retry — in-process retries within a single dispatch.
	maxAttempts int
	baseBackoff time.Duration

	// durable retry — survives restart by writing next_retry_at into
	// the DB and re-polling on a later orchestrator pass.
	maxDurableAttempts int           // hard cap on total dispatches per segment
	durableBackoffBase time.Duration // first deferred-retry delay
	durableBackoffMax  time.Duration // ceiling for exponential growth
	maxPollGap         time.Duration // hard ceiling on how long Run sleeps between polls

	// failHopelessRatio is SABnzbd's fail_hopeless threshold expressed
	// as a fraction (0.05 = 5%). When a job's failed_bytes exceed this
	// fraction of total_bytes mid-download, the orchestrator aborts
	// rather than keep wasting bandwidth on something PAR2 can't fix.
	// 0 disables.
	failHopelessRatio float64
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

	// MaxDurableAttempts caps the total number of *dispatches* per
	// segment across the whole job's lifetime — each call to
	// processSegment counts as one dispatch, regardless of how many
	// in-process retries (MaxAttempts) it ran internally. Once a
	// segment's Attempts() reaches this value, its next failure
	// becomes terminal instead of getting another durable retry.
	// Default 10.
	MaxDurableAttempts int

	// DurableBackoffBase is the first deferred-retry delay after
	// in-process retries exhaust. Default 30s. Grows exponentially
	// per attempt (30s → 60s → 120s → … capped at DurableBackoffMax).
	DurableBackoffBase time.Duration

	// DurableBackoffMax caps the deferred-retry delay. Default 30min.
	// Without a cap, segments that have failed many times would wait
	// hours, which is rarely useful — provider-side conn-limits and
	// transient outages typically clear in minutes.
	DurableBackoffMax time.Duration

	// MaxPollGap is the longest the orchestrator's Run loop will
	// sleep between re-polling for ready segments when all pending
	// segments are deferred. A small ceiling lets us detect ctx
	// cancellation and operator-driven state changes (pause, remove)
	// without hanging on a 30-min sleep. Default 60s.
	MaxPollGap time.Duration

	// FailHopelessRatio aborts the download mid-flight when failed
	// bytes exceed this fraction of total bytes. Saves bandwidth on
	// releases beyond PAR2's repair capacity. 0 disables. Reasonable
	// values: 0.05 (5%) - 0.10 (10%) for typical 10% PAR2 sets.
	FailHopelessRatio float64

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
		// 1s is the right knob for the V1500B-class hardware this runs
		// on: the original 100ms was driving the SQLite VDBE (already
		// the dominant CPU consumer at ~50% during downloads) ten times
		// harder than necessary. On crash we lose at most one second of
		// segment progress — the orchestrator re-dispatches those
		// segments cleanly on restart from the persisted "pending"
		// state, so the user-visible impact is at most a handful of
		// re-fetched articles per crash. SSE progress updates ride on
		// the same flush; 1Hz is still smoother than a typical
		// download client UI and below most users' perception of "live".
		opts.FlushInterval = 1 * time.Second
	}
	if opts.FlushBatchMax == 0 {
		// 1024 is generous enough that the batch-cap rarely flushes
		// early. At ~10-100 segments/sec across all in-flight downloads
		// we hit the ticker, not the cap.
		opts.FlushBatchMax = 1024
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
	if opts.MaxDurableAttempts == 0 {
		opts.MaxDurableAttempts = 10
	}
	if opts.DurableBackoffBase == 0 {
		opts.DurableBackoffBase = 30 * time.Second
	}
	if opts.DurableBackoffMax == 0 {
		opts.DurableBackoffMax = 30 * time.Minute
	}
	if opts.MaxPollGap == 0 {
		opts.MaxPollGap = 60 * time.Second
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
		maxAttempts:        opts.MaxAttempts,
		baseBackoff:        opts.BaseBackoff,
		poolWait:           opts.PoolWait,
		maxDurableAttempts: opts.MaxDurableAttempts,
		durableBackoffBase: opts.DurableBackoffBase,
		durableBackoffMax:  opts.DurableBackoffMax,
		maxPollGap:         opts.MaxPollGap,
		failHopelessRatio:  opts.FailHopelessRatio,
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

	jobDir := filepath.Join(o.incompleteDir, fmt.Sprintf("%d", int64(job.ID())))
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return fmt.Errorf("create job dir: %w", err)
	}

	// Outer re-poll loop: keep dispatching batches of *ready* pending
	// segments until none are ready and none are deferred. Deferred
	// segments are those with next_retry_at in the future — we wait
	// for them rather than burning workers re-fetching articles whose
	// server just rejected them for being over connection limit.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		now := o.now()
		pending := job.PendingSegments(now)
		if len(pending) > 0 {
			if err := o.runBatch(ctx, job, jobDir, pending); err != nil {
				return err
			}
			continue
		}

		// No ready segments. Anything deferred?
		next := job.NextRetryReadyAt(now)
		if next.IsZero() {
			return nil // truly done — nothing pending, nothing deferred.
		}
		wait := next.Sub(now)
		if wait > o.maxPollGap {
			wait = o.maxPollGap
		}
		if wait < 0 {
			wait = 0
		}
		o.logger.Info("orchestrator: deferring; all pending segments retry-gated",
			"job_id", int64(job.ID()),
			"next_ready_at", next,
			"sleep", wait,
		)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// runBatch dispatches one batch of ready pending segments through the
// worker pool and waits for the drainer to flush all results. Returns
// only after every worker + producer + drainer goroutine has exited,
// so the outer Run() loop can safely re-poll the (now-updated) job.
func (o *Orchestrator) runBatch(
	ctx context.Context,
	job *download.Job,
	jobDir string,
	pending []*download.Segment,
) error {
	workCh := make(chan *download.Segment, o.workers*2)
	resultCh := make(chan segmentResult, o.workers*2)

	// We block until workers + producer + drainer are all done so on
	// ctx-cancel we don't return while one of them is still alive.
	// Previously the producer was a bare `go func()` not in any
	// WaitGroup — under aggressive pause/resume + retries those orphan
	// goroutines accumulated; suspect for the live-container CPU climb.
	var workersWG sync.WaitGroup
	for i := 0; i < o.workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			o.workerLoop(ctx, job, jobDir, workCh, resultCh)
		}()
	}

	// Producer pushes segments into workCh; its `defer close(workCh)`
	// is what eventually lets the workers' range loop terminate.
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(workCh)
		for _, s := range pending {
			select {
			case workCh <- s:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Drainer reads from resultCh; it exits when resultCh is closed
	// (we do that below after workers are done) or when ctx is done.
	drainerDone := make(chan error, 1)
	go func() {
		drainerDone <- o.drainerLoop(ctx, job, resultCh)
	}()

	workersWG.Wait()
	close(resultCh)
	<-producerDone
	return <-drainerDone
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
			// Durable retry vs terminal failure decision: a transient
			// error (network blip, provider conn-limit, 4xx response)
			// gets pushed back to pending with next_retry_at = now +
			// exponential backoff, so the outer Run loop will re-poll
			// it after the gate elapses. A terminal error (yenc decode
			// failure on a complete body, 5xx that won't fix itself,
			// or simply too many dispatches already burned) goes to
			// the final Failed state.
			if r.err != nil && isTransientFetchErr(r.err) && r.seg.Attempts() < o.maxDurableAttempts {
				backoff := o.durableBackoff(r.seg.Attempts())
				if err := job.MarkSegmentForRetry(r.seg.ID(), now.Add(backoff), msg); err != nil {
					o.logger.Error("MarkSegmentForRetry", "err", err)
				} else {
					o.logger.Info("orchestrator: durable retry scheduled",
						"job_id", int64(job.ID()),
						"segment_id", int64(r.seg.ID()),
						"attempts", r.seg.Attempts()+1,
						"next_at", now.Add(backoff),
						"err", msg,
					)
				}
			} else {
				if err := job.MarkSegmentFailed(r.seg.ID(), msg, now); err != nil {
					o.logger.Error("MarkSegmentFailed", "err", err)
				}
			}
		}
		persisted = append(persisted, r)
	}
	// fail_hopeless: if the failed-bytes ratio has crossed the
	// configured threshold, abort the download phase here. Saves
	// the user bandwidth on a release PAR2 can't repair anyway.
	// 0 (default) disables — matches SABnzbd's behaviour of fail-
	// hopeless being an opt-in.
	if o.failHopelessRatio > 0 {
		job.AbortIfHopeless(o.failHopelessRatio, now)
	}
	return o.flushAggregate(ctx, job, persisted)
}

// flushAggregate persists the current job state and publishes pending
// events, plus any segment-level updates implied by batch.
//
// Two persist paths to keep the steady-state download flush cheap:
//
//   - If the Job's non-counter fields changed since the last save
//     (state transition, queue order edit, terminal-state metadata,
//     etc.), do a full Save — rewrites all 12 columns of the jobs row.
//   - Otherwise the only thing that moved is done_bytes/failed_bytes;
//     a targeted UpdateCounters rewrites just those two columns.
//
// The per-batch UpdateSegmentBatch + Publish steps are unchanged.
func (o *Orchestrator) flushAggregate(ctx context.Context, job *download.Job, batch []segmentResult) error {
	updates := buildSegmentUpdates(job, batch)
	return o.tx.InTx(ctx, func(ctx context.Context) error {
		if len(updates) > 0 {
			if err := o.repo.UpdateSegmentBatch(ctx, updates); err != nil {
				return fmt.Errorf("update segments: %w", err)
			}
		}
		if job.IsStateDirty() {
			if err := o.repo.Save(ctx, job); err != nil {
				return fmt.Errorf("save job: %w", err)
			}
		} else {
			if err := o.repo.UpdateCounters(ctx, job); err != nil {
				return fmt.Errorf("update job counters: %w", err)
			}
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

// isTransientFetchErr decides whether a segment-fetch error should be
// pushed back to the pending queue with a backoff (durable retry) or
// marked terminal-failed. The bias is forgiving: when in doubt, retry.
//
//	- ErrArticleMissing never lands here (handled separately as missing)
//	- ErrTooManyConnections: classic transient — provider just told us
//	  we're over our slot, retry in a minute
//	- ErrAuthRequired: pool may pick a different conn next time
//	- ErrAuthFailed: terminal — bad creds won't fix themselves
//	- *ProtocolError 4xx: transient by RFC 3977's "transient negative"
//	- *ProtocolError 5xx: permanent — won't fix on retry
//	- yenc decode errors: terminal — body was malformed, same body next
//	  time. Caught by string match because yenc.Decode wraps with
//	  fmt.Errorf("yenc decode: %w", ...).
//	- everything else: transient (network blips, ctx timeouts, EOF)
func isTransientFetchErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nntp.ErrTooManyConnections) {
		return true
	}
	if errors.Is(err, nntp.ErrAuthRequired) {
		return true
	}
	if errors.Is(err, nntp.ErrAuthFailed) {
		return false
	}
	if errors.Is(err, nntp.ErrUnexpectedGreeting) {
		return true
	}
	var pe *nntp.ProtocolError
	if errors.As(err, &pe) {
		return pe.IsTransient()
	}
	// yenc decode failure has no sentinel; sniff the wrap prefix.
	if strings.Contains(err.Error(), "yenc decode") {
		return false
	}
	if strings.Contains(err.Error(), "writeat") || strings.Contains(err.Error(), "truncate") || strings.Contains(err.Error(), "open tmp") {
		// Local IO errors aren't going to fix themselves on retry —
		// the disk is the disk. Mark terminal so the operator sees
		// the problem in history instead of an indefinite retry loop.
		return false
	}
	return true
}

// durableBackoff returns the next-attempt delay for a segment that
// has been retried `attempts` times (post-MarkPendingRetry-increment).
// Exponential growth with a max ceiling: base, 2×base, 4×base, … cap.
// Jitter would be nice but isn't necessary today — segments retry
// independently anyway and won't thunder.
func (o *Orchestrator) durableBackoff(attempts int) time.Duration {
	d := o.durableBackoffBase
	if attempts < 1 {
		return d
	}
	for i := 0; i < attempts && d < o.durableBackoffMax; i++ {
		d *= 2
	}
	if d > o.durableBackoffMax {
		d = o.durableBackoffMax
	}
	return d
}
