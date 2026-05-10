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

// processSegment runs one segment end-to-end: fetch via NNTP, decode
// yEnc, write to disk at the computed offset.
func (o *Orchestrator) processSegment(
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
func (o *Orchestrator) flushBatch(ctx context.Context, job *download.Job, batch []segmentResult) error {
	now := o.now()
	for _, r := range batch {
		switch {
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
	}
	return o.flushAggregate(ctx, job, batch)
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
type segmentResult struct {
	seg         *download.Segment
	done        bool
	missing     bool
	bytesOnDisk int64
	fileOffset  int64
	err         error
}

// Ensure imports are referenced (some are used only in error paths).
var _ = bytes.Buffer{}
