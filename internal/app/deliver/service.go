// Package deliver is the application layer for the deliver bounded
// context: subscribes to verify.VerifyOK, decides whether the Job is
// an archive (extract owns it) or plain-files (we move them ourselves),
// and emits DeliveryComplete / DeliveryFailed / DeliverySkipped.
//
// Naming: the per-job target dir is
// `<complete>/<category-dir>/<release-name>/`. The release name is
// the Job's NZB-derived Name. category-dir is the category's `dir`
// column (or empty → drops to <complete>/<release-name>/).
package deliver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/domain/deliver"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/tx"
	"github.com/jaenster/hoardarr/internal/domain/verify"
)

// Service drives delivery: move .tmp files from incomplete/<jobid>/
// to complete/<category-dir>/<release>/<filename>.
type Service struct {
	jobs          download.JobRepository
	deliveries    deliver.DeliveryRepository
	categories    *sqlite.CategoryRepo
	fs            deliver.Filesystem
	bus           event.Bus
	txm           tx.TransactionManager
	incompleteDir string
	completeDir   string
	logger        *slog.Logger
	now           func() time.Time

	// Post-processing toggles. Each is a callback into the runtime so
	// operator changes in Settings take effect on the next delivery
	// without restarting the service.
	deleteSamples        func() bool
	collapseSingleFolder func() bool

	subs []event.Subscription

	wg      sync.WaitGroup
	rootCtx context.Context
	cancel  context.CancelFunc
	started bool
	mu      sync.Mutex
}

// ServiceParams gathers Service dependencies.
type ServiceParams struct {
	JobRepo       download.JobRepository
	DeliveryRepo  deliver.DeliveryRepository
	CategoryRepo  *sqlite.CategoryRepo
	FS            deliver.Filesystem
	Bus           event.Bus
	TxManager     tx.TransactionManager
	IncompleteDir string
	CompleteDir   string
	Logger        *slog.Logger
	Now           func() time.Time

	// DeleteSamples returns whether sample/proof files should be removed
	// from the target dir after a successful move. Nil → off.
	DeleteSamples func() bool
	// CollapseSingleFolder returns whether a release that landed inside
	// a single redundant sub-folder should be flattened. Nil → off.
	CollapseSingleFolder func() bool
}

// New constructs a deliver Service.
func New(p ServiceParams) *Service {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	if p.DeleteSamples == nil {
		p.DeleteSamples = func() bool { return false }
	}
	if p.CollapseSingleFolder == nil {
		p.CollapseSingleFolder = func() bool { return false }
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		jobs:                 p.JobRepo,
		deliveries:           p.DeliveryRepo,
		categories:           p.CategoryRepo,
		fs:                   p.FS,
		bus:                  p.Bus,
		txm:                  p.TxManager,
		incompleteDir:        p.IncompleteDir,
		completeDir:          p.CompleteDir,
		logger:               p.Logger,
		now:                  p.Now,
		deleteSamples:        p.DeleteSamples,
		collapseSingleFolder: p.CollapseSingleFolder,
		rootCtx:              rootCtx,
		cancel:               cancel,
	}
}

// Start subscribes to verify.VerifyOK. Idempotent. Restartable after
// Stop (creates a fresh rootCtx).
func (s *Service) Start(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	s.rootCtx, s.cancel = context.WithCancel(context.Background())

	sub, err := s.bus.Subscribe("deliver-worker", "verify.ok", s.onVerifyOK)
	if err != nil {
		return fmt.Errorf("subscribe verify.ok: %w", err)
	}
	s.subs = []event.Subscription{sub}
	s.started = true
	s.logger.Info("deliver service started")

	// Startup recovery: re-drive any job stuck in download_complete or
	// repairing whose verify.ok / repair.ok event was already
	// delivered (and so won't fire again). Without this, a crash
	// between "verify.ok handler scheduled" and "delivery row
	// persisted" leaves the job orphaned indefinitely — which is what
	// happened in production after the 2026-05-12 container restart.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.recoverStuck(s.rootCtx)
	}()
	return nil
}

// recoverStuck sweeps for jobs that should be in the deliver pipeline
// but aren't because their trigger event was already consumed. Safe
// to re-run — runDelivery is idempotent (StateComplete/Skipped fast-
// paths the work).
//
// Sequential by design: each runDelivery wants to UPDATE jobs in its
// markJobCompleted tx, and modernc.org/sqlite serialises writes. The
// first attempt at this fan-out used a goroutine per job, which
// produced SQLITE_BUSY / snapshot-conflict storms when more than a
// couple of jobs needed recovery at the same time (seen during the
// 2026-05-13 production deploy — needed three restarts before all
// orphaned jobs cleared). Looping serially in a single goroutine is
// strictly faster than retry-loops at higher contention.
func (s *Service) recoverStuck(ctx context.Context) {
	active, err := s.jobs.Active(ctx)
	if err != nil {
		s.logger.Warn("deliver: recover sweep failed", "err", err)
		return
	}
	var swept int
	for _, j := range active {
		if ctx.Err() != nil {
			return
		}
		switch j.State() {
		case download.JobStateDownloadComplete, download.JobStateRepairing, download.JobStateUnpacking:
		default:
			continue
		}
		jobID := j.ID()
		if err := s.runDelivery(ctx, jobID); err != nil {
			s.logger.Warn("deliver: recovery run failed", "job_id", jobID, "err", err)
		}
		swept++
	}
	if swept > 0 {
		s.logger.Info("deliver: startup recovery swept jobs", "count", swept)
	}
}

// Stop closes subscriptions, cancels in-flight deliveries, waits for
// them to exit.
func (s *Service) Stop() error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = false
	for _, sub := range s.subs {
		_ = sub.Close()
	}
	s.subs = nil
	s.mu.Unlock()

	s.cancel()
	s.wg.Wait()
	s.logger.Info("deliver service stopped")
	return nil
}

func (s *Service) onVerifyOK(ctx context.Context, env event.Envelope) error {
	var e verify.VerifyOK
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode VerifyOK: %w", err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.runDelivery(s.rootCtx, e.JobID); err != nil {
			s.logger.Error("delivery run", "job_id", e.JobID, "err", err)
		}
	}()
	_ = ctx
	return nil
}

// runDelivery is the full move flow for one job.
func (s *Service) runDelivery(ctx context.Context, jobID download.JobID) error {
	job, err := s.jobs.ByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}

	// Idempotency: if a delivery row exists in a terminal state, the
	// move was already done. Skip the filesystem work — but still
	// ensure the Job state has advanced. A crash between "delivery
	// complete" and "Job.MarkCompleted" used to leave the job stuck
	// in download_complete forever; this pulls it across the line on
	// the next re-fire (e.g. after restart).
	existing, err := s.deliveries.ByJobID(ctx, jobID)
	if err != nil && !errors.Is(err, deliver.ErrNotFound) {
		return fmt.Errorf("load delivery: %w", err)
	}
	if existing != nil && (existing.State() == deliver.StateComplete ||
		existing.State() == deliver.StateSkipped) {
		s.logger.Info("deliver: already terminal, ensuring job advanced",
			"job_id", jobID, "state", string(existing.State()))
		// StateSkipped means an extractor is handling this job —
		// leave it alone. StateComplete is the non-archive happy
		// path; advance the Job.
		if existing.State() == deliver.StateComplete {
			return s.markJobCompleted(ctx, jobID)
		}
		return nil
	}

	// Resolve target dir based on the Job's category. Reserved '*' →
	// no subdirectory.
	subdir := ""
	cats, err := s.categories.List(ctx)
	if err != nil {
		return fmt.Errorf("list categories: %w", err)
	}
	for _, c := range cats {
		if c.Name == job.Category() {
			subdir = c.Dir
			break
		}
	}
	releaseName := sanitizeReleaseName(job.Name())
	targetDir := filepath.Join(s.completeDir, subdir, releaseName)

	// Create / reload the delivery aggregate inside a tx so the
	// pending row is durable before we touch the filesystem.
	d := existing
	now := s.now()
	if d == nil {
		d = deliver.New(deliver.NewParams{JobID: jobID, TargetDir: targetDir}, now)
		if err := s.persist(ctx, d); err != nil {
			return err
		}
	}

	// Detect archive jobs. M4-non-archive scope: if any file looks
	// like RAR, mark skipped — the extract worker (M4 RAR) will
	// handle this job once it lands.
	if isArchiveJob(job) {
		if err := d.Skip(s.now()); err != nil {
			return fmt.Errorf("skip: %w", err)
		}
		return s.persist(ctx, d)
	}

	// Begin moving.
	if err := d.Start(s.now()); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if err := s.persist(ctx, d); err != nil {
		return err
	}

	// Build src→dst pairs. Skip PAR2 files — they're scratch parity,
	// no value to the end user.
	jobDir := filepath.Join(s.incompleteDir, strconv.FormatInt(int64(jobID), 10))
	if err := s.fs.MkdirAll(targetDir); err != nil {
		return s.failDelivery(ctx, d, fmt.Errorf("mkdir target: %w", err))
	}
	var movedAny bool
	for _, f := range job.Files() {
		if f.IsPar2() {
			continue
		}
		src := filepath.Join(jobDir, strconv.FormatInt(int64(f.ID()), 10)+".tmp")
		dst := filepath.Join(targetDir, sanitizeFilename(f.Filename()))
		if err := s.fs.Move(src, dst); err != nil {
			// Missing source: every segment of this file 430'd or
			// failed. Common for non-essential sidecars (.nfo, .sfv)
			// that aren't in the PAR2 set — PAR2 said the release
			// verifies clean but the metadata didn't arrive. Don't
			// fail the whole delivery for a missing sidecar; just
			// log and keep moving.
			if errors.Is(err, fs.ErrNotExist) || strings.Contains(err.Error(), "no such file or directory") {
				s.logger.Warn("deliver: source missing, skipping",
					"job_id", jobID, "file", f.Filename(), "src", src)
				continue
			}
			return s.failDelivery(ctx, d, fmt.Errorf("move %s: %w", f.Filename(), err))
		}
		movedAny = true
	}
	if !movedAny {
		// Every data file was missing — nothing actually ended up in
		// complete/. Treat as a delivery failure rather than silently
		// "completing" an empty release.
		return s.failDelivery(ctx, d, fmt.Errorf("no data files moved"))
	}

	// Best-effort cleanup of incomplete/<jobid>/ — leftover PAR2
	// files and any empty subdirs go away.
	if err := s.fs.RemoveAll(jobDir); err != nil {
		s.logger.Warn("deliver: cleanup of incomplete dir failed",
			"job_id", jobID, "dir", jobDir, "err", err)
	}

	// Post-move processing. Each step is best-effort: failure logs but
	// doesn't fail the delivery — files are already in complete/ and
	// the move was the point. Order matters: rename obfuscated files
	// first (uses the largest-data-file heuristic, which needs to see
	// the original layout), then strip samples (frees up the "single
	// non-sample folder" shape collapse looks for), then collapse a
	// redundant outer wrap.
	// Build the PAR2 set name from job's par2 filenames so the
	// deobfuscate rename can prefer it over an obfuscated job.Name().
	var par2Names []string
	for _, f := range job.Files() {
		if f.IsPar2() {
			par2Names = append(par2Names, f.Filename())
		}
	}
	parSet := par2SetName(par2Names)
	if _, err := deobfuscateRename(targetDir, job.Name(), parSet, s.logger); err != nil {
		s.logger.Warn("deliver: deobfuscate rename failed",
			"job_id", jobID, "dir", targetDir, "err", err)
	}
	if s.deleteSamples() {
		if err := removeSamples(targetDir, s.logger); err != nil {
			s.logger.Warn("deliver: remove samples failed",
				"job_id", jobID, "dir", targetDir, "err", err)
		}
	}
	if s.collapseSingleFolder() {
		if err := collapseSingleFolder(targetDir, s.logger); err != nil {
			s.logger.Warn("deliver: collapse single folder failed",
				"job_id", jobID, "dir", targetDir, "err", err)
		}
	}

	if err := d.Complete(s.now()); err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	if err := s.persist(ctx, d); err != nil {
		return err
	}

	// Mark the Job terminal-completed. Do this in its own tx so the
	// JobCompleted event is published with the (separately-saved)
	// row state.
	return s.markJobCompleted(ctx, jobID)
}

// markJobCompleted transitions the Job to JobStateCompleted and
// publishes JobCompleted. Idempotent: if the Job is already in a
// terminal state, MarkCompleted is a no-op and no events are emitted.
//
// Called from the happy path (after a successful move) AND from the
// idempotent-skip path (when a Delivery row is already complete but
// the Job state never advanced — e.g. process crashed between the
// two writes).
func (s *Service) markJobCompleted(ctx context.Context, jobID download.JobID) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.jobs.ByID(ctx, jobID)
		if err != nil {
			return err
		}
		j.MarkCompleted(s.now())
		evts := j.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		if err := s.jobs.Save(ctx, j); err != nil {
			return err
		}
		return s.bus.Publish(ctx, evts...)
	})
}

// failDelivery flips d to failed, persists, marks the Job failed too.
func (s *Service) failDelivery(ctx context.Context, d *deliver.Delivery, cause error) error {
	if err := d.Fail(cause.Error(), s.now()); err != nil {
		return fmt.Errorf("fail: %w (cause: %v)", err, cause)
	}
	if err := s.persist(ctx, d); err != nil {
		return err
	}
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.jobs.ByID(ctx, d.JobID())
		if err != nil {
			return err
		}
		j.MarkFailed("delivery: "+cause.Error(), s.now())
		evts := j.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		if err := s.jobs.Save(ctx, j); err != nil {
			return err
		}
		return s.bus.Publish(ctx, evts...)
	})
}

// persist saves the aggregate and publishes its buffered events in one
// tx so subscribers can't see a state without its accompanying event.
func (s *Service) persist(ctx context.Context, d *deliver.Delivery) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		if err := s.deliveries.Save(ctx, d); err != nil {
			return fmt.Errorf("save delivery: %w", err)
		}
		evts := d.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		return s.bus.Publish(ctx, evts...)
	})
}

// isArchiveJob returns true if any non-PAR2 file looks like RAR. The
// extract worker owns delivery for those (M4 RAR scope).
func isArchiveJob(job *download.Job) bool {
	for _, f := range job.Files() {
		if f.IsPar2() {
			continue
		}
		if looksLikeRAR(f.Filename()) {
			return true
		}
	}
	return false
}

// looksLikeRAR matches both classic .rar / .r00..r99 and the modern
// part-N naming. We're conservative: false negatives mean we'd try
// to deliver the .rar files raw (acceptable but ugly), false positives
// mean we'd skip a release that didn't actually need extracting (which
// blocks delivery — worse). Err on the side of false negatives.
func looksLikeRAR(filename string) bool {
	low := strings.ToLower(filename)
	if strings.HasSuffix(low, ".rar") {
		return true
	}
	// .r00 .. .r99 — classic split.
	if len(low) >= 4 && low[len(low)-4] == '.' && low[len(low)-3] == 'r' {
		c1, c2 := low[len(low)-2], low[len(low)-1]
		if c1 >= '0' && c1 <= '9' && c2 >= '0' && c2 <= '9' {
			return true
		}
	}
	return false
}

// sanitizeReleaseName makes an NZB Name safe to use as a directory
// component. Strips path separators and control chars; keeps the
// release recognizable.
func sanitizeReleaseName(name string) string {
	if name == "" {
		return "untitled"
	}
	return sanitizePathComponent(name)
}

// sanitizeFilename keeps the file recognisable while ensuring it
// can't escape the target directory.
func sanitizeFilename(name string) string {
	if name == "" {
		return "untitled"
	}
	// Drop any path-traversal attempts that survived NZB parsing.
	name = filepath.Base(name)
	return sanitizePathComponent(name)
}

func sanitizePathComponent(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			continue
		}
		switch r {
		case '/', '\\', ':':
			out = append(out, '_')
		default:
			out = append(out, r)
		}
	}
	cleaned := strings.TrimSpace(string(out))
	cleaned = strings.Trim(cleaned, ".") // no leading/trailing dots → no hidden dirs
	if cleaned == "" {
		return "untitled"
	}
	return cleaned
}
