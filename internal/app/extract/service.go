// Package extract is the application layer for the extract bounded
// context: subscribes to verify.ok, picks up archive jobs (jobs whose
// non-PAR2 files are RAR volumes), unpacks them via the Extractor port
// directly into <complete>/<category-dir>/<release>/, then transitions
// the Job to Completed.
//
// Non-archive jobs are handled by the deliver service. Both services
// listen to verify.ok independently; each silently passes on jobs the
// other should handle (deliver skips archives, extract skips
// non-archives). This avoids the need for cross-context coordination.
package extract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/extract"
	"github.com/jaenster/hoardarr/internal/domain/tx"
	"github.com/jaenster/hoardarr/internal/domain/verify"
)

// Service drives extraction. Subscribes to verify.ok; when the job is
// an archive, runs the Extractor and writes the contents directly to
// <complete>/<category>/<release>/. On success, marks the Job
// completed; on failure, marks the Job failed.
type Service struct {
	jobs          download.JobRepository
	repo          extract.Repository
	categories    *sqlite.CategoryRepo
	extractor     extract.Extractor
	bus           event.Bus
	txm           tx.TransactionManager
	incompleteDir string
	completeDir   string
	logger        *slog.Logger
	now           func() time.Time

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
	ExtractRepo   extract.Repository
	CategoryRepo  *sqlite.CategoryRepo
	Extractor     extract.Extractor
	Bus           event.Bus
	TxManager     tx.TransactionManager
	IncompleteDir string
	CompleteDir   string
	Logger        *slog.Logger
	Now           func() time.Time
}

// New constructs an extract Service.
func New(p ServiceParams) *Service {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		jobs:          p.JobRepo,
		repo:          p.ExtractRepo,
		categories:    p.CategoryRepo,
		extractor:     p.Extractor,
		bus:           p.Bus,
		txm:           p.TxManager,
		incompleteDir: p.IncompleteDir,
		completeDir:   p.CompleteDir,
		logger:        p.Logger,
		now:           p.Now,
		rootCtx:       rootCtx,
		cancel:        cancel,
	}
}

// Start subscribes to verify.ok. Idempotent. Restartable after Stop.
func (s *Service) Start(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	s.rootCtx, s.cancel = context.WithCancel(context.Background())

	sub, err := s.bus.Subscribe("extract-worker", "verify.ok", s.onVerifyOK)
	if err != nil {
		return fmt.Errorf("subscribe verify.ok: %w", err)
	}
	s.subs = []event.Subscription{sub}
	s.started = true
	s.logger.Info("extract service started")
	return nil
}

// Stop closes subscriptions, cancels in-flight extractions.
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
	s.logger.Info("extract service stopped")
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
		if err := s.runExtract(s.rootCtx, e.JobID); err != nil {
			s.logger.Error("extract run", "job_id", e.JobID, "err", err)
		}
	}()
	_ = ctx
	return nil
}

// runExtract handles one job. Returns immediately for non-archive jobs
// (deliver service handles those).
func (s *Service) runExtract(ctx context.Context, jobID download.JobID) error {
	job, err := s.jobs.ByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}

	if !isArchiveJob(job) {
		// Not our problem — deliver handles non-archive jobs.
		return nil
	}
	// Rename the .tmp blobs to their original filenames so rardecode's
	// volume-sibling resolution finds the next part by .partN.rar /
	// .r0N naming. The .tmp scheme is good for download-time
	// concurrency but rardecode keys off the entry filename.
	rarPaths, err := stageArchiveFilenames(job, s.incompleteDir)
	if err != nil {
		return fmt.Errorf("stage archive filenames: %w", err)
	}

	// Idempotency.
	existing, err := s.repo.ByJobID(ctx, jobID)
	if err != nil && !errors.Is(err, extract.ErrNotFound) {
		return fmt.Errorf("load extract: %w", err)
	}
	if existing != nil && existing.State() == extract.StateComplete {
		s.logger.Info("extract: already complete, skipping", "job_id", jobID)
		return nil
	}

	// Resolve target dir.
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

	x := existing
	now := s.now()
	if x == nil {
		x = extract.New(extract.NewParams{JobID: jobID, TargetDir: targetDir}, now)
		if err := s.persist(ctx, x); err != nil {
			return err
		}
	}

	if err := x.Start(s.now()); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if err := s.persist(ctx, x); err != nil {
		return err
	}

	if _, err := s.extractor.Extract(ctx, rarPaths, targetDir); err != nil {
		return s.failExtract(ctx, x, err)
	}

	// Best-effort cleanup of the incomplete dir for this job.
	jobDir := filepath.Join(s.incompleteDir, strconv.FormatInt(int64(jobID), 10))
	if err := removeAll(jobDir); err != nil {
		s.logger.Warn("extract: cleanup of incomplete dir failed",
			"job_id", jobID, "dir", jobDir, "err", err)
	}

	if err := x.Complete(s.now()); err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	if err := s.persist(ctx, x); err != nil {
		return err
	}

	// Mark Job completed in its own tx so JobCompleted is published
	// with the saved state.
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

func (s *Service) failExtract(ctx context.Context, x *extract.Extract, cause error) error {
	if err := x.Fail(cause.Error(), s.now()); err != nil {
		return fmt.Errorf("fail: %w (cause: %v)", err, cause)
	}
	if err := s.persist(ctx, x); err != nil {
		return err
	}
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.jobs.ByID(ctx, x.JobID())
		if err != nil {
			return err
		}
		j.MarkFailed("extract: "+cause.Error(), s.now())
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

func (s *Service) persist(ctx context.Context, x *extract.Extract) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		if err := s.repo.Save(ctx, x); err != nil {
			return fmt.Errorf("save extract: %w", err)
		}
		evts := x.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		return s.bus.Publish(ctx, evts...)
	})
}

// isArchiveJob reports whether a job has any RAR-looking files.
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

// stageArchiveFilenames renames each RAR-looking file from
// <jobid>/<fileid>.tmp to <jobid>/<original-filename> so rardecode's
// volume-resolution by sibling names works. Returns the list of
// real-name paths in the order they appear in the Job aggregate.
//
// Idempotent across crashes: if the rename already happened, Stat
// confirms the destination exists and we move on.
func stageArchiveFilenames(job *download.Job, incompleteDir string) ([]string, error) {
	jobDir := filepath.Join(incompleteDir, strconv.FormatInt(int64(job.ID()), 10))
	var out []string
	for _, f := range job.Files() {
		if f.IsPar2() || !looksLikeRAR(f.Filename()) {
			continue
		}
		src := filepath.Join(jobDir, strconv.FormatInt(int64(f.ID()), 10)+".tmp")
		dst := filepath.Join(jobDir, sanitizeFilename(f.Filename()))
		if src == dst {
			out = append(out, dst)
			continue
		}
		if _, err := os.Stat(dst); err == nil {
			// Already staged from a previous (crashed) run.
			out = append(out, dst)
			continue
		}
		// If the source .tmp never materialised — every segment 430'd,
		// or the file is otherwise absent — skip it. The extract step
		// downstream can still try to assemble what's on disk. Failing
		// hard here would abort extraction on releases that are 95%
		// downloaded and missing one .r-part the user could repair.
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := os.Rename(src, dst); err != nil {
			return nil, fmt.Errorf("stage %s → %s: %w", src, dst, err)
		}
		out = append(out, dst)
	}
	return out, nil
}

// sanitizeFilename keeps the file recognisable while ensuring it can't
// escape its parent directory.
func sanitizeFilename(name string) string {
	if name == "" {
		return "untitled"
	}
	name = filepath.Base(name)
	out := make([]rune, 0, len(name))
	for _, r := range name {
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
	cleaned = strings.Trim(cleaned, ".")
	if cleaned == "" {
		return "untitled"
	}
	return cleaned
}

func looksLikeRAR(filename string) bool {
	low := strings.ToLower(filename)
	if strings.HasSuffix(low, ".rar") {
		return true
	}
	if len(low) >= 4 && low[len(low)-4] == '.' && low[len(low)-3] == 'r' {
		c1, c2 := low[len(low)-2], low[len(low)-1]
		if c1 >= '0' && c1 <= '9' && c2 >= '0' && c2 <= '9' {
			return true
		}
	}
	return false
}

func sanitizeReleaseName(name string) string {
	if name == "" {
		return "untitled"
	}
	out := make([]rune, 0, len(name))
	for _, r := range name {
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
	cleaned = strings.Trim(cleaned, ".")
	if cleaned == "" {
		return "untitled"
	}
	return cleaned
}

// removeAll wraps os.RemoveAll without pulling adapter/fs into this
// package (deliberate: extract is the only context that needs RemoveAll
// today, and importing adapter/fs would create an extra dep).
func removeAll(path string) error {
	return os.RemoveAll(path)
}
