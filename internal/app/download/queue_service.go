package download

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// QueueService groups the operational use cases over the download queue:
// Pause, Resume, Remove, plus listing helpers. AddJob lives in its own
// service because its surface (NZB parse, dedupe) is materially different.
//
// All mutators run inside TransactionManager.InTx so state changes and
// outbox events commit atomically.
type QueueService struct {
	repo          download.JobRepository
	bus           event.Bus
	txm           tx.TransactionManager
	now           func() time.Time
	logger        *slog.Logger
	incompleteDir string
}

// QueueServiceParams gathers QueueService dependencies.
type QueueServiceParams struct {
	Repo          download.JobRepository
	Bus           event.Bus
	TxManager     tx.TransactionManager
	Now           func() time.Time
	Logger        *slog.Logger
	IncompleteDir string // root of incomplete/<jobid>/ tmp files; cleaned on Remove
}

// NewQueueService wires the queue operational service.
func NewQueueService(p QueueServiceParams) *QueueService {
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	return &QueueService{
		repo:          p.Repo,
		bus:           p.Bus,
		txm:           p.TxManager,
		now:           p.Now,
		logger:        p.Logger,
		incompleteDir: p.IncompleteDir,
	}
}

// PauseJob flips the job's state to paused. The orchestrator
// service observes the JobPaused event and stops dispatching segments.
// Already-paused / terminal jobs are no-ops (no event emitted).
func (s *QueueService) PauseJob(ctx context.Context, id download.JobID) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		j.Pause(s.now())
		evts := j.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		if err := s.repo.Save(ctx, j); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		return s.bus.Publish(ctx, evts...)
	})
}

// ResumeJob flips the job's state back to queued/downloading. The
// orchestrator picks up dispatch on the JobResumed event.
func (s *QueueService) ResumeJob(ctx context.Context, id download.JobID) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		j.Resume(s.now())
		evts := j.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		if err := s.repo.Save(ctx, j); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		return s.bus.Publish(ctx, evts...)
	})
}

// RemoveJob deletes the job from the registry, emits JobRemoved, and
// removes the per-job incomplete tmp directory on disk.
//
// The DB row + event commit atomically; the on-disk cleanup happens
// after commit. If cleanup fails we log a warning — the row is gone,
// so a restart-time sweeper can prune orphaned dirs (post-v0.1).
func (s *QueueService) RemoveJob(ctx context.Context, id download.JobID) error {
	err := s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		j.MarkRemoved(s.now())
		if err := s.repo.Delete(ctx, id); err != nil {
			return err
		}
		return s.bus.Publish(ctx, j.PullEvents()...)
	})
	if err != nil {
		return err
	}

	if s.incompleteDir != "" {
		dir := filepath.Join(s.incompleteDir, strconv.FormatInt(int64(id), 10))
		if rmErr := os.RemoveAll(dir); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			s.logger.Warn("queue: failed to remove incomplete dir; orphan left for sweeper",
				"job_id", id, "dir", dir, "err", rmErr)
		}
	}
	return nil
}

// Get returns the full job tree (files, segments) by id.
func (s *QueueService) Get(ctx context.Context, id download.JobID) (*download.Job, error) {
	return s.repo.ByID(ctx, id)
}

// List returns all jobs in the queue, most recent first by queue_order.
func (s *QueueService) List(ctx context.Context) ([]*download.Job, error) {
	return s.repo.List(ctx)
}

// Active returns jobs in non-terminal states.
func (s *QueueService) Active(ctx context.Context) ([]*download.Job, error) {
	return s.repo.Active(ctx)
}
