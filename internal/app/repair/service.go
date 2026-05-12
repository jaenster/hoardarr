// Package repair is the application layer for the repair bounded
// context. Subscribes to verify.repair_needed, runs the PAR2 Reed-
// Solomon reconstruction on the damaged files, emits RepairOK or
// RepairFailed.
//
// On success the verify worker (which also subscribes to repair.ok)
// re-runs verification so deliver/extract can pick up the now-healthy
// job through the normal verify.ok event.
package repair

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/par2"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/repair"
	"github.com/jaenster/hoardarr/internal/domain/tx"
	"github.com/jaenster/hoardarr/internal/domain/verify"
)

// Service drives RS-based file reconstruction.
type Service struct {
	jobs          download.JobRepository
	repo          repair.Repository
	bus           event.Bus
	txm           tx.TransactionManager
	incompleteDir string
	logger        *slog.Logger
	now           func() time.Time

	subs []event.Subscription

	wg      sync.WaitGroup
	rootCtx context.Context
	cancel  context.CancelFunc
	started bool
	mu      sync.Mutex
}

// ServiceParams gathers dependencies.
type ServiceParams struct {
	JobRepo       download.JobRepository
	Repo          repair.Repository
	Bus           event.Bus
	TxManager     tx.TransactionManager
	IncompleteDir string
	Logger        *slog.Logger
	Now           func() time.Time
}

// New constructs a Service.
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
		repo:          p.Repo,
		bus:           p.Bus,
		txm:           p.TxManager,
		incompleteDir: p.IncompleteDir,
		logger:        p.Logger,
		now:           p.Now,
		rootCtx:       rootCtx,
		cancel:        cancel,
	}
}

// Start subscribes to verify.repair_needed.
func (s *Service) Start(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	s.rootCtx, s.cancel = context.WithCancel(context.Background())
	sub, err := s.bus.Subscribe("repair-worker", "verify.repair_needed", s.onRepairNeeded)
	if err != nil {
		return fmt.Errorf("subscribe verify.repair_needed: %w", err)
	}
	s.subs = []event.Subscription{sub}
	s.started = true
	s.logger.Info("repair service started")
	return nil
}

// Stop closes subscriptions, cancels in-flight repairs.
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
	s.logger.Info("repair service stopped")
	return nil
}

func (s *Service) onRepairNeeded(ctx context.Context, env event.Envelope) error {
	var e verify.RepairNeeded
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode RepairNeeded: %w", err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.runRepair(s.rootCtx, e.JobID); err != nil {
			s.logger.Error("repair run", "job_id", e.JobID, "err", err)
		}
	}()
	_ = ctx
	return nil
}

func (s *Service) runRepair(ctx context.Context, jobID download.JobID) error {
	job, err := s.jobs.ByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}

	// Idempotency.
	existing, err := s.repo.ByJobID(ctx, jobID)
	if err != nil && !errors.Is(err, repair.ErrNotFound) {
		return fmt.Errorf("load repair: %w", err)
	}
	if existing != nil && (existing.State() == repair.StateOK || existing.State() == repair.StateFailed) {
		s.logger.Info("repair: already terminal, skipping",
			"job_id", jobID, "state", string(existing.State()))
		return nil
	}

	r := existing
	now := s.now()
	if r == nil {
		r = repair.New(repair.NewParams{JobID: jobID}, now)
		if err := s.persist(ctx, r); err != nil {
			return err
		}
	}

	if err := r.Start(s.now()); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if err := s.persist(ctx, r); err != nil {
		return err
	}

	// Resolve paths from the Job aggregate. PAR2 files vs data files
	// are distinguished by File.IsPar2.
	jobDir := filepath.Join(s.incompleteDir, strconv.FormatInt(int64(jobID), 10))
	var par2Paths []string
	dataPaths := make(map[string]string)
	for _, f := range job.Files() {
		p := filepath.Join(jobDir, strconv.FormatInt(int64(f.ID()), 10)+".tmp")
		if f.IsPar2() {
			par2Paths = append(par2Paths, p)
		} else {
			dataPaths[f.Filename()] = p
		}
	}

	if len(par2Paths) == 0 {
		return s.failRepair(ctx, r, "no PAR2 files available")
	}

	result, err := par2.Repair(ctx, par2.RepairInput{
		Par2Paths: par2Paths,
		DataPaths: dataPaths,
	})
	// On-demand recovery-vol fetching: if PAR2 can't repair because
	// recovery slices are short AND the job has deferred recovery vols
	// it hasn't fetched yet, request them and let the orchestrator
	// re-enter. Verify+Repair will fire again once the new vols land.
	if errors.Is(err, par2.ErrUnrecoverableSet) && job.HasDeferredRecoveryVols() {
		if rerr := s.requestRecoveryVols(ctx, job); rerr != nil {
			return s.failRepair(ctx, r, fmt.Sprintf("request recovery vols: %v", rerr))
		}
		s.logger.Info("repair: insufficient slices, requesting deferred recovery vols",
			"job_id", jobID,
			"recovery_slices", len(result.AlreadyOK)+len(result.Failed))
		// Don't transition repair to failed — we're parked, waiting for
		// the second-round JobDownloadComplete to trigger verify again.
		return nil
	}
	if err != nil && !errors.Is(err, par2.ErrUnrecoverableSet) {
		return s.failRepair(ctx, r, err.Error())
	}
	if len(result.Failed) > 0 {
		// At least one file couldn't be repaired. Surface the first
		// reason for the event payload.
		reason := result.Failed[0].Filename + ": " + result.Failed[0].Reason
		return s.failRepair(ctx, r, reason)
	}

	if err := r.MarkOK(s.now()); err != nil {
		return fmt.Errorf("markok: %w", err)
	}
	if err := s.persist(ctx, r); err != nil {
		return err
	}
	s.logger.Info("repair ok", "job_id", jobID,
		"repaired", len(result.Repaired),
		"already_ok", len(result.AlreadyOK))
	return nil
}

func (s *Service) failRepair(ctx context.Context, r *repair.Repair, reason string) error {
	if err := r.MarkFailed(reason, s.now()); err != nil {
		return fmt.Errorf("markfailed: %w (reason: %s)", err, reason)
	}
	if err := s.persist(ctx, r); err != nil {
		return err
	}
	// Mark the Job terminal-failed in its own tx so the UI surfaces
	// it cleanly via History.
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.jobs.ByID(ctx, r.JobID())
		if err != nil {
			return err
		}
		j.MarkFailed("repair: "+reason, s.now())
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

// requestRecoveryVols flips the Job's fetch_recovery_vols flag to true
// and publishes RecoveryVolsRequested so the orchestrator restarts the
// runner. Caller must guarantee job.HasDeferredRecoveryVols() before
// calling. Repair state stays "running" — verify will pick the job
// up again after the second-round JobDownloadComplete.
func (s *Service) requestRecoveryVols(ctx context.Context, job *download.Job) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		if err := job.RequestRecoveryVols(s.now()); err != nil {
			return err
		}
		evts := job.PullEvents()
		if err := s.jobs.Save(ctx, job); err != nil {
			return err
		}
		return s.bus.Publish(ctx, evts...)
	})
}

func (s *Service) persist(ctx context.Context, r *repair.Repair) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		if err := s.repo.Save(ctx, r); err != nil {
			return fmt.Errorf("save repair: %w", err)
		}
		evts := r.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		return s.bus.Publish(ctx, evts...)
	})
}
