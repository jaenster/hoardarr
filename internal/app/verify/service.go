// Package verify is the application layer for the verify bounded
// context: subscribes to JobDownloadComplete events, runs PAR2
// verification, and emits VerifyOK / RepairNeeded / VerifyFailed.
//
// M3a scope: verify-only (full-file MD5 against the PAR2 FileDesc
// digest). Repair (M3b) is a separate worker that subscribes to the
// RepairNeeded event this service emits.
package verify

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
	"github.com/jaenster/hoardarr/internal/domain/tx"
	"github.com/jaenster/hoardarr/internal/domain/verify"
)

// Service drives PAR2 verification.
type Service struct {
	jobs          download.JobRepository
	verifyRepo    verify.Repository
	verifier      verify.Verifier
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

// ServiceParams gathers Service dependencies.
type ServiceParams struct {
	JobRepo       download.JobRepository
	VerifyRepo    verify.Repository
	Verifier      verify.Verifier
	Bus           event.Bus
	TxManager     tx.TransactionManager
	IncompleteDir string
	Logger        *slog.Logger
	Now           func() time.Time
}

// New constructs a verify Service.
func New(p ServiceParams) *Service {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	if p.Verifier == nil {
		p.Verifier = par2.Verifier{}
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		jobs:          p.JobRepo,
		verifyRepo:    p.VerifyRepo,
		verifier:      p.Verifier,
		bus:           p.Bus,
		txm:           p.TxManager,
		incompleteDir: p.IncompleteDir,
		logger:        p.Logger,
		now:           p.Now,
		rootCtx:       rootCtx,
		cancel:        cancel,
	}
}

// Start subscribes to the relevant bus topics. Idempotent. Restartable
// after Stop (creates a fresh rootCtx, mirroring OrchestratorService).
func (s *Service) Start(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	s.rootCtx, s.cancel = context.WithCancel(context.Background())

	sub, err := s.bus.Subscribe("verify-worker", "download.job.download_complete", s.onJobDownloadComplete)
	if err != nil {
		return fmt.Errorf("subscribe job.download_complete: %w", err)
	}
	// Also subscribe to repair.ok — after the repair worker reconstructs
	// damaged files we re-run verification, which then fires verify.ok
	// for deliver/extract.
	sub2, err := s.bus.Subscribe("verify-worker-postrepair", "repair.ok", s.onRepairOK)
	if err != nil {
		_ = sub.Close()
		return fmt.Errorf("subscribe repair.ok: %w", err)
	}
	s.subs = []event.Subscription{sub, sub2}
	s.started = true
	s.logger.Info("verify service started")
	return nil
}

// Stop closes subscriptions, cancels in-flight verifications, and
// waits for them to exit.
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
	s.logger.Info("verify service stopped")
	return nil
}

func (s *Service) onJobDownloadComplete(ctx context.Context, env event.Envelope) error {
	var e download.JobDownloadComplete
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode JobDownloadComplete: %w", err)
	}

	// Run the verify in a separate goroutine — the file hashing can
	// take a while and we don't want to block the bus dispatcher
	// (which has retry / backoff semantics tied to handler latency).
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.runVerify(s.rootCtx, e.JobID); err != nil {
			s.logger.Error("verify run", "job_id", e.JobID, "err", err)
		}
	}()
	_ = ctx
	return nil
}

// onRepairOK fires after the repair worker reconstructs damaged files.
// We reset the VerifySet to pending and re-run verification — the
// follow-up VerifyOK event is what deliver/extract listen for to take
// the job to terminal completion.
func (s *Service) onRepairOK(ctx context.Context, env event.Envelope) error {
	var e struct {
		JobID download.JobID `json:"job_id"`
	}
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode RepairOK: %w", err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.resetAndRunVerify(s.rootCtx, e.JobID); err != nil {
			s.logger.Error("verify rerun (post-repair)", "job_id", e.JobID, "err", err)
		}
	}()
	// Acknowledge to the bus immediately — we're committed to running.
	_ = ctx
	return nil
}

// resetAndRunVerify flips the existing VerifySet from repair_needed
// back to pending and then runs verify again. If the set isn't in
// repair_needed (or doesn't exist) we still run verify — it's an
// idempotent restart from the caller's perspective.
func (s *Service) resetAndRunVerify(ctx context.Context, jobID download.JobID) error {
	vset, err := s.verifyRepo.ByJobID(ctx, jobID)
	if err != nil && !errors.Is(err, verify.ErrNotFound) {
		return fmt.Errorf("load verify set for reset: %w", err)
	}
	if vset != nil && vset.State() == verify.VerifyStateRepairNeeded {
		if err := s.txm.InTx(ctx, func(ctx context.Context) error {
			if err := vset.Reset(); err != nil {
				return err
			}
			return s.verifyRepo.Save(ctx, vset)
		}); err != nil {
			return fmt.Errorf("reset verify set: %w", err)
		}
	}
	return s.runVerify(ctx, jobID)
}

// runVerify executes the full verification flow for a single job.
func (s *Service) runVerify(ctx context.Context, jobID download.JobID) error {
	job, err := s.jobs.ByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}

	jobDir := filepath.Join(s.incompleteDir, strconv.FormatInt(int64(jobID), 10))

	// Build the par2/data path partitioning from the Job aggregate.
	// IsPar2 was set during NZB parsing based on filename suffix
	// (.par2 / .vol* / .par).
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

	// Load-or-create the VerifySet for this job.
	vset, err := s.verifyRepo.ByJobID(ctx, jobID)
	if err != nil && !errors.Is(err, verify.ErrNotFound) {
		return fmt.Errorf("load verify set: %w", err)
	}
	if vset == nil {
		vset = verify.NewVerifySet(verify.NewVerifySetParams{JobID: jobID})
	} else if vset.State().IsTerminal() {
		// Already verified previously; nothing to do.
		s.logger.Info("verify: already terminal, skipping",
			"job_id", jobID, "state", string(vset.State()))
		return nil
	}

	// No PAR2 files at all → can't verify; mark failed with a clear
	// reason. M3 calls this "missing par2" — the upstream sender
	// didn't include parity. M4 / delivery still proceeds; this just
	// means we have no integrity evidence.
	if len(par2Paths) == 0 {
		return s.txm.InTx(ctx, func(ctx context.Context) error {
			vset.MarkStarted(s.now())
			_ = vset.MarkFailed("no .par2 files in job", s.now())
			if err := s.verifyRepo.Save(ctx, vset); err != nil {
				return err
			}
			return s.bus.Publish(ctx, vset.PullEvents()...)
		})
	}

	// Mark started + persist.
	if err := s.txm.InTx(ctx, func(ctx context.Context) error {
		vset.MarkStarted(s.now())
		if err := s.verifyRepo.Save(ctx, vset); err != nil {
			return err
		}
		return s.bus.Publish(ctx, vset.PullEvents()...)
	}); err != nil {
		return fmt.Errorf("mark started: %w", err)
	}

	// Run verification.
	result, err := s.verifier.Verify(ctx, par2Paths, dataPaths)
	if err != nil {
		// Transition to failed.
		_ = s.txm.InTx(ctx, func(ctx context.Context) error {
			_ = vset.MarkFailed(err.Error(), s.now())
			if err := s.verifyRepo.Save(ctx, vset); err != nil {
				return err
			}
			return s.bus.Publish(ctx, vset.PullEvents()...)
		})
		return fmt.Errorf("verifier.Verify: %w", err)
	}

	// Map result to terminal state.
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		var terr error
		switch {
		case result.AllOK():
			terr = vset.MarkOK(s.now())
		default:
			terr = vset.MarkRepairNeeded(result.FailedNames(), s.now())
		}
		if terr != nil {
			return terr
		}
		if err := s.verifyRepo.Save(ctx, vset); err != nil {
			return err
		}
		return s.bus.Publish(ctx, vset.PullEvents()...)
	})
}
