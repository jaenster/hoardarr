package download

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/server"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// OrchestratorService is hoardarr's long-lived download driver.
//
// It owns:
//   - Per-server NNTP pools (passed in at construction).
//   - A registry of per-job runners (one goroutine per active job)
//     that each instantiate the M1-style Orchestrator and run it to
//     completion.
//
// The service drives state via the event bus:
//
//	JobCreated   → start a runner
//	JobPaused    → cancel the runner (in-flight segments leak to pending)
//	JobResumed   → restart the runner
//	JobRemoved   → cancel the runner
//
// Runner cancellation is safe because the orchestrator's segment writes
// are idempotent (WriteAt at known offsets) and segments stay pending
// until persisted as done. A canceled runner can be restarted and will
// re-fetch any incomplete segments without corruption.
//
// On startup (Start), the service inspects the DB and starts runners
// for any non-paused, non-terminal jobs. This recovers cleanly from
// crashes: jobs left mid-download just resume.
type OrchestratorService struct {
	repo          download.JobRepository
	bus           event.Bus
	txm           tx.TransactionManager
	pools         map[server.ServerID]*nntp.Pool
	incompleteDir string
	logger        *slog.Logger
	now           func() time.Time
	flushInterval time.Duration

	mu      sync.Mutex
	runners map[download.JobID]*runnerHandle
	subs    []event.Subscription

	rootCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// OrchestratorServiceParams gathers dependencies for NewOrchestratorService.
type OrchestratorServiceParams struct {
	Repo          download.JobRepository
	Bus           event.Bus
	TxManager     tx.TransactionManager
	Pools         map[server.ServerID]*nntp.Pool
	IncompleteDir string
	Logger        *slog.Logger
	Now           func() time.Time
	FlushInterval time.Duration // optional, default 100ms
}

// NewOrchestratorService constructs the service.
//
// Pools must contain at least one entry. v0.1 picks the highest-priority
// enabled server (caller responsibility — bootstrap looks up the
// ServerRepo, gets the active server, and passes its pool here).
// Multi-server failover lands post-v0.1.
func NewOrchestratorService(p OrchestratorServiceParams) *OrchestratorService {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	return &OrchestratorService{
		repo:          p.Repo,
		bus:           p.Bus,
		txm:           p.TxManager,
		pools:         p.Pools,
		incompleteDir: p.IncompleteDir,
		logger:        p.Logger,
		now:           p.Now,
		flushInterval: p.FlushInterval,
		runners:       make(map[download.JobID]*runnerHandle),
		rootCtx:       rootCtx,
		cancel:        cancel,
	}
}

// Start subscribes to bus events and kicks off runners for any active
// (non-paused, non-terminal) jobs in the DB. Idempotent.
func (s *OrchestratorService) Start(ctx context.Context) error {
	if s.started {
		return nil
	}

	subs, err := s.subscribe()
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	s.subs = subs

	if len(s.pools) == 0 {
		s.logger.Warn("orchestrator service started without any NNTP pools; downloads will not run until a server is added and the daemon restarts")
	} else {
		active, err := s.repo.Active(ctx)
		if err != nil {
			return fmt.Errorf("list active jobs: %w", err)
		}
		for _, j := range active {
			if j.State() == download.JobStatePaused {
				continue
			}
			if j.State().IsTerminal() {
				continue
			}
			s.startRunner(j.ID())
		}
	}

	s.started = true
	s.logger.Info("orchestrator service started",
		"active_jobs", len(s.runners),
		"pools", len(s.pools))
	return nil
}

// Stop closes subscriptions and cancels all runners, blocking until
// they exit. Idempotent.
func (s *OrchestratorService) Stop() error {
	if !s.started {
		return nil
	}
	s.started = false

	for _, sub := range s.subs {
		_ = sub.Close()
	}
	s.subs = nil

	s.cancel()
	s.wg.Wait()
	s.logger.Info("orchestrator service stopped")
	return nil
}

// ActiveJobs returns the set of jobs currently being driven. Useful
// for /api/v1/system/status and tests.
func (s *OrchestratorService) ActiveJobs() []download.JobID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]download.JobID, 0, len(s.runners))
	for id := range s.runners {
		out = append(out, id)
	}
	return out
}

// runnerHandle tracks one in-flight per-job runner.
type runnerHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *OrchestratorService) subscribe() ([]event.Subscription, error) {
	subs := make([]event.Subscription, 0, 4)
	type spec struct {
		name    string
		topic   string
		handler event.Handler
	}
	pairs := []spec{
		{"orchestrator-job-created", "download.job.created", s.onJobCreated},
		{"orchestrator-job-paused", "download.job.paused", s.onJobPaused},
		{"orchestrator-job-resumed", "download.job.resumed", s.onJobResumed},
		{"orchestrator-job-removed", "download.job.removed", s.onJobRemoved},
	}
	for _, p := range pairs {
		sub, err := s.bus.Subscribe(p.name, p.topic, p.handler)
		if err != nil {
			for _, prior := range subs {
				_ = prior.Close()
			}
			return nil, fmt.Errorf("subscribe %s: %w", p.topic, err)
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (s *OrchestratorService) onJobCreated(_ context.Context, env event.Envelope) error {
	var e download.JobCreated
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode JobCreated: %w", err)
	}
	s.startRunner(e.ID)
	return nil
}

func (s *OrchestratorService) onJobPaused(_ context.Context, env event.Envelope) error {
	var e download.JobPaused
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode JobPaused: %w", err)
	}
	s.stopRunner(e.ID)
	return nil
}

func (s *OrchestratorService) onJobResumed(_ context.Context, env event.Envelope) error {
	var e download.JobResumed
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode JobResumed: %w", err)
	}
	s.startRunner(e.ID)
	return nil
}

func (s *OrchestratorService) onJobRemoved(_ context.Context, env event.Envelope) error {
	var e download.JobRemoved
	if err := json.Unmarshal(env.Payload, &e); err != nil {
		return fmt.Errorf("decode JobRemoved: %w", err)
	}
	s.stopRunner(e.ID)
	return nil
}

// startRunner spawns a goroutine that drives jobID to completion. If
// a runner for this id is already active, this is a no-op.
func (s *OrchestratorService) startRunner(id download.JobID) {
	s.mu.Lock()
	if _, exists := s.runners[id]; exists {
		s.mu.Unlock()
		return
	}
	runCtx, runCancel := context.WithCancel(s.rootCtx)
	handle := &runnerHandle{cancel: runCancel, done: make(chan struct{})}
	s.runners[id] = handle
	s.mu.Unlock()

	s.wg.Add(1)
	go s.runJob(runCtx, id, handle)
}

// stopRunner cancels the runner for id and waits for it to exit.
// No-op if no runner exists.
func (s *OrchestratorService) stopRunner(id download.JobID) {
	s.mu.Lock()
	handle, exists := s.runners[id]
	s.mu.Unlock()
	if !exists {
		return
	}
	handle.cancel()
	<-handle.done
}

func (s *OrchestratorService) runJob(ctx context.Context, id download.JobID, handle *runnerHandle) {
	defer s.wg.Done()
	defer close(handle.done)
	defer func() {
		s.mu.Lock()
		delete(s.runners, id)
		s.mu.Unlock()
	}()

	pool := s.pickPool()
	if pool == nil {
		s.logger.Error("orchestrator: no pool available", "job_id", id)
		return
	}
	srv := pool.Server()
	fetcher := NewPoolFetcher(pool)

	orch := NewOrchestrator(
		s.repo, fetcher, s.bus, s.txm,
		srv.ID(), srv.MaxConns(), s.incompleteDir,
		OrchestratorOptions{
			Logger:        s.logger,
			Now:           s.now,
			FlushInterval: s.flushInterval,
		},
	)

	if err := orch.Run(ctx, id); err != nil {
		if errors.Is(err, context.Canceled) {
			s.logger.Info("orchestrator: runner canceled", "job_id", id)
			return
		}
		s.logger.Error("orchestrator: runner failed", "job_id", id, "err", err)
	}
}

// pickPool returns the highest-priority pool. M2 is single-server in
// practice; multi-server failover lands later.
func (s *OrchestratorService) pickPool() *nntp.Pool {
	var best *nntp.Pool
	for _, p := range s.pools {
		if !p.Server().Enabled() {
			continue
		}
		if best == nil || p.Server().Priority() < best.Server().Priority() {
			best = p
		}
	}
	return best
}
