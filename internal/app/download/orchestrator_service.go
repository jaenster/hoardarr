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
	accounter     *ByteAccounter // optional; per-server byte tally
	limiter       *Limiter       // optional; bandwidth throttle
	incompleteDir string
	logger        *slog.Logger
	now           func() time.Time
	flushInterval time.Duration
	maxAttempts   int
	baseBackoff   time.Duration
	failHopelessRatio func() float64
	poolFactory   PoolFactory // optional; if set, server.usenet.added events hot-wire new pools
	// concurrencyCap returns the current max concurrent runners.
	// 0 means unlimited; nil means unlimited (no cap configured).
	concurrencyCap func() int

	poolsMu sync.RWMutex
	pools   map[server.ServerID]*nntp.Pool

	mu       sync.Mutex
	runners  map[download.JobID]*runnerHandle
	pending  []download.JobID // jobs waiting for a runner slot
	pendingSet map[download.JobID]struct{}
	subs     []event.Subscription

	rootCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// PoolFactory builds an nntp.Pool for the server identified by id.
// Owned by bootstrap (which knows the dialer + pool options); the
// orchestrator calls it on server.usenet.added/enabled events.
type PoolFactory interface {
	BuildPool(ctx context.Context, id server.ServerID) (*nntp.Pool, error)
}

// OrchestratorServiceParams gathers dependencies for NewOrchestratorService.
type OrchestratorServiceParams struct {
	Repo          download.JobRepository
	Bus           event.Bus
	TxManager     tx.TransactionManager
	Pools         map[server.ServerID]*nntp.Pool
	Accounter     *ByteAccounter // optional; supply to capture per-server byte tallies
	Limiter       *Limiter       // optional; bandwidth throttling
	IncompleteDir string
	Logger        *slog.Logger
	Now           func() time.Time
	FlushInterval time.Duration // optional, default 100ms
	MaxAttempts   int           // optional, default 3
	BaseBackoff   time.Duration // optional, default 200ms
	// FailHopelessRatio returns the SAB fail_hopeless threshold (0-1).
	// Called on every dispatch so live edits propagate. Nil → 0.
	FailHopelessRatio func() float64
	// PoolFactory enables hot-wiring NNTP pools when the operator
	// adds / enables / disables servers from the UI. Nil → static
	// pool set (tests + the historical behaviour).
	PoolFactory PoolFactory
	// ConcurrencyCap returns the current max number of jobs allowed
	// to run in parallel. 0 means unlimited. Nil means unlimited.
	// The orchestrator consults this on every dispatch; live edits
	// from Settings take effect on the next job that lands.
	ConcurrencyCap func() int
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
	pools := p.Pools
	if pools == nil {
		pools = make(map[server.ServerID]*nntp.Pool)
	}
	return &OrchestratorService{
		repo:           p.Repo,
		bus:            p.Bus,
		txm:            p.TxManager,
		pools:          pools,
		accounter:      p.Accounter,
		limiter:        p.Limiter,
		incompleteDir:  p.IncompleteDir,
		logger:         p.Logger,
		now:            p.Now,
		flushInterval:  p.FlushInterval,
		maxAttempts:    p.MaxAttempts,
		baseBackoff:    p.BaseBackoff,
		failHopelessRatio: p.FailHopelessRatio,
		poolFactory:    p.PoolFactory,
		concurrencyCap: p.ConcurrencyCap,
		runners:        make(map[download.JobID]*runnerHandle),
		pendingSet:     make(map[download.JobID]struct{}),
		rootCtx:        rootCtx,
		cancel:         cancel,
	}
}

// NudgePending drains the backlog up to the current concurrency cap.
// Called by the runtime config listener after the cap is raised — a
// no-op when the cap is unlimited or the backlog is empty.
func (s *OrchestratorService) NudgePending() {
	for {
		s.mu.Lock()
		if !s.started {
			s.mu.Unlock()
			return
		}
		if s.concurrencyCap != nil {
			cap := s.concurrencyCap()
			if cap > 0 && len(s.runners) >= cap {
				s.mu.Unlock()
				return
			}
		}
		if len(s.pending) == 0 {
			s.mu.Unlock()
			return
		}
		next := s.pending[0]
		s.pending = s.pending[1:]
		delete(s.pendingSet, next)
		s.mu.Unlock()
		s.startRunner(next)
	}
}

// PoolsSnapshot returns a shallow copy of the current pool set. Safe
// to iterate without holding any orchestrator lock. Used by
// TieredFetcher on every Fetch so live pool changes are picked up
// immediately.
func (s *OrchestratorService) PoolsSnapshot() map[server.ServerID]*nntp.Pool {
	s.poolsMu.RLock()
	defer s.poolsMu.RUnlock()
	out := make(map[server.ServerID]*nntp.Pool, len(s.pools))
	for k, v := range s.pools {
		out[k] = v
	}
	return out
}

// AddPool registers (or replaces) a pool for the given server. Any
// existing pool with the same id is closed before being replaced.
// Idempotent for identical (id, pool) pairs.
func (s *OrchestratorService) AddPool(id server.ServerID, pool *nntp.Pool) {
	s.poolsMu.Lock()
	defer s.poolsMu.Unlock()
	if existing, ok := s.pools[id]; ok && existing != pool {
		existing.Close()
	}
	s.pools[id] = pool
	s.logger.Info("pool registered live", "server_id", int64(id), "pools", len(s.pools))
}

// RemovePool closes and forgets the pool for the given server. No-op
// if no such pool is registered.
func (s *OrchestratorService) RemovePool(id server.ServerID) {
	s.poolsMu.Lock()
	defer s.poolsMu.Unlock()
	if pool, ok := s.pools[id]; ok {
		pool.Close()
		delete(s.pools, id)
		s.logger.Info("pool removed live", "server_id", int64(id), "pools", len(s.pools))
	}
}

// Start subscribes to bus events and kicks off runners for any active
// (non-paused, non-terminal) jobs in the DB.
//
// Idempotent across calls; restartable after Stop. Each Start
// (re)creates the internal rootCtx so a previously-cancelled service
// can be revived (the crash-recovery flow exercises this).
func (s *OrchestratorService) Start(ctx context.Context) error {
	if s.started {
		return nil
	}
	// Reset rootCtx — Stop cancelled the previous one, but a Start
	// after Stop must hand fresh contexts to new runners.
	s.rootCtx, s.cancel = context.WithCancel(context.Background())

	subs, err := s.subscribe()
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	s.subs = subs

	poolCount := len(s.PoolsSnapshot())
	if poolCount == 0 {
		if s.poolFactory != nil {
			s.logger.Info("orchestrator started with no pools; waiting for server.usenet.added events to wire them live")
		} else {
			s.logger.Warn("orchestrator started without any NNTP pools; downloads will not run until a server is added (and the daemon picks them up)")
		}
	}
	active, err := s.repo.Active(ctx)
	if err != nil {
		return fmt.Errorf("list active jobs: %w", err)
	}
	havePools := s.haveAnyUsablePool()
	for _, j := range active {
		if j.State() == download.JobStatePaused {
			continue
		}
		if j.State().IsTerminal() {
			continue
		}
		// If a previous run left jobs parked in waiting_for_server
		// and pools now exist, unpark them before starting the runner.
		if j.State() == download.JobStateWaitingForServer {
			if !havePools {
				// Still no pools — leave parked, don't even start a
				// runner (the runner would just re-park instantly).
				continue
			}
			if err := s.unparkJobWaiting(ctx, j.ID()); err != nil {
				s.logger.Warn("startup: unpark job failed",
					"job_id", int64(j.ID()), "err", err)
				continue
			}
		}
		s.startRunner(j.ID())
	}

	s.started = true
	s.logger.Info("orchestrator service started",
		"active_jobs", len(s.runners),
		"pools", poolCount)
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

	// Drop idle pool conns so the next Start dials fresh — avoids
	// reusing conns that were idle when their previous holder
	// cancelled. The pools themselves remain alive and ready for
	// new acquires.
	for _, p := range s.PoolsSnapshot() {
		p.CloseIdle()
	}

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
	if s.poolFactory != nil {
		pairs = append(pairs,
			spec{"orchestrator-server-added", "server.usenet.added", s.onServerAddedOrEnabled},
			spec{"orchestrator-server-enabled", "server.usenet.enabled", s.onServerAddedOrEnabled},
			spec{"orchestrator-server-updated", "server.usenet.updated", s.onServerAddedOrEnabled},
			spec{"orchestrator-server-disabled", "server.usenet.disabled", s.onServerDisabledOrRemoved},
			spec{"orchestrator-server-removed", "server.usenet.removed", s.onServerDisabledOrRemoved},
		)
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

// onServerAddedOrEnabled handles server.usenet.{added,enabled,updated}
// by building a pool for the now-eligible server (if the factory is
// wired) and kicking any active jobs that were sitting idle for lack
// of pools. Idempotent — AddPool replaces an existing pool with the
// same id, so duplicate events don't leak conns.
func (s *OrchestratorService) onServerAddedOrEnabled(ctx context.Context, env event.Envelope) error {
	if s.poolFactory == nil {
		return nil
	}
	var idVal struct {
		ID server.ServerID `json:"id"`
	}
	if err := json.Unmarshal(env.Payload, &idVal); err != nil {
		return fmt.Errorf("decode server event: %w", err)
	}
	pool, err := s.poolFactory.BuildPool(ctx, idVal.ID)
	if err != nil {
		s.logger.Warn("hot-wire pool failed", "server_id", int64(idVal.ID), "err", err)
		return nil
	}
	if pool == nil {
		// Server disabled or otherwise ineligible — drop any existing pool.
		s.RemovePool(idVal.ID)
		return nil
	}
	s.AddPool(idVal.ID, pool)
	s.kickIdleJobs()
	return nil
}

// onServerDisabledOrRemoved tears down the pool for a server that's
// no longer eligible to serve fetches.
func (s *OrchestratorService) onServerDisabledOrRemoved(_ context.Context, env event.Envelope) error {
	var idVal struct {
		ID server.ServerID `json:"id"`
	}
	if err := json.Unmarshal(env.Payload, &idVal); err != nil {
		return fmt.Errorf("decode server event: %w", err)
	}
	s.RemovePool(idVal.ID)
	return nil
}

// kickIdleJobs starts runners for any queued/downloading jobs that
// don't currently have one. Called after AddPool so jobs that were
// queued before a pool existed pick up automatically. Also unparks
// any jobs that were previously waiting_for_server.
func (s *OrchestratorService) kickIdleJobs() {
	active, err := s.repo.Active(s.rootCtx)
	if err != nil {
		s.logger.Warn("kick-idle: list active failed", "err", err)
		return
	}
	for _, j := range active {
		if j.State() == download.JobStatePaused {
			continue
		}
		if j.State().IsTerminal() {
			continue
		}
		if j.State() == download.JobStateWaitingForServer {
			if err := s.unparkJobWaiting(s.rootCtx, j.ID()); err != nil {
				s.logger.Warn("kick-idle: unpark job failed",
					"job_id", int64(j.ID()), "err", err)
				continue
			}
		}
		s.startRunner(j.ID())
	}
}

// parkJobWaiting transitions a job to JobStateWaitingForServer in a
// single tx, persisting the row and publishing JobWaitingForServer so
// the UI / webhooks see the transition. Idempotent.
func (s *OrchestratorService) parkJobWaiting(ctx context.Context, id download.JobID, reason string) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.repo.ByID(ctx, id)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
		j.MarkWaitingForServer(reason, s.now())
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

// unparkJobWaiting moves a job out of JobStateWaitingForServer (back
// to queued, emitting JobResumed). Called by kickIdleJobs after a
// server becomes available.
func (s *OrchestratorService) unparkJobWaiting(ctx context.Context, id download.JobID) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		j, err := s.repo.ByID(ctx, id)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
		j.ResumeFromWait(s.now())
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

// startRunner spawns a goroutine that drives jobID to completion if a
// slot is available under the concurrency cap. If the cap is reached,
// the job is appended to the pending backlog and started when a
// running job finishes. Idempotent for ids that are already running
// or already pending.
func (s *OrchestratorService) startRunner(id download.JobID) {
	s.mu.Lock()
	if _, exists := s.runners[id]; exists {
		s.mu.Unlock()
		return
	}
	if _, queued := s.pendingSet[id]; queued {
		s.mu.Unlock()
		return
	}
	if s.concurrencyCap != nil {
		cap := s.concurrencyCap()
		if cap > 0 && len(s.runners) >= cap {
			s.pending = append(s.pending, id)
			s.pendingSet[id] = struct{}{}
			s.logger.Info("orchestrator: cap reached, queuing job",
				"job_id", int64(id), "cap", cap, "active", len(s.runners),
				"pending", len(s.pending))
			s.mu.Unlock()
			return
		}
	}
	runCtx, runCancel := context.WithCancel(s.rootCtx)
	handle := &runnerHandle{cancel: runCancel, done: make(chan struct{})}
	s.runners[id] = handle
	s.mu.Unlock()

	s.wg.Add(1)
	go s.runJob(runCtx, id, handle)
}

// stopRunner cancels the runner for id and waits for it to exit. Also
// removes the id from the pending backlog so a paused-while-queued
// job doesn't get auto-started later. No-op if no runner exists.
func (s *OrchestratorService) stopRunner(id download.JobID) {
	s.mu.Lock()
	// Drop from pending if it was waiting for a slot.
	if _, queued := s.pendingSet[id]; queued {
		delete(s.pendingSet, id)
		filtered := s.pending[:0]
		for _, p := range s.pending {
			if p != id {
				filtered = append(filtered, p)
			}
		}
		s.pending = filtered
	}
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
		// A slot freed; drain the backlog if any jobs were queued.
		s.NudgePending()
	}()

	// No pools yet? Park the job in a dedicated waiting_for_server
	// state so the UI reflects what's actually going on, and exit the
	// runner cleanly. The orchestrator's server-added handler unparks
	// the job (ResumeFromWait → kickIdleJobs) the moment a server
	// becomes available, so this isn't a dead-end — it just stops the
	// runner from holding a concurrency slot while no work is possible.
	if !s.haveAnyUsablePool() {
		if err := s.parkJobWaiting(s.rootCtx, id, "no enabled usenet server"); err != nil {
			s.logger.Error("orchestrator: park job failed", "job_id", int64(id), "err", err)
		}
		return
	}

	// Tiered fetcher handles priority/backup/metered selection per
	// fetch call. The hint server id we pass to NewOrchestrator below
	// is purely for logs / metrics — TieredFetcher ignores it.
	fetcher := NewTieredFetcher(s.PoolsSnapshot, s.accounter, s.limiter, s.logger)
	hintID, hintMax := s.dispatchHint()

	hopeless := 0.0
	if s.failHopelessRatio != nil {
		hopeless = s.failHopelessRatio()
	}
	orch := NewOrchestrator(
		s.repo, fetcher, s.bus, s.txm,
		hintID, hintMax, s.incompleteDir,
		OrchestratorOptions{
			Logger:            s.logger,
			Now:               s.now,
			FlushInterval:     s.flushInterval,
			MaxAttempts:       s.maxAttempts,
			BaseBackoff:       s.baseBackoff,
			FailHopelessRatio: hopeless,
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

// haveAnyUsablePool reports whether at least one enabled, non-quota-
// exhausted pool exists. Used as a fast-fail at runner-start time.
func (s *OrchestratorService) haveAnyUsablePool() bool {
	for _, p := range s.PoolsSnapshot() {
		srv := p.Server()
		if srv.Enabled() && !srv.QuotaExhausted() {
			return true
		}
	}
	return false
}

// dispatchHint returns (id, maxConns) of the highest-priority usable
// pool. The id is a label only — actual fetches go through the
// tiered fetcher. The maxConns value sizes the orchestrator's worker
// pool; we use the highest-priority server's cap as a reasonable
// baseline (alternative: sum across the whole tier, but that risks
// hammering backup providers when primaries are healthy).
func (s *OrchestratorService) dispatchHint() (server.ServerID, int) {
	snap := s.PoolsSnapshot()
	var best *nntp.Pool
	for _, p := range snap {
		srv := p.Server()
		if !srv.Enabled() || srv.QuotaExhausted() || srv.Backup() {
			continue
		}
		if best == nil || srv.Priority() < best.Server().Priority() {
			best = p
		}
	}
	if best == nil {
		// Only backups available — use whichever first.
		for _, p := range snap {
			if p.Server().Enabled() && !p.Server().QuotaExhausted() {
				best = p
				break
			}
		}
	}
	if best == nil {
		return 0, 1
	}
	return best.Server().ID(), best.Server().MaxConns()
}
