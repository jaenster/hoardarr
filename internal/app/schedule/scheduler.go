// Package schedule wires the domain Task aggregate to a polling
// scheduler loop. Handlers are registered by task name at boot
// (e.g. "outbox.prune" → func() { ... }); the loop tails the DB for
// due tasks, claims them, and invokes the matching handler.
//
// One scheduler per process. Handlers run on a worker pool so a slow
// handler doesn't block faster tasks from claiming.
package schedule

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/schedule"
)

// Scheduler runs the polling loop and dispatches due tasks to
// registered handlers.
type Scheduler struct {
	repo     schedule.Repository
	handlers map[string]schedule.Handler
	mu       sync.RWMutex // guards handlers
	now      func() time.Time
	tick     time.Duration
	workers  int
	log      *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

// Config configures the scheduler. Zero values pick sane defaults.
type Config struct {
	Tick    time.Duration // poll interval; default 5s
	Workers int           // max concurrent handler invocations; default 4
	Now     func() time.Time
	Log     *slog.Logger
}

// New constructs a Scheduler.
func New(repo schedule.Repository, cfg Config) *Scheduler {
	if cfg.Tick <= 0 {
		cfg.Tick = 5 * time.Second
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Scheduler{
		repo:     repo,
		handlers: map[string]schedule.Handler{},
		now:      cfg.Now,
		tick:     cfg.Tick,
		workers:  cfg.Workers,
		log:      cfg.Log,
	}
}

// Register attaches a handler for tasks with the given name. Idempotent
// — re-registering replaces the previous handler. Called at bootstrap
// before Start.
func (s *Scheduler) Register(name string, h schedule.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[name] = h
}

// EnsureTask upserts a task by name. If a row already exists, only the
// cadence is updated (and the task is re-enabled if disabled); the
// next_run_at + run history are preserved so an in-flight cycle isn't
// disturbed by restart. For brand-new tasks we schedule the first run
// for `now + cadence` (recurring) or `firstRun` (oneshot).
//
// Use this at bootstrap to declare built-in tasks like outbox.prune.
func (s *Scheduler) EnsureTask(ctx context.Context, p schedule.NewParams) (schedule.TaskID, error) {
	existing, err := s.repo.ByName(ctx, p.Name)
	if err == nil {
		// Only re-set cadence if it changed; preserve next_run_at so a
		// task that's about to fire doesn't get pushed out.
		if p.Kind == schedule.KindRecurring && existing.Cadence() != p.Cadence {
			if err := existing.SetCadence(p.Cadence, s.now()); err != nil {
				return 0, err
			}
		}
		if !existing.Enabled() {
			existing.SetEnabled(true, s.now())
		}
		if err := s.repo.Save(ctx, existing); err != nil {
			return 0, err
		}
		return existing.ID(), nil
	}
	if !errors.Is(err, schedule.ErrNotFound) {
		return 0, err
	}
	t, err := schedule.New(p, s.now())
	if err != nil {
		return 0, err
	}
	if err := s.repo.Save(ctx, t); err != nil {
		return 0, err
	}
	return t.ID(), nil
}

// Start launches the scheduler loop in the background. Idempotent: a
// second call after Stop relaunches with a fresh ctx. Returns
// immediately; the goroutine survives until Stop or ctx cancellation.
func (s *Scheduler) Start(ctx context.Context) error {
	if s.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		_ = s.Run(runCtx)
	}()
	return nil
}

// Stop cancels the loop and waits for the goroutine to drain.
func (s *Scheduler) Stop() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	<-s.done
	s.cancel = nil
	s.done = nil
	return nil
}

// Run is the polling loop. Returns when ctx is cancelled.
//
// On entry it resets any stale claims left by a previous process,
// then ticks: each tick claims up to `workers` due tasks and dispatches
// them. Handler invocation is itself off-loop so a slow handler doesn't
// block the next tick.
func (s *Scheduler) Run(ctx context.Context) error {
	if n, err := s.repo.ResetStaleClaims(ctx, s.now()); err != nil {
		s.log.Warn("schedule: reset stale claims", "err", err)
	} else if n > 0 {
		s.log.Info("schedule: reset stale claims from previous run", "count", n)
	}

	t := time.NewTicker(s.tick)
	defer t.Stop()
	sem := make(chan struct{}, s.workers)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.dispatchTick(ctx, sem, &wg)
		}
	}
}

func (s *Scheduler) dispatchTick(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup) {
	// Cap the per-tick claim batch to the worker count so we don't
	// hold rows in 'running' that we can't actually invoke right away.
	due, err := s.repo.ClaimDue(ctx, s.now(), s.workers)
	if err != nil {
		s.log.Warn("schedule: claim due", "err", err)
		return
	}
	for _, task := range due {
		s.mu.RLock()
		h, ok := s.handlers[task.Name()]
		s.mu.RUnlock()
		if !ok {
			// No handler registered — release the claim with a
			// failure so the row doesn't stay 'running' forever.
			task.MarkFailed("no handler registered", s.now())
			if err := s.repo.Save(ctx, task); err != nil {
				s.log.Warn("schedule: release orphan", "name", task.Name(), "err", err)
			}
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(task *schedule.Task, h schedule.Handler) {
			defer wg.Done()
			defer func() { <-sem }()
			s.runOne(ctx, task, h)
		}(task, h)
	}
}

func (s *Scheduler) runOne(ctx context.Context, task *schedule.Task, h schedule.Handler) {
	start := s.now()
	err := h(ctx, task.Payload())
	if err != nil {
		s.log.Warn("schedule: task failed",
			"name", task.Name(),
			"err", err,
			"elapsed", s.now().Sub(start))
		task.MarkFailed(err.Error(), s.now())
	} else {
		s.log.Debug("schedule: task ok",
			"name", task.Name(),
			"elapsed", s.now().Sub(start))
		task.MarkSucceeded(s.now())
	}
	if err := s.repo.Save(ctx, task); err != nil {
		s.log.Warn("schedule: persist outcome", "name", task.Name(), "err", err)
	}
}
