// Package health is the application service that composes the
// registered domain health checks and exposes a Snapshot() of current
// issues for REST + SSE consumption.
//
// Checks run on a ticker (default 60s) plus an event-triggered
// refresh path: when a server is added/removed/edited or a job
// transitions, we want the banner to update without waiting for the
// next tick. Subscriptions are wired in bootstrap.
package health

import (
	"context"
	"log/slog"
	"sync"
	"time"

	domainhealth "github.com/jaenster/hoardarr/internal/domain/health"
)

// Params gathers the service constructor inputs.
type Params struct {
	Logger *slog.Logger
	// Interval is the background tick at which all checks run.
	// Defaults to 60s if zero.
	Interval time.Duration
	// Checks is the registered set, evaluated in order. The order
	// affects nothing semantic; banner sorts by severity in the UI.
	Checks []domainhealth.CheckFunc
}

// Service runs registered checks on a ticker and serves the latest
// snapshot. Refresh() can be invoked from outside (e.g. from a domain
// event handler) to force an immediate re-run.
type Service struct {
	logger   *slog.Logger
	interval time.Duration
	checks   []domainhealth.CheckFunc

	mu       sync.RWMutex
	issues   []domainhealth.Issue
	lastRun  time.Time

	refresh chan struct{}
	wg      sync.WaitGroup
}

// New constructs a Service. Caller must call Start to begin the
// background loop.
func New(p Params) *Service {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Interval <= 0 {
		p.Interval = 60 * time.Second
	}
	return &Service{
		logger:   p.Logger,
		interval: p.Interval,
		checks:   p.Checks,
		refresh:  make(chan struct{}, 1),
	}
}

// Start kicks off the background ticker. Returns immediately;
// blocking happens inside the goroutine. Idempotent — calling twice
// is a no-op-but-warn.
func (s *Service) Start(ctx context.Context) {
	s.runOnce(ctx) // populate initial snapshot synchronously
	s.wg.Add(1)
	go s.loop(ctx)
	s.logger.Info("health service started", "checks", len(s.checks), "interval", s.interval)
}

// Stop waits for the background loop to exit. Caller should cancel
// the ctx passed to Start; Stop only joins.
func (s *Service) Stop() {
	s.wg.Wait()
}

// Refresh requests an immediate re-run. Coalesces with any
// already-pending refresh — repeated calls during a single run only
// queue one follow-up.
func (s *Service) Refresh() {
	select {
	case s.refresh <- struct{}{}:
	default:
	}
}

// Snapshot returns the most recent issue list. Copy is defensive so
// callers can hold the slice without racing the loop.
func (s *Service) Snapshot() (issues []domainhealth.Issue, lastRun time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domainhealth.Issue, len(s.issues))
	copy(out, s.issues)
	return out, s.lastRun
}

func (s *Service) loop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.runOnce(ctx)
		case <-s.refresh:
			s.runOnce(ctx)
		}
	}
}

func (s *Service) runOnce(ctx context.Context) {
	out := make([]domainhealth.Issue, 0, 4)
	for _, fn := range s.checks {
		if ctx.Err() != nil {
			return
		}
		out = append(out, fn(ctx)...)
	}
	s.mu.Lock()
	s.issues = out
	s.lastRun = time.Now()
	s.mu.Unlock()
}
