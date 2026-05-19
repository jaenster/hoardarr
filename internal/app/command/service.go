// Package command is the application service that owns the command
// queue + worker dispatch. Handlers register against the service by
// name; Submit enqueues; a background worker claims, dispatches,
// records the result.
//
// One worker per process is sufficient: command volume is operator-
// driven (UI clicks), and handlers are intentionally short. If a
// future handler is long-running, it should kick off its work as
// goroutines tracked by the relevant domain context and return —
// the command is "dispatched", not "executed in-band".
package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	domaincommand "github.com/jaenster/hoardarr/internal/domain/command"
)

// Handler runs the work for a single command. Returning a non-nil
// error records the command as failed; nil = successful.
type Handler func(ctx context.Context, body []byte) error

// Service owns the handler registry + the worker goroutine.
type Service struct {
	logger *slog.Logger
	repo   domaincommand.Repository

	mu       sync.RWMutex
	handlers map[string]Handler

	poll  time.Duration
	stop  chan struct{}
	done  chan struct{}
	armed chan struct{} // poke the worker after Submit

	// startOnce/stopOnce keep Start and Stop safely idempotent. Tests
	// that build the App but never call Run still defer Shutdown,
	// which calls Stop — without the guards, Stop would block forever
	// on `<-s.done` because no worker loop was launched. A clean Stop
	// before any Start is a no-op.
	startOnce sync.Once
	stopOnce  sync.Once
	started   bool
}

// Params gathers New() inputs.
type Params struct {
	Logger *slog.Logger
	Repo   domaincommand.Repository
	// PollInterval is the fallback wake-up cadence when no Submit
	// has armed the worker. Defaults to 2s — submitted commands
	// fire near-instantly via the armed channel; polling is only
	// the safety net for scheduler-issued commands.
	PollInterval time.Duration
}

// New constructs the service. Call Start to begin worker dispatch.
func New(p Params) *Service {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.PollInterval <= 0 {
		p.PollInterval = 2 * time.Second
	}
	return &Service{
		logger:   p.Logger,
		repo:     p.Repo,
		handlers: map[string]Handler{},
		poll:     p.PollInterval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		armed:    make(chan struct{}, 1),
	}
}

// Register associates a handler with a name. Idempotent — re-
// registering replaces the previous handler.
func (s *Service) Register(name string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[name] = h
}

// Submit enqueues a command. The worker picks it up on its next
// wake. Returns the persisted CommandID.
func (s *Service) Submit(ctx context.Context, name string, body []byte, trigger domaincommand.Trigger) (domaincommand.CommandID, error) {
	if _, ok := s.lookupHandler(name); !ok {
		return 0, fmt.Errorf("command: no handler registered for %q", name)
	}
	c, err := domaincommand.New(domaincommand.NewParams{
		Name:    name,
		Body:    body,
		Trigger: trigger,
	}, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	if err := s.repo.Save(ctx, c); err != nil {
		return 0, fmt.Errorf("command: save: %w", err)
	}
	s.arm()
	return c.ID(), nil
}

// Start launches the background worker. Reset stale 'running' claims
// first so a crashed handler doesn't permanently park its command.
// Subsequent calls after the first are no-ops.
func (s *Service) Start(ctx context.Context) error {
	var startErr error
	s.startOnce.Do(func() {
		// Anything 'running' for >10 min when we start is presumed stale.
		cutoff := time.Now().Add(-10 * time.Minute).UnixMilli()
		if n, err := s.repo.ResetStaleClaims(ctx, cutoff); err != nil {
			s.logger.Warn("command: reset stale claims failed", "err", err)
		} else if n > 0 {
			s.logger.Info("command: reset stale running commands", "count", n)
		}
		s.started = true
		go s.loop(ctx)
		s.logger.Info("command service started")
	})
	return startErr
}

// Stop signals the worker loop to exit and waits for it. Safe to call
// before Start (no-op) and safe to call multiple times.
func (s *Service) Stop() error {
	s.stopOnce.Do(func() {
		if !s.started {
			// Nothing to wait for — close stop so any later (logically
			// erroneous) Start finds the channel already closed and
			// the worker exits immediately if it gets that far.
			close(s.stop)
			close(s.done)
			return
		}
		close(s.stop)
		<-s.done
	})
	return nil
}

func (s *Service) loop(ctx context.Context) {
	defer close(s.done)
	t := time.NewTicker(s.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-t.C:
		case <-s.armed:
		}
		// Drain in a sub-loop so a burst of submitted commands
		// runs back-to-back without waiting for the next tick.
		for {
			ran, err := s.runOne(ctx)
			if err != nil {
				s.logger.Warn("command: dispatch error", "err", err)
				break
			}
			if !ran {
				break
			}
		}
	}
}

func (s *Service) runOne(ctx context.Context) (bool, error) {
	c, err := s.repo.ClaimNext(ctx)
	if err != nil {
		return false, err
	}
	if c == nil {
		return false, nil
	}
	h, ok := s.lookupHandler(c.Name())
	if !ok {
		// Handler was unregistered between Submit and dispatch.
		// Record as failed with a clear message; don't crashloop.
		c.MarkCompleted(errors.New("no handler registered"), time.Now().UTC())
		_ = s.repo.Save(ctx, c)
		s.logger.Warn("command: unknown handler", "name", c.Name(), "id", int64(c.ID()))
		return true, nil
	}
	s.logger.Info("command: dispatching", "name", c.Name(), "id", int64(c.ID()))
	herr := h(ctx, c.Body())
	c.MarkCompleted(herr, time.Now().UTC())
	if err := s.repo.Save(ctx, c); err != nil {
		s.logger.Error("command: save completion failed", "id", int64(c.ID()), "err", err)
	}
	if herr != nil {
		s.logger.Warn("command: handler failed", "name", c.Name(), "id", int64(c.ID()), "err", herr)
	} else {
		s.logger.Info("command: completed", "name", c.Name(), "id", int64(c.ID()), "duration", c.Duration())
	}
	return true, nil
}

func (s *Service) lookupHandler(name string) (Handler, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.handlers[name]
	return h, ok
}

func (s *Service) arm() {
	select {
	case s.armed <- struct{}{}:
	default:
	}
}

// List proxies to the repo for the REST handler.
func (s *Service) List(ctx context.Context, limit int) ([]*domaincommand.Command, error) {
	return s.repo.List(ctx, limit)
}

// ByID proxies to the repo for the REST handler.
func (s *Service) ByID(ctx context.Context, id domaincommand.CommandID) (*domaincommand.Command, error) {
	return s.repo.ByID(ctx, id)
}

// Names returns the registered handler names so the UI can show a
// dropdown of available commands the operator can trigger.
func (s *Service) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.handlers))
	for n := range s.handlers {
		out = append(out, n)
	}
	return out
}
