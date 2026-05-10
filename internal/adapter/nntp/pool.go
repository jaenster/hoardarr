package nntp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// Pool manages a bounded set of NNTP connections to one Usenet server.
//
// The semaphore caps concurrent in-flight fetches at server.MaxConns.
// An idle stack (LIFO) lets recently-used conns be returned to the
// pool and reused for the next fetch without re-dialing or
// re-authenticating.
//
// Conns released with a non-nil error are closed instead of being
// returned to idle — their counterparty likely saw a protocol violation
// and the next request would error too.
//
// A background reaper closes conns idle past IdleTTL.
type Pool struct {
	srv    *server.UsenetServer
	logger *slog.Logger

	staleThreshold time.Duration
	idleTTL        time.Duration

	sem    chan struct{}
	mu     sync.Mutex
	idle   []*Conn
	closed atomic.Bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// PoolOptions tunes Pool behaviour. Zero values are sensible.
type PoolOptions struct {
	// StaleThreshold is the conn age beyond which Acquire validates it
	// with DATE before handing out. Default 30s.
	StaleThreshold time.Duration

	// IdleTTL is how long an idle conn lives before the reaper closes
	// it. Default 60s.
	IdleTTL time.Duration

	// Logger is used for reaper/health diagnostics.
	Logger *slog.Logger
}

func (o PoolOptions) withDefaults() PoolOptions {
	if o.StaleThreshold == 0 {
		o.StaleThreshold = 30 * time.Second
	}
	if o.IdleTTL == 0 {
		o.IdleTTL = 60 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// NewPool constructs a Pool for s. Starts a background reaper.
func NewPool(s *server.UsenetServer, opts PoolOptions) *Pool {
	opts = opts.withDefaults()
	p := &Pool{
		srv:            s,
		logger:         opts.Logger,
		staleThreshold: opts.StaleThreshold,
		idleTTL:        opts.IdleTTL,
		sem:            make(chan struct{}, s.MaxConns()),
		stop:           make(chan struct{}),
	}
	p.wg.Add(1)
	go p.reapLoop()
	return p
}

// Server returns the UsenetServer this pool services.
func (p *Pool) Server() *server.UsenetServer { return p.srv }

// Release is the signature of the function returned by Acquire. Pass
// a non-nil error if the caller saw a protocol violation, network
// error, or any condition that suggests the conn is unhealthy; the
// pool will close it instead of returning it to idle.
type Release func(returnErr error)

// Acquire blocks until a conn is available or ctx is done. The
// returned conn is authenticated and MODE READER has been attempted.
//
// The caller MUST call Release exactly once when finished with the
// conn. Forgetting to release leaks a semaphore token; subsequent
// Acquires will eventually block forever.
func (p *Pool) Acquire(ctx context.Context) (*Conn, Release, error) {
	if p.closed.Load() {
		return nil, nil, ErrPoolClosed
	}

	// Acquire a token (gates concurrency at MaxConns).
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	conn, err := p.checkout(ctx)
	if err != nil {
		<-p.sem
		return nil, nil, err
	}

	var released atomic.Bool
	release := Release(func(returnErr error) {
		if !released.CompareAndSwap(false, true) {
			return
		}
		if returnErr != nil || p.closed.Load() {
			_ = conn.Close()
		} else {
			p.mu.Lock()
			p.idle = append(p.idle, conn)
			p.mu.Unlock()
		}
		<-p.sem
	})
	return conn, release, nil
}

// checkout pops an idle conn (validating staleness) or dials a new one.
func (p *Pool) checkout(ctx context.Context) (*Conn, error) {
	for {
		p.mu.Lock()
		if len(p.idle) == 0 {
			p.mu.Unlock()
			break
		}
		// LIFO: newest first (most likely still healthy).
		n := len(p.idle) - 1
		c := p.idle[n]
		p.idle[n] = nil
		p.idle = p.idle[:n]
		p.mu.Unlock()

		if time.Since(c.LastUsed()) > p.staleThreshold {
			if _, err := c.Date(ctx); err != nil {
				p.logger.Debug("nntp pool: stale conn check failed; closing", "err", err)
				_ = c.Close()
				continue
			}
		}
		return c, nil
	}

	// No idle conn — dial new.
	c, err := Dial(ctx, p.srv)
	if err != nil {
		return nil, err
	}
	if err := c.Authenticate(ctx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("authenticate: %w", err)
	}
	if err := c.ModeReader(ctx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("mode reader: %w", err)
	}
	return c, nil
}

// Close stops the reaper and closes every idle conn. In-flight conns
// (held by callers via Acquire) are not interrupted; they're closed by
// release() once Pool.closed is observed.
func (p *Pool) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(p.stop)
	p.wg.Wait()

	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()

	for _, c := range idle {
		_ = c.Close()
	}
	return nil
}

// reapLoop is the background goroutine that closes conns idle past
// IdleTTL. Runs every IdleTTL/2 (bounded by 10s minimum).
func (p *Pool) reapLoop() {
	defer p.wg.Done()
	interval := p.idleTTL / 2
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.reap()
		}
	}
}

func (p *Pool) reap() {
	cutoff := time.Now().Add(-p.idleTTL)
	var toClose []*Conn

	p.mu.Lock()
	kept := p.idle[:0]
	for _, c := range p.idle {
		if c.LastUsed().Before(cutoff) {
			toClose = append(toClose, c)
		} else {
			kept = append(kept, c)
		}
	}
	// Avoid retaining backing array references for closed conns.
	for i := len(kept); i < len(p.idle); i++ {
		p.idle[i] = nil
	}
	p.idle = kept
	p.mu.Unlock()

	for _, c := range toClose {
		_ = c.Close()
	}
}

// IdleCount reports the number of conns currently in the idle stack.
// Useful for tests and metrics.
func (p *Pool) IdleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

// CloseIdle closes every conn currently in the idle stack without
// stopping the pool. Useful between orchestrator runs (e.g. after a
// pause/resume cycle) so a fresh runner always dials clean conns and
// can't trip over stale half-closed sockets that were idle when their
// previous holder cancelled.
func (p *Pool) CloseIdle() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, c := range idle {
		_ = c.Close()
	}
}

// ErrPoolClosed is returned by Acquire after Close.
var ErrPoolClosed = errors.New("nntp: pool closed")
