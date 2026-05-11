package download

// ByteAccounter aggregates per-server byte consumption in memory and
// flushes the running totals to persistence on a periodic tick (and
// on graceful shutdown).
//
// Why aggregate before persisting:
//
//   - The fetcher reports bytes on every BODY close, which for a busy
//     job is hundreds per second across N pools. Persisting each delta
//     would mean N writes/sec into a single-conn SQLite — perfectly
//     correct but wasteful, since the operator-visible granularity for
//     "block account consumption" is on the order of MB / minutes.
//
//   - On graceful shutdown we drain any held deltas, so no progress
//     is lost. On hard crash a few seconds' worth of consumption is
//     forgotten, which is acceptable for this use case.

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// ByteAccounter holds the staged deltas. Safe for concurrent Add.
type ByteAccounter struct {
	mu       sync.Mutex
	deltas   map[server.ServerID]int64
	observer func(n int64) // optional; gets every Add (n only)
}

// NewByteAccounter constructs an empty accounter.
func NewByteAccounter() *ByteAccounter {
	return &ByteAccounter{deltas: make(map[server.ServerID]int64)}
}

// WithObserver installs a per-Add callback. Used by the throughput
// tracker to record bytes/sec without coupling fetcher to system.
// Returns the accounter for chaining.
func (a *ByteAccounter) WithObserver(fn func(n int64)) *ByteAccounter {
	a.mu.Lock()
	a.observer = fn
	a.mu.Unlock()
	return a
}

// Add atomically increments the running delta for one server, then
// invokes the observer if one is registered. The observer fires
// outside the lock so it can do its own synchronization without risk
// of deadlock against Drain.
func (a *ByteAccounter) Add(id server.ServerID, n int64) {
	if n <= 0 {
		return
	}
	a.mu.Lock()
	a.deltas[id] += n
	obs := a.observer
	a.mu.Unlock()
	if obs != nil {
		obs(n)
	}
}

// Drain returns the accumulated deltas and resets the map. Used by
// the flush loop.
func (a *ByteAccounter) Drain() map[server.ServerID]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.deltas
	a.deltas = make(map[server.ServerID]int64)
	return out
}

// ServerByteRepo is the slice of the server repository the accounter
// flusher needs. Defining it here as an interface avoids importing
// the concrete sqlite type into this app package.
type ServerByteRepo interface {
	IncrementUsedBytes(ctx context.Context, id server.ServerID, n int64) error
}

// ByteFlusher periodically drains an accounter and bumps the
// persistent used_bytes counters.
type ByteFlusher struct {
	accounter *ByteAccounter
	repo      ServerByteRepo
	interval  time.Duration
	logger    *slog.Logger

	mu      sync.Mutex
	started bool

	wg     sync.WaitGroup
	stopCh chan struct{}
}

// NewByteFlusher constructs a flusher. interval defaults to 10s when
// zero.
func NewByteFlusher(a *ByteAccounter, repo ServerByteRepo, interval time.Duration, logger *slog.Logger) *ByteFlusher {
	if interval == 0 {
		interval = 10 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ByteFlusher{
		accounter: a, repo: repo, interval: interval, logger: logger,
		stopCh: make(chan struct{}),
	}
}

// Start launches the periodic flush goroutine. Idempotent.
func (f *ByteFlusher) Start(ctx context.Context) {
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return
	}
	f.started = true
	f.stopCh = make(chan struct{})
	f.mu.Unlock()

	f.wg.Add(1)
	go f.loop(ctx)
}

// Stop signals the loop to flush one last time and exit. Safe to call
// multiple times; only the first call signals.
func (f *ByteFlusher) Stop() {
	f.mu.Lock()
	started := f.started
	if started {
		select {
		case <-f.stopCh:
			// already closed
		default:
			close(f.stopCh)
		}
		f.started = false
	}
	f.mu.Unlock()
	f.wg.Wait()
}

func (f *ByteFlusher) loop(ctx context.Context) {
	defer f.wg.Done()
	t := time.NewTicker(f.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			f.flush(ctx)
		case <-f.stopCh:
			// Final drain so graceful shutdown doesn't lose deltas.
			f.flush(context.Background())
			return
		case <-ctx.Done():
			f.flush(context.Background())
			return
		}
	}
}

func (f *ByteFlusher) flush(ctx context.Context) {
	deltas := f.accounter.Drain()
	if len(deltas) == 0 {
		return
	}
	for id, n := range deltas {
		if err := f.repo.IncrementUsedBytes(ctx, id, n); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			f.logger.Warn("byte flusher: increment failed; deltas dropped",
				"server_id", id, "delta", n, "err", err)
		}
	}
}
