package download

// Bandwidth limiter: throttles total download throughput per-server and
// across-all-servers. The fetcher calls Wait(ctx, srvID, n) before
// handing n bytes to the orchestrator; if either the per-server or
// global cap is set and the request would exceed it, Wait blocks
// until tokens are available.
//
// Backed by golang.org/x/time/rate.Limiter — well-tested token-bucket
// with concurrent-safe semantics and dynamic SetLimit support so
// runtime cap changes take effect on the next Read.

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// Limiter combines a global token bucket with per-server buckets.
// Wait satisfies both gates in order; whichever has fewer tokens
// available dictates the throughput.
type Limiter struct {
	mu            sync.RWMutex
	global        *rate.Limiter
	perServer     map[server.ServerID]*rate.Limiter
	perServerCaps map[server.ServerID]int64 // cached caps so SetServerCap can detect no-op
	globalCap     int64
}

// NewLimiter constructs a Limiter with an optional global cap. A cap
// of 0 means "no global cap"; per-server caps are still honoured.
func NewLimiter(globalBytesPerSec int64) *Limiter {
	l := &Limiter{
		perServer:     make(map[server.ServerID]*rate.Limiter),
		perServerCaps: make(map[server.ServerID]int64),
	}
	l.SetGlobalCap(globalBytesPerSec)
	return l
}

// GlobalCap returns the current global cap (0 means no cap).
func (l *Limiter) GlobalCap() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.globalCap
}

// SetGlobalCap updates the global cap. 0 means "no global cap".
// Safe to call from any goroutine; in-flight Waits continue under
// the new limit on their next refill.
func (l *Limiter) SetGlobalCap(bytesPerSec int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.globalCap = bytesPerSec
	if bytesPerSec <= 0 {
		l.global = nil
		return
	}
	burst := int(bytesPerSec)
	if burst < 64*1024 {
		burst = 64 * 1024
	}
	if l.global == nil {
		l.global = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
	} else {
		l.global.SetLimit(rate.Limit(bytesPerSec))
		l.global.SetBurst(burst)
	}
}

// SetServerCap updates one server's cap. 0 means "no per-server cap"
// (the limiter is dropped from the map).
func (l *Limiter) SetServerCap(srv server.ServerID, bytesPerSec int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if bytesPerSec <= 0 {
		delete(l.perServer, srv)
		delete(l.perServerCaps, srv)
		return
	}
	if l.perServerCaps[srv] == bytesPerSec && l.perServer[srv] != nil {
		return
	}
	burst := int(bytesPerSec)
	if burst < 64*1024 {
		burst = 64 * 1024
	}
	if cur, ok := l.perServer[srv]; ok {
		cur.SetLimit(rate.Limit(bytesPerSec))
		cur.SetBurst(burst)
	} else {
		l.perServer[srv] = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
	}
	l.perServerCaps[srv] = bytesPerSec
}

// Wait blocks until n tokens are available on both the global bucket
// (if set) and the per-server bucket (if set for srv). If neither is
// configured Wait returns immediately. Returns ctx.Err() on cancel.
//
// The two buckets are filled sequentially: global first, then per-
// server. The total delay is at most max(globalDelay, perServerDelay),
// not sum, because both buckets accumulate concurrently while the
// fetcher does its work — the next Read picks up whatever's accrued.
func (l *Limiter) Wait(ctx context.Context, srv server.ServerID, n int) error {
	if n <= 0 {
		return nil
	}
	l.mu.RLock()
	global := l.global
	per := l.perServer[srv]
	l.mu.RUnlock()

	if global == nil && per == nil {
		return nil
	}
	// rate.Limiter rejects WaitN if n > burst. We cap n to each
	// bucket's burst to keep things sane; over-budget reads complete
	// after multiple WaitN calls (driven by the reader loop above).
	if global != nil {
		if err := waitChunked(ctx, global, n); err != nil {
			return err
		}
	}
	if per != nil {
		if err := waitChunked(ctx, per, n); err != nil {
			return err
		}
	}
	return nil
}

// waitChunked invokes l.WaitN repeatedly so the total approved volume
// equals n even when n exceeds the bucket's burst. The bucket's burst
// is at least 64 KiB by construction, so for typical NNTP reads (8-64
// KiB chunks) this is a single call.
func waitChunked(ctx context.Context, l *rate.Limiter, n int) error {
	burst := l.Burst()
	if burst <= 0 {
		burst = 1
	}
	for n > 0 {
		take := n
		if take > burst {
			take = burst
		}
		if err := l.WaitN(ctx, take); err != nil {
			return err
		}
		n -= take
	}
	return nil
}
