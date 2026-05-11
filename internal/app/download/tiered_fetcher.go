package download

// TieredFetcher implements download.ArticleFetcher over multiple NNTP
// pools with priority-tier failover, intra-tier flat-vs-metered
// preference, backup-tier semantics, and per-server byte accounting.
//
// Failover policy (matches what SABnzbd does in practice):
//
//   1. Group enabled, quota-available pools by priority. Within a
//      tier, flat-billed pools come before metered ones.
//   2. Append a final tier containing every server with `backup=true`
//      (so backups always go last regardless of stated priority).
//   3. Try each pool in order. On 430 → try next. On any other error
//      → return up (segment-retry budget engages); we do NOT keep
//      trying because the conn might be unhealthy across the whole
//      tier-walk we'd otherwise do.
//   4. If every pool said 430, return nntp.ErrArticleMissing.
//
// The ServerID hint on the Fetch port is ignored — TieredFetcher
// decides which server to talk to.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/server"
)

// TieredFetcher is the multi-pool ArticleFetcher.
type TieredFetcher struct {
	// poolSource returns a fresh snapshot of the current pool set on
	// every Fetch. Lets the orchestrator add/remove pools live (when
	// the operator wires up a new server in the UI) without making
	// the fetcher race on map reads.
	poolSource func() map[server.ServerID]*nntp.Pool
	accounter  *ByteAccounter
	limiter    *Limiter // optional; nil disables bandwidth throttling
	logger     *slog.Logger
}

// Compile-time check.
var _ download.ArticleFetcher = (*TieredFetcher)(nil)

// NewTieredFetcher wraps a live pool source. The accounter may be
// nil; supplied, it receives per-server byte deltas as fetched bodies
// are drained by the caller. The limiter may also be nil; supplied,
// it throttles per-read.
func NewTieredFetcher(poolSource func() map[server.ServerID]*nntp.Pool, accounter *ByteAccounter, limiter *Limiter, logger *slog.Logger) *TieredFetcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &TieredFetcher{poolSource: poolSource, accounter: accounter, limiter: limiter, logger: logger}
}

// Fetch iterates the tiered pool order and returns the first successful
// body. Ignores `srv` — the hint exists for API compatibility with
// single-pool fetchers and isn't authoritative here.
func (f *TieredFetcher) Fetch(ctx context.Context, _ server.ServerID, messageID string) (io.ReadCloser, error) {
	candidates := f.tieredOrder()
	if len(candidates) == 0 {
		return nil, errors.New("tiered: no enabled pools")
	}

	var sawMissing bool
	for _, p := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		started := time.Now()
		body, err := tryOnePool(ctx, p, messageID)
		if err == nil {
			f.logger.Debug("fetched article",
				"msg_id", messageID,
				"server", p.Server().Name(),
				"server_id", int64(p.Server().ID()),
				"acquire_ms", time.Since(started).Milliseconds(),
			)
			// Wrap so used_bytes accounting fires when the caller drains
			// and bandwidth limiter throttles per-Read.
			return f.wrapForAccounting(ctx, body, p.Server().ID()), nil
		}
		if errors.Is(err, nntp.ErrArticleMissing) {
			f.logger.Debug("article missing on server",
				"msg_id", messageID,
				"server", p.Server().Name(),
				"server_id", int64(p.Server().ID()),
			)
			sawMissing = true
			continue
		}
		f.logger.Warn("article fetch failed",
			"msg_id", messageID,
			"server", p.Server().Name(),
			"server_id", int64(p.Server().ID()),
			"err", err,
		)
		// Transient error: surface up. The segment-retry budget in the
		// orchestrator will re-call Fetch later and we'll try again from
		// the top of the tier list. If transient errors are concentrated
		// on the top tier, that's a sign of provider trouble — better to
		// expose it than mask it by silently always failing over.
		return nil, fmt.Errorf("server %q: %w", p.Server().Name(), err)
	}
	if sawMissing {
		return nil, nntp.ErrArticleMissing
	}
	return nil, errors.New("tiered: every pool ineligible (disabled or quota-exhausted)")
}

// tieredOrder returns the pool list in dispatch order. Filters out
// disabled servers and quota-exhausted metered ones.
func (f *TieredFetcher) tieredOrder() []*nntp.Pool {
	type entry struct {
		p          *nntp.Pool
		priority   int
		backup     bool
		flatFirst  int // 0 for flat, 1 for metered — sort tiebreaker
		id         server.ServerID
	}
	var list []entry
	for id, p := range f.poolSource() {
		srv := p.Server()
		if !srv.Enabled() {
			continue
		}
		if srv.QuotaExhausted() {
			continue
		}
		mode := 0
		if srv.BillingMode() == server.BillingMetered {
			mode = 1
		}
		list = append(list, entry{
			p: p, priority: srv.Priority(), backup: srv.Backup(),
			flatFirst: mode, id: id,
		})
	}
	sort.Slice(list, func(i, j int) bool {
		// Backups always last.
		if list[i].backup != list[j].backup {
			return !list[i].backup
		}
		// Priority asc.
		if list[i].priority != list[j].priority {
			return list[i].priority < list[j].priority
		}
		// Within a tier: flat before metered.
		if list[i].flatFirst != list[j].flatFirst {
			return list[i].flatFirst < list[j].flatFirst
		}
		// Stable tie-break by id.
		return list[i].id < list[j].id
	})
	out := make([]*nntp.Pool, len(list))
	for i, e := range list {
		out[i] = e.p
	}
	return out
}

// tryOnePool acquires a conn, runs BODY, returns body or err.
//
// On 430 the conn is healthy; release without close. On any other
// error the conn is potentially desynced; flag for close.
func tryOnePool(ctx context.Context, p *nntp.Pool, messageID string) (io.ReadCloser, error) {
	conn, release, err := p.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	body, err := conn.Body(ctx, messageID)
	if err != nil {
		if errors.Is(err, nntp.ErrArticleMissing) {
			release(nil)
		} else {
			release(err)
		}
		return nil, err
	}
	return &releasingReader{ReadCloser: body, release: release}, nil
}

// wrapForAccounting wraps body so that:
//   - Each Read is gated by the bandwidth limiter (if configured).
//   - Total drained bytes are reported to the accounter on Close.
//
// Bytes read but never Close()d (caller leak) won't be counted —
// acceptable because the orchestrator always closes via defer.
func (f *TieredFetcher) wrapForAccounting(ctx context.Context, body io.ReadCloser, srvID server.ServerID) io.ReadCloser {
	wrap := &countingReader{
		ReadCloser: body,
		srvID:      srvID,
	}
	if f.accounter != nil {
		wrap.on = func(n int64) { f.accounter.Add(srvID, n) }
	}
	if f.limiter != nil {
		wrap.limiter = f.limiter
		wrap.ctx = ctx
	}
	return wrap
}

// countingReader tallies bytes drained from body, throttles Reads
// via the optional limiter, and reports the total to `on` exactly
// once when Close is called.
type countingReader struct {
	io.ReadCloser
	n       int64
	closed  bool
	on      func(int64)
	limiter *Limiter
	srvID   server.ServerID
	ctx     context.Context
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.n += int64(n)
	if n > 0 && r.limiter != nil {
		// Post-read throttle: we wait AFTER reading because rate.Limiter
		// is a token bucket and the natural unit is "you got n bytes,
		// now wait for the bucket to refill those tokens." Doing this
		// pre-read would require knowing how much we're about to read
		// (Read returns a variable amount).
		if waitErr := r.limiter.Wait(r.ctx, r.srvID, n); waitErr != nil {
			// ctx-cancel mid-throttle: don't mask the actual read but
			// surface the cancel as the returned error so the caller
			// stops.
			return n, waitErr
		}
	}
	return n, err
}

func (r *countingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.ReadCloser.Close()
	if r.on != nil {
		r.on(r.n)
	}
	return err
}
