// Package system gathers process-wide health/status information for
// the /api/v1/system/status endpoint.
//
// The service is deliberately read-only and side-effect-free: each
// Status() call samples queue depth, pool occupancy, and uptime fresh
// from authoritative sources. There is no caching — the fan-out is
// small (one DB count + N pool snapshots) so polling at 1Hz from the
// UI is cheap.
package system

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// Status is the snapshot returned by Service.Status. Field naming is
// deliberately flat — the REST handler maps this 1:1 onto its DTO.
type Status struct {
	Service   string
	Version   string
	StartedAt time.Time
	Uptime    time.Duration

	QueueActive int
	QueueTotal  int

	Pools []PoolStatus
}

// PoolStatus is one Usenet server's pool snapshot.
type PoolStatus struct {
	ServerID    domainserver.ServerID
	ServerName  string
	Host        string
	Port        int
	MaxConns    int
	InUse       int
	Idle        int
	Enabled     bool
	Backup      bool
	BillingMode string
	QuotaBytes  int64
	UsedBytes   int64
}

// ServerStatRepo is the slice of the server repo system.Service needs
// to surface fresh per-server byte counters (the pool keeps a snapshot
// from startup; persistent used_bytes lives in the DB and is updated
// by the byte flusher).
type ServerStatRepo interface {
	List(ctx context.Context) ([]*domainserver.UsenetServer, error)
}

// Service composes Status snapshots from authoritative sources.
type Service struct {
	version    string
	startedAt  time.Time
	jobs       download.JobRepository
	// poolsSource returns a fresh pool snapshot on every call.
	// Reads from the orchestrator's live map so servers added at
	// runtime show up immediately on /api/v1/system/status — the
	// old behaviour (static snapshot at construction time) didn't.
	poolsSource func() map[domainserver.ServerID]*nntp.Pool
	servers     ServerStatRepo
	throughput  *Throughput
	now         func() time.Time

	// bus + ticker plumbing for SSE-push of throughput / pools.
	// Wired by Start(); nil-safe (status endpoints still work even
	// when the service isn't pushing).
	bus    event.Bus
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Params gathers Service dependencies.
type Params struct {
	Version   string
	StartedAt time.Time
	Jobs      download.JobRepository
	// PoolsSource is a callback that returns the live pool map. Wire
	// it to OrchestratorService.PoolsSnapshot so hot-wired servers
	// surface on the System page.
	PoolsSource func() map[domainserver.ServerID]*nntp.Pool
	Servers     ServerStatRepo
	Throughput  *Throughput
	// Bus, if supplied, receives periodic system.throughput and
	// system.pools envelopes once Start() is called. SSE clients pick
	// these up via the hub so the frontend doesn't need to poll
	// /api/v1/system/throughput + /system/status.
	Bus    event.Bus
	Logger *slog.Logger
	Now    func() time.Time
}

// New constructs a Service. StartedAt should be the App start time so
// uptime is process-relative, not Service-construction-relative.
func New(p Params) *Service {
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	if p.PoolsSource == nil {
		// Stable empty-snapshot fallback for tests that don't care
		// about pool stats.
		empty := map[domainserver.ServerID]*nntp.Pool{}
		p.PoolsSource = func() map[domainserver.ServerID]*nntp.Pool { return empty }
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	return &Service{
		version:     p.Version,
		startedAt:   p.StartedAt,
		jobs:        p.Jobs,
		poolsSource: p.PoolsSource,
		servers:     p.Servers,
		throughput:  p.Throughput,
		bus:         p.Bus,
		logger:      p.Logger,
		now:         p.Now,
	}
}

// Start kicks off the periodic throughput + pool emitters. Idempotent.
// No-op if the service has no bus configured.
func (s *Service) Start(_ context.Context) error {
	if s.bus == nil || s.cancel != nil {
		return nil
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(2)
	go s.throughputLoop(rootCtx)
	go s.poolsLoop(rootCtx)
	s.logger.Info("system service started", "topics", []string{"system.throughput", "system.pools"})
	return nil
}

// Stop signals the emitter loops to exit and waits for them. Safe to
// call before Start (no-op).
func (s *Service) Stop() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	s.cancel = nil
	s.wg.Wait()
	s.logger.Info("system service stopped")
	return nil
}

// throughputLoop publishes a system.throughput envelope every second
// (matches the previous polling cadence operators were used to).
// Snapshot is cheap — Throughput.Sample is in-memory.
func (s *Service) throughputLoop(ctx context.Context) {
	defer s.wg.Done()
	if s.throughput == nil {
		return
	}
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-t.C:
			snap := s.throughput.Sample()
			ev := throughputEvent{
				At:                 at.UTC(),
				CurrentBytesPerSec: snap.CurrentBytesPerSec,
				Avg10sBytesPerSec:  snap.Avg10sBytesPerSec,
				Avg60sBytesPerSec:  snap.Avg60sBytesPerSec,
				TotalBytes:         snap.Total,
				WindowSeconds:      WindowSize,
			}
			if err := s.bus.Publish(ctx, ev); err != nil {
				// Bus full or shutdown — best-effort; the next tick will
				// catch up. Logging once would spam, so we drop silently.
				_ = err
			}
		}
	}
}

// poolsLoop publishes a system.pools envelope every 5s. Slower cadence
// because pool stats change much less often than byte rates.
func (s *Service) poolsLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-t.C:
			ev := s.makePoolsEvent(at.UTC())
			_ = s.bus.Publish(ctx, ev)
		}
	}
}

// throughputEvent is published by throughputLoop. Topic constant lives
// in DefaultTopics on the SSE hub. Field naming matches the REST DTO
// so the frontend's existing types keep working — the SSE payload is
// drop-in for the polled response shape.
type throughputEvent struct {
	At                 time.Time `json:"at"`
	CurrentBytesPerSec int64     `json:"current_bytes_per_sec"`
	Avg10sBytesPerSec  int64     `json:"avg10s_bytes_per_sec"`
	Avg60sBytesPerSec  int64     `json:"avg60s_bytes_per_sec"`
	TotalBytes         int64     `json:"total_bytes"`
	WindowSeconds      int       `json:"window_seconds"`
}

func (throughputEvent) Topic() string         { return "system.throughput" }
func (throughputEvent) AggregateID() string   { return "system" }
func (e throughputEvent) OccurredAt() time.Time { return e.At }

// poolsEvent carries the current pool snapshot. Same shape as
// PoolStatus to keep frontend types unchanged.
type poolsEvent struct {
	At    time.Time    `json:"at"`
	Pools []PoolStatus `json:"pools"`
}

func (poolsEvent) Topic() string         { return "system.pools" }
func (poolsEvent) AggregateID() string   { return "system" }
func (e poolsEvent) OccurredAt() time.Time { return e.At }

// makePoolsEvent samples the live pools + augments with fresh server
// stats (same logic as Status() but without queue counts — those
// already flow via download.job.* events).
func (s *Service) makePoolsEvent(now time.Time) poolsEvent {
	freshByID := map[domainserver.ServerID]*domainserver.UsenetServer{}
	if s.servers != nil {
		if list, err := s.servers.List(context.Background()); err == nil {
			for _, sv := range list {
				freshByID[sv.ID()] = sv
			}
		}
	}
	live := s.poolsSource()
	pools := make([]PoolStatus, 0, len(live))
	for id, p := range live {
		st := p.Stats()
		srv := p.Server()
		if fresh, ok := freshByID[id]; ok {
			srv = fresh
		}
		pools = append(pools, PoolStatus{
			ServerID:    id,
			ServerName:  srv.Name(),
			Host:        srv.Host(),
			Port:        srv.Port(),
			MaxConns:    st.MaxConns,
			InUse:       st.InUse,
			Idle:        st.Idle,
			Enabled:     srv.Enabled(),
			Backup:      srv.Backup(),
			BillingMode: string(srv.BillingMode()),
			QuotaBytes:  srv.QuotaBytes(),
			UsedBytes:   srv.UsedBytes(),
		})
	}
	return poolsEvent{At: now, Pools: pools}
}

// Throughput exposes the rolling-window byte-rate tracker for the
// /api/v1/system/throughput endpoint. Returns nil if no tracker was
// supplied (system status still works, just no graph).
func (s *Service) Throughput() *Throughput { return s.throughput }

// Status samples a fresh snapshot. Repository errors propagate; pool
// snapshots can't fail (they read in-memory counters).
func (s *Service) Status(ctx context.Context) (Status, error) {
	now := s.now()

	// The /system/status response only exposes queue depth as counts —
	// don't materialise aggregates. Previously this loaded every job
	// + every file + every segment per call; pprof on the live
	// container had it eating ~38% of CPU all by itself under the
	// frontend's 2 s polling.
	activeCount, err := s.jobs.CountActive(ctx)
	if err != nil {
		return Status{}, err
	}
	totalCount, err := s.jobs.CountAll(ctx)
	if err != nil {
		return Status{}, err
	}

	// Refresh used_bytes / quota from DB. The pool's cached server
	// aggregate is frozen at construction time; the authoritative
	// runtime counter lives in the DB (flushed periodically by the
	// byte flusher). Falls back to the cached snapshot on repo error.
	freshByID := map[domainserver.ServerID]*domainserver.UsenetServer{}
	if s.servers != nil {
		if list, err := s.servers.List(ctx); err == nil {
			for _, sv := range list {
				freshByID[sv.ID()] = sv
			}
		}
	}

	live := s.poolsSource()
	pools := make([]PoolStatus, 0, len(live))
	for id, p := range live {
		st := p.Stats()
		srv := p.Server()
		if fresh, ok := freshByID[id]; ok {
			srv = fresh
		}
		pools = append(pools, PoolStatus{
			ServerID:    id,
			ServerName:  srv.Name(),
			Host:        srv.Host(),
			Port:        srv.Port(),
			MaxConns:    st.MaxConns,
			InUse:       st.InUse,
			Idle:        st.Idle,
			Enabled:     srv.Enabled(),
			Backup:      srv.Backup(),
			BillingMode: string(srv.BillingMode()),
			QuotaBytes:  srv.QuotaBytes(),
			UsedBytes:   srv.UsedBytes(),
		})
	}

	return Status{
		Service:     "hoardarr",
		Version:     s.version,
		StartedAt:   s.startedAt,
		Uptime:      now.Sub(s.startedAt),
		QueueActive: activeCount,
		QueueTotal:  totalCount,
		Pools:       pools,
	}, nil
}
