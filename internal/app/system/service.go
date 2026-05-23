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
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
	"github.com/jaenster/hoardarr/internal/metrics"
)

// Status is the snapshot returned by Service.Status. Field naming is
// deliberately flat — the REST handler maps this 1:1 onto its DTO.
type Status struct {
	Service   string
	Version   string
	Commit    string
	BuildDate string

	// Runtime / environment.
	RuntimeVersion   string // Go version (runtime.Version())
	OS               string // runtime.GOOS
	Arch             string // runtime.GOARCH
	IsDocker         bool
	DatabaseType     string // "sqlite" today; pluggable in future
	MigrationVersion int    // highest applied migration id

	StartedAt time.Time
	Uptime    time.Duration

	QueueActive int
	QueueTotal  int

	Pools []PoolStatus
}

// PoolStatus is one Usenet server's pool snapshot.
type PoolStatus struct {
	ServerID    domainserver.ServerID `json:"server_id"`
	ServerName  string                `json:"server_name"`
	Host        string                `json:"host"`
	Port        int                   `json:"port"`
	MaxConns    int                   `json:"max_conns"`
	InUse       int                   `json:"in_use"`
	Idle        int                   `json:"idle"`
	Enabled     bool                  `json:"enabled"`
	Backup      bool                  `json:"backup"`
	BillingMode string                `json:"billing_mode"`
	QuotaBytes  int64                 `json:"quota_bytes"`
	UsedBytes   int64                 `json:"used_bytes"`
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
	version          string
	commit           string
	buildDate        string
	migrationVersion int
	startedAt        time.Time
	jobs             download.JobRepository
	// poolsSource returns a fresh pool snapshot on every call.
	// Reads from the orchestrator's live map so servers added at
	// runtime show up immediately on /api/v1/system/status — the
	// old behaviour (static snapshot at construction time) didn't.
	poolsSource func() map[domainserver.ServerID]*nntp.Pool
	servers     ServerStatRepo
	throughput  *Throughput
	now         func() time.Time

	// history is optional long-term throughput persistence. nil-safe;
	// when nil the historyLoop is skipped and /speed-history serves
	// only what's in the in-memory ring.
	history       SpeedHistoryStore
	peakSave      func(v int64) (int64, error)
	retentionDays int

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
	Version          string
	Commit           string
	BuildDate        string
	MigrationVersion int
	StartedAt        time.Time
	Jobs             download.JobRepository
	// PoolsSource is a callback that returns the live pool map. Wire
	// it to OrchestratorService.PoolsSnapshot so hot-wired servers
	// surface on the System page.
	PoolsSource func() map[domainserver.ServerID]*nntp.Pool
	Servers     ServerStatRepo
	Throughput  *Throughput
	// History persists a downsampled (1 row per minute) throughput
	// record for /speed-history queries beyond the in-memory ring.
	// Optional — nil disables persistence (the long-range slice of
	// the API then returns only the in-memory window).
	History SpeedHistoryStore
	// PeakSave persists the all-time peak when a new high-water mark
	// is observed. Optional. Receives bytes/sec; should be bumps-only.
	PeakSave func(v int64) (int64, error)
	// RetentionDays caps how far back History samples survive. 0 ⇒
	// keep 30 days (default).
	RetentionDays int
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
	retention := p.RetentionDays
	if retention <= 0 {
		retention = 30
	}
	return &Service{
		version:          p.Version,
		commit:           p.Commit,
		buildDate:        p.BuildDate,
		migrationVersion: p.MigrationVersion,
		startedAt:        p.StartedAt,
		jobs:             p.Jobs,
		poolsSource:      p.PoolsSource,
		servers:          p.Servers,
		throughput:       p.Throughput,
		history:          p.History,
		peakSave:         p.PeakSave,
		retentionDays:    retention,
		bus:              p.Bus,
		logger:           p.Logger,
		now:              p.Now,
	}
}

// History exposes the persistent speed-history store. Returns nil if
// no store was supplied (the speed-history endpoint then serves only
// the in-memory ring).
func (s *Service) History() SpeedHistoryStore { return s.history }

// Start kicks off the periodic background loops: SSE throughput +
// pool emitters (when a bus is wired) and the history flusher /
// peak persister (when a history store is wired). Idempotent.
func (s *Service) Start(_ context.Context) error {
	if s.cancel != nil {
		return nil
	}
	if s.bus == nil && s.history == nil && s.peakSave == nil {
		return nil
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if s.bus != nil {
		s.wg.Add(2)
		go s.throughputLoop(rootCtx)
		go s.poolsLoop(rootCtx)
	}
	if s.history != nil || s.peakSave != nil {
		s.wg.Add(1)
		go s.historyLoop(rootCtx)
	}
	s.logger.Info("system service started",
		"has_bus", s.bus != nil,
		"has_history", s.history != nil,
		"retention_days", s.retentionDays,
	)
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
				WindowSeconds:      snap.WindowSeconds,
			}
			if err := s.bus.Publish(ctx, ev); err != nil {
				// Bus full or shutdown — best-effort; the next tick will
				// catch up. Logging once would spam, so we drop silently.
				_ = err
			}
		}
	}
}

// historyLoop persists one downsampled throughput sample per minute
// and periodically prunes rows older than the retention window. Same
// goroutine also writes the all-time peak whenever it bumps so a
// fresh start picks up the historical maximum.
//
// Aligns its first tick to the next wall-clock minute boundary so
// stored bucket_at values are predictable (helps debugging and means
// the API can stitch in-memory + DB samples without overlap-checks).
func (s *Service) historyLoop(ctx context.Context) {
	defer s.wg.Done()

	// Align to the next minute.
	now := s.now()
	delay := time.Duration(60-now.Second())*time.Second - time.Duration(now.Nanosecond())
	if delay < time.Second {
		delay += time.Minute
	}
	first := time.NewTimer(delay)
	defer first.Stop()
	select {
	case <-ctx.Done():
		return
	case <-first.C:
	}

	// Track the last persisted peak so we don't hammer the settings
	// table writing the same value every minute.
	var lastPeak int64
	if s.throughput != nil {
		lastPeak = s.throughput.AllTimePeak()
	}

	// Purge once at startup so retention is enforced even on long-
	// idling instances.
	s.purgeHistory(ctx)

	flush := time.NewTicker(time.Minute)
	defer flush.Stop()
	purge := time.NewTicker(time.Hour)
	defer purge.Stop()

	for {
		s.flushOneMinute(ctx)
		if s.throughput != nil && s.peakSave != nil {
			cur := s.throughput.AllTimePeak()
			if cur > lastPeak {
				if _, err := s.peakSave(cur); err != nil {
					s.logger.Warn("persist all-time peak", "err", err)
				} else {
					lastPeak = cur
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-purge.C:
			s.purgeHistory(ctx)
		case <-flush.C:
		}
	}
}

// flushOneMinute writes one history row holding the Avg60s value at
// the previous minute boundary. No-op when history is unset.
func (s *Service) flushOneMinute(ctx context.Context) {
	if s.history == nil || s.throughput == nil {
		return
	}
	// Bucket = the start of the previous wall-clock minute. We've
	// just crossed into a new minute, so Avg60s captures the one we
	// just finished.
	bucket := s.now().UTC().Truncate(time.Minute).Add(-time.Minute)
	snap := s.throughput.Sample()
	if err := s.history.Append(ctx, SpeedSample{At: bucket, BytesPerSec: snap.Avg60sBytesPerSec}); err != nil {
		s.logger.Warn("append speed history", "err", err)
	}
}

func (s *Service) purgeHistory(ctx context.Context) {
	if s.history == nil {
		return
	}
	cutoff := s.now().UTC().Add(-time.Duration(s.retentionDays) * 24 * time.Hour)
	if n, err := s.history.Purge(ctx, cutoff); err != nil {
		s.logger.Warn("purge speed history", "err", err)
	} else if n > 0 {
		s.logger.Info("speed history purged", "rows", n, "before", cutoff)
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

	// Cheap, on-poll metrics update. Status() runs on every
	// /api/v1/system/status hit AND on the periodic SSE poll, so the
	// gauges stay reasonably fresh without a dedicated metric ticker.
	metrics.JobsByState.WithLabelValues("active").Set(float64(activeCount))
	metrics.JobsByState.WithLabelValues("total").Set(float64(totalCount))
	for _, p := range pools {
		metrics.NNTPConnections.WithLabelValues(p.ServerName, "in_use").Set(float64(p.InUse))
		metrics.NNTPConnections.WithLabelValues(p.ServerName, "idle").Set(float64(p.Idle))
	}

	return Status{
		Service:          "hoardarr",
		Version:          s.version,
		Commit:           s.commit,
		BuildDate:        s.buildDate,
		RuntimeVersion:   runtime.Version(),
		OS:               runtime.GOOS,
		Arch:             runtime.GOARCH,
		IsDocker:         detectDocker(),
		DatabaseType:     "sqlite",
		MigrationVersion: s.migrationVersion,
		StartedAt:        s.startedAt,
		Uptime:           now.Sub(s.startedAt),
		QueueActive:      activeCount,
		QueueTotal:       totalCount,
		Pools:            pools,
	}, nil
}

// detectDocker uses the presence of /.dockerenv as the cheap signal.
// Not bulletproof (Podman-without-it slips through; rootless skips
// the marker on some setups) but matches Sonarr/Radarr's own check.
func detectDocker() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}
