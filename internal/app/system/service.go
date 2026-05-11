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
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/download"
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
	pools      map[domainserver.ServerID]*nntp.Pool
	servers    ServerStatRepo
	throughput *Throughput
	now        func() time.Time
}

// Params gathers Service dependencies.
type Params struct {
	Version    string
	StartedAt  time.Time
	Jobs       download.JobRepository
	Pools      map[domainserver.ServerID]*nntp.Pool
	Servers    ServerStatRepo
	Throughput *Throughput
	Now        func() time.Time
}

// New constructs a Service. StartedAt should be the App start time so
// uptime is process-relative, not Service-construction-relative.
func New(p Params) *Service {
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		version:    p.Version,
		startedAt:  p.StartedAt,
		jobs:       p.Jobs,
		pools:      p.Pools,
		servers:    p.Servers,
		throughput: p.Throughput,
		now:        p.Now,
	}
}

// Throughput exposes the rolling-window byte-rate tracker for the
// /api/v1/system/throughput endpoint. Returns nil if no tracker was
// supplied (system status still works, just no graph).
func (s *Service) Throughput() *Throughput { return s.throughput }

// Status samples a fresh snapshot. Repository errors propagate; pool
// snapshots can't fail (they read in-memory counters).
func (s *Service) Status(ctx context.Context) (Status, error) {
	now := s.now()

	active, err := s.jobs.Active(ctx)
	if err != nil {
		return Status{}, err
	}
	all, err := s.jobs.List(ctx)
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

	pools := make([]PoolStatus, 0, len(s.pools))
	for id, p := range s.pools {
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
		QueueActive: len(active),
		QueueTotal:  len(all),
		Pools:       pools,
	}, nil
}
