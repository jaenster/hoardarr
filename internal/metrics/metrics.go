// Package metrics holds the Prometheus collectors for the hoardarr
// process. The HTTP /metrics endpoint binds to the Registry exposed
// here; instrumentation points across the codebase (NNTP pool, deliver
// service, outbox dispatcher, etc.) call the package-level helper
// functions which increment / observe via the registered collectors.
//
// Design choices:
//
//   - One global registry rather than constructor injection. Metrics
//     are process-wide observability, not domain state — passing a
//     *Registry through every adapter constructor is more ceremony
//     than payoff. The trade-off: tests that care about cardinality
//     have to read from the same global, but the test surface stays
//     identical to production.
//
//   - Label-set discipline: server name (low cardinality, bounded by
//     the operator), HTTP status family ("2xx"/"4xx"/"5xx") rather
//     than raw status, segment-fetch status from a small enum. No
//     unbounded label values (no msg_id, no job_id).
//
//   - Native Go runtime metrics are exposed via the default collectors
//     (process_*, go_*) so consumers can correlate hoardarr workload
//     against GC pressure / goroutine count without us bolting that
//     on by hand.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process-global Prometheus registry. Exposed so the
// HTTP /metrics handler can build its promhttp.Handler against it.
var Registry = prometheus.NewRegistry()

var (
	BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hoardarr_build_info",
		Help: "Build metadata of the running binary (always 1). Labels carry version + commit + go_version.",
	}, []string{"version", "commit", "go_version"})

	JobsByState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hoardarr_jobs",
		Help: "Number of jobs currently in each download lifecycle state.",
	}, []string{"state"})

	ArticlesFetched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoardarr_nntp_articles_fetched_total",
		Help: "NNTP article fetch attempts, by server and outcome (ok / missing / failed).",
	}, []string{"server", "outcome"})

	BytesDownloaded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoardarr_nntp_bytes_downloaded_total",
		Help: "Decoded yEnc payload bytes written, by server.",
	}, []string{"server"})

	SegmentFetchSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hoardarr_nntp_segment_fetch_seconds",
		Help:    "End-to-end segment fetch duration (NNTP dial + ARTICLE + yEnc decode + disk write).",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"server", "outcome"})

	NNTPConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hoardarr_nntp_connections",
		Help: "Live NNTP connection count per server, split by state (in_use, idle).",
	}, []string{"server", "state"})

	OutboxPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hoardarr_outbox_pending",
		Help: "Pending outbox rows per subscription (events not yet delivered).",
	}, []string{"subscription"})

	OutboxDispatched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoardarr_outbox_dispatched_total",
		Help: "Successfully-delivered outbox events per subscription.",
	}, []string{"subscription"})

	OutboxFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoardarr_outbox_dispatch_failed_total",
		Help: "Failed outbox deliveries per subscription (includes retries; final-park is logged separately).",
	}, []string{"subscription"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		BuildInfo,
		JobsByState,
		ArticlesFetched,
		BytesDownloaded,
		SegmentFetchSeconds,
		NNTPConnections,
		OutboxPending,
		OutboxDispatched,
		OutboxFailed,
	)
}

// SetBuildInfo writes the singular {version, commit, go_version} = 1
// row. Call once at startup after reading the ldflags-injected
// values. Subsequent calls with different labels add new rows; reset
// before re-setting if that matters (it shouldn't — version is fixed
// for a process lifetime).
func SetBuildInfo(version, commit, goVersion string) {
	BuildInfo.WithLabelValues(version, commit, goVersion).Set(1)
}
