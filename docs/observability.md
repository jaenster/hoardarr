# Observability

hoardarr exposes a Prometheus scrape endpoint at `/metrics`, behind the
same API-key auth as the rest of the surface.

## Scrape configuration

```yaml
scrape_configs:
  - job_name: hoardarr
    scheme: http   # https if you front it with a reverse proxy
    metrics_path: /metrics
    static_configs:
      - targets: ["hoardarr:8085"]
    authorization:
      type: Bearer
      credentials: <your-api-key>
    # Alternative if your scraper doesn't speak bearer-auth on /metrics:
    # params:
    #   apikey: ["<your-api-key>"]
```

The endpoint speaks both classic Prometheus text and OpenMetrics (the
content-negotiated default for Prometheus 2.x+).

## Exposed metrics

Process / Go runtime metrics come from `prometheus/client_golang`'s
default collectors — useful for correlating workload against GC pressure
and goroutine count. The hoardarr-specific gauges and counters:

| Metric | Type | Labels | Notes |
|-|-|-|-|
| `hoardarr_build_info` | gauge | `version`, `commit`, `go_version` | Always 1. Identifies the running binary. |
| `hoardarr_jobs` | gauge | `state` | Currently emits `active` and `total`. Per-state breakdown is a follow-up. |
| `hoardarr_nntp_connections` | gauge | `server`, `state` | `state` ∈ {`in_use`, `idle`}. |
| `hoardarr_nntp_articles_fetched_total` | counter | `server`, `outcome` | Wired progressively — not all fetch sites instrument yet. |
| `hoardarr_nntp_bytes_downloaded_total` | counter | `server` | Decoded yEnc payload bytes. |
| `hoardarr_nntp_segment_fetch_seconds` | histogram | `server`, `outcome` | End-to-end segment fetch latency. |
| `hoardarr_outbox_pending` | gauge | `subscription` | Undelivered event count per subscriber. |
| `hoardarr_outbox_dispatched_total` | counter | `subscription` | Successful event deliveries. |
| `hoardarr_outbox_dispatch_failed_total` | counter | `subscription` | Failed deliveries (includes retries). |

## Suggested alerts

These are starting points — tune the thresholds to your release rate
and provider quality.

```yaml
groups:
  - name: hoardarr
    rules:
      - alert: HoardarrDown
        expr: up{job="hoardarr"} == 0
        for: 2m
        annotations:
          summary: hoardarr scrape failing for >2m

      - alert: HoardarrOutboxBacklog
        expr: hoardarr_outbox_pending > 100
        for: 10m
        annotations:
          summary: |
            Outbox subscription {{ $labels.subscription }} has been
            backlogged >100 events for 10m — a subscriber is stuck.

      - alert: HoardarrNoIdleConns
        # Every configured server has zero idle conns for 5m — the pool
        # is saturated or all connections are dropping.
        expr: sum by (server) (hoardarr_nntp_connections{state="idle"}) == 0
        for: 5m
        annotations:
          summary: |
            No idle NNTP conns to {{ $labels.server }} for 5m.
```

## Why behind auth?

`/metrics` is conventionally unauthenticated for scrapers on a private
network. hoardarr's homelab audience often runs the binary on a host
reachable from the public internet (Synology, Unraid, exposed via a
reverse proxy). Defaulting to API-key auth on `/metrics` removes the
"oops, I just published my internal telemetry" footgun. If you run on
a fully-private network and the auth ceremony bothers you, drop a
Caddy / nginx route in front that strips the auth requirement only for
your scraper's source IP.
