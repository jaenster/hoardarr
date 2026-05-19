# hoardarr REST API

hoardarr exposes two HTTP surfaces:

- **`/api/v1/*`** — hoardarr's native REST API. JSON in/out, session-cookie or `X-Api-Key` auth.
- **`/sabnzbd/api`** — SABnzbd v3 compatibility shim consumed by Sonarr / Radarr / Lidarr / Readarr / Prowlarr.

This document covers `/api/v1/*`. For the SAB-compat surface see
[`docs/sab-api.md`](sab-api.md) (or just point a *arr at it; the API
key from `Settings → Authentication` is all you need).

> **URL base.** Every path below is relative to the configured `URL_BASE`.
> A default install serves at `/api/v1/...`; a reverse-proxy install
> with `HOARDARR_URL_BASE=/hoardarr` serves at `/hoardarr/api/v1/...`.

## Authentication

Two equally-valid auth methods:

| Method | Used by | How |
|-|-|-|
| Session cookie | Browser (web UI) | `POST /api/v1/auth/login`, cookie set HTTP-only |
| API key | Headless clients (*arr, scripts, CI) | `X-Api-Key: <key>` header, or `?apikey=<key>` query param |

The API key is generated on first run and rotatable at
`Settings → Authentication`. Get it from
`GET /api/v1/config/general` (after login) or the UI.

CSRF protection is enforced on mutating endpoints for session-auth
callers; API-key callers bypass CSRF (the key itself is the
unforgeable token).

## Conventions

- All responses are JSON unless explicitly noted (downloads, SSE).
- Errors return `{"error": "<message>"}` with a 4xx/5xx status.
- Timestamps are RFC3339 in UTC.
- Byte counts are integers.
- IDs are 64-bit signed integers serialized as JSON numbers.

---

## Auth

```
POST   /api/v1/auth/setup            # create the first admin (only when no users exist)
POST   /api/v1/auth/login            # username + password → session cookie
POST   /api/v1/auth/logout           # invalidate session
POST   /api/v1/auth/change-password  # old + new password
POST   /api/v1/auth/rotate-api-key   # generate a fresh API key
GET    /api/v1/auth/whoami           # current session state
```

## Queue (downloads)

```
GET    /api/v1/queue              # list active jobs (shallow — files not hydrated)
GET    /api/v1/queue/{id}         # full job + files + segments
GET    /api/v1/queue/{id}/events  # outbox event history for one job
POST   /api/v1/queue/nzb          # multipart upload, field "nzb"; returns {job_id, duplicate?, state?, name?}
POST   /api/v1/queue/{id}/pause   # pause a job
POST   /api/v1/queue/{id}/resume  # resume a paused job
DELETE /api/v1/queue/{id}         # abort + remove
POST   /api/v1/queue/reorder      # body: {ids: [int, ...]} — reorder by priority
```

## History

```
GET    /api/v1/history            # ?since=RFC3339 &category= &state= &limit=
```

State filter: `completed|failed|aborted`.

## Servers

```
GET    /api/v1/servers            # list configured usenet servers
POST   /api/v1/servers            # add (body: name, host, port, tls, user, pass, max_conns, priority, ...)
PATCH  /api/v1/servers/{id}       # partial update
DELETE /api/v1/servers/{id}
POST   /api/v1/servers/{id}/enable
POST   /api/v1/servers/{id}/disable
POST   /api/v1/servers/test       # probe before save: returns {dial, mode_reader, date} step results
POST   /api/v1/servers/{id}/test  # probe an existing server
```

## Categories

```
GET    /api/v1/categories
POST   /api/v1/categories         # body: {name, dir?, priority?, script?}
DELETE /api/v1/categories/{name}
```

## Config

```
GET    /api/v1/config/paths       # data dir, incomplete, complete
GET    /api/v1/config/general     # listen, log_level, URL base, API key, sample-removal, etc.
PUT    /api/v1/config/general     # live-mutable settings
GET    /api/v1/config/bandwidth   # global + per-server byte-rate caps
PUT    /api/v1/config/bandwidth
```

## Webhooks / notifications

```
GET    /api/v1/subscriptions
POST   /api/v1/subscriptions          # add (kind: webhook|discord|slack|pushover)
PATCH  /api/v1/subscriptions/{id}
DELETE /api/v1/subscriptions/{id}
POST   /api/v1/subscriptions/{id}/enable
POST   /api/v1/subscriptions/{id}/disable
POST   /api/v1/subscriptions/{id}/test  # send a synthetic event
```

## System

```
GET    /api/v1/system/status         # version, commit, runtime, queue counts, pools
GET    /api/v1/system/throughput     # current + rolling-average byte rates
GET    /api/v1/system/diskspace      # per-configured-path free/total/used
GET    /api/v1/system/health         # current health issues (sorted errors-first)
POST   /api/v1/system/health/refresh # re-run all checks, return next snapshot
GET    /api/v1/system/tasks          # scheduled tasks (recurring + oneshot)
POST   /api/v1/system/tasks/{id}/run-now
GET    /api/v1/system/backups        # SQLite VACUUM INTO snapshots
POST   /api/v1/system/backups        # run a backup now
GET    /api/v1/system/backups/{name} # download a backup file
GET    /api/v1/system/logs           # in-memory ring buffer snapshot
GET    /api/v1/system/logs/tail      # SSE live tail
GET    /api/v1/system/logs/stream    # alias of /tail for back-compat
GET    /api/v1/system/logs/files     # list rotating log files on disk
GET    /api/v1/system/logs/files/{name}  # download a log file
```

## Commands (async one-off operations)

```
GET    /api/v1/commands           # list recent commands (?limit=N)
GET    /api/v1/commands/{id}      # one command's state
GET    /api/v1/commands/names     # registered handler names (UI dropdown source)
POST   /api/v1/commands           # body: {name, body?} — queue for the worker
```

Built-in handlers: `Ping` (smoke-test), `HealthRecheck`. More handlers
are added as the application service grows (`RetryFailedSegments`,
`RescanComplete`, etc. — see `internal/app/command/handlers.go`).

## Live events (SSE)

```
GET    /api/v1/events             # full live bus (domain events + system.throughput + system.pools)
```

The SSE stream is what the UI uses to tick progress bars, update queue
counts, and surface fresh health/disk/throughput data without polling.

Event envelope shape:

```json
{
  "ID": "01HXYZ...",
  "Topic": "download.segment.completed",
  "AggregateID": "147",
  "OccurredAt": "2026-05-17T16:30:00Z",
  "Payload": { /* topic-specific */ }
}
```

## Topic glossary (subset)

| Topic | Emitted when |
|-|-|
| `download.JobCreated` | NZB upload accepted |
| `download.SegmentCompleted` | One article fetched + written |
| `download.JobDownloadComplete` | All segments resolved (or terminal-missing) |
| `verify.VerifyOK` / `verify.RepairNeeded` | PAR2 verify result |
| `repair.RepairOK` / `repair.RepairFailed` | Reed-Solomon reconstruction result |
| `extract.ExtractOK` | RAR extraction done |
| `deliver.DeliveryComplete` | Files moved to `complete/<category>/<release>/` |
| `system.throughput` | Periodic byte-rate snapshot |
| `system.pools` | Periodic NNTP pool stats |

Full list: `internal/api/sse/hub.go` defines `DefaultTopics`.

## SABnzbd compat

`/sabnzbd/api?mode=...&apikey=...` mirrors SAB v3. Supported modes today:

| Mode | Purpose |
|-|-|
| `version` | Always reports `3.x.x+` so consumers don't bail |
| `get_config` | `misc.complete_dir`, categories, paths |
| `get_cats` | Categories incl. literal `*` default |
| `addfile` | Multipart NZB upload |
| `addurl` | Fetch NZB from URL (Sonarr's "Send URL" button) |
| `queue` | List + sub-actions (pause, resume, delete, change_priority) |
| `history` | List + delete + mark_as_completed |
| `pause` / `resume` | Global queue state |
| `set_speedlimit` | Bandwidth cap |
| `eval_sort` | Path template preview |
| `get_files` | Per-job file list |

See [`docs/coming-from-sabnzbd.md`](coming-from-sabnzbd.md) for the
*arr-side download-client config that drives this.

## Versioning

`/api/v1/*` is the stable surface. Breaking changes will only happen
at a major version bump (`/api/v2/...`); additive changes go straight
into v1. See [CHANGELOG.md](../CHANGELOG.md) for what shipped when.
