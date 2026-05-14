# Architecture

The "how it's made" side of hoardarr, for people reading the source.
For "how to install + run", see [README.md](../README.md).

## Layout

```
internal/
  domain/           # aggregates + ports, no framework deps
  app/              # use cases — wire ports to domain logic
  adapter/          # nntp / yenc / nzb / par2 / sqlite / rar / fs
  api/{rest,sab,sse}
  bootstrap/        # composition root
  metrics/          # Prometheus registry + collectors
frontend/           # React + TS + Vite, embedded via go:embed
cmd/                # hoardarr (main) + testserver-nntpd (e2e)
docker/             # entrypoint shim
docs/               # operator + contributor docs
```

The domain layer has zero dependencies on adapters or frameworks.
`go test ./internal/domain/...` runs without a database, an HTTP
server, or a network. Adapters depend on domain (to implement its
ports), never the other way. `bootstrap/` is the only place that
knows which adapter wires to which port — also the only place tests
swap in stubs.

## Bounded contexts

Each context owns its aggregates, ports, events, and use cases.
Contexts are coupled only through domain events on a transactional
outbox bus.

| Context | Aggregate | Responsibility |
|-|-|-|
| `download` | `Job` → `File` → `Segment` | Parse NZB into a Job; orchestrate segment fetches; assemble files |
| `verify` | `VerifySet` | Run PAR2 verification on a completed Job; emit `RepairNeeded` if damaged |
| `repair` | `RepairSession` | Reed-Solomon reconstruction of damaged slices over GF(2^16) |
| `extract` | `Archive` | Multi-part RAR3/RAR5 extraction |
| `deliver` | `Delivery` | Move/rename to `complete/<category>/<release>/`, sample removal, folder collapse |
| `server` | `UsenetServer` | Server registry: host, creds, conn caps, priority, metered quota |
| `notify` | `Subscription` | Webhook / Discord / Slack / Pushover subscribers |
| `auth` | `User`, `Session`, `APIKey` | Auth state |

## Event flow

```
addfile (REST or SAB API)
  │
  ▼
download.JobCreated ──▶ download orchestrator dispatches segments
  ▼ (all segments resolved or terminal-missing)
download.JobDownloadComplete ──▶ verify worker picks up
  ▼
verify.VerifyOK ──▶ extract worker picks up
  │
  └─▶ verify.RepairNeeded ──▶ repair worker → repair.RepairOK ──▶ extract worker
                                          └─▶ repair.RepairFailed ──▶ deliver as damaged
  ▼
extract.ExtractOK ──▶ deliver worker
  ▼
deliver.DeliveryComplete ──▶ history is written, notify dispatches subscribers
```

No direct calls between workers. Each consumes from a topic, emits to
a topic. New behaviour (e.g. "Discord notifications on completion") is
a new subscriber, not a code change in the producer.

## Transactional outbox

Domain events are written to an SQLite `outbox` table in the same
transaction as the aggregate state change that produced them. A
per-subscriber dispatcher tails `outbox_subs` and delivers events with
retry + backoff + poison-message parking after N attempts. Subscribers
can opt to backfill historic events or only see new ones — controlled
at registration.

The benefit: when you add a Discord webhook three months after a job
completed, you can choose "notify me about every completion since
project start" without re-architecting anything.

## Persistence

SQLite via `modernc.org/sqlite` (pure Go, no cgo). WAL mode,
`synchronous=NORMAL`, `busy_timeout=5000`, `cache_size=-65536` (64 MB).
`_txlock=immediate` so every BEGIN claims the RESERVED write lock
upfront — the docs/standard recommendation for any Go app with
multiple writer goroutines hitting one SQLite DB.

Migrations are numbered SQL files in
`internal/adapter/sqlite/migrations/`, embedded via `embed.FS`,
applied at startup. Forward-only — if a migration is wrong, the fix is
a new forward migration that undoes it.

## Tests

```bash
make test                          # go test ./...
make race                          # go test -race ./...
cd frontend && npx playwright test # browser e2e (boots real binary)
```

Layers:

| Layer | Mock vs real |
|-|-|
| Domain | 100% pure; no external deps |
| App use cases | In-memory event bus + in-memory repos |
| `adapter/nzb` | Golden files + fuzz |
| `adapter/yenc` | Fixtures + fuzz (security-sensitive) |
| `adapter/nntp` | In-process stub (`net.Pipe`-based) |
| `adapter/par2` | `par2cmdline`-generated fixtures as oracle |
| `adapter/sqlite` | `:memory:` SQLite + fixture builders |
| `api/*` | `httptest` + table-driven |
| Bootstrap-level e2e | Real binary + in-process NNTP stub + Playwright |

CI runs everything except real-provider integration tests (env-gated
on `HOARDARR_USENET_TEST=1`).

## Why not standard libraries

For the data path — NNTP / yEnc / NZB / PAR2 — we own the wire. RFC
3977 + the PAR2 specification are both 30-page documents; writing them
yourself avoids a dependency surface that's hard to audit in projects
where the libraries are 10+ years old and have one maintainer.

The exception is RAR extraction (`nwaples/rardecode`). RAR is
reverse-engineered, not a published spec; a from-scratch RAR3 + RAR5
reader is months of work and would block 1.0 indefinitely. The
`extract.Extractor` port abstracts RAR away from the domain so a
from-scratch reader can swap in later without touching anything else.
