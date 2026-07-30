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
frontend/           # React + TS + Vite, embedded at build time (gzipped)
src/posix/          # syscalls, the reactor, signals, fibers
src/net/            # sockets, TLS, HTTP server + client
src/core/           # logging, config, TOML, CRC32
tools/              # build-time asset embedder
docker/             # entrypoint shim
docs/               # operator + contributor docs
```

The domain layer has zero dependencies on adapters or frameworks.
`zig test src/domain/<name>.zig` runs without a database, an HTTP
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

## Concurrency

One thread, one event loop. `src/posix/reactor.zig` blocks in `epoll_wait`
(Linux) or `poll` (everywhere else) with the nearest timer deadline as its
timeout, and dispatches readiness to intrusively-registered sources. There
is no polling interval anywhere in the design, so an idle daemon makes zero
wakeups and consumes no measurable CPU — asserted by a test, not just
measured.

Connections are explicit state machines rather than blocking calls on their
own stacks. A timeout is a reactor timer; cancelling one cannot race,
because there is nothing to race with.

The one exception is TLS: `std.crypto.tls.Client` runs its handshake
synchronously across several round trips and is not resumable, so it runs on
a stackful fiber (`src/posix/fiber.zig`) that parks on the reactor when its
socket would block. Synchronous-looking TLS, still one thread.

## Persistence

SQLite, vendored as the C amalgamation at `c/sqlite3/` and compiled by the
Zig toolchain — no FFI boundary, no cgo-shaped cost. WAL mode,
`synchronous=NORMAL`, `busy_timeout=5000`, `cache_size=-65536` (64 MB), and
`BEGIN IMMEDIATE` so every transaction claims the write lock upfront rather
than discovering a conflict at COMMIT.

Compiled with `SQLITE_THREADSAFE=2`, meaning a connection must not be shared
between threads. That is modelled rather than documented: a connection is
owned by one thread and Debug builds assert the owner. `SQLITE_DQS=0` turns
a typo'd column name into an error instead of a silent string literal.

Prepared statements are cached per connection, keyed by SQL text, released
with `reset` + `clear_bindings` rather than finalized.

Migrations are numbered SQL files in
`internal/adapter/sqlite/migrations/`, embedded via `embed.FS`,
applied at startup. Forward-only — if a migration is wrong, the fix is
a new forward migration that undoes it.

## Tests

```bash
make test                          # zig build test
make test-release                  # same suite under ReleaseFast
make check                         # type-check every shipping target
make ci                            # everything CI runs
cd frontend && npx playwright test # browser e2e (boots real binary)
```

The suite runs in **both** Debug and ReleaseFast on purpose. Debug has the
safety checks; ReleaseFast has the optimiser, and the reactor and the SIMD
codecs have each had a bug that only appeared optimised.

`make check` matters because the Linux-only paths — epoll, signalfd,
eventfd, raw syscalls — cannot run on a macOS dev box. A backend that only
CI ever compiles is a backend that breaks silently.

Layers:

| Layer | Mock vs real |
|-|-|
| Domain | 100% pure; no external deps, no clock (timestamps are parameters) |
| App use cases | Injected ports with `Fake*` doubles; no database, socket or clock |
| `codec/nzb` | Golden files + fuzz |
| `codec/yenc` | Fixtures + fuzz (security-sensitive) |
| `nntp` | Scripted in-process server over a real socket |
| `codec/par2` | `par2cmdline`-generated fixtures as oracle |
| `store` | `:memory:` SQLite + fixture builders |
| `api/*` | Injected ports + requests over a real socket |
| Bootstrap-level e2e | Real binary + in-process NNTP stub + Playwright |

CI runs everything except real-provider integration tests (env-gated
on `HOARDARR_USENET_TEST=1`).

## Why not standard libraries

For the data path — NNTP / yEnc / NZB / PAR2 — we own the wire. RFC
3977 + the PAR2 specification are both 30-page documents; writing them
yourself avoids a dependency surface that's hard to audit in projects
where the libraries are 10+ years old and have one maintainer.

RAR used to be the exception, handled by a third-party library because it
is reverse-engineered rather than specified and a complete reader is months
of work. `src/codec/rar/` is now from scratch, but deliberately narrow and
loud about it: header parsing is complete for RAR3 and RAR5, and `Stored`
(`-m0`) extraction works single- and multi-volume including a file spanning
a volume boundary — which is what essentially every Usenet post is, since
the payload is already-compressed video. Everything else refuses by name
(`UnsupportedCompressionMethod` naming the method, `ArchiveEncrypted`,
`MissingVolume`). Nothing is silently approximated, because mis-extracting
a file is worse than refusing to.

The `extract.Extractor` port still abstracts it away from the domain, so
widening that scope touches one adapter.
