# Go → Zig port

Tracking document for replacing the Go implementation with Zig. Goal:
same behaviour, higher throughput, lower idle CPU, smaller container.

Ground rules for the port:

* **Tests first.** For each module, the Go test suite is translated to
  Zig `test` blocks *before* the implementation. A module is only
  "done" when its Zig tests cover what the Go tests covered and pass.
* **No Go left.** `internal/` and `cmd/` are deleted at the end, not
  kept as a fallback. The frontend (React/TS) stays as-is.
* **POSIX only.** Containers are the only deployment target. `poll(2)`
  is the portable baseline; `epoll` is the Linux fast path. No Windows,
  no fallbacks for platforms we don't ship.
* **Prove it.** Every performance claim gets a benchmark in `bench/`
  with the Go number next to the Zig number.

## Conventions

Established in `src/core/crc32.zig` — read it before adding a module.

| Rule | Detail |
|-|-|
| Layout | `src/<layer>/<module>.zig`, layers mirror the Go tree: `core`, `codec`, `posix`, `net`, `nntp`, `store`, `domain`, `app`, `api` |
| Registration | Every new file gets a line in `src/root.zig` — both the `pub const` and the `test` block, or its tests never run |
| Allocators | Take `std.mem.Allocator` as the first parameter. No global allocator, no hidden allocation in hot paths |
| Tests | Colocated `test "..."` blocks at the bottom of the file. Use `std.testing.allocator` so leaks fail the test |
| Errors | Explicit error sets on public functions. No `anyerror` in a public signature |
| Comments | Explain *why*, and document the non-obvious (SWAR/SIMD tricks, wire-format quirks). Never reference porting phases or plans |
| Naming | `snake_case` files and variables, `TitleCase` types, `camelCase` functions |

## Inventory

Go: 31,145 lines of implementation, 12,714 lines of tests,
269 test/fuzz/bench functions.

### Wave 1 — pure codecs, no I/O (parallel, no interdependencies)

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| crc32 | (stdlib) | — | `src/core/crc32.zig` | done |
| yEnc decode | `adapter/yenc/*.go` (767) | `yenc_test.go`, `yenc_bench_test.go` (380) | `src/codec/yenc.zig` | done |
| NZB parse | `adapter/nzb/nzb.go` (289) | `nzb_test.go` (159) | `src/codec/nzb.zig`, `src/codec/xml.zig` | done |
| GF(2^16) | `adapter/par2/gf16/*.go` (290) | `gf16_test.go`, `matrix_test.go` (274) | `src/codec/par2/gf16.zig` | todo |
| PAR2 parse | `adapter/par2/par2.go` (382) | `par2_test.go` (172) | `src/codec/par2/par2.zig` | todo |
| Reed-Solomon | `adapter/par2/rs.go` (260) | `rs_test.go` (145) | `src/codec/par2/rs.zig` | todo |
| TOML config | `config/config.go` (389) | `config_test.go` (226) | `src/core/toml.zig`, `src/core/config.zig` | done |
| bcrypt | `adapter/bcrypt` | — | `std.crypto.bcrypt` wrapper | todo |

### Wave 2 — I/O foundation + persistence

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| Reactor | (Go runtime) | — | `src/posix/reactor.zig`, `src/posix/sys.zig` | done |
| Structured log | `logfile/`, `loghub/` (476) | `loghub_test.go` (123) | `src/core/log.zig` | todo |
| SQLite binding | `adapter/sqlite/sqlite.go` (244) | `sqlite_test.go` (234) | `src/store/sqlite.zig` | todo |
| Migrations | `adapter/sqlite/migrate.go` (170) | — | `src/store/migrate.zig` | todo |
| Outbox | `adapter/sqlite/outbox.go` (786) | `outbox_test.go` (361) | `src/store/outbox.zig` | todo |
| Repos | `adapter/sqlite/repo_*.go` | `repo_*_test.go` | `src/store/repo_*.zig` | todo |
| NNTP wire | `adapter/nntp/conn.go` (437) | `body_reader_test.go`, `stub_test.go` (712) | `src/nntp/conn.zig` | todo |
| NNTP pool | `adapter/nntp/pool.go` (330) | `pool_test.go` (172) | `src/nntp/pool.zig` | todo |
| TLS | `crypto/tls` | — | `std.crypto.tls.Client` wrapper | todo |

### Wave 3 — domain + app

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| download domain | `domain/download/*.go` (1122) | `job_test.go` (390) | `src/domain/download.zig` | todo |
| other aggregates | `domain/{verify,repair,extract,deliver,server,notify,auth,command,schedule}` | various | `src/domain/*.zig` | todo |
| orchestrator | `app/download/*.go` (1728) | `orchestrator_test.go` + 4 more (800) | `src/app/download/*.zig` | todo |
| verify/repair/extract/deliver | `app/{verify,repair,extract,deliver}` (1575) | various | `src/app/*.zig` | todo |
| notify | `app/notify` + `adapter/notify/*` (1182) | `render_test.go`, `discord_test.go`, `slack_test.go` (528) | `src/app/notify/*.zig` | todo |
| scheduler, system, auth, command | `app/*` | various | `src/app/*.zig` | todo |
| RAR reader | `adapter/rar` (was `nwaples/rardecode`) | `rar_test.go` (72) | `src/codec/rar/*.zig` | todo |

### Wave 4 — API + shipping

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| HTTP/1.1 server | `server/*.go` (862) | `auth_test.go` (101) | `src/net/http/server.zig` | todo |
| REST API | `api/rest/*.go` (2212) | `ratelimit_test.go` (45) | `src/api/rest/*.zig` | todo |
| SAB API | `api/sab/*.go` (1056) | `sort_eval_test.go` (78) | `src/api/sab/*.zig` | todo |
| SSE hub | `api/sse/hub.go` (176) | — | `src/api/sse.zig` | todo |
| Metrics | `metrics/` | — | `src/api/metrics.zig` | todo |
| Frontend embed | `assets*.go` | — | build-step asset blob | todo |
| e2e suite | `bootstrap/e2e_*_test.go` (3000+) | — | `src/e2e/*.zig` | todo |
| Container | `Dockerfile` | — | static musl → scratch | todo |
| Benchmarks | — | — | `bench/` + `bench/REPORT.md` | todo |

## Benchmarks to publish

Go baseline vs Zig, same machine, same input:

1. yEnc decode throughput (MB/s) on a 750 KiB article
2. CRC32 throughput (GB/s)
3. NNTP body read (dot-unstuffing) throughput
4. GF(2^16) Reed-Solomon repair throughput
5. NZB parse (large multi-file NZB)
6. HTTP request/s on `/api/queue`
7. **Idle CPU** — % over 60 s with an empty queue
8. Resident memory at idle
9. Container image size
10. Cold start to first served request
