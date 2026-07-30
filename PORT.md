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
| GF(2^16) | `adapter/par2/gf16/*.go` (290) | `gf16_test.go`, `matrix_test.go` (274) | `src/codec/par2/gf16.zig`, `matrix.zig` | done |
| PAR2 parse | `adapter/par2/par2.go` (382) | `par2_test.go` (172) | `src/codec/par2/par2.zig`, `verifier.zig` | done |
| Reed-Solomon | `adapter/par2/rs.go` (260) | `rs_test.go` (145) | `src/codec/par2/rs.zig` | done |
| TOML config | `config/config.go` (389) | `config_test.go` (226) | `src/core/toml.zig`, `src/core/config.zig` | done |
| bcrypt | `adapter/bcrypt` | — | `std.crypto.bcrypt` wrapper | todo |

### Wave 2 — I/O foundation + persistence

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| Reactor | (Go runtime) | — | `src/posix/reactor.zig`, `src/posix/sys.zig` | done |
| Structured log | `logfile/`, `loghub/` (476) | `loghub_test.go` (123) | `src/core/log.zig`, `logring.zig` | done |
| SQLite C build | (vendored 3.50.4) | linkage/WAL/DQS tests | `c/sqlite3/`, `src/store/sqlite_c.zig` | done |
| SQLite binding | `adapter/sqlite/sqlite.go` (244) | `sqlite_test.go` (234) | `src/store/sqlite.zig` | in progress |
| Migrations | `adapter/sqlite/migrate.go` (170) | — | `src/store/migrate.zig` | todo |
| Outbox | `adapter/sqlite/outbox.go` (786) | `outbox_test.go` (361) | `src/store/outbox.zig` | in progress |
| Repos | `adapter/sqlite/repo_*.go` | `repo_*_test.go` | `src/store/repo_*.zig` | todo |
| NNTP protocol | `adapter/nntp/conn.go`, `errors.go` | `body_reader_test.go`, `errors_test.go` | `src/nntp/protocol.zig` | done |
| NNTP conn (I/O half) | `adapter/nntp/conn.go` (437) | `stub_test.go` (486) | `src/nntp/conn.zig` | todo |
| NNTP pool | `adapter/nntp/pool.go` (330) | `pool_test.go` (172) | `src/nntp/pool.zig` | todo |
| Fiber bridge | (Go goroutines) | — | `src/posix/fiber.zig` | in progress |
| TLS | `crypto/tls` | — | `src/net/tls.zig` | in progress |

### Wave 3 — domain + app

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| download domain | `domain/download/*.go` (1122) | `job_test.go` (390) | `src/domain/download/*.zig` | done |
| other aggregates | `domain/{verify,repair,extract,deliver,server,notify,auth,command,schedule}` | various | `src/domain/*.zig` | in progress |
| orchestrator | `app/download/*.go` (1728) | `orchestrator_test.go` + 4 more (800) | `src/app/download/*.zig` | todo |
| verify/repair/extract/deliver | `app/{verify,repair,extract,deliver}` (1575) | various | `src/app/*.zig` | todo |
| notify | `app/notify` + `adapter/notify/*` (1182) | `render_test.go`, `discord_test.go`, `slack_test.go` (528) | `src/app/notify/*.zig` | in progress |
| scheduler, system, auth, command | `app/*` | various | `src/app/*.zig` | todo |
| RAR reader | `adapter/rar` (was `nwaples/rardecode`) | `rar_test.go` (72) | `src/codec/rar/*.zig` | todo |

### Wave 4 — API + shipping

| Module | Go source | Go tests | Zig | Status |
|-|-|-|-|-|
| TCP sockets | (Go net) | — | `src/net/socket.zig` | done |
| HTTP/1.1 server | `server/*.go` (862) | `auth_test.go` (101) | `src/net/http/*.zig` | in progress |
| REST API | `api/rest/*.go` (2212) | `ratelimit_test.go` (45) | `src/api/rest/*.zig` | todo |
| SAB API | `api/sab/*.go` (1056) | `sort_eval_test.go` (78) | `src/api/sab/*.zig` | todo |
| SSE hub | `api/sse/hub.go` (176) | — | `src/api/sse.zig` | todo |
| Metrics | `metrics/` | — | `src/api/metrics.zig` | todo |
| Frontend embed | `assets*.go` | — | build-step asset blob | todo |
| e2e suite | `bootstrap/e2e_*_test.go` (3000+) | — | `src/e2e/*.zig` | todo |
| Container | `Dockerfile` | — | static musl → scratch | todo |
| Benchmarks | — | — | `bench/` + `bench/REPORT.md` | throughput done; HTTP/RSS/image pending |

## Benchmarks

Results live in `bench/REPORT.md`; reproduce with `bench/run.sh`.

Done: yEnc decode, CRC-32, NNTP body read, GF(2^16) multiply-accumulate,
NZB parse, TOML parse, reactor timer churn and dispatch, idle CPU
(asserted as a test in `src/posix/reactor.zig`), stripped binary size.

Still owed: HTTP requests/s, resident memory at idle vs Go, container
image size vs Go, cold start to first served request, and the whole table
re-run on a Linux host so the `epoll` backend is measured rather than
`poll`.

## Does it actually work yet?

**No. Not end to end.** The components are built and tested; they are not
yet connected, so the daemon serves a health check and the web UI and
cannot download anything.

| | Go | Zig |
|-|-|-|
| HTTP routes wired into the daemon | 56 | 3 |
| Add an NZB and download it | yes | **no** |
| e2e tests passing | 22 | **0** |
| Unit/integration tests | 269 funcs | 1142 |

What *is* proven: every component has real tests — yEnc, PAR2 (against a
`par2cmdline` fixture), NZB, RAR, the NNTP connection and pool against a
scripted server, the store with its outbox and thirteen repositories, the
SAB handler against goldens captured from Go's own JSON encoder, the HTTP
server, notify. Plus the daemon starts, migrates, serves, and survives both
a clean `SIGTERM` and a `SIGKILL` with WAL replay.

What that does **not** prove is that the pieces work together. A restart
test passes trivially when there are no jobs in flight to lose.

### The parity gate

`internal/bootstrap/` holds 22 end-to-end tests that boot the real binary
against an in-process NNTP server and drive a complete download — NZB in,
segments fetched, yEnc decoded, PAR2 verified and repaired, RAR extracted,
delivered, history written — plus crash recovery mid-download, multi-server
failover, throttling, auth, and webhooks.

**Nothing in this port claims functional parity until those 22 pass.** They
are the only thing that answers "is it truly the same". Porting them is
tracked as its own task, and `src/testserver/` (fixture generator +
content-addressed NNTP server with missing-fraction, throttle, latency and
connection-limit knobs) is the harness they need.

## Known debt

* `src/core/log.zig` carries `open`/`lseek`/`rename`/`unlink`/`mkdir`/
  `getdents` in a marked section at the bottom. They belong in
  `src/posix/sys.zig` once it grows a filesystem section, so `store/` and
  `deliver/` share one implementation instead of three.
* `adapter/par2/repair.go` is not ported — it needs filesystem write-back
  and job plumbing. `rs.reconstruct` and `verifier`, which it sits on,
  are done.
* The CA bundle for TLS has to be embedded at build time; the container
  has no `/etc/ssl/certs` to read.
* **TLS has never completed a handshake against a real server.** `std.crypto.tls`
  ships a client and no server, so the tests reach an inspected ClientHello
  and can drive server records back in (a fatal alert becomes `TlsAlert`,
  garbage becomes a protocol error, a truncated record becomes a transport
  failure), but nothing gets as far as ServerHello, the key schedule, or
  certificate verification. `reader()`/`writer()` compile and cross-compile
  but have never moved a plaintext byte. **This must be validated against a
  real provider before anyone relies on it.**
* Fiber stacks are 1 MiB. The canary test measured a real `Client.init` at
  148 KB under ReleaseFast and 506 KB under Debug, and 256 KiB actually
  crashed on the guard page. It is virtual address space, so 40 connections
  is ~40 MiB of VA and ~6 MiB resident.
