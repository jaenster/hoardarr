# Benchmarks: Go vs Zig

Reproduce with `bench/run.sh`. It runs both implementations back to back
so the two columns come off the same machine in the same thermal state.

**Machine:** Apple M3 Max, 16 cores, macOS.
**Toolchains:** Zig 0.16.0 (`--release=fast`), Go 1.25.0.
**Reactor backend:** `poll` (macOS). The shipping container uses `epoll`;
those numbers need a Linux host and are marked as such below.

Zig figures are the **median of 9 rounds**, each round sized to run for at
least 50 ms so clock resolution is irrelevant. Go figures come from `go
test -bench -benchtime=2s`. Inputs are matched in size and shape; payload
bytes are random on both sides, so yEnc escape density (~1.6% of encoded
bytes) matches even though the two PRNGs emit different bytes — for a
throughput measurement that is what has to be equal.

## Throughput

| Benchmark | Go | Zig | Ratio |
|-|-|-|-|
| CRC-32 (750 KiB) | 9948 MB/s | 9766 MB/s | 0.98× |
| yEnc decode + CRC (750 KiB) | 1902 MB/s | 3571 MB/s | **1.88×** |
| yEnc decode, CRC skipped | — | 6115 MB/s | — |
| NNTP body read, dot-unstuffing (750 KiB) | 7569 MB/s | 10720 MB/s | **1.42×** |
| NNTP body read, scalar reference | — | 879 MB/s | — |
| GF(2^16) multiply-accumulate (1.5 MiB slice) | 1445 MB/s | 4576 MB/s | **3.17×** |
| NZB parse (50 files × 200 segments) | 52.7 MB/s | 400 MB/s | **7.6×** |
| TOML parse (config document) | 24.9 MB/s | 304 MB/s | **12.2×** |

Run-to-run variance on this machine is a few percent, so treat the last
digit as noise. CRC-32 in particular lands either side of parity depending
on the run.

### Reading these honestly

**CRC-32 is a tie — Go was marginally ahead in this run — and that's the
expected result.** Go's
`hash/crc32` already dispatches to the ARMv8 `crc32` instructions, and so
do we. There is no win available here — both implementations are limited
by the same instruction throughput. It's in the table precisely because a
benchmark table where everything is faster is a benchmark table nobody
should trust.

**yEnc's 2× is the one that matters for download speed.** Every byte of
every article passes through this decoder. The Go version is already
SWAR-optimised (8 bytes per iteration via a packed subtract), so this is
not a fast-vs-naive comparison — it is 8-byte SWAR against 16-byte NEON
plus a cheaper escape path. Rather than scalarising a whole vector when it
contains an `=`, the Zig decoder stores `v -% 42` unconditionally and uses
`@ctz` on the escape mask: lanes before the first escape are already
correct, and lanes after it are overwritten by later stores because the
output cursor only moves forward. An escape therefore costs one loop
re-entry instead of 16 scalar iterations.

With CRC skipped the decoder reaches 6190 MB/s, which shows the combined
figure is roughly half decode and half checksum. On a real download the
CRC is not optional, so 3650 MB/s is the number that counts.

**The NNTP body reader's 1.46× is against an already-fast Go version.**
Commit `2fcb663` replaced Go's stdlib `textproto` reader with a
line-batched one and got ~15× out of it; that optimised version is what
7346 MB/s measures, not the stdlib's 0.32 GB/s. So this is a vectorised
scan against a good scalar one. The Zig scalar reference is in the table
at 879 MB/s to show what the vectorisation itself is worth — 12× — and to
make clear the 1.42× is the honest number against a fair opponent.

One caveat on that baseline: `internal/adapter/nntp` has two body-read
benchmarks with different framing, and they disagree —
`BenchmarkBodyRead_Fast` reports 4927 MB/s while the one added for this
comparison reports 7569 MB/s, because the former includes per-call reader
construction (29 allocs/op). The faster figure is quoted, since the point
is to measure the scan against the strongest version of the Go scan.

**GF(2^16)'s 3.17× is partly an implementation gap, and that's worth
saying plainly.** Go has no bulk multiply at all: `internal/adapter/par2/rs.go`
does `acc[k] ^= gf16.Mul(coef, d)` element by element, and that loop is
what the baseline measures, because it is what a Go repair actually runs.
Some of the 3.17× is SIMD and some is simply that nobody wrote the bulk
path in Go. Either way it's the real before-and-after for a repair.

**NZB and TOML are large ratios against small absolute costs.** 7.4× and
11.5× look dramatic, and the mechanism is real — no reflection, no
`interface{}` boxing, and one arena that owns every string so a parse is
freed with a single `deinit`. But be clear about what it buys: parsing a
season-pack NZB drops from ~16 ms to ~2 ms, and config parse from 13 µs to
1 µs. That is a snappier "add NZB" click and a faster start-up, not a
faster download. The throughput numbers that affect a download are yEnc,
CRC, and the NNTP body reader.

## Reactor

| Benchmark | Zig | Note |
|-|-|-|
| Timer arm + cancel, 1024-deep heap | 56.3 M ops/s | 18 ns per arm+cancel pair |
| Event dispatch, 64 registered fds | 124 K ops/s | Includes the `poll` syscall and a pipe read |

There is no Go counterpart for these: in the Go implementation the
equivalent work is done by the runtime scheduler and `time.AfterFunc`,
which cannot be isolated into a comparable measurement. They are here to
pin the reactor's own cost so a future regression is visible.

Timer churn is measured against a heap that already holds 1024 entries,
because that is the shape the download orchestrator produces — one retry
timer per in-flight segment, cancelled on success. An intrusive heap index
makes cancellation O(log n) rather than a linear scan, and 18 ns per pair
is the evidence.

## Idle CPU

The headline claim, asserted as a test rather than described in prose —
see `test "idle loop consumes no measurable CPU"` in
`src/posix/reactor.zig`. It parks the loop on a quiet fd for 200 ms of
wall time and compares process CPU time against it, failing if CPU exceeds
1% of wall.

The mechanism: there is no polling interval anywhere in the design. Idle
means every fd is registered, the nearest timer deadline is the `poll`
timeout, and the thread is blocked in one syscall. CPU consumption scales
with the number of wakeups, not with elapsed time, so an empty queue costs
nothing. A 100 ms "check for work" loop — the usual arrangement — wakes
864,000 times a day to discover nothing changed.

## Footprint

Both images built from this repo with `docker build`, same machine, both
embedding the frontend bundle:

| | Go | Zig | |
|-|-|-|-|
| **Container image** | **38.8 MB** | **429 kB** | **90× smaller** |
| Base image | `alpine:3.20` | `scratch` | |
| Binary, stripped, static | — | 233 KB (no UI) | |
| libc | musl, in the image | none — raw syscalls | |
| Dynamic loader | present | none — static | |
| Shell in image | yes (`/bin/sh`) | no | |

Verified running: `docker run --rm hoardarr:zig version` prints
`reactor backend: epoll`, so the Linux backend really is the one active in
the container rather than the `poll` fallback the macOS tests exercise.

Almost all of the 38 MB the Go image spends is base layer, not program.
The Zig runtime stage is `scratch` because nothing the alpine base provided
is needed any more:

* **No libc.** Linux goes straight to syscalls, so there is no dynamic
  loader and nothing to link at runtime. Only the vendored SQLite wants a
  libc, and musl is linked statically into the binary.
* **No `su-exec` and no shell.** Dropping to `PUID:PGID` was the
  entrypoint shim's job; we call `setgid`/`setgroups`/`setuid` in-process
  before starting the reactor.
* **No `ca-certificates`.** The CA bundle is compiled into the binary, so
  TLS to a provider does not depend on a file existing.
* **No `tzdata`.** Timestamps are stored and logged in UTC and rendered in
  the browser's zone, which is where a user's timezone actually lives.
* **No frontend directory.** The bundle is embedded, gzipped at level 9 at
  build time — 318 KB of assets become 92 KB, and the daemon never runs a
  compressor.

233 KB with no libc is the result of two decisions. Linux uses raw
syscalls via `std.os.linux` instead of libc, so there is no dynamic
loader, no libc initialisation, and no libc in the image — verified: the
binary contains 571 raw `syscall` instructions and no `libc` symbols, and
the musl and gnu targets produce byte-identical output because neither
links a C library. Second, symbols are stripped, since a crash in a
container is diagnosed from the structured log, not from a backtrace
nobody can symbolise.

## Not yet measured

These are in the goal and still owed:

- HTTP requests/s on `/api/queue`
- Resident memory at idle, Go vs Zig
- Cold start to first served request
- `epoll` numbers from a Linux host, alongside the `poll` ones above
