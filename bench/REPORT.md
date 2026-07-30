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
| CRC-32 (750 KiB) | 9409 MB/s | 9448 MB/s | 1.00× |
| yEnc decode + CRC (750 KiB) | 1830 MB/s | 3650 MB/s | **2.00×** |
| yEnc decode, CRC skipped | — | 6190 MB/s | — |
| NZB parse (50 files × 200 segments) | 49.5 MB/s | 367 MB/s | **7.4×** |
| TOML parse (config document) | 26.1 MB/s | 300 MB/s | **11.5×** |

### Reading these honestly

**CRC-32 is a tie, and that's the expected result.** Go's
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

| | Go | Zig |
|-|-|-|
| Binary, stripped, static x86_64-linux | — | **233 KB** |
| libc | — | none — raw syscalls |
| Dynamic loader | — | none — static |

The Go binary is not measured here yet because the comparison is only
meaningful once the Zig build embeds the frontend, as the Go one does.

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

- NNTP body read (dot-unstuffing) throughput
- GF(2^16) Reed-Solomon repair throughput
- HTTP requests/s on `/api/queue`
- Resident memory at idle, Go vs Zig
- Container image size, Go vs Zig
- Cold start to first served request
- `epoll` numbers from a Linux host, alongside the `poll` ones above
