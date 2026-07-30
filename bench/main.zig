//! Microbenchmark harness.
//!
//! Every performance claim about this port has to be reproducible on the
//! reader's own machine, next to the Go number it's being compared
//! against. `zig build bench` prints a table; `bench/run.sh` runs this
//! and the Go benchmarks back to back so the two columns come from the
//! same silicon in the same thermal state.
//!
//! Methodology, because benchmark numbers without it are decoration:
//!
//!   * Warm up until the working set is resident and the CPU has clocked
//!     up, then discard those samples.
//!   * Report the **median** of N timed rounds, plus the best, because
//!     the median is what you'll actually see and the best tells you how
//!     much noise there was.
//!   * Each round runs enough inner iterations to take >= 50 ms, so the
//!     clock's resolution is irrelevant.
//!   * `std.mem.doNotOptimizeAway` on every result, or the optimiser
//!     deletes the thing being measured and you get an infinity.

const std = @import("std");
const hoardarr = @import("hoardarr");

const sys = hoardarr.posix.sys;
const crc32 = hoardarr.core.crc32;
const yenc = hoardarr.codec.yenc;
const nzb = hoardarr.codec.nzb;
const toml = hoardarr.core.toml;
const protocol = hoardarr.nntp.protocol;
const gf16 = hoardarr.codec.par2.gf16;
const reactor = hoardarr.posix.reactor;

/// Timed rounds per benchmark. Odd so the median is a real sample.
const rounds = 9;
/// Minimum wall time per round.
const min_round_ns = 50 * std.time.ns_per_ms;

pub fn main(init: std.process.Init.Minimal) !u8 {
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const gpa = arena_state.allocator();

    // Optional filter: `zig build bench -- yenc` runs only matching names.
    var args = init.args.iterate();
    _ = args.next();
    const filter = args.next();

    var out: [4096]u8 = undefined;
    var w = std.Io.Writer.fixed(&out);
    try w.print("hoardarr benchmarks — {s}, reactor backend {s}\n\n", .{
        @tagName(@import("builtin").cpu.arch),
        reactor.backend_name,
    });
    try w.print("{s:<34}{s:>14}{s:>14}{s:>9}\n", .{ "benchmark", "median", "best", "unit" });
    try sys.writeAll(sys.stdout_fd, w.buffered());

    inline for (all_benchmarks) |bench| {
        const skip = if (filter) |f| std.mem.indexOf(u8, bench.name, f) == null else false;
        if (!skip) {
            const r = try bench.run(gpa);
            var line: [256]u8 = undefined;
            var lw = std.Io.Writer.fixed(&line);
            try lw.print("{s:<34}{d:>14.1}{d:>14.1}{s:>9}\n", .{ bench.name, r.median, r.best, bench.unit });
            try sys.writeAll(sys.stdout_fd, lw.buffered());
        }
    }

    return 0;
}

const Result = struct { median: f64, best: f64 };

const Benchmark = struct {
    name: []const u8,
    unit: []const u8,
    run: *const fn (gpa: std.mem.Allocator) anyerror!Result,
};

const Scale = enum { mb_per_s, ops_per_s };

/// Time `body` and convert to a rate. `work_per_iter` is bytes for MB/s
/// benchmarks and 1 for ops/s.
fn measure(
    comptime Ctx: type,
    ctx: *Ctx,
    comptime body: fn (*Ctx) anyerror!usize,
    work_per_iter: f64,
    comptime scale: Scale,
) !Result {
    // Calibrate: grow the iteration count until a round is long enough
    // that the clock's resolution stops mattering. This doubles as the
    // warmup — by the time calibration converges the working set is
    // resident and the CPU has clocked up.
    var iters: usize = 1;
    while (iters < 1 << 32) {
        const t0 = sys.monotonicNanos();
        for (0..iters) |_| {
            const produced = try body(ctx);
            std.mem.doNotOptimizeAway(produced);
        }
        const dt = sys.monotonicNanos() - t0;
        if (dt >= min_round_ns) break;
        // Scale toward the target rather than doubling blindly, so a very
        // fast body doesn't need 20 calibration passes to get there.
        const factor: usize = @intCast(@min(min_round_ns / @max(dt, 1), 1000));
        iters *= @max(2, factor);
    }

    var samples: [rounds]f64 = undefined;
    for (&samples) |*s| {
        const t0 = sys.monotonicNanos();
        for (0..iters) |_| {
            const produced = try body(ctx);
            std.mem.doNotOptimizeAway(produced);
        }
        const dt = sys.monotonicNanos() - t0;
        const per_iter_ns = @as(f64, @floatFromInt(dt)) / @as(f64, @floatFromInt(iters));
        s.* = switch (scale) {
            // bytes/ns is GB/s, so x1000 for MB/s.
            .mb_per_s => work_per_iter / per_iter_ns * 1000.0,
            .ops_per_s => 1e9 / per_iter_ns,
        };
    }

    std.mem.sort(f64, &samples, {}, std.sort.asc(f64));
    return .{ .median = samples[rounds / 2], .best = samples[rounds - 1] };
}

// ---------------------------------------------------------------------
// CRC32
// ---------------------------------------------------------------------

/// 750 KiB is the size of a typical Usenet article body, which is the
/// granularity this actually runs at in production.
const article_payload = 750 * 1024;

fn benchCrc32(gpa: std.mem.Allocator) !Result {
    const buf = try gpa.alloc(u8, article_payload);
    var prng = std.Random.DefaultPrng.init(0xBEEF);
    prng.random().bytes(buf);

    const Ctx = struct { buf: []const u8 };
    var ctx = Ctx{ .buf = buf };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            return crc32.checksum(c.buf);
        }
    }.f, @floatFromInt(article_payload), .mb_per_s);
}

// ---------------------------------------------------------------------
// yEnc
// ---------------------------------------------------------------------

/// A realistic article: 750 KiB of random payload, yEnc-encoded at
/// line=128. Random payload gives the natural ~2% escape density, not the
/// 0% you'd get from zeros or the 100% from a pathological input.
fn makeArticle(gpa: std.mem.Allocator) ![]const u8 {
    const payload = try gpa.alloc(u8, article_payload);
    var prng = std.Random.DefaultPrng.init(0x5EED);
    prng.random().bytes(payload);
    return yenc.encodeForTest(gpa, .{
        .payload = payload,
        .name = "bench.bin",
        .line_width = 128,
    });
}

fn benchYencDecode(gpa: std.mem.Allocator) !Result {
    const article = try makeArticle(gpa);
    const Ctx = struct { gpa: std.mem.Allocator, article: []const u8 };
    var ctx = Ctx{ .gpa = gpa, .article = article };
    // Rate is quoted over the *decoded* payload, since that's the number
    // that matters when asking how fast a download can go.
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            var a = try yenc.decode(c.gpa, c.article);
            defer a.deinit(c.gpa);
            return a.payload.len;
        }
    }.f, @floatFromInt(article_payload), .mb_per_s);
}

fn benchYencDecodeNoCrc(gpa: std.mem.Allocator) !Result {
    const article = try makeArticle(gpa);
    const Ctx = struct { gpa: std.mem.Allocator, article: []const u8 };
    var ctx = Ctx{ .gpa = gpa, .article = article };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            var a = try yenc.decodeUnverified(c.gpa, c.article);
            defer a.deinit(c.gpa);
            return a.payload.len;
        }
    }.f, @floatFromInt(article_payload), .mb_per_s);
}

// ---------------------------------------------------------------------
// NZB
// ---------------------------------------------------------------------

/// A 50-file, 200-segment-per-file NZB — the shape of a full season pack,
/// which is where parse time becomes noticeable in the UI.
fn makeNzb(gpa: std.mem.Allocator) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(gpa);
    const w = &buf.writer;
    try w.writeAll(
        \\<?xml version="1.0" encoding="iso-8859-1"?>
        \\<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
        \\<head><meta type="name">Bench Release</meta></head>
        \\
    );
    for (0..50) |f| {
        try w.print(
            \\<file poster="bench &lt;bench@example.invalid&gt;" date="1700000000" subject="[{d}/50] - &quot;bench.part{d:0>3}.rar&quot; yEnc (1/200)">
            \\<groups><group>alt.binaries.test</group><group>alt.binaries.misc</group></groups>
            \\<segments>
            \\
        , .{ f + 1, f + 1 });
        for (0..200) |s| {
            try w.print(
                "<segment bytes=\"768000\" number=\"{d}\">part{d}seg{d}@news.example.invalid</segment>\n",
                .{ s + 1, f, s },
            );
        }
        try w.writeAll("</segments>\n</file>\n");
    }
    try w.writeAll("</nzb>\n");
    return buf.toOwnedSlice();
}

fn benchNzbParse(gpa: std.mem.Allocator) !Result {
    const src = try makeNzb(gpa);
    const Ctx = struct { gpa: std.mem.Allocator, src: []const u8 };
    var ctx = Ctx{ .gpa = gpa, .src = src };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            var doc = try nzb.parse(c.gpa, c.src);
            defer doc.deinit();
            return doc.files.len;
        }
    }.f, @floatFromInt(src.len), .mb_per_s);
}

// ---------------------------------------------------------------------
// TOML
// ---------------------------------------------------------------------

fn benchTomlParse(gpa: std.mem.Allocator) !Result {
    // Synthetic rather than reading config.toml, so the number doesn't
    // depend on the working directory the bench was launched from.
    const src =
        \\listen = ":8085"
        \\data_dir = "/data"
        \\api_key = "0123456789abcdef0123456789abcdef"
        \\url_base = ""
        \\
        \\[log]
        \\level = "info"
        \\format = "text"
        \\max_size_mb = 32
        \\
        \\[download]
        \\incomplete_dir = "incomplete"
        \\complete_dir = "complete"
        \\bandwidth_global = 0
        \\max_connections = 40
        \\
        \\[server]
        \\hosts = ["news.example.invalid", "news2.example.invalid"]
        \\tls = true
        \\port = 563
        \\
    ;
    const Ctx = struct { gpa: std.mem.Allocator, src: []const u8 };
    var ctx = Ctx{ .gpa = gpa, .src = src };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            var p = try toml.parse(c.gpa, c.src, null);
            defer p.deinit();
            return c.src.len;
        }
    }.f, @floatFromInt(src.len), .mb_per_s);
}

// ---------------------------------------------------------------------
// NNTP body reader
// ---------------------------------------------------------------------

/// A dot-stuffed multi-line block the size of an article body. Every byte
/// of every article passes through the unstuffing scan, so this sits
/// directly on the download path.
fn makeStuffedBody(gpa: std.mem.Allocator) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(gpa);
    const w = &buf.writer;
    var prng = std.Random.DefaultPrng.init(0xD07);
    const rand = prng.random();

    var written: usize = 0;
    while (written < article_payload) {
        // 128-byte lines, matching the yEnc wrap width articles actually
        // arrive with.
        var line: [128]u8 = undefined;
        for (&line) |*b| b.* = rand.intRangeAtMost(u8, 0x21, 0x7E);
        // Every 64th line starts with a dot, so the stuffing path is
        // exercised at a realistic rate rather than never or always.
        if (written % (64 * 130) == 0) line[0] = '.';
        try w.writeAll(&line);
        try w.writeAll("\r\n");
        written += line.len + 2;
    }
    try w.writeAll(".\r\n");
    return buf.toOwnedSlice();
}

fn benchNntpBodyRead(gpa: std.mem.Allocator) !Result {
    const body = try makeStuffedBody(gpa);
    const dst = try gpa.alloc(u8, body.len);

    const Ctx = struct { body: []const u8, dst: []u8 };
    var ctx = Ctx{ .body = body, .dst = dst };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            var r: protocol.BodyReader = .{};
            const step = r.push(c.body, c.dst);
            return step.written;
        }
    }.f, @floatFromInt(body.len), .mb_per_s);
}

/// The scalar reference, for the ratio the SIMD path is claimed to win by.
fn benchNntpBodyReadScalar(gpa: std.mem.Allocator) !Result {
    const body = try makeStuffedBody(gpa);
    const dst = try gpa.alloc(u8, body.len);

    const Ctx = struct { body: []const u8, dst: []u8 };
    var ctx = Ctx{ .body = body, .dst = dst };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            var r: protocol.BodyReader = .{};
            const step = r.pushByteAtATime(c.body, c.dst);
            return step.written;
        }
    }.f, @floatFromInt(body.len), .mb_per_s);
}

// ---------------------------------------------------------------------
// GF(2^16) Reed-Solomon
// ---------------------------------------------------------------------

/// The repair hot loop: multiply a slice by a constant and XOR it into an
/// accumulator. A PAR2 repair does this once per (damaged slice x
/// recovery slice) pair, so it is the whole cost of a repair.
///
/// One slice of a real PAR2 set — the vendored fixture uses 1.5 MiB
/// slices — expressed as GF(2^16) elements.
const rs_slice_elements = (1536 * 1024) / 2;

fn benchGf16MulAdd(gpa: std.mem.Allocator) !Result {
    const acc = try gpa.alloc(u16, rs_slice_elements);
    const src = try gpa.alloc(u16, rs_slice_elements);
    var prng = std.Random.DefaultPrng.init(0x6F16);
    prng.random().bytes(std.mem.sliceAsBytes(acc));
    prng.random().bytes(std.mem.sliceAsBytes(src));

    const Ctx = struct { acc: []u16, src: []const u16, c: u16 = 0 };
    var ctx = Ctx{ .acc = acc, .src = src };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            // Vary the constant: a fixed one would let the optimiser hoist
            // table lookups out of the timed region.
            c.c +%= 0x9E37;
            gf16.mulAddSlice(c.acc, c.src, c.c | 1);
            return c.acc.len;
        }
    }.f, @floatFromInt(rs_slice_elements * 2), .mb_per_s);
}

// ---------------------------------------------------------------------
// Reactor
// ---------------------------------------------------------------------

/// Timer churn: arm and cancel against a heap that's already deep. This
/// is the operation the download orchestrator does most — a retry timer
/// per in-flight segment, cancelled on success — so O(log n) cancellation
/// is worth measuring rather than assuming.
fn benchTimerChurn(gpa: std.mem.Allocator) !Result {
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);

    const noop = struct {
        fn f(_: *reactor.Timer) void {}
    }.f;

    // Preload so we measure against a realistic heap depth rather than an
    // empty heap where sift-down never runs.
    const resident = try gpa.alloc(reactor.Timer, 1024);
    for (resident, 0..) |*t, i| {
        t.* = .{ .callback = noop };
        try loop.addTimerAt(t, @intCast(1_000_000_000 + i * 7919));
    }

    const Ctx = struct { loop: *reactor.Loop, t: reactor.Timer, n: u64 = 0 };
    var ctx = Ctx{ .loop = &loop, .t = .{ .callback = noop } };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            c.n +%= 1;
            // Vary the deadline so it lands at a different heap depth each
            // time instead of always at the root.
            try c.loop.addTimerAt(&c.t, 1_000_000_000 + (c.n *% 2654435761) % 1_000_000);
            c.loop.cancelTimer(&c.t);
            return 1;
        }
    }.f, 1, .ops_per_s);
}

/// Dispatch rate: how many ready-fd events the loop routes per second.
/// This is the ceiling on request throughput before any handler runs.
fn benchReactorDispatch(gpa: std.mem.Allocator) !Result {
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);

    const Probe = struct {
        source: reactor.Source,
        pipe: sys.Pipe,
        fired: usize = 0,

        fn onReady(src: *reactor.Source, _: reactor.Ready) void {
            const self: *@This() = @fieldParentPtr("source", src);
            var buf: [64]u8 = undefined;
            _ = sys.read(src.fd, &buf) catch {};
            self.fired += 1;
        }
    };

    // 64 registered sources with one hot. That's the realistic shape, and
    // it makes the poll backend pay its O(n) kernel-side scan rather than
    // flattering it with a single fd.
    const probes = try gpa.alloc(Probe, 64);
    for (probes) |*p| {
        const pp = try sys.pipe();
        p.* = .{
            .source = .{ .fd = pp.read_end, .interest = .readable, .callback = Probe.onReady },
            .pipe = pp,
        };
        try loop.add(&p.source);
    }

    const Ctx = struct { loop: *reactor.Loop, hot: *Probe };
    var ctx = Ctx{ .loop = &loop, .hot = &probes[32] };
    return measure(Ctx, &ctx, struct {
        fn f(c: *Ctx) anyerror!usize {
            _ = try sys.write(c.hot.pipe.write_end, "x");
            return c.loop.tick(0);
        }
    }.f, 1, .ops_per_s);
}

const all_benchmarks = [_]Benchmark{
    .{ .name = "crc32 (750 KiB)", .unit = "MB/s", .run = benchCrc32 },
    .{ .name = "yenc decode + crc (750 KiB)", .unit = "MB/s", .run = benchYencDecode },
    .{ .name = "yenc decode, no crc", .unit = "MB/s", .run = benchYencDecodeNoCrc },
    .{ .name = "nzb parse (50f x 200seg)", .unit = "MB/s", .run = benchNzbParse },
    .{ .name = "toml parse", .unit = "MB/s", .run = benchTomlParse },
    .{ .name = "nntp body read (750 KiB)", .unit = "MB/s", .run = benchNntpBodyRead },
    .{ .name = "nntp body read, scalar ref", .unit = "MB/s", .run = benchNntpBodyReadScalar },
    .{ .name = "gf16 mul-add (1.5 MiB slice)", .unit = "MB/s", .run = benchGf16MulAdd },
    .{ .name = "reactor timer arm+cancel", .unit = "ops/s", .run = benchTimerChurn },
    .{ .name = "reactor dispatch (64 fds)", .unit = "ops/s", .run = benchReactorDispatch },
};
