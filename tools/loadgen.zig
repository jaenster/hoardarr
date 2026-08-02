//! A standalone load generator: one synthetic Usenet release, served
//! over NNTP for as long as the process lives.
//!
//! Idle numbers are easy and uninteresting. What an operator actually
//! needs to know is what the daemon costs while it is saturating a link
//! — how much CPU the yEnc and PAR2 paths burn, how much resident memory
//! the segment buffers hold — and that requires something on the other
//! end of the socket handing out articles as fast as they are asked for.
//! A real provider is the wrong instrument for that: it is rate-limited,
//! it is not reproducible, and it costs money per measurement.
//!
//! So this binary is the provider. It reuses the same fixture generator
//! and the same fake NNTP server the end-to-end suite runs against, which
//! means a load run exercises exactly the code paths the tests do, at a
//! size the tests cannot afford. It writes the release's NZB to a path of
//! the caller's choosing so an external process can POST it at the
//! daemon's queue endpoint, then serves the corpus indefinitely and logs
//! the offered load on an interval.
//!
//! It links no database and no frontend: a load harness that needs a
//! writable volume is a load harness nobody runs in a `scratch`
//! container.

const std = @import("std");
const hoardarr = @import("hoardarr");

const fixture = hoardarr.testserver.fixture;
const testserver = hoardarr.testserver.nntp;
const reactor = hoardarr.posix.reactor;
const sys = hoardarr.posix.sys;
const log = hoardarr.core.log;

const usage =
    \\loadgen — serve a synthetic Usenet release over NNTP
    \\
    \\Usage: loadgen [options]
    \\
    \\Options:
    \\  --listen HOST:PORT    Listen address           (default 0.0.0.0:1119)
    \\  --nzb PATH            Where to write the NZB   (default ./release.nzb)
    \\  --files N             Data files per release   (default 8)
    \\  --file-size BYTES     Size of each data file   (default 4M)
    \\  --article-size BYTES  yEnc segment size        (default 768K)
    \\  --recovery-slices N   PAR2 recovery slices     (default 0)
    \\  --missing-fraction F  Share of articles 430'd  (default 0)
    \\  --max-connections N   0 is unlimited           (default 0)
    \\  --repeat N            Distinct releases        (default 1)
    \\  --stats-interval S    Seconds between reports  (default 5)
    \\  --seed N              PRNG seed for file bytes (default 0)
    \\
    \\Sizes accept a K, M or G suffix. With --repeat above 1 the NZBs are
    \\written as PATH.1, PATH.2, ... so concurrent jobs pull distinct data.
    \\
;

const Config = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 1119,
    nzb_path: []const u8 = "./release.nzb",
    files: usize = 8,
    file_size: usize = 4 << 20,
    /// What real posters use. Smaller segments would make the fetcher's
    /// per-article overhead dominate and flatter the daemon.
    article_size: usize = 768 << 10,
    recovery_slices: usize = 0,
    missing_fraction: f64 = 0,
    max_connections: usize = 0,
    repeat: usize = 1,
    stats_interval_s: u64 = 5,
    seed: u64 = 0,
};

pub fn main(init: std.process.Init.Minimal) !u8 {
    // Text on stdout, which is what `docker logs` on a load run is read
    // with. The generator has no config file to raise the level from, so
    // debug is off and the interval report carries everything.
    log.initDefault(.info, .text);

    // No leak checking and no free-list bookkeeping: this process
    // allocates a corpus once and then never frees anything until it
    // exits, so a general-purpose allocator would be pure overhead in a
    // binary whose whole job is to not be the bottleneck.
    const gpa = std.heap.page_allocator;

    var cfg: Config = .{};
    var args = init.args.iterate();
    _ = args.next(); // argv[0]
    if (!parseArgs(&cfg, &args)) {
        try sys.writeAll(sys.stderr_fd, usage);
        return 2;
    }

    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: testserver.Server = undefined;
    const port = try server.start(gpa, &loop, .{
        .host = cfg.host,
        .port = cfg.port,
        .missing_fraction = cfg.missing_fraction,
        .max_connections = cfg.max_connections,
    });
    defer server.deinit();

    var total_bytes: u64 = 0;
    for (0..cfg.repeat) |i| {
        total_bytes += try buildRelease(gpa, &server, cfg, i);
    }

    log.info("loadgen listening", &.{
        log.str("host", cfg.host),
        log.uint("port", port),
        log.uint("releases", cfg.repeat),
        log.uint("release_bytes", total_bytes),
        log.uint("articles", server.articleCount()),
        log.float("missing_fraction", cfg.missing_fraction),
        log.uint("max_connections", cfg.max_connections),
    });

    var stats: Stats = .{
        .loop = &loop,
        .server = &server,
        .interval_ns = cfg.stats_interval_s * std.time.ns_per_s,
        .last_ns = sys.monotonicNanos(),
    };
    try stats.arm();

    // Runs until killed. The listener and the stats timer both stay
    // registered, so `run` never decides there is nothing left to do.
    try loop.run();
    return 0;
}

/// Generates release `index`, writes its NZB and registers every article
/// with `server`. Returns the release's size in bytes.
///
/// The fixture is released as soon as the server has copied the corpus:
/// the generated files and their yEnc bodies are the same bytes twice
/// over, and holding both doubles the resident set for nothing.
fn buildRelease(
    gpa: std.mem.Allocator,
    server: *testserver.Server,
    cfg: Config,
    index: usize,
) !u64 {
    // Distinct names per release, so the message-ids never collide and
    // two concurrent jobs really do pull distinct data rather than
    // sharing a corpus.
    var name_buf: [64]u8 = undefined;
    const name = try std.fmt.bufPrint(&name_buf, "loadgen.release{d:0>3}", .{index + 1});

    var fx = try fixture.generate(gpa, .{
        .name = name,
        .file_count = cfg.files,
        .file_size = cfg.file_size,
        .article_size = cfg.article_size,
        .recovery_slices = cfg.recovery_slices,
        .seed = cfg.seed +% index,
    });
    defer fx.deinit();

    var bytes: u64 = 0;
    for (fx.files) |f| bytes += f.bytes.len;

    // The path buffer lives here rather than in `writeNzb`, which would
    // otherwise be returning a slice of its own dead frame.
    var path_buf: [sys.path_max]u8 = undefined;
    const path = if (cfg.repeat == 1)
        cfg.nzb_path
    else
        try std.fmt.bufPrint(&path_buf, "{s}.{d}", .{ cfg.nzb_path, index + 1 });

    try writeNzb(fx.nzb, path);
    for (fx.articles) |art| try server.addArticle(art.message_id, art.body);

    log.info("release generated", &.{
        log.str("name", name),
        log.str("nzb", path),
        log.uint("files", fx.files.len),
        log.uint("bytes", bytes),
        log.uint("articles", fx.articles.len),
    });
    return bytes;
}

/// Truncating rather than appending: a rerun with a different shape must
/// leave the NZB describing the corpus actually being served, not the
/// previous one with the new one glued on.
fn writeNzb(nzb: []const u8, path: []const u8) !void {
    var buf: [sys.path_max]u8 = undefined;
    const z = try sys.pathZ(&buf, path);
    const fd = try sys.open(z, .{ .mode = .write_only, .create = true, .truncate = true });
    defer sys.close(fd);
    try sys.writeAll(fd, nzb);
}

/// Periodic report on what the harness is actually offering.
///
/// Totals plus a rate over the last interval: the totals say whether the
/// client has finished, the rate is what gets compared against the
/// daemon's own throughput accounting when the two disagree.
const Stats = struct {
    timer: reactor.Timer = .{ .callback = onTick },
    loop: *reactor.Loop,
    server: *testserver.Server,
    interval_ns: u64,
    last_ns: u64,
    last_served: usize = 0,
    last_bytes: u64 = 0,

    fn arm(self: *Stats) !void {
        try self.loop.addTimer(&self.timer, self.interval_ns);
    }

    fn onTick(timer: *reactor.Timer) void {
        const self: *Stats = @fieldParentPtr("timer", timer);
        const now = sys.monotonicNanos();
        const elapsed = now - self.last_ns;
        const served = self.server.served;
        const bytes = self.server.bytes_written;

        // Guard the division rather than the clock: a monotonic clock
        // that does not advance between two ticks is impossible on the
        // platforms we run on, but a zero here would be a crash.
        const rate: f64 = if (elapsed == 0) 0 else @as(f64, @floatFromInt(bytes - self.last_bytes)) *
            @as(f64, std.time.ns_per_s) / @as(f64, @floatFromInt(elapsed));

        log.info("load offered", &.{
            log.uint("served", served),
            log.uint("bytes", bytes),
            log.uint("served_delta", served - self.last_served),
            log.float("bytes_per_sec", rate),
            log.uint("connections", self.server.open),
            log.uint("accepted", self.server.accepted),
            log.uint("refused", self.server.refused),
            log.uint("missed", self.server.missed),
        });

        self.last_ns = now;
        self.last_served = served;
        self.last_bytes = bytes;
        // Re-armed from the callback: a repeating timer in the reactor
        // would have to decide what to do about a callback that overruns
        // its period, and this one cannot.
        self.arm() catch |err| log.err("stats timer lost", &.{log.errv("err", err)});
    }
};

// ---------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------

/// Fills `cfg` from argv. False means the caller should print usage and
/// exit non-zero; the reason has already been logged.
///
/// `std.process.argsAlloc` is gone in 0.16, and the iterator walks the
/// argv the kernel handed us in place, so flag parsing needs no
/// allocator at all.
fn parseArgs(cfg: *Config, args: *std.process.Args.Iterator) bool {
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return false;

        const value = args.next() orelse {
            log.err("flag needs a value", &.{log.str("flag", arg)});
            return false;
        };

        if (std.mem.eql(u8, arg, "--listen")) {
            const colon = std.mem.lastIndexOfScalar(u8, value, ':') orelse return badValue(arg, value);
            cfg.host = value[0..colon];
            cfg.port = std.fmt.parseInt(u16, value[colon + 1 ..], 10) catch return badValue(arg, value);
            if (cfg.host.len == 0) cfg.host = "0.0.0.0";
        } else if (std.mem.eql(u8, arg, "--nzb")) {
            cfg.nzb_path = value;
        } else if (std.mem.eql(u8, arg, "--files")) {
            cfg.files = parseCount(value) orelse return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--file-size")) {
            cfg.file_size = parseSize(value) orelse return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--article-size")) {
            cfg.article_size = parseSize(value) orelse return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--recovery-slices")) {
            cfg.recovery_slices = parseCount(value) orelse return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--missing-fraction")) {
            cfg.missing_fraction = std.fmt.parseFloat(f64, value) catch return badValue(arg, value);
            if (!(cfg.missing_fraction >= 0 and cfg.missing_fraction <= 1)) return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--max-connections")) {
            cfg.max_connections = parseCount(value) orelse return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--repeat")) {
            cfg.repeat = parseCount(value) orelse return badValue(arg, value);
            if (cfg.repeat == 0) return badValue(arg, value);
        } else if (std.mem.eql(u8, arg, "--stats-interval")) {
            const s = parseCount(value) orelse return badValue(arg, value);
            if (s == 0) return badValue(arg, value);
            cfg.stats_interval_s = s;
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = std.fmt.parseInt(u64, value, 10) catch return badValue(arg, value);
        } else {
            log.err("unknown flag", &.{log.str("flag", arg)});
            return false;
        }
    }
    return true;
}

fn badValue(flag: []const u8, value: []const u8) bool {
    log.err("bad flag value", &.{ log.str("flag", flag), log.str("value", value) });
    return false;
}

fn parseCount(s: []const u8) ?usize {
    return std.fmt.parseInt(usize, s, 10) catch null;
}

/// A plain byte count, optionally with a single binary suffix. Decimal
/// suffixes are deliberately absent: everything a release is measured in
/// — segment sizes, slice sizes, buffer sizes — is a power of two, and a
/// `--file-size 4M` that meant 4,000,000 would be a trap.
fn parseSize(s: []const u8) ?usize {
    if (s.len == 0) return null;
    var digits = s;
    var mult: usize = 1;
    switch (s[s.len - 1]) {
        'k', 'K' => mult = 1 << 10,
        'm', 'M' => mult = 1 << 20,
        'g', 'G' => mult = 1 << 30,
        else => {},
    }
    if (mult != 1) digits = s[0 .. s.len - 1];
    const n = std.fmt.parseInt(usize, digits, 10) catch return null;
    return std.math.mul(usize, n, mult) catch null;
}
