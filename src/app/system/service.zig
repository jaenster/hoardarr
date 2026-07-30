//! Process-wide status for the System page, plus the two periodic jobs
//! that hang off it: publishing throughput to SSE subscribers and
//! persisting a downsampled speed history.
//!
//! # Counts, not aggregates
//!
//! `status` reports queue depth as two integers from `countActive` /
//! `countAll`. The Go version loaded every job with every file and every
//! segment to produce the same two numbers, and under the frontend's 2s
//! poll that single endpoint was ~38% of process CPU. The counts are the
//! whole of what the response exposes.
//!
//! # Deadlines, not tickers
//!
//! Go ran three goroutines with three tickers. Here each periodic job is
//! a `dueAt` the reactor arms one timer on, and the work is a plain
//! method. `historyDueAt` aligns to the next wall-clock minute so stored
//! bucket timestamps are predictable and the API can stitch the
//! in-memory window onto the persisted rows without overlap checks.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const dl_ports = @import("../download/ports.zig");
const throughput = @import("throughput.zig");
const dserver = @import("../../domain/server.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const ServerId = dserver.ServerId;
pub const Throughput = throughput.Throughput;
pub const Sample = throughput.Sample;

/// Default retention for the persisted speed history.
pub const default_retention_days: i64 = 30;

/// One server's live pool numbers.
pub const PoolStatus = struct {
    server_id: ServerId,
    /// Borrowed from the pool snapshot.
    server_name: []const u8 = "",
    max_conns: u16 = 0,
    in_use: u16 = 0,
    idle: u16 = 0,
    enabled: bool = true,
    backup: bool = false,
    metered: bool = false,
    quota_bytes: i64 = 0,
    used_bytes: i64 = 0,
};

/// Live pool occupancy. Separate from `download/ports.PoolInfo` because
/// that one is about *dispatch order* and this one is about *display*:
/// the in-use and idle counts have no business in the fetcher's
/// decisions, and the priority has no business on the System page.
pub const PoolStats = struct {
    ctx: *anyopaque,
    snapshotFn: *const fn (ctx: *anyopaque, a: Allocator) Allocator.Error![]PoolStatus,

    pub fn snapshot(self: PoolStats, a: Allocator) Allocator.Error![]PoolStatus {
        return self.snapshotFn(self.ctx, a);
    }
};

/// One persisted throughput row: the 60-second average at a minute
/// boundary.
pub const SpeedSample = struct {
    /// Start of the minute this row summarises.
    at: Timestamp,
    bytes_per_sec: i64,
};

pub const HistoryError = error{Backend} || Allocator.Error;

/// Long-term throughput persistence. Optional everywhere: without it the
/// speed-history endpoint serves the in-memory window alone.
pub const SpeedHistory = struct {
    ctx: *anyopaque,
    appendFn: *const fn (ctx: *anyopaque, s: SpeedSample) HistoryError!void,
    /// Drops rows older than `cutoff`; returns how many.
    purgeFn: *const fn (ctx: *anyopaque, cutoff: Timestamp) HistoryError!usize,
    /// Persists a new all-time peak. Bumps only.
    savePeakFn: *const fn (ctx: *anyopaque, v: i64) HistoryError!void,

    pub fn append(self: SpeedHistory, s: SpeedSample) HistoryError!void {
        return self.appendFn(self.ctx, s);
    }

    pub fn purge(self: SpeedHistory, cutoff: Timestamp) HistoryError!usize {
        return self.purgeFn(self.ctx, cutoff);
    }

    pub fn savePeak(self: SpeedHistory, v: i64) HistoryError!void {
        return self.savePeakFn(self.ctx, v);
    }
};

/// The System page's snapshot.
pub const Status = struct {
    service: []const u8 = "hoardarr",
    version: []const u8 = "",
    commit: []const u8 = "",
    build_date: []const u8 = "",
    os: []const u8 = @tagName(@import("builtin").os.tag),
    arch: []const u8 = @tagName(@import("builtin").cpu.arch),
    database_type: []const u8 = "sqlite",
    migration_version: u32 = 0,
    started_at: Timestamp = 0,
    uptime_ms: Millis = 0,
    queue_active: u32 = 0,
    queue_total: u32 = 0,
    /// Borrowed from the allocator passed to `status`.
    pools: []const PoolStatus = &.{},
};

pub const Error = dl_ports.RepoError || HistoryError;

pub const Service = struct {
    gpa: Allocator,
    jobs: dl_ports.JobStore,
    pools: PoolStats,
    clock: app_ports.Clock,
    throughput: *Throughput,
    logger: *log.Logger = &log.default,

    version: []const u8 = "",
    commit: []const u8 = "",
    build_date: []const u8 = "",
    migration_version: u32 = 0,
    started_at: Timestamp = 0,

    history: ?SpeedHistory = null,
    retention_days: i64 = default_retention_days,

    /// When the next history row is owed. 0 means "not scheduled yet";
    /// `scheduleHistory` aligns it to the next minute boundary.
    history_due_at: Timestamp = 0,
    /// When the next retention purge is owed.
    purge_due_at: Timestamp = 0,
    /// The peak already written, so an unchanged value is not rewritten
    /// every minute.
    persisted_peak: i64 = 0,

    /// A fresh snapshot. Nothing is cached: the fan-out is two counts and
    /// a pool walk, so a 1Hz poll costs nothing worth caching.
    pub fn status(self: *Service, a: Allocator) Error!Status {
        const now = self.clock.now();
        return .{
            .version = self.version,
            .commit = self.commit,
            .build_date = self.build_date,
            .migration_version = self.migration_version,
            .started_at = self.started_at,
            .uptime_ms = now - self.started_at,
            .queue_active = try self.jobs.countActive(null),
            .queue_total = try self.jobs.countAll(null),
            .pools = try self.pools.snapshot(a),
        };
    }

    /// Records bytes against the throughput window. Wired to the byte
    /// accounter's observer, which is how the fetcher feeds this without
    /// knowing the system context exists.
    pub fn observeBytes(self: *Service, n: i64) void {
        self.throughput.add(self.clock.now(), n);
    }

    /// An `Accounter.Observer` view of `observeBytes`.
    pub fn observer(self: *Service) ByteObserver {
        return .{ .ctx = @ptrCast(self), .onBytesFn = &onBytes };
    }

    /// Structurally identical to `download/byte_accounter.Accounter.Observer`.
    /// Declared here too so this module does not depend on the download
    /// context for one function-pointer pair.
    pub const ByteObserver = struct {
        ctx: *anyopaque,
        onBytesFn: *const fn (ctx: *anyopaque, n: i64) void,

        pub fn onBytes(self: ByteObserver, n: i64) void {
            self.onBytesFn(self.ctx, n);
        }
    };

    fn onBytes(ctx: *anyopaque, n: i64) void {
        const self: *Service = @ptrCast(@alignCast(ctx));
        self.observeBytes(n);
    }

    /// The current throughput snapshot, for the SSE payload and the REST
    /// endpoint.
    pub fn sample(self: *Service, buf: []i64) Sample {
        return self.throughput.sample(buf, self.clock.now());
    }

    // ---- the periodic half ------------------------------------------

    /// Arms the history schedule against the next wall-clock minute.
    ///
    /// Aligning matters: stored `bucket_at` values are then predictable
    /// minute boundaries, so the API can concatenate the in-memory window
    /// and the persisted rows without checking for overlap.
    pub fn scheduleHistory(self: *Service, now: Timestamp) void {
        self.history_due_at = nextMinuteBoundary(now);
        self.purge_due_at = now;
    }

    pub fn historyDueAt(self: *const Service) Timestamp {
        return self.history_due_at;
    }

    pub fn purgeDueAt(self: *const Service) Timestamp {
        return self.purge_due_at;
    }

    /// Writes one history row covering the minute that just ended, and
    /// persists the all-time peak when it has moved.
    ///
    /// Safe to call whether or not it is due; `history_due_at` advances
    /// by exactly one minute so a late tick does not skip a bucket.
    pub fn flushHistory(self: *Service, now: Timestamp) Error!void {
        self.history_due_at = nextMinuteBoundary(now);
        const h = self.history orelse return;

        // The bucket is the minute we have just left, and `avg60s` is
        // precisely that minute's average.
        const bucket = truncateToMinute(now) - std.time.ms_per_min;
        var buf: [throughput.long_avg_window]i64 = @splat(0);
        const snap = self.throughput.sampleRange(&buf, now, throughput.long_avg_window);
        h.append(.{ .at = bucket, .bytes_per_sec = snap.avg60s_bytes_per_sec }) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory;
            self.logger.warn("append speed history", &.{log.errv("err", e)});
        };

        const peak = self.throughput.allTimePeak();
        if (peak > self.persisted_peak) {
            h.savePeak(peak) catch |e| {
                if (e == error.OutOfMemory) return error.OutOfMemory;
                self.logger.warn("persist all-time peak", &.{log.errv("err", e)});
                return;
            };
            self.persisted_peak = peak;
        }
    }

    /// Drops history rows past the retention window. Runs once at startup
    /// so retention is enforced even on an instance that idles for weeks.
    pub fn purgeHistory(self: *Service, now: Timestamp) Error!usize {
        self.purge_due_at = now + std.time.ms_per_hour;
        const h = self.history orelse return 0;
        const cutoff = now - self.retention_days * std.time.ms_per_day;
        const n = h.purge(cutoff) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory;
            self.logger.warn("purge speed history", &.{log.errv("err", e)});
            return 0;
        };
        if (n > 0) {
            self.logger.info("speed history purged", &.{
                log.uint("rows", n),
                log.int("before", cutoff),
            });
        }
        return n;
    }
};

fn truncateToMinute(ms: Timestamp) Timestamp {
    return @divFloor(ms, std.time.ms_per_min) * std.time.ms_per_min;
}

fn nextMinuteBoundary(ms: Timestamp) Timestamp {
    return truncateToMinute(ms) + std.time.ms_per_min;
}

// =====================================================================
// Test doubles
// =====================================================================

pub const FakePools = struct {
    rows: []const PoolStatus = &.{},
    calls: usize = 0,

    pub fn stats(self: *FakePools) PoolStats {
        return .{ .ctx = @ptrCast(self), .snapshotFn = &snapshot };
    }

    fn snapshot(ctx: *anyopaque, a: Allocator) Allocator.Error![]PoolStatus {
        const self: *FakePools = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        return a.dupe(PoolStatus, self.rows);
    }
};

pub const FakeHistory = struct {
    gpa: Allocator,
    rows: std.ArrayList(SpeedSample) = .empty,
    peaks: std.ArrayList(i64) = .empty,
    purges: usize = 0,
    last_cutoff: Timestamp = 0,
    purge_returns: usize = 0,
    fail_next: ?HistoryError = null,

    pub fn init(gpa: Allocator) FakeHistory {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeHistory) void {
        self.rows.deinit(self.gpa);
        self.peaks.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn history(self: *FakeHistory) SpeedHistory {
        return .{
            .ctx = @ptrCast(self),
            .appendFn = &append,
            .purgeFn = &purge,
            .savePeakFn = &savePeak,
        };
    }

    fn append(ctx: *anyopaque, s: SpeedSample) HistoryError!void {
        const self: *FakeHistory = @ptrCast(@alignCast(ctx));
        if (self.fail_next) |e| {
            self.fail_next = null;
            return e;
        }
        try self.rows.append(self.gpa, s);
    }

    fn purge(ctx: *anyopaque, cutoff: Timestamp) HistoryError!usize {
        const self: *FakeHistory = @ptrCast(@alignCast(ctx));
        self.purges += 1;
        self.last_cutoff = cutoff;
        return self.purge_returns;
    }

    fn savePeak(ctx: *anyopaque, v: i64) HistoryError!void {
        const self: *FakeHistory = @ptrCast(@alignCast(ctx));
        try self.peaks.append(self.gpa, v);
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const job_mod = @import("../../domain/download/job.zig");
const ddevents = @import("../../domain/download/events.zig");

/// A whole minute boundary (1700000040 seconds is 60 × 28333334), which
/// keeps the alignment arithmetic legible.
const base: Timestamp = 1_700_000_040_000;

const Harness = struct {
    jobs: dl_ports.FakeJobStore = undefined,
    pools: FakePools = .{},
    hist: FakeHistory = undefined,
    clock: app_ports.FakeClock = .{ .t = base },
    tp: Throughput = .{},
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.jobs = dl_ports.FakeJobStore.init(testing.allocator);
        self.hist = FakeHistory.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .jobs = self.jobs.store(),
            .pools = self.pools.stats(),
            .clock = self.clock.clock(),
            .throughput = &self.tp,
            .logger = &self.logger,
            .version = "1.2.3",
            .started_at = base - 60_000,
            .migration_version = 7,
            .history = self.hist.history(),
        };
    }

    fn deinit(self: *Harness) void {
        self.hist.deinit();
        self.jobs.deinit();
    }

    fn seedJob(self: *Harness, hash: []const u8, terminal: bool) !void {
        const j = try dl_ports.testJob(testing.allocator, hash, "m@h", 1);
        if (terminal) _ = try j.markCompleted(1);
        const evts = try j.pullEvents();
        ddevents.deinitAll(testing.allocator, evts);
        try self.jobs.insert(j);
    }
};

test "status reports counts and uptime without loading aggregates" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.seedJob("a", false);
    try h.seedJob("b", false);
    try h.seedJob("c", true);
    h.pools.rows = &.{.{ .server_id = 1, .server_name = "primary", .max_conns = 8, .in_use = 3, .idle = 5 }};

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const s = try h.svc.status(arena.allocator());

    try testing.expectEqual(@as(u32, 2), s.queue_active);
    try testing.expectEqual(@as(u32, 3), s.queue_total);
    try testing.expectEqual(@as(Millis, 60_000), s.uptime_ms);
    try testing.expectEqualStrings("1.2.3", s.version);
    try testing.expectEqual(@as(u32, 7), s.migration_version);
    try testing.expectEqual(@as(usize, 1), s.pools.len);
    try testing.expectEqualStrings("primary", s.pools[0].server_name);
    // The counts came from COUNT queries, not from hydrated jobs.
    try testing.expectEqual(@as(usize, 0), h.jobs.loads);
}

test "an empty queue and no pools still answers" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const s = try h.svc.status(arena.allocator());
    try testing.expectEqual(@as(u32, 0), s.queue_active);
    try testing.expectEqual(@as(usize, 0), s.pools.len);
    try testing.expectEqualStrings("sqlite", s.database_type);
}

test "the byte observer feeds the throughput window" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const obs = h.svc.observer();
    obs.onBytes(500);
    obs.onBytes(250);

    var buf: [throughput.default_sample_seconds]i64 = @splat(0);
    const s = h.svc.sample(&buf);
    try testing.expectEqual(@as(i64, 750), s.current_bytes_per_sec);
    try testing.expectEqual(@as(i64, 750), h.tp.allTimePeak());
}

test "the history schedule aligns to the next minute boundary" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    // 30 seconds past a boundary.
    h.svc.scheduleHistory(base + 30_000);
    try testing.expectEqual(base + std.time.ms_per_min, h.svc.historyDueAt());
    // Exactly on a boundary still schedules the *next* one, so a bucket
    // is never written twice.
    h.svc.scheduleHistory(base);
    try testing.expectEqual(base + std.time.ms_per_min, h.svc.historyDueAt());
}

test "a flush writes the minute that just ended and advances the deadline" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    // A steady 100 bytes/sec across the previous minute.
    for (0..60) |i| h.tp.add(base + @as(i64, @intCast(i)) * 1000, 100);
    const now = base + std.time.ms_per_min;

    try h.svc.flushHistory(now);
    try testing.expectEqual(@as(usize, 1), h.hist.rows.items.len);
    // The bucket is the minute we just left, not the one we are in.
    try testing.expectEqual(base, h.hist.rows.items[0].at);
    // 98, not 100: the 60-second average excludes the second still
    // accruing, so a steady 100 B/s reads as 59/60ths of itself at the
    // boundary. Go stored the same number from the same window; the
    // graph is a trend line, not an accounting record.
    try testing.expectEqual(@as(i64, 98), h.hist.rows.items[0].bytes_per_sec);
    try testing.expectEqual(now + std.time.ms_per_min, h.svc.historyDueAt());
}

test "the all-time peak is written once per new high, not once per minute" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.tp.add(base, 5000);
    try h.svc.flushHistory(base + std.time.ms_per_min);
    try testing.expectEqualSlices(i64, &.{5000}, h.hist.peaks.items);

    // Nothing new: no second write.
    try h.svc.flushHistory(base + 2 * std.time.ms_per_min);
    try testing.expectEqualSlices(i64, &.{5000}, h.hist.peaks.items);

    // A new record does get persisted.
    h.tp.add(base + 2 * std.time.ms_per_min, 9000);
    try h.svc.flushHistory(base + 3 * std.time.ms_per_min);
    try testing.expectEqualSlices(i64, &.{ 5000, 9000 }, h.hist.peaks.items);
}

test "a history backend failure is logged, not propagated" {
    // Losing a throughput graph row must not take the daemon down.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.hist.fail_next = error.Backend;
    try h.svc.flushHistory(base + std.time.ms_per_min);
    try testing.expectEqual(@as(usize, 0), h.hist.rows.items.len);
    // The deadline still moved, so the next minute is attempted.
    try testing.expectEqual(base + 2 * std.time.ms_per_min, h.svc.historyDueAt());
}

test "purge uses the retention window and reschedules hourly" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.hist.purge_returns = 42;
    const now = base + 5 * std.time.ms_per_day;

    try testing.expectEqual(@as(usize, 42), try h.svc.purgeHistory(now));
    try testing.expectEqual(now - default_retention_days * std.time.ms_per_day, h.hist.last_cutoff);
    try testing.expectEqual(now + std.time.ms_per_hour, h.svc.purgeDueAt());

    // A shorter retention moves the cutoff forward.
    h.svc.retention_days = 1;
    _ = try h.svc.purgeHistory(now);
    try testing.expectEqual(now - std.time.ms_per_day, h.hist.last_cutoff);
}

test "without a history store the periodic jobs are inert" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.svc.history = null;
    try h.svc.flushHistory(base + std.time.ms_per_min);
    try testing.expectEqual(@as(usize, 0), try h.svc.purgeHistory(base));
    try testing.expectEqual(@as(usize, 0), h.hist.rows.items.len);
    try testing.expectEqual(@as(usize, 0), h.hist.purges);
    // The deadlines still advance, so a store attached later starts
    // clean rather than immediately overdue by hours.
    try testing.expectEqual(base + 2 * std.time.ms_per_min, h.svc.historyDueAt());
}
