//! The built-in commands an operator can trigger from the UI.
//!
//! Each is a `Handler` over a narrow port, so a handler's test needs the
//! one thing it actually touches and nothing else.
//!
//! # No cosmetic sleeps
//!
//! Go's `PingHandler` slept 150ms and `HealthRecheckHandler` slept 200ms,
//! both purely so the operator would see the "running" state instead of a
//! sub-frame flicker. A delay in the daemon to make a spinner visible is
//! the UI's problem solved in the wrong process — the frontend can hold a
//! transition for as long as it likes. The sleeps are not ported, and
//! that is the only behavioural difference in this file.

const std = @import("std");
const log = @import("../../core/log.zig");
const service = @import("service.zig");
const queue = @import("../download/queue.zig");
const dserver = @import("../../domain/server.zig");

const Allocator = std.mem.Allocator;
const Handler = service.Handler;

pub const ServerId = dserver.ServerId;

/// Widest reason string a handler produces. Handlers own their buffer so
/// the returned slice outlives the call.
pub const reason_buf_len = 160;

// ---------------------------------------------------------------------
// ping
// ---------------------------------------------------------------------

/// The smoke test: proves the worker is alive and the Commands UI renders
/// a completion.
pub const Ping = struct {
    calls: usize = 0,

    pub fn handler(self: *Ping) Handler {
        return .{ .ctx = @ptrCast(self), .runFn = &run };
    }

    fn run(ctx: *anyopaque, _: []const u8) ?[]const u8 {
        const self: *Ping = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        return null;
    }
};

// ---------------------------------------------------------------------
// retry failed segments
// ---------------------------------------------------------------------

/// Flips a job's failed and missing segments back to pending, and revives
/// the job if it had already terminated.
///
/// "Kick the wheel" semantics: the release was temporarily unavailable
/// and the operator wants a fresh attempt without re-uploading the NZB.
///
/// The body is `{"job_id": N}`.
pub const RetryFailedSegments = struct {
    queue: *queue.Service,
    logger: *log.Logger = &log.default,
    reason: [reason_buf_len]u8 = undefined,
    last_reset: usize = 0,

    pub fn handler(self: *RetryFailedSegments) Handler {
        return .{ .ctx = @ptrCast(self), .runFn = &run };
    }

    fn run(ctx: *anyopaque, body: []const u8) ?[]const u8 {
        const self: *RetryFailedSegments = @ptrCast(@alignCast(ctx));
        const job_id = parseJobId(body) orelse return "job_id required";
        self.last_reset = self.queue.retryFailedSegments(job_id) catch |e| {
            return std.fmt.bufPrint(&self.reason, "load job {d}: {t}", .{ job_id, e }) catch
                "retry failed";
        };
        return null;
    }
};

/// Pulls `job_id` out of the command body without a full JSON decode.
///
/// The body is written by our own REST layer and is a one-field object;
/// scanning for the key is cheaper than a parse and cannot fail in a way
/// that matters — anything unrecognisable yields null, which the handler
/// reports as "job_id required".
pub fn parseJobId(body: []const u8) ?i64 {
    const key = "\"job_id\"";
    const at = std.mem.indexOf(u8, body, key) orelse return null;
    var i = at + key.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':')) i += 1;
    const start = i;
    while (i < body.len and (std.ascii.isDigit(body[i]) or (i == start and body[i] == '-'))) i += 1;
    if (i == start) return null;
    const n = std.fmt.parseInt(i64, body[start..i], 10) catch return null;
    if (n <= 0) return null;
    return n;
}

// ---------------------------------------------------------------------
// pause / resume all
// ---------------------------------------------------------------------

pub const Bulk = struct {
    pub const Kind = enum { pause, resume_ };

    queue: *queue.Service,
    kind: Kind,
    logger: *log.Logger = &log.default,
    reason: [reason_buf_len]u8 = undefined,
    last_count: usize = 0,

    pub fn handler(self: *Bulk) Handler {
        return .{ .ctx = @ptrCast(self), .runFn = &run };
    }

    fn run(ctx: *anyopaque, _: []const u8) ?[]const u8 {
        const self: *Bulk = @ptrCast(@alignCast(ctx));
        const n = switch (self.kind) {
            .pause => self.queue.pauseAll(),
            .resume_ => self.queue.resumeAll(),
        } catch |e| {
            return std.fmt.bufPrint(&self.reason, "list active: {t}", .{e}) catch "bulk failed";
        };
        self.last_count = n;
        self.logger.info("command: bulk queue action", &.{
            log.str("kind", @tagName(self.kind)),
            log.uint("count", n),
        });
        return null;
    }
};

// ---------------------------------------------------------------------
// reprobe servers
// ---------------------------------------------------------------------

/// One server's identity and whether the operator has it enabled.
pub const ServerRow = struct {
    id: ServerId,
    /// Borrowed from the snapshot.
    name: []const u8,
    enabled: bool,
};

/// The slice of the server repository the reprobe handler needs.
pub const ServerLister = struct {
    ctx: *anyopaque,
    listFn: *const fn (ctx: *anyopaque, a: Allocator) Allocator.Error![]ServerRow,

    pub fn list(self: ServerLister, a: Allocator) Allocator.Error![]ServerRow {
        return self.listFn(self.ctx, a);
    }
};

/// Runs an NNTP handshake against one server.
pub const Prober = struct {
    ctx: *anyopaque,
    /// Null on success, or a reason. Never blocks the reactor for long:
    /// the real implementation is a bounded connect with a deadline.
    probeFn: *const fn (ctx: *anyopaque, id: ServerId) ?[]const u8,

    pub fn probe(self: Prober, id: ServerId) ?[]const u8 {
        return self.probeFn(self.ctx, id);
    }
};

/// Handshakes every enabled server and reports the failures.
///
/// Go fanned the probes out across goroutines behind a mutex. Sequential
/// here: an operator has a handful of servers, each probe is a bounded
/// connect, and the ordering makes the log readable.
pub const ReprobeServers = struct {
    gpa: Allocator,
    servers: ServerLister,
    prober: Prober,
    logger: *log.Logger = &log.default,
    reason: [reason_buf_len]u8 = undefined,
    last_probed: usize = 0,
    last_failed: usize = 0,

    pub fn handler(self: *ReprobeServers) Handler {
        return .{ .ctx = @ptrCast(self), .runFn = &run };
    }

    fn run(ctx: *anyopaque, _: []const u8) ?[]const u8 {
        const self: *ReprobeServers = @ptrCast(@alignCast(ctx));
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const rows = self.servers.list(a) catch |e| {
            return std.fmt.bufPrint(&self.reason, "list servers: {t}", .{e}) catch
                "list servers failed";
        };

        var probed: usize = 0;
        var failed: usize = 0;
        var first_failure: []const u8 = "";
        for (rows) |row| {
            if (!row.enabled) continue;
            probed += 1;
            if (self.prober.probe(row.id)) |err| {
                failed += 1;
                if (first_failure.len == 0) first_failure = row.name;
                self.logger.warn("reprobe: failed", &.{
                    log.str("server", row.name),
                    log.str("err", err),
                });
            } else {
                self.logger.info("reprobe: ok", &.{log.str("server", row.name)});
            }
        }
        self.last_probed = probed;
        self.last_failed = failed;

        if (failed > 0) {
            return std.fmt.bufPrint(&self.reason, "{d}/{d} servers failed, first: {s}", .{
                failed, probed, first_failure,
            }) catch "servers failed";
        }
        self.logger.info("reprobe: all servers ok", &.{log.uint("count", probed)});
        return null;
    }
};

// =====================================================================
// Test doubles
// =====================================================================

pub const FakeServers = struct {
    rows: []const ServerRow = &.{},

    pub fn lister(self: *FakeServers) ServerLister {
        return .{ .ctx = @ptrCast(self), .listFn = &list };
    }

    fn list(ctx: *anyopaque, a: Allocator) Allocator.Error![]ServerRow {
        const self: *FakeServers = @ptrCast(@alignCast(ctx));
        return a.dupe(ServerRow, self.rows);
    }
};

pub const FakeProber = struct {
    /// Ids that fail, with the reason each reports.
    failures: []const ServerId = &.{},
    probed: [8]ServerId = @splat(0),
    n_probed: usize = 0,

    pub fn prober(self: *FakeProber) Prober {
        return .{ .ctx = @ptrCast(self), .probeFn = &probe };
    }

    pub fn order(self: *const FakeProber) []const ServerId {
        return self.probed[0..self.n_probed];
    }

    fn probe(ctx: *anyopaque, id: ServerId) ?[]const u8 {
        const self: *FakeProber = @ptrCast(@alignCast(ctx));
        if (self.n_probed < self.probed.len) {
            self.probed[self.n_probed] = id;
            self.n_probed += 1;
        }
        for (self.failures) |f| {
            if (f == id) return "connection refused";
        }
        return null;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const app_ports = @import("../ports.zig");
const dl_ports = @import("../download/ports.zig");
const job_mod = @import("../../domain/download/job.zig");
const ddevents = @import("../../domain/download/events.zig");
const Job = job_mod.Job;

const QueueHarness = struct {
    store: dl_ports.FakeJobStore = undefined,
    fs: app_ports.FakeFs = undefined,
    sink: app_ports.FakeSink(ddevents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 1_000 },
    logger: log.Logger = .{},
    q: queue.Service = undefined,

    fn init(self: *QueueHarness) void {
        self.* = .{};
        self.store = dl_ports.FakeJobStore.init(testing.allocator);
        self.fs = app_ports.FakeFs.init(testing.allocator);
        self.q = .{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .sink = self.sink.sink(),
            .txm = self.ftx.manager(),
            .clock = self.clock.clock(),
            .fs = self.fs.filesystem(),
            .logger = &self.logger,
        };
    }

    fn deinit(self: *QueueHarness) void {
        self.fs.deinit();
        self.store.deinit();
    }

    fn seed(self: *QueueHarness, name: []const u8) !*Job {
        const j = try dl_ports.testJob(testing.allocator, name, "m@h", 10);
        try self.store.insert(j);
        const evts = try j.pullEvents();
        ddevents.deinitAll(testing.allocator, evts);
        return j;
    }
};

test "ping succeeds without spending any time" {
    var p: Ping = .{};
    try testing.expectEqual(@as(?[]const u8, null), p.handler().run(""));
    try testing.expectEqual(@as(usize, 1), p.calls);
}

test "job_id is extracted from the command body" {
    try testing.expectEqual(@as(?i64, 42), parseJobId("{\"job_id\":42}"));
    try testing.expectEqual(@as(?i64, 7), parseJobId("{ \"job_id\" : 7 , \"x\":1 }"));
    // Anything unrecognisable is null, which the handler reports as a
    // missing job_id rather than guessing.
    try testing.expectEqual(@as(?i64, null), parseJobId(""));
    try testing.expectEqual(@as(?i64, null), parseJobId("{}"));
    try testing.expectEqual(@as(?i64, null), parseJobId("{\"job_id\":\"42\"}"));
    try testing.expectEqual(@as(?i64, null), parseJobId("{\"job_id\":0}"));
    try testing.expectEqual(@as(?i64, null), parseJobId("{\"job_id\":-5}"));
}

test "retry resets a job's failed segments" {
    var qh: QueueHarness = undefined;
    qh.init();
    defer qh.deinit();
    const j = try qh.seed("alpha");
    try j.markSegmentMissing(j.files[0].segments[0].id, 10);
    const evts = try j.pullEvents();
    ddevents.deinitAll(testing.allocator, evts);

    var h: RetryFailedSegments = .{ .queue = &qh.q, .logger = &qh.logger };
    var body_buf: [64]u8 = undefined;
    const body = try std.fmt.bufPrint(&body_buf, "{{\"job_id\":{d}}}", .{j.id});

    try testing.expectEqual(@as(?[]const u8, null), h.handler().run(body));
    try testing.expectEqual(@as(usize, 1), h.last_reset);
    try testing.expectEqual(dl_ports.SegmentState.pending, j.files[0].segments[0].state);
    try testing.expectEqual(job_mod.JobState.queued, j.state);
}

test "retry reports a missing job_id and an unknown job" {
    var qh: QueueHarness = undefined;
    qh.init();
    defer qh.deinit();
    var h: RetryFailedSegments = .{ .queue = &qh.q, .logger = &qh.logger };

    try testing.expectEqualStrings("job_id required", h.handler().run("{}").?);
    try testing.expectEqualStrings(
        "load job 999: JobNotFound",
        h.handler().run("{\"job_id\":999}").?,
    );
}

test "retry on a healthy job succeeds having done nothing" {
    var qh: QueueHarness = undefined;
    qh.init();
    defer qh.deinit();
    const j = try qh.seed("alpha");
    var h: RetryFailedSegments = .{ .queue = &qh.q, .logger = &qh.logger };
    var body_buf: [64]u8 = undefined;
    const body = try std.fmt.bufPrint(&body_buf, "{{\"job_id\":{d}}}", .{j.id});

    try testing.expectEqual(@as(?[]const u8, null), h.handler().run(body));
    try testing.expectEqual(@as(usize, 0), h.last_reset);
}

test "pause-all and resume-all act on the eligible jobs" {
    var qh: QueueHarness = undefined;
    qh.init();
    defer qh.deinit();
    const a = try qh.seed("a");
    const b = try qh.seed("b");
    _ = try b.markStarted(1);
    const evts = try b.pullEvents();
    ddevents.deinitAll(testing.allocator, evts);

    var pause: Bulk = .{ .queue = &qh.q, .kind = .pause, .logger = &qh.logger };
    try testing.expectEqual(@as(?[]const u8, null), pause.handler().run(""));
    try testing.expectEqual(@as(usize, 2), pause.last_count);
    try testing.expectEqual(job_mod.JobState.paused, a.state);
    try testing.expectEqual(job_mod.JobState.paused, b.state);

    var res: Bulk = .{ .queue = &qh.q, .kind = .resume_, .logger = &qh.logger };
    try testing.expectEqual(@as(?[]const u8, null), res.handler().run(""));
    try testing.expectEqual(@as(usize, 2), res.last_count);
    try testing.expectEqual(job_mod.JobState.queued, a.state);
    try testing.expectEqual(job_mod.JobState.downloading, b.state);
}

test "reprobe skips disabled servers and reports success" {
    var servers: FakeServers = .{ .rows = &.{
        .{ .id = 1, .name = "primary", .enabled = true },
        .{ .id = 2, .name = "backup", .enabled = false },
        .{ .id = 3, .name = "block", .enabled = true },
    } };
    var prober: FakeProber = .{};
    var logger: log.Logger = .{};
    var h: ReprobeServers = .{
        .gpa = testing.allocator,
        .servers = servers.lister(),
        .prober = prober.prober(),
        .logger = &logger,
    };

    try testing.expectEqual(@as(?[]const u8, null), h.handler().run(""));
    try testing.expectEqual(@as(usize, 2), h.last_probed);
    try testing.expectEqual(@as(usize, 0), h.last_failed);
    // The disabled one was never dialled.
    try testing.expectEqualSlices(ServerId, &.{ 1, 3 }, prober.order());
}

test "reprobe surfaces the failures in the command's error field" {
    var servers: FakeServers = .{ .rows = &.{
        .{ .id = 1, .name = "primary", .enabled = true },
        .{ .id = 2, .name = "block", .enabled = true },
    } };
    var prober: FakeProber = .{ .failures = &.{2} };
    var logger: log.Logger = .{};
    var h: ReprobeServers = .{
        .gpa = testing.allocator,
        .servers = servers.lister(),
        .prober = prober.prober(),
        .logger = &logger,
    };

    // Named, so the operator sees which server to look at without
    // opening the log file.
    try testing.expectEqualStrings("1/2 servers failed, first: block", h.handler().run("").?);
    try testing.expectEqual(@as(usize, 1), h.last_failed);
}

test "reprobe with no servers configured succeeds" {
    var servers: FakeServers = .{};
    var prober: FakeProber = .{};
    var logger: log.Logger = .{};
    var h: ReprobeServers = .{
        .gpa = testing.allocator,
        .servers = servers.lister(),
        .prober = prober.prober(),
        .logger = &logger,
    };
    try testing.expectEqual(@as(?[]const u8, null), h.handler().run(""));
    try testing.expectEqual(@as(usize, 0), h.last_probed);
}

test "the built-ins register with the command service and dispatch" {
    var qh: QueueHarness = undefined;
    qh.init();
    defer qh.deinit();
    var cmd_store = service.FakeStore.init(testing.allocator);
    defer cmd_store.deinit();
    var clock: app_ports.FakeClock = .{ .t = 100 };
    var logger: log.Logger = .{};
    var svc: service.Service = .{
        .gpa = testing.allocator,
        .store = cmd_store.store(),
        .clock = clock.clock(),
        .logger = &logger,
    };
    defer svc.deinit();

    var ping: Ping = .{};
    var pause: Bulk = .{ .queue = &qh.q, .kind = .pause, .logger = &logger };
    try svc.register("ping", ping.handler());
    try svc.register("queue.pause_all", pause.handler());

    _ = try qh.seed("a");
    _ = try svc.submit("ping", "", .manual);
    _ = try svc.submit("queue.pause_all", "", .manual);
    try testing.expectEqual(@as(usize, 2), try svc.drain(clock.t));

    try testing.expectEqual(@as(usize, 1), ping.calls);
    try testing.expectEqual(@as(usize, 1), pause.last_count);
    try testing.expect(cmd_store.leakFree());
}
