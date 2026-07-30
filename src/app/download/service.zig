//! The long-lived download driver: which jobs are being worked on, which
//! are waiting for a slot, and which NNTP pools exist.
//!
//! # What this is, and what it is not
//!
//! Go's `OrchestratorService` owned a goroutine per active job, two
//! mutexes, a `WaitGroup`, a root context it re-created on every `Start`,
//! and a map of `runnerHandle{cancel, done}`. Nearly all of that was
//! machinery for cancelling threads safely; the *decisions* — is this job
//! already running, is there a slot free, is there a server to ask, who
//! comes off the backlog next — were a few dozen lines buried inside it.
//!
//! On a single-threaded reactor the machinery is unnecessary and the
//! decisions are all that is left. `startRunner` therefore returns a
//! verdict instead of spawning anything:
//!
//!     .started        → the composition root builds a `Runner` and
//!                       registers it with the reactor
//!     .queued         → the concurrency cap is full; it will come off
//!                       the backlog when `finishRunner` frees a slot
//!     .already_active → idempotent no-op (a duplicate JobCreated, a
//!                       resume of something already running)
//!     .parked         → no usable server; the job is now in
//!                       waiting_for_server and holds no slot
//!
//! That makes every scheduling rule assertable without a thread, and it
//! is the half of the old service that actually had bugs in it: the
//! orphaned producer goroutines that drove the live-container CPU climb
//! were a consequence of the machinery, not of the decisions.
//!
//! # Parking is not a dead end
//!
//! A job with no server available does not sit in a runner burning a
//! concurrency slot on work that cannot happen. It goes to
//! `waiting_for_server`, which the UI shows honestly, and
//! `onServerAddedOrEnabled` unparks it the moment a server appears.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const ports = @import("ports.zig");
const queue = @import("queue.zig");
const devents = @import("../../domain/download/events.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const JobId = ports.JobId;
pub const ServerId = ports.ServerId;
pub const PoolInfo = ports.PoolInfo;

pub const Error = ports.RepoError || app_ports.PublishError || app_ports.TxError;

/// What `startRunner` decided.
pub const Admission = enum {
    started,
    queued,
    already_active,
    parked,
};

/// The dispatch hint handed to a `Runner`: a label for logs plus the
/// worker count to size its concurrency with.
pub const Hint = struct {
    server_id: ServerId = 0,
    max_conns: u16 = 1,
};

pub const Service = struct {
    gpa: Allocator,
    store: ports.JobStore,
    queue: *queue.Service,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    /// Max jobs allowed to run in parallel. 0 (or absent) is unlimited.
    /// Read on every admission so a Settings edit takes effect on the
    /// next job rather than at the next restart.
    concurrency_cap: ?app_ports.Knob = null,

    running: std.ArrayList(JobId) = .empty,
    /// Jobs admitted but waiting for a slot, oldest first.
    pending: std.ArrayList(JobId) = .empty,
    pools: std.ArrayList(PoolInfo) = .empty,
    started: bool = false,

    pub fn deinit(self: *Service) void {
        self.running.deinit(self.gpa);
        self.pending.deinit(self.gpa);
        self.pools.deinit(self.gpa);
        self.* = undefined;
    }

    // ---- pools -----------------------------------------------------

    /// Registers or replaces a pool. Idempotent for the same id, so a
    /// duplicate `server.usenet.added` cannot leak a second registration.
    pub fn addPool(self: *Service, info: PoolInfo) Allocator.Error!void {
        for (self.pools.items) |*p| {
            if (p.id == info.id) {
                p.* = info;
                return;
            }
        }
        try self.pools.append(self.gpa, info);
        self.logger.info("pool registered live", &.{
            log.int("server_id", info.id),
            log.uint("pools", self.pools.items.len),
        });
    }

    pub fn removePool(self: *Service, id: ServerId) void {
        for (self.pools.items, 0..) |p, i| {
            if (p.id != id) continue;
            _ = self.pools.orderedRemove(i);
            self.logger.info("pool removed live", &.{
                log.int("server_id", id),
                log.uint("pools", self.pools.items.len),
            });
            return;
        }
    }

    pub fn poolsSnapshot(self: *const Service, a: Allocator) Allocator.Error![]PoolInfo {
        return a.dupe(PoolInfo, self.pools.items);
    }

    /// Whether at least one enabled, in-quota pool exists. The fast-fail
    /// at admission time.
    pub fn haveAnyUsablePool(self: *const Service) bool {
        for (self.pools.items) |p| {
            if (p.usable()) return true;
        }
        return false;
    }

    /// The highest-priority usable pool, for logging and for sizing the
    /// runner's worker count.
    ///
    /// The worker count comes from that one server's cap rather than the
    /// sum across the tier: summing would hammer backup providers with
    /// connections while the primaries are perfectly healthy.
    pub fn dispatchHint(self: *const Service) Hint {
        var best: ?PoolInfo = null;
        for (self.pools.items) |p| {
            if (!p.usable() or p.backup) continue;
            if (best == null or p.priority < best.?.priority) best = p;
        }
        if (best == null) {
            // Only backups left — better to use one than to refuse to
            // download.
            for (self.pools.items) |p| {
                if (p.usable()) {
                    best = p;
                    break;
                }
            }
        }
        const b = best orelse return .{};
        return .{ .server_id = b.id, .max_conns = b.max_conns };
    }

    // ---- runner registry -------------------------------------------

    pub fn isRunning(self: *const Service, id: JobId) bool {
        return std.mem.indexOfScalar(JobId, self.running.items, id) != null;
    }

    pub fn isPending(self: *const Service, id: JobId) bool {
        return std.mem.indexOfScalar(JobId, self.pending.items, id) != null;
    }

    pub fn activeCount(self: *const Service) usize {
        return self.running.items.len;
    }

    pub fn pendingCount(self: *const Service) usize {
        return self.pending.items.len;
    }

    /// Jobs currently being driven. Slice belongs to `a`.
    pub fn activeJobs(self: *const Service, a: Allocator) Allocator.Error![]JobId {
        return a.dupe(JobId, self.running.items);
    }

    fn capReached(self: *const Service) bool {
        const knob = self.concurrency_cap orelse return false;
        const cap = knob.read();
        if (cap <= 0) return false;
        return self.running.items.len >= @as(usize, @intCast(cap));
    }

    /// Admits `id` to the runner set, or explains why not.
    pub fn startRunner(self: *Service, id: JobId) Error!Admission {
        if (self.isRunning(id) or self.isPending(id)) return .already_active;

        // No server to ask: park rather than hold a slot for work that
        // cannot happen. `onServerAddedOrEnabled` unparks it later.
        if (!self.haveAnyUsablePool()) {
            self.queue.parkWaitingForServer(id, "no enabled usenet server") catch |e| {
                self.logger.err("orchestrator: park job failed", &.{
                    log.int("job_id", id),
                    log.errv("err", e),
                });
                return e;
            };
            return .parked;
        }

        if (self.capReached()) {
            try self.pending.append(self.gpa, id);
            self.logger.info("orchestrator: cap reached, queuing job", &.{
                log.int("job_id", id),
                log.uint("active", self.running.items.len),
                log.uint("pending", self.pending.items.len),
            });
            return .queued;
        }

        try self.running.append(self.gpa, id);
        return .started;
    }

    /// Drops `id` from the runner set and from the backlog. Returns true
    /// when a runner was actually cancelled, so the caller knows whether
    /// to tear one down.
    pub fn stopRunner(self: *Service, id: JobId) bool {
        if (std.mem.indexOfScalar(JobId, self.pending.items, id)) |i| {
            _ = self.pending.orderedRemove(i);
        }
        if (std.mem.indexOfScalar(JobId, self.running.items, id)) |i| {
            _ = self.running.orderedRemove(i);
            return true;
        }
        return false;
    }

    /// Called when a runner exits of its own accord. Frees the slot; the
    /// caller then calls `nudgePending` to fill it.
    pub fn finishRunner(self: *Service, id: JobId) void {
        _ = self.stopRunner(id);
    }

    /// Promotes backlog entries into runners while the cap allows.
    /// Returns the ids admitted, oldest first; the slice belongs to `a`.
    ///
    /// Called after a runner exits and after the operator raises the cap.
    pub fn nudgePending(self: *Service, a: Allocator) Error![]JobId {
        var out: std.ArrayList(JobId) = .empty;
        errdefer out.deinit(a);
        while (self.pending.items.len > 0 and !self.capReached()) {
            const next = self.pending.orderedRemove(0);
            // Re-admit through the same door: the pool set may have
            // emptied while the job sat in the backlog, in which case it
            // must park rather than start.
            switch (try self.startRunner(next)) {
                .started => try out.append(a, next),
                else => {},
            }
        }
        return out.toOwnedSlice(a);
    }

    // ---- bus handlers ----------------------------------------------

    pub fn onJobCreated(self: *Service, id: JobId) Error!Admission {
        return self.startRunner(id);
    }

    pub fn onJobResumed(self: *Service, id: JobId) Error!Admission {
        return self.startRunner(id);
    }

    /// The repair worker made deferred recovery volumes visible; re-enter
    /// the runner so `pendingSegments` picks them up.
    pub fn onRecoveryVolsRequested(self: *Service, id: JobId) Error!Admission {
        return self.startRunner(id);
    }

    pub fn onJobPaused(self: *Service, id: JobId) bool {
        return self.stopRunner(id);
    }

    pub fn onJobRemoved(self: *Service, id: JobId) bool {
        return self.stopRunner(id);
    }

    /// A server became available. Registers its pool and kicks every job
    /// that was idle for lack of one.
    ///
    /// Returns the ids that started, so the caller can build their
    /// runners. Slice belongs to `a`.
    pub fn onServerAddedOrEnabled(
        self: *Service,
        a: Allocator,
        info: PoolInfo,
    ) Error![]JobId {
        try self.addPool(info);
        return self.kickIdleJobs(a);
    }

    pub fn onServerDisabledOrRemoved(self: *Service, id: ServerId) void {
        self.removePool(id);
    }

    /// Starts runners for every non-paused, non-terminal job that does
    /// not have one, unparking anything in `waiting_for_server` first.
    pub fn kickIdleJobs(self: *Service, a: Allocator) Error![]JobId {
        const active = try self.store.active(self.gpa, null);
        defer self.gpa.free(active);

        var out: std.ArrayList(JobId) = .empty;
        errdefer out.deinit(a);
        const have_pools = self.haveAnyUsablePool();

        for (active) |row| {
            if (row.state == .paused or row.state.isTerminal()) continue;
            if (row.state == .waiting_for_server) {
                // Still nothing to fetch from: leave it parked instead of
                // starting a runner that would re-park it immediately.
                if (!have_pools) continue;
                self.queue.resumeFromWait(row.id) catch |e| {
                    self.logger.warn("orchestrator: unpark job failed", &.{
                        log.int("job_id", row.id),
                        log.errv("err", e),
                    });
                    continue;
                };
            }
            switch (try self.startRunner(row.id)) {
                .started => try out.append(a, row.id),
                else => {},
            }
        }
        return out.toOwnedSlice(a);
    }

    /// Startup recovery: adopt every job the database says is alive.
    /// Idempotent, and restartable after `stop`.
    pub fn start(self: *Service, a: Allocator) Error![]JobId {
        if (self.started) return &.{};
        self.started = true;
        if (!self.haveAnyUsablePool()) {
            self.logger.warn("orchestrator started without any usable NNTP pool; downloads park until a server is added", &.{});
        }
        const admitted = try self.kickIdleJobs(a);
        self.logger.info("orchestrator service started", &.{
            log.uint("active_jobs", self.running.items.len),
            log.uint("pools", self.pools.items.len),
        });
        return admitted;
    }

    /// Drops every runner and the backlog. The caller tears the runners
    /// down; segments left mid-fetch stay `pending`, so a later `start`
    /// re-fetches them cleanly.
    pub fn stop(self: *Service, a: Allocator) Allocator.Error![]JobId {
        if (!self.started) return &.{};
        self.started = false;
        const cancelled = try a.dupe(JobId, self.running.items);
        self.running.clearRetainingCapacity();
        self.pending.clearRetainingCapacity();
        self.logger.info("orchestrator service stopped", &.{});
        return cancelled;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const job_mod = @import("../../domain/download/job.zig");
const Job = job_mod.Job;

const Harness = struct {
    store: ports.FakeJobStore = undefined,
    fs: app_ports.FakeFs = undefined,
    sink: app_ports.FakeSink(devents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 1_000 },
    cap: app_ports.FakeKnob = .{ .v = 0 },
    logger: log.Logger = .{},
    q: queue.Service = undefined,
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.store = ports.FakeJobStore.init(testing.allocator);
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
        self.svc = .{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .queue = &self.q,
            .clock = self.clock.clock(),
            .logger = &self.logger,
            .concurrency_cap = self.cap.knob(),
        };
    }

    fn deinit(self: *Harness) void {
        self.svc.deinit();
        self.fs.deinit();
        self.store.deinit();
    }

    fn seed(self: *Harness, name: []const u8) !*Job {
        const j = try testing.allocator.create(Job);
        j.* = try Job.init(testing.allocator, .{
            .nzb_hash = name,
            .name = name,
            .files = &.{.{
                .filename = "f.bin",
                .segments = &.{.{ .seq_index = 1, .message_id = "s@h" }},
            }},
        }, 0);
        try self.store.insert(j);
        const evts = try j.pullEvents();
        devents.deinitAll(testing.allocator, evts);
        return j;
    }

    fn withPool(self: *Harness) !void {
        try self.svc.addPool(.{ .id = 1, .name = "primary", .max_conns = 8 });
    }
};

test "a pool registers once however many times the event fires" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    try h.svc.addPool(.{ .id = 1, .max_conns = 4 });
    try h.svc.addPool(.{ .id = 1, .max_conns = 8 });
    try testing.expectEqual(@as(usize, 1), h.svc.pools.items.len);
    // The replacement wins, so an operator's edit takes effect.
    try testing.expectEqual(@as(u16, 8), h.svc.pools.items[0].max_conns);

    h.svc.removePool(1);
    try testing.expectEqual(@as(usize, 0), h.svc.pools.items.len);
    // Removing an unknown id is a no-op.
    h.svc.removePool(99);
}

test "usable-pool detection ignores disabled and exhausted servers" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expect(!h.svc.haveAnyUsablePool());
    try h.svc.addPool(.{ .id = 1, .enabled = false });
    try h.svc.addPool(.{ .id = 2, .quota_exhausted = true });
    try testing.expect(!h.svc.haveAnyUsablePool());
    try h.svc.addPool(.{ .id = 3 });
    try testing.expect(h.svc.haveAnyUsablePool());
}

test "the dispatch hint picks the best non-backup, then falls back" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    // Nothing registered: a safe default rather than a crash.
    try testing.expectEqual(@as(ServerId, 0), h.svc.dispatchHint().server_id);
    try testing.expectEqual(@as(u16, 1), h.svc.dispatchHint().max_conns);

    try h.svc.addPool(.{ .id = 1, .priority = 5, .max_conns = 4 });
    try h.svc.addPool(.{ .id = 2, .priority = 1, .max_conns = 20 });
    try h.svc.addPool(.{ .id = 3, .priority = -10, .backup = true, .max_conns = 50 });
    const hint = h.svc.dispatchHint();
    // The backup's excellent priority does not win it the tier.
    try testing.expectEqual(@as(ServerId, 2), hint.server_id);
    try testing.expectEqual(@as(u16, 20), hint.max_conns);

    // With only backups left, use one rather than refuse to download.
    h.svc.removePool(1);
    h.svc.removePool(2);
    try testing.expectEqual(@as(ServerId, 3), h.svc.dispatchHint().server_id);
}

test "a job with no usable pool is parked, not admitted" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha");

    try testing.expectEqual(Admission.parked, try h.svc.startRunner(j.id));
    try testing.expectEqual(job_mod.JobState.waiting_for_server, j.state);
    try testing.expect(h.sink.has("download.job.waiting_for_server"));
    // Parked jobs hold no concurrency slot: the whole point.
    try testing.expectEqual(@as(usize, 0), h.svc.activeCount());
    try testing.expectEqual(@as(usize, 0), h.svc.pendingCount());
}

test "admission is idempotent for a job already running or queued" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    const j = try h.seed("alpha");

    try testing.expectEqual(Admission.started, try h.svc.startRunner(j.id));
    try testing.expectEqual(Admission.already_active, try h.svc.startRunner(j.id));
    try testing.expectEqual(Admission.already_active, try h.svc.onJobCreated(j.id));
    try testing.expectEqual(Admission.already_active, try h.svc.onJobResumed(j.id));
    try testing.expectEqual(@as(usize, 1), h.svc.activeCount());
}

test "the concurrency cap queues the overflow and the backlog drains in order" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    h.cap.v = 2;

    const a = try h.seed("a");
    const b = try h.seed("b");
    const c = try h.seed("c");
    const d = try h.seed("d");

    try testing.expectEqual(Admission.started, try h.svc.startRunner(a.id));
    try testing.expectEqual(Admission.started, try h.svc.startRunner(b.id));
    try testing.expectEqual(Admission.queued, try h.svc.startRunner(c.id));
    try testing.expectEqual(Admission.queued, try h.svc.startRunner(d.id));
    try testing.expectEqual(@as(usize, 2), h.svc.activeCount());
    try testing.expectEqual(@as(usize, 2), h.svc.pendingCount());

    // A runner exits; exactly one backlog entry is promoted, the oldest.
    h.svc.finishRunner(a.id);
    const promoted = try h.svc.nudgePending(testing.allocator);
    defer testing.allocator.free(promoted);
    try testing.expectEqualSlices(JobId, &.{c.id}, promoted);
    try testing.expectEqual(@as(usize, 2), h.svc.activeCount());
    try testing.expectEqual(@as(usize, 1), h.svc.pendingCount());
    try testing.expect(h.svc.isRunning(c.id));
    try testing.expect(h.svc.isPending(d.id));
}

test "raising the cap drains the whole backlog at once" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    h.cap.v = 1;

    const a = try h.seed("a");
    const b = try h.seed("b");
    const c = try h.seed("c");
    _ = try h.svc.startRunner(a.id);
    _ = try h.svc.startRunner(b.id);
    _ = try h.svc.startRunner(c.id);
    try testing.expectEqual(@as(usize, 2), h.svc.pendingCount());

    // The operator raises the cap in Settings; the next nudge fills it.
    h.cap.v = 5;
    const promoted = try h.svc.nudgePending(testing.allocator);
    defer testing.allocator.free(promoted);
    try testing.expectEqualSlices(JobId, &.{ b.id, c.id }, promoted);
    try testing.expectEqual(@as(usize, 0), h.svc.pendingCount());
}

test "a cap of zero or an absent knob means unlimited" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    h.cap.v = 0;
    for (0..5) |i| {
        var name: [8]u8 = undefined;
        const j = try h.seed(try std.fmt.bufPrint(&name, "j{d}", .{i}));
        try testing.expectEqual(Admission.started, try h.svc.startRunner(j.id));
    }
    try testing.expectEqual(@as(usize, 5), h.svc.activeCount());

    h.svc.concurrency_cap = null;
    const extra = try h.seed("extra");
    try testing.expectEqual(Admission.started, try h.svc.startRunner(extra.id));
}

test "pausing a queued-for-a-slot job takes it out of the backlog" {
    // Otherwise a job the operator paused while it waited would start
    // itself the moment a slot opened.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    h.cap.v = 1;
    const a = try h.seed("a");
    const b = try h.seed("b");
    _ = try h.svc.startRunner(a.id);
    _ = try h.svc.startRunner(b.id);

    try testing.expect(!h.svc.onJobPaused(b.id));
    try testing.expectEqual(@as(usize, 0), h.svc.pendingCount());

    h.svc.finishRunner(a.id);
    const promoted = try h.svc.nudgePending(testing.allocator);
    defer testing.allocator.free(promoted);
    try testing.expectEqual(@as(usize, 0), promoted.len);
}

test "pausing and removing a running job reports the cancellation" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    const a = try h.seed("a");
    _ = try h.svc.startRunner(a.id);

    try testing.expect(h.svc.onJobPaused(a.id));
    try testing.expectEqual(@as(usize, 0), h.svc.activeCount());
    // Nothing to cancel the second time.
    try testing.expect(!h.svc.onJobPaused(a.id));
    try testing.expect(!h.svc.onJobRemoved(a.id));
}

test "a backlog entry parks when the pools vanish while it waits" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    h.cap.v = 1;
    const a = try h.seed("a");
    const b = try h.seed("b");
    _ = try h.svc.startRunner(a.id);
    _ = try h.svc.startRunner(b.id);

    // The operator disables the only server, then the running job ends.
    h.svc.onServerDisabledOrRemoved(1);
    h.svc.finishRunner(a.id);
    const promoted = try h.svc.nudgePending(testing.allocator);
    defer testing.allocator.free(promoted);

    // Re-admitting through the same door is what catches this: starting
    // a runner with nothing to fetch from would just re-park instantly.
    try testing.expectEqual(@as(usize, 0), promoted.len);
    try testing.expectEqual(job_mod.JobState.waiting_for_server, b.state);
}

test "adding a server unparks the jobs that were waiting for one" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha");
    try testing.expectEqual(Admission.parked, try h.svc.startRunner(j.id));
    h.sink.reset();

    const started = try h.svc.onServerAddedOrEnabled(testing.allocator, .{ .id = 1, .max_conns = 8 });
    defer testing.allocator.free(started);

    try testing.expectEqualSlices(JobId, &.{j.id}, started);
    try testing.expectEqual(job_mod.JobState.queued, j.state);
    try testing.expect(h.sink.has("download.job.resumed"));
    try testing.expect(h.svc.isRunning(j.id));
}

test "kickIdleJobs leaves parked jobs parked while there is still no server" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha");
    _ = try h.svc.startRunner(j.id);
    h.sink.reset();

    const started = try h.svc.kickIdleJobs(testing.allocator);
    defer testing.allocator.free(started);
    try testing.expectEqual(@as(usize, 0), started.len);
    try testing.expectEqual(job_mod.JobState.waiting_for_server, j.state);
    // No pointless unpark/re-park churn on the bus.
    try testing.expectEqual(@as(usize, 0), h.sink.n);
}

test "kickIdleJobs skips paused and terminal jobs" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();

    const live = try h.seed("live");
    const paused = try h.seed("paused");
    _ = try paused.pause(1);
    const done = try h.seed("done");
    _ = try done.markCompleted(1);
    for ([_]*Job{ paused, done }) |j| {
        const evts = try j.pullEvents();
        devents.deinitAll(testing.allocator, evts);
    }

    const started = try h.svc.kickIdleJobs(testing.allocator);
    defer testing.allocator.free(started);
    try testing.expectEqualSlices(JobId, &.{live.id}, started);
}

test "start adopts the live jobs and is idempotent; stop hands them back" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    const a = try h.seed("a");
    const b = try h.seed("b");

    const admitted = try h.svc.start(testing.allocator);
    defer testing.allocator.free(admitted);
    try testing.expectEqual(@as(usize, 2), admitted.len);
    try testing.expect(h.svc.isRunning(a.id) and h.svc.isRunning(b.id));

    // A second start does nothing — no duplicate runners.
    const again = try h.svc.start(testing.allocator);
    defer testing.allocator.free(again);
    try testing.expectEqual(@as(usize, 0), again.len);
    try testing.expectEqual(@as(usize, 2), h.svc.activeCount());

    const cancelled = try h.svc.stop(testing.allocator);
    defer testing.allocator.free(cancelled);
    try testing.expectEqual(@as(usize, 2), cancelled.len);
    try testing.expectEqual(@as(usize, 0), h.svc.activeCount());

    // Restartable: the crash-recovery flow relies on it.
    const restarted = try h.svc.start(testing.allocator);
    defer testing.allocator.free(restarted);
    try testing.expectEqual(@as(usize, 2), restarted.len);
}

test "startup unparks jobs left in waiting_for_server when pools now exist" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha");
    _ = try j.markWaitingForServer("from a previous run", 1);
    const evts = try j.pullEvents();
    devents.deinitAll(testing.allocator, evts);

    try h.withPool();
    const admitted = try h.svc.start(testing.allocator);
    defer testing.allocator.free(admitted);
    try testing.expectEqualSlices(JobId, &.{j.id}, admitted);
    try testing.expectEqual(job_mod.JobState.queued, j.state);
}

test "recovery-vols requested re-enters the runner" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    const j = try h.seed("alpha");
    try testing.expectEqual(Admission.started, try h.svc.onRecoveryVolsRequested(j.id));
    // And is idempotent if the runner is already up.
    try testing.expectEqual(Admission.already_active, try h.svc.onRecoveryVolsRequested(j.id));
}

test "activeJobs reports what is being driven" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPool();
    const a = try h.seed("a");
    _ = try h.svc.startRunner(a.id);
    const ids = try h.svc.activeJobs(testing.allocator);
    defer testing.allocator.free(ids);
    try testing.expectEqualSlices(JobId, &.{a.id}, ids);
}
