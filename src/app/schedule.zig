//! The periodic-task scheduler: claim what is due, dispatch it to the
//! handler registered under its name, record the outcome.
//!
//! Go ran a `time.Ticker` in a goroutine, claimed a batch per tick, and
//! spawned a worker goroutine per task behind a semaphore. The reactor
//! version keeps the *policy* — claim at most `workers` tasks per tick,
//! release a claim that has no handler, record success or failure with a
//! backoff — and drops the threads: `tick` returns the tasks to run and
//! `finish` records what happened. The caller decides when a tick
//! happens, which is what lets a test drive an hour of scheduling in
//! microseconds.
//!
//! # Why a claim can be orphaned
//!
//! A task whose handler was never registered would otherwise sit in
//! `running` forever, invisible and un-runnable. It is released
//! immediately with a failure so the row comes back on a later tick and
//! the operator can see why.

const std = @import("std");
const log = @import("../core/log.zig");
const app_ports = @import("ports.zig");
const dtx = @import("../domain/tx.zig");
const dschedule = @import("../domain/schedule.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const Task = dschedule.Task;
pub const TaskId = dschedule.TaskId;
pub const Status = dschedule.Status;
pub const Kind = dschedule.Kind;

/// Default poll interval and worker count, matching Go.
pub const default_tick_ms: Millis = 5 * std.time.ms_per_s;
pub const default_workers: usize = 4;

pub const StoreError = error{
    TaskNotFound,
    Backend,
} || Allocator.Error;

/// Persistence for `Task`.
///
/// `claimDue` is the interesting one: it must atomically select the due,
/// enabled, idle tasks and flip them to `running`, so two processes
/// racing on the same database cannot both run the same task. That
/// atomicity is the store's job; this layer only assumes it.
pub const Store = struct {
    ctx: *anyopaque,
    byNameFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, name: []const u8) StoreError!*Task,
    releaseFn: *const fn (ctx: *anyopaque, t: *Task) void,
    saveFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, t: *Task) StoreError!void,
    /// Claims up to `limit` due tasks, flipping each to `running`. The
    /// slice belongs to `a`; every task in it must be handed back with
    /// `release`.
    claimDueFn: *const fn (
        ctx: *anyopaque,
        a: Allocator,
        unit: ?*app_ports.Unit,
        now: Timestamp,
        limit: usize,
    ) StoreError![]*Task,
    /// Frees claims a previous process left behind. Returns the count.
    resetStaleClaimsFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, now: Timestamp) StoreError!usize,

    pub fn byName(self: Store, unit: ?*app_ports.Unit, name: []const u8) StoreError!*Task {
        return self.byNameFn(self.ctx, unit, name);
    }

    pub fn release(self: Store, t: *Task) void {
        self.releaseFn(self.ctx, t);
    }

    pub fn save(self: Store, unit: ?*app_ports.Unit, t: *Task) StoreError!void {
        return self.saveFn(self.ctx, unit, t);
    }

    pub fn claimDue(
        self: Store,
        a: Allocator,
        unit: ?*app_ports.Unit,
        now: Timestamp,
        limit: usize,
    ) StoreError![]*Task {
        return self.claimDueFn(self.ctx, a, unit, now, limit);
    }

    pub fn resetStaleClaims(self: Store, unit: ?*app_ports.Unit, now: Timestamp) StoreError!usize {
        return self.resetStaleClaimsFn(self.ctx, unit, now);
    }
};

pub const Error = StoreError || app_ports.TxError || dschedule.TransitionError;

/// One task the caller must run. The payload borrows from the task,
/// which stays alive until `finish`.
pub const Due = struct {
    task: *Task,
    name: []const u8,
    payload: []const u8,
};

pub const Scheduler = struct {
    gpa: Allocator,
    store: Store,
    txm: app_ports.Manager,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    tick_ms: Millis = default_tick_ms,
    workers: usize = default_workers,

    /// Handler names known to this process. The handlers themselves are
    /// the caller's — this layer only needs to know whether one exists,
    /// because a task with no handler must not stay claimed.
    registered: std.ArrayList([]const u8) = .empty,
    last_tick_at: Timestamp = 0,

    pub fn deinit(self: *Scheduler) void {
        self.registered.deinit(self.gpa);
        self.* = undefined;
    }

    /// Declares that this process can run tasks called `name`.
    /// Idempotent.
    pub fn register(self: *Scheduler, name: []const u8) Allocator.Error!void {
        if (self.isRegistered(name)) return;
        try self.registered.append(self.gpa, name);
    }

    pub fn isRegistered(self: *const Scheduler, name: []const u8) bool {
        for (self.registered.items) |n| {
            if (std.mem.eql(u8, n, name)) return true;
        }
        return false;
    }

    /// Upserts a built-in task by name.
    ///
    /// An existing row keeps its `next_run_at` and its history: a
    /// restart must not push out a task that was about to fire. Only the
    /// cadence is re-applied, and only when it actually changed, and a
    /// task the operator disabled is re-enabled because declaring it at
    /// boot is a statement that it should run.
    pub fn ensureTask(self: *Scheduler, p: dschedule.NewParams) Error!TaskId {
        if (self.store.byName(null, p.name)) |existing| {
            defer self.store.release(existing);
            var changed = false;
            if (p.kind == .recurring and existing.cadence_ms != p.cadence_ms) {
                try existing.setCadence(p.cadence_ms, self.clock.now());
                changed = true;
            }
            if (!existing.enabled) {
                existing.setEnabled(true, self.clock.now());
                changed = true;
            }
            if (changed) try self.store.save(null, existing);
            return existing.id;
        } else |e| {
            if (e != error.TaskNotFound) return e;
        }

        const t = try self.gpa.create(Task);
        t.* = Task.init(self.gpa, p, self.clock.now()) catch |e| {
            self.gpa.destroy(t);
            return switch (e) {
                error.OutOfMemory => error.OutOfMemory,
                // A malformed built-in declaration is a programming
                // error at boot, not a runtime condition.
                else => error.Backend,
            };
        };
        errdefer {
            t.deinit();
            self.gpa.destroy(t);
        }
        try self.store.save(null, t);
        return t.id;
    }

    /// Clears claims a previous process died holding. Run once at start.
    pub fn recoverStaleClaims(self: *Scheduler) Error!usize {
        const n = self.store.resetStaleClaims(null, self.clock.now()) catch |e| {
            self.logger.warn("schedule: reset stale claims", &.{log.errv("err", e)});
            return 0;
        };
        if (n > 0) {
            self.logger.info("schedule: reset stale claims from previous run", &.{
                log.uint("count", n),
            });
        }
        return n;
    }

    pub fn dueAt(self: *const Scheduler) Timestamp {
        return self.last_tick_at + self.tick_ms;
    }

    pub fn isDue(self: *const Scheduler, now: Timestamp) bool {
        return now >= self.dueAt();
    }

    /// Claims the tasks that are due and returns the ones with a
    /// handler.
    ///
    /// The batch is capped at the worker count so no row sits in
    /// `running` that this process cannot actually invoke. Tasks with no
    /// registered handler are released here with a failure, so they never
    /// reach the caller.
    ///
    /// Every returned `Due.task` must be passed to `finish`.
    pub fn tick(self: *Scheduler, a: Allocator, now: Timestamp) Error![]Due {
        self.last_tick_at = now;
        const claimed = self.store.claimDue(self.gpa, null, now, self.workers) catch |e| {
            self.logger.warn("schedule: claim due", &.{log.errv("err", e)});
            return &.{};
        };
        defer self.gpa.free(claimed);

        var out: std.ArrayList(Due) = .empty;
        errdefer out.deinit(a);
        for (claimed) |t| {
            if (!self.isRegistered(t.name)) {
                // Release the claim with a failure so the row does not
                // stay 'running' forever, invisible and un-runnable.
                try t.markFailed("no handler registered", now);
                self.store.save(null, t) catch |e| {
                    self.logger.warn("schedule: release orphan", &.{
                        log.str("name", t.name),
                        log.errv("err", e),
                    });
                };
                self.store.release(t);
                continue;
            }
            try out.append(a, .{ .task = t, .name = t.name, .payload = t.payload });
        }
        return out.toOwnedSlice(a);
    }

    /// Records the outcome of one dispatched task and hands it back to
    /// the store.
    ///
    /// `failure` null means the handler returned cleanly. The domain
    /// picks the next run instant either way — success reschedules on the
    /// cadence, failure applies an exponential backoff.
    pub fn finish(self: *Scheduler, due: Due, failure: ?[]const u8, now: Timestamp) Error!void {
        defer self.store.release(due.task);
        if (failure) |reason| {
            self.logger.warn("schedule: task failed", &.{
                log.str("name", due.name),
                log.str("err", reason),
            });
            try due.task.markFailed(reason, now);
        } else {
            self.logger.debug("schedule: task ok", &.{log.str("name", due.name)});
            try due.task.markSucceeded(now);
        }
        self.store.save(null, due.task) catch |e| {
            self.logger.warn("schedule: persist outcome", &.{
                log.str("name", due.name),
                log.errv("err", e),
            });
        };
    }
};

// =====================================================================
// Test double
// =====================================================================

pub const FakeStore = struct {
    gpa: Allocator,
    tasks: std.ArrayList(*Task) = .empty,
    next_id: TaskId = 1,
    saves: usize = 0,
    loads: usize = 0,
    releases: usize = 0,
    stale_reset: usize = 0,
    fail_claim: bool = false,

    pub fn init(gpa: Allocator) FakeStore {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeStore) void {
        for (self.tasks.items) |t| {
            t.deinit();
            self.gpa.destroy(t);
        }
        self.tasks.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn store(self: *FakeStore) Store {
        return .{
            .ctx = @ptrCast(self),
            .byNameFn = &byName,
            .releaseFn = &release,
            .saveFn = &save,
            .claimDueFn = &claimDue,
            .resetStaleClaimsFn = &resetStaleClaims,
        };
    }

    pub fn insert(self: *FakeStore, t: *Task) Allocator.Error!void {
        if (t.id == 0) {
            t.setId(self.next_id);
            self.next_id += 1;
        }
        try self.tasks.append(self.gpa, t);
    }

    pub fn get(self: *FakeStore, name: []const u8) ?*Task {
        for (self.tasks.items) |t| {
            if (std.mem.eql(u8, t.name, name)) return t;
        }
        return null;
    }

    pub fn len(self: *const FakeStore) usize {
        return self.tasks.items.len;
    }

    pub fn leakFree(self: *const FakeStore) bool {
        return self.loads == self.releases;
    }

    fn byName(ctx: *anyopaque, _: ?*app_ports.Unit, name: []const u8) StoreError!*Task {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        const t = self.get(name) orelse return error.TaskNotFound;
        self.loads += 1;
        return t;
    }

    fn release(ctx: *anyopaque, _: *Task) void {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        self.releases += 1;
    }

    fn save(ctx: *anyopaque, _: ?*app_ports.Unit, t: *Task) StoreError!void {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        if (t.id == 0) try self.insert(t);
        self.saves += 1;
    }

    fn claimDue(
        ctx: *anyopaque,
        a: Allocator,
        _: ?*app_ports.Unit,
        now: Timestamp,
        limit: usize,
    ) StoreError![]*Task {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        if (self.fail_claim) return error.Backend;
        var out: std.ArrayList(*Task) = .empty;
        errdefer out.deinit(a);
        for (self.tasks.items) |t| {
            if (out.items.len == limit) break;
            if (!t.isDue(now)) continue;
            t.markClaimed(now);
            self.loads += 1;
            try out.append(a, t);
        }
        return out.toOwnedSlice(a);
    }

    fn resetStaleClaims(ctx: *anyopaque, _: ?*app_ports.Unit, now: Timestamp) StoreError!usize {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        var n: usize = 0;
        for (self.tasks.items) |t| {
            if (t.resetStaleClaim(now)) n += 1;
        }
        self.stale_reset += n;
        return n;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;

const Harness = struct {
    store: FakeStore = undefined,
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 10_000 },
    logger: log.Logger = .{},
    sched: Scheduler = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.store = FakeStore.init(testing.allocator);
        self.sched = .{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .txm = self.ftx.manager(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
        };
    }

    fn deinit(self: *Harness) void {
        self.sched.deinit();
        self.store.deinit();
    }

    fn seed(self: *Harness, name: []const u8, first_run: Timestamp) !*Task {
        const t = try testing.allocator.create(Task);
        t.* = try Task.init(testing.allocator, .{
            .name = name,
            .kind = .recurring,
            .cadence_ms = 60_000,
            .first_run = first_run,
        }, 0);
        try self.store.insert(t);
        return t;
    }
};

test "ensureTask creates a task the first time and keeps its schedule after" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    const id = try h.sched.ensureTask(.{
        .name = "outbox.prune",
        .kind = .recurring,
        .cadence_ms = 60_000,
        .first_run = h.clock.t + 60_000,
    });
    try testing.expectEqual(@as(TaskId, 1), id);
    try testing.expectEqual(@as(usize, 1), h.store.len());
    const t = h.store.get("outbox.prune").?;
    const scheduled = t.next_run_at;

    // A restart must not push out a task that is about to fire.
    const again = try h.sched.ensureTask(.{
        .name = "outbox.prune",
        .kind = .recurring,
        .cadence_ms = 60_000,
        .first_run = h.clock.t + 999_999,
    });
    try testing.expectEqual(id, again);
    try testing.expectEqual(@as(usize, 1), h.store.len());
    try testing.expectEqual(scheduled, t.next_run_at);
    try testing.expect(h.store.leakFree());
}

test "ensureTask re-applies a changed cadence and re-enables the task" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.sched.ensureTask(.{
        .name = "prune",
        .kind = .recurring,
        .cadence_ms = 60_000,
        .first_run = 1,
    });
    const t = h.store.get("prune").?;
    t.setEnabled(false, h.clock.t);
    const saves = h.store.saves;

    _ = try h.sched.ensureTask(.{
        .name = "prune",
        .kind = .recurring,
        .cadence_ms = 30_000,
        .first_run = 1,
    });
    try testing.expectEqual(@as(Millis, 30_000), t.cadence_ms);
    // Declaring a task at boot is a statement that it should run.
    try testing.expect(t.enabled);
    try testing.expectEqual(saves + 1, h.store.saves);
}

test "ensureTask writes nothing when nothing changed" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.sched.ensureTask(.{
        .name = "prune",
        .kind = .recurring,
        .cadence_ms = 60_000,
        .first_run = 1,
    });
    const saves = h.store.saves;
    _ = try h.sched.ensureTask(.{
        .name = "prune",
        .kind = .recurring,
        .cadence_ms = 60_000,
        .first_run = 1,
    });
    try testing.expectEqual(saves, h.store.saves);
}

test "a tick claims only due tasks, capped at the worker count" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.sched.workers = 2;
    try h.sched.register("a");
    try h.sched.register("b");
    try h.sched.register("c");
    try h.sched.register("later");
    _ = try h.seed("a", 1_000);
    _ = try h.seed("b", 2_000);
    _ = try h.seed("c", 3_000);
    // Not yet due.
    _ = try h.seed("later", 99_000);

    const due = try h.sched.tick(testing.allocator, h.clock.t);
    defer testing.allocator.free(due);
    try testing.expectEqual(@as(usize, 2), due.len);
    for (due) |d| try h.sched.finish(d, null, h.clock.t);
    try testing.expect(h.store.leakFree());
}

test "a claimed task with no handler is released with a failure" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const orphan = try h.seed("nobody.handles.me", 1_000);

    const due = try h.sched.tick(testing.allocator, h.clock.t);
    defer testing.allocator.free(due);
    // It never reaches the caller…
    try testing.expectEqual(@as(usize, 0), due.len);
    // …and it does not sit in `running` forever.
    try testing.expect(orphan.status != .running);
    try testing.expectEqualStrings("no handler registered", orphan.last_error);
    try testing.expectEqual(@as(u32, 1), orphan.consecutive_failures);
    try testing.expect(h.store.leakFree());
}

test "a successful run clears the error and reschedules on the cadence" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.sched.register("prune");
    const t = try h.seed("prune", 1_000);

    const due = try h.sched.tick(testing.allocator, h.clock.t);
    defer testing.allocator.free(due);
    try testing.expectEqual(@as(usize, 1), due.len);
    try testing.expectEqualStrings("prune", due[0].name);

    try h.sched.finish(due[0], null, h.clock.t);
    try testing.expectEqual(Status.idle, t.status);
    try testing.expectEqualStrings("", t.last_error);
    try testing.expectEqual(@as(u32, 0), t.consecutive_failures);
    try testing.expectEqual(h.clock.t + 60_000, t.next_run_at);
    try testing.expect(h.store.leakFree());
}

test "a failed run records the reason and backs off" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.sched.register("prune");
    const t = try h.seed("prune", 1_000);

    const due = try h.sched.tick(testing.allocator, h.clock.t);
    defer testing.allocator.free(due);
    try h.sched.finish(due[0], "database is locked", h.clock.t);

    try testing.expectEqualStrings("database is locked", t.last_error);
    try testing.expectEqual(@as(u32, 1), t.consecutive_failures);
    // The next attempt is deferred rather than immediate.
    try testing.expect(t.next_run_at > h.clock.t);
    try testing.expect(h.store.leakFree());
}

test "a claim failure yields no work instead of propagating" {
    // A scheduler that dies on a transient database error takes every
    // periodic task with it. Log and try again next tick.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.store.fail_claim = true;
    const due = try h.sched.tick(testing.allocator, h.clock.t);
    defer testing.allocator.free(due);
    try testing.expectEqual(@as(usize, 0), due.len);
}

test "the tick cadence is a deadline the caller arms a timer on" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.sched.tick_ms = 5_000;
    h.sched.last_tick_at = 1_000;
    try testing.expectEqual(@as(Timestamp, 6_000), h.sched.dueAt());
    try testing.expect(!h.sched.isDue(5_999));
    try testing.expect(h.sched.isDue(6_000));

    const due = try h.sched.tick(testing.allocator, 6_000);
    defer testing.allocator.free(due);
    try testing.expectEqual(@as(Timestamp, 11_000), h.sched.dueAt());
}

test "stale claims from a dead process are released at startup" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const t = try h.seed("prune", 1_000);
    t.markClaimed(1_000);
    try testing.expectEqual(Status.running, t.status);

    // Far enough past the claim that it is presumed dead.
    h.clock.set(1_000 + 60 * std.time.ms_per_min);
    try testing.expectEqual(@as(usize, 1), try h.sched.recoverStaleClaims());
    try testing.expectEqual(Status.idle, t.status);
    // A second sweep finds nothing.
    try testing.expectEqual(@as(usize, 0), try h.sched.recoverStaleClaims());
}

test "registration is idempotent" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.sched.register("a");
    try h.sched.register("a");
    try testing.expectEqual(@as(usize, 1), h.sched.registered.items.len);
    try testing.expect(h.sched.isRegistered("a"));
    try testing.expect(!h.sched.isRegistered("b"));
}

test "a disabled task is never claimed" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.sched.register("prune");
    const t = try h.seed("prune", 1_000);
    t.setEnabled(false, h.clock.t);

    const due = try h.sched.tick(testing.allocator, h.clock.t);
    defer testing.allocator.free(due);
    try testing.expectEqual(@as(usize, 0), due.len);
}
