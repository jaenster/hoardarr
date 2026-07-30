//! The operator command queue: submit a named command, a worker claims
//! it, dispatches it to the registered handler, and records the outcome.
//!
//! Command volume is operator-driven — UI clicks — and handlers are
//! deliberately short. A handler with real work to do should kick that
//! work off in the context that owns it and return; the command is
//! "dispatched", not "executed in-band". That is why one worker is
//! enough, and why there is no worker pool here.
//!
//! # Wake-up, not polling
//!
//! Go's loop selected over a ticker and an `armed` channel so a submitted
//! command fired immediately while the ticker caught anything the
//! scheduler had inserted directly. Same policy, no channels: `submit`
//! sets `armed`, and `dueAt` reports the earlier of "armed, so now" and
//! "the next poll". The reactor arms one timer on it.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const dcommand = @import("../../domain/command.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const Command = dcommand.Command;
pub const CommandId = dcommand.CommandId;
pub const Status = dcommand.Status;
pub const Trigger = dcommand.Trigger;

/// Fallback wake-up cadence when nothing has armed the worker. Submitted
/// commands fire immediately; the poll is the safety net for rows a
/// scheduler inserted directly.
pub const default_poll_ms: Millis = 2 * std.time.ms_per_s;

/// Anything `running` for longer than this when the process starts is
/// presumed to belong to a crashed predecessor.
pub const stale_claim_ms: Millis = 10 * std.time.ms_per_min;

pub const StoreError = error{
    CommandNotFound,
    Backend,
} || Allocator.Error;

pub const Store = struct {
    ctx: *anyopaque,
    saveFn: *const fn (ctx: *anyopaque, c: *Command) StoreError!void,
    /// Atomically claims the oldest queued command, flipping it to
    /// `running`. Null when the queue is empty. The claim must be atomic
    /// in the store, or two processes will run the same command.
    claimNextFn: *const fn (ctx: *anyopaque, now: Timestamp) StoreError!?*Command,
    releaseFn: *const fn (ctx: *anyopaque, c: *Command) void,
    /// Frees claims older than `cutoff`; returns the count.
    resetStaleClaimsFn: *const fn (ctx: *anyopaque, cutoff: Timestamp) StoreError!usize,

    pub fn save(self: Store, c: *Command) StoreError!void {
        return self.saveFn(self.ctx, c);
    }

    pub fn claimNext(self: Store, now: Timestamp) StoreError!?*Command {
        return self.claimNextFn(self.ctx, now);
    }

    pub fn release(self: Store, c: *Command) void {
        self.releaseFn(self.ctx, c);
    }

    pub fn resetStaleClaims(self: Store, cutoff: Timestamp) StoreError!usize {
        return self.resetStaleClaimsFn(self.ctx, cutoff);
    }
};

/// One command's handler. Returns null on success, or a reason on
/// failure — an error *set* would force every handler in the process to
/// share one, and a handler's failure reason is for the operator to read,
/// not for the caller to switch on.
pub const Handler = struct {
    ctx: *anyopaque,
    runFn: *const fn (ctx: *anyopaque, body: []const u8) ?[]const u8,

    pub fn run(self: Handler, body: []const u8) ?[]const u8 {
        return self.runFn(self.ctx, body);
    }
};

pub const Error = error{
    /// No handler is registered under that name. Refusing at submit time
    /// keeps un-runnable rows out of the queue entirely.
    NoHandler,
    InvalidCommand,
} || StoreError;

pub const Service = struct {
    const Registration = struct { name: []const u8, handler: Handler };

    gpa: Allocator,
    store: Store,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    poll_ms: Millis = default_poll_ms,

    handlers: std.ArrayList(Registration) = .empty,
    /// Set by `submit`; makes the next `dueAt` immediate.
    armed: bool = false,
    last_poll_at: Timestamp = 0,

    pub fn deinit(self: *Service) void {
        self.handlers.deinit(self.gpa);
        self.* = undefined;
    }

    /// Associates a handler with a name. Re-registering replaces.
    pub fn register(self: *Service, name: []const u8, handler: Handler) Allocator.Error!void {
        for (self.handlers.items) |*r| {
            if (std.mem.eql(u8, r.name, name)) {
                r.handler = handler;
                return;
            }
        }
        try self.handlers.append(self.gpa, .{ .name = name, .handler = handler });
    }

    pub fn lookup(self: *const Service, name: []const u8) ?Handler {
        for (self.handlers.items) |r| {
            if (std.mem.eql(u8, r.name, name)) return r.handler;
        }
        return null;
    }

    /// The registered names, for the UI's command dropdown.
    pub fn names(self: *const Service, a: Allocator) Allocator.Error![]const []const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        errdefer out.deinit(a);
        for (self.handlers.items) |r| try out.append(a, r.name);
        return out.toOwnedSlice(a);
    }

    /// Enqueues a command and wakes the worker.
    pub fn submit(
        self: *Service,
        name: []const u8,
        body: []const u8,
        trigger: Trigger,
    ) Error!CommandId {
        if (self.lookup(name) == null) return error.NoHandler;

        const c = try self.gpa.create(Command);
        c.* = Command.init(self.gpa, .{
            .name = name,
            .body = body,
            .trigger = trigger,
        }, self.clock.now()) catch |e| {
            self.gpa.destroy(c);
            if (e == error.OutOfMemory) return error.OutOfMemory;
            return error.InvalidCommand;
        };
        errdefer {
            c.deinit();
            self.gpa.destroy(c);
        }
        try self.store.save(c);
        self.armed = true;
        return c.id;
    }

    /// When the worker should next wake. Immediately when armed.
    pub fn dueAt(self: *const Service) Timestamp {
        if (self.armed) return self.last_poll_at;
        return self.last_poll_at + self.poll_ms;
    }

    pub fn isDue(self: *const Service, now: Timestamp) bool {
        return self.armed or now >= self.dueAt();
    }

    /// Clears claims a crashed predecessor left behind. Run once at
    /// start, before the first `drain`.
    pub fn recoverStaleClaims(self: *Service) Error!usize {
        const cutoff = self.clock.now() - stale_claim_ms;
        const n = self.store.resetStaleClaims(cutoff) catch |e| {
            self.logger.warn("command: reset stale claims failed", &.{log.errv("err", e)});
            return 0;
        };
        if (n > 0) {
            self.logger.info("command: reset stale running commands", &.{log.uint("count", n)});
        }
        return n;
    }

    /// Runs queued commands back to back until the queue is empty.
    /// Returns how many ran.
    ///
    /// Draining rather than running one per wake is what makes a burst of
    /// submitted commands finish together instead of trickling out one
    /// per poll interval.
    pub fn drain(self: *Service, now: Timestamp) Error!usize {
        self.armed = false;
        self.last_poll_at = now;
        var ran: usize = 0;
        while (true) {
            const did = self.runOne(now) catch |e| {
                self.logger.warn("command: dispatch error", &.{log.errv("err", e)});
                return ran;
            };
            if (!did) return ran;
            ran += 1;
        }
    }

    /// Claims and dispatches at most one command. False when the queue is
    /// empty.
    pub fn runOne(self: *Service, now: Timestamp) Error!bool {
        const c = (try self.store.claimNext(now)) orelse return false;
        defer self.store.release(c);

        const handler = self.lookup(c.name) orelse {
            // Unregistered between submit and dispatch. Record it as
            // failed rather than crash-looping on a row nothing can run.
            c.markCompleted("no handler registered", now) catch |e| {
                if (e == error.OutOfMemory) return error.OutOfMemory;
                return error.InvalidCommand;
            };
            self.store.save(c) catch |e| {
                self.logger.err("command: save completion failed", &.{log.errv("err", e)});
            };
            self.logger.warn("command: unknown handler", &.{
                log.str("name", c.name),
                log.int("id", c.id),
            });
            return true;
        };

        self.logger.info("command: dispatching", &.{
            log.str("name", c.name),
            log.int("id", c.id),
        });
        const failure = handler.run(c.body);
        const ended = self.clock.now();
        c.markCompleted(failure, ended) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory;
            return error.InvalidCommand;
        };
        self.store.save(c) catch |e| {
            self.logger.err("command: save completion failed", &.{
                log.int("id", c.id),
                log.errv("err", e),
            });
        };
        if (failure) |reason| {
            self.logger.warn("command: handler failed", &.{
                log.str("name", c.name),
                log.int("id", c.id),
                log.str("err", reason),
            });
        } else {
            self.logger.info("command: completed", &.{
                log.str("name", c.name),
                log.int("id", c.id),
                log.int("duration_ms", c.durationMs(ended) orelse 0),
            });
        }
        return true;
    }
};

// =====================================================================
// Test doubles
// =====================================================================

pub const FakeStore = struct {
    gpa: Allocator,
    items: std.ArrayList(*Command) = .empty,
    next_id: CommandId = 1,
    saves: usize = 0,
    releases: usize = 0,
    loads: usize = 0,
    stale_cutoff: Timestamp = 0,
    stale_reset: usize = 0,

    pub fn init(gpa: Allocator) FakeStore {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeStore) void {
        for (self.items.items) |c| {
            c.deinit();
            self.gpa.destroy(c);
        }
        self.items.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn store(self: *FakeStore) Store {
        return .{
            .ctx = @ptrCast(self),
            .saveFn = &save,
            .claimNextFn = &claimNext,
            .releaseFn = &release,
            .resetStaleClaimsFn = &resetStaleClaims,
        };
    }

    pub fn get(self: *FakeStore, id: CommandId) ?*Command {
        for (self.items.items) |c| {
            if (c.id == id) return c;
        }
        return null;
    }

    pub fn len(self: *const FakeStore) usize {
        return self.items.items.len;
    }

    pub fn leakFree(self: *const FakeStore) bool {
        return self.loads == self.releases;
    }

    fn save(ctx: *anyopaque, c: *Command) StoreError!void {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        if (c.id == 0) {
            c.setId(self.next_id);
            self.next_id += 1;
            try self.items.append(self.gpa, c);
        }
        self.saves += 1;
    }

    fn claimNext(ctx: *anyopaque, now: Timestamp) StoreError!?*Command {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        for (self.items.items) |c| {
            if (c.status != .queued) continue;
            c.markRunning(now) catch return error.Backend;
            self.loads += 1;
            return c;
        }
        return null;
    }

    fn release(ctx: *anyopaque, _: *Command) void {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        self.releases += 1;
    }

    fn resetStaleClaims(ctx: *anyopaque, cutoff: Timestamp) StoreError!usize {
        const self: *FakeStore = @ptrCast(@alignCast(ctx));
        self.stale_cutoff = cutoff;
        var n: usize = 0;
        for (self.items.items) |c| {
            if (c.status != .running) continue;
            const started = c.started_at orelse continue;
            if (started > cutoff) continue;
            c.status = .queued;
            c.started_at = null;
            n += 1;
        }
        self.stale_reset += n;
        return n;
    }
};

/// A handler that records what it was given and answers a scripted way.
pub const FakeHandler = struct {
    failure: ?[]const u8 = null,
    calls: usize = 0,
    last_body: [128]u8 = undefined,
    last_body_len: usize = 0,

    pub fn handler(self: *FakeHandler) Handler {
        return .{ .ctx = @ptrCast(self), .runFn = &run };
    }

    pub fn body(self: *const FakeHandler) []const u8 {
        return self.last_body[0..self.last_body_len];
    }

    fn run(ctx: *anyopaque, b: []const u8) ?[]const u8 {
        const self: *FakeHandler = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        const n = @min(b.len, self.last_body.len);
        @memcpy(self.last_body[0..n], b[0..n]);
        self.last_body_len = n;
        return self.failure;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;

const Harness = struct {
    store: FakeStore = undefined,
    ping: FakeHandler = .{},
    clock: app_ports.FakeClock = .{ .t = 500_000 },
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.store = FakeStore.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
        };
    }

    fn deinit(self: *Harness) void {
        self.svc.deinit();
        self.store.deinit();
    }

    fn withPing(self: *Harness) !void {
        try self.svc.register("ping", self.ping.handler());
    }
};

test "submitting an unregistered command is refused at the door" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    // Refusing here keeps un-runnable rows out of the queue entirely.
    try testing.expectError(error.NoHandler, h.svc.submit("nope", "", .api));
    try testing.expectEqual(@as(usize, 0), h.store.len());
}

test "a submitted command is queued, dispatched and completed" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();

    const id = try h.svc.submit("ping", "{\"n\":1}", .api);
    const c = h.store.get(id).?;
    try testing.expectEqual(Status.queued, c.status);

    try testing.expectEqual(@as(usize, 1), try h.svc.drain(h.clock.t));
    try testing.expectEqual(@as(usize, 1), h.ping.calls);
    try testing.expectEqualStrings("{\"n\":1}", h.ping.body());
    try testing.expectEqual(Status.completed, c.status);
    try testing.expect(c.succeeded());
    try testing.expect(h.store.leakFree());
}

test "a failing handler records the reason without stopping the queue" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    h.ping.failure = "3/4 servers failed";

    const id = try h.svc.submit("ping", "", .api);
    _ = try h.svc.drain(h.clock.t);

    const c = h.store.get(id).?;
    try testing.expectEqual(Status.completed, c.status);
    try testing.expect(!c.succeeded());
    try testing.expectEqualStrings("3/4 servers failed", c.err);
}

test "a burst drains back to back rather than one per poll" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    _ = try h.svc.submit("ping", "a", .api);
    _ = try h.svc.submit("ping", "b", .api);
    _ = try h.svc.submit("ping", "c", .scheduled);

    try testing.expectEqual(@as(usize, 3), try h.svc.drain(h.clock.t));
    try testing.expectEqual(@as(usize, 3), h.ping.calls);
    try testing.expect(h.store.leakFree());
}

test "a handler unregistered between submit and dispatch fails cleanly" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    const id = try h.svc.submit("ping", "", .api);

    // The registry is rebuilt without it — a reconfiguration, not a
    // crash-loop-worthy condition.
    h.svc.handlers.clearRetainingCapacity();
    try testing.expectEqual(@as(usize, 1), try h.svc.drain(h.clock.t));

    const c = h.store.get(id).?;
    try testing.expectEqual(Status.completed, c.status);
    try testing.expect(!c.succeeded());
    try testing.expectEqualStrings("no handler registered", c.err);
    try testing.expectEqual(@as(usize, 0), h.ping.calls);
}

test "an empty queue drains to zero" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    try testing.expectEqual(@as(usize, 0), try h.svc.drain(h.clock.t));
    try testing.expect(!try h.svc.runOne(h.clock.t));
}

test "submitting arms the worker; draining disarms it" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    h.svc.poll_ms = 2_000;
    h.svc.last_poll_at = 1_000;

    // Idle: the next wake is the poll interval away.
    try testing.expectEqual(@as(Timestamp, 3_000), h.svc.dueAt());
    try testing.expect(!h.svc.isDue(2_999));

    _ = try h.svc.submit("ping", "", .api);
    // Armed: due immediately, whatever the poll interval says.
    try testing.expect(h.svc.isDue(1_000));
    try testing.expectEqual(@as(Timestamp, 1_000), h.svc.dueAt());

    _ = try h.svc.drain(5_000);
    try testing.expect(!h.svc.armed);
    try testing.expectEqual(@as(Timestamp, 7_000), h.svc.dueAt());
}

test "registration replaces rather than duplicating" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    var first: FakeHandler = .{};
    var second: FakeHandler = .{};
    try h.svc.register("ping", first.handler());
    try h.svc.register("ping", second.handler());
    try testing.expectEqual(@as(usize, 1), h.svc.handlers.items.len);

    _ = try h.svc.submit("ping", "", .api);
    _ = try h.svc.drain(h.clock.t);
    try testing.expectEqual(@as(usize, 0), first.calls);
    try testing.expectEqual(@as(usize, 1), second.calls);
}

test "names lists what the UI may offer" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    var a: FakeHandler = .{};
    var b: FakeHandler = .{};
    try h.svc.register("ping", a.handler());
    try h.svc.register("reprobe", b.handler());

    const list = try h.svc.names(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqual(@as(usize, 2), list.len);
    try testing.expectEqualStrings("ping", list[0]);
    try testing.expectEqualStrings("reprobe", list[1]);
}

test "stale claims from a crashed process are released at startup" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    const id = try h.svc.submit("ping", "", .api);
    const c = h.store.get(id).?;
    // A predecessor claimed it and died.
    try c.markRunning(h.clock.t - stale_claim_ms - 1);

    try testing.expectEqual(@as(usize, 1), try h.svc.recoverStaleClaims());
    try testing.expectEqual(Status.queued, c.status);
    try testing.expectEqual(h.clock.t - stale_claim_ms, h.store.stale_cutoff);

    // And it runs on the next drain.
    try testing.expectEqual(@as(usize, 1), try h.svc.drain(h.clock.t));
    try testing.expectEqual(@as(usize, 1), h.ping.calls);
}

test "a recently claimed command is not treated as stale" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.withPing();
    const id = try h.svc.submit("ping", "", .api);
    const c = h.store.get(id).?;
    try c.markRunning(h.clock.t - 1_000);

    try testing.expectEqual(@as(usize, 0), try h.svc.recoverStaleClaims());
    try testing.expectEqual(Status.running, c.status);
}

test "the recorded duration comes from the injected clock" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    // A handler that "takes" 150ms by moving the fake clock, so the
    // duration is asserted exactly rather than approximated by a sleep.
    const Slow = struct {
        clock: *app_ports.FakeClock,
        fn run(ctx: *anyopaque, _: []const u8) ?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.clock.advance(150);
            return null;
        }
    };
    var slow: Slow = .{ .clock = &h.clock };
    try h.svc.register("slow", .{ .ctx = @ptrCast(&slow), .runFn = &Slow.run });

    const start = h.clock.t;
    const id = try h.svc.submit("slow", "", .api);
    _ = try h.svc.drain(start);
    const c = h.store.get(id).?;
    try testing.expectEqual(@as(?Millis, 150), c.durationMs(h.clock.t));
}
