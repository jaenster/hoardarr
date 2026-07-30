//! The `command` bounded context: one-off asynchronous operations an
//! operator (or other code) triggers, as opposed to the recurring work
//! the `schedule` context owns.
//!
//! Sonarr calls these Commands; we keep the name so the mental model —
//! and the wire shape — travels for anyone coming from an *arr.
//!
//! Aggregate root: `Command`.
//!
//!     queued ──▶ running ──▶ completed
//!
//! `completed` is the only end state; whether the work *succeeded* is a
//! separate axis (`result`), because "the command finished and the
//! handler said no" is not the same thing as "the command is still
//! running". A JSON body carries the handler-specific payload; the
//! domain never looks inside it, and the handlers live in the app layer
//! registered by name.
//!
//! # Ownership
//!
//! The aggregate owns its `name`, `body` and `err`, so it gets
//! `init(allocator, ...)` / `deinit`.
//!
//! This context emits no domain events — the Go original didn't either.
//! Nothing downstream reacts to a command; the operator polls it, and
//! the handler publishes whatever events its own context defines. Adding
//! an event queue here would be ceremony with no subscriber.
//!
//! # No clocks
//!
//! `durationMs` takes `now` as a parameter. Go read `time.Now()` when
//! the command was still running, which made the value untestable and
//! meant two calls a millisecond apart disagreed. The caller already
//! knows what "now" means for the response it is rendering.

const std = @import("std");
const event = @import("event.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;
const Millis = event.Millis;

/// Identifies a `Command`. Allocated by the repository; 0 means "not yet
/// persisted".
pub const CommandId = i64;

/// Lifecycle state.
pub const Status = enum {
    queued,
    running,
    completed,

    pub fn toString(self: Status) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Status {
        return std.meta.stringToEnum(Status, s);
    }
};

/// Outcome, meaningful only once `status == .completed`. Modelled as an
/// optional on the aggregate rather than with an extra `""` variant, so
/// "not decided yet" is unrepresentable as a value the UI could render.
pub const Result = enum {
    successful,
    failed,

    pub fn toString(self: Result) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Result {
        return std.meta.stringToEnum(Result, s);
    }
};

/// How the command came into existence. Lets the UI filter down to
/// "things I started".
pub const Trigger = enum {
    manual,
    api,
    scheduled,

    pub fn toString(self: Trigger) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Trigger {
        return std.meta.stringToEnum(Trigger, s);
    }
};

pub const ValidationError = error{
    NameRequired,
};

pub const TransitionError = error{
    /// `markRunning` from anything but `queued`. A second worker
    /// claiming a running command would run the work twice.
    NotQueued,
    /// `markCompleted` from anything but `running`. Go had no guard
    /// here, so a bug could record a result for a command that never
    /// ran — and `duration` would then report zero for something the
    /// UI showed as finished.
    NotRunning,
};

pub const InitError = ValidationError || Allocator.Error;

pub const NewParams = struct {
    /// Handler name registered with the service.
    name: []const u8,
    /// JSON payload, handler-specific. Copied.
    body: []const u8 = "",
    trigger: Trigger = .api,
};

/// The snapshot the repository hands back for a stored row. Trusted.
pub const HydrateParams = struct {
    id: CommandId,
    name: []const u8,
    body: []const u8 = "",
    trigger: Trigger = .api,
    status: Status,
    result: ?Result = null,
    err: []const u8 = "",
    queued_at: Timestamp,
    started_at: ?Timestamp = null,
    ended_at: ?Timestamp = null,
};

pub const Command = struct {
    allocator: Allocator,

    id: CommandId = 0,
    /// Owned. Trimmed on the way in.
    name: []const u8,
    /// Owned. Opaque to the domain.
    body: []const u8,
    trigger: Trigger,
    status: Status = .queued,
    result: ?Result = null,
    /// Owned. Empty unless `result == .failed`.
    err: []const u8,

    queued_at: Timestamp,
    started_at: ?Timestamp = null,
    ended_at: ?Timestamp = null,

    pub fn init(allocator: Allocator, p: NewParams, now: Timestamp) InitError!Command {
        const name = std.mem.trim(u8, p.name, &std.ascii.whitespace);
        if (name.len == 0) return error.NameRequired;

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);
        const body_copy = try allocator.dupe(u8, p.body);
        errdefer allocator.free(body_copy);
        const err_copy = try allocator.dupe(u8, "");

        return .{
            .allocator = allocator,
            .name = name_copy,
            .body = body_copy,
            .trigger = p.trigger,
            .err = err_copy,
            .queued_at = now,
        };
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!Command {
        const name = try allocator.dupe(u8, p.name);
        errdefer allocator.free(name);
        const body = try allocator.dupe(u8, p.body);
        errdefer allocator.free(body);
        const err = try allocator.dupe(u8, p.err);
        return .{
            .allocator = allocator,
            .id = p.id,
            .name = name,
            .body = body,
            .trigger = p.trigger,
            .status = p.status,
            .result = p.result,
            .err = err,
            .queued_at = p.queued_at,
            .started_at = p.started_at,
            .ended_at = p.ended_at,
        };
    }

    pub fn deinit(self: *Command) void {
        self.allocator.free(self.name);
        self.allocator.free(self.body);
        self.allocator.free(self.err);
        self.* = undefined;
    }

    pub fn setId(self: *Command, id: CommandId) void {
        self.id = id;
    }

    /// queued → running. The worker calls this after claiming and before
    /// invoking the handler.
    ///
    /// In production the repository's `ClaimNext` performs the same
    /// transition inside the claiming UPDATE, so the two can't race;
    /// this method exists for the in-process path and for tests.
    pub fn markRunning(self: *Command, now: Timestamp) TransitionError!void {
        if (self.status != .queued) return error.NotQueued;
        self.status = .running;
        self.started_at = now;
    }

    /// running → completed.
    ///
    /// `err_msg` null means the handler returned cleanly. A non-null
    /// message records `failed` and keeps the text for the operator —
    /// the domain never inspects it.
    pub fn markCompleted(
        self: *Command,
        err_msg: ?[]const u8,
        now: Timestamp,
    ) (TransitionError || Allocator.Error)!void {
        if (self.status != .running) return error.NotRunning;

        const copy = try self.allocator.dupe(u8, err_msg orelse "");
        errdefer self.allocator.free(copy);

        self.allocator.free(self.err);
        self.err = copy;
        self.status = .completed;
        self.ended_at = now;
        self.result = if (err_msg == null) .successful else .failed;
    }

    /// Wall-clock the handler spent, or null when the command never
    /// started. While still running, measured against the caller's
    /// `now`.
    pub fn durationMs(self: *const Command, now: Timestamp) ?Millis {
        const start = self.started_at orelse return null;
        const end = self.ended_at orelse now;
        return end - start;
    }

    /// True once no further transition is expected.
    pub fn isFinished(self: *const Command) bool {
        return self.status == .completed;
    }

    /// True when the command finished and the handler reported success.
    pub fn succeeded(self: *const Command) bool {
        return self.status == .completed and self.result == .successful;
    }
};

/// Errors a `Command` repository raises that callers branch on.
pub const RepositoryError = error{
    CommandNotFound,
};

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no unit tests; these are written from the
// implementation and from how `app/command/service.go` drives it.
// ---------------------------------------------------------------------

const t = std.testing;

test "init trims the name, copies the body, and starts queued" {
    var c = try Command.init(t.allocator, .{
        .name = "  RefreshDiskSpace  ",
        .body = "{\"path\":\"/data\"}",
        .trigger = .manual,
    }, 1_000);
    defer c.deinit();

    try t.expectEqualStrings("RefreshDiskSpace", c.name);
    try t.expectEqualStrings("{\"path\":\"/data\"}", c.body);
    try t.expectEqual(Trigger.manual, c.trigger);
    try t.expectEqual(Status.queued, c.status);
    try t.expectEqual(@as(?Result, null), c.result);
    try t.expectEqual(@as(Timestamp, 1_000), c.queued_at);
    try t.expectEqual(@as(?Timestamp, null), c.started_at);
    try t.expect(!c.isFinished());
}

test "init defaults the trigger to api and rejects a blank name" {
    try t.expectError(error.NameRequired, Command.init(t.allocator, .{ .name = "" }, 1));
    try t.expectError(error.NameRequired, Command.init(t.allocator, .{ .name = "  \t " }, 1));

    var c = try Command.init(t.allocator, .{ .name = "Backup" }, 1);
    defer c.deinit();
    try t.expectEqual(Trigger.api, c.trigger);
    try t.expectEqual(@as(usize, 0), c.body.len);
}

test "the body is a copy, not a borrow" {
    var scratch = [_]u8{ '{', '}', 0, 0 };
    var c = try Command.init(t.allocator, .{ .name = "x", .body = &scratch }, 1);
    defer c.deinit();
    scratch[0] = 'X';
    try t.expectEqual(@as(u8, '{'), c.body[0]);
}

test "the happy path is queued, running, completed successfully" {
    var c = try Command.init(t.allocator, .{ .name = "x" }, 100);
    defer c.deinit();
    c.setId(3);

    try c.markRunning(200);
    try t.expectEqual(Status.running, c.status);
    try t.expectEqual(@as(?Timestamp, 200), c.started_at);

    try c.markCompleted(null, 350);
    try t.expectEqual(Status.completed, c.status);
    try t.expectEqual(Result.successful, c.result.?);
    try t.expectEqual(@as(?Timestamp, 350), c.ended_at);
    try t.expectEqual(@as(usize, 0), c.err.len);
    try t.expect(c.isFinished());
    try t.expect(c.succeeded());
    try t.expectEqual(@as(?Millis, 150), c.durationMs(9_999));
}

test "a handler error records failed and keeps the message" {
    var c = try Command.init(t.allocator, .{ .name = "x" }, 1);
    defer c.deinit();
    try c.markRunning(2);
    try c.markCompleted("no handler registered", 3);

    try t.expectEqual(Status.completed, c.status);
    try t.expectEqual(Result.failed, c.result.?);
    try t.expectEqualStrings("no handler registered", c.err);
    try t.expect(c.isFinished());
    try t.expect(!c.succeeded());
}

test "markRunning only works from queued" {
    var c = try Command.init(t.allocator, .{ .name = "x" }, 1);
    defer c.deinit();
    try c.markRunning(2);
    // A second worker must not be able to claim it.
    try t.expectError(error.NotQueued, c.markRunning(3));
    try t.expectEqual(@as(?Timestamp, 2), c.started_at);

    try c.markCompleted(null, 4);
    try t.expectError(error.NotQueued, c.markRunning(5));
}

test "markCompleted requires the command to be running" {
    var queued = try Command.init(t.allocator, .{ .name = "x" }, 1);
    defer queued.deinit();
    try t.expectError(error.NotRunning, queued.markCompleted(null, 2));
    try t.expectEqual(Status.queued, queued.status);
    try t.expectEqual(@as(?Result, null), queued.result);

    var done = try Command.init(t.allocator, .{ .name = "x" }, 1);
    defer done.deinit();
    try done.markRunning(2);
    try done.markCompleted(null, 3);
    // No re-deciding the outcome.
    try t.expectError(error.NotRunning, done.markCompleted("actually it failed", 4));
    try t.expectEqual(Result.successful, done.result.?);
    try t.expectEqual(@as(?Timestamp, 3), done.ended_at);
}

test "durationMs is null before the handler starts and live while running" {
    var c = try Command.init(t.allocator, .{ .name = "x" }, 100);
    defer c.deinit();
    try t.expectEqual(@as(?Millis, null), c.durationMs(500));

    try c.markRunning(200);
    // Still running: measured against the caller's instant, so the same
    // command reports consistently for a whole response render.
    try t.expectEqual(@as(?Millis, 300), c.durationMs(500));
    try t.expectEqual(@as(?Millis, 800), c.durationMs(1_000));

    try c.markCompleted(null, 700);
    // Finished: `now` no longer matters.
    try t.expectEqual(@as(?Millis, 500), c.durationMs(500));
    try t.expectEqual(@as(?Millis, 500), c.durationMs(99_999));
}

test "hydrate restores a completed row" {
    var c = try Command.hydrate(t.allocator, .{
        .id = 8,
        .name = "Backup",
        .body = "{}",
        .trigger = .scheduled,
        .status = .completed,
        .result = .failed,
        .err = "disk full",
        .queued_at = 1,
        .started_at = 2,
        .ended_at = 5,
    });
    defer c.deinit();

    try t.expectEqual(@as(CommandId, 8), c.id);
    try t.expectEqual(Trigger.scheduled, c.trigger);
    try t.expectEqual(Result.failed, c.result.?);
    try t.expectEqualStrings("disk full", c.err);
    try t.expectEqual(@as(?Millis, 3), c.durationMs(0));
    try t.expect(!c.succeeded());
}

test "a row rescued from a crashed worker is claimable again" {
    // ResetStaleClaims flips `running` back to `queued`; the aggregate
    // must accept the resulting row.
    var c = try Command.hydrate(t.allocator, .{
        .id = 1,
        .name = "x",
        .status = .queued,
        .queued_at = 1,
        .started_at = 2, // left over from the dead attempt
    });
    defer c.deinit();
    try c.markRunning(10);
    try t.expectEqual(@as(?Timestamp, 10), c.started_at);
}

test "enums round-trip their persisted forms" {
    try t.expectEqualStrings("queued", Status.queued.toString());
    try t.expectEqualStrings("successful", Result.successful.toString());
    try t.expectEqualStrings("scheduled", Trigger.scheduled.toString());
    try t.expectEqual(Status.running, Status.parse("running").?);
    try t.expectEqual(Result.failed, Result.parse("failed").?);
    try t.expectEqual(Trigger.manual, Trigger.parse("manual").?);
    try t.expectEqual(@as(?Status, null), Status.parse("cancelled"));
    try t.expectEqual(@as(?Trigger, null), Trigger.parse("cron"));
}
