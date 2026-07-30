//! The `schedule` bounded context: durable scheduled tasks.
//!
//! Aggregate root: `Task` — one unit of work that fires either once at a
//! given instant (`oneshot`) or forever on a cadence (`recurring`), and
//! that must survive a process restart.
//!
//! Why durable rather than a thread with a timer: a timer dies with the
//! process and silently never runs the slot it missed. `next_run_at` on
//! a row means a task that should have fired during downtime fires
//! immediately when the process comes back.
//!
//!     idle ──claim──▶ running ──▶ idle          (next_run_at advanced)
//!       ▲                │
//!       └── reset stale ─┘                       (crash recovery)
//!
//! `status` tracks the worker claim; `enabled` is the operator's switch.
//! They are independent: a disabled task keeps its `next_run_at` so
//! re-enabling it resumes the original cadence rather than restarting
//! the clock.
//!
//! # Ownership
//!
//! The aggregate owns `name`, `payload` and `last_error`, so it gets
//! `init(allocator, ...)` / `deinit`. This context emits no domain
//! events — the Go original didn't either. A scheduled task's *handler*
//! publishes whatever its own context defines; "the scheduler ticked" is
//! not news to anyone.

const std = @import("std");
const event = @import("event.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;
const Millis = event.Millis;

/// Identifies a `Task`. Allocated by the repository; 0 means "not yet
/// persisted".
pub const TaskId = i64;

/// Recurring fires forever on a cadence; oneshot fires once and then
/// disables itself.
pub const Kind = enum {
    recurring,
    oneshot,

    pub fn toString(self: Kind) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Kind {
        return std.meta.stringToEnum(Kind, s);
    }
};

/// The worker-claim state.
pub const Status = enum {
    idle,
    running,

    pub fn toString(self: Status) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Status {
        return std.meta.stringToEnum(Status, s);
    }
};

/// Backoff base for a oneshot task, which has no cadence of its own.
pub const oneshot_backoff_base_ms: Millis = 30 * event.second_ms;

/// Ceiling on the retry delay. Past an hour a poison task is the
/// operator's problem, and waiting longer only makes it look dead.
pub const max_backoff_ms: Millis = event.hour_ms;

/// Highest failure count that still moves the backoff. `1 << 6 == 64`,
/// which is where the Go original clamped its multiplier; capping the
/// shift rather than the product is what keeps this free of the integer
/// overflow Go's `1 << consecutiveFailures` walks into after 31
/// failures.
pub const max_backoff_shift: u6 = 6;

pub const ValidationError = error{
    NameRequired,
    /// A recurring task without a positive cadence would be claimed
    /// again the instant it finished, in a tight loop.
    CadenceRequired,
    /// Every task needs a first instant to fire at; there is no
    /// "sometime" in a durable scheduler.
    FirstRunRequired,
};

pub const TransitionError = error{
    /// `setCadence` on a oneshot task.
    NotRecurring,
    /// `setCadence` with a non-positive value.
    CadenceNotPositive,
};

pub const InitError = ValidationError || Allocator.Error;

pub const NewParams = struct {
    name: []const u8,
    kind: Kind,
    /// Required for `recurring`; ignored for `oneshot`.
    cadence_ms: Millis = 0,
    /// Handler-specific blob. Copied; opaque to the domain.
    payload: []const u8 = "",
    /// First instant the task should fire. Must be non-zero.
    first_run: Timestamp,
};

/// The snapshot the repository hands back for a stored row. Trusted.
pub const HydrateParams = struct {
    id: TaskId,
    name: []const u8,
    kind: Kind,
    cadence_ms: Millis = 0,
    payload: []const u8 = "",
    next_run_at: Timestamp,
    last_run_at: ?Timestamp = null,
    last_error: []const u8 = "",
    consecutive_failures: u32 = 0,
    enabled: bool = true,
    status: Status = .idle,
    claimed_at: ?Timestamp = null,
    created_at: Timestamp,
    updated_at: Timestamp,
};

pub const Task = struct {
    allocator: Allocator,

    id: TaskId = 0,
    /// Unique across tasks — the register-on-startup path upserts by it.
    /// Owned.
    name: []const u8,
    kind: Kind,
    /// 0 for a oneshot.
    cadence_ms: Millis,
    /// Owned.
    payload: []const u8,

    next_run_at: Timestamp,
    last_run_at: ?Timestamp = null,
    /// Owned. Empty after a successful run.
    last_error: []const u8,
    consecutive_failures: u32 = 0,
    enabled: bool = true,
    status: Status = .idle,
    claimed_at: ?Timestamp = null,

    created_at: Timestamp,
    updated_at: Timestamp,

    pub fn init(allocator: Allocator, p: NewParams, now: Timestamp) InitError!Task {
        const name = std.mem.trim(u8, p.name, &std.ascii.whitespace);
        if (name.len == 0) return error.NameRequired;
        if (p.kind == .recurring and p.cadence_ms <= 0) return error.CadenceRequired;
        if (p.first_run == 0) return error.FirstRunRequired;

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);
        const payload_copy = try allocator.dupe(u8, p.payload);
        errdefer allocator.free(payload_copy);
        const err_copy = try allocator.dupe(u8, "");

        return .{
            .allocator = allocator,
            .name = name_copy,
            .kind = p.kind,
            // A oneshot's cadence is meaningless; normalise it away so a
            // stored row can't imply one.
            .cadence_ms = if (p.kind == .recurring) p.cadence_ms else 0,
            .payload = payload_copy,
            .next_run_at = p.first_run,
            .last_error = err_copy,
            .created_at = now,
            .updated_at = now,
        };
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!Task {
        const name = try allocator.dupe(u8, p.name);
        errdefer allocator.free(name);
        const payload = try allocator.dupe(u8, p.payload);
        errdefer allocator.free(payload);
        const last_error = try allocator.dupe(u8, p.last_error);
        return .{
            .allocator = allocator,
            .id = p.id,
            .name = name,
            .kind = p.kind,
            .cadence_ms = p.cadence_ms,
            .payload = payload,
            .next_run_at = p.next_run_at,
            .last_run_at = p.last_run_at,
            .last_error = last_error,
            .consecutive_failures = p.consecutive_failures,
            .enabled = p.enabled,
            .status = p.status,
            .claimed_at = p.claimed_at,
            .created_at = p.created_at,
            .updated_at = p.updated_at,
        };
    }

    pub fn deinit(self: *Task) void {
        self.allocator.free(self.name);
        self.allocator.free(self.payload);
        self.allocator.free(self.last_error);
        self.* = undefined;
    }

    pub fn setId(self: *Task, id: TaskId) void {
        self.id = id;
    }

    /// Whether the scheduler should claim this task at `now`.
    ///
    /// The repository's `ClaimDue` encodes the same predicate in SQL;
    /// having it here as well means the in-process scheduler and the
    /// tests agree with the query, and a change has one obvious second
    /// place to make it.
    pub fn isDue(self: *const Task, now: Timestamp) bool {
        return self.enabled and self.status == .idle and self.next_run_at <= now;
    }

    /// → running. The repository enforces the race guard: its UPDATE
    /// carries a `status = 'idle'` predicate, so a second scheduler
    /// cannot double-claim.
    pub fn markClaimed(self: *Task, now: Timestamp) void {
        self.status = .running;
        self.claimed_at = now;
        self.updated_at = now;
    }

    /// Records a successful run, clears the failure state and recomputes
    /// `next_run_at`. A oneshot disables itself; a recurring task lands
    /// one cadence out from `now` rather than from its scheduled slot, so
    /// a slow handler cannot build up a backlog of overdue firings.
    pub fn markSucceeded(self: *Task, now: Timestamp) Allocator.Error!void {
        const cleared = try self.allocator.dupe(u8, "");
        self.allocator.free(self.last_error);
        self.last_error = cleared;

        self.last_run_at = now;
        self.consecutive_failures = 0;
        self.status = .idle;
        self.claimed_at = null;
        self.updated_at = now;
        switch (self.kind) {
            .recurring => self.next_run_at = now + self.cadence_ms,
            .oneshot => self.enabled = false,
        }
    }

    /// Records a failure and backs off. See `backoffMs`.
    ///
    /// A oneshot stays enabled: it never ran successfully, so disabling
    /// it would drop the work silently. It retries on the backoff until
    /// the operator intervenes.
    pub fn markFailed(self: *Task, reason: []const u8, now: Timestamp) Allocator.Error!void {
        const copy = try self.allocator.dupe(u8, reason);
        self.allocator.free(self.last_error);
        self.last_error = copy;

        self.last_run_at = now;
        self.consecutive_failures += 1;
        self.status = .idle;
        self.claimed_at = null;
        self.updated_at = now;
        self.next_run_at = now + self.backoffMs();
    }

    /// Exponential backoff: `base × 2^failures`, where `base` is the
    /// cadence (or 30 s for a oneshot), the exponent is clamped to
    /// `max_backoff_shift`, and the product is clamped to
    /// `max_backoff_ms`.
    ///
    /// Called after `consecutive_failures` has been incremented, so the
    /// first failure already waits two cadences — a task that just broke
    /// is unlikely to be fixed by the time its normal slot comes round.
    pub fn backoffMs(self: *const Task) Millis {
        const base: Millis = if (self.cadence_ms > 0) self.cadence_ms else oneshot_backoff_base_ms;
        const shift: u6 = @intCast(@min(self.consecutive_failures, max_backoff_shift));
        const mult: Millis = @as(Millis, 1) << shift;
        // base is bounded by whatever the operator configured, so do the
        // clamp before the multiply could overflow an i64.
        if (base > @divTrunc(max_backoff_ms, mult)) return max_backoff_ms;
        return @min(base * mult, max_backoff_ms);
    }

    /// The operator's switch. A no-op when the flag already holds the
    /// requested value.
    pub fn setEnabled(self: *Task, enabled: bool, now: Timestamp) void {
        if (self.enabled == enabled) return;
        self.enabled = enabled;
        self.updated_at = now;
    }

    /// Moves `next_run_at`. Backs the admin "run now" button and a
    /// cadence edit that should take effect before the next slot.
    pub fn reschedule(self: *Task, at: Timestamp, now: Timestamp) void {
        self.next_run_at = at;
        self.updated_at = now;
    }

    /// Changes the cadence of a recurring task. Idempotent when
    /// unchanged; `next_run_at` is left alone, so the new cadence applies
    /// from the next completed run — use `reschedule` to pull it in.
    pub fn setCadence(self: *Task, cadence_ms: Millis, now: Timestamp) TransitionError!void {
        if (self.kind != .recurring) return error.NotRecurring;
        if (cadence_ms <= 0) return error.CadenceNotPositive;
        if (self.cadence_ms == cadence_ms) return;
        self.cadence_ms = cadence_ms;
        self.updated_at = now;
    }

    /// Startup recovery: a row found in `running` came from a process
    /// that died mid-run, since nothing else can leave it there. Flip it
    /// back so the scheduler picks it up again. A no-op for an idle task,
    /// which makes the startup sweep safe to run over every row.
    pub fn resetStaleClaim(self: *Task, now: Timestamp) bool {
        if (self.status != .running) return false;
        self.status = .idle;
        self.claimed_at = null;
        self.updated_at = now;
        return true;
    }
};

/// Errors a `Task` repository raises that callers branch on.
pub const RepositoryError = error{
    TaskNotFound,
    /// Another worker claimed the task between our SELECT and our
    /// UPDATE. Skip it and continue.
    ClaimLost,
    /// `name` is unique; a second insert under the same name lands here.
    DuplicateName,
};

// ---------------------------------------------------------------------
// Tests
//
// Shaped after `repo_schedule_test.go` and `app/schedule/scheduler.go`;
// the Go aggregate itself had no unit tests, so the transition and
// backoff cases below are new.
// ---------------------------------------------------------------------

const t = std.testing;

fn recurring(cadence_ms: Millis, first_run: Timestamp) !Task {
    return Task.init(t.allocator, .{
        .name = "sweep",
        .kind = .recurring,
        .cadence_ms = cadence_ms,
        .first_run = first_run,
    }, 0);
}

fn oneshot(first_run: Timestamp) !Task {
    return Task.init(t.allocator, .{
        .name = "migrate",
        .kind = .oneshot,
        .first_run = first_run,
    }, 0);
}

test "init validates and normalises" {
    try t.expectError(error.NameRequired, Task.init(t.allocator, .{
        .name = "  ",
        .kind = .oneshot,
        .first_run = 1,
    }, 0));
    try t.expectError(error.CadenceRequired, Task.init(t.allocator, .{
        .name = "n",
        .kind = .recurring,
        .cadence_ms = 0,
        .first_run = 1,
    }, 0));
    try t.expectError(error.CadenceRequired, Task.init(t.allocator, .{
        .name = "n",
        .kind = .recurring,
        .cadence_ms = -5,
        .first_run = 1,
    }, 0));
    try t.expectError(error.FirstRunRequired, Task.init(t.allocator, .{
        .name = "n",
        .kind = .oneshot,
        .first_run = 0,
    }, 0));

    // A cadence handed to a oneshot is dropped, not stored.
    var one = try Task.init(t.allocator, .{
        .name = "  migrate  ",
        .kind = .oneshot,
        .cadence_ms = 5_000,
        .first_run = 42,
    }, 7);
    defer one.deinit();
    try t.expectEqualStrings("migrate", one.name);
    try t.expectEqual(@as(Millis, 0), one.cadence_ms);
    try t.expectEqual(@as(Timestamp, 42), one.next_run_at);
    try t.expectEqual(@as(Timestamp, 7), one.created_at);
    try t.expectEqual(Status.idle, one.status);
    try t.expect(one.enabled);
    try t.expectEqual(@as(?Timestamp, null), one.last_run_at);
}

test "isDue needs enabled, idle and the slot reached" {
    var task = try recurring(1_000, 100);
    defer task.deinit();

    try t.expect(!task.isDue(99));
    try t.expect(task.isDue(100));
    try t.expect(task.isDue(1_000_000));

    task.setEnabled(false, 1);
    try t.expect(!task.isDue(1_000_000));
    task.setEnabled(true, 2);
    try t.expect(task.isDue(1_000_000));

    task.markClaimed(3);
    try t.expect(!task.isDue(1_000_000));
}

test "markClaimed records the claim" {
    var task = try recurring(1_000, 100);
    defer task.deinit();
    task.markClaimed(555);
    try t.expectEqual(Status.running, task.status);
    try t.expectEqual(@as(?Timestamp, 555), task.claimed_at);
    try t.expectEqual(@as(Timestamp, 555), task.updated_at);
}

test "a recurring success advances one cadence from now, not from the slot" {
    var task = try recurring(1_000, 100);
    defer task.deinit();
    task.markClaimed(100);

    // The handler took 400 ms and the scheduler was 50 ms late.
    try task.markSucceeded(550);
    try t.expectEqual(Status.idle, task.status);
    try t.expectEqual(@as(?Timestamp, null), task.claimed_at);
    try t.expectEqual(@as(?Timestamp, 550), task.last_run_at);
    // 1550, not 1100: no backlog of overdue firings can accumulate.
    try t.expectEqual(@as(Timestamp, 1_550), task.next_run_at);
    try t.expect(task.enabled);
    try t.expectEqual(@as(u32, 0), task.consecutive_failures);
    try t.expectEqual(@as(usize, 0), task.last_error.len);
}

test "a oneshot success disables the task" {
    var task = try oneshot(100);
    defer task.deinit();
    task.markClaimed(100);
    try task.markSucceeded(200);

    try t.expect(!task.enabled);
    try t.expectEqual(Status.idle, task.status);
    try t.expectEqual(@as(Timestamp, 100), task.next_run_at); // untouched
    try t.expect(!task.isDue(9_999));
}

test "success clears a previous failure" {
    var task = try recurring(1_000, 100);
    defer task.deinit();
    try task.markFailed("transient", 100);
    try t.expectEqual(@as(u32, 1), task.consecutive_failures);
    try t.expectEqualStrings("transient", task.last_error);

    try task.markSucceeded(200);
    try t.expectEqual(@as(u32, 0), task.consecutive_failures);
    try t.expectEqual(@as(usize, 0), task.last_error.len);
}

test "failure keeps the reason, counts up and releases the claim" {
    var task = try recurring(1_000, 100);
    defer task.deinit();
    task.markClaimed(100);

    try task.markFailed("no handler registered", 150);
    try t.expectEqualStrings("no handler registered", task.last_error);
    try t.expectEqual(@as(u32, 1), task.consecutive_failures);
    try t.expectEqual(Status.idle, task.status);
    try t.expectEqual(@as(?Timestamp, null), task.claimed_at);
    try t.expectEqual(@as(?Timestamp, 150), task.last_run_at);
    // First failure already waits two cadences.
    try t.expectEqual(@as(Timestamp, 150 + 2_000), task.next_run_at);

    try task.markFailed("again", 200);
    try t.expectEqual(@as(u32, 2), task.consecutive_failures);
    try t.expectEqualStrings("again", task.last_error);
    try t.expectEqual(@as(Timestamp, 200 + 4_000), task.next_run_at);
}

test "backoff doubles, then clamps at the shift and at one hour" {
    var task = try recurring(event.minute_ms, 1);
    defer task.deinit();

    // 1 failure → 2 min, 2 → 4, 3 → 8, 4 → 16, 5 → 32, 6 → 64 min but
    // the hour cap bites first.
    const expected = [_]Millis{
        2 * event.minute_ms,
        4 * event.minute_ms,
        8 * event.minute_ms,
        16 * event.minute_ms,
        32 * event.minute_ms,
        event.hour_ms, // 64 min clamped
        event.hour_ms, // shift clamped too
        event.hour_ms,
    };
    for (expected, 1..) |want, failures| {
        task.consecutive_failures = @intCast(failures);
        try t.expectEqual(want, task.backoffMs());
    }
}

test "a oneshot backs off from thirty seconds and stays enabled" {
    var task = try oneshot(100);
    defer task.deinit();

    try task.markFailed("boom", 100);
    try t.expect(task.enabled);
    try t.expectEqual(@as(Timestamp, 100 + 60 * event.second_ms), task.next_run_at);

    try task.markFailed("boom", 200);
    try t.expectEqual(@as(Timestamp, 200 + 120 * event.second_ms), task.next_run_at);
}

test "backoff cannot overflow on an absurd cadence" {
    var task = try recurring(std.math.maxInt(i64) / 4, 1);
    defer task.deinit();
    task.consecutive_failures = 6;
    try t.expectEqual(max_backoff_ms, task.backoffMs());
}

test "setEnabled is a no-op when unchanged" {
    var task = try recurring(1_000, 100);
    defer task.deinit();
    const created = task.updated_at;

    task.setEnabled(true, 999);
    try t.expectEqual(created, task.updated_at);

    task.setEnabled(false, 999);
    try t.expect(!task.enabled);
    try t.expectEqual(@as(Timestamp, 999), task.updated_at);
}

test "disabling preserves the slot so re-enabling resumes the cadence" {
    var task = try recurring(1_000, 5_000);
    defer task.deinit();
    task.setEnabled(false, 100);
    task.setEnabled(true, 200);
    try t.expectEqual(@as(Timestamp, 5_000), task.next_run_at);
}

test "reschedule moves the slot" {
    var task = try recurring(1_000, 5_000);
    defer task.deinit();
    task.reschedule(10, 100);
    try t.expectEqual(@as(Timestamp, 10), task.next_run_at);
    try t.expectEqual(@as(Timestamp, 100), task.updated_at);
    try t.expect(task.isDue(10));
}

test "setCadence is recurring-only and rejects non-positive values" {
    var one = try oneshot(1);
    defer one.deinit();
    try t.expectError(error.NotRecurring, one.setCadence(1_000, 2));

    var task = try recurring(1_000, 5_000);
    defer task.deinit();
    try t.expectError(error.CadenceNotPositive, task.setCadence(0, 2));
    try t.expectError(error.CadenceNotPositive, task.setCadence(-1, 2));
    try t.expectEqual(@as(Millis, 1_000), task.cadence_ms);

    const created = task.updated_at;
    try task.setCadence(1_000, 999); // idempotent
    try t.expectEqual(created, task.updated_at);

    try task.setCadence(2_000, 999);
    try t.expectEqual(@as(Millis, 2_000), task.cadence_ms);
    try t.expectEqual(@as(Timestamp, 999), task.updated_at);
    // The new cadence takes effect from the next completed run.
    try t.expectEqual(@as(Timestamp, 5_000), task.next_run_at);
    try task.markSucceeded(6_000);
    try t.expectEqual(@as(Timestamp, 8_000), task.next_run_at);
}

test "resetStaleClaim rescues a crashed run and ignores an idle task" {
    var task = try Task.hydrate(t.allocator, .{
        .id = 3,
        .name = "sweep",
        .kind = .recurring,
        .cadence_ms = 1_000,
        .next_run_at = 500,
        .status = .running,
        .claimed_at = 400,
        .created_at = 1,
        .updated_at = 400,
    });
    defer task.deinit();

    try t.expect(!task.isDue(9_999)); // claimed, so not due
    try t.expect(task.resetStaleClaim(1_000));
    try t.expectEqual(Status.idle, task.status);
    try t.expectEqual(@as(?Timestamp, null), task.claimed_at);
    try t.expectEqual(@as(Timestamp, 1_000), task.updated_at);
    try t.expect(task.isDue(1_000));

    // Safe to run over an already-idle row.
    try t.expect(!task.resetStaleClaim(2_000));
    try t.expectEqual(@as(Timestamp, 1_000), task.updated_at);
}

test "a task whose slot passed during downtime fires immediately" {
    // The whole point of the durable table.
    var task = try Task.hydrate(t.allocator, .{
        .id = 1,
        .name = "sweep",
        .kind = .recurring,
        .cadence_ms = event.hour_ms,
        .next_run_at = 1_000, // due while the process was down
        .created_at = 0,
        .updated_at = 0,
    });
    defer task.deinit();
    try t.expect(task.isDue(1_000_000));
}

test "hydrate carries the payload and failure state" {
    var task = try Task.hydrate(t.allocator, .{
        .id = 4,
        .name = "n",
        .kind = .oneshot,
        .payload = "{\"job\":7}",
        .next_run_at = 10,
        .last_run_at = 9,
        .last_error = "previous boom",
        .consecutive_failures = 3,
        .enabled = true,
        .created_at = 1,
        .updated_at = 9,
    });
    defer task.deinit();

    try t.expectEqualStrings("{\"job\":7}", task.payload);
    try t.expectEqualStrings("previous boom", task.last_error);
    try t.expectEqual(@as(u32, 3), task.consecutive_failures);
    // 30 s base × 2^3.
    try t.expectEqual(@as(Millis, 240 * event.second_ms), task.backoffMs());
}

test "the payload is a copy, not a borrow" {
    var scratch = [_]u8{ '{', '}' };
    var task = try Task.init(t.allocator, .{
        .name = "n",
        .kind = .oneshot,
        .payload = &scratch,
        .first_run = 1,
    }, 0);
    defer task.deinit();
    scratch[0] = 'X';
    try t.expectEqual(@as(u8, '{'), task.payload[0]);
}

test "enums round-trip their persisted forms" {
    try t.expectEqualStrings("recurring", Kind.recurring.toString());
    try t.expectEqualStrings("oneshot", Kind.oneshot.toString());
    try t.expectEqualStrings("running", Status.running.toString());
    try t.expectEqual(Kind.oneshot, Kind.parse("oneshot").?);
    try t.expectEqual(Status.idle, Status.parse("idle").?);
    try t.expectEqual(@as(?Kind, null), Kind.parse("cron"));
    try t.expectEqual(@as(?Status, null), Status.parse("claimed"));
}
