//! `scheduled_tasks`: the durable scheduler.
//!
//! Replaces the ad-hoc background tickers that outbox pruning, WAL
//! checkpointing and `ANALYZE` used to run on. A ticker loses its state on
//! restart; a row does not, which is the whole point — a task that is due
//! stays due across a crash.
//!
//! ## Claiming
//!
//! `claimDue` is a two-step SELECT-then-conditional-UPDATE rather than a
//! single statement, because SQLite has no `SELECT ... FOR UPDATE`. Each
//! UPDATE carries `AND status = 'idle'`, so `changes() == 1` *is* the proof
//! that this worker owns the row; a loser sees 0 and moves on. Rows we did
//! not win are never returned, so a caller can never run a task it does
//! not own.
//!
//! `resetStaleClaims` exists because a crash mid-handler leaves
//! `status = 'running'` forever. Running it once at startup is what makes
//! the claim model crash-safe: there is no lease to expire, just a sweep
//! by the process that knows it just started.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const tx = @import("tx.zig");
const migrate = @import("migrate.zig");
const schedule = @import("../domain/schedule.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Task = schedule.Task;

pub const Error = sqlite.Error || schedule.RepositoryError || error{
    /// A stored `kind` or `status` this binary does not know.
    UnknownEnum,
};

const columns =
    "id, name, kind, cadence, payload, " ++
    "next_run_at, last_run_at, last_error, consecutive_failures, " ++
    "enabled, status, claimed_at, created_at, updated_at";

/// An owned result set.
pub const TaskList = struct {
    gpa: Allocator,
    items: std.ArrayList(Task) = .empty,

    pub fn deinit(self: *TaskList) void {
        for (self.items.items) |*x| x.deinit();
        self.items.deinit(self.gpa);
    }
};

pub const ScheduleRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) ScheduleRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: ScheduleRepo, task: *Task) Error!void {
        if (task.id == 0) return self.insert(task);
        return self.update(task);
    }

    fn insert(self: ScheduleRepo, task: *Task) Error!void {
        self.conn.execute(
            \\INSERT INTO scheduled_tasks(
            \\    name, kind, cadence, payload,
            \\    next_run_at, last_run_at, last_error, consecutive_failures,
            \\    enabled, status, claimed_at, created_at, updated_at
            \\) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        , .{
            task.name,
            task.kind.toString(),
            cadenceValue(task.cadence_ms),
            payloadValue(task.payload),
            task.next_run_at,
            task.last_run_at,
            sqlite.nullIfEmpty(task.last_error),
            @as(i64, task.consecutive_failures),
            task.enabled,
            task.status.toString(),
            task.claimed_at,
            task.created_at,
            task.updated_at,
        }) catch |e| {
            if (e == error.ConstraintUnique) return error.DuplicateName;
            return e;
        };
        task.setId(self.conn.lastInsertRowid());
    }

    fn update(self: ScheduleRepo, task: *const Task) Error!void {
        self.conn.execute(
            \\UPDATE scheduled_tasks SET
            \\    name = ?, kind = ?, cadence = ?, payload = ?,
            \\    next_run_at = ?, last_run_at = ?, last_error = ?, consecutive_failures = ?,
            \\    enabled = ?, status = ?, claimed_at = ?, updated_at = ?
            \\WHERE id = ?
        , .{
            task.name,
            task.kind.toString(),
            cadenceValue(task.cadence_ms),
            payloadValue(task.payload),
            task.next_run_at,
            task.last_run_at,
            sqlite.nullIfEmpty(task.last_error),
            @as(i64, task.consecutive_failures),
            task.enabled,
            task.status.toString(),
            task.claimed_at,
            task.updated_at,
            task.id,
        }) catch |e| {
            if (e == error.ConstraintUnique) return error.DuplicateName;
            return e;
        };
        if (self.conn.changes() == 0) return error.TaskNotFound;
    }

    pub fn byId(self: ScheduleRepo, gpa: Allocator, id: schedule.TaskId) Error!Task {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM scheduled_tasks WHERE id = ?", .{id});
    }

    pub fn byName(self: ScheduleRepo, gpa: Allocator, name: []const u8) Error!Task {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM scheduled_tasks WHERE name = ?", .{name});
    }

    pub fn list(self: ScheduleRepo, gpa: Allocator) Error!TaskList {
        var out = TaskList{ .gpa = gpa };
        errdefer out.deinit();
        var st = try self.conn.query(
            "SELECT " ++ columns ++ " FROM scheduled_tasks ORDER BY name ASC",
            .{},
        );
        defer st.release();
        while (try st.step()) {
            var task = try hydrate(gpa, &st);
            errdefer task.deinit();
            out.items.append(gpa, task) catch return error.OutOfMemory;
        }
        return out;
    }

    pub fn remove(self: ScheduleRepo, id: schedule.TaskId) Error!void {
        try self.conn.execute("DELETE FROM scheduled_tasks WHERE id = ?", .{id});
        if (self.conn.changes() == 0) return error.TaskNotFound;
    }

    /// Claim up to `limit` due tasks, returning only the ones we won.
    ///
    /// The whole claim runs in one transaction so the SELECT and the
    /// UPDATEs cannot interleave with another worker's commit. The
    /// per-row `AND status = 'idle'` guard stays regardless: correctness
    /// here should not depend on the isolation level being what we think
    /// it is.
    pub fn claimDue(self: ScheduleRepo, gpa: Allocator, now: i64, limit: u32) Error!TaskList {
        var out = TaskList{ .gpa = gpa };
        errdefer out.deinit();
        if (limit == 0) return out;

        const Ctx = struct {
            repo: ScheduleRepo,
            gpa: Allocator,
            now: i64,
            limit: u32,
            out: *TaskList,
        };
        try tx.inTx(self.conn, Ctx{
            .repo = self,
            .gpa = gpa,
            .now = now,
            .limit = limit,
            .out = &out,
        }, struct {
            fn run(ctx: Ctx, conn: *Conn) Error!void {
                // Candidate ids first. Rides the partial
                // `scheduled_tasks_due` index over
                // `(next_run_at) WHERE enabled = 1 AND status = 'idle'`.
                var ids: std.ArrayList(i64) = .empty;
                defer ids.deinit(ctx.repo.gpa);
                {
                    var st = try conn.query(
                        \\SELECT id FROM scheduled_tasks
                        \\WHERE enabled = 1 AND status = 'idle' AND next_run_at <= ?
                        \\ORDER BY next_run_at ASC, id ASC
                        \\LIMIT ?
                    , .{ ctx.now, @as(i64, ctx.limit) });
                    defer st.release();
                    while (try st.step()) {
                        ids.append(ctx.repo.gpa, st.int(0)) catch return error.OutOfMemory;
                    }
                }

                for (ids.items) |id| {
                    try conn.execute(
                        \\UPDATE scheduled_tasks
                        \\SET status = 'running', claimed_at = ?, updated_at = ?
                        \\WHERE id = ? AND status = 'idle'
                    , .{ ctx.now, ctx.now, id });
                    // Zero rows changed means another worker got there
                    // first. Skip rather than fail: the point of the guard
                    // is that losing is normal.
                    if (conn.changes() != 1) continue;

                    var task = try ctx.repo.one(
                        ctx.gpa,
                        "SELECT " ++ columns ++ " FROM scheduled_tasks WHERE id = ?",
                        .{id},
                    );
                    errdefer task.deinit();
                    ctx.out.items.append(ctx.gpa, task) catch return error.OutOfMemory;
                }
            }
        }.run);
        return out;
    }

    /// Flip every `running` row back to `idle`.
    ///
    /// Called once at scheduler startup. A process that has just started
    /// owns no claims, so anything marked running belongs to a previous
    /// life and would otherwise be stuck forever.
    pub fn resetStaleClaims(self: ScheduleRepo, now: i64) Error!i64 {
        try self.conn.execute(
            \\UPDATE scheduled_tasks
            \\SET status = 'idle', claimed_at = NULL, updated_at = ?
            \\WHERE status = 'running'
        , .{now});
        return self.conn.changes();
    }

    fn one(self: ScheduleRepo, gpa: Allocator, sql: []const u8, args: anytype) Error!Task {
        var st = self.conn.queryRow(sql, args) catch |e| {
            if (e == error.NoRows) return error.TaskNotFound;
            return e;
        };
        defer st.release();
        return hydrate(gpa, &st);
    }

    fn hydrate(gpa: Allocator, st: *sqlite.Stmt) Error!Task {
        return Task.hydrate(gpa, .{
            .id = st.int(0),
            .name = st.text(1),
            .kind = schedule.Kind.parse(st.text(2)) orelse return error.UnknownEnum,
            .cadence_ms = st.optInt(3) orelse 0,
            .payload = st.bytes(4),
            .next_run_at = st.int(5),
            .last_run_at = st.optInt(6),
            .last_error = st.text(7),
            .consecutive_failures = @intCast(st.int(8)),
            .enabled = st.boolean(9),
            .status = schedule.Status.parse(st.text(10)) orelse return error.UnknownEnum,
            .claimed_at = st.optInt(11),
            .created_at = st.int(12),
            .updated_at = st.int(13),
        }) catch return error.OutOfMemory;
    }
};

/// A one-shot task has no cadence. NULL rather than 0 so "fires once" is
/// visible in the row itself, rather than being a magic number an operator
/// has to know about.
///
/// The Go original stored a duration *string* ("5m") here and parsed it on
/// load; milliseconds are stored instead, because the column feeds
/// `next_run_at = last_run_at + cadence` arithmetic and a format that has
/// to be parsed before it can be added is a format that can fail to load a
/// row.
fn cadenceValue(cadence_ms: i64) ?i64 {
    return if (cadence_ms <= 0) null else cadence_ms;
}

fn payloadValue(payload: []const u8) ?sqlite.Blob {
    return if (payload.len == 0) null else sqlite.blob(payload);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

const now0: i64 = 1_700_000_000_000;

fn recurring(name: []const u8, first_run: i64, cadence_ms: i64) !Task {
    return Task.init(t.allocator, .{
        .name = name,
        .kind = .recurring,
        .cadence_ms = cadence_ms,
        .first_run = first_run,
    }, now0);
}

test "a recurring task round-trips by name" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try recurring("outbox.prune", now0 + 5 * std.time.ms_per_min, 5 * std.time.ms_per_min);
    defer task.deinit();
    try r.save(&task);
    try t.expect(task.id != 0);

    var got = try r.byName(t.allocator, "outbox.prune");
    defer got.deinit();
    try t.expectEqual(schedule.Kind.recurring, got.kind);
    try t.expectEqual(@as(i64, 5 * std.time.ms_per_min), got.cadence_ms);
    try t.expectEqual(schedule.Status.idle, got.status);
    try t.expect(got.enabled);
}

test "a one-shot task stores a NULL cadence" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try Task.init(t.allocator, .{
        .name = "once",
        .kind = .oneshot,
        .first_run = now0,
    }, now0);
    defer task.deinit();
    try r.save(&task);

    // NULL, not 0: "fires once" is visible in the row without a magic
    // number an operator has to know about.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM scheduled_tasks WHERE cadence IS NULL",
        .{},
    ));
    var got = try r.byName(t.allocator, "once");
    defer got.deinit();
    try t.expectEqual(@as(i64, 0), got.cadence_ms);
    try t.expectEqual(schedule.Kind.oneshot, got.kind);
}

test "a payload blob survives round-tripping, NULs and all" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try Task.init(t.allocator, .{
        .name = "with.payload",
        .kind = .oneshot,
        .first_run = now0,
        .payload = "{\"a\":1}\x00binary",
    }, now0);
    defer task.deinit();
    try r.save(&task);

    var got = try r.byName(t.allocator, "with.payload");
    defer got.deinit();
    try t.expectEqualStrings("{\"a\":1}\x00binary", got.payload);
}

test "claimDue takes only due, enabled, idle tasks" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var due = try recurring("due.task", now0 - 10_000, std.time.ms_per_min);
    defer due.deinit();
    try r.save(&due);

    var future = try recurring("future.task", now0 + std.time.ms_per_hour, std.time.ms_per_min);
    defer future.deinit();
    try r.save(&future);

    var claimed = try r.claimDue(t.allocator, now0, 10);
    defer claimed.deinit();
    try t.expectEqual(@as(usize, 1), claimed.items.items.len);
    try t.expectEqualStrings("due.task", claimed.items.items[0].name);
    // The returned aggregate reflects the claim, so the caller does not
    // have to reload to know it owns the row.
    try t.expectEqual(schedule.Status.running, claimed.items.items[0].status);
    try t.expectEqual(@as(?i64, now0), claimed.items.items[0].claimed_at);
}

test "a claimed task cannot be claimed again" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var due = try recurring("due.task", now0 - 10_000, std.time.ms_per_min);
    defer due.deinit();
    try r.save(&due);

    var first = try r.claimDue(t.allocator, now0, 10);
    defer first.deinit();
    try t.expectEqual(@as(usize, 1), first.items.items.len);

    var second = try r.claimDue(t.allocator, now0, 10);
    defer second.deinit();
    try t.expectEqual(@as(usize, 0), second.items.items.len);
}

test "claimDue honours its limit and takes the most overdue first" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    for ([_]struct { name: []const u8, due: i64 }{
        .{ .name = "c", .due = now0 - 1_000 },
        .{ .name = "a", .due = now0 - 3_000 },
        .{ .name = "b", .due = now0 - 2_000 },
    }) |spec| {
        var task = try recurring(spec.name, spec.due, std.time.ms_per_min);
        defer task.deinit();
        try r.save(&task);
    }

    var claimed = try r.claimDue(t.allocator, now0, 2);
    defer claimed.deinit();
    try t.expectEqual(@as(usize, 2), claimed.items.items.len);
    try t.expectEqualStrings("a", claimed.items.items[0].name);
    try t.expectEqualStrings("b", claimed.items.items[1].name);
}

test "claimDue with a zero limit claims nothing" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);
    var due = try recurring("due.task", now0 - 10_000, std.time.ms_per_min);
    defer due.deinit();
    try r.save(&due);

    var claimed = try r.claimDue(t.allocator, now0, 0);
    defer claimed.deinit();
    try t.expectEqual(@as(usize, 0), claimed.items.items.len);
    // And the row is untouched.
    var got = try r.byName(t.allocator, "due.task");
    defer got.deinit();
    try t.expectEqual(schedule.Status.idle, got.status);
}

test "a disabled task is never claimed" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try recurring("off", now0 - 10_000, std.time.ms_per_min);
    defer task.deinit();
    task.enabled = false;
    try r.save(&task);

    var claimed = try r.claimDue(t.allocator, now0, 10);
    defer claimed.deinit();
    try t.expectEqual(@as(usize, 0), claimed.items.items.len);
}

test "resetStaleClaims makes a crashed task claimable again" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try recurring("stuck", now0 - 10_000, std.time.ms_per_min);
    defer task.deinit();
    try r.save(&task);

    // Claim it and never release — the crash-mid-handler case.
    var claimed = try r.claimDue(t.allocator, now0, 1);
    claimed.deinit();

    try t.expectEqual(@as(i64, 1), try r.resetStaleClaims(now0 + std.time.ms_per_min));
    var got = try r.byName(t.allocator, "stuck");
    defer got.deinit();
    try t.expectEqual(schedule.Status.idle, got.status);
    try t.expectEqual(@as(?i64, null), got.claimed_at);

    var again = try r.claimDue(t.allocator, now0 + std.time.ms_per_min, 10);
    defer again.deinit();
    try t.expectEqual(@as(usize, 1), again.items.items.len);
}

test "resetStaleClaims on a clean table is zero" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try t.expectEqual(@as(i64, 0), try ScheduleRepo.init(t.allocator, conn).resetStaleClaims(now0));
}

test "a one-shot task is disabled after it succeeds" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try Task.init(t.allocator, .{
        .name = "oneshot",
        .kind = .oneshot,
        .first_run = now0 - 10_000,
    }, now0);
    defer task.deinit();
    try r.save(&task);

    var claimed = try r.claimDue(t.allocator, now0, 1);
    defer claimed.deinit();
    try t.expectEqual(@as(usize, 1), claimed.items.items.len);
    try claimed.items.items[0].markSucceeded(now0);
    try r.save(&claimed.items.items[0]);

    var got = try r.byName(t.allocator, "oneshot");
    defer got.deinit();
    // Disabled rather than deleted, so an operator can still see it ran.
    try t.expect(!got.enabled);
}

test "a failure records its error and the consecutive count" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var task = try recurring("flaky", now0 - 10_000, std.time.ms_per_min);
    defer task.deinit();
    try r.save(&task);
    try task.markFailed("disk full", now0);
    try task.markFailed("disk still full", now0 + 1);
    try r.save(&task);

    var got = try r.byName(t.allocator, "flaky");
    defer got.deinit();
    try t.expectEqual(@as(u32, 2), got.consecutive_failures);
    try t.expectEqualStrings("disk still full", got.last_error);
}

test "a duplicate task name is DuplicateName" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    var first = try recurring("dup", now0, std.time.ms_per_min);
    defer first.deinit();
    try r.save(&first);
    var second = try recurring("dup", now0, std.time.ms_per_min);
    defer second.deinit();
    try t.expectError(error.DuplicateName, r.save(&second));
}

test "list is ordered by name and a missing task is TaskNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);

    for ([_][]const u8{ "zeta", "alpha", "mid" }) |name| {
        var task = try recurring(name, now0, std.time.ms_per_min);
        defer task.deinit();
        try r.save(&task);
    }
    var got = try r.list(t.allocator);
    defer got.deinit();
    try t.expectEqual(@as(usize, 3), got.items.items.len);
    try t.expectEqualStrings("alpha", got.items.items[0].name);
    try t.expectEqualStrings("mid", got.items.items[1].name);
    try t.expectEqualStrings("zeta", got.items.items[2].name);

    try t.expectError(error.TaskNotFound, r.byId(t.allocator, 9999));
    try t.expectError(error.TaskNotFound, r.byName(t.allocator, "nope"));
    try t.expectError(error.TaskNotFound, r.remove(9999));
}

test "an unknown kind or status is rejected rather than guessed" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ScheduleRepo.init(t.allocator, conn);
    var task = try recurring("weird", now0, std.time.ms_per_min);
    defer task.deinit();
    try r.save(&task);

    try conn.execute("UPDATE scheduled_tasks SET status = ? WHERE id = ?", .{ "vibing", task.id });
    try t.expectError(error.UnknownEnum, r.byId(t.allocator, task.id));
    try conn.execute("UPDATE scheduled_tasks SET status = 'idle', kind = ? WHERE id = ?", .{ "eventual", task.id });
    try t.expectError(error.UnknownEnum, r.byId(t.allocator, task.id));
}
