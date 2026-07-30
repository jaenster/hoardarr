//! `commands`: the durable queue for one-off operator actions.
//!
//! Distinct from `scheduled_tasks`, which is cron-shaped. A command fires
//! once, is surfaced individually in the UI with its status and result, and
//! records who triggered it — so an operator can filter "what did *I*
//! start" rather than wading through the scheduler's noise.
//!
//! `claimNext` is a transactional SELECT-then-guarded-UPDATE, the same
//! shape as `repo_schedule.claimDue`: SQLite has no `SELECT ... FOR
//! UPDATE`, so `AND status = 'queued'` on the UPDATE is what proves
//! ownership. There is one worker today; the guard costs nothing and means
//! adding a second is a configuration change rather than a redesign.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const tx = @import("tx.zig");
const migrate = @import("migrate.zig");
const command = @import("../domain/command.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Command = command.Command;

pub const Error = sqlite.Error || command.RepositoryError || error{
    /// A stored `status`, `trigger` or `result` this binary does not know.
    UnknownEnum,
};

const columns = "id, name, body, trigger, status, result, error, queued_at, started_at, ended_at";

/// An owned result set.
pub const CommandList = struct {
    gpa: Allocator,
    items: std.ArrayList(Command) = .empty,

    pub fn deinit(self: *CommandList) void {
        for (self.items.items) |*c| c.deinit();
        self.items.deinit(self.gpa);
    }
};

/// Default and maximum page size for `list`. Command history is unbounded
/// in principle and the UI shows a page, so the repository clamps rather
/// than trusting the caller.
pub const default_list_limit: u32 = 50;
pub const max_list_limit: u32 = 500;

pub const CommandRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) CommandRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: CommandRepo, c: *Command) Error!void {
        if (c.id == 0) {
            try self.conn.execute(
                \\INSERT INTO commands(name, body, trigger, status, result, error,
                \\    queued_at, started_at, ended_at)
                \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            , .{
                c.name,
                bodyValue(c.body),
                c.trigger.toString(),
                c.status.toString(),
                resultText(c.result),
                c.err,
                c.queued_at,
                c.started_at,
                c.ended_at,
            });
            c.setId(self.conn.lastInsertRowid());
            return;
        }
        // Only the lifecycle columns are updatable. `name`, `body` and
        // `trigger` are what the command *is*; changing them after it is
        // queued would rewrite history.
        try self.conn.execute(
            \\UPDATE commands SET status = ?, result = ?, error = ?, started_at = ?, ended_at = ?
            \\WHERE id = ?
        , .{
            c.status.toString(),
            resultText(c.result),
            c.err,
            c.started_at,
            c.ended_at,
            c.id,
        });
        if (self.conn.changes() == 0) return error.CommandNotFound;
    }

    pub fn byId(self: CommandRepo, gpa: Allocator, id: command.CommandId) Error!Command {
        var st = self.conn.queryRow(
            "SELECT " ++ columns ++ " FROM commands WHERE id = ?",
            .{id},
        ) catch |e| {
            if (e == error.NoRows) return error.CommandNotFound;
            return e;
        };
        defer st.release();
        return hydrate(gpa, &st);
    }

    /// Most recent first. Rides the `commands_recent` index on `id DESC`.
    pub fn list(self: CommandRepo, gpa: Allocator, limit: u32) Error!CommandList {
        const n: i64 = @min(if (limit == 0) default_list_limit else limit, max_list_limit);
        var out = CommandList{ .gpa = gpa };
        errdefer out.deinit();
        var st = try self.conn.query(
            "SELECT " ++ columns ++ " FROM commands ORDER BY id DESC LIMIT ?",
            .{n},
        );
        defer st.release();
        while (try st.step()) {
            var c = try hydrate(gpa, &st);
            errdefer c.deinit();
            out.items.append(gpa, c) catch return error.OutOfMemory;
        }
        return out;
    }

    /// Claim the oldest queued command, or `null` when the queue is empty.
    ///
    /// An empty queue is the common case — this is polled — so "nothing to
    /// do" is a `null`, not an error to be caught on every tick.
    pub fn claimNext(self: CommandRepo, gpa: Allocator, now: i64) Error!?Command {
        const Ctx = struct {
            repo: CommandRepo,
            gpa: Allocator,
            now: i64,
            out: *?Command,
        };
        var out: ?Command = null;
        errdefer if (out) |*c| c.deinit();

        try tx.inTx(self.conn, Ctx{
            .repo = self,
            .gpa = gpa,
            .now = now,
            .out = &out,
        }, struct {
            fn run(ctx: Ctx, conn: *Conn) Error!void {
                const id = conn.scalarInt(
                    \\SELECT id FROM commands WHERE status = 'queued'
                    \\ORDER BY queued_at ASC, id ASC LIMIT 1
                , .{}) catch |e| {
                    if (e == error.NoRows) return;
                    return e;
                };
                try conn.execute(
                    \\UPDATE commands SET status = 'running', started_at = ?
                    \\WHERE id = ? AND status = 'queued'
                , .{ ctx.now, id });
                // Another claimer won. Normal, not an error.
                if (conn.changes() != 1) return;
                ctx.out.* = try ctx.repo.byId(ctx.gpa, id);
            }
        }.run);
        return out;
    }

    /// Requeue `running` commands claimed before `older_than`.
    ///
    /// Startup recovery: a crash mid-handler otherwise parks a command in
    /// `running` forever. Passing the process start time as `older_than`
    /// requeues everything a previous life had claimed without touching
    /// anything this one is working on.
    pub fn resetStaleClaims(self: CommandRepo, older_than: i64) Error!i64 {
        try self.conn.execute(
            \\UPDATE commands SET status = 'queued', started_at = NULL
            \\WHERE status = 'running' AND (started_at IS NULL OR started_at < ?)
        , .{older_than});
        return self.conn.changes();
    }

    fn hydrate(gpa: Allocator, st: *sqlite.Stmt) Error!Command {
        // `result` is stored as '' while the command is unfinished rather
        // than NULL, because the column is NOT NULL DEFAULT '' in the
        // schema; the aggregate models "no result yet" as null.
        const result_text = st.text(5);
        const result: ?command.Result = if (result_text.len == 0)
            null
        else
            command.Result.parse(result_text) orelse return error.UnknownEnum;

        return Command.hydrate(gpa, .{
            .id = st.int(0),
            .name = st.text(1),
            .body = st.bytes(2),
            .trigger = command.Trigger.parse(st.text(3)) orelse return error.UnknownEnum,
            .status = command.Status.parse(st.text(4)) orelse return error.UnknownEnum,
            .result = result,
            .err = st.text(6),
            .queued_at = st.int(7),
            .started_at = st.optInt(8),
            .ended_at = st.optInt(9),
        }) catch return error.OutOfMemory;
    }
};

fn bodyValue(body: []const u8) ?sqlite.Blob {
    return if (body.len == 0) null else sqlite.blob(body);
}

/// The schema's `result` column is `NOT NULL DEFAULT ''`, so "not finished"
/// is the empty string rather than NULL. Keeping it that way means the
/// existing rows on operators' disks stay readable.
fn resultText(result: ?command.Result) []const u8 {
    return if (result) |x| x.toString() else "";
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

const now0: i64 = 1_700_000_000_000;

fn queued(name: []const u8, trigger: command.Trigger, at: i64) !Command {
    return Command.init(t.allocator, .{ .name = name, .trigger = trigger }, at);
}

test "a queued command round-trips" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var c = try Command.init(t.allocator, .{
        .name = "RefreshCategories",
        .body = "{\"force\":true}",
        .trigger = .manual,
    }, now0);
    defer c.deinit();
    try r.save(&c);
    try t.expect(c.id != 0);

    var got = try r.byId(t.allocator, c.id);
    defer got.deinit();
    try t.expectEqualStrings("RefreshCategories", got.name);
    try t.expectEqualStrings("{\"force\":true}", got.body);
    try t.expectEqual(command.Trigger.manual, got.trigger);
    try t.expectEqual(command.Status.queued, got.status);
    // Not finished, so no result yet — not a result of "failed".
    try t.expectEqual(@as(?command.Result, null), got.result);
    try t.expectEqual(@as(i64, now0), got.queued_at);
}

test "claimNext takes the oldest queued command" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var older = try queued("first", .api, now0);
    defer older.deinit();
    try r.save(&older);
    var newer = try queued("second", .api, now0 + 1000);
    defer newer.deinit();
    try r.save(&newer);

    var claimed = (try r.claimNext(t.allocator, now0 + 2000)).?;
    defer claimed.deinit();
    try t.expectEqualStrings("first", claimed.name);
    try t.expectEqual(command.Status.running, claimed.status);
    try t.expectEqual(@as(?i64, now0 + 2000), claimed.started_at);
}

test "claimNext on an empty queue is null, not an error" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);
    // This is polled, so "nothing to do" must be cheap and ordinary.
    try t.expectEqual(@as(?Command, null), try r.claimNext(t.allocator, now0));
}

test "a claimed command is not claimed twice" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var c = try queued("only", .api, now0);
    defer c.deinit();
    try r.save(&c);

    var first = (try r.claimNext(t.allocator, now0)).?;
    first.deinit();
    try t.expectEqual(@as(?Command, null), try r.claimNext(t.allocator, now0));
}

test "a completed command records its result and error" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var c = try queued("Failing", .scheduled, now0);
    defer c.deinit();
    try r.save(&c);
    var claimed = (try r.claimNext(t.allocator, now0 + 10)).?;
    defer claimed.deinit();
    try claimed.markCompleted("upstream refused", now0 + 20);
    try r.save(&claimed);

    var got = try r.byId(t.allocator, claimed.id);
    defer got.deinit();
    try t.expectEqual(command.Status.completed, got.status);
    try t.expectEqual(command.Result.failed, got.result.?);
    try t.expectEqualStrings("upstream refused", got.err);
    try t.expectEqual(@as(?i64, now0 + 20), got.ended_at);
}

test "a successful command has no error text" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var c = try queued("Working", .manual, now0);
    defer c.deinit();
    try r.save(&c);
    var claimed = (try r.claimNext(t.allocator, now0 + 10)).?;
    defer claimed.deinit();
    try claimed.markCompleted(null, now0 + 20);
    try r.save(&claimed);

    var got = try r.byId(t.allocator, claimed.id);
    defer got.deinit();
    try t.expectEqual(command.Result.successful, got.result.?);
    try t.expectEqualStrings("", got.err);
}

test "resetStaleClaims requeues a command a previous process was running" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var c = try queued("Stuck", .api, now0);
    defer c.deinit();
    try r.save(&c);
    var claimed = (try r.claimNext(t.allocator, now0 + 10)).?;
    claimed.deinit();

    // Anything claimed before this process started belongs to a previous
    // life.
    try t.expectEqual(@as(i64, 1), try r.resetStaleClaims(now0 + 1000));
    var requeued = (try r.claimNext(t.allocator, now0 + 2000)).?;
    defer requeued.deinit();
    try t.expectEqualStrings("Stuck", requeued.name);
}

test "resetStaleClaims leaves a freshly claimed command alone" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    var c = try queued("Running", .api, now0);
    defer c.deinit();
    try r.save(&c);
    var claimed = (try r.claimNext(t.allocator, now0 + 5000)).?;
    claimed.deinit();

    try t.expectEqual(@as(i64, 0), try r.resetStaleClaims(now0 + 1000));
}

test "list returns most recent first and clamps its limit" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    for (0..5) |i| {
        var c = try queued("c", .api, now0 + @as(i64, @intCast(i)));
        defer c.deinit();
        try r.save(&c);
    }

    var all = try r.list(t.allocator, 0);
    defer all.deinit();
    try t.expectEqual(@as(usize, 5), all.items.items.len);
    // Newest first: descending id.
    try t.expect(all.items.items[0].id > all.items.items[1].id);

    var two = try r.list(t.allocator, 2);
    defer two.deinit();
    try t.expectEqual(@as(usize, 2), two.items.items.len);

    var huge = try r.list(t.allocator, 1_000_000);
    defer huge.deinit();
    try t.expectEqual(@as(usize, 5), huge.items.items.len);
}

test "a missing command is CommandNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try t.expectError(error.CommandNotFound, CommandRepo.init(t.allocator, conn).byId(t.allocator, 1));
}

test "every trigger value round-trips" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);

    for ([_]command.Trigger{ .manual, .api, .scheduled }) |trigger| {
        var c = try queued("x", trigger, now0);
        defer c.deinit();
        try r.save(&c);
        var got = try r.byId(t.allocator, c.id);
        defer got.deinit();
        try t.expectEqual(trigger, got.trigger);
    }
}

test "an unknown stored enum is rejected rather than guessed" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);
    var c = try queued("x", .api, now0);
    defer c.deinit();
    try r.save(&c);

    try conn.execute("UPDATE commands SET status = ? WHERE id = ?", .{ "levitating", c.id });
    try t.expectError(error.UnknownEnum, r.byId(t.allocator, c.id));
    try conn.execute("UPDATE commands SET status = 'queued', result = ? WHERE id = ?", .{ "mostly", c.id });
    try t.expectError(error.UnknownEnum, r.byId(t.allocator, c.id));
}

test "an empty body is NULL in the row and empty in the aggregate" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CommandRepo.init(t.allocator, conn);
    var c = try queued("nobody", .api, now0);
    defer c.deinit();
    try r.save(&c);

    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM commands WHERE body IS NULL", .{}));
    var got = try r.byId(t.allocator, c.id);
    defer got.deinit();
    try t.expectEqualStrings("", got.body);
}
