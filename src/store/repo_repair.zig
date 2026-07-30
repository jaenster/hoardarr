//! Persistence for the `repair` aggregate: one `repairs` row per damaged
//! Job.
//!
//! Shape deliberately mirrors `repo_extract` and `repo_deliver` — three
//! post-processing contexts with the same lifecycle, so keeping the query
//! patterns uniform means one mental model covers all three, and
//! `UNIQUE(job_id)` makes retry idempotent in all three the same way.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const repair = @import("../domain/repair.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Repair = repair.Repair;

pub const Error = sqlite.Error || repair.RepositoryError || error{UnknownState};

const columns = "id, job_id, state, err_msg, created_at, started_at, finished_at";

pub const RepairRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) RepairRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: RepairRepo, x: *Repair) Error!void {
        if (x.id == 0) {
            try self.conn.execute(
                \\INSERT INTO repairs(job_id, state, err_msg, created_at, started_at, finished_at)
                \\VALUES (?, ?, ?, ?, ?, ?)
            , .{
                x.job_id,
                x.state.toString(),
                sqlite.nullIfEmpty(x.err),
                x.created_at,
                x.started_at,
                x.finished_at,
            });
            x.setId(self.conn.lastInsertRowid());
            return;
        }
        try self.conn.execute(
            \\UPDATE repairs SET state = ?, err_msg = ?, started_at = ?, finished_at = ?
            \\WHERE id = ?
        , .{
            x.state.toString(),
            sqlite.nullIfEmpty(x.err),
            x.started_at,
            x.finished_at,
            x.id,
        });
        if (self.conn.changes() == 0) return error.RepairNotFound;
    }

    pub fn byId(self: RepairRepo, gpa: Allocator, id: repair.RepairId) Error!Repair {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM repairs WHERE id = ?", id);
    }

    pub fn byJobId(self: RepairRepo, gpa: Allocator, job_id: repair.JobId) Error!Repair {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM repairs WHERE job_id = ?", job_id);
    }

    /// Rows the worker still owes work on. Rides the partial
    /// `repairs_state` index, so the scan is over pending rows only.
    pub fn pending(self: RepairRepo, gpa: Allocator) Error!std.ArrayList(Repair) {
        var out: std.ArrayList(Repair) = .empty;
        errdefer {
            for (out.items) |*x| x.deinit();
            out.deinit(gpa);
        }
        var st = try self.conn.query(
            "SELECT " ++ columns ++ " FROM repairs WHERE state IN ('pending','repairing') ORDER BY id ASC",
            .{},
        );
        defer st.release();
        while (try st.step()) {
            var x = try hydrate(gpa, &st);
            errdefer x.deinit();
            out.append(gpa, x) catch return error.OutOfMemory;
        }
        return out;
    }

    fn one(self: RepairRepo, gpa: Allocator, sql: []const u8, key: i64) Error!Repair {
        var st = self.conn.queryRow(sql, .{key}) catch |e| {
            if (e == error.NoRows) return error.RepairNotFound;
            return e;
        };
        defer st.release();
        return hydrate(gpa, &st);
    }

    fn hydrate(gpa: Allocator, st: *sqlite.Stmt) Error!Repair {
        return Repair.hydrate(gpa, .{
            .id = st.int(0),
            .job_id = st.int(1),
            .state = repair.State.parse(st.text(2)) orelse return error.UnknownState,
            .err = st.text(3),
            .created_at = st.int(4),
            .started_at = st.optInt(5),
            .finished_at = st.optInt(6),
        }) catch return error.OutOfMemory;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "a repair round-trips by id and by job" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = RepairRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 5);

    var x = try Repair.init(t.allocator, 5, 1000);
    defer x.deinit();
    try r.save(&x);
    try t.expect(x.id != 0);

    var by_id = try r.byId(t.allocator, x.id);
    defer by_id.deinit();
    var by_job = try r.byJobId(t.allocator, 5);
    defer by_job.deinit();
    try t.expectEqual(by_id.id, by_job.id);
    try t.expectEqual(@as(i64, 1000), by_job.created_at);
    try t.expectEqual(repair.State.pending, by_job.state);
}

test "a failed repair keeps its error text" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = RepairRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 6);

    var x = try Repair.init(t.allocator, 6, 1000);
    defer x.deinit();
    try r.save(&x);
    try x.start(1100);
    try x.markFailed("not enough recovery slices", 1200);
    try r.save(&x);

    var got = try r.byJobId(t.allocator, 6);
    defer got.deinit();
    try t.expectEqual(repair.State.failed, got.state);
    try t.expectEqualStrings("not enough recovery slices", got.err);
    try t.expectEqual(@as(?i64, 1100), got.started_at);
    try t.expectEqual(@as(?i64, 1200), got.finished_at);
}

test "pending lists only unfinished repairs" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = RepairRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 1);

    var open = try Repair.init(t.allocator, 1, 1);
    defer open.deinit();
    try r.save(&open);

    try migrate.seedJob(conn, 2);

    var closed = try Repair.init(t.allocator, 2, 1);
    defer closed.deinit();
    try r.save(&closed);
    try closed.start(2);
    try closed.markOk(3);
    try r.save(&closed);

    var list = try r.pending(t.allocator);
    defer {
        for (list.items) |*x| x.deinit();
        list.deinit(t.allocator);
    }
    try t.expectEqual(@as(usize, 1), list.items.len);
    try t.expectEqual(@as(i64, 1), list.items[0].job_id);
}

test "a missing repair is RepairNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = RepairRepo.init(t.allocator, conn);
    try t.expectError(error.RepairNotFound, r.byId(t.allocator, 1));
    try t.expectError(error.RepairNotFound, r.byJobId(t.allocator, 1));
}

test "one job gets at most one repair row" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = RepairRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 4);
    var a = try Repair.init(t.allocator, 4, 1);
    defer a.deinit();
    try r.save(&a);
    try migrate.seedJob(conn, 4);
    var b = try Repair.init(t.allocator, 4, 1);
    defer b.deinit();
    try t.expectError(error.ConstraintUnique, r.save(&b));
}

test "an unknown state string is rejected" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = RepairRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 8);
    var x = try Repair.init(t.allocator, 8, 1);
    defer x.deinit();
    try r.save(&x);
    // The CHECK constraint guards the states the schema knows; this
    // simulates a row written by a build that knows one more.
    try conn.exec("PRAGMA ignore_check_constraints = ON");
    try conn.execute("UPDATE repairs SET state = ? WHERE id = ?", .{ "vibing", x.id });
    try t.expectError(error.UnknownState, r.byId(t.allocator, x.id));
}
