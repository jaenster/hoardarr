//! Persistence for the `extract` aggregate: one `extracts` row per
//! archive Job.
//!
//! Shape deliberately mirrors `repo_repair` and `repo_deliver` — three
//! post-processing contexts with the same lifecycle, so one mental model
//! covers all three, and `UNIQUE(job_id)` makes retry idempotent in all
//! three the same way.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const extract = @import("../domain/extract.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Extract = extract.Extract;

pub const Error = sqlite.Error || extract.RepositoryError || error{UnknownState};

const columns = "id, job_id, state, target_dir, err_msg, created_at, started_at, finished_at";

pub const ExtractRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) ExtractRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: ExtractRepo, x: *Extract) Error!void {
        if (x.id == 0) {
            try self.conn.execute(
                \\INSERT INTO extracts(job_id, state, target_dir, err_msg, created_at, started_at, finished_at)
                \\VALUES (?, ?, ?, ?, ?, ?, ?)
            , .{
                x.job_id,
                x.state.toString(),
                x.target_dir,
                sqlite.nullIfEmpty(x.err_msg),
                x.created_at,
                x.started_at,
                x.finished_at,
            });
            x.setId(self.conn.lastInsertRowid());
            return;
        }
        try self.conn.execute(
            \\UPDATE extracts SET state = ?, target_dir = ?, err_msg = ?, started_at = ?, finished_at = ?
            \\WHERE id = ?
        , .{
            x.state.toString(),
            x.target_dir,
            sqlite.nullIfEmpty(x.err_msg),
            x.started_at,
            x.finished_at,
            x.id,
        });
        if (self.conn.changes() == 0) return error.ExtractNotFound;
    }

    pub fn byId(self: ExtractRepo, gpa: Allocator, id: extract.ExtractId) Error!Extract {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM extracts WHERE id = ?", id);
    }

    pub fn byJobId(self: ExtractRepo, gpa: Allocator, job_id: extract.JobId) Error!Extract {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM extracts WHERE job_id = ?", job_id);
    }

    /// Rows the worker still owes work on. Rides the partial
    /// `extracts_state` index, so the scan is over pending rows only.
    pub fn pending(self: ExtractRepo, gpa: Allocator) Error!std.ArrayList(Extract) {
        var out: std.ArrayList(Extract) = .empty;
        errdefer {
            for (out.items) |*x| x.deinit();
            out.deinit(gpa);
        }
        var st = try self.conn.query(
            "SELECT " ++ columns ++ " FROM extracts WHERE state IN ('pending','extracting') ORDER BY id ASC",
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

    fn one(self: ExtractRepo, gpa: Allocator, sql: []const u8, key: i64) Error!Extract {
        var st = self.conn.queryRow(sql, .{key}) catch |e| {
            if (e == error.NoRows) return error.ExtractNotFound;
            return e;
        };
        defer st.release();
        return hydrate(gpa, &st);
    }

    fn hydrate(gpa: Allocator, st: *sqlite.Stmt) Error!Extract {
        return Extract.hydrate(gpa, .{
            .id = st.int(0),
            .job_id = st.int(1),
            .state = extract.State.parse(st.text(2)) orelse return error.UnknownState,
            .target_dir = st.text(3),
            .err_msg = st.text(4),
            .created_at = st.int(5),
            .started_at = st.optInt(6),
            .finished_at = st.optInt(7),
        }) catch return error.OutOfMemory;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "an extract round-trips by id and by job" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ExtractRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 5);

    var x = try Extract.init(t.allocator, 5, "/complete/tv/Show", 1000);
    defer x.deinit();
    try r.save(&x);
    try t.expect(x.id != 0);

    var by_id = try r.byId(t.allocator, x.id);
    defer by_id.deinit();
    var by_job = try r.byJobId(t.allocator, 5);
    defer by_job.deinit();
    try t.expectEqual(by_id.id, by_job.id);
    try t.expectEqualStrings("/complete/tv/Show", by_job.target_dir);
    try t.expectEqual(extract.State.pending, by_job.state);
    try t.expectEqual(@as(i64, 1000), by_job.created_at);
}

test "a failed extract keeps its error text and timestamps" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ExtractRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 6);

    var x = try Extract.init(t.allocator, 6, "/out", 1000);
    defer x.deinit();
    try r.save(&x);
    try x.start(1100);
    try x.fail("corrupt rar header", 1200);
    try r.save(&x);

    var got = try r.byJobId(t.allocator, 6);
    defer got.deinit();
    try t.expectEqual(extract.State.failed, got.state);
    try t.expectEqualStrings("corrupt rar header", got.err_msg);
    try t.expectEqual(@as(?i64, 1100), got.started_at);
    try t.expectEqual(@as(?i64, 1200), got.finished_at);
}

test "pending lists only unfinished extracts" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ExtractRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 1);

    var open = try Extract.init(t.allocator, 1, "/a", 1);
    defer open.deinit();
    try r.save(&open);

    try migrate.seedJob(conn, 2);

    var closed = try Extract.init(t.allocator, 2, "/b", 1);
    defer closed.deinit();
    try r.save(&closed);
    try closed.start(2);
    try closed.complete(3);
    try r.save(&closed);

    var list = try r.pending(t.allocator);
    defer {
        for (list.items) |*x| x.deinit();
        list.deinit(t.allocator);
    }
    try t.expectEqual(@as(usize, 1), list.items.len);
    try t.expectEqual(@as(i64, 1), list.items[0].job_id);
}

test "a missing extract is ExtractNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ExtractRepo.init(t.allocator, conn);
    try t.expectError(error.ExtractNotFound, r.byId(t.allocator, 1));
    try t.expectError(error.ExtractNotFound, r.byJobId(t.allocator, 1));
}

test "one job gets at most one extract row" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ExtractRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 4);
    var a = try Extract.init(t.allocator, 4, "/a", 1);
    defer a.deinit();
    try r.save(&a);
    try migrate.seedJob(conn, 4);
    var b = try Extract.init(t.allocator, 4, "/a", 1);
    defer b.deinit();
    try t.expectError(error.ConstraintUnique, r.save(&b));
}

test "an unknown extract state string is rejected" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ExtractRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 8);
    var x = try Extract.init(t.allocator, 8, "/a", 1);
    defer x.deinit();
    try r.save(&x);
    // The CHECK constraint guards the states the schema knows; this
    // simulates a row written by a build that knows one more.
    try conn.exec("PRAGMA ignore_check_constraints = ON");
    try conn.execute("UPDATE extracts SET state = ? WHERE id = ?", .{ "unpacking_sideways", x.id });
    try t.expectError(error.UnknownState, r.byId(t.allocator, x.id));
}
