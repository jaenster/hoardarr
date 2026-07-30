//! Persistence for the `verify` aggregate: one `par2_sets` row per Job.
//!
//! `UNIQUE(job_id)` is what makes re-verification idempotent — a lookup
//! by job returns the one row or nothing, never two competing sets.
//!
//! `failed_files` is a JSON array in a single TEXT column. The list is
//! short, always read whole, and never queried by element, so a junction
//! table would add a join to every read in exchange for a query nobody
//! makes.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const verify = @import("../domain/verify.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const VerifySet = verify.VerifySet;

pub const Error = sqlite.Error || verify.RepositoryError || error{
    /// A stored state string this binary does not know: the database was
    /// written by a newer build.
    UnknownState,
    /// `failed_files` is not a JSON array of strings.
    MalformedFailedFiles,
};

const columns = "id, job_id, state, started_at, finished_at, error_msg, failed_files";

pub const VerifyRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) VerifyRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    /// Insert when the aggregate has no id yet, update otherwise.
    pub fn save(self: VerifyRepo, v: *VerifySet) Error!void {
        const failed = sqlite.string_array.encodeAlloc(self.gpa, v.failed_files) catch
            return error.OutOfMemory;
        defer self.gpa.free(failed);

        if (v.id == 0) {
            try self.conn.execute(
                \\INSERT INTO par2_sets(job_id, state, started_at, finished_at, error_msg, failed_files)
                \\VALUES (?, ?, ?, ?, ?, ?)
            , .{
                v.job_id,
                v.state.toString(),
                v.started_at,
                v.finished_at,
                sqlite.nullIfEmpty(v.error_msg),
                failed,
            });
            v.setId(self.conn.lastInsertRowid());
            return;
        }
        try self.conn.execute(
            \\UPDATE par2_sets SET
            \\    state = ?, started_at = ?, finished_at = ?,
            \\    error_msg = ?, failed_files = ?
            \\WHERE id = ?
        , .{
            v.state.toString(),
            v.started_at,
            v.finished_at,
            sqlite.nullIfEmpty(v.error_msg),
            failed,
            v.id,
        });
        if (self.conn.changes() == 0) return error.VerifySetNotFound;
    }

    pub fn byId(self: VerifyRepo, gpa: Allocator, id: verify.VerifySetId) Error!VerifySet {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM par2_sets WHERE id = ?", id);
    }

    /// The at-most-one set for a job, per `UNIQUE(job_id)`.
    pub fn byJobId(self: VerifyRepo, gpa: Allocator, job_id: verify.JobId) Error!VerifySet {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM par2_sets WHERE job_id = ?", job_id);
    }

    fn one(self: VerifyRepo, gpa: Allocator, sql: []const u8, key: i64) Error!VerifySet {
        var st = self.conn.queryRow(sql, .{key}) catch |e| {
            if (e == error.NoRows) return error.VerifySetNotFound;
            return e;
        };
        defer st.release();

        // The failed-files list is decoded into a scratch arena; `hydrate`
        // copies it into the aggregate's own allocator.
        var arena_state = std.heap.ArenaAllocator.init(self.gpa);
        defer arena_state.deinit();
        const failed = sqlite.string_array.decode(arena_state.allocator(), st.text(6)) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.MalformedJsonArray => return error.MalformedFailedFiles,
        };

        return VerifySet.hydrate(gpa, .{
            .id = st.int(0),
            .job_id = st.int(1),
            .state = verify.VerifyState.parse(st.text(2)) orelse return error.UnknownState,
            .started_at = st.optInt(3),
            .finished_at = st.optInt(4),
            .error_msg = st.text(5),
            .failed_files = failed,
        }) catch return error.OutOfMemory;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "a verify set round-trips including its failed-file list" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 42);

    var v = try VerifySet.init(t.allocator, 42, 1);
    defer v.deinit();
    _ = try v.markStarted(100);
    try r.save(&v);
    try t.expect(v.id != 0);

    try v.markRepairNeeded(&.{ "a.rar", "b\"quoted\".rar" }, 200);
    try r.save(&v);

    var got = try r.byJobId(t.allocator, 42);
    defer got.deinit();
    try t.expectEqual(verify.VerifyState.repair_needed, got.state);
    try t.expectEqual(@as(i64, 42), got.job_id);
    try t.expectEqual(@as(?i64, 100), got.started_at);
    try t.expectEqual(@as(usize, 2), got.failed_files.len);
    try t.expectEqualStrings("a.rar", got.failed_files[0]);
    try t.expectEqualStrings("b\"quoted\".rar", got.failed_files[1]);
}

test "an ok set has no failed files and no error message" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 7);

    var v = try VerifySet.init(t.allocator, 7, 1);
    defer v.deinit();
    _ = try v.markStarted(10);
    try v.markOk(20);
    try r.save(&v);

    var got = try r.byId(t.allocator, v.id);
    defer got.deinit();
    try t.expectEqual(verify.VerifyState.ok, got.state);
    try t.expectEqual(@as(usize, 0), got.failed_files.len);
    try t.expectEqualStrings("", got.error_msg);
    // Absent, not empty: the column is NULL so `IS NOT NULL` still works.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM par2_sets WHERE error_msg IS NULL",
        .{},
    ));
}

test "a missing set is VerifySetNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);
    try t.expectError(error.VerifySetNotFound, r.byId(t.allocator, 1));
    try t.expectError(error.VerifySetNotFound, r.byJobId(t.allocator, 1));
}

test "one job can only have one verify set" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 3);

    var first = try VerifySet.init(t.allocator, 3, 1);
    defer first.deinit();
    try r.save(&first);

    // UNIQUE(job_id) is what makes "re-verify" reuse the row instead of
    // racing a second one into existence.
    try migrate.seedJob(conn, 3);
    var second = try VerifySet.init(t.allocator, 3, 1);
    defer second.deinit();
    try t.expectError(error.ConstraintUnique, r.save(&second));
}

test "an unknown state string is rejected rather than guessed" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 9);
    var v = try VerifySet.init(t.allocator, 9, 1);
    defer v.deinit();
    try r.save(&v);

    try conn.execute("UPDATE par2_sets SET state = ? WHERE id = ?", .{ "quantum", v.id });
    try t.expectError(error.UnknownState, r.byId(t.allocator, v.id));
}

test "a malformed failed-files column is reported, not silently dropped" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 11);
    var v = try VerifySet.init(t.allocator, 11, 1);
    defer v.deinit();
    try r.save(&v);

    try conn.execute("UPDATE par2_sets SET failed_files = ? WHERE id = ?", .{ "not json", v.id });
    try t.expectError(error.MalformedFailedFiles, r.byId(t.allocator, v.id));
}

test "updating a row that has been deleted underneath us is reported" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = VerifyRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 13);
    var v = try VerifySet.init(t.allocator, 13, 1);
    defer v.deinit();
    try r.save(&v);

    try conn.execute("DELETE FROM par2_sets WHERE id = ?", .{v.id});
    _ = try v.markStarted(50);
    try t.expectError(error.VerifySetNotFound, r.save(&v));
}
