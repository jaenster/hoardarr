//! Persistence for the `deliver` aggregate: one `deliveries` row per Job
//! that reached the post-verify stage.
//!
//! Shape deliberately mirrors `repo_repair` and `repo_extract` — three
//! post-processing contexts with the same lifecycle, so one mental model
//! covers all three, and `UNIQUE(job_id)` makes retry idempotent in all
//! three the same way. Deliver has one extra terminal state, `skipped`,
//! for archive jobs that the extract context moves instead.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const deliver = @import("../domain/deliver.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Delivery = deliver.Delivery;

pub const Error = sqlite.Error || deliver.RepositoryError || error{UnknownState};

const columns = "id, job_id, state, target_dir, err_msg, created_at, started_at, finished_at";

pub const DeliveryRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) DeliveryRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: DeliveryRepo, x: *Delivery) Error!void {
        if (x.id == 0) {
            try self.conn.execute(
                \\INSERT INTO deliveries(job_id, state, target_dir, err_msg, created_at, started_at, finished_at)
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
            \\UPDATE deliveries SET state = ?, target_dir = ?, err_msg = ?, started_at = ?, finished_at = ?
            \\WHERE id = ?
        , .{
            x.state.toString(),
            x.target_dir,
            sqlite.nullIfEmpty(x.err_msg),
            x.started_at,
            x.finished_at,
            x.id,
        });
        if (self.conn.changes() == 0) return error.DeliveryNotFound;
    }

    pub fn byId(self: DeliveryRepo, gpa: Allocator, id: deliver.DeliveryId) Error!Delivery {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM deliveries WHERE id = ?", id);
    }

    pub fn byJobId(self: DeliveryRepo, gpa: Allocator, job_id: deliver.JobId) Error!Delivery {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM deliveries WHERE job_id = ?", job_id);
    }

    /// Rows the worker still owes work on. Rides the partial
    /// `deliveries_state` index, so the scan is over pending rows only.
    pub fn pending(self: DeliveryRepo, gpa: Allocator) Error!std.ArrayList(Delivery) {
        var out: std.ArrayList(Delivery) = .empty;
        errdefer {
            for (out.items) |*x| x.deinit();
            out.deinit(gpa);
        }
        var st = try self.conn.query(
            "SELECT " ++ columns ++ " FROM deliveries WHERE state IN ('pending','moving') ORDER BY id ASC",
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

    fn one(self: DeliveryRepo, gpa: Allocator, sql: []const u8, key: i64) Error!Delivery {
        var st = self.conn.queryRow(sql, .{key}) catch |e| {
            if (e == error.NoRows) return error.DeliveryNotFound;
            return e;
        };
        defer st.release();
        return hydrate(gpa, &st);
    }

    fn hydrate(gpa: Allocator, st: *sqlite.Stmt) Error!Delivery {
        return Delivery.hydrate(gpa, .{
            .id = st.int(0),
            .job_id = st.int(1),
            .state = deliver.State.parse(st.text(2)) orelse return error.UnknownState,
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

test "a delivery round-trips by id and by job" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 5);

    var d = try Delivery.init(t.allocator, 5, "/complete/tv/Show", 1000);
    defer d.deinit();
    try r.save(&d);
    try t.expect(d.id != 0);

    var by_id = try r.byId(t.allocator, d.id);
    defer by_id.deinit();
    var by_job = try r.byJobId(t.allocator, 5);
    defer by_job.deinit();
    try t.expectEqual(by_id.id, by_job.id);
    try t.expectEqualStrings("/complete/tv/Show", by_job.target_dir);
    try t.expectEqual(deliver.State.pending, by_job.state);
}

test "a completed delivery records both timestamps" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 6);

    var d = try Delivery.init(t.allocator, 6, "/out", 1000);
    defer d.deinit();
    try r.save(&d);
    try d.start(1100);
    try d.complete(1200);
    try r.save(&d);

    var got = try r.byJobId(t.allocator, 6);
    defer got.deinit();
    try t.expectEqual(deliver.State.complete, got.state);
    try t.expectEqual(@as(?i64, 1100), got.started_at);
    try t.expectEqual(@as(?i64, 1200), got.finished_at);
    try t.expectEqualStrings("", got.err_msg);
}

test "a skipped delivery is terminal and out of the pending set" {
    // Archive jobs are moved by the extract context, so their delivery
    // row exists only to record that deliver deliberately did nothing.
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 7);

    var d = try Delivery.init(t.allocator, 7, "/out", 1000);
    defer d.deinit();
    try r.save(&d);
    try d.skip(1100);
    try r.save(&d);

    var got = try r.byJobId(t.allocator, 7);
    defer got.deinit();
    try t.expectEqual(deliver.State.skipped, got.state);

    var list = try r.pending(t.allocator);
    defer {
        for (list.items) |*x| x.deinit();
        list.deinit(t.allocator);
    }
    try t.expectEqual(@as(usize, 0), list.items.len);
}

test "pending lists only unfinished deliveries" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 1);

    var open = try Delivery.init(t.allocator, 1, "/a", 1);
    defer open.deinit();
    try r.save(&open);
    try open.start(2);
    try r.save(&open);

    try migrate.seedJob(conn, 2);

    var closed = try Delivery.init(t.allocator, 2, "/b", 1);
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
    try t.expectEqual(deliver.State.moving, list.items[0].state);
}

test "a failed delivery keeps its error text" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);

    try migrate.seedJob(conn, 8);

    var d = try Delivery.init(t.allocator, 8, "/out", 1000);
    defer d.deinit();
    try r.save(&d);
    try d.start(1100);
    try d.fail("cross-device link failed", 1200);
    try r.save(&d);

    var got = try r.byJobId(t.allocator, 8);
    defer got.deinit();
    try t.expectEqual(deliver.State.failed, got.state);
    try t.expectEqualStrings("cross-device link failed", got.err_msg);
}

test "a missing delivery is DeliveryNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);
    try t.expectError(error.DeliveryNotFound, r.byId(t.allocator, 1));
    try t.expectError(error.DeliveryNotFound, r.byJobId(t.allocator, 1));
}

test "one job gets at most one delivery row" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = DeliveryRepo.init(t.allocator, conn);
    try migrate.seedJob(conn, 4);
    var a = try Delivery.init(t.allocator, 4, "/a", 1);
    defer a.deinit();
    try r.save(&a);
    try migrate.seedJob(conn, 4);
    var b = try Delivery.init(t.allocator, 4, "/a", 1);
    defer b.deinit();
    try t.expectError(error.ConstraintUnique, r.save(&b));
}
