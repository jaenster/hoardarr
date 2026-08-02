//! Rows written the way something *other than this code* writes them.
//!
//! Every other test under `src/store` builds its fixture by calling
//! `repo_*.save` and then reads it back. That proves the encoder and the
//! decoder agree with each other — which they always will, being two
//! halves of one author's round trip. It cannot prove either agrees with
//! the rows actually sitting on an operator's disk, which were written by
//! a different implementation and are still being read by this build.
//!
//! That gap cost an outage. Go's `encoding/json` marshals a nil
//! `[]string` as `null` rather than `[]`, so most `par2_sets.failed_files`
//! rows in the field held the literal text `null` — a shape no round trip
//! through this code can produce, and therefore a shape no round-trip
//! test could ever generate. The suite was green and the database was
//! unreadable.
//!
//! So the fixtures here are raw `INSERT` statements, and that is the
//! entire point of the file: **do not refactor them onto the
//! repositories.** A case built with `save` re-tests what a dozen other
//! files already test and loses every bit of the value this one has. The
//! SQL *is* the fixture; the repository appears only on the read side.
//!
//! The vocabularies come from the migration SQL — the `CHECK` constraints
//! and, where a column has none, the column comment — never from the Zig
//! enums. Reading them off the enums would reproduce the original mistake
//! of assuming the writer produced what this side expects.
//! `.insert_rejected` cases pin each `CHECK` list from the other
//! direction, so widening a constraint without teaching the matching enum
//! fails here rather than in production.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");

const repo_download = @import("repo_download.zig");
const repo_verify = @import("repo_verify.zig");
const repo_repair = @import("repo_repair.zig");
const repo_extract = @import("repo_extract.zig");
const repo_deliver = @import("repo_deliver.zig");
const repo_server = @import("repo_server.zig");
const repo_auth = @import("repo_auth.zig");
const repo_schedule = @import("repo_schedule.zig");
const repo_command = @import("repo_command.zig");
const repo_subscription = @import("repo_subscription.zig");
const repo_category = @import("repo_category.zig");
const repo_settings = @import("repo_settings.zig");
const repo_speed_history = @import("repo_speed_history.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const t = std.testing;

// ---------------------------------------------------------------------
// Case machinery
// ---------------------------------------------------------------------

/// What must happen when the repository reads the row back.
const Expect = union(enum) {
    /// Hydrates without complaint. The shape is one the schema permits,
    /// so refusing it would strand the row.
    ok,
    /// Hydrates, and the row's list column carries exactly this many
    /// entries. Zero is the documented degraded reading of `null`, `[]`
    /// and the empty string.
    list_len: usize,
    /// The repository refuses the row with exactly this error. Naming it
    /// matters: "some error" also passes when an unrelated code path
    /// happens to fail first, which is how a weak assertion survives
    /// against broken code.
    rejected: anyerror,
    /// The schema itself refuses the row. Pins a `CHECK` vocabulary so it
    /// cannot be widened without this file noticing.
    insert_rejected,
};

const Case = struct {
    /// The foreign shape this row stands for. Printed on failure, so it
    /// reads as the diagnosis rather than as a label.
    what: []const u8,
    /// Executed verbatim against a freshly migrated database. Several
    /// statements in one string are fine.
    sql: []const u8,
    expect: Expect = .ok,
};

/// Reads back the row a case inserted and returns the length of whatever
/// list the aggregate exposes, or 0 for a table that has none. Returning
/// a count rather than the aggregate lets one runner drive every
/// repository without knowing which type it is holding.
const Load = *const fn (*Conn, Allocator) anyerror!usize;

/// Parent rows a case needs before its own `INSERT` can land.
const Seed = enum { none, job };

fn run(cases: []const Case, seed: Seed, load: Load) !void {
    for (cases) |c| {
        // A fresh database per case: ids stay predictable and one case
        // cannot leave state that makes the next one pass.
        const conn = try migrate.openMigrated();
        defer conn.close();
        if (seed == .job) try migrate.seedJob(conn, 1);

        if (std.meta.activeTag(c.expect) == .insert_rejected) {
            if (conn.exec(c.sql)) |_| {
                std.debug.print("foreign corpus: schema accepted \"{s}\"\n", .{c.what});
                return error.TestUnexpectedResult;
            } else |_| continue;
        }

        conn.exec(c.sql) catch |e| {
            std.debug.print(
                "foreign corpus: fixture INSERT failed for \"{s}\": {s}\n",
                .{ c.what, @errorName(e) },
            );
            return e;
        };

        switch (c.expect) {
            .ok => _ = load(conn, t.allocator) catch |e| {
                std.debug.print(
                    "foreign corpus: \"{s}\" must hydrate, got {s}\n",
                    .{ c.what, @errorName(e) },
                );
                return e;
            },
            .list_len => |want| {
                const got = load(conn, t.allocator) catch |e| {
                    std.debug.print(
                        "foreign corpus: \"{s}\" must hydrate, got {s}\n",
                        .{ c.what, @errorName(e) },
                    );
                    return e;
                };
                if (got != want) {
                    std.debug.print(
                        "foreign corpus: \"{s}\" wanted {d} entries, got {d}\n",
                        .{ c.what, want, got },
                    );
                    return error.TestUnexpectedResult;
                }
            },
            .rejected => |want| {
                if (load(conn, t.allocator)) |_| {
                    std.debug.print(
                        "foreign corpus: \"{s}\" must be rejected with {s}, hydrated instead\n",
                        .{ c.what, @errorName(want) },
                    );
                    return error.TestUnexpectedResult;
                } else |got| if (got != want) {
                    std.debug.print(
                        "foreign corpus: \"{s}\" wanted {s}, got {s}\n",
                        .{ c.what, @errorName(want), @errorName(got) },
                    );
                    return error.TestUnexpectedResult;
                }
            },
            .insert_rejected => unreachable,
        }
    }
}

// ---------------------------------------------------------------------
// Row builders
//
// Comptime so that varying one column stays a one-line case instead of a
// hand-copied twelve-column INSERT per state.
// ---------------------------------------------------------------------

fn jobRow(comptime state: []const u8) []const u8 {
    return "INSERT INTO jobs(id, nzb_hash, name, category, priority, queue_order, state, " ++
        "total_bytes, done_bytes, failed_bytes, added_at, nzb_blob) " ++
        "VALUES (1, 'h1', 'Release.Name', 'tv', 0, 0, '" ++ state ++ "', 10, 0, 0, 1700000000000, x'00');";
}

fn fileRow(comptime state: []const u8, comptime groups: []const u8) []const u8 {
    const job = comptime jobRow("queued");
    return job ++
        "INSERT INTO files(id, job_id, filename, poster, groups, size_bytes, state, " ++
        "segment_count, segments_done) " ++
        "VALUES (1, 1, 'a.rar', 'p@example.invalid', '" ++ groups ++ "', 10, '" ++ state ++ "', 1, 0);";
}

fn segRow(comptime state: []const u8) []const u8 {
    const file = comptime fileRow("pending", "[]");
    return file ++
        "INSERT INTO segments(id, file_id, seq_index, message_id, bytes, state) " ++
        "VALUES (1, 1, 1, 'mid@example.invalid', 10, '" ++ state ++ "');";
}

fn par2Row(comptime state: []const u8, comptime failed_files: []const u8) []const u8 {
    return "INSERT INTO par2_sets(id, job_id, state, failed_files) " ++
        "VALUES (1, 1, '" ++ state ++ "', '" ++ failed_files ++ "');";
}

fn deliveryRow(comptime state: []const u8) []const u8 {
    return "INSERT INTO deliveries(id, job_id, state, target_dir, created_at) " ++
        "VALUES (1, 1, '" ++ state ++ "', '/complete/tv/Show', 1700000000000);";
}

fn extractRow(comptime state: []const u8) []const u8 {
    return "INSERT INTO extracts(id, job_id, state, target_dir, created_at) " ++
        "VALUES (1, 1, '" ++ state ++ "', '/complete/tv/Show', 1700000000000);";
}

fn repairRow(comptime state: []const u8) []const u8 {
    return "INSERT INTO repairs(id, job_id, state, created_at) " ++
        "VALUES (1, 1, '" ++ state ++ "', 1700000000000);";
}

fn subRow(comptime kind: []const u8, comptime topics: []const u8) []const u8 {
    return "INSERT INTO subscriptions(id, name, kind, url, topics, enabled, created_at, updated_at) " ++
        "VALUES (1, 'hook', '" ++ kind ++ "', 'https://example.invalid/h', '" ++ topics ++
        "', 1, 1700000000000, 1700000000000);";
}

fn serverRow(comptime billing: []const u8) []const u8 {
    return "INSERT INTO servers(id, name, host, port, tls, username, password, max_conns, " ++
        "priority, enabled, added_at, updated_at, backup, billing_mode, quota_bytes, " ++
        "used_bytes, bandwidth_bytes_per_sec) " ++
        "VALUES (1, 'main', 'news.example.invalid', 563, 1, 'u', 'p', 10, 0, 1, 1, 1, 0, '" ++
        billing ++ "', 0, 0, 0);";
}

fn userRow(comptime role: []const u8) []const u8 {
    return "INSERT INTO users(id, username, password_hash, role, created_at, updated_at) " ++
        "VALUES (1, 'admin', '$2a$10$abcdefghijklmnopqrstuv', '" ++ role ++ "', 1700000000000, 1700000000000);";
}

fn taskRow(comptime kind: []const u8, comptime status: []const u8) []const u8 {
    return "INSERT INTO scheduled_tasks(id, name, kind, cadence, payload, next_run_at, " ++
        "consecutive_failures, enabled, status, created_at, updated_at) " ++
        "VALUES (1, 'sweep', '" ++ kind ++ "', 300000, NULL, 1700000000000, 0, 1, '" ++
        status ++ "', 1700000000000, 1700000000000);";
}

fn commandRow(
    comptime trigger: []const u8,
    comptime status: []const u8,
    comptime result: []const u8,
) []const u8 {
    return "INSERT INTO commands(id, name, body, trigger, status, result, error, queued_at) " ++
        "VALUES (1, 'RefreshQueue', NULL, '" ++ trigger ++ "', '" ++ status ++ "', '" ++
        result ++ "', '', 1700000000000);";
}

// ---------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------

fn loadJob(conn: *Conn, gpa: Allocator) anyerror!usize {
    var j = try repo_download.JobRepo.init(gpa, conn).byId(gpa, 1);
    defer j.deinit();
    return if (j.files.len == 0) 0 else j.files[0].groups.len;
}

fn loadVerify(conn: *Conn, gpa: Allocator) anyerror!usize {
    var v = try repo_verify.VerifyRepo.init(gpa, conn).byId(gpa, 1);
    defer v.deinit();
    return v.failed_files.len;
}

fn loadDelivery(conn: *Conn, gpa: Allocator) anyerror!usize {
    var d = try repo_deliver.DeliveryRepo.init(gpa, conn).byJobId(gpa, 1);
    defer d.deinit();
    return 0;
}

fn loadExtract(conn: *Conn, gpa: Allocator) anyerror!usize {
    var x = try repo_extract.ExtractRepo.init(gpa, conn).byJobId(gpa, 1);
    defer x.deinit();
    return 0;
}

fn loadRepair(conn: *Conn, gpa: Allocator) anyerror!usize {
    var x = try repo_repair.RepairRepo.init(gpa, conn).byJobId(gpa, 1);
    defer x.deinit();
    return 0;
}

fn loadSubscription(conn: *Conn, gpa: Allocator) anyerror!usize {
    var s = try repo_subscription.SubscriptionRepo.init(gpa, conn).byId(gpa, 1);
    defer s.deinit();
    return s.topics.len;
}

fn loadServer(conn: *Conn, gpa: Allocator) anyerror!usize {
    var s = try repo_server.ServerRepo.init(gpa, conn).byId(gpa, 1);
    defer s.deinit();
    return 0;
}

fn loadUser(conn: *Conn, gpa: Allocator) anyerror!usize {
    var u = try repo_auth.UserRepo.init(gpa, conn).byId(gpa, 1);
    defer u.deinit();
    return 0;
}

fn loadSession(conn: *Conn, _: Allocator) anyerror!usize {
    const s = try repo_auth.SessionRepo.init(conn).get(session_token);
    return @intCast(s.user_id);
}

fn loadTask(conn: *Conn, gpa: Allocator) anyerror!usize {
    var task = try repo_schedule.ScheduleRepo.init(gpa, conn).byId(gpa, 1);
    defer task.deinit();
    return task.payload.len;
}

fn loadCommand(conn: *Conn, gpa: Allocator) anyerror!usize {
    var c = try repo_command.CommandRepo.init(gpa, conn).byId(gpa, 1);
    defer c.deinit();
    return c.body.len;
}

fn loadCategory(conn: *Conn, gpa: Allocator) anyerror!usize {
    const c = try repo_category.CategoryRepo.init(conn).get(gpa, "foreign");
    defer gpa.free(c.name);
    defer gpa.free(c.dir);
    return c.dir.len;
}

fn loadSetting(conn: *Conn, gpa: Allocator) anyerror!usize {
    const v = try repo_settings.SettingsRepo.init(conn).get(gpa, "foreign.key");
    defer gpa.free(v);
    return v.len;
}

fn loadSpeed(conn: *Conn, gpa: Allocator) anyerror!usize {
    var out = try repo_speed_history.SpeedHistoryRepo.init(conn).range(gpa, -1_000, 1 << 40);
    defer out.deinit(gpa);
    return out.items.len;
}

// ---------------------------------------------------------------------
// par2_sets — repo_verify
//
// State vocabulary from the column comment in 004_par2.sql:
// pending | verifying | ok | repair_needed | failed.
// ---------------------------------------------------------------------

const verify_cases = [_]Case{
    .{
        .what = "failed_files = null, the shape Go's encoding/json wrote for a nil slice",
        .sql = par2Row("repair_needed", "null"),
        .expect = .{ .list_len = 0 },
    },
    .{ .what = "failed_files = [] as written by a marshaller that emits empty arrays", .sql = par2Row("ok", "[]"), .expect = .{ .list_len = 0 } },
    .{ .what = "failed_files empty TEXT, from a writer predating the column", .sql = par2Row("ok", ""), .expect = .{ .list_len = 0 } },
    .{ .what = "failed_files whitespace only", .sql = par2Row("ok", "   "), .expect = .{ .list_len = 0 } },
    .{ .what = "failed_files populated", .sql = par2Row("repair_needed", "[\"a.r00\",\"a.r01\"]"), .expect = .{ .list_len = 2 } },
    .{ .what = "failed_files a JSON object", .sql = par2Row("ok", "{\"a\":1}"), .expect = .{ .rejected = error.MalformedFailedFiles } },
    .{ .what = "failed_files an array of numbers", .sql = par2Row("ok", "[1,2]"), .expect = .{ .rejected = error.MalformedFailedFiles } },
    .{ .what = "failed_files a bare JSON string", .sql = par2Row("ok", "\"a.r00\""), .expect = .{ .rejected = error.MalformedFailedFiles } },
    .{ .what = "state pending", .sql = par2Row("pending", "[]") },
    .{ .what = "state verifying", .sql = par2Row("verifying", "[]") },
    .{ .what = "state ok", .sql = par2Row("ok", "[]") },
    .{ .what = "state repair_needed", .sql = par2Row("repair_needed", "[]") },
    .{ .what = "state failed", .sql = par2Row("failed", "[]") },
    .{ .what = "state empty TEXT", .sql = par2Row("", "[]"), .expect = .{ .rejected = error.UnknownState } },
    .{ .what = "state in a casing this build does not know", .sql = par2Row("Verifying", "[]"), .expect = .{ .rejected = error.UnknownState } },
    .{
        .what = "nullable timestamps and error_msg all NULL",
        .sql = "INSERT INTO par2_sets(id, job_id, state, started_at, finished_at, error_msg, failed_files) " ++
            "VALUES (1, 1, 'pending', NULL, NULL, NULL, '[]');",
    },
    .{
        .what = "timestamps 0 where a wall-clock value is expected",
        .sql = "INSERT INTO par2_sets(id, job_id, state, started_at, finished_at, error_msg, failed_files) " ++
            "VALUES (1, 1, 'ok', 0, 0, '', '[]');",
    },
};

test "foreign par2_sets rows hydrate through VerifyRepo" {
    try run(&verify_cases, .job, &loadVerify);
}

// ---------------------------------------------------------------------
// jobs / files / segments — repo_download
//
// `jobs.state` carries no CHECK and no comment listing its values, so the
// vocabulary here is every state the writer could emit; the other two
// come from the column comments in 003_download.sql.
// ---------------------------------------------------------------------

const download_cases = [_]Case{
    .{ .what = "job state queued", .sql = jobRow("queued") },
    .{ .what = "job state downloading", .sql = jobRow("downloading") },
    .{ .what = "job state paused", .sql = jobRow("paused") },
    .{ .what = "job state download_complete", .sql = jobRow("download_complete") },
    .{ .what = "job state verifying", .sql = jobRow("verifying") },
    .{ .what = "job state repairing", .sql = jobRow("repairing") },
    .{ .what = "job state unpacking", .sql = jobRow("unpacking") },
    .{ .what = "job state completed", .sql = jobRow("completed") },
    .{ .what = "job state failed", .sql = jobRow("failed") },
    .{ .what = "job state aborted", .sql = jobRow("aborted") },
    .{ .what = "job state waiting_for_server", .sql = jobRow("waiting_for_server") },
    .{ .what = "job state written by a newer build", .sql = jobRow("quarantined"), .expect = .{ .rejected = error.UnknownState } },
    .{ .what = "job state empty TEXT", .sql = jobRow(""), .expect = .{ .rejected = error.UnknownState } },
    .{
        .what = "job with started_at, finished_at and error_msg NULL",
        .sql = "INSERT INTO jobs(id, nzb_hash, name, state, queue_order, total_bytes, added_at, " ++
            "started_at, finished_at, error_msg, nzb_blob) " ++
            "VALUES (1, 'h1', 'n', 'queued', 0, 0, 0, NULL, NULL, NULL, x'');",
    },
    .{
        .what = "job with empty name, hash, category and source",
        .sql = "INSERT INTO jobs(id, nzb_hash, name, category, source, state, queue_order, " ++
            "total_bytes, added_at, nzb_blob) " ++
            "VALUES (1, '', '', '', '', 'queued', 0, 0, 0, x'');",
    },
    .{
        .what = "job with added_at 0 and a zero-length nzb blob",
        .sql = "INSERT INTO jobs(id, nzb_hash, name, state, queue_order, total_bytes, added_at, nzb_blob) " ++
            "VALUES (1, 'h1', 'n', 'queued', 0, 0, 0, x'');",
    },
    .{
        .what = "job with done_bytes above total_bytes, as a counter flush race leaves it",
        .sql = "INSERT INTO jobs(id, nzb_hash, name, state, queue_order, total_bytes, done_bytes, " ++
            "added_at, nzb_blob) VALUES (1, 'h1', 'n', 'downloading', 0, 10, 99, 0, x'');",
    },
    .{
        .what = "job with fetch_recovery_vols 0",
        .sql = "INSERT INTO jobs(id, nzb_hash, name, state, queue_order, total_bytes, added_at, " ++
            "nzb_blob, fetch_recovery_vols) VALUES (1, 'h1', 'n', 'queued', 0, 0, 0, x'', 0);",
    },
    .{ .what = "file groups = null, Go's nil slice again", .sql = fileRow("pending", "null"), .expect = .{ .list_len = 0 } },
    .{ .what = "file groups = []", .sql = fileRow("pending", "[]"), .expect = .{ .list_len = 0 } },
    .{ .what = "file groups empty TEXT", .sql = fileRow("pending", ""), .expect = .{ .list_len = 0 } },
    .{ .what = "file groups populated", .sql = fileRow("pending", "[\"alt.binaries.a\",\"alt.binaries.b\"]"), .expect = .{ .list_len = 2 } },
    .{ .what = "file groups a JSON object", .sql = fileRow("pending", "{\"g\":1}"), .expect = .{ .rejected = error.BadGroupsJson } },
    .{ .what = "file groups an array of numbers", .sql = fileRow("pending", "[7]"), .expect = .{ .rejected = error.BadGroupsJson } },
    .{ .what = "file state pending", .sql = fileRow("pending", "[]") },
    .{ .what = "file state downloading", .sql = fileRow("downloading", "[]") },
    .{ .what = "file state complete", .sql = fileRow("complete", "[]") },
    .{ .what = "file state failed", .sql = fileRow("failed", "[]") },
    .{ .what = "file state written by a newer build", .sql = fileRow("assembling", "[]"), .expect = .{ .rejected = error.UnknownState } },
    .{
        .what = "file with NULL poster and an empty filename",
        .sql = jobRow("queued") ++
            "INSERT INTO files(id, job_id, filename, poster, groups, size_bytes, state, " ++
            "segment_count, segments_done, is_par2, is_recovery_vol) " ++
            "VALUES (1, 1, '', NULL, '[]', 0, 'pending', 0, 0, 1, 1);",
        .expect = .{ .list_len = 0 },
    },
    .{ .what = "segment state pending", .sql = segRow("pending") },
    .{ .what = "segment state inflight", .sql = segRow("inflight") },
    .{ .what = "segment state done", .sql = segRow("done") },
    .{ .what = "segment state missing", .sql = segRow("missing") },
    .{ .what = "segment state failed", .sql = segRow("failed") },
    .{ .what = "segment state written by a newer build", .sql = segRow("deferred"), .expect = .{ .rejected = error.UnknownState } },
    .{
        .what = "segment with NULL last_error and zeroed offset, attempts and retry clock",
        .sql = fileRow("pending", "[]") ++
            "INSERT INTO segments(id, file_id, seq_index, message_id, bytes, state, attempts, " ++
            "last_error, file_offset, next_retry_at) " ++
            "VALUES (1, 1, 1, '', 0, 'pending', 0, NULL, 0, 0);",
        .expect = .{ .list_len = 0 },
    },
};

test "foreign jobs, files and segments hydrate through JobRepo" {
    try run(&download_cases, .none, &loadJob);
}

// ---------------------------------------------------------------------
// deliveries — repo_deliver. CHECK in 006_deliver.sql.
// ---------------------------------------------------------------------

const deliver_cases = [_]Case{
    .{ .what = "state pending", .sql = deliveryRow("pending") },
    .{ .what = "state moving", .sql = deliveryRow("moving") },
    .{ .what = "state complete", .sql = deliveryRow("complete") },
    .{ .what = "state failed", .sql = deliveryRow("failed") },
    .{ .what = "state skipped", .sql = deliveryRow("skipped") },
    .{ .what = "state outside the CHECK list", .sql = deliveryRow("retrying"), .expect = .insert_rejected },
    .{
        .what = "unresolved target_dir, NULL err_msg, NULL transition timestamps",
        .sql = "INSERT INTO deliveries(id, job_id, state, target_dir, err_msg, created_at, " ++
            "started_at, finished_at) VALUES (1, 1, 'pending', '', NULL, 1700000000000, NULL, NULL);",
    },
    .{
        .what = "created_at 0 where a wall-clock value is expected",
        .sql = "INSERT INTO deliveries(id, job_id, state, created_at) VALUES (1, 1, 'pending', 0);",
    },
    .{
        .what = "err_msg empty TEXT rather than NULL",
        .sql = "INSERT INTO deliveries(id, job_id, state, err_msg, created_at) " ++
            "VALUES (1, 1, 'failed', '', 1700000000000);",
    },
};

test "foreign deliveries rows hydrate through DeliveryRepo" {
    try run(&deliver_cases, .job, &loadDelivery);
}

// ---------------------------------------------------------------------
// extracts — repo_extract. CHECK in 007_extract.sql.
// ---------------------------------------------------------------------

const extract_cases = [_]Case{
    .{ .what = "state pending", .sql = extractRow("pending") },
    .{ .what = "state extracting", .sql = extractRow("extracting") },
    .{ .what = "state complete", .sql = extractRow("complete") },
    .{ .what = "state failed", .sql = extractRow("failed") },
    .{ .what = "state outside the CHECK list", .sql = extractRow("skipped"), .expect = .insert_rejected },
    .{
        .what = "empty target_dir, NULL err_msg, NULL transition timestamps",
        .sql = "INSERT INTO extracts(id, job_id, state, target_dir, err_msg, created_at, " ++
            "started_at, finished_at) VALUES (1, 1, 'pending', '', NULL, 1700000000000, NULL, NULL);",
    },
    .{
        .what = "created_at 0",
        .sql = "INSERT INTO extracts(id, job_id, state, created_at) VALUES (1, 1, 'pending', 0);",
    },
};

test "foreign extracts rows hydrate through ExtractRepo" {
    try run(&extract_cases, .job, &loadExtract);
}

// ---------------------------------------------------------------------
// repairs — repo_repair. CHECK in 008_repair.sql.
// ---------------------------------------------------------------------

const repair_cases = [_]Case{
    .{ .what = "state pending", .sql = repairRow("pending") },
    .{ .what = "state repairing", .sql = repairRow("repairing") },
    .{ .what = "state ok", .sql = repairRow("ok") },
    .{ .what = "state failed", .sql = repairRow("failed") },
    .{ .what = "state outside the CHECK list", .sql = repairRow("complete"), .expect = .insert_rejected },
    .{
        .what = "NULL err_msg and NULL transition timestamps",
        .sql = "INSERT INTO repairs(id, job_id, state, err_msg, created_at, started_at, finished_at) " ++
            "VALUES (1, 1, 'pending', NULL, 1700000000000, NULL, NULL);",
    },
    .{
        .what = "err_msg empty TEXT and created_at 0",
        .sql = "INSERT INTO repairs(id, job_id, state, err_msg, created_at) VALUES (1, 1, 'failed', '', 0);",
    },
};

test "foreign repairs rows hydrate through RepairRepo" {
    try run(&repair_cases, .job, &loadRepair);
}

// ---------------------------------------------------------------------
// subscriptions — repo_subscription. 010_subscriptions.sql leaves `kind`
// unconstrained and documents 'webhook'; the enum is a superset, so every
// value it knows is exercised here.
// ---------------------------------------------------------------------

const subscription_cases = [_]Case{
    .{ .what = "topics = null, Go's nil slice", .sql = subRow("webhook", "null"), .expect = .{ .list_len = 0 } },
    .{ .what = "topics = []", .sql = subRow("webhook", "[]"), .expect = .{ .list_len = 0 } },
    .{ .what = "topics empty TEXT", .sql = subRow("webhook", ""), .expect = .{ .list_len = 0 } },
    .{ .what = "topics populated", .sql = subRow("webhook", "[\"deliver.*\",\"download.job.created\"]"), .expect = .{ .list_len = 2 } },
    .{ .what = "topics a JSON object", .sql = subRow("webhook", "{}"), .expect = .{ .rejected = error.MalformedTopics } },
    .{ .what = "topics an array of numbers", .sql = subRow("webhook", "[3]"), .expect = .{ .rejected = error.MalformedTopics } },
    .{ .what = "kind webhook", .sql = subRow("webhook", "[]"), .expect = .{ .list_len = 0 } },
    .{ .what = "kind discord", .sql = subRow("discord", "[]"), .expect = .{ .list_len = 0 } },
    .{ .what = "kind slack", .sql = subRow("slack", "[]"), .expect = .{ .list_len = 0 } },
    .{ .what = "kind this build does not know", .sql = subRow("pushover", "[]"), .expect = .{ .rejected = error.UnknownKind } },
    .{ .what = "kind empty TEXT", .sql = subRow("", "[]"), .expect = .{ .rejected = error.UnknownKind } },
    .{
        .what = "NULL secret and never-fired telemetry columns",
        .sql = "INSERT INTO subscriptions(id, name, kind, url, topics, secret, enabled, " ++
            "last_success_at, last_error_at, last_error, created_at, updated_at) " ++
            "VALUES (1, 'hook', 'webhook', 'https://example.invalid/h', '[]', NULL, 1, " ++
            "NULL, NULL, NULL, 1700000000000, 1700000000000);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "empty url, empty name, zeroed timestamps, disabled",
        .sql = "INSERT INTO subscriptions(id, name, kind, url, topics, enabled, created_at, updated_at) " ++
            "VALUES (1, '', 'webhook', '', '[]', 0, 0, 0);",
        .expect = .{ .list_len = 0 },
    },
};

test "foreign subscriptions rows hydrate through SubscriptionRepo" {
    try run(&subscription_cases, .none, &loadSubscription);
}

// ---------------------------------------------------------------------
// servers — repo_server. CHECKs in 002_servers.sql, 009_server_multi.sql
// and 011_bandwidth.sql.
// ---------------------------------------------------------------------

const server_cases = [_]Case{
    .{ .what = "billing_mode flat", .sql = serverRow("flat") },
    .{ .what = "billing_mode metered", .sql = serverRow("metered") },
    .{ .what = "billing_mode outside the CHECK list", .sql = serverRow("prepaid"), .expect = .insert_rejected },
    .{
        .what = "anonymous access: NULL username and password",
        .sql = "INSERT INTO servers(id, name, host, port, tls, username, password, max_conns, " ++
            "priority, enabled, added_at, updated_at) " ++
            "VALUES (1, 'main', 'news.example.invalid', 119, 0, NULL, NULL, 1, 0, 1, 1, 1);",
    },
    .{
        .what = "empty-string credentials, which Go wrote for an unset sql.NullString",
        .sql = "INSERT INTO servers(id, name, host, port, username, password, max_conns, added_at, updated_at) " ++
            "VALUES (1, 'main', 'news.example.invalid', 563, '', '', 1, 1, 1);",
    },
    .{
        .what = "empty name and host",
        .sql = "INSERT INTO servers(id, name, host, port, max_conns, added_at, updated_at) " ++
            "VALUES (1, '', '', 563, 1, 1, 1);",
    },
    .{
        .what = "zeroed counters, priority and timestamps",
        .sql = "INSERT INTO servers(id, name, host, port, max_conns, priority, added_at, updated_at, " ++
            "quota_bytes, used_bytes, bandwidth_bytes_per_sec) " ++
            "VALUES (1, 'main', 'h', 563, 1, 0, 0, 0, 0, 0, 0);",
    },
    .{
        .what = "disabled backup server with a metered quota",
        .sql = "INSERT INTO servers(id, name, host, port, tls, max_conns, enabled, backup, " ++
            "billing_mode, quota_bytes, used_bytes, added_at, updated_at) " ++
            "VALUES (1, 'bk', 'h', 563, 1, 4, 0, 1, 'metered', 1099511627776, 5, 1, 1);",
    },
    .{
        .what = "port 0",
        .sql = "INSERT INTO servers(id, name, host, port, max_conns, added_at, updated_at) " ++
            "VALUES (1, 'main', 'h', 0, 1, 1, 1);",
        .expect = .insert_rejected,
    },
    .{
        .what = "max_conns 0",
        .sql = "INSERT INTO servers(id, name, host, port, max_conns, added_at, updated_at) " ++
            "VALUES (1, 'main', 'h', 563, 0, 1, 1);",
        .expect = .insert_rejected,
    },
    .{
        .what = "negative used_bytes",
        .sql = "INSERT INTO servers(id, name, host, port, max_conns, added_at, updated_at, used_bytes) " ++
            "VALUES (1, 'main', 'h', 563, 1, 1, 1, -1);",
        .expect = .insert_rejected,
    },
    .{
        .what = "tls flag outside 0/1",
        .sql = "INSERT INTO servers(id, name, host, port, tls, max_conns, added_at, updated_at) " ++
            "VALUES (1, 'main', 'h', 563, 2, 1, 1, 1);",
        .expect = .insert_rejected,
    },
};

test "foreign servers rows hydrate through ServerRepo" {
    try run(&server_cases, .none, &loadServer);
}

// ---------------------------------------------------------------------
// users and sessions — repo_auth. 005_auth.sql puts no CHECK on `role`;
// its comment documents exactly one value.
// ---------------------------------------------------------------------

const user_cases = [_]Case{
    .{ .what = "role admin", .sql = userRow("admin") },
    .{ .what = "role this build does not know", .sql = userRow("viewer"), .expect = .{ .rejected = error.UnknownRole } },
    .{ .what = "role empty TEXT", .sql = userRow(""), .expect = .{ .rejected = error.UnknownRole } },
    .{
        .what = "empty password hash and zeroed timestamps",
        .sql = "INSERT INTO users(id, username, password_hash, role, created_at, updated_at) " ++
            "VALUES (1, 'admin', '', 'admin', 0, 0);",
    },
};

test "foreign users rows hydrate through UserRepo" {
    try run(&user_cases, .none, &loadUser);
}

/// 64 hex characters — `auth.session_token_len`.
const session_token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const user_for_session = userRow("admin");

const session_cases = [_]Case{
    .{
        .what = "a full-length token",
        .sql = user_for_session ++
            "INSERT INTO sessions(token, user_id, created_at, expires_at, last_seen) " ++
            "VALUES ('" ++ session_token ++ "', 1, 1700000000000, 1800000000000, 1700000000000);",
    },
    .{
        .what = "an already-expired session, which the repo must still return",
        .sql = user_for_session ++
            "INSERT INTO sessions(token, user_id, created_at, expires_at, last_seen) " ++
            "VALUES ('" ++ session_token ++ "', 1, 1, 2, 1);",
    },
    .{
        .what = "zeroed session timestamps",
        .sql = user_for_session ++
            "INSERT INTO sessions(token, user_id, created_at, expires_at, last_seen) " ++
            "VALUES ('" ++ session_token ++ "', 1, 0, 0, 0);",
    },
};

test "foreign sessions rows hydrate through SessionRepo" {
    try run(&session_cases, .none, &loadSession);
}

test "a session token of the wrong length is refused rather than truncated" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try conn.exec(user_for_session ++
        "INSERT INTO sessions(token, user_id, created_at, expires_at, last_seen) " ++
        "VALUES ('deadbeef', 1, 1, 2, 1);");

    try t.expectError(
        error.MalformedToken,
        repo_auth.SessionRepo.init(conn).get("deadbeef"),
    );
}

// ---------------------------------------------------------------------
// scheduled_tasks — repo_schedule. Vocabularies from the column comments
// in 016_scheduled_tasks.sql.
// ---------------------------------------------------------------------

const schedule_cases = [_]Case{
    .{ .what = "kind recurring", .sql = taskRow("recurring", "idle") },
    .{ .what = "kind oneshot", .sql = taskRow("oneshot", "idle") },
    .{ .what = "kind this build does not know", .sql = taskRow("cron", "idle"), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "kind empty TEXT", .sql = taskRow("", "idle"), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "status idle", .sql = taskRow("recurring", "idle") },
    .{ .what = "status running", .sql = taskRow("recurring", "running") },
    .{ .what = "status this build does not know", .sql = taskRow("recurring", "claimed"), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "status empty TEXT", .sql = taskRow("recurring", ""), .expect = .{ .rejected = error.UnknownEnum } },
    .{
        .what = "a oneshot: NULL cadence, NULL payload, never run",
        .sql = "INSERT INTO scheduled_tasks(id, name, kind, cadence, payload, next_run_at, " ++
            "last_run_at, last_error, consecutive_failures, enabled, status, claimed_at, " ++
            "created_at, updated_at) " ++
            "VALUES (1, 'once', 'oneshot', NULL, NULL, 1700000000000, NULL, NULL, 0, 1, 'idle', " ++
            "NULL, 1700000000000, 1700000000000);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "cadence 0, which is how a zero Go duration serialises",
        .sql = "INSERT INTO scheduled_tasks(id, name, kind, cadence, next_run_at, created_at, updated_at) " ++
            "VALUES (1, 'sweep', 'recurring', 0, 1700000000000, 1, 1);",
    },
    .{
        .what = "an empty payload blob rather than NULL",
        .sql = "INSERT INTO scheduled_tasks(id, name, kind, payload, next_run_at, created_at, updated_at) " ++
            "VALUES (1, 'sweep', 'recurring', x'', 1700000000000, 1, 1);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "a payload blob with content",
        .sql = "INSERT INTO scheduled_tasks(id, name, kind, payload, next_run_at, created_at, updated_at) " ++
            "VALUES (1, 'sweep', 'recurring', x'7B7D', 1700000000000, 1, 1);",
        .expect = .{ .list_len = 2 },
    },
    .{
        .what = "empty name, zeroed clocks, disabled, claimed",
        .sql = "INSERT INTO scheduled_tasks(id, name, kind, next_run_at, enabled, status, " ++
            "claimed_at, consecutive_failures, created_at, updated_at) " ++
            "VALUES (1, '', 'recurring', 0, 0, 'running', 0, 0, 0, 0);",
    },
    .{
        .what = "last_error empty TEXT rather than NULL",
        .sql = "INSERT INTO scheduled_tasks(id, name, kind, next_run_at, last_error, created_at, updated_at) " ++
            "VALUES (1, 'sweep', 'recurring', 1, '', 1, 1);",
    },
};

test "foreign scheduled_tasks rows hydrate through ScheduleRepo" {
    try run(&schedule_cases, .none, &loadTask);
}

// ---------------------------------------------------------------------
// commands — repo_command. Vocabularies from the column comments in
// 020_commands.sql.
// ---------------------------------------------------------------------

const command_cases = [_]Case{
    .{ .what = "trigger manual", .sql = commandRow("manual", "queued", "") },
    .{ .what = "trigger api", .sql = commandRow("api", "queued", "") },
    .{ .what = "trigger scheduled", .sql = commandRow("scheduled", "queued", "") },
    .{ .what = "trigger this build does not know", .sql = commandRow("cli", "queued", ""), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "trigger empty TEXT", .sql = commandRow("", "queued", ""), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "status queued", .sql = commandRow("manual", "queued", "") },
    .{ .what = "status running", .sql = commandRow("manual", "running", "") },
    .{ .what = "status completed", .sql = commandRow("manual", "completed", "successful") },
    .{ .what = "status this build does not know", .sql = commandRow("manual", "cancelled", ""), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "status empty TEXT", .sql = commandRow("manual", "", ""), .expect = .{ .rejected = error.UnknownEnum } },
    .{ .what = "result successful", .sql = commandRow("manual", "completed", "successful") },
    .{ .what = "result failed", .sql = commandRow("manual", "completed", "failed") },
    .{ .what = "result still empty because the command has not finished", .sql = commandRow("manual", "running", "") },
    .{ .what = "result this build does not know", .sql = commandRow("manual", "completed", "partial"), .expect = .{ .rejected = error.UnknownEnum } },
    .{
        .what = "NULL body and NULL transition timestamps",
        .sql = "INSERT INTO commands(id, name, body, trigger, status, result, error, queued_at, " ++
            "started_at, ended_at) " ++
            "VALUES (1, 'RefreshQueue', NULL, 'api', 'queued', '', '', 1700000000000, NULL, NULL);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "an empty body blob rather than NULL",
        .sql = "INSERT INTO commands(id, name, body, trigger, status, queued_at) " ++
            "VALUES (1, 'RefreshQueue', x'', 'api', 'queued', 1700000000000);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "a body blob with content",
        .sql = "INSERT INTO commands(id, name, body, trigger, status, queued_at) " ++
            "VALUES (1, 'RefreshQueue', x'7B7D', 'api', 'queued', 1700000000000);",
        .expect = .{ .list_len = 2 },
    },
    .{
        .what = "empty name and queued_at 0",
        .sql = "INSERT INTO commands(id, name, trigger, status, queued_at) VALUES (1, '', 'api', 'queued', 0);",
    },
};

test "foreign commands rows hydrate through CommandRepo" {
    try run(&command_cases, .none, &loadCommand);
}

test "an unfinished command's empty result column reads as no result at all" {
    // The column is NOT NULL DEFAULT '', so "no outcome yet" arrives as
    // the empty string; the aggregate models it as null. A repo that
    // parsed '' as an enum would reject every queued row on disk.
    const conn = try migrate.openMigrated();
    defer conn.close();
    try conn.exec(commandRow("api", "running", ""));

    var c = try repo_command.CommandRepo.init(t.allocator, conn).byId(t.allocator, 1);
    defer c.deinit();
    try t.expectEqual(@as(?@TypeOf(c.result.?), null), c.result);
}

// ---------------------------------------------------------------------
// categories, settings, speed_history — small tables, same treatment.
// ---------------------------------------------------------------------

const category_cases = [_]Case{
    .{
        .what = "empty dir, meaning complete_dir itself",
        .sql = "INSERT INTO categories(name, dir, post_script, priority, added_at, updated_at) " ++
            "VALUES ('foreign', '', NULL, 0, 1700000000000, 1700000000000);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "a relative dir and a post-processing script",
        .sql = "INSERT INTO categories(name, dir, post_script, priority, added_at, updated_at) " ++
            "VALUES ('foreign', 'tv', '/opt/pp.sh', 3, 1700000000000, 1700000000000);",
        .expect = .{ .list_len = 2 },
    },
    .{
        .what = "zeroed timestamps and priority",
        .sql = "INSERT INTO categories(name, dir, priority, added_at, updated_at) " ++
            "VALUES ('foreign', '', 0, 0, 0);",
        .expect = .{ .list_len = 0 },
    },
};

test "foreign categories rows hydrate through CategoryRepo" {
    try run(&category_cases, .none, &loadCategory);
}

const settings_cases = [_]Case{
    .{
        .what = "an empty value, which a foreign writer stores for an unset key",
        .sql = "INSERT INTO settings(key, value, updated_at) VALUES ('foreign.key', '', 1700000000000);",
        .expect = .{ .list_len = 0 },
    },
    .{
        .what = "a value with updated_at 0",
        .sql = "INSERT INTO settings(key, value, updated_at) VALUES ('foreign.key', 'ab', 0);",
        .expect = .{ .list_len = 2 },
    },
};

test "foreign settings rows hydrate through SettingsRepo" {
    try run(&settings_cases, .none, &loadSetting);
}

test "a settings value that is not a number is reported, not silently defaulted" {
    // A default here would hide a corrupt row behind plausible
    // behaviour, and the operator would never learn the knob is ignored.
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo_settings.SettingsRepo.init(conn);
    try conn.exec("INSERT INTO settings(key, value, updated_at) VALUES ('n', 'not-a-number', 0);");

    try t.expectError(error.Malformed, r.getIntOr("n", 7));
    // An absent key is a different thing entirely and does default.
    try t.expectEqual(@as(i64, 7), try r.getIntOr("absent", 7));
}

const speed_cases = [_]Case{
    .{
        .what = "a bucket at epoch 0 with a zero rate",
        .sql = "INSERT INTO speed_history(bucket_at, bytes_per_sec) VALUES (0, 0);",
        .expect = .{ .list_len = 1 },
    },
    .{
        .what = "an unaligned bucket, which our own writer never produces",
        .sql = "INSERT INTO speed_history(bucket_at, bytes_per_sec) VALUES (1700000037, 1024);",
        .expect = .{ .list_len = 1 },
    },
    .{
        .what = "a negative rate from a counter that wrapped",
        .sql = "INSERT INTO speed_history(bucket_at, bytes_per_sec) VALUES (1700000040, -1);",
        .expect = .{ .list_len = 1 },
    },
};

test "foreign speed_history rows hydrate through SpeedHistoryRepo" {
    try run(&speed_cases, .none, &loadSpeed);
}

// ---------------------------------------------------------------------
// Older schema versions
// ---------------------------------------------------------------------

/// Build a database at an older schema version.
///
/// `migrate.migrate` only ever runs to head, so an upgrade test has to
/// lay the older schema down itself. It does that by replaying the real
/// migration SQL out of `migrate.all` and writing the same bookkeeping
/// rows `applyOne` writes — a hand-copied historical schema would drift
/// from the one operators actually ran, and then the test would be
/// checking an upgrade nobody performs.
fn openAtVersion(target: u32) !*Conn {
    const conn = try sqlite.testing_support.openMemory();
    errdefer conn.close();

    // Creates `schema_migrations` as a side effect, so the table shape
    // comes from `migrate` rather than being duplicated here.
    _ = try migrate.currentVersion(conn);

    for (migrate.all) |mig| {
        if (mig.version > target) break;
        try conn.exec(mig.sql);
        try conn.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
            .{ @as(i64, mig.version), mig.name, @as(i64, 0) },
        );
    }
    return conn;
}

/// The three list columns as the predecessor left them: every one a
/// literal `null`, which is what `encoding/json` writes for a nil slice.
const null_lists_v22 = fileRow("pending", "null") ++
    "INSERT INTO par2_sets(id, job_id, state, failed_files) VALUES (1, 1, 'ok', 'null');" ++
    "INSERT INTO subscriptions(id, name, kind, url, topics, created_at, updated_at) " ++
    "VALUES (1, 'hook', 'webhook', 'https://example.invalid/h', 'null', 1, 1);";

test "list columns written as null before v23 survive the upgrade to head" {
    const conn = try openAtVersion(22);
    defer conn.close();
    try t.expectEqual(@as(u32, 22), try migrate.currentVersion(conn));

    try conn.exec(null_lists_v22);

    try migrate.migrate(conn);
    try t.expectEqual(migrate.latest_version, try migrate.currentVersion(conn));

    // 023 rewrites the literal, and the repositories agree with it.
    try t.expectEqual(@as(i64, 0), try conn.scalarInt(
        "SELECT COUNT(*) FROM files WHERE groups = 'null'",
        .{},
    ));
    try t.expectEqual(@as(usize, 0), try loadJob(conn, t.allocator));
    try t.expectEqual(@as(usize, 0), try loadVerify(conn, t.allocator));
    try t.expectEqual(@as(usize, 0), try loadSubscription(conn, t.allocator));
}

test "a v12 database gains its later columns with readable defaults" {
    // v12 predates jobs.source (013), files.is_recovery_vol and
    // jobs.fetch_recovery_vols (017) and segments.next_retry_at (021), so
    // these rows are written without ever naming those columns — exactly
    // what a database last touched by that build contains.
    const conn = try openAtVersion(12);
    defer conn.close();

    try conn.exec(
        "INSERT INTO jobs(id, nzb_hash, name, category, priority, queue_order, state, " ++
            "total_bytes, done_bytes, failed_bytes, added_at, nzb_blob) " ++
            "VALUES (1, 'h1', 'Old.Release', 'tv', 0, 0, 'downloading', 10, 5, 0, 1, x'00');" ++
            "INSERT INTO files(id, job_id, filename, poster, groups, size_bytes, state, " ++
            "segment_count, segments_done, is_par2) " ++
            "VALUES (1, 1, 'a.rar', NULL, 'null', 10, 'downloading', 1, 0, 0);" ++
            "INSERT INTO segments(id, file_id, seq_index, message_id, bytes, state, attempts, " ++
            "last_error, file_offset) VALUES (1, 1, 1, 'mid@x', 10, 'pending', 0, NULL, 0);",
    );

    try migrate.migrate(conn);
    try t.expectEqual(migrate.latest_version, try migrate.currentVersion(conn));

    var j = try repo_download.JobRepo.init(t.allocator, conn).byId(t.allocator, 1);
    defer j.deinit();

    try t.expectEqualStrings("", j.source);
    try t.expect(j.fetch_recovery_vols);
    try t.expectEqual(@as(usize, 1), j.files.len);
    try t.expectEqual(@as(usize, 0), j.files[0].groups.len);
    try t.expect(!j.files[0].is_recovery_vol);
    try t.expectEqual(@as(usize, 1), j.files[0].segments.len);
    try t.expectEqual(@as(?i64, null), j.files[0].segments[0].next_retry_at);
}
