//! Forward-only schema migrations.
//!
//! Each migration is a numbered SQL file in `migrations/`, embedded into
//! the binary with `@embedFile` so a container has no data files to
//! mount and no ordering ambiguity at startup. Naming is
//! `NNN_description.sql`; `NNN` is the version and the description is
//! for humans.
//!
//! Zig has no directory-walking equivalent of Go's `embed.FS`, so the
//! list below is explicit. That is not a downgrade: the Go version
//! sorted filenames at runtime and then had to *check for duplicate
//! versions*, because a typo produced two `007`s that the compiler
//! never saw. Here a duplicate or out-of-order version is a comptime
//! error, and a file that exists on disk but is missing from the list
//! cannot be applied by accident on one machine and not another.
//!
//! **Forward-only.** A migration that has shipped has already run on
//! operators' databases; editing it changes nothing for them and
//! silently diverges new installs from old ones. The fix for a bad
//! migration is always another migration.
//!
//! Each file is applied in its own transaction, so a failure leaves the
//! database at the previous version rather than half-migrated.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const tx = @import("tx.zig");

const Conn = sqlite.Conn;

/// One forward-only schema change.
pub const Migration = struct {
    version: u32,
    /// Filename minus the `NNN_` prefix and `.sql` suffix.
    name: []const u8,
    sql: []const u8,
};

/// Every migration, in application order.
///
/// Adding one means adding a file *and* a line here. The comptime block
/// below rejects a version that is not strictly greater than its
/// predecessor, which catches both duplicates and a mis-sorted insert.
pub const all = [_]Migration{
    m(1, "outbox"),
    m(2, "servers"),
    m(3, "download"),
    m(4, "par2"),
    m(5, "auth"),
    m(6, "deliver"),
    m(7, "extract"),
    m(8, "repair"),
    m(9, "server_multi"),
    m(10, "subscriptions"),
    m(11, "bandwidth"),
    m(12, "default_categories"),
    m(13, "job_source"),
    m(14, "jobs_history_index"),
    m(15, "outbox_subs_event_id"),
    m(16, "scheduled_tasks"),
    m(17, "recovery_vols"),
    m(18, "settings"),
    m(19, "more_arr_categories"),
    m(20, "commands"),
    m(21, "segment_retry"),
    m(22, "speed_history"),
};

/// Build a `Migration` from its version and name, deriving the embedded
/// path so the filename and the declared version can never disagree.
fn m(comptime version: u32, comptime name: []const u8) Migration {
    const path = std.fmt.comptimePrint("migrations/{d:0>3}_{s}.sql", .{ version, name });
    return .{ .version = version, .name = name, .sql = @embedFile(path) };
}

comptime {
    for (all[1..], all[0 .. all.len - 1]) |cur, prev| {
        if (cur.version <= prev.version) {
            @compileError(std.fmt.comptimePrint(
                "migrations out of order or duplicated: {d} follows {d}",
                .{ cur.version, prev.version },
            ));
        }
    }
}

/// The highest version this binary knows about.
pub const latest_version: u32 = all[all.len - 1].version;

const create_bookkeeping =
    \\CREATE TABLE IF NOT EXISTS schema_migrations (
    \\    version    INTEGER PRIMARY KEY,
    \\    name       TEXT NOT NULL,
    \\    applied_at INTEGER NOT NULL
    \\)
;

/// Apply every migration the database has not seen yet.
///
/// Idempotent: a second call is a no-op. Safe to run on every startup,
/// which is what makes "deploy a new image" the whole upgrade procedure.
pub fn migrate(conn: *Conn) sqlite.Error!void {
    try conn.exec(create_bookkeeping);

    const current = try conn.scalarIntOr(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        .{},
        0,
    );

    // Comparing against MAX(version) rather than the set of applied
    // versions is deliberate: a version below the maximum that is
    // somehow unrecorded means the database has been through an
    // out-of-order upgrade, and running that DDL now would apply it to a
    // schema it was never written against. Skipping is the safe move;
    // `unknownApplied` is how an operator sees the mismatch.
    for (all) |mig| {
        if (@as(i64, mig.version) <= current) continue;
        try applyOne(conn, mig);
    }
}

/// Versions recorded as applied but not present in this binary. A
/// non-empty result means the database was written by a newer build, and
/// the caller should refuse to start rather than run queries against a
/// schema it does not understand.
pub fn unknownApplied(conn: *Conn, gpa: std.mem.Allocator) sqlite.Error!std.ArrayList(u32) {
    var out: std.ArrayList(u32) = .empty;
    errdefer out.deinit(gpa);
    var st = try conn.query("SELECT version FROM schema_migrations ORDER BY version", .{});
    defer st.release();
    while (try st.step()) {
        const v: u32 = @intCast(st.int(0));
        var known = false;
        for (all) |mig| {
            if (mig.version == v) {
                known = true;
                break;
            }
        }
        if (!known) out.append(gpa, v) catch return error.OutOfMemory;
    }
    return out;
}

fn applyOne(conn: *Conn, mig: Migration) sqlite.Error!void {
    // One transaction per migration: partial failure leaves the database
    // at the previous version, never halfway through a DDL batch.
    try tx.begin(conn);
    errdefer tx.rollback(conn);

    try conn.exec(mig.sql);
    try conn.execute(
        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
        .{ @as(i64, mig.version), mig.name, nowMillis() },
    );
    try tx.commit(conn);
}

/// The highest applied version, or 0 on a fresh database.
///
/// Surfaced on the status endpoint so an operator can confirm an
/// upgraded binary actually applied its schema changes rather than
/// silently running against the old shape.
pub fn currentVersion(conn: *Conn) sqlite.Error!u32 {
    conn.exec(create_bookkeeping) catch |e| return e;
    const v = try conn.scalarIntOr("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", .{}, 0);
    return @intCast(v);
}

/// Re-exported so migration bookkeeping and the repositories agree on
/// what "now" means. See `sqlite.nowMillis`.
pub const nowMillis = sqlite.nowMillis;

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;
const support = sqlite.testing_support;

/// Test fixture: an in-memory connection with the full schema applied.
/// The equivalent of the Go suite's `openMigratedDB`.
pub fn openMigrated() !*Conn {
    const conn = try support.openMemory();
    errdefer conn.close();
    try migrate(conn);
    return conn;
}

/// Test fixture: a minimal `jobs` row with the given id.
///
/// Every post-processing table (`par2_sets`, `repairs`, `extracts`,
/// `deliveries`) has a foreign key to `jobs(id)`, and `foreign_keys` is
/// ON, so their tests need a parent row to hang off. Shared here rather
/// than copied into five files, and it doubles as proof that the
/// constraint is actually enforced — remove the seed and those tests fail
/// with `ConstraintForeignKey`.
pub fn seedJob(conn: *Conn, id: i64) sqlite.Error!void {
    var hash_buf: [32]u8 = undefined;
    const hash = std.fmt.bufPrint(&hash_buf, "seed-{d}", .{id}) catch return error.Misuse;
    try conn.execute(
        \\INSERT OR IGNORE INTO jobs(id, nzb_hash, name, queue_order, state, total_bytes, added_at, nzb_blob)
        \\VALUES (?, ?, 'seed', 0, 'queued', 0, 0, ?)
    , .{ id, hash, sqlite.blob("") });
}

test "migrate creates the schema and is idempotent" {
    const conn = try openMigrated();
    defer conn.close();

    // The outbox table is the one every other subsystem depends on.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'outbox'",
        .{},
    ));

    // Re-running changes nothing.
    try migrate(conn);
    try t.expectEqual(
        @as(i64, all.len),
        try conn.scalarInt("SELECT COUNT(*) FROM schema_migrations", .{}),
    );
    try t.expectEqual(latest_version, try currentVersion(conn));
}

test "currentVersion is zero before any migration" {
    const conn = try support.openMemory();
    defer conn.close();
    try t.expectEqual(@as(u32, 0), try currentVersion(conn));
}

test "every table the repos query exists after migration" {
    const conn = try openMigrated();
    defer conn.close();

    for ([_][]const u8{
        "outbox",   "outbox_subs",   "servers",         "categories",
        "jobs",     "files",         "segments",        "par2_sets",
        "users",    "sessions",      "deliveries",      "extracts",
        "repairs",  "subscriptions", "scheduled_tasks", "settings",
        "commands", "speed_history",
    }) |name| {
        const n = try conn.scalarInt(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            .{name},
        );
        if (n != 1) {
            std.debug.print("missing table: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

test "the indexes the hot queries depend on exist" {
    const conn = try openMigrated();
    defer conn.close();

    // Each of these was added in response to a measured production
    // problem; losing one is a silent performance regression, not a
    // failure, so it gets an assertion.
    for ([_][]const u8{
        "outbox_subs_pending",
        "outbox_subs_event_id",
        "jobs_active",
        "jobs_history",
        "seg_pending",
        "servers_active",
        "scheduled_tasks_due",
        "commands_queued",
    }) |name| {
        const n = try conn.scalarInt(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
            .{name},
        );
        if (n != 1) {
            std.debug.print("missing index: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

test "the seeded categories match what the *arr suite expects" {
    const conn = try openMigrated();
    defer conn.close();

    // Sonarr defaults to "tv", Radarr to "movies", Lidarr "music",
    // Readarr "books", and "*" is SAB's uncategorized sentinel.
    for ([_][]const u8{ "*", "tv", "movies", "music", "books" }) |name| {
        const n = try conn.scalarInt("SELECT COUNT(*) FROM categories WHERE name = ?", .{name});
        try t.expectEqual(@as(i64, 1), n);
    }
}

test "an applied version is skipped on the next run" {
    const conn = try support.openMemory();
    defer conn.close();

    try conn.exec(create_bookkeeping);
    // Pretend migration 1 already ran, then let migrate do the rest. If
    // it re-ran 001 the CREATE TABLE would fail, so reaching the end at
    // all is the assertion.
    try conn.exec(all[0].sql);
    try conn.execute(
        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
        .{ @as(i64, 1), all[0].name, @as(i64, 0) },
    );

    try migrate(conn);
    try t.expectEqual(latest_version, try currentVersion(conn));
}

test "unknownApplied reports versions this binary does not carry" {
    const conn = try openMigrated();
    defer conn.close();

    var none = try unknownApplied(conn, t.allocator);
    defer none.deinit(t.allocator);
    try t.expectEqual(@as(usize, 0), none.items.len);

    // A database written by a newer build.
    try conn.execute(
        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
        .{ @as(i64, 9999), "from_the_future", @as(i64, 0) },
    );
    var some = try unknownApplied(conn, t.allocator);
    defer some.deinit(t.allocator);
    try t.expectEqualSlices(u32, &.{9999}, some.items);
}

test "migration versions are unique, ordered, and match their filenames" {
    // The comptime block enforces ordering; this pins the count and the
    // derived paths so a hand-edited entry cannot drift from its file.
    try t.expectEqual(@as(usize, 22), all.len);
    try t.expectEqual(@as(u32, 22), latest_version);
    for (all, 1..) |mig, want_version| {
        try t.expectEqual(@as(u32, @intCast(want_version)), mig.version);
        try t.expect(mig.sql.len > 0);
    }
}

test "nowMillis is a plausible unix millisecond timestamp" {
    const now = nowMillis();
    // 2020-01-01 through 2100-01-01: catches a seconds/nanoseconds mixup
    // in either direction, which is the actual failure mode.
    try t.expect(now > 1_577_836_800_000);
    try t.expect(now < 4_102_444_800_000);
}
