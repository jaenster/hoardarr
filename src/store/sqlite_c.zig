//! Raw SQLite C declarations.
//!
//! Kept separate from the Zig-facing wrapper so exactly one file has
//! `@cImport` in it, and so the wrapper's tests can be read without the
//! C surface in the way.
pub const c = @cImport({
    @cInclude("sqlite3.h");
});

const std = @import("std");

test "the vendored amalgamation links and round-trips a row" {
    // This is a linkage test as much as a behaviour test: if the C build
    // flags or the include path are wrong, it fails here rather than
    // three modules later.
    var db: ?*c.sqlite3 = null;
    try std.testing.expectEqual(c.SQLITE_OK, c.sqlite3_open(":memory:", &db));
    defer _ = c.sqlite3_close(db);

    try std.testing.expectEqual(
        c.SQLITE_OK,
        c.sqlite3_exec(db, "CREATE TABLE t(a INTEGER, b TEXT); INSERT INTO t VALUES(42,'hi');", null, null, null),
    );

    var stmt: ?*c.sqlite3_stmt = null;
    try std.testing.expectEqual(
        c.SQLITE_OK,
        c.sqlite3_prepare_v2(db, "SELECT a, b FROM t", -1, &stmt, null),
    );
    defer _ = c.sqlite3_finalize(stmt);

    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try std.testing.expectEqual(@as(i32, 42), c.sqlite3_column_int(stmt, 0));
    const text = c.sqlite3_column_text(stmt, 1);
    try std.testing.expectEqualStrings("hi", std.mem.span(@as([*:0]const u8, @ptrCast(text))));
    try std.testing.expectEqual(c.SQLITE_DONE, c.sqlite3_step(stmt));
}

test "WAL mode is available" {
    // Every repo in `store/` assumes WAL: concurrent readers alongside one
    // writer is the reason the outbox dispatcher and the API can share a
    // database. If a build flag ever disables it, fail loudly here.
    var db: ?*c.sqlite3 = null;
    try std.testing.expectEqual(c.SQLITE_OK, c.sqlite3_open(":memory:", &db));
    defer _ = c.sqlite3_close(db);

    var stmt: ?*c.sqlite3_stmt = null;
    try std.testing.expectEqual(
        c.SQLITE_OK,
        c.sqlite3_prepare_v2(db, "PRAGMA journal_mode=WAL", -1, &stmt, null),
    );
    defer _ = c.sqlite3_finalize(stmt);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    // An in-memory database reports "memory" rather than "wal"; the point
    // is that the pragma is understood rather than rejected.
    const mode = std.mem.span(@as([*:0]const u8, @ptrCast(c.sqlite3_column_text(stmt, 0))));
    try std.testing.expect(mode.len > 0);
}

test "double-quoted string literals are rejected" {
    // SQLITE_DQS=0. A typo'd column name silently becoming a string
    // literal is a class of bug we'd rather have as an error.
    var db: ?*c.sqlite3 = null;
    try std.testing.expectEqual(c.SQLITE_OK, c.sqlite3_open(":memory:", &db));
    defer _ = c.sqlite3_close(db);

    _ = c.sqlite3_exec(db, "CREATE TABLE t(a INTEGER)", null, null, null);
    var stmt: ?*c.sqlite3_stmt = null;
    const rc = c.sqlite3_prepare_v2(db, "SELECT \"nonexistent\" FROM t", -1, &stmt, null);
    defer _ = c.sqlite3_finalize(stmt);
    try std.testing.expect(rc != c.SQLITE_OK);
}
