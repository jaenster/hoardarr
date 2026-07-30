//! `speed_history`: downsampled download throughput, one row per minute.
//!
//! The system page graphs throughput from an in-memory ring for the last
//! hour and from this table for anything longer. One row per minute is
//! ~43k rows a month, which is nothing, and it means the long-range graph
//! is a single indexed range scan rather than an aggregation.
//!
//! `bucket_at` is unix **seconds** aligned down to the minute, not
//! milliseconds — the only column in the schema that is not milliseconds,
//! because it is a bucket label rather than an instant, and aligning the
//! label is what makes `Append` idempotent. A flusher that fires twice
//! for the same minute (or once again after a restart) overwrites rather
//! than duplicating.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

/// One downsampled throughput sample.
///
/// Defined here rather than imported from an app-layer type: the app
/// layer that owns the in-memory ring is not ported yet, and this is the
/// entire vocabulary the table needs.
pub const Sample = struct {
    /// Unix seconds. Aligned to the minute on write.
    at_seconds: i64,
    bytes_per_sec: i64,
};

pub const Error = sqlite.Error;

/// Round a unix-second timestamp down to its minute bucket.
///
/// Floor rather than truncate-toward-zero: `@divFloor` keeps pre-epoch
/// timestamps monotonic, and while nobody will graph 1969, a bucket
/// function that is non-monotonic anywhere is a bucket function that can
/// be wrong at a boundary.
pub fn bucketOf(at_seconds: i64) i64 {
    return @divFloor(at_seconds, 60) * 60;
}

pub const SpeedHistoryRepo = struct {
    conn: *Conn,

    pub fn init(conn: *Conn) SpeedHistoryRepo {
        return .{ .conn = conn };
    }

    /// Insert or overwrite the row for `s`'s minute.
    pub fn append(self: SpeedHistoryRepo, s: Sample) Error!void {
        try self.conn.execute(
            \\INSERT INTO speed_history(bucket_at, bytes_per_sec) VALUES (?, ?)
            \\ON CONFLICT(bucket_at) DO UPDATE SET bytes_per_sec = excluded.bytes_per_sec
        , .{ bucketOf(s.at_seconds), s.bytes_per_sec });
    }

    /// Samples in `[from, to]` inclusive, oldest first. `bucket_at` is the
    /// primary key, so this is a range scan over the index with no sort.
    pub fn range(
        self: SpeedHistoryRepo,
        gpa: Allocator,
        from_seconds: i64,
        to_seconds: i64,
    ) Error!std.ArrayList(Sample) {
        var out: std.ArrayList(Sample) = .empty;
        errdefer out.deinit(gpa);
        var st = try self.conn.query(
            \\SELECT bucket_at, bytes_per_sec FROM speed_history
            \\WHERE bucket_at >= ? AND bucket_at <= ?
            \\ORDER BY bucket_at ASC
        , .{ from_seconds, to_seconds });
        defer st.release();
        while (try st.step()) {
            out.append(gpa, .{ .at_seconds = st.int(0), .bytes_per_sec = st.int(1) }) catch
                return error.OutOfMemory;
        }
        return out;
    }

    /// Delete rows older than `before_seconds`, returning the count. Run
    /// from the scheduler; retention is a policy decision that belongs to
    /// the caller, not to the table.
    pub fn purge(self: SpeedHistoryRepo, before_seconds: i64) Error!i64 {
        try self.conn.execute("DELETE FROM speed_history WHERE bucket_at < ?", .{before_seconds});
        return self.conn.changes();
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

const base: i64 = 1_700_000_000 - @rem(1_700_000_000, 60); // a minute boundary

test "samples round-trip and a range returns only what was asked for" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SpeedHistoryRepo.init(conn);

    for (0..5) |i| {
        try r.append(.{
            .at_seconds = base + @as(i64, @intCast(i)) * 60,
            .bytes_per_sec = @as(i64, @intCast(i + 1)) * 1_000_000,
        });
    }

    var got = try r.range(t.allocator, base + 60, base + 180);
    defer got.deinit(t.allocator);
    try t.expectEqual(@as(usize, 3), got.items.len);
    for (got.items, 0..) |s, i| {
        try t.expectEqual(@as(i64, @intCast(i + 2)) * 1_000_000, s.bytes_per_sec);
        try t.expectEqual(base + @as(i64, @intCast(i + 1)) * 60, s.at_seconds);
    }
}

test "two appends inside one minute collapse to the latest value" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SpeedHistoryRepo.init(conn);

    // A flusher firing twice for the same minute — or once again after a
    // restart — must not double the graph's resolution.
    try r.append(.{ .at_seconds = base + 5, .bytes_per_sec = 100 });
    try r.append(.{ .at_seconds = base + 45, .bytes_per_sec = 200 });

    var got = try r.range(t.allocator, base - 60, base + 60);
    defer got.deinit(t.allocator);
    try t.expectEqual(@as(usize, 1), got.items.len);
    try t.expectEqual(@as(i64, 200), got.items[0].bytes_per_sec);
    try t.expectEqual(base, got.items[0].at_seconds);
}

test "purge removes only rows before the cutoff" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SpeedHistoryRepo.init(conn);

    for (0..10) |i| {
        try r.append(.{ .at_seconds = base + @as(i64, @intCast(i)) * 60, .bytes_per_sec = 1 });
    }
    try t.expectEqual(@as(i64, 5), try r.purge(base + 5 * 60));

    var left = try r.range(t.allocator, base, base + 3600);
    defer left.deinit(t.allocator);
    try t.expectEqual(@as(usize, 5), left.items.len);
    try t.expectEqual(base + 5 * 60, left.items[0].at_seconds);
}

test "purging an empty table is zero, not an error" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try t.expectEqual(@as(i64, 0), try SpeedHistoryRepo.init(conn).purge(base));
}

test "an empty range is an empty list" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    var got = try SpeedHistoryRepo.init(conn).range(t.allocator, 0, 100);
    defer got.deinit(t.allocator);
    try t.expectEqual(@as(usize, 0), got.items.len);
}

test "bucketOf floors, including before the epoch" {
    try t.expectEqual(@as(i64, 0), bucketOf(0));
    try t.expectEqual(@as(i64, 0), bucketOf(59));
    try t.expectEqual(@as(i64, 60), bucketOf(60));
    try t.expectEqual(@as(i64, 60), bucketOf(119));
    // Truncation toward zero would give 0 here and make the bucket
    // function non-monotonic across the epoch.
    try t.expectEqual(@as(i64, -60), bucketOf(-1));
    try t.expectEqual(@as(i64, -60), bucketOf(-60));
    try t.expectEqual(@as(i64, -120), bucketOf(-61));
}
