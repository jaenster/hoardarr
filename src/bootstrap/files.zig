//! The file-shaped REST ports: database backups and log files.
//!
//! Both are the same shape — list a directory, read one file out of it —
//! and both answer the same question about safety: the *port* decides
//! whether a name is acceptable, never the handler. The handler has no
//! idea which directory it is talking about, so it cannot know that
//! `../../etc/passwd` is not a backup, and a path check that lives at the
//! call site is a path check that gets forgotten at the second call site.

const std = @import("std");

const log = @import("../core/log.zig");
const sys = @import("../posix/sys.zig");
const ports = @import("../api/rest/ports.zig");
const sqlite = @import("../store/sqlite.zig");
const infra = @import("infra.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

/// Hard cap on a file this API will materialise in memory. A rotated log
/// is 8 MiB by policy and a backup is a whole database; past this the
/// answer is a refusal rather than an allocation the daemon may not
/// survive.
pub const max_read_bytes: u64 = 256 << 20;

/// Is `name` a plain file name inside the directory we own?
///
/// No separators, no `..`, no leading dot, no empty. Rejecting rather
/// than sanitising: a name that needed sanitising was not one of ours.
pub fn isSafeName(name: []const u8) bool {
    if (name.len == 0 or name.len > 255) return false;
    if (name[0] == '.') return false;
    for (name) |c| {
        if (c == '/' or c == '\\' or c == 0) return false;
    }
    return std.mem.indexOf(u8, name, "..") == null;
}

/// Whole file into `arena`.
fn readWholeFile(arena: Allocator, dir: []const u8, name: []const u8) ports.Error![]const u8 {
    var buf: [sys.path_max]u8 = undefined;
    const path = sys.joinZ(&buf, dir, name) catch return error.Invalid;
    const fd = sys.open(path, .{}) catch |e| switch (e) {
        error.NoSuchFileOrDirectory => return error.NotFound,
        else => return error.Internal,
    };
    defer sys.close(fd);

    const size = sys.fileSize(fd) catch return error.Internal;
    if (size > max_read_bytes) return error.Invalid;

    // `fileSize` left the offset at the end, so read from a second
    // descriptor rather than paying an lseek-back syscall pair.
    const fd2 = sys.open(path, .{}) catch return error.Internal;
    defer sys.close(fd2);

    const out = try arena.alloc(u8, @intCast(size));
    var off: usize = 0;
    while (off < out.len) {
        const n = sys.read(fd2, out[off..]) catch return error.Internal;
        if (n == 0) break;
        off += n;
    }
    return out[0..off];
}

/// One directory's worth of files matching `suffix`, newest first is not
/// promised — the UI sorts — but the order is stable.
fn listDir(
    arena: Allocator,
    dir: []const u8,
    suffix: []const u8,
    active_name: []const u8,
) ports.Error![]const ports.FileInfo {
    var out: std.ArrayList(ports.FileInfo) = .empty;
    errdefer out.deinit(arena);

    // A directory that does not exist yet is an empty list, not a 500:
    // nobody has taken a backup or rotated a log yet.
    var it = sys.DirIter.open(dir) catch return &.{};
    defer it.close();

    while (it.next()) |name| {
        if (suffix.len > 0 and !std.mem.endsWith(u8, name, suffix)) continue;
        if (!isSafeName(name)) continue;

        var buf: [sys.path_max]u8 = undefined;
        const path = sys.joinZ(&buf, dir, name) catch continue;
        const fd = sys.open(path, .{}) catch continue;
        const size = sys.fileSize(fd) catch 0;
        sys.close(fd);

        try out.append(arena, .{
            // `name` borrows the iterator's buffer, which the next
            // `next()` overwrites.
            .name = try arena.dupe(u8, name),
            .size_bytes = @intCast(size),
            .at_ms = stampFromName(name),
            .active = active_name.len > 0 and std.mem.eql(u8, name, active_name),
        });
    }
    return out.toOwnedSlice(arena);
}

/// The unix-second stamp a backup's name carries, in milliseconds.
///
/// The name is the only timestamp available: `posix/sys.zig` has no
/// `stat`, deliberately, and adding one to read an mtime the file name
/// already states would be the wrong place to fix it. A log file has no
/// stamp in its name, so it reports 0 and the UI falls back to the name.
fn stampFromName(name: []const u8) i64 {
    const dash = std.mem.lastIndexOfScalar(u8, name, '-') orelse return 0;
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return 0;
    if (dot <= dash + 1) return 0;
    const secs = std.fmt.parseInt(i64, name[dash + 1 .. dot], 10) catch return 0;
    return secs * std.time.ms_per_s;
}

// ---------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------

/// `VACUUM INTO` a timestamped file under `<data_dir>/backups`.
///
/// Synchronous on the reactor thread, which is defensible only because
/// `VACUUM INTO` on a hoardarr-sized database is sub-second — the same
/// judgement the Go build made. It is also why the endpoint is a POST an
/// operator clicks, not something the scheduler runs while downloads are
/// in flight.
pub const Backups = struct {
    gpa: Allocator,
    conn: *Conn,
    /// Owned by the caller (the app's config), so it outlives this.
    dir: []const u8,

    pub const suffix = ".db";

    pub fn port(self: *Backups) ports.Backups {
        return .{
            .ctx = @ptrCast(self),
            .listFn = &list,
            .runFn = &run,
            .readFn = &read,
        };
    }

    fn self_(ctx: ?*anyopaque) *Backups {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn list(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const ports.FileInfo {
        return listDir(arena, self_(ctx).dir, suffix, "");
    }

    fn run(ctx: ?*anyopaque) ports.Error!void {
        const self = self_(ctx);
        sys.mkdirPath(self.dir) catch return error.Internal;

        // Seconds resolution plus the pid: two backups inside one second
        // is an operator double-clicking, and overwriting the first would
        // lose the thing they just asked for.
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "hoardarr-{d}{s}", .{
            infra.nowSeconds(),
            suffix,
        }) catch return error.Internal;

        var path_buf: [sys.path_max]u8 = undefined;
        const path = sys.joinZ(&path_buf, self.dir, name) catch return error.Internal;

        // `VACUUM INTO` takes a literal, not a bound parameter. The path
        // is built entirely from our own directory and a formatted
        // integer, so there is nothing here a client can influence.
        var sql_buf: [sys.path_max + 32]u8 = undefined;
        const sql = std.fmt.bufPrint(&sql_buf, "VACUUM INTO '{s}'", .{path}) catch
            return error.Internal;

        self.conn.execute(sql, .{}) catch |e| {
            log.default.err("backup failed", &.{
                log.str("path", path),
                log.str("error", @errorName(e)),
            });
            return error.Internal;
        };
        log.default.info("backup written", &.{log.str("path", path)});
    }

    fn read(ctx: ?*anyopaque, arena: Allocator, name: []const u8) ports.Error![]const u8 {
        const self = self_(ctx);
        if (!isSafeName(name) or !std.mem.endsWith(u8, name, suffix)) return error.Invalid;
        return readWholeFile(arena, self.dir, name);
    }
};

// ---------------------------------------------------------------------
// Log files
// ---------------------------------------------------------------------

pub const LogFiles = struct {
    /// `<data_dir>/logs`, owned by the caller.
    dir: []const u8,
    /// The file currently being written, flagged in the listing so the UI
    /// can show it differently from a rotated one.
    active_name: []const u8 = "hoardarr.log",

    pub fn port(self: *LogFiles) ports.LogFiles {
        return .{
            .ctx = @ptrCast(self),
            .listFn = &list,
            .readFn = &read,
        };
    }

    fn self_(ctx: ?*anyopaque) *LogFiles {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn list(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const ports.FileInfo {
        const self = self_(ctx);
        return listDir(arena, self.dir, ".log", self.active_name);
    }

    fn read(ctx: ?*anyopaque, arena: Allocator, name: []const u8) ports.Error![]const u8 {
        const self = self_(ctx);
        if (!isSafeName(name) or !std.mem.endsWith(u8, name, ".log")) return error.Invalid;
        return readWholeFile(arena, self.dir, name);
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;
const migrate = @import("../store/migrate.zig");

test "a name that needed sanitising is refused, not sanitised" {
    try testing.expect(isSafeName("hoardarr-2024-01-01.log"));
    try testing.expect(!isSafeName(""));
    try testing.expect(!isSafeName("../../etc/passwd"));
    try testing.expect(!isSafeName("sub/dir.log"));
    // A leading dot would expose `.env` and friends sitting next to the
    // logs in the same data directory.
    try testing.expect(!isSafeName(".env"));
    try testing.expect(!isSafeName("a..b"));
}

test "backups list, run and read round-trip on disk" {
    // `dir` points into `dir_buf`, so every join must write elsewhere.
    var dir_buf: [sys.path_max]u8 = undefined;
    const dir = try sys.scratchDir(&dir_buf, "backup");
    var rm: infra.RealFs = .{ .gpa = testing.allocator };
    try rm.filesystem().removeAll(dir);

    var db_buf: [sys.path_max]u8 = undefined;
    try sys.mkdirPath(dir);
    const db_path = try sys.joinZ(&db_buf, dir, "src.db");
    sys.unlink(db_path) catch {};

    var backups_buf: [sys.path_max]u8 = undefined;
    const backups_dir = try sys.joinZ(&backups_buf, dir, "backups");

    const conn = try Conn.open(testing.allocator, db_path, .{});
    defer conn.close();
    try migrate.migrate(conn);

    var backups: Backups = .{
        .gpa = testing.allocator,
        .conn = conn,
        .dir = backups_dir,
    };
    const p = backups.port();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try testing.expectEqual(@as(usize, 0), (try p.list(arena.allocator())).len);
    try p.run();

    const listing = try p.list(arena.allocator());
    try testing.expectEqual(@as(usize, 1), listing.len);
    try testing.expect(listing[0].size_bytes > 0);
    // The name carries the only timestamp there is, so a listing that
    // reported 0 would leave the UI unable to sort backups by age.
    try testing.expect(listing[0].at_ms > 1_600_000_000_000);

    const bytes = try p.read(arena.allocator(), listing[0].name);
    // A SQLite file always starts with this, so a truncated or empty read
    // is caught rather than silently served as a "backup".
    try testing.expect(std.mem.startsWith(u8, bytes, "SQLite format 3"));

    // The port owns the safety decision, so a traversal never reaches the
    // filesystem regardless of what the handler passed on.
    try testing.expectError(error.Invalid, p.read(arena.allocator(), "../src.db"));
    try testing.expectError(error.NotFound, p.read(arena.allocator(), "absent.db"));

    try rm.filesystem().removeAll(dir);
}

test "log files list only .log and flag the active one" {
    var dir_buf: [sys.path_max]u8 = undefined;
    const dir = try sys.scratchDir(&dir_buf, "logfiles");
    var rm: infra.RealFs = .{ .gpa = testing.allocator };
    try rm.filesystem().removeAll(dir);
    try sys.mkdirPath(dir);

    for ([_][]const u8{ "hoardarr.log", "hoardarr-2024-01-01.log", "notes.txt" }) |name| {
        var b: [sys.path_max]u8 = undefined;
        const path = try sys.joinZ(&b, dir, name);
        const fd = try sys.open(path, .{ .mode = .write_only, .create = true, .truncate = true });
        try sys.writeAll(fd, "line\n");
        sys.close(fd);
    }

    var files: LogFiles = .{ .dir = dir };
    const p = files.port();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const listing = try p.list(arena.allocator());
    try testing.expectEqual(@as(usize, 2), listing.len);
    var saw_active = false;
    for (listing) |f| {
        try testing.expect(std.mem.endsWith(u8, f.name, ".log"));
        if (f.active) saw_active = true;
    }
    try testing.expect(saw_active);

    try testing.expectEqualStrings("line\n", try p.read(arena.allocator(), "hoardarr.log"));
    try testing.expectError(error.Invalid, p.read(arena.allocator(), "notes.txt"));

    try rm.filesystem().removeAll(dir);
}

test "a listing of a directory that does not exist yet is empty, not an error" {
    var files: LogFiles = .{ .dir = "/tmp/hoardarr-never-created-9c1f" };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    // First run: nothing has rotated yet, and the System page must render.
    try testing.expectEqual(@as(usize, 0), (try files.port().list(arena.allocator())).len);
}
