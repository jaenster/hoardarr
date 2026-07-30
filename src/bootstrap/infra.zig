//! The infrastructure half of the composition root: the adapters that
//! satisfy `app/ports.zig` with real syscalls, a real database and a real
//! outbox.
//!
//! Everything here is deliberately thin. A port implementation is a
//! struct holding the thing it wraps plus the function pointers that
//! expose it; the logic lives in the layer being wrapped, and the only
//! decision made here is which failure of the backend maps onto which
//! member of the port's error set.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;

const sys = @import("../posix/sys.zig");
const log = @import("../core/log.zig");
const app_ports = @import("../app/ports.zig");
const dtx = @import("../domain/tx.zig");
const sqlite = @import("../store/sqlite.zig");
const store_tx = @import("../store/tx.zig");
const outbox = @import("../store/outbox.zig");
const repo_category = @import("../store/repo_category.zig");
const json = @import("../api/rest/json.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

// ---------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------

/// The wall clock, in Unix milliseconds. Stateless, so one instance is
/// shared by every service that needs it.
pub const SystemClock = struct {
    pub fn clock() app_ports.Clock {
        // The port wants a context pointer and this adapter has no state
        // to point at; a pointer to the (zero-sized) type itself is a
        // stable, never-dereferenced address.
        return .{ .ctx = @ptrCast(@constCast(&marker)), .nowFn = &read };
    }

    var marker: u8 = 0;

    fn read(_: *anyopaque) i64 {
        return nowMillis();
    }
};

pub fn nowMillis() i64 {
    return @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_ms));
}

pub fn nowSeconds() i64 {
    return @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_s));
}

// ---------------------------------------------------------------------
// Filesystem
// ---------------------------------------------------------------------

/// Positional write. Not in `posix/sys.zig` and not in `std.posix`
/// either, so it goes to the same raw syscall layer `sys.zig` wraps —
/// which is the boundary that matters, not which file the wrapper sits
/// in.
fn pwrite(fd: sys.Fd, buf: []const u8, offset: i64) sys.Error!usize {
    while (true) {
        if (sys.is_linux) {
            const rc = linux.pwrite(fd, buf.ptr, buf.len, offset);
            const e = linux.errno(rc);
            if (e == .SUCCESS) return @intCast(rc);
            if (e == .INTR) continue;
            return sys.mapError(e);
        }
        const rc = std.c.pwrite(fd, buf.ptr, buf.len, offset);
        if (rc >= 0) return @intCast(rc);
        const e: sys.E = @enumFromInt(std.c._errno().*);
        if (e == .INTR) continue;
        return sys.mapError(e);
    }
}

fn ftruncate(fd: sys.Fd, length: i64) sys.Error!void {
    while (true) {
        if (sys.is_linux) {
            const rc = linux.ftruncate(fd, length);
            const e = linux.errno(rc);
            if (e == .SUCCESS) return;
            if (e == .INTR) continue;
            return sys.mapError(e);
        }
        const rc = std.c.ftruncate(fd, @intCast(length));
        if (rc == 0) return;
        const e: sys.E = @enumFromInt(std.c._errno().*);
        if (e == .INTR) continue;
        return sys.mapError(e);
    }
}

/// Tighten an already-open file's mode.
///
/// `sys.open` creates 0644, which is wrong for a file holding an API key.
/// Changing it through the *descriptor* rather than the path is what
/// closes the window: the file is created empty, tightened, and only then
/// written, so the credential never exists on disk world-readable.
pub fn fchmod(fd: sys.Fd, mode: u32) sys.Error!void {
    while (true) {
        if (sys.is_linux) {
            const rc = linux.fchmod(fd, mode);
            const e = linux.errno(rc);
            if (e == .SUCCESS) return;
            if (e == .INTR) continue;
            return sys.mapError(e);
        }
        const rc = std.c.fchmod(fd, @intCast(mode));
        if (rc == 0) return;
        const e: sys.E = @enumFromInt(std.c._errno().*);
        if (e == .INTR) continue;
        return sys.mapError(e);
    }
}

/// Whether `path` names a directory, decided by trying to open it as one.
///
/// `O_DIRECTORY` fails with `ENOTDIR` on a regular file, which makes this
/// one syscall and no `struct stat` layout to reproduce for two
/// platforms — the same trade `sys.fileSize` makes with `lseek`.
fn isDirectory(path: [:0]const u8) bool {
    const fd = sys.open(path, .{ .directory = true }) catch return false;
    sys.close(fd);
    return true;
}

/// `app_ports.Filesystem` over the real filesystem.
pub const RealFs = struct {
    gpa: Allocator,

    pub fn filesystem(self: *RealFs) app_ports.Filesystem {
        return .{
            .ctx = @ptrCast(self),
            .mkdirAllFn = &mkdirAll,
            .writeAtFn = &writeAt,
            .moveFn = &move,
            .removeFn = &remove,
            .removeAllFn = &removeAll,
            .sizeOfFn = &sizeOf,
            .listFn = &list,
        };
    }

    fn self_(ctx: *anyopaque) *RealFs {
        return @ptrCast(@alignCast(ctx));
    }

    /// Every failure the pipeline can distinguish. Anything else is `Io`,
    /// which callers treat as "the operator has a problem".
    fn mapErr(e: anyerror) app_ports.FsError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.FileNotFound,
            error.NoSuchFileOrDirectory,
            error.NotFound,
            => error.NotFound,
            error.PathAlreadyExists, error.Exists => error.Exists,
            error.AccessDenied, error.PermissionDenied, error.Denied => error.Denied,
            error.NotDir, error.NotDirectory => error.NotDirectory,
            else => error.Io,
        };
    }

    fn mkdirAll(_: *anyopaque, path: []const u8) app_ports.FsError!void {
        sys.mkdirPath(path) catch |e| return mapErr(e);
    }

    fn writeAt(
        ctx: *anyopaque,
        path: []const u8,
        offset: i64,
        bytes: []const u8,
        min_size: i64,
    ) app_ports.FsError!void {
        _ = ctx;
        var buf: [sys.path_max]u8 = undefined;
        const p = sys.pathZ(&buf, path) catch return error.Io;

        const fd = sys.open(p, .{ .mode = .write_only, .create = true }) catch |e|
            return mapErr(e);
        defer sys.close(fd);

        // Grow first so a multi-segment file is allocated once instead of
        // extended on every write. Never shrinks: the guard keeps a
        // late-arriving low-offset segment from truncating what its
        // siblings already wrote.
        if (min_size > 0) {
            const current = sys.fileSize(fd) catch |e| return mapErr(e);
            if (current < min_size) ftruncate(fd, min_size) catch |e| return mapErr(e);
        }

        var written: usize = 0;
        while (written < bytes.len) {
            const at = offset + @as(i64, @intCast(written));
            const n = pwrite(fd, bytes[written..], at) catch |e| return mapErr(e);
            if (n == 0) return error.Io;
            written += n;
        }
    }

    fn move(_: *anyopaque, src: []const u8, dst: []const u8) app_ports.FsError!void {
        var a: [sys.path_max]u8 = undefined;
        var b: [sys.path_max]u8 = undefined;
        const from = sys.pathZ(&a, src) catch return error.Io;
        const to = sys.pathZ(&b, dst) catch return error.Io;
        sys.rename(from, to) catch |e| return mapErr(e);
    }

    fn remove(_: *anyopaque, path: []const u8) app_ports.FsError!void {
        var buf: [sys.path_max]u8 = undefined;
        const p = sys.pathZ(&buf, path) catch return error.Io;
        // `unlink` on a directory reports differently per platform, so
        // the kind is decided first rather than inferred from the errno.
        if (isDirectory(p)) {
            sys.rmdir(p) catch |e| return mapErr(e);
        } else {
            sys.unlink(p) catch |e| return mapErr(e);
        }
    }

    fn removeAll(ctx: *anyopaque, path: []const u8) app_ports.FsError!void {
        const self = self_(ctx);
        removeTree(self.gpa, path, 0) catch |e| switch (e) {
            // An absent path is success, matching `os.RemoveAll`.
            error.NotFound => return,
            else => return e,
        };
    }

    /// Depth-first delete. The recursion is bounded because a release
    /// tree is a handful of levels deep and an unbounded walk on a
    /// hostile path is a stack overflow.
    fn removeTree(gpa: Allocator, path: []const u8, depth: u8) app_ports.FsError!void {
        if (depth > 32) return error.Io;

        var buf: [sys.path_max]u8 = undefined;
        const p = sys.pathZ(&buf, path) catch return error.Io;
        if (!sys.exists(p)) return error.NotFound;
        if (!isDirectory(p)) {
            sys.unlink(p) catch |e| return mapErr(e);
            return;
        }

        // Names are borrowed from the iterator's buffer and invalidated by
        // the next `next()`, so the whole listing is taken first and the
        // deletions run afterwards.
        var names: std.ArrayList([]u8) = .empty;
        defer {
            for (names.items) |n| gpa.free(n);
            names.deinit(gpa);
        }
        {
            var it = sys.DirIter.open(path) catch |e| return mapErr(e);
            defer it.close();
            while (it.next()) |name| {
                const owned = gpa.dupe(u8, name) catch return error.OutOfMemory;
                names.append(gpa, owned) catch {
                    gpa.free(owned);
                    return error.OutOfMemory;
                };
            }
        }

        for (names.items) |name| {
            var child_buf: [sys.path_max]u8 = undefined;
            const child = sys.joinZ(&child_buf, path, name) catch return error.Io;
            try removeTree(gpa, child, depth + 1);
        }
        sys.rmdir(p) catch |e| return mapErr(e);
    }

    fn sizeOf(_: *anyopaque, path: []const u8) app_ports.FsError!?i64 {
        var buf: [sys.path_max]u8 = undefined;
        const p = sys.pathZ(&buf, path) catch return error.Io;
        if (isDirectory(p)) return null;
        const fd = sys.open(p, .{}) catch |e| return mapErr(e);
        defer sys.close(fd);
        const n = sys.fileSize(fd) catch |e| return mapErr(e);
        return @intCast(n);
    }

    fn list(_: *anyopaque, a: Allocator, path: []const u8) app_ports.FsError![]app_ports.DirEntry {
        var out: std.ArrayList(app_ports.DirEntry) = .empty;
        errdefer out.deinit(a);

        var it = sys.DirIter.open(path) catch |e| return mapErr(e);
        defer it.close();

        while (it.next()) |name| {
            var child_buf: [sys.path_max]u8 = undefined;
            const child = sys.joinZ(&child_buf, path, name) catch return error.Io;
            const is_dir = isDirectory(child);
            var size: i64 = 0;
            if (!is_dir) {
                const fd = sys.open(child, .{}) catch continue;
                size = @intCast(sys.fileSize(fd) catch 0);
                sys.close(fd);
            }
            // `name` borrows the iterator's buffer, which the next
            // `next()` overwrites, so it is duped before the loop turns.
            try out.append(a, .{
                .name = try a.dupe(u8, name),
                .is_dir = is_dir,
                .size = size,
            });
        }
        return out.toOwnedSlice(a);
    }
};

// ---------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------

/// `domain/tx.zig`'s `Manager` over one SQLite connection.
///
/// The unit is stored in the manager rather than heap-allocated per
/// transaction because connections are thread-owned and single-threaded:
/// there is exactly one open transaction per connection at a time, which
/// SQLite enforces anyway.
pub const TxManager = struct {
    conn: *Conn,

    const unit_vtable: dtx.Unit.VTable = .{ .commit = commitFn, .rollback = rollbackFn };
    const manager_vtable: dtx.Manager.VTable = .{ .begin = beginFn };

    pub fn manager(self: *TxManager) app_ports.Manager {
        return .{ .ctx = @ptrCast(self), .vtable = &manager_vtable };
    }

    fn self_(ctx: *anyopaque) *TxManager {
        return @ptrCast(@alignCast(ctx));
    }

    fn beginFn(ctx: *anyopaque) dtx.Error!dtx.Unit {
        const self = self_(ctx);
        store_tx.begin(self.conn) catch |e| {
            log.default.err("transaction begin failed", &.{log.str("error", @errorName(e))});
            return error.Begin;
        };
        return .{ .ctx = ctx, .vtable = &unit_vtable };
    }

    fn commitFn(ctx: *anyopaque) dtx.Error!void {
        const self = self_(ctx);
        store_tx.commit(self.conn) catch |e| {
            log.default.err("transaction commit failed", &.{log.str("error", @errorName(e))});
            return switch (e) {
                error.Busy, error.Locked => error.Conflict,
                else => error.Commit,
            };
        };
    }

    fn rollbackFn(ctx: *anyopaque) void {
        store_tx.rollback(self_(ctx).conn);
    }
};

// ---------------------------------------------------------------------
// Event publication
// ---------------------------------------------------------------------

/// Serialises a domain event union into the JSON body the outbox stores.
///
/// Reflective rather than hand-written per context: every event payload in
/// this codebase is a flat struct of scalars, slices and optionals, and
/// nine hand-rolled encoders would be nine places for a new field to be
/// forgotten. The topic and the aggregate id come from the union's own
/// `topic()` / `aggregateId()`, which every context already provides.
///
/// `aggregate_key` names a field to add carrying `aggregateId()`, when the
/// payload does not already have one. The download context needs it: its
/// job-level events spell the field `id` and its segment-level events
/// spell it `job_id`, and `Bus.eventsByJob` looks for `$.job_id` in the
/// payload — so without this the per-job timeline silently shows half the
/// story. It is a parameter rather than a constant because the same
/// normalisation on the auth context would put a *user* id under
/// `job_id` and pull login events into a download's timeline.
pub fn encodeEvent(
    comptime E: type,
    gpa: Allocator,
    e: E,
    comptime aggregate_key: ?[]const u8,
) Allocator.Error![]u8 {
    var w = json.Writer.init(gpa);
    errdefer w.deinit();
    try w.beginObject();
    if (aggregate_key) |key| {
        if (!comptime hasField(E, key)) {
            try w.key(key);
            try w.int(e.aggregateId());
        }
    }
    switch (e) {
        inline else => |payload| try writeFields(&w, payload),
    }
    try w.endObject();
    const items = w.items();
    const owned = try gpa.dupe(u8, items);
    w.deinit();
    return owned;
}

/// True when *every* variant of `E` already carries `name`, in which case
/// adding it would emit the key twice.
fn hasField(comptime E: type, comptime name: []const u8) bool {
    for (@typeInfo(E).@"union".fields) |v| {
        if (@typeInfo(v.type) != .@"struct") return false;
        if (!@hasField(v.type, name)) return false;
    }
    return true;
}

fn writeFields(w: *json.Writer, payload: anytype) Allocator.Error!void {
    const T = @TypeOf(payload);
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            inline for (info.fields) |f| {
                try w.key(f.name);
                try writeValue(w, @field(payload, f.name));
            }
        },
        // A payload-free variant (`void`) is a legitimate event: the
        // topic alone carries the meaning.
        else => {},
    }
}

fn writeValue(w: *json.Writer, v: anytype) Allocator.Error!void {
    const T = @TypeOf(v);
    switch (@typeInfo(T)) {
        .bool => try w.boolean(v),
        .int, .comptime_int => try w.int(@intCast(v)),
        .float, .comptime_float => try w.float(@floatCast(v)),
        .@"enum" => try w.string(@tagName(v)),
        .optional => {
            if (v) |inner| try writeValue(w, inner) else try w.nul();
        },
        .pointer => |p| {
            if (p.size == .slice and p.child == u8) {
                try w.string(v);
            } else if (p.size == .slice) {
                try w.beginArray();
                for (v) |item| try writeValue(w, item);
                try w.endArray();
            } else {
                try w.nul();
            }
        },
        .@"struct" => {
            try w.beginObject();
            try writeFields(w, v);
            try w.endObject();
        },
        .void => try w.nul(),
        else => try w.nul(),
    }
}

/// `EventSink(E)` over the outbox bus.
///
/// One instance per event union, all sharing the same bus and connection.
/// The unit is ignored on purpose: `outbox.publish` detects an open
/// transaction on the connection itself and joins it, which is the same
/// atomicity the `*Unit` parameter expresses — threading it twice would
/// let the two disagree.
pub fn Publisher(comptime E: type, comptime aggregate_key: ?[]const u8) type {
    return struct {
        const Self = @This();
        const Sink = app_ports.EventSink(E);

        gpa: Allocator,
        bus: *outbox.Bus,
        conn: *Conn,

        pub fn sink(self: *Self) Sink {
            return .{ .ctx = @ptrCast(self), .publishFn = &publish };
        }

        fn publish(ctx: *anyopaque, _: ?*app_ports.Unit, events: []const E) app_ports.PublishError!void {
            const self: *Self = @ptrCast(@alignCast(ctx));

            var arena_state = std.heap.ArenaAllocator.init(self.gpa);
            defer arena_state.deinit();
            const arena = arena_state.allocator();

            const rows = try arena.alloc(outbox.Event, events.len);
            for (events, 0..) |e, i| {
                var id_buf: [24]u8 = undefined;
                const agg = std.fmt.bufPrint(&id_buf, "{d}", .{e.aggregateId()}) catch "0";
                rows[i] = .{
                    .topic = e.topic(),
                    .aggregate_id = try arena.dupe(u8, agg),
                    .payload = try encodeEvent(E, arena, e, aggregate_key),
                };
            }

            self.bus.publish(self.conn, rows) catch |err| {
                log.default.err("outbox publish failed", &.{
                    log.str("error", @errorName(err)),
                    log.uint("events", events.len),
                });
                return switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    else => error.Backend,
                };
            };
        }
    };
}

// ---------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------

/// `app_ports.Categories` over the category table.
///
/// The lookup is per call rather than cached: a category's directory is
/// editable from Settings and a delivery that lands in the old one is a
/// support ticket. The table is a handful of rows behind a primary key,
/// so the query is cheaper than the invalidation would be.
pub const CategoryLookup = struct {
    gpa: Allocator,
    conn: *Conn,
    /// The last answer, kept alive until the next call because the port
    /// returns a borrowed slice with no allocator to hand it.
    buf: [512]u8 = undefined,
    len: usize = 0,

    pub fn categories(self: *CategoryLookup) app_ports.Categories {
        return .{ .ctx = @ptrCast(self), .dirForFn = &dirFor };
    }

    fn dirFor(ctx: *anyopaque, name: []const u8) ?[]const u8 {
        const self: *CategoryLookup = @ptrCast(@alignCast(ctx));
        if (name.len == 0 or std.mem.eql(u8, name, "*")) return null;

        const repo = repo_category.CategoryRepo.init(self.conn);
        // `get` hands back owned strings with no destructor of their own —
        // `CategoryList` frees them field by field, and a single row has
        // no list to belong to.
        const c = repo.get(self.gpa, name) catch return null;
        defer {
            self.gpa.free(c.name);
            self.gpa.free(c.dir);
        }
        if (c.dir.len > self.buf.len) return null;
        @memcpy(self.buf[0..c.dir.len], c.dir);
        self.len = c.dir.len;
        return self.buf[0..self.len];
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

const TinyEvent = union(enum) {
    created: struct { job_id: i64, name: []const u8, at: i64 },
    failed: struct { job_id: i64, reason: ?[]const u8 },

    pub fn topic(self: TinyEvent) []const u8 {
        return switch (self) {
            .created => "t.created",
            .failed => "t.failed",
        };
    }

    pub fn aggregateId(self: TinyEvent) i64 {
        return switch (self) {
            inline else => |p| p.job_id,
        };
    }
};

test "an event encodes to the flat JSON the outbox stores" {
    const a = try encodeEvent(TinyEvent, testing.allocator, .{
        .created = .{ .job_id = 7, .name = "Release", .at = 1234 },
    }, null);
    defer testing.allocator.free(a);
    try testing.expectEqualStrings(
        "{\"job_id\":7,\"name\":\"Release\",\"at\":1234}",
        a,
    );

    // A null optional has to survive as `null`, not disappear: the
    // timeline renders the field either way.
    const b = try encodeEvent(TinyEvent, testing.allocator, .{
        .failed = .{ .job_id = 9, .reason = null },
    }, null);
    defer testing.allocator.free(b);
    try testing.expectEqualStrings("{\"job_id\":9,\"reason\":null}", b);
}

test "the real filesystem writes at an offset and preallocates" {
    var fs_impl: RealFs = .{ .gpa = testing.allocator };
    const fs = fs_impl.filesystem();

    const dir = "/tmp/hoardarr-infra-test";
    try fs.removeAll(dir);
    try fs.mkdirAll(dir ++ "/nested");

    try fs.writeAt(dir ++ "/nested/a.tmp", 4, "abcd", 16);
    try testing.expectEqual(@as(?i64, 16), try fs.sizeOf(dir ++ "/nested/a.tmp"));
    // A second segment at its own offset must not disturb the first —
    // this is the property crash recovery relies on.
    try fs.writeAt(dir ++ "/nested/a.tmp", 0, "ZZZZ", 16);
    try testing.expectEqual(@as(?i64, 16), try fs.sizeOf(dir ++ "/nested/a.tmp"));

    try testing.expectEqual(@as(?i64, null), try fs.sizeOf(dir));
    try testing.expect(fs.isDir(dir));
    try testing.expect(!fs.exists(dir ++ "/ghost"));

    const kids = try fs.list(testing.allocator, dir);
    defer {
        for (kids) |k| testing.allocator.free(k.name);
        testing.allocator.free(kids);
    }
    try testing.expectEqual(@as(usize, 1), kids.len);
    try testing.expectEqualStrings("nested", kids[0].name);
    try testing.expect(kids[0].is_dir);

    try fs.move(dir ++ "/nested/a.tmp", dir ++ "/nested/b.tmp");
    try testing.expect(fs.exists(dir ++ "/nested/b.tmp"));

    // A whole subtree, and a second call on the same path is success.
    try fs.removeAll(dir);
    try testing.expect(!fs.exists(dir));
    try fs.removeAll(dir);
}

test "a missing path is NotFound rather than a generic failure" {
    var fs_impl: RealFs = .{ .gpa = testing.allocator };
    const fs = fs_impl.filesystem();
    try testing.expectError(error.NotFound, fs.sizeOf("/tmp/hoardarr-definitely-absent-4a2f"));
    try testing.expectError(error.NotFound, fs.move("/tmp/hoardarr-absent-4a2f", "/tmp/x"));
}

test "the system clock moves forward and reports milliseconds" {
    const c = SystemClock.clock();
    const a = c.now();
    try testing.expect(a > 1_600_000_000_000);
    try testing.expect(c.now() >= a);
}
