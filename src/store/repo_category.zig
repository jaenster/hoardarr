//! `categories`: the per-category output directory and post-processing
//! hook, shared by the REST API and the SAB-compat shim.
//!
//! No domain aggregate: a category is three columns with no lifecycle and
//! no events. Wrapping it in one would be ceremony. What it does need is
//! validation, and that lives here rather than in the API handler because
//! both the REST endpoint and `mode=addfile` create categories, and a
//! check that only one of them performs is not a check.
//!
//! The literal `*` is SAB's "uncategorized" sentinel. It is seeded by the
//! schema and cannot be deleted: every SAB client assumes it exists, and
//! `get_cats` without it makes Sonarr refuse to add anything.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

/// The SAB "uncategorized" sentinel.
pub const uncategorized = "*";

pub const max_name_len = 64;

pub const Error = sqlite.Error || error{
    NotFound,
    /// `*` cannot be removed.
    Reserved,
    NameEmpty,
    NameTooLong,
    /// Leading or trailing whitespace: two categories that differ only by
    /// a trailing space are indistinguishable in a dropdown.
    NameNotTrimmed,
    /// A control character, or a path separator that would let a category
    /// name escape into the directory layout.
    NameInvalidChar,
    /// An absolute directory would redirect deliveries out of the
    /// configured tree entirely.
    DirNotRelative,
    /// A `..` component, same reason.
    DirTraversal,
};

/// One row of `categories`. Slices are owned when the value came from
/// `list`, borrowed when the caller is passing one to `save`.
pub const Category = struct {
    name: []const u8,
    dir: []const u8 = "",
    priority: i32 = 0,
};

/// An owned result set.
pub const CategoryList = struct {
    gpa: Allocator,
    items: std.ArrayList(Category) = .empty,

    pub fn deinit(self: *CategoryList) void {
        for (self.items.items) |c| {
            self.gpa.free(c.name);
            self.gpa.free(c.dir);
        }
        self.items.deinit(self.gpa);
    }
};

pub const CategoryRepo = struct {
    conn: *Conn,

    pub fn init(conn: *Conn) CategoryRepo {
        return .{ .conn = conn };
    }

    /// All categories, by priority then name. Caller owns the result.
    pub fn list(self: CategoryRepo, gpa: Allocator) Error!CategoryList {
        var out = CategoryList{ .gpa = gpa };
        errdefer out.deinit();
        var st = try self.conn.query(
            "SELECT name, dir, priority FROM categories ORDER BY priority ASC, name ASC",
            .{},
        );
        defer st.release();
        while (try st.step()) {
            const name = st.textAlloc(gpa, 0) catch return error.OutOfMemory;
            errdefer gpa.free(name);
            const dir = st.textAlloc(gpa, 1) catch return error.OutOfMemory;
            errdefer gpa.free(dir);
            out.items.append(gpa, .{
                .name = name,
                .dir = dir,
                .priority = @intCast(st.int(2)),
            }) catch return error.OutOfMemory;
        }
        return out;
    }

    /// One category by name.
    pub fn get(self: CategoryRepo, gpa: Allocator, name: []const u8) Error!Category {
        var st = self.conn.queryRow(
            "SELECT name, dir, priority FROM categories WHERE name = ?",
            .{name},
        ) catch |e| {
            if (e == error.NoRows) return error.NotFound;
            return e;
        };
        defer st.release();
        const owned_name = st.textAlloc(gpa, 0) catch return error.OutOfMemory;
        errdefer gpa.free(owned_name);
        const owned_dir = st.textAlloc(gpa, 1) catch return error.OutOfMemory;
        return .{ .name = owned_name, .dir = owned_dir, .priority = @intCast(st.int(2)) };
    }

    /// Create or update by name.
    pub fn save(self: CategoryRepo, c: Category) Error!void {
        try validateName(c.name);
        try validateDir(c.dir);
        const now = sqlite.nowMillis();
        try self.conn.execute(
            \\INSERT INTO categories(name, dir, priority, added_at, updated_at)
            \\VALUES (?, ?, ?, ?, ?)
            \\ON CONFLICT(name) DO UPDATE SET
            \\    dir = excluded.dir,
            \\    priority = excluded.priority,
            \\    updated_at = excluded.updated_at
        , .{ c.name, c.dir, @as(i64, c.priority), now, now });
    }

    /// Remove a category. `*` is reserved.
    pub fn remove(self: CategoryRepo, name: []const u8) Error!void {
        if (std.mem.eql(u8, name, uncategorized)) return error.Reserved;
        try self.conn.execute("DELETE FROM categories WHERE name = ?", .{name});
        if (self.conn.changes() == 0) return error.NotFound;
    }
};

/// Reject names that would confuse the SAB API, the path builder, or a
/// human reading a dropdown. `*` is exempt — it is the sentinel.
pub fn validateName(name: []const u8) Error!void {
    if (std.mem.eql(u8, name, uncategorized)) return;
    if (name.len == 0) return error.NameEmpty;
    if (name.len > max_name_len) return error.NameTooLong;
    if (std.mem.trim(u8, name, &std.ascii.whitespace).len != name.len) return error.NameNotTrimmed;
    for (name) |ch| {
        if (ch < 0x20 or ch == 0x7F) return error.NameInvalidChar;
        // The category name becomes a directory component; a separator in
        // it would silently create a nested tree, or on `:` confuse the
        // Windows clients that talk to the SAB API.
        if (ch == '/' or ch == '\\' or ch == ':') return error.NameInvalidChar;
    }
}

/// Keep category directories relative and inside the configured tree —
/// this is the check that stops a delivery being redirected to `/etc`.
pub fn validateDir(dir: []const u8) Error!void {
    if (dir.len == 0) return;
    if (dir[0] == '/' or dir[0] == '\\') return error.DirNotRelative;
    var it = std.mem.tokenizeAny(u8, dir, "/\\");
    while (it.next()) |segment| {
        if (std.mem.eql(u8, segment, "..")) return error.DirTraversal;
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "the schema seeds the categories the *arr suite expects" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    var got = try CategoryRepo.init(conn).list(t.allocator);
    defer got.deinit();

    // '*' plus tv / movies / music / books.
    try t.expectEqual(@as(usize, 5), got.items.items.len);
    var seen_star = false;
    for (got.items.items) |c| {
        if (std.mem.eql(u8, c.name, uncategorized)) seen_star = true;
    }
    try t.expect(seen_star);
}

test "save creates and then updates by name" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CategoryRepo.init(conn);

    try r.save(.{ .name = "anime", .dir = "anime", .priority = 1 });
    const first = try r.get(t.allocator, "anime");
    defer t.allocator.free(first.name);
    defer t.allocator.free(first.dir);
    try t.expectEqual(@as(i32, 1), first.priority);

    try r.save(.{ .name = "anime", .dir = "anime/subs", .priority = 5 });
    const second = try r.get(t.allocator, "anime");
    defer t.allocator.free(second.name);
    defer t.allocator.free(second.dir);
    try t.expectEqualStrings("anime/subs", second.dir);
    try t.expectEqual(@as(i32, 5), second.priority);
    // Updated, not duplicated.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM categories WHERE name = 'anime'",
        .{},
    ));
}

test "list orders by priority then name" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CategoryRepo.init(conn);
    try conn.exec("DELETE FROM categories");

    try r.save(.{ .name = "zeta", .priority = 0 });
    try r.save(.{ .name = "alpha", .priority = 0 });
    try r.save(.{ .name = "first", .priority = -1 });

    var got = try r.list(t.allocator);
    defer got.deinit();
    try t.expectEqualStrings("first", got.items.items[0].name);
    try t.expectEqualStrings("alpha", got.items.items[1].name);
    try t.expectEqualStrings("zeta", got.items.items[2].name);
}

test "remove deletes, is not idempotent, and refuses the sentinel" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CategoryRepo.init(conn);

    try r.save(.{ .name = "temp" });
    try r.remove("temp");
    try t.expectError(error.NotFound, r.get(t.allocator, "temp"));
    try t.expectError(error.NotFound, r.remove("temp"));

    // Every SAB client assumes '*' exists; get_cats without it makes
    // Sonarr refuse to add anything.
    try t.expectError(error.Reserved, r.remove(uncategorized));
}

test "the sentinel is exempt by exact match, not by containing an asterisk" {
    try validateName(uncategorized);
    // The exemption is the literal name, so a name that merely starts with
    // it still goes through every check.
    try t.expectError(error.NameNotTrimmed, validateName("* "));
    try t.expectError(error.NameInvalidChar, validateName("*/x"));
}

test "invalid names are rejected with a specific reason" {
    try t.expectError(error.NameEmpty, validateName(""));
    try t.expectError(error.NameTooLong, validateName("x" ** 65));
    try t.expectError(error.NameNotTrimmed, validateName(" tv"));
    try t.expectError(error.NameNotTrimmed, validateName("tv "));
    try t.expectError(error.NameInvalidChar, validateName("tv/shows"));
    try t.expectError(error.NameInvalidChar, validateName("tv\\shows"));
    try t.expectError(error.NameInvalidChar, validateName("C:tv"));
    try t.expectError(error.NameInvalidChar, validateName("tv\nshows"));
    try t.expectError(error.NameInvalidChar, validateName("tv\x7f"));
    // 64 is the limit, not one less.
    try validateName("x" ** 64);
}

test "a directory must stay relative and inside the tree" {
    try validateDir("");
    try validateDir("tv");
    try validateDir("tv/hd");
    try t.expectError(error.DirNotRelative, validateDir("/etc"));
    try t.expectError(error.DirNotRelative, validateDir("\\windows"));
    try t.expectError(error.DirTraversal, validateDir("../../etc"));
    try t.expectError(error.DirTraversal, validateDir("tv/../../etc"));
    try t.expectError(error.DirTraversal, validateDir("tv\\..\\etc"));
    // A name merely containing dots is fine; only a whole `..` component
    // escapes.
    try validateDir("tv..hd");
    try validateDir("...");
}

test "save rejects an invalid category before touching the database" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = CategoryRepo.init(conn);
    const before = try conn.scalarInt("SELECT COUNT(*) FROM categories", .{});
    try t.expectError(error.DirTraversal, r.save(.{ .name = "bad", .dir = "../escape" }));
    try t.expectEqual(before, try conn.scalarInt("SELECT COUNT(*) FROM categories", .{}));
}

test "a missing category is NotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try t.expectError(error.NotFound, CategoryRepo.init(conn).get(t.allocator, "nope"));
}
