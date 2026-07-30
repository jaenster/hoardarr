//! The `settings` key/value store: runtime-mutable configuration that the
//! Settings UI owns.
//!
//! Deliberately generic — TEXT keys, TEXT values, callers parse. That
//! keeps new settings out of the migration list entirely, which matters
//! because the alternative is a schema change every time somebody adds a
//! checkbox. Bootstrap-only values (listen address, data dir, database
//! path, log level) stay in the config file: they must be known before
//! this table can be read at all.
//!
//! Typed accessors live here rather than at call sites so the
//! "missing → default" decision and the parse both happen in one place.
//! A `get` on an absent key is `error.NotFound`; the `*Or` variants
//! substitute a default, which is what nearly every caller wants.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

pub const Error = sqlite.Error || error{
    /// No row for that key.
    NotFound,
    /// The stored text does not parse as the requested type. Never
    /// silently defaulted: a malformed bandwidth cap that reads as zero
    /// would look like "unlimited".
    Malformed,
};

pub const SettingsRepo = struct {
    conn: *Conn,

    pub fn init(conn: *Conn) SettingsRepo {
        return .{ .conn = conn };
    }

    /// Raw stored value, copied into `gpa`. Caller frees.
    pub fn get(self: SettingsRepo, gpa: Allocator, key: []const u8) Error![]u8 {
        var st = self.conn.queryRow("SELECT value FROM settings WHERE key = ?", .{key}) catch |e| {
            if (e == error.NoRows) return error.NotFound;
            return e;
        };
        defer st.release();
        return st.textAlloc(gpa, 0) catch return error.OutOfMemory;
    }

    /// Upsert. `updated_at` lets an operator see which knobs were touched
    /// and when without an audit table.
    pub fn set(self: SettingsRepo, key: []const u8, value: []const u8) Error!void {
        try self.conn.execute(
            \\INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)
            \\ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        , .{ key, value, sqlite.nowMillis() });
    }

    /// Remove a key, returning whether it existed. Used by "reset to
    /// default" in the UI: deleting is how a setting goes back to
    /// following the compiled-in default, rather than freezing today's
    /// default into the row.
    pub fn remove(self: SettingsRepo, key: []const u8) Error!bool {
        try self.conn.execute("DELETE FROM settings WHERE key = ?", .{key});
        return self.conn.changes() > 0;
    }

    /// Stored string, or `dflt` when absent. The returned slice is
    /// allocated for a hit and is `dflt` itself for a miss, so callers
    /// pass a static default and free only what they own.
    pub fn getStringOr(
        self: SettingsRepo,
        gpa: Allocator,
        key: []const u8,
        dflt: []const u8,
    ) Error![]const u8 {
        return self.get(gpa, key) catch |e| switch (e) {
            error.NotFound => dflt,
            else => e,
        };
    }

    pub fn getIntOr(self: SettingsRepo, key: []const u8, dflt: i64) Error!i64 {
        var buf: [32]u8 = undefined;
        const raw = self.getInto(&buf, key) catch |e| switch (e) {
            error.NotFound => return dflt,
            else => return e,
        };
        return std.fmt.parseInt(i64, raw, 10) catch error.Malformed;
    }

    pub fn getFloatOr(self: SettingsRepo, key: []const u8, dflt: f64) Error!f64 {
        var buf: [64]u8 = undefined;
        const raw = self.getInto(&buf, key) catch |e| switch (e) {
            error.NotFound => return dflt,
            else => return e,
        };
        return std.fmt.parseFloat(f64, raw) catch error.Malformed;
    }

    /// Accepts `1`/`0` and `true`/`false` in any case. The stored form is
    /// always `0`/`1` so a human reading the table sees one convention,
    /// but a hand-edited `true` still loads.
    pub fn getBoolOr(self: SettingsRepo, key: []const u8, dflt: bool) Error!bool {
        var buf: [16]u8 = undefined;
        const raw = self.getInto(&buf, key) catch |e| switch (e) {
            error.NotFound => return dflt,
            else => return e,
        };
        if (std.mem.eql(u8, raw, "1")) return true;
        if (std.mem.eql(u8, raw, "0")) return false;
        if (std.ascii.eqlIgnoreCase(raw, "true")) return true;
        if (std.ascii.eqlIgnoreCase(raw, "false")) return false;
        return error.Malformed;
    }

    pub fn setInt(self: SettingsRepo, key: []const u8, v: i64) Error!void {
        var buf: [32]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return error.Misuse;
        return self.set(key, s);
    }

    pub fn setFloat(self: SettingsRepo, key: []const u8, v: f64) Error!void {
        var buf: [64]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return error.Misuse;
        return self.set(key, s);
    }

    /// Stored as `0`/`1` so somebody poking at the database with a SQL
    /// client can read it without knowing our conventions.
    pub fn setBool(self: SettingsRepo, key: []const u8, v: bool) Error!void {
        return self.set(key, if (v) "1" else "0");
    }

    /// Read into a caller-supplied buffer. The typed getters all deal in
    /// values a few dozen bytes long, so this keeps them allocation-free
    /// — settings are read on request paths.
    fn getInto(self: SettingsRepo, buf: []u8, key: []const u8) Error![]const u8 {
        var st = self.conn.queryRow("SELECT value FROM settings WHERE key = ?", .{key}) catch |e| {
            if (e == error.NoRows) return error.NotFound;
            return e;
        };
        defer st.release();
        const raw = st.text(0);
        if (raw.len > buf.len) return error.Malformed;
        @memcpy(buf[0..raw.len], raw);
        return buf[0..raw.len];
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "a string setting round-trips" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    try r.set("k.str", "hello");
    const got = try r.get(t.allocator, "k.str");
    defer t.allocator.free(got);
    try t.expectEqualStrings("hello", got);
}

test "typed setters and getters agree" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    try r.setInt("k.int", 42);
    try t.expectEqual(@as(i64, 42), try r.getIntOr("k.int", 0));

    try r.setInt("k.negative", -9_007_199_254_740_993);
    try t.expectEqual(@as(i64, -9_007_199_254_740_993), try r.getIntOr("k.negative", 0));

    try r.setFloat("k.float", 0.075);
    try t.expectEqual(@as(f64, 0.075), try r.getFloatOr("k.float", 0));

    for ([_]bool{ true, false }) |want| {
        try r.setBool("k.bool", want);
        try t.expectEqual(want, try r.getBoolOr("k.bool", !want));
    }
}

test "set overwrites rather than accumulating rows" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    for ([_][]const u8{ "one", "two", "three" }) |v| try r.set("k", v);
    const got = try r.get(t.allocator, "k");
    defer t.allocator.free(got);
    try t.expectEqualStrings("three", got);
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM settings WHERE key = 'k'", .{}));
}

test "a missing key is NotFound, and the Or variants substitute the default" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    try t.expectError(error.NotFound, r.get(t.allocator, "no.such.key"));
    try t.expectEqualStrings("fallback", try r.getStringOr(t.allocator, "no.such.key", "fallback"));
    try t.expectEqual(@as(i64, 7), try r.getIntOr("no.such.key", 7));
    try t.expectEqual(@as(f64, 1.5), try r.getFloatOr("no.such.key", 1.5));
    try t.expectEqual(true, try r.getBoolOr("no.such.key", true));
}

test "a bool accepts the textual spellings a human would hand-edit" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    for ([_][]const u8{ "true", "TRUE", "True", "1" }) |v| {
        try r.set("k", v);
        try t.expectEqual(true, try r.getBoolOr("k", false));
    }
    for ([_][]const u8{ "false", "FALSE", "False", "0" }) |v| {
        try r.set("k", v);
        try t.expectEqual(false, try r.getBoolOr("k", true));
    }
}

test "a malformed value is an error, never a silent default" {
    // A bandwidth cap that fails to parse and quietly reads as zero would
    // look exactly like "unlimited" — the worst possible interpretation.
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    try r.set("k.int", "not a number");
    try t.expectError(error.Malformed, r.getIntOr("k.int", 0));
    try r.set("k.float", "wat");
    try t.expectError(error.Malformed, r.getFloatOr("k.float", 0));
    try r.set("k.bool", "maybe");
    try t.expectError(error.Malformed, r.getBoolOr("k.bool", false));
}

test "remove reports whether the key existed" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    try r.set("k", "v");
    try t.expect(try r.remove("k"));
    try t.expect(!try r.remove("k"));
    // Removed, not blanked: the key now follows the compiled-in default
    // again rather than freezing today's default into a row.
    try t.expectEqual(@as(i64, 99), try r.getIntOr("k", 99));
}

test "an oversized value does not overflow the stack buffer" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);

    const long = "9" ** 200;
    try r.set("k", long);
    try t.expectError(error.Malformed, r.getIntOr("k", 0));
    // The allocating path still reads it, so nothing is unreachable.
    const got = try r.get(t.allocator, "k");
    defer t.allocator.free(got);
    try t.expectEqual(@as(usize, 200), got.len);
}

test "updated_at moves when a setting is written" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SettingsRepo.init(conn);
    try r.set("k", "v");
    const ts = try conn.scalarInt("SELECT updated_at FROM settings WHERE key = 'k'", .{});
    try t.expect(ts > 1_577_836_800_000);
}
