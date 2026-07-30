//! `servers`: the Usenet provider registry.
//!
//! `ListEnabled` is the orchestrator's dispatch input and runs on every
//! tick, so it rides the partial `servers_active` index over
//! `(priority, id) WHERE enabled = 1` — the index exists precisely so that
//! query never scans disabled rows.
//!
//! `incrementUsedBytes` is a single UPDATE rather than a read-modify-write
//! through `save`, because the byte-accounting flusher fires per download
//! batch and `used_bytes = used_bytes + ?` is atomic against a concurrent
//! flusher by construction. Doing it through the aggregate would be both
//! slower and racy.
//!
//! Passwords are stored in plaintext. Same threat model as SABnzbd and the
//! *arr suite: anybody who can read the data directory can already read
//! everything, and the process needs the cleartext to authenticate against
//! NNTP. Encrypting at rest with a key stored next to the data buys
//! nothing real.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const server = @import("../domain/server.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const UsenetServer = server.UsenetServer;

pub const Error = sqlite.Error || server.RepositoryError || error{
    /// A stored `billing_mode` this binary does not know.
    UnknownBillingMode,
};

const columns =
    "id, name, host, port, tls, username, password, " ++
    "max_conns, priority, enabled, " ++
    "backup, billing_mode, quota_bytes, used_bytes, bandwidth_bytes_per_sec, " ++
    "added_at, updated_at";

/// An owned result set.
pub const ServerList = struct {
    gpa: Allocator,
    items: std.ArrayList(UsenetServer) = .empty,

    pub fn deinit(self: *ServerList) void {
        for (self.items.items) |*s| s.deinit();
        self.items.deinit(self.gpa);
    }
};

pub const ServerRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) ServerRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: ServerRepo, s: *UsenetServer) Error!void {
        if (s.id == 0) return self.insert(s);
        return self.update(s);
    }

    fn insert(self: ServerRepo, s: *UsenetServer) Error!void {
        self.conn.execute(
            \\INSERT INTO servers(
            \\    name, host, port, tls, username, password,
            \\    max_conns, priority, enabled,
            \\    backup, billing_mode, quota_bytes, used_bytes, bandwidth_bytes_per_sec,
            \\    added_at, updated_at
            \\) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        , .{
            s.name,
            s.host,
            @as(i64, s.port),
            s.tls,
            // NULL rather than "": some providers permit anonymous access
            // for public groups, and an empty username is a real answer
            // that differs from "no credentials configured".
            sqlite.nullIfEmpty(s.username),
            sqlite.nullIfEmpty(s.password),
            @as(i64, s.max_conns),
            @as(i64, s.priority),
            s.enabled,
            s.backup,
            s.billing_mode.toString(),
            s.quota_bytes,
            s.used_bytes,
            s.bandwidth_bytes_per_sec,
            s.added_at,
            s.updated_at,
        }) catch |e| {
            if (e == error.ConstraintUnique) return error.DuplicateName;
            return e;
        };
        s.setId(self.conn.lastInsertRowid());
    }

    fn update(self: ServerRepo, s: *const UsenetServer) Error!void {
        self.conn.execute(
            \\UPDATE servers SET
            \\    name = ?, host = ?, port = ?, tls = ?, username = ?, password = ?,
            \\    max_conns = ?, priority = ?, enabled = ?,
            \\    backup = ?, billing_mode = ?, quota_bytes = ?, used_bytes = ?,
            \\    bandwidth_bytes_per_sec = ?, updated_at = ?
            \\WHERE id = ?
        , .{
            s.name,
            s.host,
            @as(i64, s.port),
            s.tls,
            sqlite.nullIfEmpty(s.username),
            sqlite.nullIfEmpty(s.password),
            @as(i64, s.max_conns),
            @as(i64, s.priority),
            s.enabled,
            s.backup,
            s.billing_mode.toString(),
            s.quota_bytes,
            s.used_bytes,
            s.bandwidth_bytes_per_sec,
            s.updated_at,
            s.id,
        }) catch |e| {
            if (e == error.ConstraintUnique) return error.DuplicateName;
            return e;
        };
        if (self.conn.changes() == 0) return error.ServerNotFound;
    }

    pub fn byId(self: ServerRepo, gpa: Allocator, id: server.ServerId) Error!UsenetServer {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM servers WHERE id = ?", .{id});
    }

    pub fn byName(self: ServerRepo, gpa: Allocator, name: []const u8) Error!UsenetServer {
        return self.one(gpa, "SELECT " ++ columns ++ " FROM servers WHERE name = ?", .{name});
    }

    /// All servers, priority ascending then id — lower priority number is
    /// higher preference, matching the SABnzbd convention operators
    /// already know.
    pub fn list(self: ServerRepo, gpa: Allocator) Error!ServerList {
        return self.many(gpa, "SELECT " ++ columns ++ " FROM servers ORDER BY priority ASC, id ASC");
    }

    /// The orchestrator's dispatch list.
    pub fn listEnabled(self: ServerRepo, gpa: Allocator) Error!ServerList {
        return self.many(
            gpa,
            "SELECT " ++ columns ++ " FROM servers WHERE enabled = 1 ORDER BY priority ASC, id ASC",
        );
    }

    /// Add `n` to `used_bytes` in one statement.
    ///
    /// Non-positive `n` is a no-op rather than an error: the flusher calls
    /// this unconditionally at the end of a batch, and a batch that
    /// downloaded nothing is normal.
    pub fn incrementUsedBytes(self: ServerRepo, id: server.ServerId, n: i64) Error!void {
        if (n <= 0) return;
        try self.conn.execute(
            "UPDATE servers SET used_bytes = used_bytes + ?, updated_at = ? WHERE id = ?",
            .{ n, sqlite.nowMillis(), id },
        );
        if (self.conn.changes() == 0) return error.ServerNotFound;
    }

    pub fn remove(self: ServerRepo, id: server.ServerId) Error!void {
        try self.conn.execute("DELETE FROM servers WHERE id = ?", .{id});
        if (self.conn.changes() == 0) return error.ServerNotFound;
    }

    fn one(self: ServerRepo, gpa: Allocator, sql: []const u8, args: anytype) Error!UsenetServer {
        var st = self.conn.queryRow(sql, args) catch |e| {
            if (e == error.NoRows) return error.ServerNotFound;
            return e;
        };
        defer st.release();
        return hydrate(gpa, &st);
    }

    fn many(self: ServerRepo, gpa: Allocator, sql: []const u8) Error!ServerList {
        var out = ServerList{ .gpa = gpa };
        errdefer out.deinit();
        var st = try self.conn.query(sql, .{});
        defer st.release();
        while (try st.step()) {
            var s = try hydrate(gpa, &st);
            errdefer s.deinit();
            out.items.append(gpa, s) catch return error.OutOfMemory;
        }
        return out;
    }

    fn hydrate(gpa: Allocator, st: *sqlite.Stmt) Error!UsenetServer {
        return UsenetServer.hydrate(gpa, .{
            .id = st.int(0),
            .name = st.text(1),
            .host = st.text(2),
            .port = @intCast(st.int(3)),
            .tls = st.boolean(4),
            .username = st.text(5),
            .password = st.text(6),
            .max_conns = @intCast(st.int(7)),
            .priority = @intCast(st.int(8)),
            .enabled = st.boolean(9),
            .backup = st.boolean(10),
            .billing_mode = server.BillingMode.parse(st.text(11)) orelse return error.UnknownBillingMode,
            .quota_bytes = st.int(12),
            .used_bytes = st.int(13),
            .bandwidth_bytes_per_sec = st.int(14),
            .added_at = st.int(15),
            .updated_at = st.int(16),
        }) catch return error.OutOfMemory;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

fn add(r: ServerRepo, name: []const u8, priority: i32) !UsenetServer {
    var s = try UsenetServer.init(t.allocator, .{
        .name = name,
        .host = "news.example.net",
        .port = 563,
        .username = "u",
        .password = "p",
        .max_conns = 10,
        .priority = priority,
    }, 1);
    errdefer s.deinit();
    try r.save(&s);
    return s;
}

test "save assigns an id and the row round-trips" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);

    var s = try add(r, "main", 0);
    defer s.deinit();
    try t.expect(s.id != 0);

    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expectEqualStrings("main", got.name);
    try t.expectEqualStrings("news.example.net", got.host);
    try t.expectEqual(@as(u16, 563), got.port);
    try t.expectEqual(@as(u16, 10), got.max_conns);
    try t.expectEqualStrings("u", got.username);
    try t.expectEqualStrings("p", got.password);
    try t.expect(got.tls);
    try t.expect(got.enabled);
    try t.expectEqual(server.BillingMode.flat, got.billing_mode);
}

test "byName finds it and a missing name is ServerNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var s = try add(r, "primary", 0);
    defer s.deinit();

    var got = try r.byName(t.allocator, "primary");
    defer got.deinit();
    try t.expectEqual(s.id, got.id);

    try t.expectError(error.ServerNotFound, r.byName(t.allocator, "missing"));
    try t.expectError(error.ServerNotFound, r.byId(t.allocator, 999));
}

test "list orders by priority ascending" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var backup = try add(r, "backup", 5);
    defer backup.deinit();
    var primary = try add(r, "primary", 0);
    defer primary.deinit();
    var tertiary = try add(r, "tertiary", 10);
    defer tertiary.deinit();

    var got = try r.list(t.allocator);
    defer got.deinit();
    try t.expectEqual(@as(usize, 3), got.items.items.len);
    try t.expectEqualStrings("primary", got.items.items[0].name);
    try t.expectEqualStrings("backup", got.items.items[1].name);
    try t.expectEqualStrings("tertiary", got.items.items[2].name);
}

test "listEnabled skips disabled servers" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var a = try add(r, "a", 0);
    defer a.deinit();
    var b = try add(r, "b", 1);
    defer b.deinit();

    // Disabled, not deleted: the operator keeps the credentials.
    try a.setEnabled(false, 2);
    try r.save(&a);

    var got = try r.listEnabled(t.allocator);
    defer got.deinit();
    try t.expectEqual(@as(usize, 1), got.items.items.len);
    try t.expectEqualStrings("b", got.items.items[0].name);

    // And it is still there in the full list.
    var all = try r.list(t.allocator);
    defer all.deinit();
    try t.expectEqual(@as(usize, 2), all.items.items.len);
}

test "an update rewrites the row" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var s = try add(r, "x", 0);
    defer s.deinit();

    try s.update(.{ .max_conns = 99 }, 5);
    try r.save(&s);

    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expectEqual(@as(u16, 99), got.max_conns);
    try t.expectEqual(@as(i64, 5), got.updated_at);
}

test "remove deletes and is not idempotent" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var s = try add(r, "x", 0);
    defer s.deinit();

    try r.remove(s.id);
    try t.expectError(error.ServerNotFound, r.byId(t.allocator, s.id));
    try t.expectError(error.ServerNotFound, r.remove(s.id));
}

test "a duplicate name is DuplicateName, not a raw constraint error" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var first = try add(r, "dup", 0);
    defer first.deinit();

    var second = try UsenetServer.init(t.allocator, .{
        .name = "dup",
        .host = "h2",
        .port = 119,
        .max_conns = 1,
    }, 1);
    defer second.deinit();
    try t.expectError(error.DuplicateName, r.save(&second));
}

test "incrementUsedBytes accumulates and ignores non-positive deltas" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var s = try add(r, "metered", 0);
    defer s.deinit();

    try r.incrementUsedBytes(s.id, 1000);
    try r.incrementUsedBytes(s.id, 2000);
    // A batch that downloaded nothing is normal, not an error.
    try r.incrementUsedBytes(s.id, 0);
    try r.incrementUsedBytes(s.id, -5);

    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expectEqual(@as(i64, 3000), got.used_bytes);
}

test "incrementUsedBytes on an unknown server is reported" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    try t.expectError(error.ServerNotFound, r.incrementUsedBytes(4242, 1));
}

test "a metered backup server round-trips every flag" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);

    var s = try UsenetServer.init(t.allocator, .{
        .name = "block",
        .host = "block.example",
        .port = 563,
        .max_conns = 4,
        .backup = true,
        .billing_mode = .metered,
        .quota_bytes = 500 * 1024 * 1024 * 1024,
        .bandwidth_bytes_per_sec = 10 * 1024 * 1024,
    }, 1);
    defer s.deinit();
    try r.save(&s);

    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expect(got.backup);
    try t.expectEqual(server.BillingMode.metered, got.billing_mode);
    try t.expectEqual(@as(i64, 500 * 1024 * 1024 * 1024), got.quota_bytes);
    try t.expectEqual(@as(i64, 10 * 1024 * 1024), got.bandwidth_bytes_per_sec);
}

test "anonymous credentials come back as empty, and are NULL in the row" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);

    var s = try UsenetServer.init(t.allocator, .{
        .name = "public",
        .host = "free.example",
        .port = 119,
        .max_conns = 1,
        .tls = false,
    }, 1);
    defer s.deinit();
    try r.save(&s);

    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM servers WHERE username IS NULL AND password IS NULL",
        .{},
    ));
    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expectEqualStrings("", got.username);
    try t.expect(!got.tls);
}

test "updating a row deleted underneath us is reported" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var s = try add(r, "gone", 0);
    defer s.deinit();

    try conn.execute("DELETE FROM servers WHERE id = ?", .{s.id});
    try s.update(.{ .max_conns = 3 }, 9);
    try t.expectError(error.ServerNotFound, r.save(&s));
}

test "an unknown billing mode is rejected rather than guessed" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = ServerRepo.init(t.allocator, conn);
    var s = try add(r, "weird", 0);
    defer s.deinit();

    try conn.exec("PRAGMA ignore_check_constraints = ON");
    try conn.execute("UPDATE servers SET billing_mode = ? WHERE id = ?", .{ "barter", s.id });
    try t.expectError(error.UnknownBillingMode, r.byId(t.allocator, s.id));
}
