//! `subscriptions`: outbound notification destinations (webhook, Discord,
//! Slack).
//!
//! `topics` is a JSON array in one column. Matching is exact or
//! trailing-wildcard (`deliver.*`) and happens in the notify service, not
//! in SQL — a `LIKE` per topic per event would be slower than the in-memory
//! prefix check, and the row count here is in the dozens.
//!
//! Operational telemetry (`last_success_at`, `last_error_at`,
//! `last_error`) lives on the row rather than in a log, because it is what
//! the Settings page shows next to each destination. A webhook that has
//! been quietly 500ing for a week is the failure the operator most needs to
//! see, and a log line scrolls away.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const notify = @import("../domain/notify.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Subscription = notify.Subscription;

pub const Error = sqlite.Error || notify.RepositoryError || error{
    /// A stored `kind` this binary does not know.
    UnknownKind,
    /// `topics` is not a JSON array of strings.
    MalformedTopics,
};

const columns =
    "id, name, kind, url, topics, secret, enabled, " ++
    "last_success_at, last_error_at, last_error, created_at, updated_at";

/// An owned result set.
pub const SubscriptionList = struct {
    gpa: Allocator,
    items: std.ArrayList(Subscription) = .empty,

    pub fn deinit(self: *SubscriptionList) void {
        for (self.items.items) |*s| s.deinit();
        self.items.deinit(self.gpa);
    }
};

pub const SubscriptionRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) SubscriptionRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: SubscriptionRepo, s: *Subscription) Error!void {
        const topics = sqlite.string_array.encodeAlloc(self.gpa, s.topics) catch
            return error.OutOfMemory;
        defer self.gpa.free(topics);

        if (s.id == 0) {
            self.conn.execute(
                \\INSERT INTO subscriptions(
                \\    name, kind, url, topics, secret, enabled,
                \\    last_success_at, last_error_at, last_error,
                \\    created_at, updated_at
                \\) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            , .{
                s.name,
                s.kind.toString(),
                s.url,
                topics,
                // NULL rather than "": "no HMAC key" and "an empty key" are
                // different configurations, and only one of them is valid.
                sqlite.nullIfEmpty(s.secret),
                s.enabled,
                s.last_success_at,
                s.last_error_at,
                sqlite.nullIfEmpty(s.last_error),
                s.created_at,
                s.updated_at,
            }) catch |e| {
                if (e == error.ConstraintUnique) return error.DuplicateName;
                return e;
            };
            s.setId(self.conn.lastInsertRowid());
            return;
        }

        self.conn.execute(
            \\UPDATE subscriptions SET
            \\    name = ?, kind = ?, url = ?, topics = ?, secret = ?, enabled = ?,
            \\    last_success_at = ?, last_error_at = ?, last_error = ?,
            \\    updated_at = ?
            \\WHERE id = ?
        , .{
            s.name,
            s.kind.toString(),
            s.url,
            topics,
            sqlite.nullIfEmpty(s.secret),
            s.enabled,
            s.last_success_at,
            s.last_error_at,
            sqlite.nullIfEmpty(s.last_error),
            s.updated_at,
            s.id,
        }) catch |e| {
            if (e == error.ConstraintUnique) return error.DuplicateName;
            return e;
        };
        if (self.conn.changes() == 0) return error.SubscriptionNotFound;
    }

    pub fn byId(self: SubscriptionRepo, gpa: Allocator, id: notify.SubscriptionId) Error!Subscription {
        var st = self.conn.queryRow(
            "SELECT " ++ columns ++ " FROM subscriptions WHERE id = ?",
            .{id},
        ) catch |e| {
            if (e == error.NoRows) return error.SubscriptionNotFound;
            return e;
        };
        defer st.release();
        return self.hydrate(gpa, &st);
    }

    /// Every destination, by name. The Settings page shows all of them,
    /// enabled or not.
    pub fn list(self: SubscriptionRepo, gpa: Allocator) Error!SubscriptionList {
        return self.many(gpa, "SELECT " ++ columns ++ " FROM subscriptions ORDER BY name ASC");
    }

    /// The dispatcher's input. Rides the partial `subscriptions_enabled`
    /// index so a long tail of disabled destinations costs nothing.
    pub fn listEnabled(self: SubscriptionRepo, gpa: Allocator) Error!SubscriptionList {
        return self.many(
            gpa,
            "SELECT " ++ columns ++ " FROM subscriptions WHERE enabled = 1 ORDER BY name ASC",
        );
    }

    pub fn remove(self: SubscriptionRepo, id: notify.SubscriptionId) Error!void {
        try self.conn.execute("DELETE FROM subscriptions WHERE id = ?", .{id});
        if (self.conn.changes() == 0) return error.SubscriptionNotFound;
    }

    fn many(self: SubscriptionRepo, gpa: Allocator, sql: []const u8) Error!SubscriptionList {
        var out = SubscriptionList{ .gpa = gpa };
        errdefer out.deinit();
        var st = try self.conn.query(sql, .{});
        defer st.release();
        while (try st.step()) {
            var s = try self.hydrate(gpa, &st);
            errdefer s.deinit();
            out.items.append(gpa, s) catch return error.OutOfMemory;
        }
        return out;
    }

    fn hydrate(self: SubscriptionRepo, gpa: Allocator, st: *sqlite.Stmt) Error!Subscription {
        var arena_state = std.heap.ArenaAllocator.init(self.gpa);
        defer arena_state.deinit();
        const topics = sqlite.string_array.decode(arena_state.allocator(), st.text(4)) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.MalformedJsonArray => return error.MalformedTopics,
        };

        return Subscription.hydrate(gpa, .{
            .id = st.int(0),
            .name = st.text(1),
            .kind = notify.Kind.parse(st.text(2)) orelse return error.UnknownKind,
            .url = st.text(3),
            .topics = topics,
            .secret = st.text(5),
            .enabled = st.boolean(6),
            .last_success_at = st.optInt(7),
            .last_error_at = st.optInt(8),
            .last_error = st.text(9),
            .created_at = st.int(10),
            .updated_at = st.int(11),
        }) catch return error.OutOfMemory;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

const now0: i64 = 1_700_000_000_000;

fn newSub(name: []const u8, kind: notify.Kind, topics: []const []const u8) !Subscription {
    return Subscription.init(t.allocator, .{
        .name = name,
        .kind = kind,
        .url = "https://example.invalid/hook",
        .topics = topics,
        .secret = "s3cr3t",
    }, now0);
}

test "a subscription round-trips with its topic list" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    var s = try newSub("ops-discord", .discord, &.{ "deliver.*", "download.job.failed" });
    defer s.deinit();
    try r.save(&s);
    try t.expect(s.id != 0);

    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expectEqualStrings("ops-discord", got.name);
    try t.expectEqual(notify.Kind.discord, got.kind);
    try t.expectEqualStrings("https://example.invalid/hook", got.url);
    try t.expectEqualStrings("s3cr3t", got.secret);
    try t.expect(got.enabled);
    try t.expectEqual(@as(usize, 2), got.topics.len);
    try t.expectEqualStrings("deliver.*", got.topics[0]);
    try t.expectEqualStrings("download.job.failed", got.topics[1]);
}

test "every kind round-trips" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    for ([_]notify.Kind{ .webhook, .discord, .slack }, 0..) |kind, i| {
        var name_buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "sub-{d}", .{i});
        var s = try newSub(name, kind, &.{"a.b"});
        defer s.deinit();
        try r.save(&s);
        var got = try r.byId(t.allocator, s.id);
        defer got.deinit();
        try t.expectEqual(kind, got.kind);
    }
}

test "delivery telemetry is persisted so the UI can show it" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    var s = try newSub("flaky", .webhook, &.{"a.b"});
    defer s.deinit();
    try r.save(&s);

    try s.markDeliveryFailure("502 Bad Gateway", now0 + 100);
    try r.save(&s);
    var failing = try r.byId(t.allocator, s.id);
    defer failing.deinit();
    // A webhook quietly 500ing for a week is the failure most worth
    // surfacing, and a log line scrolls away.
    try t.expectEqualStrings("502 Bad Gateway", failing.last_error);
    try t.expectEqual(@as(?i64, now0 + 100), failing.last_error_at);
    try t.expectEqual(@as(?i64, null), failing.last_success_at);

    try s.markDeliverySuccess(now0 + 200);
    try r.save(&s);
    var recovered = try r.byId(t.allocator, s.id);
    defer recovered.deinit();
    try t.expectEqual(@as(?i64, now0 + 200), recovered.last_success_at);
}

test "list includes disabled destinations, listEnabled does not" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    var on = try newSub("on", .webhook, &.{"a.b"});
    defer on.deinit();
    try r.save(&on);

    var off = try newSub("off", .webhook, &.{"a.b"});
    defer off.deinit();
    off.enabled = false;
    try r.save(&off);

    var all = try r.list(t.allocator);
    defer all.deinit();
    try t.expectEqual(@as(usize, 2), all.items.items.len);
    // Ordered by name.
    try t.expectEqualStrings("off", all.items.items[0].name);

    var enabled = try r.listEnabled(t.allocator);
    defer enabled.deinit();
    try t.expectEqual(@as(usize, 1), enabled.items.items.len);
    try t.expectEqualStrings("on", enabled.items.items[0].name);
}

test "a duplicate name is DuplicateName" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    var first = try newSub("dup", .webhook, &.{"a.b"});
    defer first.deinit();
    try r.save(&first);
    var second = try newSub("dup", .slack, &.{"a.b"});
    defer second.deinit();
    try t.expectError(error.DuplicateName, r.save(&second));
}

test "remove deletes and is not idempotent" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    var s = try newSub("temp", .webhook, &.{"a.b"});
    defer s.deinit();
    try r.save(&s);
    try r.remove(s.id);
    try t.expectError(error.SubscriptionNotFound, r.byId(t.allocator, s.id));
    try t.expectError(error.SubscriptionNotFound, r.remove(s.id));
}

test "no secret is NULL in the row and empty in the aggregate" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);

    var s = try Subscription.init(t.allocator, .{
        .name = "unsigned",
        .url = "https://example.invalid/hook",
        .topics = &.{"a.b"},
    }, now0);
    defer s.deinit();
    try r.save(&s);

    // "no HMAC key" and "an empty HMAC key" are different configurations.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM subscriptions WHERE secret IS NULL",
        .{},
    ));
    var got = try r.byId(t.allocator, s.id);
    defer got.deinit();
    try t.expectEqualStrings("", got.secret);
}

test "an unknown kind is rejected rather than defaulting to webhook" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);
    var s = try newSub("weird", .webhook, &.{"a.b"});
    defer s.deinit();
    try r.save(&s);

    // Guessing "webhook" would post a Discord-shaped body to a Slack URL.
    try conn.execute("UPDATE subscriptions SET kind = ? WHERE id = ?", .{ "carrier_pigeon", s.id });
    try t.expectError(error.UnknownKind, r.byId(t.allocator, s.id));
}

test "a malformed topics column is reported, not silently emptied" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);
    var s = try newSub("broken", .webhook, &.{"a.b"});
    defer s.deinit();
    try r.save(&s);

    // Silently reading an empty list would turn a configured destination
    // into one that matches nothing, with no error anywhere.
    try conn.execute("UPDATE subscriptions SET topics = ? WHERE id = ?", .{ "nope", s.id });
    try t.expectError(error.MalformedTopics, r.byId(t.allocator, s.id));
}

test "a missing subscription is SubscriptionNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try t.expectError(
        error.SubscriptionNotFound,
        SubscriptionRepo.init(t.allocator, conn).byId(t.allocator, 1),
    );
}

test "updating a row deleted underneath us is reported" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = SubscriptionRepo.init(t.allocator, conn);
    var s = try newSub("gone", .webhook, &.{"a.b"});
    defer s.deinit();
    try r.save(&s);

    try conn.execute("DELETE FROM subscriptions WHERE id = ?", .{s.id});
    try s.markDeliverySuccess(now0 + 1);
    try t.expectError(error.SubscriptionNotFound, r.save(&s));
}
