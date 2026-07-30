//! `users` and `sessions`.
//!
//! Two repositories in one file because they are one concern: a session is
//! meaningless without its user, and `ON DELETE CASCADE` on
//! `sessions.user_id` means removing a user logs them out everywhere for
//! free.
//!
//! Usernames are stored lowercased, which is what makes login
//! case-insensitive without a second index or a `COLLATE NOCASE` that
//! would then apply to every comparison. The UNIQUE index is on the stored
//! (lowercased) form, so "Admin" and "admin" cannot both exist.
//!
//! A UNIQUE violation on insert becomes `UsernameTaken` rather than
//! propagating: the signup path shows that message to a human, and leaking
//! SQL text into a web response is both ugly and a small information leak.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const migrate = @import("migrate.zig");
const auth = @import("../domain/auth.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const User = auth.User;
const Session = auth.Session;

pub const Error = sqlite.Error || auth.RepositoryError || error{
    /// A stored `role` this binary does not know.
    UnknownRole,
    /// A stored session token is not the expected length, so the row
    /// cannot be turned into a `Session`.
    MalformedToken,
};

const user_columns = "id, username, password_hash, role, created_at, updated_at";

pub const UserRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) UserRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    pub fn save(self: UserRepo, u: *User) Error!void {
        if (u.id == 0) return self.insert(u);
        return self.update(u);
    }

    fn insert(self: UserRepo, u: *User) Error!void {
        var lower_buf: [auth.max_username_len]u8 = undefined;
        const lower = lowercase(&lower_buf, u.username) catch return error.UsernameTaken;
        self.conn.execute(
            \\INSERT INTO users(username, password_hash, role, created_at, updated_at)
            \\VALUES (?, ?, ?, ?, ?)
        , .{ lower, u.password_hash, u.role.toString(), u.created_at, u.updated_at }) catch |e| {
            if (e == error.ConstraintUnique) return error.UsernameTaken;
            return e;
        };
        u.setId(self.conn.lastInsertRowid());
    }

    /// The username is deliberately not updatable: it is the login
    /// identifier and every session, audit line and event already refers
    /// to it. Renaming is "create a new user".
    fn update(self: UserRepo, u: *const User) Error!void {
        try self.conn.execute(
            "UPDATE users SET password_hash = ?, role = ?, updated_at = ? WHERE id = ?",
            .{ u.password_hash, u.role.toString(), u.updated_at, u.id },
        );
        if (self.conn.changes() == 0) return error.UserNotFound;
    }

    pub fn byId(self: UserRepo, gpa: Allocator, id: auth.UserId) Error!User {
        var st = self.conn.queryRow(
            "SELECT " ++ user_columns ++ " FROM users WHERE id = ?",
            .{id},
        ) catch |e| {
            if (e == error.NoRows) return error.UserNotFound;
            return e;
        };
        defer st.release();
        return hydrateUser(gpa, &st);
    }

    /// Case-insensitive by construction: the stored form is lowercased, so
    /// lowercasing the input is the whole comparison.
    pub fn byUsername(self: UserRepo, gpa: Allocator, username: []const u8) Error!User {
        var lower_buf: [auth.max_username_len]u8 = undefined;
        const lower = lowercase(&lower_buf, username) catch return error.UserNotFound;
        var st = self.conn.queryRow(
            "SELECT " ++ user_columns ++ " FROM users WHERE username = ?",
            .{lower},
        ) catch |e| {
            if (e == error.NoRows) return error.UserNotFound;
            return e;
        };
        defer st.release();
        return hydrateUser(gpa, &st);
    }

    /// Total users. Zero means first run, which is what unlocks the
    /// `/auth/setup` endpoint — so this is a security-relevant count, not
    /// a statistic.
    pub fn count(self: UserRepo) Error!i64 {
        return self.conn.scalarInt("SELECT COUNT(*) FROM users", .{});
    }

    pub fn remove(self: UserRepo, id: auth.UserId) Error!void {
        try self.conn.execute("DELETE FROM users WHERE id = ?", .{id});
        if (self.conn.changes() == 0) return error.UserNotFound;
    }

    fn hydrateUser(gpa: Allocator, st: *sqlite.Stmt) Error!User {
        return User.hydrate(gpa, .{
            .id = st.int(0),
            .username = st.text(1),
            .password_hash = st.text(2),
            .role = auth.Role.parse(st.text(3)) orelse return error.UnknownRole,
            .created_at = st.int(4),
            .updated_at = st.int(5),
        }) catch return error.OutOfMemory;
    }
};

pub const SessionRepo = struct {
    conn: *Conn,

    pub fn init(conn: *Conn) SessionRepo {
        return .{ .conn = conn };
    }

    pub fn put(self: SessionRepo, s: Session) Error!void {
        try self.conn.execute(
            \\INSERT INTO sessions(token, user_id, created_at, expires_at, last_seen)
            \\VALUES (?, ?, ?, ?, ?)
        , .{ s.tokenSlice(), s.user_id, s.created_at, s.expires_at, s.last_seen });
    }

    /// Look up by token. Expiry is *not* checked here: the caller decides
    /// what to do with an expired session (usually delete it and redirect),
    /// and a repository that silently hid rows would make that impossible.
    pub fn get(self: SessionRepo, token: []const u8) Error!Session {
        var st = self.conn.queryRow(
            "SELECT user_id, created_at, expires_at, last_seen FROM sessions WHERE token = ?",
            .{token},
        ) catch |e| {
            if (e == error.NoRows) return error.SessionNotFound;
            return e;
        };
        defer st.release();

        if (token.len != auth.session_token_len) return error.MalformedToken;
        var out = Session{
            .token = undefined,
            .user_id = st.int(0),
            .created_at = st.int(1),
            .expires_at = st.int(2),
            .last_seen = st.int(3),
        };
        @memcpy(&out.token, token);
        return out;
    }

    /// Bump `last_seen`. Best-effort by design: a request must not fail
    /// because the idle-session bookkeeping could not be written.
    pub fn touch(self: SessionRepo, token: []const u8, now: i64) Error!void {
        try self.conn.execute("UPDATE sessions SET last_seen = ? WHERE token = ?", .{ now, token });
    }

    /// Logout. Absent is success — a client presenting a token we already
    /// dropped is logged out either way.
    pub fn remove(self: SessionRepo, token: []const u8) Error!void {
        try self.conn.execute("DELETE FROM sessions WHERE token = ?", .{token});
    }

    /// "Log me out everywhere", and the mandatory follow-up to a password
    /// change. Rides the `sessions_user` index.
    pub fn removeForUser(self: SessionRepo, user_id: auth.UserId) Error!i64 {
        try self.conn.execute("DELETE FROM sessions WHERE user_id = ?", .{user_id});
        return self.conn.changes();
    }

    /// Reap sessions that expired before `now`. Run from the scheduler.
    pub fn purgeExpired(self: SessionRepo, now: i64) Error!i64 {
        try self.conn.execute("DELETE FROM sessions WHERE expires_at < ?", .{now});
        return self.conn.changes();
    }
};

/// Lowercase into `buf`, trimming surrounding whitespace first.
///
/// ASCII-only: usernames are validated to ASCII by the domain, and a
/// Unicode-aware fold would mean two names that differ only by a
/// non-ASCII case pair could collide differently in the index than in the
/// comparison.
fn lowercase(buf: []u8, s: []const u8) error{TooLong}![]const u8 {
    const trimmed = std.mem.trim(u8, s, &std.ascii.whitespace);
    if (trimmed.len > buf.len) return error.TooLong;
    for (trimmed, 0..) |ch, i| buf[i] = std.ascii.toLower(ch);
    return buf[0..trimmed.len];
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

fn newUser(username: []const u8) !User {
    return User.init(t.allocator, .{
        .username = username,
        .password_hash = "$2b$10$abcdefghijklmnopqrstuv",
    }, 1000);
}

test "a user round-trips and is found case-insensitively" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);

    var u = try newUser("Admin");
    defer u.deinit();
    try r.save(&u);
    try t.expect(u.id != 0);

    // Stored lowercased, so any casing on the way in finds it.
    for ([_][]const u8{ "admin", "Admin", "ADMIN", "  admin  " }) |spelling| {
        var got = try r.byUsername(t.allocator, spelling);
        defer got.deinit();
        try t.expectEqual(u.id, got.id);
        try t.expectEqualStrings("admin", got.username);
    }
}

test "a duplicate username is UsernameTaken regardless of casing" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);

    var first = try newUser("admin");
    defer first.deinit();
    try r.save(&first);

    var second = try newUser("ADMIN");
    defer second.deinit();
    try t.expectError(error.UsernameTaken, r.save(&second));
}

test "count drives first-run detection" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);

    // Zero is what unlocks /auth/setup, so it is security-relevant.
    try t.expectEqual(@as(i64, 0), try r.count());
    var u = try newUser("first");
    defer u.deinit();
    try r.save(&u);
    try t.expectEqual(@as(i64, 1), try r.count());
}

test "a password change is persisted without touching the username" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);

    var u = try newUser("admin");
    defer u.deinit();
    try r.save(&u);
    try u.setPasswordHash("$2b$10$NEWNEWNEWNEWNEWNEWNEW", 2000);
    try r.save(&u);

    var got = try r.byId(t.allocator, u.id);
    defer got.deinit();
    try t.expectEqualStrings("$2b$10$NEWNEWNEWNEWNEWNEWNEW", got.password_hash);
    try t.expectEqualStrings("admin", got.username);
    try t.expectEqual(@as(i64, 2000), got.updated_at);
}

test "a missing user is UserNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);
    try t.expectError(error.UserNotFound, r.byId(t.allocator, 1));
    try t.expectError(error.UserNotFound, r.byUsername(t.allocator, "nobody"));
    try t.expectError(error.UserNotFound, r.remove(1));
}

test "an unknown role is rejected rather than downgraded to admin" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try r.save(&u);

    // Guessing here would be a privilege decision made by a parse error.
    try conn.execute("UPDATE users SET role = ? WHERE id = ?", .{ "superduper", u.id });
    try t.expectError(error.UnknownRole, r.byId(t.allocator, u.id));
}

fn session(user_id: auth.UserId, now: i64) Session {
    var entropy: [auth.session_entropy_bytes]u8 = undefined;
    for (&entropy, 0..) |*b, i| b.* = @intCast((i * 7 + @as(usize, @intCast(user_id))) & 0xFF);
    return Session.init(user_id, entropy, auth.default_session_ttl_ms, now) catch unreachable;
}

test "a session round-trips" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try users.save(&u);

    const sessions = SessionRepo.init(conn);
    const s = session(u.id, 1000);
    try sessions.put(s);

    const got = try sessions.get(s.tokenSlice());
    try t.expectEqual(u.id, got.user_id);
    try t.expectEqual(@as(i64, 1000), got.created_at);
    try t.expectEqual(s.expires_at, got.expires_at);
    try t.expectEqualStrings(s.tokenSlice(), got.tokenSlice());
}

test "get returns an expired session so the caller decides what to do" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try users.save(&u);

    const sessions = SessionRepo.init(conn);
    const s = session(u.id, 1000);
    try sessions.put(s);

    // Hiding it here would leave the caller unable to distinguish "expired,
    // clear the cookie" from "forged token".
    const got = try sessions.get(s.tokenSlice());
    try t.expect(!got.isValid(s.expires_at + 1));
    try t.expect(got.isValid(1001));
}

test "touch moves last_seen" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try users.save(&u);

    const sessions = SessionRepo.init(conn);
    const s = session(u.id, 1000);
    try sessions.put(s);
    try sessions.touch(s.tokenSlice(), 5000);

    const got = try sessions.get(s.tokenSlice());
    try t.expectEqual(@as(i64, 5000), got.last_seen);
}

test "logout is idempotent and a forged token is SessionNotFound" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try users.save(&u);

    const sessions = SessionRepo.init(conn);
    const s = session(u.id, 1000);
    try sessions.put(s);

    try sessions.remove(s.tokenSlice());
    // A client presenting a token we already dropped is logged out either
    // way, so a second remove is success.
    try sessions.remove(s.tokenSlice());
    try t.expectError(error.SessionNotFound, sessions.get(s.tokenSlice()));
    try t.expectError(error.SessionNotFound, sessions.get("0" ** auth.session_token_len));
}

test "removeForUser logs a user out everywhere" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var a = try newUser("a");
    defer a.deinit();
    try users.save(&a);
    var b = try newUser("b");
    defer b.deinit();
    try users.save(&b);

    const sessions = SessionRepo.init(conn);
    const a1 = session(a.id, 1000);
    var a2 = session(a.id, 1000);
    // A second, distinct token for the same user.
    a2.token[0] = if (a1.token[0] == 'f') 'a' else 'f';
    try sessions.put(a1);
    try sessions.put(a2);
    try sessions.put(session(b.id, 1000));

    try t.expectEqual(@as(i64, 2), try sessions.removeForUser(a.id));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM sessions", .{}));
}

test "deleting a user cascades to their sessions" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try users.save(&u);

    const sessions = SessionRepo.init(conn);
    try sessions.put(session(u.id, 1000));
    try users.remove(u.id);
    // The FK cascade is why "log out everywhere on delete" needs no code.
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM sessions", .{}));
}

test "a session for an unknown user is refused by the foreign key" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const sessions = SessionRepo.init(conn);
    try t.expectError(error.ConstraintForeignKey, sessions.put(session(999, 1000)));
}

test "purgeExpired reaps only what has expired" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const users = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try users.save(&u);

    const sessions = SessionRepo.init(conn);
    const old = session(u.id, 1000);
    var fresh = session(u.id, 1000);
    fresh.token[0] = if (old.token[0] == 'f') 'a' else 'f';
    fresh.expires_at = old.expires_at + 1_000_000;
    try sessions.put(old);
    try sessions.put(fresh);

    try t.expectEqual(@as(i64, 1), try sessions.purgeExpired(old.expires_at + 1));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM sessions", .{}));
}

test "updating a user deleted underneath us is reported" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = UserRepo.init(t.allocator, conn);
    var u = try newUser("admin");
    defer u.deinit();
    try r.save(&u);

    try conn.execute("DELETE FROM users WHERE id = ?", .{u.id});
    try u.setPasswordHash("$2b$10$otherotherotherother", 2000);
    try t.expectError(error.UserNotFound, r.save(&u));
}
