//! First-run setup, login, logout, password change, and the session
//! check the HTTP middleware runs on every request.
//!
//! # Two deliberate non-answers
//!
//!   * `login` and `changePassword` return `InvalidCredentials` for both
//!     "no such user" and "wrong password". Distinguishing them turns the
//!     login form into a username oracle.
//!   * `logout` is idempotent and never reports an unknown token, for the
//!     same reason: a different response for a valid-but-expired token
//!     than for a fabricated one is a signal worth harvesting.
//!
//! # Entropy is a parameter
//!
//! Session tokens come from a `Random` port rather than from a call to
//! the CSPRNG inside the constructor. Go's version called `crypto/rand`
//! directly, which meant no test could assert anything about a token —
//! not its length, not that two logins differ, not that it is the value
//! the store received. Production wires the real CSPRNG; tests wire a
//! counter.

const std = @import("std");
const log = @import("../core/log.zig");
const app_ports = @import("ports.zig");
const devent = @import("../domain/event.zig");
const dtx = @import("../domain/tx.zig");
const dauth = @import("../domain/auth.zig");
const sys = @import("../posix/sys.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const User = dauth.User;
pub const UserId = dauth.UserId;
pub const Session = dauth.Session;
pub const Role = dauth.Role;
pub const Sink = app_ports.EventSink(dauth.Event);

pub const StoreError = error{
    UserNotFound,
    /// The UNIQUE(username) constraint fired.
    UsernameTaken,
    SessionNotFound,
    Backend,
} || Allocator.Error;

/// User persistence.
pub const UserStore = struct {
    ctx: *anyopaque,
    countFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit) StoreError!u32,
    byIdFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, id: UserId) StoreError!*User,
    byUsernameFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, username: []const u8) StoreError!*User,
    releaseFn: *const fn (ctx: *anyopaque, u: *User) void,
    saveFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, u: *User) StoreError!void,

    pub fn count(self: UserStore, unit: ?*app_ports.Unit) StoreError!u32 {
        return self.countFn(self.ctx, unit);
    }

    pub fn byId(self: UserStore, unit: ?*app_ports.Unit, id: UserId) StoreError!*User {
        return self.byIdFn(self.ctx, unit, id);
    }

    pub fn byUsername(self: UserStore, unit: ?*app_ports.Unit, username: []const u8) StoreError!*User {
        return self.byUsernameFn(self.ctx, unit, username);
    }

    pub fn release(self: UserStore, u: *User) void {
        self.releaseFn(self.ctx, u);
    }

    /// Persist `u`. **Does not take ownership** — the caller still owns the
    /// aggregate and must free it. An implementation that wants to keep a
    /// copy has to make one.
    ///
    /// This was ambiguous once and the two implementations disagreed: the
    /// in-memory double adopted the pointer while the SQLite repo wrote
    /// columns and walked away, so the same caller leaked against one and
    /// double-freed against the other depending on which was wired in.
    pub fn save(self: UserStore, unit: ?*app_ports.Unit, u: *User) StoreError!void {
        return self.saveFn(self.ctx, unit, u);
    }
};

/// Session persistence. Sessions are pure values, so this port passes
/// them by copy and nothing here owns anything.
pub const SessionStore = struct {
    ctx: *anyopaque,
    getFn: *const fn (ctx: *anyopaque, token: []const u8) StoreError!Session,
    putFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, s: Session) StoreError!void,
    deleteFn: *const fn (ctx: *anyopaque, unit: ?*app_ports.Unit, token: []const u8) StoreError!void,
    /// Records activity. Best-effort at every call site.
    touchFn: *const fn (ctx: *anyopaque, token: []const u8, now: Timestamp) StoreError!void,

    pub fn get(self: SessionStore, token: []const u8) StoreError!Session {
        return self.getFn(self.ctx, token);
    }

    pub fn put(self: SessionStore, unit: ?*app_ports.Unit, s: Session) StoreError!void {
        return self.putFn(self.ctx, unit, s);
    }

    pub fn delete(self: SessionStore, unit: ?*app_ports.Unit, token: []const u8) StoreError!void {
        return self.deleteFn(self.ctx, unit, token);
    }

    pub fn touch(self: SessionStore, token: []const u8, now: Timestamp) StoreError!void {
        return self.touchFn(self.ctx, token, now);
    }
};

/// Session-token entropy.
pub const Random = struct {
    ctx: *anyopaque,
    fillFn: *const fn (ctx: *anyopaque, buf: []u8) void,

    pub fn fill(self: Random, buf: []u8) void {
        self.fillFn(self.ctx, buf);
    }
};

/// The system CSPRNG.
pub const system_random: Random = .{ .ctx = undefined, .fillFn = &sysFill };

fn sysFill(_: *anyopaque, buf: []u8) void {
    sys.randomBytes(buf);
}

pub const Error = error{
    /// Wrong password, or no such user. Never distinguish them.
    InvalidCredentials,
    /// Setup was attempted when a user already exists.
    SetupAlreadyDone,
    /// The session exists but has passed its expiry.
    SessionExpired,
    /// The new password does not meet the minimum length.
    PasswordTooShort,
    /// The username or password failed the aggregate's own validation.
    InvalidUser,
} || StoreError || app_ports.PublishError || app_ports.TxError;

/// Minimum password length, checked before hashing so a short password
/// never costs a bcrypt round.
pub const min_password_len: usize = 8;

pub const Service = struct {
    gpa: Allocator,
    users: UserStore,
    sessions: SessionStore,
    sink: Sink,
    txm: app_ports.Manager,
    clock: app_ports.Clock,
    random: Random = system_random,
    logger: *log.Logger = &log.default,
    session_ttl_ms: Millis = dauth.default_session_ttl_ms,

    /// Whether no user exists yet. The REST layer gates the setup
    /// endpoint on this so a curious browser does not even see the form.
    pub fn needsSetup(self: *Service) Error!bool {
        return (try self.users.count(null)) == 0;
    }

    /// Creates the first admin user.
    ///
    /// Concurrent calls are serialised by the UNIQUE(username)
    /// constraint: the loser sees `UsernameTaken` and converts it to
    /// `SetupAlreadyDone`, which is the honest answer — somebody else
    /// completed setup.
    pub fn setupAdmin(self: *Service, username: []const u8, password: []const u8) Error!UserId {
        if (password.len < min_password_len) return error.PasswordTooShort;

        const Args = struct {
            svc: *Service,
            username: []const u8,
            password: []const u8,
            out: *UserId,
        };
        var out: UserId = 0;
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                if ((try s.users.count(unit)) > 0) return error.SetupAlreadyDone;

                var hash: dauth.password.Hash = undefined;
                var salt: dauth.password.Salt = undefined;
                s.random.fill(&salt);
                const encoded = dauth.password.hash(args.password, salt, &hash) catch
                    return error.InvalidUser;

                const u = try s.gpa.create(User);
                u.* = User.init(s.gpa, .{
                    .username = args.username,
                    .password_hash = encoded,
                    .role = .admin,
                }, s.clock.now()) catch |e| {
                    s.gpa.destroy(u);
                    if (e == error.OutOfMemory) return error.OutOfMemory;
                    return error.InvalidUser;
                };
                defer {
                    u.deinit();
                    s.gpa.destroy(u);
                }

                s.users.save(unit, u) catch |e| {
                    // Another setup request won the race.
                    if (e == error.UsernameTaken) return error.SetupAlreadyDone;
                    return e;
                };
                // Not adopted: `UserStore.save` never takes ownership, so
                // the defer above is what frees this. Marking it adopted
                // leaked one User per admin setup against the real store.
                args.out.* = u.id;
                const events = try u.pullEvents();
                defer devent.deinitAll(dauth.Event, s.gpa, events);
                try s.sink.publish(unit, events);
            }
        };
        try dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .username = username,
            .password = password,
            .out = &out,
        }, Body.run);
        return out;
    }

    /// Verifies credentials and mints a session.
    pub fn login(self: *Service, username: []const u8, password: []const u8) Error!Session {
        const u = self.users.byUsername(null, username) catch |e| {
            if (e == error.UserNotFound) return error.InvalidCredentials;
            return e;
        };
        defer self.users.release(u);

        dauth.password.verify(u.password_hash, password) catch return error.InvalidCredentials;

        var entropy: [dauth.session_entropy_bytes]u8 = undefined;
        self.random.fill(&entropy);
        const sess = Session.init(u.id, entropy, self.session_ttl_ms, self.clock.now()) catch
            return error.InvalidCredentials;

        const Args = struct { svc: *Service, sess: Session, user_id: UserId };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                try s.sessions.put(unit, args.sess);
                try s.sink.publish(unit, &.{.{ .logged_in = .{
                    .user_id = args.user_id,
                    .at = s.clock.now(),
                } }});
            }
        };
        try dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .sess = sess,
            .user_id = u.id,
        }, Body.run);
        return sess;
    }

    /// Replaces a user's password after verifying the old one.
    ///
    /// Other sessions are deliberately *not* invalidated. That matches
    /// SABnzbd and the *arr suite, and avoids logging the operator out of
    /// the browser they just changed their password in. A "sign out
    /// everywhere" affordance is a separate, explicit action.
    pub fn changePassword(
        self: *Service,
        user_id: UserId,
        old_password: []const u8,
        new_password: []const u8,
    ) Error!void {
        if (new_password.len < min_password_len) return error.PasswordTooShort;
        const u = self.users.byId(null, user_id) catch |e| {
            if (e == error.UserNotFound) return error.InvalidCredentials;
            return e;
        };
        defer self.users.release(u);
        dauth.password.verify(u.password_hash, old_password) catch return error.InvalidCredentials;

        var hash: dauth.password.Hash = undefined;
        var salt: dauth.password.Salt = undefined;
        self.random.fill(&salt);
        const encoded = dauth.password.hash(new_password, salt, &hash) catch return error.InvalidUser;

        const Args = struct { svc: *Service, u: *User, encoded: []const u8 };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                args.u.setPasswordHash(args.encoded, s.clock.now()) catch |e| {
                    if (e == error.OutOfMemory) return error.OutOfMemory;
                    return error.InvalidUser;
                };
                try s.users.save(unit, args.u);
                const events = try args.u.pullEvents();
                defer devent.deinitAll(dauth.Event, s.gpa, events);
                try s.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .u = u, .encoded = encoded }, Body.run);
    }

    /// Invalidates a session. Idempotent, and silent about unknown
    /// tokens.
    pub fn logout(self: *Service, token: []const u8) Error!void {
        // Best-effort read so `LoggedOut` can carry the right user id.
        const user_id: UserId = blk: {
            const s = self.sessions.get(token) catch |e| {
                if (e == error.SessionNotFound) break :blk 0;
                return e;
            };
            break :blk s.user_id;
        };

        const Args = struct { svc: *Service, token: []const u8, user_id: UserId };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                s.sessions.delete(unit, args.token) catch |e| {
                    if (e != error.SessionNotFound) return e;
                };
                if (args.user_id == 0) return;
                try s.sink.publish(unit, &.{.{ .logged_out = .{
                    .user_id = args.user_id,
                    .at = s.clock.now(),
                } }});
            }
        };
        return dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .token = token,
            .user_id = user_id,
        }, Body.run);
    }

    /// Resolves a session token to its user. The HTTP middleware's entry
    /// point.
    ///
    /// An expired session is deleted on the way past, so a stale cookie
    /// stops costing a lookup on every subsequent request.
    pub fn authenticate(self: *Service, token: []const u8) Error!*User {
        const sess = try self.sessions.get(token);
        if (!sess.isValid(self.clock.now())) {
            self.sessions.delete(null, token) catch {};
            return error.SessionExpired;
        }
        const u = try self.users.byId(null, sess.user_id);
        // Best-effort: a failure to record activity must not fail the
        // request the operator is actually making.
        self.sessions.touch(token, self.clock.now()) catch {};
        return u;
    }
};

// =====================================================================
// Test doubles
// =====================================================================

pub const FakeUsers = struct {
    gpa: Allocator,
    items: std.ArrayList(*User) = .empty,
    next_id: UserId = 1,
    saves: usize = 0,
    loads: usize = 0,
    releases: usize = 0,
    fail_save: ?StoreError = null,

    pub fn init(gpa: Allocator) FakeUsers {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeUsers) void {
        for (self.items.items) |u| {
            u.deinit();
            self.gpa.destroy(u);
        }
        self.items.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn store(self: *FakeUsers) UserStore {
        return .{
            .ctx = @ptrCast(self),
            .countFn = &count,
            .byIdFn = &byId,
            .byUsernameFn = &byUsername,
            .releaseFn = &release,
            .saveFn = &save,
        };
    }

    pub fn get(self: *FakeUsers, id: UserId) ?*User {
        for (self.items.items) |u| {
            if (u.id == id) return u;
        }
        return null;
    }

    pub fn leakFree(self: *const FakeUsers) bool {
        return self.loads == self.releases;
    }

    fn count(ctx: *anyopaque, _: ?*app_ports.Unit) StoreError!u32 {
        const self: *FakeUsers = @ptrCast(@alignCast(ctx));
        return @intCast(self.items.items.len);
    }

    fn byId(ctx: *anyopaque, _: ?*app_ports.Unit, id: UserId) StoreError!*User {
        const self: *FakeUsers = @ptrCast(@alignCast(ctx));
        const u = self.get(id) orelse return error.UserNotFound;
        self.loads += 1;
        return u;
    }

    fn byUsername(ctx: *anyopaque, _: ?*app_ports.Unit, username: []const u8) StoreError!*User {
        const self: *FakeUsers = @ptrCast(@alignCast(ctx));
        for (self.items.items) |u| {
            if (std.mem.eql(u8, u.username, username)) {
                self.loads += 1;
                return u;
            }
        }
        return error.UserNotFound;
    }

    fn release(ctx: *anyopaque, _: *User) void {
        const self: *FakeUsers = @ptrCast(@alignCast(ctx));
        self.releases += 1;
    }

    fn save(ctx: *anyopaque, _: ?*app_ports.Unit, u: *User) StoreError!void {
        const self: *FakeUsers = @ptrCast(@alignCast(ctx));
        if (self.fail_save) |e| {
            self.fail_save = null;
            return e;
        }
        if (u.id == 0) {
            for (self.items.items) |existing| {
                if (std.mem.eql(u8, existing.username, u.username)) return error.UsernameTaken;
            }
            u.setId(self.next_id);
            self.next_id += 1;

            // Clone rather than adopt: `save` does not take ownership, and
            // the real SQLite repo doesn't either. A double that keeps the
            // caller's pointer would let an ownership bug pass here and
            // only show up in production.
            const copy = try self.gpa.create(User);
            errdefer self.gpa.destroy(copy);
            copy.* = User.init(self.gpa, .{
                .username = u.username,
                .password_hash = u.password_hash,
                .role = u.role,
            }, u.created_at) catch return error.Backend;
            copy.setId(u.id);
            try self.items.append(self.gpa, copy);
        }
        self.saves += 1;
    }
};

pub const FakeSessions = struct {
    gpa: Allocator,
    items: std.ArrayList(Session) = .empty,
    touches: usize = 0,
    deletes: usize = 0,

    pub fn init(gpa: Allocator) FakeSessions {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeSessions) void {
        self.items.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn store(self: *FakeSessions) SessionStore {
        return .{
            .ctx = @ptrCast(self),
            .getFn = &get,
            .putFn = &put,
            .deleteFn = &del,
            .touchFn = &touch,
        };
    }

    pub fn len(self: *const FakeSessions) usize {
        return self.items.items.len;
    }

    fn find(self: *FakeSessions, token: []const u8) ?*Session {
        for (self.items.items) |*s| {
            if (std.mem.eql(u8, s.tokenSlice(), token)) return s;
        }
        return null;
    }

    fn get(ctx: *anyopaque, token: []const u8) StoreError!Session {
        const self: *FakeSessions = @ptrCast(@alignCast(ctx));
        const s = self.find(token) orelse return error.SessionNotFound;
        return s.*;
    }

    fn put(ctx: *anyopaque, _: ?*app_ports.Unit, s: Session) StoreError!void {
        const self: *FakeSessions = @ptrCast(@alignCast(ctx));
        try self.items.append(self.gpa, s);
    }

    fn del(ctx: *anyopaque, _: ?*app_ports.Unit, token: []const u8) StoreError!void {
        const self: *FakeSessions = @ptrCast(@alignCast(ctx));
        for (self.items.items, 0..) |*s, i| {
            if (!std.mem.eql(u8, s.tokenSlice(), token)) continue;
            _ = self.items.orderedRemove(i);
            self.deletes += 1;
            return;
        }
        return error.SessionNotFound;
    }

    fn touch(ctx: *anyopaque, token: []const u8, now: Timestamp) StoreError!void {
        const self: *FakeSessions = @ptrCast(@alignCast(ctx));
        const s = self.find(token) orelse return error.SessionNotFound;
        s.touch(now);
        self.touches += 1;
    }
};

/// Deterministic "entropy": a counter, so a test can assert that two
/// logins produce different tokens and that a token is what the store
/// received.
pub const FakeRandom = struct {
    seed: u8 = 0,

    pub fn random(self: *FakeRandom) Random {
        return .{ .ctx = @ptrCast(self), .fillFn = &fill };
    }

    fn fill(ctx: *anyopaque, buf: []u8) void {
        const self: *FakeRandom = @ptrCast(@alignCast(ctx));
        self.seed +%= 1;
        for (buf, 0..) |*b, i| b.* = self.seed +% @as(u8, @truncate(i));
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;

const Harness = struct {
    users: FakeUsers = undefined,
    sessions: FakeSessions = undefined,
    sink: app_ports.FakeSink(dauth.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 1_000_000 },
    rng: FakeRandom = .{},
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.users = FakeUsers.init(testing.allocator);
        self.sessions = FakeSessions.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .users = self.users.store(),
            .sessions = self.sessions.store(),
            .sink = self.sink.sink(),
            .txm = self.ftx.manager(),
            .clock = self.clock.clock(),
            .random = self.rng.random(),
            .logger = &self.logger,
        };
    }

    fn deinit(self: *Harness) void {
        self.sessions.deinit();
        self.users.deinit();
    }
};

test "setup creates the first admin and publishes UserCreated" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    try testing.expect(try h.svc.needsSetup());
    const id = try h.svc.setupAdmin("operator", "correct horse");
    try testing.expectEqual(@as(UserId, 1), id);
    try testing.expect(!try h.svc.needsSetup());

    const u = h.users.get(id).?;
    try testing.expectEqualStrings("operator", u.username);
    try testing.expectEqual(Role.admin, u.role);
    // The password is stored hashed, never in the clear.
    try testing.expect(std.mem.indexOf(u8, u.password_hash, "correct horse") == null);
    try testing.expect(h.sink.has("auth.user.created"));
    try testing.expect(h.ftx.balanced());
}

test "a second setup is refused once a user exists" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.svc.setupAdmin("first", "correct horse");
    h.sink.reset();

    try testing.expectError(error.SetupAlreadyDone, h.svc.setupAdmin("second", "another one"));
    try testing.expectEqual(@as(usize, 1), h.users.items.items.len);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.ftx.balanced());
}

test "losing the setup race reads as already-done, not as a conflict" {
    // Both requests pass the count check; the UNIQUE constraint decides.
    // The loser must not see a database error.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.users.fail_save = error.UsernameTaken;
    try testing.expectError(error.SetupAlreadyDone, h.svc.setupAdmin("operator", "correct horse"));
    try testing.expectEqual(@as(usize, 0), h.users.items.items.len);
}

test "a short password is refused before any hashing happens" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.PasswordTooShort, h.svc.setupAdmin("operator", "short"));
    try testing.expectEqual(@as(u32, 0), h.ftx.begins);
}

test "login mints a session and publishes LoggedIn" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const id = try h.svc.setupAdmin("operator", "correct horse");
    h.sink.reset();

    const sess = try h.svc.login("operator", "correct horse");
    try testing.expectEqual(id, sess.user_id);
    try testing.expectEqual(@as(usize, dauth.session_token_len), sess.tokenSlice().len);
    try testing.expectEqual(h.clock.t + dauth.default_session_ttl_ms, sess.expires_at);
    try testing.expectEqual(@as(usize, 1), h.sessions.len());
    try testing.expect(h.sink.has("auth.logged_in"));
}

test "two logins produce different tokens" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.svc.setupAdmin("operator", "correct horse");
    const a = try h.svc.login("operator", "correct horse");
    const b = try h.svc.login("operator", "correct horse");
    try testing.expect(!std.mem.eql(u8, a.tokenSlice(), b.tokenSlice()));
    try testing.expectEqual(@as(usize, 2), h.sessions.len());
}

test "a wrong password and an unknown user are indistinguishable" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.svc.setupAdmin("operator", "correct horse");

    try testing.expectError(error.InvalidCredentials, h.svc.login("operator", "wrong horse"));
    try testing.expectError(error.InvalidCredentials, h.svc.login("nobody", "correct horse"));
    // Neither attempt minted a session or published anything.
    try testing.expectEqual(@as(usize, 0), h.sessions.len());
    try testing.expect(h.users.leakFree());
}

test "a custom session TTL is honoured" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.svc.session_ttl_ms = 60_000;
    _ = try h.svc.setupAdmin("operator", "correct horse");
    const sess = try h.svc.login("operator", "correct horse");
    try testing.expectEqual(h.clock.t + 60_000, sess.expires_at);
}

test "authenticate resolves a live session and records activity" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const id = try h.svc.setupAdmin("operator", "correct horse");
    const sess = try h.svc.login("operator", "correct horse");

    h.clock.advance(1_000);
    const u = try h.svc.authenticate(sess.tokenSlice());
    defer h.users.store().release(u);
    try testing.expectEqual(id, u.id);
    try testing.expectEqual(@as(usize, 1), h.sessions.touches);
}

test "an expired session is rejected and cleaned up on the way past" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.svc.session_ttl_ms = 1_000;
    _ = try h.svc.setupAdmin("operator", "correct horse");
    const sess = try h.svc.login("operator", "correct horse");

    h.clock.advance(1_000);
    try testing.expectError(error.SessionExpired, h.svc.authenticate(sess.tokenSlice()));
    // Deleted, so a stale cookie stops costing a lookup per request.
    try testing.expectEqual(@as(usize, 0), h.sessions.len());
    try testing.expectEqual(@as(usize, 1), h.sessions.deletes);
}

test "an unknown token is a plain not-found" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.SessionNotFound, h.svc.authenticate("0" ** 64));
}

test "logout deletes the session and publishes LoggedOut" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const id = try h.svc.setupAdmin("operator", "correct horse");
    const sess = try h.svc.login("operator", "correct horse");
    h.sink.reset();

    try h.svc.logout(sess.tokenSlice());
    try testing.expectEqual(@as(usize, 0), h.sessions.len());
    try testing.expect(h.sink.has("auth.logged_out"));
    try testing.expectEqual(@as(UserId, id), id);
}

test "logout of an unknown token is silent and successful" {
    // A different answer for a fabricated token than for a real one is a
    // signal worth harvesting.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.svc.logout("f" ** 64);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.ftx.balanced());

    // And logging out twice is fine.
    _ = try h.svc.setupAdmin("operator", "correct horse");
    const sess = try h.svc.login("operator", "correct horse");
    try h.svc.logout(sess.tokenSlice());
    try h.svc.logout(sess.tokenSlice());
}

test "changePassword verifies the old one and re-hashes" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const id = try h.svc.setupAdmin("operator", "correct horse");
    const u = h.users.get(id).?;
    const old_hash = try testing.allocator.dupe(u8, u.password_hash);
    defer testing.allocator.free(old_hash);
    h.sink.reset();

    try h.svc.changePassword(id, "correct horse", "battery staple");
    try testing.expect(!std.mem.eql(u8, old_hash, u.password_hash));
    try testing.expect(h.sink.has("auth.user.password_changed"));

    // The new password works and the old one does not.
    _ = try h.svc.login("operator", "battery staple");
    try testing.expectError(error.InvalidCredentials, h.svc.login("operator", "correct horse"));
}

test "changePassword refuses a wrong old password and an unknown user alike" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const id = try h.svc.setupAdmin("operator", "correct horse");

    try testing.expectError(error.InvalidCredentials, h.svc.changePassword(id, "nope", "battery staple"));
    try testing.expectError(error.InvalidCredentials, h.svc.changePassword(999, "correct horse", "battery staple"));
    try testing.expectError(error.PasswordTooShort, h.svc.changePassword(id, "correct horse", "short"));
    // The old password still works, so nothing was half-applied.
    _ = try h.svc.login("operator", "correct horse");
    try testing.expect(h.users.leakFree());
}

test "existing sessions survive a password change" {
    // Matches SABnzbd and the *arr suite: changing your password in one
    // browser must not log you out of it.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const id = try h.svc.setupAdmin("operator", "correct horse");
    const sess = try h.svc.login("operator", "correct horse");

    try h.svc.changePassword(id, "correct horse", "battery staple");
    const u = try h.svc.authenticate(sess.tokenSlice());
    h.users.store().release(u);
    try testing.expectEqual(@as(usize, 1), h.sessions.len());
}

test "a publish failure rolls the login back" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.svc.setupAdmin("operator", "correct horse");
    h.sink.fail = error.Backend;
    try testing.expectError(error.Backend, h.svc.login("operator", "correct horse"));
    try testing.expect(h.ftx.rollbacks >= 1);
    try testing.expect(h.ftx.balanced());
    try testing.expect(h.users.leakFree());
}
