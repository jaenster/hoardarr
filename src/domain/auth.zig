//! The `auth` bounded context: human user accounts, browser sessions,
//! and the single config-driven API key.
//!
//! Two authentication modes live side by side, terminating at the same
//! `/api/v1` endpoints:
//!
//!   * **API key** — one shared secret, presented in `X-Api-Key` or the
//!     `apikey` query parameter. For *arr clients and SAB-API consumers
//!     that cannot do form-based auth. No users involved.
//!   * **User accounts** — username + password for the web UI, with the
//!     session token in an HTTP-only cookie.
//!
//! First-run flow: while the users table is empty the API exposes a
//! restricted `/api/v1/auth/setup` that creates the first admin. Once
//! any user exists, `/setup` is gone and login is required.
//!
//! # Where the crypto lives
//!
//! The `User` aggregate never sees a plaintext password and never
//! hashes one — it stores an opaque hash string and validates that it
//! is non-empty. Hashing is a boundary concern, and it sits in the
//! `password` namespace at the bottom of this file, separate from the
//! aggregate. Two properties of that namespace matter:
//!
//!   * It takes the **salt as a parameter**. `std.crypto.pwhash.bcrypt`'s
//!     `strHash` wants a `std.Io` to pull randomness from; the domain
//!     has no business holding one. `strHashWithSalt` is deterministic,
//!     so the caller at the boundary supplies 16 CSPRNG bytes and the
//!     tests supply a fixed salt and get reproducible vectors.
//!   * The cost and output format are pinned to what
//!     `internal/adapter/bcrypt` produced — cost 10, `$2a$` modular
//!     crypt. Getting either wrong locks out every existing account.
//!     See `password` for the details and the compatibility tests.

const std = @import("std");
const event = @import("event.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identifies a `User`. Allocated by the repository; 0 means "not yet
/// persisted".
pub const UserId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "auth.";

/// Permission tier. v0.1 has exactly one, and the type exists so adding
/// `read_only` later is a migration rather than a redesign. Go's
/// `Role("")` defaulted to admin; here the zero value *is* admin.
pub const Role = enum {
    admin,

    pub fn toString(self: Role) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Role {
        return std.meta.stringToEnum(Role, s);
    }
};

/// Emitted when the first admin is created (first-run) or any later user
/// is added.
pub const UserCreated = struct {
    id: UserId,
    /// Borrowed from the aggregate. Immutable for the User's lifetime —
    /// there is no rename path, the username is the lookup key.
    username: []const u8,
    role: Role,
    at: Timestamp,
};

/// The user changed their password, or had it changed for them.
pub const PasswordChanged = struct {
    id: UserId,
    at: Timestamp,
};

/// Successful authentication produced a fresh session.
pub const LoggedIn = struct {
    user_id: UserId,
    at: Timestamp,
};

/// A session was explicitly invalidated, as opposed to expiring.
pub const LoggedOut = struct {
    user_id: UserId,
    at: Timestamp,
};

pub const Kind = enum {
    user_created,
    password_changed,
    logged_in,
    logged_out,
};

pub const Event = union(Kind) {
    user_created: UserCreated,
    password_changed: PasswordChanged,
    logged_in: LoggedIn,
    logged_out: LoggedOut,

    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .user_created => topic_prefix ++ "user.created",
            .password_changed => topic_prefix ++ "user.password_changed",
            .logged_in => topic_prefix ++ "logged_in",
            .logged_out => topic_prefix ++ "logged_out",
        };
    }

    pub fn aggregateId(self: Event) UserId {
        return switch (self) {
            .user_created => |e| e.id,
            .password_changed => |e| e.id,
            .logged_in => |e| e.user_id,
            .logged_out => |e| e.user_id,
        };
    }

    pub fn occurredAt(self: Event) Timestamp {
        return switch (self) {
            inline else => |e| e.at,
        };
    }
};

/// Longest username we store. Anything the UI can render; the cap exists
/// so a hostile setup request can't put a megabyte in the users table.
pub const max_username_len = 64;

pub const ValidationError = error{
    UsernameRequired,
    UsernameTooLong,
    /// The username contained a byte below 0x21 (space and every control
    /// character) or 0x7f (DEL). See `validateUsername`.
    UsernameIllegalByte,
    /// The aggregate refuses to store an empty hash — that would make
    /// every password verify against it, depending on the hasher.
    PasswordHashRequired,
};

pub const InitError = ValidationError || Allocator.Error;

pub const NewUserParams = struct {
    username: []const u8,
    /// Already hashed. See the `password` namespace; the aggregate does
    /// not hash and cannot tell a bcrypt string from a random one.
    password_hash: []const u8,
    role: Role = .admin,
};

/// The snapshot the repository returns when loading a row. Trusted.
pub const HydrateParams = struct {
    id: UserId,
    username: []const u8,
    password_hash: []const u8,
    role: Role = .admin,
    created_at: Timestamp,
    updated_at: Timestamp,
};

/// The aggregate root: one account.
pub const User = struct {
    allocator: Allocator,

    id: UserId = 0,
    /// Lower-cased and trimmed on the way in, so lookup is a plain byte
    /// compare and "Admin" and "admin" cannot both exist.
    username: []const u8,
    /// Opaque. The aggregate only ever checks it is non-empty.
    password_hash: []const u8,
    role: Role,
    created_at: Timestamp,
    updated_at: Timestamp,

    events: event.Queue(Event) = .empty,

    pub fn init(allocator: Allocator, p: NewUserParams, now: Timestamp) InitError!User {
        const username = trim(p.username);
        try validateUsername(username);
        if (p.password_hash.len == 0) return error.PasswordHashRequired;

        const name = try allocator.alloc(u8, username.len);
        errdefer allocator.free(name);
        for (username, 0..) |c, i| name[i] = std.ascii.toLower(c);

        const hash = try allocator.dupe(u8, p.password_hash);
        errdefer allocator.free(hash);

        var u: User = .{
            .allocator = allocator,
            .username = name,
            .password_hash = hash,
            .role = p.role,
            .created_at = now,
            .updated_at = now,
        };
        try u.events.record(allocator, .{ .user_created = .{
            .id = 0,
            .username = u.username,
            .role = u.role,
            .at = now,
        } });
        return u;
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!User {
        const name = try allocator.dupe(u8, p.username);
        errdefer allocator.free(name);
        const hash = try allocator.dupe(u8, p.password_hash);
        return .{
            .allocator = allocator,
            .id = p.id,
            .username = name,
            .password_hash = hash,
            .role = p.role,
            .created_at = p.created_at,
            .updated_at = p.updated_at,
        };
    }

    pub fn deinit(self: *User) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.username);
        self.allocator.free(self.password_hash);
        self.* = undefined;
    }

    /// Assigns the database id after insert and patches the placeholder
    /// in the queued `UserCreated`.
    pub fn setId(self: *User, id: UserId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                .user_created => |*c| if (c.id == 0) {
                    c.id = id;
                },
                else => {},
            }
        }
    }

    /// Replaces the stored hash and records `PasswordChanged`.
    ///
    /// The previous hash is freed here, which is why `PasswordChanged`
    /// carries no hash: an event holding a pointer to it would dangle.
    pub fn setPasswordHash(self: *User, hash: []const u8, now: Timestamp) InitError!void {
        if (hash.len == 0) return error.PasswordHashRequired;
        const copy = try self.allocator.dupe(u8, hash);
        errdefer self.allocator.free(copy);
        // Reserve before mutating: nothing below may fail.
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);

        self.allocator.free(self.password_hash);
        self.password_hash = copy;
        self.updated_at = now;
        self.events.items.appendAssumeCapacity(.{ .password_changed = .{ .id = self.id, .at = now } });
    }

    /// Records `LoggedIn`. Called after the password verified and the
    /// session was minted, so the event commits with the session row.
    pub fn recordLogin(self: *User, now: Timestamp) Allocator.Error!void {
        try self.events.record(self.allocator, .{ .logged_in = .{ .user_id = self.id, .at = now } });
    }

    /// Records `LoggedOut` — an explicit invalidation, not an expiry.
    pub fn recordLogout(self: *User, now: Timestamp) Allocator.Error!void {
        try self.events.record(self.allocator, .{ .logged_out = .{ .user_id = self.id, .at = now } });
    }

    pub fn pullEvents(self: *User) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    pub fn pendingEvents(self: *const User) []const Event {
        return self.events.view();
    }
};

/// Default session lifetime when the caller doesn't state one.
pub const default_session_ttl_ms: event.Millis = 7 * event.day_ms;

/// Bytes of entropy behind a session token. 256 bits: guessing one is
/// not a threat model, and there is no derivation chain, so leaking one
/// session tells an attacker nothing about any other.
pub const session_entropy_bytes = 32;

/// Hex length of a session token.
pub const session_token_len = session_entropy_bytes * 2;

/// One authenticated browser session.
///
/// A pure value type — no allocator. The token is a fixed 64-byte hex
/// array rather than a slice, which is what lets a `Session` be copied,
/// stored in a map, and handed back from a repository without anyone
/// owning anything. Go used a heap `string` for the same 64 bytes.
pub const Session = struct {
    token: [session_token_len]u8,
    user_id: UserId,
    created_at: Timestamp,
    /// 0 means "unset", which `isValid` treats as invalid — the same
    /// reading Go gave `ExpiresAt.IsZero()`.
    expires_at: Timestamp,
    last_seen: Timestamp,

    pub const Error = error{UserIdRequired};

    /// Mints a session from caller-supplied entropy.
    ///
    /// `entropy` comes from the boundary's CSPRNG. Keeping it a
    /// parameter is what makes this testable at all: `crypto/rand`
    /// inside the constructor, as Go had it, means no test can assert
    /// anything about the token.
    ///
    /// `ttl_ms <= 0` selects `default_session_ttl_ms`, matching Go's
    /// treatment of a zero `TTL`.
    pub fn init(
        user_id: UserId,
        entropy: [session_entropy_bytes]u8,
        ttl_ms: event.Millis,
        now: Timestamp,
    ) Error!Session {
        if (user_id == 0) return error.UserIdRequired;
        const ttl = if (ttl_ms <= 0) default_session_ttl_ms else ttl_ms;
        return .{
            .token = std.fmt.bytesToHex(entropy, .lower),
            .user_id = user_id,
            .created_at = now,
            .expires_at = now + ttl,
            .last_seen = now,
        };
    }

    /// Still usable at `now`. Expiry is exclusive: a session expiring at
    /// T is not valid at T.
    pub fn isValid(self: Session, now: Timestamp) bool {
        return self.expires_at != 0 and now < self.expires_at;
    }

    /// Records activity. The `SessionStore.Touch` port's aggregate-side
    /// half; sliding expiry is deliberately *not* implemented, so an
    /// abandoned session still dies on schedule.
    pub fn touch(self: *Session, now: Timestamp) void {
        self.last_seen = now;
    }

    /// Milliseconds until expiry, 0 once expired.
    pub fn remainingMs(self: Session, now: Timestamp) event.Millis {
        if (!self.isValid(now)) return 0;
        return self.expires_at - now;
    }

    pub fn tokenSlice(self: *const Session) []const u8 {
        return &self.token;
    }
};

/// Constant-time comparison of a presented API key against the expected
/// one.
///
/// Both empties are rejected: an unconfigured key must not authenticate
/// an unconfigured request. Lengths that differ short-circuit to false,
/// but only after a compare of equal length has run, so the timing does
/// not leak the expected key's length.
pub fn apiKeyMatches(provided: []const u8, expected: []const u8) bool {
    if (provided.len == 0 or expected.len == 0) return false;
    if (provided.len != expected.len) {
        // Burn the same work a real compare would, against ourselves.
        _ = constantTimeEql(provided, provided);
        return false;
    }
    return constantTimeEql(provided, expected);
}

fn constantTimeEql(a: []const u8, b: []const u8) bool {
    std.debug.assert(a.len == b.len);
    var diff: u8 = 0;
    for (a, b) |x, y| diff |= x ^ y;
    // A branch on the accumulator is fine: it depends only on the final
    // result, which the caller learns anyway.
    return diff == 0;
}

/// Password hashing at the boundary.
///
/// # Compatibility contract
///
/// `internal/adapter/bcrypt` used `golang.org/x/crypto/bcrypt` with
/// `bcrypt.DefaultCost` — cost 10 — and emitted the `$2a$` modular
/// crypt form. Every stored hash in every existing database looks like
///
///     $2a$10$<22-char-salt><31-char-digest>
///
/// so both halves are pinned here:
///
///   * `cost` is 10. Not a config knob, for the same reason Go didn't
///     make it one: it is baked into every stored hash anyway, and the
///     hash string carries its own cost for verification.
///   * `hash` rewrites the variant byte `std` emits (`$2b$`) back to
///     `a`, so a hash we create is byte-identical to one Go created
///     from the same password and salt. Nothing downstream has to know
///     which implementation wrote a row.
///
/// `verify` accepts `$2a$`, `$2b$` and `$2y$`. The three differ only in
/// how they handle passwords past 72 bytes and embedded NULs; for the
/// inputs we allow the KDF is bit-identical, so normalising the variant
/// byte and comparing is correct. This matters because `std`'s
/// `strVerify` compares the *entire* hash string including the variant
/// byte, and would therefore reject every existing `$2a$` hash — which
/// is to say, lock out every existing account.
pub const password = struct {
    /// log2 of the bcrypt round count. Matches `bcrypt.DefaultCost`.
    pub const cost: u6 = 10;

    /// Length of the modular crypt string.
    pub const hash_len = std.crypto.pwhash.bcrypt.hash_length;
    pub const Hash = [hash_len]u8;

    /// Salt bytes the caller must supply.
    pub const salt_len = std.crypto.pwhash.bcrypt.salt_length;
    pub const Salt = [salt_len]u8;

    /// bcrypt's own hard limit. Go's x/crypto rejects anything longer
    /// with `ErrPasswordTooLong` rather than silently truncating, so we
    /// reject too — silently truncating would mean a 100-character
    /// passphrase is only as strong as its first 72 bytes, without the
    /// user ever being told.
    pub const max_password_len = 72;

    pub const HashError = error{
        EmptyPassword,
        PasswordTooLong,
    };

    pub const VerifyError = error{
        /// Wrong password. The only outcome a login handler may
        /// distinguish, and it must not distinguish it from "no such
        /// user" in its response.
        InvalidCredentials,
        /// The stored string is not a bcrypt modular crypt hash of a
        /// variant we understand. An operator-visible data problem, not
        /// a failed login.
        MalformedHash,
    };

    /// Deterministic bcrypt of `plain` with `salt`, written into `out`.
    /// Returns a slice of `out` so the result can go straight into a
    /// statement binding.
    pub fn hash(plain: []const u8, salt: Salt, out: *Hash) HashError![]const u8 {
        if (plain.len == 0) return error.EmptyPassword;
        if (plain.len > max_password_len) return error.PasswordTooLong;

        const s = std.crypto.pwhash.bcrypt.strHashWithSalt(plain, .{
            .params = .{ .rounds_log = cost, .silently_truncate_password = true },
            .encoding = .crypt,
        }, out, salt) catch unreachable; // `out` is exactly hash_len
        std.debug.assert(s.len == hash_len);
        // std emits the `$2b$` variant; Go emitted `$2a$`. Same KDF for
        // any input we accept, so pin the byte Go used.
        out[2] = 'a';
        return out[0..hash_len];
    }

    /// True iff `plain` hashes to `stored`.
    pub fn verify(stored: []const u8, plain: []const u8) VerifyError!void {
        if (stored.len != hash_len) return error.MalformedHash;
        if (stored[0] != '$' or stored[1] != '2' or stored[3] != '$') return error.MalformedHash;
        switch (stored[2]) {
            'a', 'b', 'y' => {},
            else => return error.MalformedHash,
        }
        if (plain.len > max_password_len) return error.InvalidCredentials;

        var normalised: Hash = undefined;
        @memcpy(&normalised, stored);
        normalised[2] = 'b';

        std.crypto.pwhash.bcrypt.strVerify(&normalised, plain, .{
            .silently_truncate_password = true,
        }) catch |err| return switch (err) {
            error.PasswordVerificationFailed => error.InvalidCredentials,
            else => error.MalformedHash,
        };
    }
};

/// Sentinel errors the repositories and the application service raise.
/// Go declared these next to the port interfaces; the ports themselves
/// live with their implementations in `store/`, but the vocabulary is
/// the domain's.
pub const RepositoryError = error{
    UserNotFound,
    SessionNotFound,
    /// The unique index on `username` rejected an insert.
    UsernameTaken,
    /// A setup request arrived when at least one user already exists.
    SetupAlreadyDone,
    SessionExpired,
};

// ---------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------

fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, &std.ascii.whitespace);
}

/// Non-empty after trim, at most 64 bytes, and no byte below 0x21 or
/// equal to 0x7f. That single range check rules out embedded spaces,
/// tabs, newlines and every C0/DEL control in one comparison — which
/// keeps usernames safe to interpolate into a log line, a cookie, or an
/// NNTP-adjacent header without further escaping.
fn validateUsername(v: []const u8) ValidationError!void {
    if (v.len == 0) return error.UsernameRequired;
    if (v.len > max_username_len) return error.UsernameTooLong;
    for (v) |b| {
        if (b < 0x21 or b == 0x7f) return error.UsernameIllegalByte;
    }
}

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no direct unit tests; these are written from the
// implementation and from how `app/auth/service.go` drives it. The
// `apiKeyMatches` cases are translated from `internal/server/auth_test.go`
// (`TestConstantTimeStringEq`).
// ---------------------------------------------------------------------

const t = std.testing;

test "init lower-cases, trims and records UserCreated" {
    var u = try User.init(t.allocator, .{
        .username = "  Admin  ",
        .password_hash = "$2a$10$notarealhash",
    }, 1_700_000_000_000);
    defer u.deinit();

    try t.expectEqualStrings("admin", u.username);
    try t.expectEqual(Role.admin, u.role);
    try t.expectEqual(@as(UserId, 0), u.id);
    try t.expectEqual(@as(Timestamp, 1_700_000_000_000), u.created_at);
    try t.expectEqual(u.created_at, u.updated_at);

    const batch = try u.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("auth.user.created", batch[0].topic());
    try t.expectEqualStrings("admin", batch[0].user_created.username);

    const again = try u.pullEvents();
    defer t.allocator.free(again);
    try t.expectEqual(@as(usize, 0), again.len);
}

test "init rejects invalid usernames and empty hashes" {
    const ok_hash = "hash";
    try t.expectError(error.UsernameRequired, User.init(t.allocator, .{ .username = "   ", .password_hash = ok_hash }, 1));
    try t.expectError(error.UsernameRequired, User.init(t.allocator, .{ .username = "", .password_hash = ok_hash }, 1));
    try t.expectError(error.UsernameIllegalByte, User.init(t.allocator, .{ .username = "two words", .password_hash = ok_hash }, 1));
    try t.expectError(error.UsernameIllegalByte, User.init(t.allocator, .{ .username = "tab\there", .password_hash = ok_hash }, 1));
    try t.expectError(error.UsernameIllegalByte, User.init(t.allocator, .{ .username = "del\x7f", .password_hash = ok_hash }, 1));
    try t.expectError(error.UsernameIllegalByte, User.init(t.allocator, .{ .username = "nul\x00", .password_hash = ok_hash }, 1));
    try t.expectError(error.PasswordHashRequired, User.init(t.allocator, .{ .username = "u", .password_hash = "" }, 1));

    const too_long = "a" ** (max_username_len + 1);
    try t.expectError(error.UsernameTooLong, User.init(t.allocator, .{ .username = too_long, .password_hash = ok_hash }, 1));

    // Exactly at the cap is fine.
    var u = try User.init(t.allocator, .{ .username = "a" ** max_username_len, .password_hash = ok_hash }, 1);
    defer u.deinit();
    try t.expectEqual(@as(usize, max_username_len), u.username.len);
}

test "setId patches the pending UserCreated" {
    var u = try User.init(t.allocator, .{ .username = "x", .password_hash = "h" }, 1);
    defer u.deinit();
    u.setId(7);

    const batch = try u.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(UserId, 7), batch[0].user_created.id);
    try t.expectEqual(@as(UserId, 7), batch[0].aggregateId());
}

test "setPasswordHash replaces the hash and records the change" {
    var u = try User.init(t.allocator, .{ .username = "x", .password_hash = "old" }, 1);
    defer u.deinit();
    u.setId(3);
    t.allocator.free(try u.pullEvents());

    try u.setPasswordHash("new", 50);
    try t.expectEqualStrings("new", u.password_hash);
    try t.expectEqual(@as(Timestamp, 50), u.updated_at);

    const batch = try u.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("auth.user.password_changed", batch[0].topic());
    try t.expectEqual(@as(UserId, 3), batch[0].aggregateId());
}

test "setPasswordHash refuses an empty hash and changes nothing" {
    var u = try User.init(t.allocator, .{ .username = "x", .password_hash = "keep" }, 1);
    defer u.deinit();
    t.allocator.free(try u.pullEvents());

    try t.expectError(error.PasswordHashRequired, u.setPasswordHash("", 50));
    try t.expectEqualStrings("keep", u.password_hash);
    try t.expectEqual(@as(Timestamp, 1), u.updated_at);
    try t.expectEqual(@as(usize, 0), u.pendingEvents().len);
}

test "login and logout events carry the user id" {
    var u = try User.init(t.allocator, .{ .username = "x", .password_hash = "h" }, 1);
    defer u.deinit();
    u.setId(11);
    t.allocator.free(try u.pullEvents());

    try u.recordLogin(20);
    try u.recordLogout(30);

    const batch = try u.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(usize, 2), batch.len);
    try t.expectEqualStrings("auth.logged_in", batch[0].topic());
    try t.expectEqual(@as(UserId, 11), batch[0].aggregateId());
    try t.expectEqual(@as(Timestamp, 20), batch[0].occurredAt());
    try t.expectEqualStrings("auth.logged_out", batch[1].topic());
    try t.expectEqual(@as(Timestamp, 30), batch[1].occurredAt());
}

test "hydrate restores a row without events" {
    var u = try User.hydrate(t.allocator, .{
        .id = 5,
        .username = "stored",
        .password_hash = "$2a$10$x",
        .created_at = 100,
        .updated_at = 200,
    });
    defer u.deinit();
    try t.expectEqual(@as(UserId, 5), u.id);
    try t.expectEqualStrings("stored", u.username);
    try t.expectEqual(@as(usize, 0), u.pendingEvents().len);
}

test "every event tag has a distinct prefixed topic" {
    var seen: [std.meta.fields(Kind).len][]const u8 = undefined;
    inline for (std.meta.fields(Kind), 0..) |f, i| {
        const e: Event = @unionInit(Event, f.name, std.mem.zeroes(@FieldType(Event, f.name)));
        const tp = e.topic();
        try t.expect(std.mem.startsWith(u8, tp, topic_prefix));
        for (seen[0..i]) |prev| try t.expect(!std.mem.eql(u8, prev, tp));
        seen[i] = tp;
    }
}

test "session token is 64 lowercase hex characters of the given entropy" {
    var entropy: [session_entropy_bytes]u8 = undefined;
    for (&entropy, 0..) |*b, i| b.* = @intCast(i);

    const s = try Session.init(9, entropy, 1000, 5_000);
    try t.expectEqual(@as(usize, 64), s.token.len);
    try t.expectEqualStrings(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        s.tokenSlice(),
    );
    try t.expectEqual(@as(UserId, 9), s.user_id);
    try t.expectEqual(@as(Timestamp, 5_000), s.created_at);
    try t.expectEqual(@as(Timestamp, 6_000), s.expires_at);
    try t.expectEqual(@as(Timestamp, 5_000), s.last_seen);
}

test "session rejects a zero user id" {
    const entropy: [session_entropy_bytes]u8 = @splat(0);
    try t.expectError(error.UserIdRequired, Session.init(0, entropy, 1000, 1));
}

test "a non-positive ttl falls back to seven days" {
    const entropy: [session_entropy_bytes]u8 = @splat(0);
    const zero = try Session.init(1, entropy, 0, 0);
    try t.expectEqual(default_session_ttl_ms, zero.expires_at);
    const negative = try Session.init(1, entropy, -1, 0);
    try t.expectEqual(default_session_ttl_ms, negative.expires_at);
    try t.expectEqual(@as(event.Millis, 604_800_000), default_session_ttl_ms);
}

test "session validity is exclusive at the expiry instant" {
    const entropy: [session_entropy_bytes]u8 = @splat(0);
    var s = try Session.init(1, entropy, 100, 1_000);

    try t.expect(s.isValid(1_000));
    try t.expect(s.isValid(1_099));
    try t.expect(!s.isValid(1_100));
    try t.expect(!s.isValid(9_999));
    try t.expectEqual(@as(event.Millis, 100), s.remainingMs(1_000));
    try t.expectEqual(@as(event.Millis, 1), s.remainingMs(1_099));
    try t.expectEqual(@as(event.Millis, 0), s.remainingMs(1_100));

    // Touching records activity but does not slide the expiry — an
    // abandoned tab must still die on schedule.
    s.touch(1_050);
    try t.expectEqual(@as(Timestamp, 1_050), s.last_seen);
    try t.expectEqual(@as(Timestamp, 1_100), s.expires_at);
    try t.expect(!s.isValid(1_100));
}

test "a session with no expiry is never valid" {
    const s: Session = .{
        .token = @splat('0'),
        .user_id = 1,
        .created_at = 0,
        .expires_at = 0,
        .last_seen = 0,
    };
    try t.expect(!s.isValid(0));
    try t.expect(!s.isValid(-1));
}

test "distinct entropy produces distinct tokens" {
    const a_entropy: [session_entropy_bytes]u8 = @splat(1);
    var b_entropy: [session_entropy_bytes]u8 = @splat(1);
    b_entropy[31] = 2;
    const a = try Session.init(1, a_entropy, 1, 0);
    const b = try Session.init(1, b_entropy, 1, 0);
    try t.expect(!std.mem.eql(u8, a.tokenSlice(), b.tokenSlice()));
}

test "apiKeyMatches" {
    // Translated from TestConstantTimeStringEq.
    try t.expect(!apiKeyMatches("", ""));
    try t.expect(!apiKeyMatches("abc", ""));
    try t.expect(!apiKeyMatches("", "abc"));
    try t.expect(apiKeyMatches("abc", "abc"));
    try t.expect(!apiKeyMatches("abc", "abcd"));
    try t.expect(!apiKeyMatches("abcd", "abc"));
    try t.expect(!apiKeyMatches("abc", "abd"));
    try t.expect(apiKeyMatches("abcdef0123", "abcdef0123"));
}

test "bcrypt cost and format match the Go adapter" {
    try t.expectEqual(@as(u6, 10), password.cost);
    try t.expectEqual(@as(usize, 60), password.hash_len);

    var out: password.Hash = undefined;
    const salt: password.Salt = @splat(0x42);
    const h = try password.hash("hunter2", salt, &out);

    try t.expectEqual(@as(usize, 60), h.len);
    try t.expectEqualStrings("$2a$10$", h[0..7]);
}

test "a hash we produce verifies, and a wrong password does not" {
    var out: password.Hash = undefined;
    const salt: password.Salt = @splat(0x11);
    const h = try password.hash("hunter2", salt, &out);

    try password.verify(h, "hunter2");
    try t.expectError(error.InvalidCredentials, password.verify(h, "hunter3"));
}

test "a hash written by golang.org/x/crypto/bcrypt still verifies" {
    // Generated by `bcrypt.GenerateFromPassword([]byte("hunter2"),
    // bcrypt.DefaultCost)`. std's own strVerify rejects this string
    // outright because it compares the `$2a$`/`$2b$` variant byte; the
    // whole point of `password.verify` is that it does not.
    const go_hash = "$2a$10$6nJnwhdu1hXwsZSwxsD4/.MI1CSi1AN8.wvvXJbLpPI.ALH89d5s2";
    try password.verify(go_hash, "hunter2");
    try t.expectError(error.InvalidCredentials, password.verify(go_hash, "wrong"));

    // Same digest, `$2y$` variant as some other tools write it.
    var y_variant: password.Hash = undefined;
    @memcpy(&y_variant, go_hash);
    y_variant[2] = 'y';
    try password.verify(&y_variant, "hunter2");
}

test "hash rejects passwords bcrypt cannot represent" {
    var out: password.Hash = undefined;
    const salt: password.Salt = @splat(0);
    try t.expectError(error.EmptyPassword, password.hash("", salt, &out));
    try t.expectError(error.PasswordTooLong, password.hash("a" ** 73, salt, &out));

    // 72 is the boundary and must be accepted; verified at cost 4 would
    // be faster, but the cost is pinned, so this is the one long hash we
    // pay for.
    const at_limit = try password.hash("a" ** 72, salt, &out);
    try t.expectEqual(@as(usize, 60), at_limit.len);
}

test "verify reports malformed stored hashes distinctly from bad passwords" {
    try t.expectError(error.MalformedHash, password.verify("", "x"));
    try t.expectError(error.MalformedHash, password.verify("not-a-hash", "x"));
    // Right length, wrong variant letter.
    try t.expectError(error.MalformedHash, password.verify("$2z$10$6nJnwhdu1hXwsZSwxsD4/.MI1CSi1AN8.wvvXJbLpPI.ALH89d5s2", "x"));
    // Right length, missing the `$` delimiters.
    try t.expectError(error.MalformedHash, password.verify("x2a$10$6nJnwhdu1hXwsZSwxsD4/.MI1CSi1AN8.wvvXJbLpPI.ALH89d5s2", "x"));
    // An over-long password is a failed login, not a data problem.
    try t.expectError(
        error.InvalidCredentials,
        password.verify("$2a$10$6nJnwhdu1hXwsZSwxsD4/.MI1CSi1AN8.wvvXJbLpPI.ALH89d5s2", "a" ** 73),
    );
}

test "role round-trips its persisted form" {
    try t.expectEqualStrings("admin", Role.admin.toString());
    try t.expectEqual(Role.admin, Role.parse("admin").?);
    try t.expectEqual(@as(?Role, null), Role.parse("root"));
}
