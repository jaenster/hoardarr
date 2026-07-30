//! The `server` bounded context: Usenet provider accounts — hosts,
//! credentials, connection caps, priority order, metered quota.
//!
//! Aggregate root: `UsenetServer`. The download orchestrator asks this
//! context *where* to dispatch an article fetch; it never handles a
//! host, port or password itself. The NNTP adapter receives a whole
//! `UsenetServer` when it builds a connection.
//!
//! # Ownership
//!
//! A `UsenetServer` owns heap copies of its four string fields (name,
//! host, username, password) — hence `init(allocator, ...)` / `deinit`.
//! Everything else is a scalar. The alternative, borrowing the caller's
//! strings, would tie the aggregate's lifetime to whatever parsed the
//! REST body, and the aggregate outlives that by design (it is cached
//! and reused for every dispatch decision).
//!
//! Events borrow: `ServerAdded.name` and `ServerRemoved.name` point at
//! the aggregate's own `name`, which is immutable for its whole
//! lifetime — the Go original had no rename path either, because the
//! name is a lookup key. A pulled batch is therefore valid until
//! `deinit`, and must be consumed (or its strings duped) before then.
//!
//! # Deviation from Go
//!
//! `Update` in Go validated and applied field by field, so
//! `Update{Host: "ok", Port: 0}` mutated the host and *then* returned
//! the port error — leaving the aggregate half-changed and the caller
//! believing nothing happened. Here validation runs over the whole
//! parameter set before a single field moves, so a rejected update is a
//! no-op. Same for allocation failure.

const std = @import("std");
const event = @import("event.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identifies a `UsenetServer`. Allocated by the repository; 0 means
/// "not yet persisted".
pub const ServerId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "server.usenet.";

/// How the provider charges, which is what decides dispatch preference.
///
/// The orchestrator prefers `flat` over `metered` inside a tier: a block
/// account is a reserve for rare or old articles, not the first thing
/// you burn.
pub const BillingMode = enum {
    flat,
    metered,

    /// The persisted / on-the-wire form. `@tagName` already matches the
    /// Go string constants, so there is no translation table.
    pub fn toString(self: BillingMode) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?BillingMode {
        return std.meta.stringToEnum(BillingMode, s);
    }
};

/// Emitted after a new server is persisted for the first time.
pub const ServerAdded = struct {
    id: ServerId,
    /// Borrowed from the aggregate.
    name: []const u8,
    at: Timestamp,
};

/// Emitted for any subset of the mutable fields changing. Deliberately
/// carries no diff — a subscriber that needs one re-reads the row.
pub const ServerUpdated = struct {
    id: ServerId,
    at: Timestamp,
};

pub const ServerEnabled = struct {
    id: ServerId,
    at: Timestamp,
};

pub const ServerDisabled = struct {
    id: ServerId,
    at: Timestamp,
};

/// Recorded immediately before the row is deleted, so the event commits
/// in the same transaction as the DELETE. The aggregate is gone after
/// this; subscribers must not try to load it.
pub const ServerRemoved = struct {
    id: ServerId,
    /// Borrowed from the aggregate — valid only until `deinit`.
    name: []const u8,
    at: Timestamp,
};

pub const Kind = enum {
    added,
    updated,
    enabled,
    disabled,
    removed,
};

pub const Event = union(Kind) {
    added: ServerAdded,
    updated: ServerUpdated,
    enabled: ServerEnabled,
    disabled: ServerDisabled,
    removed: ServerRemoved,

    /// Comptime-known constant per tag: a jump table returning pointers
    /// into .rodata, no formatting.
    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .added => topic_prefix ++ "added",
            .updated => topic_prefix ++ "updated",
            .enabled => topic_prefix ++ "enabled",
            .disabled => topic_prefix ++ "disabled",
            .removed => topic_prefix ++ "removed",
        };
    }

    pub fn aggregateId(self: Event) ServerId {
        return switch (self) {
            inline else => |e| e.id,
        };
    }

    pub fn occurredAt(self: Event) Timestamp {
        return switch (self) {
            inline else => |e| e.at,
        };
    }
};

/// Every way a caller can hand this context something invalid.
pub const ValidationError = error{
    NameRequired,
    HostRequired,
    PortOutOfRange,
    MaxConnsOutOfRange,
    /// A credential contained CR, LF or NUL — see `validateCredential`.
    CredentialControlByte,
    NegativeQuota,
    NegativeBandwidth,
};

pub const InitError = ValidationError || Allocator.Error;

/// Constructor inputs. The optional fields carry the common-case
/// default, which is why `tls` is `?bool` rather than `bool`: Go used a
/// `*bool` for exactly this reason — "unset" must be distinguishable
/// from "explicitly off", or every new server silently loses TLS.
pub const NewParams = struct {
    name: []const u8,
    host: []const u8,
    /// Wider than the stored `u16` so the domain, not the JSON decoder,
    /// owns the range check.
    port: i32,
    tls: ?bool = null,
    username: []const u8 = "",
    password: []const u8 = "",
    /// 0 → the default of 8.
    max_conns: i32 = 0,
    priority: i32 = 0,
    /// Backups are consulted only after every non-backup tier reported
    /// the article missing, regardless of priority.
    backup: bool = false,
    billing_mode: BillingMode = .flat,
    /// Total purchased byte budget for a metered account. 0 means
    /// "unknown / unlimited" — we never auto-disable on a guess.
    quota_bytes: i64 = 0,
    /// Per-server throughput cap. 0 means "no per-server cap"; the
    /// global cap still applies.
    bandwidth_bytes_per_sec: i64 = 0,
};

/// The snapshot a repository hands back for a stored row. Bypasses
/// validation — the database is trusted, and rejecting a row we
/// ourselves wrote would only make the server unbootable.
pub const HydrateParams = struct {
    id: ServerId,
    name: []const u8,
    host: []const u8,
    port: u16,
    tls: bool,
    username: []const u8 = "",
    password: []const u8 = "",
    max_conns: u16,
    priority: i32 = 0,
    enabled: bool,
    backup: bool = false,
    billing_mode: BillingMode = .flat,
    quota_bytes: i64 = 0,
    used_bytes: i64 = 0,
    bandwidth_bytes_per_sec: i64 = 0,
    added_at: Timestamp,
    updated_at: Timestamp,
};

/// Which fields an `update` should touch. `null` means "leave alone".
/// `name` is absent on purpose: it is the lookup key, and Go had no
/// rename path either.
pub const UpdateParams = struct {
    host: ?[]const u8 = null,
    port: ?i32 = null,
    tls: ?bool = null,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    max_conns: ?i32 = null,
    priority: ?i32 = null,
    backup: ?bool = null,
    billing_mode: ?BillingMode = null,
    quota_bytes: ?i64 = null,
    bandwidth_bytes_per_sec: ?i64 = null,
};

/// Default connection cap when the caller doesn't state one. Eight is
/// what most providers allow on a consumer plan.
pub const default_max_conns: u16 = 8;

/// One Usenet provider account.
///
/// Fields are public for reading — Zig has no property syntax and a
/// wall of one-line accessors buys nothing — but every mutation goes
/// through a method that checks the invariants and records the event.
pub const UsenetServer = struct {
    allocator: Allocator,

    id: ServerId = 0,
    name: []const u8,
    host: []const u8,
    port: u16,
    tls: bool,
    username: []const u8,
    password: []const u8,
    max_conns: u16,
    priority: i32,
    enabled: bool,
    backup: bool,
    billing_mode: BillingMode,
    quota_bytes: i64,
    /// Monotonic counter, bumped by the fetcher after each successful
    /// body download.
    used_bytes: i64 = 0,
    bandwidth_bytes_per_sec: i64,

    added_at: Timestamp,
    updated_at: Timestamp,

    events: event.Queue(Event) = .empty,

    /// Validates, copies the strings, and records `ServerAdded` with a
    /// placeholder id of 0 — `setId` patches it once the repository has
    /// assigned the real one.
    pub fn init(allocator: Allocator, p: NewParams, now: Timestamp) InitError!UsenetServer {
        const name = trim(p.name);
        const host = trim(p.host);
        if (name.len == 0) return error.NameRequired;
        if (host.len == 0) return error.HostRequired;
        if (p.port <= 0 or p.port > 65535) return error.PortOutOfRange;

        const max_conns: i32 = if (p.max_conns == 0) default_max_conns else p.max_conns;
        if (max_conns < 1 or max_conns > 65535) return error.MaxConnsOutOfRange;

        try validateCredential(p.username);
        try validateCredential(p.password);
        if (p.quota_bytes < 0) return error.NegativeQuota;
        if (p.bandwidth_bytes_per_sec < 0) return error.NegativeBandwidth;

        // Allocate all four strings before touching anything, so a
        // mid-way OOM leaves nothing to unwind but the strings we own.
        var owned = try OwnedStrings.dupe(allocator, name, host, p.username, p.password);
        errdefer owned.free(allocator);

        var s: UsenetServer = .{
            .allocator = allocator,
            .name = owned.name,
            .host = owned.host,
            .port = @intCast(p.port),
            .tls = p.tls orelse true,
            .username = owned.username,
            .password = owned.password,
            .max_conns = @intCast(max_conns),
            .priority = p.priority,
            .enabled = true,
            .backup = p.backup,
            .billing_mode = p.billing_mode,
            .quota_bytes = p.quota_bytes,
            .bandwidth_bytes_per_sec = p.bandwidth_bytes_per_sec,
            .added_at = now,
            .updated_at = now,
        };
        try s.events.record(allocator, .{ .added = .{ .id = 0, .name = s.name, .at = now } });
        return s;
    }

    /// Rebuilds from persistence. No events.
    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!UsenetServer {
        var owned = try OwnedStrings.dupe(allocator, p.name, p.host, p.username, p.password);
        errdefer owned.free(allocator);
        return .{
            .allocator = allocator,
            .id = p.id,
            .name = owned.name,
            .host = owned.host,
            .port = p.port,
            .tls = p.tls,
            .username = owned.username,
            .password = owned.password,
            .max_conns = p.max_conns,
            .priority = p.priority,
            .enabled = p.enabled,
            .backup = p.backup,
            .billing_mode = p.billing_mode,
            .quota_bytes = p.quota_bytes,
            .used_bytes = p.used_bytes,
            .bandwidth_bytes_per_sec = p.bandwidth_bytes_per_sec,
            .added_at = p.added_at,
            .updated_at = p.updated_at,
        };
    }

    pub fn deinit(self: *UsenetServer) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.name);
        self.allocator.free(self.host);
        self.allocator.free(self.username);
        self.allocator.free(self.password);
        self.* = undefined;
    }

    /// Whether a metered account has burnt its budget.
    ///
    /// Flat accounts and metered accounts with `quota_bytes == 0`
    /// (unknown) always report false: auto-disable only fires when the
    /// operator explicitly stated a quota. Guessing here would silently
    /// take a working provider offline.
    pub fn quotaExhausted(self: *const UsenetServer) bool {
        if (self.billing_mode != .metered or self.quota_bytes == 0) return false;
        return self.used_bytes >= self.quota_bytes;
    }

    /// Whether the orchestrator may dispatch to this server right now.
    pub fn usable(self: *const UsenetServer) bool {
        return self.enabled and !self.quotaExhausted();
    }

    /// Bumps the byte counter after a successful BODY download.
    /// Non-positive deltas are ignored — the fetcher reports 0 for a
    /// 430, and a negative would corrupt the quota accounting.
    pub fn addBytesUsed(self: *UsenetServer, n: i64, now: Timestamp) void {
        if (n <= 0) return;
        self.used_bytes += n;
        self.updated_at = now;
    }

    /// Assigns the database id after a first successful save, and
    /// patches the placeholder in the queued `ServerAdded`.
    pub fn setId(self: *UsenetServer, id: ServerId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                .added => |*a| if (a.id == 0) {
                    a.id = id;
                },
                else => {},
            }
        }
    }

    /// Flips the soft-enable flag. A no-op when the flag already holds
    /// the requested value, so a repeated PUT doesn't spam subscribers.
    pub fn setEnabled(self: *UsenetServer, enabled: bool, now: Timestamp) Allocator.Error!void {
        if (self.enabled == enabled) return;
        self.enabled = enabled;
        self.updated_at = now;
        try self.events.record(self.allocator, if (enabled)
            .{ .enabled = .{ .id = self.id, .at = now } }
        else
            .{ .disabled = .{ .id = self.id, .at = now } });
    }

    /// Applies a batch of field changes and records one `ServerUpdated`
    /// describing the result. A no-op — and therefore eventless — when
    /// every supplied field already holds the given value.
    ///
    /// Validation is complete before the first mutation: see the module
    /// header.
    pub fn update(self: *UsenetServer, p: UpdateParams, now: Timestamp) InitError!void {
        // --- validate everything first ---------------------------------
        var new_host: ?[]const u8 = null;
        if (p.host) |raw| {
            const v = trim(raw);
            if (v.len == 0) return error.HostRequired;
            if (!std.mem.eql(u8, v, self.host)) new_host = v;
        }
        if (p.port) |v| {
            if (v <= 0 or v > 65535) return error.PortOutOfRange;
        }
        if (p.max_conns) |v| {
            if (v < 1 or v > 65535) return error.MaxConnsOutOfRange;
        }
        var new_username: ?[]const u8 = null;
        if (p.username) |v| {
            try validateCredential(v);
            if (!std.mem.eql(u8, v, self.username)) new_username = v;
        }
        var new_password: ?[]const u8 = null;
        if (p.password) |v| {
            try validateCredential(v);
            if (!std.mem.eql(u8, v, self.password)) new_password = v;
        }
        if (p.quota_bytes) |v| {
            if (v < 0) return error.NegativeQuota;
        }
        if (p.bandwidth_bytes_per_sec) |v| {
            if (v < 0) return error.NegativeBandwidth;
        }

        // --- allocate the replacements, still without mutating ---------
        var host_copy: ?[]u8 = null;
        var user_copy: ?[]u8 = null;
        var pass_copy: ?[]u8 = null;
        errdefer {
            if (host_copy) |c| self.allocator.free(c);
            if (user_copy) |c| self.allocator.free(c);
            if (pass_copy) |c| self.allocator.free(c);
        }
        if (new_host) |v| host_copy = try self.allocator.dupe(u8, v);
        if (new_username) |v| user_copy = try self.allocator.dupe(u8, v);
        if (new_password) |v| pass_copy = try self.allocator.dupe(u8, v);

        // Reserve the event slot too: past this point nothing may fail.
        try self.events.items.ensureUnusedCapacity(self.allocator, 2);

        // --- apply -----------------------------------------------------
        var changed = false;
        if (host_copy) |c| {
            self.allocator.free(self.host);
            self.host = c;
            changed = true;
        }
        if (user_copy) |c| {
            self.allocator.free(self.username);
            self.username = c;
            changed = true;
        }
        if (pass_copy) |c| {
            self.allocator.free(self.password);
            self.password = c;
            changed = true;
        }
        if (p.port) |v| {
            const port: u16 = @intCast(v);
            if (port != self.port) {
                self.port = port;
                changed = true;
            }
        }
        if (p.tls) |v| if (v != self.tls) {
            self.tls = v;
            changed = true;
        };
        if (p.max_conns) |v| {
            const mc: u16 = @intCast(v);
            if (mc != self.max_conns) {
                self.max_conns = mc;
                changed = true;
            }
        }
        if (p.priority) |v| if (v != self.priority) {
            self.priority = v;
            changed = true;
        };
        if (p.backup) |v| if (v != self.backup) {
            self.backup = v;
            changed = true;
        };
        if (p.billing_mode) |v| if (v != self.billing_mode) {
            self.billing_mode = v;
            changed = true;
        };
        if (p.quota_bytes) |v| if (v != self.quota_bytes) {
            self.quota_bytes = v;
            changed = true;
        };
        if (p.bandwidth_bytes_per_sec) |v| if (v != self.bandwidth_bytes_per_sec) {
            self.bandwidth_bytes_per_sec = v;
            changed = true;
        };

        if (changed) {
            self.updated_at = now;
            self.events.items.appendAssumeCapacity(.{ .updated = .{ .id = self.id, .at = now } });
        }
    }

    /// Records `ServerRemoved` just before the row is deleted, so the
    /// event lands in the same transaction as the DELETE. Go built this
    /// event in the application service, which meant a crash between
    /// DELETE and publish lost it.
    pub fn markRemoved(self: *UsenetServer, now: Timestamp) Allocator.Error!void {
        try self.events.record(self.allocator, .{
            .removed = .{ .id = self.id, .name = self.name, .at = now },
        });
    }

    /// Hands the buffered events, and the slice holding them, to the
    /// caller. Call it after the repository save succeeds.
    pub fn pullEvents(self: *UsenetServer) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    /// Read-only peek, for assertions and for a bus that publishes
    /// without taking ownership.
    pub fn pendingEvents(self: *const UsenetServer) []const Event {
        return self.events.view();
    }
};

/// Orders enabled servers the way the orchestrator dispatches: backups
/// last unconditionally, then ascending priority (lower number = tried
/// first), then flat before metered within a tier, then by id so the
/// order is total and therefore stable across restarts.
pub fn dispatchLessThan(_: void, a: *const UsenetServer, b: *const UsenetServer) bool {
    if (a.backup != b.backup) return !a.backup;
    if (a.priority != b.priority) return a.priority < b.priority;
    if (a.billing_mode != b.billing_mode) return a.billing_mode == .flat;
    return a.id < b.id;
}

/// Errors a `UsenetServer` repository raises that callers branch on.
pub const RepositoryError = error{
    ServerNotFound,
    /// `name` is unique; a second insert under the same name lands here.
    DuplicateName,
};

// ---------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------

fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, &std.ascii.whitespace);
}

/// Rejects credentials that could corrupt the NNTP wire protocol.
///
/// NNTP commands are CRLF-terminated, so a CR or LF inside `AUTHINFO
/// USER <name>` lets whoever can write the config append arbitrary
/// commands to the same connection. NUL is rejected because it
/// terminates the string for any C-side consumer.
///
/// Empty is allowed: anonymous-access servers exist.
fn validateCredential(v: []const u8) ValidationError!void {
    for (v) |b| {
        if (b == '\r' or b == '\n' or b == 0) return error.CredentialControlByte;
    }
}

/// The four owned strings, allocated as a unit so an OOM part-way
/// through construction has exactly one cleanup path.
const OwnedStrings = struct {
    name: []u8,
    host: []u8,
    username: []u8,
    password: []u8,

    fn dupe(
        allocator: Allocator,
        name: []const u8,
        host: []const u8,
        username: []const u8,
        password: []const u8,
    ) Allocator.Error!OwnedStrings {
        const n = try allocator.dupe(u8, name);
        errdefer allocator.free(n);
        const h = try allocator.dupe(u8, host);
        errdefer allocator.free(h);
        const u = try allocator.dupe(u8, username);
        errdefer allocator.free(u);
        const p = try allocator.dupe(u8, password);
        return .{ .name = n, .host = h, .username = u, .password = p };
    }

    fn free(self: OwnedStrings, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.host);
        allocator.free(self.username);
        allocator.free(self.password);
    }
};

// ---------------------------------------------------------------------
// Tests
//
// The first six mirror `internal/domain/server/server_test.go`; the rest
// cover the billing/quota/backup behaviour the Go suite never touched.
// ---------------------------------------------------------------------

const t = std.testing;

fn newTestServer(now: Timestamp) !UsenetServer {
    return UsenetServer.init(t.allocator, .{
        .name = "x",
        .host = "h",
        .port = 1,
    }, now);
}

test "init applies the documented defaults and records ServerAdded" {
    const now: Timestamp = 1_700_000_000_000;
    var s = try UsenetServer.init(t.allocator, .{
        .name = "main",
        .host = "news.example.com",
        .port = 563,
        .username = "u",
        .password = "p",
    }, now);
    defer s.deinit();

    try t.expect(s.tls);
    try t.expectEqual(default_max_conns, s.max_conns);
    try t.expect(s.enabled);
    try t.expect(!s.backup);
    try t.expectEqual(BillingMode.flat, s.billing_mode);
    try t.expectEqual(@as(i64, 0), s.quota_bytes);
    try t.expectEqual(now, s.added_at);
    try t.expectEqual(now, s.updated_at);

    const batch = try s.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqual(Kind.added, std.meta.activeTag(batch[0]));
    try t.expectEqualStrings("server.usenet.added", batch[0].topic());

    // Drained.
    const again = try s.pullEvents();
    defer t.allocator.free(again);
    try t.expectEqual(@as(usize, 0), again.len);
}

test "init trims and rejects invalid input" {
    const now: Timestamp = 1;

    try t.expectError(error.NameRequired, UsenetServer.init(t.allocator, .{ .name = "  ", .host = "h", .port = 563 }, now));
    try t.expectError(error.HostRequired, UsenetServer.init(t.allocator, .{ .name = "n", .host = "", .port = 563 }, now));
    try t.expectError(error.PortOutOfRange, UsenetServer.init(t.allocator, .{ .name = "n", .host = "h", .port = 0 }, now));
    try t.expectError(error.PortOutOfRange, UsenetServer.init(t.allocator, .{ .name = "n", .host = "h", .port = 99999 }, now));
    try t.expectError(error.NegativeQuota, UsenetServer.init(t.allocator, .{ .name = "n", .host = "h", .port = 1, .quota_bytes = -1 }, now));
    try t.expectError(error.NegativeBandwidth, UsenetServer.init(t.allocator, .{ .name = "n", .host = "h", .port = 1, .bandwidth_bytes_per_sec = -1 }, now));
    try t.expectError(error.MaxConnsOutOfRange, UsenetServer.init(t.allocator, .{ .name = "n", .host = "h", .port = 1, .max_conns = -3 }, now));

    // max_conns 0 is not an error — it means "use the default".
    var ok = try UsenetServer.init(t.allocator, .{ .name = "  n  ", .host = " h ", .port = 1, .max_conns = 0 }, now);
    defer ok.deinit();
    try t.expectEqualStrings("n", ok.name);
    try t.expectEqualStrings("h", ok.host);
    try t.expectEqual(default_max_conns, ok.max_conns);
}

test "init rejects credentials that could inject NNTP commands" {
    const now: Timestamp = 1;
    try t.expectError(error.CredentialControlByte, UsenetServer.init(t.allocator, .{
        .name = "n",
        .host = "h",
        .port = 1,
        .username = "user\r\nAUTHINFO PASS hunter2",
    }, now));
    try t.expectError(error.CredentialControlByte, UsenetServer.init(t.allocator, .{
        .name = "n",
        .host = "h",
        .port = 1,
        .password = "pass\nQUIT",
    }, now));
    try t.expectError(error.CredentialControlByte, UsenetServer.init(t.allocator, .{
        .name = "n",
        .host = "h",
        .port = 1,
        .password = "pass\x00",
    }, now));

    // Empty credentials are fine — anonymous servers exist.
    var anon = try UsenetServer.init(t.allocator, .{ .name = "n", .host = "h", .port = 1 }, now);
    defer anon.deinit();
    try t.expectEqual(@as(usize, 0), anon.username.len);
}

test "setId patches the pending ServerAdded" {
    var s = try newTestServer(1);
    defer s.deinit();
    s.setId(42);

    const batch = try s.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(ServerId, 42), batch[0].added.id);
    try t.expectEqual(@as(ServerId, 42), batch[0].aggregateId());
}

test "setEnabled emits the matching event and stays quiet on a no-op" {
    const now: Timestamp = 5;
    var s = try newTestServer(now);
    defer s.deinit();
    s.setId(1);
    t.allocator.free(try s.pullEvents()); // drain ServerAdded

    try s.setEnabled(true, now); // already enabled
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);

    try s.setEnabled(false, now + 1);
    {
        const batch = try s.pullEvents();
        defer t.allocator.free(batch);
        try t.expectEqual(@as(usize, 1), batch.len);
        try t.expectEqual(Kind.disabled, std.meta.activeTag(batch[0]));
        try t.expectEqual(@as(Timestamp, 6), batch[0].occurredAt());
    }
    try t.expect(!s.enabled);
    try t.expectEqual(@as(Timestamp, 6), s.updated_at);

    try s.setEnabled(true, now + 2);
    {
        const batch = try s.pullEvents();
        defer t.allocator.free(batch);
        try t.expectEqual(Kind.enabled, std.meta.activeTag(batch[0]));
    }
}

test "update with no effective change emits nothing" {
    const now: Timestamp = 10;
    var s = try newTestServer(now);
    defer s.deinit();
    s.setId(1);
    t.allocator.free(try s.pullEvents());

    try s.update(.{ .host = "h" }, now + 100);
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
    // updated_at must not move for a no-op either.
    try t.expectEqual(now, s.updated_at);
}

test "update applies every mutable field and emits one ServerUpdated" {
    const now: Timestamp = 1;
    var s = try newTestServer(now);
    defer s.deinit();
    s.setId(7);
    t.allocator.free(try s.pullEvents());

    const later = now + event.hour_ms;
    try s.update(.{
        .host = "new",
        .port = 563,
        .tls = false,
        .username = "user",
        .password = "pass",
        .max_conns = 20,
        .priority = 2,
        .backup = true,
        .billing_mode = .metered,
        .quota_bytes = 1024,
        .bandwidth_bytes_per_sec = 2048,
    }, later);

    try t.expectEqualStrings("new", s.host);
    try t.expectEqual(@as(u16, 563), s.port);
    try t.expect(!s.tls);
    try t.expectEqualStrings("user", s.username);
    try t.expectEqualStrings("pass", s.password);
    try t.expectEqual(@as(u16, 20), s.max_conns);
    try t.expectEqual(@as(i32, 2), s.priority);
    try t.expect(s.backup);
    try t.expectEqual(BillingMode.metered, s.billing_mode);
    try t.expectEqual(@as(i64, 1024), s.quota_bytes);
    try t.expectEqual(@as(i64, 2048), s.bandwidth_bytes_per_sec);
    try t.expectEqual(later, s.updated_at);

    const batch = try s.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqual(Kind.updated, std.meta.activeTag(batch[0]));
    try t.expectEqual(@as(ServerId, 7), batch[0].aggregateId());
}

test "update rejects invalid values" {
    var s = try newTestServer(1);
    defer s.deinit();

    try t.expectError(error.PortOutOfRange, s.update(.{ .port = 0 }, 2));
    try t.expectError(error.PortOutOfRange, s.update(.{ .port = 70000 }, 2));
    try t.expectError(error.MaxConnsOutOfRange, s.update(.{ .max_conns = 0 }, 2));
    try t.expectError(error.HostRequired, s.update(.{ .host = "   " }, 2));
    try t.expectError(error.CredentialControlByte, s.update(.{ .username = "a\rb" }, 2));
    try t.expectError(error.NegativeQuota, s.update(.{ .quota_bytes = -1 }, 2));
    try t.expectError(error.NegativeBandwidth, s.update(.{ .bandwidth_bytes_per_sec = -5 }, 2));
}

test "a rejected update leaves the aggregate untouched" {
    // The Go original validated field by field and applied as it went,
    // so this update moved the host before failing on the port.
    var s = try newTestServer(1);
    defer s.deinit();
    s.setId(1);
    t.allocator.free(try s.pullEvents());

    try t.expectError(error.PortOutOfRange, s.update(.{ .host = "moved", .port = 0 }, 99));
    try t.expectEqualStrings("h", s.host);
    try t.expectEqual(@as(Timestamp, 1), s.updated_at);
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
}

test "hydrate carries the row through without events" {
    var s = try UsenetServer.hydrate(t.allocator, .{
        .id = 9,
        .name = "stored",
        .host = "news.host",
        .port = 119,
        .tls = false,
        .username = "u",
        .password = "p",
        .max_conns = 3,
        .priority = -1,
        .enabled = false,
        .backup = true,
        .billing_mode = .metered,
        .quota_bytes = 100,
        .used_bytes = 40,
        .bandwidth_bytes_per_sec = 7,
        .added_at = 11,
        .updated_at = 12,
    });
    defer s.deinit();

    try t.expectEqual(@as(ServerId, 9), s.id);
    try t.expectEqualStrings("stored", s.name);
    try t.expect(!s.enabled);
    try t.expectEqual(@as(i64, 40), s.used_bytes);
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
}

test "quota is only exhausted for a metered server with a stated budget" {
    var flat = try UsenetServer.hydrate(t.allocator, .{
        .id = 1,
        .name = "flat",
        .host = "h",
        .port = 563,
        .tls = true,
        .max_conns = 8,
        .enabled = true,
        .billing_mode = .flat,
        .quota_bytes = 10,
        .used_bytes = 1000,
        .added_at = 0,
        .updated_at = 0,
    });
    defer flat.deinit();
    try t.expect(!flat.quotaExhausted());
    try t.expect(flat.usable());

    var unknown = try UsenetServer.hydrate(t.allocator, .{
        .id = 2,
        .name = "block-unknown",
        .host = "h",
        .port = 563,
        .tls = true,
        .max_conns = 8,
        .enabled = true,
        .billing_mode = .metered,
        .quota_bytes = 0,
        .used_bytes = 1_000_000,
        .added_at = 0,
        .updated_at = 0,
    });
    defer unknown.deinit();
    try t.expect(!unknown.quotaExhausted());

    var metered = try UsenetServer.hydrate(t.allocator, .{
        .id = 3,
        .name = "block",
        .host = "h",
        .port = 563,
        .tls = true,
        .max_conns = 8,
        .enabled = true,
        .billing_mode = .metered,
        .quota_bytes = 100,
        .used_bytes = 99,
        .added_at = 0,
        .updated_at = 0,
    });
    defer metered.deinit();
    try t.expect(!metered.quotaExhausted());
    try t.expect(metered.usable());

    metered.addBytesUsed(1, 50);
    try t.expectEqual(@as(i64, 100), metered.used_bytes);
    try t.expectEqual(@as(Timestamp, 50), metered.updated_at);
    try t.expect(metered.quotaExhausted());
    try t.expect(!metered.usable());
}

test "addBytesUsed ignores non-positive deltas" {
    var s = try newTestServer(1);
    defer s.deinit();

    s.addBytesUsed(0, 99);
    s.addBytesUsed(-5, 99);
    try t.expectEqual(@as(i64, 0), s.used_bytes);
    // No mutation means no touch of updated_at either.
    try t.expectEqual(@as(Timestamp, 1), s.updated_at);

    s.addBytesUsed(7, 99);
    try t.expectEqual(@as(i64, 7), s.used_bytes);
    try t.expectEqual(@as(Timestamp, 99), s.updated_at);
}

test "a disabled server is never usable regardless of quota" {
    var s = try newTestServer(1);
    defer s.deinit();
    try s.setEnabled(false, 2);
    try t.expect(!s.usable());
}

test "markRemoved records the tombstone with the aggregate's name" {
    var s = try newTestServer(1);
    defer s.deinit();
    s.setId(4);
    t.allocator.free(try s.pullEvents());

    try s.markRemoved(77);
    const batch = try s.pullEvents();
    defer t.allocator.free(batch);
    try t.expectEqual(Kind.removed, std.meta.activeTag(batch[0]));
    try t.expectEqualStrings("x", batch[0].removed.name);
    try t.expectEqualStrings("server.usenet.removed", batch[0].topic());
    try t.expectEqual(@as(Timestamp, 77), batch[0].occurredAt());
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

test "billing mode round-trips its persisted form" {
    try t.expectEqualStrings("flat", BillingMode.flat.toString());
    try t.expectEqualStrings("metered", BillingMode.metered.toString());
    try t.expectEqual(BillingMode.metered, BillingMode.parse("metered").?);
    try t.expectEqual(@as(?BillingMode, null), BillingMode.parse("prepaid"));
}

test "dispatch order puts backups last, then priority, then flat first" {
    // Built as plain values: `dispatchLessThan` only reads the four
    // ordering fields, so there is nothing to allocate here.
    const mk = struct {
        fn f(id: ServerId, priority: i32, backup: bool, billing: BillingMode) UsenetServer {
            return .{
                .allocator = undefined,
                .id = id,
                .name = "",
                .host = "",
                .port = 563,
                .tls = true,
                .username = "",
                .password = "",
                .max_conns = 8,
                .priority = priority,
                .enabled = true,
                .backup = backup,
                .billing_mode = billing,
                .quota_bytes = 0,
                .bandwidth_bytes_per_sec = 0,
                .added_at = 0,
                .updated_at = 0,
            };
        }
    }.f;

    const a = mk(1, 0, false, .flat);
    const b = mk(2, 0, false, .metered);
    const c = mk(3, 1, false, .flat);
    const d = mk(4, 0, true, .flat);

    try t.expect(dispatchLessThan({}, &a, &b)); // flat beats metered in a tier
    try t.expect(dispatchLessThan({}, &b, &c)); // lower priority number first
    try t.expect(dispatchLessThan({}, &c, &d)); // backup last despite priority
    try t.expect(!dispatchLessThan({}, &d, &a));

    var list = [_]*const UsenetServer{ &d, &c, &b, &a };
    std.mem.sort(*const UsenetServer, &list, {}, dispatchLessThan);
    try t.expectEqual(@as(ServerId, 1), list[0].id);
    try t.expectEqual(@as(ServerId, 2), list[1].id);
    try t.expectEqual(@as(ServerId, 3), list[2].id);
    try t.expectEqual(@as(ServerId, 4), list[3].id);
}
