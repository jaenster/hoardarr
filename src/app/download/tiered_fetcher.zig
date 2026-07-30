//! `ArticleFetcher` over many NNTP pools, with priority-tier failover.
//!
//! The order is the one SABnzbd converged on:
//!
//!   1. Enabled, in-quota pools grouped by priority ascending. Inside a
//!      tier, flat-billed pools come before metered ones — a block
//!      account is money, a subscription is not.
//!   2. Every `backup = true` server last, whatever its stated priority.
//!   3. Walk in order. A 430 means "not here, try the next". Over-capacity
//!      means "not now, try the next". Anything else stops the walk and
//!      surfaces, because a fault concentrated on the top tier is
//!      information the operator needs, not something to paper over by
//!      silently failing over on every article.
//!   4. If every candidate said 430, the article is genuinely missing.
//!
//! The `hint` on the `ArticleFetcher` port is ignored: this fetcher
//! chooses, and reports its choice in `Body.server_id`.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const ports = @import("ports.zig");
const bandwidth = @import("bandwidth.zig");
const byte_accounter = @import("byte_accounter.zig");
const dserver = @import("../../domain/server.zig");

const Allocator = std.mem.Allocator;

pub const ServerId = ports.ServerId;
pub const PoolInfo = ports.PoolInfo;
pub const FetchError = ports.FetchError;
pub const Body = ports.Body;
pub const Timestamp = app_ports.Timestamp;

/// Dispatch order: backups last, then priority ascending, then flat
/// before metered, then id for a stable tie-break.
///
/// A stable final key matters more than it looks: without it the walk
/// order depends on hash-map iteration order, and a "sometimes it uses
/// the backup first" bug is one of the harder ones to reproduce.
pub fn lessThan(_: void, a: PoolInfo, b: PoolInfo) bool {
    if (a.backup != b.backup) return !a.backup;
    if (a.priority != b.priority) return a.priority < b.priority;
    if (a.metered != b.metered) return !a.metered;
    return a.id < b.id;
}

/// The usable pools in dispatch order. Slice belongs to `a`.
pub fn tieredOrder(a: Allocator, snapshot: []const PoolInfo) Allocator.Error![]PoolInfo {
    var out: std.ArrayList(PoolInfo) = .empty;
    errdefer out.deinit(a);
    for (snapshot) |p| {
        if (!p.usable()) continue;
        try out.append(a, p);
    }
    std.mem.sort(PoolInfo, out.items, {}, lessThan);
    return out.toOwnedSlice(a);
}

pub const TieredFetcher = struct {
    gpa: Allocator,
    pools: ports.PoolSet,
    logger: *log.Logger = &log.default,
    /// Optional per-server byte tally.
    accounter: ?*byte_accounter.Accounter = null,
    /// Optional throttle. The owed delay rides out on
    /// `Body.throttle_ms`; nothing here sleeps.
    limiter: ?*bandwidth.Limiter = null,
    clock: ?app_ports.Clock = null,

    pub fn fetcher(self: *TieredFetcher) ports.ArticleFetcher {
        return .{ .ctx = @ptrCast(self), .fetchFn = &doFetch };
    }

    fn doFetch(
        ctx: *anyopaque,
        a: Allocator,
        _: ServerId,
        message_id: []const u8,
    ) FetchError!Body {
        const self: *TieredFetcher = @ptrCast(@alignCast(ctx));
        return self.fetch(a, message_id);
    }

    pub fn fetch(self: *TieredFetcher, a: Allocator, message_id: []const u8) FetchError!Body {
        const snapshot = try self.pools.snapshot(self.gpa);
        defer self.gpa.free(snapshot);
        const candidates = try tieredOrder(self.gpa, snapshot);
        defer self.gpa.free(candidates);
        if (candidates.len == 0) return error.NoPoolsAvailable;

        var saw_missing = false;
        for (candidates) |p| {
            const bytes = self.pools.fetchOne(a, p.id, message_id) catch |e| switch (e) {
                error.ArticleMissing => {
                    self.logger.debug("nntp: article missing on server", &.{
                        log.str("msg_id", message_id),
                        log.str("server", p.name),
                        log.int("server_id", p.id),
                    });
                    saw_missing = true;
                    continue;
                },
                error.TooManyConnections => {
                    // The provider is over capacity. Treat this pool as
                    // unavailable for this article and fall through; the
                    // pool has already latched its own back-off, so the
                    // next article will not pile on fresh dials.
                    self.logger.info("nntp: server over-capacity, trying next pool", &.{
                        log.str("server", p.name),
                        log.int("server_id", p.id),
                    });
                    continue;
                },
                else => {
                    self.logger.warn("nntp: article fetch failed", &.{
                        log.str("msg_id", message_id),
                        log.str("server", p.name),
                        log.int("server_id", p.id),
                        log.errv("err", e),
                    });
                    return e;
                },
            };

            var body: Body = .{ .bytes = bytes, .server_id = p.id };
            if (self.accounter) |acc| try acc.add(p.id, @intCast(bytes.len));
            if (self.limiter) |lim| {
                const now = if (self.clock) |c| c.now() else 0;
                body.throttle_ms = lim.reserve(now, p.id, @intCast(bytes.len));
            }
            return body;
        }

        if (saw_missing) return error.ArticleMissing;
        // Every candidate became ineligible during the walk — a quota
        // tripped mid-loop, say. Same answer as starting with none.
        return error.NoPoolsAvailable;
    }
};

// =====================================================================
// Test double
// =====================================================================

/// A scripted `PoolSet`. Each pool answers with a body, an error, or
/// nothing at all.
pub const FakePoolSet = struct {
    pub const Entry = struct {
        info: PoolInfo,
        /// Returned when this pool is asked. Null means it errors with
        /// `reply_err`.
        body: ?[]const u8 = null,
        reply_err: FetchError = error.ArticleMissing,
    };

    entries: []Entry,
    /// Ids asked, in order.
    asked: [16]ServerId = @splat(0),
    n_asked: usize = 0,

    pub fn poolSet(self: *FakePoolSet) ports.PoolSet {
        return .{ .ctx = @ptrCast(self), .snapshotFn = &snapshot, .fetchOneFn = &fetchOne };
    }

    pub fn askOrder(self: *const FakePoolSet) []const ServerId {
        return self.asked[0..self.n_asked];
    }

    fn snapshot(ctx: *anyopaque, a: Allocator) Allocator.Error![]PoolInfo {
        const self: *FakePoolSet = @ptrCast(@alignCast(ctx));
        const out = try a.alloc(PoolInfo, self.entries.len);
        for (self.entries, out) |e, *o| o.* = e.info;
        return out;
    }

    fn fetchOne(ctx: *anyopaque, a: Allocator, id: ServerId, _: []const u8) FetchError![]u8 {
        const self: *FakePoolSet = @ptrCast(@alignCast(ctx));
        if (self.n_asked < self.asked.len) {
            self.asked[self.n_asked] = id;
            self.n_asked += 1;
        }
        for (self.entries) |e| {
            if (e.info.id != id) continue;
            if (e.body) |b| return a.dupe(u8, b);
            return e.reply_err;
        }
        return error.ArticleMissing;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

fn order(a: Allocator, snap: []const PoolInfo) ![]PoolInfo {
    return tieredOrder(a, snap);
}

test "dispatch order is priority, then flat before metered, then id" {
    const snap = [_]PoolInfo{
        .{ .id = 5, .priority = 1, .metered = true },
        .{ .id = 4, .priority = 1, .metered = false },
        .{ .id = 3, .priority = 0 },
        .{ .id = 2, .priority = 0 },
    };
    const got = try order(testing.allocator, &snap);
    defer testing.allocator.free(got);
    const ids = [_]ServerId{ got[0].id, got[1].id, got[2].id, got[3].id };
    try testing.expectEqualSlices(ServerId, &.{ 2, 3, 4, 5 }, &ids);
}

test "backups sort last however good their stated priority" {
    const snap = [_]PoolInfo{
        .{ .id = 1, .priority = -100, .backup = true },
        .{ .id = 2, .priority = 50 },
    };
    const got = try order(testing.allocator, &snap);
    defer testing.allocator.free(got);
    try testing.expectEqual(@as(ServerId, 2), got[0].id);
    try testing.expectEqual(@as(ServerId, 1), got[1].id);
}

test "disabled and quota-exhausted pools are not candidates" {
    const snap = [_]PoolInfo{
        .{ .id = 1, .enabled = false },
        .{ .id = 2, .quota_exhausted = true },
        .{ .id = 3 },
    };
    const got = try order(testing.allocator, &snap);
    defer testing.allocator.free(got);
    try testing.expectEqual(@as(usize, 1), got.len);
    try testing.expectEqual(@as(ServerId, 3), got[0].id);
}

test "an empty pool set is NoPoolsAvailable, not a fetch failure" {
    var pools: FakePoolSet = .{ .entries = &.{} };
    var tf: TieredFetcher = .{ .gpa = testing.allocator, .pools = pools.poolSet() };
    try testing.expectError(error.NoPoolsAvailable, tf.fetch(testing.allocator, "m@h"));

    // Same answer when every pool exists but none is usable — the
    // operator disabled them all.
    var all_off = [_]FakePoolSet.Entry{
        .{ .info = .{ .id = 1, .enabled = false } },
        .{ .info = .{ .id = 2, .quota_exhausted = true } },
    };
    var off: FakePoolSet = .{ .entries = &all_off };
    var tf2: TieredFetcher = .{ .gpa = testing.allocator, .pools = off.poolSet() };
    try testing.expectError(error.NoPoolsAvailable, tf2.fetch(testing.allocator, "m@h"));
}

test "a 430 falls through to the next tier and the body comes back" {
    var entries = [_]FakePoolSet.Entry{
        .{ .info = .{ .id = 1, .priority = 0 }, .reply_err = error.ArticleMissing },
        .{ .info = .{ .id = 2, .priority = 1 }, .body = "BODY" },
    };
    var pools: FakePoolSet = .{ .entries = &entries };
    var logger: log.Logger = .{};
    var tf: TieredFetcher = .{
        .gpa = testing.allocator,
        .pools = pools.poolSet(),
        .logger = &logger,
    };

    const body = try tf.fetch(testing.allocator, "m@h");
    defer testing.allocator.free(body.bytes);
    try testing.expectEqualStrings("BODY", body.bytes);
    // The winner is reported, so byte accounting charges the right
    // account.
    try testing.expectEqual(@as(ServerId, 2), body.server_id);
    try testing.expectEqualSlices(ServerId, &.{ 1, 2 }, pools.askOrder());
}

test "every server saying 430 means the article is missing" {
    var entries = [_]FakePoolSet.Entry{
        .{ .info = .{ .id = 1 }, .reply_err = error.ArticleMissing },
        .{ .info = .{ .id = 2 }, .reply_err = error.ArticleMissing },
    };
    var pools: FakePoolSet = .{ .entries = &entries };
    var logger: log.Logger = .{};
    var tf: TieredFetcher = .{ .gpa = testing.allocator, .pools = pools.poolSet(), .logger = &logger };
    try testing.expectError(error.ArticleMissing, tf.fetch(testing.allocator, "m@h"));
    try testing.expectEqual(@as(usize, 2), pools.n_asked);
}

test "over-capacity falls through, other errors stop the walk" {
    // Over-capacity is "not now": try the next candidate.
    var ok_after_busy = [_]FakePoolSet.Entry{
        .{ .info = .{ .id = 1 }, .reply_err = error.TooManyConnections },
        .{ .info = .{ .id = 2, .priority = 1 }, .body = "OK" },
    };
    var pools: FakePoolSet = .{ .entries = &ok_after_busy };
    var logger: log.Logger = .{};
    var tf: TieredFetcher = .{ .gpa = testing.allocator, .pools = pools.poolSet(), .logger = &logger };
    const body = try tf.fetch(testing.allocator, "m@h");
    defer testing.allocator.free(body.bytes);
    try testing.expectEqual(@as(ServerId, 2), body.server_id);

    // Anything else surfaces immediately: a fault concentrated on the
    // top tier is information, and masking it by always failing over
    // hides provider trouble.
    var stops = [_]FakePoolSet.Entry{
        .{ .info = .{ .id = 1 }, .reply_err = error.AuthFailed },
        .{ .info = .{ .id = 2, .priority = 1 }, .body = "OK" },
    };
    var pools2: FakePoolSet = .{ .entries = &stops };
    var tf2: TieredFetcher = .{ .gpa = testing.allocator, .pools = pools2.poolSet(), .logger = &logger };
    try testing.expectError(error.AuthFailed, tf2.fetch(testing.allocator, "m@h"));
    try testing.expectEqualSlices(ServerId, &.{1}, pools2.askOrder());
}

test "over-capacity everywhere reports no pools rather than a missing article" {
    var entries = [_]FakePoolSet.Entry{
        .{ .info = .{ .id = 1 }, .reply_err = error.TooManyConnections },
        .{ .info = .{ .id = 2 }, .reply_err = error.TooManyConnections },
    };
    var pools: FakePoolSet = .{ .entries = &entries };
    var logger: log.Logger = .{};
    var tf: TieredFetcher = .{ .gpa = testing.allocator, .pools = pools.poolSet(), .logger = &logger };
    // Nobody said 430, so the article is not known to be missing — the
    // segment must not be marked missing on this evidence.
    try testing.expectError(error.NoPoolsAvailable, tf.fetch(testing.allocator, "m@h"));
}

test "a successful fetch charges the accounter for exactly the bytes read" {
    var entries = [_]FakePoolSet.Entry{.{ .info = .{ .id = 3 }, .body = "1234567890" }};
    var pools: FakePoolSet = .{ .entries = &entries };
    var acc = byte_accounter.Accounter.init(testing.allocator);
    defer acc.deinit();
    var logger: log.Logger = .{};
    var tf: TieredFetcher = .{
        .gpa = testing.allocator,
        .pools = pools.poolSet(),
        .logger = &logger,
        .accounter = &acc,
    };

    const body = try tf.fetch(testing.allocator, "m@h");
    defer testing.allocator.free(body.bytes);
    try testing.expectEqual(@as(i64, 10), acc.staged(3));
    // A 430 walk-past charges nothing: no bytes crossed the wire from a
    // server that did not have the article.
    try testing.expectEqual(@as(i64, 0), acc.staged(1));
}

test "the limiter's owed delay rides out on the body instead of sleeping" {
    var entries = [_]FakePoolSet.Entry{.{ .info = .{ .id = 1 }, .body = "x" ** 200 }};
    var pools: FakePoolSet = .{ .entries = &entries };
    var lim = bandwidth.Limiter.init(testing.allocator, 100, 0);
    defer lim.deinit();
    var clock: app_ports.FakeClock = .{ .t = 0 };
    var logger: log.Logger = .{};
    var tf: TieredFetcher = .{
        .gpa = testing.allocator,
        .pools = pools.poolSet(),
        .logger = &logger,
        .limiter = &lim,
        .clock = clock.clock(),
    };

    // The first read fits inside the 64 KiB burst floor, so nothing is
    // owed yet.
    const first = try tf.fetch(testing.allocator, "m@h");
    defer testing.allocator.free(first.bytes);
    try testing.expectEqual(@as(i64, 0), first.throttle_ms);

    // Drain the burst, then the throttle shows up as a number the caller
    // can hand to a reactor timer.
    _ = lim.reserve(0, 1, 64 * 1024);
    const second = try tf.fetch(testing.allocator, "m@h");
    defer testing.allocator.free(second.bytes);
    try testing.expect(second.throttle_ms > 0);
}
