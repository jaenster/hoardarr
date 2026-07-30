//! Bandwidth throttling: one global token bucket plus one per server.
//!
//! # Reserve, don't wait
//!
//! Go's `Limiter.Wait` blocked the calling goroutine on
//! `golang.org/x/time/rate`. There is no goroutine to block here, and
//! there is no `x/time/rate`, so the bucket answers a different
//! question: *how many milliseconds do you owe before those bytes may
//! pass?* The caller turns that into a reactor timer.
//!
//! That inversion is what makes the throttle testable. Go's bandwidth
//! tests assert "the second Wait took at least 500ms" and `t.Skip` under
//! `-short`, because there was no way to observe the bucket without
//! spending the delay. Here the delay is a return value.
//!
//! # Sequential gates, concurrent refill
//!
//! Go satisfied the global bucket and then the per-server bucket in
//! sequence, so a pessimistic reading of its code says the delays add.
//! Its own comment says the effective delay is the *maximum*, because
//! both buckets accrue while the fetcher works. `reserve` returns the
//! maximum, which is what the comment promised and what the observable
//! behaviour was.
//!
//! # Debt, not rejection
//!
//! `x/time/rate.WaitN` refuses any `n` larger than the burst, which is
//! why Go needed `waitChunked` to split an over-budget read into
//! burst-sized pieces. This bucket lets the token count go negative
//! instead: a read larger than the burst simply owes proportionally
//! more time. Same total throughput, no chunking loop, and no way to
//! configure a burst that makes a legitimate read fail outright.

const std = @import("std");
const app_ports = @import("../ports.zig");
const dserver = @import("../../domain/server.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const ServerId = dserver.ServerId;

/// Smallest burst we will configure, matching Go. A burst below one
/// typical NNTP read would make every single read owe a delay, turning a
/// throttle into a stall.
pub const min_burst: i64 = 64 * 1024;

/// One token bucket. `rate_bps == 0` means unlimited, in which case
/// nothing is tracked at all.
pub const Bucket = struct {
    /// Bytes per second. 0 disables the bucket.
    rate_bps: i64 = 0,
    burst: i64 = min_burst,
    /// Available tokens; may go negative, which is the debt a read
    /// larger than the burst incurs.
    tokens: f64 = 0,
    /// When `tokens` was last brought up to date.
    last: Timestamp = 0,

    pub fn init(rate_bps: i64, now: Timestamp) Bucket {
        var b: Bucket = .{ .last = now };
        b.setRate(rate_bps, now);
        return b;
    }

    pub fn unlimited(self: Bucket) bool {
        return self.rate_bps <= 0;
    }

    /// Changes the rate. An in-flight debt is preserved but clamped to
    /// the new burst, so lowering the cap cannot be used to erase a debt
    /// and raising it takes effect immediately.
    pub fn setRate(self: *Bucket, rate_bps: i64, now: Timestamp) void {
        self.refill(now);
        const was_unlimited = self.unlimited();
        self.rate_bps = rate_bps;
        if (rate_bps <= 0) {
            self.tokens = 0;
            self.burst = min_burst;
            return;
        }
        self.burst = @max(rate_bps, min_burst);
        self.tokens = if (was_unlimited)
            // A bucket that has just been given a rate starts full, the
            // way `rate.NewLimiter` does. Starting empty would make the
            // very first read after an operator sets a cap pay a full
            // burst's worth of delay, which reads as a stall.
            @floatFromInt(self.burst)
        else
            @min(self.tokens, @as(f64, @floatFromInt(self.burst)));
    }

    fn refill(self: *Bucket, now: Timestamp) void {
        if (self.unlimited()) {
            self.last = now;
            return;
        }
        const elapsed = now - self.last;
        if (elapsed <= 0) return;
        self.last = now;
        const gained = @as(f64, @floatFromInt(elapsed)) *
            @as(f64, @floatFromInt(self.rate_bps)) / @as(f64, std.time.ms_per_s);
        self.tokens = @min(self.tokens + gained, @as(f64, @floatFromInt(self.burst)));
    }

    /// Consumes `n` bytes and returns the delay owed before they may
    /// pass. 0 when the bucket had the tokens.
    pub fn reserve(self: *Bucket, now: Timestamp, n: i64) Millis {
        if (self.unlimited() or n <= 0) return 0;
        self.refill(now);
        self.tokens -= @floatFromInt(n);
        if (self.tokens >= 0) return 0;
        // Round up: owing a fraction of a millisecond still means the
        // bytes are not yet paid for.
        const owed = -self.tokens * @as(f64, std.time.ms_per_s) /
            @as(f64, @floatFromInt(self.rate_bps));
        return @intFromFloat(@ceil(owed));
    }
};

/// The global bucket plus per-server buckets.
pub const Limiter = struct {
    const Entry = struct { id: ServerId, bucket: Bucket };

    gpa: Allocator,
    global: Bucket = .{},
    /// Small and linearly scanned: an operator with more than a handful
    /// of Usenet providers is rare, and a hash map for four entries is
    /// worse in both memory and cache behaviour.
    per_server: std.ArrayList(Entry) = .empty,

    pub fn init(gpa: Allocator, global_bps: i64, now: Timestamp) Limiter {
        return .{ .gpa = gpa, .global = Bucket.init(global_bps, now) };
    }

    pub fn deinit(self: *Limiter) void {
        self.per_server.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn globalCap(self: *const Limiter) i64 {
        return self.global.rate_bps;
    }

    pub fn setGlobalCap(self: *Limiter, bps: i64, now: Timestamp) void {
        self.global.setRate(bps, now);
    }

    pub fn serverCap(self: *const Limiter, id: ServerId) i64 {
        for (self.per_server.items) |e| {
            if (e.id == id) return e.bucket.rate_bps;
        }
        return 0;
    }

    /// 0 removes the per-server cap entirely.
    pub fn setServerCap(self: *Limiter, id: ServerId, bps: i64, now: Timestamp) Allocator.Error!void {
        for (self.per_server.items, 0..) |*e, i| {
            if (e.id != id) continue;
            if (bps <= 0) {
                _ = self.per_server.orderedRemove(i);
                return;
            }
            e.bucket.setRate(bps, now);
            return;
        }
        if (bps <= 0) return;
        try self.per_server.append(self.gpa, .{ .id = id, .bucket = Bucket.init(bps, now) });
    }

    /// Milliseconds the caller owes before `n` bytes from `id` may pass.
    ///
    /// Both gates are charged — the bytes really did cross both — but the
    /// delay is the larger of the two, because the buckets refill in
    /// parallel.
    pub fn reserve(self: *Limiter, now: Timestamp, id: ServerId, n: i64) Millis {
        if (n <= 0) return 0;
        const g = self.global.reserve(now, n);
        var p: Millis = 0;
        for (self.per_server.items) |*e| {
            if (e.id == id) {
                p = e.bucket.reserve(now, n);
                break;
            }
        }
        return @max(g, p);
    }
};

// ---------------------------------------------------------------------
// Tests — internal/app/download/bandwidth_test.go, minus the sleeps.
// ---------------------------------------------------------------------

const testing = std.testing;

test "no cap never owes a delay" {
    var l = Limiter.init(testing.allocator, 0, 0);
    defer l.deinit();
    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 1, 1024 * 1024));
    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 1, 1024 * 1024));
    try testing.expectEqual(@as(i64, 0), l.globalCap());
}

test "the global cap throttles once the burst is spent" {
    // 32 KiB/s, asked for 64 KiB twice. The burst floor is 64 KiB, so the
    // first request passes free and the second owes a full refill of
    // 64 KiB at 32 KiB/s = 2000ms. Go asserted "at least 500ms" because
    // it had to measure the sleep; here the number is exact.
    const cap: i64 = 32 * 1024;
    const need: i64 = 64 * 1024;
    var l = Limiter.init(testing.allocator, cap, 0);
    defer l.deinit();

    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 1, need));
    try testing.expectEqual(@as(Millis, 2000), l.reserve(0, 1, need));
    // Once the owed time has passed the bucket is square again.
    try testing.expectEqual(@as(Millis, 2000), l.reserve(2000, 1, need));
}

test "a read larger than the burst owes proportionally instead of failing" {
    // x/time/rate rejects n > burst outright, which is why Go needed a
    // chunking loop. Debt makes the chunking unnecessary.
    var l = Limiter.init(testing.allocator, 1024, 0);
    defer l.deinit();
    // Burst floors at 64 KiB; ask for 1 MiB in one go.
    const owed = l.reserve(0, 1, 1024 * 1024);
    try testing.expectEqual(@as(Millis, (1024 * 1024 - 64 * 1024) * 1000 / 1024), owed);
}

test "setting the global cap to zero clears the debt" {
    var l = Limiter.init(testing.allocator, 1024, 0);
    defer l.deinit();
    _ = l.reserve(0, 1, 1024 * 1024);
    try testing.expect(l.reserve(0, 1, 1) > 0);

    l.setGlobalCap(0, 0);
    try testing.expectEqual(@as(i64, 0), l.globalCap());
    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 1, 1024 * 1024));
}

test "per-server caps are isolated from each other and from the global" {
    var l = Limiter.init(testing.allocator, 0, 0);
    defer l.deinit();
    try l.setServerCap(1, 1024, 0);

    // Server 1 is capped; spend its burst and it owes.
    _ = l.reserve(0, 1, 64 * 1024);
    try testing.expect(l.reserve(0, 1, 1024) > 0);
    // Server 2 has no cap of its own and no global cap: free.
    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 2, 1024 * 1024));
}

test "a per-server cap can be raised, lowered and removed" {
    var l = Limiter.init(testing.allocator, 0, 0);
    defer l.deinit();
    try l.setServerCap(7, 1024, 0);
    try testing.expectEqual(@as(i64, 1024), l.serverCap(7));
    try l.setServerCap(7, 2048, 0);
    try testing.expectEqual(@as(i64, 2048), l.serverCap(7));
    try l.setServerCap(7, 0, 0);
    try testing.expectEqual(@as(i64, 0), l.serverCap(7));
    try testing.expectEqual(@as(usize, 0), l.per_server.items.len);
    // Removing one that was never set is a no-op, not an insert.
    try l.setServerCap(9, 0, 0);
    try testing.expectEqual(@as(usize, 0), l.per_server.items.len);
}

test "the reported delay is the larger gate, not their sum" {
    // Global 1 KiB/s, per-server 4 KiB/s. Both are charged, but the
    // caller waits for the slower one only — the buckets refill in
    // parallel while it does.
    var l = Limiter.init(testing.allocator, 1024, 0);
    defer l.deinit();
    try l.setServerCap(1, 4096, 0);

    const n: i64 = 128 * 1024;
    const owed = l.reserve(0, 1, n);
    const global_only = (n - 64 * 1024) * 1000 / 1024;
    const server_only = (n - 64 * 1024) * 1000 / 4096;
    try testing.expectEqual(global_only, owed);
    try testing.expect(owed > server_only);
    // Both buckets really were charged.
    try testing.expect(l.per_server.items[0].bucket.tokens < 0);
}

test "the global cap round-trips through its accessor" {
    var l = Limiter.init(testing.allocator, 123_456, 0);
    defer l.deinit();
    try testing.expectEqual(@as(i64, 123_456), l.globalCap());
    l.setGlobalCap(789, 0);
    try testing.expectEqual(@as(i64, 789), l.globalCap());
    l.setGlobalCap(0, 0);
    try testing.expectEqual(@as(i64, 0), l.globalCap());
}

test "a bucket refills at its rate and saturates at the burst" {
    var b = Bucket.init(1024, 0);
    try testing.expectEqual(@as(i64, min_burst), b.burst);
    // Spend everything plus a little.
    _ = b.reserve(0, min_burst + 1024);
    try testing.expect(b.tokens < 0);
    // One second at 1 KiB/s returns 1024 tokens.
    b.refill(1000);
    try testing.expectApproxEqAbs(@as(f64, 0), b.tokens, 0.001);
    // A long idle cannot bank more than the burst.
    b.refill(1000 + 1000 * 1000);
    try testing.expectEqual(@as(f64, @floatFromInt(min_burst)), b.tokens);
}

test "zero and negative byte counts are free" {
    var l = Limiter.init(testing.allocator, 1, 0);
    defer l.deinit();
    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 1, 0));
    try testing.expectEqual(@as(Millis, 0), l.reserve(0, 1, -5));
}

test "a clock that does not move cannot conjure tokens" {
    var b = Bucket.init(1024, 500);
    _ = b.reserve(500, min_burst);
    const first = b.reserve(500, 1024);
    const second = b.reserve(500, 1024);
    try testing.expect(second > first);
}
