//! Per-IP request throttling for the auth endpoints.
//!
//! `/auth/login` and `/auth/setup` are the only public endpoints that
//! cost real CPU: both end in a bcrypt verification, which at cost 10 is
//! deliberately ~100 ms of work. Without a limiter one attacker turns
//! that into a denial of service against every other user of the box,
//! and gets an unmetered password oracle on top.
//!
//! ## Why a sliding window and not a token bucket
//!
//! The Go build did not use `golang.org/x/time/rate` here — it kept a
//! per-IP list of attempt timestamps and rejected once the count inside
//! the window reached the cap. That is a stricter shape than a bucket
//! and the difference matters for this endpoint:
//!
//!   * A bucket refilling at 10/minute hands out a token every 6 s, so
//!     after the initial burst an attacker keeps a steady 10 attempts a
//!     minute *forever*.
//!   * The window blocks every attempt until the oldest of the last
//!     `max` falls out, so a burst of 10 buys 60 s of silence.
//!
//! So the window is what is ported, timestamps and all. What changed is
//! the clock: `now` is a parameter, always monotonic nanoseconds from
//! `sys.monotonicNanos()`. The wall clock is not usable here — an NTP
//! step backwards would freeze the limiter, and a step forwards would
//! flush it, and both are reachable on a NAS that boots without a
//! network and then syncs.
//!
//! ## Memory
//!
//! O(tracked_ips * max) timestamps. Pruning of a bucket is lazy (on the
//! next call from that IP); the map is swept at most every 5 minutes,
//! same as Go. Unlike Go there is a hard cap on distinct tracked IPs:
//! the key is attacker-controlled through `X-Forwarded-For`, and a map
//! that grows once per forged header value is a memory leak with a
//! remote trigger.

const std = @import("std");
const sys = @import("../../posix/sys.zig");

const Allocator = std.mem.Allocator;

/// Attempts per window for the login/setup pair. Straight from the Go
/// `mountAuth`: `NewIPRateLimiter(10, time.Minute)`.
pub const auth_max_attempts: usize = 10;
pub const auth_window_ns: u64 = 60 * std.time.ns_per_s;

/// `Retry-After` seconds sent with the 429. A hint for well-behaved
/// clients, not a security property.
pub const auth_retry_after_s: u32 = 60;

/// How often the whole map is swept for dead entries.
const gc_interval_ns: u64 = 5 * 60 * std.time.ns_per_s;

/// Distinct source addresses tracked at once. Reaching it evicts the
/// entry that has been quiet longest, which under a forged-header flood
/// means the flood evicts itself and a real client keeps its bucket only
/// if it is active. Chosen so the worst case stays trivial:
/// 4096 * 10 * 8 B of timestamps is 320 KiB.
pub const default_max_tracked: usize = 4096;

pub const Limiter = struct {
    gpa: Allocator,
    /// Attempts allowed inside `window_ns`.
    max: usize,
    window_ns: u64,
    max_tracked: usize = default_max_tracked,

    entries: std.StringHashMapUnmanaged(Bucket) = .empty,
    last_gc_ns: u64 = 0,
    /// Buckets refused because the tracking table was full and nothing
    /// could be evicted. Surfaced for the metrics endpoint.
    evictions: u64 = 0,

    const Bucket = struct {
        /// Attempt instants, oldest first. Allocated once at `max`
        /// entries and never resized: the prune is a memmove of at most
        /// `max` u64s, which for max=10 is cheaper than any cleverness.
        ts: []u64,
        len: usize = 0,
        /// Last time this bucket was touched, for eviction order.
        seen_ns: u64 = 0,
    };

    pub fn init(gpa: Allocator, max: usize, window_ns: u64, now_ns: u64) Limiter {
        return .{
            .gpa = gpa,
            .max = @max(max, 1),
            .window_ns = window_ns,
            .last_gc_ns = now_ns,
        };
    }

    /// The login/setup limiter: 10 attempts per minute per IP.
    pub fn initAuth(gpa: Allocator, now_ns: u64) Limiter {
        return init(gpa, auth_max_attempts, auth_window_ns, now_ns);
    }

    pub fn deinit(self: *Limiter) void {
        var it = self.entries.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(e.value_ptr.ts);
        }
        self.entries.deinit(self.gpa);
        self.* = undefined;
    }

    /// Records an attempt from `key` and reports whether it is within
    /// the limit. `false` means the caller answers 429.
    ///
    /// On allocation failure the attempt is *allowed*: an OOM in the
    /// limiter must not become a lockout of the admin UI, and by the
    /// time the process cannot allocate 80 bytes there is a much larger
    /// problem than a password guesser.
    pub fn allow(self: *Limiter, key: []const u8, now_ns: u64) bool {
        self.maybeGc(now_ns);

        const cut = self.cutoff(now_ns);

        if (self.entries.getPtr(key)) |b| {
            b.seen_ns = now_ns;
            prune(b, cut);
            if (b.len >= self.max) return false;
            b.ts[b.len] = now_ns;
            b.len += 1;
            return true;
        }

        if (self.entries.count() >= self.max_tracked and !self.evictOldest()) {
            self.evictions += 1;
            return true;
        }

        const owned_key = self.gpa.dupe(u8, key) catch return true;
        const ts = self.gpa.alloc(u64, self.max) catch {
            self.gpa.free(owned_key);
            return true;
        };
        ts[0] = now_ns;
        self.entries.put(self.gpa, owned_key, .{
            .ts = ts,
            .len = 1,
            .seen_ns = now_ns,
        }) catch {
            self.gpa.free(owned_key);
            self.gpa.free(ts);
            return true;
        };
        return true;
    }

    /// Live attempts recorded for `key`. Test and metrics use only — it
    /// prunes, so it is not `const`.
    pub fn countFor(self: *Limiter, key: []const u8, now_ns: u64) usize {
        const b = self.entries.getPtr(key) orelse return 0;
        prune(b, self.cutoff(now_ns));
        return b.len;
    }

    pub fn tracked(self: *const Limiter) usize {
        return self.entries.count();
    }

    /// Seconds until the oldest in-window attempt falls out, i.e. the
    /// earliest moment a blocked caller could succeed. Rounded up, and
    /// never zero, so a client that honours it does not immediately
    /// retry into another rejection.
    pub fn retryAfterSeconds(self: *Limiter, key: []const u8, now_ns: u64) u32 {
        const b = self.entries.getPtr(key) orelse return 1;
        if (b.len == 0) return 1;
        const oldest = b.ts[0];
        const expires_at = oldest + self.window_ns;
        if (expires_at <= now_ns) return 1;
        const remaining = expires_at - now_ns;
        const secs = (remaining + std.time.ns_per_s - 1) / std.time.ns_per_s;
        return @intCast(@max(secs, 1));
    }

    /// A monotonic clock starts at an arbitrary point that can be less
    /// than one window past zero (it is uptime on Linux), so the cutoff
    /// is computed in signed space. Saturating at zero instead would
    /// expire an attempt made at exactly `now == 0`, and clamping is not
    /// what Go's `now.Add(-window)` did.
    fn cutoff(self: *const Limiter, now_ns: u64) i128 {
        return @as(i128, now_ns) - @as(i128, self.window_ns);
    }

    fn maybeGc(self: *Limiter, now_ns: u64) void {
        if (now_ns -% self.last_gc_ns <= gc_interval_ns) return;
        self.last_gc_ns = now_ns;
        const cut = self.cutoff(now_ns);

        // Collect first, delete after: mutating a hash map through a
        // live iterator is undefined here as it is in Go.
        var dead: [64][]const u8 = undefined;
        var n: usize = 0;
        var it = self.entries.iterator();
        while (it.next()) |e| {
            prune(e.value_ptr, cut);
            if (e.value_ptr.len == 0) {
                if (n == dead.len) break;
                dead[n] = e.key_ptr.*;
                n += 1;
            }
        }
        for (dead[0..n]) |k| self.removeKey(k);
    }

    fn evictOldest(self: *Limiter) bool {
        var oldest_key: ?[]const u8 = null;
        var oldest_seen: u64 = std.math.maxInt(u64);
        var it = self.entries.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.seen_ns < oldest_seen) {
                oldest_seen = e.value_ptr.seen_ns;
                oldest_key = e.key_ptr.*;
            }
        }
        const k = oldest_key orelse return false;
        self.removeKey(k);
        return true;
    }

    fn removeKey(self: *Limiter, key: []const u8) void {
        const kv = self.entries.fetchRemove(key) orelse return;
        self.gpa.free(kv.key);
        self.gpa.free(kv.value.ts);
    }

    /// Drop attempts at or before `cut`. Go's `pruneBefore` kept
    /// `ts.After(cutoff)`, so an attempt exactly on the boundary is
    /// expired; same comparison here.
    fn prune(b: *Bucket, cut: i128) void {
        var i: usize = 0;
        while (i < b.len and @as(i128, b.ts[i]) <= cut) i += 1;
        if (i == 0) return;
        const keep = b.len - i;
        if (keep > 0) std.mem.copyForwards(u64, b.ts[0..keep], b.ts[i..b.len]);
        b.len = keep;
    }
};

// ---------------------------------------------------------------------
// Client address
// ---------------------------------------------------------------------

/// Longest textual IPv6 address plus a zone id, which is the widest key
/// `clientKey` can return.
pub const max_key_len = 64;

/// The bucket key for a request: the first `X-Forwarded-For` hop, else
/// `X-Real-IP`, else `direct_key`.
///
/// Both headers are attacker-supplied, so the value is only accepted
/// when it parses as an IP address. That is not authentication — anyone
/// who can reach the port can forge a different IP per attempt and get a
/// fresh bucket — it is what keeps a forged value from being an
/// arbitrary-length arbitrary-content map key. The tracking cap in
/// `Limiter` is the second half of that defence.
///
/// hoardarr's HTTP layer does not expose the peer address yet (there is
/// no `getpeername` in `posix/sys.zig`), so a request arriving without
/// either header shares one bucket with every other such request. For a
/// self-hosted box behind a reverse proxy — the deployment the headers
/// exist for — that path is not taken. Where it is taken it fails
/// *closed*: the login endpoint gets slower for everyone rather than
/// staying open for the attacker.
pub const direct_key = "direct";

pub fn clientKey(out: *[max_key_len]u8, xff: ?[]const u8, real_ip: ?[]const u8) []const u8 {
    if (xff) |raw| {
        // First hop is the closest client-originated address; everything
        // after it was added by a proxy on the way in.
        const comma = std.mem.indexOfScalar(u8, raw, ',') orelse raw.len;
        if (normalizeIp(out, trimAsciiSpace(raw[0..comma]))) |ip| return ip;
    }
    if (real_ip) |raw| {
        if (normalizeIp(out, trimAsciiSpace(raw))) |ip| return ip;
    }
    return direct_key;
}

/// Validate and copy an IP literal. Returns null for anything that is
/// not one, including the empty string.
fn normalizeIp(out: *[max_key_len]u8, text: []const u8) ?[]const u8 {
    if (text.len == 0 or text.len > max_key_len) return null;
    // Bracketed IPv6, as some proxies emit in XFF: [::1]:1234 or [::1].
    var s = text;
    if (s.len >= 2 and s[0] == '[') {
        const close = std.mem.indexOfScalar(u8, s, ']') orelse return null;
        s = s[1..close];
        if (s.len == 0) return null;
    }
    _ = std.Io.net.IpAddress.parse(s, 0) catch return null;
    @memcpy(out[0..s.len], s);
    return out[0..s.len];
}

fn trimAsciiSpace(s: []const u8) []const u8 {
    var out = s;
    while (out.len > 0 and (out[0] == ' ' or out[0] == '\t')) out = out[1..];
    while (out.len > 0 and (out[out.len - 1] == ' ' or out[out.len - 1] == '\t')) out = out[0 .. out.len - 1];
    return out;
}

/// The real clock, for production call sites.
pub fn nowNanos() u64 {
    return sys.monotonicNanos();
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;
const minute = 60 * std.time.ns_per_s;

// -- the Go ratelimit_test.go, ported ---------------------------------

test "allows until max" {
    var l = Limiter.init(testing.allocator, 3, minute, 0);
    defer l.deinit();

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        try testing.expect(l.allow("1.2.3.4", 1000));
    }
    try testing.expect(!l.allow("1.2.3.4", 1000));
}

test "per-IP isolation" {
    var l = Limiter.init(testing.allocator, 2, minute, 0);
    defer l.deinit();

    try testing.expect(l.allow("10.0.0.1", 1));
    try testing.expect(l.allow("10.0.0.1", 2));
    try testing.expect(!l.allow("10.0.0.1", 3));
    // b has its own quota and is unaffected by a's exhaustion.
    try testing.expect(l.allow("10.0.0.2", 4));
}

test "window expiry lets the next attempt through" {
    var l = Limiter.init(testing.allocator, 2, 50 * std.time.ns_per_ms, 0);
    defer l.deinit();

    try testing.expect(l.allow("x", 0));
    try testing.expect(l.allow("x", 1));
    try testing.expect(!l.allow("x", 2));
    // No sleeping: the clock is a parameter. 70 ms past the first two.
    try testing.expect(l.allow("x", 70 * std.time.ns_per_ms));
}

// -- the tests the Go suite did not have ------------------------------

test "the window slides rather than resetting in blocks" {
    var l = Limiter.init(testing.allocator, 2, 100, 0);
    defer l.deinit();

    try testing.expect(l.allow("a", 10)); // expires after t=110
    try testing.expect(l.allow("a", 60)); // expires after t=160
    try testing.expect(!l.allow("a", 100));
    // The first attempt has rolled off, the second has not: exactly one
    // slot is free, which is the property a fixed-window counter loses.
    try testing.expect(l.allow("a", 111));
    try testing.expect(!l.allow("a", 112));
    try testing.expect(l.allow("a", 161));
}

test "an attempt exactly on the cutoff is expired, matching Go's After()" {
    var l = Limiter.init(testing.allocator, 1, 100, 0);
    defer l.deinit();

    try testing.expect(l.allow("a", 100));
    // cutoff at now=200 is 100; ts[0] == 100 is not After(cutoff).
    try testing.expect(l.allow("a", 200));
    try testing.expectEqual(@as(usize, 1), l.countFor("a", 200));
}

test "a blocked bucket does not accumulate beyond max" {
    var l = Limiter.init(testing.allocator, 2, minute, 0);
    defer l.deinit();

    var i: usize = 0;
    while (i < 100) : (i += 1) _ = l.allow("a", 1000);
    try testing.expectEqual(@as(usize, 2), l.countFor("a", 1000));
    // And the rejections did not extend the block: the two recorded
    // attempts still expire one window after they were *made*.
    try testing.expect(l.allow("a", 1000 + minute + 1));
}

test "retry-after reports when the oldest attempt falls out" {
    var l = Limiter.init(testing.allocator, 1, 60 * std.time.ns_per_s, 0);
    defer l.deinit();

    try testing.expect(l.allow("a", 0));
    try testing.expect(!l.allow("a", 0));
    try testing.expectEqual(@as(u32, 60), l.retryAfterSeconds("a", 0));
    try testing.expectEqual(@as(u32, 30), l.retryAfterSeconds("a", 30 * std.time.ns_per_s));
    // Never zero, so a client honouring it cannot hot-loop.
    try testing.expectEqual(@as(u32, 1), l.retryAfterSeconds("a", 60 * std.time.ns_per_s));
    try testing.expectEqual(@as(u32, 1), l.retryAfterSeconds("unknown", 0));
}

test "the map is swept once the gc interval passes" {
    var l = Limiter.init(testing.allocator, 2, minute, 0);
    defer l.deinit();

    try testing.expect(l.allow("a", 1));
    try testing.expect(l.allow("b", 2));
    try testing.expectEqual(@as(usize, 2), l.tracked());

    // Under the interval: nothing is swept even though both are stale.
    _ = l.allow("c", 4 * 60 * std.time.ns_per_s);
    try testing.expectEqual(@as(usize, 3), l.tracked());

    // Past it: the two long-dead buckets go, the fresh one stays.
    const t = 6 * 60 * std.time.ns_per_s;
    _ = l.allow("c", t);
    try testing.expectEqual(@as(usize, 1), l.tracked());
    try testing.expect(l.entries.contains("c"));
}

test "the tracking table is capped and evicts the quietest entry" {
    var l = Limiter.init(testing.allocator, 1, minute, 0);
    l.max_tracked = 3;
    defer l.deinit();

    var buf: [16]u8 = undefined;
    for (0..3) |i| {
        const k = try std.fmt.bufPrint(&buf, "1.0.0.{d}", .{i});
        try testing.expect(l.allow(k, @as(u64, i) + 1));
    }
    try testing.expectEqual(@as(usize, 3), l.tracked());

    // Refresh the first so the second is now the quietest.
    _ = l.allow("1.0.0.0", 10);
    try testing.expect(l.allow("9.9.9.9", 11));
    try testing.expectEqual(@as(usize, 3), l.tracked());
    try testing.expect(!l.entries.contains("1.0.0.1"));
    try testing.expect(l.entries.contains("1.0.0.0"));
    try testing.expect(l.entries.contains("9.9.9.9"));
}

test "auth limits are the Go ones" {
    try testing.expectEqual(@as(usize, 10), auth_max_attempts);
    try testing.expectEqual(@as(u64, 60 * std.time.ns_per_s), auth_window_ns);

    var l = Limiter.initAuth(testing.allocator, 0);
    defer l.deinit();
    for (0..10) |_| try testing.expect(l.allow("1.2.3.4", 5));
    try testing.expect(!l.allow("1.2.3.4", 5));
    try testing.expect(l.allow("1.2.3.4", 5 + auth_window_ns + 1));
}

test "a monotonic clock near zero does not wrap the cutoff" {
    var l = Limiter.init(testing.allocator, 1, minute, 0);
    defer l.deinit();
    // now < window. An unsigned cutoff would wrap to ~2^64 and expire
    // every entry immediately, i.e. no limiting at all for the first
    // minute of uptime; clamping it to 0 would instead expire an entry
    // recorded at exactly t=0.
    try testing.expect(l.allow("a", 0));
    try testing.expect(!l.allow("a", 0));
    try testing.expect(l.allow("b", 5));
    try testing.expect(!l.allow("b", 6));
}

test "client key prefers the first forwarded hop" {
    var buf: [max_key_len]u8 = undefined;
    try testing.expectEqualStrings("203.0.113.7", clientKey(&buf, "203.0.113.7, 10.0.0.1, 10.0.0.2", null));
    try testing.expectEqualStrings("203.0.113.7", clientKey(&buf, "  203.0.113.7\t", null));
    try testing.expectEqualStrings("2001:db8::1", clientKey(&buf, "2001:db8::1", null));
    try testing.expectEqualStrings("::1", clientKey(&buf, "[::1]", null));
}

test "client key falls back to X-Real-IP and then to a shared bucket" {
    var buf: [max_key_len]u8 = undefined;
    try testing.expectEqualStrings("198.51.100.4", clientKey(&buf, null, "198.51.100.4"));
    // A garbage XFF does not shadow a good X-Real-IP.
    try testing.expectEqualStrings("198.51.100.4", clientKey(&buf, "not-an-ip", "198.51.100.4"));
    try testing.expectEqualStrings(direct_key, clientKey(&buf, null, null));
    try testing.expectEqualStrings(direct_key, clientKey(&buf, "", ""));
}

test "hostile forwarded-for values cannot become map keys" {
    var buf: [max_key_len]u8 = undefined;
    const hostile = [_][]const u8{
        "'; DROP TABLE users;--",
        "\x00\x01\x02",
        "999.999.999.999",
        "1.2.3.4.5",
        "0x7f000001",
        "localhost",
        "1.2.3.4:80", // host:port is not an address literal
        "a" ** 300,
        ",",
        "[",
        "[]",
    };
    for (hostile) |h| {
        try testing.expectEqualStrings(direct_key, clientKey(&buf, h, null));
    }
}

test "a forged-header flood cannot grow the table without bound" {
    var l = Limiter.init(testing.allocator, 10, minute, 0);
    l.max_tracked = 32;
    defer l.deinit();

    var buf: [max_key_len]u8 = undefined;
    var key_buf: [32]u8 = undefined;
    for (0..5000) |i| {
        const forged = try std.fmt.bufPrint(&key_buf, "10.{d}.{d}.{d}", .{
            (i >> 16) & 0xff, (i >> 8) & 0xff, i & 0xff,
        });
        const key = clientKey(&buf, forged, null);
        _ = l.allow(key, @intCast(i + 1));
    }
    try testing.expect(l.tracked() <= 32);
}
