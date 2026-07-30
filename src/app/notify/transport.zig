//! The seam between notification adapters and the network.
//!
//! Every adapter here is split in two: a **pure** half that turns a bus
//! envelope into a `Request` (bytes, URL, headers) and an **impure**
//! half that hands that request to a `Transport`. The split is not
//! decoration — it is what lets the whole notify module be tested with
//! zero sockets, and it is why wiring the reactor-driven HTTP client in
//! later touches exactly one struct literal per process rather than
//! every adapter.
//!
//! `Transport` is a two-function vtable rather than a Zig interface
//! pattern with `anytype`, because the notify service holds one for the
//! process lifetime and stores it in a struct field: a concrete type
//! with a `*anyopaque` context keeps `Service` a plain non-generic
//! struct.
//!
//! # Retry and backoff
//!
//! `deliver` is the retry driver, and its parameters are the Go
//! implementation's, unchanged:
//!
//!   * `RetryPolicy.webhook` — 3 attempts, backoff `200ms << (attempt-1)`
//!     so the two gaps are 200 ms and 400 ms. Network errors, 408, 429
//!     and 5xx are retried; every other 4xx stops immediately because it
//!     is a subscriber misconfiguration that a retry cannot fix.
//!   * `RetryPolicy.chat` — a single attempt, which is what the Go
//!     Discord and Slack senders did. Discord's rate limiter is
//!     unforgiving and a burst of retries against a webhook that is
//!     already 429-ing is how an integration gets shut off; the outbox
//!     already guarantees at-least-once across restarts, so a failed
//!     chat delivery is retried at the *bus* cadence, not in a tight
//!     in-process loop.
//!
//! # Secrets
//!
//! A webhook URL *is* the credential — `discord.com/api/webhooks/<id>/
//! <token>` needs no other authentication. Nothing in this module logs
//! a `Request.url`, and `safeUrl` exists so that the parts of a URL
//! worth logging can be logged without the operator having to think
//! about it. `Delivery.describe` is likewise built only from status
//! codes and error names.

const std = @import("std");
const sys = @import("../../posix/sys.zig");

// ---------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

/// One outbound HTTP request, fully materialised. Every slice is
/// borrowed: the caller's arena owns them and must outlive the
/// `Transport.post` call.
pub const Request = struct {
    method: []const u8 = "POST",
    url: []const u8,
    headers: []const Header = &.{},
    body: []const u8,
};

pub const Response = struct {
    status: u16,
};

/// Transport-level failures — everything that happens before an HTTP
/// status exists. A non-2xx *response* is not an error here; it is a
/// `Response` the retry driver classifies.
pub const Error = error{
    /// DNS, connect, or reset.
    Connect,
    /// The per-attempt deadline expired.
    Timeout,
    /// Handshake or certificate failure.
    Tls,
    /// Read/write failure mid-exchange, or a malformed response.
    Io,
    /// Shutdown while the request was in flight.
    Canceled,
};

fn realSleep(_: *anyopaque, nanos: u64) void {
    sys.sleep(nanos);
}

/// The injectable network. Tests substitute a struct that records
/// requests and replays a scripted sequence of responses; production
/// substitutes the reactor's HTTP client.
pub const Transport = struct {
    ctx: *anyopaque,

    /// Performs one request and waits for the status line. Must not
    /// retry internally — `deliver` owns the retry budget.
    postFn: *const fn (ctx: *anyopaque, req: Request) Error!Response,

    /// Backoff sleep, injectable so a test can assert the delay
    /// schedule without spending it. Defaults to a real sleep.
    sleepFn: *const fn (ctx: *anyopaque, nanos: u64) void = &realSleep,

    pub fn post(self: Transport, req: Request) Error!Response {
        return self.postFn(self.ctx, req);
    }

    pub fn sleep(self: Transport, nanos: u64) void {
        self.sleepFn(self.ctx, nanos);
    }
};

// ---------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------

pub const RetryPolicy = struct {
    /// Total attempts, not retries. 1 means "no retry".
    max_attempts: u8,
    /// Delay before attempt N+1 is `base_backoff_ms << (N-1)`.
    base_backoff_ms: u32,

    /// Discord / Slack: one shot. See the module comment for why.
    pub const chat: RetryPolicy = .{ .max_attempts = 1, .base_backoff_ms = 0 };

    /// Generic webhook subscribers: 3 attempts, 200 ms then 400 ms.
    pub const webhook: RetryPolicy = .{ .max_attempts = 3, .base_backoff_ms = 200 };

    /// Backoff before the attempt following `attempt` (1-based).
    /// Saturates rather than shifting off the end, which only matters if
    /// somebody configures an absurd attempt count.
    pub fn backoffMs(self: RetryPolicy, attempt: u8) u32 {
        if (attempt == 0) return 0;
        const shift: u5 = @intCast(@min(attempt - 1, 31));
        return std.math.shlExact(u32, self.base_backoff_ms, shift) catch
            std.math.maxInt(u32);
    }
};

/// A 4xx that is worth retrying anyway: the request was fine, the
/// server was momentarily unable. Everything else in 4xx is the
/// subscriber's configuration.
pub fn retryableStatus(code: u16) bool {
    if (code == 408 or code == 429) return true;
    return code >= 500 and code <= 599;
}

pub const SendError = error{
    /// The subscriber answered with a status that will not improve on
    /// retry — a 4xx other than 408/429.
    Rejected,
    /// Every attempt failed with a retryable condition.
    Unavailable,
} || Error;

/// What one `deliver` call did. Returned by value rather than as an
/// error union so the caller has the status code and attempt count for
/// telemetry even on success.
pub const Delivery = struct {
    attempts: u8,
    /// Status of the final attempt; 0 when it never got a response.
    status: u16 = 0,
    /// Set when the final attempt failed below the HTTP layer.
    transport_error: ?Error = null,
    /// True when the loop stopped because the status was non-retryable.
    rejected: bool = false,

    pub fn ok(self: Delivery) bool {
        return self.transport_error == null and self.status >= 200 and self.status < 300;
    }

    /// Error-union view, for call sites that prefer `try`.
    pub fn toError(self: Delivery) SendError!void {
        if (self.ok()) return;
        if (self.transport_error) |e| return e;
        if (self.rejected) return error.Rejected;
        return error.Unavailable;
    }

    /// Widest output of `describe`.
    pub const DescribeBuf = [96]u8;

    /// A reason string for the subscription's `last_error` column and
    /// for the log. Built only from the status code, the attempt count
    /// and an error name — **never** from the URL, so a token cannot
    /// reach a log line or the Settings UI through this path.
    pub fn describe(self: Delivery, buf: *DescribeBuf) []const u8 {
        if (self.ok()) return "ok";
        if (self.transport_error) |e| {
            return std.fmt.bufPrint(buf, "{d} attempt(s) failed: {t}", .{
                self.attempts, e,
            }) catch unreachable;
        }
        if (self.rejected) {
            return std.fmt.bufPrint(buf, "non-retryable status {d}", .{self.status}) catch unreachable;
        }
        return std.fmt.bufPrint(buf, "{d} attempt(s) failed: status {d}", .{
            self.attempts, self.status,
        }) catch unreachable;
    }
};

/// Posts `req` under `policy`, sleeping the backoff between attempts.
///
/// The loop is Go's: attempt, classify, stop on success or on a
/// non-retryable status, otherwise sleep and go again. The sleep happens
/// only *between* attempts — never after the last one, which would add
/// latency to a delivery that is already known to have failed.
pub fn deliver(t: Transport, policy: RetryPolicy, req: Request) Delivery {
    std.debug.assert(policy.max_attempts >= 1);
    var out: Delivery = .{ .attempts = 0 };
    var attempt: u8 = 1;
    while (attempt <= policy.max_attempts) : (attempt += 1) {
        out.attempts = attempt;
        out.status = 0;
        out.transport_error = null;

        if (t.post(req)) |resp| {
            out.status = resp.status;
            if (out.ok()) return out;
            if (!retryableStatus(resp.status)) {
                out.rejected = true;
                return out;
            }
        } else |e| {
            out.transport_error = e;
            // A cancelled request means the process is shutting down.
            // Burning the remaining budget on it is pointless and delays
            // the shutdown by the whole backoff schedule.
            if (e == error.Canceled) return out;
        }

        if (attempt < policy.max_attempts) {
            const ms = policy.backoffMs(attempt);
            if (ms != 0) t.sleep(@as(u64, ms) * std.time.ns_per_ms);
        }
    }
    return out;
}

// ---------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------

/// What an adapter needs to know about a subscription.
///
/// The domain aggregate (`domain/notify.zig`'s `Subscription`) is the
/// source of truth; the service narrows it to this before calling an
/// adapter. Narrowing is deliberate: payload construction becomes a
/// pure function of four strings, the adapters never see the aggregate
/// (so they cannot accidentally render an accessor that holds a
/// secret), and the adapter tests need no domain module at all.
pub const Target = struct {
    /// Subscription name. Safe to render — it is operator-chosen and
    /// appears in the notification footer.
    name: []const u8,
    /// Full webhook URL, token included. **Never log this.** Use
    /// `safeUrl`.
    url: []const u8,
    /// HMAC key for the generic webhook sender. Unused by Discord and
    /// Slack, which have no signature scheme.
    secret: []const u8 = "",

    /// `scheme://host[:port]` — the whole of a webhook URL that is safe
    /// to put in a log line or an error message. Everything a Discord or
    /// Slack webhook uses for authentication lives in the path, so
    /// dropping the path drops the credential.
    ///
    /// Returns a borrowed prefix, so this allocates nothing and can be
    /// called from a logging path. An input with no `://` yields the
    /// empty string: if we cannot find the authority we say nothing
    /// rather than guessing and leaking.
    pub fn safeUrl(url: []const u8) []const u8 {
        const scheme_end = std.mem.indexOf(u8, url, "://") orelse return "";
        const authority_start = scheme_end + 3;
        const rest = url[authority_start..];
        // A userinfo component (`user:pass@host`) is itself a
        // credential, so cut before it too.
        const end = std.mem.indexOfAny(u8, rest, "/?#@") orelse return url;
        if (rest[end] == '@') return url[0..authority_start];
        return url[0 .. authority_start + end];
    }

    pub fn safe(self: Target) []const u8 {
        return safeUrl(self.url);
    }
};

// ---------------------------------------------------------------------
// Test double
// ---------------------------------------------------------------------

/// A scripted `Transport` for tests: hands out `script` in order,
/// records every request body and every backoff it was asked to sleep.
///
/// Public because the adapter and service tests in sibling files use it,
/// and duplicating it three times is how the three copies drift.
pub const FakeTransport = struct {
    /// Replayed in order. The last entry repeats once exhausted, so a
    /// one-element script covers "always fails".
    script: []const Error!Response,
    calls: usize = 0,
    /// Last request seen, for assertions on body/url/headers.
    last: ?Request = null,
    /// Backoffs requested, in nanoseconds.
    slept: [8]u64 = @splat(0),
    n_slept: usize = 0,

    pub fn transport(self: *FakeTransport) Transport {
        return .{
            .ctx = @ptrCast(self),
            .postFn = &post,
            .sleepFn = &recordSleep,
        };
    }

    fn post(ctx: *anyopaque, req: Request) Error!Response {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        std.debug.assert(self.script.len > 0);
        self.last = req;
        const i = @min(self.calls, self.script.len - 1);
        self.calls += 1;
        return self.script[i];
    }

    fn recordSleep(ctx: *anyopaque, nanos: u64) void {
        const self: *FakeTransport = @ptrCast(@alignCast(ctx));
        if (self.n_slept < self.slept.len) {
            self.slept[self.n_slept] = nanos;
            self.n_slept += 1;
        }
    }

    pub fn sleeps(self: *const FakeTransport) []const u64 {
        return self.slept[0..self.n_slept];
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

const dummy_req: Request = .{ .url = "https://example.test/hook", .body = "{}" };

test "a 2xx on the first attempt costs one call and no sleep" {
    var fake: FakeTransport = .{ .script = &.{.{ .status = 204 }} };
    const d = deliver(fake.transport(), .webhook, dummy_req);
    try testing.expect(d.ok());
    try d.toError();
    try testing.expectEqual(@as(usize, 1), fake.calls);
    try testing.expectEqual(@as(usize, 0), fake.n_slept);
    try testing.expectEqual(@as(u8, 1), d.attempts);
}

test "webhook policy retries three times with 200ms then 400ms" {
    var fake: FakeTransport = .{ .script = &.{.{ .status = 500 }} };
    const d = deliver(fake.transport(), .webhook, dummy_req);
    try testing.expect(!d.ok());
    try testing.expectError(error.Unavailable, d.toError());
    try testing.expectEqual(@as(usize, 3), fake.calls);
    try testing.expectEqual(@as(u8, 3), d.attempts);
    // Two gaps for three attempts, doubling — and none after the last.
    try testing.expectEqualSlices(u64, &.{
        200 * std.time.ns_per_ms,
        400 * std.time.ns_per_ms,
    }, fake.sleeps());
}

test "a retryable failure followed by success stops early" {
    var fake: FakeTransport = .{ .script = &.{
        error.Connect,
        .{ .status = 200 },
    } };
    const d = deliver(fake.transport(), .webhook, dummy_req);
    try testing.expect(d.ok());
    try testing.expectEqual(@as(usize, 2), fake.calls);
    try testing.expectEqual(@as(usize, 1), fake.n_slept);
    // The successful attempt clears the earlier transport error.
    try testing.expectEqual(@as(?Error, null), d.transport_error);
}

test "a non-retryable status terminates immediately" {
    for ([_]u16{ 400, 401, 403, 404, 410, 422 }) |code| {
        var fake: FakeTransport = .{ .script = &.{.{ .status = code }} };
        const d = deliver(fake.transport(), .webhook, dummy_req);
        try testing.expectError(error.Rejected, d.toError());
        try testing.expectEqual(@as(usize, 1), fake.calls);
        try testing.expectEqual(@as(usize, 0), fake.n_slept);
        try testing.expect(d.rejected);
    }
}

test "408, 429 and 5xx are the retryable statuses" {
    try testing.expect(retryableStatus(408));
    try testing.expect(retryableStatus(429));
    try testing.expect(retryableStatus(500));
    try testing.expect(retryableStatus(503));
    try testing.expect(retryableStatus(599));
    try testing.expect(!retryableStatus(400));
    try testing.expect(!retryableStatus(404));
    try testing.expect(!retryableStatus(200));
    try testing.expect(!retryableStatus(302));
    try testing.expect(!retryableStatus(600));
}

test "chat policy is one attempt so a 429 is not amplified" {
    var fake: FakeTransport = .{ .script = &.{.{ .status = 429 }} };
    const d = deliver(fake.transport(), .chat, dummy_req);
    try testing.expectError(error.Unavailable, d.toError());
    try testing.expectEqual(@as(usize, 1), fake.calls);
    try testing.expectEqual(@as(usize, 0), fake.n_slept);
}

test "cancellation abandons the remaining budget" {
    var fake: FakeTransport = .{ .script = &.{error.Canceled} };
    const d = deliver(fake.transport(), .webhook, dummy_req);
    try testing.expectError(error.Canceled, d.toError());
    try testing.expectEqual(@as(usize, 1), fake.calls);
    try testing.expectEqual(@as(usize, 0), fake.n_slept);
}

test "backoff schedule doubles and saturates" {
    const p: RetryPolicy = .webhook;
    try testing.expectEqual(@as(u32, 200), p.backoffMs(1));
    try testing.expectEqual(@as(u32, 400), p.backoffMs(2));
    try testing.expectEqual(@as(u32, 800), p.backoffMs(3));
    try testing.expectEqual(@as(u32, 0), p.backoffMs(0));
    // Absurd attempt counts saturate instead of shifting off the end.
    try testing.expectEqual(@as(u32, std.math.maxInt(u32)), p.backoffMs(200));
    try testing.expectEqual(@as(u32, 0), RetryPolicy.chat.backoffMs(1));
}

test "describe reports the failure without the URL" {
    var buf: Delivery.DescribeBuf = undefined;
    try testing.expectEqualStrings("ok", (Delivery{ .attempts = 1, .status = 200 }).describe(&buf));
    try testing.expectEqualStrings(
        "non-retryable status 404",
        (Delivery{ .attempts = 1, .status = 404, .rejected = true }).describe(&buf),
    );
    try testing.expectEqualStrings(
        "3 attempt(s) failed: status 503",
        (Delivery{ .attempts = 3, .status = 503 }).describe(&buf),
    );
    try testing.expectEqualStrings(
        "2 attempt(s) failed: Timeout",
        (Delivery{ .attempts = 2, .transport_error = error.Timeout }).describe(&buf),
    );
}

test "safeUrl keeps the authority and drops the credential" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{
            "https://discord.com/api/webhooks/123456789/aVerySecretToken",
            "https://discord.com",
        },
        .{
            "https://hooks.slack.com/services/T00/B00/XXXXXXXXXXXXXXXXXXXXXXXX",
            "https://hooks.slack.com",
        },
        .{ "http://127.0.0.1:8080/hook?token=abc", "http://127.0.0.1:8080" },
        .{ "https://example.test", "https://example.test" },
        .{ "https://example.test/", "https://example.test" },
        .{ "https://example.test#frag", "https://example.test" },
        // Userinfo is itself a credential: cut before it.
        .{ "https://user:pass@example.test/hook", "https://" },
        // Nothing recognisable — say nothing rather than guess.
        .{ "example.test/hook", "" },
        .{ "", "" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], Target.safeUrl(c[0]));
}

test "no safeUrl output contains a token from the input" {
    const token = "aVerySecretToken";
    const url = "https://discord.com/api/webhooks/123/" ++ token;
    const safe = Target.safeUrl(url);
    try testing.expect(std.mem.indexOf(u8, safe, token) == null);
    const t: Target = .{ .name = "n", .url = url };
    try testing.expect(std.mem.indexOf(u8, t.safe(), token) == null);
}
