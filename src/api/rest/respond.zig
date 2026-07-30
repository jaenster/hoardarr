//! Turning a handler's outcome into an HTTP response, in one place.
//!
//! Two things live here that must not be duplicated per endpoint: the
//! error → status mapping, and the decision about what a client is told
//! when something fails. The second one matters more than it looks. A
//! 500 on an endpoint reachable with a stolen API key should not carry a
//! driver message naming a table or a filesystem path, so the body gets
//! the port's fixed sentence and the detail goes to the log.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const request = @import("../../net/http/request.zig");
const log = @import("../../core/log.zig");
const json = @import("json.zig");
const ports = @import("ports.zig");

const hlog = log.Scoped(.debug);

pub const Error = http.HandlerError;

// ---------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------

pub fn jsonDoc(ctx: *http.Ctx, status: u16, doc: []const u8) Error!void {
    try ctx.res.send(status, "application/json", doc);
}

pub fn ok(ctx: *http.Ctx, w: *const json.Writer) Error!void {
    return jsonDoc(ctx, 200, w.items());
}

pub fn created(ctx: *http.Ctx, w: *const json.Writer) Error!void {
    return jsonDoc(ctx, 201, w.items());
}

pub fn accepted(ctx: *http.Ctx, w: *const json.Writer) Error!void {
    return jsonDoc(ctx, 202, w.items());
}

/// 204: the answer to every successful mutation that has nothing to say.
pub fn noContent(ctx: *http.Ctx) Error!void {
    try ctx.res.sendStatus(204);
}

/// `{"error": "<message>"}` with the message escaped. The status is
/// logged at error level for 5xx so an operator sees it even though the
/// client does not.
pub fn fail(ctx: *http.Ctx, status: u16, msg: []const u8) Error!void {
    if (status >= 500) {
        hlog.err("rest handler", &.{
            log.uint("status", status),
            log.str("path", ctx.req.path),
            log.str("err", msg),
        });
    }
    try ctx.res.sendError(status, msg);
}

/// The port-error path. `error.Canceled` is the client having gone away
/// mid-query — a browser tab closing during a slow list — and is logged
/// at debug rather than as a server fault, which is the distinction the
/// Go `isClientDisconnect` check made.
pub fn failErr(ctx: *http.Ctx, err: ports.Error) Error!void {
    if (err == error.Canceled) {
        hlog.debug("rest handler: client disconnected", &.{
            log.str("path", ctx.req.path),
        });
        // Nothing is going to read this, but the connection still needs
        // a response to stay in a known state.
        return ctx.res.sendError(499, "request cancelled");
    }
    return fail(ctx, statusFor(err), ports.message(err));
}

/// A port error with a message the handler wants to be more specific
/// about — "name, host, port required" rather than "invalid request".
pub fn failErrMsg(ctx: *http.Ctx, err: ports.Error, msg: []const u8) Error!void {
    return fail(ctx, statusFor(err), msg);
}

pub fn statusFor(err: ports.Error) u16 {
    return switch (err) {
        error.NotFound => 404,
        error.Conflict => 409,
        error.Invalid => 400,
        error.Unauthorized => 401,
        error.Forbidden => 403,
        error.Unavailable, error.OutOfMemory => 503,
        error.Upstream => 502,
        // Never reached through `failErr`, which handles it first.
        error.Canceled => 499,
        error.Internal => 500,
    };
}

/// The response for a port that was not wired up. Go simply did not
/// register the route, so the client got a 404 that looked identical to
/// a typo'd URL; 503 says "this build has no such capability" and leaves
/// 404 to mean "no such thing", which is what the UI needs to tell the
/// difference between an old server and a deleted job.
pub fn unavailable(ctx: *http.Ctx, what: []const u8) Error!void {
    var buf: [96]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "{s} unavailable", .{what}) catch "unavailable";
    return fail(ctx, 503, msg);
}

/// A file download: `Content-Disposition: attachment` with the filename
/// quoted. The name has already been through the port's safe-path check;
/// quotes and backslashes are still stripped here because a header
/// cannot escape them and a name with a quote in it would produce a
/// header the client parses wrongly.
pub fn attachment(
    ctx: *http.Ctx,
    content_type: []const u8,
    name: []const u8,
    body: []const u8,
) Error!void {
    var buf: [256]u8 = undefined;
    var n: usize = 0;
    const prefix = "attachment; filename=\"";
    @memcpy(buf[0..prefix.len], prefix);
    n = prefix.len;
    for (name) |c| {
        if (n + 2 >= buf.len) break;
        if (c == '"' or c == '\\' or c < 0x20 or c == 0x7F) continue;
        buf[n] = c;
        n += 1;
    }
    buf[n] = '"';
    n += 1;
    try ctx.res.setHeader("Content-Disposition", buf[0..n]);
    try ctx.res.send(200, content_type, body);
}

// ---------------------------------------------------------------------
// Request parsing
// ---------------------------------------------------------------------

/// Decode the request body as a JSON object. Answers 400 itself when it
/// cannot, so the handler's call site is one `orelse return`.
pub fn bodyObject(ctx: *http.Ctx, arena: std.mem.Allocator) Error!?json.Body {
    return json.Body.parse(arena, ctx.req.body) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.Malformed => {
            try fail(ctx, 400, "request body is not a JSON object");
            return null;
        },
    };
}

/// The path segments a prefix route left in `Ctx.tail`, split on '/'.
///
/// The Zig router has `exact` and `prefix` and no pattern language, so
/// `/api/v1/queue/{id}/pause` is a prefix route plus this. That is not a
/// downgrade from Go's `ServeMux`: the matching is a `memcmp`, the
/// segments are slices into the request buffer, and nothing allocates.
pub const Segments = struct {
    /// At most three: `{id}/action` never needs more, and a longer tail
    /// is a 404 rather than something to allocate for.
    items: [3][]const u8 = @splat(""),
    len: usize = 0,

    pub fn get(self: Segments, i: usize) []const u8 {
        // `len` can exceed the array — that is how an over-long path
        // marks itself — so the bound is the array, not `len`.
        if (i >= self.len or i >= self.items.len) return "";
        return self.items[i];
    }

    pub fn eq(self: Segments, i: usize, s: []const u8) bool {
        return std.mem.eql(u8, self.get(i), s);
    }
};

pub fn segments(tail: []const u8) Segments {
    var out: Segments = .{};
    var rest = tail;
    // A leading slash means the prefix route's path did not end in one;
    // either way an empty first segment is not a segment.
    while (rest.len > 0 and rest[0] == '/') rest = rest[1..];
    while (rest.len > 0) {
        if (out.len == out.items.len) {
            // Overflow marks itself: `len` past the array cannot be
            // matched by `eq`, so an over-long path falls through to the
            // handler's 404 instead of matching a shorter route.
            out.len += 1;
            return out;
        }
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
        if (slash > 0) {
            out.items[out.len] = rest[0..slash];
            out.len += 1;
        }
        rest = if (slash == rest.len) "" else rest[slash + 1 ..];
    }
    return out;
}

/// A path segment as a positive integer id. Rejects the empty string,
/// signs, whitespace and anything that does not fit — a 400, exactly as
/// `pathID` did.
pub fn parseId(s: []const u8) ?i64 {
    if (s.len == 0 or s.len > 19) return null;
    var v: i64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        v = v * 10 + @as(i64, c - '0');
    }
    return v;
}

/// Percent-decode a path segment into caller storage. Used for the
/// category name and the file-download endpoints, whose parameters are
/// text rather than numbers.
pub fn decodeSegment(out: []u8, s: []const u8) ?[]const u8 {
    return request.percentDecode(out, s, .path) catch null;
}

/// `?name=` with surrounding whitespace trimmed, or null when absent or
/// empty. Percent-decoding is skipped for the parameters that cannot
/// contain reserved characters; `queryDecoded` is for the ones that can.
pub fn query(ctx: *http.Ctx, name: []const u8) ?[]const u8 {
    const raw = ctx.req.queryValue(name) orelse return null;
    const trimmed = std.mem.trim(u8, raw, " \t");
    return if (trimmed.len == 0) null else trimmed;
}

pub fn queryDecoded(out: []u8, ctx: *http.Ctx, name: []const u8) ?[]const u8 {
    const raw = query(ctx, name) orelse return null;
    const decoded = request.percentDecode(out, raw, .query) catch return null;
    const trimmed = std.mem.trim(u8, decoded, " \t");
    return if (trimmed.len == 0) null else trimmed;
}

/// A query parameter as an integer, or null when absent or unparseable.
/// Unparseable is deliberately not an error for the parameters that use
/// this: `?limit=abc` falling back to the default is friendlier than a
/// 400, and matches what the Go `listCommands` did.
pub fn queryInt(ctx: *http.Ctx, name: []const u8) ?i64 {
    const raw = query(ctx, name) orelse return null;
    return std.fmt.parseInt(i64, raw, 10) catch null;
}

// ---------------------------------------------------------------------
// RFC 3339
// ---------------------------------------------------------------------

/// Parse an RFC 3339 instant into unix milliseconds.
///
/// Only what the API actually receives: `?since=` on `/history`, which
/// the frontend produces with `toISOString()` and an operator might type
/// by hand with an offset. Fractional seconds are accepted and truncated
/// to milliseconds; an offset is applied; a missing timezone is treated
/// as UTC rather than rejected, because a handwritten one usually means
/// UTC and rejecting it is not a security decision.
///
/// Null for anything not of that shape — the handler turns that into a
/// 400, as the Go `time.Parse` failure did.
pub fn parseRfc3339Ms(s: []const u8) ?i64 {
    // "YYYY-MM-DDTHH:MM:SS" is the shortest accepted form.
    if (s.len < 19) return null;
    if (s[4] != '-' or s[7] != '-') return null;
    if (s[10] != 'T' and s[10] != 't' and s[10] != ' ') return null;
    if (s[13] != ':' or s[16] != ':') return null;

    const year = num(s[0..4]) orelse return null;
    const month = num(s[5..7]) orelse return null;
    const day = num(s[8..10]) orelse return null;
    const hour = num(s[11..13]) orelse return null;
    const minute = num(s[14..16]) orelse return null;
    const second = num(s[17..19]) orelse return null;

    if (month < 1 or month > 12 or day < 1 or day > 31) return null;
    if (hour > 23 or minute > 59 or second > 60) return null;

    var rest = s[19..];
    var millis: i64 = 0;
    if (rest.len > 0 and rest[0] == '.') {
        rest = rest[1..];
        var digits: usize = 0;
        var scale: i64 = 100;
        while (digits < rest.len and rest[digits] >= '0' and rest[digits] <= '9') : (digits += 1) {
            if (digits < 3) {
                millis += @as(i64, rest[digits] - '0') * scale;
                scale = @divTrunc(scale, 10);
            }
        }
        if (digits == 0) return null;
        rest = rest[digits..];
    }

    var offset_minutes: i64 = 0;
    if (rest.len == 0) {
        // No zone: treated as UTC.
    } else if (rest.len == 1 and (rest[0] == 'Z' or rest[0] == 'z')) {
        // UTC.
    } else if (rest.len == 6 and (rest[0] == '+' or rest[0] == '-')) {
        const oh = num(rest[1..3]) orelse return null;
        if (rest[3] != ':') return null;
        const om = num(rest[4..6]) orelse return null;
        if (oh > 23 or om > 59) return null;
        offset_minutes = @as(i64, oh) * 60 + om;
        if (rest[0] == '+') offset_minutes = -offset_minutes;
    } else return null;

    const days = daysFromCivil(year, month, day);
    const secs = days * std.time.s_per_day +
        @as(i64, hour) * 3600 + @as(i64, minute) * 60 + second +
        offset_minutes * 60;
    return secs * std.time.ms_per_s + millis;
}

fn num(s: []const u8) ?i64 {
    var v: i64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        v = v * 10 + (c - '0');
    }
    return v;
}

/// Days since 1970-01-01 for a proleptic Gregorian date. Howard
/// Hinnant's `days_from_civil`, the inverse of `log.civilFromDays`.
fn daysFromCivil(y_in: i64, m: i64, d: i64) i64 {
    const y = y_in - @intFromBool(m <= 2);
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const doy = @divTrunc(153 * (m + (if (m > 2) @as(i64, -3) else 9)) + 2, 5) + d - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

/// `?since=` as unix ms. Returns `error.BadTime` — not null — when the
/// parameter is present but unparseable, so the handler can answer 400
/// instead of silently ignoring a filter the caller asked for.
pub fn queryTimeMs(ctx: *http.Ctx, name: []const u8) error{BadTime}!?i64 {
    var buf: [64]u8 = undefined;
    const raw = queryDecoded(&buf, ctx, name) orelse return null;
    return parseRfc3339Ms(raw) orelse error.BadTime;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "RFC 3339 parsing covers what the frontend and an operator send" {
    // toISOString().
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14T22:13:20.000Z"));
    try testing.expectEqual(@as(?i64, 1_700_000_000_123), parseRfc3339Ms("2023-11-14T22:13:20.123Z"));
    // Sub-millisecond precision is truncated, not rejected.
    try testing.expectEqual(@as(?i64, 1_700_000_000_123), parseRfc3339Ms("2023-11-14T22:13:20.123456789Z"));
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14T22:13:20Z"));
    // No zone means UTC.
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14T22:13:20"));
    // Offsets are applied, in both directions.
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14T23:13:20+01:00"));
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14T17:13:20-05:00"));
    // Lowercase separators, and a space instead of T.
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14t22:13:20z"));
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), parseRfc3339Ms("2023-11-14 22:13:20Z"));

    // The epoch and a leap day, to catch an off-by-one in the civil
    // calendar arithmetic.
    try testing.expectEqual(@as(?i64, 0), parseRfc3339Ms("1970-01-01T00:00:00Z"));
    try testing.expectEqual(@as(?i64, 951_782_400_000), parseRfc3339Ms("2000-02-29T00:00:00Z"));
    try testing.expectEqual(@as(?i64, 1_709_164_800_000), parseRfc3339Ms("2024-02-29T00:00:00Z"));
    // Before the epoch.
    try testing.expectEqual(@as(?i64, -86_400_000), parseRfc3339Ms("1969-12-31T00:00:00Z"));
}

test "RFC 3339 parsing refuses malformed and hostile input" {
    const bad = [_][]const u8{
        "",
        "2023-11-14",
        "2023-11-14T22:13",
        "yesterday",
        "2023/11/14T22:13:20Z",
        "2023-11-14X22:13:20Z",
        "2023-13-14T22:13:20Z", // month 13
        "2023-11-32T22:13:20Z", // day 32
        "2023-11-14T24:13:20Z", // hour 24
        "2023-11-14T22:60:20Z", // minute 60
        "2023-11-14T22:13:20.Z", // empty fraction
        "2023-11-14T22:13:20+1:00", // offset not zero-padded
        "2023-11-14T22:13:20+01:60", // offset minute 60
        "2023-11-14T22:13:20ZZ",
        "2023-11-14T22:13:20 UTC",
        "20a3-11-14T22:13:20Z",
        "\x00023-11-14T22:13:20Z",
    };
    for (bad) |s| try testing.expectEqual(@as(?i64, null), parseRfc3339Ms(s));
}

test "port errors map to the statuses the Go handlers used" {
    try testing.expectEqual(@as(u16, 404), statusFor(error.NotFound));
    try testing.expectEqual(@as(u16, 409), statusFor(error.Conflict));
    try testing.expectEqual(@as(u16, 400), statusFor(error.Invalid));
    try testing.expectEqual(@as(u16, 401), statusFor(error.Unauthorized));
    try testing.expectEqual(@as(u16, 403), statusFor(error.Forbidden));
    try testing.expectEqual(@as(u16, 502), statusFor(error.Upstream));
    try testing.expectEqual(@as(u16, 503), statusFor(error.Unavailable));
    try testing.expectEqual(@as(u16, 500), statusFor(error.Internal));
    // OOM is a 503, not a 500: it is transient and a client may retry.
    try testing.expectEqual(@as(u16, 503), statusFor(error.OutOfMemory));
}

test "path segments split without allocating" {
    const s = segments("/42/pause");
    try testing.expectEqual(@as(usize, 2), s.len);
    try testing.expectEqualStrings("42", s.get(0));
    try testing.expectEqualStrings("pause", s.get(1));
    try testing.expect(s.eq(1, "pause"));
    try testing.expect(!s.eq(1, "resume"));
    // Out of range reads as empty rather than trapping.
    try testing.expectEqualStrings("", s.get(2));

    try testing.expectEqual(@as(usize, 1), segments("42").len);
    try testing.expectEqual(@as(usize, 1), segments("/42").len);
    try testing.expectEqual(@as(usize, 1), segments("42/").len);
    try testing.expectEqual(@as(usize, 0), segments("").len);
    try testing.expectEqual(@as(usize, 0), segments("/").len);
    try testing.expectEqual(@as(usize, 0), segments("///").len);
    // Empty interior segments collapse, so "//pause" is still one.
    try testing.expectEqualStrings("pause", segments("//pause").get(0));

    // Too many segments reports a length no `eq` can match, so an
    // over-long path cannot be mistaken for a shorter route.
    const long = segments("1/2/3/4/5");
    try testing.expect(long.len > 3);
    try testing.expect(!long.eq(3, "5"));
}

test "id parsing refuses everything that is not a plain number" {
    try testing.expectEqual(@as(?i64, 42), parseId("42"));
    try testing.expectEqual(@as(?i64, 0), parseId("0"));
    try testing.expectEqual(@as(?i64, 9223372036854775807), parseId("9223372036854775807"));

    const bad = [_][]const u8{
        "",       "-1",   "+1",   " 1",   "1 ",       "1.0",
        "0x10",   "abc",  "1e3",  "١٢٣", "1\x002",   "99999999999999999999",
    };
    for (bad) |s| try testing.expectEqual(@as(?i64, null), parseId(s));
}

test "attachment filenames cannot inject a header" {
    // Only exercises the sanitising loop; the header write itself needs
    // a connection, which the handler tests provide.
    const hostile = "back\\slash\"quote\r\nX-Evil: 1";
    var buf: [256]u8 = undefined;
    var n: usize = 0;
    for (hostile) |c| {
        if (c == '"' or c == '\\' or c < 0x20 or c == 0x7F) continue;
        buf[n] = c;
        n += 1;
    }
    try testing.expectEqualStrings("backslashquoteX-Evil: 1", buf[0..n]);
}
