//! `/api/v1/auth/*` — the authentication surface.
//!
//! ## The scheme, exactly
//!
//! Two credentials, either sufficient, decided by `http.Auth.authorize`
//! before a handler ever runs:
//!
//!   * **Session cookie** `hoardarr_session`, minted by `/auth/login`,
//!     HttpOnly, `SameSite=Lax`, scoped to the URL base. The browser
//!     path, and the only credential that identifies a *user*.
//!   * **API key**, presented as `X-Api-Key` or `?apikey=`, compared in
//!     constant time. The path for Sonarr, Radarr, Prowlarr and the
//!     SAB-compatible clients, none of which can do a login form.
//!
//! Public endpoints — reachable with neither — are exactly three:
//! `/auth/whoami`, `/auth/setup` and `/auth/login`. Everything else in
//! the API is `protected`, which is the router's default, so a route
//! added without thinking about auth fails closed.
//!
//! `whoami` is public deliberately: the frontend has to ask which screen
//! to render *before* it has any credential. It leaks nothing — the
//! answer is one of "needs_setup", "needs_login", or the identity of the
//! session the caller already holds.
//!
//! `change-password` is stricter than the middleware: it requires a
//! session cookie specifically, and rejects an API-key-only caller. A
//! shared key identifies the deployment, not a person, and there is no
//! defensible way to decide *whose* password it may change.
//!
//! ## Rate limiting
//!
//! `setup` and `login` both end in a bcrypt verification, which at cost
//! 10 is ~100 ms of CPU on purpose. Ten attempts per minute per source
//! address — the Go limits, unchanged. See `ratelimit.zig` for why the
//! window is a window and not a bucket.
//!
//! ## Setup
//!
//! `/auth/setup` is public and creates the admin. It is a one-shot: the
//! port answers `Conflict` once an admin exists, and that check is
//! inside the same transaction as the insert, so two simultaneous
//! first-run requests cannot both win.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const json = @import("json.zig");
const ports = @import("ports.zig");
const respond = @import("respond.zig");
const ratelimit = @import("ratelimit.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// Same name the Go build issued, because existing browser sessions have
/// to keep working across the port. Declared by the HTTP layer, which is
/// what reads it on the way in.
pub const cookie_name = http.session_cookie_name;

/// Minimum length `setup` enforces. The Go handler's number.
pub const min_password_len = 8;

// ---------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------

/// GET /api/v1/auth/whoami — public.
///
/// The frontend probes this on load:
///   - `{"state":"needs_setup"}`    → render the first-run admin form
///   - `{"state":"needs_login"}`    → render the login form
///   - `{"state":"authenticated"}`  → render the app
pub fn whoami(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const arena = api.beginRequest();
    var w = json.Writer.init(arena);

    // No auth port at all: this build has authentication disabled, so
    // everyone is "authenticated". Matches the Go handler; the API key
    // check in the HTTP layer is unaffected and still applies to every
    // protected route.
    const auth = api.auth orelse {
        try w.beginObject();
        try w.strField("state", "authenticated");
        try w.endObject();
        return respond.ok(ctx, &w);
    };

    const needs_setup = auth.needsSetup() catch |err| return respond.failErr(ctx, err);
    if (needs_setup) {
        try w.beginObject();
        try w.strField("state", "needs_setup");
        try w.endObject();
        return respond.ok(ctx, &w);
    }

    if (ctx.req.cookie(cookie_name)) |token| {
        if (token.len > 0) {
            if (auth.authenticate(arena, token)) |who| {
                try w.beginObject();
                try w.strField("state", "authenticated");
                try w.key("user");
                try w.beginObject();
                try w.intField("id", who.user_id);
                try w.strField("username", who.username);
                try w.strField("role", who.role);
                try w.endObject();
                try w.endObject();
                return respond.ok(ctx, &w);
            } else |_| {
                // An expired or forged cookie is not an error here: the
                // answer is simply "log in".
            }
        }
    }

    try w.beginObject();
    try w.strField("state", "needs_login");
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/auth/setup — public, rate limited.
pub fn setup(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    if (try rateLimited(ctx, api)) return;

    const auth = api.auth orelse return respond.fail(ctx, 503, "auth disabled");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const username = body.trimmedString("username") orelse "";
    const password = body.string("password") orelse "";
    if (username.len == 0 or password.len == 0) {
        return respond.fail(ctx, 400, "username and password required");
    }
    if (password.len < min_password_len) {
        return respond.fail(ctx, 400, "password must be at least 8 characters");
    }

    const id = auth.setupAdmin(username, password) catch |err| switch (err) {
        error.Conflict => return respond.fail(ctx, 409, "setup has already been completed"),
        else => return respond.failErr(ctx, err),
    };

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.intField("id", id);
    try w.endObject();

    // Issue a session straight away so the operator is not asked to log
    // in one second after choosing the password. A failure here is not
    // fatal: setup succeeded, and the frontend falls through to the
    // login form.
    if (auth.login(arena, username, password)) |session| {
        try setSessionCookie(ctx, api, session);
    } else |_| {}

    return respond.created(ctx, &w);
}

/// POST /api/v1/auth/login — public, rate limited.
pub fn login(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    if (try rateLimited(ctx, api)) return;

    const auth = api.auth orelse return respond.fail(ctx, 503, "auth disabled");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const username = body.string("username") orelse "";
    const password = body.string("password") orelse "";

    const session = auth.login(arena, username, password) catch |err| switch (err) {
        // One message for "no such user" and "wrong password" both, so
        // the endpoint is not a username oracle.
        error.Unauthorized => return respond.fail(ctx, 401, "invalid credentials"),
        else => return respond.failErr(ctx, err),
    };

    try setSessionCookie(ctx, api, session);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.boolField("ok", true);
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/auth/logout — protected.
///
/// Always 204, even when there was no session: logging out of nothing is
/// not an error, and reporting the difference would tell a caller
/// whether a token it presented was live.
pub fn logout(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const auth = api.auth orelse return respond.noContent(ctx);
    if (ctx.req.cookie(cookie_name)) |token| {
        if (token.len > 0) auth.logout(token);
    }
    try clearSessionCookie(ctx, api);
    return respond.noContent(ctx);
}

/// POST /api/v1/auth/change-password — protected, session only.
pub fn changePassword(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const auth = api.auth orelse return respond.fail(ctx, 503, "auth disabled");
    const arena = api.beginRequest();

    // The middleware let this through on either credential; changing a
    // password needs the one that names a user.
    const token = ctx.req.cookie(cookie_name) orelse
        return respond.fail(ctx, 401, "session required");
    if (token.len == 0) return respond.fail(ctx, 401, "session required");
    const who = auth.authenticate(arena, token) catch
        return respond.fail(ctx, 401, "session invalid");

    const body = try respond.bodyObject(ctx, arena) orelse return;
    const old = body.string("old_password") orelse "";
    const new = body.string("new_password") orelse "";
    if (old.len == 0 or new.len == 0) {
        return respond.fail(ctx, 400, "old_password and new_password required");
    }

    auth.changePassword(who.user_id, old, new) catch |err| switch (err) {
        error.Unauthorized => return respond.fail(ctx, 401, "current password is incorrect"),
        error.Invalid => return respond.fail(ctx, 400, "password must be at least 8 characters"),
        else => return respond.failErr(ctx, err),
    };
    return respond.noContent(ctx);
}

/// POST /api/v1/auth/rotate-api-key — protected.
///
/// The new key is in the response body, once. The old one stops working
/// on the very next request, because the HTTP layer reads the key
/// through the same port on every request rather than capturing it.
/// Browser sessions are unaffected.
pub fn rotateApiKey(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cfg = api.config orelse return respond.fail(ctx, 503, "runtime config unavailable");
    const writer = cfg.writer orelse return respond.fail(ctx, 503, "runtime config is read-only");
    const arena = api.beginRequest();

    const key = writer.rotateApiKeyFn(writer.ctx, arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.strField("api_key", key);
    try w.endObject();
    return respond.ok(ctx, &w);
}

// ---------------------------------------------------------------------
// Cookies
// ---------------------------------------------------------------------

/// `Secure` is deliberately absent. Most self-hosted deployments sit
/// behind a reverse proxy that terminates TLS, and the hop from the
/// proxy to hoardarr is plain HTTP — a `Secure` cookie would never come
/// back. `HttpOnly` and `SameSite=Lax` are both set, which is what
/// actually defends the token: script cannot read it and a cross-site
/// POST cannot ride it.
fn setSessionCookie(ctx: *http.Ctx, api: *Api, session: ports.Auth.SessionInfo) Error!void {
    // A token that could contain a `;` or a control byte would let the
    // value forge cookie attributes. Ours are hex, so anything else is a
    // bug in the auth adapter and is refused rather than sent.
    if (!isCookieSafe(session.token)) return respond.fail(ctx, 500, "malformed session token");

    var path_buf: [256]u8 = undefined;
    const path = cookiePath(api, &path_buf);

    // Max-Age rather than Expires: no dependence on the client's clock
    // being right, which on a NAS-hosted browser it often is not.
    const max_age = maxAgeSeconds(session.expires_at_ms, wallMillis());

    var buf: [512]u8 = undefined;
    const cookie = std.fmt.bufPrint(&buf, "{s}={s}; Path={s}; Max-Age={d}; HttpOnly; SameSite=Lax", .{
        cookie_name, session.token, path, max_age,
    }) catch return respond.fail(ctx, 500, "session cookie too large");
    try ctx.res.setHeader("Set-Cookie", cookie);
}

fn clearSessionCookie(ctx: *http.Ctx, api: *Api) Error!void {
    var path_buf: [256]u8 = undefined;
    const path = cookiePath(api, &path_buf);
    var buf: [512]u8 = undefined;
    const cookie = std.fmt.bufPrint(&buf, "{s}=; Path={s}; Max-Age=0; HttpOnly; SameSite=Lax", .{
        cookie_name, path,
    }) catch return;
    try ctx.res.setHeader("Set-Cookie", cookie);
}

/// The cookie's `Path`, from the live URL base so it follows a change
/// made in Settings without a restart.
fn cookiePath(api: *Api, buf: []u8) []const u8 {
    const cfg = api.config orelse return "/";
    return cfg.sessionCookiePath(buf);
}

/// Seconds from now until the session expires, floored at zero. A
/// negative value would be `Max-Age=0`, i.e. "delete this cookie", which
/// is the right answer for an already-expired session anyway.
pub fn maxAgeSeconds(expires_at_ms: i64, now_ms: i64) i64 {
    if (expires_at_ms <= now_ms) return 0;
    return @divTrunc(expires_at_ms - now_ms, std.time.ms_per_s);
}

/// Cookie-value safety: printable ASCII with none of the characters that
/// separate a cookie from its attributes.
pub fn isCookieSafe(token: []const u8) bool {
    if (token.len == 0 or token.len > 256) return false;
    for (token) |c| {
        if (c <= 0x20 or c >= 0x7F) return false;
        if (c == '"' or c == ';' or c == ',' or c == '\\' or c == '=') return false;
    }
    return true;
}

fn wallMillis() i64 {
    const sys = @import("../../posix/sys.zig");
    return @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_ms));
}

// ---------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------

/// Consults the limiter and, when over, answers 429 itself. Returns true
/// when the caller should stop.
fn rateLimited(ctx: *http.Ctx, api: *Api) Error!bool {
    var key_buf: [ratelimit.max_key_len]u8 = undefined;
    const key = ratelimit.clientKey(
        &key_buf,
        ctx.req.header("x-forwarded-for"),
        ctx.req.header("x-real-ip"),
    );
    const now = api.now();
    if (api.login_limiter.allow(key, now)) return false;

    var retry_buf: [8]u8 = undefined;
    const retry = std.fmt.bufPrint(&retry_buf, "{d}", .{
        api.login_limiter.retryAfterSeconds(key, now),
    }) catch "60";
    try ctx.res.setHeader("Retry-After", retry);
    try respond.fail(ctx, 429, "rate limit exceeded; try again later");
    return true;
}

// ---------------------------------------------------------------------
// Tests
//
// The endpoint-level tests live in `handlers.zig`, where a real server
// and a real client exercise routing, the auth middleware and these
// handlers together. What is here is the logic that has no request in
// it.
// ---------------------------------------------------------------------

const testing = std.testing;

test "cookie max-age counts down and never goes negative" {
    const hour = 60 * 60 * 1000;
    try testing.expectEqual(@as(i64, 3600), maxAgeSeconds(hour, 0));
    try testing.expectEqual(@as(i64, 1800), maxAgeSeconds(hour, hour / 2));
    // Already expired, or a clock that jumped forward: delete the cookie
    // rather than emitting a negative age.
    try testing.expectEqual(@as(i64, 0), maxAgeSeconds(hour, hour));
    try testing.expectEqual(@as(i64, 0), maxAgeSeconds(hour, hour * 2));
    try testing.expectEqual(@as(i64, 0), maxAgeSeconds(0, 0));
}

test "a session token that could forge cookie attributes is refused" {
    // What the domain actually mints: 64 hex characters.
    try testing.expect(isCookieSafe("0123456789abcdef" ** 4));
    try testing.expect(isCookieSafe("short-but-fine"));

    const hostile = [_][]const u8{
        "",
        "has space",
        "semi;colon",
        "comma,separated",
        "equals=sign",
        "quote\"mark",
        "back\\slash",
        "newline\nX-Evil: 1",
        "carriage\rreturn",
        "nul\x00byte",
        "high\xffbyte",
        "tab\there",
        "a" ** 257,
    };
    for (hostile) |t| try testing.expect(!isCookieSafe(t));
}

test "the password floor is the Go one" {
    try testing.expectEqual(@as(usize, 8), min_password_len);
}
