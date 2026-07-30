//! Port of `internal/bootstrap/e2e_auth_test.go` — `TestAuth_E2E_FullFlow`.
//!
//! The whole `/api/v1/auth/*` flow over real HTTP against a booted
//! daemon: setup, login, cookie, logout, password change, and the API
//! key path an *arr uses because it cannot do cookies.
//!
//! Cookies are handled by hand rather than by a jar. Go's client has
//! one; here every request states which session it is presenting, which
//! is more explicit anyway — step 12 replays a *stale* cookie on
//! purpose, and a jar would have quietly dropped it.
//!
//! bcrypt at cost 10 runs several times in this test, and in a Debug
//! build that is the wall clock. It is real work rather than a sleep,
//! and the harness deadline is generous enough for it.
//!
//! # These two tests currently FAIL, and the failure is the point
//!
//! They pass every functional assertion and then fail on leaked
//! allocations, because the daemon leaks a user aggregate on two paths.
//! Both are one-line fixes and neither is in this directory:
//!
//!   * `bootstrap/rest.zig`'s `Auth.authenticate` calls
//!     `svc.authenticate(token)`, which loads a `*User` through
//!     `UserStore.byId` — a fresh heap allocation per call — copies the
//!     strings it needs into the request arena, and never calls
//!     `svc.users.release(u)`. **Every cookie-authenticated request
//!     leaks one user aggregate**, and the UI polls every two seconds.
//!     This is unbounded growth in the running daemon, not a test
//!     artefact.
//!   * `app/auth.zig`'s `setupAdmin` sets `adopted = true` after
//!     `users.save`, but the SQLite `UserStore.save` writes columns and
//!     does not take ownership, so the aggregate is dropped on the
//!     floor. Once per setup, so it is the smaller of the two.
//!
//! The assertions are deliberately not relaxed to `ArenaAllocator`.
//! A leak that only a booted daemon reveals is exactly what this suite
//! exists to find, and hiding it would make the test worse than absent.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");

const username = "admin";
const password = "supers3cret";
const new_password = "brandnew1234";

fn creds(comptime user: []const u8, comptime pass: []const u8) []const u8 {
    return "{\"username\":\"" ++ user ++ "\",\"password\":\"" ++ pass ++ "\"}";
}

test "auth: the full setup, login, logout and change-password flow" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "auth", .{});
    defer fx.deinit();

    // 1. whoami on an empty database says setup is needed. This is the
    //    one auth endpoint that must answer without credentials — it is
    //    what the UI asks before it has any.
    {
        var r = try fx.request(.{ .path = "/api/v1/auth/whoami", .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "whoami on an empty database");
        try r.expectField("state", "needs_setup");
    }

    // 2. the queue without credentials is refused.
    {
        var r = try fx.request(.{ .path = "/api/v1/queue", .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 401, "queue with no credentials");
    }

    // 3. setup creates the first admin and issues a session in the same
    //    response, so the operator is logged in without a second round
    //    trip.
    var session: []u8 = &.{};
    defer gpa.free(session);
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/setup",
            .body = creds(username, password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 201, "first /auth/setup");
        const c = r.sessionCookie();
        if (c.len == 0) return error.SetupIssuedNoSessionCookie;
        session = try gpa.dupe(u8, c);
    }

    // 4. that cookie authenticates a protected read.
    {
        var r = try fx.request(.{ .path = "/api/v1/queue", .api_key = "", .session = session });
        defer r.deinit();
        try h.expectStatus(&r, 200, "queue with the setup cookie");
    }

    // 5. and whoami reports who it belongs to.
    {
        var r = try fx.request(.{ .path = "/api/v1/auth/whoami", .api_key = "", .session = session });
        defer r.deinit();
        try h.expectStatus(&r, 200, "whoami with a cookie");
        try r.expectField("state", "authenticated");
        try r.expectField("username", username);
    }

    // 6. setup a second time is refused. Without this, anyone who
    //    reaches the daemon before the operator does can take it over.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/setup",
            .body = creds("admin2", "anotherone"),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 409, "second /auth/setup");
    }

    // 7. anonymous whoami now says login rather than setup.
    {
        var r = try fx.request(.{ .path = "/api/v1/auth/whoami", .api_key = "" });
        defer r.deinit();
        try r.expectField("state", "needs_login");
    }

    // 8. wrong password is refused.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/login",
            .body = creds(username, "wrong-password"),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 401, "login with a wrong password");
    }

    // 9. so is an unknown user — with the same status, so the response
    //    cannot be used to enumerate accounts.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/login",
            .body = creds("ghost", password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 401, "login as an unknown user");
    }

    // 10. a correct login mints a fresh session.
    var fresh: []u8 = &.{};
    defer gpa.free(fresh);
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/login",
            .body = creds(username, password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "login");
        const c = r.sessionCookie();
        if (c.len == 0) return error.LoginIssuedNoSessionCookie;
        fresh = try gpa.dupe(u8, c);
        try testing.expect(!std.mem.eql(u8, fresh, session));
    }

    // 11. logout.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/logout",
            .api_key = "",
            .session = fresh,
        });
        defer r.deinit();
        try h.expectStatus(&r, 204, "logout");
    }

    // 12. the same cookie replayed afterwards is refused. This is the
    //     assertion that matters: it proves the session row was deleted
    //     server-side rather than the cookie merely cleared in a
    //     browser that could choose not to.
    {
        var r = try fx.request(.{ .path = "/api/v1/queue", .api_key = "", .session = fresh });
        defer r.deinit();
        try h.expectStatus(&r, 401, "queue with a logged-out cookie");
    }

    // 13. the API key path still works, for clients that cannot do
    //     cookies at all.
    {
        var r = try fx.get("/api/v1/queue");
        defer r.deinit();
    }

    // 15. change-password. Log in fresh first, because the flow is
    //     authenticated by the session it is about to invalidate.
    var changer: []u8 = &.{};
    defer gpa.free(changer);
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/login",
            .body = creds(username, password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "login before change-password");
        changer = try gpa.dupe(u8, r.sessionCookie());
        try testing.expect(changer.len > 0);
    }

    // Wrong old password → 401.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/change-password",
            .body = "{\"old_password\":\"wrong\",\"new_password\":\"" ++ new_password ++ "\"}",
            .api_key = "",
            .session = changer,
        });
        defer r.deinit();
        try h.expectStatus(&r, 401, "change-password with a wrong old password");
    }

    // Too-short new password → 400.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/change-password",
            .body = "{\"old_password\":\"" ++ password ++ "\",\"new_password\":\"short\"}",
            .api_key = "",
            .session = changer,
        });
        defer r.deinit();
        try h.expectStatus(&r, 400, "change-password with a short new password");
    }

    // Happy path → 204.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/change-password",
            .body = "{\"old_password\":\"" ++ password ++ "\",\"new_password\":\"" ++ new_password ++ "\"}",
            .api_key = "",
            .session = changer,
        });
        defer r.deinit();
        try h.expectStatus(&r, 204, "change-password");
    }

    // The old password no longer logs in...
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/login",
            .body = creds(username, password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 401, "login with the old password after a change");
    }

    // ...and the new one does.
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/login",
            .body = creds(username, new_password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "login with the new password");
    }
}

test "auth: a session survives a restart, because it is a database row" {
    // Not in the Go suite, and it is the auth half of the question the
    // whole e2e effort exists to answer: an operator logged in before a
    // container update must not be logged out by it.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "auth-restart", .{});
    defer fx.deinit();

    var session: []u8 = &.{};
    defer gpa.free(session);
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/auth/setup",
            .body = creds(username, password),
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 201, "setup");
        session = try gpa.dupe(u8, r.sessionCookie());
        try testing.expect(session.len > 0);
    }

    try fx.restart(.{});

    var r = try fx.request(.{ .path = "/api/v1/queue", .api_key = "", .session = session });
    defer r.deinit();
    try h.expectStatus(&r, 200, "queue with a pre-restart cookie");
}
