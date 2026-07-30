//! Partial port of `internal/bootstrap/e2e_server_edit_test.go` —
//! `TestServerEditAndTestConnection_E2E`.
//!
//! **Two of the Go test's five steps cannot be ported and are not
//! faked here.** Both are missing capability, not missing effort:
//!
//!   * `POST /api/v1/servers/test` and `POST /servers/{id}/test` — the
//!     probe port is deliberately null in `App.wire` (dialling a
//!     provider needs the NNTP client driven from a fiber, which the
//!     orchestrator wiring owns and this build does not have). Both
//!     answer 503. That is asserted below, so the day the probe is
//!     wired this test fails and has to be completed rather than
//!     quietly continuing to pass.
//!   * `PATCH /api/v1/servers/{id}` — the route exists and the handler
//!     is wired, but `net/http/client.zig`'s `Method` enum has no
//!     `patch`, so the async client cannot issue the request at all.
//!     Nothing in `src/e2e/` can work around that without editing the
//!     client. The edit round-trip (port, backup, billing mode, quota,
//!     per-server cap) is therefore **not covered**.
//!
//! What is covered — add, list, enable, disable, delete, and the
//! rejection of a malformed add — is driven over real HTTP against the
//! booted daemon and the real servers table.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");

test "servers: add, list, enable, disable and delete over real HTTP" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "server-edit", .{});
    defer fx.deinit();

    // A fake provider to point the row at. Nothing dials it here, but a
    // real port keeps the row honest — a test that invents a port
    // number would not notice a daemon that stored the wrong one.
    const port = try fx.startNntp(.{});

    const add_body = try std.fmt.allocPrint(gpa,
        \\{{"name":"stub","host":"127.0.0.1","port":{d},"tls":false,
        \\"username":"user","password":"pw","max_conns":4,"priority":1}}
    , .{port});
    defer gpa.free(add_body);

    var id: i64 = 0;
    {
        var r = try fx.request(.{ .method = .post, .path = "/api/v1/servers", .body = add_body });
        defer r.deinit();
        try h.expectStatus(&r, 201, "POST /api/v1/servers");
        id = try std.fmt.parseInt(i64, r.field("id") orelse return error.NoServerId, 10);
        try testing.expect(id > 0);
    }

    // The row round-trips through the list the UI reads. Asserting the
    // port specifically because it is the field an operator most often
    // gets wrong and the one a silent truncation would corrupt.
    {
        var r = try fx.get("/api/v1/servers");
        defer r.deinit();
        try r.expectField("name", "stub");
        var buf: [32]u8 = undefined;
        try r.expectField("port", try std.fmt.bufPrint(&buf, "{d}", .{port}));
        try r.expectField("max_conns", "4");
        try r.expectField("priority", "1");
        // The password must not come back out. A settings page that
        // echoes credentials is a credential leak into every browser
        // cache and screenshot.
        if (r.contains("\"pw\"")) {
            std.debug.print("\nthe server list echoed the password: {s}\n", .{r.body});
            return error.PasswordEchoed;
        }
    }

    // Both probe routes answer 503 today, and this is the assertion
    // that will fail the moment the probe is wired — at which point the
    // real Go assertions (dial, auth, mode_reader, date) belong here.
    // The probe port is wired now. This asserted 503 while it was null,
    // deliberately, so that wiring it would fail loudly rather than leave a
    // stale expectation passing — which is exactly what happened.
    //
    // The fixture's NNTP server is a real one, so an ad-hoc probe against it
    // should get all the way through the handshake.
    {
        const probe = try std.fmt.allocPrint(gpa,
            \\{{"host":"127.0.0.1","port":{d},"tls":false,"username":"user","password":"pw"}}
        , .{port});
        defer gpa.free(probe);

        var r = try fx.request(.{ .method = .post, .path = "/api/v1/servers/test", .body = probe });
        defer r.deinit();
        try h.expectStatus(&r, 200, "POST /api/v1/servers/test");
        // Reaching the greeting is what proves the probe actually dialled
        // rather than reporting a canned answer.
        try r.expectField("dial", "true");
    }
    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/servers/{d}/test", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .post, .path = path });
        defer r.deinit();
        try h.expectStatus(&r, 200, "POST /api/v1/servers/{id}/test");
    }

    // disable / enable round trip.
    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/servers/{d}/disable", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .post, .path = path });
        defer r.deinit();
        try h.expectStatus(&r, 204, "POST /servers/{id}/disable");
    }
    {
        var r = try fx.get("/api/v1/servers");
        defer r.deinit();
        try r.expectField("enabled", "false");
    }
    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/servers/{d}/enable", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .post, .path = path });
        defer r.deinit();
        try h.expectStatus(&r, 204, "POST /servers/{id}/enable");
    }
    {
        var r = try fx.get("/api/v1/servers");
        defer r.deinit();
        try r.expectField("enabled", "true");
    }

    // The row survives a restart. A server list that empties on a
    // container update is the single most annoying way to lose a
    // configuration.
    try fx.restart(.{});
    {
        var r = try fx.get("/api/v1/servers");
        defer r.deinit();
        try r.expectField("name", "stub");
    }

    // Delete, and it is really gone.
    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/servers/{d}", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .delete, .path = path });
        defer r.deinit();
        try h.expectStatus(&r, 204, "DELETE /servers/{id}");
    }
    {
        var r = try fx.get("/api/v1/servers");
        defer r.deinit();
        try testing.expect(!r.contains("\"stub\""));
    }
}

test "servers: an add missing its host is refused rather than stored" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "server-bad-add", .{});
    defer fx.deinit();

    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/servers",
            .body = "{\"name\":\"a\"}",
        });
        defer r.deinit();
        if (r.status < 400 or r.status >= 500) {
            std.debug.print("\nadd with no host answered {d}: {s}\n", .{ r.status, r.body });
            return error.MalformedAddAccepted;
        }
    }

    var r = try fx.get("/api/v1/servers");
    defer r.deinit();
    try testing.expect(!r.contains("\"a\""));
}
