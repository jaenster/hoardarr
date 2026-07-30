//! `/api/v1/servers` — Usenet server configuration, and the connection
//! probe behind the Settings page's "Test" buttons.
//!
//! Passwords go in and never come back out: `AddServerCmd` carries one,
//! `dto.usenetServer` has no branch that could emit one, and the probe's
//! failure text is built from status lines rather than from the request.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const json = @import("json.zig");
const dto = @import("dto.zig");
const ports = @import("ports.zig");
const respond = @import("respond.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// GET /api/v1/servers
pub fn list(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const servers = api.servers orelse return respond.unavailable(ctx, "servers");
    const arena = api.beginRequest();

    const all = servers.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("servers");
    try w.beginArray();
    for (all) |s| try dto.usenetServer(&w, s);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/servers
pub fn add(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const servers = api.servers orelse return respond.unavailable(ctx, "servers");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const name = body.trimmedString("name") orelse "";
    const host = body.trimmedString("host") orelse "";
    const port = body.int("port") orelse 0;
    if (name.len == 0 or host.len == 0 or port == 0) {
        return respond.fail(ctx, 400, "name, host, port required");
    }
    if (port < 1 or port > 65535) return respond.fail(ctx, 400, "port out of range");

    const id = servers.add(.{
        .name = name,
        .host = host,
        .port = @intCast(port),
        .tls = body.boolean("tls"),
        .username = body.string("username") orelse "",
        .password = body.string("password") orelse "",
        .max_conns = clampInt(body.int("max_conns")),
        .priority = clampInt(body.int("priority")),
        .backup = body.boolean("backup") orelse false,
        .billing_mode = body.string("billing_mode") orelse "",
        .quota_bytes = body.int("quota_bytes") orelse 0,
        .bandwidth_bytes_per_sec = body.int("bandwidth_bytes_per_sec") orelse 0,
    }) catch |err| switch (err) {
        error.Conflict => return respond.fail(ctx, 409, "a server with that name already exists"),
        else => return respond.failErr(ctx, err),
    };

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.intField("id", id);
    try w.endObject();
    return respond.created(ctx, &w);
}

/// PATCH /api/v1/servers/{id}
///
/// Absent keys are left alone; a key present with a value is applied,
/// including one set back to its zero value. That distinction is the
/// whole reason the command's fields are optional.
pub fn patch(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const servers = api.servers orelse return respond.unavailable(ctx, "servers");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    var cmd: ports.UpdateServerCmd = .{ .id = id };
    if (body.has("host")) cmd.host = body.trimmedString("host");
    if (body.has("port")) {
        const p = body.int("port") orelse return respond.fail(ctx, 400, "port must be a number");
        if (p < 1 or p > 65535) return respond.fail(ctx, 400, "port out of range");
        cmd.port = @intCast(p);
    }
    if (body.has("tls")) cmd.tls = body.boolean("tls");
    if (body.has("username")) cmd.username = body.string("username");
    if (body.has("password")) cmd.password = body.string("password");
    if (body.has("max_conns")) cmd.max_conns = clampInt(body.int("max_conns"));
    if (body.has("priority")) cmd.priority = clampInt(body.int("priority"));
    if (body.has("backup")) cmd.backup = body.boolean("backup");
    if (body.has("billing_mode")) cmd.billing_mode = body.string("billing_mode");
    if (body.has("quota_bytes")) cmd.quota_bytes = body.int("quota_bytes");
    if (body.has("bandwidth_bytes_per_sec")) cmd.bandwidth_bytes_per_sec = body.int("bandwidth_bytes_per_sec");

    servers.update(cmd) catch |err| return respond.failErr(ctx, err);
    return respond.noContent(ctx);
}

/// DELETE /api/v1/servers/{id}
pub fn remove(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const servers = api.servers orelse return respond.unavailable(ctx, "servers");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    servers.remove(id) catch |err| return respond.failErr(ctx, err);
    return respond.noContent(ctx);
}

/// POST /api/v1/servers/{id}/{test,enable,disable}
pub fn postByPath(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const servers = api.servers orelse return respond.unavailable(ctx, "servers");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 2) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    if (seg.eq(1, "enable") or seg.eq(1, "disable")) {
        servers.setEnabled(id, seg.eq(1, "enable")) catch |err| return respond.failErr(ctx, err);
        return respond.noContent(ctx);
    }
    if (seg.eq(1, "test")) return testExisting(ctx, api, id);
    return respond.fail(ctx, 404, "not found");
}

/// POST /api/v1/servers/test — probe credentials that have not been
/// saved yet, which is what the "Test" button on the add-server form
/// posts to.
pub fn testUnsaved(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const probe = api.probe orelse return respond.unavailable(ctx, "connection test");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const host = body.trimmedString("host") orelse "";
    const port = body.int("port") orelse 0;
    if (host.len == 0 or port == 0) return respond.fail(ctx, 400, "host + port required");
    if (port < 1 or port > 65535) return respond.fail(ctx, 400, "port out of range");

    const result = try probe.probe(arena, .{
        .host = host,
        .port = @intCast(port),
        // TLS on unless the caller says otherwise: a probe that silently
        // fell back to plaintext would send the password in the clear.
        .tls = body.boolean("tls") orelse true,
        .username = body.string("username") orelse "",
        .password = body.string("password") orelse "",
    });

    var w = json.Writer.init(arena);
    try dto.probeResult(&w, result);
    return respond.ok(ctx, &w);
}

/// Probe a saved server with its stored credentials, so the per-row
/// Test button does not ask the operator to retype the password.
fn testExisting(ctx: *http.Ctx, api: *Api, id: ports.ServerId) Error!void {
    const servers = api.servers.?;
    const probe = api.probe orelse return respond.unavailable(ctx, "connection test");
    const arena = api.beginRequest();

    const srv = servers.get(arena, id) catch |err| return respond.failErr(ctx, err);
    const result = try probe.probe(arena, .{
        .host = srv.host,
        .port = srv.port,
        .tls = srv.tls,
        .username = srv.username,
        .password = srv.password,
    });

    var w = json.Writer.init(arena);
    try dto.probeResult(&w, result);
    return respond.ok(ctx, &w);
}

/// A JSON number into an `i32` field, saturating rather than wrapping.
/// The domain validates the range; what must not happen is 2^32+1
/// arriving as 1.
fn clampInt(v: ?i64) i32 {
    const n = v orelse return 0;
    return @intCast(std.math.clamp(n, std.math.minInt(i32), std.math.maxInt(i32)));
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "oversized integers saturate instead of wrapping" {
    try testing.expectEqual(@as(i32, 0), clampInt(null));
    try testing.expectEqual(@as(i32, 20), clampInt(20));
    try testing.expectEqual(@as(i32, std.math.maxInt(i32)), clampInt(4_294_967_297));
    try testing.expectEqual(@as(i32, std.math.minInt(i32)), clampInt(-4_294_967_297));
}
