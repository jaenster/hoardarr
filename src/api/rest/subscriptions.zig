//! `/api/v1/subscriptions` — outbound webhooks and chat notifications.
//!
//! A subscription URL *is* a credential for Discord and Slack: anyone
//! holding `discord.com/api/webhooks/<id>/<token>` can post as that
//! integration. The URL is still returned in the list, because the
//! Settings UI has to show which webhook a row is — but nothing here
//! logs one, and the HMAC secret is never echoed at all.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const json = @import("json.zig");
const dto = @import("dto.zig");
const ports = @import("ports.zig");
const respond = @import("respond.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// GET /api/v1/subscriptions
pub fn list(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const subs = api.subscriptions orelse return respond.unavailable(ctx, "subscriptions");
    const arena = api.beginRequest();

    const all = subs.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("subscriptions");
    try w.beginArray();
    for (all) |s| try dto.subscription(&w, s);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/subscriptions
pub fn add(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const subs = api.subscriptions orelse return respond.unavailable(ctx, "subscriptions");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const name = body.trimmedString("name") orelse "";
    const url = body.trimmedString("url") orelse "";
    if (name.len == 0) return respond.fail(ctx, 400, "name required");
    if (url.len == 0) return respond.fail(ctx, 400, "url required");

    const topics = (try body.stringArray(arena, "topics")) orelse &[_][]const u8{};

    const id = subs.add(.{
        .name = name,
        // Empty means webhook; the domain owns the vocabulary and
        // rejects anything it does not know.
        .kind = body.trimmedString("kind") orelse "",
        .url = url,
        .topics = topics,
        .secret = body.string("secret") orelse "",
    }) catch |err| switch (err) {
        error.Conflict => return respond.fail(ctx, 409, "a subscription with that name already exists"),
        error.Invalid => return respond.fail(ctx, 400, "subscription is not valid"),
        else => return respond.failErr(ctx, err),
    };

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.intField("id", id);
    try w.endObject();
    return respond.created(ctx, &w);
}

/// PATCH /api/v1/subscriptions/{id}
pub fn patch(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const subs = api.subscriptions orelse return respond.unavailable(ctx, "subscriptions");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    var cmd: ports.UpdateSubscriptionCmd = .{};
    if (body.has("url")) cmd.url = body.trimmedString("url");
    if (body.has("topics")) {
        cmd.topics = (try body.stringArray(arena, "topics")) orelse
            return respond.fail(ctx, 400, "topics must be an array of strings");
    }
    if (body.has("secret")) cmd.secret = body.string("secret");
    if (body.has("enabled")) cmd.enabled = body.boolean("enabled");

    subs.update(id, cmd) catch |err| switch (err) {
        error.Invalid => return respond.fail(ctx, 400, "subscription is not valid"),
        else => return respond.failErr(ctx, err),
    };
    return respond.noContent(ctx);
}

/// DELETE /api/v1/subscriptions/{id}
pub fn remove(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const subs = api.subscriptions orelse return respond.unavailable(ctx, "subscriptions");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    subs.remove(id) catch |err| return respond.failErr(ctx, err);
    return respond.noContent(ctx);
}

/// POST /api/v1/subscriptions/{id}/{test,enable,disable}
pub fn postByPath(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const subs = api.subscriptions orelse return respond.unavailable(ctx, "subscriptions");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 2) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    if (seg.eq(1, "enable") or seg.eq(1, "disable")) {
        subs.setEnabled(id, seg.eq(1, "enable")) catch |err| return respond.failErr(ctx, err);
        return respond.noContent(ctx);
    }
    if (seg.eq(1, "test")) {
        // A subscriber that answers badly is a 502: we are the proxy
        // here, and the failure is theirs, not the caller's.
        subs.sendTest(id) catch |err| switch (err) {
            error.NotFound => return respond.fail(ctx, 404, "not found"),
            else => return respond.fail(ctx, 502, "test delivery failed"),
        };
        return respond.noContent(ctx);
    }
    return respond.fail(ctx, 404, "not found");
}
