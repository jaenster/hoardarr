//! `/api/v1/categories` — the category → directory mapping the *arr
//! clients select with `?cat=`.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const json = @import("json.zig");
const dto = @import("dto.zig");
const ports = @import("ports.zig");
const respond = @import("respond.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// GET /api/v1/categories
pub fn list(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cats = api.categories orelse return respond.unavailable(ctx, "categories");
    const arena = api.beginRequest();

    const all = cats.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("categories");
    try w.beginArray();
    for (all) |c| try dto.category(&w, c);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/categories — insert or update by name, and return the
/// stored shape so the UI can render the row it just saved.
pub fn upsert(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cats = api.categories orelse return respond.unavailable(ctx, "categories");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const c: ports.Category = .{
        .name = body.trimmedString("name") orelse "",
        .dir = body.trimmedString("dir") orelse "",
        .priority = @as(i32, @intCast(std.math.clamp(
            body.int("priority") orelse 0,
            std.math.minInt(i32),
            std.math.maxInt(i32),
        ))),
    };
    if (c.name.len == 0) return respond.fail(ctx, 400, "name required");

    cats.save(c) catch |err| switch (err) {
        error.Invalid => return respond.fail(ctx, 400, "category name is not valid"),
        else => return respond.failErr(ctx, err),
    };

    var w = json.Writer.init(arena);
    try dto.category(&w, c);
    return respond.ok(ctx, &w);
}

/// DELETE /api/v1/categories/{name}
///
/// The name is a path segment, so it arrives percent-encoded — a
/// category may legitimately contain a space, and could contain a slash
/// that must stay `%2F` until it is decoded here rather than being
/// mistaken for a path separator by the router.
pub fn remove(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cats = api.categories orelse return respond.unavailable(ctx, "categories");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");

    var buf: [256]u8 = undefined;
    const name = respond.decodeSegment(&buf, seg.get(0)) orelse
        return respond.fail(ctx, 400, "name is not valid");
    if (name.len == 0) return respond.fail(ctx, 400, "name required");

    cats.delete(name) catch |err| switch (err) {
        error.Forbidden => return respond.fail(ctx, 403, "that category cannot be removed"),
        else => return respond.failErr(ctx, err),
    };
    return respond.noContent(ctx);
}
