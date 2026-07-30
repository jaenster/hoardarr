//! `/api/v1/queue`, `/api/v1/history` and the live event stream.
//!
//! The list endpoints return jobs *without* files or segments. That is
//! not a shortcut: the wire DTO for a list never emitted per-file rows,
//! and hydrating them meant one query per job on an endpoint that Sonarr
//! and Radarr poll every few seconds. The detail endpoint
//! (`/queue/{id}`) is the one that hydrates everything, and it is opened
//! by a human looking at one job.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const json = @import("json.zig");
const dto = @import("dto.zig");
const ports = @import("ports.zig");
const respond = @import("respond.zig");
const multipart = @import("multipart.zig");
const stream = @import("stream.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// `?limit=` on `/history`, clamped server-side. The Go handler passed
/// the value through to the repository, which clamped it; doing it here
/// too means a client asking for 100000 gets 500 rows rather than a
/// query that scans the table.
pub const history_limit_max: i64 = 500;

// ---------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------

/// GET /api/v1/queue — active jobs, or everything with `?include=all`.
pub fn list(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const arena = api.beginRequest();

    const include_all = if (respond.query(ctx, "include")) |v|
        std.mem.eql(u8, v, "all")
    else
        false;

    const jobs = (if (include_all)
        queue.listAll(arena)
    else
        queue.listActive(arena)) catch |err| return respond.failErr(ctx, err);

    var w = json.Writer.init(arena);
    try dto.jobList(&w, jobs);
    return respond.ok(ctx, &w);
}

/// GET /api/v1/history — terminal-state jobs, newest first.
///
///     ?since=<RFC3339>   only jobs finished after this instant
///     ?category=<name>   exact match
///     ?state=<terminal>  completed | failed | aborted
///     ?limit=<n>         clamped to [1, 500]
pub fn history(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const arena = api.beginRequest();

    var q: ports.HistoryQuery = .{};

    var cat_buf: [128]u8 = undefined;
    if (respond.queryDecoded(&cat_buf, ctx, "category")) |c| q.category = c;

    if (respond.query(ctx, "state")) |s| {
        // An unknown state is a 400 rather than an empty list: silently
        // returning nothing for a typo is how a UI bug hides.
        q.state = ports.JobState.parse(s) orelse
            return respond.fail(ctx, 400, "state is not a known job state");
    }

    q.since_ms = respond.queryTimeMs(ctx, "since") catch
        return respond.fail(ctx, 400, "since must be an RFC 3339 timestamp");

    if (respond.query(ctx, "limit")) |raw| {
        const n = std.fmt.parseInt(i64, raw, 10) catch
            return respond.fail(ctx, 400, "limit must be a number");
        q.limit = @intCast(std.math.clamp(n, 1, history_limit_max));
    }

    const jobs = queue.history(arena, q) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.jobList(&w, jobs);
    return respond.ok(ctx, &w);
}

/// POST /api/v1/queue/nzb — `multipart/form-data` with an `nzb` file
/// part and an optional `category` field.
pub fn addNzb(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const arena = api.beginRequest();

    const content_type = ctx.req.header("content-type") orelse "";
    const boundary = multipart.boundary(content_type) orelse
        return respond.fail(ctx, 400, "expected multipart/form-data with a boundary");

    const part = multipart.field(ctx.req.body, boundary, "nzb") orelse
        return respond.fail(ctx, 400, "nzb file required");
    if (part.body.len == 0) return respond.fail(ctx, 400, "nzb file is empty");

    // Display name is the uploaded filename minus its extension.
    // Operators upload "Release.Name.S01E01.1080p.WEB-DL.nzb" and expect
    // to see that in the queue, not an id out of the NZB's file list.
    const name = trimNzbSuffix(baseName(part.filename));

    const result = queue.add(arena, .{
        .nzb = part.body,
        .name = name,
        .category = multipart.value(ctx.req.body, boundary, "category"),
        .source = ctx.req.header("user-agent") orelse "",
    }) catch |err| return respond.failErr(ctx, err);

    var w = json.Writer.init(arena);
    if (result.duplicate) {
        // 200, not a 4xx: Sonarr re-posting a release it already sent is
        // routine, and an error status makes it retry forever.
        try w.beginObject();
        try w.intField("job_id", result.id);
        try w.boolField("duplicate", true);
        try w.optStrField("state", result.state);
        try w.optStrField("name", result.name);
        try w.endObject();
        return respond.ok(ctx, &w);
    }

    try w.beginObject();
    try w.intField("job_id", result.id);
    try w.endObject();
    return respond.created(ctx, &w);
}

/// POST /api/v1/queue/reorder — `{"ids": [...]}`, first id first.
pub fn reorder(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const arena = api.beginRequest();

    const body = try respond.bodyObject(ctx, arena) orelse return;
    const ids = (try body.intArray(arena, "ids")) orelse
        return respond.fail(ctx, 400, "ids must be an array of job ids");
    // An empty list is a no-op, not an error — the UI sends one when a
    // drag ends where it started.
    if (ids.len == 0) return respond.noContent(ctx);

    queue.reorder(ids) catch |err| return respond.failErr(ctx, err);
    return respond.noContent(ctx);
}

/// GET /api/v1/queue/{id} and /api/v1/queue/{id}/events.
///
/// One handler for both because the router matches a prefix and the tail
/// is what distinguishes them. `/queue/stream` is an exact route and
/// wins over this one before it is ever reached.
pub fn getByPath(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const seg = respond.segments(ctx.tail);
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    if (seg.len == 2 and seg.eq(1, "events")) return jobEvents(ctx, api, id);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");

    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const arena = api.beginRequest();
    const job = queue.get(arena, id) catch |err| return respond.failErr(ctx, err);

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("job");
    try dto.job(&w, job);
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/queue/{id}/pause and .../resume.
pub fn postByPath(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const seg = respond.segments(ctx.tail);
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");
    if (seg.len != 2) return respond.fail(ctx, 404, "not found");

    if (seg.eq(1, "pause")) {
        queue.pause(id) catch |err| return respond.failErr(ctx, err);
        return respond.noContent(ctx);
    }
    if (seg.eq(1, "resume")) {
        queue.unpause(id) catch |err| return respond.failErr(ctx, err);
        return respond.noContent(ctx);
    }
    return respond.fail(ctx, 404, "not found");
}

/// DELETE /api/v1/queue/{id}.
pub fn deleteByPath(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const queue = api.queue orelse return respond.unavailable(ctx, "queue");
    const seg = respond.segments(ctx.tail);
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");

    queue.remove(id) catch |err| return respond.failErr(ctx, err);
    return respond.noContent(ctx);
}

/// GET /api/v1/queue/{id}/events — the per-job timeline.
fn jobEvents(ctx: *http.Ctx, api: *Api, id: ports.JobId) Error!void {
    const events = api.events orelse return respond.unavailable(ctx, "event history");
    const arena = api.beginRequest();
    const list_ = events.byJob(arena, id) catch |err| return respond.failErr(ctx, err);

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("events");
    try w.beginArray();
    for (list_) |e| try dto.envelope(&w, e, arena);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

// ---------------------------------------------------------------------
// Live stream
// ---------------------------------------------------------------------

/// GET /api/v1/queue/stream and GET /api/v1/events.
///
/// Both paths are the same handler, as they were in Go: the second name
/// exists because ad-blocking filter lists match "stream" often enough
/// to break the queue view for people who never find out why.
///
/// Protected like everything else. `EventSource` cannot set headers, so
/// the browser reaches this with the session cookie and an *arr client
/// with `?apikey=` — both of which the HTTP layer already accepts.
pub fn eventStream(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const hub = api.events_hub orelse return respond.unavailable(ctx, "event stream");

    const token = stream.attach(api, ctx, hub) catch |err| switch (err) {
        error.TooManySubscribers => return respond.fail(ctx, 503, "too many open streams"),
        error.OutOfMemory => return error.OutOfMemory,
        else => return respond.fail(ctx, 500, "could not open stream"),
    };
    // Greeting, so the client knows it is connected before the first
    // event — which on an idle queue could be minutes away.
    hub.sendHello(token);
}

// ---------------------------------------------------------------------
// Filenames
// ---------------------------------------------------------------------

/// The last path component. A browser sends a bare filename, but a
/// hand-rolled client may send a whole path, and the display name should
/// not contain one. Both separators are handled: a Windows client sends
/// backslashes.
pub fn baseName(path: []const u8) []const u8 {
    var out = path;
    if (std.mem.lastIndexOfAny(u8, out, "/\\")) |i| out = out[i + 1 ..];
    return out;
}

/// Strip a trailing `.nzb`, case-insensitively. Go did it twice, for
/// `.nzb` and `.NZB`; one case-insensitive compare covers `.Nzb` too.
pub fn trimNzbSuffix(name: []const u8) []const u8 {
    if (name.len <= 4) return name;
    const tail = name[name.len - 4 ..];
    if (std.ascii.eqlIgnoreCase(tail, ".nzb")) return name[0 .. name.len - 4];
    return name;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "the display name is the filename without its path or extension" {
    try testing.expectEqualStrings("Release.Name.S01E01", trimNzbSuffix(baseName("Release.Name.S01E01.nzb")));
    try testing.expectEqualStrings("Release", trimNzbSuffix(baseName("Release.NZB")));
    try testing.expectEqualStrings("Release", trimNzbSuffix(baseName("Release.Nzb")));
    try testing.expectEqualStrings("Release", trimNzbSuffix(baseName("/tmp/downloads/Release.nzb")));
    try testing.expectEqualStrings("Release", trimNzbSuffix(baseName("C:\\Users\\me\\Release.nzb")));
    // Not an NZB suffix, or nothing left after stripping: unchanged.
    try testing.expectEqualStrings("Release.rar", trimNzbSuffix(baseName("Release.rar")));
    try testing.expectEqualStrings(".nzb", trimNzbSuffix(".nzb"));
    try testing.expectEqualStrings("", trimNzbSuffix(baseName("")));
    try testing.expectEqualStrings("", trimNzbSuffix(baseName("/")));
    // A traversal attempt is reduced to its last component here; nothing
    // downstream builds a path from it either.
    try testing.expectEqualStrings("passwd", trimNzbSuffix(baseName("../../etc/passwd")));
}

test "the history limit is clamped to the Go range" {
    try testing.expectEqual(@as(i64, 500), history_limit_max);
    try testing.expectEqual(@as(i64, 1), std.math.clamp(@as(i64, 0), 1, history_limit_max));
    try testing.expectEqual(@as(i64, 1), std.math.clamp(@as(i64, -5), 1, history_limit_max));
    try testing.expectEqual(@as(i64, 500), std.math.clamp(@as(i64, 100000), 1, history_limit_max));
    try testing.expectEqual(@as(i64, 50), std.math.clamp(@as(i64, 50), 1, history_limit_max));
}
