//! `/api/v1/system/*` and `/api/v1/commands/*` — everything the System
//! page renders: status, throughput, health, scheduled tasks, disk
//! space, logs, backups and the command feed.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const logring = @import("../../core/logring.zig");
const sse = @import("../sse.zig");
const metrics_mod = @import("../metrics.zig");
const json = @import("json.zig");
const dto = @import("dto.zig");
const ports = @import("ports.zig");
const respond = @import("respond.zig");
const stream = @import("stream.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// `?range=` values and their span in seconds. The Go
/// `speedHistoryRanges` map, unchanged — the frontend's selector sends
/// exactly these five strings.
pub const speed_ranges = [_]struct { key: []const u8, seconds: i32 }{
    .{ .key = "5m", .seconds = 5 * 60 },
    .{ .key = "1h", .seconds = 60 * 60 },
    .{ .key = "6h", .seconds = 6 * 60 * 60 },
    .{ .key = "24h", .seconds = 24 * 60 * 60 },
    .{ .key = "7d", .seconds = 7 * 24 * 60 * 60 },
};

pub fn spanFor(range: []const u8) ?i32 {
    for (speed_ranges) |r| {
        if (std.mem.eql(u8, r.key, range)) return r.seconds;
    }
    return null;
}

/// Default and maximum page size for `/commands`. An operator-driven
/// feed does not need cursor pagination.
pub const commands_default_limit: i64 = 50;
pub const commands_max_limit: i64 = 200;

// ---------------------------------------------------------------------
// Status, throughput, speed history
// ---------------------------------------------------------------------

pub fn status(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const system = api.system orelse return respond.unavailable(ctx, "system status");
    const arena = api.beginRequest();

    const st = system.status(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.systemStatus(&w, st);
    return respond.ok(ctx, &w);
}

pub fn throughput(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const system = api.system orelse return respond.unavailable(ctx, "system status");
    const arena = api.beginRequest();

    const sample = system.throughput(arena, ports.default_sample_seconds) catch |err|
        return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.throughput(&w, sample, globalCap(api));
    return respond.ok(ctx, &w);
}

pub fn speedHistory(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const system = api.system orelse return respond.unavailable(ctx, "system status");
    const arena = api.beginRequest();

    const range = respond.query(ctx, "range") orelse "5m";
    const span = spanFor(range) orelse
        return respond.fail(ctx, 400, "range must be one of 5m, 1h, 6h, 24h, 7d");

    const h = system.speedHistory(arena, span) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.speedHistory(&w, range, h, globalCap(api));
    return respond.ok(ctx, &w);
}

fn globalCap(api: *Api) i64 {
    const bw = api.bandwidth orelse return 0;
    return bw.globalCap();
}

// ---------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------

pub fn health(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const h = api.health orelse return respond.unavailable(ctx, "health checks");
    const arena = api.beginRequest();

    const snap = h.snapshot(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.healthSnapshot(&w, snap);
    return respond.ok(ctx, &w);
}

/// POST /api/v1/system/health/refresh
///
/// Asks for a re-run and returns the snapshot *as it is now*. The Go
/// handler slept 200 ms first so the fresh result usually landed in the
/// same response; on a single reactor thread that would stall every
/// other connection for 200 ms, which is a far worse trade than the UI
/// re-polling once. 202 rather than 200 says the run is not finished.
pub fn healthRefresh(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const h = api.health orelse return respond.unavailable(ctx, "health checks");
    h.refresh();

    const arena = api.beginRequest();
    const snap = h.snapshot(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.healthSnapshot(&w, snap);
    return respond.accepted(ctx, &w);
}

// ---------------------------------------------------------------------
// Scheduled tasks
// ---------------------------------------------------------------------

pub fn tasks(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const schedule = api.schedule orelse return respond.unavailable(ctx, "scheduler");
    const arena = api.beginRequest();

    const all = schedule.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("tasks");
    try w.beginArray();
    for (all) |t| try dto.task(&w, t);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/system/tasks/{id}/run-now
///
/// Pulls the task's next run forward; the scheduler's next tick picks it
/// up. Running it inline would block the request on a task that may take
/// minutes and would bypass the claim that stops two runs overlapping.
pub fn taskRunNow(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const schedule = api.schedule orelse return respond.unavailable(ctx, "scheduler");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 2 or !seg.eq(1, "run-now")) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    schedule.runNow(id) catch |err| return respond.failErr(ctx, err);

    const arena = api.beginRequest();
    const t = schedule.byId(arena, id) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("task");
    try dto.task(&w, t);
    try w.endObject();
    return respond.ok(ctx, &w);
}

// ---------------------------------------------------------------------
// Disk space
// ---------------------------------------------------------------------

pub fn diskSpace(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const disk = api.disk orelse return respond.unavailable(ctx, "disk space");
    const arena = api.beginRequest();

    const entries = disk.snapshot(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.diskEntries(&w, entries);
    return respond.ok(ctx, &w);
}

// ---------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------

/// GET /api/v1/system/logs — the ring's contents, oldest first. The
/// System page loads this once and then opens the tail below.
pub fn logSnapshot(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const ring = api.log_ring orelse return respond.unavailable(ctx, "log ring");
    const arena = api.beginRequest();

    // One `Entry` is half a kilobyte of inline text, so the snapshot
    // buffer is the arena's, not the stack's.
    const buf = try arena.alloc(logring.Entry, ring.capacity());
    const n = ring.snapshot(buf);

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("entries");
    try w.beginArray();
    for (buf[0..n]) |*e| try sse.renderLogEntry(&w, e);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// GET /api/v1/system/logs/tail, and /stream as a back-compatible alias
/// — ad-blocking filter lists match "stream" often enough to break the
/// live view for people who never find out why.
pub fn logStream(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const hub = api.logs_hub orelse return respond.unavailable(ctx, "log stream");

    const token = stream.attach(api, ctx, hub) catch |err| switch (err) {
        error.TooManySubscribers => return respond.fail(ctx, 503, "too many open streams"),
        error.OutOfMemory => return error.OutOfMemory,
        else => return respond.fail(ctx, 500, "could not open stream"),
    };
    // The Go handler opened with `event: ready`; the client waits for it
    // before clearing its "connecting" state.
    hub.sendTo(token, "ready", "{}");
}

/// GET /api/v1/system/logs/files
pub fn logFiles(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const files = api.log_files orelse return respond.unavailable(ctx, "log files");
    const arena = api.beginRequest();

    const all = files.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.fileList(&w, "files", all, "updated_at", true);
    return respond.ok(ctx, &w);
}

/// GET /api/v1/system/logs/files/{name}
///
/// The name is *not* turned into a path here. It goes to the port, which
/// owns the directory and therefore owns the "is this one of ours"
/// decision — the same split `logfile.SafePath` enforced, and the reason
/// a traversal attempt is a 400 from the adapter rather than a read.
pub fn logFileDownload(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const files = api.log_files orelse return respond.unavailable(ctx, "log files");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");

    var buf: [256]u8 = undefined;
    const name = respond.decodeSegment(&buf, seg.get(0)) orelse
        return respond.fail(ctx, 400, "name is not valid");

    const arena = api.beginRequest();
    const body = files.read(arena, name) catch |err| switch (err) {
        error.Invalid => return respond.fail(ctx, 400, "name is not a log file"),
        else => return respond.failErr(ctx, err),
    };
    return respond.attachment(ctx, "text/plain; charset=utf-8", name, body);
}

// ---------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------

pub fn backups(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const b = api.backups orelse return respond.unavailable(ctx, "backups");
    const arena = api.beginRequest();

    const all = b.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.fileList(&w, "backups", all, "created_at", false);
    return respond.ok(ctx, &w);
}

/// POST /api/v1/system/backups — runs one now and answers with the new
/// list. `VACUUM INTO` on a hoardarr-sized database is sub-second, so
/// the operator gets immediate confirmation rather than a job to poll.
pub fn runBackup(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const b = api.backups orelse return respond.unavailable(ctx, "backups");
    b.run() catch |err| return respond.failErr(ctx, err);

    const arena = api.beginRequest();
    const all = b.list(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try dto.fileList(&w, "backups", all, "created_at", false);
    return respond.ok(ctx, &w);
}

/// GET /api/v1/system/backups/{name}
pub fn downloadBackup(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const b = api.backups orelse return respond.unavailable(ctx, "backups");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");

    var buf: [256]u8 = undefined;
    const name = respond.decodeSegment(&buf, seg.get(0)) orelse
        return respond.fail(ctx, 400, "name is not valid");

    const arena = api.beginRequest();
    const body = b.read(arena, name) catch |err| switch (err) {
        error.Invalid => return respond.fail(ctx, 400, "name is not a backup"),
        else => return respond.failErr(ctx, err),
    };
    return respond.attachment(ctx, "application/octet-stream", name, body);
}

// ---------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------

pub fn listCommands(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const commands = api.commands orelse return respond.unavailable(ctx, "commands");
    const arena = api.beginRequest();

    // An unparseable or out-of-range limit falls back to the default
    // rather than 400ing: this is a feed, not a query.
    var limit = commands_default_limit;
    if (respond.queryInt(ctx, "limit")) |n| {
        if (n > 0 and n <= commands_max_limit) limit = n;
    }

    const all = commands.list(arena, @intCast(limit)) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("commands");
    try w.beginArray();
    for (all) |c| try dto.command(&w, c, arena);
    try w.endArray();
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// POST /api/v1/commands — `{"name": "...", "body": {...}}`.
pub fn submitCommand(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const commands = api.commands orelse return respond.unavailable(ctx, "commands");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const name = body.trimmedString("name") orelse "";
    if (name.len == 0) return respond.fail(ctx, 400, "name required");
    // The command body is opaque here: re-encoded rather than
    // interpreted, and handed to the handler that understands it.
    const payload = (try body.rawJson(arena, "body")) orelse "";

    const id = commands.submit(name, payload) catch |err| switch (err) {
        error.NotFound, error.Invalid => return respond.fail(ctx, 400, "no such command"),
        else => return respond.failErr(ctx, err),
    };

    const c = commands.byId(arena, id) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("command");
    try dto.command(&w, c, arena);
    try w.endObject();
    return respond.accepted(ctx, &w);
}

/// GET /api/v1/commands/{id}
pub fn getCommand(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const commands = api.commands orelse return respond.unavailable(ctx, "commands");
    const seg = respond.segments(ctx.tail);
    if (seg.len != 1) return respond.fail(ctx, 404, "not found");
    const id = respond.parseId(seg.get(0)) orelse
        return respond.fail(ctx, 400, "id must be a number");

    const arena = api.beginRequest();
    const c = commands.byId(arena, id) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.key("command");
    try dto.command(&w, c, arena);
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// GET /api/v1/commands/names — populates the UI's trigger dropdown.
pub fn commandNames(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const commands = api.commands orelse return respond.unavailable(ctx, "commands");
    const arena = api.beginRequest();

    const names = commands.names(arena) catch |err| return respond.failErr(ctx, err);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.strArrayField("names", names);
    try w.endObject();
    return respond.ok(ctx, &w);
}

// ---------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------

/// GET /metrics — Prometheus scrape, behind the same API key as
/// everything else. Not public: the label values name the operator's
/// servers, and the counters describe their traffic.
pub fn prometheus(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const registry = api.metrics orelse return respond.unavailable(ctx, "metrics");
    const arena = api.beginRequest();

    const doc = try registry.render(arena);
    try ctx.res.send(200, metrics_mod.content_type, doc);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "the speed-history ranges are the Go map" {
    try testing.expectEqual(@as(i32, 300), spanFor("5m").?);
    try testing.expectEqual(@as(i32, 3600), spanFor("1h").?);
    try testing.expectEqual(@as(i32, 21600), spanFor("6h").?);
    try testing.expectEqual(@as(i32, 86400), spanFor("24h").?);
    try testing.expectEqual(@as(i32, 604800), spanFor("7d").?);

    try testing.expect(spanFor("") == null);
    try testing.expect(spanFor("30m") == null);
    try testing.expect(spanFor("5M") == null);
    try testing.expect(spanFor("7d ") == null);
}

test "the command feed's page sizes are the Go ones" {
    try testing.expectEqual(@as(i64, 50), commands_default_limit);
    try testing.expectEqual(@as(i64, 200), commands_max_limit);
}
