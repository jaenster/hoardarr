//! Domain aggregate → wire shape.
//!
//! This is the whole of hoardarr's first-party JSON contract, and the
//! direction is one-way on purpose: aggregates are projected out here,
//! and request bodies are decoded into commands in the handlers. Nothing
//! in this file reads a request.
//!
//! Field names, `omitempty` behaviour and timestamp formatting are the
//! Go `dto.go`'s, byte for byte, because `frontend/src/api/types.ts`
//! declares the other half of the contract and it is not being rewritten
//! with the backend. Where a name looks wrong — `error` as a member
//! name, `go_version` on build info, the capitalised `Envelope` keys —
//! it is because that is what is on the wire today.

const std = @import("std");
const json = @import("json.zig");
const ports = @import("ports.zig");
const download = @import("../../domain/download/job.zig");
const notify_domain = @import("../../domain/notify.zig");
const schedule_domain = @import("../../domain/schedule.zig");
const command_domain = @import("../../domain/command.zig");
const server_domain = @import("../../domain/server.zig");
const event = @import("../../domain/event.zig");

const Writer = json.Writer;
const Error = json.Error;

// ---------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------

/// One job. `segments` on each file are emitted only when the aggregate
/// carries them: the list endpoints hydrate jobs without files at all,
/// and the detail endpoint hydrates the lot. That is the same
/// distinction the Go DTO drew by leaving `Segments` nil.
pub fn job(w: *Writer, j: *const download.Job) Error!void {
    try w.beginObject();
    try w.intField("id", j.id);
    try w.strField("nzb_hash", j.nzb_hash);
    try w.strField("name", j.name);
    try w.strField("category", j.category);
    try w.intField("priority", j.priority);
    try w.strField("state", j.state.toString());
    try w.optStrField("source", j.source);
    try w.intField("total_bytes", j.total_bytes);
    try w.intField("done_bytes", j.done_bytes);
    try w.intField("failed_bytes", j.failed_bytes);
    try w.timeField("added_at", j.added_at);
    try w.optTimeField("started_at", j.started_at);
    try w.optTimeField("finished_at", j.finished_at);
    try w.optStrField("error", j.errorMsg());
    try w.key("files");
    try w.beginArray();
    for (j.files) |*f| try file(w, f);
    try w.endArray();
    try w.endObject();
}

pub fn file(w: *Writer, f: *const download.File) Error!void {
    try w.beginObject();
    try w.intField("id", f.id);
    try w.strField("filename", f.filename);
    try w.intField("size_bytes", f.size_bytes);
    try w.strField("state", f.state.toString());
    try w.intField("segment_count", f.segment_count);
    try w.intField("segments_done", f.segments_done);
    try w.boolField("is_par2", f.is_par2);
    try w.boolField("is_recovery_vol", f.is_recovery_vol);
    if (f.segments.len > 0) {
        try w.key("segments");
        try w.beginArray();
        for (f.segments) |*s| try segment(w, s);
        try w.endArray();
    }
    try w.endObject();
}

pub fn segment(w: *Writer, s: *const download.Segment) Error!void {
    try w.beginObject();
    try w.intField("id", s.id);
    try w.intField("seq_index", s.seq_index);
    try w.strField("message_id", s.message_id);
    try w.intField("bytes", s.bytes);
    try w.strField("state", s.state.toString());
    try w.intField("attempts", s.attempts);
    try w.optStrField("last_error", s.lastError());
    try w.intField("file_offset", s.file_offset);
    try w.endObject();
}

/// `{"jobs":[…]}` — the envelope both `/queue` and `/history` return.
pub fn jobList(w: *Writer, jobs: []const *download.Job) Error!void {
    try w.beginObject();
    try w.key("jobs");
    try w.beginArray();
    for (jobs) |j| try job(w, j);
    try w.endArray();
    try w.endObject();
}

// ---------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------

/// Passwords are absent by construction: there is no branch here that
/// could emit one, which is a stronger guarantee than remembering to
/// strip it.
pub fn usenetServer(w: *Writer, s: *const server_domain.UsenetServer) Error!void {
    try w.beginObject();
    try w.intField("id", s.id);
    try w.strField("name", s.name);
    try w.strField("host", s.host);
    try w.intField("port", s.port);
    try w.boolField("tls", s.tls);
    try w.optStrField("username", s.username);
    try w.intField("max_conns", s.max_conns);
    try w.intField("priority", s.priority);
    try w.boolField("enabled", s.enabled);
    try w.boolField("backup", s.backup);
    try w.strField("billing_mode", s.billing_mode.toString());
    try w.intField("quota_bytes", s.quota_bytes);
    try w.intField("used_bytes", s.used_bytes);
    try w.intField("bandwidth_bytes_per_sec", s.bandwidth_bytes_per_sec);
    try w.timeField("added_at", s.added_at);
    try w.timeField("updated_at", s.updated_at);
    try w.endObject();
}

pub fn probeResult(w: *Writer, r: ports.ProbeResult) Error!void {
    try w.beginObject();
    try w.boolField("ok", r.ok);
    try w.boolField("dial", r.dial);
    try w.boolField("greeted", r.greeted);
    try w.boolField("auth", r.auth);
    try w.boolField("mode_reader", r.mode_reader);
    try w.boolField("date", r.date);
    try w.optStrField("server_date", r.server_date);
    try w.optStrField("err", r.err);
    try w.intField("elapsed_ms", r.elapsed_ms);
    try w.endObject();
}

// ---------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------

pub fn category(w: *Writer, c: ports.Category) Error!void {
    try w.beginObject();
    try w.strField("name", c.name);
    try w.strField("dir", c.dir);
    try w.intField("priority", c.priority);
    try w.endObject();
}

// ---------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------

/// The secret is never echoed — only whether one is set. A "reveal
/// secret" flow, if it is ever wanted, gets its own endpoint that
/// returns it once, rather than every list response carrying it.
pub fn subscription(w: *Writer, s: *const notify_domain.Subscription) Error!void {
    try w.beginObject();
    try w.intField("id", s.id);
    try w.strField("name", s.name);
    try w.strField("kind", s.kind.toString());
    try w.strField("url", s.url);
    try w.strArrayField("topics", s.topics);
    try w.boolField("has_secret", s.secret.len > 0);
    try w.boolField("enabled", s.enabled);
    try w.optTimeField("last_success_at", s.last_success_at);
    try w.optTimeField("last_error_at", s.last_error_at);
    try w.optStrField("last_error", s.last_error);
    try w.timeField("created_at", s.created_at);
    try w.timeField("updated_at", s.updated_at);
    try w.endObject();
}

// ---------------------------------------------------------------------
// Scheduled tasks
// ---------------------------------------------------------------------

/// Cadence is emitted in seconds because the frontend formats it with
/// `humaniseDuration`; the domain stores milliseconds.
pub fn task(w: *Writer, t: *const schedule_domain.Task) Error!void {
    try w.beginObject();
    try w.intField("id", t.id);
    try w.strField("name", t.name);
    try w.strField("kind", t.kind.toString());
    try w.intField("cadence_seconds", @divTrunc(t.cadence_ms, std.time.ms_per_s));
    try w.timeField("next_run_at", t.next_run_at);
    try w.boolField("enabled", t.enabled);
    try w.strField("status", t.status.toString());
    try w.intField("consecutive_failures", t.consecutive_failures);
    try w.optTimeField("last_run_at", t.last_run_at);
    try w.optStrField("last_error", t.last_error);
    try w.optTimeField("claimed_at", t.claimed_at);
    try w.endObject();
}

// ---------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------

pub fn command(w: *Writer, c: *const command_domain.Command, scratch: std.mem.Allocator) Error!void {
    try w.beginObject();
    try w.intField("id", c.id);
    try w.strField("name", c.name);
    try w.strField("trigger", c.trigger.toString());
    try w.strField("status", c.status.toString());
    try w.timeField("queued_at", c.queued_at);
    try w.optTimeField("started_at", c.started_at);
    try w.optTimeField("ended_at", c.ended_at);
    if (c.result) |r| try w.strField("result", r.toString());
    try w.optStrField("error", c.err);
    if (c.started_at) |started| {
        if (c.ended_at) |ended| {
            if (ended > started) try w.intField("duration_ms", ended - started);
        }
    }
    if (c.body.len > 0) {
        try w.key("body");
        // The body is operator-supplied JSON that went through the
        // database. Splicing it unchecked would let one bad row break
        // the whole list response, so it is validated first and becomes
        // null if it is not JSON any more.
        try w.rawOrNull(c.body, scratch);
    }
    try w.endObject();
}

// ---------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------

/// The bus envelope, with the capitalised keys Go's default struct
/// marshalling produced. `frontend/src/api/types.ts` declares exactly
/// these, and the same shape goes down the SSE stream, so renaming them
/// to snake_case would break the live views in two places at once.
pub fn envelope(w: *Writer, e: event.Envelope, scratch: std.mem.Allocator) Error!void {
    var buf: event.Uuid.TextBuf = undefined;
    try w.beginObject();
    try w.strField("ID", e.id.writeText(&buf));
    try w.strField("Topic", e.topic);
    try w.strField("AggregateID", e.aggregate_id);
    try w.timeField("OccurredAt", e.occurred_at);
    try w.key("Payload");
    try w.rawOrNull(e.payload, scratch);
    try w.uintField("Attempts", e.attempts);
    try w.endObject();
}

// ---------------------------------------------------------------------
// System
// ---------------------------------------------------------------------

pub fn systemStatus(w: *Writer, st: ports.SystemStatus) Error!void {
    try w.beginObject();
    try w.strField("service", st.service);
    try w.strField("version", st.version);
    try w.strField("commit", st.commit);
    try w.strField("build_date", st.build_date);
    try w.strField("runtime_version", st.runtime_version);
    try w.strField("os", st.os);
    try w.strField("arch", st.arch);
    try w.boolField("is_docker", st.is_docker);
    try w.strField("database_type", st.database_type);
    try w.intField("migration_version", st.migration_version);
    try w.timeField("started_at", st.started_at_ms);
    try w.intField("uptime_ms", st.uptime_ms);
    try w.key("queue");
    try w.beginObject();
    try w.intField("active", st.queue_active);
    try w.intField("total", st.queue_total);
    try w.endObject();
    try w.key("pools");
    try w.beginArray();
    for (st.pools) |p| {
        try w.beginObject();
        try w.intField("server_id", p.server_id);
        try w.strField("server_name", p.server_name);
        try w.strField("host", p.host);
        try w.intField("port", p.port);
        try w.intField("max_conns", p.max_conns);
        try w.intField("in_use", p.in_use);
        try w.intField("idle", p.idle);
        try w.boolField("enabled", p.enabled);
        try w.boolField("backup", p.backup);
        try w.strField("billing_mode", p.billing_mode);
        try w.intField("quota_bytes", p.quota_bytes);
        try w.intField("used_bytes", p.used_bytes);
        try w.endObject();
    }
    try w.endArray();
    try w.endObject();
}

pub fn throughput(w: *Writer, s: ?ports.ThroughputSample, global_cap: i64) Error!void {
    try w.beginObject();
    if (s) |t| {
        try w.intField("window_seconds", t.window_seconds);
        try w.intArrayField("series", t.series);
        try w.intField("total_bytes", t.total_bytes);
        try w.intField("current_bytes_per_sec", t.current_bytes_per_sec);
        try w.intField("avg10s_bytes_per_sec", t.avg10s_bytes_per_sec);
        try w.intField("avg60s_bytes_per_sec", t.avg60s_bytes_per_sec);
        try w.intField("peak_window_bytes_per_sec", t.window_peak_bytes_per_sec);
        try w.intField("peak_alltime_bytes_per_sec", t.all_time_peak_bytes_per_sec);
    } else {
        // No tracker running. The Go handler answered zeroes rather than
        // an error, because the graph should render empty instead of the
        // page showing a failure.
        try w.intField("window_seconds", ports.default_sample_seconds);
        try w.intArrayField("series", &.{});
        try w.intField("total_bytes", 0);
        try w.intField("current_bytes_per_sec", 0);
        try w.intField("avg10s_bytes_per_sec", 0);
        try w.intField("avg60s_bytes_per_sec", 0);
        try w.intField("peak_window_bytes_per_sec", 0);
        try w.intField("peak_alltime_bytes_per_sec", 0);
    }
    try w.intField("global_cap_bytes_per_sec", global_cap);
    try w.endObject();
}

pub fn speedHistory(w: *Writer, range: []const u8, h: ports.SpeedHistory, global_cap: i64) Error!void {
    try w.beginObject();
    try w.strField("range", range);
    try w.intField("resolution_seconds", h.resolution_seconds);
    try w.key("samples");
    try w.beginArray();
    for (h.samples) |s| {
        try w.beginObject();
        try w.timeField("at", s.at_ms);
        try w.intField("bytes_per_sec", s.bytes_per_sec);
        try w.endObject();
    }
    try w.endArray();
    try w.intField("peak_window_bytes_per_sec", h.window_peak_bytes_per_sec);
    try w.intField("peak_alltime_bytes_per_sec", h.all_time_peak_bytes_per_sec);
    try w.intField("global_cap_bytes_per_sec", global_cap);
    try w.endObject();
}

/// Errors before warnings, so the UI can render them in priority order
/// without sorting. The Go handler did the same partition.
pub fn healthSnapshot(w: *Writer, snap: ports.HealthSnapshot) Error!void {
    try w.beginObject();
    try w.key("issues");
    try w.beginArray();
    for ([_]ports.Severity{ .err, .warning }) |want| {
        for (snap.issues) |i| {
            if (i.severity != want) continue;
            try w.beginObject();
            try w.strField("source", i.source);
            try w.strField("severity", i.severity.text());
            try w.strField("message", i.message);
            try w.optStrField("docs_url", i.docs_url);
            try w.endObject();
        }
    }
    try w.endArray();
    try w.timeField("last_run", snap.last_run_ms);
    try w.endObject();
}

pub fn diskEntries(w: *Writer, entries: []const ports.DiskEntry) Error!void {
    try w.beginObject();
    try w.key("entries");
    try w.beginArray();
    for (entries) |e| {
        try w.beginObject();
        try w.strField("label", e.label);
        try w.strField("path", e.path);
        try w.intField("free_bytes", e.free_bytes);
        try w.intField("total_bytes", e.total_bytes);
        try w.intField("used_bytes", e.used_bytes);
        try w.boolField("reachable", e.reachable);
        try w.optStrField("error", e.err);
        try w.endObject();
    }
    try w.endArray();
    try w.endObject();
}

/// Backups and log files share a shape but not their timestamp's name:
/// a backup has `created_at`, a log file `updated_at` plus `active`.
pub fn fileList(
    w: *Writer,
    key: []const u8,
    files: []const ports.FileInfo,
    time_key: []const u8,
    with_active: bool,
) Error!void {
    try w.beginObject();
    try w.key(key);
    try w.beginArray();
    for (files) |f| {
        try w.beginObject();
        try w.strField("name", f.name);
        try w.intField("size_bytes", f.size_bytes);
        try w.timeField(time_key, f.at_ms);
        if (with_active) try w.boolField("active", f.active);
        try w.endObject();
    }
    try w.endArray();
    try w.endObject();
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

fn parse(a: std.mem.Allocator, doc: []const u8) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, doc, .{});
}

test "a job with files and segments matches the Go DTO" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var j = try download.Job.hydrate(a, .{
        .id = 7,
        .nzb_hash = "abc123",
        .name = "Release.Name.S01E01.1080p",
        .category = "tv",
        .source = "Sonarr/4.0",
        .priority = 2,
        .state = .downloading,
        .total_bytes = 1000,
        .done_bytes = 400,
        .failed_bytes = 10,
        .added_at = 1_700_000_000_000,
        .started_at = 1_700_000_001_000,
        .files = &.{
            .{
                .id = 11,
                .filename = "release.part01.rar",
                .size_bytes = 500,
                .state = .downloading,
                .segment_count = 2,
                .segments_done = 1,
                .segments = &.{
                    .{ .id = 101, .seq_index = 1, .message_id = "a@b", .bytes = 250, .state = .done, .attempts = 1 },
                    .{ .id = 102, .seq_index = 2, .message_id = "c@d", .bytes = 250, .state = .failed, .attempts = 3, .last_error = "430 no such article" },
                },
            },
            .{ .id = 12, .filename = "release.vol00+01.par2", .size_bytes = 100, .is_par2 = true, .is_recovery_vol = true, .segment_count = 1 },
        },
    });
    defer j.deinit();

    var w = json.Writer.init(a);
    try job(&w, &j);
    const v = try parse(a, w.items());

    try testing.expectEqual(@as(i64, 7), v.object.get("id").?.integer);
    try testing.expectEqualStrings("abc123", v.object.get("nzb_hash").?.string);
    try testing.expectEqualStrings("downloading", v.object.get("state").?.string);
    try testing.expectEqualStrings("Sonarr/4.0", v.object.get("source").?.string);
    try testing.expectEqual(@as(i64, 400), v.object.get("done_bytes").?.integer);
    try testing.expectEqualStrings("2023-11-14T22:13:20.000Z", v.object.get("added_at").?.string);
    try testing.expectEqualStrings("2023-11-14T22:13:21.000Z", v.object.get("started_at").?.string);
    // Absent, not null: no finish, no error.
    try testing.expect(v.object.get("finished_at") == null);
    try testing.expect(v.object.get("error") == null);

    const files = v.object.get("files").?.array.items;
    try testing.expectEqual(@as(usize, 2), files.len);
    try testing.expectEqualStrings("release.part01.rar", files[0].object.get("filename").?.string);
    try testing.expectEqual(false, files[0].object.get("is_par2").?.bool);
    try testing.expectEqual(true, files[1].object.get("is_recovery_vol").?.bool);

    const segs = files[0].object.get("segments").?.array.items;
    try testing.expectEqual(@as(usize, 2), segs.len);
    try testing.expectEqualStrings("a@b", segs[0].object.get("message_id").?.string);
    try testing.expect(segs[0].object.get("last_error") == null);
    try testing.expectEqualStrings("430 no such article", segs[1].object.get("last_error").?.string);
    try testing.expectEqual(@as(i64, 3), segs[1].object.get("attempts").?.integer);

    // A file with no hydrated segments omits the key entirely, which is
    // what keeps the list endpoints' payload bounded.
    try testing.expect(files[1].object.get("segments") == null);
}

test "a job name straight off the internet cannot break the document" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Release names come from NZBs written by strangers.
    var j = try download.Job.hydrate(a, .{
        .id = 1,
        .nzb_hash = "h",
        .name = "Bad\"Name\\\n\x00\xff</script>",
        .category = "\",\"admin\":true",
        .state = .failed,
        .error_msg = "430 \"no such article\"\r\n",
        .files = &.{},
    });
    defer j.deinit();

    var w = json.Writer.init(a);
    try job(&w, &j);
    const v = try parse(a, w.items());
    try testing.expect(v.object.get("admin") == null);
    try testing.expectEqualStrings("\",\"admin\":true", v.object.get("category").?.string);
    try testing.expectEqualStrings("430 \"no such article\"\r\n", v.object.get("error").?.string);
}

test "a job list is an object with a jobs array, even when empty" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = json.Writer.init(a);
    try jobList(&w, &.{});
    try testing.expectEqualStrings("{\"jobs\":[]}", w.items());
}

test "a server DTO never carries the password" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var s = try server_domain.UsenetServer.hydrate(a, .{
        .id = 3,
        .name = "eweka",
        .host = "news.eweka.nl",
        .port = 563,
        .tls = true,
        .username = "user",
        .password = "hunter2-super-secret",
        .max_conns = 20,
        .priority = 1,
        .enabled = true,
        .billing_mode = .metered,
        .quota_bytes = 1024,
        .used_bytes = 512,
        .bandwidth_bytes_per_sec = 100,
        .added_at = 1_700_000_000_000,
        .updated_at = 1_700_000_000_000,
    });
    defer s.deinit();

    var w = json.Writer.init(a);
    try usenetServer(&w, &s);
    try testing.expect(std.mem.indexOf(u8, w.items(), "hunter2") == null);
    try testing.expect(std.mem.indexOf(u8, w.items(), "password") == null);

    const v = try parse(a, w.items());
    try testing.expectEqualStrings("eweka", v.object.get("name").?.string);
    try testing.expectEqualStrings("user", v.object.get("username").?.string);
    try testing.expectEqual(@as(i64, 563), v.object.get("port").?.integer);
    try testing.expectEqualStrings("metered", v.object.get("billing_mode").?.string);
    try testing.expectEqual(@as(i64, 512), v.object.get("used_bytes").?.integer);
}

test "a subscription DTO reports that a secret exists but never what it is" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var s = try notify_domain.Subscription.hydrate(a, .{
        .id = 5,
        .name = "discord",
        .kind = .discord,
        .url = "https://discord.com/api/webhooks/1/token-that-is-the-credential",
        .topics = &.{ "download.job.completed", "download.job.failed" },
        .secret = "hmac-key",
        .enabled = true,
        .last_success_at = 1_700_000_000_000,
        .last_error = "",
        .created_at = 1_699_000_000_000,
        .updated_at = 1_700_000_000_000,
    });
    defer s.deinit();

    var w = json.Writer.init(a);
    try subscription(&w, &s);
    try testing.expect(std.mem.indexOf(u8, w.items(), "hmac-key") == null);

    const v = try parse(a, w.items());
    try testing.expectEqual(true, v.object.get("has_secret").?.bool);
    try testing.expectEqual(@as(usize, 2), v.object.get("topics").?.array.items.len);
    try testing.expect(v.object.get("last_error_at") == null);
    try testing.expect(v.object.get("last_error") == null);
    try testing.expectEqualStrings("discord", v.object.get("kind").?.string);
}

test "a task DTO reports cadence in seconds" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var t = try schedule_domain.Task.hydrate(a, .{
        .id = 2,
        .name = "backup",
        .kind = .recurring,
        .cadence_ms = 6 * 60 * 60 * 1000,
        .next_run_at = 1_700_000_000_000,
        .last_run_at = 1_699_978_400_000,
        .last_error = "disk full",
        .consecutive_failures = 2,
        .enabled = true,
        .status = .running,
        .claimed_at = 1_699_999_999_000,
        .created_at = 1,
        .updated_at = 2,
    });
    defer t.deinit();

    var w = json.Writer.init(a);
    try task(&w, &t);
    const v = try parse(a, w.items());
    try testing.expectEqual(@as(i64, 21600), v.object.get("cadence_seconds").?.integer);
    try testing.expectEqualStrings("recurring", v.object.get("kind").?.string);
    try testing.expectEqualStrings("running", v.object.get("status").?.string);
    try testing.expectEqualStrings("disk full", v.object.get("last_error").?.string);
    try testing.expectEqual(@as(i64, 2), v.object.get("consecutive_failures").?.integer);
}

test "a command DTO carries a duration only once it has ended" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var running = try command_domain.Command.hydrate(a, .{
        .id = 1,
        .name = "RefreshHealth",
        .body = "{\"deep\":true}",
        .trigger = .manual,
        .status = .running,
        .queued_at = 1_700_000_000_000,
        .started_at = 1_700_000_001_000,
    });
    defer running.deinit();

    var w = json.Writer.init(a);
    try command(&w, &running, a);
    var v = try parse(a, w.items());
    try testing.expect(v.object.get("duration_ms") == null);
    try testing.expect(v.object.get("result") == null);
    try testing.expectEqual(true, v.object.get("body").?.object.get("deep").?.bool);

    var done = try command_domain.Command.hydrate(a, .{
        .id = 2,
        .name = "Backup",
        .body = "not json any more",
        .trigger = .scheduled,
        .status = .completed,
        .result = .failed,
        .err = "vacuum failed",
        .queued_at = 1_700_000_000_000,
        .started_at = 1_700_000_001_000,
        .ended_at = 1_700_000_004_500,
    });
    defer done.deinit();

    var w2 = json.Writer.init(a);
    try command(&w2, &done, a);
    v = try parse(a, w2.items());
    try testing.expectEqual(@as(i64, 3500), v.object.get("duration_ms").?.integer);
    try testing.expectEqualStrings("failed", v.object.get("result").?.string);
    try testing.expectEqualStrings("vacuum failed", v.object.get("error").?.string);
    // A body that is no longer JSON becomes null rather than corrupting
    // the response around it.
    try testing.expect(v.object.get("body").? == .null);
}

test "the envelope keeps the capitalised keys the frontend declares" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const e = event.Envelope{
        .id = event.Uuid.v7(1_700_000_000_000, @splat(0xAB)),
        .topic = "download.job.completed",
        .aggregate_id = "42",
        .occurred_at = 1_700_000_000_000,
        .payload = "{\"job_id\":42,\"name\":\"x\"}",
        .attempts = 2,
    };

    var w = json.Writer.init(a);
    try envelope(&w, e, a);
    const v = try parse(a, w.items());
    try testing.expectEqualStrings("download.job.completed", v.object.get("Topic").?.string);
    try testing.expectEqualStrings("42", v.object.get("AggregateID").?.string);
    try testing.expectEqualStrings("2023-11-14T22:13:20.000Z", v.object.get("OccurredAt").?.string);
    try testing.expectEqual(@as(i64, 42), v.object.get("Payload").?.object.get("job_id").?.integer);
    try testing.expectEqual(@as(i64, 2), v.object.get("Attempts").?.integer);
    try testing.expectEqual(@as(usize, 36), v.object.get("ID").?.string.len);
}

test "health issues come out errors first" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = json.Writer.init(a);
    try healthSnapshot(&w, .{
        .issues = &.{
            .{ .source = "disk", .severity = .warning, .message = "less than 10 GB free" },
            .{ .source = "servers", .severity = .err, .message = "no enabled server", .docs_url = "https://example/docs" },
            .{ .source = "quota", .severity = .warning, .message = "quota nearly used" },
        },
        .last_run_ms = 1_700_000_000_000,
    });
    const v = try parse(a, w.items());
    const issues = v.object.get("issues").?.array.items;
    try testing.expectEqual(@as(usize, 3), issues.len);
    try testing.expectEqualStrings("error", issues[0].object.get("severity").?.string);
    try testing.expectEqualStrings("servers", issues[0].object.get("source").?.string);
    try testing.expectEqualStrings("https://example/docs", issues[0].object.get("docs_url").?.string);
    try testing.expectEqualStrings("warning", issues[1].object.get("severity").?.string);
    try testing.expect(issues[1].object.get("docs_url") == null);
}

test "throughput with no tracker is zeroes, not an error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = json.Writer.init(a);
    try throughput(&w, null, 1_048_576);
    const v = try parse(a, w.items());
    try testing.expectEqual(@as(i64, 300), v.object.get("window_seconds").?.integer);
    try testing.expectEqual(@as(usize, 0), v.object.get("series").?.array.items.len);
    try testing.expectEqual(@as(i64, 0), v.object.get("current_bytes_per_sec").?.integer);
    try testing.expectEqual(@as(i64, 1_048_576), v.object.get("global_cap_bytes_per_sec").?.integer);

    var w2 = json.Writer.init(a);
    try throughput(&w2, .{
        .window_seconds = 3,
        .series = &.{ 10, 20, 30 },
        .total_bytes = 60,
        .current_bytes_per_sec = 30,
        .avg10s_bytes_per_sec = 20,
        .avg60s_bytes_per_sec = 20,
        .window_peak_bytes_per_sec = 30,
        .all_time_peak_bytes_per_sec = 99,
    }, 0);
    const v2 = try parse(a, w2.items());
    try testing.expectEqual(@as(i64, 3), v2.object.get("window_seconds").?.integer);
    try testing.expectEqual(@as(i64, 30), v2.object.get("series").?.array.items[2].integer);
    try testing.expectEqual(@as(i64, 99), v2.object.get("peak_alltime_bytes_per_sec").?.integer);
}

test "system status nests queue and pools" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = json.Writer.init(a);
    try systemStatus(&w, .{
        .version = "0.2.0",
        .commit = "deadbeef",
        .runtime_version = "zig 0.16.0",
        .os = "linux",
        .arch = "aarch64",
        .is_docker = true,
        .migration_version = 12,
        .started_at_ms = 1_700_000_000_000,
        .uptime_ms = 3_600_000,
        .queue_active = 2,
        .queue_total = 9,
        .pools = &.{
            .{ .server_id = 1, .server_name = "eweka", .host = "news.eweka.nl", .port = 563, .max_conns = 20, .in_use = 4, .idle = 16, .enabled = true, .billing_mode = "flat" },
        },
    });
    const v = try parse(a, w.items());
    try testing.expectEqualStrings("hoardarr", v.object.get("service").?.string);
    try testing.expectEqual(@as(i64, 2), v.object.get("queue").?.object.get("active").?.integer);
    try testing.expectEqual(@as(i64, 9), v.object.get("queue").?.object.get("total").?.integer);
    const pools = v.object.get("pools").?.array.items;
    try testing.expectEqual(@as(i64, 4), pools[0].object.get("in_use").?.integer);
    try testing.expectEqualStrings("news.eweka.nl", pools[0].object.get("host").?.string);
}

test "file lists name their timestamp per endpoint" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = json.Writer.init(a);
    try fileList(&w, "backups", &.{
        .{ .name = "hoardarr-20231114.db", .size_bytes = 4096, .at_ms = 1_700_000_000_000 },
    }, "created_at", false);
    var v = try parse(a, w.items());
    try testing.expect(v.object.get("backups").?.array.items[0].object.get("created_at") != null);
    try testing.expect(v.object.get("backups").?.array.items[0].object.get("active") == null);

    var w2 = json.Writer.init(a);
    try fileList(&w2, "files", &.{
        .{ .name = "hoardarr.log", .size_bytes = 10, .at_ms = 1_700_000_000_000, .active = true },
    }, "updated_at", true);
    v = try parse(a, w2.items());
    const f = v.object.get("files").?.array.items[0].object;
    try testing.expect(f.get("updated_at") != null);
    try testing.expectEqual(true, f.get("active").?.bool);
}
