//! The SABnzbd JSON projections.
//!
//! Everything about these payloads is a compatibility constraint, not a
//! design choice. Sonarr, Radarr, Lidarr, Readarr and Prowlarr parse them,
//! and they parse them the way they parse real SAB v3.7.x — which means:
//!
//!   * **Numbers arrive as strings.** `mb`, `mbleft`, `percentage`,
//!     `kbpersec`, `speedlimit`, `unpackopts` are all quoted. `bytes`,
//!     `missing`, `time_added`, `completed`, `noofslots` are not. Flipping
//!     either direction breaks a consumer silently.
//!   * **`priority` is a name, not a number.** See `priorityToSab`.
//!   * **Key order is Go's.** The Go handler built these out of
//!     `map[string]any`, and `encoding/json` sorts map keys. So the field
//!     order below is alphabetical, not logical, and the tests pin the
//!     exact bytes of a representative payload against output captured
//!     from the Go implementation.
//!   * **A trailing newline.** `json.NewEncoder(w).Encode` writes one
//!     after the document. Reproduced by the top-level writers.
//!
//! ## One deliberate divergence
//!
//! Go's `encoding/json` escapes `<`, `>`, `&`, U+2028 and U+2029 even
//! inside strings (its HTML-safety default). We do not: string escaping
//! here is `app/notify/render.zig`'s `writeJsonString`, which is
//! `core/log.zig`'s algorithm, and keeping one escaper in the process is
//! worth more than byte-equality on a character that every JSON reader
//! decodes identically. Everything else — control characters as
//! `\u00XX`, invalid UTF-8 as U+FFFD — matches Go byte for byte.
//!
//! That escaper is total: no release name, however hostile, can produce
//! invalid JSON or invalid UTF-8 out of this file. That matters because
//! `filename` comes from an NZB off the internet.

const std = @import("std");
const render = @import("../../app/notify/render.zig");
const f = @import("fmt.zig");
const nzo = @import("nzo.zig");
const ports = @import("ports.zig");

const JobView = ports.JobView;
const FileView = ports.FileView;
const Category = ports.Category;
const JobState = ports.JobState;
const FileState = ports.FileState;

pub const Error = std.Io.Writer.Error;
const Writer = std.Io.Writer;

/// The SAB version we claim to be. The *arr suite gates a handful of
/// features on `version >= 3.0.0` and refuses to talk to a client it
/// cannot place, so this lies — 3.7.2 is what a stock SABnzbd of this
/// generation reports and what every other SAB-replacement claims.
pub const reported_version = "3.7.2";

// ---------------------------------------------------------------------
// Field helpers
// ---------------------------------------------------------------------

/// `"key":"escaped value"`.
fn str(w: *Writer, comptime k: []const u8, v: []const u8) Error!void {
    try render.writeJsonField(w, k, v);
}

/// `"key":<raw>` for a comptime-known literal: a number, `true`, `false`,
/// `null`, `[]`. Never used with runtime text, so nothing here can inject.
fn raw(w: *Writer, comptime k: []const u8, comptime v: []const u8) Error!void {
    try render.writeJsonKey(w, k);
    try w.writeAll(v);
}

/// `"key":<number>`.
fn num(w: *Writer, comptime k: []const u8, v: i64) Error!void {
    try render.writeJsonKey(w, k);
    try w.print("{d}", .{v});
}

// ---------------------------------------------------------------------
// Vocabulary
// ---------------------------------------------------------------------

/// Maps our integer priority to the *string name* real SAB v3.7.x returns
/// in `queue.slot.priority`.
///
/// This is not cosmetic. Returning `"0"` / `"1"` / `"-1"`, as an earlier
/// version did, confuses Sonarr's queue tracker badly enough that it
/// issues a `queue.delete` against the job it just grabbed, seconds after
/// grabbing it.
///
/// SAB's table, from `sabnzbd/constants.py`:
///
///     DEFAULT_PRIORITY = -100  "Default"
///     STOP_PRIORITY    = -4    "Stop"
///     DUP_PRIORITY     = -3    "Duplicate"
///     PAUSED/REPAIR    = -2    "Repair"
///     LOW_PRIORITY     = -1    "Low"
///     NORMAL_PRIORITY  = 0     "Normal"
///     HIGH_PRIORITY    = 1     "High"
///     FORCE_PRIORITY   = 2     "Force"
pub fn priorityToSab(p: i32) []const u8 {
    if (p <= -100) return "Default";
    if (p == -4) return "Stop";
    if (p == -3) return "Duplicate";
    if (p == -2) return "Repair";
    if (p < 0) return "Low";
    if (p == 0) return "Normal";
    if (p == 1) return "High";
    return "Force";
}

/// An empty category is SAB's `"*"` — the catch-all.
pub fn catOrStar(c: []const u8) []const u8 {
    return if (c.len == 0) "*" else c;
}

/// Queue-slot `status`. hoardarr's post-download states collapse onto
/// SAB's smaller vocabulary; `waiting_for_server` has no SAB equivalent
/// and lands on "Unknown", which is what the Go switch's default did.
pub fn stateToSabStatus(s: JobState) []const u8 {
    return switch (s) {
        .queued => "Queued",
        .downloading => "Downloading",
        .paused => "Paused",
        .download_complete, .verifying => "Verifying",
        .repairing => "Repairing",
        .unpacking => "Extracting",
        .completed => "Completed",
        .failed => "Failed",
        .aborted => "Aborted",
        .waiting_for_server => "Unknown",
    };
}

/// History-slot `status`. SAB's history only knows two outcomes, so an
/// aborted job reports as Failed — which is what *arr should act on.
pub fn historyStateToSab(s: JobState) []const u8 {
    return switch (s) {
        .completed => "Completed",
        .failed, .aborted => "Failed",
        else => "Unknown",
    };
}

/// `queue.status`. Any job downloading makes the whole queue
/// "Downloading"; an empty queue is "Idle"; anything else reads as
/// "Paused" even when the jobs are merely queued, because that is what
/// the Go implementation reported and *arr only distinguishes
/// downloading from not.
pub fn queueStatus(jobs: []const JobView) []const u8 {
    for (jobs) |j| {
        if (j.state == .downloading) return "Downloading";
    }
    if (jobs.len == 0) return "Idle";
    return "Paused";
}

/// Per-file `status`. *arr displays whatever it gets here, so the exact
/// vocabulary matters less than never being empty.
pub fn fileStatusToSab(s: FileState) []const u8 {
    return switch (s) {
        .complete => "Finished",
        .downloading => "Active",
        .pending => "Queued",
        .failed => "Failed",
    };
}

// ---------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------

/// One `queue.slots[]` entry.
///
/// `per_job_bps` is this job's share of the overall rate; 0 means "no
/// estimate", and then `timeleft` / `eta` fall back to SAB's unknown
/// sentinels, which real SAB also emits early in a download.
pub fn writeQueueSlot(w: *Writer, v: JobView, per_job_bps: i64, now_unix: i64) Error!void {
    const total = v.total_bytes;
    const left = total - v.done_bytes;
    const pct: i64 = if (total > 0) @divTrunc(v.done_bytes * 100, total) else 0;

    var timeleft_buf: f.HmsBuf = undefined;
    var eta_buf: f.EtaBuf = undefined;
    var timeleft: []const u8 = "0:00:00";
    var eta: []const u8 = "unknown";
    if (per_job_bps > 0 and left > 0 and v.state == .downloading) {
        const secs = @divTrunc(left, per_job_bps);
        timeleft = f.hms(&timeleft_buf, secs);
        // Real SAB v3's ETA layout. The older "Mon 15:04" form parses to
        // a zero time in Sonarr, after which its stuck-download
        // heuristic cancels the grab.
        eta = f.etaSab(&eta_buf, now_unix + secs);
    }

    var handle: nzo.Buf = undefined;
    var mb_buf: f.ScaledBuf = undefined;
    var mbleft_buf: f.ScaledBuf = undefined;
    var done_buf: f.ScaledBuf = undefined;
    var size_buf: f.HumanBuf = undefined;
    var sizeleft_buf: f.HumanBuf = undefined;

    try w.writeByte('{');
    // `_doneMB` sorts first: '_' is 0x5F, below every lowercase letter.
    // Not a SAB field — hoardarr's own addition, which the UI reads.
    try str(w, "_doneMB", f.mbString(&done_buf, v.done_bytes));
    try w.writeByte(',');
    try str(w, "avg_age", "0d");
    try w.writeByte(',');
    try str(w, "cat", catOrStar(v.category));
    try w.writeByte(',');
    // SAB: an int unpack-progress percentage, or null when idle.
    try raw(w, "direct_unpack", "null");
    try w.writeByte(',');
    try str(w, "eta", eta);
    try w.writeByte(',');
    try str(w, "filename", v.name);
    try w.writeByte(',');
    // Always 0, as the Go handler emitted it. *arr keys off nzo_id and
    // ignores the position entirely.
    try raw(w, "index", "0");
    try w.writeByte(',');
    try raw(w, "labels", "[]");
    try w.writeByte(',');
    try str(w, "mb", f.mbString(&mb_buf, total));
    try w.writeByte(',');
    try str(w, "mbleft", f.mbString(&mbleft_buf, left));
    try w.writeByte(',');
    try str(w, "mbmissing", "0.00");
    try w.writeByte(',');
    try raw(w, "missing", "0");
    try w.writeByte(',');
    try str(w, "nzo_id", nzo.encodeWithHash(&handle, v.id, v.nzb_hash));
    try w.writeByte(',');
    try str(w, "password", "");
    try w.writeByte(',');
    try render.writeJsonKey(w, "percentage");
    try w.print("\"{d}\"", .{pct});
    try w.writeByte(',');
    try str(w, "priority", priorityToSab(v.priority));
    try w.writeByte(',');
    try str(w, "script", "None");
    try w.writeByte(',');
    try str(w, "size", f.bytesHuman(&size_buf, total));
    try w.writeByte(',');
    try str(w, "sizeleft", f.bytesHuman(&sizeleft_buf, left));
    try w.writeByte(',');
    try str(w, "status", stateToSabStatus(v.state));
    try w.writeByte(',');
    // Unix *seconds as a number*. Some Sonarr versions read an ISO string
    // here as zero and then trip their stuck-download heuristic.
    try num(w, "time_added", f.unixSeconds(v.added_at_ms));
    try w.writeByte(',');
    try str(w, "timeleft", timeleft);
    try w.writeByte(',');
    // Post-processing options bitmask, as a string. 3 = repair + unpack +
    // delete, which is what hoardarr always does.
    try str(w, "unpackopts", "3");
    try w.writeByte('}');
}

/// The `mode=queue` document, trailing newline included.
///
/// `rate` is the overall throughput in bytes/sec. hoardarr fetches jobs
/// concurrently (one runner per job), so the rate is shared evenly across
/// the jobs that are actually moving to get a per-slot ETA.
pub fn writeQueue(w: *Writer, jobs: []const JobView, rate: i64, now_unix: i64) Error!void {
    // Two different "bytes left" sums, and the difference is deliberate:
    // `total_left` drives the ETA and therefore skips paused jobs, while
    // `sizeleft` / `mbleft` report the whole queue including them.
    var total_left: i64 = 0;
    var active_count: i64 = 0;
    var sum_total: i64 = 0;
    var sum_left: i64 = 0;
    for (jobs) |j| {
        sum_total += j.total_bytes;
        sum_left += j.total_bytes - j.done_bytes;
        if (j.state == .paused) continue;
        const left = j.total_bytes - j.done_bytes;
        if (left > 0) {
            total_left += left;
            active_count += 1;
        }
    }
    var per_job_rate = rate;
    if (active_count > 1 and rate > 0) per_job_rate = @divTrunc(rate, active_count);

    var qtl_buf: f.HmsBuf = undefined;
    var queue_timeleft: []const u8 = "0:00:00";
    if (rate > 0 and total_left > 0) {
        queue_timeleft = f.hms(&qtl_buf, @divTrunc(total_left, rate));
    }

    var speed_buf: f.HumanBuf = undefined;
    var kb_buf: f.ScaledBuf = undefined;
    var size_buf: f.HumanBuf = undefined;
    var sizeleft_buf: f.HumanBuf = undefined;
    var mb_buf: f.ScaledBuf = undefined;
    var mbleft_buf: f.ScaledBuf = undefined;

    try w.writeAll("{\"queue\":{");
    // Disk space is reported as "0": hoardarr does not survey the
    // filesystem, and *arr does not act on these. Strings, as SAB has
    // them.
    try str(w, "diskspace1", "0");
    try w.writeByte(',');
    try str(w, "diskspace2", "0");
    try w.writeByte(',');
    try str(w, "diskspacetotal1", "0");
    try w.writeByte(',');
    try str(w, "diskspacetotal2", "0");
    try w.writeByte(',');
    try num(w, "finish", @intCast(jobs.len));
    try w.writeByte(',');
    try str(w, "kbpersec", f.scaled(&kb_buf, rate, 10, 2));
    try w.writeByte(',');
    try num(w, "limit", @intCast(jobs.len));
    try w.writeByte(',');
    try str(w, "mb", f.mbString(&mb_buf, sum_total));
    try w.writeByte(',');
    try str(w, "mbleft", f.mbString(&mbleft_buf, sum_left));
    try w.writeByte(',');
    try num(w, "noofslots", @intCast(jobs.len));
    try w.writeByte(',');
    try num(w, "noofslots_total", @intCast(jobs.len));
    try w.writeByte(',');
    // Global pause is not modelled; per-job pause is. A consumer that
    // read `true` here would stop submitting entirely.
    try raw(w, "paused", "false");
    try w.writeByte(',');
    try str(w, "size", f.bytesHuman(&size_buf, sum_total));
    try w.writeByte(',');
    try str(w, "sizeleft", f.bytesHuman(&sizeleft_buf, sum_left));
    try w.writeByte(',');
    try render.writeJsonKey(w, "slots");
    try w.writeByte('[');
    for (jobs, 0..) |j, i| {
        if (i > 0) try w.writeByte(',');
        try writeQueueSlot(w, j, per_job_rate, now_unix);
    }
    try w.writeByte(']');
    try w.writeByte(',');
    try str(w, "speed", f.bytesPerSec(&speed_buf, rate));
    try w.writeByte(',');
    try str(w, "speedlimit", "0");
    try w.writeByte(',');
    try str(w, "speedlimit_abs", "");
    try w.writeByte(',');
    try raw(w, "start", "0");
    try w.writeByte(',');
    try str(w, "status", queueStatus(jobs));
    try w.writeByte(',');
    try str(w, "timeleft", queue_timeleft);
    try w.writeByte(',');
    try str(w, "version", reported_version);
    try w.writeAll("}}\n");
}

// ---------------------------------------------------------------------
// History
// ---------------------------------------------------------------------

/// One `history.slots[]` entry. `complete_dir` is only used to guess
/// `storage`; see the comment on that field.
pub fn writeHistorySlot(w: *Writer, v: JobView, complete_dir: []const u8) Error!void {
    // Best-effort "<complete>/<release>". The category subdirectory is
    // not resolved here — that would need the category list — but Sonarr
    // and Radarr import from the path *they* configured, not from this
    // string, so a near miss costs nothing.
    var storage_buf: [1024]u8 = undefined;
    var storage: []const u8 = "";
    if (v.state == .completed and complete_dir.len + v.name.len + 2 <= storage_buf.len) {
        storage = f.join(&storage_buf, complete_dir, v.name);
    }

    var handle: nzo.Buf = undefined;
    var size_buf: f.HumanBuf = undefined;

    try w.writeByte('{');
    try str(w, "action_line", "");
    try w.writeByte(',');
    try raw(w, "archive", "false");
    try w.writeByte(',');
    try num(w, "bytes", v.total_bytes);
    try w.writeByte(',');
    try str(w, "category", catOrStar(v.category));
    try w.writeByte(',');
    try num(w, "completed", f.unixSeconds(v.finished_at_ms));
    try w.writeByte(',');
    try raw(w, "completeness", "null");
    try w.writeByte(',');
    try raw(w, "download_time", "0");
    try w.writeByte(',');
    try num(w, "downloaded", v.total_bytes);
    try w.writeByte(',');
    try str(w, "duplicate_key", "");
    try w.writeByte(',');
    try str(w, "fail_message", v.error_msg);
    try w.writeByte(',');
    try num(w, "id", v.id);
    try w.writeByte(',');
    try raw(w, "loaded", "false");
    try w.writeByte(',');
    try str(w, "md5sum", "");
    try w.writeByte(',');
    try raw(w, "meta", "null");
    try w.writeByte(',');
    try str(w, "name", v.name);
    try w.writeByte(',');
    // SAB reports the NZB's filename; ours is derived from the job name.
    try render.writeJsonKey(w, "nzb_name");
    try w.writeByte('"');
    try render.writeJsonInner(w, v.name);
    try w.writeAll(".nzb\"");
    try w.writeByte(',');
    try str(w, "nzo_id", nzo.encodeWithHash(&handle, v.id, v.nzb_hash));
    try w.writeByte(',');
    try str(w, "password", "");
    try w.writeByte(',');
    try str(w, "path", storage);
    try w.writeByte(',');
    try raw(w, "postproc_time", "0");
    try w.writeByte(',');
    // Post-processing options, encoded. Always "X": hoardarr always
    // verifies, extracts and delivers.
    try str(w, "pp", "X");
    try w.writeByte(',');
    try str(w, "report", "");
    try w.writeByte(',');
    try raw(w, "retry", "false");
    try w.writeByte(',');
    try str(w, "script", "None");
    try w.writeByte(',');
    try str(w, "script_line", "");
    try w.writeByte(',');
    try str(w, "series", "");
    try w.writeByte(',');
    try str(w, "size", f.bytesHuman(&size_buf, v.total_bytes));
    try w.writeByte(',');
    try raw(w, "stage_log", "[]");
    try w.writeByte(',');
    try str(w, "status", historyStateToSab(v.state));
    try w.writeByte(',');
    try str(w, "storage", storage);
    try w.writeByte(',');
    try num(w, "time_added", f.unixSeconds(v.added_at_ms));
    try w.writeByte(',');
    try str(w, "url", "");
    try w.writeByte(',');
    try str(w, "url_info", "");
    try w.writeByte('}');
}

/// The `mode=history` document. The rolling size fields are "0 B": SAB
/// derives them from its own accounting tables, nothing reads them, and
/// synthesising a number would be a lie with no consumer.
pub fn writeHistory(w: *Writer, jobs: []const JobView, complete_dir: []const u8) Error!void {
    var sum_total: i64 = 0;
    for (jobs) |j| sum_total += j.total_bytes;

    var total_buf: f.HumanBuf = undefined;

    try w.writeAll("{\"history\":{");
    try str(w, "day_size", "0 B");
    try w.writeByte(',');
    try str(w, "month_size", "0 B");
    try w.writeByte(',');
    try num(w, "noofslots", @intCast(jobs.len));
    try w.writeByte(',');
    try render.writeJsonKey(w, "slots");
    try w.writeByte('[');
    for (jobs, 0..) |j, i| {
        if (i > 0) try w.writeByte(',');
        try writeHistorySlot(w, j, complete_dir);
    }
    try w.writeByte(']');
    try w.writeByte(',');
    try str(w, "total_size", f.bytesHuman(&total_buf, sum_total));
    try w.writeByte(',');
    try str(w, "version", reported_version);
    try w.writeByte(',');
    try str(w, "week_size", "0 B");
    try w.writeAll("}}\n");
}

// ---------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------

/// One `files[]` entry. Note `mb` / `mbleft` keep both decimals here
/// while the queue slot's trim a whole `.00` — SAB is inconsistent
/// between the two views and so are we.
pub fn writeFileEntry(w: *Writer, v: FileView) Error!void {
    const total = v.size_bytes;
    // Segment-count-weighted progress: the segments are equal-sized on
    // the wire, so this is as good an estimate as the data allows.
    var done_bytes: i64 = 0;
    if (v.segment_count > 0) {
        done_bytes = @divTrunc(total * @as(i64, v.segments_done), @as(i64, v.segment_count));
    }

    var mb_buf: f.ScaledBuf = undefined;
    var mbleft_buf: f.ScaledBuf = undefined;

    try w.writeByte('{');
    try num(w, "bytes", total);
    try w.writeByte(',');
    try num(w, "easy_id", v.id);
    try w.writeByte(',');
    try str(w, "filename", v.filename);
    try w.writeByte(',');
    try str(w, "mb", f.mbPlain(&mb_buf, total));
    try w.writeByte(',');
    try str(w, "mbleft", f.mbPlain(&mbleft_buf, total - done_bytes));
    try w.writeByte(',');
    try render.writeJsonKey(w, "nzf_id");
    try w.print("\"nzf_{d}\"", .{v.id});
    try w.writeByte(',');
    // PAR2 set membership. hoardarr does not group by set here.
    try str(w, "set", "");
    try w.writeByte(',');
    try str(w, "status", fileStatusToSab(v.state));
    try w.writeByte('}');
}

pub fn writeFiles(w: *Writer, files: []const FileView) Error!void {
    try w.writeAll("{\"files\":[");
    for (files, 0..) |file, i| {
        if (i > 0) try w.writeByte(',');
        try writeFileEntry(w, file);
    }
    try w.writeAll("]}\n");
}

// ---------------------------------------------------------------------
// Config, categories, small documents
// ---------------------------------------------------------------------

/// `mode=get_config`. Real SAB returns a vast nested structure; this
/// returns only what consumers actually read — `misc.complete_dir` and
/// the category list — because inventing the rest would be inventing
/// values that clients might then act on.
pub fn writeConfig(w: *Writer, complete_dir: []const u8, cats: []const Category) Error!void {
    try w.writeAll("{\"config\":{");
    try render.writeJsonKey(w, "categories");
    try w.writeByte('[');
    for (cats, 0..) |c, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('{');
        try str(w, "dir", c.dir);
        try w.writeByte(',');
        try str(w, "name", c.name);
        try w.writeByte(',');
        // Newzbin is long dead; the field survives in SAB's schema.
        try str(w, "newzbin", "");
        try w.writeByte(',');
        try num(w, "order", c.priority);
        try w.writeByte(',');
        try str(w, "pp", "");
        try w.writeByte(',');
        try num(w, "priority", c.priority);
        try w.writeByte(',');
        try str(w, "script", "None");
        try w.writeByte('}');
    }
    try w.writeAll("],");
    try render.writeJsonKey(w, "misc");
    try w.writeByte('{');
    try str(w, "complete_dir", complete_dir);
    try w.writeByte(',');
    try str(w, "version", reported_version);
    try w.writeAll("}}}\n");
}

/// `mode=get_cats` — just the names, SAB's `{"categories":[...]}` shape.
pub fn writeCats(w: *Writer, cats: []const Category) Error!void {
    try w.writeAll("{\"categories\":[");
    for (cats, 0..) |c, i| {
        if (i > 0) try w.writeByte(',');
        try render.writeJsonString(w, c.name);
    }
    try w.writeAll("]}\n");
}

pub fn writeVersion(w: *Writer) Error!void {
    try w.writeAll("{\"version\":\"" ++ reported_version ++ "\"}\n");
}

/// `{"nzo_ids":[...],"status":true}` — the answer to every mutating mode.
pub fn writeStatusIds(w: *Writer, ids: []const []const u8) Error!void {
    try w.writeAll("{\"nzo_ids\":[");
    for (ids, 0..) |id, i| {
        if (i > 0) try w.writeByte(',');
        try render.writeJsonString(w, id);
    }
    try w.writeAll("],\"status\":true}\n");
}

pub fn writeEvalSort(w: *Writer, result: []const u8) Error!void {
    try w.writeByte('{');
    try str(w, "result", result);
    try w.writeAll(",\"status\":true}\n");
}

/// The error document. `status:false` plus a message, which is the shape
/// SAB uses and the one *arr surfaces in its UI.
pub fn writeError(w: *Writer, message: []const u8) Error!void {
    try w.writeByte('{');
    try str(w, "error", message);
    try w.writeAll(",\"status\":false}\n");
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Renders through a growable writer and returns the bytes; the caller
/// frees. Every golden test goes through this.
fn renderTo(
    comptime fun: anytype,
    args: anytype,
) ![]u8 {
    var alloc: std.Io.Writer.Allocating = .init(testing.allocator);
    errdefer alloc.deinit();
    try @call(.auto, fun, .{&alloc.writer} ++ args);
    return alloc.toOwnedSlice();
}

const job_downloading: JobView = .{
    .id = 42,
    .nzb_hash = "deadbeefcafe",
    .name = "Some.Release.S01E02.1080p-GRP",
    .category = "tv",
    .priority = 0,
    .state = .downloading,
    .total_bytes = 1234567890,
    .done_bytes = 456789012,
    .added_at_ms = 1700000000_000,
};

const job_paused: JobView = .{
    .id = 7,
    .nzb_hash = "cafebabe",
    .name = "Second.Release",
    .priority = -1,
    .state = .paused,
};

const now: i64 = 1700000000;

test "queue slot bytes match the Go handler exactly" {
    // Captured from the Go implementation: one active job, 1.5 MiB/s, so
    // per-job rate == overall rate.
    const want =
        "{\"_doneMB\":\"435.63\",\"avg_age\":\"0d\",\"cat\":\"tv\"," ++
        "\"direct_unpack\":null,\"eta\":\"22:21 Tue 14 Nov\"," ++
        "\"filename\":\"Some.Release.S01E02.1080p-GRP\",\"index\":0,\"labels\":[]," ++
        "\"mb\":\"1177.38\",\"mbleft\":\"741.75\",\"mbmissing\":\"0.00\",\"missing\":0," ++
        "\"nzo_id\":\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\",\"password\":\"\"," ++
        "\"percentage\":\"36\",\"priority\":\"Normal\",\"script\":\"None\"," ++
        "\"size\":\"1.15 GB\",\"sizeleft\":\"741.75 MB\",\"status\":\"Downloading\"," ++
        "\"time_added\":1700000000,\"timeleft\":\"0:08:14\",\"unpackopts\":\"3\"}";
    const got = try renderTo(writeQueueSlot, .{ job_downloading, 1572864, now });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "a paused, empty slot falls back to SAB's unknown sentinels" {
    const want =
        "{\"_doneMB\":\"0\",\"avg_age\":\"0d\",\"cat\":\"*\"," ++
        "\"direct_unpack\":null,\"eta\":\"unknown\",\"filename\":\"Second.Release\"," ++
        "\"index\":0,\"labels\":[],\"mb\":\"0\",\"mbleft\":\"0\",\"mbmissing\":\"0.00\"," ++
        "\"missing\":0,\"nzo_id\":\"SABnzbd_nzo_G45GGYLGMVRGCYTF\",\"password\":\"\"," ++
        "\"percentage\":\"0\",\"priority\":\"Low\",\"script\":\"None\",\"size\":\"0 B\"," ++
        "\"sizeleft\":\"0 B\",\"status\":\"Paused\",\"time_added\":0," ++
        "\"timeleft\":\"0:00:00\",\"unpackopts\":\"3\"}";
    const got = try renderTo(writeQueueSlot, .{ job_paused, 1572864, now });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "mode=queue document bytes match the Go handler exactly" {
    const want =
        "{\"queue\":{\"diskspace1\":\"0\",\"diskspace2\":\"0\"," ++
        "\"diskspacetotal1\":\"0\",\"diskspacetotal2\":\"0\",\"finish\":2," ++
        "\"kbpersec\":\"1536.00\",\"limit\":2,\"mb\":\"1177.38\",\"mbleft\":\"741.75\"," ++
        "\"noofslots\":2,\"noofslots_total\":2,\"paused\":false,\"size\":\"1.15 GB\"," ++
        "\"sizeleft\":\"741.75 MB\",\"slots\":[" ++
        "{\"_doneMB\":\"435.63\",\"avg_age\":\"0d\",\"cat\":\"tv\",\"direct_unpack\":null," ++
        "\"eta\":\"22:21 Tue 14 Nov\",\"filename\":\"Some.Release.S01E02.1080p-GRP\"," ++
        "\"index\":0,\"labels\":[],\"mb\":\"1177.38\",\"mbleft\":\"741.75\"," ++
        "\"mbmissing\":\"0.00\",\"missing\":0,\"nzo_id\":\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\"," ++
        "\"password\":\"\",\"percentage\":\"36\",\"priority\":\"Normal\",\"script\":\"None\"," ++
        "\"size\":\"1.15 GB\",\"sizeleft\":\"741.75 MB\",\"status\":\"Downloading\"," ++
        "\"time_added\":1700000000,\"timeleft\":\"0:08:14\",\"unpackopts\":\"3\"}," ++
        "{\"_doneMB\":\"0\",\"avg_age\":\"0d\",\"cat\":\"*\",\"direct_unpack\":null," ++
        "\"eta\":\"unknown\",\"filename\":\"Second.Release\",\"index\":0,\"labels\":[]," ++
        "\"mb\":\"0\",\"mbleft\":\"0\",\"mbmissing\":\"0.00\",\"missing\":0," ++
        "\"nzo_id\":\"SABnzbd_nzo_G45GGYLGMVRGCYTF\",\"password\":\"\",\"percentage\":\"0\"," ++
        "\"priority\":\"Low\",\"script\":\"None\",\"size\":\"0 B\",\"sizeleft\":\"0 B\"," ++
        "\"status\":\"Paused\",\"time_added\":0,\"timeleft\":\"0:00:00\",\"unpackopts\":\"3\"}" ++
        "],\"speed\":\"1.5 MB/s\",\"speedlimit\":\"0\",\"speedlimit_abs\":\"\",\"start\":0," ++
        "\"status\":\"Downloading\",\"timeleft\":\"0:08:14\",\"version\":\"3.7.2\"}}\n";
    const jobs = [_]JobView{ job_downloading, job_paused };
    const got = try renderTo(writeQueue, .{ @as([]const JobView, &jobs), @as(i64, 1572864), now });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "an idle queue is still a full document" {
    const want =
        "{\"queue\":{\"diskspace1\":\"0\",\"diskspace2\":\"0\",\"diskspacetotal1\":\"0\"," ++
        "\"diskspacetotal2\":\"0\",\"finish\":0,\"kbpersec\":\"0.00\",\"limit\":0," ++
        "\"mb\":\"0\",\"mbleft\":\"0\",\"noofslots\":0,\"noofslots_total\":0," ++
        "\"paused\":false,\"size\":\"0 B\",\"sizeleft\":\"0 B\",\"slots\":[]," ++
        "\"speed\":\"0 B/s\",\"speedlimit\":\"0\",\"speedlimit_abs\":\"\",\"start\":0," ++
        "\"status\":\"Idle\",\"timeleft\":\"0:00:00\",\"version\":\"3.7.2\"}}\n";
    const got = try renderTo(writeQueue, .{ @as([]const JobView, &.{}), @as(i64, 0), now });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "queue totals include paused jobs but the ETA does not" {
    // The Go implementation kept two sums on purpose: `sizeleft` covers
    // the whole queue, `timeleft` only the part that is moving.
    const dl: JobView = .{
        .id = 1,
        .name = "A",
        .state = .downloading,
        .total_bytes = 3 << 20,
        .done_bytes = 1 << 20,
    };
    const paused: JobView = .{ .id = 2, .name = "B", .state = .paused, .total_bytes = 2 << 20 };
    const jobs = [_]JobView{ dl, paused };
    const got = try renderTo(writeQueue, .{ @as([]const JobView, &jobs), @as(i64, 1 << 20), now });
    defer testing.allocator.free(got);
    // 5 MiB total, 4 MiB left across both jobs...
    try testing.expect(std.mem.indexOf(u8, got, "\"size\":\"5.00 MB\"") != null);
    try testing.expect(std.mem.indexOf(u8, got, "\"sizeleft\":\"4.00 MB\"") != null);
    try testing.expect(std.mem.indexOf(u8, got, "\"mbleft\":\"4\"") != null);
    // ...but only the 2 MiB on the downloading job counts toward the ETA,
    // at 1 MiB/s.
    try testing.expect(std.mem.indexOf(u8, got, "\"timeleft\":\"0:00:02\"") != null);
}

test "the rate is split across the jobs that are moving" {
    const a: JobView = .{ .id = 1, .name = "A", .state = .downloading, .total_bytes = 4 << 20 };
    const b: JobView = .{ .id = 2, .name = "B", .state = .downloading, .total_bytes = 8 << 20 };
    const jobs = [_]JobView{ a, b };
    // 2 MiB/s overall, two movers -> 1 MiB/s each, so the slots need 4 s
    // and 8 s while the queue as a whole needs 12 MiB / 2 MiB/s = 6 s.
    const got = try renderTo(writeQueue, .{ @as([]const JobView, &jobs), @as(i64, 2 << 20), now });
    defer testing.allocator.free(got);
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, got, "\"timeleft\":\"0:00:04\""));
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, got, "\"timeleft\":\"0:00:08\""));
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, got, "\"timeleft\":\"0:00:06\""));
}

test "history slot bytes match the Go handler exactly" {
    const completed: JobView = .{
        .id = 42,
        .nzb_hash = "deadbeefcafe",
        .name = "Some.Release.S01E02.1080p-GRP",
        .category = "tv",
        .state = .completed,
        .total_bytes = 1234567890,
        .done_bytes = 1234567890,
        .added_at_ms = 1700000000_000,
        .finished_at_ms = 1700003600_000,
    };
    const want =
        "{\"action_line\":\"\",\"archive\":false,\"bytes\":1234567890,\"category\":\"tv\"," ++
        "\"completed\":1700003600,\"completeness\":null,\"download_time\":0," ++
        "\"downloaded\":1234567890,\"duplicate_key\":\"\",\"fail_message\":\"\",\"id\":42," ++
        "\"loaded\":false,\"md5sum\":\"\",\"meta\":null," ++
        "\"name\":\"Some.Release.S01E02.1080p-GRP\"," ++
        "\"nzb_name\":\"Some.Release.S01E02.1080p-GRP.nzb\"," ++
        "\"nzo_id\":\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\",\"password\":\"\"," ++
        "\"path\":\"/data/complete/Some.Release.S01E02.1080p-GRP\",\"postproc_time\":0," ++
        "\"pp\":\"X\",\"report\":\"\",\"retry\":false,\"script\":\"None\"," ++
        "\"script_line\":\"\",\"series\":\"\",\"size\":\"1.15 GB\",\"stage_log\":[]," ++
        "\"status\":\"Completed\",\"storage\":\"/data/complete/Some.Release.S01E02.1080p-GRP\"," ++
        "\"time_added\":1700000000,\"url\":\"\",\"url_info\":\"\"}";
    const got = try renderTo(writeHistorySlot, .{ completed, @as([]const u8, "/data/complete") });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "a failed history slot carries the error and no storage path" {
    const failed: JobView = .{
        .id = 9,
        .name = "Failed.Release",
        .state = .failed,
        .total_bytes = 1024,
        .added_at_ms = 1699990000_000,
        .finished_at_ms = 1699991000_000,
        .error_msg = "all segments missing",
    };
    const want =
        "{\"action_line\":\"\",\"archive\":false,\"bytes\":1024,\"category\":\"*\"," ++
        "\"completed\":1699991000,\"completeness\":null,\"download_time\":0," ++
        "\"downloaded\":1024,\"duplicate_key\":\"\"," ++
        "\"fail_message\":\"all segments missing\",\"id\":9,\"loaded\":false," ++
        "\"md5sum\":\"\",\"meta\":null,\"name\":\"Failed.Release\"," ++
        "\"nzb_name\":\"Failed.Release.nzb\",\"nzo_id\":\"SABnzbd_nzo_HE\"," ++
        "\"password\":\"\",\"path\":\"\",\"postproc_time\":0,\"pp\":\"X\"," ++
        "\"report\":\"\",\"retry\":false,\"script\":\"None\",\"script_line\":\"\"," ++
        "\"series\":\"\",\"size\":\"1.00 KB\",\"stage_log\":[],\"status\":\"Failed\"," ++
        "\"storage\":\"\",\"time_added\":1699990000,\"url\":\"\",\"url_info\":\"\"}";
    const got = try renderTo(writeHistorySlot, .{ failed, @as([]const u8, "/data/complete") });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "an empty history is still a full document" {
    const want =
        "{\"history\":{\"day_size\":\"0 B\",\"month_size\":\"0 B\",\"noofslots\":0," ++
        "\"slots\":[],\"total_size\":\"0 B\",\"version\":\"3.7.2\",\"week_size\":\"0 B\"}}\n";
    const got = try renderTo(writeHistory, .{ @as([]const JobView, &.{}), @as([]const u8, "/x") });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "history envelope wraps its slots and sums their bytes" {
    const a: JobView = .{ .id = 1, .name = "A", .state = .completed, .total_bytes = 1024 };
    const b: JobView = .{ .id = 2, .name = "B", .state = .failed, .total_bytes = 1024 };
    const jobs = [_]JobView{ a, b };
    const got = try renderTo(writeHistory, .{ @as([]const JobView, &jobs), @as([]const u8, "") });
    defer testing.allocator.free(got);
    try testing.expect(std.mem.startsWith(u8, got, "{\"history\":{\"day_size\":\"0 B\""));
    try testing.expect(std.mem.indexOf(u8, got, "\"noofslots\":2") != null);
    try testing.expect(std.mem.indexOf(u8, got, "\"total_size\":\"2.00 KB\"") != null);
    // An empty complete_dir still yields a storage path for a completed
    // job — filepath.Join drops the empty element.
    try testing.expect(std.mem.indexOf(u8, got, "\"storage\":\"A\"") != null);
    try testing.expect(std.mem.indexOf(u8, got, "\"storage\":\"\"") != null);
}

test "mode=get_files bytes match the Go handler exactly" {
    const files = [_]FileView{
        .{
            .id = 7,
            .filename = "release.part01.rar",
            .size_bytes = 52428800,
            .segment_count = 100,
            .segments_done = 50,
            .state = .downloading,
        },
        .{
            .id = 8,
            .filename = "release.par2",
            .size_bytes = 1048576,
            .segment_count = 4,
            .segments_done = 4,
            .state = .complete,
        },
    };
    const want =
        "{\"files\":[{\"bytes\":52428800,\"easy_id\":7,\"filename\":\"release.part01.rar\"," ++
        "\"mb\":\"50.00\",\"mbleft\":\"25.00\",\"nzf_id\":\"nzf_7\",\"set\":\"\"," ++
        "\"status\":\"Active\"},{\"bytes\":1048576,\"easy_id\":8," ++
        "\"filename\":\"release.par2\",\"mb\":\"1.00\",\"mbleft\":\"0.00\"," ++
        "\"nzf_id\":\"nzf_8\",\"set\":\"\",\"status\":\"Finished\"}]}\n";
    const got = try renderTo(writeFiles, .{@as([]const FileView, &files)});
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
    const empty = try renderTo(writeFiles, .{@as([]const FileView, &.{})});
    defer testing.allocator.free(empty);
    try testing.expectEqualStrings("{\"files\":[]}\n", empty);
}

test "a file with no segments reports nothing done rather than dividing by zero" {
    const files = [_]FileView{.{ .id = 1, .filename = "x", .size_bytes = 1048576 }};
    const got = try renderTo(writeFiles, .{@as([]const FileView, &files)});
    defer testing.allocator.free(got);
    try testing.expect(std.mem.indexOf(u8, got, "\"mb\":\"1.00\",\"mbleft\":\"1.00\"") != null);
}

test "mode=get_config bytes match the Go handler exactly" {
    const cats = [_]Category{
        .{ .name = "*", .dir = "", .priority = 0 },
        .{ .name = "tv", .dir = "tv", .priority = 1 },
    };
    const want =
        "{\"config\":{\"categories\":[{\"dir\":\"\",\"name\":\"*\",\"newzbin\":\"\"," ++
        "\"order\":0,\"pp\":\"\",\"priority\":0,\"script\":\"None\"},{\"dir\":\"tv\"," ++
        "\"name\":\"tv\",\"newzbin\":\"\",\"order\":1,\"pp\":\"\",\"priority\":1," ++
        "\"script\":\"None\"}],\"misc\":{\"complete_dir\":\"/data/complete\"," ++
        "\"version\":\"3.7.2\"}}}\n";
    const got = try renderTo(writeConfig, .{ @as([]const u8, "/data/complete"), @as([]const Category, &cats) });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);

    const empty = try renderTo(writeConfig, .{ @as([]const u8, ""), @as([]const Category, &.{}) });
    defer testing.allocator.free(empty);
    try testing.expectEqualStrings(
        "{\"config\":{\"categories\":[],\"misc\":{\"complete_dir\":\"\",\"version\":\"3.7.2\"}}}\n",
        empty,
    );
}

test "the small documents match the Go handler exactly" {
    const cats = [_]Category{ .{ .name = "*" }, .{ .name = "tv" } };
    const c = try renderTo(writeCats, .{@as([]const Category, &cats)});
    defer testing.allocator.free(c);
    try testing.expectEqualStrings("{\"categories\":[\"*\",\"tv\"]}\n", c);

    const c0 = try renderTo(writeCats, .{@as([]const Category, &.{})});
    defer testing.allocator.free(c0);
    try testing.expectEqualStrings("{\"categories\":[]}\n", c0);

    const v = try renderTo(writeVersion, .{});
    defer testing.allocator.free(v);
    try testing.expectEqualStrings("{\"version\":\"3.7.2\"}\n", v);

    const ids = [_][]const u8{ "SABnzbd_nzo_GQZDUZDFMFSGEZLFMY", "SABnzbd_nzo_G45GGYLGMVRGCYTF" };
    const s = try renderTo(writeStatusIds, .{@as([]const []const u8, &ids)});
    defer testing.allocator.free(s);
    try testing.expectEqualStrings(
        "{\"nzo_ids\":[\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\",\"SABnzbd_nzo_G45GGYLGMVRGCYTF\"]," ++
            "\"status\":true}\n",
        s,
    );

    const s0 = try renderTo(writeStatusIds, .{@as([]const []const u8, &.{})});
    defer testing.allocator.free(s0);
    try testing.expectEqualStrings("{\"nzo_ids\":[],\"status\":true}\n", s0);

    const e = try renderTo(writeEvalSort, .{@as([]const u8, "Great Show (2020)/Season 03/S03E07.mkv")});
    defer testing.allocator.free(e);
    try testing.expectEqualStrings(
        "{\"result\":\"Great Show (2020)/Season 03/S03E07.mkv\",\"status\":true}\n",
        e,
    );

    const err = try renderTo(writeError, .{@as([]const u8, "mode=\"bogus\" not implemented")});
    defer testing.allocator.free(err);
    try testing.expectEqualStrings(
        "{\"error\":\"mode=\\\"bogus\\\" not implemented\",\"status\":false}\n",
        err,
    );
}

// -- vocabulary ------------------------------------------------------

test "priorityToSab reproduces SAB's integer-to-name table" {
    try testing.expectEqualStrings("Default", priorityToSab(-100));
    try testing.expectEqualStrings("Default", priorityToSab(-1000));
    try testing.expectEqualStrings("Stop", priorityToSab(-4));
    try testing.expectEqualStrings("Duplicate", priorityToSab(-3));
    try testing.expectEqualStrings("Repair", priorityToSab(-2));
    try testing.expectEqualStrings("Low", priorityToSab(-1));
    try testing.expectEqualStrings("Normal", priorityToSab(0));
    try testing.expectEqualStrings("High", priorityToSab(1));
    try testing.expectEqualStrings("Force", priorityToSab(2));
    try testing.expectEqualStrings("Force", priorityToSab(99));
    // Between DEFAULT (-100) and STOP (-4) there is no SAB name, and Go
    // fell through to "Low".
    try testing.expectEqualStrings("Low", priorityToSab(-50));
    try testing.expectEqualStrings("Low", priorityToSab(-5));
}

test "state vocabularies cover every domain state" {
    for (std.enums.values(JobState)) |s| {
        try testing.expect(stateToSabStatus(s).len > 0);
        try testing.expect(historyStateToSab(s).len > 0);
    }
    try testing.expectEqualStrings("Verifying", stateToSabStatus(.download_complete));
    try testing.expectEqualStrings("Verifying", stateToSabStatus(.verifying));
    try testing.expectEqualStrings("Extracting", stateToSabStatus(.unpacking));
    // No SAB equivalent; Go's default arm produced this.
    try testing.expectEqualStrings("Unknown", stateToSabStatus(.waiting_for_server));
    try testing.expectEqualStrings("Failed", historyStateToSab(.aborted));
    try testing.expectEqualStrings("Unknown", historyStateToSab(.queued));

    for (std.enums.values(FileState)) |s| {
        try testing.expect(fileStatusToSab(s).len > 0);
    }
    try testing.expectEqualStrings("Finished", fileStatusToSab(.complete));
    try testing.expectEqualStrings("Queued", fileStatusToSab(.pending));
}

test "queueStatus distinguishes idle from stalled" {
    try testing.expectEqualStrings("Idle", queueStatus(&.{}));
    try testing.expectEqualStrings("Paused", queueStatus(&.{.{ .state = .queued }}));
    try testing.expectEqualStrings("Paused", queueStatus(&.{.{ .state = .paused }}));
    try testing.expectEqualStrings(
        "Downloading",
        queueStatus(&.{ .{ .state = .paused }, .{ .state = .downloading } }),
    );
}

test "catOrStar substitutes SAB's catch-all" {
    try testing.expectEqualStrings("*", catOrStar(""));
    try testing.expectEqualStrings("tv", catOrStar("tv"));
}

// -- hostile input ---------------------------------------------------

/// Names that came off the internet. Every one of these has to produce
/// JSON that `std.json` accepts, with the value decoding to the expected
/// text — which is the same guarantee Go's encoder gave.
const hostile = [_]struct { in: []const u8, want: []const u8 }{
    .{ .in = "He said \"hi\" \\ backslash", .want = "He said \"hi\" \\ backslash" },
    .{ .in = "tab\there\nnewline\rcr", .want = "tab\there\nnewline\rcr" },
    .{ .in = "ctrl\x00\x01\x1f", .want = "ctrl\x00\x01\x1f" },
    .{ .in = "amp & lt < gt > html", .want = "amp & lt < gt > html" },
    // Invalid UTF-8 becomes one U+FFFD per bad byte, matching Go.
    .{ .in = "invalid \xff\xfe utf8", .want = "invalid \u{FFFD}\u{FFFD} utf8" },
    .{ .in = "truncated \xe2\x82", .want = "truncated \u{FFFD}\u{FFFD}" },
    .{ .in = "lone surrogate \xed\xa0\x80", .want = "lone surrogate \u{FFFD}\u{FFFD}\u{FFFD}" },
    .{ .in = "overlong \xc0\xaf", .want = "overlong \u{FFFD}\u{FFFD}" },
    .{ .in = "emoji \u{1F3AC} ok", .want = "emoji \u{1F3AC} ok" },
    .{ .in = "line sep \u{2028} para \u{2029}", .want = "line sep \u{2028} para \u{2029}" },
    .{ .in = "\"},{\"injected\":\"yes", .want = "\"},{\"injected\":\"yes" },
    // DEL is legal raw in a JSON string; neither Go nor we escape it.
    .{ .in = "del\x7f here", .want = "del\x7f here" },
    .{ .in = "", .want = "" },
};

test "a hostile release name cannot break the queue document" {
    for (hostile) |c| {
        const v: JobView = .{ .id = 1, .name = c.in, .state = .downloading, .total_bytes = 1024 };
        const jobs = [_]JobView{v};
        const got = try renderTo(writeQueue, .{ @as([]const JobView, &jobs), @as(i64, 1024), now });
        defer testing.allocator.free(got);

        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, got, .{});
        defer parsed.deinit();
        const slot = parsed.value.object.get("queue").?.object.get("slots").?.array.items[0];
        try testing.expectEqualStrings(c.want, slot.object.get("filename").?.string);
    }
}

test "a hostile release name cannot break the history document" {
    for (hostile) |c| {
        const v: JobView = .{ .id = 1, .name = c.in, .state = .failed, .error_msg = c.in };
        const jobs = [_]JobView{v};
        const got = try renderTo(writeHistory, .{ @as([]const JobView, &jobs), @as([]const u8, "/c") });
        defer testing.allocator.free(got);

        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, got, .{});
        defer parsed.deinit();
        const slot = parsed.value.object.get("history").?.object.get("slots").?.array.items[0];
        try testing.expectEqualStrings(c.want, slot.object.get("name").?.string);
        try testing.expectEqualStrings(c.want, slot.object.get("fail_message").?.string);
        // nzb_name is the name with a suffix appended *inside* the string
        // literal, so it is the one field where escaping and
        // concatenation meet.
        const nzb_name = slot.object.get("nzb_name").?.string;
        try testing.expect(std.mem.endsWith(u8, nzb_name, ".nzb"));
        try testing.expectEqualStrings(c.want, nzb_name[0 .. nzb_name.len - 4]);
    }
}

test "a hostile filename cannot break the files document" {
    for (hostile) |c| {
        const files = [_]FileView{.{ .id = 1, .filename = c.in, .size_bytes = 1 }};
        const got = try renderTo(writeFiles, .{@as([]const FileView, &files)});
        defer testing.allocator.free(got);

        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, got, .{});
        defer parsed.deinit();
        const arr = parsed.value.object.get("files").?.array.items[0];
        try testing.expectEqualStrings(c.want, arr.object.get("filename").?.string);
    }
}

test "a hostile category name cannot break get_cats or get_config" {
    for (hostile) |c| {
        const cats = [_]Category{.{ .name = c.in, .dir = c.in }};
        const got = try renderTo(writeCats, .{@as([]const Category, &cats)});
        defer testing.allocator.free(got);
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, got, .{});
        defer parsed.deinit();
        try testing.expectEqualStrings(c.want, parsed.value.object.get("categories").?.array.items[0].string);

        const cfg = try renderTo(writeConfig, .{ @as([]const u8, c.in), @as([]const Category, &cats) });
        defer testing.allocator.free(cfg);
        const p2 = try std.json.parseFromSlice(std.json.Value, testing.allocator, cfg, .{});
        defer p2.deinit();
        const cat0 = p2.value.object.get("config").?.object.get("categories").?.array.items[0];
        try testing.expectEqualStrings(c.want, cat0.object.get("name").?.string);
        try testing.expectEqualStrings(
            c.want,
            p2.value.object.get("config").?.object.get("misc").?.object.get("complete_dir").?.string,
        );
    }
}

test "a hostile error message cannot break the error document" {
    for (hostile) |c| {
        const got = try renderTo(writeError, .{c.in});
        defer testing.allocator.free(got);
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, got, .{});
        defer parsed.deinit();
        try testing.expectEqualStrings(c.want, parsed.value.object.get("error").?.string);
        try testing.expectEqual(false, parsed.value.object.get("status").?.bool);
    }
}

test "every document is valid JSON for every hostile name" {
    // A blunt whole-document check: no matter what goes in, std.json
    // parses what comes out. This is the property that keeps a malformed
    // NZB from taking Sonarr's queue poll down.
    for (hostile) |c| {
        const v: JobView = .{ .id = 3, .nzb_hash = c.in, .name = c.in, .category = c.in, .error_msg = c.in };
        const jobs = [_]JobView{v};
        inline for (.{ writeQueueDoc, writeHistoryDoc }) |fun| {
            const got = try fun(@as([]const JobView, &jobs));
            defer testing.allocator.free(got);
            try testing.expect(std.json.validate(testing.allocator, got) catch false);
        }
    }
}

fn writeQueueDoc(jobs: []const JobView) ![]u8 {
    return renderTo(writeQueue, .{ jobs, @as(i64, 0), now });
}

fn writeHistoryDoc(jobs: []const JobView) ![]u8 {
    return renderTo(writeHistory, .{ jobs, @as([]const u8, "/c") });
}
