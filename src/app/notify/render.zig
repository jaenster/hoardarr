//! Normalises a bus envelope into a flat `View` that the chat-style
//! notification adapters (Discord, Slack) pour into their own widget
//! shapes, plus the JSON string writer both of them emit through.
//!
//! Two halves, mirroring the Go original's `render.go` + `parse.go`:
//!
//!   * **Projection.** `View.from` decodes the enriched payload the
//!     notify service produces (original event under `event`, job
//!     snapshot under `job`) and flattens it: release name, a friendly
//!     verb for the topic, who sent the NZB, category, size, file
//!     count, quality, state, error message.
//!   * **Release-name heuristics.** `cleanReleaseName`, `parseQuality`,
//!     `sourceName`, `bytesHuman` — string mangling that turns scene
//!     names and User-Agents into something an operator wants to read.
//!     Go used two `regexp`s for the quality tokens; here they are two
//!     ordered token tables with an explicit word-boundary check, which
//!     is both smaller and allocation-free.
//!
//! # Hostile input
//!
//! Everything in a `View` other than the verb ultimately came out of an
//! NZB off the internet: release names carry quotes, backslashes,
//! control bytes and invalid UTF-8 as a matter of routine, and one of
//! them will eventually be crafted. Two consequences:
//!
//!   * `writeJsonString` is total — no input produces invalid JSON or
//!     invalid UTF-8. It is the same algorithm `core/log.zig` uses for
//!     its JSON handler (short escapes, `\u00XX` for the remaining C0
//!     bytes, `�` for anything that is not a valid UTF-8
//!     sequence), deliberately not a second dialect of escaping.
//!   * A payload that does not decode is a *diagnostic*, never a
//!     failure and never a half-rendered message: `View.diag` is set,
//!     the topic-derived fields still render, and the adapters surface
//!     the diagnostic to the operator.
//!
//! # String ownership
//!
//! Every slice in a `View` is either a string literal, borrowed from
//! the envelope, or allocated from the arena passed to `View.from`.
//! There is no `deinit`: notification rendering is a short-lived unit
//! of work, so the caller resets one arena and the whole projection
//! goes with it.

const std = @import("std");
const log = @import("../../core/log.zig");
const event = @import("../../domain/event.zig");

const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------

/// Semantic classification of a topic. Adapters use it to pick a colour
/// or an emoji without knowing the full topic taxonomy.
pub const Outcome = enum {
    /// Neutral default — "added to queue", "verifying", "test
    /// notification".
    info,
    /// Success — verified, repaired, delivered, completed.
    ok,
    /// User attention may be needed but the job did not fail outright.
    /// Currently only `verify.repair_needed`.
    warn,
    /// Terminal failure.
    fail,

    /// Hex RGB for a Discord `embed.color` / Slack attachment colour.
    /// The palette is tuned to read the same in dark and light themes;
    /// the values are asserted in a test because a silent drift here is
    /// the kind of thing nobody notices until a screenshot.
    pub fn color(self: Outcome) u32 {
        return switch (self) {
            .ok => 0x2ECC71, // green
            .fail => 0xF85149, // red
            .warn => 0xD29922, // amber
            .info => 0x4A90E2, // hoardarr blue
        };
    }
};

/// Maps a bus topic to a label fit for a notification title. Unknown
/// topics fall through verbatim so a debug event stays visible rather
/// than silently mis-rendering as something else.
pub fn verb(topic: []const u8) []const u8 {
    const table = .{
        .{ "download.job.created", "Added to queue" },
        .{ "download.job.download_complete", "Download complete" },
        .{ "download.job.completed", "Completed" },
        .{ "download.job.failed", "Failed" },
        .{ "download.job.download_failed", "Failed" },
        .{ "download.job.paused", "Paused" },
        .{ "download.job.resumed", "Resumed" },
        .{ "download.job.removed", "Removed" },
        .{ "verify.started", "Verifying" },
        .{ "verify.ok", "Verified" },
        .{ "verify.failed", "Verify failed" },
        .{ "verify.repair_needed", "Repair needed" },
        .{ "repair.started", "Repairing" },
        .{ "repair.ok", "Repaired" },
        .{ "repair.failed", "Repair failed" },
        .{ "extract.started", "Extracting" },
        .{ "extract.complete", "Extracted" },
        .{ "extract.failed", "Extract failed" },
        .{ "deliver.started", "Delivering" },
        .{ "deliver.complete", "Delivered" },
        .{ "deliver.failed", "Delivery failed" },
        .{ "notify.test", "Test notification" },
    };
    inline for (table) |row| {
        if (std.mem.eql(u8, topic, row[0])) return row[1];
    }
    return topic;
}

/// Classifies a topic by suffix. The mapping exploits the naming
/// convention every bounded context follows (`.ok` / `.complete` /
/// `.completed` → ok, `.failed` → fail, `.repair_needed` → warn),
/// which is why a new context's events render sensibly without
/// touching this function.
pub fn outcomeFor(topic: []const u8) Outcome {
    if (std.mem.endsWith(u8, topic, ".failed")) return .fail;
    if (std.mem.endsWith(u8, topic, ".download_failed")) return .fail;
    if (std.mem.endsWith(u8, topic, ".repair_needed")) return .warn;
    if (std.mem.endsWith(u8, topic, ".ok")) return .ok;
    if (std.mem.endsWith(u8, topic, ".complete")) return .ok;
    if (std.mem.endsWith(u8, topic, ".completed")) return .ok;
    return .info;
}

// ---------------------------------------------------------------------
// Release-name heuristics
// ---------------------------------------------------------------------

const ascii_space = " \t\r\n";

/// Converts a scene release name into something pleasant enough for a
/// notification title:
///
///   Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson
///     → "Euphoria US S03E06 PROPER MULTi DV HDR 2160p WEB H265"
///
/// Dots and underscores become spaces and a trailing `-GROUP` token is
/// dropped. We deliberately do *not* try to extract just the show
/// title: that needs TVDB/TMDB metadata we do not have, and guessing
/// wrong mangles the one string the operator uses to identify the
/// release.
pub fn cleanReleaseName(arena: Allocator, raw: []const u8) Allocator.Error![]const u8 {
    var s = std.mem.trim(u8, raw, ascii_space);
    if (s.len == 0) return "";

    // Strip a trailing -GROUP. Only the *last* hyphen is considered and
    // only when the suffix looks like a group, because movie titles
    // legitimately contain hyphens ("Mission: Impossible - Fallout")
    // and nuking half the title is worse than keeping the group.
    if (std.mem.lastIndexOfScalar(u8, s, '-')) |i| {
        if (i + 1 < s.len and isReleaseGroup(s[i + 1 ..])) {
            s = std.mem.trimEnd(u8, s[0..i], " .");
        }
    }

    // Single pass: separators to spaces, runs of whitespace collapsed.
    var out: std.ArrayList(u8) = .empty;
    try out.ensureTotalCapacity(arena, s.len);
    var prev_space = false;
    for (s) |c| {
        const is_space = c == ' ' or c == '\t' or c == '.' or c == '_';
        if (is_space) {
            if (!prev_space) out.appendAssumeCapacity(' ');
            prev_space = true;
            continue;
        }
        out.appendAssumeCapacity(c);
        prev_space = false;
    }
    return std.mem.trim(u8, out.items, ascii_space);
}

/// Heuristic for "is this trailing token a release group": short, no
/// spaces, ASCII alnum plus `_` and `.`. Keeps subtitle suffixes like
/// " - The Movie" intact.
fn isReleaseGroup(s: []const u8) bool {
    if (s.len == 0 or s.len > 24) return false;
    for (s) |c| switch (c) {
        'a'...'z', 'A'...'Z', '0'...'9', '_', '.' => {},
        else => return false,
    };
    return true;
}

/// Resolution tokens, in the order Go's alternation listed them.
const res_tokens = [_][]const u8{ "2160p", "1080p", "720p", "480p", "4k", "uhd" };

/// Source tokens. **Order is load-bearing**: `WEB-DL` has to be tried
/// before `WEB`, because `WEB` alone also satisfies the word-boundary
/// test against the following hyphen and would win otherwise. Go's
/// leftmost-first alternation had the same requirement, which is why
/// its pattern listed `WEB[- ]?DL` first.
const source_tokens = [_][]const u8{
    "WEB-DL", "WEB DL", "WEBDL", "WEBRip", "WEB",
    "BluRay", "BDRip",  "BRRip", "HDRip",  "DVDRip",
    "HDTV",   "PDTV",   "REMUX",
};

fn isWordByte(c: u8) bool {
    return switch (c) {
        'a'...'z', 'A'...'Z', '0'...'9', '_' => true,
        else => false,
    };
}

/// Case-insensitive search for `needle` in `hay` at a position framed
/// by word boundaries — the `\b(...)\b` of the Go regexes. Returns the
/// matched slice of `hay` (preserving the input's casing, like
/// `regexp.FindString`) or null.
fn findToken(hay: []const u8, needle: []const u8) ?[]const u8 {
    if (needle.len == 0 or needle.len > hay.len) return null;
    var i: usize = 0;
    while (i + needle.len <= hay.len) : (i += 1) {
        if (!std.ascii.eqlIgnoreCase(hay[i..][0..needle.len], needle)) continue;
        // Leading boundary: only required when the token itself starts
        // with a word byte, which every token here does.
        if (i > 0 and isWordByte(hay[i - 1]) and isWordByte(needle[0])) continue;
        const end = i + needle.len;
        if (end < hay.len and isWordByte(hay[end]) and isWordByte(needle[needle.len - 1])) continue;
        return hay[i..end];
    }
    return null;
}

/// Leftmost match across a token table. `regexp` alternation picks the
/// earliest *position*, and within a position the earliest listed
/// alternative — reproduced here by tracking the winning offset.
fn findFirstToken(hay: []const u8, comptime tokens: []const []const u8) ?[]const u8 {
    var best: ?[]const u8 = null;
    var best_at: usize = std.math.maxInt(usize);
    for (tokens) |tok| {
        const hit = findToken(hay, tok) orelse continue;
        const at = @intFromPtr(hit.ptr) - @intFromPtr(hay.ptr);
        // Strictly-less keeps the table order as the tie-break, which is
        // what makes WEB-DL beat WEB at the same offset.
        if (at < best_at) {
            best_at = at;
            best = hit;
        }
    }
    return best;
}

/// Canonical casing/spacing for a source token, so `web-dl`, `WEB DL`
/// and `WEBDL` all render as `WEB-DL`. Unrecognised input is passed
/// through, which cannot happen for tokens from `source_tokens` but
/// keeps the function total.
fn normaliseSource(s: []const u8) []const u8 {
    // Compare with hyphens and spaces removed — that is the only thing
    // that varies between the spellings scene names use.
    var buf: [16]u8 = undefined;
    var n: usize = 0;
    for (s) |c| {
        if (c == '-' or c == ' ') continue;
        if (n == buf.len) return s;
        buf[n] = std.ascii.toLower(c);
        n += 1;
    }
    const key = buf[0..n];
    const table = .{
        .{ "webdl", "WEB-DL" },  .{ "webrip", "WEBRip" }, .{ "web", "WEB" },
        .{ "bluray", "BluRay" }, .{ "bdrip", "BDRip" },   .{ "brrip", "BRRip" },
        .{ "hdrip", "HDRip" },   .{ "dvdrip", "DVDRip" }, .{ "hdtv", "HDTV" },
        .{ "pdtv", "PDTV" },     .{ "remux", "REMUX" },
    };
    inline for (table) |row| {
        if (std.mem.eql(u8, key, row[0])) return row[1];
    }
    return s;
}

/// Short quality string parsed from a release name: `"WEB-DL 2160p"`,
/// `"BluRay 1080p"`, `"1080p"`, or `""` when neither a source nor a
/// resolution token is present. Mirrors Sonarr's quality string so the
/// two agree in a side-by-side notification.
pub fn parseQuality(arena: Allocator, release: []const u8) Allocator.Error![]const u8 {
    if (release.len == 0) return "";
    const src_raw = findFirstToken(release, &source_tokens);
    const res_raw = findFirstToken(release, &res_tokens);
    const src: []const u8 = if (src_raw) |s| normaliseSource(s) else "";
    if (res_raw == null) return src;

    // Resolutions are canonically lowercase ("2160p", "4k", "uhd").
    const res = try std.ascii.allocLowerString(arena, res_raw.?);
    if (src.len == 0) return res;
    return std.mem.concat(arena, u8, &.{ src, " ", res });
}

/// Turns the job's `source` — the requesting client's User-Agent,
/// captured at addfile time — into a friendly app name. Unknown agents
/// pass through verbatim so the operator can still tell who sent the
/// job; an empty agent means the UI uploaded the NZB directly.
pub fn sourceName(ua: []const u8) []const u8 {
    const s = std.mem.trim(u8, ua, ascii_space);
    if (s.len == 0) return "Manual";
    const table = .{
        .{ "sonarr", "Sonarr" },     .{ "radarr", "Radarr" },
        .{ "lidarr", "Lidarr" },     .{ "readarr", "Readarr" },
        .{ "prowlarr", "Prowlarr" }, .{ "whisparr", "Whisparr" },
    };
    inline for (table) |row| {
        if (containsIgnoreCase(s, row[0])) return row[1];
    }
    return ua;
}

/// `needle` must already be lowercase.
fn containsIgnoreCase(hay: []const u8, comptime needle: []const u8) bool {
    if (needle.len > hay.len) return false;
    var i: usize = 0;
    while (i + needle.len <= hay.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(hay[i..][0..needle.len], needle)) return true;
    }
    return false;
}

/// Widest output of `bytesHuman`.
pub const BytesBuf = [24]u8;

/// SAB-style byte rendering: `"12.34 GB"`, `"456 B"`. 1024-base (so
/// MiB-as-MB), matching every other wire formatter in hoardarr — the
/// SAB API's own numbers are 1024-base and a notification that
/// disagreed with the queue view would just look like a bug.
pub fn bytesHuman(buf: *BytesBuf, bytes: i64) []const u8 {
    const n: u64 = if (bytes < 0) 0 else @intCast(bytes);
    const k = 1024;
    const f: f64 = @floatFromInt(n);
    return if (n < k)
        std.fmt.bufPrint(buf, "{d} B", .{n}) catch unreachable
    else if (n < k * k)
        std.fmt.bufPrint(buf, "{d:.2} KB", .{f / k}) catch unreachable
    else if (n < k * k * k)
        std.fmt.bufPrint(buf, "{d:.2} MB", .{f / (k * k)}) catch unreachable
    else
        std.fmt.bufPrint(buf, "{d:.2} GB", .{f / (k * k * k)}) catch unreachable;
}

// ---------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------

/// "YYYY-MM-DDTHH:MM:SSZ"
pub const Rfc3339Buf = [20]u8;

/// RFC3339 at second precision, which is what Go's `time.RFC3339`
/// produced for the Discord `embed.timestamp` field. Second precision
/// rather than the millisecond form `core/log.zig` emits: Discord and
/// Slack both display these to the user, and a trailing `.000` in a
/// notification looks like a leaked debug string.
pub fn rfc3339(buf: *Rfc3339Buf, ms: event.Timestamp) []const u8 {
    const secs = @divFloor(ms, 1000);
    const days = @divFloor(secs, std.time.s_per_day);
    const sod: u64 = @intCast(@mod(secs, std.time.s_per_day));
    const d = log.civilFromDays(days);
    const year: u64 = if (d.year < 0) 0 else if (d.year > 9999) 9999 else @intCast(d.year);
    _ = std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year,       d.month,           d.day,
        sod / 3600, (sod % 3600) / 60, sod % 60,
    }) catch unreachable;
    return buf;
}

// ---------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------

/// JSON escape for U+FFFD, emitted in place of invalid UTF-8. The
/// escape form rather than the raw three bytes keeps the payload ASCII
/// when the input was ASCII-plus-garbage, which makes a captured
/// request body readable in a terminal.
const replacement = "\\ufffd";

fn writeUnicodeEscape(w: *std.Io.Writer, c: u8) std.Io.Writer.Error!void {
    const hex = "0123456789abcdef";
    try w.writeAll(&[_]u8{ '\\', 'u', '0', '0', hex[c >> 4], hex[c & 0xF] });
}

/// Emits `s` as a JSON string literal, both quotes included.
pub fn writeJsonString(w: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
    try w.writeByte('"');
    try writeJsonInner(w, s);
    try w.writeByte('"');
}

/// Escapes `s` into an *already open* JSON string — no quotes emitted.
/// The adapters need this because a Discord description is markdown
/// assembled around escaped fragments (`**verb**\n```release```), so
/// the string literal's quotes are written by the caller.
///
/// Total by construction: every byte either maps to a legal escape, is
/// copied as part of a validated UTF-8 sequence, or is replaced by
/// U+FFFD. There is no input — however hostile — that produces invalid
/// JSON or invalid UTF-8, which matters because release names reach
/// here straight from an NZB.
///
/// This is `core/log.zig`'s `putJsonString` algorithm over a growable
/// writer instead of its fixed line buffer. Kept as one algorithm on
/// purpose: two escaping dialects in one process is two things to audit
/// and one of them eventually rots.
pub fn writeJsonInner(w: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (c < 0x80) {
            switch (c) {
                '"' => try w.writeAll("\\\""),
                '\\' => try w.writeAll("\\\\"),
                '\n' => try w.writeAll("\\n"),
                '\r' => try w.writeAll("\\r"),
                '\t' => try w.writeAll("\\t"),
                0x08 => try w.writeAll("\\b"),
                0x0C => try w.writeAll("\\f"),
                // Everything else below 0x20 (NUL included) has no short
                // escape and must not appear raw in a JSON string.
                else => if (c < 0x20) try writeUnicodeEscape(w, c) else try w.writeByte(c),
            }
            i += 1;
            continue;
        }
        // Multi-byte: validate before copying. Copying a truncated or
        // overlong sequence would produce invalid UTF-8, which strict
        // JSON readers reject outright.
        const n = std.unicode.utf8ByteSequenceLength(c) catch {
            try w.writeAll(replacement);
            i += 1;
            continue;
        };
        if (i + n > s.len) {
            try w.writeAll(replacement);
            i += 1;
            continue;
        }
        _ = std.unicode.utf8Decode(s[i..][0..n]) catch {
            try w.writeAll(replacement);
            i += 1;
            continue;
        };
        try w.writeAll(s[i..][0..n]);
        i += n;
    }
}

/// `"key":` — the one place the key is known to be a literal, so it
/// skips escaping.
pub fn writeJsonKey(w: *std.Io.Writer, comptime key: []const u8) std.Io.Writer.Error!void {
    comptime {
        for (key) |c| {
            if (c < 0x20 or c == '"' or c == '\\' or c >= 0x7F) {
                @compileError("json key needs escaping: " ++ key);
            }
        }
    }
    try w.writeAll("\"" ++ key ++ "\":");
}

/// `"key":"value"` with the value escaped.
pub fn writeJsonField(
    w: *std.Io.Writer,
    comptime key: []const u8,
    value: []const u8,
) std.Io.Writer.Error!void {
    try writeJsonKey(w, key);
    try writeJsonString(w, value);
}

/// Caps a string at `n` bytes so a runaway error message cannot blow
/// past Discord's 1024-character per-field limit. The cut is by byte,
/// as Go's was — a UTF-8 sequence split by the cut is neutralised by
/// `writeJsonString`, so the payload stays valid either way.
pub fn truncate(s: []const u8, n: usize) []const u8 {
    if (s.len <= n) return s;
    return s[0 .. n - 1];
}

// ---------------------------------------------------------------------
// View
// ---------------------------------------------------------------------

/// Why a payload did not project cleanly. Never fatal: the adapters
/// still render the topic-derived fields and surface the diagnostic, so
/// a malformed event produces a notification that says what went wrong
/// rather than nothing at all (or half a message).
pub const Diagnostic = enum {
    /// The payload bytes are not JSON.
    invalid_json,
    /// Valid JSON, but not an object — there is nowhere for `event` /
    /// `job` to live.
    payload_not_object,
    /// Valid JSON object, but `job` / `event` were not objects either.
    unexpected_shape,

    pub fn message(self: Diagnostic) []const u8 {
        return switch (self) {
            .invalid_json => "event payload is not valid JSON",
            .payload_not_object => "event payload is not a JSON object",
            .unexpected_shape => "event payload has an unexpected shape",
        };
    }
};

/// The normalised projection of one event, ready for rendering. Fields
/// are string-typed on purpose: adapters pour them straight into their
/// widget structures and never re-parse.
pub const View = struct {
    /// Raw bus topic. Adapters put it in a footer for debugging.
    topic: []const u8 = "",
    /// Raw aggregate id — the fallback identity when no job hydrated.
    aggregate_id: []const u8 = "",
    occurred_at: event.Timestamp = 0,
    /// Human-friendly "what happened".
    verb: []const u8 = "",
    outcome: Outcome = .info,
    /// Raw release name, as it appears in the NZB.
    release: []const u8 = "",
    /// `release` with separators normalised and the group stripped.
    clean_title: []const u8 = "",
    /// "Sonarr", "Radarr", "Manual", or the raw User-Agent.
    source: []const u8 = "",
    /// Job category; empty means the catch-all.
    category: []const u8 = "",
    /// `job.total_bytes` formatted like "10.71 GB"; empty if unknown.
    size_human: []const u8 = "",
    /// Job state at event time.
    state: []const u8 = "",
    file_count: i64 = 0,
    /// Parsed quality marker; empty if nothing was detected.
    quality: []const u8 = "",
    /// Error message from the payload, for `.failed` topics.
    error_msg: []const u8 = "",
    /// Set when the payload could not be projected. See `Diagnostic`.
    diag: ?Diagnostic = null,

    /// Projects an envelope. `arena` backs every derived string; the
    /// borrowed ones point into `env`, so the result must not outlive
    /// the envelope.
    ///
    /// Both envelope shapes are accepted: enriched (original event
    /// under `event`, job snapshot under `job`, as
    /// `notify.Service.enrich` produces) and raw (payload at the top
    /// level, as `notify.test` produces).
    pub fn from(arena: Allocator, env: event.Envelope) Allocator.Error!View {
        var v: View = .{
            .topic = env.topic,
            .aggregate_id = env.aggregate_id,
            .occurred_at = env.occurred_at,
            .verb = verb(env.topic),
            .outcome = outcomeFor(env.topic),
        };
        if (env.payload.len == 0) return v;

        const root = std.json.parseFromSliceLeaky(
            std.json.Value,
            arena,
            env.payload,
            .{},
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            // Anything else is a malformed payload, not our problem to
            // fix — but the operator gets told.
            else => {
                v.diag = .invalid_json;
                return v;
            },
        };
        const raw = switch (root) {
            .object => |o| o,
            else => {
                v.diag = .payload_not_object;
                return v;
            },
        };

        // Enriched envelopes nest the original under "event" and the job
        // snapshot under "job"; un-enriched ones have the payload at the
        // top level. A present-but-wrongly-typed "event"/"job" is a
        // diagnostic rather than a silent fallback, because it means the
        // producer changed shape under us.
        var original = raw;
        if (raw.get("event")) |inner| switch (inner) {
            .object => |o| original = o,
            else => v.diag = .unexpected_shape,
        };
        var job: ?std.json.ObjectMap = null;
        if (raw.get("job")) |j| switch (j) {
            .object => |o| job = o,
            .null => {},
            else => v.diag = .unexpected_shape,
        };

        // Release name: prefer job.name, which is always set once
        // hydrated. Otherwise take the first of the keys raw producers
        // use.
        if (job) |j| {
            if (stringField(j, "name")) |s| v.release = s;
        }
        if (v.release.len == 0) {
            for ([_][]const u8{ "name", "filename", "release" }) |k| {
                if (stringField(original, k)) |s| {
                    if (s.len != 0) {
                        v.release = s;
                        break;
                    }
                }
            }
        }
        v.clean_title = try cleanReleaseName(arena, v.release);
        v.quality = try parseQuality(arena, v.release);

        if (job) |j| {
            if (stringField(j, "source")) |s| v.source = sourceName(s);
            if (stringField(j, "category")) |s| v.category = s;
            if (stringField(j, "state")) |s| v.state = s;
            if (numericField(j, "total_bytes")) |n| {
                if (n > 0) {
                    var buf: BytesBuf = undefined;
                    v.size_human = try arena.dupe(u8, bytesHuman(&buf, n));
                }
            }
            if (numericField(j, "file_count")) |n| v.file_count = n;
        }
        if (v.source.len == 0) v.source = "Manual";

        // Failure topics: surface the reason. Producers are inconsistent
        // about the key — accept all of them.
        for ([_][]const u8{ "err", "error", "fail_message", "message" }) |k| {
            if (stringField(original, k)) |s| {
                if (s.len != 0) {
                    v.error_msg = s;
                    break;
                }
            }
        }
        return v;
    }
};

fn stringField(m: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const v = m.get(key) orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

/// JSON numbers land as `.integer` when they fit an i64, `.float` when
/// they were written with a fraction, and `.number_string` when they
/// overflow i64. All three are accepted; a float is truncated the way
/// Go's `int64(n)` truncated a `float64`.
fn numericField(m: std.json.ObjectMap, key: []const u8) ?i64 {
    const v = m.get(key) orelse return null;
    return switch (v) {
        .integer => |n| n,
        .float => |f| if (std.math.isFinite(f) and
            f >= @as(f64, @floatFromInt(std.math.minInt(i64))) and
            f <= @as(f64, @floatFromInt(std.math.maxInt(i64))))
            @intFromFloat(f)
        else
            null,
        .number_string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Arena over the testing allocator: the arena is what `View.from`
/// wants, and the testing allocator underneath still fails the test on
/// a leak.
/// Envelope with the bus-supplied fields filled in, so a test only
/// states the topic and payload it cares about.
fn testEnv(topic: []const u8, payload: []const u8) event.Envelope {
    return .{
        .id = .nil,
        .topic = topic,
        .aggregate_id = "1",
        .occurred_at = 1_700_000_000_000,
        .payload = payload,
    };
}

const TestArena = struct {
    a: std.heap.ArenaAllocator,

    fn init() TestArena {
        return .{ .a = std.heap.ArenaAllocator.init(testing.allocator) };
    }
    fn deinit(self: *TestArena) void {
        self.a.deinit();
    }
    fn alloc(self: *TestArena) Allocator {
        return self.a.allocator();
    }
};

test "verb maps known topics and passes unknown ones through" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "download.job.created", "Added to queue" },
        .{ "download.job.completed", "Completed" },
        .{ "download.job.failed", "Failed" },
        .{ "verify.ok", "Verified" },
        .{ "verify.repair_needed", "Repair needed" },
        .{ "deliver.complete", "Delivered" },
        .{ "deliver.failed", "Delivery failed" },
        .{ "notify.test", "Test notification" },
        .{ "unknown.topic", "unknown.topic" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], verb(c[0]));
}

test "outcomeFor classifies by suffix" {
    const cases = [_]struct { []const u8, Outcome }{
        .{ "verify.ok", .ok },
        .{ "repair.failed", .fail },
        .{ "download.job.failed", .fail },
        .{ "download.job.download_failed", .fail },
        .{ "verify.repair_needed", .warn },
        .{ "deliver.complete", .ok },
        .{ "download.job.created", .info },
        .{ "download.job.completed", .ok },
        .{ "notify.test", .info },
    };
    for (cases) |c| try testing.expectEqual(c[1], outcomeFor(c[0]));
}

test "outcome palette has not drifted" {
    try testing.expectEqual(@as(u32, 0x2ECC71), Outcome.ok.color());
    try testing.expectEqual(@as(u32, 0xF85149), Outcome.fail.color());
    try testing.expectEqual(@as(u32, 0xD29922), Outcome.warn.color());
    try testing.expectEqual(@as(u32, 0x4A90E2), Outcome.info.color());
}

test "cleanReleaseName strips the group and normalises separators" {
    var ta = TestArena.init();
    defer ta.deinit();
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "", "" },
        .{ "Foo.Bar.S01E02.1080p.WEB-RLS", "Foo Bar S01E02 1080p WEB" },
        .{
            "Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson",
            "Euphoria US S03E06 PROPER MULTi DV HDR 2160p WEB H265",
        },
        // A hyphen inside the title: the suffix has spaces, so it is not
        // a group and nothing is stripped.
        .{ "Some.Title - The Movie.2024.1080p", "Some Title - The Movie 2024 1080p" },
        .{ "foo_bar_2024-RLSGRP", "foo bar 2024" },
        // Only the last hyphen is considered — one strip, not two.
        .{ "Foo.Bar.S01E02.1080p.WEB-DL-RLSGRP", "Foo Bar S01E02 1080p WEB-DL" },
    };
    for (cases) |c| {
        try testing.expectEqualStrings(c[1], try cleanReleaseName(ta.alloc(), c[0]));
    }
}

test "cleanReleaseName survives degenerate input" {
    var ta = TestArena.init();
    defer ta.deinit();
    // All-separator, trailing hyphen, lone hyphen, only whitespace.
    try testing.expectEqualStrings("", try cleanReleaseName(ta.alloc(), "...___..."));
    try testing.expectEqualStrings("a-", try cleanReleaseName(ta.alloc(), "a-"));
    try testing.expectEqualStrings("", try cleanReleaseName(ta.alloc(), "   \t "));
    try testing.expectEqualStrings("", try cleanReleaseName(ta.alloc(), "-G"));
    // A very long group-shaped suffix (>24) is not a group.
    const long = "Title-" ++ ("A" ** 30);
    try testing.expectEqualStrings(long, try cleanReleaseName(ta.alloc(), long));
}

test "parseQuality mirrors Sonarr's quality string" {
    var ta = TestArena.init();
    defer ta.deinit();
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "Foo.S01E01.2160p.WEB-DL.H265-X", "WEB-DL 2160p" },
        .{ "Foo.S01E01.1080p.BluRay-X", "BluRay 1080p" },
        .{ "Foo.720p.HDTV-X", "HDTV 720p" },
        .{ "Foo.WEBRip-X", "WEBRip" },
        .{ "Foo.2024.1080p", "1080p" },
        .{ "Foo", "" },
        .{ "", "" },
        // Lowercase and space-separated spellings canonicalise.
        .{ "foo.2160p.web dl", "WEB-DL 2160p" },
        .{ "foo.webdl.4K", "WEB-DL 4k" },
        // A token embedded in a word is not a token: WEBBED has no
        // boundary after WEB, 21080p has none before 1080p.
        .{ "Foo.WEBBED.21080p", "" },
        // REMUX and UHD.
        .{ "Foo.UHD.REMUX", "REMUX uhd" },
    };
    for (cases) |c| {
        try testing.expectEqualStrings(c[1], try parseQuality(ta.alloc(), c[0]));
    }
}

test "sourceName recognises the *arr family" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "", "Manual" },
        .{ "   ", "Manual" },
        .{ "Sonarr/4.0.0.123", "Sonarr" },
        .{ "radarr/5.0", "Radarr" },
        .{ "Prowlarr/1.10", "Prowlarr" },
        .{ "Lidarr/2", "Lidarr" },
        .{ "readarr", "Readarr" },
        .{ "Whisparr/1", "Whisparr" },
        .{ "unknownclient/1.0", "unknownclient/1.0" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], sourceName(c[0]));
}

test "bytesHuman is 1024-base and matches the SAB formatter" {
    var buf: BytesBuf = undefined;
    try testing.expectEqualStrings("0 B", bytesHuman(&buf, 0));
    try testing.expectEqualStrings("500 B", bytesHuman(&buf, 500));
    try testing.expectEqualStrings("2.00 KB", bytesHuman(&buf, 2 * 1024));
    try testing.expectEqualStrings("5.00 MB", bytesHuman(&buf, 5 * 1024 * 1024));
    try testing.expectEqualStrings("10.71 GB", bytesHuman(&buf, 11_500_000_000));
    try testing.expectEqualStrings("5.00 GB", bytesHuman(&buf, 5_368_709_120));
    // Negative clamps rather than wrapping.
    try testing.expectEqualStrings("0 B", bytesHuman(&buf, -1));
    // The widest case still fits BytesBuf.
    _ = bytesHuman(&buf, std.math.maxInt(i64));
}

test "rfc3339 renders at second precision" {
    var buf: Rfc3339Buf = undefined;
    try testing.expectEqualStrings("2023-11-14T22:13:20Z", rfc3339(&buf, 1_700_000_000_000));
    try testing.expectEqualStrings("1970-01-01T00:00:00Z", rfc3339(&buf, 0));
    // Sub-second components are dropped, not rounded.
    try testing.expectEqualStrings("2023-11-14T22:13:20Z", rfc3339(&buf, 1_700_000_000_999));
}

test "writeJsonString escapes control characters, quotes and backslashes" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try writeJsonString(&out.writer, "a\"b\\c\nd\te\r\x00f\x1F");
    try testing.expectEqualStrings(
        "\"a\\\"b\\\\c\\nd\\te\\r\\u0000f\\u001f\"",
        out.written(),
    );
}

test "writeJsonString replaces invalid UTF-8 and keeps valid sequences" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    // Lone continuation byte, truncated 3-byte sequence, then a valid
    // 2-byte sequence and a valid 4-byte one.
    try writeJsonString(&out.writer, "\x80" ++ "\xE2\x82" ++ "é" ++ "🎬");
    try testing.expectEqualStrings(
        "\"\\ufffd\\ufffd\\ufffdé🎬\"",
        out.written(),
    );
    try testing.expect(std.unicode.utf8ValidateSlice(out.written()));
}

test "writeJsonString output parses as JSON for adversarial bytes" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    // Every byte value, twice, plus JSON metacharacters.
    var hostile: [512 + 8]u8 = undefined;
    for (0..512) |i| hostile[i] = @intCast(i % 256);
    @memcpy(hostile[512..], "\"}{[],:\\");

    try writeJsonString(&out.writer, &hostile);
    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        testing.allocator,
        out.written(),
        .{},
    );
    defer parsed.deinit();
    try testing.expect(parsed.value == .string);
    try testing.expect(std.unicode.utf8ValidateSlice(out.written()));
}

test "truncate caps by byte and leaves short strings alone" {
    try testing.expectEqualStrings("abc", truncate("abc", 10));
    try testing.expectEqualStrings("abc", truncate("abc", 3));
    try testing.expectEqualStrings("abcd", truncate("abcdef", 5));
    try testing.expectEqualStrings("a", truncate("abcdef", 2));
}

test "View.from projects an enriched envelope" {
    var ta = TestArena.init();
    defer ta.deinit();
    const payload =
        \\{
        \\  "event": {"job_id": 148},
        \\  "job": {
        \\    "id": 148,
        \\    "name": "Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson",
        \\    "category": "tv",
        \\    "state": "completed",
        \\    "source": "Sonarr/4.0.0.123",
        \\    "total_bytes": 11500000000,
        \\    "file_count": 7
        \\  }
        \\}
    ;
    const v = try View.from(ta.alloc(), .{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "148",
        .occurred_at = 1_700_000_000_000,
        .payload = payload,
    });
    try testing.expectEqual(@as(?Diagnostic, null), v.diag);
    try testing.expectEqualStrings("Delivered", v.verb);
    try testing.expectEqual(Outcome.ok, v.outcome);
    try testing.expectEqualStrings("Sonarr", v.source);
    try testing.expectEqualStrings("tv", v.category);
    try testing.expectEqualStrings("10.71 GB", v.size_human);
    try testing.expectEqual(@as(i64, 7), v.file_count);
    try testing.expectEqualStrings("completed", v.state);
    try testing.expectEqualStrings(
        "Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson",
        v.release,
    );
    try testing.expectEqualStrings(
        "Euphoria US S03E06 PROPER MULTi DV HDR 2160p WEB H265",
        v.clean_title,
    );
    try testing.expectEqualStrings("WEB 2160p", v.quality);
}

test "View.from handles an un-hydrated envelope" {
    var ta = TestArena.init();
    defer ta.deinit();
    const v = try View.from(ta.alloc(), .{
        .id = .nil,
        .topic = "notify.test",
        .aggregate_id = "0",
        .occurred_at = 1_700_000_000_000,
        .payload = "{\"name\":\"smoke probe\"}",
    });
    try testing.expectEqualStrings("Test notification", v.verb);
    try testing.expectEqualStrings("smoke probe", v.release);
    try testing.expectEqualStrings("Manual", v.source);
    try testing.expectEqualStrings("", v.size_human);
    try testing.expectEqual(@as(i64, 0), v.file_count);
}

test "View.from surfaces the failure reason under any of the keys" {
    var ta = TestArena.init();
    defer ta.deinit();
    const keys = [_][]const u8{ "err", "error", "fail_message", "message" };
    for (keys) |k| {
        var buf: [256]u8 = undefined;
        const payload = try std.fmt.bufPrint(
            &buf,
            "{{\"event\":{{\"{s}\":\"target FS read-only\"}},\"job\":{{\"name\":\"X.S01E01-G\"}}}}",
            .{k},
        );
        const v = try View.from(ta.alloc(), testEnv("deliver.failed", payload));
        try testing.expectEqual(Outcome.fail, v.outcome);
        try testing.expectEqualStrings("target FS read-only", v.error_msg);
    }
}

test "View.from reports a malformed payload instead of failing" {
    var ta = TestArena.init();
    defer ta.deinit();
    const bad = [_]struct { []const u8, Diagnostic }{
        .{ "not json at all", .invalid_json },
        .{ "{\"unterminated\": ", .invalid_json },
        .{ "[1,2,3]", .payload_not_object },
        .{ "\"a bare string\"", .payload_not_object },
        .{ "42", .payload_not_object },
        .{ "{\"job\": 7}", .unexpected_shape },
        .{ "{\"event\": \"nope\"}", .unexpected_shape },
    };
    for (bad) |c| {
        const v = try View.from(ta.alloc(), testEnv("deliver.complete", c[0]));
        try testing.expectEqual(@as(?Diagnostic, c[1]), v.diag);
        // Topic-derived fields still render — never a half-view.
        try testing.expectEqualStrings("Delivered", v.verb);
        try testing.expectEqual(Outcome.ok, v.outcome);
        try testing.expect(c[1].message().len > 0);
    }
}

test "View.from tolerates an empty payload" {
    var ta = TestArena.init();
    defer ta.deinit();
    const v = try View.from(ta.alloc(), testEnv("verify.ok", ""));
    try testing.expectEqual(@as(?Diagnostic, null), v.diag);
    try testing.expectEqualStrings("Verified", v.verb);
    // No payload means no source lookup happened, so no "Manual"
    // default either — the field is simply absent, as in Go.
    try testing.expectEqualStrings("", v.source);
}

test "View.from accepts float, string and oversized numbers" {
    var ta = TestArena.init();
    defer ta.deinit();
    // total_bytes as a float, file_count as a float, and a byte count
    // beyond i64 which must not be trusted into a wrong number.
    const v = try View.from(ta.alloc(), testEnv(
        "verify.ok",
        "{\"job\":{\"total_bytes\":2048.0,\"file_count\":3.9}}",
    ));
    try testing.expectEqualStrings("2.00 KB", v.size_human);
    try testing.expectEqual(@as(i64, 3), v.file_count);

    const huge = try View.from(ta.alloc(), testEnv(
        "verify.ok",
        "{\"job\":{\"total_bytes\":99999999999999999999999}}",
    ));
    try testing.expectEqualStrings("", huge.size_human);
}

test "View.from keeps a hostile release name intact and unescaped" {
    var ta = TestArena.init();
    defer ta.deinit();
    // Quotes and a backslash arrive JSON-escaped and must come back out
    // as the raw bytes — escaping is the adapters' job, not the view's.
    const v = try View.from(ta.alloc(), testEnv(
        "deliver.complete",
        "{\"job\":{\"name\":\"a\\\"b\\\\c.1080p-G\"}}",
    ));
    try testing.expectEqualStrings("a\"b\\c.1080p-G", v.release);
    try testing.expectEqualStrings("a\"b\\c 1080p", v.clean_title);
}
