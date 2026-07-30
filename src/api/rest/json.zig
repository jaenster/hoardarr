//! JSON output for the REST layer: a small append-only writer.
//!
//! ## Why not `std.json.stringify`
//!
//! Every response body here is assembled from domain aggregates whose
//! shape does not match the wire shape — `omitempty` fields, `*time.Time`
//! rendered as RFC 3339 or absent, nested arrays hydrated lazily. Driving
//! that through a reflection-based encoder means declaring a mirror
//! struct per endpoint and then fighting it over the optional cases. An
//! append-only writer is less code, allocates once per response into an
//! arena, and keeps the wire contract visible in the handler.
//!
//! ## Escaping
//!
//! `appendString` is the same dialect as `putJsonString` in
//! `core/log.zig`, byte for byte: the seven short escapes, `\u00xx` for
//! every other control byte, and UTF-8 validated before it is copied so a
//! truncated, overlong or surrogate-encoding sequence becomes U+FFFD
//! rather than invalid output.
//!
//! That last part is not theoretical. Release names come out of NZB files
//! downloaded from strangers; `Segment.last_error` can carry bytes from a
//! server's greeting. If a single one of those reached the browser
//! unescaped the response would be unparseable at best, and at worst the
//! attacker would be choosing JSON structure. There is no input to
//! `appendString` — no matter how hostile — that produces anything but a
//! valid JSON string.

const std = @import("std");
const log = @import("../../core/log.zig");

const Allocator = std.mem.Allocator;

/// U+FFFD, for bytes that cannot be part of a valid sequence.
const replacement = "\u{FFFD}";

/// Nesting the writer can track. Deepest response is
/// object → array → object → array → object (job → files → segments), so
/// 8 is double what any endpoint needs and keeps the writer a fixed-size
/// struct.
pub const max_depth = 8;

pub const Error = Allocator.Error;

/// Growable JSON document. The buffer is the caller's arena in practice:
/// `deinit` exists for the test path and for a handler that wants the
/// memory back before it returns.
pub const Writer = struct {
    gpa: Allocator,
    buf: std.ArrayList(u8) = .empty,
    /// One flag per open container: has anything been emitted into it
    /// yet? Drives the separating comma without a lookbehind on the
    /// buffer, which would misfire on a string ending in '{'.
    empty: [max_depth]bool = @splat(true),
    depth: usize = 0,

    pub fn init(gpa: Allocator) Writer {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Writer) void {
        self.buf.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn items(self: *const Writer) []const u8 {
        return self.buf.items;
    }

    /// Drop the contents, keep the capacity. Lets one writer serve a
    /// whole connection's worth of responses with a single allocation.
    pub fn reset(self: *Writer) void {
        self.buf.clearRetainingCapacity();
        self.depth = 0;
        self.empty = @splat(true);
    }

    // -- containers ---------------------------------------------------

    pub fn beginObject(self: *Writer) Error!void {
        try self.separate();
        try self.buf.append(self.gpa, '{');
        self.push();
    }

    pub fn endObject(self: *Writer) Error!void {
        self.pop();
        try self.buf.append(self.gpa, '}');
    }

    pub fn beginArray(self: *Writer) Error!void {
        try self.separate();
        try self.buf.append(self.gpa, '[');
        self.push();
    }

    pub fn endArray(self: *Writer) Error!void {
        self.pop();
        try self.buf.append(self.gpa, ']');
    }

    /// `"name":` — the next value written lands in this member.
    pub fn key(self: *Writer, name: []const u8) Error!void {
        try self.separate();
        try appendString(&self.buf, self.gpa, name);
        try self.buf.append(self.gpa, ':');
        // The value that follows must not emit another comma.
        self.empty[self.depth - 1] = true;
    }

    // -- scalars ------------------------------------------------------

    pub fn string(self: *Writer, v: []const u8) Error!void {
        try self.separate();
        try appendString(&self.buf, self.gpa, v);
    }

    pub fn int(self: *Writer, v: i64) Error!void {
        try self.separate();
        var tmp: [24]u8 = undefined;
        try self.buf.appendSlice(self.gpa, std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable);
    }

    pub fn uint(self: *Writer, v: u64) Error!void {
        try self.separate();
        var tmp: [24]u8 = undefined;
        try self.buf.appendSlice(self.gpa, std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable);
    }

    /// JSON has no NaN and no Infinity, so a non-finite value degrades to
    /// `null` — the same choice `core/log.zig` makes. Emitting `NaN`
    /// would produce a body no parser accepts.
    pub fn float(self: *Writer, v: f64) Error!void {
        try self.separate();
        if (!std.math.isFinite(v)) {
            try self.buf.appendSlice(self.gpa, "null");
            return;
        }
        var tmp: [64]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "{d}", .{v}) catch {
            try self.buf.appendSlice(self.gpa, "null");
            return;
        };
        try self.buf.appendSlice(self.gpa, s);
    }

    pub fn boolean(self: *Writer, v: bool) Error!void {
        try self.separate();
        try self.buf.appendSlice(self.gpa, if (v) "true" else "false");
    }

    pub fn nul(self: *Writer) Error!void {
        try self.separate();
        try self.buf.appendSlice(self.gpa, "null");
    }

    /// Splice in a document that is already JSON — an outbox payload, a
    /// stored command body. **Not** validated or escaped: the only
    /// callers are ports whose contract says the bytes are JSON, and
    /// `rawOrNull` is there for when that cannot be assumed.
    pub fn raw(self: *Writer, v: []const u8) Error!void {
        try self.separate();
        try self.buf.appendSlice(self.gpa, v);
    }

    /// `raw` with a validity check: anything that is not parseable JSON
    /// becomes `null` rather than corrupting the response around it.
    /// Used for values that came from the database, where a bad row must
    /// not be able to break every other row in the list.
    pub fn rawOrNull(self: *Writer, v: []const u8, scratch: Allocator) Error!void {
        if (v.len == 0 or !isValidJson(v, scratch)) return self.nul();
        return self.raw(v);
    }

    // -- fields (key + value in one call) -----------------------------

    pub fn strField(self: *Writer, name: []const u8, v: []const u8) Error!void {
        try self.key(name);
        try self.string(v);
    }

    /// Go's `,omitempty` on a string: absent when empty.
    pub fn optStrField(self: *Writer, name: []const u8, v: []const u8) Error!void {
        if (v.len == 0) return;
        try self.strField(name, v);
    }

    pub fn intField(self: *Writer, name: []const u8, v: i64) Error!void {
        try self.key(name);
        try self.int(v);
    }

    pub fn uintField(self: *Writer, name: []const u8, v: u64) Error!void {
        try self.key(name);
        try self.uint(v);
    }

    pub fn floatField(self: *Writer, name: []const u8, v: f64) Error!void {
        try self.key(name);
        try self.float(v);
    }

    pub fn boolField(self: *Writer, name: []const u8, v: bool) Error!void {
        try self.key(name);
        try self.boolean(v);
    }

    pub fn rawField(self: *Writer, name: []const u8, v: []const u8) Error!void {
        try self.key(name);
        try self.raw(v);
    }

    /// A unix-millisecond instant as RFC 3339, which is what Go's
    /// `time.Time` marshalled to and what the frontend's `new Date(...)`
    /// expects.
    pub fn timeField(self: *Writer, name: []const u8, ms: i64) Error!void {
        var buf: [log.ts_len]u8 = undefined;
        try self.strField(name, log.formatTimestamp(&buf, @as(i128, ms) * std.time.ns_per_ms));
    }

    /// Go's `*time.Time` with `,omitempty`: absent for null or zero.
    pub fn optTimeField(self: *Writer, name: []const u8, ms: ?i64) Error!void {
        const v = ms orelse return;
        if (v == 0) return;
        try self.timeField(name, v);
    }

    pub fn strArrayField(self: *Writer, name: []const u8, vs: []const []const u8) Error!void {
        try self.key(name);
        try self.beginArray();
        for (vs) |v| try self.string(v);
        try self.endArray();
    }

    pub fn intArrayField(self: *Writer, name: []const u8, vs: []const i64) Error!void {
        try self.key(name);
        try self.beginArray();
        for (vs) |v| try self.int(v);
        try self.endArray();
    }

    // -- internals ----------------------------------------------------

    fn separate(self: *Writer) Error!void {
        if (self.depth == 0) return;
        if (self.empty[self.depth - 1]) {
            self.empty[self.depth - 1] = false;
            return;
        }
        try self.buf.append(self.gpa, ',');
    }

    fn push(self: *Writer) void {
        // A document nested deeper than `max_depth` is a bug in the
        // handler, not a runtime condition: the shapes are all literal.
        std.debug.assert(self.depth < max_depth);
        self.empty[self.depth] = true;
        self.depth += 1;
    }

    fn pop(self: *Writer) void {
        std.debug.assert(self.depth > 0);
        self.depth -= 1;
        // The closed container counts as content of its parent.
        if (self.depth > 0) self.empty[self.depth - 1] = false;
    }
};

// ---------------------------------------------------------------------
// String escaping
// ---------------------------------------------------------------------

/// Append `s` as a JSON string literal, quotes included.
///
/// Total by construction — see the module comment. This is the same
/// algorithm as `putJsonString` in `core/log.zig`; the difference is that
/// this one grows instead of truncating, so there is no reserve to keep
/// for the closing quote.
pub fn appendString(buf: *std.ArrayList(u8), gpa: Allocator, s: []const u8) Error!void {
    try buf.ensureUnusedCapacity(gpa, s.len + 2);
    buf.appendAssumeCapacity('"');
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (c < 0x80) {
            switch (c) {
                '"' => try buf.appendSlice(gpa, "\\\""),
                '\\' => try buf.appendSlice(gpa, "\\\\"),
                '\n' => try buf.appendSlice(gpa, "\\n"),
                '\r' => try buf.appendSlice(gpa, "\\r"),
                '\t' => try buf.appendSlice(gpa, "\\t"),
                0x08 => try buf.appendSlice(gpa, "\\b"),
                0x0C => try buf.appendSlice(gpa, "\\f"),
                // Everything else below 0x20 (NUL included) has no short
                // escape and must not appear raw inside a JSON string.
                else => if (c < 0x20)
                    try appendUnicodeEscape(buf, gpa, c)
                else
                    try buf.append(gpa, c),
            }
            i += 1;
            continue;
        }
        // Multi-byte: validate before copying. Copying unchecked would
        // emit invalid UTF-8, which strict readers reject outright.
        const n = std.unicode.utf8ByteSequenceLength(c) catch {
            try buf.appendSlice(gpa, replacement);
            i += 1;
            continue;
        };
        if (i + n > s.len) {
            try buf.appendSlice(gpa, replacement);
            i += 1;
            continue;
        }
        _ = std.unicode.utf8Decode(s[i..][0..n]) catch {
            try buf.appendSlice(gpa, replacement);
            i += 1;
            continue;
        };
        try buf.appendSlice(gpa, s[i..][0..n]);
        i += n;
    }
    try buf.append(gpa, '"');
}

fn appendUnicodeEscape(buf: *std.ArrayList(u8), gpa: Allocator, c: u8) Error!void {
    const hex = "0123456789abcdef";
    try buf.appendSlice(gpa, &[_]u8{ '\\', 'u', '0', '0', hex[c >> 4], hex[c & 0xF] });
}

/// `{"error":"<escaped>"}`, allocated. The one error shape the REST API
/// emits, matching what the Go handlers wrote.
pub fn errorDocument(gpa: Allocator, message: []const u8) Error![]u8 {
    var w = Writer.init(gpa);
    errdefer w.deinit();
    try w.beginObject();
    try w.strField("error", message);
    try w.endObject();
    return w.buf.toOwnedSlice(gpa);
}

fn isValidJson(v: []const u8, scratch: Allocator) bool {
    var arena = std.heap.ArenaAllocator.init(scratch);
    defer arena.deinit();
    _ = std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), v, .{}) catch return false;
    return true;
}

// ---------------------------------------------------------------------
// Request-body reading
// ---------------------------------------------------------------------

/// Field lookup over a parsed request body. Everything is optional
/// because a request body is attacker-supplied: a handler asks for what
/// it wants and gets null if the client sent something else, rather than
/// a type error it would have to translate.
///
/// Absent and null are deliberately *not* distinguished for scalars —
/// both mean "not supplied", which is what Go's decode-into-pointer gave
/// for the PATCH handlers. `has` is there for the one case that cares.
pub const Body = struct {
    root: std.json.Value,

    pub const ParseError = error{Malformed} || Allocator.Error;

    /// Parses `bytes` into `arena`. An empty body is an empty object, so
    /// a PATCH with no fields is a no-op instead of a 400 — Go's
    /// `json.Decode` rejected it, but every field is optional anyway and
    /// the frontend does send `{}`.
    pub fn parse(arena: Allocator, bytes: []const u8) ParseError!Body {
        const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
        if (trimmed.len == 0) return Body.parse(arena, "{}");
        const v = std.json.parseFromSliceLeaky(std.json.Value, arena, trimmed, .{}) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.Malformed,
            };
        };
        // A body that is not an object cannot satisfy any handler here,
        // and reporting it now gives a clearer 400 than every field
        // coming back null would.
        if (v != .object) return error.Malformed;
        return .{ .root = v };
    }

    pub fn has(self: Body, name: []const u8) bool {
        return self.root.object.contains(name);
    }

    pub fn string(self: Body, name: []const u8) ?[]const u8 {
        const v = self.root.object.get(name) orelse return null;
        return switch (v) {
            .string => |s| s,
            else => null,
        };
    }

    /// A string field with surrounding ASCII whitespace removed, which is
    /// what the Go handlers did with `strings.TrimSpace` before their
    /// "required" checks.
    pub fn trimmedString(self: Body, name: []const u8) ?[]const u8 {
        const s = self.string(name) orelse return null;
        return std.mem.trim(u8, s, " \t\r\n");
    }

    /// Accepts a JSON number and also a numeric string, because the
    /// frontend's number inputs submit strings when they are bound to
    /// text fields.
    pub fn int(self: Body, name: []const u8) ?i64 {
        const v = self.root.object.get(name) orelse return null;
        return switch (v) {
            .integer => |i| i,
            .float => |f| if (std.math.isFinite(f)) @as(i64, @intFromFloat(f)) else null,
            .number_string, .string => |s| std.fmt.parseInt(i64, s, 10) catch null,
            else => null,
        };
    }

    pub fn float(self: Body, name: []const u8) ?f64 {
        const v = self.root.object.get(name) orelse return null;
        return switch (v) {
            .float => |f| f,
            .integer => |i| @floatFromInt(i),
            .number_string, .string => |s| std.fmt.parseFloat(f64, s) catch null,
            else => null,
        };
    }

    pub fn boolean(self: Body, name: []const u8) ?bool {
        const v = self.root.object.get(name) orelse return null;
        return switch (v) {
            .bool => |b| b,
            else => null,
        };
    }

    /// Array of strings, allocated in `arena`. A non-array, or an array
    /// with a non-string in it, yields null: partial acceptance of a
    /// topic list would silently subscribe to the wrong thing.
    pub fn stringArray(self: Body, arena: Allocator, name: []const u8) Allocator.Error!?[]const []const u8 {
        const v = self.root.object.get(name) orelse return null;
        if (v != .array) return null;
        const src = v.array.items;
        const out = try arena.alloc([]const u8, src.len);
        for (src, 0..) |item, i| {
            if (item != .string) return null;
            out[i] = item.string;
        }
        return out;
    }

    pub fn intArray(self: Body, arena: Allocator, name: []const u8) Allocator.Error!?[]const i64 {
        const v = self.root.object.get(name) orelse return null;
        if (v != .array) return null;
        const src = v.array.items;
        const out = try arena.alloc(i64, src.len);
        for (src, 0..) |item, i| {
            out[i] = switch (item) {
                .integer => |n| n,
                .float => |f| if (std.math.isFinite(f)) @as(i64, @intFromFloat(f)) else return null,
                .number_string, .string => |s| std.fmt.parseInt(i64, s, 10) catch return null,
                else => return null,
            };
        }
        return out;
    }

    /// The raw JSON text of a member, re-encoded. Used for the opaque
    /// `body` of a submitted command, which the API forwards verbatim.
    pub fn rawJson(self: Body, arena: Allocator, name: []const u8) Allocator.Error!?[]const u8 {
        const v = self.root.object.get(name) orelse return null;
        if (v == .null) return null;
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(arena);
        var w: std.Io.Writer.Allocating = .fromArrayList(arena, &out);
        std.json.Stringify.value(v, .{}, &w.writer) catch return error.OutOfMemory;
        return w.written();
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Assert the document parses, then hand back the parsed tree so a test
/// can check values rather than byte-compare a string it just wrote.
fn parseDoc(arena: Allocator, doc: []const u8) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, arena, doc, .{});
}

test "an object with mixed scalars" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.beginObject();
    try w.strField("name", "Release.Name");
    try w.intField("id", -7);
    try w.uintField("bytes", 18446744073709551615);
    try w.boolField("enabled", true);
    try w.floatField("ratio", 0.5);
    try w.key("missing");
    try w.nul();
    try w.endObject();

    try testing.expectEqualStrings(
        "{\"name\":\"Release.Name\",\"id\":-7,\"bytes\":18446744073709551615," ++
            "\"enabled\":true,\"ratio\":0.5,\"missing\":null}",
        w.items(),
    );
}

test "nested containers separate correctly" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.beginObject();
    try w.key("jobs");
    try w.beginArray();
    for (0..2) |i| {
        try w.beginObject();
        try w.intField("id", @intCast(i));
        try w.key("files");
        try w.beginArray();
        try w.beginObject();
        try w.strField("filename", "a.rar");
        try w.endObject();
        try w.endArray();
        try w.endObject();
    }
    try w.endArray();
    try w.boolField("truncated", false);
    try w.endObject();

    try testing.expectEqualStrings(
        "{\"jobs\":[" ++
            "{\"id\":0,\"files\":[{\"filename\":\"a.rar\"}]}," ++
            "{\"id\":1,\"files\":[{\"filename\":\"a.rar\"}]}" ++
            "],\"truncated\":false}",
        w.items(),
    );

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const v = try parseDoc(arena.allocator(), w.items());
    try testing.expectEqual(@as(usize, 2), v.object.get("jobs").?.array.items.len);
}

test "empty containers" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.beginObject();
    try w.key("jobs");
    try w.beginArray();
    try w.endArray();
    try w.key("meta");
    try w.beginObject();
    try w.endObject();
    try w.endObject();
    try testing.expectEqualStrings("{\"jobs\":[],\"meta\":{}}", w.items());
}

test "omitempty fields are absent rather than empty" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.beginObject();
    try w.optStrField("error", "");
    try w.optStrField("source", "Sonarr/4.0");
    try w.optTimeField("started_at", null);
    try w.optTimeField("finished_at", 0);
    try w.optTimeField("added_at", 1_700_000_000_000);
    try w.endObject();
    try testing.expectEqualStrings(
        "{\"source\":\"Sonarr/4.0\",\"added_at\":\"2023-11-14T22:13:20.000Z\"}",
        w.items(),
    );
}

test "non-finite floats degrade to null instead of breaking the document" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var w = Writer.init(arena.allocator());
    try w.beginObject();
    try w.floatField("nan", std.math.nan(f64));
    try w.floatField("inf", std.math.inf(f64));
    try w.floatField("ninf", -std.math.inf(f64));
    try w.endObject();
    const v = try parseDoc(arena.allocator(), w.items());
    try testing.expect(v.object.get("nan").? == .null);
    try testing.expect(v.object.get("inf").? == .null);
    try testing.expect(v.object.get("ninf").? == .null);
}

// -- escaping: the hostile cases --------------------------------------

test "escaping survives adversarial values and std.json agrees" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const hostile = [_][]const u8{
        "\"",
        "\\",
        "\",\"admin\":true",
        "\\\"",
        "}{",
        "line\nbreak",
        "carriage\rreturn",
        "tab\there",
        "\x00nul",
        "\x01\x02\x03\x1f",
        "bell\x07and\x08back\x0cspace",
        "del\x7f",
        // Invalid UTF-8 of every flavour.
        "\xff\xfe",
        "\x80continuation",
        "\xc3", // truncated 2-byte
        "\xe2\x82", // truncated 3-byte
        "\xf0\x9f\x92", // truncated 4-byte
        "\xc0\xaf", // overlong slash
        "\xe0\x80\xaf", // overlong slash, 3-byte
        "\xf4\x90\x80\x80", // above U+10FFFF
        "\xed\xa0\x80", // lone high surrogate D800
        "\xed\xb0\x80", // lone low surrogate DC00
        "\xed\xa0\x80\xed\xb0\x80", // surrogate pair, CESU-8 style
        // Valid UTF-8 must survive intact.
        "Ünïcödé.Rêlèasé.2160p",
        "日本語.字幕",
        "emoji \u{1F4E6} pack",
        "\u{FFFD}",
        // The classic script-injection payloads: JSON string escaping is
        // not HTML escaping, but they must not break the document.
        "</script><script>alert(1)</script>",
        "\u{2028}\u{2029}",
    };

    for (hostile) |raw| {
        var w = Writer.init(a);
        try w.beginObject();
        try w.strField("name", raw);
        try w.strField("second", "sentinel");
        try w.endObject();

        const v = std.json.parseFromSliceLeaky(std.json.Value, a, w.items(), .{}) catch |err| {
            std.debug.print("unparseable for {x}: {s}\n", .{ raw, w.items() });
            return err;
        };
        // Structure intact: exactly the two members we wrote, and the
        // second one still says what we put in it. That is what catches
        // an escape that lets the value break out of its string.
        try testing.expectEqual(@as(usize, 2), v.object.count());
        try testing.expectEqualStrings("sentinel", v.object.get("second").?.string);
        const got = v.object.get("name").?.string;
        // Valid UTF-8 in, identical bytes out.
        if (std.unicode.utf8ValidateSlice(raw)) {
            try testing.expectEqualStrings(raw, got);
        } else {
            try testing.expect(std.unicode.utf8ValidateSlice(got));
        }
    }
}

test "every byte value, alone and in pairs, produces valid JSON" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var b: usize = 0;
    while (b < 256) : (b += 1) {
        const one = [_]u8{@intCast(b)};
        var w = Writer.init(a);
        try w.beginObject();
        try w.strField("v", &one);
        try w.endObject();
        _ = try parseDoc(a, w.items());

        var c: usize = 0;
        while (c < 256) : (c += 1) {
            const two = [_]u8{ @intCast(b), @intCast(c) };
            var w2 = Writer.init(a);
            try w2.beginObject();
            try w2.strField("v", &two);
            try w2.endObject();
            _ = std.json.parseFromSliceLeaky(std.json.Value, a, w2.items(), .{}) catch |err| {
                std.debug.print("pair {x} {x} produced {s}\n", .{ b, c, w2.items() });
                return err;
            };
        }
        // The arena would otherwise hold 65k documents at once.
        _ = arena.reset(.retain_capacity);
    }
}

test "control bytes use the same escapes core/log.zig does" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.string("\x00\x01\x08\x09\x0a\x0b\x0c\x0d\x1e\x1f");
    try testing.expectEqualStrings(
        "\"\\u0000\\u0001\\b\\t\\n\\u000b\\f\\r\\u001e\\u001f\"",
        w.items(),
    );
}

test "keys are escaped too" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.beginObject();
    try w.strField("weird\"key\n", "v");
    try w.endObject();
    try testing.expectEqualStrings("{\"weird\\\"key\\n\":\"v\"}", w.items());
}

test "error document" {
    const doc = try errorDocument(testing.allocator, "job \"7\" not found\n");
    defer testing.allocator.free(doc);
    try testing.expectEqualStrings("{\"error\":\"job \\\"7\\\" not found\\n\"}", doc);
}

test "raw splices JSON, rawOrNull refuses garbage" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w = Writer.init(a);
    try w.beginObject();
    try w.rawField("payload", "{\"job_id\":7}");
    try w.key("bad");
    try w.rawOrNull("{not json", a);
    try w.key("empty");
    try w.rawOrNull("", a);
    try w.key("good");
    try w.rawOrNull("[1,2]", a);
    try w.endObject();

    const v = try parseDoc(a, w.items());
    try testing.expectEqual(@as(i64, 7), v.object.get("payload").?.object.get("job_id").?.integer);
    try testing.expect(v.object.get("bad").? == .null);
    try testing.expect(v.object.get("empty").? == .null);
    try testing.expectEqual(@as(usize, 2), v.object.get("good").?.array.items.len);
}

test "reset keeps the writer usable for the next response" {
    var w = Writer.init(testing.allocator);
    defer w.deinit();
    try w.beginObject();
    try w.strField("a", "1");
    try w.endObject();
    w.reset();
    try w.beginObject();
    try w.strField("b", "2");
    try w.endObject();
    try testing.expectEqualStrings("{\"b\":\"2\"}", w.items());
}

// -- request bodies ---------------------------------------------------

test "body fields decode by type" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const b = try Body.parse(a,
        \\{"name":"  spaced  ","port":563,"tls":true,"ratio":0.75,
        \\ "topics":["a","b"],"ids":[3,1,2],"nested":{"k":1},"nil":null,
        \\ "numeric_string":"42"}
    );
    try testing.expectEqualStrings("  spaced  ", b.string("name").?);
    try testing.expectEqualStrings("spaced", b.trimmedString("name").?);
    try testing.expectEqual(@as(i64, 563), b.int("port").?);
    try testing.expectEqual(true, b.boolean("tls").?);
    try testing.expectEqual(@as(f64, 0.75), b.float("ratio").?);
    try testing.expectEqual(@as(i64, 42), b.int("numeric_string").?);
    try testing.expectEqual(@as(usize, 2), (try b.stringArray(a, "topics")).?.len);
    try testing.expectEqualSlices(i64, &.{ 3, 1, 2 }, (try b.intArray(a, "ids")).?);
    try testing.expectEqualStrings("{\"k\":1}", (try b.rawJson(a, "nested")).?);

    // Absent, null, and wrong-typed all read as "not supplied".
    try testing.expect(b.string("absent") == null);
    try testing.expect(b.int("name") == null);
    try testing.expect(b.boolean("port") == null);
    try testing.expect(b.string("nil") == null);
    try testing.expect(try b.rawJson(a, "nil") == null);
    try testing.expect(try b.stringArray(a, "ids") == null);
    // `has` still distinguishes an explicit null from an absent key.
    try testing.expect(b.has("nil"));
    try testing.expect(!b.has("absent"));
}

test "an empty body is an empty object; malformed and non-object are refused" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const empty = try Body.parse(a, "");
    try testing.expect(empty.string("anything") == null);
    _ = try Body.parse(a, "   \r\n ");
    _ = try Body.parse(a, "{}");

    try testing.expectError(error.Malformed, Body.parse(a, "{"));
    try testing.expectError(error.Malformed, Body.parse(a, "not json"));
    try testing.expectError(error.Malformed, Body.parse(a, "[1,2]"));
    try testing.expectError(error.Malformed, Body.parse(a, "\"string\""));
    try testing.expectError(error.Malformed, Body.parse(a, "7"));
    try testing.expectError(error.Malformed, Body.parse(a, "{\"a\":}"));
    try testing.expectError(error.Malformed, Body.parse(a, "{\"a\":1}{\"b\":2}"));
}

test "a hostile body round-trips through the writer without breaking out" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A name chosen to break a naive echo: quotes, a brace, a newline and
    // an invalid byte smuggled in as an escape std.json will decode.
    const b = try Body.parse(a, "{\"name\":\"a\\\"b\\\\c\\u0000d\\ne\"}");
    const name = b.string("name").?;

    var w = Writer.init(a);
    try w.beginObject();
    try w.strField("echo", name);
    try w.boolField("ok", true);
    try w.endObject();

    const v = try parseDoc(a, w.items());
    try testing.expectEqualStrings(name, v.object.get("echo").?.string);
    try testing.expectEqual(true, v.object.get("ok").?.bool);
}
