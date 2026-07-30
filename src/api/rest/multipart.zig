//! `multipart/form-data`, just enough of it for the NZB upload.
//!
//! One endpoint needs this — `POST /api/v1/queue/nzb`, which the UI's
//! file picker and every *arr client post to — and it needs two things
//! out of the body: the `nzb` file part and the `category` text field.
//! Go got that from `net/http`'s `ParseMultipartForm`, which also
//! spools to temporary files, decodes transfer encodings and builds a
//! map. None of that is wanted here: the body is already in memory,
//! bounded by the server's `max_body`, and spooling it to disk to read
//! it back would be slower and would need a filesystem the container
//! may not have.
//!
//! So this is a scanner over the body, returning slices into it. No
//! allocation, no copying, and no state beyond a cursor.
//!
//! ## What is deliberately not supported
//!
//! `Content-Transfer-Encoding` (base64, quoted-printable) is ignored —
//! it was deprecated for HTTP forms decades ago and no browser or *arr
//! client emits it. Nested `multipart/mixed` likewise. A part using
//! either is returned with its raw bytes; the NZB parser then rejects it
//! as malformed XML, which is the correct outcome and not one worth
//! writing a decoder to reach.

const std = @import("std");

/// Parts scanned before the rest of the body is ignored. A form with
/// more than this is not one of ours.
pub const max_parts = 16;

/// Per-part header block. A `Content-Disposition` with a long filename
/// is a few hundred bytes; 2 KiB is generous and bounds the scan.
pub const max_part_headers = 2 << 10;

pub const Part = struct {
    /// The `name=` parameter of `Content-Disposition`.
    name: []const u8 = "",
    /// The `filename=` parameter, empty for a plain field.
    filename: []const u8 = "",
    content_type: []const u8 = "",
    /// The part's bytes, borrowed from the request body.
    body: []const u8 = "",

    pub fn isFile(self: Part) bool {
        return self.filename.len > 0;
    }
};

/// The `boundary=` parameter of a `multipart/form-data` content type.
///
/// Returns null for any other media type, for a missing boundary, and
/// for a boundary that is empty or longer than the 70 characters RFC
/// 2046 allows — an over-long one is either a bug or an attempt to make
/// the scan quadratic.
pub fn boundary(content_type: []const u8) ?[]const u8 {
    var rest = content_type;
    const semi = std.mem.indexOfScalar(u8, rest, ';') orelse return null;
    const media = std.mem.trim(u8, rest[0..semi], " \t");
    if (!std.ascii.eqlIgnoreCase(media, "multipart/form-data")) return null;
    rest = rest[semi + 1 ..];

    while (rest.len > 0) {
        const end = std.mem.indexOfScalar(u8, rest, ';') orelse rest.len;
        const param = std.mem.trim(u8, rest[0..end], " \t");
        rest = if (end == rest.len) "" else rest[end + 1 ..];

        const eq = std.mem.indexOfScalar(u8, param, '=') orelse continue;
        const key = std.mem.trim(u8, param[0..eq], " \t");
        if (!std.ascii.eqlIgnoreCase(key, "boundary")) continue;

        var v = std.mem.trim(u8, param[eq + 1 ..], " \t");
        if (v.len >= 2 and v[0] == '"' and v[v.len - 1] == '"') v = v[1 .. v.len - 1];
        if (v.len == 0 or v.len > 70) return null;
        return v;
    }
    return null;
}

/// Walks the parts of a body. Every returned slice borrows from `body`.
pub const Iterator = struct {
    body: []const u8,
    boundary: []const u8,
    pos: usize = 0,
    /// Set once the closing `--boundary--` is seen, or on anything
    /// malformed. A truncated body simply ends the iteration: the parts
    /// that did arrive whole are still usable, and the one that did not
    /// is not returned.
    done: bool = false,
    seen: usize = 0,

    /// `--` + boundary, built once at init so neither `next` nor the
    /// delimiter search has to rebuild it.
    dash_buf: [74]u8 = undefined,

    pub fn init(body: []const u8, bound: []const u8) Iterator {
        var it: Iterator = .{ .body = body, .boundary = bound };
        it.dash_buf[0] = '-';
        it.dash_buf[1] = '-';
        @memcpy(it.dash_buf[2..][0..bound.len], bound);
        // The preamble before the first delimiter is ignorable text, so
        // the first delimiter is searched for rather than assumed at
        // offset zero.
        const first = std.mem.indexOf(u8, body, it.delimiter()) orelse {
            it.done = true;
            return it;
        };
        it.pos = first + bound.len + 2;
        return it;
    }

    fn delimiter(self: *const Iterator) []const u8 {
        return self.dash_buf[0 .. self.boundary.len + 2];
    }

    pub fn next(self: *Iterator) ?Part {
        if (self.done or self.seen >= max_parts) return null;
        const body = self.body;

        // At `pos` we are just past a `--boundary`. What follows is
        // either `--` (the close delimiter), or optional whitespace and
        // a CRLF introducing a part.
        var p = self.pos;
        if (p + 2 <= body.len and body[p] == '-' and body[p + 1] == '-') {
            self.done = true;
            return null;
        }
        // Transport padding: spaces and tabs are allowed after the
        // delimiter and before the line break.
        while (p < body.len and (body[p] == ' ' or body[p] == '\t')) p += 1;
        if (p + 2 > body.len or body[p] != '\r' or body[p + 1] != '\n') {
            // A bare LF is not legal framing here, but it is what a
            // hand-rolled client produces often enough to accept.
            if (p < body.len and body[p] == '\n') {
                p += 1;
            } else {
                self.done = true;
                return null;
            }
        } else {
            p += 2;
        }

        // Header block, terminated by a blank line.
        const scan_end = @min(body.len, p + max_part_headers);
        const head_end = std.mem.indexOfPos(u8, body[0..scan_end], p, "\r\n\r\n") orelse {
            self.done = true;
            return null;
        };
        const headers = body[p..head_end];
        const data_start = head_end + 4;

        // The part ends at the next `\r\n--boundary`. The CRLF belongs
        // to the delimiter, not to the data — getting that wrong appends
        // two bytes to every uploaded file.
        var sep_buf: [76]u8 = undefined;
        sep_buf[0] = '\r';
        sep_buf[1] = '\n';
        @memcpy(sep_buf[2..][0 .. self.boundary.len + 2], self.delimiter());
        const sep = sep_buf[0 .. self.boundary.len + 4];

        const data_end = std.mem.indexOfPos(u8, body, data_start, sep) orelse {
            // Truncated upload: the part never closed, so it is not
            // returned at all rather than handed over half-read.
            self.done = true;
            return null;
        };

        self.pos = data_end + sep.len;
        self.seen += 1;

        var part: Part = .{ .body = body[data_start..data_end] };
        parseHeaders(headers, &part);
        return part;
    }
};

fn parseHeaders(headers: []const u8, part: *Part) void {
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        const v = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "content-disposition")) {
            part.name = parameter(v, "name") orelse part.name;
            part.filename = parameter(v, "filename") orelse part.filename;
        } else if (std.ascii.eqlIgnoreCase(name, "content-type")) {
            part.content_type = v;
        }
    }
}

/// One `; key="value"` parameter out of a header value.
///
/// Only the quoted form is read. The unquoted form is legal in the grammar
/// but no client emits it for these two parameters, and accepting it would
/// mean guessing where a value with a space in it ends — which is exactly
/// how a filename gets truncated at the first space.
fn parameter(header_value: []const u8, key: []const u8) ?[]const u8 {
    var rest = header_value;
    while (std.mem.indexOfScalar(u8, rest, ';')) |semi| {
        rest = rest[semi + 1 ..];
        const trimmed = std.mem.trimStart(u8, rest, " \t");
        if (!std.ascii.startsWithIgnoreCase(trimmed, key)) continue;
        var after = trimmed[key.len..];
        after = std.mem.trimStart(u8, after, " \t");
        // `filename` must not match a request for `name`.
        if (after.len == 0 or after[0] != '=') continue;
        after = std.mem.trimStart(u8, after[1..], " \t");
        if (after.len == 0 or after[0] != '"') continue;
        const close = std.mem.indexOfScalar(u8, after[1..], '"') orelse return null;
        return after[1 .. 1 + close];
    }
    return null;
}

/// The first part with this name, or null.
pub fn field(body: []const u8, bound: []const u8, name: []const u8) ?Part {
    var it = Iterator.init(body, bound);
    while (it.next()) |p| {
        if (std.mem.eql(u8, p.name, name)) return p;
    }
    return null;
}

/// A named text field's value, trimmed. Empty and absent are the same
/// thing here, matching Go's `FormValue`.
pub fn value(body: []const u8, bound: []const u8, name: []const u8) []const u8 {
    const p = field(body, bound, name) orelse return "";
    return std.mem.trim(u8, p.body, " \t\r\n");
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

const b = "----WebKitFormBoundary7MA4YWxkTrZu0gW";

test "boundary extraction" {
    try testing.expectEqualStrings(b, boundary("multipart/form-data; boundary=" ++ b).?);
    try testing.expectEqualStrings(b, boundary("multipart/form-data; boundary=\"" ++ b ++ "\"").?);
    try testing.expectEqualStrings("x", boundary("Multipart/Form-Data; charset=utf-8; BOUNDARY=x").?);
    try testing.expectEqualStrings("x", boundary("multipart/form-data ;  boundary =  x ").?);

    try testing.expect(boundary("application/json") == null);
    try testing.expect(boundary("multipart/form-data") == null);
    try testing.expect(boundary("multipart/form-data; charset=utf-8") == null);
    try testing.expect(boundary("multipart/form-data; boundary=") == null);
    try testing.expect(boundary("multipart/form-data; boundary=\"\"") == null);
    // Over 70 characters: refused rather than scanned for.
    try testing.expect(boundary("multipart/form-data; boundary=" ++ ("a" ** 71)) == null);
    // Not the right media type, even with a boundary.
    try testing.expect(boundary("multipart/mixed; boundary=x") == null);
}

test "a browser upload yields the file and the text field" {
    const body =
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"category\"\r\n\r\n" ++
        "tv\r\n" ++
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"; filename=\"Release.Name.S01E01.nzb\"\r\n" ++
        "Content-Type: application/x-nzb\r\n\r\n" ++
        "<?xml version=\"1.0\"?><nzb></nzb>\r\n" ++
        "--" ++ b ++ "--\r\n";

    const nzb = field(body, b, "nzb").?;
    try testing.expectEqualStrings("Release.Name.S01E01.nzb", nzb.filename);
    try testing.expectEqualStrings("application/x-nzb", nzb.content_type);
    try testing.expectEqualStrings("<?xml version=\"1.0\"?><nzb></nzb>", nzb.body);
    try testing.expect(nzb.isFile());

    try testing.expectEqualStrings("tv", value(body, b, "category"));
    const cat = field(body, b, "category").?;
    try testing.expect(!cat.isFile());
    try testing.expectEqualStrings("", cat.content_type);

    try testing.expect(field(body, b, "absent") == null);
    try testing.expectEqualStrings("", value(body, b, "absent"));
}

test "binary content survives byte for byte" {
    // Every byte value, including the CR and LF that frame the parts and
    // a run that looks like the start of a delimiter.
    var payload: [512]u8 = undefined;
    for (&payload, 0..) |*c, i| c.* = @truncate(i);
    payload[100] = '\r';
    payload[101] = '\n';
    payload[102] = '-';
    payload[103] = '-';

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try buf.appendSlice(testing.allocator, "--" ++ b ++ "\r\nContent-Disposition: form-data; name=\"nzb\"; filename=\"x.nzb\"\r\n\r\n");
    try buf.appendSlice(testing.allocator, &payload);
    try buf.appendSlice(testing.allocator, "\r\n--" ++ b ++ "--\r\n");

    const p = field(buf.items, b, "nzb").?;
    try testing.expectEqualSlices(u8, &payload, p.body);
}

test "an empty part body is a part, not a failure" {
    const body =
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"category\"\r\n\r\n" ++
        "\r\n" ++
        "--" ++ b ++ "--\r\n";
    const p = field(body, b, "category").?;
    try testing.expectEqualStrings("", p.body);
    try testing.expectEqualStrings("", value(body, b, "category"));
}

test "malformed bodies yield no parts instead of garbage" {
    const cases = [_][]const u8{
        "",
        "not multipart at all",
        // Never opens.
        "--wrongboundary\r\nContent-Disposition: form-data; name=\"nzb\"\r\n\r\ndata\r\n",
        // Opens and never closes: the part is not returned half-read.
        "--" ++ b ++ "\r\nContent-Disposition: form-data; name=\"nzb\"\r\n\r\ntruncated",
        // No blank line after the headers.
        "--" ++ b ++ "\r\nContent-Disposition: form-data; name=\"nzb\"\r\ndata\r\n--" ++ b ++ "--",
        // Delimiter with no CRLF after it.
        "--" ++ b,
        "--" ++ b ++ "\r\n",
    };
    for (cases) |body| {
        try testing.expect(field(body, b, "nzb") == null);
        try testing.expectEqualStrings("", value(body, b, "nzb"));
    }
}

test "a header block that never ends is bounded rather than scanned forever" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try buf.appendSlice(testing.allocator, "--" ++ b ++ "\r\n");
    // 64 KiB of header lines with no blank line: the scan stops at
    // max_part_headers and the part is refused.
    for (0..2048) |_| try buf.appendSlice(testing.allocator, "X-Pad: aaaaaaaaaaaaaaaaaaaaaaaa\r\n");
    try buf.appendSlice(testing.allocator, "\r\ndata\r\n--" ++ b ++ "--\r\n");

    var it = Iterator.init(buf.items, b);
    try testing.expect(it.next() == null);
}

test "the part count is capped" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    for (0..100) |i| {
        var name: [16]u8 = undefined;
        const n = try std.fmt.bufPrint(&name, "f{d}", .{i});
        try buf.appendSlice(testing.allocator, "--" ++ b ++ "\r\nContent-Disposition: form-data; name=\"");
        try buf.appendSlice(testing.allocator, n);
        try buf.appendSlice(testing.allocator, "\"\r\n\r\nv\r\n");
    }
    try buf.appendSlice(testing.allocator, "--" ++ b ++ "--\r\n");

    var it = Iterator.init(buf.items, b);
    var count: usize = 0;
    while (it.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, max_parts), count);
}

test "parameter parsing does not confuse filename with name" {
    const body =
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; filename=\"only-a-filename.nzb\"; name=\"nzb\"\r\n\r\n" ++
        "x\r\n--" ++ b ++ "--\r\n";
    const p = field(body, b, "nzb").?;
    try testing.expectEqualStrings("nzb", p.name);
    try testing.expectEqualStrings("only-a-filename.nzb", p.filename);
}

test "a hostile filename is returned verbatim for the caller to sanitise" {
    // Path traversal, quotes and a newline. This layer's job is to
    // delimit it correctly; whoever turns it into a path is the one that
    // must refuse it, and does.
    const body =
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"; filename=\"../../etc/passwd\"\r\n\r\n" ++
        "x\r\n--" ++ b ++ "--\r\n";
    try testing.expectEqualStrings("../../etc/passwd", field(body, b, "nzb").?.filename);

    // An unterminated quote yields no filename rather than running to
    // the end of the header block.
    const bad =
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"; filename=\"unterminated\r\n\r\n" ++
        "x\r\n--" ++ b ++ "--\r\n";
    try testing.expectEqualStrings("", field(bad, b, "nzb").?.filename);
}

test "a preamble before the first delimiter is ignored" {
    const body =
        "This is a multi-part message in MIME format.\r\n" ++
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"; filename=\"a.nzb\"\r\n\r\n" ++
        "payload\r\n--" ++ b ++ "--\r\n";
    try testing.expectEqualStrings("payload", field(body, b, "nzb").?.body);
}

test "transport padding after a delimiter is tolerated" {
    const body =
        "--" ++ b ++ "  \r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"\r\n\r\n" ++
        "payload\r\n--" ++ b ++ "--\r\n";
    try testing.expectEqualStrings("payload", field(body, b, "nzb").?.body);
}
