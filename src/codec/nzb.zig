//! NZB parsing — the XML manifest describing a Usenet release split
//! across many articles.
//!
//! An NZB names one or more files, each split into "segments" (articles
//! posted to Usenet). This module is a faithful translation of the XML
//! into a `Document`; turning one into a download job is the
//! application layer's business.
//!
//! Reference: https://sabnzbd.org/wiki/extra/nzb-spec — with the caveat
//! that real NZBs deviate freely (custom xmlns, missing meta, non-UTF-8
//! encodings, creative subject formats). The parser is lenient about
//! everything it can be lenient about and fails only on structural
//! impossibility: not an `<nzb>` document, no files at all, a file with
//! no segments, or a segment whose article reference is unusable.
//!
//! Every string in a returned `Document` lives in that document's
//! arena, so the whole parse is released with one `deinit` and no
//! per-field ownership bookkeeping.

const std = @import("std");
const xml = @import("xml.zig");

/// Bounds on what an untrusted document may cost us. An NZB arrives
/// from an indexer over the internet; none of these are reachable by a
/// legitimate release (the largest real NZBs run to a few tens of
/// thousands of segments).
pub const Limits = struct {
    max_input: usize = 512 << 20,
    max_files: u32 = 100_000,
    max_segments_per_file: u32 = 200_000,
    max_groups_per_file: u32 = 256,
    max_meta: u32 = 1024,
    xml: xml.Limits = .{},
};

pub const Error = error{
    /// Root element is not `<nzb>`.
    NotAnNzb,
    /// A well-formed `<nzb>` that declares no files.
    NoFiles,
    FileHasNoSegments,
    MissingMessageId,
    /// A message-id containing bytes that would corrupt an NNTP
    /// command line. See `validateMessageId`.
    InvalidMessageId,
    InvalidSegmentNumber,
    InputTooLarge,
    UnsupportedCharset,
    TooManyFiles,
    TooManySegments,
    TooManyGroups,
    TooManyMeta,
} || xml.Error;

/// One `<meta type="...">value</meta>` entry under `<head>`.
pub const Meta = struct {
    /// The `type` attribute — `title`, `category`, `password`, ...
    kind: []const u8,
    value: []const u8,
};

/// One `<segment>`: a reference to a single article.
pub const Segment = struct {
    /// Declared article size on the wire, overhead included — not the
    /// size of the decoded payload. Zero when the attribute is absent
    /// or unparseable; callers size buffers from the yEnc header.
    bytes: i64,
    /// 1-based segment number.
    number: u32,
    /// Message-id with any surrounding angle brackets removed. The NNTP
    /// transport adds them back when issuing ARTICLE/BODY.
    message_id: []const u8,
};

/// One `<file>`: a logical file split across segments.
pub const File = struct {
    /// The `poster` attribute. Empty when absent.
    poster: []const u8,
    /// Unix seconds from the `date` attribute; null when absent or
    /// unparseable.
    date_unix: ?i64,
    /// The raw `subject` attribute.
    subject: []const u8,
    /// Filename recovered from `subject`; empty when nothing looked
    /// like one. Always a slice of `subject`.
    filename: []const u8,
    /// Newsgroups the segments live in, empty entries dropped.
    groups: []const []const u8,
    /// Article references in document order. Deliberately *not* sorted
    /// by `number` — callers that need ordering sort explicitly.
    segments: []const Segment,
};

pub const Document = struct {
    /// Backing store for every string and slice reachable from here.
    arena: std.heap.ArenaAllocator,
    meta: []const Meta,
    files: []const File,

    pub fn deinit(self: *Document) void {
        self.arena.deinit();
    }

    /// First `<meta>` value with the given type, or null.
    pub fn metaValue(self: Document, kind: []const u8) ?[]const u8 {
        for (self.meta) |m| {
            if (std.mem.eql(u8, m.kind, kind)) return m.value;
        }
        return null;
    }
};

pub fn parse(gpa: std.mem.Allocator, src: []const u8) Error!Document {
    return parseWithLimits(gpa, src, .{});
}

pub fn parseWithLimits(gpa: std.mem.Allocator, src: []const u8, limits: Limits) Error!Document {
    if (src.len > limits.max_input) return error.InputTooLarge;

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();

    // Transcode before scanning: the scanner works in UTF-8 only. The
    // transcoded copy lives in the arena because filenames and subjects
    // are sliced out of it.
    const body = if (xml.declaredEncoding(src)) |label| switch (charsetFromLabel(label) orelse
        return error.UnsupportedCharset) {
        .utf8 => src,
        .latin1 => try xml.latin1ToUtf8(a, src),
    } else src;

    var sc = xml.Scanner.init(gpa, body, limits.xml);
    defer sc.deinit();

    var p: Parser = .{ .gpa = gpa, .arena = a, .sc = &sc, .limits = limits };
    defer p.deinit();

    const root = switch (try sc.next()) {
        .open => |e| e,
        else => return error.NotAnNzb,
    };
    if (!std.mem.eql(u8, root.local(), "nzb")) return error.NotAnNzb;

    var meta: std.ArrayList(Meta) = .empty;
    defer meta.deinit(gpa);
    var files: std.ArrayList(File) = .empty;
    defer files.deinit(gpa);

    while (true) {
        const ev = try sc.next();
        switch (ev) {
            .open => |e| {
                const name = e.local();
                if (std.mem.eql(u8, name, "head")) {
                    try p.parseHead(&meta);
                } else if (std.mem.eql(u8, name, "file")) {
                    if (files.items.len >= limits.max_files) return error.TooManyFiles;
                    const f = try p.parseFile(e);
                    try files.append(gpa, f);
                } else {
                    // Unknown child of <nzb>. Real documents carry
                    // indexer-specific extensions here.
                    try sc.skipElement();
                }
            },
            .text => {},
            .close, .eof => break,
        }
    }

    if (files.items.len == 0) return error.NoFiles;

    // The element lists were built with `gpa` so their doubling growth
    // does not leave dead copies in the arena; only the final sizes are
    // committed. Both dupes must happen before `arena` is copied into
    // the result: copying snapshots the arena's bump state, and any
    // allocation made afterwards through the local would be invisible
    // to — and so leaked by — the returned copy.
    const meta_out = try a.dupe(Meta, meta.items);
    const files_out = try a.dupe(File, files.items);
    return .{ .arena = arena, .meta = meta_out, .files = files_out };
}

const Charset = enum { utf8, latin1 };

/// Only the encodings NZBs actually use. `windows-1252` is treated as
/// ISO-8859-1, which is what the Go implementation did: the two differ
/// only in 0x80–0x9F, a range no NZB relies on.
fn charsetFromLabel(label: []const u8) ?Charset {
    const eq = struct {
        fn f(a: []const u8, b: []const u8) bool {
            return std.ascii.eqlIgnoreCase(a, b);
        }
    }.f;
    if (label.len == 0) return .latin1; // the NZB DTD's declared default
    if (eq(label, "utf-8") or eq(label, "utf8")) return .utf8;
    if (eq(label, "iso-8859-1") or eq(label, "iso_8859-1") or
        eq(label, "latin1") or eq(label, "windows-1252")) return .latin1;
    return null;
}

/// Drives the scanner. Holds the reusable scratch buffer that text
/// content is gathered into before being copied into the arena.
const Parser = struct {
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    sc: *xml.Scanner,
    limits: Limits,
    scratch: std.ArrayList(u8) = .empty,

    fn deinit(self: *Parser) void {
        self.scratch.deinit(self.gpa);
    }

    /// Concatenates the text content of the element whose `open` was
    /// just consumed, discarding any child elements, and consumes its
    /// `close`. The result borrows `scratch` — copy it before the next
    /// call.
    fn readText(self: *Parser) Error![]const u8 {
        self.scratch.clearRetainingCapacity();
        var depth: usize = 1;
        while (depth > 0) {
            switch (try self.sc.next()) {
                .open => depth += 1,
                .close => depth -= 1,
                .text => |s| if (depth == 1) {
                    // The scanner caps a single text event; this caps
                    // the sum of the runs a hostile document can splice
                    // together with comments or child elements.
                    if (self.scratch.items.len + s.len > self.limits.xml.max_value_len)
                        return error.ValueTooLong;
                    try self.scratch.appendSlice(self.gpa, s);
                },
                .eof => return error.UnclosedElement,
            }
        }
        return self.scratch.items;
    }

    fn parseHead(self: *Parser, out: *std.ArrayList(Meta)) Error!void {
        var depth: usize = 1;
        while (depth > 0) {
            switch (try self.sc.next()) {
                .open => |e| {
                    if (depth == 1 and std.mem.eql(u8, e.local(), "meta")) {
                        if (out.items.len >= self.limits.max_meta) return error.TooManyMeta;
                        const kind = try self.arena.dupe(u8, trimSpace(e.attr("type") orelse ""));
                        const value = try self.arena.dupe(u8, trimSpace(try self.readText()));
                        try out.append(self.gpa, .{ .kind = kind, .value = value });
                    } else {
                        depth += 1;
                    }
                },
                .close => depth -= 1,
                .text => {},
                .eof => return error.UnclosedElement,
            }
        }
    }

    fn parseFile(self: *Parser, el: xml.Element) Error!File {
        // Attributes are only valid until the next scanner call, so
        // everything we need comes out of `el` first.
        const poster = try self.arena.dupe(u8, el.attr("poster") orelse "");
        const subject = try self.arena.dupe(u8, el.attr("subject") orelse "");
        const date: ?i64 = if (el.attr("date")) |d|
            std.fmt.parseInt(i64, d, 10) catch null
        else
            null;

        var groups: std.ArrayList([]const u8) = .empty;
        defer groups.deinit(self.gpa);
        var segments: std.ArrayList(Segment) = .empty;
        defer segments.deinit(self.gpa);

        var depth: usize = 1;
        while (depth > 0) {
            switch (try self.sc.next()) {
                .open => |e| {
                    const name = e.local();
                    if (depth == 1 and std.mem.eql(u8, name, "groups")) {
                        try self.parseGroups(&groups);
                    } else if (depth == 1 and std.mem.eql(u8, name, "segments")) {
                        try self.parseSegments(&segments);
                    } else {
                        depth += 1;
                    }
                },
                .close => depth -= 1,
                .text => {},
                .eof => return error.UnclosedElement,
            }
        }

        if (segments.items.len == 0) return error.FileHasNoSegments;
        return .{
            .poster = poster,
            .date_unix = date,
            .subject = subject,
            .filename = filenameFromSubject(subject),
            .groups = try self.arena.dupe([]const u8, groups.items),
            .segments = try self.arena.dupe(Segment, segments.items),
        };
    }

    fn parseGroups(self: *Parser, out: *std.ArrayList([]const u8)) Error!void {
        var depth: usize = 1;
        while (depth > 0) {
            switch (try self.sc.next()) {
                .open => |e| {
                    if (depth == 1 and std.mem.eql(u8, e.local(), "group")) {
                        if (out.items.len >= self.limits.max_groups_per_file) return error.TooManyGroups;
                        const g = trimSpace(try self.readText());
                        if (g.len != 0) try out.append(self.gpa, try self.arena.dupe(u8, g));
                    } else {
                        depth += 1;
                    }
                },
                .close => depth -= 1,
                .text => {},
                .eof => return error.UnclosedElement,
            }
        }
    }

    fn parseSegments(self: *Parser, out: *std.ArrayList(Segment)) Error!void {
        var depth: usize = 1;
        while (depth > 0) {
            switch (try self.sc.next()) {
                .open => |e| {
                    if (depth == 1 and std.mem.eql(u8, e.local(), "segment")) {
                        if (out.items.len >= self.limits.max_segments_per_file) return error.TooManySegments;
                        try out.append(self.gpa, try self.parseSegment(e));
                    } else {
                        depth += 1;
                    }
                },
                .close => depth -= 1,
                .text => {},
                .eof => return error.UnclosedElement,
            }
        }
    }

    fn parseSegment(self: *Parser, el: xml.Element) Error!Segment {
        // Both attributes must be read before `readText` invalidates
        // them. A missing or broken `bytes` is tolerated; a missing or
        // non-positive `number` is not, because segment order is how
        // the payload gets reassembled.
        const bytes = std.fmt.parseInt(i64, el.attr("bytes") orelse "", 10) catch 0;
        const number = std.fmt.parseInt(u32, el.attr("number") orelse "", 10) catch
            return error.InvalidSegmentNumber;
        if (number < 1) return error.InvalidSegmentNumber;

        var mid = trimSpace(try self.readText());
        if (mid.len == 0) return error.MissingMessageId;
        if (mid[0] == '<') mid = mid[1..];
        if (mid.len != 0 and mid[mid.len - 1] == '>') mid = mid[0 .. mid.len - 1];
        try validateMessageId(mid);

        return .{ .bytes = bytes, .number = number, .message_id = try self.arena.dupe(u8, mid) };
    }
};

/// Rejects message-ids containing bytes that would corrupt an NNTP
/// command line. NNTP terminates commands with CRLF, so a CR or LF
/// inside a message-id would let a hostile NZB append a second command
/// to the connection — the article reference is attacker-controlled
/// text that we splice straight into `BODY <...>`.
///
/// RFC 5536 restricts message-ids to printable US-ASCII minus angle
/// brackets, whitespace and a few reserved characters. We apply the
/// permissive but injection-proof subset: no controls, no whitespace,
/// no DEL.
fn validateMessageId(mid: []const u8) error{ MissingMessageId, InvalidMessageId }!void {
    if (mid.len == 0) return error.MissingMessageId;
    for (mid) |b| {
        if (b < 0x21 or b == 0x7f) return error.InvalidMessageId;
    }
}

/// ASCII whitespace only. Go trimmed by `unicode.IsSpace`, which also
/// covers NEL and NBSP; neither appears in a message-id, a group name
/// or a meta value in practice, and treating NBSP as significant is the
/// safer default for a filename.
fn trimSpace(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n\x0b\x0c");
}

/// Recovers a filename from a Usenet subject line.
///
/// The canonical convention double-quotes it:
///
///     [Group] [1/15] - "filename.rar" - [4194304/4194304] yEnc (1/8)
///
/// Several private indexers obfuscate instead, putting the name in a
/// bracket group and leaving the quote slot empty:
///
///     [PRiVATE]-[WtFnZb]-[Monster.2022.S02E09.mkv]-[1/2] - "" yEnc 7337 (1/10237)
///
/// The bracket pass handles those. Without it the quoted pass sees only
/// an empty string and the token fallback throws the name away, because
/// the same whitespace-delimited token also contains `[1/2]` and the
/// fallback refuses path separators. Loosening the fallback instead
/// would misfire on real paths, so the bracket pass sits between them.
///
/// Returns a slice of `subject`, or empty when nothing matched.
pub fn filenameFromSubject(subject: []const u8) []const u8 {
    // 1. Canonical: the leftmost quoted run of at least one character.
    // An empty `""` is skipped rather than matched, mirroring how the
    // original regex `"([^"]+)"` retries from the next position.
    var i: usize = 0;
    while (std.mem.indexOfScalarPos(u8, subject, i, '"')) |open| {
        const close = std.mem.indexOfScalarPos(u8, subject, open + 1, '"') orelse break;
        if (close > open + 1) return trimSpace(subject[open + 1 .. close]);
        i = open + 1;
    }

    // 2. Obfuscated: the leftmost `[...]` group whose contents look
    // like `name.ext` with no nested brackets and no path separators.
    var b: usize = 0;
    while (std.mem.indexOfScalarPos(u8, subject, b, '[')) |open| {
        b = open + 1;
        const body = bracketBody(subject, open) orelse continue;
        if (hasShortExtension(body)) return trimSpace(body);
    }

    // 3. Last resort: the rightmost whitespace-delimited token that
    // contains a dot and no path separator, stripped of wrapping
    // punctuation. Scanning forward and keeping the last hit is the
    // same thing as scanning backwards for the first.
    var best: []const u8 = "";
    var it = std.mem.tokenizeAny(u8, subject, " \t\r\n\x0b\x0c");
    while (it.next()) |raw| {
        const tok = std.mem.trimStart(u8, std.mem.trimEnd(u8, raw, ",;)]}\"'"), "([{\"'");
        if (std.mem.indexOfScalar(u8, tok, '.') != null and
            std.mem.indexOfAny(u8, tok, "/\\") == null) best = tok;
    }
    return best;
}

/// Contents of the bracket group opening at `open`, or null when it is
/// unterminated or contains a nested bracket or a path separator.
fn bracketBody(s: []const u8, open: usize) ?[]const u8 {
    var j = open + 1;
    while (j < s.len) : (j += 1) {
        switch (s[j]) {
            ']' => return s[open + 1 .. j],
            '[', '/', '\\' => return null,
            else => {},
        }
    }
    return null;
}

/// True when `s` ends in `.` plus one to five alphanumerics and has a
/// non-empty stem. Only the last dot needs checking: an earlier dot
/// would put a `.` inside the extension, which is not alphanumeric.
fn hasShortExtension(s: []const u8) bool {
    const dot = std.mem.lastIndexOfScalar(u8, s, '.') orelse return false;
    if (dot == 0) return false;
    const ext = s[dot + 1 ..];
    if (ext.len == 0 or ext.len > 5) return false;
    for (ext) |c| {
        if (!std.ascii.isAlphanumeric(c)) return false;
    }
    return true;
}

// ---------------------------------------------------------------- tests

const t = std.testing;

const minimal_nzb =
    \\<?xml version="1.0" encoding="UTF-8"?>
    \\<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
    \\<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
    \\  <head>
    \\    <meta type="title">Some.Release.Title</meta>
    \\    <meta type="category">tv</meta>
    \\  </head>
    \\  <file poster="user@host.invalid" date="1700000000" subject='[1/2] - "release.r00" yEnc (1/2)'>
    \\    <groups>
    \\      <group>alt.binaries.test</group>
    \\      <group>alt.binaries.misc</group>
    \\    </groups>
    \\    <segments>
    \\      <segment bytes="715000" number="1">msg1@host</segment>
    \\      <segment bytes="615000" number="2">msg2@host</segment>
    \\    </segments>
    \\  </file>
    \\  <file poster="user@host.invalid" date="1700000001" subject='[2/2] - "release.par2" yEnc (1/1)'>
    \\    <groups>
    \\      <group>alt.binaries.test</group>
    \\    </groups>
    \\    <segments>
    \\      <segment bytes="100000" number="1">par2-msg@host</segment>
    \\    </segments>
    \\  </file>
    \\</nzb>
    \\
;

test "parse minimal nzb" {
    var doc = try parse(t.allocator, minimal_nzb);
    defer doc.deinit();

    try t.expectEqual(@as(usize, 2), doc.meta.len);
    try t.expectEqualStrings("title", doc.meta[0].kind);
    try t.expectEqualStrings("Some.Release.Title", doc.meta[0].value);
    try t.expectEqualStrings("tv", doc.metaValue("category").?);
    try t.expect(doc.metaValue("password") == null);

    try t.expectEqual(@as(usize, 2), doc.files.len);
    const f0 = doc.files[0];
    try t.expectEqualStrings("user@host.invalid", f0.poster);
    try t.expectEqualStrings("release.r00", f0.filename);
    try t.expectEqualStrings("[1/2] - \"release.r00\" yEnc (1/2)", f0.subject);
    try t.expectEqual(@as(?i64, 1_700_000_000), f0.date_unix);
    try t.expectEqual(@as(usize, 2), f0.groups.len);
    try t.expectEqualStrings("alt.binaries.test", f0.groups[0]);
    try t.expectEqualStrings("alt.binaries.misc", f0.groups[1]);
    try t.expectEqual(@as(usize, 2), f0.segments.len);
    try t.expectEqualStrings("msg1@host", f0.segments[0].message_id);
    try t.expectEqual(@as(i64, 715000), f0.segments[0].bytes);
    try t.expectEqual(@as(u32, 1), f0.segments[0].number);
    try t.expectEqualStrings("msg2@host", f0.segments[1].message_id);

    const f1 = doc.files[1];
    try t.expectEqualStrings("release.par2", f1.filename);
    try t.expectEqual(@as(?i64, 1_700_000_001), f1.date_unix);
    try t.expectEqual(@as(usize, 1), f1.segments.len);
    try t.expectEqualStrings("par2-msg@host", f1.segments[0].message_id);
}

test "strips angle brackets from message-id" {
    const body = try std.mem.replaceOwned(u8, t.allocator, minimal_nzb, ">msg1@host<", ">&lt;msg1@host&gt;<");
    defer t.allocator.free(body);
    var doc = try parse(t.allocator, body);
    defer doc.deinit();
    try t.expectEqualStrings("msg1@host", doc.files[0].segments[0].message_id);
}

test "iso-8859-1 document is transcoded" {
    // The é is 0xE9 in Latin-1 and must come out as U+00E9.
    const body = "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n" ++
        "<nzb><head/><file poster=\"x\" date=\"0\" subject='\"caf\xE9.rar\"'>\n" ++
        "<groups><group>g</group></groups>\n" ++
        "<segments><segment bytes=\"1\" number=\"1\">m@h</segment></segments>\n" ++
        "</file></nzb>";
    var doc = try parse(t.allocator, body);
    defer doc.deinit();
    try t.expectEqualStrings("café.rar", doc.files[0].filename);
    try t.expectEqual(@as(?i64, 0), doc.files[0].date_unix);
}

test "unsupported charset is rejected" {
    const body = "<?xml version=\"1.0\" encoding=\"shift_jis\"?><nzb><head/></nzb>";
    try t.expectError(error.UnsupportedCharset, parse(t.allocator, body));
}

test "rejects a document with no files" {
    try t.expectError(error.NoFiles, parse(t.allocator, "<nzb><head/></nzb>"));
}

test "rejects a file with no segments" {
    const body = "<nzb><head/><file poster=\"x\" date=\"0\" subject='\"x.rar\"'>" ++
        "<groups><group>g</group></groups><segments></segments></file></nzb>";
    try t.expectError(error.FileHasNoSegments, parse(t.allocator, body));
}

test "rejects a non-nzb root" {
    try t.expectError(error.NotAnNzb, parse(t.allocator, "<html><body/></html>"));
    try t.expectError(error.NoRootElement, parse(t.allocator, ""));
}

/// Wraps a `<segments>` body in an otherwise valid single-file NZB.
fn oneFile(segments: []const u8) ![]u8 {
    return std.mem.concat(t.allocator, u8, &.{
        "<nzb><head/><file subject='\"a.rar\"'><groups><group>g</group></groups><segments>",
        segments,
        "</segments></file></nzb>",
    });
}

test "segment attributes and message-id validation" {
    const cases = [_]struct { seg: []const u8, want: anyerror }{
        // A message-id is the only thing we splice into an NNTP command
        // line, so anything that could terminate one is fatal.
        .{ .seg = "<segment bytes=\"1\" number=\"1\">a@h&#13;&#10;BODY b@h</segment>", .want = error.InvalidMessageId },
        .{ .seg = "<segment bytes=\"1\" number=\"1\">a b@h</segment>", .want = error.InvalidMessageId },
        .{ .seg = "<segment bytes=\"1\" number=\"1\">a\x7fh</segment>", .want = error.InvalidMessageId },
        .{ .seg = "<segment bytes=\"1\" number=\"1\"></segment>", .want = error.MissingMessageId },
        .{ .seg = "<segment bytes=\"1\" number=\"1\">   </segment>", .want = error.MissingMessageId },
        .{ .seg = "<segment bytes=\"1\" number=\"1\">&lt;&gt;</segment>", .want = error.MissingMessageId },
        .{ .seg = "<segment bytes=\"1\" number=\"0\">a@h</segment>", .want = error.InvalidSegmentNumber },
        .{ .seg = "<segment bytes=\"1\" number=\"-1\">a@h</segment>", .want = error.InvalidSegmentNumber },
        .{ .seg = "<segment bytes=\"1\">a@h</segment>", .want = error.InvalidSegmentNumber },
        .{ .seg = "<segment bytes=\"1\" number=\"9999999999999\">a@h</segment>", .want = error.InvalidSegmentNumber },
    };
    for (cases) |c| {
        const body = try oneFile(c.seg);
        defer t.allocator.free(body);
        try t.expectError(c.want, parse(t.allocator, body));
    }
}

test "missing or broken bytes attribute defaults to zero" {
    const body = try oneFile("<segment number=\"1\">a@h</segment><segment bytes=\"junk\" number=\"2\">b@h</segment>");
    defer t.allocator.free(body);
    var doc = try parse(t.allocator, body);
    defer doc.deinit();
    try t.expectEqual(@as(i64, 0), doc.files[0].segments[0].bytes);
    try t.expectEqual(@as(i64, 0), doc.files[0].segments[1].bytes);
}

test "tolerates namespace prefixes, missing attributes and unknown elements" {
    const body =
        \\<n:nzb xmlns:n="http://www.newzbin.com/DTD/2003/nzb">
        \\  <n:head><n:meta type="title">T</n:meta></n:head>
        \\  <indexer:extension xmlns:indexer="x"><nested><deep/></nested></indexer:extension>
        \\  <n:file n:subject='"a.rar"'>
        \\    <n:groups><n:group> a.b.c </n:group><n:group>  </n:group></n:groups>
        \\    <n:segments><n:segment n:number="1">m@h</n:segment></n:segments>
        \\  </n:file>
        \\</n:nzb>
    ;
    var doc = try parse(t.allocator, body);
    defer doc.deinit();
    try t.expectEqualStrings("T", doc.metaValue("title").?);
    const f = doc.files[0];
    try t.expectEqualStrings("", f.poster);
    try t.expectEqual(@as(?i64, null), f.date_unix);
    // The blank <group> is dropped and the other is trimmed.
    try t.expectEqual(@as(usize, 1), f.groups.len);
    try t.expectEqualStrings("a.b.c", f.groups[0]);
    try t.expectEqualStrings("a.rar", f.filename);
}

test "unparseable date yields null rather than a bogus timestamp" {
    const body = "<nzb><head/><file date=\"not-a-number\" subject='\"a.rar\"'><groups/>" ++
        "<segments><segment number=\"1\">m@h</segment></segments></file></nzb>";
    var doc = try parse(t.allocator, body);
    defer doc.deinit();
    try t.expectEqual(@as(?i64, null), doc.files[0].date_unix);
    try t.expectEqual(@as(usize, 0), doc.files[0].groups.len);
}

test "cdata and character references in a message-id" {
    const body = try oneFile("<segment number=\"1\"><![CDATA[part1]]>&#45;<![CDATA[part2@h]]></segment>");
    defer t.allocator.free(body);
    var doc = try parse(t.allocator, body);
    defer doc.deinit();
    try t.expectEqualStrings("part1-part2@h", doc.files[0].segments[0].message_id);
}

test "filename from subject" {
    const cases = [_]struct { subject: []const u8, want: []const u8 }{
        .{ .subject = "[1/2] - \"release.r00\" yEnc (1/2)", .want = "release.r00" },
        .{ .subject = "Some.Release [01/12] - \"release.par2\" yEnc (1/1)", .want = "release.par2" },
        .{ .subject = "\"file.rar\" yEnc (1/3)", .want = "file.rar" },
        .{ .subject = "Foo Bar - file.rar - more", .want = "file.rar" },
        .{ .subject = "", .want = "" },
        // Obfuscated subjects from private indexers: the filename is
        // bracketed and the quote slot is empty.
        .{
            .subject = "[N3wZ] \\jmAl6g259274\\::[PRiVATE]-[WtFnZb]-[Monster.2022.S02E09.mkv]-[1/2] - \"\" yEnc  7337320579 (1/10237)",
            .want = "Monster.2022.S02E09.mkv",
        },
        .{
            .subject = "[PRiVATE]-[WtFnZb]-[Release.Name.2160p.HDR.mkv]-[1/2] - \"\" yEnc 1234",
            .want = "Release.Name.2160p.HDR.mkv",
        },
        .{ .subject = "Foo - [release.par2]-[1/1] - \"\" yEnc (1/1)", .want = "release.par2" },
        // Nothing filename-shaped at all.
        .{ .subject = "no filename here", .want = "" },
        .{ .subject = "[1/2] - [] yEnc", .want = "" },
        // A bracket group with a path separator or a nested bracket is
        // not a filename; the fallback rejects it too.
        .{ .subject = "[dir/name.rar]", .want = "" },
        // Extension longer than five characters fails the bracket pass
        // and falls through to the token scan.
        .{ .subject = "[name.extension]", .want = "name.extension" },
    };
    for (cases) |c| {
        try t.expectEqualStrings(c.want, filenameFromSubject(c.subject));
    }
}

test "limits bound a hostile document" {
    // Too many files.
    var many: std.ArrayList(u8) = .empty;
    defer many.deinit(t.allocator);
    try many.appendSlice(t.allocator, "<nzb><head/>");
    for (0..40) |_| {
        try many.appendSlice(t.allocator, "<file subject='\"a.rar\"'><segments>" ++
            "<segment number=\"1\">m@h</segment></segments></file>");
    }
    try many.appendSlice(t.allocator, "</nzb>");
    try t.expectError(error.TooManyFiles, parseWithLimits(t.allocator, many.items, .{ .max_files = 8 }));

    // Too many segments in one file.
    var segs: std.ArrayList(u8) = .empty;
    defer segs.deinit(t.allocator);
    for (0..40) |_| try segs.appendSlice(t.allocator, "<segment number=\"1\">m@h</segment>");
    const body = try oneFile(segs.items);
    defer t.allocator.free(body);
    try t.expectError(error.TooManySegments, parseWithLimits(t.allocator, body, .{ .max_segments_per_file = 8 }));

    // Too many meta entries.
    var meta: std.ArrayList(u8) = .empty;
    defer meta.deinit(t.allocator);
    try meta.appendSlice(t.allocator, "<nzb><head>");
    for (0..40) |_| try meta.appendSlice(t.allocator, "<meta type=\"x\">y</meta>");
    try meta.appendSlice(t.allocator, "</head></nzb>");
    try t.expectError(error.TooManyMeta, parseWithLimits(t.allocator, meta.items, .{ .max_meta = 8 }));

    // Oversized input is rejected before anything is allocated.
    try t.expectError(error.InputTooLarge, parseWithLimits(t.allocator, minimal_nzb, .{ .max_input = 16 }));

    // Too many groups.
    var groups: std.ArrayList(u8) = .empty;
    defer groups.deinit(t.allocator);
    try groups.appendSlice(t.allocator, "<nzb><head/><file subject='\"a.rar\"'><groups>");
    for (0..40) |_| try groups.appendSlice(t.allocator, "<group>g</group>");
    try groups.appendSlice(t.allocator, "</groups><segments><segment number=\"1\">m@h</segment></segments></file></nzb>");
    try t.expectError(error.TooManyGroups, parseWithLimits(t.allocator, groups.items, .{ .max_groups_per_file = 8 }));
}

test "hostile documents are rejected without expanding" {
    // An entity bomb: the internal subset is never interpreted, so
    // `&c;` stays literal text and the message-id validator kills it.
    const bomb =
        \\<!DOCTYPE nzb [
        \\<!ENTITY a "aaaaaaaaaa">
        \\<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">
        \\<!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">
        \\]>
        \\<nzb><head><meta type="t">&c;</meta></head>
        \\<file subject='"a.rar"'><segments><segment number="1">&c;</segment></segments></file></nzb>
    ;
    var doc = try parse(t.allocator, bomb);
    defer doc.deinit();
    try t.expectEqualStrings("&c;", doc.metaValue("t").?);
    try t.expectEqualStrings("&c;", doc.files[0].segments[0].message_id);

    // Deep nesting inside a skipped element still hits the depth cap.
    var deep: std.ArrayList(u8) = .empty;
    defer deep.deinit(t.allocator);
    try deep.appendSlice(t.allocator, "<nzb><junk>");
    for (0..500) |_| try deep.appendSlice(t.allocator, "<a>");
    try t.expectError(error.DepthExceeded, parse(t.allocator, deep.items));

    // A huge subject is capped rather than copied.
    var huge: std.ArrayList(u8) = .empty;
    defer huge.deinit(t.allocator);
    try huge.appendSlice(t.allocator, "<nzb><head/><file subject=\"");
    for (0..5000) |_| try huge.appendSlice(t.allocator, "x");
    try huge.appendSlice(t.allocator, "\"/></nzb>");
    try t.expectError(error.ValueTooLong, parseWithLimits(t.allocator, huge.items, .{
        .xml = .{ .max_value_len = 512 },
    }));

    // Text spliced together from many runs is capped in total, not just
    // per run.
    var spliced: std.ArrayList(u8) = .empty;
    defer spliced.deinit(t.allocator);
    try spliced.appendSlice(t.allocator, "<nzb><head><meta type=\"t\">");
    for (0..200) |_| try spliced.appendSlice(t.allocator, "yyyyyyyyyy<!--x-->");
    try spliced.appendSlice(t.allocator, "</meta></head></nzb>");
    try t.expectError(error.ValueTooLong, parseWithLimits(t.allocator, spliced.items, .{
        .xml = .{ .max_value_len = 64 },
    }));

    // Truncated markup.
    try t.expectError(error.UnterminatedCdata, parse(t.allocator, "<nzb><head/><file><segments>" ++
        "<segment number=\"1\"><![CDATA[m@h</segment></segments></file></nzb>"));
}

test "truncations and byte pokes never crash or leak" {
    // Stand-in for the Go fuzz target: every prefix of a valid document
    // and every single-byte substitution with a byte the parsers treat
    // specially. Any error is fine; a crash or a leak is not.
    for (0..minimal_nzb.len) |i| {
        if (parse(t.allocator, minimal_nzb[0..i])) |d| {
            var doc = d;
            doc.deinit();
        } else |e| switch (e) {
            error.OutOfMemory => return e,
            else => {},
        }
    }

    var buf: [minimal_nzb.len]u8 = undefined;
    const pokes = "<>/&;\"'![]-#x\x00\xFF=. \n";
    for (0..minimal_nzb.len) |i| {
        for (pokes) |p| {
            @memcpy(&buf, minimal_nzb);
            buf[i] = p;
            if (parse(t.allocator, &buf)) |d| {
                var doc = d;
                doc.deinit();
            } else |e| switch (e) {
                error.OutOfMemory => return e,
                else => {},
            }
        }
    }
}
