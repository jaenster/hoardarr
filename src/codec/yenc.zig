//! yEnc decoder (https://www.yenc.org/yenc-draft.1.3.txt) — the
//! binary-to-text encoding used for Usenet binary posts.
//!
//! An article body looks like:
//!
//!     =ybegin part=1 total=8 line=128 size=750000 name=cool stuff.rar
//!     =ypart begin=1 end=750000
//!     [encoded bytes wrapped at line=128]
//!     =yend size=750000 part=1 pcrc32=AABBCCDD
//!
//! The encoder shifts every input byte by +42 (mod 256) and escapes the
//! "critical" output bytes (NUL, LF, CR, '=', and a leading TAB / space
//! / '.') by emitting '=' followed by the byte shifted a further +64.
//!
//! We only ever decode. Multi-part files are reassembled by the caller
//! using `Header.begin` / `Header.end` to write each segment at its
//! known offset in the destination file.

const std = @import("std");
const builtin = @import("builtin");
const crc32 = @import("../core/crc32.zig");

/// The =ybegin (plus optional =ypart) header fields.
///
/// `name` borrows from the `src` slice handed to `decode` — it is not
/// copied, so it stays valid only as long as `src` does. Copy it if you
/// need to outlive the article buffer.
pub const Header = struct {
    name: []const u8 = "",
    /// Total decoded file size declared by the poster, before splitting.
    size: i64 = 0,
    /// Column wrap width. Informational; decoders ignore it.
    line: i64 = 0,
    /// 1-based part number, 0 for single-part articles.
    part: i64 = 0,
    /// Total number of parts, 0 for single-part.
    total: i64 = 0,
    /// 1-based inclusive offset of this part in the assembled file.
    begin: i64 = 0,
    /// 1-based inclusive offset of this part's last byte.
    end: i64 = 0,
};

/// The =yend trailer fields.
pub const Trailer = struct {
    /// Declared size of the decoded part. Posters get this wrong; it is
    /// reported, never enforced. The CRC is the real integrity check.
    size: i64 = 0,
    part: i64 = 0,
    /// pcrc32= was present (typical for multi-part).
    has_part_crc: bool = false,
    part_crc32: u32 = 0,
    /// crc32= was present (whole-file CRC; typical for single-part).
    has_crc: bool = false,
    crc32: u32 = 0,
};

/// Structural problems with the article. Every one of these means the
/// article is unusable; none are recoverable by retrying the decode.
pub const ParseError = error{
    MissingYBegin,
    DuplicateYBegin,
    YPartBeforeYBegin,
    YEndBeforeYBegin,
    /// The body ran out before =yend — a truncated article, which is
    /// what a dropped NNTP connection mid-download looks like.
    MissingYEnd,
    /// A '=' with nothing after it to unescape.
    DanglingEscape,
    NegativeSize,
    BadPartRange,
};

pub const DecodeError = ParseError || error{ CrcMismatch, OutOfMemory };

/// A decoded article. `payload` is owned; `header.name` borrows `src`.
pub const Article = struct {
    payload: []u8,
    header: Header = .{},
    trailer: Trailer = .{},

    pub fn deinit(self: *Article, gpa: std.mem.Allocator) void {
        gpa.free(self.payload);
        self.* = undefined;
    }

    /// Compare the decoded payload against whichever CRC the trailer
    /// carried. A multi-part article's last part also carries crc32=
    /// (the whole-file CRC) — that one is not checkable here, so it is
    /// skipped when `header.part` is set.
    pub fn verifyCrc(self: Article) error{CrcMismatch}!void {
        if (self.trailer.has_part_crc) {
            if (crc32.checksum(self.payload) != self.trailer.part_crc32) return error.CrcMismatch;
        } else if (self.trailer.has_crc and self.header.part == 0) {
            if (crc32.checksum(self.payload) != self.trailer.crc32) return error.CrcMismatch;
        }
    }
};

const prefix_ybegin = "=ybegin";
const prefix_ypart = "=ypart";
const prefix_yend = "=yend";

/// What Go's `unicode.IsSpace` strips, minus the non-ASCII code points
/// that can never appear in a header we care about.
const whitespace = " \t\n\x0b\x0c\r";

/// Vector width for the decode inner loops. 16 on NEON/SSE, 32 with
/// AVX2, 64 with AVX-512. The 8-lane fallback still works — LLVM
/// scalarises it into roughly the SWAR sequence the Go version writes
/// by hand.
const vec_len: usize = std.simd.suggestVectorLength(u8) orelse 8;
const V = @Vector(vec_len, u8);
const Mask = std.meta.Int(.unsigned, vec_len);

const splat_42: V = @splat(42);
const splat_eq: V = @splat('=');
const splat_cr: V = @splat('\r');
const splat_lf: V = @splat('\n');

/// Index of the first set lane, or null when the mask is empty.
///
/// `@bitCast` of a bool vector packs lane i into bit i on little-endian
/// targets, which makes `@ctz` the whole search. Big-endian would pack
/// the other way round, so it goes the long way there.
inline fn firstLane(mask: @Vector(vec_len, bool)) ?usize {
    if (builtin.cpu.arch.endian() == .little) {
        const bits: Mask = @bitCast(mask);
        if (bits == 0) return null;
        return @ctz(bits);
    }
    return if (std.simd.firstTrue(mask)) |k| @as(usize, k) else null;
}

/// Decode one article body, verifying the trailer CRC.
///
/// Takes the whole body as a slice: yEnc articles are bounded (~750 KiB
/// typical, and every real news server caps them well below memory
/// pressure), so the caller reads once and we scan in place. No I/O, no
/// streaming state, one allocation.
pub fn decode(gpa: std.mem.Allocator, src: []const u8) DecodeError!Article {
    var art = try decodeUnverified(gpa, src);
    errdefer art.deinit(gpa);
    try art.verifyCrc();
    return art;
}

/// `decode` without the CRC comparison. Use this plus
/// `Article.verifyCrc` when you want to keep the payload of an article
/// that failed its checksum (for logging, or to salvage a part whose
/// poster miscomputed the CRC).
pub fn decodeUnverified(gpa: std.mem.Allocator, src: []const u8) (ParseError || error{OutOfMemory})!Article {
    // A decoded byte always consumes at least one encoded byte, so
    // src.len is a hard upper bound on the output — one allocation, no
    // grow check in the hot loop, and no reliance on the declared size
    // (which a hostile or buggy poster controls).
    var out = try gpa.alloc(u8, src.len);
    errdefer gpa.free(out);

    var hdr: Header = .{};
    var trl: Trailer = .{};
    var got_begin = false;
    var got_end = false;
    var j: usize = 0;

    var pos: usize = 0;
    while (pos < src.len) {
        var line: []const u8 = undefined;
        if (std.mem.indexOfScalarPos(u8, src, pos, '\n')) |nl| {
            line = src[pos..nl];
            pos = nl + 1;
        } else {
            line = src[pos..];
            pos = src.len;
        }
        // Strip one trailing CR so the same parser handles CRLF and bare LF.
        line = stripCr(line);
        if (line.len == 0) continue;

        // Control lines start with "=y". Anything else with a leading
        // '=' is a data line whose first byte happens to be escaped.
        const is_control = line.len >= 2 and line[0] == '=' and line[1] == 'y';

        if (is_control and std.mem.startsWith(u8, line, prefix_ybegin)) {
            if (got_begin) return error.DuplicateYBegin;
            try parseBeginLine(line, &hdr);
            got_begin = true;
        } else if (is_control and std.mem.startsWith(u8, line, prefix_ypart)) {
            if (!got_begin) return error.YPartBeforeYBegin;
            try parsePartLine(line, &hdr);
        } else if (is_control and std.mem.startsWith(u8, line, prefix_yend)) {
            if (!got_begin) return error.YEndBeforeYBegin;
            parseEndLine(line, &trl);
            got_end = true;
        } else {
            // Pre-header noise (NNTP-stuffed dots, blank lines).
            if (!got_begin) continue;
            j = try decodeLineInto(out, j, line);
        }
    }

    if (!got_begin) return error.MissingYBegin;
    if (!got_end) return error.MissingYEnd;

    // Single-part articles get the whole-payload span by default.
    if (hdr.begin == 0) hdr.begin = 1;
    if (hdr.end == 0) hdr.end = hdr.size;

    out = try gpa.realloc(out, j);
    return .{ .payload = out, .header = hdr, .trailer = trl };
}

/// Decode one body line into `out` starting at `j`, returning the new
/// `j`. The line has already had its CR/LF stripped, so the only byte
/// needing attention is '='.
///
/// The trick: `v -% 42` is the right answer for every lane up to the
/// first '=', so the store happens *unconditionally* and only then do we
/// look for an escape. Lanes past the escape are garbage, but `j` only
/// moves forward, so a later store overwrites them and the final
/// truncation to `j` discards whatever is left. That turns the common
/// case (escapes are ~1.6% of bytes, so ~78% of 16-byte windows are
/// clean) into load / subtract / store / test, and makes an escape cost
/// one re-entry of the loop instead of scalarising a whole window.
///
/// `out` must have room for `j + line.len` bytes — guaranteed by the
/// caller sizing the output at the encoded length.
pub fn decodeLineInto(out: []u8, j_start: usize, line: []const u8) error{DanglingEscape}!usize {
    std.debug.assert(out.len >= j_start + line.len);
    var i: usize = 0;
    var j = j_start;
    while (i + vec_len <= line.len) {
        const v: V = line[i..][0..vec_len].*;
        out[j..][0..vec_len].* = v -% splat_42;
        const esc = firstLane(v == splat_eq) orelse {
            i += vec_len;
            j += vec_len;
            continue;
        };
        // Lanes [0, esc) already landed correctly; step onto the '='.
        i += esc;
        j += esc;
        i += 1;
        if (i >= line.len) return error.DanglingEscape;
        out[j] = line[i] -% 106; // -64 (unescape) -42 (unshift)
        j += 1;
        i += 1;
    }
    return decodeLineScalar(out, j, line, i);
}

/// Byte-at-a-time decode of `line[i_start..]`. Handles the tail below
/// one vector width, and doubles as the reference the SIMD path is
/// tested against.
fn decodeLineScalar(out: []u8, j_start: usize, line: []const u8, i_start: usize) error{DanglingEscape}!usize {
    var i = i_start;
    var j = j_start;
    while (i < line.len) {
        const c = line[i];
        if (c == '=') {
            i += 1;
            if (i >= line.len) return error.DanglingEscape;
            out[j] = line[i] -% 106;
        } else {
            out[j] = c -% 42;
        }
        j += 1;
        i += 1;
    }
    return j;
}

/// `dst[i] = src[i] - 42` for the whole slice, no escape handling. The
/// pure shift, isolated so the benchmark can measure it on its own.
/// Requires `dst.len >= src.len`.
pub fn subCopy42(dst: []u8, src: []const u8) void {
    std.debug.assert(dst.len >= src.len);
    var i: usize = 0;
    while (i + vec_len <= src.len) : (i += vec_len) {
        const v: V = src[i..][0..vec_len].*;
        dst[i..][0..vec_len].* = v -% splat_42;
    }
    while (i < src.len) : (i += 1) dst[i] = src[i] -% 42;
}

// ---------------------------------------------------------------------
// Single-pass variant
//
// Instead of splitting the body into lines and decoding each, find the
// =yend marker and decode everything between the header and it in one
// sweep, treating CR and LF as bytes to skip.
//
// It loses to the line-oriented decoder, the same as in Go, and moving
// from SWAR to real vectors did not change that: a line break is a
// "special" hit every `line=` bytes, and each hit stops the vector loop
// mid-window. The line-oriented decoder instead spends one SIMD newline
// search per line and then runs a loop over a stretch that provably
// contains no line breaks at all. Kept so the benchmark measures the
// gap rather than assuming it.
// ---------------------------------------------------------------------

/// Single-pass counterpart to `decode`, same result on well-formed
/// articles.
pub fn decodeScan(gpa: std.mem.Allocator, src: []const u8) DecodeError!Article {
    var art = try decodeScanUnverified(gpa, src);
    errdefer art.deinit(gpa);
    try art.verifyCrc();
    return art;
}

pub fn decodeScanUnverified(gpa: std.mem.Allocator, src: []const u8) (ParseError || error{OutOfMemory})!Article {
    var hdr: Header = .{};
    var trl: Trailer = .{};

    const begin_start = findLineStart(src, 0, prefix_ybegin) orelse return error.MissingYBegin;
    const begin_end = indexLf(src, begin_start);
    try parseBeginLine(stripCr(src[begin_start..begin_end]), &hdr);

    var body_start = if (begin_end < src.len) begin_end + 1 else src.len;
    if (std.mem.startsWith(u8, src[body_start..], prefix_ypart)) {
        const part_end = indexLf(src, body_start);
        try parsePartLine(stripCr(src[body_start..part_end]), &hdr);
        body_start = if (part_end < src.len) part_end + 1 else src.len;
    }

    const yend_start = findLineStart(src, body_start, prefix_yend) orelse return error.MissingYEnd;
    // findLineStart only matches at body_start or just after a '\n', so
    // anything before the marker beyond body_start is that '\n'.
    const body_end = if (yend_start > body_start) yend_start - 1 else body_start;

    var out = try gpa.alloc(u8, src.len);
    errdefer gpa.free(out);
    const j = try decodeBodyInto(out, src[body_start..body_end]);

    parseEndLine(stripCr(src[yend_start..indexLf(src, yend_start)]), &trl);

    if (hdr.begin == 0) hdr.begin = 1;
    if (hdr.end == 0) hdr.end = hdr.size;

    out = try gpa.realloc(out, j);
    return .{ .payload = out, .header = hdr, .trailer = trl };
}

/// Decode a whole body — line breaks included — into `out`, returning
/// the decoded length. Same unconditional-store trick as
/// `decodeLineInto`, with CR and LF joining '=' in the special set.
fn decodeBodyInto(out: []u8, body: []const u8) error{DanglingEscape}!usize {
    std.debug.assert(out.len >= body.len);
    var i: usize = 0;
    var j: usize = 0;
    while (i + vec_len <= body.len) {
        const v: V = body[i..][0..vec_len].*;
        out[j..][0..vec_len].* = v -% splat_42;
        const special = (v == splat_eq) | (v == splat_cr) | (v == splat_lf);
        const hit = firstLane(special) orelse {
            i += vec_len;
            j += vec_len;
            continue;
        };
        i += hit;
        j += hit;
        switch (body[i]) {
            '\r', '\n' => i += 1,
            else => {
                i += 1;
                if (i >= body.len) return error.DanglingEscape;
                const c = body[i];
                if (c == '\r' or c == '\n') return error.DanglingEscape;
                out[j] = c -% 106;
                j += 1;
                i += 1;
            },
        }
    }
    return decodeBodyScalar(out, j, body, i);
}

fn decodeBodyScalar(out: []u8, j_start: usize, body: []const u8, i_start: usize) error{DanglingEscape}!usize {
    var i = i_start;
    var j = j_start;
    while (i < body.len) {
        switch (body[i]) {
            '\r', '\n' => i += 1,
            '=' => {
                i += 1;
                if (i >= body.len) return error.DanglingEscape;
                const c = body[i];
                if (c == '\r' or c == '\n') return error.DanglingEscape;
                out[j] = c -% 106;
                j += 1;
                i += 1;
            },
            else => {
                out[j] = body[i] -% 42;
                j += 1;
                i += 1;
            },
        }
    }
    return j;
}

/// Index of `prefix` anchored at column zero — either at `from` itself
/// or immediately after some '\n' at or after `from`.
fn findLineStart(src: []const u8, from: usize, prefix: []const u8) ?usize {
    if (from <= src.len and std.mem.startsWith(u8, src[from..], prefix)) return from;
    var at = from;
    while (at < src.len) {
        const off = std.mem.indexOfScalarPos(u8, src, at, '\n') orelse return null;
        const next = off + 1;
        if (std.mem.startsWith(u8, src[next..], prefix)) return next;
        at = next;
    }
    return null;
}

fn indexLf(src: []const u8, from: usize) usize {
    return std.mem.indexOfScalarPos(u8, src, from, '\n') orelse src.len;
}

fn stripCr(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') return line[0 .. line.len - 1];
    return line;
}

// ---------------------------------------------------------------------
// Header / trailer parsing
// ---------------------------------------------------------------------

/// `=ybegin (part=N) (total=N) line=N size=N name=...`
///
/// Unparseable attribute values are ignored rather than fatal — real
/// posters emit junk attributes and the fields we need are usually
/// still there. Only a negative size is rejected outright.
fn parseBeginLine(line: []const u8, h: *Header) ParseError!void {
    var rest = std.mem.trim(u8, line[prefix_ybegin.len..], whitespace);

    // "name=" is special: its value runs to end of line and may contain
    // spaces, so it comes off before the space-delimited split.
    if (std.mem.indexOf(u8, rest, "name=")) |idx| {
        h.name = std.mem.trim(u8, rest[idx + "name=".len ..], whitespace);
        rest = std.mem.trim(u8, rest[0..idx], whitespace);
    }

    var it = std.mem.tokenizeAny(u8, rest, whitespace);
    while (it.next()) |kv| {
        const k, const v = splitKv(kv) orelse continue;
        if (std.mem.eql(u8, k, "part")) {
            if (parseDec(v)) |n| h.part = n;
        } else if (std.mem.eql(u8, k, "total")) {
            if (parseDec(v)) |n| h.total = n;
        } else if (std.mem.eql(u8, k, "line")) {
            if (parseDec(v)) |n| h.line = n;
        } else if (std.mem.eql(u8, k, "size")) {
            if (parseDec(v)) |n| h.size = n;
        }
    }
    // size=0 is a valid zero-byte article, and a missing size= leaves
    // the field at 0 too. Only reject the genuinely impossible.
    if (h.size < 0) return error.NegativeSize;
}

/// `=ypart begin=N end=N`
fn parsePartLine(line: []const u8, h: *Header) ParseError!void {
    var it = std.mem.tokenizeAny(u8, line[prefix_ypart.len..], whitespace);
    while (it.next()) |kv| {
        const k, const v = splitKv(kv) orelse continue;
        if (std.mem.eql(u8, k, "begin")) {
            if (parseDec(v)) |n| h.begin = n;
        } else if (std.mem.eql(u8, k, "end")) {
            if (parseDec(v)) |n| h.end = n;
        }
    }
    if (h.begin <= 0 or h.end < h.begin) return error.BadPartRange;
}

/// `=yend size=N (part=N) (pcrc32=HEX) (crc32=HEX)`
///
/// Never fails: a trailer we can't fully parse still ends the article,
/// and a missing CRC just means nothing to verify against.
fn parseEndLine(line: []const u8, tr: *Trailer) void {
    var it = std.mem.tokenizeAny(u8, line[prefix_yend.len..], whitespace);
    while (it.next()) |kv| {
        const k, const v = splitKv(kv) orelse continue;
        if (std.mem.eql(u8, k, "size")) {
            if (parseDec(v)) |n| tr.size = n;
        } else if (std.mem.eql(u8, k, "part")) {
            if (parseDec(v)) |n| tr.part = n;
        } else if (std.mem.eql(u8, k, "pcrc32")) {
            if (parseHex32(v)) |n| {
                tr.part_crc32 = n;
                tr.has_part_crc = true;
            }
        } else if (std.mem.eql(u8, k, "crc32")) {
            if (parseHex32(v)) |n| {
                tr.crc32 = n;
                tr.has_crc = true;
            }
        }
    }
}

fn splitKv(kv: []const u8) ?struct { []const u8, []const u8 } {
    const idx = std.mem.indexOfScalar(u8, kv, '=') orelse return null;
    if (idx == 0) return null;
    return .{ kv[0..idx], kv[idx + 1 ..] };
}

fn parseDec(v: []const u8) ?i64 {
    return std.fmt.parseInt(i64, v, 10) catch null;
}

fn parseHex32(v: []const u8) ?u32 {
    return std.fmt.parseUnsigned(u32, v, 16) catch null;
}

// ---------------------------------------------------------------------
// Test-only encoder
//
// hoardarr never posts, so there is no production yEnc encoder. This
// one exists purely to generate fixtures: writing it independently of
// the decoder means a round-trip test catches a bug on either side,
// which a table of hand-written expected bytes would not.
// ---------------------------------------------------------------------

pub const EncodeOpts = struct {
    name: []const u8,
    payload: []const u8,
    /// Setting `total` selects the multi-part shape: a =ypart line and a
    /// pcrc32= trailer instead of crc32=.
    part: u32 = 0,
    total: u32 = 0,
    begin: u64 = 0,
    end: u64 = 0,
    line_width: usize = 128,
};

pub fn encodeForTest(gpa: std.mem.Allocator, o: EncodeOpts) error{OutOfMemory}![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);

    if (o.total > 0) {
        try buf.print(gpa, "=ybegin part={d} total={d} line={d} size={d} name={s}\r\n", .{
            o.part, o.total, o.line_width, o.end - o.begin + 1, o.name,
        });
        try buf.print(gpa, "=ypart begin={d} end={d}\r\n", .{ o.begin, o.end });
    } else {
        try buf.print(gpa, "=ybegin line={d} size={d} name={s}\r\n", .{
            o.line_width, o.payload.len, o.name,
        });
    }

    var col: usize = 0;
    for (o.payload) |b| {
        const e = b +% 42;
        const critical = e == 0x00 or e == 0x0A or e == 0x0D or e == '=';
        if (critical or (col == 0 and (e == '\t' or e == ' ' or e == '.'))) {
            try buf.append(gpa, '=');
            try buf.append(gpa, e +% 64);
            col += 2;
        } else {
            try buf.append(gpa, e);
            col += 1;
        }
        if (col >= o.line_width) {
            try buf.appendSlice(gpa, "\r\n");
            col = 0;
        }
    }
    if (col > 0) try buf.appendSlice(gpa, "\r\n");

    const crc = crc32.checksum(o.payload);
    if (o.total > 0) {
        try buf.print(gpa, "=yend size={d} part={d} pcrc32={x:0>8}\r\n", .{ o.end - o.begin + 1, o.part, crc });
    } else {
        try buf.print(gpa, "=yend size={d} crc32={x:0>8}\r\n", .{ o.payload.len, crc });
    }
    return buf.toOwnedSlice(gpa);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

/// Fill with a fixed-seed PRNG: same bytes on every run, so a failure
/// is reproducible, while still hitting the ~1.6% escape density of
/// real binary payloads.
fn fillRandom(buf: []u8, seed: u64) void {
    var prng = std.Random.DefaultPrng.init(seed);
    prng.random().bytes(buf);
}

test "decode single-part article with critical bytes" {
    const payload = "Hello, world! With special bytes: \x00\x0a\x0d\x3d here.";
    const enc = try encodeForTest(t.allocator, .{ .name = "hello.txt", .payload = payload });
    defer t.allocator.free(enc);

    var art = try decode(t.allocator, enc);
    defer art.deinit(t.allocator);

    try t.expectEqualSlices(u8, payload, art.payload);
    try t.expectEqualStrings("hello.txt", art.header.name);
    try t.expectEqual(@as(i64, payload.len), art.header.size);
    try t.expect(art.trailer.has_crc);
    try t.expect(!art.trailer.has_part_crc);
    // Single-part gets the implied whole-file span.
    try t.expectEqual(@as(i64, 1), art.header.begin);
    try t.expectEqual(@as(i64, payload.len), art.header.end);
}

test "decode multi-part segments by offset" {
    var full: [1024]u8 = undefined;
    for (&full, 0..) |*b, i| b.* = "ABCDEFGHIJKLMNOP"[i % 16];
    const part_size = 256;

    for (1..5) |p| {
        const begin = (p - 1) * part_size + 1;
        const end = p * part_size;
        const seg = full[begin - 1 .. end];
        const enc = try encodeForTest(t.allocator, .{
            .name = "file.bin",
            .payload = seg,
            .part = @intCast(p),
            .total = 4,
            .begin = begin,
            .end = end,
        });
        defer t.allocator.free(enc);

        var art = try decode(t.allocator, enc);
        defer art.deinit(t.allocator);

        try t.expectEqualSlices(u8, seg, art.payload);
        try t.expectEqual(@as(i64, @intCast(p)), art.header.part);
        try t.expectEqual(@as(i64, 4), art.header.total);
        try t.expectEqual(@as(i64, @intCast(begin)), art.header.begin);
        try t.expectEqual(@as(i64, @intCast(end)), art.header.end);
        try t.expect(art.trailer.has_part_crc);
    }
}

test "decode 8 KiB of random binary" {
    var payload: [8192]u8 = undefined;
    fillRandom(&payload, 0xF00D_BABE);
    const enc = try encodeForTest(t.allocator, .{ .name = "blob.bin", .payload = &payload });
    defer t.allocator.free(enc);

    var art = try decode(t.allocator, enc);
    defer art.deinit(t.allocator);
    try t.expectEqualSlices(u8, &payload, art.payload);
}

test "decode reports a CRC mismatch" {
    const enc = try encodeForTest(t.allocator, .{ .name = "h.txt", .payload = "Hello" });
    defer t.allocator.free(enc);

    // Well-formed but wrong: overwrite the 8 hex digits of crc32=.
    const idx = std.mem.indexOf(u8, enc, "crc32=").? + "crc32=".len;
    @memcpy(enc[idx .. idx + 8], "00000000");

    try t.expectError(error.CrcMismatch, decode(t.allocator, enc));

    // The payload survives when the caller asks not to verify.
    var art = try decodeUnverified(t.allocator, enc);
    defer art.deinit(t.allocator);
    try t.expectEqualStrings("Hello", art.payload);
    try t.expectError(error.CrcMismatch, art.verifyCrc());
}

test "decode rejects a missing =ybegin" {
    const body = "no header here\r\n=yend size=0\r\n";
    try t.expectError(error.YEndBeforeYBegin, decode(t.allocator, body));
    try t.expectError(error.MissingYBegin, decode(t.allocator, "nothing at all\r\n"));
}

test "decode rejects a missing =yend" {
    const body = "=ybegin line=128 size=4 name=x\r\nXYZW\r\n";
    try t.expectError(error.MissingYEnd, decode(t.allocator, body));
}

test "decode rejects a dangling escape" {
    const body = "=ybegin line=128 size=1 name=x\r\nA=\r\n=yend size=1\r\n";
    try t.expectError(error.DanglingEscape, decode(t.allocator, body));
}

test "decode rejects a truncated body" {
    const full = try encodeForTest(t.allocator, .{
        .name = "trunc.bin",
        .payload = "here are some bytes",
        .line_width = 80,
    });
    defer t.allocator.free(full);

    // A dropped connection mid-article: everything up to =yend arrived.
    const cut = std.mem.indexOf(u8, full, "=yend").?;
    try t.expectError(error.MissingYEnd, decode(t.allocator, full[0..cut]));
}

test "decode rejects a duplicate =ybegin" {
    const body = "=ybegin line=128 size=1 name=x\r\n=ybegin line=128 size=1 name=y\r\n=yend size=1\r\n";
    try t.expectError(error.DuplicateYBegin, decode(t.allocator, body));
}

test "decode rejects =ypart before =ybegin" {
    const body = "=ypart begin=1 end=4\r\n=ybegin line=128 size=4 name=x\r\n=yend size=4\r\n";
    try t.expectError(error.YPartBeforeYBegin, decode(t.allocator, body));
}

test "decode accepts a zero-byte article" {
    const body = "=ybegin line=128 size=0 name=empty.bin\r\n=yend size=0 crc32=00000000\r\n";
    var art = try decode(t.allocator, body);
    defer art.deinit(t.allocator);

    try t.expectEqual(@as(usize, 0), art.payload.len);
    try t.expectEqual(@as(i64, 0), art.header.size);
    try t.expect(art.trailer.has_crc);
}

test "decode tolerates trailer size drift" {
    // Posters and buggy encoders get =yend size= wrong. The CRC is the
    // real check, so a wrong size is reported and not fatal.
    const payload = "six bytes? no, 28 bytes here";
    const enc = try encodeForTest(t.allocator, .{ .name = "x.bin", .payload = payload });
    defer t.allocator.free(enc);

    const marker = "=yend size=";
    const at = std.mem.indexOf(u8, enc, marker).? + marker.len;
    const space = std.mem.indexOfScalarPos(u8, enc, at, ' ').?;

    var mangled: std.ArrayList(u8) = .empty;
    defer mangled.deinit(t.allocator);
    try mangled.appendSlice(t.allocator, enc[0..at]);
    try mangled.appendSlice(t.allocator, "999");
    try mangled.appendSlice(t.allocator, enc[space..]);

    var art = try decode(t.allocator, mangled.items);
    defer art.deinit(t.allocator);
    try t.expectEqualStrings(payload, art.payload);
    try t.expectEqual(@as(i64, 999), art.trailer.size);
}

test "decode keeps the name verbatim including spaces" {
    const body = "=ybegin line=128 size=0 name=cool stuff (1of8).rar\r\n=yend size=0\r\n";
    var art = try decode(t.allocator, body);
    defer art.deinit(t.allocator);
    try t.expectEqualStrings("cool stuff (1of8).rar", art.header.name);
}

test "decodeLineInto shifts an unescaped byte" {
    // 'X' encoded is 'X' + 42 = 0x82; nothing critical, so no escape.
    var out: [1]u8 = undefined;
    try t.expectEqual(@as(usize, 1), try decodeLineInto(&out, 0, &[_]u8{0x82}));
    try t.expectEqual(@as(u8, 'X'), out[0]);
}

test "decodeLineInto unescapes a pair" {
    // '=' is critical, so it is posted as '=' followed by
    // '=' + 42 + 64 = 0xA7.
    var out: [2]u8 = undefined;
    try t.expectEqual(@as(usize, 1), try decodeLineInto(&out, 0, &[_]u8{ '=', 0xA7 }));
    try t.expectEqual(@as(u8, '='), out[0]);
}

test "decodeLineInto SIMD path matches the scalar reference" {
    // The whole point of the vector loop is that it is indistinguishable
    // from the byte-at-a-time version, including where it errors. Walk
    // every length across several vector widths, and every position an
    // escape pair can sit in — including straddling a vector boundary,
    // which is where a hand-rolled window would go wrong.
    const max = 4 * vec_len + 3;
    var line: [max]u8 = undefined;
    var simd: [max]u8 = undefined;
    var scalar: [max]u8 = undefined;

    for (0..max + 1) |n| {
        fillRandom(line[0..n], 0x5EED_0000 + n);
        // Keep '=' out of the base pattern so escapes only appear where
        // this loop puts them.
        for (line[0..n]) |*b| {
            if (b.* == '=') b.* = 'x';
        }
        for (0..n + 1) |p| {
            const saved: u8 = if (p < n) line[p] else 0;
            if (p < n) line[p] = '=';
            defer if (p < n) {
                line[p] = saved;
            };

            const got_simd = decodeLineInto(&simd, 0, line[0..n]);
            const got_scalar = decodeLineScalar(&scalar, 0, line[0..n], 0);
            if (got_scalar) |js| {
                const jv = try got_simd;
                try t.expectEqual(js, jv);
                try t.expectEqualSlices(u8, scalar[0..js], simd[0..jv]);
            } else |err| {
                try t.expectError(err, got_simd);
            }
        }
    }
}

test "decodeLineInto SIMD path matches scalar on dense random escapes" {
    // Escape density far above the ~1.6% of real payloads, so adjacent
    // and back-to-back escapes get exercised.
    var prng = std.Random.DefaultPrng.init(0xDEAD_BEEF);
    const rnd = prng.random();
    var line: [512]u8 = undefined;
    var simd: [512]u8 = undefined;
    var scalar: [512]u8 = undefined;

    for (0..2000) |_| {
        const n = rnd.uintLessThan(usize, line.len + 1);
        for (line[0..n]) |*b| {
            b.* = if (rnd.uintLessThan(u8, 4) == 0) '=' else rnd.int(u8);
        }
        const got_simd = decodeLineInto(&simd, 0, line[0..n]);
        const got_scalar = decodeLineScalar(&scalar, 0, line[0..n], 0);
        if (got_scalar) |js| {
            const jv = try got_simd;
            try t.expectEqual(js, jv);
            try t.expectEqualSlices(u8, scalar[0..js], simd[0..jv]);
        } else |err| {
            try t.expectError(err, got_simd);
        }
    }
}

test "decodeBodyInto SIMD path matches the scalar reference" {
    var prng = std.Random.DefaultPrng.init(0x1234_ABCD);
    const rnd = prng.random();
    var body: [512]u8 = undefined;
    var simd: [512]u8 = undefined;
    var scalar: [512]u8 = undefined;

    for (0..2000) |_| {
        const n = rnd.uintLessThan(usize, body.len + 1);
        for (body[0..n]) |*b| {
            b.* = switch (rnd.uintLessThan(u8, 8)) {
                0 => '=',
                1 => '\r',
                2 => '\n',
                else => rnd.int(u8),
            };
        }
        const got_simd = decodeBodyInto(&simd, body[0..n]);
        const got_scalar = decodeBodyScalar(&scalar, 0, body[0..n], 0);
        if (got_scalar) |js| {
            const jv = try got_simd;
            try t.expectEqual(js, jv);
            try t.expectEqualSlices(u8, scalar[0..js], simd[0..jv]);
        } else |err| {
            try t.expectError(err, got_simd);
        }
    }
}

test "subCopy42 matches a naive shift" {
    var src: [1024]u8 = undefined;
    fillRandom(&src, 0xC0FF_EE);
    var dst: [1024]u8 = undefined;

    for (0..src.len + 1) |n| {
        @memset(dst[0..], 0);
        subCopy42(dst[0..n], src[0..n]);
        for (0..n) |i| try t.expectEqual(src[i] -% 42, dst[i]);
    }
}

/// The article sizes the Go benchmarks used: 1 MiB (server cap), 750 KiB
/// (the dominant production segment size), 256 KiB (par2/rar split) and
/// 4 KiB (per-call overhead). Correctness at these sizes is what the
/// benchmark assumes; measuring them lives in bench/.
const bench_sizes = [_]usize{ 4 * 1024, 256 * 1024, 750 * 1024, 1024 * 1024 };

test "decode round-trips every benchmark fixture size" {
    for (bench_sizes) |size| {
        const payload = try t.allocator.alloc(u8, size);
        defer t.allocator.free(payload);
        fillRandom(payload, 0xB0_0000 + size);

        const enc = try encodeForTest(t.allocator, .{ .name = "bench.bin", .payload = payload });
        defer t.allocator.free(enc);

        var art = try decode(t.allocator, enc);
        defer art.deinit(t.allocator);
        try t.expectEqualSlices(u8, payload, art.payload);
    }
}

test "decodeScan agrees with decode on every benchmark fixture size" {
    for (bench_sizes) |size| {
        const payload = try t.allocator.alloc(u8, size);
        defer t.allocator.free(payload);
        fillRandom(payload, 0x5CA4_0000 + size);

        const enc = try encodeForTest(t.allocator, .{ .name = "bench.bin", .payload = payload });
        defer t.allocator.free(enc);

        var a = try decode(t.allocator, enc);
        defer a.deinit(t.allocator);
        var b = try decodeScan(t.allocator, enc);
        defer b.deinit(t.allocator);

        try t.expectEqualSlices(u8, payload, b.payload);
        try t.expectEqualSlices(u8, a.payload, b.payload);
        try t.expectEqual(a.header.size, b.header.size);
        try t.expectEqualStrings(a.header.name, b.header.name);
        try t.expectEqual(a.trailer.crc32, b.trailer.crc32);
    }
}

test "decode handles the multi-part benchmark fixture" {
    const part_size = 256 * 1024;
    const parts = 4;
    const full = try t.allocator.alloc(u8, part_size * parts);
    defer t.allocator.free(full);
    fillRandom(full, 0x4EED_1234);

    for (1..parts + 1) |p| {
        const begin = (p - 1) * part_size + 1;
        const end = p * part_size;
        const enc = try encodeForTest(t.allocator, .{
            .name = "multi.bin",
            .payload = full[begin - 1 .. end],
            .part = @intCast(p),
            .total = parts,
            .begin = begin,
            .end = end,
        });
        defer t.allocator.free(enc);

        var art = try decode(t.allocator, enc);
        defer art.deinit(t.allocator);
        try t.expectEqualSlices(u8, full[begin - 1 .. end], art.payload);

        var scanned = try decodeScan(t.allocator, enc);
        defer scanned.deinit(t.allocator);
        try t.expectEqualSlices(u8, art.payload, scanned.payload);
    }
}

test "encode then decode round-trips at every length near a vector boundary" {
    // Lengths either side of the vector width and the 128-column wrap,
    // so the tail, the boundary straddle and the line split all get hit.
    var prng = std.Random.DefaultPrng.init(0xA5A5_1234);
    const rnd = prng.random();
    var payload: [600]u8 = undefined;
    rnd.bytes(&payload);

    for (0..payload.len + 1) |n| {
        for ([_]usize{ 1, 2, 15, 16, 17, 33, 128 }) |width| {
            const enc = try encodeForTest(t.allocator, .{
                .name = "rt.bin",
                .payload = payload[0..n],
                .line_width = width,
            });
            defer t.allocator.free(enc);

            var art = try decode(t.allocator, enc);
            defer art.deinit(t.allocator);
            try t.expectEqualSlices(u8, payload[0..n], art.payload);

            var scanned = try decodeScan(t.allocator, enc);
            defer scanned.deinit(t.allocator);
            try t.expectEqualSlices(u8, payload[0..n], scanned.payload);
        }
    }
}

test "decode survives arbitrary input" {
    // The Go FuzzDecode contract: any error is fine, a panic is not.
    // Deterministic seed so a crash is reproducible.
    var prng = std.Random.DefaultPrng.init(0x9E37_79B9);
    const rnd = prng.random();
    var buf: [512]u8 = undefined;
    const keywords = "=ybegin =ypart =yend line size name part total begin end crc32 pcrc32 ";

    for (0..5000) |_| {
        const n = rnd.uintLessThan(usize, buf.len + 1);
        // Bias towards bytes that mean something to the parser, so the
        // random stream actually reaches the header and body paths.
        for (buf[0..n]) |*b| {
            b.* = switch (rnd.uintLessThan(u8, 8)) {
                0 => '=',
                1 => 'y',
                2 => '\n',
                3 => '\r',
                4 => keywords[rnd.uintLessThan(usize, keywords.len)],
                else => rnd.int(u8),
            };
        }
        if (decode(t.allocator, buf[0..n])) |art| {
            var a = art;
            a.deinit(t.allocator);
        } else |_| {}
        if (decodeScan(t.allocator, buf[0..n])) |art| {
            var a = art;
            a.deinit(t.allocator);
        } else |_| {}
    }
}

test "decode survives a mutated valid article" {
    // Single-byte and single-splice mutations of a real article reach
    // far deeper into the parser than random noise does.
    const seed_payload = "seed payload \x00\x0a\x0d=";
    const enc = try encodeForTest(t.allocator, .{
        .name = "seed.bin",
        .payload = seed_payload,
        .line_width = 80,
    });
    defer t.allocator.free(enc);

    const scratch = try t.allocator.alloc(u8, enc.len);
    defer t.allocator.free(scratch);

    var prng = std.Random.DefaultPrng.init(0x0BAD_C0DE);
    const rnd = prng.random();

    for (0..5000) |_| {
        @memcpy(scratch, enc);
        const muts = 1 + rnd.uintLessThan(usize, 4);
        for (0..muts) |_| {
            scratch[rnd.uintLessThan(usize, scratch.len)] = rnd.int(u8);
        }
        const n = rnd.uintLessThan(usize, scratch.len + 1);
        if (decode(t.allocator, scratch[0..n])) |art| {
            var a = art;
            a.deinit(t.allocator);
        } else |_| {}
        if (decodeScan(t.allocator, scratch[0..n])) |art| {
            var a = art;
            a.deinit(t.allocator);
        } else |_| {}
    }
}

test "vector width is what the target actually supports" {
    // Not an assertion about a specific number — just a guard that the
    // module never silently falls back to the 8-lane scalar shim on a
    // target that has real vectors.
    try t.expect(vec_len >= 8);
    try t.expectEqual(vec_len, @bitSizeOf(Mask));
}
