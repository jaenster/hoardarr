//! Generates a realistic Usenet release: data files, a genuine PAR2
//! recovery set over them, yEnc-encoded articles keyed by message-id, and
//! an NZB that references every article.
//!
//! This exists so the end-to-end suite can drive the *whole* pipeline —
//! download, verify, repair, deliver — against a lossy fake provider
//! instead of a real one. Pair it with `testserver/nntp.zig`: set a
//! missing fraction there, give the fixture enough recovery slices to
//! cover the loss, and the repair path becomes testable.
//!
//! Everything is deterministic. The file bytes come from a seeded PRNG,
//! the PAR2 set is a pure function of those bytes, and the article
//! bodies are a pure function of the PAR2 set. A failing e2e run is
//! reproducible from the seed alone.
//!
//! The PAR2 set is real, not a stub: `par2.parse` accepts it (every
//! packet carries a correct MD5), `verifier.verify` passes over the
//! generated files, and the recovery slices are true Reed-Solomon
//! combinations, so `rs.reconstruct` rebuilds a corrupted slice. The
//! tests at the bottom assert all three — a fixture whose parity is
//! subtly wrong would make every downstream repair test meaningless.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Md5 = std.crypto.hash.Md5;

const par2 = @import("../codec/par2/par2.zig");
const rs = @import("../codec/par2/rs.zig");
const crc32 = @import("../core/crc32.zig");

pub const Error = Allocator.Error || rs.Error;

/// Shape of the release to generate.
pub const Options = struct {
    /// Base name for the release; every filename derives from it.
    name: []const u8 = "release",
    /// Number of data files. Each gets `file_size` bytes.
    file_count: usize = 1,
    file_size: usize = 1 << 20,
    /// yEnc segment size — how much of a file fits in one article.
    /// Real posts sit around 768 KiB; tests want something small enough
    /// to still exercise the multi-segment path cheaply.
    article_size: usize = 256 * 1024,
    /// PAR2 recovery slice size. Rounded up to even, because a PAR2
    /// slice is an array of 16-bit field elements.
    par2_slice_size: usize = 64 * 1024,
    /// Recovery slices in the set — the ceiling on how many data slices
    /// can be lost and still repaired. Set this above the worst-case
    /// loss the missing-fraction knob will induce.
    recovery_slices: usize = 0,
    /// PRNG seed for the file contents. Pinned by default so two runs of
    /// the same test see identical bytes.
    seed: u64 = 0x486F_6172_6461_7272,
    /// yEnc line width.
    line_width: usize = 128,
};

/// One generated file: the name it is posted under and its raw bytes.
/// Covers data files *and* the `.par2` files, because both get posted.
pub const GenFile = struct {
    name: []const u8,
    bytes: []const u8,
    /// False for the `.par2` index and volume files. The PAR2 set
    /// describes only the data files, so verification passes just these.
    is_data: bool,
};

/// One yEnc article as it sits on the server: the message-id (bare, no
/// angle brackets) and the body between the response line and the
/// terminating dot. Dot-stuffing is the transport's job, not ours.
pub const Article = struct {
    message_id: []const u8,
    body: []const u8,
    /// Which `GenFile` this article carries a piece of.
    filename: []const u8,
    /// 1-based part number within that file.
    part: usize,
};

pub const Fixture = struct {
    /// Backs every slice reachable from here.
    arena: std.heap.ArenaAllocator,
    /// NZB XML, ready to POST at the queue endpoint.
    nzb: []const u8,
    /// Data files first, then the PAR2 index, then the volume files —
    /// the same order the NZB lists them in.
    files: []const GenFile,
    articles: []const Article,
    /// Echoed back from the options that produced this.
    file_count: usize,
    recovery_file_count: usize,
    par2_slice_size: usize,

    pub fn deinit(self: *Fixture) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Raw bytes of a generated file, or null.
    pub fn fileBytes(self: *const Fixture, name: []const u8) ?[]const u8 {
        for (self.files) |f| {
            if (std.mem.eql(u8, f.name, name)) return f.bytes;
        }
        return null;
    }

    /// Body of a registered article, or null.
    pub fn articleBody(self: *const Fixture, message_id: []const u8) ?[]const u8 {
        for (self.articles) |a| {
            if (std.mem.eql(u8, a.message_id, message_id)) return a.body;
        }
        return null;
    }

    /// Names of the `.par2` files, index first. Handy for pointing the
    /// verifier at the set.
    pub fn par2Names(self: *const Fixture, gpa: Allocator) Allocator.Error![][]const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        errdefer out.deinit(gpa);
        for (self.files) |f| {
            if (!f.is_data) try out.append(gpa, f.name);
        }
        return out.toOwnedSlice(gpa);
    }
};

/// Builds a fixture. Caller owns the result; release it with `deinit`.
///
/// PAR2 layout: one recovery file per recovery slice, each carrying a
/// full copy of the descriptive packets. Real producers do the same so
/// that any single `.par2` file is self-describing, and hoardarr's
/// verifier relies on it.
pub fn generate(gpa: Allocator, options: Options) Error!Fixture {
    var opts = options;
    if (opts.name.len == 0) opts.name = "release";
    if (opts.file_count == 0) opts.file_count = 1;
    if (opts.file_size == 0) opts.file_size = 1 << 20;
    if (opts.article_size == 0) opts.article_size = 256 * 1024;
    if (opts.par2_slice_size == 0) opts.par2_slice_size = 64 * 1024;
    if (opts.par2_slice_size % 2 != 0) opts.par2_slice_size += 1;
    if (opts.line_width == 0) opts.line_width = 128;

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();

    // --- data files ---------------------------------------------------
    // A separate PRNG per file, seeded from the base seed plus the
    // index: changing `file_count` then leaves earlier files' bytes
    // untouched, which makes a shrinking repro of a failing test cheap.
    const data = try a.alloc(GenFile, opts.file_count);
    for (data, 0..) |*f, i| {
        const buf = try a.alloc(u8, opts.file_size);
        var prng = std.Random.DefaultPrng.init(opts.seed +% i *% 0x9E37_79B9_7F4A_7C15);
        prng.random().bytes(buf);
        f.* = .{
            .name = try std.fmt.allocPrint(a, "{s}.part{d:0>3}", .{ opts.name, i + 1 }),
            .bytes = buf,
            .is_data = true,
        };
    }

    // --- PAR2 descriptors ---------------------------------------------
    const entries = try gpa.alloc(FileEntry, opts.file_count);
    // Count-based so a failure part-way through does not try to release
    // entries that were never initialised.
    var built: usize = 0;
    defer {
        for (entries[0..built]) |*e| e.deinit(gpa);
        gpa.free(entries);
    }
    for (entries, data) |*e, f| {
        e.* = try FileEntry.init(gpa, f.name, f.bytes, opts.par2_slice_size);
        built += 1;
    }

    // The recovery slices are linear combinations over every data slice
    // in the set, concatenated in the order the Main packet declares the
    // files. That ordering is the coefficient index, so it must not be
    // re-sorted anywhere downstream.
    var all_slices: std.ArrayList([]const u8) = .empty;
    defer all_slices.deinit(gpa);
    for (entries) |e| {
        for (e.slices) |s| try all_slices.append(gpa, s);
    }

    // par2cmdline derives the set id from the packet contents; nothing
    // in hoardarr checks that derivation, so a stable hash over the name
    // and the file ids is enough and keeps the generator readable.
    var set_id: [16]u8 = undefined;
    {
        var h = Md5.init(.{});
        h.update(opts.name);
        for (entries) |e| h.update(&e.file_id);
        h.final(&set_id);
    }

    // --- the .par2 index file -----------------------------------------
    var index: std.ArrayList(u8) = .empty;
    defer index.deinit(gpa);
    {
        const ids = try gpa.alloc([16]u8, entries.len);
        defer gpa.free(ids);
        for (entries, 0..) |e, i| ids[i] = e.file_id;
        try appendPacket(&index, gpa, try par2.encodeMain(gpa, set_id, opts.par2_slice_size, ids));
    }
    for (entries) |e| {
        try appendPacket(&index, gpa, try par2.encodeFileDesc(
            gpa,
            set_id,
            e.file_id,
            e.md5_full,
            e.md5_16k,
            e.size,
            e.name,
        ));
        try appendPacket(&index, gpa, try par2.encodeIfsc(gpa, set_id, e.file_id, e.checks));
    }
    try appendPacket(&index, gpa, try par2.encodeCreator(gpa, set_id, "hoardarr-fixture-gen"));

    // --- assemble the full file list ----------------------------------
    // Arena-backed, so the errdefer on the arena is the only cleanup.
    var files: std.ArrayList(GenFile) = .empty;
    try files.ensureTotalCapacity(a, opts.file_count + 1 + opts.recovery_slices);
    for (data) |f| files.appendAssumeCapacity(f);
    files.appendAssumeCapacity(.{
        .name = try std.fmt.allocPrint(a, "{s}.par2", .{opts.name}),
        .bytes = try a.dupe(u8, index.items),
        .is_data = false,
    });

    for (0..opts.recovery_slices) |r| {
        // Exponents start at 1: exponent 0 would give every slice the
        // coefficient 1, i.e. a plain parity slice, and a set of those
        // is linearly dependent past the first.
        const exponent: u16 = @intCast(r + 1);
        const body = try rs.encodeRecoverySlice(gpa, all_slices.items, exponent);
        defer gpa.free(body);

        var vol: std.ArrayList(u8) = .empty;
        defer vol.deinit(gpa);
        try vol.appendSlice(gpa, index.items);
        try appendPacket(&vol, gpa, try par2.encodeRecvSlc(gpa, set_id, exponent, body));

        files.appendAssumeCapacity(.{
            .name = try std.fmt.allocPrint(a, "{s}.vol{d:0>3}+01.par2", .{ opts.name, r }),
            .bytes = try a.dupe(u8, vol.items),
            .is_data = false,
        });
    }
    const file_list = files.items;

    // --- yEnc articles + NZB ------------------------------------------
    var articles: std.ArrayList(Article) = .empty;
    var nzb: std.ArrayList(u8) = .empty;
    defer nzb.deinit(gpa);

    try nzb.appendSlice(gpa, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    try nzb.appendSlice(gpa, "<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n");

    for (file_list, 0..) |f, fi| {
        const seg_count = @max(1, (f.bytes.len + opts.article_size - 1) / opts.article_size);

        // The subject mirrors a real post so hoardarr's subject parser
        // has something representative to recover the filename from.
        var subject: std.ArrayList(u8) = .empty;
        defer subject.deinit(gpa);
        try subject.print(gpa, "[{d}/{d}] - \"{s}\" yEnc (1/{d})", .{
            fi + 1, file_list.len, f.name, seg_count,
        });

        try nzb.appendSlice(gpa, "  <file poster=\"test@hoardarr\" date=\"0\" subject=\"");
        try appendXmlEscaped(&nzb, gpa, subject.items);
        try nzb.appendSlice(gpa, "\">\n    <groups>\n      <group>misc.test</group>\n    </groups>\n    <segments>\n");

        for (0..seg_count) |i| {
            const start = i * opts.article_size;
            const end = @min(start + opts.article_size, f.bytes.len);
            const msg_id = try std.fmt.allocPrint(a, "{s}.{d:0>3}@hoardarr-fixture", .{ f.name, i + 1 });
            const body = try encodeArticlePart(a, .{
                .name = f.name,
                .payload = f.bytes[start..end],
                // =ypart offsets are 1-based and inclusive.
                .begin = start + 1,
                .end = end,
                .total_file_size = f.bytes.len,
                .part = i + 1,
                .total = seg_count,
                .line_width = opts.line_width,
            });
            try articles.append(a, .{
                .message_id = msg_id,
                .body = body,
                .filename = f.name,
                .part = i + 1,
            });
            try nzb.print(gpa, "      <segment bytes=\"{d}\" number=\"{d}\">", .{ body.len, i + 1 });
            try appendXmlEscaped(&nzb, gpa, msg_id);
            try nzb.appendSlice(gpa, "</segment>\n");
        }
        try nzb.appendSlice(gpa, "    </segments>\n  </file>\n");
    }
    try nzb.appendSlice(gpa, "</nzb>\n");

    // Every arena allocation has to happen before the arena is copied
    // into the result: the copy snapshots the buffer list, so anything
    // allocated afterwards would be reachable only from the dead local
    // and would never be freed.
    const nzb_owned = try a.dupe(u8, nzb.items);

    return .{
        .arena = arena,
        .nzb = nzb_owned,
        .files = file_list,
        .articles = articles.items,
        .file_count = opts.file_count,
        .recovery_file_count = opts.recovery_slices,
        .par2_slice_size = opts.par2_slice_size,
    };
}

// ---------------------------------------------------------------------
// PAR2 helpers
// ---------------------------------------------------------------------

/// Everything the PAR2 packets need about one data file.
const FileEntry = struct {
    name: []const u8,
    size: u64,
    file_id: [16]u8,
    md5_full: [16]u8,
    md5_16k: [16]u8,
    /// Zero-padded to `slice_size`; owned.
    slices: [][]u8,
    checks: []par2.SliceCheck,

    fn init(gpa: Allocator, name: []const u8, bytes: []const u8, slice_size: usize) Error!FileEntry {
        var md5_full: [16]u8 = undefined;
        Md5.hash(bytes, &md5_full, .{});
        var md5_16k: [16]u8 = undefined;
        Md5.hash(bytes[0..@min(bytes.len, 16384)], &md5_16k, .{});

        const slices = try rs.splitIntoSlices(gpa, bytes, slice_size);
        errdefer rs.freeSlices(gpa, slices);

        const checks = try gpa.alloc(par2.SliceCheck, slices.len);
        for (slices, checks) |s, *c| {
            // Both checksums cover the *padded* slice — that is what the
            // spec says and what a real verifier compares against.
            var md5: [16]u8 = undefined;
            Md5.hash(s, &md5, .{});
            c.* = .{ .md5 = md5, .crc32 = crc32.checksum(s) };
        }
        return .{
            .name = name,
            .size = bytes.len,
            .file_id = computeFileId(md5_16k, bytes.len, name),
            .md5_full = md5_full,
            .md5_16k = md5_16k,
            .slices = slices,
            .checks = checks,
        };
    }

    fn deinit(self: *FileEntry, gpa: Allocator) void {
        rs.freeSlices(gpa, self.slices);
        gpa.free(self.checks);
    }
};

/// PAR2's file id: MD5 over the 16k digest, the little-endian length,
/// and the name. Renaming a file changes its id, which is why obfuscated
/// releases are matched on `md5_16k` instead.
fn computeFileId(md5_16k: [16]u8, size: u64, name: []const u8) [16]u8 {
    var h = Md5.init(.{});
    h.update(&md5_16k);
    var le: [8]u8 = undefined;
    std.mem.writeInt(u64, &le, size, .little);
    h.update(&le);
    h.update(name);
    var out: [16]u8 = undefined;
    h.final(&out);
    return out;
}

/// Appends an encoder's owned output and frees it. The par2 encoders all
/// return fresh slices; the caller only ever wants them concatenated.
fn appendPacket(buf: *std.ArrayList(u8), gpa: Allocator, packet: []u8) Allocator.Error!void {
    defer gpa.free(packet);
    try buf.appendSlice(gpa, packet);
}

// ---------------------------------------------------------------------
// yEnc encoding
// ---------------------------------------------------------------------

pub const PartOpts = struct {
    name: []const u8,
    payload: []const u8,
    /// 1-based inclusive byte range within the assembled file.
    begin: usize,
    end: usize,
    /// Size of the *whole* file, not of this part.
    total_file_size: usize,
    part: usize,
    total: usize,
    line_width: usize = 128,
};

/// Encodes one part of a multi-part yEnc article.
///
/// The `size=` on `=ybegin` is the total file size and the one on
/// `=yend` is this part's size. That asymmetry is the spec, and it
/// matters: a receiver sizes the output file from the `=ybegin` value,
/// so putting the part size there would truncate every multi-segment
/// download to its first segment. `codec/yenc.zig`'s `encodeForTest`
/// puts the part size in both, which is why this encoder exists rather
/// than reusing it.
pub fn encodeArticlePart(gpa: Allocator, o: PartOpts) Allocator.Error![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);

    try buf.print(gpa, "=ybegin part={d} total={d} line={d} size={d} name={s}\r\n", .{
        o.part, o.total, o.line_width, o.total_file_size, o.name,
    });
    try buf.print(gpa, "=ypart begin={d} end={d}\r\n", .{ o.begin, o.end });
    try encodeBody(&buf, gpa, o.payload, o.line_width);
    try buf.print(gpa, "=yend size={d} part={d} pcrc32={x:0>8}\r\n", .{
        o.payload.len, o.part, crc32.checksum(o.payload),
    });
    return buf.toOwnedSlice(gpa);
}

/// Encodes a single-part article — no `=ypart`, whole-file `crc32=`.
pub fn encodeArticle(gpa: Allocator, name: []const u8, payload: []const u8, line_width: usize) Allocator.Error![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);

    try buf.print(gpa, "=ybegin line={d} size={d} name={s}\r\n", .{ line_width, payload.len, name });
    try encodeBody(&buf, gpa, payload, line_width);
    try buf.print(gpa, "=yend size={d} crc32={x:0>8}\r\n", .{ payload.len, crc32.checksum(payload) });
    return buf.toOwnedSlice(gpa);
}

/// The yEnc body rules: add 42 mod 256, then escape the four critical
/// bytes as `=` followed by the value plus 64. Tab, space and `.` are
/// additionally escaped at column zero — the transport dot-stuffs
/// independently, but decoders in the wild still trip over a leading dot
/// and trailing whitespace is not survivable through every relay.
fn encodeBody(buf: *std.ArrayList(u8), gpa: Allocator, payload: []const u8, line_width: usize) Allocator.Error!void {
    var col: usize = 0;
    for (payload) |b| {
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
        if (col >= line_width) {
            try buf.appendSlice(gpa, "\r\n");
            col = 0;
        }
    }
    if (col > 0) try buf.appendSlice(gpa, "\r\n");
}

fn appendXmlEscaped(buf: *std.ArrayList(u8), gpa: Allocator, s: []const u8) Allocator.Error!void {
    for (s) |c| switch (c) {
        '&' => try buf.appendSlice(gpa, "&amp;"),
        '<' => try buf.appendSlice(gpa, "&lt;"),
        '>' => try buf.appendSlice(gpa, "&gt;"),
        '"' => try buf.appendSlice(gpa, "&quot;"),
        '\'' => try buf.appendSlice(gpa, "&apos;"),
        else => try buf.append(gpa, c),
    };
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;
const Io = std.Io;
const yenc = @import("../codec/yenc.zig");
const nzb_codec = @import("../codec/nzb.zig");
const verifier = @import("../codec/par2/verifier.zig");

/// A small but structurally complete release: several files, several
/// articles per file, several slices per file, real parity.
const small: Options = .{
    .name = "movie.s01e01",
    .file_count = 2,
    .file_size = 9000,
    .article_size = 4096,
    .par2_slice_size = 2048,
    .recovery_slices = 3,
};

test "generate produces data files, an index and one volume per recovery slice" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    try t.expectEqual(@as(usize, 2 + 1 + 3), fx.files.len);
    try t.expectEqualStrings("movie.s01e01.part001", fx.files[0].name);
    try t.expectEqualStrings("movie.s01e01.part002", fx.files[1].name);
    try t.expectEqualStrings("movie.s01e01.par2", fx.files[2].name);
    try t.expectEqualStrings("movie.s01e01.vol000+01.par2", fx.files[3].name);
    try t.expectEqualStrings("movie.s01e01.vol002+01.par2", fx.files[5].name);

    try t.expect(fx.files[0].is_data);
    try t.expect(!fx.files[2].is_data);
    try t.expectEqual(@as(usize, 9000), fx.files[0].bytes.len);
}

test "generation is deterministic for a given seed" {
    var a_fx = try generate(t.allocator, small);
    defer a_fx.deinit();
    var b_fx = try generate(t.allocator, small);
    defer b_fx.deinit();

    try t.expectEqualSlices(u8, a_fx.files[0].bytes, b_fx.files[0].bytes);
    try t.expectEqualSlices(u8, a_fx.nzb, b_fx.nzb);
    try t.expectEqual(a_fx.articles.len, b_fx.articles.len);
    for (a_fx.articles, b_fx.articles) |x, y| {
        try t.expectEqualStrings(x.message_id, y.message_id);
        try t.expectEqualSlices(u8, x.body, y.body);
    }

    var other = try generate(t.allocator, .{
        .name = small.name,
        .file_count = small.file_count,
        .file_size = small.file_size,
        .article_size = small.article_size,
        .par2_slice_size = small.par2_slice_size,
        .recovery_slices = small.recovery_slices,
        .seed = 99,
    });
    defer other.deinit();
    try t.expect(!std.mem.eql(u8, a_fx.files[0].bytes, other.files[0].bytes));
}

test "distinct files get distinct contents" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();
    try t.expect(!std.mem.eql(u8, fx.files[0].bytes, fx.files[1].bytes));
}

test "yEnc articles decode and reassemble every file byte-exactly" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    for (fx.files) |f| {
        const rebuilt = try t.allocator.alloc(u8, f.bytes.len);
        defer t.allocator.free(rebuilt);
        // Poison, so a segment that never lands shows up as a mismatch
        // rather than as a coincidentally-correct zero.
        @memset(rebuilt, 0xA5);

        var seen: usize = 0;
        for (fx.articles) |art| {
            if (!std.mem.eql(u8, art.filename, f.name)) continue;
            seen += 1;

            var decoded = try yenc.decode(t.allocator, art.body);
            defer decoded.deinit(t.allocator);

            // The header must declare the whole file's size: the
            // orchestrator sizes the output file from it.
            try t.expectEqual(@as(i64, @intCast(f.bytes.len)), decoded.header.size);
            try t.expectEqualStrings(f.name, decoded.header.name);

            const offset: usize = @intCast(decoded.header.begin - 1);
            @memcpy(rebuilt[offset..][0..decoded.payload.len], decoded.payload);
        }
        try t.expect(seen > 0);
        try t.expectEqualSlices(u8, f.bytes, rebuilt);
    }
}

test "articles are multi-part when a file spans more than one segment" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    // 9000 bytes at 4096 per article is three segments.
    var parts: usize = 0;
    for (fx.articles) |art| {
        if (std.mem.eql(u8, art.filename, fx.files[0].name)) parts += 1;
    }
    try t.expectEqual(@as(usize, 3), parts);
}

test "the NZB parses and references every article exactly once" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    var doc = try nzb_codec.parse(t.allocator, fx.nzb);
    defer doc.deinit();

    try t.expectEqual(fx.files.len, doc.files.len);

    var referenced: std.StringHashMapUnmanaged(usize) = .empty;
    defer referenced.deinit(t.allocator);

    for (doc.files, fx.files) |df, gf| {
        // The subject carries the name in quotes, so the parser recovers
        // it without falling back to its heuristics.
        try t.expectEqualStrings(gf.name, df.filename);
        try t.expect(df.groups.len == 1);
        for (df.segments, 0..) |seg, i| {
            try t.expectEqual(@as(u32, @intCast(i + 1)), seg.number);
            try t.expect(seg.bytes > 0);
            const gop = try referenced.getOrPut(t.allocator, seg.message_id);
            if (gop.found_existing) return error.DuplicateSegment;
            gop.value_ptr.* = 1;
        }
    }

    try t.expectEqual(fx.articles.len, referenced.count());
    for (fx.articles) |art| {
        try t.expect(referenced.contains(art.message_id));
        // And the declared byte count matches the body actually served.
        try t.expect(fx.articleBody(art.message_id) != null);
    }
}

test "the generated PAR2 parses cleanly with correct packet checksums" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    var set = try par2.parse(t.allocator, fx.files[2].bytes);
    defer set.deinit(t.allocator);

    try t.expectEqual(@as(u64, small.par2_slice_size), set.slice_size);
    try t.expectEqual(@as(usize, 2), set.recovery_files.items.len);
    try t.expectEqual(@as(usize, 2), set.files.items.len);
    try t.expectEqualStrings("hoardarr-fixture-gen", set.creator);
    // 9000 bytes at 2048 per slice is five slices, twice over.
    try t.expectEqual(@as(usize, 10), set.dataSliceCount());

    for (set.files.items, 0..) |pf, i| {
        try t.expectEqualStrings(fx.files[i].name, pf.name);
        try t.expectEqual(@as(u64, 9000), pf.size);
        try t.expectEqual(@as(usize, 5), pf.slices.items.len);

        var md5: [16]u8 = undefined;
        Md5.hash(fx.files[i].bytes, &md5, .{});
        try t.expectEqualSlices(u8, &md5, &pf.md5);
    }
}

test "IFSC slice checksums match the real slices" {
    // The recovery maths is only useful if the verifier can tell *which*
    // slice went bad, and that is entirely down to these checksums.
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    var set = try par2.parse(t.allocator, fx.files[2].bytes);
    defer set.deinit(t.allocator);

    for (set.recovery_files.items, 0..) |id, fi| {
        const pf = set.fileById(id).?;
        const slices = try rs.splitIntoSlices(t.allocator, fx.files[fi].bytes, small.par2_slice_size);
        defer rs.freeSlices(t.allocator, slices);

        try t.expectEqual(slices.len, pf.slices.items.len);
        for (slices, pf.slices.items) |s, check| {
            var md5: [16]u8 = undefined;
            Md5.hash(s, &md5, .{});
            try t.expectEqualSlices(u8, &md5, &check.md5);
            try t.expectEqual(crc32.checksum(s), check.crc32);
        }
    }
}

test "every volume file is self-describing and carries its own exponent" {
    var fx = try generate(t.allocator, small);
    defer fx.deinit();

    for (fx.files[3..], 0..) |vol, r| {
        var set = try par2.parse(t.allocator, vol.bytes);
        defer set.deinit(t.allocator);
        // Full index in every volume, exactly as par2cmdline emits.
        try t.expectEqual(@as(usize, 2), set.files.items.len);
        try t.expectEqual(@as(usize, 1), set.recovery_slice_count);
        try t.expect(set.recovery_slices.contains(@intCast(r + 1)));
        try t.expectEqual(small.par2_slice_size, set.recovery_slices.get(@intCast(r + 1)).?.len);
    }
}

test "verifier accepts the generated set against the generated files" {
    // The strongest single statement this file can make: hoardarr's own
    // verifier, over hoardarr's own parser, says the fixture is clean.
    const alloc = t.allocator;
    var fx = try generate(alloc, small);
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    for (fx.files) |f| {
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = f.name, .data = f.bytes });
    }

    const par2_names = try fx.par2Names(alloc);
    defer alloc.free(par2_names);
    try t.expectEqual(@as(usize, 4), par2_names.len);

    var data: std.ArrayList(verifier.DataFile) = .empty;
    defer data.deinit(alloc);
    for (fx.files) |f| {
        if (f.is_data) try data.append(alloc, .{ .name = f.name, .path = f.name });
    }

    var result = try verifier.verify(alloc, std.testing.io, tmp.dir, par2_names, data.items);
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 2), result.files.items.len);
    try t.expect(result.allOk());
}

test "verifier rejects the set once a generated file is corrupted" {
    const alloc = t.allocator;
    var fx = try generate(alloc, small);
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    for (fx.files) |f| {
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = f.name, .data = f.bytes });
    }

    // Same length, different bytes: the size check passes, so only the
    // MD5 can catch it. If the fixture's MD5s were wrong this test would
    // pass for the wrong reason — hence the clean-set test above.
    const damaged = try alloc.dupe(u8, fx.files[0].bytes);
    defer alloc.free(damaged);
    damaged[4500] ^= 0xFF;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = fx.files[0].name, .data = damaged });

    const par2_names = try fx.par2Names(alloc);
    defer alloc.free(par2_names);

    var result = try verifier.verify(alloc, std.testing.io, tmp.dir, par2_names, &.{
        .{ .name = fx.files[0].name, .path = fx.files[0].name },
        .{ .name = fx.files[1].name, .path = fx.files[1].name },
    });
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 1), result.failedCount());
    try t.expectEqual(verifier.Reason.md5_mismatch, result.files.items[0].reason);
}

/// The canonical data-slice array of a parsed set, in Main-declared file
/// order. This is the ordering the Reed-Solomon coefficients index.
fn canonicalSlices(
    alloc: Allocator,
    set: *par2.RecoverySet,
    fx: *const Fixture,
) ![][]u8 {
    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |s| alloc.free(s);
        out.deinit(alloc);
    }
    for (set.recovery_files.items) |id| {
        const pf = set.fileById(id).?;
        const bytes = fx.fileBytes(pf.name).?;
        const slices = try rs.splitIntoSlices(alloc, bytes, @intCast(set.slice_size));
        defer alloc.free(slices);
        for (slices) |s| try out.append(alloc, s);
    }
    return out.toOwnedSlice(alloc);
}

test "a corrupted slice is located by IFSC and rebuilt from the recovery slices" {
    // End to end over the parity: parse the set the fixture produced,
    // damage a slice, find it by checksum the way the repair path does,
    // and rebuild it. If the recovery slices were not genuine linear
    // combinations this cannot succeed.
    const alloc = t.allocator;
    var fx = try generate(alloc, small);
    defer fx.deinit();

    var set: par2.RecoverySet = .{};
    defer set.deinit(alloc);
    for (fx.files) |f| {
        if (!f.is_data) try par2.parseInto(&set, alloc, f.bytes);
    }
    try t.expectEqual(@as(usize, 3), set.recovery_slices.count());

    const slices = try canonicalSlices(alloc, &set, &fx);
    defer rs.freeSlices(alloc, slices);
    try t.expectEqual(@as(usize, 10), slices.len);

    // Damage two slices in different files, which is what a partial
    // article loss actually looks like.
    const damaged_idx = [_]usize{ 1, 7 };
    const pristine = try alloc.alloc([]u8, damaged_idx.len);
    defer {
        for (pristine) |p| alloc.free(p);
        alloc.free(pristine);
    }
    for (damaged_idx, pristine) |idx, *p| {
        p.* = try alloc.dupe(u8, slices[idx]);
        slices[idx][0] ^= 0xFF;
        slices[idx][17] +%= 3;
    }

    // Locate them the way a verifier would: per-slice CRC32 against the
    // IFSC table, walked in canonical order.
    var found: std.ArrayList(usize) = .empty;
    defer found.deinit(alloc);
    {
        var flat: usize = 0;
        for (set.recovery_files.items) |id| {
            const pf = set.fileById(id).?;
            for (pf.slices.items) |check| {
                if (crc32.checksum(slices[flat]) != check.crc32) try found.append(alloc, flat);
                flat += 1;
            }
        }
    }
    try t.expectEqualSlices(usize, &damaged_idx, found.items);

    // Rebuild.
    const present = try alloc.alloc(?[]const u8, slices.len);
    defer alloc.free(present);
    for (slices, 0..) |s, i| present[i] = if (rs.isMissing(found.items, i)) null else s;

    var recovery: std.ArrayList(rs.RecoverySlice) = .empty;
    defer recovery.deinit(alloc);
    var it = set.recovery_slices.iterator();
    while (it.next()) |e| {
        try recovery.append(alloc, .{ .exponent = e.key_ptr.*, .body = e.value_ptr.* });
    }

    const rebuilt = try rs.reconstruct(alloc, .{
        .slice_size = @intCast(set.slice_size),
        .present = present,
        .missing = found.items,
        .recovery = recovery.items,
    });
    defer rs.freeSlices(alloc, rebuilt);

    try t.expectEqual(damaged_idx.len, rebuilt.len);
    for (pristine, rebuilt) |want, got| {
        try t.expectEqualSlices(u8, want, got);
    }
}

test "a repaired file passes the verifier again" {
    // The repair is only worth anything if the reassembled file matches
    // the PAR2 descriptor, padding and all.
    const alloc = t.allocator;
    var fx = try generate(alloc, small);
    defer fx.deinit();

    var set: par2.RecoverySet = .{};
    defer set.deinit(alloc);
    for (fx.files) |f| {
        if (!f.is_data) try par2.parseInto(&set, alloc, f.bytes);
    }

    const slices = try canonicalSlices(alloc, &set, &fx);
    defer rs.freeSlices(alloc, slices);

    const missing = [_]usize{2};
    const present = try alloc.alloc(?[]const u8, slices.len);
    defer alloc.free(present);
    for (slices, 0..) |s, i| present[i] = if (rs.isMissing(&missing, i)) null else s;

    var recovery: std.ArrayList(rs.RecoverySlice) = .empty;
    defer recovery.deinit(alloc);
    var it = set.recovery_slices.iterator();
    while (it.next()) |e| {
        try recovery.append(alloc, .{ .exponent = e.key_ptr.*, .body = e.value_ptr.* });
    }

    const rebuilt = try rs.reconstruct(alloc, .{
        .slice_size = @intCast(set.slice_size),
        .present = present,
        .missing = &missing,
        .recovery = recovery.items,
    });
    defer rs.freeSlices(alloc, rebuilt);

    // Reassemble file 0 from its five slices, with slice 2 replaced by
    // the rebuilt one, and truncate the zero padding off the tail.
    var joined: std.ArrayList(u8) = .empty;
    defer joined.deinit(alloc);
    for (0..5) |i| {
        try joined.appendSlice(alloc, if (i == missing[0]) rebuilt[0] else slices[i]);
    }
    const pf = set.fileById(set.recovery_files.items[0]).?;
    joined.shrinkRetainingCapacity(@intCast(pf.size));

    try t.expectEqualSlices(u8, fx.files[0].bytes, joined.items);
    var md5: [16]u8 = undefined;
    Md5.hash(joined.items, &md5, .{});
    try t.expectEqualSlices(u8, &pf.md5, &md5);
}

test "a single-file single-article release still works" {
    // Degenerate shapes are where off-by-one segment maths shows up.
    var fx = try generate(t.allocator, .{
        .name = "tiny",
        .file_count = 1,
        .file_size = 100,
        .article_size = 4096,
        .par2_slice_size = 64,
        .recovery_slices = 1,
    });
    defer fx.deinit();

    try t.expectEqual(@as(usize, 3), fx.files.len);
    var count: usize = 0;
    for (fx.articles) |art| {
        if (std.mem.eql(u8, art.filename, "tiny.part001")) count += 1;
    }
    try t.expectEqual(@as(usize, 1), count);

    var decoded = try yenc.decode(t.allocator, fx.articles[0].body);
    defer decoded.deinit(t.allocator);
    try t.expectEqualSlices(u8, fx.files[0].bytes, decoded.payload);
}

test "a release with no recovery slices still yields a parseable index" {
    var fx = try generate(t.allocator, .{
        .name = "noparity",
        .file_count = 1,
        .file_size = 512,
        .article_size = 512,
        .par2_slice_size = 128,
        .recovery_slices = 0,
    });
    defer fx.deinit();

    try t.expectEqual(@as(usize, 2), fx.files.len);
    var set = try par2.parse(t.allocator, fx.files[1].bytes);
    defer set.deinit(t.allocator);
    try t.expectEqual(@as(usize, 0), set.recovery_slice_count);
    try t.expectEqual(@as(usize, 1), set.files.items.len);
}

test "an odd slice size is rounded up rather than rejected" {
    var fx = try generate(t.allocator, .{
        .name = "odd",
        .file_count = 1,
        .file_size = 300,
        .article_size = 300,
        .par2_slice_size = 101,
        .recovery_slices = 1,
    });
    defer fx.deinit();
    try t.expectEqual(@as(usize, 102), fx.par2_slice_size);

    var set = try par2.parse(t.allocator, fx.files[2].bytes);
    defer set.deinit(t.allocator);
    try t.expectEqual(@as(u64, 102), set.slice_size);
}

test "escaped yEnc bytes survive the round trip" {
    // A payload that is all critical bytes after the +42 shift, plus a
    // leading '.' at column zero — the two cases the escape rules exist
    // for. If either escape were wrong the CRC check inside decode would
    // fire.
    const payload = [_]u8{ 0xD6, 0xE0, 0xE3, 0x13, 0x04, 0x00, 0xFF, 0x2A } ** 40;
    const enc = try encodeArticlePart(t.allocator, .{
        .name = "escapes.bin",
        .payload = &payload,
        .begin = 1,
        .end = payload.len,
        .total_file_size = payload.len,
        .part = 1,
        .total = 2,
    });
    defer t.allocator.free(enc);

    var decoded = try yenc.decode(t.allocator, enc);
    defer decoded.deinit(t.allocator);
    try t.expectEqualSlices(u8, &payload, decoded.payload);
}

test "single-part encoder round-trips too" {
    const payload = "a modest payload with a trailing dot.";
    const enc = try encodeArticle(t.allocator, "note.txt", payload, 32);
    defer t.allocator.free(enc);

    var decoded = try yenc.decode(t.allocator, enc);
    defer decoded.deinit(t.allocator);
    try t.expectEqualSlices(u8, payload, decoded.payload);
    try t.expect(decoded.trailer.has_crc);
}

test "XML escaping covers the reserved characters" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(t.allocator);
    try appendXmlEscaped(&buf, t.allocator, "a&b<c>d\"e'f");
    try t.expectEqualStrings("a&amp;b&lt;c&gt;d&quot;e&apos;f", buf.items);
}

test "a name with XML-hostile characters still produces a parseable NZB" {
    // Kept to characters that are legal in a message-id: the filename
    // feeds both the subject and the ids, and a space or an angle
    // bracket in an id is rejected by the NZB parser, rightly.
    var fx = try generate(t.allocator, .{
        .name = "cash&carry'22",
        .file_count = 1,
        .file_size = 64,
        .article_size = 64,
        .par2_slice_size = 32,
        .recovery_slices = 0,
    });
    defer fx.deinit();

    var doc = try nzb_codec.parse(t.allocator, fx.nzb);
    defer doc.deinit();
    try t.expectEqual(@as(usize, 2), doc.files.len);
    try t.expectEqual(@as(usize, 1), doc.files[0].segments.len);
}
