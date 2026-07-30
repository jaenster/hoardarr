//! PAR2 (Parity Archive Volume 2) packet parser and encoder, per the
//! reference specification at parchive.sourceforge.net.
//!
//! Wire format, briefly. A PAR2 file is a bare concatenation of
//! self-describing packets, each one:
//!
//! | offset | size | field |
//! |-|-|-|
//! | 0 | 8 | magic `PAR2\0PKT` |
//! | 8 | 8 | total packet length, LE, includes this 64-byte header |
//! | 16 | 16 | MD5 of everything from offset 32 to the end of the packet |
//! | 32 | 16 | recovery set id |
//! | 48 | 16 | packet type, ASCII, NUL-right-padded |
//! | 64 | … | body |
//!
//! Two consequences drive the design here. First, packets carry their
//! own length and checksum, so a stream may contain arbitrary non-PAR2
//! bytes between packets and a damaged packet can be skipped rather than
//! aborting the parse — that is what the spec asks for, and what real
//! Usenet posts require. Second, the descriptive packets (Main,
//! FileDesc, IFSC, Creator) are *repeated* in every `.par2` and
//! `.volNN+MM.par2` file of a set, so assembling a set means deduping
//! them; first arrival wins.
//!
//! Parsing works over a byte slice rather than a stream. PAR2 index
//! files are kilobytes and volume files are at most a few tens of MiB,
//! the recovery-slice bodies have to be retained anyway, and a slice
//! turns the "resync past a corrupt packet" logic into arithmetic
//! instead of buffered-reader state.

const std = @import("std");
const Io = std.Io;
const Md5 = std.crypto.hash.Md5;

/// The 8-byte packet preamble.
pub const magic = [8]u8{ 'P', 'A', 'R', '2', 0, 'P', 'K', 'T' };

/// Every packet header is this long, magic included.
pub const header_len = 64;

/// Packet type identifiers: 16 bytes of ASCII, NUL-right-padded.
pub const packet_type = struct {
    pub const main = pad("PAR 2.0\x00Main");
    pub const file_desc = pad("PAR 2.0\x00FileDesc");
    pub const ifsc = pad("PAR 2.0\x00IFSC");
    pub const recv_slice = pad("PAR 2.0\x00RecvSlic");
    pub const creator = pad("PAR 2.0\x00Creator");

    fn pad(comptime s: []const u8) [16]u8 {
        var out: [16]u8 = @splat(0);
        @memcpy(out[0..s.len], s);
        return out;
    }
};

pub const Error = error{
    OutOfMemory,
    /// A Main packet with fewer than 12 body bytes, or one whose
    /// declared file count runs past the end of the body.
    BadMainPacket,
    /// A FileDesc body shorter than the fixed 56-byte prefix.
    BadFileDescPacket,
    /// An IFSC body shorter than the 16-byte file id, or whose
    /// remainder is not a whole number of 20-byte checksum entries.
    BadIfscPacket,
    /// A RecvSlic body shorter than its 4-byte exponent, or one whose
    /// exponent does not fit in the 16 bits PAR2 allows.
    BadRecoverySlicePacket,
    /// The stream mixes packets from two different recovery sets.
    /// Merging them would silently produce nonsense.
    MixedSetIds,
    /// A packet header declares a body that runs past the end of input.
    TruncatedPacket,
};

/// One slice's integrity bundle, from an IFSC packet.
pub const SliceCheck = struct {
    md5: [16]u8,
    crc32: u32,
};

/// One recoverable file: its FileDesc metadata plus, once the IFSC
/// packet has been seen, its per-slice checksums.
pub const ParFile = struct {
    id: [16]u8,
    /// Owned. Empty until a FileDesc for this id arrives — an IFSC can
    /// legally precede it.
    name: []u8 = &.{},
    size: u64 = 0,
    /// MD5 of the whole file.
    md5: [16]u8 = @splat(0),
    /// MD5 of the first 16384 bytes, or of the whole file if shorter.
    /// Obfuscated releases rename files freely, so this is the only
    /// reliable way to match a file on disk to its PAR2 descriptor.
    md5_16k: [16]u8 = @splat(0),
    slices: std.ArrayList(SliceCheck) = .empty,

    fn deinit(f: *ParFile, alloc: std.mem.Allocator) void {
        alloc.free(f.name);
        f.slices.deinit(alloc);
    }
};

/// A verified packet handed to the set assembler.
pub const Packet = struct {
    set_id: [16]u8,
    type: [16]u8,
    body: []const u8,
};

/// The consolidated view of one recovery set, across however many
/// `.par2` files contributed to it.
pub const RecoverySet = struct {
    set_id: [16]u8 = @splat(0),
    /// Bytes per slice. Always even — PAR2 slices are arrays of
    /// GF(2^16) elements.
    slice_size: u64 = 0,
    /// File ids listed by the Main packet, in its order. This is the
    /// canonical data-slice ordering the Reed-Solomon coefficients are
    /// indexed by, so it must not be re-sorted.
    recovery_files: std.ArrayList([16]u8) = .empty,
    files: std.ArrayList(ParFile) = .empty,
    /// Informational: the producing client's name. Owned.
    creator: []u8 = &.{},
    /// Total RecvSlic packets seen, duplicates included. Tells the
    /// repair path whether enough volume files have been fetched.
    recovery_slice_count: usize = 0,
    /// Recovery-slice bodies keyed by exponent. First arrival wins;
    /// volume files overlap. Values are owned.
    recovery_slices: std.AutoHashMapUnmanaged(u16, []u8) = .empty,

    /// True once any packet has been consumed, which is when `set_id`
    /// became meaningful.
    have_set_id: bool = false,

    pub fn deinit(s: *RecoverySet, alloc: std.mem.Allocator) void {
        s.recovery_files.deinit(alloc);
        for (s.files.items) |*f| f.deinit(alloc);
        s.files.deinit(alloc);
        alloc.free(s.creator);
        var it = s.recovery_slices.valueIterator();
        while (it.next()) |v| alloc.free(v.*);
        s.recovery_slices.deinit(alloc);
        s.* = undefined;
    }

    /// The file descriptor for `id`, or null. The returned pointer is
    /// invalidated by any subsequent append to `files`.
    pub fn fileById(s: *RecoverySet, id: [16]u8) ?*ParFile {
        for (s.files.items) |*f| {
            if (std.mem.eql(u8, &f.id, &id)) return f;
        }
        return null;
    }

    /// Number of data slices in the set: the sum over files of
    /// ceil(size / slice_size). This is the N the Reed-Solomon
    /// coefficients are indexed against.
    pub fn dataSliceCount(s: *const RecoverySet) usize {
        if (s.slice_size == 0) return 0;
        var total: usize = 0;
        for (s.recovery_files.items) |id| {
            for (s.files.items) |f| {
                if (!std.mem.eql(u8, &f.id, &id)) continue;
                total += @intCast((f.size + s.slice_size - 1) / s.slice_size);
                break;
            }
        }
        return total;
    }

    fn consume(s: *RecoverySet, alloc: std.mem.Allocator, pkt: Packet) Error!void {
        if (!s.have_set_id) {
            s.set_id = pkt.set_id;
            s.have_set_id = true;
        } else if (!std.mem.eql(u8, &s.set_id, &pkt.set_id)) {
            return error.MixedSetIds;
        }

        if (std.mem.eql(u8, &pkt.type, &packet_type.main)) {
            return s.consumeMain(alloc, pkt.body);
        } else if (std.mem.eql(u8, &pkt.type, &packet_type.file_desc)) {
            return s.consumeFileDesc(alloc, pkt.body);
        } else if (std.mem.eql(u8, &pkt.type, &packet_type.ifsc)) {
            return s.consumeIfsc(alloc, pkt.body);
        } else if (std.mem.eql(u8, &pkt.type, &packet_type.recv_slice)) {
            s.recovery_slice_count += 1;
            return s.consumeRecvSlc(alloc, pkt.body);
        } else if (std.mem.eql(u8, &pkt.type, &packet_type.creator)) {
            const trimmed = std.mem.trimEnd(u8, pkt.body, "\x00 \r\n\t");
            const copy = try alloc.dupe(u8, trimmed);
            alloc.free(s.creator);
            s.creator = copy;
            return;
        }
        // Unknown packet type — the spec says ignore it.
    }

    fn consumeMain(s: *RecoverySet, alloc: std.mem.Allocator, body: []const u8) Error!void {
        if (body.len < 12) return error.BadMainPacket;
        s.slice_size = std.mem.readInt(u64, body[0..8], .little);
        const num_files = std.mem.readInt(u32, body[8..12], .little);
        const rest = body[12..];
        if (@as(usize, num_files) * 16 > rest.len) return error.BadMainPacket;
        s.recovery_files.clearRetainingCapacity();
        try s.recovery_files.ensureTotalCapacity(alloc, num_files);
        for (0..num_files) |i| {
            s.recovery_files.appendAssumeCapacity(rest[i * 16 ..][0..16].*);
        }
    }

    fn consumeFileDesc(s: *RecoverySet, alloc: std.mem.Allocator, body: []const u8) Error!void {
        if (body.len < 56) return error.BadFileDescPacket;
        const id: [16]u8 = body[0..16].*;

        // The filename runs from offset 56 to the end of the body,
        // NUL-padded to a 4-byte boundary.
        const name = std.mem.trimEnd(u8, body[56..], "\x00");

        if (s.fileById(id)) |existing| {
            // FileDesc repeats in every .par2 of the set. If an IFSC
            // created a bare stub first, fill in the descriptor now;
            // otherwise this is a duplicate and the first wins.
            if (existing.name.len != 0 or existing.size != 0) return;
            const copy = try alloc.dupe(u8, name);
            existing.md5 = body[16..32].*;
            existing.md5_16k = body[32..48].*;
            existing.size = std.mem.readInt(u64, body[48..56], .little);
            alloc.free(existing.name);
            existing.name = copy;
            return;
        }

        const copy = try alloc.dupe(u8, name);
        errdefer alloc.free(copy);
        try s.files.append(alloc, .{
            .id = id,
            .name = copy,
            .size = std.mem.readInt(u64, body[48..56], .little),
            .md5 = body[16..32].*,
            .md5_16k = body[32..48].*,
        });
    }

    fn consumeIfsc(s: *RecoverySet, alloc: std.mem.Allocator, body: []const u8) Error!void {
        if (body.len < 16) return error.BadIfscPacket;
        const id: [16]u8 = body[0..16].*;
        const rest = body[16..];
        if (rest.len % 20 != 0) return error.BadIfscPacket;

        if (s.fileById(id) == null) {
            // IFSC arrived before FileDesc — legal, order is not fixed.
            try s.files.append(alloc, .{ .id = id });
        }
        const f = s.fileById(id).?;
        // Already populated from another .par2; by spec they agree.
        if (f.slices.items.len > 0) return;

        const count = rest.len / 20;
        try f.slices.ensureTotalCapacity(alloc, count);
        for (0..count) |i| {
            const e = rest[i * 20 ..];
            f.slices.appendAssumeCapacity(.{
                .md5 = e[0..16].*,
                .crc32 = std.mem.readInt(u32, e[16..20], .little),
            });
        }
    }

    fn consumeRecvSlc(s: *RecoverySet, alloc: std.mem.Allocator, body: []const u8) Error!void {
        if (body.len < 4) return error.BadRecoverySlicePacket;
        const exp = std.mem.readInt(u32, body[0..4], .little);
        if (exp > 0xFFFF) return error.BadRecoverySlicePacket;
        const key: u16 = @intCast(exp);

        const gop = try s.recovery_slices.getOrPut(alloc, key);
        if (gop.found_existing) return;
        gop.value_ptr.* = alloc.dupe(u8, body[4..]) catch |err| {
            // Leave no half-inserted entry behind.
            _ = s.recovery_slices.remove(key);
            return err;
        };
    }
};

/// Parses every valid packet in `bytes` into a fresh `RecoverySet`.
pub fn parse(alloc: std.mem.Allocator, bytes: []const u8) Error!RecoverySet {
    var set: RecoverySet = .{};
    errdefer set.deinit(alloc);
    try parseInto(&set, alloc, bytes);
    return set;
}

/// Merges the packets in `bytes` into an existing set. Call once per
/// `.par2` file of a recovery set; duplicate descriptive packets are
/// deduped and a differing set id is rejected.
pub fn parseInto(set: *RecoverySet, alloc: std.mem.Allocator, bytes: []const u8) Error!void {
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, bytes, pos, &magic)) |start| {
        const after_header = start + header_len;
        // A header that runs off the end is just where the file stopped;
        // that is not corruption worth reporting.
        if (after_header > bytes.len) return;
        const hdr = bytes[start..after_header];

        const length = std.mem.readInt(u64, hdr[8..16], .little);
        if (length < header_len) {
            // Nonsense length. Resync from past the bad header rather
            // than trusting any of its fields.
            pos = after_header;
            continue;
        }
        const body_len = length - header_len;
        if (body_len > bytes.len - after_header) return error.TruncatedPacket;
        const body = bytes[after_header..][0..@intCast(body_len)];

        const declared_md5: [16]u8 = hdr[16..32].*;
        const set_id: [16]u8 = hdr[32..48].*;
        const pkt_type: [16]u8 = hdr[48..64].*;

        // The checksum covers set id + type + body, i.e. everything from
        // offset 32 onwards.
        var h = Md5.init(.{});
        h.update(&set_id);
        h.update(&pkt_type);
        h.update(body);
        var actual: [16]u8 = undefined;
        h.final(&actual);

        // Resync past the body either way: a packet that fails its own
        // checksum is skipped whole, which is what the spec asks for.
        pos = after_header + body.len;
        if (!std.mem.eql(u8, &actual, &declared_md5)) continue;

        try set.consume(alloc, .{ .set_id = set_id, .type = pkt_type, .body = body });
    }
}

pub const ReadError = Error || Io.Dir.ReadFileAllocError || error{NoPar2Files};

/// Largest single `.par2` file we will read into memory. Volume files
/// are recovery data, so this bounds how much parity one set can carry
/// per file, not the size of the release.
pub const max_par2_file_bytes = 1 << 31;

/// Parses the named files, in the order given, into one set.
pub fn parseFiles(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    paths: []const []const u8,
) ReadError!RecoverySet {
    if (paths.len == 0) return error.NoPar2Files;
    var set: RecoverySet = .{};
    errdefer set.deinit(alloc);
    for (paths) |p| {
        const bytes = try dir.readFileAlloc(io, p, alloc, .limited(max_par2_file_bytes));
        defer alloc.free(bytes);
        try parseInto(&set, alloc, bytes);
    }
    return set;
}

pub const ParseDirError = ReadError || Io.Dir.Iterator.Error;

/// Finds every `*.par2` in `dir` and parses them into one set. `dir`
/// must have been opened with `.iterate = true`.
///
/// Names are sorted before parsing so that "first arrival wins" is
/// deterministic: the index file (`name.par2`) sorts before its volume
/// files (`name.volNN+MM.par2`), which is also the order that puts the
/// descriptive packets in front of the recovery data.
pub fn parseDir(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
) ParseDirError!RecoverySet {
    var names: std.ArrayList([]u8) = .empty;
    defer {
        for (names.items) |n| alloc.free(n);
        names.deinit(alloc);
    }

    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind == .directory) continue;
        if (!hasPar2Suffix(entry.name)) continue;
        try names.append(alloc, try alloc.dupe(u8, entry.name));
    }
    if (names.items.len == 0) return error.NoPar2Files;

    std.mem.sort([]u8, names.items, {}, struct {
        fn lessThan(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    var set: RecoverySet = .{};
    errdefer set.deinit(alloc);
    for (names.items) |n| {
        const bytes = try dir.readFileAlloc(io, n, alloc, .limited(max_par2_file_bytes));
        defer alloc.free(bytes);
        try parseInto(&set, alloc, bytes);
    }
    return set;
}

/// `parseDir` for a directory named relative to `parent`.
pub fn parseDirPath(
    alloc: std.mem.Allocator,
    io: Io,
    parent: Io.Dir,
    sub_path: []const u8,
) (ParseDirError || Io.Dir.OpenError)!RecoverySet {
    var dir = try parent.openDir(io, sub_path, .{ .iterate = true });
    defer dir.close(io);
    return parseDir(alloc, io, dir);
}

/// Case-insensitive `.par2` extension test.
pub fn hasPar2Suffix(name: []const u8) bool {
    if (name.len < 5) return false;
    return std.ascii.eqlIgnoreCase(name[name.len - 5 ..], ".par2");
}

// ---------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------

/// Builds one complete packet — header plus body — with a valid MD5, so
/// the output is indistinguishable from a real producer's. Used by the
/// tests here, by the end-to-end suite for synthesising fixtures, and
/// by the repair path when it rewrites a set.
pub fn encodePacket(
    alloc: std.mem.Allocator,
    set_id: [16]u8,
    pkt_type: [16]u8,
    body: []const u8,
) error{OutOfMemory}![]u8 {
    const total = header_len + body.len;
    const out = try alloc.alloc(u8, total);
    errdefer alloc.free(out);

    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u64, out[8..16], total, .little);
    @memcpy(out[32..48], &set_id);
    @memcpy(out[48..64], &pkt_type);
    @memcpy(out[64..], body);

    var h = Md5.init(.{});
    h.update(out[32..]);
    h.final(out[16..32]);
    return out;
}

pub fn encodeMain(
    alloc: std.mem.Allocator,
    set_id: [16]u8,
    slice_size: u64,
    file_ids: []const [16]u8,
) error{OutOfMemory}![]u8 {
    const body = try alloc.alloc(u8, 12 + file_ids.len * 16);
    defer alloc.free(body);
    std.mem.writeInt(u64, body[0..8], slice_size, .little);
    std.mem.writeInt(u32, body[8..12], @intCast(file_ids.len), .little);
    for (file_ids, 0..) |id, i| @memcpy(body[12 + i * 16 ..][0..16], &id);
    return encodePacket(alloc, set_id, packet_type.main, body);
}

pub fn encodeFileDesc(
    alloc: std.mem.Allocator,
    set_id: [16]u8,
    file_id: [16]u8,
    md5_full: [16]u8,
    md5_16k: [16]u8,
    size: u64,
    name: []const u8,
) error{OutOfMemory}![]u8 {
    // Bodies are padded to a 4-byte boundary.
    const padded = std.mem.alignForward(usize, 56 + name.len, 4);
    const body = try alloc.alloc(u8, padded);
    defer alloc.free(body);
    @memset(body, 0);
    @memcpy(body[0..16], &file_id);
    @memcpy(body[16..32], &md5_full);
    @memcpy(body[32..48], &md5_16k);
    std.mem.writeInt(u64, body[48..56], size, .little);
    @memcpy(body[56..][0..name.len], name);
    return encodePacket(alloc, set_id, packet_type.file_desc, body);
}

pub fn encodeIfsc(
    alloc: std.mem.Allocator,
    set_id: [16]u8,
    file_id: [16]u8,
    slices: []const SliceCheck,
) error{OutOfMemory}![]u8 {
    const body = try alloc.alloc(u8, 16 + slices.len * 20);
    defer alloc.free(body);
    @memcpy(body[0..16], &file_id);
    for (slices, 0..) |s, i| {
        const e = body[16 + i * 20 ..];
        @memcpy(e[0..16], &s.md5);
        std.mem.writeInt(u32, e[16..20], s.crc32, .little);
    }
    return encodePacket(alloc, set_id, packet_type.ifsc, body);
}

/// The exponent is a 16-bit value in PAR2 but occupies 4 bytes on the
/// wire, high half zero — matching what par2cmdline emits.
pub fn encodeRecvSlc(
    alloc: std.mem.Allocator,
    set_id: [16]u8,
    exponent: u16,
    slice_body: []const u8,
) error{OutOfMemory}![]u8 {
    const body = try alloc.alloc(u8, 4 + slice_body.len);
    defer alloc.free(body);
    std.mem.writeInt(u32, body[0..4], exponent, .little);
    @memcpy(body[4..], slice_body);
    return encodePacket(alloc, set_id, packet_type.recv_slice, body);
}

pub fn encodeCreator(
    alloc: std.mem.Allocator,
    set_id: [16]u8,
    name: []const u8,
) error{OutOfMemory}![]u8 {
    const padded = std.mem.alignForward(usize, name.len, 4);
    const body = try alloc.alloc(u8, padded);
    defer alloc.free(body);
    @memset(body, 0);
    @memcpy(body[0..name.len], name);
    return encodePacket(alloc, set_id, packet_type.creator, body);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const crc32 = @import("../../core/crc32.zig");

fn hex16(comptime s: *const [32]u8) [16]u8 {
    var out: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&out, s) catch unreachable;
    return out;
}

/// Appends `packet` to `buf` and frees it — the encoders return owned
/// slices and the tests only want them concatenated.
fn append(buf: *std.ArrayList(u8), alloc: std.mem.Allocator, packet: []u8) !void {
    defer alloc.free(packet);
    try buf.appendSlice(alloc, packet);
}

test "parse round-trips an encoded stream" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_id = hex16("00112233445566778899aabbccddeeff");
    const file_id = hex16("ff112233445566778899aabbccddeeff");

    const payload = "hello par2 verify world - twelve dozen test bytes";
    var file_md5: [16]u8 = undefined;
    Md5.hash(payload, &file_md5, .{});
    // The payload is well under 16 KiB, so MD516k covers all of it.
    const md5_16k = file_md5;

    const checks = [_]SliceCheck{.{ .md5 = file_md5, .crc32 = crc32.checksum(payload) }};

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    try append(&stream, alloc, try encodeMain(alloc, set_id, payload.len, &.{file_id}));
    try append(&stream, alloc, try encodeFileDesc(alloc, set_id, file_id, file_md5, md5_16k, payload.len, "hello.bin"));
    try append(&stream, alloc, try encodeIfsc(alloc, set_id, file_id, &checks));
    try append(&stream, alloc, try encodeCreator(alloc, set_id, "hoardarr-test"));

    var set = try parse(alloc, stream.items);
    defer set.deinit(alloc);

    try t.expectEqualSlices(u8, &set_id, &set.set_id);
    try t.expectEqual(@as(u64, payload.len), set.slice_size);
    try t.expectEqual(@as(usize, 1), set.recovery_files.items.len);
    try t.expectEqualSlices(u8, &file_id, &set.recovery_files.items[0]);
    try t.expectEqual(@as(usize, 1), set.files.items.len);

    const f = set.files.items[0];
    try t.expectEqualStrings("hello.bin", f.name);
    try t.expectEqual(@as(u64, payload.len), f.size);
    try t.expectEqualSlices(u8, &file_md5, &f.md5);
    try t.expectEqualSlices(u8, &md5_16k, &f.md5_16k);
    try t.expectEqual(@as(usize, 1), f.slices.items.len);
    try t.expectEqualSlices(u8, &file_md5, &f.slices.items[0].md5);
    try t.expectEqual(crc32.checksum(payload), f.slices.items[0].crc32);
    try t.expectEqualStrings("hoardarr-test", set.creator);
    try t.expectEqual(@as(usize, 1), set.dataSliceCount());
}

test "a packet whose body fails its own MD5 is skipped" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_id = hex16("00112233445566778899aabbccddeeff");

    const corrupt = try encodeCreator(alloc, set_id, "corrupt!");
    defer alloc.free(corrupt);
    corrupt[corrupt.len - 1] ^= 0xFF;

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    try stream.appendSlice(alloc, corrupt);
    try append(&stream, alloc, try encodeCreator(alloc, set_id, "good"));

    var set = try parse(alloc, stream.items);
    defer set.deinit(alloc);
    try t.expectEqualStrings("good", set.creator);
}

test "non-PAR2 bytes around the packets are ignored" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_id = hex16("00112233445566778899aabbccddeeff");

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    // A partial magic in the junk must not throw off the scan.
    try stream.appendSlice(alloc, "PAR2\x00PK garbage prefix PAR2");
    try append(&stream, alloc, try encodeCreator(alloc, set_id, "found me"));
    try stream.appendSlice(alloc, "trailing rubbish");

    var set = try parse(alloc, stream.items);
    defer set.deinit(alloc);
    try t.expectEqualStrings("found me", set.creator);
}

test "mixed set ids are rejected" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_a = hex16("00112233445566778899aabbccddeeff");
    const set_b = hex16("ff000000000000000000000000000000");

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    try append(&stream, alloc, try encodeCreator(alloc, set_a, "set a"));
    try append(&stream, alloc, try encodeCreator(alloc, set_b, "set b"));

    try t.expectError(error.MixedSetIds, parse(alloc, stream.items));
}

test "IFSC arriving before FileDesc still lands on one file" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_id = hex16("aa112233445566778899aabbccddeeff");
    const file_id = hex16("bb112233445566778899aabbccddeeff");
    const zero: [16]u8 = @splat(0);

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    try append(&stream, alloc, try encodeIfsc(alloc, set_id, file_id, &.{
        .{ .md5 = zero, .crc32 = 7 },
        .{ .md5 = zero, .crc32 = 9 },
    }));
    try append(&stream, alloc, try encodeFileDesc(alloc, set_id, file_id, zero, zero, 2048, "late.bin"));

    var set = try parse(alloc, stream.items);
    defer set.deinit(alloc);
    try t.expectEqual(@as(usize, 1), set.files.items.len);
    try t.expectEqualStrings("late.bin", set.files.items[0].name);
    try t.expectEqual(@as(u64, 2048), set.files.items[0].size);
    try t.expectEqual(@as(usize, 2), set.files.items[0].slices.items.len);
    try t.expectEqual(@as(u32, 9), set.files.items[0].slices.items[1].crc32);
}

test "recovery slices are collected and deduped by exponent" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_id = hex16("aa112233445566778899aabbccddeeff");

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    try append(&stream, alloc, try encodeRecvSlc(alloc, set_id, 1, "first-body-------"));
    try append(&stream, alloc, try encodeRecvSlc(alloc, set_id, 2, "second-body------"));
    // Same exponent again with different content: first arrival wins.
    try append(&stream, alloc, try encodeRecvSlc(alloc, set_id, 1, "duplicate--------"));

    var set = try parse(alloc, stream.items);
    defer set.deinit(alloc);
    try t.expectEqual(@as(usize, 3), set.recovery_slice_count);
    try t.expectEqual(@as(u32, 2), set.recovery_slices.count());
    try t.expectEqualStrings("first-body-------", set.recovery_slices.get(1).?);
    try t.expectEqualStrings("second-body------", set.recovery_slices.get(2).?);
}

test "malformed packets are reported" {
    const t = std.testing;
    const alloc = t.allocator;
    const set_id = hex16("aa112233445566778899aabbccddeeff");

    // Main packet with only 8 body bytes.
    {
        const p = try encodePacket(alloc, set_id, packet_type.main, &[_]u8{0} ** 8);
        defer alloc.free(p);
        try t.expectError(error.BadMainPacket, parse(alloc, p));
    }
    // Main packet claiming two files but carrying one id.
    {
        var body: [12 + 16]u8 = @splat(0);
        std.mem.writeInt(u32, body[8..12], 2, .little);
        const p = try encodePacket(alloc, set_id, packet_type.main, &body);
        defer alloc.free(p);
        try t.expectError(error.BadMainPacket, parse(alloc, p));
    }
    // IFSC remainder that is not a multiple of 20.
    {
        const p = try encodePacket(alloc, set_id, packet_type.ifsc, &[_]u8{0} ** 30);
        defer alloc.free(p);
        try t.expectError(error.BadIfscPacket, parse(alloc, p));
    }
    // FileDesc shorter than its fixed prefix.
    {
        const p = try encodePacket(alloc, set_id, packet_type.file_desc, &[_]u8{0} ** 20);
        defer alloc.free(p);
        try t.expectError(error.BadFileDescPacket, parse(alloc, p));
    }
    // Header declaring more body than the input holds.
    {
        const p = try encodeCreator(alloc, set_id, "x");
        defer alloc.free(p);
        try t.expectError(error.TruncatedPacket, parse(alloc, p[0 .. p.len - 1]));
    }
}

test "hasPar2Suffix" {
    const t = std.testing;
    try t.expect(hasPar2Suffix("release.par2"));
    try t.expect(hasPar2Suffix("release.PAR2"));
    try t.expect(hasPar2Suffix("release.vol00+01.Par2"));
    try t.expect(hasPar2Suffix(".par2")); // bare extension still counts
    try t.expect(!hasPar2Suffix("release.rar"));
    try t.expect(!hasPar2Suffix("par2"));
    try t.expect(!hasPar2Suffix(""));
}

test "parseDir consolidates every .par2 in a directory" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id = hex16("aa112233445566778899aabbccddeeff");
    const file_id = hex16("bb112233445566778899aabbccddeeff");
    const zero: [16]u8 = @splat(0);

    // The index file carries the descriptive packets.
    var index: std.ArrayList(u8) = .empty;
    defer index.deinit(alloc);
    try append(&index, alloc, try encodeMain(alloc, set_id, 1024, &.{file_id}));
    try append(&index, alloc, try encodeFileDesc(alloc, set_id, file_id, zero, zero, 2048, "release.bin"));
    try append(&index, alloc, try encodeCreator(alloc, set_id, "test"));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "release.par2", .data = index.items });

    // The volume file repeats them and adds IFSC + recovery data.
    var vol: std.ArrayList(u8) = .empty;
    defer vol.deinit(alloc);
    try append(&vol, alloc, try encodeMain(alloc, set_id, 1024, &.{file_id}));
    try append(&vol, alloc, try encodeFileDesc(alloc, set_id, file_id, zero, zero, 2048, "release.bin"));
    try append(&vol, alloc, try encodeIfsc(alloc, set_id, file_id, &.{
        .{ .md5 = zero, .crc32 = 1 },
        .{ .md5 = zero, .crc32 = 2 },
    }));
    try append(&vol, alloc, try encodeRecvSlc(alloc, set_id, 0, &[_]u8{0xAB} ** 1024));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "release.vol00+01.par2", .data = vol.items });

    // A non-PAR2 file in the same directory must be ignored.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "release.bin", .data = "not par2" });

    var set = try parseDir(alloc, std.testing.io, tmp.dir);
    defer set.deinit(alloc);

    try t.expectEqualSlices(u8, &set_id, &set.set_id);
    try t.expectEqual(@as(u64, 1024), set.slice_size);
    // Deduped across the two files.
    try t.expectEqual(@as(usize, 1), set.files.items.len);
    try t.expectEqual(@as(usize, 2), set.files.items[0].slices.items.len);
    try t.expectEqual(@as(usize, 2), set.dataSliceCount());
    try t.expectEqual(@as(u32, 1), set.recovery_slices.count());
    try t.expectEqualStrings("test", set.creator);
}

test "parseDir with no .par2 files" {
    const t = std.testing;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "not_par2.txt", .data = "hello" });

    try t.expectError(error.NoPar2Files, parseDir(t.allocator, std.testing.io, tmp.dir));
}

test "parseFiles rejects an empty path list" {
    const t = std.testing;
    try t.expectError(
        error.NoPar2Files,
        parseFiles(t.allocator, std.testing.io, Io.Dir.cwd(), &.{}),
    );
}

/// A real PAR2 index captured off a live download, produced by ParPar
/// (not by us), for a 37-part obfuscated release. It is the only oracle
/// in this file that our own encoder did not write, so it is the one
/// that would catch a wrong field offset.
///
/// Path is relative to the repository root, which is where the test
/// binary runs from. If the fixture is not there the test is skipped
/// rather than failed — it is checked in, but a stripped checkout should
/// not look broken.
pub const fixture_dir = "testdata/repair-bug-job38";

test "parses a real ParPar-produced index" {
    const t = std.testing;
    const alloc = t.allocator;

    var set = parseFiles(alloc, std.testing.io, Io.Dir.cwd(), &.{
        fixture_dir ++ "/main.par2",
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer set.deinit(alloc);

    var set_id: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&set_id, "f0ba77a5e6e5510eb0d8f5ca586bb641");
    try t.expectEqualSlices(u8, &set_id, &set.set_id);
    try t.expectEqual(@as(u64, 1572864), set.slice_size);
    try t.expectEqual(@as(usize, 37), set.files.items.len);
    try t.expectEqual(@as(usize, 37), set.recovery_files.items.len);
    // An index file carries no recovery data, only the descriptors.
    try t.expectEqual(@as(usize, 0), set.recovery_slice_count);
    try t.expectEqualStrings("ParPar v0.3.2 [https://animetosho.org/app/parpar]", set.creator);

    // 36 full parts plus a short tail: 36 * 51380224 + 14664921 bytes at
    // 1572864 per slice = 36 * 33 + 10.
    try t.expectEqual(@as(usize, 36 * 33 + 10), set.dataSliceCount());

    // Spot-check one descriptor end to end. Every field here comes from
    // a different offset in the FileDesc body, so a shifted read shows up
    // immediately.
    const first = set.files.items[0];
    try t.expectEqualStrings("8TMnqVXYerVDFiwYeD33oWTdeSMg2.part10.rar", first.name);
    try t.expectEqual(@as(u64, 51380224), first.size);
    var md5_16k: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&md5_16k, "47be302344c1261ce877d5ec56d5ff63");
    try t.expectEqualSlices(u8, &md5_16k, &first.md5_16k);
    try t.expectEqual(@as(usize, 33), first.slices.items.len);

    // The tail part is the only one with a different length, and its
    // slice count has to be the rounded-up one.
    var found_tail = false;
    for (set.files.items) |f| {
        if (f.size == 14664921) {
            found_tail = true;
            try t.expectEqualStrings("8TMnqVXYerVDFiwYeD33oWTdeSMg2.part37.rar", f.name);
            try t.expectEqual(@as(usize, 10), f.slices.items.len);
        } else {
            try t.expectEqual(@as(u64, 51380224), f.size);
            try t.expectEqual(@as(usize, 33), f.slices.items.len);
        }
    }
    try t.expect(found_tail);

    // Every descriptor names a distinct file, and every file id the Main
    // packet listed has a descriptor.
    for (set.recovery_files.items) |id| {
        try t.expect(set.fileById(id) != null);
    }
}
