//! RAR3 (RAR 1.5 … 4.x archive format) header parsing.
//!
//! Written against UnRAR's `arcread.cpp` / `encname.cpp` and
//! cross-checked against `nwaples/rardecode`'s `archive15.go`. As with
//! RAR5 there is no published spec, so the comments here are the spec.
//!
//! # File layout
//!
//!     [SFX stub]     optional
//!     marker block   7 bytes: "Rar!\x1a\x07\x00"
//!     archive header block (type 0x73)
//!     block …        file / subblock / end headers, each optionally
//!                    followed by a data area
//!
//! The marker is itself shaped like a block header — CRC 0x6152, type
//! 0x72, flags 0x1a21, size 7 — which is a historical accident, not
//! something a reader needs to exploit.
//!
//! # Block layout
//!
//! | offset | size | field |
//! |-|-|-|
//! | 0 | 2 | HEAD_CRC: low 16 bits of CRC32 over bytes 2 … HEAD_SIZE |
//! | 2 | 1 | HEAD_TYPE |
//! | 3 | 2 | HEAD_FLAGS, LE |
//! | 5 | 2 | HEAD_SIZE, LE — the whole header including these 7 bytes |
//! | 7 | 4 | ADD_SIZE, LE — data area length; present only if flags & 0x8000 |
//! | 11 | … | type-specific fields |
//!
//! Everything is 16- or 32-bit little-endian: no vints, and therefore a
//! hard 64 KiB ceiling on a header, since HEAD_SIZE is a u16.
//!
//! # File header (type 0x74)
//!
//! Offsets are from the start of the block. ADD_SIZE doubles as the low
//! 32 bits of PACK_SIZE, which is why file blocks always set 0x8000.
//!
//! | offset | size | field |
//! |-|-|-|
//! | 7 | 4 | PACK_SIZE low 32 bits (= ADD_SIZE) |
//! | 11 | 4 | UNP_SIZE low 32 bits |
//! | 15 | 1 | HOST_OS: 0 MS-DOS, 1 OS/2, 2 Windows, 3 Unix, 4 macOS, 5 BeOS |
//! | 16 | 4 | FILE_CRC32 of the unpacked file |
//! | 20 | 4 | FTIME in MS-DOS packed format |
//! | 24 | 1 | UNP_VER: 15, 20, 26, 29 … |
//! | 25 | 1 | METHOD: 0x30 store, 0x31…0x35 fastest…best |
//! | 26 | 2 | NAME_SIZE |
//! | 28 | 4 | ATTR |
//! | 32 | 4 | HIGH_PACK_SIZE — only if flags & 0x0100 |
//! | 36 | 4 | HIGH_UNP_SIZE — only if flags & 0x0100 |
//! | … | NAME_SIZE | FILE_NAME |
//! | … | 8 | SALT — only if flags & 0x0400 |
//! | … | … | EXT_TIME — only if flags & 0x1000 |
//!
//! Sizes are split into low/high 32-bit halves rather than being 64-bit:
//! `HIGH_*` only appears for files over 4 GiB. Both halves have to be
//! reassembled or a 5 GiB file silently becomes a 1 GiB one.

const std = @import("std");
const crc32 = @import("../../core/crc32.zig");
const cursor = @import("cursor.zig");
const source = @import("source.zig");

const Cursor = cursor.Cursor;
const Source = source.Source;

/// "Rar!\x1a\x07\x00" — the same six-byte prefix as RAR5, then a 0
/// version byte instead of 1.
pub const signature = [7]u8{ 'R', 'a', 'r', '!', 0x1a, 0x07, 0x00 };

/// HEAD_SIZE is a u16, so this is the format's own ceiling.
pub const max_header_size = 0xffff;

/// The 7 bytes every block starts with.
pub const prefix_len = 7;

pub const BlockType = enum(u8) {
    marker = 0x72,
    archive = 0x73,
    file = 0x74,
    comment = 0x75,
    /// Old-style authenticity information.
    auth_old = 0x76,
    sub_old = 0x77,
    recovery = 0x78,
    auth = 0x79,
    /// RAR 3.x subblock: NTFS ACLs, streams, the recovery record's
    /// metadata. Its data area is never file content.
    sub = 0x7a,
    end = 0x7b,
    _,
};

/// Flag common to all block types.
pub const block_flag = struct {
    /// ADD_SIZE is present at offset 7 and a data area follows the
    /// header. UnRAR calls this LONG_BLOCK.
    pub const long_block: u16 = 0x8000;
    /// Blocks with this set may be skipped by an updater that does not
    /// understand them.
    pub const skip_if_unknown: u16 = 0x4000;
};

/// Archive header (0x73) flags.
pub const archive_flag = struct {
    /// This file is one volume of a multi-volume set.
    pub const volume: u16 = 0x0001;
    /// An archive comment is embedded in this header (RAR 2.x style).
    pub const comment: u16 = 0x0002;
    pub const locked: u16 = 0x0004;
    pub const solid: u16 = 0x0008;
    /// Volumes are named `name.partNN.rar` rather than
    /// `name.rar`/`name.r00`.
    pub const new_naming: u16 = 0x0010;
    pub const auth_info: u16 = 0x0020;
    pub const recovery: u16 = 0x0040;
    /// Every block header after this one is AES-encrypted (`-hp`).
    /// Detected, never decrypted.
    pub const encrypted_headers: u16 = 0x0080;
    /// Set by RAR 3.0+ on the first volume of a set.
    pub const first_volume: u16 = 0x0100;
};

/// File header (0x74) flags. UnRAR's LHD_* constants.
pub const file_flag = struct {
    /// The file's data continues from the previous volume.
    pub const split_before: u16 = 0x0001;
    /// The file's data continues into the next volume.
    pub const split_after: u16 = 0x0002;
    /// The data area is AES-encrypted with a password-derived key.
    pub const encrypted: u16 = 0x0004;
    pub const comment: u16 = 0x0008;
    pub const solid: u16 = 0x0010;
    /// Bits 5..7 hold the dictionary size. The all-ones value is
    /// overloaded to mean "directory", which is why a directory entry
    /// has no dictionary.
    pub const window_mask: u16 = 0x00e0;
    /// HIGH_PACK_SIZE / HIGH_UNP_SIZE are present.
    pub const large: u16 = 0x0100;
    /// FILE_NAME uses the compressed UTF-16 encoding.
    pub const unicode: u16 = 0x0200;
    /// An 8-byte AES salt follows the name.
    pub const salt: u16 = 0x0400;
    pub const version: u16 = 0x0800;
    /// High-precision timestamps follow.
    pub const ext_time: u16 = 0x1000;
};

/// `flags & window_mask == window_mask` means directory.
pub const directory_mask: u16 = file_flag.window_mask;

/// End-of-archive (0x7b) flags.
pub const end_flag = struct {
    /// Not the last volume of the set.
    pub const not_last: u16 = 0x0001;
    pub const has_data_crc: u16 = 0x0002;
    pub const rev_space: u16 = 0x0004;
    pub const has_volume_number: u16 = 0x0008;
};

/// METHOD byte values. Stored as ASCII digits, so `-m0` is 0x30.
pub const Method = enum(u8) {
    store = 0x30,
    fastest = 0x31,
    fast = 0x32,
    normal = 0x33,
    good = 0x34,
    best = 0x35,
    _,

    pub fn name(m: Method) []const u8 {
        return switch (m) {
            .store => "store (-m0)",
            .fastest => "fastest (-m1)",
            .fast => "fast (-m2)",
            .normal => "normal (-m3)",
            .good => "good (-m4)",
            .best => "best (-m5)",
            _ => "unknown",
        };
    }
};

pub const Error = cursor.Error || source.Error || error{
    BadHeaderCrc,
    /// HEAD_SIZE smaller than the 7-byte prefix, or a header that does
    /// not hold the fields its type requires.
    CorruptHeader,
    TruncatedVolume,
    HeaderTooLarge,
};

pub const NameError = error{
    /// The compressed UTF-16 name is malformed, or decodes to unpaired
    /// surrogates.
    InvalidName,
    /// The decoded name exceeds `max_name_chars`.
    NameTooLong,
};

/// Longest name we will decode, in UTF-16 code units. UnRAR's own limit
/// is 2048; 1024 is comfortably above any real release name and keeps
/// the decode buffer small enough to live on the stack.
pub const max_name_chars = 1024;

pub const BlockHeader = struct {
    kind: BlockType,
    flags: u16,
    /// HEAD_SIZE, including the 7-byte prefix.
    header_size: u16,
    /// Type-specific fields: bytes 7 (or 11, when ADD_SIZE is present)
    /// through HEAD_SIZE. Borrowed from the caller's header buffer.
    fields: []const u8,
    data_offset: u64,
    data_size: u64,
    next_offset: u64,

    pub fn hasFlag(h: BlockHeader, f: u16) bool {
        return h.flags & f != 0;
    }

    pub fn isFirstPiece(h: BlockHeader) bool {
        return !h.hasFlag(file_flag.split_before);
    }

    pub fn isLastPiece(h: BlockHeader) bool {
        return !h.hasFlag(file_flag.split_after);
    }
};

/// Locates the RAR3 marker block. Returns the offset just past it.
pub fn findSignature(src: Source, scan_limit: u64, buf: []u8) source.Error!?u64 {
    const rar5 = @import("rar5.zig");
    return rar5.findSignatureBytes(src, scan_limit, buf, &signature);
}

pub fn readBlockHeader(src: Source, offset: u64, buf: []u8) Error!BlockHeader {
    std.debug.assert(buf.len >= 64);
    if (offset >= src.size) return error.TruncatedVolume;

    const avail = src.size - offset;
    const want: usize = @intCast(@min(@as(u64, buf.len), avail));
    const got = try src.readAt(offset, buf[0..want]);
    const raw = buf[0..got];
    if (raw.len < prefix_len) return error.TruncatedVolume;

    const want_crc = std.mem.readInt(u16, raw[0..2], .little);
    const kind: BlockType = @enumFromInt(raw[2]);
    const flags = std.mem.readInt(u16, raw[3..5], .little);
    const header_size = std.mem.readInt(u16, raw[5..7], .little);

    // A RAR 2.x archive header can carry an inline comment, in which
    // case HEAD_SIZE covers the comment too but the CRC only covers the
    // first 13 bytes. UnRAR special-cases exactly this.
    var crc_len: usize = header_size;
    if (kind == .archive and flags & archive_flag.comment != 0) {
        if (header_size < 13) return error.CorruptHeader;
        crc_len = 13;
    } else if (kind == .comment) {
        if (header_size < 13) return error.CorruptHeader;
        crc_len = 13;
    } else if (header_size < prefix_len) {
        return error.CorruptHeader;
    }
    if (header_size > buf.len) return error.HeaderTooLarge;
    if (header_size > raw.len) return error.TruncatedVolume;

    // CRC16 = the low half of a CRC32 over HEAD_TYPE onwards.
    const sum: u16 = @truncate(crc32.checksum(raw[2..crc_len]));
    if (sum != want_crc) return error.BadHeaderCrc;

    var c = Cursor.init(raw[prefix_len..header_size]);
    var data_size: u64 = 0;
    if (flags & block_flag.long_block != 0) {
        data_size = try c.u32le();
    }

    // Files over 4 GiB split their sizes across two 32-bit halves. The
    // high half of PACK_SIZE sits at offset 21 of the post-ADD_SIZE
    // field area (block offset 32).
    if ((kind == .file or kind == .sub) and flags & file_flag.large != 0) {
        const fields = c.rest();
        if (fields.len < 25) return error.CorruptHeader;
        const high = std.mem.readInt(u32, fields[21..25], .little);
        data_size |= @as(u64, high) << 32;
    }

    if (header_size == 0) return error.CorruptHeader;
    const header_end = offset + header_size;
    if (data_size > src.size - header_end) return error.TruncatedVolume;
    // Guard the caller's forward progress: a zero-size header would spin.
    if (header_size < prefix_len) return error.CorruptHeader;

    return .{
        .kind = kind,
        .flags = flags,
        .header_size = header_size,
        .fields = c.rest(),
        .data_offset = header_end,
        .data_size = data_size,
        .next_offset = header_end + data_size,
    };
}

pub const ArchiveHeader = struct {
    multi_volume: bool,
    solid: bool,
    /// False means the old `.rar`/`.r00`/`.r01` naming scheme.
    new_naming: bool,
    encrypted_headers: bool,
    first_volume: bool,
};

pub fn parseArchive(h: BlockHeader) ArchiveHeader {
    return .{
        .multi_volume = h.hasFlag(archive_flag.volume),
        .solid = h.hasFlag(archive_flag.solid),
        .new_naming = h.hasFlag(archive_flag.new_naming),
        .encrypted_headers = h.hasFlag(archive_flag.encrypted_headers),
        .first_volume = h.hasFlag(archive_flag.first_volume),
    };
}

pub const FileHeader = struct {
    /// Total unpacked length of the whole file, both 32-bit halves
    /// reassembled.
    unpacked_size: u64,
    size_unknown: bool,
    host_os: u8,
    crc32: u32,
    dos_time: u32,
    unpack_version: u8,
    method: Method,
    attributes: u32,
    is_dir: bool,
    encrypted: bool,
    solid: bool,
    /// Raw name bytes as stored, before decoding. Borrowed.
    raw_name: []const u8,
    /// The name used the compressed UTF-16 encoding.
    unicode_name: bool,
};

pub fn parseFile(h: BlockHeader) Error!FileHeader {
    var c = Cursor.init(h.fields);
    var unpacked: u64 = try c.u32le();
    const host_os = try c.byte();
    const file_crc = try c.u32le();
    const dos_time = try c.u32le();
    const unpack_version = try c.byte();
    const method: Method = @enumFromInt(try c.byte());
    const name_size = try c.u16le();
    const attributes = try c.u32le();

    var size_unknown = false;
    if (h.hasFlag(file_flag.large)) {
        _ = try c.u32le(); // HIGH_PACK_SIZE, already folded into data_size
        unpacked |= @as(u64, try c.u32le()) << 32;
        size_unknown = unpacked == std.math.maxInt(u64);
    } else if (unpacked == std.math.maxInt(u32)) {
        // A 32-bit -1 is UnRAR's "size not known in advance" marker,
        // used when the packer read from a pipe.
        size_unknown = true;
    }

    if (name_size > c.remaining()) return error.CorruptHeader;
    const raw_name = try c.take(name_size);

    return .{
        .unpacked_size = unpacked,
        .size_unknown = size_unknown,
        .host_os = host_os,
        .crc32 = file_crc,
        .dos_time = dos_time,
        .unpack_version = unpack_version,
        .method = method,
        .attributes = attributes,
        .is_dir = h.flags & directory_mask == directory_mask,
        .encrypted = h.hasFlag(file_flag.encrypted),
        .solid = h.hasFlag(file_flag.solid),
        .raw_name = raw_name,
        .unicode_name = h.hasFlag(file_flag.unicode),
    };
}

pub fn parseEnd(h: BlockHeader) struct { not_last: bool } {
    return .{ .not_last = h.hasFlag(end_flag.not_last) };
}

/// True when a Unix-hosted entry's attributes describe a symlink.
/// `ATTR` holds `st_mode` for Unix hosts, so the file type is in the top
/// four bits of the low 16 (`S_IFMT >> 12 == 0xa` for a symlink). A
/// symlink's "data" is its target path, and materialising it would let a
/// crafted archive plant a link out of the target directory.
pub fn isUnixSymlink(f: FileHeader) bool {
    // HOST_OS 3 is Unix; UnRAR also treats 4 (macOS) and 5 (BeOS) as
    // Unix-like for the attribute layout.
    if (f.host_os != 3 and f.host_os != 4 and f.host_os != 5) return false;
    return (f.attributes & 0xf000) == 0xa000;
}

// ---------------------------------------------------------------------
// Filename decoding
//
// RAR3 predates universal UTF-8. A name is stored either as raw bytes in
// the packer's OEM code page, or — when LHD_UNICODE is set — as the
// "compressed unicode" form UnRAR calls `EncodeFileName`:
//
//     <ascii name> 0x00 <high byte> <flag byte> <encoded stream>
//
// The encoded stream is read two bits at a time out of `flag byte`,
// refilling from the stream every four operations:
//
//   00  next stream byte is a code unit on its own (U+0000…U+00FF)
//   01  next stream byte OR (high byte << 8)
//   10  next two stream bytes are a little-endian code unit
//   11  a run: next byte is a length; bit 7 set means the run also
//       carries a correction byte added to each ASCII byte before the
//       high byte is applied. Run bytes come from the *ASCII* name, which
//       is why both halves are needed to decode either.
//
// The run case is what makes the format compact for names that are
// mostly ASCII with a few accented characters, and it is also why a
// naive decoder walks off the end of the ASCII half. Every index here is
// bounded.
// ---------------------------------------------------------------------

/// Decodes a stored RAR3 name into UTF-8 in `out`. Returns the used
/// prefix. `out` must hold at least `3 * max_name_chars` bytes for the
/// unicode form, or `2 * raw.len` for the OEM form.
pub fn decodeName(raw: []const u8, unicode: bool, out: []u8) NameError![]u8 {
    if (raw.len == 0) return error.InvalidName;
    if (raw.len > max_name_chars) return error.NameTooLong;

    const zero = std.mem.indexOfScalar(u8, raw, 0);

    if (!unicode) {
        // The name is raw bytes in the packer's code page. A NUL has no
        // meaning here, so it is hostile: truncating at it would let
        // `ok.txt\x00/../../etc/passwd` present a harmless name to a
        // component check and a different one to the syscall. Refuse.
        if (zero != null) return error.InvalidName;
        return plainName(raw, out);
    }
    if (zero == null) {
        // LHD_UNICODE was set but no encoded half was stored. UnRAR
        // falls back to the plain bytes.
        return plainName(raw, out);
    }

    const ascii = raw[0..zero.?];
    const enc = raw[zero.? + 1 ..];
    if (enc.len < 2) return error.InvalidName;

    var units: [max_name_chars]u16 = undefined;
    var n: usize = 0;

    const high_byte: u16 = @as(u16, enc[0]) << 8;
    var pos: usize = 1;
    var flags: u8 = enc[pos];
    pos += 1;
    var flag_bits: u4 = 8;

    while (n < ascii.len and pos < enc.len) {
        if (flag_bits == 0) {
            flags = enc[pos];
            pos += 1;
            flag_bits = 8;
            if (pos >= enc.len) break;
        }
        switch (flags >> 6) {
            0 => {
                units[n] = enc[pos];
                pos += 1;
                n += 1;
            },
            1 => {
                units[n] = @as(u16, enc[pos]) | high_byte;
                pos += 1;
                n += 1;
            },
            2 => {
                if (enc.len - pos < 2) break;
                units[n] = std.mem.readInt(u16, enc[pos..][0..2], .little);
                pos += 2;
                n += 1;
            },
            3 => {
                const len_byte = enc[pos];
                pos += 1;
                var run = ascii[n..];
                const claimed: usize = @as(usize, len_byte & 0x7f) + 2;
                if (claimed < run.len) run = run[0..claimed];
                if (len_byte & 0x80 != 0) {
                    if (pos >= enc.len) break;
                    const correction = enc[pos];
                    pos += 1;
                    for (run) |ch| {
                        if (n == max_name_chars) return error.NameTooLong;
                        units[n] = @as(u16, ch +% correction) | high_byte;
                        n += 1;
                    }
                } else {
                    for (run) |ch| {
                        if (n == max_name_chars) return error.NameTooLong;
                        units[n] = ch;
                        n += 1;
                    }
                }
            },
            else => unreachable,
        }
        if (n > max_name_chars) return error.NameTooLong;
        flags <<= 2;
        flag_bits -= 2;
    }
    if (n == 0) return error.InvalidName;
    return utf16ToUtf8(units[0..n], out);
}

/// A name with no encoded half: keep the bytes if they are already valid
/// UTF-8, otherwise read them as Latin-1. Latin-1 is the best guess
/// available for an unlabelled OEM code page and, unlike a lossy
/// replacement, it always round-trips to a valid name.
fn plainName(raw: []const u8, out: []u8) NameError![]u8 {
    if (raw.len == 0) return error.InvalidName;
    if (std.unicode.utf8ValidateSlice(raw)) {
        if (out.len < raw.len) return error.NameTooLong;
        @memcpy(out[0..raw.len], raw);
        return out[0..raw.len];
    }
    return latin1ToUtf8(raw, out);
}

/// UTF-16 to UTF-8, written out rather than delegated so surrogate
/// pairing is explicit: an unpaired half means the name is malformed and
/// must not be silently replaced with U+FFFD, since a replacement
/// character changes the name a caller then writes to disk.
fn utf16ToUtf8(units: []const u16, out: []u8) NameError![]u8 {
    var w: usize = 0;
    var i: usize = 0;
    while (i < units.len) {
        var cp: u21 = units[i];
        i += 1;
        if (cp >= 0xd800 and cp <= 0xdbff) {
            if (i == units.len) return error.InvalidName;
            const lo = units[i];
            if (lo < 0xdc00 or lo > 0xdfff) return error.InvalidName;
            i += 1;
            cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
        } else if (cp >= 0xdc00 and cp <= 0xdfff) {
            return error.InvalidName;
        }
        var tmp: [4]u8 = undefined;
        const len = std.unicode.utf8Encode(cp, &tmp) catch return error.InvalidName;
        if (w + len > out.len) return error.NameTooLong;
        @memcpy(out[w..][0..len], tmp[0..len]);
        w += len;
    }
    return out[0..w];
}

fn latin1ToUtf8(in: []const u8, out: []u8) NameError![]u8 {
    var w: usize = 0;
    for (in) |b| {
        if (b < 0x80) {
            if (w + 1 > out.len) return error.NameTooLong;
            out[w] = b;
            w += 1;
        } else {
            if (w + 2 > out.len) return error.NameTooLong;
            out[w] = 0xc0 | (b >> 6);
            out[w + 1] = 0x80 | (b & 0x3f);
            w += 2;
        }
    }
    return out[0..w];
}

// ---------------------------------------------------------------------
// Fixture builder — see the note in rar5.zig for why fixtures are
// hand-assembled rather than produced by a `rar` binary.
// ---------------------------------------------------------------------

pub const Builder = struct {
    buf: std.ArrayList(u8) = .empty,
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) Builder {
        return .{ .gpa = gpa };
    }

    pub fn deinit(b: *Builder) void {
        b.buf.deinit(b.gpa);
    }

    pub fn bytes(b: *const Builder) []const u8 {
        return b.buf.items;
    }

    pub fn raw(b: *Builder, data: []const u8) !void {
        try b.buf.appendSlice(b.gpa, data);
    }

    pub fn addSignature(b: *Builder) !void {
        try b.raw(&signature);
    }

    /// Emits a block. `fields` excludes the 7-byte prefix and the
    /// ADD_SIZE field; ADD_SIZE is derived from `declared_data_size`.
    pub fn addBlock(
        b: *Builder,
        kind: u8,
        flags: u16,
        fields: []const u8,
        declared_data_size: ?u64,
        data: []const u8,
    ) !void {
        var eff_flags = flags;
        if (declared_data_size != null) eff_flags |= block_flag.long_block;

        var head: std.ArrayList(u8) = .empty;
        defer head.deinit(b.gpa);
        try head.appendNTimes(b.gpa, 0, 2); // CRC placeholder
        try head.append(b.gpa, kind);
        try head.appendNTimes(b.gpa, 0, 2); // flags placeholder
        try head.appendNTimes(b.gpa, 0, 2); // size placeholder
        if (declared_data_size) |n| {
            var le: [4]u8 = undefined;
            std.mem.writeInt(u32, &le, @truncate(n), .little);
            try head.appendSlice(b.gpa, &le);
        }
        try head.appendSlice(b.gpa, fields);

        std.mem.writeInt(u16, head.items[3..5], eff_flags, .little);
        std.mem.writeInt(u16, head.items[5..7], @intCast(head.items.len), .little);
        const sum: u16 = @truncate(crc32.checksum(head.items[2..]));
        std.mem.writeInt(u16, head.items[0..2], sum, .little);

        try b.raw(head.items);
        try b.raw(data);
    }

    pub fn addBlockBadCrc(b: *Builder, kind: u8, flags: u16, fields: []const u8) !void {
        const before = b.buf.items.len;
        try b.addBlock(kind, flags, fields, null, &.{});
        b.buf.items[before] ^= 0xff;
    }

    pub fn addArchiveHeader(b: *Builder, opts: struct {
        multi_volume: bool = false,
        solid: bool = false,
        new_naming: bool = true,
        encrypted_headers: bool = false,
        first_volume: bool = false,
    }) !void {
        var flags: u16 = 0;
        if (opts.multi_volume) flags |= archive_flag.volume;
        if (opts.solid) flags |= archive_flag.solid;
        if (opts.new_naming) flags |= archive_flag.new_naming;
        if (opts.encrypted_headers) flags |= archive_flag.encrypted_headers;
        if (opts.first_volume) flags |= archive_flag.first_volume;
        // RESERVED1 (2) + RESERVED2 (4): HEAD_SIZE lands on 13.
        try b.addBlock(@intFromEnum(BlockType.archive), flags, &[_]u8{0} ** 6, null, &.{});
    }

    pub const FileOpts = struct {
        name: []const u8,
        data: []const u8 = &.{},
        unpacked_size: ?u64 = null,
        declared_data_size: ?u64 = null,
        crc32: u32 = 0,
        method: u8 = 0x30,
        unpack_version: u8 = 29,
        is_dir: bool = false,
        split_before: bool = false,
        split_after: bool = false,
        encrypted: bool = false,
        solid: bool = false,
        unicode_name: bool = false,
        large: bool = false,
        host_os: u8 = 3,
        attributes: u32 = 0,
        dos_time: u32 = 0,
        salt: bool = false,
    };

    pub fn addFile(b: *Builder, opts: FileOpts) !void {
        const unpacked = opts.unpacked_size orelse opts.data.len;
        const declared = opts.declared_data_size orelse opts.data.len;

        var flags: u16 = 0;
        if (opts.split_before) flags |= file_flag.split_before;
        if (opts.split_after) flags |= file_flag.split_after;
        if (opts.encrypted) flags |= file_flag.encrypted;
        if (opts.solid) flags |= file_flag.solid;
        if (opts.unicode_name) flags |= file_flag.unicode;
        if (opts.large) flags |= file_flag.large;
        if (opts.salt) flags |= file_flag.salt;
        if (opts.is_dir) {
            flags |= directory_mask;
        } else {
            // Dictionary bits: 0x60 == 4 MiB window, what RAR 3.x uses.
            flags |= 0x0060;
        }

        var f: std.ArrayList(u8) = .empty;
        defer f.deinit(b.gpa);
        try appendU32(&f, b.gpa, @truncate(unpacked));
        try f.append(b.gpa, opts.host_os);
        try appendU32(&f, b.gpa, opts.crc32);
        try appendU32(&f, b.gpa, opts.dos_time);
        try f.append(b.gpa, opts.unpack_version);
        try f.append(b.gpa, opts.method);
        try appendU16(&f, b.gpa, @intCast(opts.name.len));
        try appendU32(&f, b.gpa, opts.attributes);
        if (opts.large) {
            try appendU32(&f, b.gpa, @truncate(declared >> 32));
            try appendU32(&f, b.gpa, @truncate(unpacked >> 32));
        }
        try f.appendSlice(b.gpa, opts.name);
        if (opts.salt) try f.appendNTimes(b.gpa, 0x5a, 8);

        try b.addBlock(@intFromEnum(BlockType.file), flags, f.items, declared, opts.data);
    }

    /// A 0x7a subblock, whose data area must be skipped rather than
    /// extracted.
    pub fn addSubBlock(b: *Builder, name: []const u8, data: []const u8) !void {
        var f: std.ArrayList(u8) = .empty;
        defer f.deinit(b.gpa);
        try appendU32(&f, b.gpa, @intCast(data.len));
        try f.append(b.gpa, 3);
        try appendU32(&f, b.gpa, 0);
        try appendU32(&f, b.gpa, 0);
        try f.append(b.gpa, 29);
        try f.append(b.gpa, 0x30);
        try appendU16(&f, b.gpa, @intCast(name.len));
        try appendU32(&f, b.gpa, 0);
        try f.appendSlice(b.gpa, name);
        try b.addBlock(@intFromEnum(BlockType.sub), 0x0060, f.items, data.len, data);
    }

    pub fn addEnd(b: *Builder, not_last: bool) !void {
        try b.addBlock(
            @intFromEnum(BlockType.end),
            if (not_last) end_flag.not_last else 0,
            &.{},
            null,
            &.{},
        );
    }

    fn appendU16(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: u16) !void {
        var le: [2]u8 = undefined;
        std.mem.writeInt(u16, &le, v, .little);
        try list.appendSlice(gpa, &le);
    }

    fn appendU32(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: u32) !void {
        var le: [4]u8 = undefined;
        std.mem.writeInt(u32, &le, v, .little);
        try list.appendSlice(gpa, &le);
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "marker block is located" {
    var buf: [64]u8 = undefined;
    var plain = source.Memory.init(&signature);
    try t.expectEqual(@as(?u64, 7), try findSignature(plain.source(), 1 << 20, &buf));

    // A RAR5 signature must not satisfy the RAR3 search: byte 6 differs.
    const rar5 = @import("rar5.zig");
    var five = source.Memory.init(&rar5.signature);
    try t.expectEqual(@as(?u64, null), try findSignature(five.source(), 1 << 20, &buf));
}

test "archive and file headers round-trip" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{ .multi_volume = true, .first_volume = true });
    try b.addFile(.{
        .name = "dir\\movie.mkv",
        .data = "hello world",
        .crc32 = crc32.checksum("hello world"),
        .attributes = 0o100644,
    });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;

    const arc = try readBlockHeader(src, signature.len, &hdr_buf);
    try t.expectEqual(BlockType.archive, arc.kind);
    try t.expectEqual(@as(u16, 13), arc.header_size);
    const ah = parseArchive(arc);
    try t.expect(ah.multi_volume);
    try t.expect(ah.new_naming);
    try t.expect(!ah.encrypted_headers);

    const fh = try readBlockHeader(src, arc.next_offset, &hdr_buf);
    try t.expectEqual(BlockType.file, fh.kind);
    try t.expectEqual(@as(u64, 11), fh.data_size);
    try t.expect(fh.isFirstPiece());
    try t.expect(fh.isLastPiece());

    const f = try parseFile(fh);
    try t.expectEqualStrings("dir\\movie.mkv", f.raw_name);
    try t.expectEqual(@as(u64, 11), f.unpacked_size);
    try t.expectEqual(Method.store, f.method);
    try t.expectEqual(crc32.checksum("hello world"), f.crc32);
    try t.expect(!f.is_dir);
    try t.expect(!f.encrypted);

    var data: [11]u8 = undefined;
    try src.readAll(fh.data_offset, &data);
    try t.expectEqualStrings("hello world", &data);
}

test "directory entries are flagged by the window mask" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addFile(.{ .name = "subdir", .is_dir = true });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    const arc = try readBlockHeader(src, signature.len, &hdr_buf);
    const fh = try readBlockHeader(src, arc.next_offset, &hdr_buf);
    try t.expect((try parseFile(fh)).is_dir);
}

test "large files reassemble both halves of the size fields" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    // 5 GiB declared, but the data area is only what is present. The
    // header parser must reject the mismatch rather than trust it.
    try b.addFile(.{
        .name = "big.bin",
        .data = "AAAA",
        .large = true,
        .unpacked_size = 5 << 30,
        .declared_data_size = 4,
    });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    const arc = try readBlockHeader(src, signature.len, &hdr_buf);
    const fh = try readBlockHeader(src, arc.next_offset, &hdr_buf);
    try t.expectEqual(@as(u64, 4), fh.data_size);
    const f = try parseFile(fh);
    try t.expectEqual(@as(u64, 5 << 30), f.unpacked_size);
}

test "a data area larger than the volume is refused" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addFile(.{
        .name = "huge.bin",
        .data = "AAAA",
        .large = true,
        .declared_data_size = 900 << 30,
    });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    const arc = try readBlockHeader(src, signature.len, &hdr_buf);
    try t.expectError(error.TruncatedVolume, readBlockHeader(src, arc.next_offset, &hdr_buf));
}

test "a corrupt header CRC is refused" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addBlockBadCrc(@intFromEnum(BlockType.archive), 0, &[_]u8{0} ** 6);
    var mem = source.Memory.init(b.bytes());
    var hdr_buf: [1024]u8 = undefined;
    try t.expectError(error.BadHeaderCrc, readBlockHeader(mem.source(), signature.len, &hdr_buf));
}

test "a HEAD_SIZE below the prefix length is refused" {
    // Hand-built: CRC over type..size, HEAD_SIZE = 3.
    var block: [7]u8 = .{ 0, 0, @intFromEnum(BlockType.file), 0, 0, 3, 0 };
    const sum: u16 = @truncate(crc32.checksum(block[2..3]));
    std.mem.writeInt(u16, block[0..2], sum, .little);
    var mem = source.Memory.init(&block);
    var hdr_buf: [1024]u8 = undefined;
    try t.expectError(error.CorruptHeader, readBlockHeader(mem.source(), 0, &hdr_buf));
}

test "a header claiming more bytes than remain is refused" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    const full = b.bytes();
    // Cut the archive header in half.
    var mem = source.Memory.init(full[0 .. full.len - 6]);
    var hdr_buf: [1024]u8 = undefined;
    try t.expectError(error.TruncatedVolume, readBlockHeader(mem.source(), signature.len, &hdr_buf));
}

test "unix symlink entries are recognised from the attributes" {
    var f: FileHeader = std.mem.zeroes(FileHeader);
    f.host_os = 3;
    f.attributes = 0o120777; // S_IFLNK | 0777
    try t.expect(isUnixSymlink(f));
    f.attributes = 0o100644; // S_IFREG
    try t.expect(!isUnixSymlink(f));
    // Windows-hosted attributes are a completely different bitfield, so
    // the same value must not be read as a symlink.
    f.host_os = 2;
    f.attributes = 0o120777;
    try t.expect(!isUnixSymlink(f));
}

// --- filename decoding -----------------------------------------------

fn expectDecoded(want: []const u8, raw: []const u8, unicode: bool) !void {
    var out: [3 * max_name_chars]u8 = undefined;
    try t.expectEqualStrings(want, try decodeName(raw, unicode, &out));
}

test "non-unicode names pass through when they are valid UTF-8" {
    try expectDecoded("movie.mkv", "movie.mkv", false);
    try expectDecoded("dir\\movie.mkv", "dir\\movie.mkv", false);
    // Already UTF-8 in the raw field: keep the bytes.
    try expectDecoded("Wüste.mkv", "Wüste.mkv", false);
}

test "non-unicode names that are not UTF-8 are read as Latin-1" {
    // 0xfc is U+00FC in Latin-1, and not valid UTF-8 on its own.
    try expectDecoded("W\u{00fc}ste.mkv", "W\xfcste.mkv", false);
}

test "a NUL in a non-unicode name is refused, never truncated" {
    // Without the unicode flag a NUL has no meaning in the format, so
    // this is an attempt to show one name to a validator and another to
    // the kernel.
    var out: [3 * max_name_chars]u8 = undefined;
    try t.expectError(error.InvalidName, decodeName("ok.txt\x00/../../etc/passwd", false, &out));
    try t.expectError(error.InvalidName, decodeName("a\x00b", false, &out));
    try t.expectError(error.InvalidName, decodeName("\x00", false, &out));
}

test "unicode names decode through the two-bit opcode stream" {
    // Opcode 0: each stream byte is a code unit of its own. Two chars
    // "ab" then the flag byte is exhausted.
    //   ascii "ab", 0, high byte 0x00, flags 0b00000000, stream 'a','b'
    try expectDecoded("ab", "ab\x00\x00\x00ab", true);

    // Opcode 1: stream byte OR (high << 8). high 0x04, 0x3f -> U+043F.
    //   flags 0b01_000000 = 0x40. The stream runs out after one
    //   operation, so decoding stops there — the ASCII half is *not*
    //   appended as a fallback. UnRAR behaves the same way: the encoded
    //   half is authoritative for however far it reaches.
    try expectDecoded("\u{043f}", "ab\x00\x04\x40\x3f", true);

    // Opcode 2: two little-endian stream bytes are one code unit.
    //   flags 0b10_000000 = 0x80, stream 0xac 0x20 -> U+20AC (euro).
    try expectDecoded("\u{20ac}", "ab\x00\x00\x80\xac\x20", true);

    // Opcode 3 without correction: copy a run straight from the ASCII
    // half. length byte 0 means 2 characters.
    try expectDecoded("ab", "ab\x00\x00\xc0\x00", true);

    // Opcode 3 with correction: length 0x80 (2 chars) plus a correction
    // byte added to each ASCII byte, then OR'd with the high byte.
    //   'a'(0x61) + 0x20 = 0x81, high 0x00 -> U+0081; 'b' -> U+0082.
    try expectDecoded("\u{81}\u{82}", "ab\x00\x00\xc0\x80\x20", true);
}

test "unicode name decoding is bounded on malformed input" {
    var out: [3 * max_name_chars]u8 = undefined;
    // No encoded half at all after the NUL.
    try t.expectError(error.InvalidName, decodeName("ab\x00", true, &out));
    try t.expectError(error.InvalidName, decodeName("ab\x00\x00", true, &out));
    // Empty.
    try t.expectError(error.InvalidName, decodeName("", true, &out));
    // Leading NUL: no ASCII half, so nothing can be decoded.
    try t.expectError(error.InvalidName, decodeName("\x00\x00\x00a", true, &out));
    // Stream ends after one opcode: decoding stops there rather than
    // reading past the end of the buffer.
    try expectDecoded("a", "ab\x00\x00\x00a", true);
    // Opcode 2 needs two stream bytes and only one is left, so nothing
    // decodes at all and the name is refused rather than silently
    // becoming the empty string.
    try t.expectError(error.InvalidName, decodeName("ab\x00\x00\x80\x61", true, &out));

    // A name longer than the cap.
    var long: [max_name_chars + 8]u8 = @splat('a');
    try t.expectError(error.NameTooLong, decodeName(&long, false, &out));
}

test "unpaired surrogates are rejected rather than replaced" {
    var out: [64]u8 = undefined;
    // Opcode 2 emitting a lone high surrogate 0xD800.
    try t.expectError(error.InvalidName, decodeName("ab\x00\x00\x80\x00\xd8", true, &out));
    // Lone low surrogate.
    try t.expectError(error.InvalidName, decodeName("ab\x00\x00\x80\x00\xdc", true, &out));
}

test "surrogate pairs decode to one code point" {
    // U+1F600: D83D DE00, emitted by two opcode-2 operations.
    // flags 0b10_10_0000 = 0xa0.
    try expectDecoded("\u{1f600}", "ab\x00\x00\xa0\x3d\xd8\x00\xde", true);
}
