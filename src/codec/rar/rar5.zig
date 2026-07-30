//! RAR5 (RAR 5.0 / 7.0 archive format) header parsing.
//!
//! RAR has no published specification. This is written against the
//! `technote.txt` shipped with the UnRAR source and cross-checked field
//! by field against `nwaples/rardecode`, which the Go implementation
//! used. The comments below *are* the spec for this repository, so every
//! offset, flag bit and magic number is spelled out.
//!
//! # File layout
//!
//! An archive volume is:
//!
//!     [SFX stub]  optional, up to 1 MiB, searched for the signature
//!     signature   8 bytes: "Rar!\x1a\x07\x01\x00"
//!     block       main archive header (type 1), or a crypt header first
//!     block …     file / service / end headers, each optionally
//!                 followed by a data area
//!
//! # Block layout
//!
//! Every block, including the main header, is:
//!
//! | offset | size | field |
//! |-|-|-|
//! | 0 | 4 | CRC32 of `header size` … end of extra area, LE |
//! | 4 | vint | header size: `header type` … end of extra area |
//! | — | vint | header type (1 main, 2 file, 3 service, 4 crypt, 5 end) |
//! | — | vint | header flags |
//! | — | vint | extra area size — only if flags & 0x0001 |
//! | — | vint | data area size — only if flags & 0x0002 |
//! | — | … | type-specific fields |
//! | — | … | extra area |
//! | — | … | data area, `data area size` bytes, outside the header |
//!
//! Note what the CRC covers: it starts at the *header size* vint, not
//! after it. So the CRC'd span is `len(size vint) + size` bytes. That is
//! easy to get wrong by one field and it is the first thing to check
//! when a hand-built fixture fails to verify.
//!
//! Two independent size limits apply. `technote.txt` says the header
//! size vint "must not be longer than 3 bytes in current
//! implementation", capping a header at 2 MB. We additionally refuse
//! anything larger than `max_header_size` (64 KiB), because a legitimate
//! header is a few hundred bytes: names are capped at 1 KiB, archive
//! comments live in a service block's *data* area rather than its
//! header, and the extra area holds a handful of small records. That cap
//! is what makes it safe to parse headers into one reused buffer instead
//! of allocating per header.

const std = @import("std");
const crc32 = @import("../../core/crc32.zig");
const cursor = @import("cursor.zig");
const source = @import("source.zig");

const Cursor = cursor.Cursor;
const Source = source.Source;

/// "Rar!\x1a\x07" followed by version byte 1 and a NUL. The RAR3
/// signature is the same six bytes followed by a single 0 byte, which is
/// why the version byte has to be inspected to tell them apart.
pub const signature = [8]u8{ 'R', 'a', 'r', '!', 0x1a, 0x07, 0x01, 0x00 };

/// Largest header we will parse. See the module comment.
pub const max_header_size = 64 * 1024;

/// The header size vint is limited to 3 bytes by the format itself.
pub const max_size_vint_len = 3;

pub const BlockType = enum(u64) {
    main = 1,
    file = 2,
    /// Carries archive comments, NTFS streams, recovery records and the
    /// quick-open index. Skipped: its data area is not file content.
    service = 3,
    /// Present only when the archive was made with `-hp`, i.e. the
    /// headers themselves are AES-encrypted. Detected, never decrypted.
    crypt = 4,
    end = 5,
    _,
};

/// Flags common to every block header.
pub const block_flag = struct {
    /// Extra area is present, and its size vint precedes the
    /// type-specific fields.
    pub const has_extra: u64 = 0x0001;
    /// Data area is present, and its size vint precedes the
    /// type-specific fields.
    pub const has_data: u64 = 0x0002;
    /// Unknown block types carrying this must be preserved when
    /// updating. Irrelevant to a reader.
    pub const skip_if_unknown: u64 = 0x0004;
    /// The data area continues a data area from the previous volume.
    /// Set on the second and later pieces of a split file.
    pub const data_not_first: u64 = 0x0008;
    /// The data area continues into the next volume. Set on every piece
    /// of a split file except the last.
    pub const data_not_last: u64 = 0x0010;
    /// This block depends on the preceding one.
    pub const depends_on_prev: u64 = 0x0020;
    pub const preserve_child: u64 = 0x0040;
};

/// Main archive header flags.
pub const archive_flag = struct {
    /// The archive is one volume of a multi-volume set.
    pub const volume: u64 = 0x0001;
    /// The volume number field follows. Set on every volume except the
    /// first, so its absence means volume 0.
    pub const volume_number: u64 = 0x0002;
    /// Solid archive: files share a compression window, so entries
    /// cannot be decoded independently.
    pub const solid: u64 = 0x0004;
    pub const recovery: u64 = 0x0008;
    pub const locked: u64 = 0x0010;
};

/// File / service header flags.
pub const file_flag = struct {
    pub const directory: u64 = 0x0001;
    /// A 4-byte Unix mtime follows the attributes field.
    pub const has_unix_mtime: u64 = 0x0002;
    /// A 4-byte CRC32 of the unpacked file follows.
    pub const has_crc32: u64 = 0x0004;
    /// Unpacked size is unknown (streamed input); the field is still
    /// present but meaningless.
    pub const unp_size_unknown: u64 = 0x0008;
};

/// End-of-archive header flags.
pub const end_flag = struct {
    /// This volume is not the last of the set: the reader must continue
    /// in the next volume file.
    pub const not_last: u64 = 0x0001;
};

/// Extra-area record types inside a file or service header.
pub const extra_type = struct {
    /// AES parameters for an encrypted entry (`-p`). Detected only.
    pub const encryption: u64 = 1;
    /// BLAKE2sp hash of the unpacked file, replacing the CRC32.
    pub const hash: u64 = 2;
    /// High-precision / multiple timestamps.
    pub const time: u64 = 3;
    pub const version: u64 = 4;
    /// Symlink, hardlink, junction or file copy. The entry's data area
    /// is the *target path*, not file content, so materialising it as a
    /// regular file would be wrong — and a symlink out of the target
    /// directory is a write primitive.
    pub const redirection: u64 = 5;
    pub const unix_owner: u64 = 6;
    pub const service_data: u64 = 7;
};

/// Compression method, from bits 7..9 of the compression-info vint.
pub const Method = enum(u3) {
    /// `-m0`. The data area is the file, byte for byte. The only method
    /// this module can extract.
    store = 0,
    fastest = 1,
    fast = 2,
    normal = 3,
    good = 4,
    best = 5,
    _,

    pub fn name(m: Method) []const u8 {
        return switch (m) {
            .store => "store (-m0)",
            .fastest => "fastest (-m1)",
            .fast => "fast (-m2)",
            .normal => "normal (-m3)",
            .good => "good (-m4)",
            .best => "best (-m5)",
            _ => "reserved",
        };
    }
};

pub const Error = cursor.Error || source.Error || error{
    /// The header's own CRC32 did not match. The header is corrupt, and
    /// nothing in it may be trusted — including its size, which is why
    /// we cannot resynchronise past it.
    BadHeaderCrc,
    /// A header size vint longer than 3 bytes, or a header larger than
    /// `max_header_size`.
    HeaderTooLarge,
    /// A block header, or a data area, that runs past the end of the
    /// volume file. Either the download is incomplete or a length field
    /// lied about how much data follows.
    TruncatedVolume,
    /// The extra area size, data area size or name length inside a
    /// header does not fit the header.
    CorruptHeader,
};

/// A parsed block header. `body` and `extra` borrow from the caller's
/// header buffer and are invalidated by the next `readBlockHeader`.
pub const BlockHeader = struct {
    kind: BlockType,
    flags: u64,
    /// Type-specific fields, with the common prefix already consumed.
    body: []const u8,
    /// Raw extra area; walk it with `ExtraIterator`.
    extra: []const u8,
    /// Absolute offset of the data area within the volume.
    data_offset: u64,
    /// Declared data area length. Already checked to fit in the volume.
    data_size: u64,
    /// Absolute offset of the byte after the data area — where the next
    /// block header starts.
    next_offset: u64,

    pub fn hasFlag(h: BlockHeader, f: u64) bool {
        return h.flags & f != 0;
    }

    /// First piece of a file: no data continuing from a previous volume.
    pub fn isFirstPiece(h: BlockHeader) bool {
        return !h.hasFlag(block_flag.data_not_first);
    }

    /// Last piece of a file: nothing continues into the next volume.
    pub fn isLastPiece(h: BlockHeader) bool {
        return !h.hasFlag(block_flag.data_not_last);
    }
};

/// Locates the RAR5 signature. Returns the offset of the first byte
/// *after* it, or null when this is not a RAR5 volume.
///
/// The signature is not always at offset 0: a self-extracting archive
/// prepends an executable. UnRAR scans the first 1 MiB, and so do we —
/// bounded, because otherwise a 4 GB file of near-misses is a free scan.
pub fn findSignature(src: Source, scan_limit: u64, buf: []u8) source.Error!?u64 {
    return findSignatureBytes(src, scan_limit, buf, &signature);
}

/// Shared by `findSignature` and the RAR3 equivalent: both signatures
/// start with the same 6 bytes and differ only in the tail.
pub fn findSignatureBytes(src: Source, scan_limit: u64, buf: []u8, sig: []const u8) source.Error!?u64 {
    std.debug.assert(buf.len >= 2 * sig.len);
    var base: u64 = 0;
    while (base <= scan_limit and base < src.size) {
        const n = try src.readAt(base, buf);
        if (n < sig.len) return null;
        if (std.mem.indexOf(u8, buf[0..n], sig)) |hit| {
            const abs = base + hit;
            if (abs > scan_limit) return null;
            return abs + sig.len;
        }
        // A short read means we reached the end of the volume.
        if (n < buf.len) return null;
        // Overlap the next window by sig.len-1 so a signature straddling
        // the window boundary is still found.
        base += n - (sig.len - 1);
    }
    return null;
}

/// Reads and verifies the block header at `offset`. `buf` is the reused
/// header scratch buffer; it must be at least 64 bytes and at most
/// `max_header_size` is ever used from it.
pub fn readBlockHeader(src: Source, offset: u64, buf: []u8) Error!BlockHeader {
    std.debug.assert(buf.len >= 64);
    if (offset >= src.size) return error.TruncatedVolume;

    // One read covers the header in every real archive; the buffer is
    // clamped to what is left in the volume so a short volume shows up
    // as EndOfHeader rather than a bogus success.
    const avail = src.size - offset;
    const want: usize = @intCast(@min(@as(u64, buf.len), avail));
    const got = try src.readAt(offset, buf[0..want]);
    const raw = buf[0..got];
    // 4-byte CRC + at least a 1-byte size + type + flags.
    if (raw.len < 7) return error.TruncatedVolume;

    const want_crc = std.mem.readInt(u32, raw[0..4], .little);

    var head = Cursor.init(raw[4..]);
    const size_vint_len = try head.peekVintLen();
    if (size_vint_len > max_size_vint_len) return error.HeaderTooLarge;
    const size = try head.vint();
    if (size > max_header_size) return error.HeaderTooLarge;
    if (size == 0) return error.CorruptHeader;

    // The CRC covers the size vint plus `size` bytes after it.
    const crc_len = size_vint_len + size;
    if (4 + crc_len > raw.len) {
        // Distinguish "we didn't buffer enough" (impossible, since size
        // <= max_header_size <= buf.len) from "the volume ends here".
        return error.TruncatedVolume;
    }
    const crc_span = raw[4..][0..@intCast(crc_len)];
    if (crc32.checksum(crc_span) != want_crc) return error.BadHeaderCrc;

    // Everything below reads inside the CRC-verified span only.
    var body = Cursor.init(crc_span[size_vint_len..]);
    const kind: BlockType = @enumFromInt(try body.vint());
    const flags = try body.vint();
    const extra_size = if (flags & block_flag.has_extra != 0) try body.vint() else 0;
    const data_size = if (flags & block_flag.has_data != 0) try body.vint() else 0;

    if (extra_size > body.remaining()) return error.CorruptHeader;
    const type_fields_len = body.remaining() - @as(usize, @intCast(extra_size));
    const type_fields = try body.take(type_fields_len);
    const extra = try body.take(@intCast(extra_size));

    const header_end = offset + 4 + crc_len;
    // The data area must fit in this volume. This is the check that
    // makes a header claiming a 900 GB payload inside a 40 MB volume an
    // error instead of a very long wait.
    if (data_size > src.size - header_end) return error.TruncatedVolume;

    return .{
        .kind = kind,
        .flags = flags,
        .body = type_fields,
        .extra = extra,
        .data_offset = header_end,
        .data_size = data_size,
        .next_offset = header_end + data_size,
    };
}

/// Main archive header (type 1).
pub const MainHeader = struct {
    multi_volume: bool,
    solid: bool,
    /// 0-based, from the header when present. Absent means volume 0.
    volume_number: u64,
    has_volume_number: bool,
};

pub fn parseMain(h: BlockHeader) Error!MainHeader {
    var c = Cursor.init(h.body);
    const flags = try c.vint();
    const has_num = flags & archive_flag.volume_number != 0;
    return .{
        .multi_volume = flags & archive_flag.volume != 0,
        .solid = flags & archive_flag.solid != 0,
        .volume_number = if (has_num) try c.vint() else 0,
        .has_volume_number = has_num,
    };
}

/// End-of-archive header (type 5).
pub fn parseEnd(h: BlockHeader) Error!struct { not_last: bool } {
    var c = Cursor.init(h.body);
    const flags = try c.vint();
    return .{ .not_last = flags & end_flag.not_last != 0 };
}

/// File header (type 2) or service header (type 3).
pub const FileHeader = struct {
    is_dir: bool,
    /// Total unpacked length of the whole file, not of this piece.
    /// Meaningless when `size_unknown`.
    unpacked_size: u64,
    size_unknown: bool,
    attributes: u64,
    mtime_unix: ?u32,
    /// CRC32 of the unpacked file. On a split file the value in the
    /// *last* piece's header is the one that covers the whole file,
    /// which is what UnRAR and rardecode both verify against.
    crc32: ?u32,
    method: Method,
    /// 0 for the RAR 5.0 algorithm, 1 for RAR 7.0.
    algorithm_version: u6,
    solid: bool,
    /// Minimum dictionary size the decompressor would need, in bytes.
    /// Informational here — nothing in this module decompresses.
    dictionary_size: u64,
    /// 0 Windows, 1 Unix.
    host_os: u64,
    /// Raw name bytes, borrowed from the header buffer. Declared UTF-8
    /// by the format; validate before use.
    name: []const u8,
    /// An encryption record was present: the data area is AES-encrypted.
    encrypted: bool,
    /// A redirection record was present: this entry is a link, and its
    /// data area is a target path rather than file content.
    redirect: bool,
    /// A BLAKE2sp hash record replaced the CRC32.
    blake2_hash: bool,
};

pub fn parseFile(h: BlockHeader) Error!FileHeader {
    var c = Cursor.init(h.body);

    // Field order, all vint unless noted:
    //   file flags, unpacked size, attributes,
    //   [mtime u32 if flags & 0x0002], [data CRC32 u32 if flags & 0x0004],
    //   compression info, host OS, name length, name.
    const flags = try c.vint();
    const unpacked_size = try c.vint();
    const attributes = try c.vint();
    const mtime = if (flags & file_flag.has_unix_mtime != 0) try c.u32le() else null;
    const data_crc = if (flags & file_flag.has_crc32 != 0) try c.u32le() else null;

    // Compression info bit layout:
    //   0..5   algorithm version (0 = RAR 5.0, 1 = RAR 7.0)
    //   6      solid: reuses the previous file's window
    //   7..9   method (0 = store, 1..5 = fastest..best)
    //   10..14 dictionary size: 128 KiB << value
    //   15..19 dictionary size fraction (RAR 7.0)
    //   20     RAR 5.0 compatibility
    const comp_info = try c.vint();
    const algorithm: u6 = @truncate(comp_info & 0x3f);
    const method: Method = @enumFromInt(@as(u3, @truncate((comp_info >> 7) & 0x7)));
    // 0x20000 << n, saturating: the shift count is 5 bits, so this
    // cannot overflow u64 (0x20000 << 31 fits).
    const dict_size: u64 = @as(u64, 0x20000) << @intCast((comp_info >> 10) & 0x1f);

    const host_os = try c.vint();
    const name_len = try c.vint();
    if (name_len > c.remaining()) return error.CorruptHeader;
    const name = try c.take(@intCast(name_len));

    var out: FileHeader = .{
        .is_dir = flags & file_flag.directory != 0,
        .unpacked_size = unpacked_size,
        .size_unknown = flags & file_flag.unp_size_unknown != 0,
        .attributes = attributes,
        .mtime_unix = mtime,
        .crc32 = data_crc,
        .method = method,
        .algorithm_version = algorithm,
        .solid = comp_info & 0x40 != 0,
        .dictionary_size = dict_size,
        .host_os = host_os,
        .name = name,
        .encrypted = false,
        .redirect = false,
        .blake2_hash = false,
    };

    // Extra area: a sequence of length-prefixed records. We only need to
    // know *which* records are present — anything we cannot honour turns
    // into a refusal upstream rather than a silently wrong extraction.
    var it = ExtraIterator{ .rest = h.extra };
    while (try it.next()) |rec| switch (rec.kind) {
        extra_type.encryption => out.encrypted = true,
        extra_type.hash => out.blake2_hash = true,
        extra_type.redirection => out.redirect = true,
        else => {},
    };
    return out;
}

/// Walks the extra area. Each record is `size vint` (covering the type
/// vint and the record data) followed by that many bytes.
pub const ExtraIterator = struct {
    rest: []const u8,

    pub const Record = struct {
        kind: u64,
        /// Record payload after the type vint. Borrowed.
        data: []const u8,
    };

    pub fn next(it: *ExtraIterator) Error!?Record {
        if (it.rest.len == 0) return null;
        var c = Cursor.init(it.rest);
        const size = try c.vint();
        if (size > c.remaining()) return error.CorruptHeader;
        // A zero-size record would never terminate the walk.
        if (size == 0) return error.CorruptHeader;
        const rec = try c.take(@intCast(size));
        it.rest = c.rest();

        var rc = Cursor.init(rec);
        const kind = try rc.vint();
        return .{ .kind = kind, .data = rc.rest() };
    }
};

// ---------------------------------------------------------------------
// Fixture builder
//
// No `rar` binary exists on the build hosts (7z can read RAR but not
// write it), so the fixtures are assembled byte by byte. That is the
// better outcome for a reverse-engineered format anyway: the builder is
// an executable restatement of the layout documented above, and a
// mistake in it shows up as a CRC mismatch rather than as a mystery.
//
// It lives in the implementation file, not in the tests, because
// `rar.zig` needs it too.
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

    fn appendVint(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: u64) !void {
        var tmp: [cursor.max_vint_len]u8 = undefined;
        try list.appendSlice(gpa, cursor.encodeVint(v, &tmp));
    }

    /// Emits one block. `body` is the type-specific field area, `extra`
    /// the extra area, `data` the data area. Computes the header size
    /// and CRC exactly as the format defines them.
    pub fn addBlock(
        b: *Builder,
        kind: u64,
        flags: u64,
        body: []const u8,
        extra: []const u8,
        data: []const u8,
    ) !void {
        var eff_flags = flags;
        if (extra.len != 0) eff_flags |= block_flag.has_extra;
        if (data.len != 0) eff_flags |= block_flag.has_data;

        // Inner span: type … end of extra area. This is what the header
        // size counts.
        var inner: std.ArrayList(u8) = .empty;
        defer inner.deinit(b.gpa);
        try appendVint(&inner, b.gpa, kind);
        try appendVint(&inner, b.gpa, eff_flags);
        if (eff_flags & block_flag.has_extra != 0) try appendVint(&inner, b.gpa, extra.len);
        if (eff_flags & block_flag.has_data != 0) try appendVint(&inner, b.gpa, data.len);
        try inner.appendSlice(b.gpa, body);
        try inner.appendSlice(b.gpa, extra);

        var size_vint: [cursor.max_vint_len]u8 = undefined;
        const sv = cursor.encodeVint(inner.items.len, &size_vint);

        var c = crc32.Crc32.init();
        c.update(sv);
        c.update(inner.items);

        var crc_le: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_le, c.value(), .little);
        try b.raw(&crc_le);
        try b.raw(sv);
        try b.raw(inner.items);
        try b.raw(data);
    }

    /// Same as `addBlock` but with an explicitly wrong header CRC, for
    /// the corruption tests.
    pub fn addBlockBadCrc(b: *Builder, kind: u64, flags: u64, body: []const u8) !void {
        const before = b.buf.items.len;
        try b.addBlock(kind, flags, body, &.{}, &.{});
        b.buf.items[before] ^= 0xff;
    }

    pub fn addMain(b: *Builder, opts: struct {
        multi_volume: bool = false,
        solid: bool = false,
        volume_number: ?u64 = null,
    }) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(b.gpa);
        var flags: u64 = 0;
        if (opts.multi_volume) flags |= archive_flag.volume;
        if (opts.solid) flags |= archive_flag.solid;
        if (opts.volume_number != null) flags |= archive_flag.volume_number;
        try appendVint(&body, b.gpa, flags);
        if (opts.volume_number) |n| try appendVint(&body, b.gpa, n);
        try b.addBlock(@intFromEnum(BlockType.main), 0, body.items, &.{}, &.{});
    }

    pub const FileOpts = struct {
        name: []const u8,
        data: []const u8 = &.{},
        /// Declared unpacked size. Defaults to `data.len`; override to
        /// build a header that lies.
        unpacked_size: ?u64 = null,
        /// Declared data area size. Defaults to `data.len`; override to
        /// build a header that lies.
        declared_data_size: ?u64 = null,
        crc32: ?u32 = null,
        is_dir: bool = false,
        method: u3 = 0,
        split_before: bool = false,
        split_after: bool = false,
        mtime: ?u32 = null,
        attributes: u64 = 0,
        host_os: u64 = 1,
        encrypted: bool = false,
        redirect: bool = false,
        solid: bool = false,
    };

    pub fn addFile(b: *Builder, opts: FileOpts) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(b.gpa);

        var fflags: u64 = 0;
        if (opts.is_dir) fflags |= file_flag.directory;
        if (opts.mtime != null) fflags |= file_flag.has_unix_mtime;
        if (opts.crc32 != null) fflags |= file_flag.has_crc32;
        try appendVint(&body, b.gpa, fflags);
        try appendVint(&body, b.gpa, opts.unpacked_size orelse opts.data.len);
        try appendVint(&body, b.gpa, opts.attributes);
        if (opts.mtime) |m| {
            var le: [4]u8 = undefined;
            std.mem.writeInt(u32, &le, m, .little);
            try body.appendSlice(b.gpa, &le);
        }
        if (opts.crc32) |v| {
            var le: [4]u8 = undefined;
            std.mem.writeInt(u32, &le, v, .little);
            try body.appendSlice(b.gpa, &le);
        }
        var comp: u64 = @as(u64, opts.method) << 7;
        if (opts.solid) comp |= 0x40;
        try appendVint(&body, b.gpa, comp);
        try appendVint(&body, b.gpa, opts.host_os);
        try appendVint(&body, b.gpa, opts.name.len);
        try body.appendSlice(b.gpa, opts.name);

        var extra: std.ArrayList(u8) = .empty;
        defer extra.deinit(b.gpa);
        if (opts.encrypted) {
            // version 0, flags 0, kdf count, 16-byte salt, 16-byte IV.
            var rec: std.ArrayList(u8) = .empty;
            defer rec.deinit(b.gpa);
            try appendVint(&rec, b.gpa, extra_type.encryption);
            try appendVint(&rec, b.gpa, 0);
            try appendVint(&rec, b.gpa, 0);
            try rec.append(b.gpa, 15);
            try rec.appendNTimes(b.gpa, 0xAA, 32);
            try appendVint(&extra, b.gpa, rec.items.len);
            try extra.appendSlice(b.gpa, rec.items);
        }
        if (opts.redirect) {
            var rec: std.ArrayList(u8) = .empty;
            defer rec.deinit(b.gpa);
            try appendVint(&rec, b.gpa, extra_type.redirection);
            try appendVint(&rec, b.gpa, 1); // unix symlink
            try appendVint(&rec, b.gpa, 0); // flags
            try appendVint(&rec, b.gpa, 9);
            try rec.appendSlice(b.gpa, "/etc/pwnd");
            try appendVint(&extra, b.gpa, rec.items.len);
            try extra.appendSlice(b.gpa, rec.items);
        }

        var bflags: u64 = 0;
        if (opts.split_before) bflags |= block_flag.data_not_first;
        if (opts.split_after) bflags |= block_flag.data_not_last;
        if (opts.declared_data_size) |n| {
            // Force the data-size field on even when it disagrees with
            // the bytes actually appended.
            bflags |= block_flag.has_data;
            try b.addBlockDeclared(
                @intFromEnum(BlockType.file),
                bflags,
                body.items,
                extra.items,
                n,
                opts.data,
            );
            return;
        }
        try b.addBlock(@intFromEnum(BlockType.file), bflags, body.items, extra.items, opts.data);
    }

    /// `addBlock` with the declared data size decoupled from the bytes
    /// written, for building deliberately inconsistent fixtures.
    pub fn addBlockDeclared(
        b: *Builder,
        kind: u64,
        flags: u64,
        body: []const u8,
        extra: []const u8,
        declared_data_size: u64,
        data: []const u8,
    ) !void {
        var eff_flags = flags | block_flag.has_data;
        if (extra.len != 0) eff_flags |= block_flag.has_extra;

        var inner: std.ArrayList(u8) = .empty;
        defer inner.deinit(b.gpa);
        try appendVint(&inner, b.gpa, kind);
        try appendVint(&inner, b.gpa, eff_flags);
        if (eff_flags & block_flag.has_extra != 0) try appendVint(&inner, b.gpa, extra.len);
        try appendVint(&inner, b.gpa, declared_data_size);
        try inner.appendSlice(b.gpa, body);
        try inner.appendSlice(b.gpa, extra);

        var size_vint: [cursor.max_vint_len]u8 = undefined;
        const sv = cursor.encodeVint(inner.items.len, &size_vint);
        var c = crc32.Crc32.init();
        c.update(sv);
        c.update(inner.items);
        var crc_le: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_le, c.value(), .little);
        try b.raw(&crc_le);
        try b.raw(sv);
        try b.raw(inner.items);
        try b.raw(data);
    }

    pub fn addEnd(b: *Builder, not_last: bool) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(b.gpa);
        try appendVint(&body, b.gpa, if (not_last) end_flag.not_last else 0);
        try b.addBlock(@intFromEnum(BlockType.end), 0, body.items, &.{}, &.{});
    }

    /// A `-hp` archive: the crypt block announces that every header
    /// after it is AES-encrypted.
    pub fn addCrypt(b: *Builder) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(b.gpa);
        try appendVint(&body, b.gpa, 0); // version
        try appendVint(&body, b.gpa, 1); // password check present
        try body.append(b.gpa, 15); // kdf count
        try body.appendNTimes(b.gpa, 0x11, 16); // salt
        try body.appendNTimes(b.gpa, 0x22, 12); // check value
        try b.addBlock(@intFromEnum(BlockType.crypt), 0, body.items, &.{}, &.{});
    }

    /// A service block, e.g. the `CMT` archive comment. Its data area
    /// must be skipped rather than treated as file content.
    pub fn addService(b: *Builder, name: []const u8, data: []const u8) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(b.gpa);
        try appendVint(&body, b.gpa, 0); // file flags
        try appendVint(&body, b.gpa, data.len); // unpacked size
        try appendVint(&body, b.gpa, 0); // attributes
        try appendVint(&body, b.gpa, 0); // compression info: store
        try appendVint(&body, b.gpa, 0); // host OS
        try appendVint(&body, b.gpa, name.len);
        try body.appendSlice(b.gpa, name);
        try b.addBlock(@intFromEnum(BlockType.service), 0, body.items, &.{}, data);
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "signature is found at offset zero and after an SFX stub" {
    var buf: [64]u8 = undefined;

    var plain = source.Memory.init(&signature);
    try t.expectEqual(@as(?u64, 8), try findSignature(plain.source(), 1 << 20, &buf));

    var stub_bytes: [40]u8 = @splat('M');
    @memcpy(stub_bytes[16..24], &signature);
    var stub = source.Memory.init(&stub_bytes);
    try t.expectEqual(@as(?u64, 24), try findSignature(stub.source(), 1 << 20, &buf));

    // RAR3's signature must not be mistaken for RAR5's: the two differ
    // only in the byte after "Rar!\x1a\x07".
    var rar3 = source.Memory.init(&[_]u8{ 'R', 'a', 'r', '!', 0x1a, 0x07, 0x00 });
    try t.expectEqual(@as(?u64, null), try findSignature(rar3.source(), 1 << 20, &buf));

    var nothing = source.Memory.init("not an archive at all");
    try t.expectEqual(@as(?u64, null), try findSignature(nothing.source(), 1 << 20, &buf));

    // Truncated signature.
    var short = source.Memory.init(signature[0..7]);
    try t.expectEqual(@as(?u64, null), try findSignature(short.source(), 1 << 20, &buf));

    // The scan limit is honoured: the signature exists but past it.
    try t.expectEqual(@as(?u64, null), try findSignature(stub.source(), 8, &buf));
}

test "block header round-trips through the builder" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{ .multi_volume = true, .volume_number = 3, .solid = true });

    var mem = source.Memory.init(b.bytes());
    var hdr_buf: [1024]u8 = undefined;
    const h = try readBlockHeader(mem.source(), signature.len, &hdr_buf);
    try t.expectEqual(BlockType.main, h.kind);
    try t.expectEqual(@as(u64, 0), h.data_size);

    const main = try parseMain(h);
    try t.expect(main.multi_volume);
    try t.expect(main.solid);
    try t.expect(main.has_volume_number);
    try t.expectEqual(@as(u64, 3), main.volume_number);
}

test "file header fields survive a round trip" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{
        .name = "dir/movie.mkv",
        .data = "hello world",
        .crc32 = crc32.checksum("hello world"),
        .mtime = 0x6000_0000,
        .attributes = 0o644,
        .host_os = 1,
    });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    const main = try readBlockHeader(src, signature.len, &hdr_buf);
    const h = try readBlockHeader(src, main.next_offset, &hdr_buf);
    try t.expectEqual(BlockType.file, h.kind);
    try t.expectEqual(@as(u64, 11), h.data_size);
    try t.expect(h.isFirstPiece());
    try t.expect(h.isLastPiece());

    const f = try parseFile(h);
    try t.expectEqualStrings("dir/movie.mkv", f.name);
    try t.expectEqual(@as(u64, 11), f.unpacked_size);
    try t.expectEqual(Method.store, f.method);
    try t.expectEqual(@as(?u32, crc32.checksum("hello world")), f.crc32);
    try t.expectEqual(@as(?u32, 0x6000_0000), f.mtime_unix);
    try t.expect(!f.is_dir);
    try t.expect(!f.encrypted);
    try t.expect(!f.redirect);
    try t.expectEqual(@as(u64, 1), f.host_os);

    // The data area sits immediately after the header.
    var data: [11]u8 = undefined;
    try src.readAll(h.data_offset, &data);
    try t.expectEqualStrings("hello world", &data);
}

test "a corrupt header CRC is refused" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addBlockBadCrc(@intFromEnum(BlockType.main), 0, &.{0});

    var mem = source.Memory.init(b.bytes());
    var hdr_buf: [1024]u8 = undefined;
    try t.expectError(error.BadHeaderCrc, readBlockHeader(mem.source(), signature.len, &hdr_buf));
}

test "a header size vint over three bytes is refused" {
    // 4 bytes of CRC then a 4-byte size vint.
    const bad = [_]u8{ 0, 0, 0, 0, 0x80, 0x80, 0x80, 0x01, 0, 0, 0 };
    var mem = source.Memory.init(&bad);
    var hdr_buf: [1024]u8 = undefined;
    try t.expectError(error.HeaderTooLarge, readBlockHeader(mem.source(), 0, &hdr_buf));
}

test "a header claiming more bytes than the volume holds is refused" {
    // Declared header size of 0x4000 with only a few bytes present.
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(t.allocator);
    try raw.appendSlice(t.allocator, &[_]u8{ 0, 0, 0, 0 });
    var tmp: [cursor.max_vint_len]u8 = undefined;
    try raw.appendSlice(t.allocator, cursor.encodeVint(0x4000, &tmp));
    try raw.appendSlice(t.allocator, &[_]u8{ 1, 0 });

    var mem = source.Memory.init(raw.items);
    var hdr_buf: [max_header_size]u8 = undefined;
    try t.expectError(error.TruncatedVolume, readBlockHeader(mem.source(), 0, &hdr_buf));
}

test "a header larger than the parse buffer is refused, not truncated" {
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(t.allocator);
    try raw.appendSlice(t.allocator, &[_]u8{ 0, 0, 0, 0 });
    var tmp: [cursor.max_vint_len]u8 = undefined;
    // 3-byte vint, but larger than max_header_size.
    try raw.appendSlice(t.allocator, cursor.encodeVint(max_header_size + 1, &tmp));
    try raw.appendNTimes(t.allocator, 0, 64);

    var mem = source.Memory.init(raw.items);
    var hdr_buf: [4096]u8 = undefined;
    try t.expectError(error.HeaderTooLarge, readBlockHeader(mem.source(), 0, &hdr_buf));
}

test "a data area running past the end of the volume is refused" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    // Claims 900 GB of payload; the volume holds a few dozen bytes.
    try b.addFile(.{
        .name = "huge.bin",
        .data = "short",
        .declared_data_size = 900 * (1 << 30),
    });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    const main = try readBlockHeader(src, signature.len, &hdr_buf);
    try t.expectError(error.TruncatedVolume, readBlockHeader(src, main.next_offset, &hdr_buf));
}

test "a name length past the end of the header is refused" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(t.allocator);
    var tmp: [cursor.max_vint_len]u8 = undefined;
    for ([_]u64{ 0, 0, 0, 0, 1, 4000 }) |v| {
        try body.appendSlice(t.allocator, cursor.encodeVint(v, &tmp));
    }
    try body.appendSlice(t.allocator, "abc"); // 3 bytes, 4000 claimed
    try b.addBlock(@intFromEnum(BlockType.file), 0, body.items, &.{}, &.{});

    var mem = source.Memory.init(b.bytes());
    var hdr_buf: [4096]u8 = undefined;
    const h = try readBlockHeader(mem.source(), 0, &hdr_buf);
    try t.expectError(error.CorruptHeader, parseFile(h));
}

test "extra records are detected: encryption and redirection" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "secret.mkv", .data = "xxxx", .encrypted = true });
    try b.addFile(.{ .name = "link", .data = "/etc/pwnd", .redirect = true });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    var h = try readBlockHeader(src, signature.len, &hdr_buf);
    h = try readBlockHeader(src, h.next_offset, &hdr_buf);
    const enc = try parseFile(h);
    try t.expect(enc.encrypted);
    try t.expect(!enc.redirect);

    h = try readBlockHeader(src, h.next_offset, &hdr_buf);
    const link = try parseFile(h);
    try t.expect(link.redirect);
    try t.expect(!link.encrypted);
}

test "a zero-length extra record cannot stall the walk" {
    var it = ExtraIterator{ .rest = &[_]u8{0} };
    try t.expectError(error.CorruptHeader, it.next());

    // Record size larger than the extra area.
    var over = ExtraIterator{ .rest = &[_]u8{ 9, 1 } };
    try t.expectError(error.CorruptHeader, over.next());
}

test "compression info decodes method, version and dictionary size" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    inline for (.{ 0, 1, 2, 3, 4, 5 }) |m| {
        try b.addFile(.{ .name = "f", .data = "abc", .method = m });
    }

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    var h = try readBlockHeader(src, signature.len, &hdr_buf);
    inline for (.{ 0, 1, 2, 3, 4, 5 }) |m| {
        h = try readBlockHeader(src, h.next_offset, &hdr_buf);
        const f = try parseFile(h);
        try t.expectEqual(@as(Method, @enumFromInt(m)), f.method);
        try t.expectEqual(@as(u6, 0), f.algorithm_version);
        try t.expectEqual(@as(u64, 0x20000), f.dictionary_size);
    }
    try t.expectEqualStrings("store (-m0)", Method.store.name());
    try t.expectEqualStrings("best (-m5)", Method.best.name());
}

test "end-of-archive flags say whether another volume follows" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addEnd(true);
    try b.addEnd(false);

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [1024]u8 = undefined;
    var h = try readBlockHeader(src, signature.len, &hdr_buf);
    try t.expectEqual(BlockType.end, h.kind);
    try t.expect((try parseEnd(h)).not_last);
    h = try readBlockHeader(src, h.next_offset, &hdr_buf);
    try t.expect(!(try parseEnd(h)).not_last);
}

test "split flags mark the pieces of a file that spans volumes" {
    var b = Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{ .multi_volume = true });
    try b.addFile(.{ .name = "big.bin", .data = "AAAA", .split_after = true });

    var mem = source.Memory.init(b.bytes());
    const src = mem.source();
    var hdr_buf: [4096]u8 = undefined;
    var h = try readBlockHeader(src, signature.len, &hdr_buf);
    h = try readBlockHeader(src, h.next_offset, &hdr_buf);
    try t.expect(h.isFirstPiece());
    try t.expect(!h.isLastPiece());
}
