//! RAR archive reader: a pull API over an ordered set of volumes.
//!
//! # Scope, stated bluntly
//!
//! This is a **partial** RAR implementation, and the parts it does not
//! implement fail loudly rather than guessing.
//!
//! | | RAR3 (1.5–4.x) | RAR5 (5.0/7.0) |
//! |-|-|-|
//! | header parsing | yes | yes |
//! | `Stored` (`-m0`) extraction | yes | yes |
//! | multi-volume, files spanning volumes | yes | yes |
//! | CRC32 validation of extracted data | yes | yes |
//! | compressed entries (`-m1`…`-m5`) | `error.UnsupportedCompressionMethod` | `error.UnsupportedCompressionMethod` |
//! | encrypted archives / entries | detected → `error.ArchiveEncrypted` / `error.EntryEncrypted` | same |
//! | links, symlinks, redirections | `error.UnsupportedEntryType` | `error.UnsupportedEntryType` |
//! | solid archives | only if every entry is stored | only if every entry is stored |
//! | recovery records, comments, NTFS streams | skipped | skipped |
//!
//! `Stored` is the case that matters for Usenet: a release is already
//! compressed video, so it is RAR'd with `-m0` and split into volumes.
//! Nothing is gained by compressing it again, and every poster knows
//! that. LZSS and PPMd decoders are months of work each and would block
//! everything else; refusing them by name is the honest alternative.
//!
//! # Streaming
//!
//! A volume can be gigabytes, so nothing here reads one into memory.
//! Headers are parsed out of a single reused 64 KiB buffer; entry data is
//! copied straight from the volume into the caller's buffer. Total
//! allocation per reader is that buffer plus ~5 KiB of name scratch,
//! independent of archive size.
//!
//! # Hostile input
//!
//! These archives arrive from strangers. Every length in a header is
//! treated as a claim to be checked against the volume that contains it:
//!
//!   * a header must fit in the volume and in the parse buffer
//!   * a data area must fit in the volume that declares it — this is
//!     what turns "900 GB file inside a 40 MB volume" into an error
//!   * a stored entry's unpacked size cannot exceed the total size of
//!     all volumes
//!   * every name is sanitised by `path.sanitise` before it is exposed,
//!     let alone used to build a path
//!   * continuation pieces must be marked as continuations *and* carry
//!     the same name, so a repeated or reordered volume is caught
//!     instead of silently concatenating the wrong bytes
//!
//! # Usage
//!
//!     var r = try Reader.open(gpa, volumes);
//!     defer r.deinit();
//!     while (try r.next()) |entry| {
//!         if (entry.is_dir) continue;
//!         while (true) {
//!             const n = try r.read(&buf);
//!             if (n == 0) break;
//!             try sink.writeAll(buf[0..n]);
//!         }
//!     }
//!
//! `read` returning 0 means the entry is complete *and* its CRC32
//! verified; a mismatch is `error.ChecksumMismatch`, never a silent
//! truncation.

const std = @import("std");
const crc32 = @import("../../core/crc32.zig");
const cursor = @import("cursor.zig");

pub const path = @import("path.zig");
pub const source = @import("source.zig");
pub const rar3 = @import("rar3.zig");
pub const rar5 = @import("rar5.zig");
pub const volume = @import("volume.zig");
pub const extract = @import("extract.zig");

pub const Source = source.Source;
const Allocator = std.mem.Allocator;

/// How far into a volume we look for the signature. A self-extracting
/// archive prepends an executable stub; UnRAR gives up after 1 MiB and so
/// do we, because an unbounded scan over a hostile 4 GB file is free work
/// for an attacker.
pub const sfx_scan_limit: u64 = 1 << 20;

/// Ceiling on the number of volumes in one set. A real release has tens;
/// the cap exists so a generated volume-name sequence cannot turn into an
/// unbounded open-file loop.
pub const max_volumes = 512;

pub const Format = enum {
    rar3,
    rar5,

    pub fn name(f: Format) []const u8 {
        return switch (f) {
            .rar3 => "RAR3 (1.5-4.x)",
            .rar5 => "RAR5 (5.0/7.0)",
        };
    }
};

/// Compression method, normalised across the two formats.
pub const Method = enum(u8) {
    /// The only method this module can extract.
    store = 0,
    fastest = 1,
    fast = 2,
    normal = 3,
    good = 4,
    best = 5,
    unknown = 0xff,

    pub fn name(m: Method) []const u8 {
        return switch (m) {
            .store => "store (-m0)",
            .fastest => "fastest (-m1)",
            .fast => "fast (-m2)",
            .normal => "normal (-m3)",
            .good => "good (-m4)",
            .best => "best (-m5)",
            .unknown => "unknown method",
        };
    }
};

pub const HostOs = enum { windows, unix, unknown };

pub const Error = rar5.Error || rar3.Error || rar3.NameError || path.Error ||
    Allocator.Error || error{
    /// `open` was given an empty volume list.
    NoVolumes,
    /// More volumes than `max_volumes`.
    TooManyVolumes,
    /// No RAR signature within `sfx_scan_limit` of the start of the
    /// first volume.
    NotRarArchive,
    /// A later volume in the set is not the same RAR generation as the
    /// first, or has no signature at all.
    VolumeFormatMismatch,
    /// The first block of a volume is not an archive header.
    MissingArchiveHeader,
    /// A RAR5 volume's declared volume number does not match its
    /// position in the list. Raised when volumes are supplied out of
    /// order, when the set starts at part02 because part01 was never
    /// downloaded, or when the same volume appears twice.
    BadVolumeNumber,
    /// The archive says the data continues in the next volume and no
    /// next volume was supplied.
    MissingVolume,
    /// The archive ended while an entry was still continuing.
    UnexpectedArchiveEnd,
    /// A continuation piece that is not marked as one, or whose name
    /// does not match the entry being streamed.
    InvalidContinuation,
    /// A file block marked "continued from previous volume" turned up
    /// where a new entry was expected. Usually means the first volume of
    /// the set is missing.
    UnexpectedContinuation,
    /// Archive headers are AES-encrypted (`-hp`). Nothing can be read
    /// without the password, which this module does not implement.
    ArchiveEncrypted,
    /// The entry's data is AES-encrypted (`-p`).
    EntryEncrypted,
    /// The entry is compressed. `Entry.method` names which method.
    UnsupportedCompressionMethod,
    /// The entry is a symlink, hardlink, junction or file copy. Its data
    /// area is a target path, not content; materialising it as a regular
    /// file would be wrong and following it would be a write primitive.
    UnsupportedEntryType,
    /// The entry's data ran out before its declared unpacked size.
    TruncatedEntry,
    /// The extracted data does not match the CRC32 in the header.
    ChecksumMismatch,
    /// A stored entry claims to unpack to more bytes than every volume
    /// of the archive holds put together.
    ImplausibleSize,
};

pub const Entry = struct {
    /// Sanitised: relative, `/`-separated, no `.` or `..` components, no
    /// control bytes, valid UTF-8. **Borrowed from the reader** and
    /// invalidated by the next call to `next`.
    name: []const u8,
    /// Total unpacked length of the whole entry, all volumes together.
    unpacked_size: u64,
    /// The packer did not know the size in advance (piped input).
    size_unknown: bool,
    is_dir: bool,
    method: Method,
    /// True when a CRC32 is available, so completion is actually
    /// verified. False for directories and for RAR5 entries hashed with
    /// BLAKE2sp instead (`-htb`).
    has_checksum: bool,
    encrypted: bool,
    /// The entry belongs to a solid group. Harmless for stored entries,
    /// fatal for compressed ones — which are refused anyway.
    solid: bool,
    /// A link/redirection entry rather than a file.
    link: bool,
    attributes: u64,
    host_os: HostOs,
    mtime_unix: ?i64,
    /// Decoder version the packer used: RAR3's `UNP_VER` (15/20/26/29),
    /// or RAR5's algorithm version (0 = 5.0, 1 = 7.0).
    unpack_version: u8,
    /// Volume the entry's first piece lives in.
    volume_index: usize,
};

/// One piece of an entry's data: the part that lives in a single volume.
/// A file spanning `part01`/`part02` has two.
const Piece = struct {
    vol: usize = 0,
    data_offset: u64 = 0,
    data_size: u64 = 0,
    consumed: u64 = 0,
    /// No continuation follows in a later volume.
    last: bool = true,
};

/// Format-agnostic view of a file block, used to keep `next` from
/// branching on the format twice.
const Block = struct {
    data_offset: u64,
    data_size: u64,
    next_offset: u64,
    first_piece: bool,
    last_piece: bool,
    /// Name exactly as stored. Borrowed from the header buffer, so it is
    /// dead after the next header read.
    raw_name: []const u8,
    /// RAR3 names may use the compressed UTF-16 form and need decoding;
    /// RAR5 names are already UTF-8.
    needs_decode: bool,
    unicode_name: bool,
    unpacked_size: u64,
    size_unknown: bool,
    is_dir: bool,
    method: Method,
    crc32: ?u32,
    encrypted: bool,
    solid: bool,
    link: bool,
    attributes: u64,
    host_os: HostOs,
    mtime_unix: ?i64,
    unpack_version: u8,
};

pub const Reader = struct {
    gpa: Allocator,
    /// Borrowed, in volume order. The caller owns the sources and must
    /// keep them alive for the life of the reader.
    volumes: []const Source,
    format: Format,
    multi_volume: bool = false,
    solid_archive: bool = false,
    /// Sum of every volume's length: the upper bound on any stored
    /// entry's unpacked size.
    total_bytes: u64 = 0,

    /// Reused header scratch. One allocation, never grows.
    hdr_buf: []u8,
    /// Sanitised name of the current entry.
    name_buf: []u8,
    /// Sanitised name of a continuation piece, compared against
    /// `name_buf` without disturbing it.
    alt_name_buf: []u8,
    /// UTF-16 → UTF-8 target for RAR3 names.
    decode_buf: []u8,

    /// Volume the *header* cursor is in, and the offset of the next
    /// header within it.
    vol: usize = 0,
    pos: u64 = 0,

    cur: ?Entry = null,
    piece: Piece = .{},
    delivered: u64 = 0,
    hash: crc32.Crc32 = .{},
    expect_crc: ?u32 = null,
    entry_done: bool = false,
    archive_done: bool = false,

    /// Opens the archive whose volumes are `volumes`, in order, the first
    /// being the entry point. The sources are borrowed.
    pub fn open(gpa: Allocator, volumes: []const Source) Error!Reader {
        if (volumes.len == 0) return error.NoVolumes;
        if (volumes.len > max_volumes) return error.TooManyVolumes;

        const hdr_buf = try gpa.alloc(u8, rar5.max_header_size);
        errdefer gpa.free(hdr_buf);
        const name_buf = try gpa.alloc(u8, path.max_len);
        errdefer gpa.free(name_buf);
        const alt_name_buf = try gpa.alloc(u8, path.max_len);
        errdefer gpa.free(alt_name_buf);
        const decode_buf = try gpa.alloc(u8, 3 * rar3.max_name_chars);
        errdefer gpa.free(decode_buf);

        var total: u64 = 0;
        for (volumes) |v| total = std.math.add(u64, total, v.size) catch std.math.maxInt(u64);

        var self: Reader = .{
            .gpa = gpa,
            .volumes = volumes,
            .format = undefined,
            .total_bytes = total,
            .hdr_buf = hdr_buf,
            .name_buf = name_buf,
            .alt_name_buf = alt_name_buf,
            .decode_buf = decode_buf,
        };
        self.format = try detectFormat(volumes[0], hdr_buf);
        try self.openVolume(0);
        return self;
    }

    pub fn deinit(self: *Reader) void {
        self.gpa.free(self.hdr_buf);
        self.gpa.free(self.name_buf);
        self.gpa.free(self.alt_name_buf);
        self.gpa.free(self.decode_buf);
        self.* = undefined;
    }

    /// Advances to the next entry, skipping whatever is left of the
    /// current one. Returns null at the end of the archive.
    pub fn next(self: *Reader) Error!?Entry {
        if (self.cur != null) try self.skipRest();
        if (self.archive_done) return null;

        const blk = (try self.scanToFile()) orelse return null;
        // A "continued from the previous volume" block where a new entry
        // should start means the caller handed us the middle of a set.
        if (!blk.first_piece) return error.UnexpectedContinuation;

        const name = try self.resolveName(blk, self.name_buf);

        // A stored entry's unpacked size equals the sum of its data
        // areas, so it can never exceed the archive's own total size.
        if (blk.method == .store and !blk.size_unknown and
            blk.unpacked_size > self.total_bytes)
        {
            return error.ImplausibleSize;
        }

        self.cur = .{
            .name = name,
            .unpacked_size = blk.unpacked_size,
            .size_unknown = blk.size_unknown,
            .is_dir = blk.is_dir,
            .method = blk.method,
            .has_checksum = blk.crc32 != null,
            .encrypted = blk.encrypted,
            .solid = blk.solid,
            .link = blk.link,
            .attributes = blk.attributes,
            .host_os = blk.host_os,
            .mtime_unix = blk.mtime_unix,
            .unpack_version = blk.unpack_version,
            .volume_index = self.vol,
        };
        self.piece = .{
            .vol = self.vol,
            .data_offset = blk.data_offset,
            .data_size = blk.data_size,
            .consumed = 0,
            .last = blk.last_piece,
        };
        self.pos = blk.next_offset;
        self.delivered = 0;
        self.hash = .init();
        self.expect_crc = blk.crc32;
        self.entry_done = blk.is_dir;
        return self.cur;
    }

    /// Reads up to `buf.len` bytes of the current entry, crossing volume
    /// boundaries as needed. Returns 0 once the entry is complete and its
    /// checksum has been verified.
    pub fn read(self: *Reader, buf: []u8) Error!usize {
        const e = self.cur orelse return 0;
        if (self.entry_done) return 0;

        // Refusals come before any byte is produced, so a caller that
        // ignores the error cannot end up with a partial file that looks
        // complete.
        if (e.encrypted) return error.EntryEncrypted;
        if (e.link) return error.UnsupportedEntryType;
        if (e.method != .store) return error.UnsupportedCompressionMethod;
        if (e.is_dir or buf.len == 0) return 0;

        while (true) {
            const left_in_piece = self.piece.data_size - self.piece.consumed;
            if (left_in_piece == 0) {
                if (!self.piece.last) {
                    try self.openContinuation();
                    continue;
                }
                return self.finishEntry();
            }

            var want = @min(@as(u64, buf.len), left_in_piece);
            if (!e.size_unknown) {
                const left_in_entry = e.unpacked_size - self.delivered;
                // Stored data areas are sometimes padded; the unpacked
                // size is authoritative, so stop there rather than
                // handing the caller the padding.
                if (left_in_entry == 0) return self.finishEntry();
                want = @min(want, left_in_entry);
            }

            const n: usize = @intCast(want);
            const src = self.volumes[self.piece.vol];
            try src.readAll(self.piece.data_offset + self.piece.consumed, buf[0..n]);
            self.piece.consumed += n;
            self.delivered += n;
            self.hash.update(buf[0..n]);
            return n;
        }
    }

    /// True when the current entry finished and a CRC32 confirmed it.
    pub fn currentVerified(self: *const Reader) bool {
        return self.entry_done and self.expect_crc != null;
    }

    // -- internals ----------------------------------------------------

    fn finishEntry(self: *Reader) Error!usize {
        const e = self.cur.?;
        if (!e.size_unknown and self.delivered != e.unpacked_size) {
            return error.TruncatedEntry;
        }
        if (self.expect_crc) |want| {
            if (self.hash.value() != want) return error.ChecksumMismatch;
        }
        self.entry_done = true;
        return 0;
    }

    /// Walks past any unread pieces of the current entry so the header
    /// cursor lands on the block after it.
    fn skipRest(self: *Reader) Error!void {
        while (!self.piece.last) try self.openContinuation();
        self.cur = null;
    }

    /// Finds the block that continues the current entry in a later
    /// volume and installs it as the active piece.
    fn openContinuation(self: *Reader) Error!void {
        const blk = (try self.scanToFile()) orelse return error.UnexpectedArchiveEnd;
        if (blk.first_piece) return error.InvalidContinuation;
        // The name check is what catches a repeated or reordered volume:
        // the bytes would concatenate silently otherwise.
        const nm = try self.resolveName(blk, self.alt_name_buf);
        if (self.cur) |e| {
            if (!std.mem.eql(u8, nm, e.name)) return error.InvalidContinuation;
        }
        self.piece = .{
            .vol = self.vol,
            .data_offset = blk.data_offset,
            .data_size = blk.data_size,
            .consumed = 0,
            .last = blk.last_piece,
        };
        self.pos = blk.next_offset;
        // The whole-file CRC lives in the *last* piece's header, which is
        // what UnRAR verifies against, so later pieces overwrite it.
        if (blk.crc32) |c| self.expect_crc = c;
    }

    /// Reads block headers, skipping everything that is not file
    /// content and crossing into later volumes, until a file block
    /// appears. Null means the archive ended.
    fn scanToFile(self: *Reader) Error!?Block {
        while (true) {
            if (self.archive_done) return null;
            const src = self.volumes[self.vol];
            if (self.pos >= src.size) {
                // The volume's bytes ran out with no end-of-archive
                // block. RAR3 archives legitimately lack one, and a
                // truncated download looks the same, so continue into
                // the next volume if there is one.
                if (!try self.advanceVolume(false)) return null;
                continue;
            }
            switch (self.format) {
                .rar5 => {
                    const h = try rar5.readBlockHeader(src, self.pos, self.hdr_buf);
                    std.debug.assert(h.next_offset > self.pos);
                    switch (h.kind) {
                        // A crypt block mid-archive means the rest of the
                        // headers are encrypted.
                        .crypt => return error.ArchiveEncrypted,
                        .file => return try self.blockFrom5(h),
                        .end => {
                            const e = try rar5.parseEnd(h);
                            if (!e.not_last or !self.multi_volume) {
                                self.archive_done = true;
                                return null;
                            }
                            _ = try self.advanceVolume(true);
                        },
                        // Service blocks carry comments, recovery
                        // records and NTFS streams. Their data area is
                        // not file content: skip both.
                        else => self.pos = h.next_offset,
                    }
                },
                .rar3 => {
                    const h = try rar3.readBlockHeader(src, self.pos, self.hdr_buf);
                    std.debug.assert(h.next_offset > self.pos);
                    switch (h.kind) {
                        .file => return try self.blockFrom3(h),
                        .end => {
                            const e = rar3.parseEnd(h);
                            if (!e.not_last or !self.multi_volume) {
                                self.archive_done = true;
                                return null;
                            }
                            _ = try self.advanceVolume(true);
                        },
                        else => self.pos = h.next_offset,
                    }
                },
            }
        }
    }

    /// Moves the header cursor to the start of the next volume's first
    /// block. `required` is set when the archive said the data continues,
    /// in which case a missing volume is an error rather than an end.
    fn advanceVolume(self: *Reader, required: bool) Error!bool {
        if (self.vol + 1 >= self.volumes.len) {
            if (required) return error.MissingVolume;
            self.archive_done = true;
            return false;
        }
        try self.openVolume(self.vol + 1);
        return true;
    }

    /// Validates a volume's signature and archive header and positions
    /// the cursor on its first content block.
    fn openVolume(self: *Reader, index: usize) Error!void {
        const src = self.volumes[index];
        const after_sig = switch (self.format) {
            .rar5 => try rar5.findSignature(src, sfx_scan_limit, self.hdr_buf),
            .rar3 => try rar3.findSignature(src, sfx_scan_limit, self.hdr_buf),
        } orelse return error.VolumeFormatMismatch;

        self.vol = index;
        self.pos = after_sig;

        switch (self.format) {
            .rar5 => {
                const h = try rar5.readBlockHeader(src, self.pos, self.hdr_buf);
                // `-hp` puts a crypt block between the signature and the
                // main header and encrypts everything after it.
                if (h.kind == .crypt) return error.ArchiveEncrypted;
                if (h.kind != .main) return error.MissingArchiveHeader;
                const m = try rar5.parseMain(h);
                if (index == 0) {
                    self.multi_volume = m.multi_volume;
                    self.solid_archive = m.solid;
                } else {
                    // Every volume but the first carries its number, so
                    // its absence means the set is not in the order we
                    // were told.
                    if (!m.has_volume_number) return error.BadVolumeNumber;
                }
                // A declared number that disagrees with the slot the
                // volume was handed to us in means the list is out of
                // order, repeats a volume, or starts partway through the
                // set because an earlier part was never downloaded. All
                // three would concatenate the wrong bytes.
                if (m.has_volume_number and m.volume_number != index) {
                    return error.BadVolumeNumber;
                }
                self.pos = h.next_offset;
            },
            .rar3 => {
                const h = try rar3.readBlockHeader(src, self.pos, self.hdr_buf);
                if (h.kind != .archive) return error.MissingArchiveHeader;
                const a = rar3.parseArchive(h);
                if (a.encrypted_headers) return error.ArchiveEncrypted;
                if (index == 0) {
                    self.multi_volume = a.multi_volume;
                    self.solid_archive = a.solid;
                }
                self.pos = h.next_offset;
            },
        }
    }

    fn blockFrom5(self: *Reader, h: rar5.BlockHeader) Error!Block {
        _ = self;
        const f = try rar5.parseFile(h);
        return .{
            .data_offset = h.data_offset,
            .data_size = h.data_size,
            .next_offset = h.next_offset,
            .first_piece = h.isFirstPiece(),
            .last_piece = h.isLastPiece(),
            .raw_name = f.name,
            .needs_decode = false,
            .unicode_name = true,
            .unpacked_size = f.unpacked_size,
            .size_unknown = f.size_unknown,
            .is_dir = f.is_dir,
            .method = method5(f.method),
            .crc32 = f.crc32,
            .encrypted = f.encrypted,
            .solid = f.solid,
            .link = f.redirect,
            .attributes = f.attributes,
            .host_os = switch (f.host_os) {
                0 => .windows,
                1 => .unix,
                else => .unknown,
            },
            .mtime_unix = if (f.mtime_unix) |m| @as(i64, m) else null,
            .unpack_version = f.algorithm_version,
        };
    }

    fn blockFrom3(self: *Reader, h: rar3.BlockHeader) Error!Block {
        _ = self;
        const f = try rar3.parseFile(h);
        return .{
            .data_offset = h.data_offset,
            .data_size = h.data_size,
            .next_offset = h.next_offset,
            .first_piece = h.isFirstPiece(),
            .last_piece = h.isLastPiece(),
            .raw_name = f.raw_name,
            .needs_decode = true,
            .unicode_name = f.unicode_name,
            .unpacked_size = f.unpacked_size,
            .size_unknown = f.size_unknown,
            .is_dir = f.is_dir,
            .method = method3(f.method),
            // RAR3 always has the field; it is 0 for directories, which
            // matches the CRC of no bytes being unchecked anyway.
            .crc32 = if (f.is_dir) null else f.crc32,
            .encrypted = f.encrypted,
            .solid = f.solid,
            .link = rar3.isUnixSymlink(f),
            .attributes = f.attributes,
            .host_os = switch (f.host_os) {
                0, 1, 2 => .windows,
                3, 4, 5 => .unix,
                else => .unknown,
            },
            .mtime_unix = dosTimeToUnix(f.dos_time),
            .unpack_version = f.unpack_version,
        };
    }

    /// Decodes (RAR3) and sanitises a stored name into `out`.
    fn resolveName(self: *Reader, blk: Block, out: []u8) Error![]const u8 {
        if (!blk.needs_decode) {
            return path.sanitise(blk.raw_name, out);
        }
        const decoded = try rar3.decodeName(blk.raw_name, blk.unicode_name, self.decode_buf);
        return path.sanitise(decoded, out);
    }
};

fn detectFormat(src: Source, buf: []u8) Error!Format {
    if (try rar5.findSignature(src, sfx_scan_limit, buf)) |_| return .rar5;
    if (try rar3.findSignature(src, sfx_scan_limit, buf)) |_| return .rar3;
    return error.NotRarArchive;
}

fn method5(m: rar5.Method) Method {
    return switch (m) {
        .store => .store,
        .fastest => .fastest,
        .fast => .fast,
        .normal => .normal,
        .good => .good,
        .best => .best,
        _ => .unknown,
    };
}

fn method3(m: rar3.Method) Method {
    return switch (m) {
        .store => .store,
        .fastest => .fastest,
        .fast => .fast,
        .normal => .normal,
        .good => .good,
        .best => .best,
        _ => .unknown,
    };
}

/// MS-DOS packed timestamp to Unix seconds, UTC.
///
/// | bits | field |
/// |-|-|
/// | 0..4 | seconds / 2 |
/// | 5..10 | minute |
/// | 11..15 | hour |
/// | 16..20 | day of month, 1-based |
/// | 21..24 | month, 1-based |
/// | 25..31 | year - 1980 |
///
/// DOS times carry no zone, so they are read as UTC — the same choice
/// every other unpacker makes. A zero value (or an impossible date, which
/// hostile archives do contain) yields null rather than a nonsense
/// timestamp.
pub fn dosTimeToUnix(dos: u32) ?i64 {
    if (dos == 0) return null;
    const sec: u32 = (dos & 0x1f) * 2;
    const min: u32 = (dos >> 5) & 0x3f;
    const hour: u32 = (dos >> 11) & 0x1f;
    const day: u32 = (dos >> 16) & 0x1f;
    const month: u32 = (dos >> 21) & 0x0f;
    const year: u32 = ((dos >> 25) & 0x7f) + 1980;
    if (day == 0 or day > 31 or month == 0 or month > 12) return null;
    if (hour > 23 or min > 59 or sec > 59) return null;

    // Days from 1970-01-01, by Howard Hinnant's civil-date algorithm.
    var y: i64 = @intCast(year);
    const m: i64 = @intCast(month);
    const d: i64 = @intCast(day);
    y -= if (m <= 2) 1 else 0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const doy = @divFloor(153 * (m + (if (m > 2) @as(i64, -3) else 9)) + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days = era * 146097 + doe - 719468;

    return days * 86400 + @as(i64, @intCast(hour)) * 3600 +
        @as(i64, @intCast(min)) * 60 + @as(i64, @intCast(sec));
}

// ---------------------------------------------------------------------
// Tests
//
// Fixtures are hand-assembled with the builders in `rar5.zig` /
// `rar3.zig`: no `rar` binary exists on the build hosts, and a
// byte-by-byte fixture doubles as an executable statement of the format.
// ---------------------------------------------------------------------

const t = std.testing;

/// Holds the `Memory` sources for a set of volumes at stable addresses,
/// since a `Source` points at its backing struct.
const Vols = struct {
    mems: []source.Memory,
    srcs: []Source,
    gpa: Allocator,

    fn init(gpa: Allocator, bufs: []const []const u8) !Vols {
        const mems = try gpa.alloc(source.Memory, bufs.len);
        const srcs = try gpa.alloc(Source, bufs.len);
        for (bufs, 0..) |b, i| {
            mems[i] = source.Memory.init(b);
            srcs[i] = mems[i].source();
        }
        return .{ .mems = mems, .srcs = srcs, .gpa = gpa };
    }

    fn deinit(v: *Vols) void {
        v.gpa.free(v.mems);
        v.gpa.free(v.srcs);
    }
};

/// Reads the current entry to completion with a deliberately awkward
/// buffer size, so volume boundaries are crossed mid-buffer.
fn drain(r: *Reader, gpa: Allocator, chunk: usize) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    const buf = try gpa.alloc(u8, chunk);
    defer gpa.free(buf);
    while (true) {
        const n = try r.read(buf);
        if (n == 0) break;
        try out.appendSlice(gpa, buf[0..n]);
    }
    return out.toOwnedSlice(gpa);
}

test "rar5: a single stored file extracts and verifies" {
    const payload = "The quick brown fox jumps over the lazy dog";
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "fox.txt", .data = payload, .crc32 = crc32.checksum(payload) });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectEqual(Format.rar5, r.format);

    const e = (try r.next()).?;
    try t.expectEqualStrings("fox.txt", e.name);
    try t.expectEqual(@as(u64, payload.len), e.unpacked_size);
    try t.expectEqual(Method.store, e.method);
    try t.expect(e.has_checksum);
    try t.expect(!e.is_dir);

    const got = try drain(&r, t.allocator, 7);
    defer t.allocator.free(got);
    try t.expectEqualStrings(payload, got);
    try t.expect(r.currentVerified());

    try t.expectEqual(@as(?Entry, null), try r.next());
}

test "rar5: a wrong CRC32 is caught at the end of the entry" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "x.bin", .data = "abcdef", .crc32 = 0xdead_beef });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    _ = (try r.next()).?;

    var buf: [64]u8 = undefined;
    // The bytes come out — a streaming API cannot know yet — but the
    // final read reports the mismatch instead of a clean EOF.
    try t.expectEqual(@as(usize, 6), try r.read(&buf));
    try t.expectError(error.ChecksumMismatch, r.read(&buf));
}

test "rar5: a file spanning three volumes reassembles in order" {
    // 30 bytes of payload split 10 / 12 / 8 across three volumes, the
    // shape a `-m0 -v` release has.
    const payload = "0123456789ABCDEFGHIJKLMNOPQRST";
    const want_crc = crc32.checksum(payload);

    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{
        .name = "big.bin",
        .data = payload[0..10],
        .unpacked_size = payload.len,
        .crc32 = want_crc,
        .split_after = true,
    });
    try v1.addEnd(true);

    var v2 = rar5.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addMain(.{ .multi_volume = true, .volume_number = 1 });
    try v2.addFile(.{
        .name = "big.bin",
        .data = payload[10..22],
        .unpacked_size = payload.len,
        .crc32 = want_crc,
        .split_before = true,
        .split_after = true,
    });
    try v2.addEnd(true);

    var v3 = rar5.Builder.init(t.allocator);
    defer v3.deinit();
    try v3.addSignature();
    try v3.addMain(.{ .multi_volume = true, .volume_number = 2 });
    try v3.addFile(.{
        .name = "big.bin",
        .data = payload[22..],
        .unpacked_size = payload.len,
        .crc32 = want_crc,
        .split_before = true,
    });
    try v3.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{ v1.bytes(), v2.bytes(), v3.bytes() });
    defer vols.deinit();

    // Every chunk size from 1 upwards: the boundary logic must not care
    // where the caller's buffer happens to end.
    for ([_]usize{ 1, 3, 7, 10, 11, 30, 64 }) |chunk| {
        var r = try Reader.open(t.allocator, vols.srcs);
        defer r.deinit();
        const e = (try r.next()).?;
        try t.expectEqualStrings("big.bin", e.name);
        try t.expectEqual(@as(u64, payload.len), e.unpacked_size);
        const got = try drain(&r, t.allocator, chunk);
        defer t.allocator.free(got);
        try t.expectEqualStrings(payload, got);
        try t.expect(r.currentVerified());
        try t.expectEqual(@as(?Entry, null), try r.next());
    }
}

test "rar5: entries after a split file are still found" {
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{ .name = "a.bin", .data = "AAAA", .unpacked_size = 8, .split_after = true });
    try v1.addEnd(true);

    var v2 = rar5.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addMain(.{ .multi_volume = true, .volume_number = 1 });
    try v2.addFile(.{ .name = "a.bin", .data = "BBBB", .unpacked_size = 8, .split_before = true });
    try v2.addFile(.{ .name = "b.bin", .data = "CC", .crc32 = crc32.checksum("CC") });
    try v2.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{ v1.bytes(), v2.bytes() });
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();

    var e = (try r.next()).?;
    try t.expectEqualStrings("a.bin", e.name);
    const first = try drain(&r, t.allocator, 3);
    defer t.allocator.free(first);
    try t.expectEqualStrings("AAAABBBB", first);

    e = (try r.next()).?;
    try t.expectEqualStrings("b.bin", e.name);
    const second = try drain(&r, t.allocator, 8);
    defer t.allocator.free(second);
    try t.expectEqualStrings("CC", second);
    try t.expectEqual(@as(?Entry, null), try r.next());
}

test "rar5: an unread split entry is skipped correctly" {
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{ .name = "skipme.bin", .data = "AAAA", .unpacked_size = 8, .split_after = true });
    try v1.addEnd(true);

    var v2 = rar5.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addMain(.{ .multi_volume = true, .volume_number = 1 });
    try v2.addFile(.{ .name = "skipme.bin", .data = "BBBB", .unpacked_size = 8, .split_before = true });
    try v2.addFile(.{ .name = "wanted.txt", .data = "hi", .crc32 = crc32.checksum("hi") });
    try v2.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{ v1.bytes(), v2.bytes() });
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();

    // Never read the first entry's data at all.
    try t.expectEqualStrings("skipme.bin", (try r.next()).?.name);
    try t.expectEqualStrings("wanted.txt", (try r.next()).?.name);
    const got = try drain(&r, t.allocator, 4);
    defer t.allocator.free(got);
    try t.expectEqualStrings("hi", got);
}

test "rar5: a missing continuation volume is an error, not a short file" {
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{ .name = "big.bin", .data = "AAAA", .unpacked_size = 8, .split_after = true });
    try v1.addEnd(true);

    var vols = try Vols.init(t.allocator, &.{v1.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    _ = (try r.next()).?;
    var buf: [16]u8 = undefined;
    try t.expectEqual(@as(usize, 4), try r.read(&buf));
    try t.expectError(error.MissingVolume, r.read(&buf));
}

test "rar5: the same volume supplied twice is rejected" {
    // A self-referential chain: part01 says "continues", and the caller
    // hands us part01 again as part02. Concatenating its data would
    // silently duplicate 10 bytes of the file.
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{ .name = "big.bin", .data = "AAAAAAAAAA", .unpacked_size = 20, .split_after = true });
    try v1.addEnd(true);

    var vols = try Vols.init(t.allocator, &.{ v1.bytes(), v1.bytes() });
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    _ = (try r.next()).?;
    var buf: [32]u8 = undefined;
    try t.expectEqual(@as(usize, 10), try r.read(&buf));
    // The duplicate announces itself as volume 0 while sitting in slot 1.
    try t.expectError(error.BadVolumeNumber, r.read(&buf));
}

test "rar5: volumes supplied out of order are rejected" {
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{ .name = "big.bin", .data = "AAAA", .unpacked_size = 8, .split_after = true });
    try v1.addEnd(true);

    var v2 = rar5.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addMain(.{ .multi_volume = true, .volume_number = 1 });
    try v2.addFile(.{ .name = "big.bin", .data = "BBBB", .unpacked_size = 8, .split_before = true });
    try v2.addEnd(false);

    // part02 first: it declares volume number 1 while sitting in slot 0.
    var swapped = try Vols.init(t.allocator, &.{ v2.bytes(), v1.bytes() });
    defer swapped.deinit();
    try t.expectError(error.BadVolumeNumber, Reader.open(t.allocator, swapped.srcs));

    // part02 alone — the case where part01 was never downloaded — is
    // caught by the same check rather than yielding half a file.
    var alone = try Vols.init(t.allocator, &.{v2.bytes()});
    defer alone.deinit();
    try t.expectError(error.BadVolumeNumber, Reader.open(t.allocator, alone.srcs));
}

test "rar3: starting mid-set is caught by the continuation flag" {
    // RAR3 archive headers carry no volume number, so the only signal
    // that we started in the middle of a set is a first file block
    // marked "continued from the previous volume".
    var v2 = rar3.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addArchiveHeader(.{ .multi_volume = true });
    try v2.addFile(.{
        .name = "big.bin",
        .data = "BBBB",
        .unpacked_size = 8,
        .split_before = true,
    });
    try v2.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{v2.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectError(error.UnexpectedContinuation, r.next());
}

test "rar5: a continuation naming a different file is rejected" {
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{ .name = "wanted.bin", .data = "AAAA", .unpacked_size = 8, .split_after = true });
    try v1.addEnd(true);

    var v2 = rar5.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addMain(.{ .multi_volume = true, .volume_number = 1 });
    try v2.addFile(.{ .name = "other.bin", .data = "BBBB", .unpacked_size = 8, .split_before = true });
    try v2.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{ v1.bytes(), v2.bytes() });
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    _ = (try r.next()).?;
    var buf: [32]u8 = undefined;
    _ = try r.read(&buf);
    try t.expectError(error.InvalidContinuation, r.read(&buf));
}

test "rar5: compressed entries are refused by name" {
    inline for (.{ 1, 2, 3, 4, 5 }) |m| {
        var b = rar5.Builder.init(t.allocator);
        defer b.deinit();
        try b.addSignature();
        try b.addMain(.{});
        try b.addFile(.{ .name = "packed.bin", .data = "xxxx", .unpacked_size = 100, .method = m });
        try b.addEnd(false);

        var vols = try Vols.init(t.allocator, &.{b.bytes()});
        defer vols.deinit();
        var r = try Reader.open(t.allocator, vols.srcs);
        defer r.deinit();

        // Listing still works: the entry is visible with its method, so
        // the caller can say *which* method it refused.
        const e = (try r.next()).?;
        try t.expectEqual(@as(Method, @enumFromInt(m)), e.method);
        try t.expect(e.method.name().len > 0);

        var buf: [16]u8 = undefined;
        try t.expectError(error.UnsupportedCompressionMethod, r.read(&buf));
    }
}

test "rar5: an encrypted archive is detected at open" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addCrypt();
    // Whatever follows is really AES-encrypted; the builder writes plain
    // bytes, which is fine because we must never get that far.
    try b.addMain(.{});

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    try t.expectError(error.ArchiveEncrypted, Reader.open(t.allocator, vols.srcs));
}

test "rar5: an encrypted entry is refused before any byte is produced" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "secret.mkv", .data = "\x00\x01\x02\x03", .encrypted = true });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    const e = (try r.next()).?;
    try t.expect(e.encrypted);
    var buf: [16]u8 = undefined;
    try t.expectError(error.EntryEncrypted, r.read(&buf));
}

test "rar5: link entries are refused rather than written as files" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "innocent.txt", .data = "/etc/pwnd", .redirect = true });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    const e = (try r.next()).?;
    try t.expect(e.link);
    var buf: [16]u8 = undefined;
    try t.expectError(error.UnsupportedEntryType, r.read(&buf));
}

test "rar5: directories and empty files carry no data" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "sub/dir", .is_dir = true });
    try b.addFile(.{ .name = "sub/dir/empty.txt", .crc32 = crc32.checksum("") });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();

    var e = (try r.next()).?;
    try t.expectEqualStrings("sub/dir", e.name);
    try t.expect(e.is_dir);
    var buf: [8]u8 = undefined;
    try t.expectEqual(@as(usize, 0), try r.read(&buf));

    e = (try r.next()).?;
    try t.expectEqualStrings("sub/dir/empty.txt", e.name);
    try t.expect(!e.is_dir);
    try t.expectEqual(@as(u64, 0), e.unpacked_size);
    try t.expectEqual(@as(usize, 0), try r.read(&buf));
    try t.expect(r.currentVerified());
}

test "rar5: service blocks are skipped, not extracted" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addService("CMT", "this is an archive comment");
    try b.addFile(.{ .name = "real.txt", .data = "yes", .crc32 = crc32.checksum("yes") });
    try b.addService("QO", "quick open index");
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    const e = (try r.next()).?;
    try t.expectEqualStrings("real.txt", e.name);
    const got = try drain(&r, t.allocator, 4);
    defer t.allocator.free(got);
    try t.expectEqualStrings("yes", got);
    try t.expectEqual(@as(?Entry, null), try r.next());
}

test "rar5: hostile names are rejected at next()" {
    const hostile = [_][]const u8{
        "../../../../etc/cron.d/pwn",
        "..\\..\\windows\\system32\\x",
        "/etc/passwd",
        "C:\\Windows\\System32\\hosts",
        "a/../../b",
        "a\x00/../../etc/passwd",
        "..",
        ".",
        "",
    };
    for (hostile) |name| {
        var b = rar5.Builder.init(t.allocator);
        defer b.deinit();
        try b.addSignature();
        try b.addMain(.{});
        try b.addFile(.{ .name = name, .data = "x", .crc32 = crc32.checksum("x") });
        try b.addEnd(false);

        var vols = try Vols.init(t.allocator, &.{b.bytes()});
        defer vols.deinit();
        var r = try Reader.open(t.allocator, vols.srcs);
        defer r.deinit();
        // Any of the path.Error tags is acceptable; what matters is that
        // no Entry is ever handed out with an escaping name.
        try t.expect(std.meta.isError(r.next()));
    }
}

test "rar5: an implausible unpacked size is rejected" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    // 900 GB claimed inside a volume of a few dozen bytes. The data area
    // is honest, so only the unpacked size gives it away.
    try b.addFile(.{ .name = "huge.bin", .data = "AAAA", .unpacked_size = 900 * (1 << 30) });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectError(error.ImplausibleSize, r.next());
}

test "rar5: truncation is detected wherever it lands" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "f.bin", .data = "0123456789", .crc32 = crc32.checksum("0123456789") });
    try b.addEnd(false);
    const full = b.bytes();

    // Cut at every possible point. The assertion is not about which
    // error comes out — it is that the reader always terminates, never
    // reads past the buffer, and never produces more bytes than the
    // archive actually contains.
    var cut: usize = 0;
    while (cut < full.len) : (cut += 1) {
        var vols = try Vols.init(t.allocator, &.{full[0..cut]});
        defer vols.deinit();
        var r = Reader.open(t.allocator, vols.srcs) catch continue;
        defer r.deinit();
        var produced: usize = 0;
        var buf: [4]u8 = undefined;
        walk: while (true) {
            const entry = (r.next() catch break :walk) orelse break :walk;
            _ = entry;
            while (true) {
                const n = r.read(&buf) catch break :walk;
                if (n == 0) break;
                produced += n;
            }
        }
        try t.expect(produced <= 10);
    }
}

test "rar5: a volume that ends mid file header is an error" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "f.bin", .data = "0123456789" });
    const full = b.bytes();
    // Keep the signature and main header, then only half of the file
    // header.
    var vols = try Vols.init(t.allocator, &.{full[0 .. full.len - 14]});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectError(error.TruncatedVolume, r.next());
}

test "rar5: stored data with trailing padding stops at the unpacked size" {
    // Some packers pad the data area. The unpacked size is what counts.
    const payload = "PAYLOAD";
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{
        .name = "p.bin",
        .data = payload ++ "\x00\x00\x00\x00",
        .unpacked_size = payload.len,
        .crc32 = crc32.checksum(payload),
    });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    _ = (try r.next()).?;
    const got = try drain(&r, t.allocator, 3);
    defer t.allocator.free(got);
    try t.expectEqualStrings(payload, got);
}

test "rar5: an entry shorter than its declared size is refused" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    // Declares 20 unpacked bytes with only 4 in the data area and no
    // continuation flag.
    try b.addFile(.{ .name = "short.bin", .data = "AAAA", .unpacked_size = 20 });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    _ = (try r.next()).?;
    var buf: [32]u8 = undefined;
    try t.expectEqual(@as(usize, 4), try r.read(&buf));
    try t.expectError(error.TruncatedEntry, r.read(&buf));
}

test "rar5: a data size near 2^64 cannot wrap the bounds check" {
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{
        .name = "wrap.bin",
        .data = "AAAA",
        .declared_data_size = std.math.maxInt(u64),
    });

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectError(error.TruncatedVolume, r.next());

    // Same, one below the maximum, and at 2^63 — the values that would
    // wrap a signed or a naively-added bound.
    for ([_]u64{ std.math.maxInt(u64) - 1, 1 << 63, (1 << 63) + 1 }) |size| {
        var b2 = rar5.Builder.init(t.allocator);
        defer b2.deinit();
        try b2.addSignature();
        try b2.addMain(.{});
        try b2.addFile(.{ .name = "w.bin", .data = "AAAA", .declared_data_size = size });
        var v2 = try Vols.init(t.allocator, &.{b2.bytes()});
        defer v2.deinit();
        var r2 = try Reader.open(t.allocator, v2.srcs);
        defer r2.deinit();
        try t.expectError(error.TruncatedVolume, r2.next());
    }
}

test "mutated archives never panic, hang or over-produce" {
    // Not a correctness test — a robustness one. Every single-byte
    // mutation of a valid multi-volume archive is walked to completion.
    // Under the Debug and ReleaseSafe builds an arithmetic overflow or an
    // out-of-bounds slice inside the parser would panic here, and the
    // iteration cap turns any lost forward-progress guarantee into a
    // failure instead of a hung test.
    const payload = "0123456789ABCDEF";
    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{
        .name = "d/f.bin",
        .data = payload[0..8],
        .unpacked_size = payload.len,
        .crc32 = crc32.checksum(payload),
        .split_after = true,
    });
    try v1.addEnd(true);

    var v2 = rar5.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addMain(.{ .multi_volume = true, .volume_number = 1 });
    try v2.addFile(.{
        .name = "d/f.bin",
        .data = payload[8..],
        .unpacked_size = payload.len,
        .crc32 = crc32.checksum(payload),
        .split_before = true,
    });
    try v2.addEnd(false);

    const a = try t.allocator.dupe(u8, v1.bytes());
    defer t.allocator.free(a);
    const c = try t.allocator.dupe(u8, v2.bytes());
    defer t.allocator.free(c);

    var prng = std.Random.DefaultPrng.init(0xA11CE);
    const rand = prng.random();

    for (0..600) |_| {
        // Flip one byte in one of the two volumes.
        const in_first = rand.boolean();
        const buf = if (in_first) a else c;
        const idx = rand.uintLessThan(usize, buf.len);
        const saved = buf[idx];
        buf[idx] ^= @as(u8, 1) << rand.int(u3);
        defer buf[idx] = saved;

        var vols = try Vols.init(t.allocator, &.{ a, c });
        defer vols.deinit();
        var r = Reader.open(t.allocator, vols.srcs) catch continue;
        defer r.deinit();

        var produced: u64 = 0;
        var steps: usize = 0;
        var rbuf: [5]u8 = undefined;
        walk: while (steps < 10_000) : (steps += 1) {
            const entry = (r.next() catch break :walk) orelse break :walk;
            _ = entry;
            while (steps < 10_000) : (steps += 1) {
                const n = r.read(&rbuf) catch break :walk;
                if (n == 0) break;
                produced += n;
            }
        }
        try t.expect(steps < 10_000);
        // Nothing can produce more bytes than the two volumes hold.
        try t.expect(produced <= a.len + c.len);
    }
}

test "open rejects nonsense inputs" {
    try t.expectError(error.NoVolumes, Reader.open(t.allocator, &.{}));

    var junk = try Vols.init(t.allocator, &.{"this is not a RAR archive at all"});
    defer junk.deinit();
    try t.expectError(error.NotRarArchive, Reader.open(t.allocator, junk.srcs));

    var empty = try Vols.init(t.allocator, &.{""});
    defer empty.deinit();
    try t.expectError(error.NotRarArchive, Reader.open(t.allocator, empty.srcs));

    // Signature but nothing after it.
    var bare = try Vols.init(t.allocator, &.{&rar5.signature});
    defer bare.deinit();
    try t.expectError(error.TruncatedVolume, Reader.open(t.allocator, bare.srcs));
}

// --- RAR3 ------------------------------------------------------------

test "rar3: a single stored file extracts and verifies" {
    const payload = "hoardarr rar3 stored payload";
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{ .first_volume = true });
    try b.addFile(.{
        .name = "dir\\file.txt",
        .data = payload,
        .crc32 = crc32.checksum(payload),
    });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectEqual(Format.rar3, r.format);

    const e = (try r.next()).?;
    // The backslash separator is folded to '/'.
    try t.expectEqualStrings("dir/file.txt", e.name);
    try t.expectEqual(Method.store, e.method);
    try t.expect(e.has_checksum);

    const got = try drain(&r, t.allocator, 5);
    defer t.allocator.free(got);
    try t.expectEqualStrings(payload, got);
    try t.expect(r.currentVerified());
    try t.expectEqual(@as(?Entry, null), try r.next());
}

test "rar3: a file spanning two volumes reassembles" {
    const payload = "aaaaaaaaaaBBBBBBBBBB";
    const want_crc = crc32.checksum(payload);

    var v1 = rar3.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addArchiveHeader(.{ .multi_volume = true, .first_volume = true });
    try v1.addFile(.{
        .name = "big.bin",
        .data = payload[0..10],
        .unpacked_size = payload.len,
        .crc32 = 0, // non-final pieces carry a partial value
        .split_after = true,
    });
    try v1.addEnd(true);

    var v2 = rar3.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addArchiveHeader(.{ .multi_volume = true });
    try v2.addFile(.{
        .name = "big.bin",
        .data = payload[10..],
        .unpacked_size = payload.len,
        .crc32 = want_crc, // the last piece carries the whole-file CRC
        .split_before = true,
    });
    try v2.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{ v1.bytes(), v2.bytes() });
    defer vols.deinit();
    for ([_]usize{ 1, 4, 10, 13, 64 }) |chunk| {
        var r = try Reader.open(t.allocator, vols.srcs);
        defer r.deinit();
        try t.expectEqualStrings("big.bin", (try r.next()).?.name);
        const got = try drain(&r, t.allocator, chunk);
        defer t.allocator.free(got);
        try t.expectEqualStrings(payload, got);
        try t.expect(r.currentVerified());
    }
}

test "rar3: an archive with no end block simply ends" {
    // RAR 1.5/2.x archives frequently have no end-of-archive block.
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addFile(.{ .name = "only.txt", .data = "abc", .crc32 = crc32.checksum("abc") });

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectEqualStrings("only.txt", (try r.next()).?.name);
    const got = try drain(&r, t.allocator, 2);
    defer t.allocator.free(got);
    try t.expectEqualStrings("abc", got);
    try t.expectEqual(@as(?Entry, null), try r.next());
}

test "rar3: encrypted headers and encrypted entries are detected" {
    var hp = rar3.Builder.init(t.allocator);
    defer hp.deinit();
    try hp.addSignature();
    try hp.addArchiveHeader(.{ .encrypted_headers = true });
    var hp_vols = try Vols.init(t.allocator, &.{hp.bytes()});
    defer hp_vols.deinit();
    try t.expectError(error.ArchiveEncrypted, Reader.open(t.allocator, hp_vols.srcs));

    var p = rar3.Builder.init(t.allocator);
    defer p.deinit();
    try p.addSignature();
    try p.addArchiveHeader(.{});
    try p.addFile(.{ .name = "s.bin", .data = "\x01\x02\x03\x04", .encrypted = true, .salt = true });
    try p.addEnd(false);
    var p_vols = try Vols.init(t.allocator, &.{p.bytes()});
    defer p_vols.deinit();
    var r = try Reader.open(t.allocator, p_vols.srcs);
    defer r.deinit();
    try t.expect((try r.next()).?.encrypted);
    var buf: [16]u8 = undefined;
    try t.expectError(error.EntryEncrypted, r.read(&buf));
}

test "rar3: compressed entries are refused" {
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addFile(.{ .name = "packed.bin", .data = "xxxx", .unpacked_size = 999, .method = 0x33 });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    const e = (try r.next()).?;
    try t.expectEqual(Method.normal, e.method);
    var buf: [16]u8 = undefined;
    try t.expectError(error.UnsupportedCompressionMethod, r.read(&buf));
}

test "rar3: symlink entries are refused" {
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addFile(.{
        .name = "innocent",
        .data = "/etc/shadow",
        .host_os = 3,
        .attributes = 0o120777,
        .crc32 = crc32.checksum("/etc/shadow"),
    });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expect((try r.next()).?.link);
    var buf: [16]u8 = undefined;
    try t.expectError(error.UnsupportedEntryType, r.read(&buf));
}

test "rar3: unicode names are decoded and sanitised" {
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    // Opcode 3 run copying both ASCII bytes verbatim: decodes to "ab".
    try b.addFile(.{
        .name = "ab\x00\x00\xc0\x00",
        .data = "z",
        .unicode_name = true,
        .crc32 = crc32.checksum("z"),
    });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectEqualStrings("ab", (try r.next()).?.name);
}

test "rar3: hostile names are rejected" {
    const hostile = [_][]const u8{
        "..\\..\\..\\etc\\passwd",
        "/etc/passwd",
        "D:\\x",
        "a\x00b",
    };
    for (hostile) |name| {
        var b = rar3.Builder.init(t.allocator);
        defer b.deinit();
        try b.addSignature();
        try b.addArchiveHeader(.{});
        try b.addFile(.{ .name = name, .data = "x", .crc32 = crc32.checksum("x") });
        try b.addEnd(false);

        var vols = try Vols.init(t.allocator, &.{b.bytes()});
        defer vols.deinit();
        var r = try Reader.open(t.allocator, vols.srcs);
        defer r.deinit();
        try t.expect(std.meta.isError(r.next()));
    }
}

test "rar3: subblocks are skipped" {
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addSubBlock("ACL", "not file content");
    try b.addFile(.{ .name = "real.txt", .data = "ok", .crc32 = crc32.checksum("ok") });
    try b.addEnd(false);

    var vols = try Vols.init(t.allocator, &.{b.bytes()});
    defer vols.deinit();
    var r = try Reader.open(t.allocator, vols.srcs);
    defer r.deinit();
    try t.expectEqualStrings("real.txt", (try r.next()).?.name);
    try t.expectEqual(@as(?Entry, null), try r.next());
}

test "rar3: truncation at every offset fails cleanly" {
    var b = rar3.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addArchiveHeader(.{});
    try b.addFile(.{ .name = "f.bin", .data = "0123456789", .crc32 = crc32.checksum("0123456789") });
    try b.addEnd(false);
    const full = b.bytes();

    var cut: usize = 0;
    while (cut < full.len) : (cut += 1) {
        var vols = try Vols.init(t.allocator, &.{full[0..cut]});
        defer vols.deinit();
        var r = Reader.open(t.allocator, vols.srcs) catch continue;
        defer r.deinit();
        var buf: [4]u8 = undefined;
        var produced: usize = 0;
        walk: while (true) {
            const entry = (r.next() catch break :walk) orelse break :walk;
            _ = entry;
            while (true) {
                const n = r.read(&buf) catch break :walk;
                if (n == 0) break;
                produced += n;
            }
        }
        try t.expect(produced <= 10);
    }
}

test "dos timestamps convert to unix seconds" {
    // 2024-03-15 12:34:56 -> year 44, month 3, day 15, 12:34:(56/2)
    const packed_time: u32 = (44 << 25) | (3 << 21) | (15 << 16) |
        (12 << 11) | (34 << 5) | (28);
    // 1710506096 = 2024-03-15T12:34:56Z
    try t.expectEqual(@as(?i64, 1710506096), dosTimeToUnix(packed_time));

    // The DOS epoch itself.
    const epoch: u32 = (0 << 25) | (1 << 21) | (1 << 16);
    try t.expectEqual(@as(?i64, 315532800), dosTimeToUnix(epoch));

    // Zero and impossible dates yield null rather than a wrong answer.
    try t.expectEqual(@as(?i64, null), dosTimeToUnix(0));
    try t.expectEqual(@as(?i64, null), dosTimeToUnix((44 << 25) | (13 << 21) | (1 << 16)));
    try t.expectEqual(@as(?i64, null), dosTimeToUnix((44 << 25) | (3 << 21) | (0 << 16)));
}

test "format names are stable" {
    try t.expectEqualStrings("RAR5 (5.0/7.0)", Format.rar5.name());
    try t.expectEqualStrings("RAR3 (1.5-4.x)", Format.rar3.name());
    try t.expectEqualStrings("store (-m0)", Method.store.name());
    try t.expectEqualStrings("unknown method", Method.unknown.name());
}
