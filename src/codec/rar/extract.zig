//! Filesystem extraction: the `extract.Extractor` port surface.
//!
//! Mirrors what the Go adapter did — take the archive paths a job
//! collected, unpack into a target directory, return the relative paths
//! written — with the same volume-picking rules, so a job that worked
//! before works now.
//!
//! Three differences from the Go version, all deliberate:
//!
//!   * Names are *rejected* rather than cleaned. Go ran
//!     `filepath.Clean` and then checked the result stayed under the
//!     target; see `path.zig` for why rejecting is the safer contract.
//!   * Entries are additionally created inside a directory handle opened
//!     on the target, with `resolve_beneath` where the OS supports it.
//!     Sanitisation is the guarantee; this is the belt to its braces.
//!   * An entry with no usable checksum fails by default
//!     (`require_checksum`). Extracting bytes we cannot verify and
//!     calling the job complete is how a corrupt release reaches a media
//!     library.
//!
//! Volume discovery follows the entry-point name rather than the supplied
//! list, exactly as `rardecode.OpenReader` did: the successor name is
//! generated, the new scheme tried first and the old one second, and the
//! chain stops at the first name that does not exist.

const std = @import("std");
const rar = @import("rar.zig");
const source = @import("source.zig");
const volume = @import("volume.zig");
const path = @import("path.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Source = source.Source;

/// Copy buffer size. Large enough that the per-read overhead disappears
/// against the memcpy, small enough to stay out of the way.
const copy_buf_len = 128 * 1024;

pub const Options = struct {
    /// Every archive path the job collected. The entry-point volume is
    /// picked out of these by `volume.pickFirst`; the rest are ignored,
    /// because volume order comes from the naming scheme, not from list
    /// order.
    archive_paths: []const []const u8,
    /// Where entries land, relative to the `base` directory (or
    /// absolute).
    target_dir: []const u8,
    /// Refuse entries whose integrity cannot be checked.
    require_checksum: bool = true,
    /// Upper bound on entries in one archive. A RAR bomb with a million
    /// zero-length entries is otherwise a cheap way to make us do a
    /// million syscalls.
    max_entries: usize = 65536,
    /// fsync each file before reporting it written, so a crash cannot
    /// leave a job "complete" with unflushed contents.
    sync: bool = true,
};

pub const Error = rar.Error || volume.Error || Allocator.Error ||
    Io.File.OpenError || Io.File.StatError || Io.File.Writer.Error ||
    Io.File.SyncError || Io.Dir.CreateDirPathError || Io.Dir.OpenError ||
    error{
        /// `archive_paths` was empty.
        NoArchivePaths,
        /// The entry-point volume could not be opened.
        ArchiveNotFound,
        /// An entry carries no CRC32 and `require_checksum` is set.
        UnverifiableEntry,
        /// More entries than `max_entries`.
        TooManyEntries,
    };

pub const Result = struct {
    /// Relative paths written, in archive order. Owned.
    files: [][]u8,
    gpa: Allocator,

    pub fn deinit(r: *Result) void {
        for (r.files) |f| r.gpa.free(f);
        r.gpa.free(r.files);
        r.* = undefined;
    }
};

/// Extracts every entry of the archive into `opts.target_dir`.
///
/// `base` is the directory the paths are resolved against — normally
/// `std.Io.Dir.cwd()`. Absolute paths work either way on POSIX.
pub fn extract(gpa: Allocator, io: Io, base: Io.Dir, opts: Options) Error!Result {
    if (opts.archive_paths.len == 0) return error.NoArchivePaths;
    const entry_path = volume.pickFirst(opts.archive_paths) orelse return error.NoArchivePaths;

    // Volumes are opened by basename inside the archive's own directory,
    // which keeps the successor-name logic working on names rather than
    // on paths.
    const dir_part = volume.dirname(entry_path);
    var arc_dir = if (dir_part.len == 0)
        base
    else
        try base.openDir(io, dir_part, .{});
    defer if (dir_part.len != 0) arc_dir.close(io);

    var vols: Volumes = .{ .gpa = gpa, .io = io };
    defer vols.deinit();
    try vols.discover(arc_dir, volume.basename(entry_path));

    var reader = try rar.Reader.open(gpa, vols.sources);
    defer reader.deinit();

    try base.createDirPath(io, opts.target_dir);
    var target = try base.openDir(io, opts.target_dir, .{});
    defer target.close(io);

    const copy_buf = try gpa.alloc(u8, copy_buf_len);
    defer gpa.free(copy_buf);

    var written: std.ArrayList([]u8) = .empty;
    errdefer {
        for (written.items) |f| gpa.free(f);
        written.deinit(gpa);
    }

    var count: usize = 0;
    while (try reader.next()) |entry| {
        count += 1;
        if (count > opts.max_entries) return error.TooManyEntries;

        if (entry.is_dir) {
            try target.createDirPath(io, entry.name);
            continue;
        }
        if (opts.require_checksum and !entry.has_checksum and entry.unpacked_size != 0) {
            return error.UnverifiableEntry;
        }
        // The name is already sanitised by the reader, so the parent is a
        // plain relative path.
        if (path.parent(entry.name)) |parent_dir| {
            try target.createDirPath(io, parent_dir);
        }

        // Reserve the bookkeeping slot before the file exists, so the
        // only fallible step after the handle is opened is the write
        // itself and the close cannot be reached twice.
        try written.ensureUnusedCapacity(gpa, 1);

        {
            var file = try target.createFile(io, entry.name, .{
                .truncate = true,
                .resolve_beneath = true,
            });
            defer file.close(io);

            while (true) {
                const n = try reader.read(copy_buf);
                if (n == 0) break;
                try file.writeStreamingAll(io, copy_buf[0..n]);
            }
            // The entry is only reported once its bytes are on the disk:
            // a crash between here and the job being marked complete
            // must not leave an empty file behind a "delivered" status.
            if (opts.sync) try file.sync(io);
        }

        written.appendAssumeCapacity(try gpa.dupe(u8, entry.name));
    }

    return .{ .files = try written.toOwnedSlice(gpa), .gpa = gpa };
}

/// The open volume files of one archive, in order.
///
/// `Source` points at its backing `source.File`, so the file array must
/// not move once the sources are built: everything is appended first,
/// then `sources` is filled in one pass.
const Volumes = struct {
    gpa: Allocator,
    io: Io,
    files: std.ArrayList(source.File) = .empty,
    sources: []Source = &.{},

    fn deinit(v: *Volumes) void {
        for (v.files.items) |*f| f.close();
        v.files.deinit(v.gpa);
        v.gpa.free(v.sources);
    }

    /// Opens `first` and then every successor that exists, following the
    /// new naming scheme where possible and the old one otherwise.
    fn discover(v: *Volumes, dir: Io.Dir, first: []const u8) Error!void {
        try v.files.ensureTotalCapacityPrecise(v.gpa, rar.max_volumes);

        const head = source.File.open(v.io, dir, first) catch return error.ArchiveNotFound;
        v.files.appendAssumeCapacity(head);

        // The entry point's extension decides where the successor search
        // starts: `.exe` (self-extracting) and a missing extension both
        // become `.rar` first.
        var buf_a: [volume.max_name_len]u8 = undefined;
        var buf_b: [volume.max_name_len]u8 = undefined;
        var cur = volume.fixExtension(first, &buf_a) catch first;
        var old_scheme = false;

        while (v.files.items.len < rar.max_volumes) {
            const next_name = blk: {
                if (!old_scheme) {
                    if (volume.nextNewName(cur, &buf_b)) |n| {
                        if (exists(v.io, dir, n)) break :blk n;
                    } else |_| {}
                    // The new scheme produced nothing openable; fall
                    // back to `.rar` -> `.r00` and stay there.
                    const o = volume.nextOldName(cur, &buf_b) catch break;
                    if (!exists(v.io, dir, o)) break;
                    old_scheme = true;
                    break :blk o;
                }
                const o = volume.nextOldName(cur, &buf_b) catch break;
                if (!exists(v.io, dir, o)) break;
                break :blk o;
            };

            const f = source.File.open(v.io, dir, next_name) catch break;
            v.files.appendAssumeCapacity(f);
            // Swap the buffers so `cur` keeps pointing at live bytes.
            const len = next_name.len;
            @memcpy(buf_a[0..len], next_name);
            cur = buf_a[0..len];
        }

        v.sources = try v.gpa.alloc(Source, v.files.items.len);
        for (v.files.items, 0..) |*f, i| v.sources[i] = f.source();
    }

    fn exists(io: Io, dir: Io.Dir, name: []const u8) bool {
        dir.access(io, name, .{}) catch return false;
        return true;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;
const crc32 = @import("../../core/crc32.zig");
const rar5 = @import("rar5.zig");
const rar3 = @import("rar3.zig");

fn writeVol(dir: Io.Dir, name: []const u8, data: []const u8) !void {
    try dir.writeFile(t.io, .{ .sub_path = name, .data = data });
}

fn readBack(gpa: Allocator, dir: Io.Dir, name: []const u8) ![]u8 {
    return dir.readFileAlloc(t.io, name, gpa, .limited(1 << 20));
}

test "extract writes a single stored RAR5 file" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "the payload of a stored rar5 entry";
    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "sub/file.txt", .data = payload, .crc32 = crc32.checksum(payload) });
    try b.addEnd(false);
    try writeVol(tmp.dir, "release.rar", b.bytes());

    var res = try extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"release.rar"},
        .target_dir = "out",
    });
    defer res.deinit();

    try t.expectEqual(@as(usize, 1), res.files.len);
    try t.expectEqualStrings("sub/file.txt", res.files[0]);

    var out = try tmp.dir.openDir(t.io, "out", .{});
    defer out.close(t.io);
    const got = try readBack(t.allocator, out, "sub/file.txt");
    defer t.allocator.free(got);
    try t.expectEqualStrings(payload, got);
}

test "extract follows a new-scheme multi-volume set" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "0123456789ABCDEFGHIJ";
    const want_crc = crc32.checksum(payload);

    var v1 = rar5.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addMain(.{ .multi_volume = true });
    try v1.addFile(.{
        .name = "movie.mkv",
        .data = payload[0..12],
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
        .name = "movie.mkv",
        .data = payload[12..],
        .unpacked_size = payload.len,
        .crc32 = want_crc,
        .split_before = true,
    });
    try v2.addEnd(false);

    try writeVol(tmp.dir, "rel.part01.rar", v1.bytes());
    try writeVol(tmp.dir, "rel.part02.rar", v2.bytes());

    // Paths deliberately out of order: pickFirst has to find part01.
    var res = try extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{ "rel.part02.rar", "rel.part01.rar" },
        .target_dir = "out",
    });
    defer res.deinit();

    var out = try tmp.dir.openDir(t.io, "out", .{});
    defer out.close(t.io);
    const got = try readBack(t.allocator, out, "movie.mkv");
    defer t.allocator.free(got);
    try t.expectEqualStrings(payload, got);
}

test "extract follows an old-scheme multi-volume set" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "aaaaaaaaaaaaaaabbbbbbbbbbbbbbb";
    const want_crc = crc32.checksum(payload);

    var v1 = rar3.Builder.init(t.allocator);
    defer v1.deinit();
    try v1.addSignature();
    try v1.addArchiveHeader(.{ .multi_volume = true, .new_naming = false, .first_volume = true });
    try v1.addFile(.{
        .name = "show.mkv",
        .data = payload[0..15],
        .unpacked_size = payload.len,
        .split_after = true,
    });
    try v1.addEnd(true);

    var v2 = rar3.Builder.init(t.allocator);
    defer v2.deinit();
    try v2.addSignature();
    try v2.addArchiveHeader(.{ .multi_volume = true, .new_naming = false });
    try v2.addFile(.{
        .name = "show.mkv",
        .data = payload[15..],
        .unpacked_size = payload.len,
        .crc32 = want_crc,
        .split_before = true,
    });
    try v2.addEnd(false);

    try writeVol(tmp.dir, "show.rar", v1.bytes());
    try writeVol(tmp.dir, "show.r00", v2.bytes());

    var res = try extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{ "show.rar", "show.r00" },
        .target_dir = "out",
    });
    defer res.deinit();
    try t.expectEqual(@as(usize, 1), res.files.len);

    var out = try tmp.dir.openDir(t.io, "out", .{});
    defer out.close(t.io);
    const got = try readBack(t.allocator, out, "show.mkv");
    defer t.allocator.free(got);
    try t.expectEqualStrings(payload, got);
}

test "extract creates directory entries and nested parents" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "Release", .is_dir = true });
    try b.addFile(.{ .name = "Release/Sample/s.mkv", .data = "s", .crc32 = crc32.checksum("s") });
    try b.addEnd(false);
    try writeVol(tmp.dir, "a.rar", b.bytes());

    var res = try extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"a.rar"},
        .target_dir = "out",
    });
    defer res.deinit();
    // Directories are not reported as written files, matching the Go
    // adapter's behaviour.
    try t.expectEqual(@as(usize, 1), res.files.len);
    try t.expectEqualStrings("Release/Sample/s.mkv", res.files[0]);

    var out = try tmp.dir.openDir(t.io, "out", .{});
    defer out.close(t.io);
    const got = try readBack(t.allocator, out, "Release/Sample/s.mkv");
    defer t.allocator.free(got);
    try t.expectEqualStrings("s", got);
}

test "extract refuses a traversal entry and writes nothing outside the target" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{
        .name = "../../escaped.txt",
        .data = "pwned",
        .crc32 = crc32.checksum("pwned"),
    });
    try b.addEnd(false);
    try writeVol(tmp.dir, "evil.rar", b.bytes());

    try t.expectError(error.ParentTraversal, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"evil.rar"},
        .target_dir = "out",
    }));

    // Nothing was created beside the target directory.
    try t.expectError(error.FileNotFound, tmp.dir.access(t.io, "escaped.txt", .{}));
    try t.expectError(error.FileNotFound, tmp.dir.access(t.io, "out/escaped.txt", .{}));
}

test "extract refuses an absolute-path entry" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "/etc/pwned", .data = "x", .crc32 = crc32.checksum("x") });
    try b.addEnd(false);
    try writeVol(tmp.dir, "evil.rar", b.bytes());

    try t.expectError(error.AbsolutePath, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"evil.rar"},
        .target_dir = "out",
    }));
}

test "extract refuses an entry with a NUL in its name" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "ok.txt\x00/../../x", .data = "x", .crc32 = crc32.checksum("x") });
    try b.addEnd(false);
    try writeVol(tmp.dir, "evil.rar", b.bytes());

    try t.expectError(error.ControlCharacter, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"evil.rar"},
        .target_dir = "out",
    }));
}

test "extract refuses a compressed entry by name" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{
        .name = "packed.bin",
        .data = "compressed bytes",
        .unpacked_size = 1000,
        .method = 3,
        .crc32 = 0,
    });
    try b.addEnd(false);
    try writeVol(tmp.dir, "packed.rar", b.bytes());

    try t.expectError(error.UnsupportedCompressionMethod, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"packed.rar"},
        .target_dir = "out",
    }));
}

test "extract refuses an unverifiable entry unless told not to" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    // No CRC32 flag: nothing to check the bytes against.
    try b.addFile(.{ .name = "unchecked.bin", .data = "abcd" });
    try b.addEnd(false);
    try writeVol(tmp.dir, "nocrc.rar", b.bytes());

    try t.expectError(error.UnverifiableEntry, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"nocrc.rar"},
        .target_dir = "out",
    }));

    var res = try extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"nocrc.rar"},
        .target_dir = "out2",
        .require_checksum = false,
    });
    defer res.deinit();
    try t.expectEqual(@as(usize, 1), res.files.len);
}

test "extract surfaces a checksum mismatch" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "corrupt.bin", .data = "abcd", .crc32 = 0x1234_5678 });
    try b.addEnd(false);
    try writeVol(tmp.dir, "bad.rar", b.bytes());

    try t.expectError(error.ChecksumMismatch, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"bad.rar"},
        .target_dir = "out",
    }));
}

test "extract reports a missing archive rather than panicking" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try t.expectError(error.ArchiveNotFound, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"nope.rar"},
        .target_dir = "out",
    }));
    try t.expectError(error.NoArchivePaths, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{},
        .target_dir = "out",
    }));
}

test "extract stops at max_entries" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    for (0..4) |i| {
        var name_buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "f{d}.txt", .{i});
        try b.addFile(.{ .name = name, .data = "x", .crc32 = crc32.checksum("x") });
    }
    try b.addEnd(false);
    try writeVol(tmp.dir, "many.rar", b.bytes());

    try t.expectError(error.TooManyEntries, extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"many.rar"},
        .target_dir = "out",
        .max_entries = 2,
    }));
}

test "extract works from a subdirectory path" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(t.io, "incomplete/job1");

    var b = rar5.Builder.init(t.allocator);
    defer b.deinit();
    try b.addSignature();
    try b.addMain(.{});
    try b.addFile(.{ .name = "x.txt", .data = "deep", .crc32 = crc32.checksum("deep") });
    try b.addEnd(false);

    var sub = try tmp.dir.openDir(t.io, "incomplete/job1", .{});
    defer sub.close(t.io);
    try writeVol(sub, "a.rar", b.bytes());

    var res = try extract(t.allocator, t.io, tmp.dir, .{
        .archive_paths = &.{"incomplete/job1/a.rar"},
        .target_dir = "complete/job1",
    });
    defer res.deinit();
    try t.expectEqual(@as(usize, 1), res.files.len);

    var out = try tmp.dir.openDir(t.io, "complete/job1", .{});
    defer out.close(t.io);
    const got = try readBack(t.allocator, out, "x.txt");
    defer t.allocator.free(got);
    try t.expectEqualStrings("deep", got);
}
