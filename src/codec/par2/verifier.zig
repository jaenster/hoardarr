//! Verifies a downloaded release against its PAR2 metadata.
//!
//! Policy: full-file MD5 only. The per-slice MD5 + CRC32 pairs from IFSC
//! packets are parsed and kept (the repair path needs them to *locate*
//! damage), but verification hashes each file once. That catches all
//! corruption just as well; slice precision only buys you the position.
//!
//! Two concessions to how releases actually arrive on Usenet:
//!
//!   * **Obfuscated names.** The filename in the NZB frequently has
//!     nothing to do with the one PAR2 recorded. FileDesc carries an MD5
//!     of the first 16 KiB precisely so a file can be matched by content
//!     instead, and that fallback is used whenever the name lookup
//!     misses. SABnzbd does the same thing.
//!   * **Missing sidecars.** Posters routinely drop the `.nfo`, `.sfv`,
//!     subtitles and cover art. A missing sidecar must not fail a
//!     release whose main file is intact, so those are dropped from the
//!     result entirely rather than reported as failures. A sidecar that
//!     *is* present and mismatches still fails normally.

const std = @import("std");
const Io = std.Io;
const Md5 = std.crypto.hash.Md5;
const par2 = @import("par2.zig");

/// MD516k covers the first 16384 bytes, or the whole file if shorter.
pub const md5_16k_len = 16384;

/// Chunk size for whole-file hashing. Big enough that syscall overhead
/// disappears, small enough to sit on the stack.
const hash_chunk = 64 * 1024;

/// Extensions treated as optional metadata. Mirrors SABnzbd's
/// quick-check-ignore list in `newsunpack.py`.
const quick_check_ignore_exts = [_][]const u8{
    ".nfo",    ".sfv", ".srr",  ".srt", ".idx",
    ".sub",    ".jpg", ".jpeg", ".png", ".txt",
    ".readme",
};

/// True when a file absent from disk should be skipped silently.
pub fn isQuickCheckIgnorable(name: []const u8) bool {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return false;
    const ext = name[dot..];
    for (quick_check_ignore_exts) |candidate| {
        if (std.ascii.eqlIgnoreCase(ext, candidate)) return true;
    }
    return false;
}

/// Per-file I/O failures. Kept concrete rather than `anyerror` so callers
/// can switch on it.
pub const IoFailure = Io.File.OpenError || Io.File.ReadPositionalError || Io.File.StatError;

pub const Error = par2.ReadError;

/// One file in the NZB: the name it is known by, and where it landed.
/// `path` is resolved relative to the directory handed to `verify`.
pub const DataFile = struct {
    name: []const u8,
    path: []const u8,
};

/// Why a file failed, or `ok`.
pub const Reason = union(enum) {
    ok,
    /// PAR2 declares this file but the NZB has no such file, and nothing
    /// on disk matched it by content either.
    not_in_nzb,
    missing_on_disk,
    size_mismatch: struct { got: u64, want: u64 },
    md5_mismatch,
    /// Unreadable for a reason other than absence.
    io_failed: IoFailure,

    /// A short static label. `size_mismatch` drops its numbers here;
    /// callers that want them read the union.
    pub fn text(r: Reason) []const u8 {
        return switch (r) {
            .ok => "",
            .not_in_nzb => "not in NZB",
            .missing_on_disk => "missing on disk",
            .size_mismatch => "size mismatch",
            .md5_mismatch => "md5 mismatch",
            .io_failed => "read failed",
        };
    }
};

pub const FileResult = struct {
    /// The PAR2-declared filename. Owned.
    filename: []u8,
    ok: bool,
    reason: Reason,
};

pub const Result = struct {
    files: std.ArrayList(FileResult) = .empty,

    pub fn deinit(r: *Result, alloc: std.mem.Allocator) void {
        for (r.files.items) |f| alloc.free(f.filename);
        r.files.deinit(alloc);
        r.* = undefined;
    }

    pub fn allOk(r: *const Result) bool {
        for (r.files.items) |f| {
            if (!f.ok) return false;
        }
        return true;
    }

    pub fn failedCount(r: *const Result) usize {
        var n: usize = 0;
        for (r.files.items) |f| {
            if (!f.ok) n += 1;
        }
        return n;
    }
};

/// Parses `par2_paths` and checks every file they declare against `data`.
/// All paths are relative to `dir`.
pub fn verify(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    par2_paths: []const []const u8,
    data: []const DataFile,
) Error!Result {
    var set = try par2.parseFiles(alloc, io, dir, par2_paths);
    defer set.deinit(alloc);
    return verifySet(alloc, io, dir, &set, data);
}

/// `verify` against an already-parsed recovery set.
pub fn verifySet(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    set: *const par2.RecoverySet,
    data: []const DataFile,
) error{OutOfMemory}!Result {
    var result: Result = .{};
    errdefer result.deinit(alloc);

    // The content-addressed index is built on first need, so a release
    // whose names all line up never reads a byte beyond the full-file
    // hashing it has to do anyway.
    var index: ?Md516kIndex = null;
    defer if (index) |*i| i.deinit(alloc);

    for (set.files.items) |pf| {
        var path: ?[]const u8 = lookupByName(data, pf.name);
        if (path == null) {
            if (index == null) index = try buildMd516kIndex(alloc, io, dir, data);
            path = index.?.map.get(pf.md5_16k);
        }

        const p = path orelse {
            // Optional sidecar that never arrived: drop it from the
            // result so it does not even count towards "any failure
            // means repair".
            if (isQuickCheckIgnorable(pf.name)) continue;
            try result.files.append(alloc, .{
                .filename = try alloc.dupe(u8, pf.name),
                .ok = false,
                .reason = .not_in_nzb,
            });
            continue;
        };

        const reason = verifyFile(io, dir, p, pf.md5, pf.size);
        try result.files.append(alloc, .{
            .filename = try alloc.dupe(u8, pf.name),
            .ok = reason == .ok,
            .reason = reason,
        });
    }
    return result;
}

fn lookupByName(data: []const DataFile, name: []const u8) ?[]const u8 {
    for (data) |d| {
        if (std.mem.eql(u8, d.name, name)) return d.path;
    }
    return null;
}

/// Hashes `path` and compares against the PAR2-declared digest. A size
/// mismatch short-circuits — no point reading a file that cannot match.
pub fn verifyFile(
    io: Io,
    dir: Io.Dir,
    path: []const u8,
    expect_md5: [16]u8,
    expect_size: u64,
) Reason {
    var file = dir.openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return .missing_on_disk,
        else => return .{ .io_failed = err },
    };
    defer file.close(io);

    if (expect_size > 0) {
        const st = file.stat(io) catch |err| return .{ .io_failed = err };
        if (st.size != expect_size) {
            return .{ .size_mismatch = .{ .got = st.size, .want = expect_size } };
        }
    }

    var h = Md5.init(.{});
    // On the stack: MD5 is the bottleneck here, not the read size, so
    // there is nothing to gain from a heap buffer and an allocator
    // parameter on a pure predicate.
    var buf: [hash_chunk]u8 = undefined;

    var offset: u64 = 0;
    while (true) {
        const n = file.readPositionalAll(io, &buf, offset) catch |err| {
            return .{ .io_failed = err };
        };
        if (n == 0) break;
        h.update(buf[0..n]);
        offset += n;
        // A short read from a positional read-all means end of file.
        if (n < buf.len) break;
    }

    var sum: [16]u8 = undefined;
    h.final(&sum);
    if (!std.mem.eql(u8, &sum, &expect_md5)) return .md5_mismatch;
    return .ok;
}

const Md516kIndex = struct {
    map: std.AutoHashMapUnmanaged([16]u8, []const u8),

    fn deinit(i: *Md516kIndex, alloc: std.mem.Allocator) void {
        i.map.deinit(alloc);
    }
};

/// Digest of the first 16 KiB of every data file, mapped to its path.
///
/// First entry wins on a collision: vanishingly unlikely for unrelated
/// files, entirely possible for byte-identical duplicates, and either way
/// one path is enough. Unreadable files are skipped — they will fail the
/// name-based check on their own.
///
/// 16 KiB per file is nothing next to the full-file hashing that follows,
/// let alone Reed-Solomon reconstruction.
fn buildMd516kIndex(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    data: []const DataFile,
) error{OutOfMemory}!Md516kIndex {
    var map: std.AutoHashMapUnmanaged([16]u8, []const u8) = .empty;
    errdefer map.deinit(alloc);
    try map.ensureTotalCapacity(alloc, @intCast(data.len));

    for (data) |d| {
        const digest = md5First16k(io, dir, d.path) catch continue;
        const gop = map.getOrPutAssumeCapacity(digest);
        if (!gop.found_existing) gop.value_ptr.* = d.path;
    }
    return .{ .map = map };
}

/// MD5 of the first 16 KiB of `path`, or of the whole file if shorter —
/// exactly what PAR2's FileDesc.MD516k field holds.
pub fn md5First16k(io: Io, dir: Io.Dir, path: []const u8) IoFailure![16]u8 {
    var file = try dir.openFile(io, path, .{});
    defer file.close(io);

    var buf: [md5_16k_len]u8 = undefined;
    const n = try file.readPositionalAll(io, &buf, 0);
    var out: [16]u8 = undefined;
    Md5.hash(buf[0..n], &out, .{});
    return out;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const crc32 = @import("../../core/crc32.zig");

test "quick-check-ignorable extensions" {
    const t = std.testing;
    try t.expect(isQuickCheckIgnorable("scene.nfo"));
    try t.expect(isQuickCheckIgnorable("SCENE.NFO"));
    try t.expect(isQuickCheckIgnorable("cover.jpeg"));
    try t.expect(isQuickCheckIgnorable("subs/movie.srt"));
    try t.expect(!isQuickCheckIgnorable("movie.mkv"));
    try t.expect(!isQuickCheckIgnorable("movie.part01.rar"));
    try t.expect(!isQuickCheckIgnorable("noextension"));
}

/// The PAR2 FileDesc fields for one on-disk file.
const Fixture = struct {
    id: [16]u8,
    name: []const u8,
    md5: [16]u8,
    md5_16k: [16]u8,
    size: u64,
};

fn writeFixture(dir: Io.Dir, id_byte: u8, name: []const u8, data: []const u8) !Fixture {
    try dir.writeFile(std.testing.io, .{ .sub_path = name, .data = data });
    var md5: [16]u8 = undefined;
    Md5.hash(data, &md5, .{});
    var md5_16k: [16]u8 = undefined;
    Md5.hash(data[0..@min(data.len, md5_16k_len)], &md5_16k, .{});
    return .{
        .id = @splat(id_byte),
        .name = name,
        .md5 = md5,
        .md5_16k = md5_16k,
        .size = data.len,
    };
}

/// Builds a PAR2 index stream (Main + one FileDesc per fixture) and
/// writes it under `stem`.
fn writePar2(
    alloc: std.mem.Allocator,
    dir: Io.Dir,
    set_id: [16]u8,
    stem: []const u8,
    fixtures: []const Fixture,
) !void {
    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);

    const ids = try alloc.alloc([16]u8, fixtures.len);
    defer alloc.free(ids);
    for (fixtures, 0..) |f, i| ids[i] = f.id;

    const main = try par2.encodeMain(alloc, set_id, 1024, ids);
    defer alloc.free(main);
    try stream.appendSlice(alloc, main);

    for (fixtures) |f| {
        const fd = try par2.encodeFileDesc(alloc, set_id, f.id, f.md5, f.md5_16k, f.size, f.name);
        defer alloc.free(fd);
        try stream.appendSlice(alloc, fd);
    }

    try dir.writeFile(std.testing.io, .{ .sub_path = stem, .data = stream.items });
}

test "verify passes when every file matches" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id: [16]u8 = @splat(0xAA);
    const a = try writeFixture(tmp.dir, 1, "movie.mkv", "the main feature, such as it is");
    const b = try writeFixture(tmp.dir, 2, "sample.mkv", "a smaller sample clip");
    try writePar2(alloc, tmp.dir, set_id, "release.par2", &.{ a, b });

    var result = try verify(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "movie.mkv", .path = "movie.mkv" },
        .{ .name = "sample.mkv", .path = "sample.mkv" },
    });
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 2), result.files.items.len);
    try t.expect(result.allOk());
    try t.expectEqual(@as(usize, 0), result.failedCount());
}

test "verify reports a corrupted file" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id: [16]u8 = @splat(0xAA);
    const a = try writeFixture(tmp.dir, 1, "movie.mkv", "sixteen bytes ok");
    try writePar2(alloc, tmp.dir, set_id, "release.par2", &.{a});

    // Same length, different content — the size check passes, so the MD5
    // is what has to catch it.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "movie.mkv", .data = "sixteen bytes XX" });

    var result = try verify(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "movie.mkv", .path = "movie.mkv" },
    });
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 1), result.files.items.len);
    try t.expect(!result.allOk());
    try t.expectEqual(Reason.md5_mismatch, result.files.items[0].reason);
}

test "verify reports a truncated file as a size mismatch" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id: [16]u8 = @splat(0xAA);
    const a = try writeFixture(tmp.dir, 1, "movie.mkv", "the whole thing, all of it");
    try writePar2(alloc, tmp.dir, set_id, "release.par2", &.{a});
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "movie.mkv", .data = "the whole" });

    var result = try verify(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "movie.mkv", .path = "movie.mkv" },
    });
    defer result.deinit(alloc);

    const r = result.files.items[0].reason;
    try t.expect(r == .size_mismatch);
    try t.expectEqual(@as(u64, 9), r.size_mismatch.got);
    try t.expectEqual(@as(u64, 26), r.size_mismatch.want);
}

test "verify matches an obfuscated file by its first-16k MD5" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id: [16]u8 = @splat(0xAA);
    const payload = "obfuscated releases rename everything";
    // PAR2 knows it as movie.mkv...
    const a = try writeFixture(tmp.dir, 1, "movie.mkv", payload);
    try writePar2(alloc, tmp.dir, set_id, "release.par2", &.{a});
    // ...but on disk it landed under a hash-like name, and the NZB has no
    // entry called movie.mkv at all.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "a1b2c3d4e5", .data = payload });
    try tmp.dir.deleteFile(std.testing.io, "movie.mkv");

    var result = try verify(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "a1b2c3d4e5", .path = "a1b2c3d4e5" },
    });
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 1), result.files.items.len);
    try t.expectEqualStrings("movie.mkv", result.files.items[0].filename);
    try t.expect(result.allOk());
}

test "a file absent from the NZB is reported, unless it is an optional sidecar" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id: [16]u8 = @splat(0xAA);
    const main_file = try writeFixture(tmp.dir, 1, "movie.mkv", "present and correct");
    // Declared by PAR2, never written, never in the NZB.
    const missing_video: Fixture = .{
        .id = @splat(2),
        .name = "extras.mkv",
        .md5 = @splat(0x11),
        .md5_16k = @splat(0x22),
        .size = 4096,
    };
    const missing_nfo: Fixture = .{
        .id = @splat(3),
        .name = "scene.nfo",
        .md5 = @splat(0x33),
        .md5_16k = @splat(0x44),
        .size = 512,
    };
    try writePar2(alloc, tmp.dir, set_id, "release.par2", &.{ main_file, missing_video, missing_nfo });

    var result = try verify(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "movie.mkv", .path = "movie.mkv" },
    });
    defer result.deinit(alloc);

    // The .nfo is dropped entirely; extras.mkv is a real failure.
    try t.expectEqual(@as(usize, 2), result.files.items.len);
    try t.expectEqualStrings("movie.mkv", result.files.items[0].filename);
    try t.expect(result.files.items[0].ok);
    try t.expectEqualStrings("extras.mkv", result.files.items[1].filename);
    try t.expectEqual(Reason.not_in_nzb, result.files.items[1].reason);
    try t.expectEqual(@as(usize, 1), result.failedCount());
}

test "a file in the NZB but absent from disk is reported as missing" {
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const set_id: [16]u8 = @splat(0xAA);
    const a = try writeFixture(tmp.dir, 1, "movie.mkv", "was here once");
    try writePar2(alloc, tmp.dir, set_id, "release.par2", &.{a});
    try tmp.dir.deleteFile(std.testing.io, "movie.mkv");

    var result = try verify(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "movie.mkv", .path = "movie.mkv" },
    });
    defer result.deinit(alloc);

    try t.expectEqual(Reason.missing_on_disk, result.files.items[0].reason);
}

test "verify with no PAR2 files" {
    const t = std.testing;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    try t.expectError(
        error.NoPar2Files,
        verify(t.allocator, std.testing.io, tmp.dir, &.{}, &.{}),
    );
}

test "md5First16k truncates at 16 KiB" {
    const t = std.testing;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const data = try t.allocator.alloc(u8, md5_16k_len + 4096);
    defer t.allocator.free(data);
    var prng = std.Random.DefaultPrng.init(0x16000);
    prng.random().bytes(data);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "big.bin", .data = data });

    var want: [16]u8 = undefined;
    Md5.hash(data[0..md5_16k_len], &want, .{});
    const got = try md5First16k(std.testing.io, tmp.dir, "big.bin");
    try t.expectEqualSlices(u8, &want, &got);
}

test "obfuscated matching against the real ParPar fixture" {
    // The checked-in fixture is a real PAR2 index for a 37-part release
    // plus seven of its data parts, saved under NZB-side obfuscated names
    // that share nothing with the PAR2-recorded ones. The Go
    // implementation resolves exactly seven of the 37 descriptors by
    // MD516k against these files; anything else here means the digest is
    // being computed over the wrong bytes.
    const t = std.testing;
    const alloc = t.allocator;

    var dir = Io.Dir.cwd().openDir(std.testing.io, par2.fixture_dir, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer dir.close(std.testing.io);

    const samples = [_][]const u8{
        "sample-1221.bin", "sample-1222.bin", "sample-1223.bin", "sample-1230.bin",
        "sample-1240.bin", "sample-1250.bin", "sample-1255.bin",
    };
    var data: [samples.len]DataFile = undefined;
    for (samples, 0..) |s, i| data[i] = .{ .name = s, .path = s };

    var set = try par2.parseFiles(alloc, std.testing.io, dir, &.{"main.par2"});
    defer set.deinit(alloc);

    var index = try buildMd516kIndex(alloc, std.testing.io, dir, &data);
    defer index.deinit(alloc);
    // Seven distinct files, seven distinct digests.
    try t.expectEqual(@as(u32, samples.len), index.map.count());

    var matched: usize = 0;
    for (set.files.items) |pf| {
        if (index.map.get(pf.md5_16k) != null) matched += 1;
    }
    try t.expectEqual(@as(usize, 7), matched);

    // And the specific pairing the Go test logs, so a reshuffle would be
    // caught rather than just the count.
    var expect: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&expect, "03c04be4d9c9a54494e4517eeea709ac");
    try t.expectEqualStrings("sample-1221.bin", index.map.get(expect).?);
    _ = try std.fmt.hexToBytes(&expect, "9d47a3fc6690459f6ad1441bbd25e0f6");
    try t.expectEqualStrings("sample-1255.bin", index.map.get(expect).?);

    // The data files are exactly 16 KiB, so MD516k of the file equals the
    // MD5 of the whole thing — which lets one fixture check both paths.
    const whole = try dir.readFileAlloc(std.testing.io, "sample-1221.bin", alloc, .limited(1 << 20));
    defer alloc.free(whole);
    try t.expectEqual(@as(usize, md5_16k_len), whole.len);
    var whole_md5: [16]u8 = undefined;
    Md5.hash(whole, &whole_md5, .{});
    _ = try std.fmt.hexToBytes(&expect, "03c04be4d9c9a54494e4517eeea709ac");
    try t.expectEqualSlices(u8, &expect, &whole_md5);
    try t.expectEqual(
        Reason.ok,
        verifyFile(std.testing.io, dir, "sample-1221.bin", whole_md5, md5_16k_len),
    );
}

test "whole-file hashing spans multiple read chunks" {
    // The hash loop reads in 256 KiB chunks; a larger file proves the
    // offset advances and nothing is double-counted.
    const t = std.testing;
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const data = try alloc.alloc(u8, hash_chunk * 2 + 1234);
    defer alloc.free(data);
    var prng = std.Random.DefaultPrng.init(0xB16);
    prng.random().bytes(data);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "big.bin", .data = data });

    var md5: [16]u8 = undefined;
    Md5.hash(data, &md5, .{});
    try t.expectEqual(Reason.ok, verifyFile(std.testing.io, tmp.dir, "big.bin", md5, data.len));

    // The shared CRC32 agrees with std over the same bytes — that is the
    // implementation the per-slice IFSC checks will use.
    try t.expectEqual(crc32.checksum(data), std.hash.crc.Crc32.hash(data));
}
