//! Turning release and file names into path components that are safe to
//! write, plus the "is this an archive" test.
//!
//! Go had this code twice — once in `app/extract`, once in `app/deliver`
//! — with the two copies already subtly different (`extract`'s
//! `sanitizeFilename` and `deliver`'s were separate functions with the
//! same body). Both services ask the same question about the same
//! strings, so there is one copy here.
//!
//! # The safety property
//!
//! Every string that reaches these functions came from an NZB, which came
//! from an indexer, which got it from whoever posted the release. A
//! filename is therefore untrusted input that is about to become a path.
//! `sanitizeFilename` takes the basename first (so `../../etc/passwd`
//! becomes `passwd`), then replaces the separators that survive, strips
//! control characters, and refuses leading or trailing dots so nothing
//! lands as a hidden file or as `..`.

const std = @import("std");

/// Whether the filename looks like part of a RAR set: `.rar`, or the
/// classic `.r00`–`.r99` split.
///
/// Conservative on purpose. A false negative means delivering the `.rar`
/// files raw — ugly but harmless. A false positive means deliver skips
/// the job expecting extract to take it, and extract finds nothing to
/// unpack, so the release never lands at all. Err towards false
/// negatives.
pub fn looksLikeRAR(filename: []const u8) bool {
    if (endsWithIgnoreCase(filename, ".rar")) return true;
    if (filename.len < 4) return false;
    const tail = filename[filename.len - 4 ..];
    if (tail[0] != '.') return false;
    if (tail[1] != 'r' and tail[1] != 'R') return false;
    return std.ascii.isDigit(tail[2]) and std.ascii.isDigit(tail[3]);
}

fn endsWithIgnoreCase(s: []const u8, suffix: []const u8) bool {
    if (suffix.len > s.len) return false;
    return std.ascii.eqlIgnoreCase(s[s.len - suffix.len ..], suffix);
}

/// Largest sanitised component this module will produce. Longer inputs
/// are truncated, because a filesystem's `NAME_MAX` is 255 on every
/// target we ship to and an over-long name fails the write rather than
/// the sanitise.
pub const max_component = 255;

/// Makes an arbitrary string safe as a single path component.
///
/// Control characters are dropped, `/`, `\` and `:` become `_`, and
/// leading/trailing whitespace and dots are trimmed. An input that
/// sanitises away to nothing becomes "untitled" rather than an empty
/// component, which would silently reparent the file.
///
/// The result is written into `buf` and borrowed from it.
pub fn sanitizeComponent(buf: *[max_component]u8, s: []const u8) []const u8 {
    var n: usize = 0;
    for (s) |c| {
        if (n == buf.len) break;
        if (c < 0x20 or c == 0x7f) continue;
        buf[n] = switch (c) {
            '/', '\\', ':' => '_',
            else => c,
        };
        n += 1;
    }
    const trimmed = std.mem.trim(u8, buf[0..n], " \t\r\n");
    const cleaned = std.mem.trim(u8, trimmed, ".");
    if (cleaned.len == 0) return "untitled";
    return cleaned;
}

/// `sanitizeComponent` after reducing a path to its basename, so a
/// filename carrying `../` cannot escape its directory.
pub fn sanitizeFilename(buf: *[max_component]u8, name: []const u8) []const u8 {
    if (name.len == 0) return "untitled";
    return sanitizeComponent(buf, std.fs.path.basename(name));
}

/// The directory component a release lands in.
pub fn sanitizeReleaseName(buf: *[max_component]u8, name: []const u8) []const u8 {
    if (name.len == 0) return "untitled";
    return sanitizeComponent(buf, name);
}

/// Whether any non-parity file in the job looks like a RAR volume.
///
/// This is the switch that decides which of the two `verify.ok`
/// subscribers owns a job: extract takes archives, deliver takes
/// everything else. Both run the same predicate over the same aggregate,
/// so exactly one of them acts — no cross-context coordination needed.
pub fn isArchiveJob(comptime Job: type, job: *const Job) bool {
    for (job.files) |f| {
        if (f.is_par2) continue;
        if (looksLikeRAR(f.filename)) return true;
    }
    return false;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "RAR detection covers .rar and the classic numbered split" {
    const yes = [_][]const u8{
        "release.rar", "RELEASE.RAR", "release.part01.rar",
        "release.r00", "release.r99", "release.R42",
    };
    for (yes) |n| try testing.expect(looksLikeRAR(n));

    const no = [_][]const u8{
        "release.par2", "movie.mkv", "release.r0", "release.rar.txt",
        "release.raa",  "r00",       "",           "abc",
    };
    for (no) |n| try testing.expect(!looksLikeRAR(n));
}

test "sanitising replaces separators and drops control characters" {
    var buf: [max_component]u8 = undefined;
    try testing.expectEqualStrings("a_b_c", sanitizeComponent(&buf, "a/b\\c"));
    try testing.expectEqualStrings("a_b", sanitizeComponent(&buf, "a:b"));
    try testing.expectEqualStrings("ab", sanitizeComponent(&buf, "a\x00\x1f\x7fb"));
    try testing.expectEqualStrings("Great.Movie.2020", sanitizeComponent(&buf, "  Great.Movie.2020  "));
}

test "nothing sanitises down to an empty or hidden component" {
    var buf: [max_component]u8 = undefined;
    // A leading dot would make a hidden file; a trailing one is a
    // Windows-ism and reads badly everywhere else.
    try testing.expectEqualStrings("hidden", sanitizeComponent(&buf, ".hidden."));
    try testing.expectEqualStrings("untitled", sanitizeComponent(&buf, "..."));
    try testing.expectEqualStrings("untitled", sanitizeComponent(&buf, "   "));
    try testing.expectEqualStrings("untitled", sanitizeComponent(&buf, "\x01\x02"));
    try testing.expectEqualStrings("untitled", sanitizeFilename(&buf, ""));
    try testing.expectEqualStrings("untitled", sanitizeReleaseName(&buf, ""));
}

test "a filename cannot escape its directory" {
    var buf: [max_component]u8 = undefined;
    try testing.expectEqualStrings("passwd", sanitizeFilename(&buf, "../../etc/passwd"));
    try testing.expectEqualStrings("evil.mkv", sanitizeFilename(&buf, "/absolute/evil.mkv"));
    // A traversal that survives basename() because the separator is a
    // backslash still cannot produce a separator in the output, and the
    // leading dots are trimmed on the way past.
    try testing.expectEqualStrings("_.._evil", sanitizeFilename(&buf, "..\\..\\evil"));
    // Bare "..' is not a usable component either.
    try testing.expectEqualStrings("untitled", sanitizeFilename(&buf, ".."));
}

test "an over-long name is truncated rather than rejected" {
    var buf: [max_component]u8 = undefined;
    const long = "x" ** 400;
    const got = sanitizeComponent(&buf, long);
    try testing.expectEqual(@as(usize, max_component), got.len);
}

const FakeFile = struct { filename: []const u8, is_par2: bool = false };
const FakeJob = struct { files: []const FakeFile };

test "a job is an archive job when any non-parity file is a RAR volume" {
    const archive = FakeJob{ .files = &.{
        .{ .filename = "rel.par2", .is_par2 = true },
        .{ .filename = "rel.part01.rar" },
    } };
    try testing.expect(isArchiveJob(FakeJob, &archive));

    const plain = FakeJob{ .files = &.{
        .{ .filename = "rel.par2", .is_par2 = true },
        .{ .filename = "movie.mkv" },
    } };
    try testing.expect(!isArchiveJob(FakeJob, &plain));

    // A parity file whose name happens to look like a RAR does not make
    // the job an archive — otherwise deliver and extract would both skip
    // it and the release would never land.
    const parity_only = FakeJob{ .files = &.{
        .{ .filename = "rel.r00", .is_par2 = true },
        .{ .filename = "movie.mkv" },
    } };
    try testing.expect(!isArchiveJob(FakeJob, &parity_only));
}
