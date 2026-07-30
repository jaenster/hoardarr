//! Multi-volume file naming.
//!
//! A split RAR set is a sequence of files whose names follow one of two
//! schemes, and nothing inside the archive says which — the reader has
//! to guess from the name it was handed and then check whether the guess
//! exists on disk.
//!
//! **New scheme** (`-vn` off, the default since RAR 3):
//!
//!     release.part01.rar  release.part02.rar  …  release.part57.rar
//!
//! The volume number is a digit run inside the name; its width is
//! preserved, so `part09` is followed by `part10` and `part099` by
//! `part100`.
//!
//! **Old scheme:**
//!
//!     release.rar  release.r00  release.r01  …  release.r99  release.s00
//!
//! The first volume keeps `.rar`; the rest use a two-digit extension
//! whose letter increments on overflow. Usenet posts of any age use this
//! one, which is why it is still here.
//!
//! Both functions are pure string transformations, ported from
//! `nwaples/rardecode`'s `volume.go` so the Zig port picks the same
//! successor file as the Go implementation did for every input.

const std = @import("std");

/// Enough for any volume name we will construct: the successor is never
/// more than one byte longer than its predecessor.
pub const max_name_len = 512;

pub const Error = error{
    NameTooLong,
    /// The name has no digit run to increment and no extension to
    /// replace, so no successor can be derived.
    NoVolumeNumber,
};

/// Picks the volume to open first out of a set of paths in arbitrary
/// order. Mirrors the Go `pickFirstVolume` exactly:
///
///   * a `*.part1.rar` / `*.part01.rar` / `*.part001.rar` wins
///   * otherwise the lexicographically smallest `*.rar`, which is how
///     the old scheme's first volume is identified (`.r00` sorts after
///     `.rar` only because 'a' < 'r' is false — hence the explicit
///     extension test rather than a bare sort)
///   * otherwise the lexicographically smallest path
pub fn pickFirst(paths: []const []const u8) ?[]const u8 {
    if (paths.len == 0) return null;
    if (paths.len == 1) return paths[0];

    var best: ?[]const u8 = null;
    for (paths) |p| {
        const base = basename(p);
        if (endsWithIgnoreCase(base, ".part1.rar") or
            endsWithIgnoreCase(base, ".part01.rar") or
            endsWithIgnoreCase(base, ".part001.rar"))
        {
            if (best == null or std.mem.lessThan(u8, p, best.?)) best = p;
        }
    }
    if (best) |b| return b;

    for (paths) |p| {
        if (endsWithIgnoreCase(p, ".rar")) {
            if (best == null or std.mem.lessThan(u8, p, best.?)) best = p;
        }
    }
    if (best) |b| return b;

    for (paths) |p| {
        if (best == null or std.mem.lessThan(u8, p, best.?)) best = p;
    }
    return best;
}

/// True when `name` looks like the first volume of a new-scheme set.
pub fn isFirstNewScheme(name: []const u8) bool {
    const base = basename(name);
    return endsWithIgnoreCase(base, ".part1.rar") or
        endsWithIgnoreCase(base, ".part01.rar") or
        endsWithIgnoreCase(base, ".part001.rar");
}

/// New-scheme successor: increments the volume digit run in place,
/// preserving its width.
///
/// Which digit run is the volume number is genuinely ambiguous —
/// `Show.2024.part03.rar` has two — so the rule is rardecode's:
///
///   * one digit run: that is the volume number
///   * two or more: consider the last two. If they are separated by a
///     `.`, or nothing before the first one contains a `.`, the volume
///     number is the *last* run. Otherwise it is the second-to-last,
///     which is the `name.part###of###.rar` shape.
pub fn nextNewName(name: []const u8, out: []u8) Error![]u8 {
    // Collect digit-run boundaries as [start, end) pairs.
    var runs: [32]usize = undefined;
    var n: usize = 0;
    var in_digit = false;
    for (name, 0..) |c, i| {
        const is_digit = c >= '0' and c <= '9';
        if (is_digit and !in_digit) {
            if (n == runs.len) break;
            runs[n] = i;
            n += 1;
            in_digit = true;
        } else if (!is_digit and in_digit) {
            if (n == runs.len) break;
            runs[n] = i;
            n += 1;
            in_digit = false;
        }
    }
    if (in_digit and n < runs.len) {
        runs[n] = name.len;
        n += 1;
    }
    if (n < 2) return error.NoVolumeNumber;

    var lo = runs[n - 2];
    var hi = runs[n - 1];
    if (n >= 4) {
        const a_start = runs[n - 4];
        const a_end = runs[n - 3];
        const b_start = runs[n - 2];
        const between = name[a_end..b_start];
        if (std.mem.indexOfScalar(u8, between, '.') != null or
            std.mem.indexOfScalar(u8, name[0..a_start], '.') == null)
        {
            lo = b_start;
            hi = runs[n - 1];
        } else {
            lo = a_start;
            hi = a_end;
        }
    }

    const width = hi - lo;
    const value = std.fmt.parseUnsigned(u64, name[lo..hi], 10) catch 0;
    var digits: [20]u8 = undefined;
    const printed = std.fmt.bufPrint(&digits, "{d}", .{value + 1}) catch return error.NameTooLong;
    const pad = if (printed.len < width) width - printed.len else 0;

    const total = lo + pad + printed.len + (name.len - hi);
    if (total > out.len) return error.NameTooLong;
    var w: usize = 0;
    @memcpy(out[w..][0..lo], name[0..lo]);
    w += lo;
    @memset(out[w..][0..pad], '0');
    w += pad;
    @memcpy(out[w..][0..printed.len], printed);
    w += printed.len;
    @memcpy(out[w..][0 .. name.len - hi], name[hi..]);
    w += name.len - hi;
    return out[0..w];
}

/// Old-scheme successor: `.rar` → `.r00` → `.r01` → … → `.r99` →
/// `.s00`. Anything whose extension is not `<letter><digit><digit>`
/// restarts at `<first letter of the extension>00`.
pub fn nextOldName(name: []const u8, out: []u8) Error![]u8 {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return error.NoVolumeNumber;
    const ext = name[dot + 1 ..];
    if (name.len + 4 > out.len) return error.NameTooLong;

    // `.rar` (or any extension whose 2nd/3rd byte is not a digit) becomes
    // `.r00`: keep the first extension byte, drop the rest.
    if (ext.len < 3 or !isDigit(ext[1]) or !isDigit(ext[2])) {
        if (ext.len < 1) return error.NoVolumeNumber;
        const head = name[0 .. dot + 2];
        @memcpy(out[0..head.len], head);
        out[head.len] = '0';
        out[head.len + 1] = '0';
        return out[0 .. head.len + 2];
    }

    var e: [3]u8 = .{ ext[0], ext[1], ext[2] };
    var j: usize = 3;
    while (j > 0) {
        j -= 1;
        if (e[j] != '9') {
            e[j] += 1;
            break;
        }
        // Digit overflow. The leftmost position is a letter, so `.r99`
        // rolls into `.s00`.
        if (j == 0) {
            e[j] = 'A';
        } else {
            e[j] = '0';
        }
    }
    const head = name[0 .. dot + 1];
    @memcpy(out[0..head.len], head);
    @memcpy(out[head.len..][0..3], &e);
    return out[0 .. head.len + 3];
}

/// Normalises the entry-point name before successors are derived, the
/// way rardecode's `fixFileExtension` does: a missing extension gets
/// `.rar`, and a self-extracting archive's `.exe` / `.sfx` is replaced
/// so the successor logic sees the archive extension it expects.
pub fn fixExtension(name: []const u8, out: []u8) Error![]u8 {
    if (name.len + 4 > out.len) return error.NameTooLong;
    const dot = std.mem.lastIndexOfScalar(u8, name, '.');
    if (dot == null or dot.? < lastSeparator(name)) {
        @memcpy(out[0..name.len], name);
        @memcpy(out[name.len..][0..4], ".rar");
        return out[0 .. name.len + 4];
    }
    const ext = name[dot.? + 1 ..];
    if (ext.len == 0 or eqlIgnoreCase(ext, "exe") or eqlIgnoreCase(ext, "sfx")) {
        const head = name[0 .. dot.? + 1];
        @memcpy(out[0..head.len], head);
        @memcpy(out[head.len..][0..3], "rar");
        return out[0 .. head.len + 3];
    }
    @memcpy(out[0..name.len], name);
    return out[0..name.len];
}

pub fn basename(p: []const u8) []const u8 {
    const i = lastSeparator(p);
    return if (i == 0 and (p.len == 0 or p[0] != '/')) p else p[i..];
}

/// Directory part of `p`, including the trailing separator, or "".
pub fn dirname(p: []const u8) []const u8 {
    const i = std.mem.lastIndexOfScalar(u8, p, '/') orelse return "";
    return p[0 .. i + 1];
}

fn lastSeparator(p: []const u8) usize {
    const i = std.mem.lastIndexOfScalar(u8, p, '/') orelse return 0;
    return i + 1;
}

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
}

fn endsWithIgnoreCase(haystack: []const u8, suffix: []const u8) bool {
    if (haystack.len < suffix.len) return false;
    return eqlIgnoreCase(haystack[haystack.len - suffix.len ..], suffix);
}

// ---------------------------------------------------------------------
// Tests
//
// The `pickFirst` cases are a direct translation of Go's
// `TestPickFirstVolume`, so the two implementations are known to agree.
// ---------------------------------------------------------------------

const t = std.testing;

test "pickFirst matches the Go implementation" {
    try t.expectEqualStrings("/tmp/a.rar", pickFirst(&.{"/tmp/a.rar"}).?);
    try t.expectEqualStrings("/tmp/x.part1.rar", pickFirst(&.{
        "/tmp/x.part2.rar", "/tmp/x.part1.rar", "/tmp/x.part3.rar",
    }).?);
    try t.expectEqualStrings("/tmp/x.part01.rar", pickFirst(&.{
        "/tmp/x.part02.rar", "/tmp/x.part01.rar",
    }).?);
    try t.expectEqualStrings("/tmp/x.rar", pickFirst(&.{
        "/tmp/x.r02", "/tmp/x.rar", "/tmp/x.r01",
    }).?);
    try t.expectEqualStrings("/tmp/aaa.rar", pickFirst(&.{
        "/tmp/zzz.rar", "/tmp/aaa.rar",
    }).?);
    try t.expectEqualStrings("/tmp/x.r01", pickFirst(&.{
        "/tmp/x.r02", "/tmp/x.r01",
    }).?);
    try t.expectEqual(@as(?[]const u8, null), pickFirst(&.{}));

    // Case-insensitive, because Windows-made posts exist.
    try t.expectEqualStrings("/tmp/X.PART01.RAR", pickFirst(&.{
        "/tmp/X.PART02.RAR", "/tmp/X.PART01.RAR",
    }).?);
}

test "isFirstNewScheme" {
    try t.expect(isFirstNewScheme("a/b/x.part01.rar"));
    try t.expect(isFirstNewScheme("x.part1.rar"));
    try t.expect(isFirstNewScheme("x.part001.rar"));
    try t.expect(!isFirstNewScheme("x.part02.rar"));
    try t.expect(!isFirstNewScheme("x.rar"));
}

fn expectNextNew(want: []const u8, name: []const u8) !void {
    var buf: [max_name_len]u8 = undefined;
    try t.expectEqualStrings(want, try nextNewName(name, &buf));
}

test "nextNewName increments the volume run and keeps its width" {
    try expectNextNew("x.part02.rar", "x.part01.rar");
    try expectNextNew("x.part10.rar", "x.part09.rar");
    try expectNextNew("x.part100.rar", "x.part099.rar");
    // Width grows only when the number outgrows it.
    try expectNextNew("x.part100.rar", "x.part99.rar");
    try expectNextNew("x.part2.rar", "x.part1.rar");
    // A digit run in the release name must not be mistaken for the
    // volume number.
    try expectNextNew("Show.2024.part04.rar", "Show.2024.part03.rar");
    try expectNextNew("S01E05.part11.rar", "S01E05.part10.rar");
    // `name.part003of057.rar`: the first of the pair is the volume.
    try expectNextNew("name.part004of057.rar", "name.part003of057.rar");
    // Path prefixes are preserved.
    try expectNextNew("/data/x.part03.rar", "/data/x.part02.rar");

    var buf: [max_name_len]u8 = undefined;
    try t.expectError(error.NoVolumeNumber, nextNewName("x.rar", &buf));
}

fn expectNextOld(want: []const u8, name: []const u8) !void {
    var buf: [max_name_len]u8 = undefined;
    try t.expectEqualStrings(want, try nextOldName(name, &buf));
}

test "nextOldName walks .rar then .r00 upwards" {
    try expectNextOld("x.r00", "x.rar");
    try expectNextOld("x.r01", "x.r00");
    try expectNextOld("x.r10", "x.r09");
    try expectNextOld("x.s00", "x.r99");
    try expectNextOld("x.t00", "x.s99");
    // Non-numeric extension restarts the sequence.
    try expectNextOld("x.z00", "x.zip");
    try expectNextOld("/data/x.r00", "/data/x.rar");

    var buf: [max_name_len]u8 = undefined;
    try t.expectError(error.NoVolumeNumber, nextOldName("noextension", &buf));
}

test "fixExtension normalises the entry point" {
    var buf: [max_name_len]u8 = undefined;
    try t.expectEqualStrings("x.rar", try fixExtension("x.rar", &buf));
    try t.expectEqualStrings("x.rar", try fixExtension("x.exe", &buf));
    try t.expectEqualStrings("x.rar", try fixExtension("x.SFX", &buf));
    try t.expectEqualStrings("x.rar", try fixExtension("x.", &buf));
    try t.expectEqualStrings("noext.rar", try fixExtension("noext", &buf));
    try t.expectEqualStrings("x.part01.rar", try fixExtension("x.part01.rar", &buf));
    // A dot in a parent directory is not an extension.
    try t.expectEqualStrings("a.b/x.rar", try fixExtension("a.b/x", &buf));
}

test "basename and dirname" {
    try t.expectEqualStrings("x.rar", basename("/a/b/x.rar"));
    try t.expectEqualStrings("x.rar", basename("x.rar"));
    try t.expectEqualStrings("/a/b/", dirname("/a/b/x.rar"));
    try t.expectEqualStrings("", dirname("x.rar"));
}
