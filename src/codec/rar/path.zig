//! Archive-entry name sanitisation.
//!
//! This is the security boundary of the whole module. An archive entry
//! name is attacker-controlled text that we are about to turn into a
//! path and write to. `../../../etc/cron.d/x`, `/etc/passwd`,
//! `C:\Windows\System32\x`, or a name with an embedded NUL are all a
//! remote write primitive if any of them survives to `createFile`.
//!
//! The policy is *reject*, not *repair*. The Go implementation cleaned
//! the path and then checked the result stayed under the target
//! directory; cleaning is where the interesting bugs live (`a/..%2f..`,
//! `..\` on a mixed-separator name, `....//`), so instead:
//!
//!   * any `..` component is a hard error — no resolution, no rewriting.
//!     Real archives from real packers never contain one.
//!   * a leading separator is a hard error.
//!   * a `<letter>:` component prefix is a hard error, so a Windows
//!     absolute path can never be reinterpreted as a relative one.
//!   * bytes below 0x20 and 0x7f are a hard error. That covers NUL,
//!     which would truncate the path at the syscall boundary and make
//!     `a\x00/../../x` pass a naive component check.
//!   * `\` is folded to `/` *before* components are examined, because
//!     RAR3 uses `\` as its separator and a Windows-packed RAR5 entry
//!     can carry either.
//!
//! What survives is a relative, `/`-separated, `.`-free, NFC-agnostic
//! path with no empty components. Callers still open the target
//! directory with `resolve_beneath` where the OS supports it; that is
//! defence in depth, not a substitute for this.
//!
//! Not enforced, because we only ever ship Linux containers: Windows
//! reserved device names (`CON`, `NUL`, `LPT1`), trailing dots and
//! trailing spaces. They are legal POSIX filenames and rejecting them
//! would break real releases.

const std = @import("std");

pub const Error = error{
    /// Nothing left after normalisation: "", ".", "./", "//".
    EmptyName,
    NameTooLong,
    ComponentTooLong,
    /// Starts with a separator.
    AbsolutePath,
    /// Contains a `..` component.
    ParentTraversal,
    /// A component starts with `<ascii letter>:`, i.e. a Windows drive
    /// or an NTFS alternate data stream reference.
    DriveLetter,
    /// A byte below 0x20, or 0x7f. NUL is the dangerous one.
    ControlCharacter,
    /// RAR5 declares names to be UTF-8; RAR3 names are transcoded to it.
    /// Either way an invalid sequence means the name is not what the
    /// packer thought it was, so it is not safe to interpret.
    InvalidUtf8,
};

/// Longest accepted whole path. Linux `PATH_MAX` is 4096 including the
/// NUL, and we still have to prepend a target directory, so cap the
/// entry name well below it.
pub const max_len = 1024;

/// Longest accepted single component. Linux `NAME_MAX`.
pub const max_component_len = 255;

/// Normalises `raw` into `out` and returns the used prefix of `out`.
/// `out` must be at least `raw.len` bytes; normalisation never grows a
/// name.
pub fn sanitise(raw: []const u8, out: []u8) Error![]u8 {
    if (raw.len == 0) return error.EmptyName;
    if (raw.len > max_len) return error.NameTooLong;
    std.debug.assert(out.len >= raw.len);

    for (raw) |c| {
        if (c < 0x20 or c == 0x7f) return error.ControlCharacter;
    }
    if (!std.unicode.utf8ValidateSlice(raw)) return error.InvalidUtf8;

    // Separator folding happens first so that everything below sees one
    // separator, and so `..\x` cannot slip past a `/`-only split.
    if (raw[0] == '/' or raw[0] == '\\') return error.AbsolutePath;

    var len: usize = 0;
    var it = ComponentIterator{ .rest = raw };
    while (it.next()) |comp| {
        // Empty (`//`) and `.` components carry no meaning; drop them
        // rather than erroring, since packers emit `./name` freely.
        if (comp.len == 0 or std.mem.eql(u8, comp, ".")) continue;
        if (std.mem.eql(u8, comp, "..")) return error.ParentTraversal;
        if (comp.len > max_component_len) return error.ComponentTooLong;
        if (comp.len >= 2 and std.ascii.isAlphabetic(comp[0]) and comp[1] == ':') {
            return error.DriveLetter;
        }
        if (len != 0) {
            out[len] = '/';
            len += 1;
        }
        @memcpy(out[len..][0..comp.len], comp);
        len += comp.len;
    }
    if (len == 0) return error.EmptyName;
    return out[0..len];
}

/// Splits on both separators. RAR3 stores `\`; RAR5 stores `/` but
/// Windows packers have been observed emitting `\` there too.
const ComponentIterator = struct {
    rest: []const u8,
    done: bool = false,

    fn next(it: *ComponentIterator) ?[]const u8 {
        if (it.done) return null;
        var i: usize = 0;
        while (i < it.rest.len) : (i += 1) {
            if (it.rest[i] == '/' or it.rest[i] == '\\') {
                const out = it.rest[0..i];
                it.rest = it.rest[i + 1 ..];
                return out;
            }
        }
        it.done = true;
        return it.rest;
    }
};

/// The parent directory portion of an already-sanitised name, or null
/// when the name is a bare filename. Borrowed from `name`.
pub fn parent(name: []const u8) ?[]const u8 {
    const i = std.mem.lastIndexOfScalar(u8, name, '/') orelse return null;
    return name[0..i];
}

// ---------------------------------------------------------------------
// Tests
//
// The hostile cases are the point of this file, so they come first and
// they are exhaustive rather than representative.
// ---------------------------------------------------------------------

const t = std.testing;

fn expectSanitised(want: []const u8, raw: []const u8) !void {
    var buf: [max_len]u8 = undefined;
    try t.expectEqualStrings(want, try sanitise(raw, &buf));
}

fn expectRejected(want: Error, raw: []const u8) !void {
    var buf: [max_len]u8 = undefined;
    try t.expectError(want, sanitise(raw, &buf));
}

test "path traversal is rejected, never repaired" {
    try expectRejected(error.ParentTraversal, "..");
    try expectRejected(error.ParentTraversal, "../x");
    try expectRejected(error.ParentTraversal, "../../etc/passwd");
    try expectRejected(error.ParentTraversal, "../../../../../../etc/cron.d/pwn");
    try expectRejected(error.ParentTraversal, "a/../b");
    try expectRejected(error.ParentTraversal, "a/../../b");
    try expectRejected(error.ParentTraversal, "a/b/..");
    try expectRejected(error.ParentTraversal, "./../x");
    // Backslash separators must not hide a traversal.
    try expectRejected(error.ParentTraversal, "..\\x");
    try expectRejected(error.ParentTraversal, "a\\..\\..\\b");
    try expectRejected(error.ParentTraversal, "a/..\\b");
    // Mixed with redundant components.
    try expectRejected(error.ParentTraversal, "a/.././b");
    try expectRejected(error.ParentTraversal, "a//..//b");

    // `...` and `..a` are ordinary names, not traversals.
    try expectSanitised("...", "...");
    try expectSanitised("..a", "..a");
    try expectSanitised("a/...b", "a/...b");
    // Four dots with a separator: components are `....` and `` — legal,
    // because nothing here resolves anything.
    try expectSanitised("....", "....//");
}

test "absolute paths are rejected in every spelling" {
    try expectRejected(error.AbsolutePath, "/etc/passwd");
    try expectRejected(error.AbsolutePath, "/");
    try expectRejected(error.AbsolutePath, "//server/share/x");
    try expectRejected(error.AbsolutePath, "\\etc\\passwd");
    try expectRejected(error.AbsolutePath, "\\\\server\\share\\x");
    try expectRejected(error.AbsolutePath, "/../x");
}

test "windows drive letters are rejected" {
    try expectRejected(error.DriveLetter, "C:\\Windows\\System32\\drivers\\etc\\hosts");
    try expectRejected(error.DriveLetter, "c:/windows/x");
    try expectRejected(error.DriveLetter, "C:x");
    // A drive letter reintroduced deeper in the path is just as bad:
    // `join(target, "a/C:/x")` is harmless on POSIX but the name is a
    // lie, and on any path-normalising consumer downstream it is not.
    try expectRejected(error.DriveLetter, "a/C:/x");
    try expectRejected(error.DriveLetter, "a\\D:x");
    // NTFS alternate data stream syntax lands in the same trap.
    try expectRejected(error.DriveLetter, "a/b:stream");

    // A colon that is not a drive-letter prefix stays legal: it is a
    // valid POSIX filename byte and appears in real release names.
    try expectSanitised("Show - 01:30.mkv", "Show - 01:30.mkv");
    try expectSanitised("a/1:2", "a/1:2");
}

test "control characters and NUL are rejected" {
    try expectRejected(error.ControlCharacter, "a\x00b");
    // The classic: NUL-truncation would make the syscall see "a" while
    // a component check saw a harmless-looking tail.
    try expectRejected(error.ControlCharacter, "a\x00/../../etc/passwd");
    try expectRejected(error.ControlCharacter, "\x00");
    try expectRejected(error.ControlCharacter, "a\nb");
    try expectRejected(error.ControlCharacter, "a\rb");
    try expectRejected(error.ControlCharacter, "a\tb");
    try expectRejected(error.ControlCharacter, "a\x1bb");
    try expectRejected(error.ControlCharacter, "a\x7fb");
}

test "invalid UTF-8 is rejected" {
    try expectRejected(error.InvalidUtf8, "a\xffb");
    try expectRejected(error.InvalidUtf8, "\xc3");
    // Overlong encoding of '/' — the classic separator smuggle.
    try expectRejected(error.InvalidUtf8, "a\xc0\xafb");
    // Lone surrogate half.
    try expectRejected(error.InvalidUtf8, "a\xed\xa0\x80b");

    // Well-formed multi-byte names pass through untouched.
    try expectSanitised("Wüstenplanet.mkv", "Wüstenplanet.mkv");
    try expectSanitised("日本語/字幕.srt", "日本語\\字幕.srt");
}

test "length limits" {
    var long: [max_len + 1]u8 = @splat('a');
    try expectRejected(error.NameTooLong, &long);

    var comp: [max_component_len + 1]u8 = @splat('b');
    try expectRejected(error.ComponentTooLong, &comp);

    // Exactly at the component limit is fine.
    var ok: [max_component_len]u8 = @splat('c');
    try expectSanitised(&ok, &ok);
}

test "empty results are rejected" {
    try expectRejected(error.EmptyName, "");
    try expectRejected(error.EmptyName, ".");
    try expectRejected(error.EmptyName, "./");
    try expectRejected(error.EmptyName, "./.");
    try expectRejected(error.EmptyName, "././/.");
}

test "ordinary names normalise" {
    try expectSanitised("file.mkv", "file.mkv");
    try expectSanitised("dir/file.mkv", "dir/file.mkv");
    try expectSanitised("dir/file.mkv", "dir\\file.mkv");
    try expectSanitised("dir/file.mkv", "./dir/./file.mkv");
    try expectSanitised("dir/file.mkv", "dir//file.mkv");
    try expectSanitised("a/b/c/d.txt", "a/b//c/./d.txt");
    // A trailing separator (directory entries carry them) is dropped.
    try expectSanitised("dir", "dir/");
    try expectSanitised("a/b", "a/b/");
    try expectSanitised("Some.Release.2024/Sample/sample.mkv", "Some.Release.2024\\Sample\\sample.mkv");
}

test "parent" {
    try t.expectEqual(@as(?[]const u8, null), parent("file.mkv"));
    try t.expectEqualStrings("dir", parent("dir/file.mkv").?);
    try t.expectEqualStrings("a/b", parent("a/b/c").?);
}
