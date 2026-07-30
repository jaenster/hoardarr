//! What happens to a release directory after the files land in it:
//! rename an obfuscated main file, strip sample clips, and flatten a
//! redundant outer folder.
//!
//! Order matters and is not arbitrary. Deobfuscation runs first, because
//! its "largest file, by a wide margin" heuristic needs the original
//! layout — strip the samples first and a small release can suddenly look
//! like it has one dominant file when it does not. Sample removal runs
//! second, which frees up the "one sub-directory, no top-level files"
//! shape that collapse looks for. Collapse runs last.
//!
//! Every step is best-effort. The files are already in `complete/` and
//! the move was the point; a failed rename is a cosmetic problem, not a
//! reason to fail a delivery.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const naming = @import("../naming.zig");
const obf = @import("obfuscation.zig");

const Allocator = std.mem.Allocator;
const Filesystem = app_ports.Filesystem;
const FsError = app_ports.FsError;

/// Floor on the size of a "largest data file" before the fallback rename
/// will touch it. Sidecars, metadata and short clips are not load-bearing
/// enough to risk renaming.
pub const min_rename_size: i64 = 10 * 1024 * 1024;

/// The largest file must be at least this many times the second largest.
/// A release with several comparable data files is an episode pack, and
/// renaming the biggest to the job name would either collide with its
/// siblings or destroy episode-level naming.
pub const rename_ratio: f64 = 3.0;

pub const Entry = struct {
    /// Full path. Owned by the allocator passed to `listDataFiles`.
    path: []const u8,
    size: i64,
};

/// Every regular file under `dir`, recursively, minus the extensions the
/// rename must never touch.
pub fn listDataFiles(a: Allocator, fs: Filesystem, dir: []const u8) FsError![]Entry {
    var out: std.ArrayList(Entry) = .empty;
    errdefer out.deinit(a);
    try walk(a, fs, dir, &out);
    return out.toOwnedSlice(a);
}

fn walk(a: Allocator, fs: Filesystem, dir: []const u8, out: *std.ArrayList(Entry)) FsError!void {
    const kids = fs.list(a, dir) catch |e| switch (e) {
        // A directory that vanished mid-walk is not worth failing over.
        error.NotFound, error.NotDirectory => return,
        else => return e,
    };
    for (kids) |k| {
        const p = try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, k.name });
        if (k.is_dir) {
            try walk(a, fs, p, out);
            continue;
        }
        if (obf.isExcludedExt(k.name)) continue;
        try out.append(a, .{ .path = p, .size = k.size });
    }
}

/// Strips a PAR2 recovery-volume suffix. Covers every convention seen in
/// the wild: `foo.vol000+01`, `foo.vol000-001`, `foo.vol-01`, `foo.vol01`.
///
/// Go used `\.vol\d+([+-]\d+)?$|\.vol-?\d+$`; this is the same grammar
/// spelled out, because a regex engine is a large dependency for one
/// pattern.
pub fn trimVolSuffix(s: []const u8) []const u8 {
    var i = s.len;
    while (i > 0) {
        i -= 1;
        if (i + 4 > s.len) continue;
        if (!std.ascii.eqlIgnoreCase(s[i..][0..4], ".vol")) continue;
        var rest = s[i + 4 ..];
        // An optional leading '-' before the first number: `.vol-01`.
        if (rest.len > 0 and rest[0] == '-') rest = rest[1..];
        const first = digitRun(rest);
        if (first == 0) continue;
        rest = rest[first..];
        if (rest.len == 0) return s[0..i];
        // `+NN` or `-NNN` tail.
        if (rest[0] == '+' or rest[0] == '-') {
            const second = digitRun(rest[1..]);
            if (second != 0 and second + 1 == rest.len) return s[0..i];
        }
    }
    return s;
}

fn digitRun(s: []const u8) usize {
    var n: usize = 0;
    while (n < s.len and std.ascii.isDigit(s[n])) n += 1;
    return n;
}

/// The release-name prefix a PAR2 set's filenames share, or "" when they
/// disagree or there are none.
///
/// Obfuscated releases routinely have an obfuscated NZB-level name — the
/// bot that posted them strips meaning from the binary parts' filenames —
/// but keep the canonical release name in the PAR2 filenames, because
/// that is the recovery-set name the producer used at `par2create` time.
/// SABnzbd exploits this; so do we.
///
/// The returned slice borrows from `par2_filenames`, in the original
/// case: matching is case-insensitive but the answer is what the poster
/// actually wrote.
pub fn par2SetName(par2_filenames: []const []const u8) []const u8 {
    var prefix: ?[]const u8 = null;
    for (par2_filenames) |n| {
        const base = std.fs.path.basename(n);
        if (!std.ascii.endsWithIgnoreCase(base, ".par2")) continue;
        const stem = base[0 .. base.len - 5];
        const trimmed = trimVolSuffix(stem);
        if (trimmed.len == 0) continue;
        if (prefix) |p| {
            // Any disagreement and we do not trust the inference at all.
            if (!std.ascii.eqlIgnoreCase(p, trimmed)) return "";
        } else {
            prefix = trimmed;
        }
    }
    return prefix orelse "";
}

/// The result of a deobfuscation pass.
pub const RenameResult = struct {
    /// The new path, or "" when nothing was renamed.
    renamed_to: []const u8 = "",

    pub fn happened(self: RenameResult) bool {
        return self.renamed_to.len != 0;
    }
};

/// Renames the dominant data file to a human-readable name, when every
/// guard agrees it is safe.
///
/// The guards, in order, and why each exists:
///
///   * disc structure — renaming inside `VIDEO_TS` destroys the disc
///   * nothing to rename — an empty directory
///   * 10 MiB floor — sidecars are never "the main file"
///   * excluded extension — archive parts and parity are off-limits
///   * 3× ratio over the runner-up — an episode pack has no main file
///   * the current name must look obfuscated — hand-named files pass
///
/// The target name is the PAR2 set name when that is itself
/// human-shaped, otherwise the NZB-derived job name.
pub fn deobfuscateRename(
    a: Allocator,
    fs: Filesystem,
    logger: *log.Logger,
    dir: []const u8,
    job_name: []const u8,
    par2_set_name: []const u8,
) FsError!RenameResult {
    if (obf.isDiscStructure(fs, dir)) {
        logger.debug("deobfuscate: disc structure detected, skipping", &.{log.str("dir", dir)});
        return .{};
    }
    const entries = try listDataFiles(a, fs, dir);
    if (entries.len == 0) return .{};

    var largest = entries[0];
    var second: Entry = .{ .path = "", .size = 0 };
    for (entries[1..]) |e| {
        if (e.size > largest.size) {
            second = largest;
            largest = e;
        } else if (e.size > second.size) {
            second = e;
        }
    }

    if (largest.size < min_rename_size) {
        logger.debug("deobfuscate: largest under min size, skipping", &.{
            log.str("dir", dir),
            log.int("size", largest.size),
        });
        return .{};
    }
    if (obf.isExcludedExt(largest.path)) return .{};
    if (second.size > 0) {
        const ratio = @as(f64, @floatFromInt(largest.size)) / @as(f64, @floatFromInt(second.size));
        if (ratio < rename_ratio) {
            logger.debug("deobfuscate: multiple comparable files, skipping", &.{
                log.str("dir", dir),
            });
            return .{};
        }
    }
    if (!obf.isProbablyObfuscated(std.fs.path.basename(largest.path))) {
        logger.debug("deobfuscate: largest looks hand-named, skipping", &.{
            log.str("dir", dir),
        });
        return .{};
    }

    // The PAR2 set name is the better label when it is not itself
    // obfuscated — an obfuscated release usually has both an obfuscated
    // NZB name and obfuscated filenames, but a real name in its parity.
    const target_name = if (par2_set_name.len != 0 and !obf.isProbablyObfuscated(par2_set_name))
        par2_set_name
    else
        job_name;

    var name_buf: [naming.max_component]u8 = undefined;
    const safe = naming.sanitizeFilename(&name_buf, target_name);
    const ext = obf.extensionOf(largest.path);
    const parent = std.fs.path.dirname(largest.path) orelse dir;
    const target = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ parent, safe, ext });

    if (std.mem.eql(u8, target, largest.path)) return .{};
    if (fs.exists(target)) {
        logger.warn("deobfuscate: target name already exists, skipping", &.{
            log.str("dir", dir),
        });
        return .{};
    }
    try fs.move(largest.path, target);
    logger.info("deobfuscate: renamed obfuscated file", &.{ log.str("dir", dir) });
    return .{ .renamed_to = target };
}

/// Whether the path (relative to the release root) matches SABnzbd's
/// sample/proof pattern: `(^|[\W_])(sample|proof)`, case-insensitive.
///
/// Anchoring to a non-word boundary is what keeps "examplefoo.mkv" out of
/// it while catching "movie-sample.mkv" and "Sample/preview.mkv".
pub fn looksLikeSample(rel_path: []const u8) bool {
    for ([_][]const u8{ "sample", "proof" }) |needle| {
        var i: usize = 0;
        while (i + needle.len <= rel_path.len) : (i += 1) {
            if (!std.ascii.eqlIgnoreCase(rel_path[i..][0..needle.len], needle)) continue;
            if (i == 0) return true;
            if (!std.ascii.isAlphanumeric(rel_path[i - 1])) return true;
        }
    }
    return false;
}

/// Deletes sample and proof clips, then sweeps any directory the removal
/// emptied.
///
/// The safety check is SABnzbd's: if *every* file matches, the release is
/// genuinely called something like "sample-pack" and deleting the lot
/// would destroy it. Refuse.
///
/// Returns the number of files removed.
pub fn removeSamples(
    a: Allocator,
    fs: Filesystem,
    logger: *log.Logger,
    dir: []const u8,
) FsError!usize {
    var all: std.ArrayList([]const u8) = .empty;
    defer all.deinit(a);
    var matches: std.ArrayList([]const u8) = .empty;
    defer matches.deinit(a);
    try collectFiles(a, fs, dir, dir, &all, &matches);

    if (matches.items.len == 0) return 0;
    if (matches.items.len == all.items.len) {
        logger.info("samples: all files matched sample regex, refusing to nuke release", &.{
            log.str("dir", dir),
            log.uint("count", matches.items.len),
        });
        return 0;
    }
    var removed: usize = 0;
    for (matches.items) |p| {
        fs.remove(p) catch |e| {
            logger.warn("samples: remove failed", &.{ log.str("file", p), log.errv("err", e) });
            continue;
        };
        removed += 1;
    }
    try sweepEmptyDirs(a, fs, dir, dir);
    return removed;
}

/// Collects every file under `dir`, and separately those whose path
/// *relative to the release root* matches. Matching on the relative path
/// is what catches `Sample/preview.mkv`, where the file itself has a
/// neutral name but lives in a sample folder.
fn collectFiles(
    a: Allocator,
    fs: Filesystem,
    root: []const u8,
    dir: []const u8,
    all: *std.ArrayList([]const u8),
    matches: *std.ArrayList([]const u8),
) FsError!void {
    const kids = fs.list(a, dir) catch |e| switch (e) {
        error.NotFound, error.NotDirectory => return,
        else => return e,
    };
    for (kids) |k| {
        const p = try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, k.name });
        if (k.is_dir) {
            try collectFiles(a, fs, root, p, all, matches);
            continue;
        }
        try all.append(a, p);
        const rel = if (p.len > root.len + 1) p[root.len + 1 ..] else p;
        if (looksLikeSample(rel)) try matches.append(a, p);
    }
}

/// Removes directories left empty by the sample sweep, deepest first.
fn sweepEmptyDirs(a: Allocator, fs: Filesystem, root: []const u8, dir: []const u8) FsError!void {
    const kids = fs.list(a, dir) catch return;
    for (kids) |k| {
        if (!k.is_dir) continue;
        const p = try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, k.name });
        try sweepEmptyDirs(a, fs, root, p);
    }
    if (std.mem.eql(u8, dir, root)) return;
    const after = fs.list(a, dir) catch return;
    if (after.len == 0) fs.remove(dir) catch {};
}

/// Lifts `dir/inner/...` up to `dir/...` when `dir` holds exactly one
/// sub-directory and no top-level files.
///
/// This removes the redundant outer wrap some releases ship inside the
/// NZB. Anything else about the shape — two sub-directories, a top-level
/// file, a name collision — and it silently does nothing, because
/// guessing here means moving a user's files somewhere they did not ask
/// for.
///
/// Returns the number of entries lifted.
pub fn collapseSingleFolder(
    a: Allocator,
    fs: Filesystem,
    logger: *log.Logger,
    dir: []const u8,
) FsError!usize {
    const top = try fs.list(a, dir);
    var subdir: ?[]const u8 = null;
    for (top) |e| {
        // Any top-level file disqualifies the collapse.
        if (!e.is_dir) return 0;
        if (subdir != null) return 0;
        subdir = e.name;
    }
    const name = subdir orelse return 0;

    const inner_path = try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, name });
    const inner = try fs.list(a, inner_path);

    // Check every destination first: a partial collapse is worse than
    // none, because the operator is left with the release split across
    // two directories.
    for (inner) |e| {
        const dst = try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, e.name });
        if (fs.exists(dst)) {
            logger.warn("collapse: target name collision, skipping", &.{
                log.str("dir", dir),
                log.str("name", e.name),
            });
            return 0;
        }
    }
    for (inner) |e| {
        const src = try std.fmt.allocPrint(a, "{s}/{s}", .{ inner_path, e.name });
        const dst = try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, e.name });
        try fs.move(src, dst);
    }
    fs.remove(inner_path) catch |e| {
        logger.warn("collapse: remove inner failed", &.{
            log.str("dir", inner_path),
            log.errv("err", e),
        });
        return inner.len;
    };
    logger.info("collapse: flattened single-folder release", &.{
        log.str("dir", dir),
        log.uint("files", inner.len),
    });
    return inner.len;
}

// =====================================================================
// Tests — internal/app/deliver/postprocess_test.go
// =====================================================================

const testing = std.testing;

const Fixture = struct {
    fs: app_ports.FakeFs,
    arena: std.heap.ArenaAllocator,
    logger: log.Logger = .{},

    fn init(self: *Fixture) void {
        self.* = .{
            .fs = app_ports.FakeFs.init(testing.allocator),
            .arena = std.heap.ArenaAllocator.init(testing.allocator),
        };
    }

    fn deinit(self: *Fixture) void {
        self.arena.deinit();
        self.fs.deinit();
    }

    fn a(self: *Fixture) Allocator {
        return self.arena.allocator();
    }

    fn f(self: *Fixture) Filesystem {
        return self.fs.filesystem();
    }
};

const mib = 1024 * 1024;

test "an obfuscated dominant file is renamed to the job name" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/abcdef1234567890abcdef1234567890.mkv", 12 * mib);
    try fx.fs.addFile("/rel/info.txt", 200);

    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Great.Movie.2020", "");
    try testing.expect(r.happened());
    try testing.expectEqualStrings("/rel/Great.Movie.2020.mkv", r.renamed_to);
    try testing.expect(fx.fs.has("/rel/Great.Movie.2020.mkv"));
    try testing.expect(!fx.fs.has("/rel/abcdef1234567890abcdef1234567890.mkv"));
}

test "a hand-named dominant file is left alone" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/Some.Movie.2020.1080p.x264.mkv", 12 * mib);

    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Other.Name", "");
    try testing.expect(!r.happened());
    try testing.expect(fx.fs.has("/rel/Some.Movie.2020.1080p.x264.mkv"));
}

test "a disc structure blocks the rename entirely" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addDir("/rel/VIDEO_TS");
    try fx.fs.addFile("/rel/VIDEO_TS/abcdef1234567890abcdef1234567890.vob", 12 * mib);

    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Original.Movie", "");
    try testing.expect(!r.happened());
}

test "comparable siblings block the rename" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    // An episode pack: two roughly equal files and no main one.
    try fx.fs.addFile("/rel/abcdef1234567890abcdef1234567890.mkv", 12 * mib);
    try fx.fs.addFile("/rel/fedcba0987654321fedcba0987654321.mkv", 11 * mib);

    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Series.Pack", "");
    try testing.expect(!r.happened());
}

test "a file under the size floor is never renamed" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/abcdef1234567890abcdef1234567890.mkv", 5 * mib);
    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Tiny", "");
    try testing.expect(!r.happened());
}

test "the PAR2 set name wins over an obfuscated job name" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/abcdef1234567890abcdef1234567890.mkv", 12 * mib);

    const r = try deobfuscateRename(
        fx.a(),
        fx.f(),
        &fx.logger,
        "/rel",
        // An obfuscated NZB-level name…
        "xB9UmnVrVGWCcoAXsTktt8alQBewFvZH",
        // …and the real release name, preserved in the parity filenames.
        "Chicago.Med.S11E21.XviD-AFG",
    );
    try testing.expectEqualStrings("/rel/Chicago.Med.S11E21.XviD-AFG.mkv", r.renamed_to);
}

test "an existing target name blocks the rename rather than clobbering" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/abcdef1234567890abcdef1234567890.mkv", 12 * mib);
    try fx.fs.addFile("/rel/Great.Movie.mkv", 1);

    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Great.Movie", "");
    try testing.expect(!r.happened());
    try testing.expect(fx.fs.has("/rel/abcdef1234567890abcdef1234567890.mkv"));
}

test "an empty release directory is a no-op" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addDir("/rel");
    const r = try deobfuscateRename(fx.a(), fx.f(), &fx.logger, "/rel", "Name", "");
    try testing.expect(!r.happened());
}

test "par2SetName extracts the shared prefix across every vol convention" {
    const cases = [_]struct { []const []const u8, []const u8 }{
        .{ &.{
            "Chicago.Med.S11E21.XviD-AFG.par2",
            "Chicago.Med.S11E21.XviD-AFG.vol-01.par2",
            "Chicago.Med.S11E21.XviD-AFG.vol-02.par2",
            "Chicago.Med.S11E21.XviD-AFG.vol-07.par2",
        }, "Chicago.Med.S11E21.XviD-AFG" },
        .{ &.{
            "Some.Release.par2",
            "Some.Release.vol000+01.par2",
            "Some.Release.vol001+02.par2",
        }, "Some.Release" },
        .{ &.{
            "Movie.2020.par2",
            "Movie.2020.vol000-001.par2",
            "Movie.2020.vol002-007.par2",
        }, "Movie.2020" },
        // Disagreement means we do not trust the inference at all.
        .{ &.{ "Show.A.par2", "Show.B.vol-01.par2" }, "" },
        .{ &.{}, "" },
        // An obfuscated set name comes back as-is; the caller filters.
        .{ &.{
            "abcdef1234567890abcdef1234567890.par2",
            "abcdef1234567890abcdef1234567890.vol-01.par2",
        }, "abcdef1234567890abcdef1234567890" },
        // Non-par2 entries are ignored.
        .{ &.{ "movie.mkv", "Rel.par2" }, "Rel" },
    };
    for (cases) |c| {
        try testing.expectEqualStrings(c[1], par2SetName(c[0]));
    }
}

test "trimVolSuffix only strips a real volume suffix" {
    try testing.expectEqualStrings("rel", trimVolSuffix("rel.vol01"));
    try testing.expectEqualStrings("rel", trimVolSuffix("rel.vol-01"));
    try testing.expectEqualStrings("rel", trimVolSuffix("rel.vol000+01"));
    try testing.expectEqualStrings("rel", trimVolSuffix("rel.vol000-001"));
    // Not a volume suffix: no digits, or trailing junk.
    try testing.expectEqualStrings("rel.volume", trimVolSuffix("rel.volume"));
    try testing.expectEqualStrings("rel.vol01x", trimVolSuffix("rel.vol01x"));
    try testing.expectEqualStrings("rel", trimVolSuffix("rel"));
}

test "sample and proof clips are deleted and the empty folder swept" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/movie.mkv", 100);
    try fx.fs.addFile("/rel/movie-sample.mkv", 50);
    try fx.fs.addFile("/rel/Sample/preview.mkv", 50);
    try fx.fs.addFile("/rel/proof.png", 20);
    try fx.fs.addFile("/rel/subs/movie.srt", 30);

    const n = try removeSamples(fx.a(), fx.f(), &fx.logger, "/rel");
    try testing.expectEqual(@as(usize, 3), n);

    try testing.expect(fx.fs.has("/rel/movie.mkv"));
    try testing.expect(fx.fs.has("/rel/subs/movie.srt"));
    try testing.expect(!fx.fs.has("/rel/movie-sample.mkv"));
    try testing.expect(!fx.fs.has("/rel/Sample/preview.mkv"));
    // The folder the clip lived in went with it.
    try testing.expect(!fx.fs.has("/rel/Sample"));
    try testing.expect(!fx.fs.has("/rel/proof.png"));
    // A directory that still holds something is untouched.
    try testing.expect(fx.fs.has("/rel/subs"));
}

test "a release where everything matches is not nuked" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/sample-pack-01.mkv", 100);
    try fx.fs.addFile("/rel/sample-pack-02.mkv", 100);

    try testing.expectEqual(@as(usize, 0), try removeSamples(fx.a(), fx.f(), &fx.logger, "/rel"));
    try testing.expect(fx.fs.has("/rel/sample-pack-01.mkv"));
    try testing.expect(fx.fs.has("/rel/sample-pack-02.mkv"));
}

test "the sample pattern needs a non-word boundary" {
    try testing.expect(looksLikeSample("sample.mkv"));
    try testing.expect(looksLikeSample("movie-sample.mkv"));
    try testing.expect(looksLikeSample("movie_sample.mkv"));
    try testing.expect(looksLikeSample("Sample/preview.mkv"));
    try testing.expect(looksLikeSample("PROOF.png"));
    // The false positive the anchoring exists to prevent.
    try testing.expect(!looksLikeSample("examplefoo.mkv"));
    try testing.expect(!looksLikeSample("resample.mkv"));
    try testing.expect(!looksLikeSample("movie.mkv"));
}

test "a nested release is flattened" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/Release.Inner.Name/movie.mkv", 100);
    try fx.fs.addFile("/rel/Release.Inner.Name/info.nfo", 50);

    try testing.expectEqual(@as(usize, 2), try collapseSingleFolder(fx.a(), fx.f(), &fx.logger, "/rel"));
    try testing.expect(fx.fs.has("/rel/movie.mkv"));
    try testing.expect(fx.fs.has("/rel/info.nfo"));
    try testing.expect(!fx.fs.has("/rel/Release.Inner.Name"));
}

test "two sub-directories are not a redundant wrap" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/a/f.mkv", 100);
    try fx.fs.addFile("/rel/b/g.mkv", 100);

    try testing.expectEqual(@as(usize, 0), try collapseSingleFolder(fx.a(), fx.f(), &fx.logger, "/rel"));
    try testing.expect(fx.fs.has("/rel/a/f.mkv"));
    try testing.expect(fx.fs.has("/rel/b/g.mkv"));
}

test "a top-level file disqualifies the collapse" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/top.mkv", 100);
    try fx.fs.addFile("/rel/inner/nested.mkv", 100);

    try testing.expectEqual(@as(usize, 0), try collapseSingleFolder(fx.a(), fx.f(), &fx.logger, "/rel"));
    try testing.expect(fx.fs.has("/rel/top.mkv"));
    try testing.expect(fx.fs.has("/rel/inner/nested.mkv"));
}

test "a name collision aborts the collapse before moving anything" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/inner/movie.mkv", 100);
    try fx.fs.addFile("/rel/inner/extra.nfo", 10);
    // A pre-existing entry the collapse would have to overwrite. It also
    // makes the top level hold a file, but the collision check is the
    // one being exercised here, so give it a directory instead.
    try fx.fs.addDir("/rel/movie.mkv");

    try testing.expectEqual(@as(usize, 0), try collapseSingleFolder(fx.a(), fx.f(), &fx.logger, "/rel"));
    // Nothing moved: a half-collapsed release is worse than an
    // un-collapsed one.
    try testing.expect(fx.fs.has("/rel/inner/movie.mkv"));
    try testing.expect(fx.fs.has("/rel/inner/extra.nfo"));
}

test "listDataFiles recurses and skips the excluded extensions" {
    var fx: Fixture = undefined;
    fx.init();
    defer fx.deinit();
    try fx.fs.addFile("/rel/movie.mkv", 10);
    try fx.fs.addFile("/rel/rel.par2", 20);
    try fx.fs.addFile("/rel/rel.part01.rar", 30);
    try fx.fs.addFile("/rel/subs/movie.srt", 5);
    try fx.fs.addFile("/rel/extras/behind.mkv", 40);

    const entries = try listDataFiles(fx.a(), fx.f(), "/rel");
    try testing.expectEqual(@as(usize, 2), entries.len);
    var total: i64 = 0;
    for (entries) |e| total += e.size;
    try testing.expectEqual(@as(i64, 50), total);
}
