//! `mode=eval_sort` — rendering a SABnzbd sort template.
//!
//! Sonarr and Radarr call this to preview where an import will land before
//! they commit to the download client. A 4xx here makes them refuse the
//! client outright, so returning *something sensible* beats being strict:
//! unknown `%tokens` pass through verbatim rather than failing the
//! request, because a literal in a path is recoverable and a rejected
//! download client is not.
//!
//! Tokens, matching real SAB's `sorter.py`:
//!
//!     %title %t        release title
//!     %year %y         release year
//!     %season %s       season number, unpadded
//!     %0s              season number, zero-padded to width 2
//!     %episode %e      episode number, unpadded
//!     %0e              episode, zero-padded to width 2
//!     %cat %c          category name
//!     %ext             extension *including* the leading dot
//!     %fn              filename without extension
//!     %dn              dotted release name
//!     %desc            episode title / description
//!     %r               resolution
//!
//! Curly-brace tokens (`{title}`, `{season:02d}`) are also accepted:
//! *arr-side configs sometimes share a naming string with Sonarr v4's own
//! engine, which uses that dialect. Unlike `%tokens`, an unknown curly
//! token substitutes to empty rather than passing through — Go's
//! behaviour, and the tests below pin it.

const std = @import("std");
const fmt = @import("fmt.zig");

const Allocator = std.mem.Allocator;

/// The canonical key set. Missing values are empty strings, which every
/// substitution tolerates.
pub const SortContext = struct {
    title: []const u8 = "",
    year: []const u8 = "",
    season: []const u8 = "",
    episode: []const u8 = "",
    cat: []const u8 = "",
    ext: []const u8 = "",
    /// `fn` is a keyword, so the field is quoted. The wire name is still
    /// `fn`, which is what `get` looks up.
    @"fn": []const u8 = "",
    dn: []const u8 = "",
    desc: []const u8 = "",
    r: []const u8 = "",

    /// Case-insensitive lookup by wire name; unknown names are empty.
    pub fn get(self: SortContext, key: []const u8) []const u8 {
        inline for (@typeInfo(SortContext).@"struct".fields) |f| {
            if (std.ascii.eqlIgnoreCase(f.name, key)) return @field(self, f.name);
        }
        return "";
    }
};

/// A `func(string) string`, as a vtable so the caller can back it with a
/// request form, a map, or a literal table.
pub const Getter = struct {
    ctx: *const anyopaque,
    getFn: *const fn (ctx: *const anyopaque, key: []const u8) []const u8,

    pub fn get(self: Getter, key: []const u8) []const u8 {
        return self.getFn(self.ctx, key);
    }
};

/// Collects the union of the parameter names the different *arr clients
/// send. Each sends a slightly different subset; missing values render
/// empty. `ext` defaults to `.mkv` because a template ending in `%ext`
/// that rendered to a bare filename would look like a broken preview.
pub fn buildSortContext(g: Getter) SortContext {
    return .{
        .title = g.get("title"),
        .year = g.get("year"),
        .season = firstNonEmpty(&.{ g.get("season"), g.get("season_num") }),
        .episode = firstNonEmpty(&.{ g.get("episode"), g.get("episode_num") }),
        .cat = g.get("cat"),
        .ext = firstNonEmpty(&.{ g.get("ext"), ".mkv" }),
        .@"fn" = g.get("fn"),
        .dn = g.get("dn"),
        .desc = firstNonEmpty(&.{ g.get("desc"), g.get("episode_title") }),
        .r = firstNonEmpty(&.{ g.get("resolution"), g.get("r") }),
    };
}

pub fn firstNonEmpty(vals: []const []const u8) []const u8 {
    for (vals) |v| {
        if (v.len > 0) return v;
    }
    return "";
}

const Sub = struct {
    token: []const u8,
    key: []const u8,
    /// 0 = substitute verbatim, N = zero-pad a numeric value to width N.
    pad: u8 = 0,
};

/// Order matters: the longer alias has to be replaced first or `%title`
/// gets eaten by `%t`, leaving a stray `itle`.
const subs = [_]Sub{
    .{ .token = "%title", .key = "title" },
    .{ .token = "%desc", .key = "desc" },
    .{ .token = "%cat", .key = "cat" },
    .{ .token = "%year", .key = "year" },
    .{ .token = "%fn", .key = "fn" },
    .{ .token = "%dn", .key = "dn" },
    .{ .token = "%ext", .key = "ext" },
    .{ .token = "%r", .key = "r" },
    .{ .token = "%0s", .key = "season", .pad = 2 },
    .{ .token = "%season", .key = "season" },
    .{ .token = "%s", .key = "season" },
    .{ .token = "%0e", .key = "episode", .pad = 2 },
    .{ .token = "%episode", .key = "episode" },
    .{ .token = "%e", .key = "episode" },
    .{ .token = "%t", .key = "title" },
    .{ .token = "%y", .key = "year" },
    .{ .token = "%c", .key = "cat" },
};

/// Renders `template` against `ctx`. The result is owned by the caller.
pub fn evalSort(gpa: Allocator, template: []const u8, ctx: SortContext) Allocator.Error![]u8 {
    if (template.len == 0) return gpa.dupe(u8, "");

    var cur = try gpa.dupe(u8, template);
    errdefer gpa.free(cur);

    var pad_buf: [24]u8 = undefined;
    for (subs) |s| {
        var val = ctx.get(s.key);
        if (s.pad > 0) {
            // A non-numeric value is left alone, exactly as Go's
            // `strconv.Atoi` failure path did.
            if (std.fmt.parseInt(i64, val, 10)) |n| {
                val = padZero(&pad_buf, n, s.pad);
            } else |_| {}
        }
        const next = try replaceAll(gpa, cur, s.token, val);
        gpa.free(cur);
        cur = next;
    }

    const curly = try replaceCurly(gpa, cur, ctx);
    gpa.free(cur);
    cur = curly;

    // Clean shrinks or keeps length, and needs one spare byte for the
    // empty-input "." case.
    const cleaned = try gpa.alloc(u8, cur.len + 1);
    defer gpa.free(cleaned);
    const out = fmt.clean(cleaned, cur);
    const owned = try gpa.dupe(u8, out);
    gpa.free(cur);
    return owned;
}

fn padZero(buf: []u8, n: i64, width: u8) []const u8 {
    var tmp: [24]u8 = undefined;
    const s = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch unreachable;
    if (s.len >= width) {
        @memcpy(buf[0..s.len], s);
        return buf[0..s.len];
    }
    const zeros = width - s.len;
    @memset(buf[0..zeros], '0');
    @memcpy(buf[zeros..][0..s.len], s);
    return buf[0 .. zeros + s.len];
}

fn replaceAll(
    gpa: Allocator,
    hay: []const u8,
    needle: []const u8,
    repl: []const u8,
) Allocator.Error![]u8 {
    std.debug.assert(needle.len > 0);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var i: usize = 0;
    while (i < hay.len) {
        if (std.mem.startsWith(u8, hay[i..], needle)) {
            try out.appendSlice(gpa, repl);
            i += needle.len;
        } else {
            try out.append(gpa, hay[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(gpa);
}

/// Hand-rolled scanner for Go's `\{[a-zA-Z][a-zA-Z0-9_]*(?::[^}]+)?\}`.
/// A `{` that does not start a complete match is copied verbatim, which
/// is what `ReplaceAllStringFunc` does for non-matching text.
fn replaceCurly(gpa: Allocator, hay: []const u8, ctx: SortContext) Allocator.Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var pad_buf: [24]u8 = undefined;

    var i: usize = 0;
    while (i < hay.len) {
        if (hay[i] != '{') {
            try out.append(gpa, hay[i]);
            i += 1;
            continue;
        }
        const m = matchCurly(hay[i..]) orelse {
            try out.append(gpa, hay[i]);
            i += 1;
            continue;
        };
        const inner = hay[i + 1 .. i + m - 1];
        const colon = std.mem.indexOfScalar(u8, inner, ':');
        const key_raw = if (colon) |c| inner[0..c] else inner;
        var key_buf: [64]u8 = undefined;
        const key = lowerTrim(&key_buf, key_raw);
        var val = ctx.get(key);
        if (colon) |c| {
            const spec = inner[c + 1 ..];
            // Only "0Nd" is honoured — the one form clients actually
            // send. Anything else substitutes unformatted.
            if (spec.len >= 2 and spec[0] == '0' and spec[spec.len - 1] == 'd') {
                if (std.fmt.parseInt(u8, spec[1 .. spec.len - 1], 10)) |w| {
                    if (std.fmt.parseInt(i64, val, 10)) |n| {
                        val = padZero(&pad_buf, n, w);
                    } else |_| {}
                } else |_| {}
            }
        }
        try out.appendSlice(gpa, val);
        i += m;
    }
    return out.toOwnedSlice(gpa);
}

/// Length of the `{...}` token starting at `s[0]`, or null when it is not
/// one.
fn matchCurly(s: []const u8) ?usize {
    if (s.len < 3 or s[0] != '{') return null;
    if (!std.ascii.isAlphabetic(s[1])) return null;
    var i: usize = 2;
    while (i < s.len and (std.ascii.isAlphanumeric(s[i]) or s[i] == '_')) i += 1;
    if (i == s.len) return null;
    if (s[i] == '}') return i + 1;
    if (s[i] != ':') return null;
    // `[^}]+` — at least one character before the brace.
    const spec_start = i + 1;
    var j = spec_start;
    while (j < s.len and s[j] != '}') j += 1;
    if (j == s.len or j == spec_start) return null;
    return j + 1;
}

fn lowerTrim(buf: []u8, s: []const u8) []const u8 {
    const t = std.mem.trim(u8, s, " \t\r\n");
    const n = @min(t.len, buf.len);
    for (t[0..n], 0..) |c, i| buf[i] = std.ascii.toLower(c);
    return buf[0..n];
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Backs a `Getter` with a literal key/value table, the way the Go test
/// used a `map[string]string`.
const TableGetter = struct {
    rows: []const [2][]const u8,

    fn getter(self: *const TableGetter) Getter {
        return .{ .ctx = @ptrCast(self), .getFn = &lookup };
    }

    fn lookup(ctx: *const anyopaque, key: []const u8) []const u8 {
        const self: *const TableGetter = @ptrCast(@alignCast(ctx));
        for (self.rows) |row| {
            if (std.mem.eql(u8, row[0], key)) return row[1];
        }
        return "";
    }
};

test "evalSort renders the SAB token dialect" {
    const ctx: SortContext = .{
        .title = "Great Show",
        .year = "2020",
        .season = "3",
        .episode = "7",
        .cat = "tv",
        .ext = ".mkv",
        .r = "1080p",
    };
    const cases = [_]struct { []const u8, []const u8 }{
        // %ext already carries the leading dot, so templates must not add
        // one of their own.
        .{ "%title (%year)/Season %0s/S%0sE%0e%ext", "Great Show (2020)/Season 03/S03E07.mkv" },
        .{ "%t (%y) %r", "Great Show (2020) 1080p" },
        .{ "%cat/%title/%t.S%0sE%0e%ext", "tv/Great Show/Great Show.S03E07.mkv" },
        .{ "%s.%e", "3.7" },
        .{
            "{title} ({year})/Season {season:02d}/S{season:02d}E{episode:02d}{ext}",
            "Great Show (2020)/Season 03/S03E07.mkv",
        },
    };
    for (cases) |c| {
        const got = try evalSort(testing.allocator, c[0], ctx);
        defer testing.allocator.free(got);
        try testing.expectEqualStrings(c[1], got);
    }
}

test "evalSort preserves unknown percent tokens and empties unknown curly ones" {
    const got = try evalSort(testing.allocator, "%title %unknownpercent {also_unknown}", .{ .title = "X" });
    defer testing.allocator.free(got);
    // %unknownpercent is not a registered token so it survives verbatim;
    // {also_unknown} always substitutes, just to nothing. Clean keeps the
    // trailing whitespace as part of the path element.
    try testing.expectEqualStrings("X %unknownpercent ", got);
}

test "evalSort runs the result through filepath.Clean" {
    const cases = [_]struct { []const u8, []const u8 }{
        // An empty template short-circuits before Clean, so it stays "".
        .{ "", "" },
        // A template that renders to nothing does not: Clean("") is ".".
        .{ "{unknown}", "." },
        // Empty values collapse to a bare separator, which Clean keeps
        // as the root — a template like this is a misconfiguration, and
        // Go returned "/" for it too.
        .{ "%cat//%title", "/" },
        .{ "a/./b", "a/b" },
        .{ "a/../b", "b" },
    };
    for (cases) |c| {
        const got = try evalSort(testing.allocator, c[0], .{});
        defer testing.allocator.free(got);
        try testing.expectEqualStrings(c[1], got);
    }
}

test "evalSort leaves a non-numeric season unpadded" {
    const got = try evalSort(testing.allocator, "S%0sE%0e", .{ .season = "abc", .episode = "4" });
    defer testing.allocator.free(got);
    try testing.expectEqualStrings("SabcE04", got);
}

test "evalSort tolerates hostile templates without losing bytes" {
    // A release name reaches this path through `title`, so the renderer
    // has to survive anything an NZB can carry.
    const ctx: SortContext = .{ .title = "He said \"hi\" \\ & <b>", .season = "1" };
    const got = try evalSort(testing.allocator, "%title/S%0s", ctx);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings("He said \"hi\" \\ & <b>/S01", got);
}

test "evalSort curly spec forms that Go does not honour pass through unformatted" {
    const ctx: SortContext = .{ .season = "3" };
    const cases = [_]struct { []const u8, []const u8 }{
        // Matches the regex, spec is not "0Nd": substitutes unpadded.
        .{ "{season:d}", "3" },
        .{ "{season:2d}", "3" },
        .{ "{season:03d}", "003" },
        // `[^}]+` needs a character, so an empty spec is not a token at
        // all and the whole thing is literal.
        .{ "{season:}", "{season:}" },
        // Leading digit is not `[a-zA-Z]`.
        .{ "{1season}", "{1season}" },
        // Unterminated.
        .{ "{season", "{season" },
        .{ "{}", "{}" },
    };
    for (cases) |c| {
        const got = try evalSort(testing.allocator, c[0], ctx);
        defer testing.allocator.free(got);
        try testing.expectEqualStrings(c[1], got);
    }
}

test "buildSortContext collects the aliases each *arr sends" {
    const table = TableGetter{ .rows = &.{
        .{ "title", "Foo" },
        .{ "season_num", "1" },
        .{ "resolution", "720p" },
    } };
    const ctx = buildSortContext(table.getter());
    try testing.expectEqualStrings("Foo", ctx.title);
    try testing.expectEqualStrings("1", ctx.season);
    try testing.expectEqualStrings("720p", ctx.r);
    // ext falls back so a template ending in %ext still previews sanely.
    try testing.expectEqualStrings(".mkv", ctx.ext);
    try testing.expectEqualStrings("", ctx.desc);
    try testing.expectEqualStrings("", ctx.episode);
}

test "buildSortContext prefers the primary name over the alias" {
    const table = TableGetter{ .rows = &.{
        .{ "season", "9" },
        .{ "season_num", "1" },
        .{ "episode", "2" },
        .{ "episode_num", "8" },
        .{ "desc", "Pilot" },
        .{ "episode_title", "Other" },
        .{ "ext", ".mp4" },
        .{ "resolution", "2160p" },
        .{ "r", "480p" },
    } };
    const ctx = buildSortContext(table.getter());
    try testing.expectEqualStrings("9", ctx.season);
    try testing.expectEqualStrings("2", ctx.episode);
    try testing.expectEqualStrings("Pilot", ctx.desc);
    try testing.expectEqualStrings(".mp4", ctx.ext);
    try testing.expectEqualStrings("2160p", ctx.r);
}

test "SortContext.get maps wire names onto fields, fn included" {
    const ctx: SortContext = .{ .@"fn" = "release", .dn = "The.Movie.2020", .title = "T" };
    try testing.expectEqualStrings("release", ctx.get("fn"));
    try testing.expectEqualStrings("The.Movie.2020", ctx.get("dn"));
    try testing.expectEqualStrings("T", ctx.get("TITLE"));
    try testing.expectEqualStrings("", ctx.get("nope"));
}
