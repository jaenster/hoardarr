//! Number, duration and path formatters for the SABnzbd wire format.
//!
//! Every function here exists because SABnzbd's JSON is *stringly typed*
//! in specific, load-bearing ways: `mb` and `mbleft` are decimal strings
//! with two places, `size` is `"1.15 GB"`, `speed` is `"1.5 MB/s"`,
//! `timeleft` is `"h:mm:ss"`, and `eta` is a formatted local timestamp.
//! Sonarr and Radarr parse these strings. A `1.150` where SAB writes
//! `1.15`, or a `1,15`, is a broken download client from their side.
//!
//! ## Why the fixed-point formatter is hand-rolled
//!
//! The Go implementation reached these strings through
//! `fmt.Sprintf("%.2f", float64(n)/(1024*1024))`. Go's `%.2f` is
//! *correctly rounded against the exact binary value* of the float, with
//! ties resolved to even. Zig's `{d:.2}` rounds the shortest decimal
//! representation with ties away from zero, so the two disagree on real
//! inputs:
//!
//! | value | Go `%.2f` | Zig `{d:.2}` |
//! |-|-|-|
//! | 0.125 | 0.12 | 0.13 |
//! | 0.015 | 0.01 | 0.02 |
//! | 2.675 | 2.67 | 2.68 |
//!
//! Every quantity we format is `<integer bytes> / 2^k`, so the exact
//! value is a dyadic rational and `scaled` computes the correctly rounded
//! decimal with integer arithmetic — no float in the path at all, and
//! byte-identical to Go for every input. (Above 2^53 the *conversion*
//! `float64(n)` is itself lossy in Go; `scaled` reproduces that rounding
//! so even absurd byte counts agree.)

const std = @import("std");
const log = @import("../../core/log.zig");

// ---------------------------------------------------------------------
// Fixed-point decimal
// ---------------------------------------------------------------------

/// Widest output of `scaled`: 19 integer digits, a point, and up to 18
/// decimals, plus a sign.
pub const ScaledBuf = [40]u8;

/// Renders `n / 2^shift` with exactly `precision` decimal places,
/// byte-identical to Go's `fmt.Sprintf("%.*f", precision, float64(n) /
/// (1 << shift))`.
///
/// Rounding is half-to-even on the exact value, which is what Go's
/// `strconv.FormatFloat(_, 'f', prec, 64)` does. See the module comment.
pub fn scaled(buf: []u8, n: i64, shift: u6, precision: u8) []const u8 {
    std.debug.assert(precision <= 18);
    const wide: i128 = n;
    const neg = wide < 0;
    var mag: u128 = @intCast(if (neg) -wide else wide);

    // Go converts to float64 before dividing. Past 2^53 that conversion
    // rounds to the nearest representable integer; do the same so the
    // two implementations agree everywhere rather than almost everywhere.
    if (mag > (@as(u128, 1) << 53)) {
        const f: f64 = @floatFromInt(mag);
        mag = @intFromFloat(f);
    }

    const den: u128 = @as(u128, 1) << shift;
    const num: u128 = mag * pow10(precision);
    var q = num / den;
    const r = num % den;
    // Half-to-even. `2*r` cannot overflow: r < den <= 2^63.
    if (2 * r > den) {
        q += 1;
    } else if (2 * r == den and (q & 1) == 1) {
        q += 1;
    }

    var digits: [48]u8 = undefined;
    const ds = std.fmt.bufPrint(&digits, "{d}", .{q}) catch unreachable;

    var out: usize = 0;
    if (neg and q != 0) {
        buf[out] = '-';
        out += 1;
    }
    const p: usize = precision;
    if (ds.len > p) {
        const int_len = ds.len - p;
        @memcpy(buf[out..][0..int_len], ds[0..int_len]);
        out += int_len;
    } else {
        buf[out] = '0';
        out += 1;
    }
    if (p > 0) {
        buf[out] = '.';
        out += 1;
        // Left-pad the fraction when q has fewer than `precision` digits
        // (0.07 has q == 7).
        var pad = p - @min(p, ds.len);
        while (pad > 0) : (pad -= 1) {
            buf[out] = '0';
            out += 1;
        }
        const frac = ds[ds.len - @min(p, ds.len) ..];
        @memcpy(buf[out..][0..frac.len], frac);
        out += frac.len;
    }
    return buf[0..out];
}

fn pow10(e: u8) u128 {
    var v: u128 = 1;
    var i: u8 = 0;
    while (i < e) : (i += 1) v *= 10;
    return v;
}

// ---------------------------------------------------------------------
// Byte counts
// ---------------------------------------------------------------------

/// Widest output of `bytesHuman` — `"8589934592.00 GB"`.
pub const HumanBuf = [48]u8;

/// SAB-style: `"12.34 MB"`, `"1.15 GB"`, `"512 B"`. 1024-base (MiB
/// labelled MB) which is what SAB itself emits despite the notation, and
/// therefore what consumers expect.
pub fn bytesHuman(buf: *HumanBuf, n_in: i64) []const u8 {
    const n: i64 = if (n_in < 0) 0 else n_in;
    const k = 1024;
    var num: ScaledBuf = undefined;
    if (n < k) return std.fmt.bufPrint(buf, "{d} B", .{n}) catch unreachable;
    if (n < k * k) return std.fmt.bufPrint(buf, "{s} KB", .{scaled(&num, n, 10, 2)}) catch unreachable;
    if (n < k * k * k) return std.fmt.bufPrint(buf, "{s} MB", .{scaled(&num, n, 20, 2)}) catch unreachable;
    return std.fmt.bufPrint(buf, "{s} GB", .{scaled(&num, n, 30, 2)}) catch unreachable;
}

/// `mb` / `mbleft` / `_doneMB`: megabytes with two decimals, except that
/// a whole number loses the `.00`. That trailing-zero trim is SAB's, not
/// ours — `"1024"` and `"1177.38"` both appear in real SAB output, so a
/// consumer that assumed a fixed two decimals was already broken against
/// the real thing.
pub fn mbString(buf: *ScaledBuf, n_in: i64) []const u8 {
    const n: i64 = if (n_in < 0) 0 else n_in;
    const s = scaled(buf, n, 20, 2);
    if (std.mem.endsWith(u8, s, ".00")) return s[0 .. s.len - 3];
    return s;
}

/// Plain two-decimal megabytes, no trim. `get_files` uses this shape
/// (SAB is inconsistent between the two; we reproduce the
/// inconsistency).
pub fn mbPlain(buf: *ScaledBuf, n: i64) []const u8 {
    return scaled(buf, n, 20, 2);
}

/// `queue.speed`: `"<value> <unit>/s"`, one decimal, 1024-base. Values
/// under 1 KiB/s are whole bytes with no decimal at all.
pub fn bytesPerSec(buf: *HumanBuf, bps: i64) []const u8 {
    if (bps <= 0) return "0 B/s";
    if (bps < 1024) return std.fmt.bufPrint(buf, "{d} B/s", .{bps}) catch unreachable;
    const units = [_][]const u8{ "K", "M", "G", "T" };
    // Go steps the unit while the *divided* value is still >= 1024, which
    // is the same as stepping while `bps` reaches the next power of 2^10.
    var i: usize = 0;
    while (i < units.len - 1 and bps >= (@as(i64, 1) << @intCast(10 * (i + 2)))) i += 1;
    var num: ScaledBuf = undefined;
    const shift: u6 = @intCast(10 * (i + 1));
    return std.fmt.bufPrint(buf, "{s} {s}B/s", .{ scaled(&num, bps, shift, 1), units[i] }) catch unreachable;
}

// ---------------------------------------------------------------------
// Durations and timestamps
// ---------------------------------------------------------------------

/// `"h:mm:ss"` — hours are not padded, minutes and seconds are, and the
/// hour field is unbounded (`"99:59:59"` is legal SAB output).
pub const HmsBuf = [32]u8;

pub fn hms(buf: *HmsBuf, seconds_in: i64) []const u8 {
    const seconds: i64 = if (seconds_in < 0) 0 else seconds_in;
    // Unsigned, because Zig's `{d:0>2}` renders a sign for signed types
    // and `0:+0:+0` is not a duration.
    const h: u64 = @intCast(@divTrunc(seconds, 3600));
    const m: u64 = @intCast(@divTrunc(@rem(seconds, 3600), 60));
    const s: u64 = @intCast(@rem(seconds, 60));
    return std.fmt.bufPrint(buf, "{d}:{d:0>2}:{d:0>2}", .{ h, m, s }) catch unreachable;
}

/// `"15:04 Mon 02 Jan"` — real SAB v3's ETA shape, and the only one
/// Sonarr's parser accepts. The older `"Mon 15:04"` form parses to a zero
/// time there, after which its stuck-download heuristic fires and the
/// grab is cancelled.
pub const EtaBuf = [32]u8;

const weekday_names = [7][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
const month_names = [12][]const u8{
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
};

/// Formats `unix_secs` in **UTC**. Go used the process-local zone; the
/// container ships without a tzdata copy and runs UTC, so this matches
/// what the Go build actually emitted in production. The field is
/// advisory — Sonarr reads the shape, not the instant.
pub fn etaSab(buf: *EtaBuf, unix_secs: i64) []const u8 {
    const days = @divFloor(unix_secs, std.time.s_per_day);
    const sod: u64 = @intCast(@mod(unix_secs, std.time.s_per_day));
    const d = log.civilFromDays(days);
    // 1970-01-01 was a Thursday, index 4 in a Sunday-first table.
    const wd: usize = @intCast(@mod(days + 4, 7));
    const mi: usize = @min(@as(usize, d.month), 12) - 1;
    return std.fmt.bufPrint(buf, "{d:0>2}:{d:0>2} {s} {d:0>2} {s}", .{
        sod / 3600,
        (sod % 3600) / 60,
        weekday_names[wd],
        d.day,
        month_names[mi],
    }) catch unreachable;
}

/// Milliseconds (the domain's `Timestamp`) to the unix *seconds* the SAB
/// wire format carries. `null` — the domain's spelling of Go's zero
/// `time.Time` — becomes 0, which is what `unixOrZero` produced.
pub fn unixSeconds(ms: ?i64) i64 {
    const v = ms orelse return 0;
    return @divFloor(v, 1000);
}

// ---------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------

/// Go's `filepath.Clean` for the POSIX separator, writing into `buf`,
/// which must be at least `path.len + 1` bytes (Clean never grows its
/// input, but the empty path becomes `"."`).
///
/// Needed twice: `eval_sort` runs its rendered template through Clean, and
/// the history slot's `storage` is a `filepath.Join`. Both are observable,
/// so this is the real algorithm rather than an approximation.
pub fn clean(buf: []u8, path: []const u8) []const u8 {
    std.debug.assert(buf.len >= path.len + 1);
    if (path.len == 0) {
        buf[0] = '.';
        return buf[0..1];
    }
    const rooted = path[0] == '/';
    const n = path.len;
    var w: usize = 0;
    var r: usize = 0;
    var dotdot: usize = 0;
    if (rooted) {
        buf[w] = '/';
        w += 1;
        r = 1;
        dotdot = 1;
    }
    while (r < n) {
        if (path[r] == '/') {
            r += 1;
        } else if (path[r] == '.' and (r + 1 == n or path[r + 1] == '/')) {
            // A lone "." element.
            r += 1;
        } else if (path[r] == '.' and r + 1 < n and path[r + 1] == '.' and
            (r + 2 == n or path[r + 2] == '/'))
        {
            r += 2;
            if (w > dotdot) {
                // Back up over the previous element.
                w -= 1;
                while (w > dotdot and buf[w] != '/') w -= 1;
            } else if (!rooted) {
                // Cannot back up past the start of a relative path, so
                // the ".." has to survive.
                if (w > 0) {
                    buf[w] = '/';
                    w += 1;
                }
                buf[w] = '.';
                buf[w + 1] = '.';
                w += 2;
                dotdot = w;
            }
        } else {
            if ((rooted and w != 1) or (!rooted and w != 0)) {
                buf[w] = '/';
                w += 1;
            }
            while (r < n and path[r] != '/') {
                buf[w] = path[r];
                w += 1;
                r += 1;
            }
        }
    }
    if (w == 0) {
        buf[0] = '.';
        return buf[0..1];
    }
    return buf[0..w];
}

/// Go's `filepath.Join` for two elements: skip the empty ones, join the
/// rest with '/', then Clean. `buf` must hold `a.len + b.len + 2`.
pub fn join(buf: []u8, a: []const u8, b: []const u8) []const u8 {
    std.debug.assert(buf.len >= a.len + b.len + 2);
    // Joined form is built in the tail of `buf` so `clean` can write over
    // the head without reading what it has not consumed yet. Simpler: a
    // scratch pass through a temporary region of the same buffer.
    var tmp: usize = 0;
    const scratch = buf[0 .. a.len + b.len + 2];
    if (a.len > 0) {
        @memcpy(scratch[tmp..][0..a.len], a);
        tmp += a.len;
    }
    if (b.len > 0) {
        if (tmp > 0) {
            scratch[tmp] = '/';
            tmp += 1;
        }
        @memcpy(scratch[tmp..][0..b.len], b);
        tmp += b.len;
    }
    if (tmp == 0) return "";
    // Clean in place is safe: it only ever writes at or behind its read
    // cursor. Verified by the tests below, which include the cases where
    // the two cursors are closest ("a//b", "a/../b").
    return clean(scratch[0 .. tmp + 1], scratch[0..tmp]);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "scaled matches Go's %.2f rounding of the exact value" {
    var buf: ScaledBuf = undefined;
    // Ties resolve to even, which is where Zig's own float formatter
    // disagrees: it would give 0.13 and 0.38.
    try testing.expectEqualStrings("0.12", scaled(&buf, 1, 3, 2)); // 1/8
    try testing.expectEqualStrings("0.38", scaled(&buf, 3, 3, 2)); // 3/8
    try testing.expectEqualStrings("0.62", scaled(&buf, 5, 3, 2)); // 5/8
    try testing.expectEqualStrings("0.88", scaled(&buf, 7, 3, 2)); // 7/8
    // Exact, no rounding involved.
    try testing.expectEqualStrings("0.50", scaled(&buf, 1, 1, 2));
    try testing.expectEqualStrings("1.00", scaled(&buf, 1, 0, 2));
    try testing.expectEqualStrings("0", scaled(&buf, 1, 3, 0));
    try testing.expectEqualStrings("0.1250", scaled(&buf, 1, 3, 4));
    try testing.expectEqualStrings("-0.38", scaled(&buf, -3, 3, 2));
    try testing.expectEqualStrings("0.00", scaled(&buf, 0, 20, 2));
}

test "bytesHuman reproduces the Go table exactly" {
    // Every row was produced by the Go implementation.
    const cases = [_]struct { i64, []const u8 }{
        .{ 0, "0 B" },
        .{ 1, "1 B" },
        .{ 512, "512 B" },
        .{ 1023, "1023 B" },
        .{ 1024, "1.00 KB" },
        .{ 1025, "1.00 KB" },
        .{ 1536, "1.50 KB" },
        .{ 1048575, "1024.00 KB" },
        .{ 1048576, "1.00 MB" },
        .{ 1073741823, "1024.00 MB" },
        .{ 1073741824, "1.00 GB" },
        .{ 1234567890, "1.15 GB" },
        .{ 9223372036854775807, "8589934592.00 GB" },
        .{ -1, "0 B" },
    };
    var buf: HumanBuf = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[1], bytesHuman(&buf, c[0]));
}

test "mbString trims a whole-megabyte .00 the way SAB does" {
    const cases = [_]struct { i64, []const u8 }{
        .{ 0, "0" },
        .{ 1, "0" },
        .{ 1048575, "1" },
        .{ 1048576, "1" },
        .{ 1073741823, "1024" },
        .{ 1234567890, "1177.38" },
        .{ 777778878, "741.75" },
        .{ 456789012, "435.63" },
        .{ 9223372036854775807, "8796093022208" },
        .{ -1, "0" },
    };
    var buf: ScaledBuf = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[1], mbString(&buf, c[0]));
    // The untrimmed variant keeps both places.
    try testing.expectEqualStrings("50.00", mbPlain(&buf, 52428800));
    try testing.expectEqualStrings("25.00", mbPlain(&buf, 26214400));
    try testing.expectEqualStrings("0.00", mbPlain(&buf, 0));
}

test "bytesPerSec reproduces the Go table exactly" {
    const cases = [_]struct { i64, []const u8 }{
        .{ 0, "0 B/s" },
        .{ -5, "0 B/s" },
        .{ 1, "1 B/s" },
        .{ 512, "512 B/s" },
        .{ 1023, "1023 B/s" },
        .{ 1024, "1.0 KB/s" },
        .{ 1025, "1.0 KB/s" },
        .{ 1536, "1.5 KB/s" },
        .{ 1048576, "1.0 MB/s" },
        .{ 1572864, "1.5 MB/s" },
        .{ 1073741824, "1.0 GB/s" },
        .{ 1099511627776, "1.0 TB/s" },
        // Saturates at TB rather than inventing a PB unit, as Go's
        // four-element table did.
        .{ 1125899906842624, "1024.0 TB/s" },
    };
    var buf: HumanBuf = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[1], bytesPerSec(&buf, c[0]));
}

test "hms reproduces the Go table exactly" {
    const cases = [_]struct { i64, []const u8 }{
        .{ 0, "0:00:00" },
        .{ -1, "0:00:00" },
        .{ 59, "0:00:59" },
        .{ 60, "0:01:00" },
        .{ 61, "0:01:01" },
        .{ 3599, "0:59:59" },
        .{ 3600, "1:00:00" },
        .{ 3661, "1:01:01" },
        .{ 86399, "23:59:59" },
        .{ 86400, "24:00:00" },
        .{ 359999, "99:59:59" },
    };
    var buf: HmsBuf = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[1], hms(&buf, c[0]));
}

test "etaSab matches Go's \"15:04 Mon 02 Jan\" layout" {
    const cases = [_]struct { i64, []const u8 }{
        .{ 1700000000, "22:13 Tue 14 Nov" },
        .{ 0, "00:00 Thu 01 Jan" },
        .{ 1700000777, "22:26 Tue 14 Nov" },
        .{ 1767225600, "00:00 Thu 01 Jan" },
        .{ 1704067199, "23:59 Sun 31 Dec" },
    };
    var buf: EtaBuf = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[1], etaSab(&buf, c[0]));
}

test "unixSeconds floors milliseconds and maps null to zero" {
    try testing.expectEqual(@as(i64, 0), unixSeconds(null));
    try testing.expectEqual(@as(i64, 1700000000), unixSeconds(1700000000_000));
    try testing.expectEqual(@as(i64, 1700000000), unixSeconds(1700000000_999));
    try testing.expectEqual(@as(i64, 0), unixSeconds(0));
}

test "clean reproduces filepath.Clean" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "", "." },
        .{ ".", "." },
        .{ "a/b", "a/b" },
        .{ "a//b", "a/b" },
        .{ "a/./b", "a/b" },
        .{ "a/../b", "b" },
        .{ "/a/b/", "/a/b" },
        .{ "a/b/..", "a" },
        .{ "../a", "../a" },
        .{ "/../a", "/a" },
        .{ "X %unknownpercent ", "X %unknownpercent " },
        .{ "Great Show (2020)/Season 03/S03E07.mkv", "Great Show (2020)/Season 03/S03E07.mkv" },
        .{ "3.7", "3.7" },
        .{ "a/", "a" },
        .{ "//a", "/a" },
        .{ "a/b/c/../../d", "a/d" },
        .{ "../../a", "../../a" },
        .{ "./a/..", "." },
    };
    var buf: [128]u8 = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[1], clean(&buf, c[0]));
}

test "join reproduces filepath.Join" {
    const cases = [_]struct { []const u8, []const u8, []const u8 }{
        .{ "/data/complete", "Rel", "/data/complete/Rel" },
        .{ "", "Rel", "Rel" },
        .{ "/data/", "/Rel/", "/data/Rel" },
        .{ "/data", "", "/data" },
        .{ "", "", "" },
        .{ "/data", "../escape", "/escape" },
    };
    var buf: [256]u8 = undefined;
    for (cases) |c| try testing.expectEqualStrings(c[2], join(&buf, c[0], c[1]));
}
