//! Structured logging. Every subsystem in the process logs through
//! here, so the cost of one record has to be low enough that nobody is
//! tempted to reach for a raw `write` instead.
//!
//! Three properties drive the design:
//!
//!   * **Disabled call sites vanish.** `log(level, ...)` takes the level
//!     as a comptime argument and compares it against `compile_min`
//!     (overridable with a `log_compile_min` decl in the root file). A
//!     record below that floor is `if (false)` and the optimiser deletes
//!     the whole call, including the argument tuple construction.
//!   * **No heap on the record path.** A record is encoded into a
//!     stack buffer (`line_buf_size` bytes) and written from there. The
//!     only allocation in this file is the ring buffer in `logring.zig`,
//!     which is sized once at startup.
//!   * **One `write` per record per sink.** The newline is part of the
//!     encoded buffer, so a record is a single `writeAll`. Two writes
//!     would let a concurrent logger interleave between them and shred
//!     the line, and no amount of locking on our side fixes that if the
//!     fd is also inherited by a child process.
//!
//! Attributes are a tagged union rather than `anytype`, which is what
//! lets the JSON encoder quote strings and emit numbers bare. String
//! escaping is total: control characters, quotes, backslashes and
//! invalid UTF-8 are all neutralised, because attribute values come
//! from NZB filenames and NNTP subjects — attacker-controlled text.

const std = @import("std");
const builtin = @import("builtin");
const root = @import("root");
const sys = @import("../posix/sys.zig");

// ---------------------------------------------------------------------
// Levels
// ---------------------------------------------------------------------

/// Ordered so that `@intFromEnum` comparisons are the level filter.
/// Matches Go's `slog` ordering; `err` is spelled short because `error`
/// is a keyword.
pub const Level = enum(u8) {
    debug = 0,
    info = 1,
    warn = 2,
    err = 3,

    /// Upper-case label used in the encoded record, matching slog's
    /// `level=INFO`, so existing log-scraping stays valid.
    pub fn label(self: Level) []const u8 {
        return switch (self) {
            .debug => "DEBUG",
            .info => "INFO",
            .warn => "WARN",
            .err => "ERROR",
        };
    }

    /// Lower-case name — the vocabulary of `server.log_level` in the
    /// config file and of the UI's log filter dropdown. Go called this
    /// `loghub.LevelName`.
    pub fn name(self: Level) []const u8 {
        return switch (self) {
            .debug => "debug",
            .info => "info",
            .warn => "warn",
            .err => "error",
        };
    }

    /// Parse a config value. `null` for anything unrecognised; callers
    /// decide whether that is a hard error (config validation) or a
    /// fallback to `.info` (env override).
    pub fn parse(s: []const u8) ?Level {
        if (std.mem.eql(u8, s, "debug")) return .debug;
        if (std.mem.eql(u8, s, "info")) return .info;
        if (std.mem.eql(u8, s, "warn")) return .warn;
        if (std.mem.eql(u8, s, "error")) return .err;
        return null;
    }
};

/// Compile-time floor. Anything below this is not merely skipped at
/// runtime — the call site is removed by the optimiser, so a
/// `log.debug` in the yEnc inner loop costs literally zero once the
/// release build sets this to `.info`.
///
/// Override by declaring `pub const log_compile_min: log.Level = .info;`
/// in the root source file. Same mechanism `std.log` uses for
/// `std.options.log_level`.
pub const compile_min: Level = if (@hasDecl(root, "log_compile_min"))
    root.log_compile_min
else
    .debug;

// ---------------------------------------------------------------------
// Attributes
// ---------------------------------------------------------------------

/// A typed attribute value. The tag is what lets the JSON encoder emit
/// `{"bytes":4096}` rather than `{"bytes":"4096"}` — the UI's log view
/// and any downstream log shipper both care about that distinction.
pub const Value = union(enum) {
    str: []const u8,
    int: i64,
    uint: u64,
    float: f64,
    boolean: bool,
    /// Errors are carried as the error value, not a pre-formatted
    /// string, so the encoder can use `@errorName` without the caller
    /// needing a buffer.
    err: anyerror,
    /// Absent value. Encodes as JSON `null` / text `<null>`.
    none: void,
};

pub const Attr = struct {
    key: []const u8,
    value: Value,
};

pub fn str(key: []const u8, v: []const u8) Attr {
    return .{ .key = key, .value = .{ .str = v } };
}

pub fn int(key: []const u8, v: i64) Attr {
    return .{ .key = key, .value = .{ .int = v } };
}

pub fn uint(key: []const u8, v: u64) Attr {
    return .{ .key = key, .value = .{ .uint = v } };
}

pub fn float(key: []const u8, v: f64) Attr {
    return .{ .key = key, .value = .{ .float = v } };
}

pub fn boolean(key: []const u8, v: bool) Attr {
    return .{ .key = key, .value = .{ .boolean = v } };
}

/// `errv` rather than `err` because `err` is the module-level
/// error-severity log function.
pub fn errv(key: []const u8, v: anyerror) Attr {
    return .{ .key = key, .value = .{ .err = v } };
}

pub fn none(key: []const u8) Attr {
    return .{ .key = key, .value = .none };
}

/// Comptime-dispatched constructor for call-site brevity. This is the
/// only `anytype` in the module and it collapses to one of the typed
/// constructors above before any encoding happens, so the encoders
/// still only ever see `Value`.
pub fn any(key: []const u8, v: anytype) Attr {
    const T = @TypeOf(v);
    return switch (@typeInfo(T)) {
        .bool => boolean(key, v),
        .int => |i| if (i.signedness == .signed)
            int(key, @intCast(v))
        else
            uint(key, @intCast(v)),
        .comptime_int => if (v < 0) int(key, v) else uint(key, v),
        .float, .comptime_float => float(key, @floatCast(v)),
        .error_set => errv(key, v),
        .null => none(key),
        .void => none(key),
        .@"enum" => str(key, @tagName(v)),
        .optional => if (v) |inner| any(key, inner) else none(key),
        .pointer => |p| switch (p.size) {
            .slice => if (p.child == u8)
                str(key, v)
            else
                @compileError("log.any: unsupported slice type " ++ @typeName(T)),
            .one => switch (@typeInfo(p.child)) {
                .array => |a| if (a.child == u8)
                    str(key, v)
                else
                    @compileError("log.any: unsupported array type " ++ @typeName(T)),
                else => @compileError("log.any: unsupported pointer type " ++ @typeName(T)),
            },
            else => @compileError("log.any: unsupported pointer type " ++ @typeName(T)),
        },
        else => @compileError("log.any: unsupported type " ++ @typeName(T)),
    };
}

// ---------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------

pub const Format = enum { text, json };

/// Per-record stack budget. Two of these live on the stack during
/// `emit` (one per format actually in use), so 2 KiB keeps the worst
/// case at 4 KiB — fine on any thread stack, and comfortably above the
/// longest realistic record (a 255-byte filename plus a dozen attrs).
pub const line_buf_size = 2048;

/// Tail of the buffer held back from normal writes so the encoder can
/// always append a well-formed terminator (closing quote, closing
/// brace, truncation marker, newline) no matter where it ran out.
const reserve = 32;

/// JSON escape for U+FFFD. Emitted in place of invalid UTF-8 so the
/// output is both valid JSON and valid UTF-8 regardless of input. Using
/// the escape form rather than the raw bytes keeps the whole line ASCII
/// when the input was ASCII-plus-garbage.
const replacement = "\\ufffd";

const trunc_json = ",\"log_truncated\":true}\n";
const trunc_text = " log_truncated=true\n";

comptime {
    // The reserve has to cover the worst terminator sequence: a forced
    // closing quote from a truncated string, plus the marker.
    std.debug.assert(trunc_json.len + 1 <= reserve);
    std.debug.assert(trunc_text.len + 1 <= reserve);
}

/// A fixed-capacity line builder.
///
/// `put` is all-or-nothing: a write that would cross `soft` is dropped
/// entirely rather than truncated. That matters because the JSON string
/// encoder feeds it whole escape sequences: half of a two-byte escape
/// would be a JSON syntax error, whereas a dropped one is merely a
/// shorter string.
const LineBuf = struct {
    buf: []u8,
    len: usize = 0,
    /// Capacity available to `put`; the gap up to `buf.len` is the
    /// terminator reserve.
    soft: usize,
    /// Set once anything has been dropped.
    over: bool = false,

    fn init(b: []u8) LineBuf {
        std.debug.assert(b.len > reserve);
        return .{ .buf = b, .soft = b.len - reserve };
    }

    fn put(self: *LineBuf, s: []const u8) void {
        if (self.len + s.len > self.soft) {
            self.over = true;
            return;
        }
        @memcpy(self.buf[self.len..][0..s.len], s);
        self.len += s.len;
    }

    fn putByte(self: *LineBuf, c: u8) void {
        if (self.len + 1 > self.soft) {
            self.over = true;
            return;
        }
        self.buf[self.len] = c;
        self.len += 1;
    }

    /// Copies as much as fits. Only for raw (unescaped) runs where a
    /// partial copy is still meaningful — never for escape sequences.
    fn putTrunc(self: *LineBuf, s: []const u8) void {
        const n = @min(s.len, self.soft - self.len);
        if (n < s.len) self.over = true;
        @memcpy(self.buf[self.len..][0..n], s[0..n]);
        self.len += n;
    }

    /// Writes into the reserve. Guaranteed to fit by the comptime
    /// assertions above plus the invariant that at most one forced
    /// closing quote precedes a terminator.
    fn force(self: *LineBuf, s: []const u8) void {
        std.debug.assert(self.len + s.len <= self.buf.len);
        @memcpy(self.buf[self.len..][0..s.len], s);
        self.len += s.len;
    }

    fn slice(self: *const LineBuf) []const u8 {
        return self.buf[0..self.len];
    }
};

/// "YYYY-MM-DDTHH:MM:SS.mmmZ"
pub const ts_len = 24;

/// Proleptic Gregorian civil date.
pub const Date = struct {
    year: i32,
    month: u8,
    day: u8,

    pub fn eql(a: Date, b: Date) bool {
        return a.year == b.year and a.month == b.month and a.day == b.day;
    }
};

/// Days-from-epoch to civil date, Howard Hinnant's `civil_from_days`.
/// Shifts the era so that the leap-year cycle starts on March 1, which
/// removes the February special case entirely — no lookup tables, no
/// branches on month length.
pub fn civilFromDays(days: i64) Date {
    const z = days + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe: u64 = @intCast(z - era * 146097); // [0, 146096]
    const yoe: u64 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    const y: i64 = @as(i64, @intCast(yoe)) + era * 400;
    const doy: u64 = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    const mp: u64 = (5 * doy + 2) / 153; // [0, 11]
    const d: u64 = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    const m: u64 = if (mp < 10) mp + 3 else mp - 9; // [1, 12]
    return .{
        .year = @intCast(y + @as(i64, if (m <= 2) 1 else 0)),
        .month = @intCast(m),
        .day = @intCast(d),
    };
}

/// Wall-clock nanoseconds to the date component only. Used by the file
/// writer's midnight rotation check.
pub fn dateOf(ns: i128) Date {
    const secs: i64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    return civilFromDays(@divFloor(secs, std.time.s_per_day));
}

fn padDigits(out: []u8, value: u64) void {
    var v = value;
    var i = out.len;
    while (i > 0) {
        i -= 1;
        out[i] = '0' + @as(u8, @intCast(v % 10));
        v /= 10;
    }
}

/// RFC3339 with millisecond precision and a literal `Z`. Fixed width,
/// which makes log lines column-aligned and cheap to encode: no
/// `bufPrint`, just digit stamping.
pub fn formatTimestamp(out: *[ts_len]u8, ns: i128) []const u8 {
    const secs: i64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    const sub: u64 = @intCast(@mod(ns, std.time.ns_per_s));
    const days = @divFloor(secs, std.time.s_per_day);
    const sod: u64 = @intCast(@mod(secs, std.time.s_per_day));
    const d = civilFromDays(days);

    // Years outside four digits would change the field width and break
    // the fixed-layout promise; clamp rather than widen.
    const year: u64 = if (d.year < 0) 0 else if (d.year > 9999) 9999 else @intCast(d.year);

    padDigits(out[0..4], year);
    out[4] = '-';
    padDigits(out[5..7], d.month);
    out[7] = '-';
    padDigits(out[8..10], d.day);
    out[10] = 'T';
    padDigits(out[11..13], sod / 3600);
    out[13] = ':';
    padDigits(out[14..16], (sod % 3600) / 60);
    out[16] = ':';
    padDigits(out[17..19], sod % 60);
    out[19] = '.';
    padDigits(out[20..23], sub / std.time.ns_per_ms);
    out[23] = 'Z';
    return out;
}

fn putUnicodeEscape(lb: *LineBuf, c: u8) void {
    const hex = "0123456789abcdef";
    lb.put(&[_]u8{ '\\', 'u', '0', '0', hex[c >> 4], hex[c & 0xF] });
}

/// Emit `s` as a JSON string literal, including both quotes.
///
/// Total by construction: every byte either maps to a legal escape, is
/// copied as part of a validated UTF-8 sequence, or is replaced by
/// U+FFFD. There is no input — however hostile — that produces invalid
/// JSON. On overflow the string is cut short and the closing quote is
/// written from the reserve, so the *structure* stays valid even when
/// the content does not fit.
fn putJsonString(lb: *LineBuf, s: []const u8) void {
    lb.putByte('"');
    var i: usize = 0;
    while (i < s.len) {
        if (lb.over) break;
        const c = s[i];
        if (c < 0x80) {
            switch (c) {
                '"' => lb.put("\\\""),
                '\\' => lb.put("\\\\"),
                '\n' => lb.put("\\n"),
                '\r' => lb.put("\\r"),
                '\t' => lb.put("\\t"),
                0x08 => lb.put("\\b"),
                0x0C => lb.put("\\f"),
                // Everything else below 0x20 (including NUL) has no
                // short escape and must not appear raw in a JSON string.
                else => if (c < 0x20) putUnicodeEscape(lb, c) else lb.putByte(c),
            }
            i += 1;
            continue;
        }
        // Multi-byte: validate before copying. An unchecked copy of a
        // truncated or overlong sequence would produce invalid UTF-8,
        // which strict JSON readers reject.
        const n = std.unicode.utf8ByteSequenceLength(c) catch {
            lb.put(replacement);
            i += 1;
            continue;
        };
        if (i + n > s.len) {
            lb.put(replacement);
            i += 1;
            continue;
        }
        _ = std.unicode.utf8Decode(s[i..][0..n]) catch {
            lb.put(replacement);
            i += 1;
            continue;
        };
        lb.put(s[i..][0..n]);
        i += n;
    }
    lb.force("\"");
}

/// slog's logfmt rule: a value is written bare when it is unambiguous,
/// quoted otherwise.
fn needsQuote(s: []const u8) bool {
    if (s.len == 0) return true;
    for (s) |c| {
        if (c <= ' ' or c == '"' or c == '=' or c == 0x7F) return true;
    }
    return false;
}

fn putTextString(lb: *LineBuf, s: []const u8) void {
    if (needsQuote(s)) {
        putJsonString(lb, s);
    } else {
        lb.putTrunc(s);
    }
}

fn putInt(lb: *LineBuf, v: i64) void {
    var tmp: [24]u8 = undefined;
    lb.put(std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable);
}

fn putUint(lb: *LineBuf, v: u64) void {
    var tmp: [24]u8 = undefined;
    lb.put(std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable);
}

fn putFloat(lb: *LineBuf, v: f64, format: Format) void {
    // JSON has no NaN or Infinity. Emitting them would produce a line
    // no parser accepts, so non-finite values degrade to null.
    if (!std.math.isFinite(v)) {
        switch (format) {
            .json => lb.put("null"),
            .text => lb.put(if (std.math.isNan(v)) "NaN" else if (v > 0) "+Inf" else "-Inf"),
        }
        return;
    }
    var tmp: [64]u8 = undefined;
    const s = std.fmt.bufPrint(&tmp, "{d}", .{v}) catch {
        lb.put(if (format == .json) "null" else "?");
        return;
    };
    lb.put(s);
}

fn putJsonValue(lb: *LineBuf, v: Value) void {
    switch (v) {
        .str => |s| putJsonString(lb, s),
        .int => |x| putInt(lb, x),
        .uint => |x| putUint(lb, x),
        .float => |x| putFloat(lb, x, .json),
        .boolean => |x| lb.put(if (x) "true" else "false"),
        .err => |e| putJsonString(lb, @errorName(e)),
        .none => lb.put("null"),
    }
}

fn putTextValue(lb: *LineBuf, v: Value) void {
    switch (v) {
        .str => |s| putTextString(lb, s),
        .int => |x| putInt(lb, x),
        .uint => |x| putUint(lb, x),
        .float => |x| putFloat(lb, x, .text),
        .boolean => |x| lb.put(if (x) "true" else "false"),
        .err => |e| putTextString(lb, @errorName(e)),
        .none => lb.put("<null>"),
    }
}

/// Encode one record into `out`, returning the complete line including
/// its trailing newline. Never fails: an oversized record is truncated
/// at an attribute boundary and flagged with `log_truncated`.
///
/// Field order matches slog (time, level, msg, then attrs in call
/// order) so text output is diff-comparable with the Go build's.
pub fn encode(
    out: []u8,
    format: Format,
    ts_ns: i128,
    level: Level,
    msg: []const u8,
    attrs: []const Attr,
) []const u8 {
    var lb = LineBuf.init(out);
    var ts: [ts_len]u8 = undefined;
    const stamp = formatTimestamp(&ts, ts_ns);

    switch (format) {
        .text => {
            lb.put("time=");
            lb.put(stamp);
            lb.put(" level=");
            lb.put(level.label());
            lb.put(" msg=");
            putTextString(&lb, msg);
            for (attrs) |a| {
                if (lb.over) break;
                // Attributes are written as a unit and rolled back
                // whole on overflow. A half-written `key=` is noise; in
                // the JSON encoder it would be a syntax error.
                const mark = lb.len;
                lb.putByte(' ');
                putTextString(&lb, a.key);
                lb.putByte('=');
                putTextValue(&lb, a.value);
                if (lb.over) {
                    lb.len = mark;
                    break;
                }
            }
            lb.force(if (lb.over) trunc_text else "\n");
        },
        .json => {
            lb.put("{\"time\":\"");
            lb.put(stamp);
            lb.put("\",\"level\":\"");
            lb.put(level.label());
            lb.put("\",\"msg\":");
            putJsonString(&lb, msg);
            for (attrs) |a| {
                if (lb.over) break;
                const mark = lb.len;
                lb.putByte(',');
                putJsonString(&lb, a.key);
                lb.putByte(':');
                putJsonValue(&lb, a.value);
                if (lb.over) {
                    lb.len = mark;
                    break;
                }
            }
            lb.force(if (lb.over) trunc_json else "}\n");
        },
    }
    return lb.slice();
}

// ---------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------

/// A plain thread mutex.
///
/// `std.Io.Mutex` needs an `Io` handle to park a waiter, which the
/// record path neither has nor should need — writing a log line is not
/// an async operation. This is the same three-state algorithm against
/// the OS futex directly: the uncontended path is one atomic
/// compare-exchange and no syscall, which is what keeps a log record
/// cheap when only one thread is logging.
///
/// Public because `logring.zig` needs the same primitive.
pub const Mutex = struct {
    pub const State = enum(u32) { unlocked, locked, contended };

    state: std.atomic.Value(State) = .init(.unlocked),

    /// The critical section is a single `write`, so a contender is
    /// usually only waiting out one syscall — spinning briefly beats
    /// paying for a park/unpark pair.
    const spin_limit = 64;

    pub fn lock(m: *Mutex) void {
        if (m.state.cmpxchgWeak(.unlocked, .locked, .acquire, .monotonic) == null) {
            @branchHint(.likely);
            return;
        }
        m.lockSlow();
    }

    fn lockSlow(m: *Mutex) void {
        @branchHint(.cold);
        var spins: usize = 0;
        while (spins < spin_limit) : (spins += 1) {
            if (m.state.load(.monotonic) == .unlocked and
                m.state.cmpxchgWeak(.unlocked, .locked, .acquire, .monotonic) == null) return;
            std.atomic.spinLoopHint();
        }
        // Marking the lock contended on every attempt costs one
        // spurious wake on unlock but removes the need to track a
        // waiter count.
        while (m.state.swap(.contended, .acquire) != .unlocked) {
            futexWait(&m.state);
        }
    }

    pub fn unlock(m: *Mutex) void {
        switch (m.state.swap(.unlocked, .release)) {
            .unlocked => unreachable, // unlock without lock
            .locked => {},
            .contended => {
                @branchHint(.unlikely);
                futexWake(&m.state);
            },
        }
    }

    fn futexWait(v: *std.atomic.Value(State)) void {
        if (sys.is_linux) {
            _ = linux.futex_4arg(
                &v.raw,
                .{ .cmd = .WAIT, .private = true },
                @intFromEnum(State.contended),
                null,
            );
        } else {
            // Darwin's private futex. NO_ERRNO because we ignore the
            // result either way — a spurious wake just re-checks the
            // state.
            _ = std.c.__ulock_wait(
                .{ .op = .COMPARE_AND_WAIT, .NO_ERRNO = true },
                &v.raw,
                @intFromEnum(State.contended),
                0,
            );
        }
    }

    fn futexWake(v: *std.atomic.Value(State)) void {
        if (sys.is_linux) {
            _ = linux.futex_3arg(&v.raw, .{ .cmd = .WAKE, .private = true }, 1);
        } else {
            _ = std.c.__ulock_wake(.{ .op = .COMPARE_AND_WAIT, .NO_ERRNO = true }, &v.raw, 0);
        }
    }
};

/// Mirror hook. The in-memory ring buffer that feeds the UI's live log
/// view registers here (see `logring.zig`). Kept as an opaque callback
/// rather than a direct `*Ring` so this file has no dependency on the
/// ring — the import goes one way, ring → logger.
pub const Mirror = struct {
    ctx: *anyopaque,
    publish: *const fn (
        ctx: *anyopaque,
        ts_ns: i128,
        level: Level,
        msg: []const u8,
        attrs: []const Attr,
    ) void,
};

/// Where an encoded record goes. `fd` is stdout/stderr or any
/// already-open descriptor; `file` is the rotating log file.
pub const Sink = struct {
    target: union(enum) {
        fd: sys.Fd,
        file: *FileWriter,
    },
    format: Format,
};

/// Upper bound on tee'd destinations. The real deployment uses two
/// (stdout for `docker logs`, the rotating file for post-hoc download);
/// four leaves room without making `Logger` big.
pub const max_sinks = 4;

pub const AddSinkError = error{TooManySinks};

/// The logger. One per process in practice (`default`), but not a
/// singleton — tests construct their own.
///
/// Locking: `mu` is held across encode-and-write for all sinks. The
/// critical section is one `writeAll` per sink and no syscall other
/// than that, because the record was fully encoded into a stack buffer
/// first. `FileWriter` has its own mutex and is only ever locked while
/// holding `mu`, never the reverse, so the order is total and there is
/// no deadlock.
pub const Logger = struct {
    mu: Mutex = .{},
    /// Runtime floor, adjustable after config load. Go used a
    /// `slog.LevelVar` for the same reason: the handler and every child
    /// logger stay valid across a level change.
    min: std.atomic.Value(u8) = .init(@intFromEnum(Level.info)),
    sinks: [max_sinks]Sink = undefined,
    n_sinks: usize = 0,
    mirror: ?Mirror = null,
    /// Injectable so tests get deterministic timestamps.
    clock: *const fn () i128 = sys.realtimeNanos,

    pub fn setLevel(self: *Logger, lvl: Level) void {
        self.min.store(@intFromEnum(lvl), .monotonic);
    }

    pub fn level(self: *const Logger) Level {
        return @enumFromInt(self.min.load(.monotonic));
    }

    pub fn addFd(self: *Logger, fd: sys.Fd, format: Format) AddSinkError!void {
        if (self.n_sinks == max_sinks) return error.TooManySinks;
        self.sinks[self.n_sinks] = .{ .target = .{ .fd = fd }, .format = format };
        self.n_sinks += 1;
    }

    pub fn addFile(self: *Logger, w: *FileWriter, format: Format) AddSinkError!void {
        if (self.n_sinks == max_sinks) return error.TooManySinks;
        self.sinks[self.n_sinks] = .{ .target = .{ .file = w }, .format = format };
        self.n_sinks += 1;
    }

    pub fn setMirror(self: *Logger, m: ?Mirror) void {
        self.mirror = m;
    }

    /// Drop all sinks. Used by tests and by the reconfigure path that
    /// swaps stdout-only for stdout-plus-file after config load.
    pub fn clearSinks(self: *Logger) void {
        self.n_sinks = 0;
    }

    /// Two-stage filter: the comptime half deletes the call site, the
    /// runtime half honours the configured level. `inline` is what makes
    /// the first half reach the caller.
    pub inline fn enabled(self: *const Logger, comptime lvl: Level) bool {
        if (comptime @intFromEnum(lvl) < @intFromEnum(compile_min)) return false;
        return @intFromEnum(lvl) >= self.min.load(.monotonic);
    }

    pub inline fn log(self: *Logger, comptime lvl: Level, msg: []const u8, attrs: []const Attr) void {
        // `comptime` on the first test means the whole body — including
        // materialising `attrs` — is dead code for a filtered level.
        if (comptime @intFromEnum(lvl) < @intFromEnum(compile_min)) return;
        if (@intFromEnum(lvl) < self.min.load(.monotonic)) return;
        self.emit(lvl, msg, attrs);
    }

    pub inline fn debug(self: *Logger, msg: []const u8, attrs: []const Attr) void {
        self.log(.debug, msg, attrs);
    }

    pub inline fn info(self: *Logger, msg: []const u8, attrs: []const Attr) void {
        self.log(.info, msg, attrs);
    }

    pub inline fn warn(self: *Logger, msg: []const u8, attrs: []const Attr) void {
        self.log(.warn, msg, attrs);
    }

    pub inline fn err(self: *Logger, msg: []const u8, attrs: []const Attr) void {
        self.log(.err, msg, attrs);
    }

    /// The cold path. `noinline` so the two 2 KiB buffers never inflate
    /// the frame of a hot function that merely *contains* a log call.
    noinline fn emit(self: *Logger, lvl: Level, msg: []const u8, attrs: []const Attr) void {
        const ts = self.clock();

        var text_buf: [line_buf_size]u8 = undefined;
        var json_buf: [line_buf_size]u8 = undefined;
        var text: ?[]const u8 = null;
        var json: ?[]const u8 = null;

        self.mu.lock();
        defer self.mu.unlock();

        for (self.sinks[0..self.n_sinks]) |sink| {
            // Encode lazily and at most once per format, so teeing to
            // stdout and a file costs one encode, two writes.
            const line = switch (sink.format) {
                .text => text orelse blk: {
                    const l = encode(&text_buf, .text, ts, lvl, msg, attrs);
                    text = l;
                    break :blk l;
                },
                .json => json orelse blk: {
                    const l = encode(&json_buf, .json, ts, lvl, msg, attrs);
                    json = l;
                    break :blk l;
                },
            };
            switch (sink.target) {
                // A failing log sink must not fail the caller — a full
                // disk should not take the downloader down with it.
                .fd => |fd| sys.writeAll(fd, line) catch {},
                .file => |f| f.write(line) catch {},
            }
        }

        // Published under `mu` so the ring's order matches the file's.
        if (self.mirror) |m| m.publish(m.ctx, ts, lvl, msg, attrs);
    }
};

/// Process-wide logger, the equivalent of Go's `slog.SetDefault`.
pub var default: Logger = .{};

pub inline fn debug(msg: []const u8, attrs: []const Attr) void {
    default.log(.debug, msg, attrs);
}

pub inline fn info(msg: []const u8, attrs: []const Attr) void {
    default.log(.info, msg, attrs);
}

pub inline fn warn(msg: []const u8, attrs: []const Attr) void {
    default.log(.warn, msg, attrs);
}

pub inline fn err(msg: []const u8, attrs: []const Attr) void {
    default.log(.err, msg, attrs);
}

pub fn setLevel(lvl: Level) void {
    default.setLevel(lvl);
}

/// Minimal startup wiring: stdout only, matching what `main` does
/// before config has been read. The file sink is added later, once
/// `server.data_dir` is known.
pub fn initDefault(lvl: Level, format: Format) void {
    default.clearSinks();
    default.setLevel(lvl);
    default.addFd(sys.stdout_fd, format) catch unreachable;
}

/// Full wiring, the equivalent of what Go's `cmdServe` assembled by
/// hand out of a `LevelVar`, two `TextHandler`s, a `multiHandler` and
/// the loghub tee.
pub const SetupOptions = struct {
    level: Level = .info,
    /// Format for the stdout sink — `docker logs` consumers usually
    /// want text, log shippers want json.
    stdout_format: Format = .text,
    stdout: bool = true,
    /// Rotating file sink. Null keeps logging stdout-only, which is the
    /// documented fallback when the log directory cannot be opened —
    /// file logging failing must not stop the daemon.
    file: ?*FileWriter = null,
    file_format: Format = .text,
    /// The live-tail mirror, i.e. `ring.mirror()`.
    mirror: ?Mirror = null,
};

pub fn setup(logger: *Logger, opts: SetupOptions) AddSinkError!void {
    logger.clearSinks();
    logger.setLevel(opts.level);
    if (opts.stdout) try logger.addFd(sys.stdout_fd, opts.stdout_format);
    if (opts.file) |f| try logger.addFile(f, opts.file_format);
    logger.setMirror(opts.mirror);
}

/// A comptime-floored view over the default logger.
///
/// This is how a hot subsystem opts out of logging entirely without a
/// build flag: `const hlog = log.Scoped(.warn);` at the top of a file
/// makes every `hlog.debug(...)` in it a compile-time no-op, while
/// `hlog.warn(...)` still works. The floor is the *stricter* of `floor`
/// and the module-wide `compile_min`.
pub fn Scoped(comptime floor: Level) type {
    return struct {
        pub const min_level: Level =
            if (@intFromEnum(floor) > @intFromEnum(compile_min)) floor else compile_min;

        /// Comptime-known: `Scoped(.warn).enabled(.debug)` folds to
        /// `false`, which is what lets the optimiser delete the call.
        pub inline fn enabled(comptime lvl: Level) bool {
            return @intFromEnum(lvl) >= @intFromEnum(min_level);
        }

        pub inline fn to(logger: *Logger, comptime lvl: Level, msg: []const u8, attrs: []const Attr) void {
            if (comptime !enabled(lvl)) return;
            logger.log(lvl, msg, attrs);
        }

        pub inline fn debug(msg: []const u8, attrs: []const Attr) void {
            to(&default, .debug, msg, attrs);
        }

        pub inline fn info(msg: []const u8, attrs: []const Attr) void {
            to(&default, .info, msg, attrs);
        }

        pub inline fn warn(msg: []const u8, attrs: []const Attr) void {
            to(&default, .warn, msg, attrs);
        }

        pub inline fn err(msg: []const u8, attrs: []const Attr) void {
            to(&default, .err, msg, attrs);
        }
    };
}

// ---------------------------------------------------------------------
// Rotating file writer
// ---------------------------------------------------------------------

/// Longest directory path we accept. Log directories live under the
/// configured `data_dir`; 1 KiB is well past anything a container
/// mount produces and keeps `FileWriter` a fixed-size struct with no
/// allocator.
pub const path_max = 1024;

/// Longest rotated filename we recognise while pruning. `hoardarr-`
/// plus a date plus a uniquifier is 30-odd bytes.
const name_max = 64;

/// Rotated names tracked in one prune pass. A directory with more
/// rotated files than this converges over successive rotations rather
/// than in one, which is fine — the bound exists to keep the prune
/// scan on the stack.
const prune_scan_max = 64;

pub const FileWriter = struct {
    pub const Options = struct {
        /// Cap per file. Go picked 8 MiB over SAB's 1 MiB: fewer
        /// rotation events on a chatty debug day, still small enough to
        /// attach to a bug report.
        max_bytes: u64 = 8 << 20,
        /// Upper bound on retained rotated files.
        retain: usize = 14,
        active_name: []const u8 = "hoardarr.log",
        /// Prefix shared by rotated files; also the prune filter.
        prefix: []const u8 = "hoardarr-",
        /// Injectable clock so rotation tests can cross midnight
        /// without waiting for it.
        clock: *const fn () i128 = sys.realtimeNanos,
    };

    pub const OpenError = error{
        DirRequired,
        NameTooLong,
    } || sys.Error;

    pub const WriteError = sys.Error;

    mu: Mutex = .{},
    dir_buf: [path_max]u8 = undefined,
    dir_len: usize = 0,
    fd: sys.Fd = sys.invalid_fd,
    written: u64 = 0,
    opened_on: Date = .{ .year = 0, .month = 1, .day = 1 },
    opts: Options = .{},

    /// In-place init: `FileWriter` contains a mutex and is referenced by
    /// `Sink`, so it must never be copied after first use. Taking
    /// `*FileWriter` makes that impossible to get wrong.
    pub fn open(self: *FileWriter, dir: []const u8, opts: Options) OpenError!void {
        if (dir.len == 0) return error.DirRequired;
        if (dir.len >= path_max) return error.NameTooLong;
        self.* = .{ .opts = opts };
        @memcpy(self.dir_buf[0..dir.len], dir);
        self.dir_len = dir.len;
        try mkdirPath(self.dirSlice());
        try self.openActive();
    }

    fn dirSlice(self: *const FileWriter) []const u8 {
        return self.dir_buf[0..self.dir_len];
    }

    fn joinZ(self: *const FileWriter, out: *[path_max]u8, name: []const u8) error{NameTooLong}![:0]const u8 {
        const dir = self.dirSlice();
        const total = dir.len + 1 + name.len;
        if (total + 1 > path_max) return error.NameTooLong;
        @memcpy(out[0..dir.len], dir);
        out[dir.len] = '/';
        @memcpy(out[dir.len + 1 ..][0..name.len], name);
        out[total] = 0;
        return out[0..total :0];
    }

    fn openActive(self: *FileWriter) OpenError!void {
        var path: [path_max]u8 = undefined;
        const p = try self.joinZ(&path, self.opts.active_name);
        // O_APPEND is what makes concurrent writers (us plus, say, a
        // sidecar) safe at the kernel level: each write is positioned
        // at the current end atomically.
        self.fd = try openFile(p, .{ .create = true, .append = true });
        self.written = try fileSize(self.fd);
        self.opened_on = dateOf(self.opts.clock());
    }

    /// Append one already-encoded record. Rotation is checked here
    /// rather than on a timer so there is no background thread and no
    /// idle wakeup — the whole point of the POSIX-only design.
    pub fn write(self: *FileWriter, bytes: []const u8) WriteError!void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.fd == sys.invalid_fd) return;
        if (self.shouldRotate()) {
            // A failed rotation must not lose the record: fall through
            // and append to the current file, which then simply grows
            // past the cap until the next attempt succeeds.
            self.rotate() catch {};
        }
        try sys.writeAll(self.fd, bytes);
        self.written += bytes.len;
    }

    fn shouldRotate(self: *const FileWriter) bool {
        if (self.written >= self.opts.max_bytes) return true;
        return !self.opened_on.eql(dateOf(self.opts.clock()));
    }

    /// Close, rename to `<prefix>YYYY-MM-DD[-N].log`, prune, reopen.
    fn rotate(self: *FileWriter) OpenError!void {
        sys.close(self.fd);
        self.fd = sys.invalid_fd;

        var name_buf: [name_max]u8 = undefined;
        var from: [path_max]u8 = undefined;
        var to: [path_max]u8 = undefined;

        const target = self.rotatedName(&name_buf) catch |e| {
            // Reopen so logging survives even if we could not pick a
            // name; the file just keeps growing.
            try self.openActive();
            return e;
        };
        const src = try self.joinZ(&from, self.opts.active_name);
        const dst = try self.joinZ(&to, target);
        renameFile(src, dst) catch {};
        self.pruneOld();
        try self.openActive();
    }

    /// First unused `<prefix><date>.log`, `<prefix><date>-1.log`, ...
    /// The date is the day the file was *opened*, so a file rotated at
    /// 00:00:01 is stamped with the day it actually covers.
    fn rotatedName(self: *const FileWriter, out: *[name_max]u8) error{NameTooLong}![]const u8 {
        const d = self.opened_on;
        const base_len = self.opts.prefix.len + 10; // prefix + YYYY-MM-DD
        if (base_len + 16 > name_max) return error.NameTooLong;
        @memcpy(out[0..self.opts.prefix.len], self.opts.prefix);
        var p = self.opts.prefix.len;
        const year: u64 = if (d.year < 0) 0 else @intCast(d.year);
        padDigits(out[p..][0..4], year);
        p += 4;
        out[p] = '-';
        p += 1;
        padDigits(out[p..][0..2], d.month);
        p += 2;
        out[p] = '-';
        p += 1;
        padDigits(out[p..][0..2], d.day);
        p += 2;
        const stem = p;

        var i: usize = 0;
        while (i < 1000) : (i += 1) {
            var q = stem;
            if (i > 0) {
                const suffix = std.fmt.bufPrint(out[q..], "-{d}", .{i}) catch return error.NameTooLong;
                q += suffix.len;
            }
            const tail = ".log";
            if (q + tail.len > name_max) return error.NameTooLong;
            @memcpy(out[q..][0..tail.len], tail);
            q += tail.len;
            const candidate = out[0..q];

            var probe: [path_max]u8 = undefined;
            const full = try self.joinZ(&probe, candidate);
            if (!pathExists(full)) return candidate;
        }
        return error.NameTooLong;
    }

    /// Delete the oldest rotated files beyond `retain`. Names sort
    /// lexicographically because the stamp is `YYYY-MM-DD`, so "oldest"
    /// is just "smallest".
    fn pruneOld(self: *FileWriter) void {
        var names: [prune_scan_max][name_max]u8 = undefined;
        var lens: [prune_scan_max]u8 = undefined;
        var n: usize = 0;

        var it = DirIter.open(self.dirSlice()) catch return;
        defer it.close();
        while (it.next()) |entry| {
            if (n == prune_scan_max) break;
            if (entry.len > name_max) continue;
            if (std.mem.eql(u8, entry, self.opts.active_name)) continue;
            if (!std.mem.startsWith(u8, entry, self.opts.prefix)) continue;
            if (!std.mem.endsWith(u8, entry, ".log")) continue;
            @memcpy(names[n][0..entry.len], entry);
            lens[n] = @intCast(entry.len);
            n += 1;
        }
        if (n <= self.opts.retain) return;

        // Index sort: moving 64-byte name buffers around is pointless
        // when we only need the ordering.
        var order: [prune_scan_max]usize = undefined;
        for (0..n) |i| order[i] = i;
        const Ctx = struct {
            names: *const [prune_scan_max][name_max]u8,
            lens: *const [prune_scan_max]u8,
            fn lessThan(c: @This(), a: usize, b: usize) bool {
                const sa = c.names[a][0..c.lens[a]];
                const sb = c.names[b][0..c.lens[b]];
                return std.mem.order(u8, sa, sb) == .lt;
            }
        };
        std.mem.sort(usize, order[0..n], Ctx{ .names = &names, .lens = &lens }, Ctx.lessThan);

        const drop = n - self.opts.retain;
        for (order[0..drop]) |idx| {
            var path: [path_max]u8 = undefined;
            const p = self.joinZ(&path, names[idx][0..lens[idx]]) catch continue;
            unlinkFile(p) catch {};
        }
    }

    pub fn close(self: *FileWriter) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.fd == sys.invalid_fd) return;
        sys.close(self.fd);
        self.fd = sys.invalid_fd;
    }

    pub fn sink(self: *FileWriter, format: Format) Sink {
        return .{ .target = .{ .file = self }, .format = format };
    }
};

/// Reject anything that is not one of our log files, and anything with
/// a path separator or a `.` component. Guards the download endpoint's
/// filename parameter — the Go original is `logfile.SafePath`.
pub fn safeName(name: []const u8, opts: FileWriter.Options) bool {
    if (name.len == 0 or name.len > name_max) return false;
    if (std.mem.indexOfAny(u8, name, "/\\") != null) return false;
    if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) return false;
    if (std.mem.indexOf(u8, name, "..") != null) return false;
    if (std.mem.eql(u8, name, opts.active_name)) return true;
    return std.mem.startsWith(u8, name, opts.prefix) and std.mem.endsWith(u8, name, ".log");
}

// ---------------------------------------------------------------------
// Filesystem syscalls
//
// `posix/sys.zig` covers sockets, poll and the clocks but not yet the
// path-based file calls, and this module must not reach for
// `std.Io.File` (which wants an `Io` instance and a buffered writer —
// both at odds with "one write syscall per record"). These are the same
// dual-path shape sys.zig uses and belong there once it grows a
// filesystem section.
// ---------------------------------------------------------------------

const linux = std.os.linux;

const O = if (sys.is_linux) linux.O else std.c.O;

/// `sys.Error` covers errno but not our own path-length limit.
pub const PathError = error{NameTooLong} || sys.Error;

fn errnoOf(rc: usize) sys.E {
    return linux.errno(rc);
}

const OpenFlags = struct {
    create: bool = false,
    append: bool = false,
    directory: bool = false,
};

fn openFile(path: [:0]const u8, flags: OpenFlags) sys.Error!sys.Fd {
    while (true) {
        if (sys.is_linux) {
            var o: O = .{ .CLOEXEC = true };
            if (flags.directory) {
                o.ACCMODE = .RDONLY;
                o.DIRECTORY = true;
            } else {
                o.ACCMODE = .WRONLY;
                o.CREAT = flags.create;
                o.APPEND = flags.append;
            }
            const rc = linux.open(path.ptr, o, 0o644);
            switch (errnoOf(rc)) {
                .SUCCESS => return @intCast(rc),
                .INTR => continue,
                else => |e| return sys.mapError(e),
            }
        } else {
            var o: O = .{ .CLOEXEC = true };
            if (flags.directory) {
                o.ACCMODE = .RDONLY;
                o.DIRECTORY = true;
            } else {
                o.ACCMODE = .WRONLY;
                o.CREAT = flags.create;
                o.APPEND = flags.append;
            }
            const rc = std.c.open(path.ptr, o, @as(c_uint, 0o644));
            if (rc >= 0) return rc;
            const e = cErrno();
            if (e == .INTR) continue;
            return sys.mapError(e);
        }
    }
}

fn cErrno() sys.E {
    return @enumFromInt(std.c._errno().*);
}

fn fileSize(fd: sys.Fd) sys.Error!u64 {
    // lseek to END rather than fstat: one syscall, no `struct stat`
    // layout differences between Linux and Darwin to reproduce, and the
    // fd is O_APPEND so the offset is meaningless for writing anyway.
    if (sys.is_linux) {
        const SEEK_END: usize = 2;
        const rc = linux.lseek(fd, 0, SEEK_END);
        switch (errnoOf(rc)) {
            .SUCCESS => return @intCast(rc),
            else => |e| return sys.mapError(e),
        }
    }
    const rc = std.c.lseek(fd, 0, @as(std.c.whence_t, 2));
    if (rc < 0) return sys.mapError(cErrno());
    return @intCast(rc);
}

fn pathExists(path: [:0]const u8) bool {
    if (sys.is_linux) {
        const F_OK: u32 = 0;
        return errnoOf(linux.access(path.ptr, F_OK)) == .SUCCESS;
    }
    return std.c.access(path.ptr, 0) == 0;
}

fn renameFile(from: [:0]const u8, to: [:0]const u8) sys.Error!void {
    if (sys.is_linux) {
        const rc = linux.rename(from.ptr, to.ptr);
        switch (errnoOf(rc)) {
            .SUCCESS => return,
            else => |e| return sys.mapError(e),
        }
    }
    if (std.c.rename(from.ptr, to.ptr) != 0) return sys.mapError(cErrno());
}

fn unlinkFile(path: [:0]const u8) sys.Error!void {
    if (sys.is_linux) {
        const rc = linux.unlink(path.ptr);
        switch (errnoOf(rc)) {
            .SUCCESS => return,
            else => |e| return sys.mapError(e),
        }
    }
    if (std.c.unlink(path.ptr) != 0) return sys.mapError(cErrno());
}

fn mkdirOne(path: [:0]const u8) sys.Error!void {
    if (sys.is_linux) {
        const rc = linux.mkdir(path.ptr, 0o755);
        switch (errnoOf(rc)) {
            .SUCCESS, .EXIST => return,
            else => |e| return sys.mapError(e),
        }
    } else {
        if (std.c.mkdir(path.ptr, 0o755) == 0) return;
        const e = cErrno();
        if (e == .EXIST) return;
        return sys.mapError(e);
    }
}

/// `mkdir -p`. Walks the path creating each component; the log
/// directory is `<data_dir>/logs` and neither level is guaranteed to
/// exist on a fresh volume.
fn mkdirPath(dir: []const u8) PathError!void {
    var buf: [path_max]u8 = undefined;
    if (dir.len + 1 > path_max) return error.NameTooLong;
    @memcpy(buf[0..dir.len], dir);
    buf[dir.len] = 0;

    var i: usize = if (dir[0] == '/') 1 else 0;
    while (i < dir.len) : (i += 1) {
        if (buf[i] != '/') continue;
        buf[i] = 0;
        // Intermediate failures are ignored: a component may already
        // exist in a form we cannot stat but can still traverse. Only
        // the final mkdir has to succeed.
        mkdirOne(buf[0..i :0]) catch {};
        buf[i] = '/';
    }
    try mkdirOne(buf[0..dir.len :0]);
}

/// Directory iteration. Linux uses `getdents64`; Darwin uses
/// `getdirentries` with its own `dirent` layout (16-bit `namlen`
/// instead of a NUL-terminated name).
const DirIter = struct {
    fd: sys.Fd,
    buf: [4096]u8 align(8) = undefined,
    index: usize = 0,
    end: usize = 0,
    seek: i64 = 0,
    done: bool = false,

    fn open(dir: []const u8) PathError!DirIter {
        var buf: [path_max]u8 = undefined;
        if (dir.len + 1 > path_max) return error.NameTooLong;
        @memcpy(buf[0..dir.len], dir);
        buf[dir.len] = 0;
        const fd = try openFile(buf[0..dir.len :0], .{ .directory = true });
        return .{ .fd = fd };
    }

    fn close(self: *DirIter) void {
        sys.close(self.fd);
        self.fd = sys.invalid_fd;
    }

    fn refill(self: *DirIter) bool {
        if (self.done) return false;
        while (true) {
            const n: usize = if (sys.is_linux) blk: {
                const rc = linux.getdents64(self.fd, &self.buf, self.buf.len);
                switch (errnoOf(rc)) {
                    .SUCCESS => break :blk @intCast(rc),
                    .INTR => continue,
                    else => {
                        self.done = true;
                        return false;
                    },
                }
            } else blk: {
                const rc = std.c.getdirentries(self.fd, &self.buf, self.buf.len, &self.seek);
                if (rc < 0) {
                    if (cErrno() == .INTR) continue;
                    self.done = true;
                    return false;
                }
                break :blk @intCast(rc);
            };
            if (n == 0) {
                self.done = true;
                return false;
            }
            self.index = 0;
            self.end = n;
            return true;
        }
    }

    /// Returns a name borrowed from the internal buffer, valid until
    /// the next `next()` call.
    fn next(self: *DirIter) ?[]const u8 {
        while (true) {
            if (self.index >= self.end) {
                if (!self.refill()) return null;
            }
            if (sys.is_linux) {
                const e: *align(1) const linux.dirent64 = @ptrCast(&self.buf[self.index]);
                if (e.reclen == 0) {
                    self.done = true;
                    return null;
                }
                const name_ptr: [*:0]const u8 = @ptrCast(&self.buf[self.index + @offsetOf(linux.dirent64, "name")]);
                self.index += e.reclen;
                const name = std.mem.span(name_ptr);
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
                return name;
            } else {
                const e: *align(1) const std.c.dirent = @ptrCast(&self.buf[self.index]);
                if (e.reclen == 0) {
                    self.done = true;
                    return null;
                }
                const base = self.index + @offsetOf(std.c.dirent, "name");
                const namlen = e.namlen;
                self.index += e.reclen;
                if (e.ino == 0) continue;
                const name = self.buf[base..][0..namlen];
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
                return name;
            }
        }
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const t = std.testing;

test {
    // Pulls the ring buffer's own test block into this binary so
    // `zig test` on log.zig covers both halves of the module.
    _ = @import("logring.zig");
}

test "level labels and names" {
    try t.expectEqualStrings("DEBUG", Level.debug.label());
    try t.expectEqualStrings("INFO", Level.info.label());
    try t.expectEqualStrings("WARN", Level.warn.label());
    try t.expectEqualStrings("ERROR", Level.err.label());
    // Lower-case names are the config/UI vocabulary; note `error`, not `err`.
    try t.expectEqualStrings("error", Level.err.name());
}

test "level parse matches config vocabulary" {
    // config.zig validates server.log_level against exactly this set
    // and lower-cases env overrides, so parse takes lower-case only.
    try t.expectEqual(Level.debug, Level.parse("debug").?);
    try t.expectEqual(Level.info, Level.parse("info").?);
    try t.expectEqual(Level.warn, Level.parse("warn").?);
    try t.expectEqual(Level.err, Level.parse("error").?);
    try t.expect(Level.parse("verbose") == null);
    try t.expect(Level.parse("DEBUG") == null);
}

test "level ordering is the filter" {
    try t.expect(@intFromEnum(Level.debug) < @intFromEnum(Level.info));
    try t.expect(@intFromEnum(Level.info) < @intFromEnum(Level.warn));
    try t.expect(@intFromEnum(Level.warn) < @intFromEnum(Level.err));
}

test "civil date round-trips known days" {
    try t.expectEqual(Date{ .year = 1970, .month = 1, .day = 1 }, civilFromDays(0));
    try t.expectEqual(Date{ .year = 1969, .month = 12, .day = 31 }, civilFromDays(-1));
    try t.expectEqual(Date{ .year = 2000, .month = 3, .day = 1 }, civilFromDays(11017));
    try t.expectEqual(Date{ .year = 2024, .month = 2, .day = 29 }, civilFromDays(19782));
}

test "timestamp is fixed-width RFC3339" {
    var buf: [ts_len]u8 = undefined;
    const s = formatTimestamp(&buf, 0);
    try t.expectEqualStrings("1970-01-01T00:00:00.000Z", s);

    // 2024-02-29T13:45:07.123Z
    const ns: i128 = (@as(i128, 19782) * std.time.s_per_day + 13 * 3600 + 45 * 60 + 7) *
        std.time.ns_per_s + 123 * std.time.ns_per_ms;
    try t.expectEqualStrings("2024-02-29T13:45:07.123Z", formatTimestamp(&buf, ns));
    try t.expectEqual(@as(usize, ts_len), formatTimestamp(&buf, ns).len);
}

test "text encoding matches slog field order" {
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .text, 0, .info, "config loaded", &.{
        str("path", "./config.toml"),
        str("log_level", "debug"),
        uint("conns", 8),
    });
    try t.expectEqualStrings(
        "time=1970-01-01T00:00:00.000Z level=INFO msg=\"config loaded\" " ++
            "path=./config.toml log_level=debug conns=8\n",
        line,
    );
}

test "text encoding quotes only when needed" {
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .text, 0, .warn, "plain", &.{
        str("bare", "value"),
        str("spaced", "two words"),
        str("empty", ""),
        str("equals", "a=b"),
        boolean("ok", true),
        int("delta", -5),
    });
    try t.expectEqualStrings(
        "time=1970-01-01T00:00:00.000Z level=WARN msg=plain " ++
            "bare=value spaced=\"two words\" empty=\"\" equals=\"a=b\" ok=true delta=-5\n",
        line,
    );
}

test "record ends in exactly one newline" {
    var buf: [line_buf_size]u8 = undefined;
    for ([_]Format{ .text, .json }) |f| {
        const line = encode(&buf, f, 0, .info, "m", &.{str("k", "v")});
        try t.expect(line.len > 1);
        try t.expectEqual(@as(u8, '\n'), line[line.len - 1]);
        // One write syscall per record means the newline must be the
        // only one and must be inside the buffer.
        try t.expectEqual(@as(?usize, line.len - 1), std.mem.indexOfScalar(u8, line, '\n'));
    }
}

test "json encoding emits numbers unquoted and strings quoted" {
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .json, 0, .err, "download failed", &.{
        str("nzb", "Some.Release.nzb"),
        uint("bytes", 4096),
        int("delta", -17),
        boolean("retry", false),
        errv("err", error.ConnectionRefused),
        none("cause"),
    });
    try t.expectEqualStrings(
        "{\"time\":\"1970-01-01T00:00:00.000Z\",\"level\":\"ERROR\",\"msg\":\"download failed\"," ++
            "\"nzb\":\"Some.Release.nzb\",\"bytes\":4096,\"delta\":-17,\"retry\":false," ++
            "\"err\":\"ConnectionRefused\",\"cause\":null}\n",
        line,
    );
}

test "json escapes control characters, quotes and backslashes" {
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .json, 0, .info, "m", &.{
        str("v", "a\"b\\c\nd\te\rf\x00g\x1Fh"),
    });
    try t.expect(std.mem.indexOf(u8, line, "\\\"") != null);
    try t.expect(std.mem.indexOf(u8, line, "\\\\") != null);
    try t.expect(std.mem.indexOf(u8, line, "\\n") != null);
    try t.expect(std.mem.indexOf(u8, line, "\\t") != null);
    try t.expect(std.mem.indexOf(u8, line, "\\r") != null);
    try t.expect(std.mem.indexOf(u8, line, "\\u0000") != null);
    try t.expect(std.mem.indexOf(u8, line, "\\u001f") != null);
    // No raw control byte may survive into the line.
    for (line[0 .. line.len - 1]) |c| try t.expect(c >= 0x20 or c == '\t' or c == '\n' or c == '\r');
    try t.expect(std.mem.indexOfScalar(u8, line, 0) == null);
}

test "json output parses for adversarial attribute values" {
    // The values here stand in for NZB filenames and NNTP subjects,
    // which arrive from the internet. Every one of them must produce a
    // line std.json accepts.
    var long: [4096]u8 = undefined;
    @memset(&long, 'A');
    var quotes: [512]u8 = undefined;
    @memset(&quotes, '"');
    var nuls: [512]u8 = undefined;
    @memset(&nuls, 0);
    var high: [256]u8 = undefined;
    for (&high, 0..) |*b, i| b.* = @intCast(0x80 + (i % 0x80));

    const cases = [_][]const u8{
        "",
        "\"",
        "\\",
        "\\\"",
        "\n\r\t",
        "\x00",
        "a\x00b",
        "\x1b[31mred\x1b[0m",
        // Invalid UTF-8: lone continuation, truncated 3-byte lead,
        // overlong encoding, 5-byte lead, encoded surrogate half.
        "\x80",
        "\xC3",
        "\xE2\x82",
        "\xC0\xAF",
        "\xF8\x88\x80\x80\x80",
        "\xED\xA0\x80",
        // Valid multi-byte must survive unchanged.
        "héllo → 世界 🎬",
        &long,
        &quotes,
        &nuls,
        &high,
        "{\"injected\":true}",
        "},{\"level\":\"FAKE\"",
    };

    for (cases) |v| {
        var buf: [line_buf_size]u8 = undefined;
        const line = encode(&buf, .json, 1_700_000_000_000_000_000, .info, "hostile", &.{
            str("value", v),
            str(v, "key-is-hostile-too"),
            uint("n", 1),
        });
        try t.expect(std.mem.endsWith(u8, line, "\n"));
        const body = line[0 .. line.len - 1];
        try t.expect(try std.json.validate(t.allocator, body));

        // And it must parse into the shape we promised.
        const parsed = try std.json.parseFromSlice(std.json.Value, t.allocator, body, .{});
        defer parsed.deinit();
        try t.expect(parsed.value == .object);
        try t.expectEqualStrings("hostile", parsed.value.object.get("msg").?.string);
        try t.expectEqualStrings("INFO", parsed.value.object.get("level").?.string);
        // Output is always valid UTF-8 — invalid input bytes became U+FFFD.
        try t.expect(std.unicode.utf8ValidateSlice(body));
    }
}

test "message survives truncation with a valid closing brace" {
    var huge: [line_buf_size * 4]u8 = undefined;
    @memset(&huge, 'x');
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .json, 0, .info, &huge, &.{str("dropped", "attr")});
    try t.expect(line.len <= line_buf_size);
    try t.expect(std.mem.endsWith(u8, line, trunc_json));
    const body = line[0 .. line.len - 1];
    try t.expect(try std.json.validate(t.allocator, body));
    // The attribute was rolled back whole rather than half-written.
    try t.expect(std.mem.indexOf(u8, line, "dropped") == null);
}

test "overflowing attributes truncate at an attribute boundary" {
    var value: [400]u8 = undefined;
    @memset(&value, 'v');
    var attrs: [16]Attr = undefined;
    for (&attrs, 0..) |*a, i| {
        _ = i;
        a.* = str("key", &value);
    }
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .json, 0, .info, "m", &attrs);
    try t.expect(line.len <= line_buf_size);
    try t.expect(std.mem.endsWith(u8, line, trunc_json));
    const body = line[0 .. line.len - 1];
    try t.expect(try std.json.validate(t.allocator, body));

    const text_line = encode(&buf, .text, 0, .info, "m", &attrs);
    try t.expect(std.mem.endsWith(u8, text_line, trunc_text));
}

test "any() infers attribute types at comptime" {
    const E = enum { alpha, beta };
    var buf: [line_buf_size]u8 = undefined;
    const opt: ?u16 = null;
    const line = encode(&buf, .json, 0, .info, "m", &.{
        any("s", "text"),
        any("i", @as(i32, -3)),
        any("u", @as(u8, 200)),
        any("f", @as(f32, 1.5)),
        any("b", true),
        any("e", E.beta),
        any("o", opt),
        any("er", error.Timeout),
    });
    try t.expectEqualStrings(
        "{\"time\":\"1970-01-01T00:00:00.000Z\",\"level\":\"INFO\",\"msg\":\"m\"," ++
            "\"s\":\"text\",\"i\":-3,\"u\":200,\"f\":1.5,\"b\":true,\"e\":\"beta\"," ++
            "\"o\":null,\"er\":\"Timeout\"}\n",
        line,
    );
}

test "non-finite floats degrade to null in json" {
    var buf: [line_buf_size]u8 = undefined;
    const line = encode(&buf, .json, 0, .info, "m", &.{
        float("nan", std.math.nan(f64)),
        float("inf", std.math.inf(f64)),
    });
    try t.expect(std.mem.indexOf(u8, line, "\"nan\":null") != null);
    try t.expect(std.mem.indexOf(u8, line, "\"inf\":null") != null);
    try t.expect(try std.json.validate(t.allocator, line[0 .. line.len - 1]));

    const text_line = encode(&buf, .text, 0, .info, "m", &.{float("inf", -std.math.inf(f64))});
    try t.expect(std.mem.indexOf(u8, text_line, "inf=-Inf") != null);
}

fn fixedClock() i128 {
    return 1_700_000_000_000_000_000;
}

test "logger honours the runtime level" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var fw: FileWriter = undefined;
    try fw.open(dir, .{ .clock = fixedClock });
    defer fw.close();

    var logger: Logger = .{ .clock = fixedClock };
    try logger.addFile(&fw, .text);
    logger.setLevel(.warn);

    logger.debug("noisy", &.{});
    logger.info("also-ignored", &.{});
    logger.warn("important", &.{});
    logger.err("worse", &.{});

    const body = try readActive(dir, &fw);
    defer t.allocator.free(body);
    try t.expect(std.mem.indexOf(u8, body, "noisy") == null);
    try t.expect(std.mem.indexOf(u8, body, "also-ignored") == null);
    try t.expect(std.mem.indexOf(u8, body, "important") != null);
    try t.expect(std.mem.indexOf(u8, body, "worse") != null);
    try t.expectEqual(@as(usize, 2), std.mem.count(u8, body, "\n"));
}

test "logger tees one encode to two sinks" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var text_w: FileWriter = undefined;
    try text_w.open(dir, .{ .clock = fixedClock });
    defer text_w.close();

    var json_w: FileWriter = undefined;
    try json_w.open(dir, .{ .active_name = "json.log", .prefix = "json-", .clock = fixedClock });
    defer json_w.close();

    var logger: Logger = .{ .clock = fixedClock };
    try logger.addFile(&text_w, .text);
    try logger.addFile(&json_w, .json);
    logger.setLevel(.info);
    logger.info("teed", &.{str("k", "v")});

    const text_body = try readFileAlloc(dir, "hoardarr.log");
    defer t.allocator.free(text_body);
    const json_body = try readFileAlloc(dir, "json.log");
    defer t.allocator.free(json_body);

    try t.expect(std.mem.startsWith(u8, text_body, "time="));
    try t.expect(std.mem.startsWith(u8, json_body, "{\"time\":"));
    try t.expect(try std.json.validate(t.allocator, std.mem.trimEnd(u8, json_body, "\n")));
}

test "compile-time floor removes call sites below it" {
    // The project default floor is `.debug`, so nothing is elided in
    // this build; the assertion documents the wiring, and the constant
    // is what a release root overrides.
    try t.expectEqual(Level.debug, compile_min);

    var logger: Logger = .{ .clock = fixedClock };
    logger.setLevel(.debug);
    // With no sinks this is a no-op, but it proves `enabled` composes
    // the comptime and runtime halves without a sink attached.
    try t.expect(logger.enabled(.debug));
    logger.setLevel(.err);
    try t.expect(!logger.enabled(.warn));
    try t.expect(logger.enabled(.err));
}

test "scoped floor is comptime-known, which is what deletes the call site" {
    const Hot = Scoped(.warn);
    // Folding these in a `comptime` block is the actual proof: if the
    // filter were not comptime-known this would not compile.
    comptime {
        std.debug.assert(!Hot.enabled(.debug));
        std.debug.assert(!Hot.enabled(.info));
        std.debug.assert(Hot.enabled(.warn));
        std.debug.assert(Hot.enabled(.err));
    }
    // The scope tightens but never loosens the module floor.
    try t.expectEqual(Level.warn, Hot.min_level);
    try t.expectEqual(compile_min, Scoped(.debug).min_level);
}

test "scoped floor wins over the runtime level" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var fw: FileWriter = undefined;
    try fw.open(dir, .{ .clock = fixedClock });
    defer fw.close();

    var logger: Logger = .{ .clock = fixedClock };
    try logger.addFile(&fw, .text);
    // Runtime level wide open: only the comptime floor can suppress.
    logger.setLevel(.debug);

    const Hot = Scoped(.warn);
    Hot.to(&logger, .debug, "compiled-out", &.{});
    Hot.to(&logger, .info, "compiled-out-too", &.{});
    Hot.to(&logger, .warn, "kept", &.{});

    const body = try readActive(dir, &fw);
    defer t.allocator.free(body);
    try t.expect(std.mem.indexOf(u8, body, "compiled-out") == null);
    try t.expect(std.mem.indexOf(u8, body, "kept") != null);
    try t.expectEqual(@as(usize, 1), std.mem.count(u8, body, "\n"));
}

test "setup wires stdout, file and mirror in one call" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var fw: FileWriter = undefined;
    try fw.open(dir, .{ .clock = fixedClock });
    defer fw.close();

    const Counter = struct {
        var seen: usize = 0;
        fn publish(_: *anyopaque, _: i128, _: Level, _: []const u8, _: []const Attr) void {
            seen += 1;
        }
    };
    Counter.seen = 0;

    var logger: Logger = .{ .clock = fixedClock };
    var dummy: u8 = 0;
    // stdout off so the test does not pollute the test runner's output.
    try setup(&logger, .{
        .level = .info,
        .stdout = false,
        .file = &fw,
        .file_format = .json,
        .mirror = .{ .ctx = &dummy, .publish = Counter.publish },
    });
    try t.expectEqual(@as(usize, 1), logger.n_sinks);
    logger.debug("filtered", &.{});
    logger.info("wired", &.{});

    // The mirror is a tee: it sees exactly what passed the level filter,
    // no more and no less.
    try t.expectEqual(@as(usize, 1), Counter.seen);
    const body = try readActive(dir, &fw);
    defer t.allocator.free(body);
    try t.expect(std.mem.startsWith(u8, body, "{\"time\":"));
    try t.expect(std.mem.indexOf(u8, body, "\"msg\":\"wired\"") != null);
    try t.expect(std.mem.indexOf(u8, body, "filtered") == null);

    // Re-running setup replaces sinks rather than accumulating them —
    // the daemon does exactly this once config has loaded.
    try setup(&logger, .{ .level = .warn, .stdout = false, .file = &fw });
    try t.expectEqual(@as(usize, 1), logger.n_sinks);
    try t.expect(logger.mirror == null);
}

test "sink limit is reported, not silently dropped" {
    var logger: Logger = .{ .clock = fixedClock };
    for (0..max_sinks) |_| try logger.addFd(sys.stderr_fd, .text);
    try t.expectError(error.TooManySinks, logger.addFd(sys.stderr_fd, .text));
    logger.clearSinks();
    try logger.addFd(sys.stderr_fd, .text);
}

test "safeName rejects traversal and foreign files" {
    const o = FileWriter.Options{};
    try t.expect(safeName("hoardarr.log", o));
    try t.expect(safeName("hoardarr-2026-07-30.log", o));
    try t.expect(safeName("hoardarr-2026-07-30-3.log", o));
    try t.expect(!safeName("", o));
    try t.expect(!safeName("..", o));
    try t.expect(!safeName("../../etc/passwd", o));
    try t.expect(!safeName("hoardarr-../x.log", o));
    try t.expect(!safeName("/etc/passwd", o));
    try t.expect(!safeName("config.toml", o));
    try t.expect(!safeName("hoardarr-2026-07-30.txt", o));
}

// --- file writer -----------------------------------------------------

/// Absolute path of a `testing.tmpDir`. `tmpDir` hands back an open
/// `Io.Dir` but the writer is path-based, so rebuild the path the same
/// way `testing` constructs it.
fn tmpPath(tmp: *t.TmpDir) ![]const u8 {
    // Relative to the test process's cwd, which is where `tmpDir`
    // itself rooted the directory. Keeps the writer's path handling
    // under test without depending on an absolute-path helper.
    return std.fmt.allocPrint(t.allocator, ".zig-cache/tmp/{s}/logs", .{tmp.sub_path});
}

fn readFileAlloc(dir: []const u8, name: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(t.allocator, "{s}/{s}", .{ dir, name });
    defer t.allocator.free(path);
    var io_threaded: std.Io.Threaded = .init(t.allocator, .{});
    defer io_threaded.deinit();
    const io = io_threaded.io();
    return std.Io.Dir.cwd().readFileAlloc(io, path, t.allocator, .unlimited);
}

fn readActive(dir: []const u8, w: *FileWriter) ![]u8 {
    return readFileAlloc(dir, w.opts.active_name);
}

fn countFiles(dir: []const u8, prefix: []const u8) !usize {
    var it = try DirIter.open(dir);
    defer it.close();
    var n: usize = 0;
    while (it.next()) |name| {
        if (std.mem.startsWith(u8, name, prefix) and std.mem.endsWith(u8, name, ".log")) n += 1;
    }
    return n;
}

test "file writer creates the directory and appends" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var w: FileWriter = undefined;
    // Two levels deep: mkdirPath has to create both.
    const nested = try std.fmt.allocPrint(t.allocator, "{s}/nested", .{dir});
    defer t.allocator.free(nested);
    try w.open(nested, .{ .clock = fixedClock });
    defer w.close();

    try w.write("first\n");
    try w.write("second\n");
    const body = try readFileAlloc(nested, "hoardarr.log");
    defer t.allocator.free(body);
    try t.expectEqualStrings("first\nsecond\n", body);
}

test "reopening an existing file resumes its size" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var w: FileWriter = undefined;
    try w.open(dir, .{ .clock = fixedClock });
    try w.write("0123456789");
    try t.expectEqual(@as(u64, 10), w.written);
    w.close();

    var w2: FileWriter = undefined;
    try w2.open(dir, .{ .clock = fixedClock });
    defer w2.close();
    // Picked up from lseek(END), not reset to zero — otherwise a
    // restart-happy container would never rotate.
    try t.expectEqual(@as(u64, 10), w2.written);
    try w2.write("abc");
    const body = try readFileAlloc(dir, "hoardarr.log");
    defer t.allocator.free(body);
    try t.expectEqualStrings("0123456789abc", body);
}

test "file writer rotates at the size threshold and preserves order" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var w: FileWriter = undefined;
    try w.open(dir, .{ .max_bytes = 64, .clock = fixedClock });
    defer w.close();

    // 20 records of 9 bytes against a 64-byte cap: the writer rotates
    // before the write that would cross it, so every file holds a whole
    // number of records.
    const rec_len = 9;
    for (0..20) |i| {
        var line: [rec_len]u8 = undefined;
        const s = try std.fmt.bufPrint(&line, "line{d:0>4}\n", .{i});
        try t.expectEqual(@as(usize, rec_len), s.len);
        try w.write(s);
    }

    // The active file holds the tail, ending on the newest record.
    const active = try readFileAlloc(dir, "hoardarr.log");
    defer t.allocator.free(active);
    try t.expect(active.len > 0);
    try t.expect(active.len < 64 + rec_len);
    try t.expect(std.mem.endsWith(u8, active, "line0019\n"));

    // 20 * 9 = 180 bytes over a 64-byte cap: at least two rotations.
    const rotated = try countFiles(dir, "hoardarr-");
    try t.expect(rotated >= 2);

    // Every byte written must still be on disk somewhere, and no record
    // may be split across a rotation boundary.
    var total: usize = 0;
    var it = try DirIter.open(dir);
    defer it.close();
    while (it.next()) |name| {
        if (!std.mem.endsWith(u8, name, ".log")) continue;
        const body = try readFileAlloc(dir, name);
        defer t.allocator.free(body);
        try t.expect(body.len % rec_len == 0);
        total += body.len;
    }
    try t.expectEqual(@as(usize, 20 * rec_len), total);
}

test "rotated names get a uniquifier within the same day" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var w: FileWriter = undefined;
    try w.open(dir, .{ .max_bytes = 16, .clock = fixedClock });
    defer w.close();

    for (0..6) |_| try w.write("0123456789abcdef\n");

    // The clock is frozen, so every rotation stamps the same date and
    // the uniquifier is the only thing separating them.
    var it = try DirIter.open(dir);
    defer it.close();
    var saw_plain = false;
    var saw_suffixed = false;
    while (it.next()) |name| {
        if (std.mem.eql(u8, name, "hoardarr-2023-11-14.log")) saw_plain = true;
        if (std.mem.startsWith(u8, name, "hoardarr-2023-11-14-")) saw_suffixed = true;
    }
    try t.expect(saw_plain);
    try t.expect(saw_suffixed);
}

var movable_now: i128 = 1_700_000_000_000_000_000;

fn movableClock() i128 {
    return movable_now;
}

test "file writer rotates when the date crosses" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    movable_now = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
    var w: FileWriter = undefined;
    try w.open(dir, .{ .clock = movableClock });
    defer w.close();
    try w.write("before midnight\n");

    // Nowhere near max_bytes — only the date change may trigger this.
    movable_now += 2 * std.time.ns_per_s * std.time.s_per_day;
    try w.write("after midnight\n");

    const active = try readFileAlloc(dir, "hoardarr.log");
    defer t.allocator.free(active);
    try t.expectEqualStrings("after midnight\n", active);

    const rotated = try readFileAlloc(dir, "hoardarr-2023-11-14.log");
    defer t.allocator.free(rotated);
    try t.expectEqualStrings("before midnight\n", rotated);
}

test "prune keeps at most retain rotated files" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var w: FileWriter = undefined;
    try w.open(dir, .{ .max_bytes = 8, .retain = 3, .clock = fixedClock });
    defer w.close();

    for (0..12) |_| try w.write("12345678\n");

    const rotated = try countFiles(dir, "hoardarr-");
    try t.expect(rotated <= 3);
    try t.expect(rotated > 0);
}

test "concurrent appends never interleave a record" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpPath(&tmp);
    defer t.allocator.free(dir);

    var w: FileWriter = undefined;
    // Large cap: this test is about interleaving, not rotation.
    try w.open(dir, .{ .max_bytes = 1 << 30, .clock = fixedClock });
    defer w.close();

    var logger: Logger = .{ .clock = fixedClock };
    try logger.addFile(&w, .json);
    logger.setLevel(.info);

    const threads = 8;
    const per_thread = 200;
    const Worker = struct {
        fn run(lg: *Logger, id: usize) void {
            for (0..per_thread) |i| {
                lg.info("concurrent", &.{
                    uint("thread", id),
                    uint("seq", i),
                    // A value with characters that would be visible if
                    // two writes ever interleaved mid-record.
                    str("payload", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                });
            }
        }
    };

    var pool: [threads]std.Thread = undefined;
    for (&pool, 0..) |*th, id| th.* = try std.Thread.spawn(.{}, Worker.run, .{ &logger, id });
    for (&pool) |th| th.join();

    const body = try readFileAlloc(dir, "hoardarr.log");
    defer t.allocator.free(body);

    // Every line must be a complete, parseable record. A torn write
    // would show up as a line that does not parse.
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, body, "\n"), '\n');
    while (lines.next()) |line| {
        count += 1;
        try t.expect(try std.json.validate(t.allocator, line));
    }
    try t.expectEqual(@as(usize, threads * per_thread), count);
}
