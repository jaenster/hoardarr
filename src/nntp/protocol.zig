//! NNTP (RFC 3977) wire protocol — the pure half of the client.
//!
//! Everything here is a function of bytes: no sockets, no TLS, no
//! allocator in the hot path, no clock. The socket layer owns the
//! reactor, the read buffer and the connection state; it calls into
//! this module to turn bytes into decisions and decisions into bytes.
//!
//! Three pieces:
//!
//!   * **Status lines.** `parseStatusLine` splits `"430 No such
//!     article\r\n"` into a code and the borrowed text, plus the
//!     `hasMultilineBlock` table that says whether a block follows.
//!   * **Commands.** `body`, `authinfoUser`, … format into a
//!     caller-supplied buffer and reject anything that could split the
//!     command line (CR / LF / NUL) before it reaches the wire.
//!   * **The body reader.** `BodyReader` is the dot-unstuffing state
//!     machine, fed arbitrary chunks and emitting the payload with
//!     CRLF normalised to LF. Every byte of every article passes
//!     through it, so the plain-data stretches are found with a
//!     vector scan rather than a byte-at-a-time walk.
//!
//! Wire-format reminders for the dot-stuffing rules (RFC 3977 §3.1.1):
//!
//!   * Every line ends CRLF. Bare LF is tolerated — real servers and
//!     recorded cassettes both produce it.
//!   * A line consisting solely of "." ends the block.
//!   * Any other line whose first byte is "." had an extra "."
//!     prepended by the sender; exactly one leading dot is stripped.
//!
//! CRLF terminators are rewritten to bare LF. That is not laziness:
//! it matches Go's `textproto.DotReader` contract, which the rest of
//! the system (yEnc decode, cassette record/replay, the SAB handler)
//! was written against.

const std = @import("std");
const builtin = @import("builtin");

// ---------------------------------------------------------------------
// Vector configuration
// ---------------------------------------------------------------------

/// Vector width for the line scan. 16 on NEON/SSE, 32 with AVX2, 64
/// with AVX-512. The 8-lane fallback still works; LLVM scalarises it
/// into a SWAR-shaped sequence.
const vec_len: usize = std.simd.suggestVectorLength(u8) orelse 8;
const V = @Vector(vec_len, u8);
const Mask = std.meta.Int(.unsigned, vec_len);

const splat_cr: V = @splat('\r');
const splat_lf: V = @splat('\n');

// ---------------------------------------------------------------------
// Status codes
// ---------------------------------------------------------------------

/// The response codes this client reasons about. Named so call sites
/// read as protocol rather than as magic numbers.
pub const codes = struct {
    pub const capabilities_follow: u16 = 101;
    pub const date_follows: u16 = 111;
    /// Greeting: service available, posting allowed.
    pub const service_available: u16 = 200;
    /// Greeting: service available, posting prohibited. Equally fine
    /// for us — we never post.
    pub const service_available_no_posting: u16 = 201;
    pub const closing: u16 = 205;
    pub const group_selected: u16 = 211;
    pub const article_follows: u16 = 220;
    pub const head_follows: u16 = 221;
    pub const body_follows: u16 = 222;
    pub const article_exists: u16 = 223;
    pub const auth_accepted: u16 = 281;
    pub const auth_password_required: u16 = 381;
    pub const service_discontinued: u16 = 400;
    pub const no_such_group: u16 = 411;
    pub const no_such_article: u16 = 430;
    pub const auth_required: u16 = 480;
    pub const auth_rejected: u16 = 481;
    pub const auth_out_of_sequence: u16 = 482;
    pub const unknown_command: u16 = 500;
    pub const syntax_error: u16 = 501;
    pub const command_unavailable: u16 = 502;
    pub const feature_not_supported: u16 = 503;
};

/// The five RFC 3977 response classes, by first digit.
pub const Kind = enum {
    /// 1xx — informational.
    informational,
    /// 2xx — command completed.
    success,
    /// 3xx — command OK so far, send the rest of it (AUTHINFO PASS).
    intermediate,
    /// 4xx — transient negative: the command may work later, or on a
    /// different server.
    transient,
    /// 5xx — permanent negative: retrying the same command is pointless.
    permanent,
    /// Not a three-digit NNTP class at all.
    unknown,
};

pub fn kindOf(status_code: u16) Kind {
    return switch (status_code / 100) {
        1 => .informational,
        2 => .success,
        3 => .intermediate,
        4 => .transient,
        5 => .permanent,
        else => .unknown,
    };
}

/// 4xx — "transient negative". Mirrors `ProtocolError.IsTransient`.
pub fn isTransient(status_code: u16) bool {
    return kindOf(status_code) == .transient;
}

/// 5xx — "permanent negative". Mirrors `ProtocolError.IsPermanent`.
pub fn isPermanent(status_code: u16) bool {
    return kindOf(status_code) == .permanent;
}

/// A greeting we are willing to proceed on.
pub fn isGreetingOk(status_code: u16) bool {
    return status_code == codes.service_available or
        status_code == codes.service_available_no_posting;
}

/// MODE READER is advisory. Some transit-mode servers require it;
/// plenty of providers do not implement it at all and answer 500, 501
/// or 502. In every one of those cases the right move is to carry on,
/// because BODY / STAT work regardless — treating a 5xx here as fatal
/// would refuse to download from servers that work fine.
pub fn modeReaderAcceptable(status_code: u16) bool {
    return switch (status_code) {
        codes.service_available,
        codes.service_available_no_posting,
        codes.unknown_command,
        codes.syntax_error,
        codes.command_unavailable,
        => true,
        else => false,
    };
}

/// The octets that end a multi-line block.
pub const terminator = ".\r\n";

/// Response codes whose status line is followed by a multi-line block.
/// Indexed by `code - 100`; ~500 bytes of .rodata, built at comptime.
///
/// 211 is deliberately absent. GROUP answers 211 with a *single* line
/// ("211 count low high name") while LISTGROUP answers 211 with a
/// block, so the code alone cannot tell them apart — the caller has to
/// know which command it sent. We never send LISTGROUP, so treating
/// 211 as single-line is the correct default here.
const multiline_table = blk: {
    var table = [_]bool{false} ** 500;
    for ([_]u16{
        100, // HELP
        101, // CAPABILITIES
        215, // LIST / NEWGROUPS
        220, // ARTICLE
        221, // HEAD
        222, // BODY
        224, // OVER
        225, // HDR
        230, // NEWNEWS
        231, // NEWGROUPS
    }) |c| table[c - 100] = true;
    break :blk table;
};

/// True when a `terminator`-delimited block follows this status line.
pub fn hasMultilineBlock(status_code: u16) bool {
    if (status_code < 100 or status_code >= 600) return false;
    return multiline_table[status_code - 100];
}

// ---------------------------------------------------------------------
// Status line parsing
// ---------------------------------------------------------------------

/// A parsed response line. `text` borrows the input.
pub const Status = struct {
    code: u16,
    /// Everything after the code and its separator, with the trailing
    /// CR/LF removed. Empty when the server sent a bare code.
    text: []const u8,
    /// The code was followed by '-' instead of SP. NNTP has no
    /// SMTP-style continuation, but Go's textproto accepted it and
    /// some odd middleboxes emit it, so it is recorded rather than
    /// rejected.
    continued: bool,

    pub fn kind(self: Status) Kind {
        return kindOf(self.code);
    }

    pub fn hasBlock(self: Status) bool {
        return hasMultilineBlock(self.code);
    }
};

pub const StatusError = error{
    /// Fewer than three octets, or a separator that is neither SP
    /// nor '-'.
    ShortResponse,
    /// The first three octets are not digits, or they encode a value
    /// below 100.
    InvalidCode,
};

/// Parse one response line. `raw` may or may not still carry its
/// CR/LF; both are accepted.
///
/// Divergence from Go's `textproto.parseCodeLine`, which requires at
/// least four octets: a bare three-digit line ("205") parses here with
/// empty text. RFC 3977 §3.1 makes the text optional, and rejecting a
/// technically legal response would drop the connection for no reason.
pub fn parseStatusLine(raw: []const u8) StatusError!Status {
    const line = stripEol(raw);
    if (line.len < 3) return error.ShortResponse;

    var value: u16 = 0;
    for (line[0..3]) |c| {
        if (c < '0' or c > '9') return error.InvalidCode;
        value = value * 10 + (c - '0');
    }
    // A leading zero would make this not a response code at all.
    if (value < 100) return error.InvalidCode;

    if (line.len == 3) return .{ .code = value, .text = "", .continued = false };
    const sep = line[3];
    if (sep != ' ' and sep != '-') return error.ShortResponse;
    return .{ .code = value, .text = line[4..], .continued = sep == '-' };
}

/// Strip one trailing line terminator: CRLF, bare LF, or bare CR.
fn stripEol(line: []const u8) []const u8 {
    var s = line;
    if (s.len > 0 and s[s.len - 1] == '\n') s = s[0 .. s.len - 1];
    if (s.len > 0 and s[s.len - 1] == '\r') s = s[0 .. s.len - 1];
    return s;
}

// ---------------------------------------------------------------------
// DATE (RFC 3977 §7.1)
// ---------------------------------------------------------------------

/// The server clock as reported by DATE. Kept as fields rather than an
/// epoch so a caller can log it verbatim; `toUnix` converts.
pub const Timestamp = struct {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,

    /// Seconds since 1970-01-01T00:00:00Z. DATE is defined as UTC.
    ///
    /// Uses Howard Hinnant's days-from-civil: shift the year so March
    /// is month 1, which makes the leap day the last day of the year
    /// and turns the month-length table into one exact expression.
    pub fn toUnix(self: Timestamp) i64 {
        const y: i64 = @as(i64, self.year) - @intFromBool(self.month <= 2);
        const m: i64 = self.month;
        const d: i64 = self.day;
        const era = @divFloor(y, 400);
        const yoe = y - era * 400; // [0, 399]
        const mp = @mod(m + 9, 12); // March = 0
        const doy = @divTrunc(153 * mp + 2, 5) + d - 1; // [0, 365]
        const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
        const days = era * 146097 + doe - 719468;
        return days * 86400 +
            @as(i64, self.hour) * 3600 +
            @as(i64, self.minute) * 60 +
            @as(i64, self.second);
    }
};

pub const DateError = error{BadDate};

/// Parse the text of a `111` response: exactly `yyyymmddhhmmss`.
///
/// Surrounding whitespace is trimmed (providers pad inconsistently);
/// anything else about the shape is rejected, including out-of-range
/// components, so a garbled clock cannot silently become a plausible
/// timestamp the pool would then trust.
pub fn parseDate(text: []const u8) DateError!Timestamp {
    const s = std.mem.trim(u8, text, " \t\r\n");
    if (s.len != 14) return error.BadDate;
    for (s) |c| if (c < '0' or c > '9') return error.BadDate;

    const ts: Timestamp = .{
        .year = digits(u16, s[0..4]),
        .month = digits(u8, s[4..6]),
        .day = digits(u8, s[6..8]),
        .hour = digits(u8, s[8..10]),
        .minute = digits(u8, s[10..12]),
        .second = digits(u8, s[12..14]),
    };
    if (ts.month < 1 or ts.month > 12) return error.BadDate;
    if (ts.day < 1 or ts.day > daysInMonth(ts.year, ts.month)) return error.BadDate;
    if (ts.hour > 23 or ts.minute > 59 or ts.second > 59) return error.BadDate;
    return ts;
}

fn digits(comptime T: type, s: []const u8) T {
    var v: T = 0;
    for (s) |c| v = v * 10 + (c - '0');
    return v;
}

fn daysInMonth(year: u16, month: u8) u8 {
    const lengths = [12]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (month == 2 and isLeap(year)) return 29;
    return lengths[month - 1];
}

fn isLeap(year: u16) bool {
    return (year % 4 == 0 and year % 100 != 0) or year % 400 == 0;
}

// ---------------------------------------------------------------------
// GROUP (RFC 3977 §6.1.1)
// ---------------------------------------------------------------------

/// The four fields of a `211` GROUP response.
pub const GroupInfo = struct {
    /// Server's *estimate* of the article count — explicitly allowed to
    /// be wrong by the RFC, and often is. Never use it as a range.
    estimate: u64,
    low: u64,
    high: u64,
    /// Borrows the input text.
    name: []const u8,
};

pub const GroupError = error{BadGroupResponse};

/// Parse the text of a `211` response: "count low high name".
pub fn parseGroupResponse(text: []const u8) GroupError!GroupInfo {
    var it = std.mem.tokenizeScalar(u8, std.mem.trim(u8, text, " \t\r\n"), ' ');
    const estimate = try parseU64(it.next() orelse return error.BadGroupResponse);
    const low = try parseU64(it.next() orelse return error.BadGroupResponse);
    const high = try parseU64(it.next() orelse return error.BadGroupResponse);
    const name = it.next() orelse return error.BadGroupResponse;
    return .{ .estimate = estimate, .low = low, .high = high, .name = name };
}

fn parseU64(s: []const u8) GroupError!u64 {
    if (s.len == 0 or s.len > 20) return error.BadGroupResponse;
    var v: u64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.BadGroupResponse;
        v = std.math.mul(u64, v, 10) catch return error.BadGroupResponse;
        v = std.math.add(u64, v, c - '0') catch return error.BadGroupResponse;
    }
    return v;
}

// ---------------------------------------------------------------------
// Command formatting
// ---------------------------------------------------------------------

/// RFC 3977 §3.1 caps a command line at 512 octets including CRLF.
/// Size command buffers at this and `NoSpaceLeft` becomes unreachable
/// for every command except an absurdly long AUTHINFO argument.
pub const max_command_len: usize = 512;

pub const CommandError = error{
    /// An argument contained CR, LF or NUL. See `findControlCharacter`.
    ControlCharacter,
    /// The formatted command does not fit in the supplied buffer.
    NoSpaceLeft,
};

pub const MessageIdError = CommandError || error{
    /// Empty, or containing a byte that cannot appear in an RFC 5536
    /// msg-id: SP, TAB, '<' or '>'.
    InvalidMessageId,
};

/// Offset of the first byte that would split the command line, or null
/// when the argument is safe.
///
/// An attacker-controlled substring — a hostile NZB message-id, a
/// password pasted from a compromised source — could otherwise
/// terminate the current command and inject a second one on the same
/// connection. This is defence in depth; callers validate at parse
/// time too.
pub fn findControlCharacter(arg: []const u8) ?usize {
    for (arg, 0..) |c, i| switch (c) {
        '\r', '\n', 0 => return i,
        else => {},
    };
    return null;
}

/// Reject any byte that would split a command in the wire protocol.
pub fn validateCommandLine(line: []const u8) CommandError!void {
    if (findControlCharacter(line) != null) return error.ControlCharacter;
}

/// Concatenate `parts` into `buf` and append CRLF. Returns the slice of
/// `buf` to hand to the socket layer.
fn writeCommand(buf: []u8, parts: []const []const u8) CommandError![]const u8 {
    var n: usize = 0;
    for (parts) |p| {
        try validateCommandLine(p);
        if (n + p.len > buf.len) return error.NoSpaceLeft;
        @memcpy(buf[n..][0..p.len], p);
        n += p.len;
    }
    if (n + 2 > buf.len) return error.NoSpaceLeft;
    buf[n] = '\r';
    buf[n + 1] = '\n';
    return buf[0 .. n + 2];
}

/// Normalise a message-id to its bare form: one surrounding pair of
/// angle brackets is removed if present, so callers may pass either
/// `abc@host` or `<abc@host>`. The command formatters add the brackets
/// back exactly once.
fn bareMessageId(message_id: []const u8) MessageIdError![]const u8 {
    var id = message_id;
    if (id.len >= 2 and id[0] == '<' and id[id.len - 1] == '>') id = id[1 .. id.len - 1];
    if (id.len == 0) return error.InvalidMessageId;
    for (id) |c| switch (c) {
        '<', '>', ' ', '\t' => return error.InvalidMessageId,
        else => {},
    };
    try validateCommandLine(id);
    return id;
}

pub fn quit(buf: []u8) CommandError![]const u8 {
    return writeCommand(buf, &.{"QUIT"});
}

pub fn date(buf: []u8) CommandError![]const u8 {
    return writeCommand(buf, &.{"DATE"});
}

pub fn capabilities(buf: []u8) CommandError![]const u8 {
    return writeCommand(buf, &.{"CAPABILITIES"});
}

pub fn modeReader(buf: []u8) CommandError![]const u8 {
    return writeCommand(buf, &.{"MODE READER"});
}

pub fn authinfoUser(buf: []u8, username: []const u8) CommandError![]const u8 {
    return writeCommand(buf, &.{ "AUTHINFO USER ", username });
}

pub fn authinfoPass(buf: []u8, password: []const u8) CommandError![]const u8 {
    return writeCommand(buf, &.{ "AUTHINFO PASS ", password });
}

pub fn group(buf: []u8, name: []const u8) CommandError![]const u8 {
    return writeCommand(buf, &.{ "GROUP ", name });
}

pub fn body(buf: []u8, message_id: []const u8) MessageIdError![]const u8 {
    return writeCommand(buf, &.{ "BODY <", try bareMessageId(message_id), ">" });
}

pub fn article(buf: []u8, message_id: []const u8) MessageIdError![]const u8 {
    return writeCommand(buf, &.{ "ARTICLE <", try bareMessageId(message_id), ">" });
}

pub fn head(buf: []u8, message_id: []const u8) MessageIdError![]const u8 {
    return writeCommand(buf, &.{ "HEAD <", try bareMessageId(message_id), ">" });
}

pub fn stat(buf: []u8, message_id: []const u8) MessageIdError![]const u8 {
    return writeCommand(buf, &.{ "STAT <", try bareMessageId(message_id), ">" });
}

// ---------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------

/// What a negative response means for retry policy. Getting a code
/// into the wrong bucket is a real bug: `too_many_connections`
/// misfiled as `auth_failed` disables a working server, and
/// `article_missing` misfiled as `transient` burns the segment's whole
/// retry budget re-asking a server that will never have the article.
///
/// | Class | Meaning | What the caller should do |
/// |-|-|-|
/// | `article_missing` | 430 — not on *this* server | Try the next server tier; do not retry here |
/// | `too_many_connections` | Account over its slot limit | Back off, shrink the pool, retry the same server |
/// | `auth_required` | 480 — command needs AUTHINFO first | Re-dial and authenticate, then retry |
/// | `auth_failed` | Credentials rejected | Fatal for this server's config; stop dialling it |
/// | `transient` | Other 4xx | Retry later or elsewhere |
/// | `permanent` | Other 5xx | Do not retry the same command |
/// | `unexpected` | 1xx/2xx/3xx reached the classifier | Protocol desync; drop the connection |
pub const Class = enum {
    article_missing,
    too_many_connections,
    auth_required,
    auth_failed,
    transient,
    permanent,
    unexpected,
};

/// Phrases providers use for "you are over your connection limit".
/// Lower-case; matched case-insensitively as substrings.
const conn_limit_phrases = [_][]const u8{
    "too many connection",
    "max connections",
    "connection limit",
};

/// True when the response text says the account is out of slots.
pub fn mentionsConnectionLimit(message: []const u8) bool {
    for (conn_limit_phrases) |p| {
        if (containsIgnoreCase(message, p)) return true;
    }
    return false;
}

/// Classify a negative response.
///
/// Connection-limit detection runs *before* the code switch because
/// providers signal over-capacity on a grab-bag of codes: Eweka sends
/// "502 too many connections" mid-session, others use 400, and some
/// answer 481 on AUTHINFO PASS when the slot is already taken. Only
/// the message text is common to all of them, and routing on the code
/// alone would send a temporary capacity problem down the auth-failed
/// path and permanently disable a healthy server.
pub fn classifyResponse(status_code: u16, message: []const u8) Class {
    if (mentionsConnectionLimit(message)) return .too_many_connections;
    return switch (status_code) {
        codes.no_such_article => .article_missing,
        codes.auth_required => .auth_required,
        codes.auth_rejected, codes.auth_out_of_sequence, codes.command_unavailable => .auth_failed,
        else => switch (kindOf(status_code)) {
            .transient => .transient,
            .permanent => .permanent,
            else => .unexpected,
        },
    };
}

/// Outcome of the greeting read on a fresh connection.
pub const GreetingClass = enum {
    /// 200 or 201 — proceed.
    ok,
    /// Refused because the account is over its slot limit. The pool
    /// should back off briefly rather than treating the server as dead.
    too_many_connections,
    /// Any other greeting. The connection is unusable.
    unexpected,
};

pub fn classifyGreeting(status_code: u16, message: []const u8) GreetingClass {
    if (isGreetingOk(status_code)) return .ok;
    if (mentionsConnectionLimit(message)) return .too_many_connections;
    return .unexpected;
}

/// ASCII case-insensitive substring search. `needle_lower` must already
/// be lower-case, which lets us fold only the haystack and avoids the
/// allocation Go's `strings.ToLower(msg)` does per response.
fn containsIgnoreCase(haystack: []const u8, needle_lower: []const u8) bool {
    if (needle_lower.len == 0) return true;
    if (haystack.len < needle_lower.len) return false;
    const last = haystack.len - needle_lower.len;
    var i: usize = 0;
    while (i <= last) : (i += 1) {
        var j: usize = 0;
        while (j < needle_lower.len and
            std.ascii.toLower(haystack[i + j]) == needle_lower[j]) : (j += 1)
        {}
        if (j == needle_lower.len) return true;
    }
    return false;
}

// ---------------------------------------------------------------------
// Multi-line body reader
// ---------------------------------------------------------------------

/// Result of one `BodyReader.push`.
pub const Step = struct {
    /// Bytes of `src` consumed. The caller advances its read buffer by
    /// this much; on `terminated` it points exactly past the ".\r\n",
    /// so the next status line starts at `src[consumed..]`.
    consumed: usize,
    /// Bytes written to `dst`.
    written: usize,
    /// The end-of-block terminator has been consumed.
    terminated: bool,
};

/// Dot-unstuffing state machine for a multi-line block.
///
/// Feed it whatever the socket handed you — the chunk boundaries are
/// invisible to the output, including a boundary that lands in the
/// middle of the `\r\n.\r\n` terminator. Emits the payload with CRLF
/// normalised to LF and leading stuffing dots removed.
///
/// Why the flags rather than an enum state: the ambiguous cases are
/// independent, and expressing them as three booleans keeps the fast
/// path a single `if` chain that the branch predictor gets right on
/// every byte of a long line.
///
/// The one non-obvious trick is the stuffing dot. A leading "." is
/// removed from *every* line, terminator or not, so it can be dropped
/// the moment it is seen — no need to hold it back and wait for the
/// rest of the line. What follows then decides: an immediate line end
/// means the line was just ".", i.e. the terminator; anything else
/// means it was stuffing and the line continues.
pub const BodyReader = struct {
    /// A CR has been consumed but not resolved: the next byte decides
    /// whether it closed a CRLF line end (emit LF) or was literal data
    /// (emit CR). Held across `push` calls, which is what makes a chunk
    /// boundary between the CR and the LF harmless.
    pending_cr: bool = false,
    /// The next byte begins a logical line, so a "." there is stuffing.
    at_line_start: bool = true,
    /// This line so far is exactly one stripped leading ".", so a line
    /// end arriving now is the end-of-block terminator.
    dot_only: bool = false,
    /// The terminator has been consumed. `push` is a no-op afterwards.
    terminated: bool = false,

    /// Consume from `src`, writing unstuffed payload into `dst`.
    ///
    /// Returns when `src` is exhausted, `dst` is full, or the
    /// terminator is reached. `dst` must be at least one byte; the
    /// caller loops, draining `dst` between calls, until `src` is
    /// consumed or `terminated` is set.
    pub fn push(self: *BodyReader, src: []const u8, dst: []u8) Step {
        return self.run(src, dst, true);
    }

    /// Byte-at-a-time reference implementation. Identical output to
    /// `push` for every input and every chunking — the tests assert it
    /// — and kept as the thing the vectorised path is checked against.
    pub fn pushByteAtATime(self: *BodyReader, src: []const u8, dst: []u8) Step {
        return self.run(src, dst, false);
    }

    /// Signal end of input. A CR that was still waiting for its LF
    /// turns out to have been literal data, so it is flushed here;
    /// `dst` needs one byte. Returns the bytes written (0 or 1).
    ///
    /// Check `terminated` afterwards: false means the peer stopped
    /// before the ".\r\n", i.e. a truncated article. That is the
    /// socket layer's call to make — a truncated body is still worth
    /// handing to the yEnc decoder, which will report a missing =yend.
    pub fn finish(self: *BodyReader, dst: []u8) usize {
        if (self.pending_cr and dst.len > 0) {
            self.pending_cr = false;
            dst[0] = '\r';
            return 1;
        }
        return 0;
    }

    fn run(self: *BodyReader, src: []const u8, dst: []u8, comptime bulk: bool) Step {
        if (self.terminated) return .{ .consumed = 0, .written = 0, .terminated = true };

        var i: usize = 0; // consumed from src
        var o: usize = 0; // written to dst

        while (i < src.len) {
            // ---- Resolve a CR held from an earlier byte or chunk. ----
            if (self.pending_cr) {
                if (src[i] == '\n') {
                    if (self.dot_only) {
                        // ".\r\n" — end of block. Writes nothing, so it
                        // completes even with dst full.
                        i += 1;
                        self.pending_cr = false;
                        self.dot_only = false;
                        self.at_line_start = true;
                        self.terminated = true;
                        return .{ .consumed = i, .written = o, .terminated = true };
                    }
                    if (o == dst.len) break;
                    i += 1;
                    self.pending_cr = false;
                    dst[o] = '\n';
                    o += 1;
                    self.at_line_start = true;
                    continue;
                }
                // Lone CR: literal data. src[i] is *not* consumed here;
                // it is reprocessed as an ordinary byte next iteration.
                if (o == dst.len) break;
                self.pending_cr = false;
                dst[o] = '\r';
                o += 1;
                self.dot_only = false;
                continue;
            }

            // ---- Strip one stuffing dot at the start of a line. ----
            if (self.at_line_start) {
                self.at_line_start = false;
                self.dot_only = false;
                if (src[i] == '.') {
                    i += 1;
                    self.dot_only = true;
                    continue;
                }
            }

            const c = src[i];
            if (c == '\r') {
                i += 1;
                self.pending_cr = true;
                continue;
            }
            if (c == '\n') {
                if (self.dot_only) {
                    // Lenient ".\n" terminator — servers and cassettes
                    // both drop the CR occasionally.
                    i += 1;
                    self.dot_only = false;
                    self.at_line_start = true;
                    self.terminated = true;
                    return .{ .consumed = i, .written = o, .terminated = true };
                }
                if (o == dst.len) break;
                i += 1;
                dst[o] = '\n';
                o += 1;
                self.at_line_start = true;
                continue;
            }

            // ---- Plain data. ----
            if (o == dst.len) break;
            if (bulk) {
                // src[i] is neither CR nor LF, so the scan returns at
                // least 1 and the loop always makes progress. This is
                // where the throughput lives: one vector scan per line
                // followed by a memcpy over a stretch that provably
                // contains no line break, instead of a per-byte state
                // machine.
                const rest = src[i..];
                const run_len = findLineEnd(rest) orelse rest.len;
                const n = @min(run_len, dst.len - o);
                @memcpy(dst[o..][0..n], rest[0..n]);
                i += n;
                o += n;
            } else {
                dst[o] = c;
                o += 1;
                i += 1;
            }
            self.dot_only = false;
        }

        return .{ .consumed = i, .written = o, .terminated = self.terminated };
    }
};

/// Offset of the first CR or LF in `src`, or null when there is none.
///
/// Vectorised: compare a whole register against CR and against LF,
/// pack both results to bitmasks, OR them, and `@ctz` the result.
/// Everything else about the body reader is per-line work, so this
/// scan is the only place the per-byte cost of an article shows up.
pub fn findLineEnd(src: []const u8) ?usize {
    var i: usize = 0;
    while (i + vec_len <= src.len) : (i += vec_len) {
        const v: V = src[i..][0..vec_len].*;
        if (firstCrLf(v)) |k| return i + k;
    }
    return findLineEndScalar(src, i);
}

/// Scalar reference for `findLineEnd`, also used for the sub-vector
/// tail. `from` lets the vector loop hand over mid-slice.
pub fn findLineEndScalar(src: []const u8, from: usize) ?usize {
    var i = from;
    while (i < src.len) : (i += 1) {
        if (src[i] == '\r' or src[i] == '\n') return i;
    }
    return null;
}

/// Index of the first CR or LF lane, or null when the register is
/// clean.
///
/// `@bitCast` of a bool vector packs lane i into bit i on
/// little-endian targets, which makes the search a single `@ctz` over
/// the OR of the two comparison masks. Big-endian packs the other way
/// round, so it goes through `std.simd.firstTrue` instead — same
/// answer, and the only target we ship on is little-endian anyway.
inline fn firstCrLf(v: V) ?usize {
    if (builtin.cpu.arch.endian() == .little) {
        const cr: Mask = @bitCast(v == splat_cr);
        const lf: Mask = @bitCast(v == splat_lf);
        const bits = cr | lf;
        if (bits == 0) return null;
        return @ctz(bits);
    }
    const all_true: @Vector(vec_len, bool) = @splat(true);
    const hit = @select(bool, v == splat_cr, all_true, v == splat_lf);
    return if (std.simd.firstTrue(hit)) |k| @as(usize, k) else null;
}

// =====================================================================
// Tests
// =====================================================================

const t = std.testing;
const Allocator = std.mem.Allocator;

// ---- Status lines ----------------------------------------------------

test "parse status line" {
    {
        const s = try parseStatusLine("200 stub server ready\r\n");
        try t.expectEqual(@as(u16, 200), s.code);
        try t.expectEqualStrings("stub server ready", s.text);
        try t.expect(!s.continued);
        try t.expectEqual(Kind.success, s.kind());
    }
    {
        // Bare LF, as recorded cassettes sometimes hold it.
        const s = try parseStatusLine("430 No such article\n");
        try t.expectEqual(@as(u16, 430), s.code);
        try t.expectEqualStrings("No such article", s.text);
    }
    {
        // No terminator at all — the socket layer may have stripped it.
        const s = try parseStatusLine("222 0 <msg1@host>");
        try t.expectEqual(@as(u16, 222), s.code);
        try t.expectEqualStrings("0 <msg1@host>", s.text);
        try t.expect(s.hasBlock());
    }
    {
        // Bare code, no text. Go's textproto rejects this; we do not.
        const s = try parseStatusLine("205\r\n");
        try t.expectEqual(@as(u16, 205), s.code);
        try t.expectEqualStrings("", s.text);
    }
    {
        const s = try parseStatusLine("100-help follows");
        try t.expectEqual(@as(u16, 100), s.code);
        try t.expect(s.continued);
        try t.expectEqualStrings("help follows", s.text);
    }
    {
        // Empty text after the separator.
        const s = try parseStatusLine("281 ");
        try t.expectEqual(@as(u16, 281), s.code);
        try t.expectEqualStrings("", s.text);
    }
}

test "parse status line rejects malformed input" {
    try t.expectError(error.ShortResponse, parseStatusLine(""));
    try t.expectError(error.ShortResponse, parseStatusLine("20"));
    try t.expectError(error.ShortResponse, parseStatusLine("\r\n"));
    // Separator must be SP or '-'.
    try t.expectError(error.ShortResponse, parseStatusLine("200X ready"));
    try t.expectError(error.InvalidCode, parseStatusLine("2x0 ready"));
    try t.expectError(error.InvalidCode, parseStatusLine("abc def"));
    // Below 100 is not a response code.
    try t.expectError(error.InvalidCode, parseStatusLine("099 nope"));
}

test "status kind buckets" {
    try t.expectEqual(Kind.informational, kindOf(111));
    try t.expectEqual(Kind.success, kindOf(222));
    try t.expectEqual(Kind.intermediate, kindOf(381));
    try t.expectEqual(Kind.transient, kindOf(430));
    try t.expectEqual(Kind.permanent, kindOf(502));
    try t.expectEqual(Kind.unknown, kindOf(999));

    try t.expect(isTransient(400));
    try t.expect(!isTransient(500));
    try t.expect(isPermanent(503));
    try t.expect(!isPermanent(430));
}

test "multi-line block codes" {
    try t.expect(hasMultilineBlock(222)); // BODY
    try t.expect(hasMultilineBlock(220)); // ARTICLE
    try t.expect(hasMultilineBlock(221)); // HEAD
    try t.expect(hasMultilineBlock(101)); // CAPABILITIES
    try t.expect(!hasMultilineBlock(223)); // STAT
    try t.expect(!hasMultilineBlock(111)); // DATE
    try t.expect(!hasMultilineBlock(430));
    // GROUP's 211 is single-line; only LISTGROUP's is a block, and we
    // never send LISTGROUP.
    try t.expect(!hasMultilineBlock(211));
    try t.expect(!hasMultilineBlock(0));
    try t.expect(!hasMultilineBlock(999));
}

test "greeting acceptance" {
    try t.expect(isGreetingOk(200));
    try t.expect(isGreetingOk(201));
    try t.expect(!isGreetingOk(502));
    try t.expect(!isGreetingOk(400));
}

// Ports TestModeReader_TolerantOf500: a provider that does not
// implement MODE READER must not fail the connection.
test "mode reader tolerates 500 501 502" {
    for ([_]u16{ 200, 201, 500, 501, 502 }) |c| {
        try t.expect(modeReaderAcceptable(c));
    }
    for ([_]u16{ 400, 430, 480, 503 }) |c| {
        try t.expect(!modeReaderAcceptable(c));
    }
}

// ---- Commands --------------------------------------------------------

test "format commands" {
    var buf: [max_command_len]u8 = undefined;
    try t.expectEqualStrings("QUIT\r\n", try quit(&buf));
    try t.expectEqualStrings("DATE\r\n", try date(&buf));
    try t.expectEqualStrings("CAPABILITIES\r\n", try capabilities(&buf));
    try t.expectEqualStrings("MODE READER\r\n", try modeReader(&buf));
    try t.expectEqualStrings("AUTHINFO USER user\r\n", try authinfoUser(&buf, "user"));
    try t.expectEqualStrings("AUTHINFO PASS pass\r\n", try authinfoPass(&buf, "pass"));
    try t.expectEqualStrings("GROUP alt.binaries.test\r\n", try group(&buf, "alt.binaries.test"));
    try t.expectEqualStrings("BODY <msg1@host>\r\n", try body(&buf, "msg1@host"));
    try t.expectEqualStrings("ARTICLE <msg1@host>\r\n", try article(&buf, "msg1@host"));
    try t.expectEqualStrings("HEAD <msg1@host>\r\n", try head(&buf, "msg1@host"));
    try t.expectEqualStrings("STAT <msg1@host>\r\n", try stat(&buf, "msg1@host"));
}

test "message-id angle brackets are normalised, not doubled" {
    var buf: [max_command_len]u8 = undefined;
    try t.expectEqualStrings("BODY <a@b>\r\n", try body(&buf, "a@b"));
    try t.expectEqualStrings("BODY <a@b>\r\n", try body(&buf, "<a@b>"));

    try t.expectError(error.InvalidMessageId, body(&buf, ""));
    try t.expectError(error.InvalidMessageId, body(&buf, "<>"));
    try t.expectError(error.InvalidMessageId, body(&buf, "a@b> BODY <c@d"));
    try t.expectError(error.InvalidMessageId, body(&buf, "has space@host"));
    try t.expectError(error.InvalidMessageId, body(&buf, "has\ttab@host"));
}

// Ports TestSend_RejectsControlCharacters: defence in depth against an
// injected second command riding along on an attacker-controlled
// argument.
test "command formatting rejects control characters" {
    var buf: [max_command_len]u8 = undefined;

    try t.expectError(error.ControlCharacter, authinfoUser(&buf, "good\r\nQUIT"));
    try t.expectError(error.ControlCharacter, authinfoPass(&buf, "bad\npassword"));
    try t.expectError(error.ControlCharacter, group(&buf, "a.b\r\nQUIT"));
    // A message-id carrying CRLF fails as an invalid id (the '>' is
    // rejected first); either way it never reaches the wire.
    try t.expect(std.meta.isError(body(&buf, "evil\r\n")));
    try t.expectError(error.ControlCharacter, body(&buf, "null\x00byte@host"));

    try t.expectEqual(@as(?usize, null), findControlCharacter("BODY <safe@host>"));
    try t.expectEqual(@as(?usize, 4), findControlCharacter("safe\r\n"));
    try t.expectEqual(@as(?usize, 0), findControlCharacter("\x00"));
    try t.expectEqualStrings("BODY <safe@host>\r\n", try body(&buf, "safe@host"));
}

test "command buffer too small" {
    var tiny: [8]u8 = undefined;
    try t.expectError(error.NoSpaceLeft, authinfoUser(&tiny, "someone"));
    // "QUIT\r\n" is 6 bytes and fits; 5 bytes of room does not.
    var five: [5]u8 = undefined;
    try t.expectError(error.NoSpaceLeft, quit(&five));
    var six: [6]u8 = undefined;
    try t.expectEqualStrings("QUIT\r\n", try quit(&six));
}

// ---- Error classification -------------------------------------------

// Ports TestClassifyResponse_TooManyConnections. Provider
// "too many connections" text on any 4xx/5xx code must land in
// too_many_connections, never auth_failed — misclassification used to
// burn the segment retry budget on real over-capacity (Eweka returns
// "502 too many connections" mid-session; others use 481/400).
test "classify response: connection limit beats the code" {
    const cases = [_]struct { code: u16, message: []const u8 }{
        .{ .code = 502, .message = "Too many connections" },
        .{ .code = 481, .message = "too many connections from your IP" },
        .{ .code = 400, .message = "connection limit reached" },
        .{ .code = 503, .message = "max connections exceeded" },
        // Case folding must work in both directions.
        .{ .code = 482, .message = "MAX CONNECTIONS EXCEEDED" },
        .{ .code = 502, .message = "Connection Limit Reached" },
    };
    for (cases) |c| {
        const got = classifyResponse(c.code, c.message);
        try t.expectEqual(Class.too_many_connections, got);
        try t.expect(got != .auth_failed);
    }
}

// Ports TestClassifyResponse_AuthFailedKept.
test "classify response: a real auth failure stays auth_failed" {
    try t.expectEqual(Class.auth_failed, classifyResponse(481, "authentication rejected"));
    try t.expectEqual(Class.auth_failed, classifyResponse(482, "authentication out of sequence"));
    try t.expectEqual(Class.auth_failed, classifyResponse(502, "permission denied"));
}

// Ports TestClassifyResponse_ArticleMissing.
test "classify response: 430 is article_missing" {
    try t.expectEqual(Class.article_missing, classifyResponse(430, "no such article"));
    // Even an oddly worded 430 stays in the bucket.
    try t.expectEqual(Class.article_missing, classifyResponse(430, ""));
}

// Ports the protocol half of TestBody_AuthRequired480.
test "classify response: 480 is auth_required, not auth_failed" {
    const got = classifyResponse(480, "authentication required");
    try t.expectEqual(Class.auth_required, got);
    try t.expect(got != .auth_failed);
}

test "classify response: unmatched codes fall back on their class" {
    try t.expectEqual(Class.transient, classifyResponse(400, "service temporarily unavailable"));
    try t.expectEqual(Class.transient, classifyResponse(411, "no such group"));
    try t.expectEqual(Class.permanent, classifyResponse(500, "unknown command"));
    try t.expectEqual(Class.permanent, classifyResponse(501, "syntax error"));
    try t.expectEqual(Class.permanent, classifyResponse(503, "feature not supported"));
    // A success code reaching the classifier means the caller and the
    // server disagree about the conversation.
    try t.expectEqual(Class.unexpected, classifyResponse(200, "ready"));
    try t.expectEqual(Class.unexpected, classifyResponse(381, "password required"));
}

test "classify greeting" {
    try t.expectEqual(GreetingClass.ok, classifyGreeting(200, "stub server ready"));
    try t.expectEqual(GreetingClass.ok, classifyGreeting(201, "no posting allowed"));
    try t.expectEqual(GreetingClass.too_many_connections, classifyGreeting(502, "too many connections"));
    try t.expectEqual(GreetingClass.too_many_connections, classifyGreeting(400, "max connections"));
    // Ports TestDial_RejectsBadGreeting.
    try t.expectEqual(GreetingClass.unexpected, classifyGreeting(502, "service unavailable"));
    try t.expectEqual(GreetingClass.unexpected, classifyGreeting(400, "service discontinued"));
}

test "connection-limit phrase matching" {
    try t.expect(mentionsConnectionLimit("too many connections"));
    try t.expect(mentionsConnectionLimit("482 Too Many Connection"));
    try t.expect(mentionsConnectionLimit("prefix max connections suffix"));
    try t.expect(!mentionsConnectionLimit(""));
    try t.expect(!mentionsConnectionLimit("authentication rejected"));
    // Truncated phrase must not match.
    try t.expect(!mentionsConnectionLimit("too many connectio"));
    try t.expect(!mentionsConnectionLimit("connection"));
}

// ---- DATE / GROUP ----------------------------------------------------

// Ports TestDate: "111 20260510120000".
test "parse DATE response" {
    const ts = try parseDate("20260510120000");
    try t.expectEqual(@as(u16, 2026), ts.year);
    try t.expectEqual(@as(u8, 5), ts.month);
    try t.expectEqual(@as(u8, 10), ts.day);
    try t.expectEqual(@as(u8, 12), ts.hour);
    try t.expectEqual(@as(u8, 0), ts.minute);
    try t.expectEqual(@as(u8, 0), ts.second);
    // Providers pad inconsistently.
    _ = try parseDate("  20260510120000  \r\n");

    // Epoch anchors, checked against known Unix timestamps.
    try t.expectEqual(@as(i64, 0), (try parseDate("19700101000000")).toUnix());
    try t.expectEqual(@as(i64, 1_000_000_000), (try parseDate("20010909014640")).toUnix());
    try t.expectEqual(@as(i64, 1_778_414_400), (try parseDate("20260510120000")).toUnix());
    // 2000 is a leap year via the 400 rule; 1900 was not.
    try t.expectEqual(@as(i64, 951_782_400), (try parseDate("20000229000000")).toUnix());
}

test "parse DATE rejects garbage" {
    try t.expectError(error.BadDate, parseDate(""));
    try t.expectError(error.BadDate, parseDate("2026051012000"));
    try t.expectError(error.BadDate, parseDate("202605101200000"));
    try t.expectError(error.BadDate, parseDate("2026x510120000"));
    try t.expectError(error.BadDate, parseDate("20261310120000")); // month 13
    try t.expectError(error.BadDate, parseDate("20260500120000")); // day 0
    try t.expectError(error.BadDate, parseDate("20260532120000")); // day 32
    try t.expectError(error.BadDate, parseDate("20260229120000")); // 2026 is not a leap year
    try t.expectError(error.BadDate, parseDate("20260510240000")); // hour 24
    try t.expectError(error.BadDate, parseDate("20260510126000")); // minute 60
    try t.expectError(error.BadDate, parseDate("20260510120060")); // second 60
    // 2024 *is* a leap year.
    _ = try parseDate("20240229120000");
}

test "parse GROUP response" {
    const g = try parseGroupResponse("1234 3000234 3002322 misc.test");
    try t.expectEqual(@as(u64, 1234), g.estimate);
    try t.expectEqual(@as(u64, 3_000_234), g.low);
    try t.expectEqual(@as(u64, 3_002_322), g.high);
    try t.expectEqualStrings("misc.test", g.name);

    // Empty group: RFC 3977 allows count 0 with low > high.
    const e = try parseGroupResponse("0 1 0 alt.empty\r\n");
    try t.expectEqual(@as(u64, 0), e.estimate);
    try t.expectEqual(@as(u64, 1), e.low);
    try t.expectEqual(@as(u64, 0), e.high);

    try t.expectError(error.BadGroupResponse, parseGroupResponse(""));
    try t.expectError(error.BadGroupResponse, parseGroupResponse("1234 3000234 3002322"));
    try t.expectError(error.BadGroupResponse, parseGroupResponse("x 1 2 g"));
    try t.expectError(error.BadGroupResponse, parseGroupResponse("99999999999999999999999 1 2 g"));
}

// ---- Line scan -------------------------------------------------------

test "findLineEnd: vector path matches scalar over random input" {
    var data: [2048]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0x1EED_0001);
    const rnd = prng.random();

    // Sparse CR/LF (long lines, the common case), then dense (worst
    // case for the vector loop), then none at all.
    for ([_]u8{ 0, 2, 16, 255 }) |density| {
        for (&data) |*b| {
            const v = rnd.int(u8);
            b.* = if (density != 0 and v < density) (if (v & 1 == 0) '\r' else '\n') else 'A' + (v % 26);
        }
        // Every length, so the vector/tail handover is covered at every
        // offset relative to the register width.
        for (0..data.len + 1) |n| {
            const slice = data[0..n];
            try t.expectEqual(findLineEndScalar(slice, 0), findLineEnd(slice));
        }
    }
}

test "findLineEnd: CR and LF are both found, whichever comes first" {
    try t.expectEqual(@as(?usize, 0), findLineEnd("\r"));
    try t.expectEqual(@as(?usize, 0), findLineEnd("\n"));
    try t.expectEqual(@as(?usize, 3), findLineEnd("abc\r\n"));
    try t.expectEqual(@as(?usize, 3), findLineEnd("abc\ndef\r"));
    try t.expectEqual(@as(?usize, null), findLineEnd(""));
    try t.expectEqual(@as(?usize, null), findLineEnd("no line ends here at all"));
}

// ---- Body reader helpers --------------------------------------------

/// Render `payload` as an NNTP wire body: CRLF-terminated lines, a line
/// starting with "." prefixed with another ".", and a final ".\r\n".
/// Mirrors `nntpBody` from body_reader_test.go.
fn nntpBody(gpa: Allocator, payload: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var pos: usize = 0;
    while (pos < payload.len) {
        const end = if (std.mem.indexOfScalarPos(u8, payload, pos, '\n')) |k| k + 1 else payload.len;
        const stripped = std.mem.trimEnd(u8, payload[pos..end], "\r\n");
        pos = end;
        if (stripped.len > 0 and stripped[0] == '.') try out.append(gpa, '.'); // stuff
        try out.appendSlice(gpa, stripped);
        try out.appendSlice(gpa, "\r\n");
    }
    try out.appendSlice(gpa, terminator);
    return out.toOwnedSlice(gpa);
}

/// Independent oracle: split the whole wire body into lines the way the
/// Go reader's `bufio.ReadSlice('\n')` does, then apply the three rules
/// (terminator / strip one leading dot / CRLF -> LF). Deliberately
/// naive and non-incremental, so it cannot share a bug with the state
/// machine it is checking.
fn unstuffOracle(gpa: Allocator, wire: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var pos: usize = 0;
    while (pos < wire.len) {
        const end = if (std.mem.indexOfScalarPos(u8, wire, pos, '\n')) |k| k + 1 else wire.len;
        var line = wire[pos..end];
        pos = end;
        if (std.mem.eql(u8, line, ".\r\n") or std.mem.eql(u8, line, ".\n")) break;
        if (line[0] == '.') line = line[1..];
        const m = line.len;
        if (m >= 2 and line[m - 2] == '\r' and line[m - 1] == '\n') {
            try out.appendSlice(gpa, line[0 .. m - 2]);
            try out.append(gpa, '\n');
        } else {
            try out.appendSlice(gpa, line);
        }
    }
    return out.toOwnedSlice(gpa);
}

/// How the wire bytes are handed to the reader.
const Feed = struct {
    /// Fixed chunk size; 0 means "everything remaining in one slab".
    fixed: usize = 0,
    /// When set, chunk sizes are drawn from this instead of `fixed`.
    rnd: ?std.Random = null,
    max_random: usize = 97,

    fn size(self: Feed, remaining: usize) usize {
        if (self.rnd) |r| return r.intRangeAtMost(usize, 1, @min(self.max_random, remaining));
        if (self.fixed == 0) return remaining;
        return @min(self.fixed, remaining);
    }
};

const Run = struct {
    payload: []u8,
    /// Bytes of the wire body the reader consumed.
    consumed: usize,
    terminated: bool,
};

/// Drive a `BodyReader` over `wire`, feeding chunks per `feed` and
/// draining through a `dst_len`-byte window. `bulk` selects `push`
/// (vectorised) or `pushByteAtATime` (scalar reference).
fn runReader(
    gpa: Allocator,
    wire: []const u8,
    feed: Feed,
    dst_len: usize,
    comptime bulk: bool,
) !Run {
    std.debug.assert(dst_len >= 1);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    const dst = try gpa.alloc(u8, dst_len);
    defer gpa.free(dst);

    var r: BodyReader = .{};
    var pos: usize = 0;
    while (pos < wire.len and !r.terminated) {
        const chunk = wire[pos..][0..feed.size(wire.len - pos)];
        var off: usize = 0;
        while (off < chunk.len and !r.terminated) {
            const step = if (bulk)
                r.push(chunk[off..], dst)
            else
                r.pushByteAtATime(chunk[off..], dst);
            try out.appendSlice(gpa, dst[0..step.written]);
            if (step.consumed == 0 and step.written == 0) return error.NoProgress;
            off += step.consumed;
        }
        pos += off;
    }
    const n = r.finish(dst);
    try out.appendSlice(gpa, dst[0..n]);

    return .{
        .payload = try out.toOwnedSlice(gpa),
        .consumed = pos,
        .terminated = r.terminated,
    };
}

/// Read `wire` in one slab through a generous window — the shape the
/// socket layer uses when a whole article is already buffered.
fn readAll(gpa: Allocator, wire: []const u8) ![]u8 {
    const run = try runReader(gpa, wire, .{}, 64 * 1024, true);
    return run.payload;
}

// ---- Body reader: ported Go cases ------------------------------------

// Ports TestFastBodyReader_ShortPayload.
test "body reader: short payload, CRLF normalised to LF" {
    const wire = try nntpBody(t.allocator, "hello\nworld\n");
    defer t.allocator.free(wire);
    try t.expectEqualStrings("hello\r\nworld\r\n.\r\n", wire);

    const got = try readAll(t.allocator, wire);
    defer t.allocator.free(got);
    try t.expectEqualStrings("hello\nworld\n", got);
}

// Ports TestFastBodyReader_DotStuffing.
test "body reader: dot unstuffing" {
    const wire = try nntpBody(t.allocator, ".alpha\nnormal\n..double\n");
    defer t.allocator.free(wire);
    try t.expectEqualStrings("..alpha\r\nnormal\r\n...double\r\n.\r\n", wire);

    const got = try readAll(t.allocator, wire);
    defer t.allocator.free(got);
    try t.expectEqualStrings(".alpha\nnormal\n..double\n", got);
}

// Ports the protocol half of TestBody_DotStuffing (stub_test.go): the
// wire holds a line starting with ".." which de-stuffs to ".".
test "body reader: wire \"..\" becomes \".\"" {
    const got = try readAll(t.allocator, ".. dotted\r\nnormal line\r\n.\r\n");
    defer t.allocator.free(got);
    try t.expectEqualStrings(". dotted\nnormal line\n", got);
}

// Ports TestFastBodyReader_EmptyBody.
test "body reader: empty body" {
    const run = try runReader(t.allocator, ".\r\n", .{}, 64, true);
    defer t.allocator.free(run.payload);
    try t.expectEqual(@as(usize, 0), run.payload.len);
    try t.expect(run.terminated);
    try t.expectEqual(@as(usize, 3), run.consumed);
}

// Ports TestFastBodyReader_BareLF: a terminator with no CR.
test "body reader: bare LF terminator" {
    const run = try runReader(t.allocator, "hi\n.\n", .{}, 64, true);
    defer t.allocator.free(run.payload);
    try t.expectEqualStrings("hi\n", run.payload);
    try t.expect(run.terminated);
    try t.expectEqual(@as(usize, 5), run.consumed);
}

// Ports TestFastBodyReader_NoTerminator: the peer stopped early. The
// bytes read so far are still handed over; `terminated` reports the
// truncation so the socket layer can decide.
test "body reader: no terminator" {
    const run = try runReader(t.allocator, "partial\n", .{}, 64, true);
    defer t.allocator.free(run.payload);
    try t.expectEqualStrings("partial\n", run.payload);
    try t.expect(!run.terminated);
}

// Ports TestFastBodyReader_MatchesStdlib, with our own oracle standing
// in for textproto.DotReader.
test "body reader: matches the oracle on a 64 KiB random body" {
    const payload = try t.allocator.alloc(u8, 64 * 1024);
    defer t.allocator.free(payload);
    var prng = std.Random.DefaultPrng.init(0xFEED_C0DE);
    prng.random().bytes(payload);

    const wire = try nntpBody(t.allocator, payload);
    defer t.allocator.free(wire);

    const want = try unstuffOracle(t.allocator, wire);
    defer t.allocator.free(want);
    const got = try readAll(t.allocator, wire);
    defer t.allocator.free(got);

    try t.expectEqualSlices(u8, want, got);
}

// ---- Body reader: cases the Go suite lacks ---------------------------

test "body reader: CR and LF edge cases" {
    const cases = [_]struct { wire: []const u8, want: []const u8, term: bool }{
        // A CR that is not part of a CRLF survives as data; the CRLF
        // that actually ends the line becomes one LF.
        .{ .wire = "a\r\r\n.\r\n", .want = "a\r\n", .term = true },
        // Empty line, both terminator styles.
        .{ .wire = "\r\n.\r\n", .want = "\n", .term = true },
        .{ .wire = "\n.\n", .want = "\n", .term = true },
        // Mixed line endings in one body.
        .{ .wire = "a\nb\r\nc\n.\r\n", .want = "a\nb\nc\n", .term = true },
        // A dot line that is not the terminator.
        .{ .wire = "..\r\n.\r\n", .want = ".\n", .term = true },
        // A line of dots.
        .{ .wire = ".....\r\n.\r\n", .want = "....\n", .term = true },
        // Stripped dot followed by a lone CR: the line is not empty, so
        // this is not the terminator.
        .{ .wire = ".\rx\r\n.\r\n", .want = "\rx\n", .term = true },
        // Truncation right after the stripped dot: nothing to emit.
        .{ .wire = "hi\n.", .want = "hi\n", .term = false },
        // Truncation on a held CR: it was literal data after all.
        .{ .wire = "hi\n.\r", .want = "hi\n\r", .term = false },
        .{ .wire = "abc\r", .want = "abc\r", .term = false },
        // No trailing newline before the terminator.
        .{ .wire = "abc\r\n.\r\n", .want = "abc\n", .term = true },
        // Terminator immediately, twice over: only the first counts.
        .{ .wire = ".\r\n.\r\n", .want = "", .term = true },
    };
    for (cases) |c| {
        const run = try runReader(t.allocator, c.wire, .{}, 64, true);
        defer t.allocator.free(run.payload);
        try t.expectEqualStrings(c.want, run.payload);
        try t.expectEqual(c.term, run.terminated);

        // The oracle must agree on all of them too.
        const want = try unstuffOracle(t.allocator, c.wire);
        defer t.allocator.free(want);
        try t.expectEqualSlices(u8, want, run.payload);
    }
}

// The terminator is five bytes (\r\n.\r\n) and the reader is fed
// whatever the socket happened to deliver, so every one of those
// boundaries has to be invisible. Split the body at *every* offset and
// assert the output never changes.
test "body reader: terminator split across every chunk boundary" {
    const wire = "line one\r\nline two\r\n..stuffed\r\n.\r\n";
    const want = "line one\nline two\n.stuffed\n";

    for (0..wire.len + 1) |split| {
        var r: BodyReader = .{};
        var dst: [64]u8 = undefined;
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(t.allocator);

        for ([_][]const u8{ wire[0..split], wire[split..] }) |chunk| {
            var off: usize = 0;
            while (off < chunk.len and !r.terminated) {
                const step = r.push(chunk[off..], &dst);
                try out.appendSlice(t.allocator, dst[0..step.written]);
                off += step.consumed;
            }
        }
        try t.expect(r.terminated);
        try t.expectEqualStrings(want, out.items);
    }
}

// The property most likely to be broken by a fast implementation: the
// same body fed one byte at a time, in random-sized chunks, and as one
// slab must produce byte-identical output — and so must every
// destination window size, including one byte.
test "body reader: chunking and window size do not change the output" {
    var payload: [16 * 1024]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0x5EED_1234);
    const rnd = prng.random();
    // Random bytes with newlines injected every ~128 columns, matching
    // the shape of a yEnc article, plus enough dots to exercise the
    // stuffing path at line starts.
    rnd.bytes(&payload);
    var k: usize = 128;
    while (k < payload.len) : (k += 128) {
        payload[k - 1] = '\n';
        if (k % 512 == 0) payload[k] = '.';
    }

    const wire = try nntpBody(t.allocator, &payload);
    defer t.allocator.free(wire);
    const want = try unstuffOracle(t.allocator, wire);
    defer t.allocator.free(want);

    const chunks = [_]usize{ 1, 2, 3, 7, 64, 1023, 0 }; // 0 = one slab
    const windows = [_]usize{ 1, 2, 3, 17, 4096, 64 * 1024 };
    for (chunks) |chunk| {
        for (windows) |window| {
            const run = try runReader(t.allocator, wire, .{ .fixed = chunk }, window, true);
            defer t.allocator.free(run.payload);
            try t.expect(run.terminated);
            try t.expectEqualSlices(u8, want, run.payload);
        }
    }

    // Random chunk sizes, fixed seed so a failure is reproducible.
    var chunk_prng = std.Random.DefaultPrng.init(0xC0FFEE_15);
    for (windows) |window| {
        const run = try runReader(
            t.allocator,
            wire,
            .{ .rnd = chunk_prng.random() },
            window,
            true,
        );
        defer t.allocator.free(run.payload);
        try t.expect(run.terminated);
        try t.expectEqualSlices(u8, want, run.payload);
    }
}

// The vectorised bulk-copy path must be exactly equivalent to the
// byte-at-a-time state machine, at every chunking and window size.
test "body reader: vectorised path matches the byte-at-a-time reference" {
    var prng = std.Random.DefaultPrng.init(0xABCD_1234);
    const rnd = prng.random();

    var round: usize = 0;
    while (round < 32) : (round += 1) {
        // Vary the payload length across and around the vector width so
        // the scan's tail handover lands at every alignment.
        const len = rnd.intRangeAtMost(usize, 0, 600);
        const payload = try t.allocator.alloc(u8, len);
        defer t.allocator.free(payload);
        for (payload) |*b| {
            // Heavy on the bytes that matter: CR, LF, '.', plus filler.
            b.* = switch (rnd.uintLessThan(u8, 8)) {
                0 => '\r',
                1 => '\n',
                2 => '.',
                else => rnd.int(u8),
            };
        }

        // Feed the raw bytes as a wire body directly (not through
        // nntpBody) so malformed and half-terminated shapes get
        // exercised too, then again properly framed.
        var framed: std.ArrayList(u8) = .empty;
        defer framed.deinit(t.allocator);
        try framed.appendSlice(t.allocator, payload);
        try framed.appendSlice(t.allocator, terminator);

        for ([_][]const u8{ payload, framed.items }) |wire| {
            for ([_]usize{ 1, 5, 0 }) |chunk| {
                for ([_]usize{ 1, 4, 1024 }) |window| {
                    const fast = try runReader(t.allocator, wire, .{ .fixed = chunk }, window, true);
                    defer t.allocator.free(fast.payload);
                    const slow = try runReader(t.allocator, wire, .{ .fixed = chunk }, window, false);
                    defer t.allocator.free(slow.payload);

                    try t.expectEqualSlices(u8, slow.payload, fast.payload);
                    try t.expectEqual(slow.terminated, fast.terminated);
                    try t.expectEqual(slow.consumed, fast.consumed);

                    // And both against the oracle.
                    const want = try unstuffOracle(t.allocator, wire);
                    defer t.allocator.free(want);
                    try t.expectEqualSlices(u8, want, fast.payload);
                }
            }
        }
    }
}

// The socket layer reads the block and the next status line into the
// same buffer, so `consumed` has to stop exactly on the terminator.
test "body reader: stops on the terminator and leaves the rest alone" {
    const stream = "one\r\ntwo\r\n.\r\n205 closing\r\n";

    var r: BodyReader = .{};
    var dst: [64]u8 = undefined;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(t.allocator);

    var off: usize = 0;
    while (off < stream.len and !r.terminated) {
        const step = r.push(stream[off..], &dst);
        try out.appendSlice(t.allocator, dst[0..step.written]);
        off += step.consumed;
    }
    try t.expect(r.terminated);
    try t.expectEqualStrings("one\ntwo\n", out.items);
    try t.expectEqualStrings("205 closing\r\n", stream[off..]);

    // A push after the terminator is a no-op, so a caller that loops
    // one time too many cannot eat the next response.
    const after = r.push(stream[off..], &dst);
    try t.expectEqual(@as(usize, 0), after.consumed);
    try t.expectEqual(@as(usize, 0), after.written);
    try t.expect(after.terminated);

    // And the leftover parses as the status line it is.
    const status = try parseStatusLine(stream[off..]);
    try t.expectEqual(codes.closing, status.code);
}

test "body reader: a body with no lines at all" {
    // Terminator only, fed one byte at a time.
    const run = try runReader(t.allocator, ".\r\n", .{ .fixed = 1 }, 1, true);
    defer t.allocator.free(run.payload);
    try t.expectEqual(@as(usize, 0), run.payload.len);
    try t.expect(run.terminated);
}

test "body reader: 750 KiB article, one slab" {
    // The production article size. Mostly a smoke test that nothing
    // degrades at scale, and that the oracle and the reader still
    // agree when the vector loop runs tens of thousands of times.
    const payload = try t.allocator.alloc(u8, 750 * 1024);
    defer t.allocator.free(payload);
    var prng = std.Random.DefaultPrng.init(0x750_1024);
    prng.random().bytes(payload);
    var k: usize = 128;
    while (k < payload.len) : (k += 128) payload[k - 1] = '\n';

    const wire = try nntpBody(t.allocator, payload);
    defer t.allocator.free(wire);
    const want = try unstuffOracle(t.allocator, wire);
    defer t.allocator.free(want);
    const got = try readAll(t.allocator, wire);
    defer t.allocator.free(got);
    try t.expectEqualSlices(u8, want, got);
}
