//! HTTP/1.1 request parsing — a pure state machine over caller-owned
//! bytes. No sockets, no allocator, no clock.
//!
//! ## Shape
//!
//! `Parser` is fed the *same growing buffer* on every call:
//!
//!     var p: Parser = .{ .limits = .{} };
//!     while (true) {
//!         len += try stream.read(buf[len..]);
//!         switch (try p.parse(buf[0..len])) {
//!             .incomplete => continue,
//!             .complete => break,
//!         }
//!     }
//!
//! It never rescans what it has already examined, so feeding one byte at
//! a time costs the same total work as feeding the whole request at
//! once. That is also why it is correct across *any* chunk boundary:
//! there is no lookahead beyond "is the byte after this CR an LF", and
//! that decision is deferred until the byte exists.
//!
//! ## Ownership — read this before using a parsed request
//!
//! Every slice in `Request` (`target`, `path`, `query`, and both halves
//! of every header) points **into the caller's buffer**. Nothing is
//! copied and nothing is allocated. They stay valid exactly as long as
//! the bytes they point at are neither overwritten nor moved. In
//! practice that means:
//!
//!   * valid until the connection reuses or compacts its read buffer;
//!   * therefore valid for the duration of a handler call, and *not*
//!     beyond it. A handler that keeps the connection open (SSE) must
//!     copy anything it still needs.
//!
//! `Request.rebase` exists for the one case where the server does have
//! to move the bytes: a request with a body reuses the read buffer for
//! the body, so it first copies the header block elsewhere and rebases
//! the slices onto the copy.
//!
//! ## Strictness
//!
//! Line terminators must be CRLF. A bare LF and a bare CR are both
//! rejected, and so is obsolete line folding. This is not pedantry: the
//! daemon usually sits behind a reverse proxy, and every request
//! smuggling bug in the last decade came from two hops disagreeing about
//! where a message ended. One accepted framing means there is nothing to
//! disagree about. Real clients all send CRLF.
//!
//! For the same reason `Content-Length` together with
//! `Transfer-Encoding` is a hard rejection rather than a
//! "prefer chunked" preference, a repeated `Content-Length` is rejected
//! even when the values agree, and any `Transfer-Encoding` other than
//! bare `chunked` is a 501.

const std = @import("std");

// ---------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------

/// Compile-time ceiling on stored headers. `Limits.max_headers` is the
/// runtime knob and must not exceed this; the array lives inline in
/// `Request` so a request costs no allocation at all.
pub const max_header_slots: usize = 128;

/// Everything an attacker can inflate has a number here. Defaults are
/// sized for a homelab UI plus *arr clients behind a proxy: nginx's own
/// defaults are 8 KiB per header line and 4×8 KiB total, so anything we
/// would accept beyond that could never reach us in production anyway.
pub const Limits = struct {
    max_request_line: usize = 8 << 10,
    max_header_line: usize = 8 << 10,
    /// Total bytes of request line + all headers + the terminating CRLF.
    max_header_bytes: usize = 32 << 10,
    max_headers: usize = 100,
    /// Cap on a request body, decoded. NZB uploads are the big ones and
    /// a large multi-file NZB is a few MiB.
    max_body: u64 = 16 << 20,
    /// Cap on a single chunk's declared size. A chunk header claiming
    /// 2^63 bytes must be refused before it is used for arithmetic.
    max_chunk_size: u64 = 16 << 20,
    /// Trailer lines after the last chunk. Nothing we serve uses
    /// trailers; a handful are tolerated and ignored.
    max_trailer_lines: usize = 16,
};

pub const ParseError = error{
    /// Framing.
    BareCr,
    BareLf,
    ObsoleteLineFolding,
    /// Request line.
    MalformedRequestLine,
    BadMethod,
    BadTarget,
    BadVersion,
    UnsupportedVersion,
    /// Headers.
    MissingColon,
    BadHeaderName,
    BadHeaderValue,
    MissingHost,
    DuplicateHost,
    /// Framing metadata.
    DuplicateContentLength,
    BadContentLength,
    ContentLengthWithTransferEncoding,
    UnsupportedTransferEncoding,
    DuplicateTransferEncoding,
    ExpectationFailed,
    /// Size limits.
    RequestLineTooLong,
    HeaderLineTooLong,
    HeadersTooLarge,
    TooManyHeaders,
    BodyTooLarge,
    /// Chunked body.
    BadChunkSize,
    ChunkTooLarge,
    BadChunkTerminator,
    TooManyTrailers,
};

/// The status code to answer a rejected request with. Kept next to the
/// error set so a new error cannot silently default to 400 in one place
/// and 500 in another.
pub fn statusFor(err: ParseError) u16 {
    return switch (err) {
        error.RequestLineTooLong => 414, // URI Too Long
        error.HeaderLineTooLong, error.HeadersTooLarge, error.TooManyHeaders => 431,
        error.BodyTooLarge, error.ChunkTooLarge => 413,
        error.UnsupportedVersion => 505,
        error.UnsupportedTransferEncoding => 501,
        error.ExpectationFailed => 417,
        else => 400,
    };
}

// ---------------------------------------------------------------------
// Method / version
// ---------------------------------------------------------------------

pub const Method = enum {
    get,
    head,
    post,
    put,
    patch,
    delete,
    options,
    trace,
    connect,
    /// A syntactically valid token we don't recognise. Routing will not
    /// match it, so it becomes a 405 rather than a parse failure.
    other,

    pub fn parse(s: []const u8) Method {
        // Ordered by expected frequency; these are short enough that a
        // chain of `eql` beats any table.
        if (std.mem.eql(u8, s, "GET")) return .get;
        if (std.mem.eql(u8, s, "POST")) return .post;
        if (std.mem.eql(u8, s, "HEAD")) return .head;
        if (std.mem.eql(u8, s, "PUT")) return .put;
        if (std.mem.eql(u8, s, "DELETE")) return .delete;
        if (std.mem.eql(u8, s, "PATCH")) return .patch;
        if (std.mem.eql(u8, s, "OPTIONS")) return .options;
        if (std.mem.eql(u8, s, "TRACE")) return .trace;
        if (std.mem.eql(u8, s, "CONNECT")) return .connect;
        return .other;
    }

    pub fn name(self: Method) []const u8 {
        return switch (self) {
            .get => "GET",
            .head => "HEAD",
            .post => "POST",
            .put => "PUT",
            .patch => "PATCH",
            .delete => "DELETE",
            .options => "OPTIONS",
            .trace => "TRACE",
            .connect => "CONNECT",
            .other => "?",
        };
    }
};

pub const Version = enum {
    http_1_0,
    http_1_1,

    pub fn text(self: Version) []const u8 {
        return switch (self) {
            .http_1_0 => "HTTP/1.0",
            .http_1_1 => "HTTP/1.1",
        };
    }
};

// ---------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------

pub const Header = struct {
    /// As sent, case preserved. Compare with `Headers.get`, never with
    /// `mem.eql`.
    name: []const u8,
    value: []const u8,
};

/// Field name comparison is ASCII case-insensitive (RFC 9110 §5.1).
/// Hand-rolled rather than `std.ascii.eqlIgnoreCase` because that one
/// lowercases both sides through a function call per byte; this is the
/// hottest loop in the parser after the line scan.
pub fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (x == y) continue;
        if (lower(x) != lower(y)) return false;
    }
    return true;
}

inline fn lower(c: u8) u8 {
    return if (c >= 'A' and c <= 'Z') c | 0x20 else c;
}

pub const Headers = struct {
    entries: [max_header_slots]Header = undefined,
    len: usize = 0,

    pub fn slice(self: *const Headers) []const Header {
        return self.entries[0..self.len];
    }

    /// First value for `name`, or null. First rather than last because
    /// every header we act on is rejected outright when repeated, so
    /// "first" and "only" are the same thing for those.
    pub fn get(self: *const Headers, name: []const u8) ?[]const u8 {
        for (self.slice()) |e| {
            if (eqlIgnoreCase(e.name, name)) return e.value;
        }
        return null;
    }

    pub fn count(self: *const Headers, name: []const u8) usize {
        var n: usize = 0;
        for (self.slice()) |e| {
            if (eqlIgnoreCase(e.name, name)) n += 1;
        }
        return n;
    }

    pub fn has(self: *const Headers, name: []const u8) bool {
        return self.get(name) != null;
    }
};

// ---------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------

/// How the body is framed on the wire.
pub const BodyKind = enum { none, length, chunked };

pub const Request = struct {
    method: Method = .other,
    /// The raw method token. Needed for `Allow:` and the access log when
    /// `method == .other`.
    method_raw: []const u8 = "",
    /// Request-target exactly as received, minus any absolute-form
    /// scheme and authority.
    target: []const u8 = "",
    /// `target` up to the first '?'. Still percent-encoded — routing
    /// compares encoded bytes so that `%2F` can never be mistaken for a
    /// path separator. Use `decodePath` when you want the real thing.
    path: []const u8 = "",
    /// Everything after the first '?', without the '?'.
    query: []const u8 = "",
    version: Version = .http_1_1,
    headers: Headers = .{},

    /// Declared body length for `.length` framing.
    content_length: u64 = 0,
    body_kind: BodyKind = .none,
    /// Post-`Connection:`-analysis verdict, already accounting for the
    /// protocol version default.
    keep_alive: bool = true,
    expect_continue: bool = false,

    /// Decoded body. Owned by the server, not by the parser; empty
    /// until the body has been fully read.
    body: []const u8 = "",

    pub fn header(self: *const Request, name: []const u8) ?[]const u8 {
        return self.headers.get(name);
    }

    pub fn hasBody(self: *const Request) bool {
        return self.body_kind != .none;
    }

    /// Raw (still percent-encoded) value of a query parameter, or null.
    /// The last occurrence wins, matching Go's `url.Values.Get` on a
    /// re-parsed query only in the single-value case — which is the only
    /// case anything here produces.
    pub fn queryValue(self: *const Request, name: []const u8) ?[]const u8 {
        var it = QueryIter{ .rest = self.query };
        var found: ?[]const u8 = null;
        while (it.next()) |kv| {
            if (std.mem.eql(u8, kv.key, name)) found = kv.value;
        }
        return found;
    }

    /// Percent-decoded query parameter written into `out`. `+` decodes to
    /// space, matching `application/x-www-form-urlencoded`, which is what
    /// browsers and the *arr clients emit.
    pub fn queryValueDecoded(
        self: *const Request,
        out: []u8,
        name: []const u8,
    ) DecodeError!?[]const u8 {
        const raw = self.queryValue(name) orelse return null;
        return try percentDecode(out, raw, .query);
    }

    /// Percent-decoded path written into `out`.
    pub fn decodePath(self: *const Request, out: []u8) DecodeError![]const u8 {
        return percentDecode(out, self.path, .path);
    }

    /// Value of one cookie from the `Cookie` header, or null. Cookie
    /// values are returned verbatim: no percent- or quote-decoding,
    /// because the only cookie we read is a session token we minted
    /// ourselves out of URL-safe bytes.
    pub fn cookie(self: *const Request, name: []const u8) ?[]const u8 {
        const raw = self.headers.get("cookie") orelse return null;
        var rest = raw;
        while (rest.len > 0) {
            const semi = std.mem.indexOfScalar(u8, rest, ';') orelse rest.len;
            const pair = trimOws(rest[0..semi]);
            rest = if (semi == rest.len) "" else rest[semi + 1 ..];
            const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
            if (!std.mem.eql(u8, pair[0..eq], name)) continue;
            var v = pair[eq + 1 ..];
            // A quoted cookie value is legal; strip the quotes.
            if (v.len >= 2 and v[0] == '"' and v[v.len - 1] == '"') v = v[1 .. v.len - 1];
            return v;
        }
        return null;
    }

    /// Move every borrowed slice from `from` onto `to`, which must be a
    /// byte-identical copy of it. The server calls this when it needs the
    /// read buffer back for the body: the header block is copied out
    /// first, then the request is rebased onto the copy.
    ///
    /// Asserts each slice actually lies inside `from`, so a rebase of a
    /// request that was already rebased (or never parsed from `from`)
    /// trips in debug rather than producing a wild pointer.
    pub fn rebase(self: *Request, from: []const u8, to: []const u8) void {
        std.debug.assert(from.len == to.len);
        self.method_raw = moveSlice(self.method_raw, from, to);
        self.target = moveSlice(self.target, from, to);
        self.path = moveSlice(self.path, from, to);
        self.query = moveSlice(self.query, from, to);
        for (self.headers.entries[0..self.headers.len]) |*e| {
            e.name = moveSlice(e.name, from, to);
            e.value = moveSlice(e.value, from, to);
        }
    }
};

fn moveSlice(s: []const u8, from: []const u8, to: []const u8) []const u8 {
    if (s.len == 0) return to[0..0];
    const off = @intFromPtr(s.ptr) - @intFromPtr(from.ptr);
    std.debug.assert(off + s.len <= from.len);
    return to[off..][0..s.len];
}

// ---------------------------------------------------------------------
// Query iteration
// ---------------------------------------------------------------------

pub const QueryPair = struct { key: []const u8, value: []const u8 };

/// Splits a raw query string on '&' and '='. Values stay
/// percent-encoded; a key with no '=' yields an empty value.
pub const QueryIter = struct {
    rest: []const u8,

    pub fn next(self: *QueryIter) ?QueryPair {
        while (self.rest.len > 0) {
            const amp = std.mem.indexOfScalar(u8, self.rest, '&') orelse self.rest.len;
            const pair = self.rest[0..amp];
            self.rest = if (amp == self.rest.len) "" else self.rest[amp + 1 ..];
            if (pair.len == 0) continue; // "a=1&&b=2"
            const eq = std.mem.indexOfScalar(u8, pair, '=') orelse {
                return .{ .key = pair, .value = "" };
            };
            return .{ .key = pair[0..eq], .value = pair[eq + 1 ..] };
        }
        return null;
    }
};

// ---------------------------------------------------------------------
// Percent decoding
// ---------------------------------------------------------------------

pub const DecodeError = error{
    /// `%` not followed by two hex digits, including a `%` at the very
    /// end of the input.
    BadPercentEncoding,
    /// `%00`. Decoded NUL has no legitimate use in a path or a query and
    /// is a classic way to smuggle a different string past a downstream
    /// consumer that stops at the NUL.
    EmbeddedNul,
    NoSpaceLeft,
};

pub const DecodeMode = enum {
    /// `+` is a literal plus.
    path,
    /// `+` decodes to a space.
    query,
};

pub fn percentDecode(out: []u8, s: []const u8, mode: DecodeMode) DecodeError![]const u8 {
    var n: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        const byte: u8 = switch (c) {
            '%' => blk: {
                // Covers both "%" at the very end and "%A" with one digit.
                if (i + 3 > s.len) return error.BadPercentEncoding;
                const hi = hexVal(s[i + 1]) orelse return error.BadPercentEncoding;
                const lo = hexVal(s[i + 2]) orelse return error.BadPercentEncoding;
                i += 3;
                break :blk (hi << 4) | lo;
            },
            '+' => blk: {
                i += 1;
                break :blk if (mode == .query) ' ' else '+';
            },
            else => blk: {
                i += 1;
                break :blk c;
            },
        };
        if (byte == 0) return error.EmbeddedNul;
        if (n == out.len) return error.NoSpaceLeft;
        out[n] = byte;
        n += 1;
    }
    return out[0..n];
}

fn hexVal(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

/// True when the path contains a `.` or `..` segment, in raw or
/// percent-encoded form. Anything serving files from a path has to check
/// this; routing itself does not, because every route is an exact or
/// prefix match against a literal.
pub fn hasDotSegment(path: []const u8) bool {
    var rest = path;
    while (rest.len > 0) {
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
        const seg = rest[0..slash];
        rest = if (slash == rest.len) "" else rest[slash + 1 ..];
        if (seg.len == 0) continue;
        if (std.mem.eql(u8, seg, ".") or std.mem.eql(u8, seg, "..")) return true;
        // %2e is '.', and "%2e%2e" is the encoded traversal that catches
        // servers which check before decoding.
        if (seg.len <= 6 and std.mem.indexOfScalar(u8, seg, '%') != null) {
            var buf: [8]u8 = undefined;
            const dec = percentDecode(&buf, seg, .path) catch continue;
            if (std.mem.eql(u8, dec, ".") or std.mem.eql(u8, dec, "..")) return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------
// Token / value character classes
// ---------------------------------------------------------------------

/// RFC 9110 `tchar`. Used for both the method and header field names, so
/// a space before the colon (`"Host : x"`) and a NUL are both rejected
/// here rather than being normalised away.
const tchar_table: [256]bool = blk: {
    var t: [256]bool = @splat(false);
    for ("!#$%&'*+-.^_`|~") |c| t[c] = true;
    for ('0'..'9' + 1) |c| t[c] = true;
    for ('a'..'z' + 1) |c| t[c] = true;
    for ('A'..'Z' + 1) |c| t[c] = true;
    break :blk t;
};

inline fn isTchar(c: u8) bool {
    return tchar_table[c];
}

fn isToken(s: []const u8) bool {
    if (s.len == 0) return false;
    for (s) |c| {
        if (!isTchar(c)) return false;
    }
    return true;
}

/// Visible ASCII. The request-target must be entirely within this set:
/// it rules out NUL, every other control character, DEL, raw space and
/// all 8-bit bytes in one test.
inline fn isVchar(c: u8) bool {
    return c >= 0x21 and c <= 0x7E;
}

/// Legal in a field value: HTAB, visible ASCII, space, and obs-text
/// (0x80..0xFF, which real clients still emit in `Content-Disposition`
/// filenames). Everything else — CR, LF, NUL, other controls, DEL — is a
/// rejection.
inline fn isFieldValueChar(c: u8) bool {
    return c == '\t' or (c >= 0x20 and c != 0x7F);
}

fn trimOws(s: []const u8) []const u8 {
    var a: usize = 0;
    var b: usize = s.len;
    while (a < b and (s[a] == ' ' or s[a] == '\t')) a += 1;
    while (b > a and (s[b - 1] == ' ' or s[b - 1] == '\t')) b -= 1;
    return s[a..b];
}

// ---------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------

pub const Status = enum { incomplete, complete };

pub const Parser = struct {
    limits: Limits = .{},

    state: State = .before_request_line,
    /// Next index to examine. Everything below it has been consumed;
    /// this is what makes byte-at-a-time feeding as cheap as one shot.
    scan: usize = 0,
    /// Start of the line currently being accumulated.
    line_start: usize = 0,
    /// Empty lines skipped before the request line. RFC 9112 says a
    /// server SHOULD ignore at least one; a slowloris that sends nothing
    /// but blank lines gets cut off after two.
    blanks: usize = 0,

    req: Request = .{},

    pub const State = enum { before_request_line, headers, complete };

    /// Reuse the parser for the next request on a keep-alive connection.
    /// The buffer is expected to have been compacted so the next request
    /// starts at index 0.
    pub fn reset(self: *Parser) void {
        self.* = .{ .limits = self.limits };
    }

    /// Index one past the final CRLF of the header block. Body bytes, if
    /// any, start here. Only meaningful once `parse` returned
    /// `.complete`.
    pub fn headerEnd(self: *const Parser) usize {
        return self.scan;
    }

    /// Consume as much of `buf` as possible. `buf` must be the same
    /// bytes as last time plus zero or more appended, starting at the
    /// same origin.
    pub fn parse(self: *Parser, buf: []const u8) ParseError!Status {
        std.debug.assert(buf.len >= self.scan);
        std.debug.assert(self.limits.max_headers <= max_header_slots);

        while (true) switch (self.state) {
            .complete => return .complete,
            .before_request_line => {
                const line = (try self.nextLine(buf, self.limits.max_request_line, error.RequestLineTooLong)) orelse
                    return .incomplete;
                if (line.len == 0) {
                    self.blanks += 1;
                    if (self.blanks > 2) return error.MalformedRequestLine;
                    continue;
                }
                try self.parseRequestLine(line);
                self.state = .headers;
            },
            .headers => {
                const line = (try self.nextLine(buf, self.limits.max_header_line, error.HeaderLineTooLong)) orelse
                    return .incomplete;
                if (line.len == 0) {
                    try self.finalize();
                    self.state = .complete;
                    return .complete;
                }
                try self.addHeader(line);
                if (self.scan > self.limits.max_header_bytes) return error.HeadersTooLarge;
            },
        };
    }

    /// One CRLF-terminated line, without the terminator, or null when
    /// more bytes are needed.
    ///
    /// The only place framing strictness is enforced, deliberately: a
    /// single choke point means there is no second implementation to
    /// disagree with.
    fn nextLine(
        self: *Parser,
        buf: []const u8,
        max_len: usize,
        comptime too_long: ParseError,
    ) ParseError!?[]const u8 {
        var i = self.scan;
        while (i < buf.len) : (i += 1) {
            if (buf[i] != '\n') continue;
            // A LF that is not preceded by CR is a bare LF.
            if (i == self.line_start or buf[i - 1] != '\r') return error.BareLf;
            const line = buf[self.line_start .. i - 1];
            if (line.len > max_len) return too_long;
            // Any other CR in the line is a bare CR: some proxies treat
            // it as a terminator, we refuse to guess.
            if (std.mem.indexOfScalar(u8, line, '\r') != null) return error.BareCr;
            self.line_start = i + 1;
            self.scan = i + 1;
            return line;
        }
        self.scan = buf.len;
        // Bound the *incomplete* line too, or a client can hold the
        // connection while feeding an unterminated 4 GiB header.
        if (buf.len - self.line_start > max_len) return too_long;
        if (buf.len > self.limits.max_header_bytes) return error.HeadersTooLarge;
        return null;
    }

    fn parseRequestLine(self: *Parser, line: []const u8) ParseError!void {
        // Exactly two single spaces. Tolerating runs of spaces or tabs
        // here is another framing disagreement waiting to happen.
        const sp1 = std.mem.indexOfScalar(u8, line, ' ') orelse return error.MalformedRequestLine;
        const rest = line[sp1 + 1 ..];
        const sp2_rel = std.mem.indexOfScalar(u8, rest, ' ') orelse return error.MalformedRequestLine;
        const method = line[0..sp1];
        const target = rest[0..sp2_rel];
        const version = rest[sp2_rel + 1 ..];
        if (std.mem.indexOfScalar(u8, version, ' ') != null) return error.MalformedRequestLine;

        if (!isToken(method)) return error.BadMethod;
        self.req.method_raw = method;
        self.req.method = Method.parse(method);

        try self.parseTarget(target);

        if (std.mem.eql(u8, version, "HTTP/1.1")) {
            self.req.version = .http_1_1;
            self.req.keep_alive = true;
        } else if (std.mem.eql(u8, version, "HTTP/1.0")) {
            self.req.version = .http_1_0;
            // 1.0 is close-by-default; `Connection: keep-alive` opts in.
            self.req.keep_alive = false;
        } else if (std.mem.startsWith(u8, version, "HTTP/")) {
            return error.UnsupportedVersion;
        } else {
            return error.BadVersion;
        }
    }

    fn parseTarget(self: *Parser, target: []const u8) ParseError!void {
        if (target.len == 0) return error.BadTarget;
        for (target) |c| {
            if (!isVchar(c)) return error.BadTarget;
        }

        var t = target;
        if (t[0] != '/') {
            if (std.mem.eql(u8, t, "*")) {
                // asterisk-form, only legal for OPTIONS.
                if (self.req.method != .options) return error.BadTarget;
                self.req.target = t;
                self.req.path = t;
                self.req.query = "";
                return;
            }
            // absolute-form: a client that thinks we are a proxy, or a
            // proxy passing the original line through. Drop the scheme
            // and authority and route on what is left.
            const scheme_end = std.mem.indexOf(u8, t, "://") orelse return error.BadTarget;
            const scheme = t[0..scheme_end];
            if (!eqlIgnoreCase(scheme, "http") and !eqlIgnoreCase(scheme, "https")) return error.BadTarget;
            const after = t[scheme_end + 3 ..];
            const slash = std.mem.indexOfScalar(u8, after, '/') orelse {
                // "http://host" — the effective target is "/".
                self.req.target = "/";
                self.req.path = "/";
                self.req.query = "";
                return;
            };
            t = after[slash..];
        }

        self.req.target = t;
        const q = std.mem.indexOfScalar(u8, t, '?') orelse {
            self.req.path = t;
            self.req.query = "";
            return;
        };
        self.req.path = t[0..q];
        self.req.query = t[q + 1 ..];
    }

    fn addHeader(self: *Parser, line: []const u8) ParseError!void {
        // A line starting with space or tab is a continuation of the
        // previous one under the obsolete folding rules. Rejected: it is
        // the other half of the smuggling family, since a folded
        // `Content-Length` reads differently to different parsers.
        if (line[0] == ' ' or line[0] == '\t') return error.ObsoleteLineFolding;

        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.MissingColon;
        const name = line[0..colon];
        if (!isToken(name)) return error.BadHeaderName;

        const value = trimOws(line[colon + 1 ..]);
        for (value) |c| {
            if (!isFieldValueChar(c)) return error.BadHeaderValue;
        }

        if (self.req.headers.len >= self.limits.max_headers) return error.TooManyHeaders;
        self.req.headers.entries[self.req.headers.len] = .{ .name = name, .value = value };
        self.req.headers.len += 1;
    }

    /// Derive framing from the collected headers. Everything that can
    /// make two HTTP implementations disagree about message boundaries is
    /// decided here, once.
    fn finalize(self: *Parser) ParseError!void {
        const r = &self.req;

        var host_count: usize = 0;
        var cl_count: usize = 0;
        var te_count: usize = 0;
        var content_length: u64 = 0;
        var chunked = false;

        for (r.headers.slice()) |e| {
            if (eqlIgnoreCase(e.name, "host")) {
                host_count += 1;
            } else if (eqlIgnoreCase(e.name, "content-length")) {
                cl_count += 1;
                content_length = try parseContentLength(e.value);
            } else if (eqlIgnoreCase(e.name, "transfer-encoding")) {
                te_count += 1;
                // Only bare `chunked` is supported. `identity` is a
                // relic that means "no encoding"; a list such as
                // "gzip, chunked" would need a decoder we don't have,
                // and "chunked, chunked" is a smuggling probe.
                const v = trimOws(e.value);
                if (eqlIgnoreCase(v, "chunked")) {
                    chunked = true;
                } else if (eqlIgnoreCase(v, "identity")) {
                    // no framing change
                } else {
                    return error.UnsupportedTransferEncoding;
                }
            } else if (eqlIgnoreCase(e.name, "connection")) {
                var it = TokenIter{ .rest = e.value };
                while (it.next()) |tok| {
                    if (eqlIgnoreCase(tok, "close")) {
                        r.keep_alive = false;
                    } else if (eqlIgnoreCase(tok, "keep-alive")) {
                        // Only meaningful for 1.0; harmless on 1.1.
                        if (r.version == .http_1_0) r.keep_alive = true;
                    }
                }
            } else if (eqlIgnoreCase(e.name, "expect")) {
                if (!eqlIgnoreCase(trimOws(e.value), "100-continue")) return error.ExpectationFailed;
                r.expect_continue = true;
            }
        }

        // Repeated Content-Length is rejected even when the values agree.
        // Two identical values are harmless in isolation but they only
        // ever appear from a rewriting middlebox, and the next hop may
        // well take the *other* one.
        if (cl_count > 1) return error.DuplicateContentLength;
        if (te_count > 1) return error.DuplicateTransferEncoding;
        if (cl_count > 0 and chunked) return error.ContentLengthWithTransferEncoding;

        if (host_count > 1) return error.DuplicateHost;
        // HTTP/1.1 requires exactly one Host. 1.0 predates it.
        if (host_count == 0 and r.version == .http_1_1) return error.MissingHost;

        if (chunked) {
            r.body_kind = .chunked;
            r.content_length = 0;
        } else if (cl_count > 0 and content_length > 0) {
            if (content_length > self.limits.max_body) return error.BodyTooLarge;
            r.body_kind = .length;
            r.content_length = content_length;
        } else {
            r.body_kind = .none;
            r.content_length = 0;
        }

        // A 1.0 client that cannot be told where the body ends must not
        // be kept alive: chunked is a 1.1 feature.
        if (chunked and r.version == .http_1_0) return error.UnsupportedTransferEncoding;
    }
};

/// Digits only. No sign, no whitespace, no comma-separated list — each
/// of those is a place where two parsers pick different numbers.
fn parseContentLength(s: []const u8) ParseError!u64 {
    if (s.len == 0 or s.len > 19) return error.BadContentLength;
    var n: u64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.BadContentLength;
        n = n * 10 + (c - '0');
    }
    return n;
}

/// Comma-separated token list, OWS-trimmed. Used for `Connection`.
pub const TokenIter = struct {
    rest: []const u8,

    pub fn next(self: *TokenIter) ?[]const u8 {
        while (self.rest.len > 0) {
            const comma = std.mem.indexOfScalar(u8, self.rest, ',') orelse self.rest.len;
            const tok = trimOws(self.rest[0..comma]);
            self.rest = if (comma == self.rest.len) "" else self.rest[comma + 1 ..];
            if (tok.len > 0) return tok;
        }
        return null;
    }
};

// ---------------------------------------------------------------------
// Body decoding
// ---------------------------------------------------------------------

/// Turns wire bytes into body bytes for both framings.
///
/// Pure and allocation-free: `feed` returns a slice *of its input* plus
/// how much it consumed, and the caller decides where to put it. Call it
/// in a loop until `consumed` covers the input or `done` is set — one
/// call yields at most one contiguous run, because a chunked stream can
/// hold several runs separated by framing bytes.
pub const BodyDecoder = struct {
    kind: BodyKind,
    limits: Limits = .{},

    /// Bytes still to come in the current chunk, or in the whole body for
    /// `.length` framing.
    remaining: u64 = 0,
    /// Decoded bytes emitted so far, checked against `max_body`.
    total: u64 = 0,
    state: State = .initial,
    /// Chunk size line under construction. 40 bytes covers 16 hex digits
    /// plus a generous extension.
    line: [40]u8 = undefined,
    line_len: usize = 0,
    trailers: usize = 0,

    pub const State = enum { initial, size_line, data, data_crlf, trailer, done };

    pub const Result = struct {
        consumed: usize,
        data: []const u8,
        done: bool,
    };

    pub fn init(req: *const Request, limits: Limits) BodyDecoder {
        return .{
            .kind = req.body_kind,
            .limits = limits,
            .remaining = if (req.body_kind == .length) req.content_length else 0,
            .state = switch (req.body_kind) {
                .none => .done,
                .length => .data,
                .chunked => .size_line,
            },
        };
    }

    pub fn isDone(self: *const BodyDecoder) bool {
        return self.state == .done;
    }

    pub fn feed(self: *BodyDecoder, in: []const u8) ParseError!Result {
        switch (self.state) {
            .done => return .{ .consumed = 0, .data = in[0..0], .done = true },
            .initial => unreachable, // init always leaves a live state
            .data => {
                const n: usize = @intCast(@min(self.remaining, in.len));
                self.remaining -= n;
                self.total += n;
                if (self.total > self.limits.max_body) return error.BodyTooLarge;
                if (self.remaining == 0) {
                    self.state = if (self.kind == .chunked) .data_crlf else .done;
                }
                return .{ .consumed = n, .data = in[0..n], .done = self.state == .done };
            },
            .data_crlf => {
                // Exactly CRLF, then the next size line.
                var i: usize = 0;
                while (i < in.len and self.line_len < 2) : (i += 1) {
                    const want: u8 = if (self.line_len == 0) '\r' else '\n';
                    if (in[i] != want) return error.BadChunkTerminator;
                    self.line_len += 1;
                }
                if (self.line_len == 2) {
                    self.line_len = 0;
                    self.state = .size_line;
                }
                return .{ .consumed = i, .data = in[0..0], .done = false };
            },
            // Both are line-oriented framing with no payload of their
            // own. `takeLine` always consumes at least one byte when
            // given one, so the caller's loop cannot spin.
            .size_line, .trailer => {
                const consumed = try self.takeLine(in);
                return .{ .consumed = consumed, .data = in[0..0], .done = self.state == .done };
            },
        }
    }

    /// Accumulate into `line` until CRLF, then act on it. Returns bytes
    /// consumed; the state changes only once a full line arrived.
    fn takeLine(self: *BodyDecoder, in: []const u8) ParseError!usize {
        var i: usize = 0;
        while (i < in.len) {
            const c = in[i];
            i += 1;
            if (c == '\n') {
                if (self.line_len == 0 or self.line[self.line_len - 1] != '\r') return error.BadChunkTerminator;
                const line = self.line[0 .. self.line_len - 1];
                if (std.mem.indexOfScalar(u8, line, '\r') != null) return error.BadChunkTerminator;
                self.line_len = 0;
                try self.onLine(line);
                return i;
            }
            if (self.line_len == self.line.len) {
                return if (self.state == .trailer) error.TooManyTrailers else error.BadChunkSize;
            }
            self.line[self.line_len] = c;
            self.line_len += 1;
        }
        return i;
    }

    fn onLine(self: *BodyDecoder, line: []const u8) ParseError!void {
        switch (self.state) {
            .size_line => {
                // Chunk extensions (";name=value") are legal and ignored,
                // but they still have to be visible ASCII.
                const semi = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
                const digits = line[0..semi];
                for (line[semi..]) |c| {
                    if (!isVchar(c)) return error.BadChunkSize;
                }
                if (digits.len == 0 or digits.len > 16) return error.BadChunkSize;
                var size: u64 = 0;
                for (digits) |c| {
                    const v = hexVal(c) orelse return error.BadChunkSize;
                    size = (size << 4) | v;
                }
                if (size > self.limits.max_chunk_size) return error.ChunkTooLarge;
                if (self.total + size > self.limits.max_body) return error.BodyTooLarge;
                if (size == 0) {
                    self.state = .trailer;
                } else {
                    self.remaining = size;
                    self.state = .data;
                }
            },
            .trailer => {
                if (line.len == 0) {
                    self.state = .done;
                    return;
                }
                self.trailers += 1;
                if (self.trailers > self.limits.max_trailer_lines) return error.TooManyTrailers;
                // Trailers are parsed only enough to reject garbage; we
                // never act on one.
                if (std.mem.indexOfScalar(u8, line, ':') == null) return error.BadChunkTerminator;
            },
            else => unreachable,
        }
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Parse a whole request in one shot.
fn parseAll(buf: []const u8, limits: Limits) ParseError!Parser {
    var p: Parser = .{ .limits = limits };
    _ = try p.parse(buf);
    return p;
}

/// Parse the same bytes at every possible single split, and with a
/// byte-at-a-time feed, asserting all of them agree with the one-shot
/// result. This is the property that makes the parser usable on a
/// socket: nothing may depend on how the kernel happened to slice the
/// stream.
fn expectSplitInvariant(raw: []const u8) !void {
    const oneshot = try parseAll(raw, .{});

    var split: usize = 1;
    while (split < raw.len) : (split += 1) {
        var p: Parser = .{};
        const first = try p.parse(raw[0..split]);
        if (split < oneshot.headerEnd()) {
            try testing.expectEqual(Status.incomplete, first);
        }
        try testing.expectEqual(Status.complete, try p.parse(raw));
        try testing.expectEqual(oneshot.headerEnd(), p.headerEnd());
        try testing.expectEqualStrings(oneshot.req.path, p.req.path);
        try testing.expectEqualStrings(oneshot.req.query, p.req.query);
        try testing.expectEqual(oneshot.req.headers.len, p.req.headers.len);
        for (oneshot.req.headers.slice(), p.req.headers.slice()) |a, b| {
            try testing.expectEqualStrings(a.name, b.name);
            try testing.expectEqualStrings(a.value, b.value);
        }
        try testing.expectEqual(oneshot.req.body_kind, p.req.body_kind);
        try testing.expectEqual(oneshot.req.content_length, p.req.content_length);
        try testing.expectEqual(oneshot.req.keep_alive, p.req.keep_alive);
    }

    // One byte at a time.
    var p: Parser = .{};
    var i: usize = 0;
    while (i < raw.len) : (i += 1) {
        const st = try p.parse(raw[0 .. i + 1]);
        if (st == .complete) break;
    }
    try testing.expectEqual(Status.complete, try p.parse(raw));
    try testing.expectEqual(oneshot.headerEnd(), p.headerEnd());
}

test "minimal GET" {
    const raw = "GET /api/v1/health HTTP/1.1\r\nHost: h\r\n\r\n";
    const p = try parseAll(raw, .{});
    try testing.expectEqual(Method.get, p.req.method);
    try testing.expectEqualStrings("/api/v1/health", p.req.path);
    try testing.expectEqualStrings("", p.req.query);
    try testing.expectEqual(Version.http_1_1, p.req.version);
    try testing.expect(p.req.keep_alive);
    try testing.expectEqual(BodyKind.none, p.req.body_kind);
    try testing.expectEqual(raw.len, p.headerEnd());
}

test "request survives every chunk boundary" {
    try expectSplitInvariant("GET /a?b=1&c=2 HTTP/1.1\r\nHost: h\r\nX-Api-Key: k\r\n\r\n");
    try expectSplitInvariant("POST /up HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello");
    try expectSplitInvariant("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
}

test "path and query are split, and stay percent-encoded" {
    const p = try parseAll("GET /a%2Fb?q=%20x&apikey=abc HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    // Routing must see the encoded form so %2F can never masquerade as a
    // path separator.
    try testing.expectEqualStrings("/a%2Fb", p.req.path);
    try testing.expectEqualStrings("q=%20x&apikey=abc", p.req.query);
    try testing.expectEqualStrings("abc", p.req.queryValue("apikey").?);

    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("/a/b", try p.req.decodePath(&buf));
    try testing.expectEqualStrings(" x", (try p.req.queryValueDecoded(&buf, "q")).?);
}

test "header lookup is case-insensitive, values are OWS-trimmed" {
    const p = try parseAll(
        "GET / HTTP/1.1\r\nHost: h\r\nX-Api-Key:   spaced\t \r\nAccept:*/*\r\n\r\n",
        .{},
    );
    try testing.expectEqualStrings("spaced", p.req.header("x-api-key").?);
    try testing.expectEqualStrings("spaced", p.req.header("X-API-KEY").?);
    try testing.expectEqualStrings("*/*", p.req.header("accept").?);
    try testing.expectEqual(@as(?[]const u8, null), p.req.header("x-missing"));
}

test "empty header value is allowed" {
    const p = try parseAll("GET / HTTP/1.1\r\nHost: h\r\nX-Empty:\r\n\r\n", .{});
    try testing.expectEqualStrings("", p.req.header("x-empty").?);
}

test "leading blank lines are tolerated, a flood is not" {
    const p = try parseAll("\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqual(Method.get, p.req.method);
    try testing.expectError(
        error.MalformedRequestLine,
        parseAll("\r\n\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n", .{}),
    );
}

test "connection close is honoured, 1.0 defaults to close" {
    const a = try parseAll("GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n", .{});
    try testing.expect(!a.req.keep_alive);

    const b = try parseAll("GET / HTTP/1.0\r\n\r\n", .{});
    try testing.expect(!b.req.keep_alive);

    const c = try parseAll("GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n", .{});
    try testing.expect(c.req.keep_alive);

    // Token list, not a whole-value compare.
    const d = try parseAll("GET / HTTP/1.1\r\nHost: h\r\nConnection: TE, Close\r\n\r\n", .{});
    try testing.expect(!d.req.keep_alive);
}

test "expect: 100-continue is recognised, anything else is 417" {
    const a = try parseAll("POST / HTTP/1.1\r\nHost: h\r\nExpect: 100-Continue\r\nContent-Length: 1\r\n\r\n", .{});
    try testing.expect(a.req.expect_continue);
    try testing.expectError(
        error.ExpectationFailed,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nExpect: the-spanish-inquisition\r\n\r\n", .{}),
    );
    try testing.expectEqual(@as(u16, 417), statusFor(error.ExpectationFailed));
}

test "absolute-form and asterisk-form targets" {
    const a = try parseAll("GET http://example.org/x?y=1 HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqualStrings("/x", a.req.path);
    try testing.expectEqualStrings("y=1", a.req.query);

    const b = try parseAll("GET https://example.org HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqualStrings("/", b.req.path);

    const c = try parseAll("OPTIONS * HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqualStrings("*", c.req.path);

    // Asterisk-form is only legal for OPTIONS, and a bare word is not a
    // target at all.
    try testing.expectError(error.BadTarget, parseAll("GET * HTTP/1.1\r\nHost: h\r\n\r\n", .{}));
    try testing.expectError(error.BadTarget, parseAll("GET nonsense HTTP/1.1\r\nHost: h\r\n\r\n", .{}));
}

test "host is mandatory on 1.1 and must not repeat" {
    try testing.expectError(error.MissingHost, parseAll("GET / HTTP/1.1\r\n\r\n", .{}));
    try testing.expectError(
        error.DuplicateHost,
        parseAll("GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", .{}),
    );
    // 1.0 predates Host.
    const p = try parseAll("GET / HTTP/1.0\r\n\r\n", .{});
    try testing.expectEqual(Method.get, p.req.method);
}

test "version handling" {
    try testing.expectError(error.UnsupportedVersion, parseAll("GET / HTTP/2.0\r\nHost: h\r\n\r\n", .{}));
    try testing.expectError(error.UnsupportedVersion, parseAll("GET / HTTP/1.9\r\nHost: h\r\n\r\n", .{}));
    try testing.expectError(error.BadVersion, parseAll("GET / SPDY/3\r\nHost: h\r\n\r\n", .{}));
    try testing.expectEqual(@as(u16, 505), statusFor(error.UnsupportedVersion));
}

test "malformed request lines" {
    try testing.expectError(error.MalformedRequestLine, parseAll("GET\r\n\r\n", .{}));
    try testing.expectError(error.MalformedRequestLine, parseAll("GET /\r\n\r\n", .{}));
    // Two spaces between method and target: not one token boundary, so
    // the "version" ends up holding a space and the line is refused.
    try testing.expectError(error.MalformedRequestLine, parseAll("GET  / HTTP/1.1\r\nHost: h\r\n\r\n", .{}));
    // Trailing junk after the version.
    try testing.expectError(
        error.MalformedRequestLine,
        parseAll("GET / HTTP/1.1 extra\r\nHost: h\r\n\r\n", .{}),
    );
    try testing.expectError(error.BadMethod, parseAll("G(E)T / HTTP/1.1\r\nHost: h\r\n\r\n", .{}));
}

// -- hostile input -----------------------------------------------------

test "hostile: absurdly long request line" {
    const limits = Limits{};
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try buf.appendSlice(testing.allocator, "GET /");
    try buf.appendNTimes(testing.allocator, 'a', limits.max_request_line + 1);
    try buf.appendSlice(testing.allocator, " HTTP/1.1\r\nHost: h\r\n\r\n");

    try testing.expectError(error.RequestLineTooLong, parseAll(buf.items, limits));
    // And it is refused *before* the terminator arrives, so a client
    // cannot pin the connection by never sending one.
    var p: Parser = .{};
    try testing.expectError(error.RequestLineTooLong, p.parse(buf.items[0 .. limits.max_request_line + 10]));
    try testing.expectEqual(@as(u16, 414), statusFor(error.RequestLineTooLong));
}

test "hostile: 10k headers" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try buf.appendSlice(testing.allocator, "GET / HTTP/1.1\r\nHost: h\r\n");
    for (0..10_000) |i| {
        var line: [64]u8 = undefined;
        try buf.appendSlice(testing.allocator, try std.fmt.bufPrint(&line, "X-Pad-{d}: v\r\n", .{i}));
    }
    try buf.appendSlice(testing.allocator, "\r\n");

    // Whichever bound trips first, it must be a refusal and never a
    // write past the header array.
    if (parseAll(buf.items, .{})) |_| {
        return error.TestExpectedRejection;
    } else |e| {
        try testing.expect(e == error.TooManyHeaders or e == error.HeadersTooLarge);
        try testing.expectEqual(@as(u16, 431), statusFor(e));
    }
}

test "hostile: a single enormous header line" {
    const limits = Limits{};
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try buf.appendSlice(testing.allocator, "GET / HTTP/1.1\r\nHost: h\r\nX-Big: ");
    try buf.appendNTimes(testing.allocator, 'z', limits.max_header_line + 1);
    try buf.appendSlice(testing.allocator, "\r\n\r\n");
    try testing.expectError(error.HeaderLineTooLong, parseAll(buf.items, limits));
}

test "hostile: header with no colon" {
    try testing.expectError(
        error.MissingColon,
        parseAll("GET / HTTP/1.1\r\nHost: h\r\nThisIsNotAHeader\r\n\r\n", .{}),
    );
}

test "hostile: space before the colon" {
    // "Host : x" is the classic way to get one parser to see a header
    // that another does not.
    try testing.expectError(
        error.BadHeaderName,
        parseAll("GET / HTTP/1.1\r\nHost: h\r\nContent-Length : 5\r\n\r\n", .{}),
    );
}

test "hostile: obsolete line folding" {
    try testing.expectError(
        error.ObsoleteLineFolding,
        parseAll("GET / HTTP/1.1\r\nHost: h\r\nX-A: one\r\n  two\r\n\r\n", .{}),
    );
}

test "hostile: duplicate Content-Length" {
    try testing.expectError(
        error.DuplicateContentLength,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello", .{}),
    );
    try testing.expectError(
        error.DuplicateContentLength,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nhello", .{}),
    );
    // A comma list in one header is the same trick in another spelling.
    try testing.expectError(
        error.BadContentLength,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5, 5\r\n\r\nhello", .{}),
    );
    try testing.expectError(
        error.BadContentLength,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: +5\r\n\r\nhello", .{}),
    );
    try testing.expectError(
        error.BadContentLength,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0x5\r\n\r\nhello", .{}),
    );
}

test "hostile: Content-Length plus Transfer-Encoding is smuggling" {
    try testing.expectError(
        error.ContentLengthWithTransferEncoding,
        parseAll(
            "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            .{},
        ),
    );
    // Header order must not matter.
    try testing.expectError(
        error.ContentLengthWithTransferEncoding,
        parseAll(
            "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nContent-Length: 6\r\n\r\n0\r\n\r\n",
            .{},
        ),
    );
    // Two TE headers, and a TE we cannot decode.
    try testing.expectError(
        error.DuplicateTransferEncoding,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", .{}),
    );
    try testing.expectError(
        error.UnsupportedTransferEncoding,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", .{}),
    );
    try testing.expectEqual(@as(u16, 501), statusFor(error.UnsupportedTransferEncoding));
    // chunked is a 1.1 feature; a 1.0 message claiming it is desync bait.
    try testing.expectError(
        error.UnsupportedTransferEncoding,
        parseAll("POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", .{}),
    );
}

test "hostile: NUL bytes" {
    try testing.expectError(
        error.BadTarget,
        parseAll("GET /a\x00b HTTP/1.1\r\nHost: h\r\n\r\n", .{}),
    );
    try testing.expectError(
        error.BadHeaderValue,
        parseAll("GET / HTTP/1.1\r\nHost: h\r\nX-A: a\x00b\r\n\r\n", .{}),
    );
    try testing.expectError(
        error.BadHeaderName,
        parseAll("GET / HTTP/1.1\r\nHost: h\r\nX\x00A: b\r\n\r\n", .{}),
    );
    // And percent-encoded NUL does not survive decoding either.
    const p = try parseAll("GET /a%00b HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    var buf: [32]u8 = undefined;
    try testing.expectError(error.EmbeddedNul, p.req.decodePath(&buf));
}

test "hostile: bare CR" {
    try testing.expectError(
        error.BareCr,
        parseAll("GET / HTTP/1.1\r\nHost: h\rX-Evil: 1\r\n\r\n", .{}),
    );
    try testing.expectError(
        error.BareCr,
        parseAll("GET /\r HTTP/1.1\r\nHost: h\r\n\r\n", .{}),
    );
}

test "hostile: LF-only line endings" {
    // Strict CRLF: an LF-only message is rejected rather than
    // interpreted, because a proxy in front of us may frame it
    // differently.
    try testing.expectError(error.BareLf, parseAll("GET / HTTP/1.1\nHost: h\n\n", .{}));
    try testing.expectError(
        error.BareLf,
        parseAll("GET / HTTP/1.1\r\nHost: h\nContent-Length: 5\r\n\r\n", .{}),
    );
}

test "hostile: percent-encoding edge cases in the path" {
    var buf: [64]u8 = undefined;

    const trunc = try parseAll("GET /a%2 HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectError(error.BadPercentEncoding, trunc.req.decodePath(&buf));

    const dangling = try parseAll("GET /a% HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectError(error.BadPercentEncoding, dangling.req.decodePath(&buf));

    const nonhex = try parseAll("GET /a%zz HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectError(error.BadPercentEncoding, nonhex.req.decodePath(&buf));

    const mixed = try parseAll("GET /a%2fb%2Fc HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqualStrings("/a/b/c", try mixed.req.decodePath(&buf));

    // Decoding into too small a buffer reports it rather than truncating.
    var small: [2]u8 = undefined;
    try testing.expectError(error.NoSpaceLeft, mixed.req.decodePath(&small));

    // Double-encoded traversal is caught in encoded form.
    try testing.expect(hasDotSegment("/x/%2e%2e/y"));
    try testing.expect(hasDotSegment("/x/../y"));
    try testing.expect(hasDotSegment("/./y"));
    try testing.expect(!hasDotSegment("/x/y.z"));
    try testing.expect(!hasDotSegment("/assets/index-a1b2c3.js"));
}

test "hostile: Content-Length beyond max_body" {
    const limits = Limits{ .max_body = 1024 };
    try testing.expectError(
        error.BodyTooLarge,
        parseAll("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1025\r\n\r\n", limits),
    );
    try testing.expectEqual(@as(u16, 413), statusFor(error.BodyTooLarge));
}

test "rebase moves every slice onto a copy" {
    var src: [128]u8 = undefined;
    const raw = "GET /p?q=1 HTTP/1.1\r\nHost: h\r\nX-A: v\r\n\r\n";
    @memcpy(src[0..raw.len], raw);

    var p: Parser = .{};
    _ = try p.parse(src[0..raw.len]);

    var dst: [128]u8 = undefined;
    @memcpy(dst[0..raw.len], src[0..raw.len]);
    p.req.rebase(src[0..raw.len], dst[0..raw.len]);

    // Same content, and now provably pointing at the copy — so the
    // original buffer is free for the body.
    try testing.expectEqualStrings("/p", p.req.path);
    try testing.expectEqualStrings("q=1", p.req.query);
    try testing.expectEqualStrings("v", p.req.header("x-a").?);
    try testing.expect(@intFromPtr(p.req.path.ptr) >= @intFromPtr(&dst));
    try testing.expect(@intFromPtr(p.req.path.ptr) < @intFromPtr(&dst) + dst.len);

    @memset(src[0..raw.len], 0xAA);
    try testing.expectEqualStrings("/p", p.req.path);
    try testing.expectEqualStrings("v", p.req.header("x-a").?);
}

test "cookie parsing" {
    const p = try parseAll(
        "GET / HTTP/1.1\r\nHost: h\r\nCookie: a=1; hoardarr_session=tok3n; b=\"quoted\"\r\n\r\n",
        .{},
    );
    try testing.expectEqualStrings("tok3n", p.req.cookie("hoardarr_session").?);
    try testing.expectEqualStrings("1", p.req.cookie("a").?);
    try testing.expectEqualStrings("quoted", p.req.cookie("b").?);
    try testing.expectEqual(@as(?[]const u8, null), p.req.cookie("nope"));

    const none = try parseAll("GET / HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqual(@as(?[]const u8, null), none.req.cookie("hoardarr_session"));
}

test "query iteration handles empties and repeats" {
    const p = try parseAll("GET /?a=1&&b&c=&d=2 HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    try testing.expectEqualStrings("1", p.req.queryValue("a").?);
    try testing.expectEqualStrings("", p.req.queryValue("b").?);
    try testing.expectEqualStrings("", p.req.queryValue("c").?);
    try testing.expectEqualStrings("2", p.req.queryValue("d").?);
    try testing.expectEqual(@as(?[]const u8, null), p.req.queryValue("e"));
}

test "keep-alive reuses the parser for a pipelined request" {
    const first = "GET /a HTTP/1.1\r\nHost: h\r\n\r\n";
    const second = "GET /b HTTP/1.1\r\nHost: h\r\n\r\n";
    var buf: [256]u8 = undefined;
    @memcpy(buf[0..first.len], first);
    @memcpy(buf[first.len..][0..second.len], second);
    const both = buf[0 .. first.len + second.len];

    var p: Parser = .{};
    try testing.expectEqual(Status.complete, try p.parse(both));
    try testing.expectEqualStrings("/a", p.req.path);
    try testing.expectEqual(first.len, p.headerEnd());

    // The server compacts the leftover to the front, then resets.
    std.mem.copyForwards(u8, buf[0..second.len], both[first.len..]);
    p.reset();
    try testing.expectEqual(Status.complete, try p.parse(buf[0..second.len]));
    try testing.expectEqualStrings("/b", p.req.path);
}

// -- body decoding -----------------------------------------------------

/// Drive a decoder over `wire` in fixed-size slices, returning the
/// decoded body. Chunking the input is the point: the decoder must not
/// care where the boundaries fall.
fn decodeBody(
    gpa: std.mem.Allocator,
    req: *const Request,
    limits: Limits,
    wire: []const u8,
    step: usize,
) !std.ArrayList(u8) {
    var dec = BodyDecoder.init(req, limits);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);

    var fed: usize = 0;
    while (fed < wire.len) {
        const end = @min(fed + step, wire.len);
        var window = wire[fed..end];
        while (window.len > 0) {
            const r = try dec.feed(window);
            try out.appendSlice(gpa, r.data);
            window = window[r.consumed..];
            if (r.done) return out;
            if (r.consumed == 0) break; // needs more input
        }
        fed = end;
    }
    if (!dec.isDone()) return error.Incomplete;
    return out;
}

test "identity body of a declared length" {
    const gpa = testing.allocator;
    const p = try parseAll("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\n", .{});
    try testing.expectEqual(BodyKind.length, p.req.body_kind);
    try testing.expectEqual(@as(u64, 11), p.req.content_length);

    for ([_]usize{ 1, 2, 3, 5, 11, 64 }) |step| {
        var body = try decodeBody(gpa, &p.req, .{}, "hello world", step);
        defer body.deinit(gpa);
        try testing.expectEqualStrings("hello world", body.items);
    }
}

test "chunked body across every step size" {
    const gpa = testing.allocator;
    const p = try parseAll("POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n", .{});
    try testing.expectEqual(BodyKind.chunked, p.req.body_kind);

    const wire = "5\r\nhello\r\n1\r\n \r\n6\r\nworld!\r\n0\r\n\r\n";
    for ([_]usize{ 1, 2, 3, 7, 13, 1024 }) |step| {
        var body = try decodeBody(gpa, &p.req, .{}, wire, step);
        defer body.deinit(gpa);
        try testing.expectEqualStrings("hello world!", body.items);
    }
}

test "chunked extensions and trailers are ignored" {
    const gpa = testing.allocator;
    const p = try parseAll("POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n", .{});
    const wire = "4;name=value\r\nabcd\r\n0\r\nX-Trailer: t\r\n\r\n";
    var body = try decodeBody(gpa, &p.req, .{}, wire, 3);
    defer body.deinit(gpa);
    try testing.expectEqualStrings("abcd", body.items);
}

test "chunked framing violations" {
    const gpa = testing.allocator;
    const p = try parseAll("POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n", .{});

    // Non-hex size.
    try testing.expectError(
        error.BadChunkSize,
        decodeBody(gpa, &p.req, .{}, "zz\r\nabcd\r\n0\r\n\r\n", 4),
    );
    // Chunk data not followed by CRLF.
    try testing.expectError(
        error.BadChunkTerminator,
        decodeBody(gpa, &p.req, .{}, "4\r\nabcdXX0\r\n\r\n", 4),
    );
    // LF-only chunk framing — the classic chunked desync.
    try testing.expectError(
        error.BadChunkTerminator,
        decodeBody(gpa, &p.req, .{}, "4\nabcd\n0\n\n", 4),
    );
    // A size line that never ends.
    try testing.expectError(
        error.BadChunkSize,
        decodeBody(gpa, &p.req, .{}, "1111111111111111111111111111111111111111111111\r\n", 8),
    );
    // Absurd single chunk.
    try testing.expectError(
        error.ChunkTooLarge,
        decodeBody(gpa, &p.req, .{ .max_chunk_size = 16 }, "FF\r\n", 4),
    );
    // Chunks that sum past max_body.
    try testing.expectError(
        error.BodyTooLarge,
        decodeBody(gpa, &p.req, .{ .max_body = 8 }, "8\r\naaaaaaaa\r\n8\r\nbbbbbbbb\r\n0\r\n\r\n", 4),
    );
}

test "a body that never arrives leaves the decoder unfinished" {
    const gpa = testing.allocator;
    const p = try parseAll("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 10\r\n\r\n", .{});
    // Two bytes of a promised ten. The decoder reports "not done" rather
    // than inventing an end — the server's body timeout is what closes
    // this connection.
    try testing.expectError(error.Incomplete, decodeBody(gpa, &p.req, .{}, "ab", 1));
}

test "no body means the decoder is immediately done" {
    const p = try parseAll("GET / HTTP/1.1\r\nHost: h\r\n\r\n", .{});
    var dec = BodyDecoder.init(&p.req, .{});
    try testing.expect(dec.isDone());
    const r = try dec.feed("leftover");
    try testing.expectEqual(@as(usize, 0), r.consumed);
    try testing.expect(r.done);
}
