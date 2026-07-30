//! HTTP/1.1 response building.
//!
//! ## Why this looks the way it does
//!
//! The shape of this API is dictated by Server-Sent Events. The UI keeps
//! one `/api/v1/events` connection open for as long as the tab is open,
//! and the hub pushes queue updates into it as they happen — for hours,
//! with no idea in advance how many bytes that will be. So a response
//! cannot be "build it, then send it":
//!
//!   * **Nothing buffers the body.** Every `write` goes to the sink
//!     immediately. The only buffer in here is the few KiB of staged
//!     *headers*, which are bounded and sent before the first body byte.
//!   * **The body length may be unknown.** `beginChunked` sends the head
//!     with `Transfer-Encoding: chunked` and then each `write` is one
//!     chunk, framed and flushed on the spot. The response stays open
//!     until `finish`, which may be never.
//!   * **Backpressure is visible.** `pending()` reports bytes the kernel
//!     has not taken yet. A producer that outruns a client can consult it
//!     and drop events instead of growing the socket's queue without
//!     bound — the socket layer caps that queue and errors past it, and
//!     an SSE hub should never get near the cap.
//!
//! ## Sink
//!
//! Output goes through `Sink`, a two-function vtable, so nothing in here
//! knows about sockets or the reactor and every test runs against an
//! in-memory buffer with zero I/O.
//!
//! ## Framing rules enforced here
//!
//!   * A fixed-length response that writes more or fewer bytes than it
//!     declared is a protocol violation, and one that desynchronises a
//!     keep-alive connection. `write` and `finish` both refuse it.
//!   * `HEAD` sends the headers, including `Content-Length`, and swallows
//!     the body. Handlers need no special case.
//!   * 1xx / 204 / 304 must carry no body and no `Content-Length`.
//!   * chunked is HTTP/1.1 only. Asking for a stream on a 1.0 connection
//!     silently degrades to a close-delimited body, which forces
//!     `Connection: close` — that is the only framing 1.0 has for a body
//!     of unknown length.

const std = @import("std");
const request = @import("request.zig");

const Version = request.Version;

/// The largest header block we will emit. 4 KiB is far more than any
/// response here needs (a redirect plus cookies is a few hundred bytes)
/// and keeps `Response` a fixed-size struct with no allocator.
pub const max_header_block: usize = 4 << 10;

/// Payload size up to which chunked framing is coalesced into a single
/// write. SSE messages are well under this, so a queue update costs one
/// syscall rather than three.
const coalesce_limit: usize = 2048;

/// What the sink may report. The server maps socket and allocator
/// failures onto these; nothing else in this file cares which happened.
pub const SinkError = error{
    /// The peer is gone.
    StreamClosed,
    /// The outbound queue is at its cap: a client that has stopped
    /// reading. The caller should give up on this connection.
    QueueFull,
    OutOfMemory,
    IoFailed,
};

pub const Error = SinkError || error{
    /// The staged header block does not fit in `max_header_block`.
    HeaderBufferFull,
    /// Tried to stage a header, or pick a framing, after the head went
    /// out.
    HeadAlreadySent,
    /// A fixed-length body wrote more than it declared, or `finish` came
    /// while it still owed bytes.
    ContentLengthMismatch,
    /// Write or finish on a response that is already done.
    ResponseFinished,
};

// ---------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------

pub const Sink = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Must either take all of `bytes` (queueing what the kernel
        /// won't accept yet) or fail. A partial success has no
        /// representation on purpose: it would put the burden of
        /// remembering the tail on every caller.
        write: *const fn (ctx: *anyopaque, bytes: []const u8) SinkError!void,
        /// Bytes queued but not yet handed to the kernel.
        pending: *const fn (ctx: *anyopaque) usize,
    };

    pub fn write(self: Sink, bytes: []const u8) SinkError!void {
        return self.vtable.write(self.ctx, bytes);
    }

    pub fn pending(self: Sink) usize {
        return self.vtable.pending(self.ctx);
    }
};

// ---------------------------------------------------------------------
// Status codes
// ---------------------------------------------------------------------

/// Reason phrases for the codes this daemon actually emits. Unknown
/// codes get a generic phrase per class rather than a lookup failure —
/// the phrase is decorative, no client parses it.
pub fn reasonPhrase(status: u16) []const u8 {
    return switch (status) {
        100 => "Continue",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Content Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        417 => "Expectation Failed",
        422 => "Unprocessable Content",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        else => switch (status / 100) {
            1 => "Informational",
            2 => "OK",
            3 => "Redirect",
            4 => "Client Error",
            else => "Server Error",
        },
    };
}

/// Statuses that must not carry a body, and must not announce one
/// either. Getting this wrong desynchronises keep-alive: the client would
/// wait for bytes that never come.
pub fn bodyForbidden(status: u16) bool {
    return status == 204 or status == 304 or (status >= 100 and status < 200);
}

// ---------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------

pub const Framing = enum {
    /// No body at all.
    none,
    /// `Content-Length`.
    fixed,
    /// `Transfer-Encoding: chunked`.
    chunked,
    /// Body ends when the connection does. HTTP/1.0's only option for an
    /// unknown length, and therefore always paired with `Connection:
    /// close`.
    close_delimited,
};

pub const Phase = enum {
    /// Headers may still be staged; nothing has gone out.
    staging,
    /// The head is on the wire; body writes are open.
    body,
    /// Done. The server decides whether to keep the connection.
    finished,
};

pub const Response = struct {
    sink: Sink,
    version: Version = .http_1_1,
    /// The request was `HEAD`: announce the length, send no body.
    head_only: bool = false,
    /// Set from the request, then cleared by anything that makes the
    /// connection unusable for another exchange.
    keep_alive: bool = true,

    status: u16 = 200,
    framing: Framing = .none,
    phase: Phase = .staging,
    /// Bytes still owed under `.fixed` framing.
    remaining: u64 = 0,
    /// Body bytes accepted from the handler. Reported in the access log
    /// and used to verify a fixed-length response.
    body_bytes: u64 = 0,

    /// Staged headers, already formatted as `Name: value\r\n` runs. One
    /// buffer rather than a list of pairs: the only operation is "append"
    /// and the only consumer is a single write.
    hbuf: [max_header_block]u8 = undefined,
    hlen: usize = 0,

    pub fn init(sink: Sink, req: *const request.Request) Response {
        return .{
            .sink = sink,
            .version = req.version,
            .head_only = req.method == .head,
            .keep_alive = req.keep_alive,
        };
    }

    /// For tests and for error responses generated before a request could
    /// be parsed at all.
    pub fn initRaw(sink: Sink, version: Version, head_only: bool, keep_alive: bool) Response {
        return .{ .sink = sink, .version = version, .head_only = head_only, .keep_alive = keep_alive };
    }

    pub fn headSent(self: *const Response) bool {
        return self.phase != .staging;
    }

    pub fn isFinished(self: *const Response) bool {
        return self.phase == .finished;
    }

    /// True while the response is open and unbounded — an SSE stream that
    /// the server must not tear down when the handler returns.
    pub fn isStreaming(self: *const Response) bool {
        return self.phase == .body and (self.framing == .chunked or self.framing == .close_delimited);
    }

    pub fn pending(self: *const Response) usize {
        return self.sink.pending();
    }

    // -- headers ------------------------------------------------------

    pub fn setHeader(self: *Response, name: []const u8, value: []const u8) Error!void {
        if (self.phase != .staging) return error.HeadAlreadySent;
        const need = name.len + 2 + value.len + 2;
        if (self.hlen + need > self.hbuf.len) return error.HeaderBufferFull;
        var p = self.hlen;
        @memcpy(self.hbuf[p..][0..name.len], name);
        p += name.len;
        self.hbuf[p] = ':';
        self.hbuf[p + 1] = ' ';
        p += 2;
        @memcpy(self.hbuf[p..][0..value.len], value);
        p += value.len;
        self.hbuf[p] = '\r';
        self.hbuf[p + 1] = '\n';
        self.hlen = p + 2;
    }

    pub fn setHeaderInt(self: *Response, name: []const u8, v: u64) Error!void {
        var tmp: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable;
        return self.setHeader(name, s);
    }

    /// Discard staged headers. Used when a handler fails midway and the
    /// server replaces its half-built response with an error page.
    pub fn resetHeaders(self: *Response) void {
        self.hlen = 0;
    }

    // -- one-shot bodies ----------------------------------------------

    /// Status, `Content-Type`, body, done. The common case, and the only
    /// one a REST handler needs.
    pub fn send(self: *Response, status: u16, content_type: []const u8, body: []const u8) Error!void {
        if (content_type.len > 0) try self.setHeader("Content-Type", content_type);
        try self.beginFixed(status, body.len);
        if (body.len > 0) try self.write(body);
        try self.finish();
    }

    /// A response with no body: 204, a redirect, or a bare error code.
    pub fn sendStatus(self: *Response, status: u16) Error!void {
        try self.beginFixed(status, 0);
        try self.finish();
    }

    /// `Location:` plus an empty body.
    pub fn redirect(self: *Response, status: u16, location: []const u8) Error!void {
        try self.setHeader("Location", location);
        try self.sendStatus(status);
    }

    /// A minimal JSON error document, the same shape the Go middleware
    /// produced: `{"error":"..."}`. The message is escaped, because some
    /// of them quote request-supplied text.
    pub fn sendError(self: *Response, status: u16, message: []const u8) Error!void {
        var buf: [512]u8 = undefined;
        const body = jsonError(&buf, message);
        try self.send(status, "application/json", body);
    }

    // -- framed bodies ------------------------------------------------

    /// Announce a body of exactly `len` bytes and send the head.
    pub fn beginFixed(self: *Response, status: u16, len: u64) Error!void {
        if (self.phase != .staging) return error.HeadAlreadySent;
        self.status = status;
        if (bodyForbidden(status)) {
            self.framing = .none;
            self.remaining = 0;
        } else {
            self.framing = .fixed;
            self.remaining = len;
            try self.setHeaderInt("Content-Length", len);
        }
        try self.sendHead();
    }

    /// Announce a body of unknown length and send the head. This is the
    /// SSE entry point: after this, every `write` is delivered to the
    /// client as it happens, and the response stays open until `finish`.
    ///
    /// On HTTP/1.1 that is `Transfer-Encoding: chunked`, which keeps the
    /// connection reusable afterwards. On 1.0 there is no such framing,
    /// so the body runs to end-of-connection and keep-alive is dropped.
    pub fn beginChunked(self: *Response, status: u16) Error!void {
        if (self.phase != .staging) return error.HeadAlreadySent;
        self.status = status;
        if (bodyForbidden(status)) {
            self.framing = .none;
        } else if (self.version == .http_1_1) {
            self.framing = .chunked;
            try self.setHeader("Transfer-Encoding", "chunked");
        } else {
            self.framing = .close_delimited;
            self.keep_alive = false;
        }
        try self.sendHead();
    }

    /// Everything an SSE endpoint needs: the streaming framing plus the
    /// headers that stop intermediaries from buffering or caching it.
    /// `X-Accel-Buffering` is nginx-specific and harmless elsewhere.
    pub fn beginEventStream(self: *Response) Error!void {
        try self.setHeader("Content-Type", "text/event-stream");
        try self.setHeader("Cache-Control", "no-cache");
        try self.setHeader("X-Accel-Buffering", "no");
        try self.beginChunked(200);
    }

    /// Append body bytes. Under chunked framing this emits one chunk and
    /// flushes it, so a caller writing one SSE message per call gets one
    /// message on the wire per call — no explicit flush, because there is
    /// nothing held back to flush.
    pub fn write(self: *Response, bytes: []const u8) Error!void {
        switch (self.phase) {
            .staging => {
                // A handler that writes without choosing a framing wants
                // a stream; that is the only framing that can accept an
                // unknown number of further writes.
                try self.beginChunked(200);
            },
            .finished => return error.ResponseFinished,
            .body => {},
        }
        if (bytes.len == 0) return;

        switch (self.framing) {
            .none => return error.ContentLengthMismatch,
            .fixed => {
                if (bytes.len > self.remaining) return error.ContentLengthMismatch;
                self.remaining -= bytes.len;
                self.body_bytes += bytes.len;
                // HEAD announced the length and must send nothing.
                if (!self.head_only) try self.sink.write(bytes);
            },
            .close_delimited => {
                self.body_bytes += bytes.len;
                if (!self.head_only) try self.sink.write(bytes);
            },
            .chunked => {
                self.body_bytes += bytes.len;
                if (self.head_only) return;
                try self.writeChunk(bytes);
            },
        }
    }

    /// Complete the response. Idempotent-ish: calling it twice reports
    /// `ResponseFinished` rather than emitting a second terminator.
    pub fn finish(self: *Response) Error!void {
        switch (self.phase) {
            .finished => return error.ResponseFinished,
            .staging => {
                // A handler that returned without writing anything still
                // owes the client a response.
                try self.beginFixed(200, 0);
            },
            .body => {},
        }
        switch (self.framing) {
            .none => {},
            .fixed => if (self.remaining != 0) {
                // We already sent a Content-Length we cannot honour, so
                // the connection is unusable: the client would read the
                // next response as the tail of this body.
                self.keep_alive = false;
                self.phase = .finished;
                return error.ContentLengthMismatch;
            },
            .chunked => if (!self.head_only) try self.sink.write("0\r\n\r\n"),
            .close_delimited => self.keep_alive = false,
        }
        self.phase = .finished;
    }

    /// `HTTP/1.1 100 Continue`, sent before reading a body from a client
    /// that asked to be told first. Deliberately does not touch phase or
    /// staged headers — an interim response is not the response.
    pub fn sendContinue(self: *Response) Error!void {
        if (self.phase != .staging) return error.HeadAlreadySent;
        try self.sink.write("HTTP/1.1 100 Continue\r\n\r\n");
    }

    // -- internals ----------------------------------------------------

    fn sendHead(self: *Response) Error!void {
        var line: [64]u8 = undefined;
        const status_line = std.fmt.bufPrint(&line, "{s} {d} {s}\r\n", .{
            self.version.text(),
            self.status,
            reasonPhrase(self.status),
        }) catch unreachable;

        // The connection header is decided last, because framing choices
        // above may have cleared keep_alive.
        const conn: []const u8 = if (!self.keep_alive)
            "Connection: close\r\n"
        else if (self.version == .http_1_0)
            // 1.0 needs the explicit opt-in echoed back.
            "Connection: keep-alive\r\n"
        else
            "";

        // One write for the whole head: the status line, the staged
        // headers and the terminator. Three writes would be three
        // syscalls and would let a slow client see a torn head.
        var buf: [max_header_block + 128]u8 = undefined;
        var n: usize = 0;
        @memcpy(buf[n..][0..status_line.len], status_line);
        n += status_line.len;
        @memcpy(buf[n..][0..conn.len], conn);
        n += conn.len;
        @memcpy(buf[n..][0..self.hlen], self.hbuf[0..self.hlen]);
        n += self.hlen;
        buf[n] = '\r';
        buf[n + 1] = '\n';
        n += 2;

        self.phase = .body;
        try self.sink.write(buf[0..n]);
    }

    fn writeChunk(self: *Response, bytes: []const u8) Error!void {
        var head: [24]u8 = undefined;
        const hdr = std.fmt.bufPrint(&head, "{x}\r\n", .{bytes.len}) catch unreachable;

        if (bytes.len <= coalesce_limit) {
            var buf: [coalesce_limit + 32]u8 = undefined;
            var n: usize = 0;
            @memcpy(buf[n..][0..hdr.len], hdr);
            n += hdr.len;
            @memcpy(buf[n..][0..bytes.len], bytes);
            n += bytes.len;
            buf[n] = '\r';
            buf[n + 1] = '\n';
            n += 2;
            return self.sink.write(buf[0..n]);
        }
        // Big payloads (file bodies) are not worth a copy; the extra two
        // writes are noise next to the data itself.
        try self.sink.write(hdr);
        try self.sink.write(bytes);
        try self.sink.write("\r\n");
    }
};

/// `{"error":"<escaped>"}` in a caller-supplied buffer. Escaping is
/// minimal but total: quotes, backslashes and every control byte become
/// escapes, so no message can break out of the string.
pub fn jsonError(out: []u8, message: []const u8) []const u8 {
    const prefix = "{\"error\":\"";
    const suffix = "\"}";
    var n: usize = 0;
    @memcpy(out[0..prefix.len], prefix);
    n = prefix.len;
    for (message) |c| {
        // Worst case per byte is a 6-byte \u escape, plus the suffix.
        if (n + 6 + suffix.len > out.len) break;
        switch (c) {
            '"' => {
                @memcpy(out[n..][0..2], "\\\"");
                n += 2;
            },
            '\\' => {
                @memcpy(out[n..][0..2], "\\\\");
                n += 2;
            },
            '\n' => {
                @memcpy(out[n..][0..2], "\\n");
                n += 2;
            },
            '\r' => {
                @memcpy(out[n..][0..2], "\\r");
                n += 2;
            },
            '\t' => {
                @memcpy(out[n..][0..2], "\\t");
                n += 2;
            },
            else => {
                if (c < 0x20 or c == 0x7F) {
                    const hex = "0123456789abcdef";
                    @memcpy(out[n..][0..6], &[_]u8{ '\\', 'u', '0', '0', hex[c >> 4], hex[c & 0xF] });
                    n += 6;
                } else {
                    out[n] = c;
                    n += 1;
                }
            },
        }
    }
    @memcpy(out[n..][0..suffix.len], suffix);
    return out[0 .. n + suffix.len];
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// An in-memory sink. Everything in this file is testable with zero I/O,
/// which is the point of the vtable.
const MemSink = struct {
    gpa: std.mem.Allocator,
    buf: std.ArrayList(u8) = .empty,
    /// Fail every write once this many bytes have been accepted. 0 means
    /// never fail.
    fail_after: usize = 0,
    /// Reported by `pending`, to exercise backpressure-aware callers.
    fake_pending: usize = 0,

    fn deinit(self: *MemSink) void {
        self.buf.deinit(self.gpa);
    }

    fn sink(self: *MemSink) Sink {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable: Sink.VTable = .{ .write = writeFn, .pending = pendingFn };

    fn writeFn(ctx: *anyopaque, bytes: []const u8) SinkError!void {
        const self: *MemSink = @ptrCast(@alignCast(ctx));
        if (self.fail_after != 0 and self.buf.items.len + bytes.len > self.fail_after) {
            return error.StreamClosed;
        }
        self.buf.appendSlice(self.gpa, bytes) catch return error.OutOfMemory;
    }

    fn pendingFn(ctx: *anyopaque) usize {
        const self: *MemSink = @ptrCast(@alignCast(ctx));
        return self.fake_pending;
    }
};

fn get11(target: []const u8) request.Parser {
    var p: request.Parser = .{};
    var buf: [256]u8 = undefined;
    const raw = std.fmt.bufPrint(&buf, "GET {s} HTTP/1.1\r\nHost: h\r\n\r\n", .{target}) catch unreachable;
    _ = p.parse(raw) catch unreachable;
    // The parsed slices point into `buf`, which dies with this function.
    // Only `method`, `version` and `keep_alive` are read by the response
    // layer, so that is fine here and nowhere else.
    p.req.target = "";
    p.req.path = "";
    p.req.query = "";
    p.req.headers.len = 0;
    p.req.method_raw = "";
    return p;
}

test "fixed-length response" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);

    try res.send(200, "application/json", "{\"ok\":true}");

    try testing.expectEqualStrings(
        "HTTP/1.1 200 OK\r\n" ++
            "Content-Type: application/json\r\n" ++
            "Content-Length: 11\r\n" ++
            "\r\n" ++
            "{\"ok\":true}",
        ms.buf.items,
    );
    try testing.expect(res.isFinished());
    try testing.expect(res.keep_alive);
    try testing.expectEqual(@as(u64, 11), res.body_bytes);
}

test "connection: close is echoed" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, false);
    try res.send(200, "text/plain", "hi");
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "Connection: close\r\n") != null);
}

test "http/1.0 keep-alive is echoed explicitly" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_0, false, true);
    try res.send(200, "text/plain", "hi");
    try testing.expect(std.mem.startsWith(u8, ms.buf.items, "HTTP/1.0 200 OK\r\n"));
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "Connection: keep-alive\r\n") != null);
}

test "HEAD announces the length and sends no body" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, true, true);

    try res.send(200, "text/plain", "twelve bytes");

    try testing.expectEqualStrings(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\n",
        ms.buf.items,
    );
    // The handler's bytes were counted even though none were sent, so a
    // fixed-length HEAD still validates.
    try testing.expectEqual(@as(u64, 12), res.body_bytes);
}

test "204 carries no body and no Content-Length" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.sendStatus(204);
    try testing.expectEqualStrings("HTTP/1.1 204 No Content\r\n\r\n", ms.buf.items);
    // And a body write against it is refused rather than silently
    // desynchronising the connection.
    try testing.expectError(error.ResponseFinished, res.write("x"));
}

test "chunked streaming writes one chunk per call" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);

    try res.setHeader("Content-Type", "text/plain");
    try res.beginChunked(200);
    try testing.expect(res.isStreaming());

    // Each write must be on the wire before the next one is made: that is
    // what makes SSE work.
    const head_len = ms.buf.items.len;
    try res.write("abc");
    try testing.expectEqualStrings("3\r\nabc\r\n", ms.buf.items[head_len..]);
    const after_first = ms.buf.items.len;
    try res.write("defgh");
    try testing.expectEqualStrings("5\r\ndefgh\r\n", ms.buf.items[after_first..]);

    try res.finish();
    try testing.expect(std.mem.endsWith(u8, ms.buf.items, "0\r\n\r\n"));
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "Transfer-Encoding: chunked\r\n") != null);
    // Keep-alive survives a chunked response — the terminator delimits it.
    try testing.expect(res.keep_alive);
    try testing.expect(!res.isStreaming());
}

test "an SSE stream stays open across many writes and buffers nothing" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);

    try res.beginEventStream();
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "Content-Type: text/event-stream\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "X-Accel-Buffering: no\r\n") != null);

    // 10k events, and the response object never grows: its only buffer is
    // the staged header block, which was consumed by the head.
    var i: usize = 0;
    while (i < 10_000) : (i += 1) {
        var msg: [128]u8 = undefined;
        const text = try std.fmt.bufPrint(&msg, "event: download.segment.completed\ndata: {{\"n\":{d}}}\n\n", .{i});
        try res.write(text);
        try testing.expect(res.isStreaming());
        try testing.expect(!res.isFinished());
    }
    try testing.expect(res.body_bytes > 400_000);

    const streamed = res.body_bytes;
    try res.finish();

    // The framing is intact: reassembling the chunks yields exactly the
    // bytes the handler wrote.
    var decoded: std.ArrayList(u8) = .empty;
    defer decoded.deinit(testing.allocator);
    try dechunk(testing.allocator, &decoded, bodyOf(ms.buf.items));
    try testing.expectEqual(streamed, decoded.items.len);
    try testing.expect(std.mem.startsWith(u8, decoded.items, "event: download.segment.completed\n"));
}

test "large chunks skip the coalescing copy but keep the framing" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.beginChunked(200);

    const big = try testing.allocator.alloc(u8, coalesce_limit * 3 + 7);
    defer testing.allocator.free(big);
    @memset(big, 'Q');
    try res.write(big);
    try res.write("tail");
    try res.finish();

    var decoded: std.ArrayList(u8) = .empty;
    defer decoded.deinit(testing.allocator);
    try dechunk(testing.allocator, &decoded, bodyOf(ms.buf.items));
    try testing.expectEqual(big.len + 4, decoded.items.len);
    try testing.expectEqualStrings("tail", decoded.items[big.len..]);
}

test "http/1.0 stream degrades to close-delimited" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_0, false, true);

    try res.beginChunked(200);
    try testing.expectEqual(Framing.close_delimited, res.framing);
    // No framing means the only way to end the body is to close, so the
    // connection cannot be reused.
    try testing.expect(!res.keep_alive);
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "Transfer-Encoding") == null);
    try testing.expect(std.mem.indexOf(u8, ms.buf.items, "Connection: close\r\n") != null);

    try res.write("raw bytes");
    try res.finish();
    try testing.expect(std.mem.endsWith(u8, ms.buf.items, "raw bytes"));
}

test "a fixed body that overruns or underruns is refused" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();

    var over = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try over.beginFixed(200, 3);
    try testing.expectError(error.ContentLengthMismatch, over.write("four"));

    var ms2 = MemSink{ .gpa = testing.allocator };
    defer ms2.deinit();
    var under = Response.initRaw(ms2.sink(), .http_1_1, false, true);
    try under.beginFixed(200, 10);
    try under.write("short");
    try testing.expectError(error.ContentLengthMismatch, under.finish());
    // The client is now waiting for five bytes that will never come, so
    // the connection must not be reused.
    try testing.expect(!under.keep_alive);
}

test "headers cannot be staged after the head went out" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.beginFixed(200, 0);
    try testing.expectError(error.HeadAlreadySent, res.setHeader("X-Late", "1"));
    try testing.expectError(error.HeadAlreadySent, res.beginChunked(200));
    try testing.expectError(error.HeadAlreadySent, res.sendContinue());
}

test "the staged header block is bounded" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);

    const value = [_]u8{'v'} ** 512;
    var n: usize = 0;
    while (n < 32) : (n += 1) {
        res.setHeader("X-Pad", &value) catch |err| {
            try testing.expectEqual(error.HeaderBufferFull, err);
            break;
        };
    }
    try testing.expect(n < 32);
    try testing.expect(res.hlen <= max_header_block);
}

test "sink failures propagate instead of being swallowed" {
    var ms = MemSink{ .gpa = testing.allocator, .fail_after = 10 };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try testing.expectError(error.StreamClosed, res.send(200, "text/plain", "hello"));
}

test "backpressure is visible to the producer" {
    var ms = MemSink{ .gpa = testing.allocator, .fake_pending = 1 << 20 };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.beginEventStream();
    // An SSE hub consults this and drops events rather than letting the
    // socket queue grow to its cap.
    try testing.expectEqual(@as(usize, 1 << 20), res.pending());
}

test "an unwritten response still answers" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.finish();
    try testing.expectEqualStrings("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", ms.buf.items);
    try testing.expectError(error.ResponseFinished, res.finish());
}

test "a write with no framing chosen becomes a stream" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.write("hello");
    try testing.expectEqual(Framing.chunked, res.framing);
    try res.finish();
}

test "100-continue is an interim response, not the response" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.sendContinue();
    try res.send(200, "text/plain", "ok");
    try testing.expectEqualStrings(
        "HTTP/1.1 100 Continue\r\n\r\n" ++
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nok",
        ms.buf.items,
    );
}

test "json error escaping is total" {
    var buf: [512]u8 = undefined;
    try testing.expectEqualStrings(
        "{\"error\":\"authentication required\"}",
        jsonError(&buf, "authentication required"),
    );
    try testing.expectEqualStrings(
        "{\"error\":\"a\\\"b\\\\c\\u0000d\\ne\"}",
        jsonError(&buf, "a\"b\\c\x00d\ne"),
    );
    // Oversized messages are truncated, never overflowed, and the
    // document stays well-formed.
    var small: [24]u8 = undefined;
    const out = jsonError(&small, "x" ** 100);
    try testing.expect(out.len <= small.len);
    try testing.expect(std.mem.endsWith(u8, out, "\"}"));
}

test "redirect" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    var res = Response.initRaw(ms.sink(), .http_1_1, false, true);
    try res.redirect(301, "/hoardarr/");
    try testing.expectEqualStrings(
        "HTTP/1.1 301 Moved Permanently\r\nLocation: /hoardarr/\r\nContent-Length: 0\r\n\r\n",
        ms.buf.items,
    );
}

test "init from a parsed request carries method and keep-alive across" {
    var ms = MemSink{ .gpa = testing.allocator };
    defer ms.deinit();
    const p = get11("/x");
    var res = Response.init(ms.sink(), &p.req);
    try testing.expect(!res.head_only);
    try testing.expect(res.keep_alive);
    try res.send(200, "text/plain", "ok");
}

// -- helpers -----------------------------------------------------------

/// Everything after the first blank line.
fn bodyOf(raw: []const u8) []const u8 {
    const sep = std.mem.indexOf(u8, raw, "\r\n\r\n") orelse return raw[raw.len..];
    return raw[sep + 4 ..];
}

/// Reassemble a chunked body. Deliberately independent of
/// `request.BodyDecoder` so a shared bug cannot hide behind itself.
fn dechunk(gpa: std.mem.Allocator, out: *std.ArrayList(u8), body: []const u8) !void {
    var rest = body;
    while (true) {
        const eol = std.mem.indexOf(u8, rest, "\r\n") orelse return error.Truncated;
        const size = try std.fmt.parseInt(usize, rest[0..eol], 16);
        rest = rest[eol + 2 ..];
        if (size == 0) return;
        if (rest.len < size + 2) return error.Truncated;
        try out.appendSlice(gpa, rest[0..size]);
        if (!std.mem.eql(u8, rest[size..][0..2], "\r\n")) return error.BadTerminator;
        rest = rest[size + 2 ..];
    }
}
