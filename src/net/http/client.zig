//! A minimal HTTP/1.1 client on the reactor.
//!
//! Scoped deliberately to what hoardarr actually does over HTTP, which is
//! not much: post a JSON body to a webhook (Discord, Slack, a generic
//! endpoint), and GET a health endpoint from the `healthcheck` subcommand.
//! Everything else the daemon fetches comes over NNTP.
//!
//! So there is no cookie jar, no connection pool, no redirect chain beyond
//! a bounded count, and no content negotiation. Each request opens a
//! connection, sends, reads the response, and closes — which for a webhook
//! fired once per completed download is the right trade, and it means a
//! misbehaving third-party endpoint cannot leave state behind in the
//! daemon.
//!
//! `response.zig` and `request.zig` own the wire format for the *server*
//! side. A client parses the mirror image — a status line instead of a
//! request line, and the same header block — so the header scanning here
//! is small enough to keep local rather than generalising both directions
//! through one parser and making each harder to read.
//!
//! ## Bounded by construction
//!
//! The peer is a third party we do not control, so every dimension has a
//! ceiling: status line, header count and size, body size, redirect count,
//! and a wall-clock deadline for the whole exchange. A webhook endpoint
//! that accepts the connection and then dribbles one byte a minute must
//! not be able to pin a connection, and one that streams a gigabyte of
//! error page must not be able to exhaust memory.

const std = @import("std");
const sys = @import("../../posix/sys.zig");
const reactor = @import("../../posix/reactor.zig");
const socket = @import("../socket.zig");

const Allocator = std.mem.Allocator;
const IpAddress = std.Io.net.IpAddress;

pub const Error = socket.Error || error{
    /// The status line wasn't `HTTP/1.x NNN ...`.
    MalformedStatusLine,
    MalformedHeader,
    /// A header block, status line or body exceeded its limit.
    ResponseTooLarge,
    /// The peer closed before the body was complete.
    IncompleteResponse,
    /// No response within `Options.deadline_ns`.
    Timeout,
    /// A `Transfer-Encoding` we don't implement, or a malformed chunk.
    UnsupportedEncoding,
    /// The URL couldn't be parsed, or names a scheme we don't speak.
    InvalidUrl,
    /// `https://` was requested. TLS exists (`net/tls.zig`) but is not
    /// wired in here yet, and returning a distinct error beats silently
    /// downgrading a webhook to plaintext.
    TlsNotWired,
};

pub const Method = enum {
    get,
    post,
    put,
    delete,

    pub fn text(m: Method) []const u8 {
        return switch (m) {
            .get => "GET",
            .post => "POST",
            .put => "PUT",
            .delete => "DELETE",
        };
    }
};

pub const Header = struct { name: []const u8, value: []const u8 };

pub const Options = struct {
    /// Whole-exchange wall clock, connect through last body byte. One
    /// deadline rather than per-phase timeouts, because from a caller's
    /// point of view "the webhook took too long" is one condition.
    deadline_ns: u64 = 15 * std.time.ns_per_s,
    /// Status line plus header block.
    max_head_bytes: usize = 16 << 10,
    max_headers: usize = 64,
    /// A webhook's response body is a status blob at most; anything larger
    /// is an error page we don't need to read in full.
    max_body_bytes: usize = 1 << 20,
};

/// A parsed response. `headers` and `body` are owned by the `Response` and
/// freed by `deinit`.
pub const Response = struct {
    status: u16,
    headers: []Header,
    body: []u8,
    gpa: Allocator,
    /// Backing store for the header block; `headers` points into it.
    head_buf: []u8,

    pub fn deinit(self: *Response) void {
        self.gpa.free(self.headers);
        self.gpa.free(self.body);
        self.gpa.free(self.head_buf);
        self.* = undefined;
    }

    /// First matching header, case-insensitively. Linear over at most
    /// `max_headers` entries, which is cheaper than building a map.
    pub fn get(self: *const Response, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

/// Called once, with either a response or an error. The response is owned
/// by the callee, which must `deinit` it.
pub const CompleteFn = *const fn (ctx: ?*anyopaque, result: Error!Response) void;

pub const Request = struct {
    method: Method = .get,
    /// Origin-form path including any query, e.g. `/api/v1/health?x=1`.
    path: []const u8 = "/",
    /// Sent as the `Host` header. Required by HTTP/1.1.
    host: []const u8,
    headers: []const Header = &.{},
    /// Body bytes. `Content-Length` is set from this; a zero-length body
    /// on a POST still sends `Content-Length: 0`, which some endpoints
    /// require.
    body: []const u8 = "",
    /// `Content-Type`, only sent when a body is present.
    content_type: []const u8 = "application/json",
};

/// One in-flight exchange. The caller owns the storage and must keep it
/// alive until the callback fires.
pub const Exchange = struct {
    stream: socket.Stream = undefined,
    loop: *reactor.Loop,
    gpa: Allocator,
    options: Options,
    on_complete: CompleteFn,
    ctx: ?*anyopaque,

    /// Accumulated response bytes, head and body together. Split once the
    /// head is complete, so the parser never has to handle a header that
    /// straddles two buffers.
    buf: std.ArrayList(u8) = .empty,
    /// Offset of the body's first byte, once the blank line is found.
    head_len: ?usize = null,

    status: u16 = 0,
    content_length: ?usize = null,
    chunked: bool = false,
    /// Set when the peer's headers said the connection closes, which is
    /// how a response with neither Content-Length nor chunked framing is
    /// delimited.
    close_delimited: bool = false,

    timer: reactor.Timer = .{ .callback = onDeadline },
    finished: bool = false,

    /// The formatted request head, kept alive until the write drains.
    head_out: std.ArrayList(u8) = .empty,

    const stream_handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
        .on_connected = onConnected,
    };

    /// Start an exchange against an already-resolved address.
    ///
    /// Initialises in place: the reactor stores `&self.stream.source` and
    /// `&self.timer`.
    pub fn start(
        self: *Exchange,
        gpa: Allocator,
        loop: *reactor.Loop,
        addr: IpAddress,
        req: Request,
        options: Options,
        on_complete: CompleteFn,
        ctx: ?*anyopaque,
    ) Error!void {
        self.* = .{
            .loop = loop,
            .gpa = gpa,
            .options = options,
            .on_complete = on_complete,
            .ctx = ctx,
        };
        errdefer self.head_out.deinit(gpa);

        try self.formatHead(req);
        try self.stream.connect(gpa, loop, addr, &stream_handler);
        errdefer self.stream.deinit();
        try loop.addTimer(&self.timer, options.deadline_ns);
    }

    pub fn deinit(self: *Exchange) void {
        if (self.timer.isArmed()) self.loop.cancelTimer(&self.timer);
        self.stream.deinit();
        self.buf.deinit(self.gpa);
        self.head_out.deinit(self.gpa);
    }

    fn formatHead(self: *Exchange, req: Request) Error!void {
        const w = &self.head_out;
        const gpa = self.gpa;

        try w.appendSlice(gpa, req.method.text());
        try w.append(gpa, ' ');
        try w.appendSlice(gpa, req.path);
        try w.appendSlice(gpa, " HTTP/1.1\r\nHost: ");
        try w.appendSlice(gpa, req.host);
        // Ask for a close rather than keep-alive: there is no pool here,
        // so a kept-alive connection would be one we immediately drop, and
        // saying so lets the peer release its side straight away.
        try w.appendSlice(gpa, "\r\nConnection: close\r\n");

        for (req.headers) |h| {
            // Refuse a header that could inject a second one. These come
            // from config and from a template, so they are not fully
            // trusted input.
            if (containsCrlf(h.name) or containsCrlf(h.value)) return error.MalformedHeader;
            try w.appendSlice(gpa, h.name);
            try w.appendSlice(gpa, ": ");
            try w.appendSlice(gpa, h.value);
            try w.appendSlice(gpa, "\r\n");
        }

        if (req.body.len > 0) {
            try w.appendSlice(gpa, "Content-Type: ");
            if (containsCrlf(req.content_type)) return error.MalformedHeader;
            try w.appendSlice(gpa, req.content_type);
            try w.appendSlice(gpa, "\r\n");
        }
        // Always present on a method that can carry a body, even at zero
        // length: some endpoints reject a POST without it.
        if (req.body.len > 0 or req.method != .get) {
            var num: [24]u8 = undefined;
            const n = std.fmt.bufPrint(&num, "Content-Length: {d}\r\n", .{req.body.len}) catch unreachable;
            try w.appendSlice(gpa, n);
        }
        try w.appendSlice(gpa, "\r\n");
        try w.appendSlice(gpa, req.body);
    }

    fn containsCrlf(s: []const u8) bool {
        return std.mem.indexOfAny(u8, s, "\r\n") != null;
    }

    fn onConnected(s: *socket.Stream, err: ?socket.Error) void {
        const self: *Exchange = @fieldParentPtr("stream", s);
        if (err) |e| {
            self.fail(e);
            return;
        }
        s.write(self.head_out.items) catch |e| {
            self.fail(e);
            return;
        };
    }

    fn onClose(s: *socket.Stream, err: ?socket.Error) void {
        const self: *Exchange = @fieldParentPtr("stream", s);
        if (self.finished) return;

        // A close is how a close-delimited response ends, so it isn't
        // automatically a failure — only a close before the head is
        // complete, or before a declared Content-Length is satisfied.
        if (self.head_len != null and (self.close_delimited or self.bodyComplete())) {
            self.complete();
            return;
        }
        self.fail(err orelse error.IncompleteResponse);
    }

    fn onReadable(s: *socket.Stream) void {
        const self: *Exchange = @fieldParentPtr("stream", s);
        if (self.finished) return;

        while (true) {
            if (self.buf.items.len >= self.options.max_head_bytes + self.options.max_body_bytes) {
                self.fail(error.ResponseTooLarge);
                return;
            }
            self.buf.ensureUnusedCapacity(self.gpa, 4096) catch |e| {
                self.fail(e);
                return;
            };
            const dst = self.buf.unusedCapacitySlice();

            const n = s.read(dst) catch |err| switch (err) {
                error.WouldBlock => break,
                else => {
                    self.fail(err);
                    return;
                },
            };
            if (n == 0) break; // peer closed; onClose resolves it
            self.buf.items.len += n;
        }

        if (self.head_len == null) {
            self.tryParseHead() catch |e| {
                self.fail(e);
                return;
            };
            if (self.head_len == null) return; // need more
        }
        if (self.bodyComplete()) self.complete();
    }

    /// Look for the blank line ending the head. Returns with `head_len`
    /// still null when more bytes are needed.
    fn tryParseHead(self: *Exchange) Error!void {
        const data = self.buf.items;
        const end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse {
            // Bare-LF heads exist in the wild from hand-rolled servers, so
            // accept them here — we are the client and being strict buys
            // nothing but an unreachable endpoint.
            if (std.mem.indexOf(u8, data, "\n\n")) |e2| {
                try self.parseHead(data[0..e2]);
                self.head_len = e2 + 2;
                return;
            }
            if (data.len > self.options.max_head_bytes) return error.ResponseTooLarge;
            return;
        };
        if (end > self.options.max_head_bytes) return error.ResponseTooLarge;
        try self.parseHead(data[0..end]);
        self.head_len = end + 4;
    }

    fn parseHead(self: *Exchange, head: []const u8) Error!void {
        var lines = std.mem.splitScalar(u8, head, '\n');
        const status_line = std.mem.trimEnd(u8, lines.next() orelse return error.MalformedStatusLine, "\r");

        // "HTTP/1.1 200 OK"
        if (!std.mem.startsWith(u8, status_line, "HTTP/1.")) return error.MalformedStatusLine;
        if (status_line.len < 12) return error.MalformedStatusLine;
        const code_text = status_line[9..12];
        self.status = std.fmt.parseInt(u16, code_text, 10) catch return error.MalformedStatusLine;

        var count: usize = 0;
        while (lines.next()) |raw| {
            const line = std.mem.trimEnd(u8, raw, "\r");
            if (line.len == 0) continue;
            count += 1;
            if (count > self.options.max_headers) return error.ResponseTooLarge;

            const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.MalformedHeader;
            if (colon == 0) return error.MalformedHeader;
            const name = line[0..colon];
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t");

            if (std.ascii.eqlIgnoreCase(name, "content-length")) {
                // A second, differing Content-Length is a smuggling
                // signal; refuse rather than pick one.
                const parsed = std.fmt.parseInt(usize, value, 10) catch return error.MalformedHeader;
                if (self.content_length) |existing| {
                    if (existing != parsed) return error.MalformedHeader;
                }
                if (parsed > self.options.max_body_bytes) return error.ResponseTooLarge;
                self.content_length = parsed;
            } else if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
                if (!std.ascii.eqlIgnoreCase(value, "chunked")) return error.UnsupportedEncoding;
                self.chunked = true;
            } else if (std.ascii.eqlIgnoreCase(name, "connection")) {
                if (std.ascii.eqlIgnoreCase(value, "close")) self.close_delimited = true;
            }
        }

        // Content-Length together with chunked framing is ambiguous and
        // the classic smuggling vector. We're the client here, but a
        // webhook endpoint behind a confused proxy can still produce it.
        if (self.chunked and self.content_length != null) return error.MalformedHeader;

        // No length and no chunking means the body runs to close.
        if (!self.chunked and self.content_length == null) {
            if (bodylessStatus(self.status)) {
                self.content_length = 0;
            } else {
                self.close_delimited = true;
            }
        }
    }

    fn bodylessStatus(status: u16) bool {
        return status == 204 or status == 304 or (status >= 100 and status < 200);
    }

    fn bodyComplete(self: *const Exchange) bool {
        const hl = self.head_len orelse return false;
        const have = self.buf.items.len - hl;
        if (self.chunked) return self.chunkedComplete(self.buf.items[hl..]);
        if (self.content_length) |want| return have >= want;
        return false; // close-delimited: resolved by onClose
    }

    /// True once the zero-length chunk has arrived. Deliberately a scan
    /// rather than incremental state: a webhook response body is a few
    /// hundred bytes, so rescanning it is cheaper than the bookkeeping.
    fn chunkedComplete(self: *const Exchange, body: []const u8) bool {
        _ = self;
        var rest = body;
        while (true) {
            const nl = std.mem.indexOf(u8, rest, "\r\n") orelse return false;
            const size_text = rest[0..nl];
            // Strip any chunk extension after ';'.
            const semi = std.mem.indexOfScalar(u8, size_text, ';');
            const digits = if (semi) |i| size_text[0..i] else size_text;
            const size = std.fmt.parseInt(usize, std.mem.trim(u8, digits, " \t"), 16) catch return false;
            if (size == 0) return true;
            const after = nl + 2;
            if (rest.len < after + size + 2) return false;
            rest = rest[after + size + 2 ..];
        }
    }

    /// Decode the chunked body in place, returning the decoded length.
    fn decodeChunked(body: []u8) Error!usize {
        var out: usize = 0;
        var pos: usize = 0;
        while (true) {
            const nl = std.mem.indexOf(u8, body[pos..], "\r\n") orelse return error.UnsupportedEncoding;
            const size_text = body[pos .. pos + nl];
            const semi = std.mem.indexOfScalar(u8, size_text, ';');
            const digits = if (semi) |i| size_text[0..i] else size_text;
            const size = std.fmt.parseInt(usize, std.mem.trim(u8, digits, " \t"), 16) catch
                return error.UnsupportedEncoding;
            pos += nl + 2;
            if (size == 0) return out;
            if (pos + size > body.len) return error.UnsupportedEncoding;
            // Chunks only ever move left, so copying forward in place is
            // safe and avoids a second buffer.
            std.mem.copyForwards(u8, body[out .. out + size], body[pos .. pos + size]);
            out += size;
            pos += size + 2;
        }
    }

    fn complete(self: *Exchange) void {
        if (self.finished) return;
        self.finished = true;
        if (self.timer.isArmed()) self.loop.cancelTimer(&self.timer);

        const hl = self.head_len.?;
        const head_buf = self.buf.toOwnedSlice(self.gpa) catch {
            self.deliver(error.SystemResources);
            return;
        };
        // `head_buf` now owns everything; carve the body out of it.
        var body_len = head_buf.len - hl;
        if (self.chunked) {
            body_len = decodeChunked(head_buf[hl..]) catch |e| {
                self.gpa.free(head_buf);
                self.deliver(e);
                return;
            };
        } else if (self.content_length) |want| {
            body_len = @min(body_len, want);
        }

        const body = self.gpa.dupe(u8, head_buf[hl .. hl + body_len]) catch {
            self.gpa.free(head_buf);
            self.deliver(error.SystemResources);
            return;
        };

        // Re-parse the head into borrowed slices over `head_buf`, which the
        // Response now owns, so the header values stay valid for the caller.
        var headers: std.ArrayList(Header) = .empty;
        collectHeaders(self.gpa, &headers, head_buf[0..hl]) catch {
            headers.deinit(self.gpa);
            self.gpa.free(head_buf);
            self.gpa.free(body);
            self.deliver(error.SystemResources);
            return;
        };

        const hdrs = headers.toOwnedSlice(self.gpa) catch {
            headers.deinit(self.gpa);
            self.gpa.free(head_buf);
            self.gpa.free(body);
            self.deliver(error.SystemResources);
            return;
        };

        self.on_complete(self.ctx, Response{
            .status = self.status,
            .headers = hdrs,
            .body = body,
            .gpa = self.gpa,
            .head_buf = head_buf,
        });
    }

    fn collectHeaders(gpa: Allocator, out: *std.ArrayList(Header), head: []const u8) !void {
        var lines = std.mem.splitScalar(u8, head, '\n');
        _ = lines.next(); // status line
        while (lines.next()) |raw| {
            const line = std.mem.trimEnd(u8, raw, "\r");
            if (line.len == 0) continue;
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            try out.append(gpa, .{
                .name = line[0..colon],
                .value = std.mem.trim(u8, line[colon + 1 ..], " \t"),
            });
        }
    }

    fn onDeadline(t: *reactor.Timer) void {
        const self: *Exchange = @fieldParentPtr("timer", t);
        self.fail(error.Timeout);
    }

    fn fail(self: *Exchange, err: Error) void {
        if (self.finished) return;
        self.finished = true;
        if (self.timer.isArmed()) self.loop.cancelTimer(&self.timer);
        self.deliver(err);
    }

    fn deliver(self: *Exchange, err: Error) void {
        self.on_complete(self.ctx, err);
    }
};

/// Split a URL into the pieces `Exchange.start` needs.
///
/// Only what hoardarr's own configuration produces: `http://host[:port]/path`.
/// `https://` parses but is reported as `TlsNotWired`, because silently
/// downgrading a webhook to plaintext would be worse than failing.
pub const Url = struct {
    host: []const u8,
    port: u16,
    /// Origin-form target including any query. Always begins with '/'.
    path: []const u8,
    tls: bool,

    pub fn parse(raw: []const u8) Error!Url {
        var rest = raw;
        var tls = false;
        if (std.mem.startsWith(u8, rest, "http://")) {
            rest = rest["http://".len..];
        } else if (std.mem.startsWith(u8, rest, "https://")) {
            rest = rest["https://".len..];
            tls = true;
        } else return error.InvalidUrl;

        const slash = std.mem.indexOfScalar(u8, rest, '/');
        const authority = if (slash) |i| rest[0..i] else rest;
        const path = if (slash) |i| rest[i..] else "/";
        if (authority.len == 0) return error.InvalidUrl;

        // Reject userinfo outright: it's a credential in a URL, and we'd
        // only have to be careful never to log it.
        if (std.mem.indexOfScalar(u8, authority, '@') != null) return error.InvalidUrl;

        var host = authority;
        var port: u16 = if (tls) 443 else 80;
        if (std.mem.lastIndexOfScalar(u8, authority, ':')) |i| {
            // Not a bracketed IPv6 literal's internal colon.
            if (std.mem.indexOfScalar(u8, authority, ']') == null or i > std.mem.indexOfScalar(u8, authority, ']').?) {
                host = authority[0..i];
                port = std.fmt.parseInt(u16, authority[i + 1 ..], 10) catch return error.InvalidUrl;
            }
        }
        if (host.len == 0) return error.InvalidUrl;
        return .{ .host = host, .port = port, .path = path, .tls = tls };
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;
const server_mod = @import("server.zig");

const Collected = struct {
    result: ?(Error!Response) = null,

    fn cb(ctx: ?*anyopaque, result: Error!Response) void {
        const self: *Collected = @ptrCast(@alignCast(ctx.?));
        self.result = result;
    }

    fn deinit(self: *Collected) void {
        if (self.result) |r| {
            if (r) |resp| {
                var m = resp;
                m.deinit();
            } else |_| {}
        }
    }
};

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

/// A handler that echoes a fixed JSON body, used as the far end. Testing
/// the client against our own server exercises both sides of the framing,
/// which is worth more than a hand-written fake would be.
fn handleJson(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.send(200, "application/json", "{\"ok\":true}");
}

fn handleTeapot(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.send(418, "text/plain", "short and stout");
}

fn handleEmpty(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.sendStatus(204);
}

const test_routes = [_]server_mod.Route{
    .{ .method = .get, .path = "/json", .handler = handleJson, .access = .public },
    .{ .method = .post, .path = "/json", .handler = handleJson, .access = .public },
    .{ .method = .get, .path = "/teapot", .handler = handleTeapot, .access = .public },
    .{ .method = .get, .path = "/empty", .handler = handleEmpty, .access = .public },
};

fn startServer(gpa: Allocator, loop: *reactor.Loop, srv: *server_mod.Server) !u16 {
    srv.init(gpa, loop, .{}, &test_routes);
    try srv.listen(try IpAddress.parse("127.0.0.1", 0));
    return srv.boundPort();
}

test "GET against our own server round-trips" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    const port = try startServer(gpa, &loop, &srv);
    defer srv.deinit();

    var col: Collected = .{};
    defer col.deinit();

    var ex: Exchange = undefined;
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .method = .get,
        .path = "/json",
        .host = "127.0.0.1",
    }, .{}, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collected) bool {
            return c.result != null;
        }
    }.f);

    const resp = try col.result.?;
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqualStrings("{\"ok\":true}", resp.body);
    try testing.expect(resp.get("Content-Type") != null);
}

test "POST sends a body and Content-Length" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    const port = try startServer(gpa, &loop, &srv);
    defer srv.deinit();

    var col: Collected = .{};
    defer col.deinit();

    var ex: Exchange = undefined;
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .method = .post,
        .path = "/json",
        .host = "127.0.0.1",
        .body = "{\"content\":\"hello webhook\"}",
    }, .{}, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collected) bool {
            return c.result != null;
        }
    }.f);

    // Our server rejects a body-bearing request without Content-Length, so
    // a 200 here is also proof the header was sent.
    const resp = try col.result.?;
    try testing.expectEqual(@as(u16, 200), resp.status);
}

test "a non-2xx status is delivered, not turned into an error" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    const port = try startServer(gpa, &loop, &srv);
    defer srv.deinit();

    var col: Collected = .{};
    defer col.deinit();

    var ex: Exchange = undefined;
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .path = "/teapot",
        .host = "127.0.0.1",
    }, .{}, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collected) bool {
            return c.result != null;
        }
    }.f);

    // The notify layer's retry policy keys off the status code, so a 4xx
    // has to arrive as a response rather than as a transport error.
    const resp = try col.result.?;
    try testing.expectEqual(@as(u16, 418), resp.status);
    try testing.expectEqualStrings("short and stout", resp.body);
}

test "a 204 has no body and completes immediately" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    const port = try startServer(gpa, &loop, &srv);
    defer srv.deinit();

    var col: Collected = .{};
    defer col.deinit();

    var ex: Exchange = undefined;
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .path = "/empty",
        .host = "127.0.0.1",
    }, .{}, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collected) bool {
            return c.result != null;
        }
    }.f);

    // Waiting for a body on a 204 would hang until the deadline.
    const resp = try col.result.?;
    try testing.expectEqual(@as(u16, 204), resp.status);
    try testing.expectEqual(@as(usize, 0), resp.body.len);
}

test "an endpoint that accepts and says nothing times out" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Accept and hold, never reply. A webhook endpoint behaving like this
    // must not be able to pin the exchange forever.
    var mute: socket.Listener = undefined;
    var held: sys.Fd = sys.invalid_fd;
    const Holder = struct {
        var slot: *sys.Fd = undefined;
        fn f(_: *socket.Listener, fd: sys.Fd) void {
            slot.* = fd;
        }
    };
    Holder.slot = &held;
    try mute.listen(try IpAddress.parse("127.0.0.1", 0), Holder.f, 4);
    const port = try mute.boundPort();
    try loop.add(&mute.source);
    defer {
        loop.remove(&mute.source);
        mute.close();
        if (held != sys.invalid_fd) sys.close(held);
    }

    var col: Collected = .{};
    defer col.deinit();

    var ex: Exchange = undefined;
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .path = "/",
        .host = "127.0.0.1",
    }, .{ .deadline_ns = 80 * std.time.ns_per_ms }, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 5000, &col, struct {
        fn f(c: *Collected) bool {
            return c.result != null;
        }
    }.f);

    try testing.expectError(error.Timeout, col.result.?);
}

test "connect refused is reported, not retried forever" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var probe: socket.Listener = undefined;
    try probe.listen(try IpAddress.parse("127.0.0.1", 0), struct {
        fn f(_: *socket.Listener, fd: sys.Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead = try probe.boundPort();
    probe.close();

    var col: Collected = .{};
    defer col.deinit();

    var ex: Exchange = undefined;
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", dead), .{
        .path = "/",
        .host = "127.0.0.1",
    }, .{}, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collected) bool {
            return c.result != null;
        }
    }.f);

    try testing.expectError(error.ConnectionRefused, col.result.?);
}

test "a header carrying CRLF is refused before the write" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var col: Collected = .{};
    var ex: Exchange = undefined;

    // Webhook headers come from configuration and from a rendered
    // template, so a value that injects a second header must be refused
    // rather than sent.
    try testing.expectError(error.MalformedHeader, ex.start(
        gpa,
        &loop,
        try IpAddress.parse("127.0.0.1", 1),
        .{
            .path = "/",
            .host = "example.invalid",
            .headers = &.{.{ .name = "X-Evil", .value = "a\r\nX-Injected: yes" }},
        },
        .{},
        Collected.cb,
        &col,
    ));
}

test "url parsing covers what config actually produces" {
    const cases = .{
        .{ "http://example.invalid/hook", "example.invalid", @as(u16, 80), "/hook", false },
        .{ "http://example.invalid:8080/a/b?c=d", "example.invalid", @as(u16, 8080), "/a/b?c=d", false },
        .{ "http://example.invalid", "example.invalid", @as(u16, 80), "/", false },
        .{ "https://discord.com/api/webhooks/1/x", "discord.com", @as(u16, 443), "/api/webhooks/1/x", true },
    };
    inline for (cases) |c| {
        const u = try Url.parse(c[0]);
        try testing.expectEqualStrings(c[1], u.host);
        try testing.expectEqual(c[2], u.port);
        try testing.expectEqualStrings(c[3], u.path);
        try testing.expectEqual(c[4], u.tls);
    }
}

test "url parsing rejects what we won't handle" {
    // A credential in a URL is something we'd then have to be careful
    // never to log; refusing it is simpler and safer than redacting it.
    try testing.expectError(error.InvalidUrl, Url.parse("http://user:pass@example.invalid/"));
    try testing.expectError(error.InvalidUrl, Url.parse("ftp://example.invalid/"));
    try testing.expectError(error.InvalidUrl, Url.parse("example.invalid/"));
    try testing.expectError(error.InvalidUrl, Url.parse("http:///nohost"));
    try testing.expectError(error.InvalidUrl, Url.parse("http://example.invalid:notaport/"));
}

test "chunked decoding, including extensions" {
    var body = "5\r\nhello\r\n7;ext=1\r\n, world\r\n0\r\n\r\n".*;
    const n = try Exchange.decodeChunked(&body);
    try testing.expectEqualStrings("hello, world", body[0..n]);
}

test "a malformed chunk length is refused rather than guessed" {
    var body = "zz\r\nhello\r\n0\r\n\r\n".*;
    try testing.expectError(error.UnsupportedEncoding, Exchange.decodeChunked(&body));

    // A chunk claiming more than it delivers must not read past the buffer.
    var lying = "ff\r\nshort\r\n0\r\n\r\n".*;
    try testing.expectError(error.UnsupportedEncoding, Exchange.decodeChunked(&lying));
}
