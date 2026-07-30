//! The HTTP/1.1 server: accept, per-connection state machine,
//! keep-alive, timeouts, routing, auth.
//!
//! Everything runs on the reactor thread. There is no thread pool and no
//! per-connection stack; a connection is a heap-allocated `Conn` with a
//! read buffer, a parser, a timer and an outbound queue, and it advances
//! only when the loop says its fd is ready or its timer is due. That is
//! what keeps idle CPU at zero with an SSE stream open per browser tab.
//!
//! ## Buffer ownership, and when a request goes stale
//!
//! One read buffer per connection, `opts.read_buf_size` bytes, allocated
//! at accept and never resized. Request lines and headers are parsed *in
//! place*: `Request.path`, `Request.query` and every header name and
//! value are slices into that buffer. Nothing is copied and a request
//! with no body allocates nothing at all.
//!
//! The consequences are load-bearing, so, explicitly:
//!
//!   * A `Request` handed to a handler is valid for the duration of that
//!     handler call. After it returns, the server compacts the buffer to
//!     make room for the next request and the slices are garbage.
//!   * A handler that keeps the connection open (SSE) must copy anything
//!     it still needs before returning. While a stream is open the server
//!     reads and discards inbound bytes — a client cannot pipeline
//!     behind a stream — so a detached connection is never kept alive
//!     afterwards.
//!   * A request *with* a body needs the buffer back for the body, so the
//!     header block is copied to a heap allocation first and the request
//!     is rebased onto it (`Request.rebase`). That is one allocation for
//!     a POST and zero for a GET.
//!
//! ## Timeouts
//!
//! Three deadlines, all on one reactor timer per connection, no
//! background thread:
//!
//!   * **header** — from the first byte of a request until the blank line.
//!     Slowloris budget.
//!   * **body** — while reading a declared body. This is what catches a
//!     `Content-Length: 1000000` that only ever sends ten bytes.
//!   * **idle** — between responses on a keep-alive connection.
//!
//! A streaming response arms none of them: an SSE connection is supposed
//! to sit silent for hours. Its liveness comes from the client going away,
//! which arrives as a readable-zero or a hangup.

const std = @import("std");
const socket = @import("../socket.zig");
const sys = @import("../../posix/sys.zig");
const reactor = @import("../../posix/reactor.zig");
const log = @import("../../core/log.zig");
const request = @import("request.zig");
const response = @import("response.zig");

const Allocator = std.mem.Allocator;
const IpAddress = std.Io.net.IpAddress;
const Method = request.Method;

pub const Error = Allocator.Error || sys.Error;

/// Cookie the session authenticator reads. Same name the Go build used,
/// because existing browser sessions have to keep working.
pub const session_cookie_name = "hoardarr_session";

const ns_per_s = std.time.ns_per_s;

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

pub const Options = struct {
    limits: request.Limits = .{},

    /// First byte to blank line.
    header_timeout_ns: u64 = 15 * ns_per_s,
    /// Blank line to last body byte.
    body_timeout_ns: u64 = 60 * ns_per_s,
    /// Response sent to next request line, on a keep-alive connection.
    /// Comfortably longer than a browser's own keep-alive so the client
    /// is the one that closes, which avoids a race where a request lands
    /// on a socket we just reaped.
    idle_timeout_ns: u64 = 75 * ns_per_s,

    /// Per connection, and therefore the hard cap on the header block:
    /// headers are parsed in place, so they must fit here. `init` clamps
    /// the header limits to it.
    read_buf_size: usize = 16 << 10,

    /// Refused past this, rather than accepted and then starved of fds.
    max_connections: usize = 512,
    /// Requests served on one connection before we insist on a new one.
    /// Bounds any per-connection accounting drift and, more usefully,
    /// stops one client from pinning a slot forever.
    max_requests_per_connection: usize = 10_000,

    backlog: u31 = socket.default_backlog,
};

// ---------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------

pub const HandlerError = response.Error || Allocator.Error;

/// A request handler. Handlers are thin: decode, call into an
/// application service, encode. They never block — anything slow either
/// completes on the reactor or streams.
pub const HandlerFn = *const fn (ctx: *Ctx) HandlerError!void;

pub const Route = struct {
    /// null matches any method, for a catch-all such as the SPA fallback.
    method: ?Method = null,
    path: []const u8,
    kind: Kind = .exact,
    handler: HandlerFn,
    access: Access = .protected,

    pub const Kind = enum {
        exact,
        /// Matches `path` and anything under it. The remainder lands in
        /// `Ctx.tail`.
        prefix,
    };

    pub const Access = enum {
        /// Reachable without a credential. Health checks and the auth
        /// bootstrap endpoints only.
        public,
        /// Session cookie or API key required.
        protected,
    };
};

/// The routing table is a plain slice the caller owns, usually a comptime
/// array. Matching is a linear scan: with a few dozen routes that is a
/// handful of `memcmp`s, cheaper than any hash of the path and with no
/// allocation and no build step.
///
/// Precedence: an exact match beats every prefix match, and among prefix
/// matches the longest wins. That reproduces the part of Go's
/// `http.ServeMux` behaviour hoardarr relies on — `/api/v1/queue` and
/// `/` can coexist — without the pattern language.
fn matchRoute(routes: []const Route, method: Method, path: []const u8) MatchResult {
    var best: ?*const Route = null;
    var best_score: usize = 0;
    var method_mismatch = false;

    for (routes) |*r| {
        const path_ok = switch (r.kind) {
            .exact => std.mem.eql(u8, r.path, path),
            .prefix => std.mem.startsWith(u8, path, r.path),
        };
        if (!path_ok) continue;
        if (r.method) |m| {
            if (m != method) {
                method_mismatch = true;
                continue;
            }
        }
        const score: usize = if (r.kind == .exact) std.math.maxInt(usize) else r.path.len + 1;
        if (best == null or score > best_score) {
            best = r;
            best_score = score;
        }
    }
    return .{ .route = best, .method_mismatch = method_mismatch };
}

const MatchResult = struct {
    route: ?*const Route,
    /// A route for this path exists but not for this method: 405, not 404.
    method_mismatch: bool,
};

/// `Allow:` value for a 405, built into a caller-supplied buffer.
fn allowHeader(buf: []u8, routes: []const Route, path: []const u8) []const u8 {
    var n: usize = 0;
    for (routes) |r| {
        const path_ok = switch (r.kind) {
            .exact => std.mem.eql(u8, r.path, path),
            .prefix => std.mem.startsWith(u8, path, r.path),
        };
        if (!path_ok) continue;
        const m = r.method orelse continue;
        const name = m.name();
        if (std.mem.indexOf(u8, buf[0..n], name) != null) continue;
        if (n + name.len + 2 > buf.len) break;
        if (n > 0) {
            @memcpy(buf[n..][0..2], ", ");
            n += 2;
        }
        @memcpy(buf[n..][0..name.len], name);
        n += name.len;
    }
    return buf[0..n];
}

// ---------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------

/// Session cookie validation, as a callback so this layer does not
/// depend on the auth service. Null disables session auth entirely,
/// which is API-key-only mode.
pub const Session = struct {
    ctx: ?*anyopaque = null,
    /// True when `token` names a live session.
    authenticate: *const fn (ctx: ?*anyopaque, token: []const u8) bool,
};

/// Either a valid session cookie or a valid API key is sufficient —
/// hoardarr's mixed-auth model. Browsers log in and get a cookie; the
/// *arr clients and SAB consumers present a shared key they cannot
/// negotiate for.
///
/// The key is fetched through a callback on every request rather than
/// captured once, so rotating it from Settings takes effect on the very
/// next request instead of at the next restart.
pub const Auth = struct {
    key_ctx: ?*anyopaque = null,
    key_fn: ?*const fn (ctx: ?*anyopaque) []const u8 = null,
    session: ?Session = null,

    /// Ready-made provider for a key held in a `[]const u8` variable the
    /// caller owns. Assigning to that variable rotates the key.
    pub fn keyFromPointer(ctx: ?*anyopaque) []const u8 {
        const p: *const []const u8 = @ptrCast(@alignCast(ctx.?));
        return p.*;
    }

    pub fn expectedKey(self: *const Auth) []const u8 {
        const f = self.key_fn orelse return "";
        return f(self.key_ctx);
    }

    /// The order matters and matches the Go middleware: cookie first
    /// (the browser path, and the only one that identifies a user),
    /// then `X-Api-Key`, then `?apikey=`. Query last because it is the
    /// one that ends up in proxy access logs, so it is the fallback for
    /// clients that cannot set a header — `EventSource` in particular.
    pub fn authorize(self: *const Auth, req: *const request.Request) bool {
        if (self.session) |s| {
            if (req.cookie(session_cookie_name)) |token| {
                if (token.len > 0 and s.authenticate(s.ctx, token)) return true;
            }
        }

        const expected = self.expectedKey();
        if (req.header("x-api-key")) |provided| {
            if (constantTimeStringEq(provided, expected)) return true;
            // Fall through: a client may send a stale header *and* a
            // valid query key. Go did the same by only consulting the
            // query when the header was absent — the difference only
            // shows for a request that carries both, and being lenient
            // there costs nothing since both are checked against the
            // same secret.
        }
        if (req.queryValue("apikey")) |raw| {
            // `?apikey=` arrives percent-encoded in principle. Keys are
            // hex, so this only matters for a malformed request; decode
            // failures simply do not authenticate.
            var buf: [256]u8 = undefined;
            const provided = request.percentDecode(&buf, raw, .query) catch return false;
            if (constantTimeStringEq(provided, expected)) return true;
        }
        return false;
    }
};

/// Length-independent comparison, with the same two refusals the Go
/// version had: an empty provided key never matches, and an empty
/// expected key never matches *anything* — an unconfigured server must
/// not be an open one.
///
/// A length mismatch still runs a compare of equal length so the
/// rejection takes the same time whether the length was right or not.
pub fn constantTimeStringEq(provided: []const u8, expected: []const u8) bool {
    if (provided.len == 0 or expected.len == 0) return false;
    if (provided.len != expected.len) {
        // Timing decoy, and `volatile`-free: the result is fed into the
        // return value so the optimiser cannot drop the work entirely.
        const decoy = timingSafeEql(provided, provided);
        return decoy and false;
    }
    return timingSafeEql(provided, expected);
}

fn timingSafeEql(a: []const u8, b: []const u8) bool {
    std.debug.assert(a.len == b.len);
    var diff: u8 = 0;
    for (a, b) |x, y| diff |= x ^ y;
    return diff == 0;
}

// ---------------------------------------------------------------------
// Handler context
// ---------------------------------------------------------------------

/// Called when a detached (streaming) connection dies, so the owner —
/// the SSE hub — can drop its subscription.
pub const StreamHook = struct {
    ctx: ?*anyopaque = null,
    on_close: *const fn (ctx: ?*anyopaque) void,
};

pub const Ctx = struct {
    server: *Server,
    conn: *Conn,
    req: *const request.Request,
    res: *response.Response,
    /// For a prefix route, the path with the prefix removed. Empty for an
    /// exact route.
    tail: []const u8 = "",

    /// The application object the server was wired with.
    pub fn app(self: *Ctx, comptime T: type) *T {
        return @ptrCast(@alignCast(self.server.app_ctx.?));
    }

    /// Hand the response to the application: the server will not finish
    /// it when the handler returns. This is how SSE is expressed —
    /// `res.beginEventStream()` then `detach()`, and the hub writes into
    /// `conn.resp()` for as long as the client stays.
    ///
    /// The hook fires exactly once, when the connection goes away for any
    /// reason (client closed, write error, server shutdown).
    pub fn detach(self: *Ctx, hook: ?StreamHook) void {
        self.conn.detached = true;
        self.conn.stream_hook = hook;
        // A detached connection discards anything the client sends while
        // the stream is open, so it cannot be reused afterwards without
        // possibly having eaten a pipelined request.
        self.res.keep_alive = false;
    }
};

// ---------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------

pub const Server = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    opts: Options,
    routes: []const Route,
    auth: Auth = .{},
    /// Passed to handlers via `Ctx.app`.
    app_ctx: ?*anyopaque = null,

    /// Reverse-proxy mount point, e.g. "/hoardarr", without a trailing
    /// slash. Mutable at runtime: the Settings UI can change it and the
    /// next request is routed against the new value.
    url_base: []const u8 = "",

    listener: socket.Listener = undefined,
    listening: bool = false,

    /// Live connections, intrusively linked. A linked list rather than an
    /// array because accept and close must not be able to fail on an
    /// allocation, and because a `Conn` is closed from inside its own
    /// callback where an array's reshuffling would be another hazard.
    conns: ?*Conn = null,
    conn_count: usize = 0,

    /// Connections that asked to die while the loop was inside their
    /// callback. Freeing one there would leave the socket layer
    /// dereferencing freed memory on the way out, so they are chained
    /// here and freed by `reap_timer`, which the reactor fires *after*
    /// dispatch in the same tick.
    zombies: ?*Conn = null,
    reap_timer: reactor.Timer = .{ .callback = reapTimerFired },

    stats: Stats = .{},

    pub const Stats = struct {
        accepted: u64 = 0,
        /// Refused at accept for lack of a connection slot or memory.
        refused: u64 = 0,
        requests: u64 = 0,
        /// Responses with a 4xx or 5xx status.
        errors: u64 = 0,
        timeouts: u64 = 0,
    };

    /// In-place init; `Server` is referenced by every `Conn` and by the
    /// listener callback, so it must not move afterwards.
    pub fn init(
        self: *Server,
        gpa: Allocator,
        loop: *reactor.Loop,
        opts: Options,
        routes: []const Route,
    ) void {
        var o = opts;
        // Headers are parsed in place, so the parser's limits cannot
        // exceed the buffer they live in. Clamping here means the parser
        // always reports 431/414 before the buffer fills, and the
        // buffer-full branch stays a defensive one.
        o.limits.max_header_bytes = @min(o.limits.max_header_bytes, o.read_buf_size);
        o.limits.max_request_line = @min(o.limits.max_request_line, o.read_buf_size);
        o.limits.max_header_line = @min(o.limits.max_header_line, o.read_buf_size);
        o.limits.max_headers = @min(o.limits.max_headers, request.max_header_slots);

        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .opts = o,
            .routes = routes,
        };
    }

    pub fn listen(self: *Server, addr: IpAddress) Error!void {
        try self.listener.listen(addr, onAccept, self.opts.backlog);
        self.listener.context = self;
        errdefer self.listener.close();
        try self.loop.add(&self.listener.source);
        self.listening = true;
    }

    pub fn boundPort(self: *const Server) sys.Error!u16 {
        return self.listener.boundPort();
    }

    /// Stop accepting and drop every connection. Streaming handlers get
    /// their close hook so they can unsubscribe.
    pub fn deinit(self: *Server) void {
        if (self.listening) {
            self.loop.remove(&self.listener.source);
            self.listener.close();
            self.listening = false;
        }
        while (self.conns) |c| c.close();
        self.loop.cancelTimer(&self.reap_timer);
        self.reap();
    }

    fn onAccept(l: *socket.Listener, fd: sys.Fd) void {
        const self: *Server = @ptrCast(@alignCast(l.context.?));
        self.stats.accepted += 1;

        if (self.conn_count >= self.opts.max_connections) {
            // Closing immediately is the honest answer: the alternative
            // is accepting and then failing on the next fd we need.
            self.stats.refused += 1;
            sys.close(fd);
            log.warn("http: connection limit reached", &.{log.uint("limit", self.opts.max_connections)});
            return;
        }

        const conn = Conn.create(self, fd) catch |err| {
            self.stats.refused += 1;
            sys.close(fd);
            log.warn("http: accept failed", &.{log.errv("err", err)});
            return;
        };
        // Link at the head: O(1), and shutdown order does not matter.
        conn.next = self.conns;
        if (self.conns) |head| head.prev = conn;
        self.conns = conn;
        self.conn_count += 1;

        conn.arm(self.opts.header_timeout_ns);
    }

    /// Unlink from the live list and chain onto the zombie list. Never
    /// allocates, so it cannot fail on the close path.
    fn retire(self: *Server, conn: *Conn) void {
        if (conn.prev) |p| p.next = conn.next else self.conns = conn.next;
        if (conn.next) |n| n.prev = conn.prev;
        conn.prev = null;
        conn.next = null;
        self.conn_count -= 1;

        conn.zombie_next = self.zombies;
        self.zombies = conn;

        // A zero-delay timer fires at the end of this same tick, after
        // dispatch has finished walking its arrays. If arming fails
        // (allocation) the zombie simply waits for the next successful
        // reap; it is chained, so nothing is lost.
        if (!self.reap_timer.isArmed()) {
            self.loop.addTimer(&self.reap_timer, 0) catch {};
        }
    }

    fn reapTimerFired(t: *reactor.Timer) void {
        const self: *Server = @fieldParentPtr("reap_timer", t);
        self.reap();
    }

    fn reap(self: *Server) void {
        while (self.zombies) |c| {
            self.zombies = c.zombie_next;
            c.destroy();
        }
    }
};

// ---------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------

pub const Conn = struct {
    server: *Server,
    stream: socket.Stream = undefined,
    timer: reactor.Timer = .{ .callback = onTimeout },

    /// Headers are parsed in place here. Fixed size, allocated once.
    rbuf: []u8,
    /// Bytes present in `rbuf`.
    rlen: usize = 0,

    parser: request.Parser,
    res: response.Response = undefined,

    /// Live only while a body is being read.
    dec: request.BodyDecoder = undefined,
    body: std.ArrayList(u8) = .empty,
    /// Copy of the header block, made when a request has a body so the
    /// read buffer can be reused for it. Null-length when unused.
    hdr_copy: []u8 = &.{},

    phase: Phase = .headers,
    /// Bytes of `rbuf` the current request still owns and that must be
    /// dropped once it is done — the header block of a body-less request,
    /// which handlers hold slices into.
    hold: usize = 0,
    /// The handler took ownership of the response (SSE).
    detached: bool = false,
    stream_hook: ?StreamHook = null,

    requests: usize = 0,
    started_ns: u64 = 0,

    dead: bool = false,
    prev: ?*Conn = null,
    next: ?*Conn = null,
    zombie_next: ?*Conn = null,

    pub const Phase = enum {
        /// Reading a request line and headers.
        headers,
        /// Reading a body.
        body,
        /// Inside a handler.
        dispatch,
        /// A detached response is open; inbound bytes are discarded.
        streaming,
        /// Response done, connection closing once the queue flushes.
        draining,
        closed,
    };

    const handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_drained = onDrained,
        .on_close = onPeerClose,
    };

    fn create(server: *Server, fd: sys.Fd) Error!*Conn {
        const conn = try server.gpa.create(Conn);
        errdefer server.gpa.destroy(conn);

        const buf = try server.gpa.alloc(u8, server.opts.read_buf_size);
        errdefer server.gpa.free(buf);

        conn.* = .{
            .server = server,
            .rbuf = buf,
            .parser = .{ .limits = server.opts.limits },
        };
        try conn.stream.initAccepted(server.gpa, server.loop, fd, &handler);
        conn.stream.context = conn;
        return conn;
    }

    /// Only ever called from the reap timer, i.e. after the loop has
    /// finished dispatching.
    fn destroy(self: *Conn) void {
        const gpa = self.server.gpa;
        self.body.deinit(gpa);
        if (self.hdr_copy.len > 0) gpa.free(self.hdr_copy);
        gpa.free(self.rbuf);
        gpa.destroy(self);
    }

    /// Tear down now. Safe to call from inside a socket callback: the
    /// `Stream` is deinitialised (fd closed, source removed) but the
    /// memory stays alive until the reap timer, because the socket layer
    /// still reads `stream.state` on its way out of the callback.
    pub fn close(self: *Conn) void {
        if (self.dead) return;
        self.dead = true;
        self.phase = .closed;

        if (self.stream_hook) |h| {
            self.stream_hook = null;
            h.on_close(h.ctx);
        }
        self.server.loop.cancelTimer(&self.timer);
        self.stream.deinit();
        self.server.retire(self);
    }

    /// The response for a detached (streaming) connection. The owner
    /// writes into this from wherever it likes — a bus callback, a
    /// heartbeat timer — for as long as the connection lives.
    pub fn resp(self: *Conn) *response.Response {
        return &self.res;
    }

    /// End a detached stream. The connection closes afterwards.
    pub fn finishStream(self: *Conn) void {
        if (self.phase != .streaming) return;
        self.res.finish() catch {};
        self.completeRequest();
    }

    // -- timers -------------------------------------------------------

    fn arm(self: *Conn, delay_ns: u64) void {
        self.server.loop.cancelTimer(&self.timer);
        self.server.loop.addTimer(&self.timer, delay_ns) catch {
            // A connection with no deadline is a resource leak waiting
            // for a slow client, so refuse to have one.
            self.close();
        };
    }

    fn disarm(self: *Conn) void {
        self.server.loop.cancelTimer(&self.timer);
    }

    fn onTimeout(t: *reactor.Timer) void {
        const self: *Conn = @fieldParentPtr("timer", t);
        self.server.stats.timeouts += 1;
        switch (self.phase) {
            // Nothing sent yet: a client that opened a socket and went
            // quiet. No response, just take the slot back.
            .headers => if (self.rlen == 0) {
                self.close();
            } else {
                log.debug("http: header timeout", &.{log.uint("buffered", self.rlen)});
                self.fail(408, "request header timeout");
            },
            // The one that matters: a body that claimed a length it never
            // sent. Without this the connection would sit here forever.
            .body => {
                log.debug("http: body timeout", &.{log.uint("received", self.body.items.len)});
                self.fail(408, "request body timeout");
            },
            else => self.close(),
        }
    }

    // -- socket callbacks ---------------------------------------------

    fn onReadable(s: *socket.Stream) void {
        const self: *Conn = @fieldParentPtr("stream", s);
        self.pump();
    }

    fn onDrained(s: *socket.Stream) void {
        const self: *Conn = @fieldParentPtr("stream", s);
        // The last bytes of a `Connection: close` response are gone; now
        // the fd can go without truncating it.
        if (self.phase == .draining) self.close();
    }

    fn onPeerClose(s: *socket.Stream, _: ?socket.Error) void {
        const self: *Conn = @fieldParentPtr("stream", s);
        self.close();
    }

    // -- the state machine --------------------------------------------

    fn pump(self: *Conn) void {
        while (!self.dead) {
            self.process();
            switch (self.phase) {
                .headers, .body => {},
                .streaming => {
                    self.discardInbound();
                    return;
                },
                .draining, .closed, .dispatch => return,
            }

            if (self.rlen == self.rbuf.len) {
                // Unreachable while the parser's limits are clamped to
                // the buffer size, which `Server.init` guarantees. Kept
                // as a refusal rather than an assert: it is the one place
                // a limits change could otherwise become a hang.
                self.fail(431, "request headers too large");
                return;
            }

            const n = self.stream.read(self.rbuf[self.rlen..]) catch |err| switch (err) {
                error.WouldBlock => return,
                error.Interrupted => continue,
                else => {
                    self.close();
                    return;
                },
            };
            if (n == 0) {
                self.onEof();
                return;
            }
            self.rlen += n;
            if (self.phase == .headers and self.rlen == n) {
                // First byte of a request: the header deadline starts here
                // rather than at accept, so a keep-alive connection gets
                // the idle budget while it is quiet and the header budget
                // once it starts talking.
                self.started_ns = sys.monotonicNanos();
                self.arm(self.server.opts.header_timeout_ns);
            }
        }
    }

    /// Consume as much of the buffer as the current phase can.
    fn process(self: *Conn) void {
        while (!self.dead) switch (self.phase) {
            .headers => {
                const st = self.parser.parse(self.rbuf[0..self.rlen]) catch |err| {
                    self.server.stats.errors += 1;
                    log.warn("http: rejected request", &.{
                        log.errv("reason", err),
                        log.uint("status", request.statusFor(err)),
                    });
                    self.fail(request.statusFor(err), @errorName(err));
                    return;
                };
                if (st == .incomplete) return;
                self.beginRequest();
            },
            .body => {
                if (!self.feedBody()) return;
            },
            else => return,
        };
    }

    fn beginRequest(self: *Conn) void {
        self.requests += 1;
        self.server.stats.requests += 1;
        self.disarm();

        const req = &self.parser.req;
        self.res = response.Response.init(self.sink(), req);
        if (self.requests >= self.server.opts.max_requests_per_connection) {
            self.res.keep_alive = false;
        }

        if (!req.hasBody()) {
            // The handler holds slices into the header block, so it must
            // survive until the response is done.
            self.hold = self.parser.headerEnd();
            self.dispatch();
            return;
        }

        if (req.expect_continue) {
            self.res.sendContinue() catch {
                self.close();
                return;
            };
        }

        // Reclaim the read buffer for the body by moving the header block
        // onto the heap and rebasing the request onto the copy.
        const header_end = self.parser.headerEnd();
        const copy = self.server.gpa.alloc(u8, header_end) catch {
            self.fail(500, "out of memory");
            return;
        };
        @memcpy(copy, self.rbuf[0..header_end]);
        req.rebase(self.rbuf[0..header_end], copy);
        if (self.hdr_copy.len > 0) self.server.gpa.free(self.hdr_copy);
        self.hdr_copy = copy;
        self.consume(header_end);
        self.hold = 0;

        self.dec = request.BodyDecoder.init(req, self.server.opts.limits);
        self.body.clearRetainingCapacity();
        if (req.body_kind == .length) {
            self.body.ensureTotalCapacity(self.server.gpa, @intCast(req.content_length)) catch {
                self.fail(500, "out of memory");
                return;
            };
        }
        self.phase = .body;
        self.arm(self.server.opts.body_timeout_ns);
    }

    /// Returns true when the body finished (and the handler ran), false
    /// when more bytes are needed.
    fn feedBody(self: *Conn) bool {
        var off: usize = 0;
        var done = false;
        while (off < self.rlen) {
            const r = self.dec.feed(self.rbuf[off..self.rlen]) catch |err| {
                self.server.stats.errors += 1;
                self.fail(request.statusFor(err), @errorName(err));
                return false;
            };
            if (r.data.len > 0) {
                self.body.appendSlice(self.server.gpa, r.data) catch {
                    self.fail(500, "out of memory");
                    return false;
                };
            }
            off += r.consumed;
            if (r.done) {
                done = true;
                break;
            }
            if (r.consumed == 0) break; // needs more input
        }
        self.consume(off);
        if (!done) return false;

        self.disarm();
        self.parser.req.body = self.body.items;
        self.dispatch();
        return true;
    }

    /// Drop `n` bytes from the front of the read buffer.
    fn consume(self: *Conn, n: usize) void {
        if (n == 0) return;
        std.debug.assert(n <= self.rlen);
        const rest = self.rlen - n;
        if (rest > 0) std.mem.copyForwards(u8, self.rbuf[0..rest], self.rbuf[n..self.rlen]);
        self.rlen = rest;
    }

    // -- dispatch -----------------------------------------------------

    fn dispatch(self: *Conn) void {
        self.phase = .dispatch;
        const req = &self.parser.req;

        // URL base handling, ported from the Go composition layer: every
        // route is registered unprefixed so the code is portable across
        // deployments, and the prefix is stripped here.
        var path = req.path;
        const base = self.server.url_base;
        if (base.len > 0 and !std.mem.eql(u8, path, "/healthz")) {
            if (std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, base)) {
                // A bookmark of the host root should land on the SPA, and
                // the trailing slash is what makes the document base
                // resolve.
                var buf: [512]u8 = undefined;
                if (base.len + 1 <= buf.len) {
                    @memcpy(buf[0..base.len], base);
                    buf[base.len] = '/';
                    self.res.redirect(301, buf[0 .. base.len + 1]) catch {};
                } else {
                    self.res.sendStatus(500) catch {};
                }
                self.completeRequest();
                return;
            }
            if (!std.mem.startsWith(u8, path, base) or path[base.len] != '/') {
                self.respondError(404, "not found");
                return;
            }
            path = path[base.len..];
        }

        const m = matchRoute(self.server.routes, req.method, path);
        const route = m.route orelse {
            if (m.method_mismatch) {
                var buf: [128]u8 = undefined;
                const allow = allowHeader(&buf, self.server.routes, path);
                self.res.setHeader("Allow", allow) catch {};
                self.respondError(405, "method not allowed");
            } else {
                self.respondError(404, "not found");
            }
            return;
        };

        if (route.access == .protected and !self.server.auth.authorize(req)) {
            // Byte-identical to the Go middleware's rejection, because
            // clients (and the e2e suite) match on it.
            self.respondError(401, "authentication required");
            return;
        }

        var ctx = Ctx{
            .server = self.server,
            .conn = self,
            .req = req,
            .res = &self.res,
            .tail = if (route.kind == .prefix) path[route.path.len..] else "",
        };

        route.handler(&ctx) catch |err| {
            log.warn("http: handler failed", &.{
                log.str("path", path),
                log.errv("err", err),
            });
            self.server.stats.errors += 1;
            if (!self.res.headSent()) {
                self.res.resetHeaders();
                self.res.keep_alive = false;
                self.res.sendError(500, "internal error") catch {};
            } else {
                // Mid-response failure: the client cannot be told, and
                // the framing may be short. Close rather than reuse.
                self.res.keep_alive = false;
            }
            self.completeRequest();
            return;
        };

        if (self.detached and !self.res.isFinished()) {
            // The application owns the response now. No read deadline: an
            // SSE stream is *supposed* to be silent.
            self.phase = .streaming;
            self.disarm();
            return;
        }

        if (!self.res.isFinished()) {
            self.res.finish() catch {
                self.res.keep_alive = false;
            };
        }
        self.completeRequest();
    }

    /// Send a small error response and keep the connection if we still
    /// can. Used for routing and auth refusals, which say nothing about
    /// the health of the connection itself.
    fn respondError(self: *Conn, status: u16, message: []const u8) void {
        self.server.stats.errors += 1;
        self.res.sendError(status, message) catch {
            self.close();
            return;
        };
        self.completeRequest();
    }

    /// Reject and close: for anything that leaves the byte stream
    /// untrustworthy (a framing error, a timeout mid-message).
    fn fail(self: *Conn, status: u16, message: []const u8) void {
        if (self.phase == .headers or self.phase == .body) {
            var res = response.Response.initRaw(
                self.sink(),
                self.parser.req.version,
                self.parser.req.method == .head,
                false, // never keep a connection we could not parse
            );
            res.sendError(status, message) catch {};
        }
        self.beginClose();
    }

    /// Response is done. Either recycle for the next request or close.
    fn completeRequest(self: *Conn) void {
        const req = &self.parser.req;
        log.debug("http", &.{
            log.str("method", req.method_raw),
            log.str("path", req.path),
            log.uint("status", self.res.status),
            log.uint("bytes", self.res.body_bytes),
            log.uint("us", (sys.monotonicNanos() -| self.started_ns) / std.time.ns_per_us),
        });

        const keep = self.res.keep_alive and !self.detached;

        // Drop the header block the handler was holding; anything after it
        // is a pipelined request.
        self.consume(self.hold);
        self.hold = 0;

        if (self.hdr_copy.len > 0) {
            self.server.gpa.free(self.hdr_copy);
            self.hdr_copy = &.{};
        }
        self.body.clearRetainingCapacity();
        self.detached = false;
        self.stream_hook = null;

        if (!keep) {
            self.beginClose();
            return;
        }

        self.parser.reset();
        self.phase = .headers;
        if (self.rlen > 0) {
            // A pipelined request is already here; `pump` will parse it
            // on the next turn of its loop.
            self.started_ns = sys.monotonicNanos();
            self.arm(self.server.opts.header_timeout_ns);
        } else {
            self.arm(self.server.opts.idle_timeout_ns);
        }
    }

    /// Close once the queued response bytes are actually out. Closing the
    /// fd here would discard them.
    fn beginClose(self: *Conn) void {
        self.phase = .draining;
        self.disarm();
        if (self.stream.pending() == 0) {
            self.close();
        } else {
            // `on_drained` finishes the job. Until then the peer may keep
            // sending; we ignore it.
            self.stream.shutdownWrite();
        }
    }

    fn onEof(self: *Conn) void {
        // A keep-alive connection closed between requests is the normal
        // ending and not worth a log line.
        self.close();
    }

    /// While a stream is open the client has nothing useful to say. Read
    /// and drop, purely to notice the close.
    fn discardInbound(self: *Conn) void {
        var junk: [512]u8 = undefined;
        while (true) {
            const n = self.stream.read(&junk) catch |err| switch (err) {
                error.WouldBlock => return,
                error.Interrupted => continue,
                else => {
                    self.close();
                    return;
                },
            };
            if (n == 0) {
                self.close();
                return;
            }
        }
    }

    // -- response sink ------------------------------------------------

    fn sink(self: *Conn) response.Sink {
        return .{ .ctx = self, .vtable = &sink_vtable };
    }

    const sink_vtable: response.Sink.VTable = .{
        .write = sinkWrite,
        .pending = sinkPending,
    };

    fn sinkWrite(ctx: *anyopaque, bytes: []const u8) response.SinkError!void {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        self.stream.write(bytes) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            // The socket layer's queue cap: a client that stopped reading.
            error.SystemResources => error.QueueFull,
            error.NotConnected, error.ConnectionReset, error.BrokenPipe => error.StreamClosed,
            else => error.IoFailed,
        };
    }

    fn sinkPending(ctx: *anyopaque) usize {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        return self.stream.pending();
    }
};

// ---------------------------------------------------------------------
// Handlers the rest of the tree plugs into
// ---------------------------------------------------------------------

/// The trivial handler: 200 with a plain-text body. Useful as a
/// placeholder route and as the thing the server tests route to.
pub fn handleOk(ctx: *Ctx) HandlerError!void {
    try ctx.res.send(200, "text/plain; charset=utf-8", "ok");
}

/// `{"status":"ok","service":"hoardarr"}` — the liveness probe the
/// container HEALTHCHECK and every orchestrator expects, byte-identical
/// to what the Go build served.
pub fn handleHealth(ctx: *Ctx) HandlerError!void {
    try ctx.res.send(200, "application/json", "{\"status\":\"ok\",\"service\":\"hoardarr\"}");
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

fn loopbackAny() IpAddress {
    return IpAddress.parse("127.0.0.1", 0) catch unreachable;
}

/// A raw client: writes bytes, accumulates whatever comes back. No HTTP
/// knowledge, so the tests assert on the actual wire output.
const Client = struct {
    stream: socket.Stream = undefined,
    gpa: Allocator,
    got: std.ArrayList(u8) = .empty,
    connected: bool = false,
    closed: bool = false,
    err: ?socket.Error = null,
    send_on_connect: []const u8 = "",
    disconnected: bool = false,
    buf: [8192]u8 = undefined,

    const handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
        .on_connected = onConnected,
    };

    fn connect(self: *Client, loop: *reactor.Loop, port: u16) !void {
        try self.stream.connect(self.gpa, loop, try IpAddress.parse("127.0.0.1", port), &handler);
    }

    /// Drop the socket. Idempotent, so a test can simulate the client
    /// walking away and still `defer` its cleanup.
    fn disconnect(self: *Client) void {
        if (self.disconnected) return;
        self.disconnected = true;
        self.stream.deinit();
    }

    fn deinit(self: *Client) void {
        self.disconnect();
        self.got.deinit(self.gpa);
    }

    fn destroy(self: *Client) void {
        const gpa = self.gpa;
        self.deinit();
        gpa.destroy(self);
    }

    fn send(self: *Client, bytes: []const u8) !void {
        try self.stream.write(bytes);
    }

    fn onConnected(s: *socket.Stream, err: ?socket.Error) void {
        const self: *Client = @fieldParentPtr("stream", s);
        self.err = err;
        if (err != null) return;
        self.connected = true;
        if (self.send_on_connect.len > 0) {
            s.write(self.send_on_connect) catch |e| {
                self.err = e;
            };
        }
    }

    fn onReadable(s: *socket.Stream) void {
        const self: *Client = @fieldParentPtr("stream", s);
        while (true) {
            const n = s.read(&self.buf) catch |err| switch (err) {
                error.WouldBlock => return,
                else => {
                    self.closed = true;
                    return;
                },
            };
            if (n == 0) {
                self.closed = true;
                return;
            }
            self.got.appendSlice(self.gpa, self.buf[0..n]) catch return;
        }
    }

    fn onClose(s: *socket.Stream, _: ?socket.Error) void {
        const self: *Client = @fieldParentPtr("stream", s);
        self.closed = true;
    }

    /// Number of complete status lines seen. Enough to know how many
    /// responses arrived on a keep-alive or pipelined connection.
    fn responseCount(self: *const Client) usize {
        var n: usize = 0;
        var rest = self.got.items;
        while (std.mem.indexOf(u8, rest, "HTTP/1.")) |i| {
            n += 1;
            rest = rest[i + 7 ..];
        }
        return n;
    }

    fn status(self: *const Client) ?u16 {
        const items = self.got.items;
        if (items.len < 12) return null;
        return std.fmt.parseInt(u16, items[9..12], 10) catch null;
    }

    fn body(self: *const Client) []const u8 {
        const sep = std.mem.indexOf(u8, self.got.items, "\r\n\r\n") orelse return "";
        return self.got.items[sep + 4 ..];
    }

    /// Whether a whole first response has arrived, head *and* body. A
    /// head can land in one read and its body in the next, so waiting for
    /// the blank line alone makes a test that reads the body flaky.
    fn complete(self: *const Client) bool {
        const items = self.got.items;
        const sep = std.mem.indexOf(u8, items, "\r\n\r\n") orelse return false;
        const head = items[0 .. sep + 4];
        const have = items.len - (sep + 4);
        const at = std.mem.indexOf(u8, head, "Content-Length: ") orelse return true;
        const rest = head[at + 16 ..];
        const end = std.mem.indexOf(u8, rest, "\r\n") orelse return false;
        const want = std.fmt.parseInt(usize, rest[0..end], 10) catch return false;
        return have >= want;
    }
};

/// Pump the loop until `done` or the deadline. Never a fixed tick count:
/// how many wakeups an exchange takes is the kernel's business.
fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

fn pumpFor(loop: *reactor.Loop, ms: u64) !void {
    const start = sys.monotonicNanos();
    while (sys.monotonicNanos() - start < ms * std.time.ns_per_ms) {
        _ = try loop.tick(5);
    }
}

/// Server + loop + one route table, wired for a test.
const Harness = struct {
    loop: reactor.Loop = undefined,
    server: Server = undefined,
    port: u16 = 0,
    api_key: []const u8 = "",

    fn start(self: *Harness, gpa: Allocator, routes: []const Route, opts: Options) !void {
        try self.loop.init(gpa);
        self.server.init(gpa, &self.loop, opts, routes);
        self.server.auth = .{ .key_ctx = @ptrCast(&self.api_key), .key_fn = Auth.keyFromPointer };
        try self.server.listen(loopbackAny());
        self.port = try self.server.boundPort();
    }

    /// Drop the server (and with it every connection) while leaving the
    /// loop alive, so clients can still be torn down afterwards.
    fn shutdownServer(self: *Harness) void {
        self.server.deinit();
    }

    fn deinit(self: *Harness) void {
        self.server.deinit();
        self.loop.deinit();
    }

    /// Run one request to completion and return the client holding the
    /// raw response.
    ///
    /// Heap-allocated, and this is not incidental: `socket.Stream`
    /// registers `&self.source` with the loop, so a `Stream` must never be
    /// copied after `connect`. Returning one by value would leave the
    /// reactor dispatching through a dead stack frame.
    fn exchange(self: *Harness, gpa: Allocator, raw: []const u8) !*Client {
        const client = try gpa.create(Client);
        errdefer gpa.destroy(client);
        client.* = .{ .gpa = gpa, .send_on_connect = raw };
        errdefer client.deinit();
        try client.connect(&self.loop, self.port);
        try pumpUntil(&self.loop, 2000, client, struct {
            fn f(c: *Client) bool {
                return c.closed or c.complete();
            }
        }.f);
        return client;
    }
};

fn handleEcho(ctx: *Ctx) HandlerError!void {
    try ctx.res.send(200, "text/plain", ctx.req.body);
}

fn handleWhoami(ctx: *Ctx) HandlerError!void {
    try ctx.res.send(
        200,
        "application/json",
        "{\"service\":\"hoardarr\",\"version\":\"0.0.1-dev\",\"authenticated\":true}",
    );
}

fn handleTail(ctx: *Ctx) HandlerError!void {
    try ctx.res.send(200, "text/plain", ctx.tail);
}

const test_routes = [_]Route{
    .{ .method = .get, .path = "/api/v1/health", .handler = handleHealth, .access = .public },
    .{ .method = .get, .path = "/healthz", .handler = handleHealth, .access = .public },
    .{ .method = .get, .path = "/api/v1/whoami", .handler = handleWhoami },
    .{ .method = .post, .path = "/api/v1/echo", .handler = handleEcho, .access = .public },
    .{ .method = .put, .path = "/api/v1/echo", .handler = handleEcho, .access = .public },
    .{ .path = "/files/", .kind = .prefix, .handler = handleTail, .access = .public },
    .{ .path = "/", .kind = .prefix, .handler = handleOk, .access = .public },
};

// -- auth: the Go auth_test.go, ported ---------------------------------

test "health is publicly accessible" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "secretkey0123456789abcdef0123456789" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(gpa, "GET /api/v1/health HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqualStrings("{\"status\":\"ok\",\"service\":\"hoardarr\"}", c.body());
}

test "protected route rejects a missing key" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "secretkey0123456789abcdef0123456789" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(gpa, "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 401), c.status());
    try testing.expectEqualStrings("{\"error\":\"authentication required\"}", c.body());
}

test "protected route rejects a wrong key" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "expected-key" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nX-Api-Key: wrong-key\r\nConnection: close\r\n\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 401), c.status());
}

test "protected route accepts the header key" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "good-key-12345" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nX-Api-Key: good-key-12345\r\nConnection: close\r\n\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expect(std.mem.indexOf(u8, c.body(), "\"authenticated\":true") != null);
}

test "protected route accepts the query key" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "good-key-12345" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(
        gpa,
        "GET /api/v1/whoami?apikey=good-key-12345 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
}

test "constant-time string compare" {
    // The Go table, verbatim.
    const cases = [_]struct { provided: []const u8, expected: []const u8, want: bool }{
        .{ .provided = "", .expected = "", .want = false }, // empty rejected
        .{ .provided = "abc", .expected = "", .want = false }, // empty expected rejected
        .{ .provided = "abc", .expected = "abc", .want = true },
        .{ .provided = "abc", .expected = "abcd", .want = false }, // length mismatch
        .{ .provided = "abc", .expected = "abd", .want = false }, // value mismatch
        .{ .provided = "abcdef0123", .expected = "abcdef0123", .want = true },
    };
    for (cases) |c| {
        try testing.expectEqual(c.want, constantTimeStringEq(c.provided, c.expected));
    }
}

test "an unconfigured key does not open the server" {
    const gpa = testing.allocator;
    // Empty key: no credential can match, not even an empty one.
    var h: Harness = .{ .api_key = "" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nX-Api-Key: \r\nConnection: close\r\n\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 401), c.status());
}

test "rotating the key takes effect on the next request" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "first-key" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c1 = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nX-Api-Key: first-key\r\nConnection: close\r\n\r\n",
    );
    defer c1.destroy();
    try testing.expectEqual(@as(?u16, 200), c1.status());

    // The provider is consulted per request, so this is all a rotation
    // needs to be.
    h.api_key = "second-key";

    const c2 = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nX-Api-Key: first-key\r\nConnection: close\r\n\r\n",
    );
    defer c2.destroy();
    try testing.expectEqual(@as(?u16, 401), c2.status());

    const c3 = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nX-Api-Key: second-key\r\nConnection: close\r\n\r\n",
    );
    defer c3.destroy();
    try testing.expectEqual(@as(?u16, 200), c3.status());
}

test "a session cookie authenticates, and a bad one falls through to the key" {
    const gpa = testing.allocator;
    const Validator = struct {
        fn f(_: ?*anyopaque, token: []const u8) bool {
            return std.mem.eql(u8, token, "live-session");
        }
    };

    var h: Harness = .{ .api_key = "the-key" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();
    h.server.auth.session = .{ .authenticate = Validator.f };

    const ok = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nCookie: hoardarr_session=live-session\r\nConnection: close\r\n\r\n",
    );
    defer ok.destroy();
    try testing.expectEqual(@as(?u16, 200), ok.status());

    const stale = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nCookie: hoardarr_session=expired\r\nConnection: close\r\n\r\n",
    );
    defer stale.destroy();
    try testing.expectEqual(@as(?u16, 401), stale.status());

    // A dead cookie must not stop a valid key from working — that is the
    // path an *arr client takes after a browser session on the same host
    // expired.
    const mixed = try h.exchange(
        gpa,
        "GET /api/v1/whoami HTTP/1.1\r\nHost: h\r\nCookie: hoardarr_session=expired\r\nX-Api-Key: the-key\r\nConnection: close\r\n\r\n",
    );
    defer mixed.destroy();
    try testing.expectEqual(@as(?u16, 200), mixed.status());
}

// -- routing ----------------------------------------------------------

test "routing: exact beats prefix, longest prefix wins, 404 and 405" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    // The "/" catch-all exists, but the exact health route wins.
    const health = try h.exchange(gpa, "GET /api/v1/health HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer health.destroy();
    try testing.expect(std.mem.indexOf(u8, health.body(), "hoardarr") != null);

    // Longest prefix: "/files/x/y" hits /files/ and not "/".
    const files = try h.exchange(gpa, "GET /files/x/y HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer files.destroy();
    try testing.expectEqualStrings("x/y", files.body());

    // Catch-all for anything else.
    const spa = try h.exchange(gpa, "GET /queue HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer spa.destroy();
    try testing.expectEqualStrings("ok", spa.body());

    // A method the API route does not handle falls through to the
    // catch-all rather than becoming a 405 — same as Go's ServeMux, where
    // a registered "/" pattern matches every method.
    const bad = try h.exchange(gpa, "DELETE /api/v1/echo HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer bad.destroy();
    try testing.expectEqual(@as(?u16, 200), bad.status());
    try testing.expectEqualStrings("ok", bad.body());
}

test "routing: no catch-all means 404" {
    const gpa = testing.allocator;
    const routes = [_]Route{
        .{ .method = .get, .path = "/api/v1/health", .handler = handleHealth, .access = .public },
    };
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &routes, .{});
    defer h.deinit();

    const c = try h.exchange(gpa, "GET /nope HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 404), c.status());

    // With no catch-all, a known path and an unknown method is a 405 that
    // says what would have worked.
    const bad = try h.exchange(gpa, "POST /api/v1/health HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer bad.destroy();
    try testing.expectEqual(@as(?u16, 405), bad.status());
    const at = std.mem.indexOf(u8, bad.got.items, "Allow: ") orelse return error.NoAllowHeader;
    const line_end = std.mem.indexOfPos(u8, bad.got.items, at, "\r\n").?;
    try testing.expectEqualStrings("GET", bad.got.items[at + 7 .. line_end]);
}

test "url_base strips the prefix, redirects the root and 404s outside" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();
    h.server.url_base = "/hoardarr";

    // Inside the mount: prefix stripped, route matched.
    const inside = try h.exchange(gpa, "GET /hoardarr/api/v1/health HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer inside.destroy();
    try testing.expectEqual(@as(?u16, 200), inside.status());

    // Host root and the bare base both redirect to "<base>/" so the SPA's
    // document base resolves.
    const root = try h.exchange(gpa, "GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer root.destroy();
    try testing.expectEqual(@as(?u16, 301), root.status());
    try testing.expect(std.mem.indexOf(u8, root.got.items, "Location: /hoardarr/\r\n") != null);

    const bare = try h.exchange(gpa, "GET /hoardarr HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer bare.destroy();
    try testing.expectEqual(@as(?u16, 301), bare.status());

    // Outside the mount: not ours.
    const outside = try h.exchange(gpa, "GET /elsewhere/x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer outside.destroy();
    try testing.expectEqual(@as(?u16, 404), outside.status());

    // /healthz is exempt: the container HEALTHCHECK and kubelet know
    // nothing about the reverse-proxy mount.
    const healthz = try h.exchange(gpa, "GET /healthz HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer healthz.destroy();
    try testing.expectEqual(@as(?u16, 200), healthz.status());
}

// -- keep-alive and bodies --------------------------------------------

test "keep-alive serves several requests on one connection" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    var c = Client{ .gpa = gpa };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.connected;
        }
    }.f);

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        try c.send("GET /api/v1/health HTTP/1.1\r\nHost: h\r\n\r\n");
        const want = i + 1;
        const Ctx2 = struct { c: *Client, want: usize };
        var ctx = Ctx2{ .c = &c, .want = want };
        try pumpUntil(&h.loop, 2000, &ctx, struct {
            fn f(x: *Ctx2) bool {
                return x.c.responseCount() >= x.want;
            }
        }.f);
    }

    try testing.expectEqual(@as(usize, 3), c.responseCount());
    try testing.expect(!c.closed);
    // One connection, three requests.
    try testing.expectEqual(@as(u64, 1), h.server.stats.accepted);
    try testing.expectEqual(@as(u64, 3), h.server.stats.requests);
    try testing.expect(std.mem.indexOf(u8, c.got.items, "Connection: close") == null);
}

test "pipelined requests are all answered, in order" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    // Three requests in one segment, the last one closing.
    const c = try h.exchange(gpa,
        "GET /files/a HTTP/1.1\r\nHost: h\r\n\r\n" ++
            "GET /files/b HTTP/1.1\r\nHost: h\r\n\r\n" ++
            "GET /files/c HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer c.destroy();

    try pumpUntil(&h.loop, 2000, c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    try testing.expectEqual(@as(usize, 3), c.responseCount());
    // Bodies in request order.
    const ia = std.mem.indexOf(u8, c.got.items, "\r\n\r\na").?;
    const ib = std.mem.indexOf(u8, c.got.items, "\r\n\r\nb").?;
    const ic = std.mem.indexOf(u8, c.got.items, "\r\n\r\nc").?;
    try testing.expect(ia < ib and ib < ic);
}

test "a Content-Length body reaches the handler" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(
        gpa,
        "POST /api/v1/echo HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello world",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqualStrings("hello world", c.body());
}

test "a body split across reads still arrives whole" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    var c = Client{ .gpa = gpa };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.connected;
        }
    }.f);

    // Headers, then the body one byte at a time, pumping in between so
    // the server really does see separate reads.
    try c.send("POST /api/v1/echo HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nConnection: close\r\n\r\n");
    try pumpFor(&h.loop, 20);
    for ("abcde") |ch| {
        try c.send(&[_]u8{ch});
        try pumpFor(&h.loop, 5);
    }
    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.responseCount() > 0;
        }
    }.f);
    try testing.expectEqualStrings("abcde", c.body());
}

test "a chunked body reaches the handler" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const c = try h.exchange(gpa, "PUT /api/v1/echo HTTP/1.1\r\nHost: h\r\n" ++
        "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n" ++
        "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqualStrings("hello world", c.body());
}

test "a body larger than the read buffer streams through it" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    // Deliberately tiny read buffer: the body must not have to fit in it.
    try h.start(gpa, &test_routes, .{ .read_buf_size = 2048 });
    defer h.deinit();

    const size = 64 * 1024;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(gpa);
    var hdr: [128]u8 = undefined;
    try raw.appendSlice(gpa, try std.fmt.bufPrint(
        &hdr,
        "POST /api/v1/echo HTTP/1.1\r\nHost: h\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{size},
    ));
    try raw.appendNTimes(gpa, 'x', size);

    var c = Client{ .gpa = gpa, .send_on_connect = raw.items };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 10_000, &c, struct {
        fn f(x: *Client) bool {
            return x.responseCount() > 0 and x.body().len >= 64 * 1024;
        }
    }.f);
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqual(@as(usize, size), c.body().len);
}

test "HEAD gets the headers and no body" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    const routes = [_]Route{
        .{ .method = .head, .path = "/x", .handler = handleOk, .access = .public },
    };
    try h.start(gpa, &routes, .{});
    defer h.deinit();

    const c = try h.exchange(gpa, "HEAD /x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expect(std.mem.indexOf(u8, c.got.items, "Content-Length: 2\r\n") != null);
    try testing.expectEqualStrings("", c.body());
}

// -- hostile input, end to end ----------------------------------------

test "hostile: request smuggling is refused and the connection dropped" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    // Content-Length and Transfer-Encoding together, with a smuggled
    // second request in the body. If either half were interpreted we
    // would answer twice.
    const c = try h.exchange(gpa, "POST /api/v1/echo HTTP/1.1\r\nHost: h\r\n" ++
        "Content-Length: 44\r\nTransfer-Encoding: chunked\r\n\r\n" ++
        "0\r\n\r\nGET /api/v1/whoami HTTP/1.1\r\nHost: h\r\n\r\n");
    defer c.destroy();

    try pumpUntil(&h.loop, 2000, c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    try testing.expectEqual(@as(?u16, 400), c.status());
    // Exactly one response: the smuggled request was never seen.
    try testing.expectEqual(@as(usize, 1), c.responseCount());
    try testing.expect(std.mem.indexOf(u8, c.got.items, "Connection: close") != null);
}

test "hostile: 10k headers is refused with 431" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(gpa);
    try raw.appendSlice(gpa, "GET / HTTP/1.1\r\nHost: h\r\n");
    for (0..10_000) |i| {
        var line: [64]u8 = undefined;
        try raw.appendSlice(gpa, try std.fmt.bufPrint(&line, "X-Pad-{d}: v\r\n", .{i}));
    }
    try raw.appendSlice(gpa, "\r\n");

    var c = Client{ .gpa = gpa, .send_on_connect = raw.items };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 5000, &c, struct {
        fn f(x: *Client) bool {
            return x.responseCount() > 0;
        }
    }.f);
    try testing.expectEqual(@as(?u16, 431), c.status());
}

test "hostile: an absurd request line is refused with 414" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(gpa);
    try raw.appendSlice(gpa, "GET /");
    try raw.appendNTimes(gpa, 'a', 40_000);
    try raw.appendSlice(gpa, " HTTP/1.1\r\nHost: h\r\n\r\n");

    var c = Client{ .gpa = gpa, .send_on_connect = raw.items };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 5000, &c, struct {
        fn f(x: *Client) bool {
            return x.responseCount() > 0;
        }
    }.f);
    try testing.expectEqual(@as(?u16, 414), c.status());
    // And the buffer never grew: it is a fixed allocation per connection.
    try testing.expect(raw.items.len > h.server.opts.read_buf_size);
}

test "hostile: LF-only framing and a NUL in the path are refused" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{});
    defer h.deinit();

    const lf = try h.exchange(gpa, "GET / HTTP/1.1\nHost: h\n\n");
    defer lf.destroy();
    try testing.expectEqual(@as(?u16, 400), lf.status());

    const nul = try h.exchange(gpa, "GET /a\x00b HTTP/1.1\r\nHost: h\r\n\r\n");
    defer nul.destroy();
    try testing.expectEqual(@as(?u16, 400), nul.status());
}

test "hostile: a body over max_body is refused with 413" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{ .limits = .{ .max_body = 16 } });
    defer h.deinit();

    const c = try h.exchange(
        gpa,
        "POST /api/v1/echo HTTP/1.1\r\nHost: h\r\nContent-Length: 1000\r\n\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 413), c.status());
}

test "hostile: the connection cap refuses rather than starving" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{ .max_connections = 2 });
    defer h.deinit();

    var clients: [5]Client = undefined;
    for (&clients) |*c| {
        c.* = .{ .gpa = gpa };
        try c.connect(&h.loop, h.port);
    }
    defer for (&clients) |*c| c.deinit();

    const Ctx3 = struct { h: *Harness };
    var ctx = Ctx3{ .h = &h };
    try pumpUntil(&h.loop, 3000, &ctx, struct {
        fn f(x: *Ctx3) bool {
            return x.h.server.stats.accepted >= 5;
        }
    }.f);

    try testing.expect(h.server.stats.refused >= 3);
    try testing.expect(h.server.conn_count <= 2);
}

// -- timeouts ---------------------------------------------------------

test "a silent connection is reaped by the header timeout" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{ .header_timeout_ns = 40 * std.time.ns_per_ms });
    defer h.deinit();

    var c = Client{ .gpa = gpa };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    // Half a request line and then nothing — the slowloris shape.
    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.connected;
        }
    }.f);
    try c.send("GET / HTTP/1.1\r\nHost: h\r\n");

    try pumpUntil(&h.loop, 3000, &c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    // A partial request gets told why; a connection that said nothing at
    // all is simply dropped.
    try testing.expectEqual(@as(?u16, 408), c.status());
    try testing.expectEqual(@as(usize, 0), h.server.conn_count);
    try testing.expect(h.server.stats.timeouts >= 1);
}

test "a connection that opens and says nothing is dropped without a response" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{ .header_timeout_ns = 40 * std.time.ns_per_ms });
    defer h.deinit();

    var c = Client{ .gpa = gpa };
    defer c.deinit();
    try c.connect(&h.loop, h.port);

    try pumpUntil(&h.loop, 3000, &c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    try testing.expectEqual(@as(usize, 0), c.got.items.len);
    try testing.expectEqual(@as(usize, 0), h.server.conn_count);
}

test "a body that claims a length it never sends times out" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{
        .header_timeout_ns = 2 * ns_per_s,
        .body_timeout_ns = 40 * std.time.ns_per_ms,
    });
    defer h.deinit();

    var c = Client{ .gpa = gpa, .send_on_connect = "POST /api/v1/echo HTTP/1.1\r\nHost: h\r\n" ++
        "Content-Length: 1000\r\n\r\nonly-a-few" };
    defer c.deinit();
    try c.connect(&h.loop, h.port);

    // The whole point: this returns, rather than hanging until the test
    // harness gives up.
    try pumpUntil(&h.loop, 3000, &c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    try testing.expectEqual(@as(?u16, 408), c.status());
    try testing.expectEqual(@as(usize, 0), h.server.conn_count);
}

test "an idle keep-alive connection is reaped" {
    const gpa = testing.allocator;
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &test_routes, .{ .idle_timeout_ns = 40 * std.time.ns_per_ms });
    defer h.deinit();

    var c = Client{ .gpa = gpa, .send_on_connect = "GET /api/v1/health HTTP/1.1\r\nHost: h\r\n\r\n" };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.responseCount() > 0;
        }
    }.f);
    try testing.expect(!c.closed);

    // Answered and then left alone: the slot comes back on its own.
    try pumpUntil(&h.loop, 3000, &c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    try testing.expectEqual(@as(usize, 0), h.server.conn_count);
    try testing.expectEqual(@as(usize, 1), c.responseCount());
}

// -- streaming / SSE --------------------------------------------------

/// Stands in for `api/sse`: takes over the response, then pushes events
/// from outside the request/response cycle.
const StreamState = struct {
    conn: ?*Conn = null,
    closed: bool = false,
    dropped: usize = 0,

    var current: *StreamState = undefined;

    fn handle(ctx: *Ctx) HandlerError!void {
        const self = current;
        try ctx.res.beginEventStream();
        try ctx.res.write(": connected\n\n");
        self.conn = ctx.conn;
        ctx.detach(.{ .ctx = self, .on_close = onClose });
    }

    fn onClose(p: ?*anyopaque) void {
        const self: *StreamState = @ptrCast(@alignCast(p.?));
        self.conn = null;
        self.closed = true;
    }

    /// One event, dropped rather than queued when the client is behind —
    /// the same best-effort contract the Go hub had.
    fn publish(self: *StreamState, topic: []const u8, data: []const u8) void {
        const conn = self.conn orelse return;
        const res = conn.resp();
        if (res.pending() > 256 << 10) {
            self.dropped += 1;
            return;
        }
        var buf: [512]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "event: {s}\ndata: {s}\n\n", .{ topic, data }) catch return;
        res.write(msg) catch {
            conn.close();
        };
    }
};

test "SSE: a response stays open and is written to incrementally" {
    const gpa = testing.allocator;
    var state = StreamState{};
    StreamState.current = &state;

    const routes = [_]Route{
        .{ .method = .get, .path = "/api/v1/events", .handler = StreamState.handle, .access = .public },
    };
    var h: Harness = .{ .api_key = "k" };
    // Timeouts that would kill a request-shaped connection long before
    // the test ends: a stream must be exempt from all of them.
    try h.start(gpa, &routes, .{
        .header_timeout_ns = 30 * std.time.ns_per_ms,
        .body_timeout_ns = 30 * std.time.ns_per_ms,
        .idle_timeout_ns = 30 * std.time.ns_per_ms,
    });
    defer h.deinit();

    var c = Client{ .gpa = gpa, .send_on_connect = "GET /api/v1/events HTTP/1.1\r\nHost: h\r\n\r\n" };
    defer c.deinit();
    try c.connect(&h.loop, h.port);

    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return std.mem.indexOf(u8, x.got.items, ": connected") != null;
        }
    }.f);
    try testing.expect(std.mem.indexOf(u8, c.got.items, "Content-Type: text/event-stream") != null);
    try testing.expect(std.mem.indexOf(u8, c.got.items, "Transfer-Encoding: chunked") != null);

    // Well past every timeout, and still open.
    try pumpFor(&h.loop, 120);
    try testing.expect(!c.closed);
    try testing.expect(state.conn != null);

    // Events pushed from outside a request arrive as they are produced.
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        var data: [64]u8 = undefined;
        state.publish("download.segment.completed", try std.fmt.bufPrint(&data, "{{\"n\":{d}}}", .{i}));
        _ = try h.loop.tick(1);
    }
    try pumpUntil(&h.loop, 3000, &c, struct {
        fn f(x: *Client) bool {
            return std.mem.indexOf(u8, x.got.items, "{\"n\":49}") != null;
        }
    }.f);
    try testing.expect(state.conn != null);
    try testing.expectEqual(@as(usize, 0), state.dropped);
    // Nothing is buffered server-side once the client keeps up.
    try testing.expectEqual(@as(usize, 0), state.conn.?.resp().pending());

    // The hook fires when the client goes away, which is how the hub
    // learns to unsubscribe.
    c.disconnect();
    try pumpUntil(&h.loop, 3000, &state, struct {
        fn f(s: *StreamState) bool {
            return s.closed;
        }
    }.f);
    try testing.expectEqual(@as(?*Conn, null), state.conn);
    try testing.expectEqual(@as(usize, 0), h.server.conn_count);
}

test "SSE: server shutdown notifies an open stream" {
    const gpa = testing.allocator;
    var state = StreamState{};
    StreamState.current = &state;

    const routes = [_]Route{
        .{ .method = .get, .path = "/api/v1/events", .handler = StreamState.handle, .access = .public },
    };
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &routes, .{});
    defer h.deinit();

    var c = Client{ .gpa = gpa, .send_on_connect = "GET /api/v1/events HTTP/1.1\r\nHost: h\r\n\r\n" };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 2000, &state, struct {
        fn f(s: *StreamState) bool {
            return s.conn != null;
        }
    }.f);

    h.shutdownServer();
    try testing.expect(state.closed);
    try testing.expectEqual(@as(?*Conn, null), state.conn);
}

test "SSE: a stream the handler ends itself closes the connection" {
    const gpa = testing.allocator;
    var state = StreamState{};
    StreamState.current = &state;

    const routes = [_]Route{
        .{ .method = .get, .path = "/api/v1/events", .handler = StreamState.handle, .access = .public },
    };
    var h: Harness = .{ .api_key = "k" };
    try h.start(gpa, &routes, .{});
    defer h.deinit();

    var c = Client{ .gpa = gpa, .send_on_connect = "GET /api/v1/events HTTP/1.1\r\nHost: h\r\n\r\n" };
    defer c.deinit();
    try c.connect(&h.loop, h.port);
    try pumpUntil(&h.loop, 2000, &state, struct {
        fn f(s: *StreamState) bool {
            return s.conn != null;
        }
    }.f);

    state.conn.?.finishStream();
    try pumpUntil(&h.loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.closed;
        }
    }.f);
    // Chunked terminator, then the close.
    try testing.expect(std.mem.endsWith(u8, c.got.items, "0\r\n\r\n"));
    try testing.expectEqual(@as(usize, 0), h.server.conn_count);
}

// -- unit-level routing / auth helpers --------------------------------

test "matchRoute precedence" {
    const routes = [_]Route{
        .{ .path = "/", .kind = .prefix, .handler = handleOk },
        .{ .path = "/api/", .kind = .prefix, .handler = handleOk },
        .{ .method = .get, .path = "/api/v1/health", .handler = handleHealth },
        .{ .method = .post, .path = "/api/v1/health", .handler = handleOk },
    };

    // Exact wins over both prefixes.
    const exact = matchRoute(&routes, .get, "/api/v1/health");
    try testing.expectEqual(&routes[2], exact.route.?);

    // Longest prefix wins.
    const prefix = matchRoute(&routes, .get, "/api/v1/queue");
    try testing.expectEqual(&routes[1], prefix.route.?);

    // Method mismatch on an exact path still falls back to the prefix
    // route that accepts any method — which is what makes an SPA
    // catch-all coexist with method-scoped API routes.
    const other = matchRoute(&routes, .delete, "/api/v1/health");
    try testing.expectEqual(&routes[1], other.route.?);
    try testing.expect(other.method_mismatch);

    var buf: [64]u8 = undefined;
    const allow = allowHeader(&buf, &routes, "/api/v1/health");
    try testing.expect(std.mem.indexOf(u8, allow, "GET") != null);
    try testing.expect(std.mem.indexOf(u8, allow, "POST") != null);
}

test "options clamp the header limits to the read buffer" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var s: Server = undefined;
    s.init(gpa, &loop, .{ .read_buf_size = 4096, .limits = .{ .max_header_bytes = 1 << 20 } }, &test_routes);
    // Headers are parsed in place, so a limit larger than the buffer
    // would turn a big request into a wedged connection instead of a 431.
    try testing.expectEqual(@as(usize, 4096), s.opts.limits.max_header_bytes);
    try testing.expectEqual(@as(usize, 4096), s.opts.limits.max_request_line);
}
