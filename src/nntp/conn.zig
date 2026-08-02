//! An NNTP connection as an explicit state machine on the reactor.
//!
//! `protocol.zig` owns the wire format and knows nothing about sockets;
//! this file owns the socket and the conversation's shape. The split is
//! what lets every framing edge case be tested without a network.
//!
//! ## Why a state machine rather than blocking calls
//!
//! The Go version was straight-line code — `send`, `readCodeLine`,
//! `read body` — with a goroutine per connection and a context watcher
//! goroutine per request to implement cancellation. Forty provider
//! connections meant eighty goroutines, and cancellation was a channel
//! race that commit history shows getting fixed more than once.
//!
//! Here there is one thread. A connection is a small enum plus a cursor,
//! advanced by readability callbacks, and a timeout is a reactor timer
//! rather than a watcher. Cancellation is cancelling that timer, which
//! cannot race because there is nothing to race with.
//!
//! NNTP is strictly serial — one command outstanding at a time, responses
//! in order — so the machine needs no request queue, only a "what did I
//! ask for" field. Pipelining is possible in principle but providers vary
//! in whether they honour it, and the throughput comes from running many
//! connections rather than many requests per connection.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const socket = @import("../net/socket.zig");
const protocol = @import("protocol.zig");
const transport = @import("transport.zig");

const Allocator = std.mem.Allocator;
const IpAddress = std.Io.net.IpAddress;

pub const Transport = transport.Transport;
pub const Security = transport.Security;
pub const TlsConfig = transport.TlsConfig;
pub const Trust = transport.Trust;
pub const CaStore = transport.CaStore;

pub const Error = transport.Error || error{
    /// The server said something that doesn't belong in this state. Almost
    /// always means a desynchronised stream, so the connection is dead.
    ProtocolDesync,
    /// Credentials rejected. Retrying is pointless; the operator has to
    /// fix the configuration.
    AuthFailed,
    /// Server is at its connection limit. Retrying later is correct, and
    /// so is trying a different server first.
    TooManyConnections,
    /// The article isn't on this server. Try the next server in the tier.
    ArticleMissing,
    /// 4xx that isn't one of the above — retry is reasonable.
    Transient,
    /// 5xx that isn't one of the above — retry is not.
    Permanent,
    /// No response within the deadline.
    Timeout,
    /// The peer closed before the block terminator arrived.
    TruncatedBody,
    CommandTooLong,
    InvalidMessageId,
    /// A command argument carried a control character. Refusing before the
    /// write is the point: a CRLF inside a message id would let an
    /// attacker append an arbitrary NNTP command.
    ControlCharacter,
    /// The formatted command exceeded RFC 3977's 512-byte line limit.
    NoSpaceLeft,
};

/// Credentials and behaviour for one provider.
pub const Config = struct {
    /// `AUTHINFO USER`/`PASS` are skipped entirely when username is empty,
    /// which is how the free/open servers are configured.
    username: []const u8 = "",
    password: []const u8 = "",
    /// Some providers require `MODE READER` before serving articles, and
    /// some reject the command outright. `protocol.modeReaderAcceptable`
    /// treats 500/501/502 as success for exactly that reason.
    mode_reader: bool = true,
    /// How long to wait for a response before giving up on a command.
    /// Applies per command, not per connection.
    timeout_ns: u64 = 30 * std.time.ns_per_s,
    /// Read buffer. One article body is ~750 KiB and arrives over many
    /// reads, so this only needs to be large enough that the syscall
    /// count stays sane.
    read_buf_size: usize = 64 * 1024,
    /// Plaintext or TLS. Defaults to plaintext because that is what an
    /// address literal on 119 means; every commercial provider wants
    /// `.tls`, and choosing it means choosing a `Trust` by name.
    security: Security = .plaintext,
};

/// Where the conversation is. Ordered roughly by lifecycle so a
/// `@intFromEnum` comparison can answer "am I usable yet".
pub const State = enum {
    /// TCP connect in flight.
    connecting,
    /// Connected, waiting for the 200/201 greeting.
    greeting,
    /// `AUTHINFO USER` sent, expecting 381 or 281.
    auth_user,
    /// `AUTHINFO PASS` sent, expecting 281.
    auth_pass,
    /// `MODE READER` sent.
    mode_reader,
    /// Handshake complete, no command outstanding. The only state in
    /// which `fetchBody` may be called.
    ready,
    /// `BODY <id>` sent, waiting for the status line.
    body_status,
    /// Status was 222; consuming the multi-line block.
    body_data,
    /// `QUIT` sent.
    quitting,
    closed,
};

/// What the owner hears about. All fire on the reactor thread with the
/// full runtime available.
pub const Handler = struct {
    /// Handshake finished; the connection is usable.
    on_ready: *const fn (c: *Conn) void,
    /// A body arrived complete. `payload` is the unstuffed article,
    /// CRLF-normalised, owned by the connection and **valid only for the
    /// duration of this call** — copy it or decode it here. Reusing one
    /// buffer per connection is why a fetch allocates nothing.
    on_body: *const fn (c: *Conn, payload: []const u8) void,
    /// The connection failed. It is unusable afterwards; the owner must
    /// call `deinit`. `err` distinguishes retry-here, retry-elsewhere and
    /// give-up, which is what the tiered fetcher dispatches on.
    on_error: *const fn (c: *Conn, err: Error) void,
};

pub const Conn = struct {
    /// A plain socket or a TLS session, chosen by `Config.security`. The
    /// field keeps its name because everything above it only ever asks
    /// for `write`, and the plaintext path is byte-for-byte what it was.
    stream: Transport,
    loop: *reactor.Loop,
    gpa: Allocator,
    handler: *const Handler,
    config: Config,

    state: State = .connecting,

    /// Bytes read from the socket but not yet consumed by the parser. A
    /// status line can straddle a read, and so can the boundary between a
    /// status line and the block that follows it.
    in: []u8,
    in_len: usize = 0,

    /// Accumulated body payload for the current fetch.
    body: std.ArrayList(u8) = .empty,
    reader: protocol.BodyReader = .{},

    /// Per-command deadline. Armed on send, cancelled on response.
    timer: reactor.Timer,

    /// Delivers a failure that was raised while the TLS session fiber was
    /// running, from the loop's stack instead.
    ///
    /// `on_error` is entitled to destroy this connection — the pool does
    /// exactly that — and destroying it frees the fiber's stack. Doing
    /// that from a frame *on* that stack is a `munmap` of the caller.
    /// Zero-delay timer, same answer `bootstrap/runtime.zig` uses for the
    /// mirror-image problem.
    defer_timer: reactor.Timer,
    deferred_err: ?Error = null,

    /// Scratch for formatting commands. `protocol.max_command_len` is the
    /// RFC's 512-byte limit.
    cmd_buf: [protocol.max_command_len]u8 = undefined,

    context: ?*anyopaque = null,

    const stream_handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
        .on_connected = onConnected,
    };

    /// What the TLS session calls back into. The plaintext path has no
    /// equivalent because there the loop calls us, not the other way
    /// round.
    const tls_driver_vtable = .{
        .on_open = tlsOpen,
        .space = tlsSpace,
        .filled = tlsFilled,
        .awaiting = tlsAwaiting,
        .on_closed = tlsClosed,
    };

    /// Start connecting. Initialises in place, because the reactor stores
    /// `&self.stream.plain.source` and `&self.timer`.
    pub fn connect(
        self: *Conn,
        gpa: Allocator,
        loop: *reactor.Loop,
        addr: IpAddress,
        config: Config,
        handler: *const Handler,
    ) Error!void {
        const in = try gpa.alloc(u8, config.read_buf_size);
        errdefer gpa.free(in);

        self.* = .{
            .stream = .{},
            .loop = loop,
            .gpa = gpa,
            .handler = handler,
            .config = config,
            .in = in,
            .timer = .{ .callback = onTimeout },
            .defer_timer = .{ .callback = onDeferredFail },
        };

        switch (config.security) {
            .plaintext => {
                try self.stream.plain.connect(gpa, loop, addr, &stream_handler);
                try self.armTimeout();
            },
            .tls => |cfg| {
                // The fiber does the TCP connect too: it has to own the
                // fd's readiness, and a `socket.Stream` owning it first
                // would be a second registration of the same fd.
                const t = try transport.Tls.create(gpa, loop, addr, cfg, .{
                    .ctx = self,
                    .on_open = tls_driver_vtable.on_open,
                    .space = tls_driver_vtable.space,
                    .filled = tls_driver_vtable.filled,
                    .awaiting = tls_driver_vtable.awaiting,
                    .on_closed = tls_driver_vtable.on_closed,
                });
                self.stream.tls = t;
                // Armed before the session runs: the handshake — TCP,
                // certificate exchange and all — is under the same
                // per-command deadline as everything else, and `begin`
                // can fail outright.
                try self.armTimeout();
                t.begin();
            },
        }
    }

    pub fn deinit(self: *Conn) void {
        if (self.timer.isArmed()) self.loop.cancelTimer(&self.timer);
        if (self.defer_timer.isArmed()) self.loop.cancelTimer(&self.defer_timer);
        // Before `self.in` is freed: a parked session's stack may still
        // hold a slice of it.
        self.stream.deinit();
        self.body.deinit(self.gpa);
        self.gpa.free(self.in);
        self.state = .closed;
    }

    /// The peer's own reason for refusing the handshake, if it gave one.
    /// Null on the plaintext path and on every error that is not
    /// `error.TlsAlert`.
    pub fn tlsAlert(self: *const Conn) ?std.crypto.tls.Alert {
        const t = self.stream.tls orelse return null;
        return t.alert;
    }

    /// The unmapped `std` error behind a `Tls*` failure. For logs only.
    pub fn tlsDetail(self: *const Conn) ?anyerror {
        const t = self.stream.tls orelse return null;
        return t.detail;
    }

    pub fn isReady(self: *const Conn) bool {
        return self.state == .ready;
    }

    /// Ask for one article body. Only valid in `.ready`; the caller is the
    /// pool, which tracks that.
    pub fn fetchBody(self: *Conn, message_id: []const u8) Error!void {
        std.debug.assert(self.state == .ready);
        const cmd = try protocol.body(&self.cmd_buf, message_id);
        self.body.clearRetainingCapacity();
        self.reader = .{};
        try self.send(cmd, .body_status);
    }

    /// Send QUIT and let the server close. Politer than a bare close and
    /// it lets a provider's connection accounting settle immediately
    /// rather than on a timeout.
    pub fn quit(self: *Conn) void {
        if (self.state == .closed or self.state == .connecting) return;
        const cmd = protocol.quit(&self.cmd_buf) catch return;
        self.send(cmd, .quitting) catch {};
    }

    // -- internals ----------------------------------------------------

    fn send(self: *Conn, line: []const u8, next: State) Error!void {
        try self.stream.write(line);
        self.state = next;
        try self.armTimeout();
    }

    fn armTimeout(self: *Conn) Error!void {
        if (self.timer.isArmed()) self.loop.cancelTimer(&self.timer);
        try self.loop.addTimer(&self.timer, self.config.timeout_ns);
    }

    fn disarmTimeout(self: *Conn) void {
        if (self.timer.isArmed()) self.loop.cancelTimer(&self.timer);
    }

    fn onTimeout(t: *reactor.Timer) void {
        const self: *Conn = @fieldParentPtr("timer", t);
        self.fail(error.Timeout);
    }

    /// Recover the connection from one of `socket.Stream`'s callbacks.
    /// Two hops now that the stream sits inside a `Transport`.
    fn fromStream(s: *socket.Stream) *Conn {
        const t: *Transport = @fieldParentPtr("plain", s);
        return @fieldParentPtr("stream", t);
    }

    fn onConnected(s: *socket.Stream, err: ?socket.Error) void {
        const self: *Conn = fromStream(s);
        if (err) |e| {
            self.fail(e);
            return;
        }
        // Nothing to send: the server speaks first with its greeting.
        self.state = .greeting;
    }

    fn onClose(s: *socket.Stream, err: ?socket.Error) void {
        const self: *Conn = fromStream(s);
        // A close while consuming a block means a truncated article, which
        // is a different problem from a close between commands and worth
        // reporting as such.
        if (self.state == .body_data) {
            self.fail(error.TruncatedBody);
            return;
        }
        if (self.state == .quitting or self.state == .closed) {
            self.state = .closed;
            return;
        }
        self.fail(err orelse error.ConnectionReset);
    }

    fn onReadable(s: *socket.Stream) void {
        const self: *Conn = fromStream(s);

        while (true) {
            // Compact rather than grow: a status line always fits, and a
            // body is drained into `self.body` as it arrives, so the only
            // way this fills is a server sending a >64 KiB status line.
            if (self.in_len == self.in.len) {
                if (!self.compact()) {
                    self.fail(error.ProtocolDesync);
                    return;
                }
            }

            const n = s.read(self.in[self.in_len..]) catch |err| switch (err) {
                error.WouldBlock => return,
                else => {
                    self.fail(err);
                    return;
                },
            };
            if (n == 0) {
                // Peer closed. onClose will follow, but body_data has to
                // be resolved here so the partial payload isn't lost.
                if (self.state == .body_data) self.fail(error.TruncatedBody);
                return;
            }
            self.in_len += n;

            self.drive() catch |err| {
                self.fail(err);
                return;
            };
            if (self.state == .closed) return;
        }
    }

    // -- the TLS side of the transport --------------------------------
    //
    // The first three run on the session fiber's stack. That is fine for
    // everything the state machine does — including `on_body`, which the
    // owner is expected to answer with the next `fetchBody`, and which
    // then queues rather than writes. It is *not* fine for `on_error`;
    // see `fail`.

    /// The handshake completed. Same point the plaintext path reaches in
    /// `onConnected`: connected, and the server speaks first.
    fn tlsOpen(ctx: *anyopaque) void {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        if (self.state != .connecting) return;
        self.state = .greeting;
    }

    /// Decrypt straight into the connection's own input buffer, so a TLS
    /// body costs no more copies than a plaintext one.
    fn tlsSpace(ctx: *anyopaque) []u8 {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        if (self.in_len >= self.in.len) return &.{};
        return self.in[self.in_len..];
    }

    fn tlsFilled(ctx: *anyopaque, n: usize) bool {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        self.in_len += n;
        self.drive() catch |err| {
            self.fail(err);
            return false;
        };
        return self.state != .closed;
    }

    /// Whether the state machine is waiting on the server. False means
    /// the session may park idle instead of blocking in a TLS read that
    /// nothing is going to answer.
    ///
    /// State only. Bytes left in `in` are by definition ones the current
    /// state cannot consume — `drive` loops until they are gone — so
    /// counting them as "outstanding" would put the session into a read
    /// it can never be woken out of for the next command.
    fn tlsAwaiting(ctx: *anyopaque) bool {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        return switch (self.state) {
            .connecting, .ready, .closed => false,
            else => true,
        };
    }

    /// The session ended. Always on the loop's stack, so this is where a
    /// TLS failure becomes an ordinary `on_error`.
    fn tlsClosed(ctx: *anyopaque, err: Error) void {
        const self: *Conn = @ptrCast(@alignCast(ctx));
        // A close while consuming a block means a truncated article,
        // which is a different problem from a close between commands —
        // exactly the distinction `onClose` makes for plaintext.
        if (self.state == .body_data) {
            self.fail(error.TruncatedBody);
            return;
        }
        if (self.state == .quitting or self.state == .closed) {
            self.state = .closed;
            return;
        }
        self.fail(err);
    }

    fn onDeferredFail(t: *reactor.Timer) void {
        const self: *Conn = @fieldParentPtr("defer_timer", t);
        const err = self.deferred_err orelse return;
        self.deferred_err = null;
        // Unwind the session before telling anyone, so the handler is
        // free to destroy this connection.
        if (self.stream.tls) |tl| tl.halt();
        self.handler.on_error(self, err);
    }

    /// Consume as much of the input buffer as the current state can.
    fn drive(self: *Conn) Error!void {
        while (self.in_len > 0) {
            switch (self.state) {
                .body_data => {
                    if (!try self.consumeBody()) return;
                },
                .connecting, .ready, .closed => return,
                else => {
                    if (!try self.consumeStatusLine()) return;
                },
            }
        }
    }

    /// Consume one status line if a complete one is buffered. Returns
    /// false when more bytes are needed.
    fn consumeStatusLine(self: *Conn) Error!bool {
        const buf = self.in[0..self.in_len];
        const nl = std.mem.indexOfScalar(u8, buf, '\n') orelse return false;
        const line = buf[0 .. nl + 1];

        const status = protocol.parseStatusLine(line) catch return error.ProtocolDesync;
        self.consume(nl + 1);
        self.disarmTimeout();

        try self.handleStatus(status);
        return true;
    }

    fn handleStatus(self: *Conn, status: protocol.Status) Error!void {
        switch (self.state) {
            .greeting => {
                switch (protocol.classifyGreeting(status.code, status.text)) {
                    .ok => {},
                    .too_many_connections => return error.TooManyConnections,
                    .unexpected => return error.ProtocolDesync,
                }
                try self.afterGreeting();
            },

            .auth_user => switch (status.code) {
                // 381: password wanted. 281: username alone sufficed.
                protocol.codes.auth_password_required => {
                    const cmd = try protocol.authinfoPass(&self.cmd_buf, self.config.password);
                    try self.send(cmd, .auth_pass);
                },
                protocol.codes.auth_accepted => try self.afterAuth(),
                else => return classify(status),
            },

            .auth_pass => switch (status.code) {
                protocol.codes.auth_accepted => try self.afterAuth(),
                else => return classify(status),
            },

            .mode_reader => {
                // Providers that don't implement MODE READER answer
                // 500/501/502, and that is not a failure — we only sent it
                // because some providers require it.
                if (!protocol.modeReaderAcceptable(status.code)) return classify(status);
                self.becomeReady();
            },

            .body_status => switch (status.code) {
                protocol.codes.body_follows => {
                    self.state = .body_data;
                    try self.armTimeout();
                },
                else => return classify(status),
            },

            .quitting => {
                self.state = .closed;
            },

            else => return error.ProtocolDesync,
        }
    }

    fn afterGreeting(self: *Conn) Error!void {
        if (self.config.username.len > 0) {
            const cmd = try protocol.authinfoUser(&self.cmd_buf, self.config.username);
            return self.send(cmd, .auth_user);
        }
        return self.afterAuth();
    }

    fn afterAuth(self: *Conn) Error!void {
        if (self.config.mode_reader) {
            const cmd = try protocol.modeReader(&self.cmd_buf);
            return self.send(cmd, .mode_reader);
        }
        self.becomeReady();
    }

    fn becomeReady(self: *Conn) void {
        self.state = .ready;
        self.disarmTimeout();
        self.handler.on_ready(self);
    }

    /// Feed buffered bytes through the dot-unstuffing reader. Returns
    /// false when the input is exhausted without reaching the terminator.
    fn consumeBody(self: *Conn) Error!bool {
        const src = self.in[0..self.in_len];

        // Write straight into the body list's spare capacity so the
        // unstuffed payload is never copied twice.
        var total_consumed: usize = 0;
        while (total_consumed < src.len) {
            try self.body.ensureUnusedCapacity(self.gpa, 4096);
            const dst = self.body.unusedCapacitySlice();

            const step = self.reader.push(src[total_consumed..], dst);
            total_consumed += step.consumed;
            self.body.items.len += step.written;

            if (step.terminated) {
                self.consume(total_consumed);
                self.disarmTimeout();
                // Back to ready *before* the callback: the handler is
                // expected to issue the next fetch from inside it, and it
                // must find the connection usable.
                self.state = .ready;
                self.handler.on_body(self, self.body.items);
                return true;
            }
            // No progress and not terminated means `dst` filled exactly;
            // loop to grow it. `consumed == 0` with a non-empty dst would
            // be a reader bug, so guard against spinning.
            if (step.consumed == 0 and step.written == 0) break;
        }

        self.consume(total_consumed);
        return false;
    }

    fn consume(self: *Conn, n: usize) void {
        std.debug.assert(n <= self.in_len);
        const rest = self.in_len - n;
        if (rest > 0) std.mem.copyForwards(u8, self.in[0..rest], self.in[n..self.in_len]);
        self.in_len = rest;
    }

    /// Nothing to reclaim means the buffer holds one unterminated line
    /// longer than the buffer, which no real server sends.
    fn compact(self: *Conn) bool {
        return self.in_len < self.in.len;
    }

    fn classify(status: protocol.Status) Error {
        return switch (protocol.classifyResponse(status.code, status.text)) {
            .article_missing => error.ArticleMissing,
            .too_many_connections => error.TooManyConnections,
            .auth_required, .auth_failed => error.AuthFailed,
            .transient => error.Transient,
            .permanent => error.Permanent,
            .unexpected => error.ProtocolDesync,
        };
    }

    /// Report a fatal error once, and never from a stack the handler is
    /// allowed to free.
    ///
    /// On the plaintext path that is unconditional: the handler runs from
    /// a reactor callback and destroying the connection there is what the
    /// pool already does. On the TLS path the caller may be the session
    /// fiber itself, whose stack `on_error` would `munmap`, so the report
    /// is postponed to a zero-delay timer.
    fn fail(self: *Conn, err: Error) void {
        if (self.state == .closed) return;
        self.state = .closed;
        self.disarmTimeout();

        if (self.stream.tls) |t| {
            // Tell the session to stop before anything else: it must not
            // report this again from `on_closed`.
            t.stop = true;
            if (!t.isDone() and self.deferred_err == null) {
                self.deferred_err = err;
                if (self.loop.addTimer(&self.defer_timer, 0)) |_| return else |_| {
                    // No timer available. Reporting inline is worse than
                    // this being reported at all, so fall through — and
                    // unwind the session first so its stack is idle.
                    self.deferred_err = null;
                    t.halt();
                }
            }
        }
        self.handler.on_error(self, err);
    }
};

// ---------------------------------------------------------------------
// Scripted stub server (test support)
// ---------------------------------------------------------------------

/// A scripted NNTP server for tests, and the Zig equivalent of the Go
/// suite's `stub_test.go`.
///
/// It is `pub` on purpose: the connection tests, the pool tests and
/// eventually the end-to-end suite all need the same thing, and three
/// copies of a fake server is three places for the fake to drift from the
/// real protocol.
///
/// The script is a list of (expected command prefix, response) pairs. An
/// unexpected command fails the test rather than being tolerated, because
/// a client that sends the wrong thing and still passes is worse than no
/// test at all.
///
/// Once the conversation has gone off script the stub stops answering
/// altogether, on every connection. A stub that kept replying would let
/// the client finish its exchange and the test then assert happily on the
/// bytes that came back — which is the failure mode a scripted peer is
/// most prone to, because the script does not depend on the client having
/// sent anything at all. Every line is kept in `received` so a test can
/// assert on what was asked rather than only on what was answered.
pub const StubServer = struct {
    listener: socket.Listener,
    loop: *reactor.Loop,
    gpa: Allocator,
    script: []const Exchange,
    greeting: []const u8,

    conns: std.ArrayList(*StubConn) = .empty,
    accepted: usize = 0,
    /// Set when a client sent something the script didn't expect. Holds
    /// the reason; `unexpected` holds the line that caused it.
    desync: ?[]const u8 = null,
    /// The offending line, terminator stripped. Owned.
    unexpected: ?[]u8 = null,
    /// Every command line, in arrival order, across all connections.
    /// Owned.
    received: std.ArrayList([]u8) = .empty,

    pub const Exchange = struct {
        /// Matched as a prefix, case-insensitively, against the command
        /// line the client sent.
        expect: []const u8,
        /// Sent verbatim. Include CRLFs, and the block terminator when the
        /// response is multi-line.
        reply: []const u8,
        /// Close the connection instead of replying — for testing the
        /// truncated-body and dropped-connection paths.
        then_close: bool = false,
    };

    const StubConn = struct {
        stream: socket.Stream,
        server: *StubServer,
        step: usize = 0,
        in: [8192]u8 = undefined,
        in_len: usize = 0,

        const handler: socket.Handler = .{
            .on_readable = onReadable,
            .on_close = onClose,
        };

        fn onReadable(s: *socket.Stream) void {
            const self: *StubConn = @fieldParentPtr("stream", s);
            while (true) {
                // A line longer than the buffer is a client bug, not a
                // reason to stall: returning silently used to leave the
                // test waiting on a reply for a command the stub had
                // decided to ignore.
                if (self.in_len == self.in.len) {
                    self.server.fail("command line longer than the stub's buffer", self.in[0..self.in_len]);
                    self.in_len = 0;
                    return;
                }
                const n = s.read(self.in[self.in_len..]) catch return;
                if (n == 0) return;
                self.in_len += n;

                // One command per line; answer each in script order.
                while (std.mem.indexOfScalar(u8, self.in[0..self.in_len], '\n')) |nl| {
                    const line = self.in[0 .. nl + 1];
                    self.respond(line);
                    const rest = self.in_len - (nl + 1);
                    if (rest > 0) std.mem.copyForwards(u8, self.in[0..rest], self.in[nl + 1 .. self.in_len]);
                    self.in_len = rest;
                }
            }
        }

        fn respond(self: *StubConn, line: []const u8) void {
            const trimmed = std.mem.trimEnd(u8, line, "\r\n");
            self.server.note(trimmed);

            // Latched: after one wrong command the stub answers nothing
            // more, so the client fails on the command it got wrong
            // instead of on some later one.
            if (self.server.desync != null) return;

            if (self.step >= self.server.script.len) {
                self.server.fail("more commands than the script expected", trimmed);
                return;
            }
            const ex = self.server.script[self.step];
            self.step += 1;

            if (!std.ascii.startsWithIgnoreCase(trimmed, ex.expect)) {
                self.server.fail(ex.expect, trimmed);
                return;
            }
            if (ex.then_close) {
                self.stream.state = .closed;
                sys.shutdown(self.stream.source.fd, .both);
                return;
            }
            self.stream.write(ex.reply) catch {};
        }

        fn onClose(s: *socket.Stream, _: ?socket.Error) void {
            s.state = .closed;
        }
    };

    pub fn start(
        self: *StubServer,
        gpa: Allocator,
        loop: *reactor.Loop,
        greeting: []const u8,
        script: []const Exchange,
    ) !u16 {
        self.* = .{
            .listener = undefined,
            .loop = loop,
            .gpa = gpa,
            .script = script,
            .greeting = greeting,
        };
        try self.listener.listen(try IpAddress.parse("127.0.0.1", 0), onAccept, 16);
        self.listener.context = self;
        try loop.add(&self.listener.source);
        return self.listener.boundPort();
    }

    pub fn deinit(self: *StubServer) void {
        for (self.conns.items) |c| {
            c.stream.deinit();
            self.gpa.destroy(c);
        }
        self.conns.deinit(self.gpa);
        for (self.received.items) |line| self.gpa.free(line);
        self.received.deinit(self.gpa);
        if (self.unexpected) |u| self.gpa.free(u);
        self.loop.remove(&self.listener.source);
        self.listener.close();
    }

    /// Records a command line. Recording is unconditional — the lines
    /// sent *after* things went wrong are usually what explains why.
    fn note(self: *StubServer, line: []const u8) void {
        const copy = self.gpa.dupe(u8, line) catch return;
        self.received.append(self.gpa, copy) catch self.gpa.free(copy);
    }

    /// Latches the first desync. Later ones are consequences of it.
    fn fail(self: *StubServer, reason: []const u8, line: []const u8) void {
        if (self.desync != null) return;
        self.desync = reason;
        self.unexpected = self.gpa.dupe(u8, line) catch null;
    }

    /// Fails the test when the client went off script, naming both what
    /// was expected and what arrived.
    ///
    /// Worth calling even in a test that already asserts on the reply
    /// bytes: those come from the script, so they are the same whether
    /// the client sent the right command, the wrong one, or nothing at
    /// all past its own buffer.
    pub fn expectClean(self: *const StubServer) !void {
        const reason = self.desync orelse return;
        std.debug.print("\nthe stub went off script: expected '{s}', got '{s}'\n", .{
            reason,
            self.unexpected orelse "<nothing>",
        });
        for (self.received.items) |line| std.debug.print("  sent: {s}\n", .{line});
        return error.StubDesync;
    }

    /// The command lines the client sent, in order.
    pub fn commands(self: *const StubServer) []const []u8 {
        return self.received.items;
    }

    fn onAccept(l: *socket.Listener, fd: sys.Fd) void {
        const self: *StubServer = @ptrCast(@alignCast(l.context.?));
        self.accepted += 1;

        const c = self.gpa.create(StubConn) catch {
            sys.close(fd);
            return;
        };
        c.* = .{ .stream = undefined, .server = self };
        c.stream.initAccepted(self.gpa, self.loop, fd, &StubConn.handler) catch {
            sys.close(fd);
            self.gpa.destroy(c);
            return;
        };
        self.conns.append(self.gpa, c) catch {
            c.stream.deinit();
            self.gpa.destroy(c);
            return;
        };
        // The server speaks first in NNTP.
        c.stream.write(self.greeting) catch {};
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

const Client = struct {
    conn: Conn = undefined,
    ready: bool = false,
    bodies: std.ArrayList([]u8) = .empty,
    err: ?Error = null,
    gpa: Allocator,

    const handler: Handler = .{
        .on_ready = onReady,
        .on_body = onBody,
        .on_error = onError,
    };

    fn deinit(self: *Client) void {
        self.conn.deinit();
        for (self.bodies.items) |b| self.gpa.free(b);
        self.bodies.deinit(self.gpa);
    }

    fn onReady(c: *Conn) void {
        const self: *Client = @fieldParentPtr("conn", c);
        self.ready = true;
    }

    fn onBody(c: *Conn, payload: []const u8) void {
        const self: *Client = @fieldParentPtr("conn", c);
        // The payload is only valid during the callback, so copy it —
        // which is also a test that the contract is really that.
        const copy = self.gpa.dupe(u8, payload) catch return;
        self.bodies.append(self.gpa, copy) catch self.gpa.free(copy);
    }

    fn onError(c: *Conn, err: Error) void {
        const self: *Client = @fieldParentPtr("conn", c);
        self.err = err;
    }
};

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

const greeting_ok = "200 news.example.invalid ready\r\n";

test "handshake without credentials reaches ready" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader mode\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.err);
    try testing.expect(client.ready);
    try stub.expectClean();

    // With no username configured the handshake is MODE READER and
    // nothing else. Asserting on the reply alone could not see an
    // AUTHINFO here: the script answers "200 reader mode" to whatever
    // arrives first, so a client that offered an empty credential would
    // reach ready just the same.
    try testing.expectEqual(@as(usize, 1), stub.commands().len);
    try testing.expectEqualStrings("MODE READER", stub.commands()[0]);
}

test "AUTHINFO USER then PASS" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "AUTHINFO USER alice", .reply = "381 password required\r\n" },
        .{ .expect = "AUTHINFO PASS s3cret", .reply = "281 authenticated\r\n" },
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .username = "alice",
        .password = "s3cret",
    }, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.err);
    try testing.expect(client.ready);
    try stub.expectClean();

    // The whole conversation, in order and with the credentials in it.
    try testing.expectEqual(@as(usize, 3), stub.commands().len);
    try testing.expectEqualStrings("AUTHINFO USER alice", stub.commands()[0]);
    try testing.expectEqualStrings("AUTHINFO PASS s3cret", stub.commands()[1]);
    try testing.expectEqualStrings("MODE READER", stub.commands()[2]);
}

test "username alone accepted with 281 skips PASS" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        // Sending PASS after a 281 would be a protocol error; the script
        // expecting MODE READER next is what proves we don't.
        .{ .expect = "AUTHINFO USER alice", .reply = "281 authenticated\r\n" },
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .username = "alice",
        .password = "s3cret",
    }, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try testing.expect(client.ready);
    try testing.expectEqual(@as(?[]const u8, null), stub.desync);
}

test "MODE READER rejection with 500 is not a failure" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        // Plenty of providers don't implement it. Treating this as fatal
        // would lock users out of those servers entirely.
        .{ .expect = "MODE READER", .reply = "500 unknown command\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.err);
    try testing.expect(client.ready);
}

test "bad credentials surface as AuthFailed, not a retry" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "AUTHINFO USER alice", .reply = "381 password required\r\n" },
        .{ .expect = "AUTHINFO PASS wrong", .reply = "481 authentication rejected\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .username = "alice",
        .password = "wrong",
    }, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    // Retrying bad credentials just gets the account locked, so this must
    // be distinguishable from a transient failure.
    try testing.expectEqual(@as(?Error, error.AuthFailed), client.err);
    try testing.expect(!client.ready);
}

test "greeting reporting too many connections is classified as such" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    // Providers signal this at greeting time with assorted codes, so the
    // text is what identifies it.
    const port = try stub.start(gpa, &loop, "502 too many connections from your IP\r\n", &.{});
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, error.TooManyConnections), client.err);
}

test "body fetch returns the unstuffed payload" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
        .{
            .expect = "BODY <a@b>",
            // Includes a stuffed dot line, which must come back as a
            // single leading dot.
            .reply = "222 0 <a@b> body\r\nline one\r\n..dotted\r\nline three\r\n.\r\n",
        },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);
    try testing.expect(client.ready);

    try client.conn.fetchBody("<a@b>");
    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.bodies.items.len > 0 or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.err);
    try testing.expectEqual(@as(usize, 1), client.bodies.items.len);
    try testing.expectEqualStrings("line one\n.dotted\nline three\n", client.bodies.items[0]);
    // Back to ready, so the pool can immediately reuse it.
    try testing.expect(client.conn.isReady());
}

test "missing article is ArticleMissing so the tier can move on" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
        .{ .expect = "BODY <gone@x>", .reply = "430 no such article\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try client.conn.fetchBody("<gone@x>");
    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.bodies.items.len > 0;
        }
    }.f);

    // 430 means "not here", which is a different decision from "try again
    // later" — the tiered fetcher moves to the next server on this.
    try testing.expectEqual(@as(?Error, error.ArticleMissing), client.err);
}

test "a body split across many reads reassembles" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Build a body far larger than the stub's write chunking, so it
    // necessarily arrives over many reads and the terminator has a real
    // chance of landing on a boundary.
    var reply: std.ArrayList(u8) = .empty;
    defer reply.deinit(gpa);
    try reply.appendSlice(gpa, "222 0 <big@x> body\r\n");
    var expected: std.ArrayList(u8) = .empty;
    defer expected.deinit(gpa);
    for (0..4000) |i| {
        var line: [64]u8 = undefined;
        const s = try std.fmt.bufPrint(&line, "line {d} of the article payload", .{i});
        try reply.appendSlice(gpa, s);
        try reply.appendSlice(gpa, "\r\n");
        try expected.appendSlice(gpa, s);
        try expected.append(gpa, '\n');
    }
    try reply.appendSlice(gpa, ".\r\n");

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
        .{ .expect = "BODY <big@x>", .reply = reply.items },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try client.conn.fetchBody("<big@x>");
    try pumpUntil(&loop, 20_000, &client, struct {
        fn f(c: *Client) bool {
            return c.bodies.items.len > 0 or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.err);
    try testing.expectEqualStrings(expected.items, client.bodies.items[0]);
}

test "two fetches on one connection, back to back" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
        .{ .expect = "BODY <one@x>", .reply = "222 0 <one@x>\r\nfirst\r\n.\r\n" },
        .{ .expect = "BODY <two@x>", .reply = "222 0 <two@x>\r\nsecond\r\n.\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try client.conn.fetchBody("<one@x>");
    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.bodies.items.len >= 1 or c.err != null;
        }
    }.f);

    // Connection reuse is the whole reason for the pool, so a second
    // fetch on the same connection has to work without a reconnect.
    try client.conn.fetchBody("<two@x>");
    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.bodies.items.len >= 2 or c.err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.err);
    try testing.expectEqualStrings("first\n", client.bodies.items[0]);
    try testing.expectEqualStrings("second\n", client.bodies.items[1]);
    try testing.expectEqual(@as(usize, 1), stub.accepted);
}

test "a peer that closes mid-body reports TruncatedBody" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
        // Status line and two lines, then no terminator and a close.
        .{ .expect = "BODY <cut@x>", .reply = "222 0 <cut@x>\r\npartial line\r\n" },
        .{ .expect = "", .reply = "", .then_close = true },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    try client.conn.fetchBody("<cut@x>");
    // Nudge the stub into closing.
    _ = try client.conn.stream.write("\r\n");

    try pumpUntil(&loop, 5000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.bodies.items.len > 0;
        }
    }.f);

    // Distinguishing a truncated article from a clean close matters:
    // one is worth retrying, the other means the connection is simply done.
    try testing.expectEqual(@as(?Error, error.TruncatedBody), client.err);
}

test "a command with no response times out rather than hanging" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    // Greeting, then silence forever.
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .timeout_ns = 80 * std.time.ns_per_ms,
    }, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 5000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    // A stalled provider must not pin a connection forever — this is what
    // the per-command reactor timer buys, with no watcher thread.
    try testing.expectEqual(@as(?Error, error.Timeout), client.err);
}

test "connecting to a dead port fails fast" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Bind then release, so nothing is listening.
    var probe: socket.Listener = undefined;
    try probe.listen(try IpAddress.parse("127.0.0.1", 0), struct {
        fn f(_: *socket.Listener, fd: sys.Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead = try probe.boundPort();
    probe.close();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", dead), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 3000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    try testing.expectEqual(@as(?Error, error.ConnectionRefused), client.err);
}

test "a rejected message id never reaches the wire" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try stub.start(gpa, &loop, greeting_ok, &.{
        .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
    });
    defer stub.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{}, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *Client) bool {
            return c.ready or c.err != null;
        }
    }.f);

    // A message id carrying CRLF would let an attacker inject an
    // arbitrary NNTP command. It has to be refused before the write, and
    // the connection must stay usable afterwards.
    try testing.expectError(error.InvalidMessageId, client.conn.fetchBody("<a@b>\r\nQUIT"));
    try testing.expect(client.conn.isReady());
    try testing.expectEqual(@as(?[]const u8, null), stub.desync);
}

// ---------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------
//
// What these do and do not prove, stated plainly so nobody reads more
// into them than is there.
//
// They cover: the fd being dialled at all when `security = .tls`, a real
// `std.crypto.tls.Client` ClientHello reaching a real socket with SNI in
// it, the mapping from a peer's refusal to a distinguishable NNTP error,
// the guarantee that no NNTP command — credentials included — is written
// before the handshake succeeds, the per-command deadline reaching a
// fiber parked mid-handshake, and the fiber's stack being released on
// every one of those paths.
//
// They do **not** cover a *completed* handshake. `std.crypto.tls` ships a
// client and no server, so nothing in-tree can shake hands, and no test
// below reaches ServerHello, the key schedule, certificate chain
// verification, or a plaintext byte over TLS.
//
// Those paths were exercised out of tree, by hand, against OpenSSL 3.4.1
// (`openssl s_server`, TLS 1.3) — see the recipe at the bottom of
// `transport.zig`. That run is what found the buffer sizing this file
// depends on, and it is not a substitute for a test: nothing in CI
// re-runs it. Treat "TLS works" as verified once, not as guarded.

const fiber_mod = @import("../posix/fiber.zig");

/// A peer that speaks raw bytes on a socket: it collects whatever the
/// client sends and answers with a fixed reply once anything arrives.
///
/// Deliberately not a TLS implementation. Everything above it — the
/// fiber, the parking transport, `std.crypto.tls.Client`'s record layer
/// and its error reporting — is the real thing; this is the wire.
const RawPeer = struct {
    listener: socket.Listener = undefined,
    source: reactor.Source = undefined,
    loop: *reactor.Loop,
    gpa: Allocator,
    fd: sys.Fd = sys.invalid_fd,

    /// Sent once the first byte arrives. Empty means "say nothing".
    reply: []const u8 = "",
    /// Hang up after replying.
    hang_up: bool = true,

    got: std.ArrayList(u8) = .empty,
    replied: bool = false,
    accepted: usize = 0,

    fn start(self: *RawPeer, gpa: Allocator, loop: *reactor.Loop, reply: []const u8, hang_up: bool) !u16 {
        self.* = .{ .loop = loop, .gpa = gpa, .reply = reply, .hang_up = hang_up };
        try self.listener.listen(try IpAddress.parse("127.0.0.1", 0), onAccept, 16);
        self.listener.context = self;
        try loop.add(&self.listener.source);
        return self.listener.boundPort();
    }

    fn deinit(self: *RawPeer) void {
        if (self.fd != sys.invalid_fd) {
            if (self.source.isRegistered()) self.loop.remove(&self.source);
            sys.close(self.fd);
            self.fd = sys.invalid_fd;
        }
        if (self.listener.source.isRegistered()) self.loop.remove(&self.listener.source);
        self.listener.close();
        self.got.deinit(self.gpa);
    }

    fn onAccept(l: *socket.Listener, fd: sys.Fd) void {
        const self: *RawPeer = @ptrCast(@alignCast(l.context.?));
        self.accepted += 1;
        if (self.fd != sys.invalid_fd) {
            sys.close(fd);
            return;
        }
        self.fd = fd;
        // Readable only. A connected socket is writable essentially
        // always, so registering both would spin the loop — the same
        // reason `socket.Stream` drops write interest when its queue
        // empties.
        self.source = .{ .fd = fd, .interest = .readable, .callback = onReady };
        self.loop.add(&self.source) catch {
            sys.close(fd);
            self.fd = sys.invalid_fd;
        };
    }

    fn onReady(src: *reactor.Source, ready: reactor.Ready) void {
        const self: *RawPeer = @fieldParentPtr("source", src);
        if (ready.read) {
            var buf: [16384]u8 = undefined;
            while (true) {
                const n = sys.read(src.fd, &buf) catch break;
                if (n == 0) break;
                self.got.appendSlice(self.gpa, buf[0..n]) catch break;
            }
        }
        if (self.got.items.len == 0 or self.replied) return;
        self.replied = true;
        if (self.reply.len > 0) sys.writeAll(src.fd, self.reply) catch {};
        if (self.hang_up) sys.shutdown(src.fd, .both);
    }
};

const tls_insecure: Config = .{
    .security = .{
        .tls = .{
            .host = "news.example.com",
            // Not the insecure mode by default: `std` omits SNI entirely when
            // host verification is off, and the SNI assertion below is half
            // the point of these tests.
            .trust = .self_signed_only,
        },
    },
};

test "a TLS server is dialled and gets a well-formed ClientHello with our SNI" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var peer: RawPeer = undefined;
    const port = try peer.start(gpa, &loop, "", true);
    defer peer.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), tls_insecure, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 10_000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    // The connection was actually made — the whole gap this closes is a
    // `tls = true` server never being dialled at all.
    try testing.expectEqual(@as(usize, 1), peer.accepted);

    const hello = peer.got.items;
    try testing.expect(hello.len > 64);
    // TLS record header: handshake content type, then the legacy record
    // version every TLS 1.3 ClientHello still carries.
    try testing.expectEqual(@as(u8, 0x16), hello[0]);
    try testing.expectEqual(@as(u8, 0x03), hello[1]);
    try testing.expectEqual(@as(u8, 0x01), hello[2]);
    try testing.expectEqual(@as(u8, 0x01), hello[5]); // client_hello
    // Without SNI a provider on a shared address hands back the wrong
    // certificate and every connection fails hostname verification.
    try testing.expect(std.mem.indexOf(u8, hello, "news.example.com") != null);

    // And the peer going away is reported rather than swallowed.
    try testing.expect(client.err != null);
    try testing.expect(!client.ready);
}

test "credentials never reach the wire before the handshake succeeds" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var peer: RawPeer = undefined;
    // Answer the ClientHello with a plausible NNTP greeting. A client that
    // had its state machine wired to the raw socket would take it, and
    // send AUTHINFO in the clear.
    const port = try peer.start(gpa, &loop, "200 news ready\r\n", true);
    defer peer.deinit();

    var cfg = tls_insecure;
    cfg.username = "alice";
    cfg.password = "s3cret";

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), cfg, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 10_000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    // This is the security property, not a nicety: everything after the
    // ClientHello is encrypted, so a password can never appear in what
    // the peer received.
    try testing.expect(std.mem.indexOf(u8, peer.got.items, "s3cret") == null);
    try testing.expect(std.mem.indexOf(u8, peer.got.items, "alice") == null);
    try testing.expect(std.mem.indexOf(u8, peer.got.items, "AUTHINFO") == null);
    try testing.expect(std.mem.indexOf(u8, peer.got.items, "MODE READER") == null);
    try testing.expect(!client.ready);
    try testing.expect(client.err != null);
}

test "a fatal alert from a TLS peer surfaces as TlsAlert with the peer's reason" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // A real, minimal TLS alert record: content type 21, version 3.3,
    // length 2, level fatal (2), description handshake_failure (40).
    const alert_record = [_]u8{ 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 };

    var peer: RawPeer = undefined;
    const port = try peer.start(gpa, &loop, &alert_record, true);
    defer peer.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), tls_insecure, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 10_000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    // The peer told us why it refused, and that is the single most useful
    // thing to put in a log line about a provider that will not connect.
    try testing.expectEqual(@as(?Error, error.TlsAlert), client.err);
    try testing.expect(client.conn.tlsAlert() != null);
    try testing.expectEqual(
        std.crypto.tls.Alert.Description.handshake_failure,
        client.conn.tlsAlert().?.description,
    );
}

test "a plaintext NNTP server on the TLS port is a protocol error, not a hang" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // The classic misconfiguration: 119 typed where 563 was meant. The
    // greeting is a perfectly good NNTP line and complete nonsense as a
    // TLS record.
    var peer: RawPeer = undefined;
    const port = try peer.start(gpa, &loop, "200 news.example.invalid ready\r\n", true);
    defer peer.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), tls_insecure, &Client.handler);
    defer client.deinit();

    try pumpUntil(&loop, 10_000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    try testing.expect(!client.ready);
    // Not a certificate problem: pointing the operator at their roots
    // when they typed the wrong port wastes an evening.
    try testing.expectEqual(@as(?Error, error.TlsProtocolError), client.err);
    try testing.expect(client.conn.tlsDetail() != null);
}

test "a TLS peer that never answers hits the per-command deadline" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const before = fiber_mod.liveStacks();

    // Accepts, reads the ClientHello, and says nothing ever again. The
    // session fiber is parked inside `Client.init` when the timer fires,
    // which is the case a deadline has to be able to reach.
    var peer: RawPeer = undefined;
    const port = try peer.start(gpa, &loop, "", false);
    defer peer.deinit();

    var cfg = tls_insecure;
    cfg.timeout_ns = 120 * std.time.ns_per_ms;

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), cfg, &Client.handler);

    try pumpUntil(&loop, 10_000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    // A stalled provider must not pin a connection forever, and a TLS one
    // is the easiest to stall: the handshake is several round trips
    // before a single byte of NNTP.
    try testing.expectEqual(@as(?Error, error.Timeout), client.err);

    client.deinit();
    // The fiber was parked when the deadline fired; it has to have been
    // unwound and its 1 MiB mapping released, not abandoned.
    try testing.expectEqual(before, fiber_mod.liveStacks());
}

test "a TLS connect to a dead port fails fast and frees its stack" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const before = fiber_mod.liveStacks();

    // Bind then release, so nothing is listening.
    var probe: socket.Listener = undefined;
    try probe.listen(try IpAddress.parse("127.0.0.1", 0), struct {
        fn f(_: *socket.Listener, fd: sys.Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead = try probe.boundPort();
    probe.close();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", dead), tls_insecure, &Client.handler);

    try pumpUntil(&loop, 5000, &client, struct {
        fn f(c: *Client) bool {
            return c.err != null or c.ready;
        }
    }.f);

    // The TCP failure has to survive the trip out through the fiber
    // unchanged; reporting it as a TLS problem would send the operator
    // looking at certificates for a closed port.
    try testing.expectEqual(@as(?Error, error.ConnectionRefused), client.err);

    client.deinit();
    try testing.expectEqual(before, fiber_mod.liveStacks());
}

test "a TLS connection that is torn down mid-handshake leaks no stack" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const before = fiber_mod.liveStacks();

    var peer: RawPeer = undefined;
    const port = try peer.start(gpa, &loop, "", false);
    defer peer.deinit();

    var client: Client = .{ .gpa = gpa };
    try client.conn.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), tls_insecure, &Client.handler);

    // Let the handshake get as far as parking on the peer's silence,
    // then destroy the connection under it — which is what the pool's
    // reaper and a shutdown both do.
    try pumpUntil(&loop, 5000, &peer, struct {
        fn f(p: *RawPeer) bool {
            return p.got.items.len > 0;
        }
    }.f);
    try testing.expect(!client.ready);

    client.deinit();
    try testing.expectEqual(before, fiber_mod.liveStacks());
    // And nothing is left armed to wake the loop.
    try testing.expect(!client.conn.timer.isArmed());
}

test "a TLS pool connection reports its failure through the pool's accounting" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const before = fiber_mod.liveStacks();

    const alert_record = [_]u8{ 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 };
    var peer: RawPeer = undefined;
    const port = try peer.start(gpa, &loop, &alert_record, true);
    defer peer.deinit();

    // The path that matters in production: the failure is raised from
    // inside the session fiber, and the handler it reaches destroys the
    // connection — and with it the fiber's own stack. Doing that from a
    // frame on that stack is a `munmap` of the caller, which is why the
    // report is deferred to a timer.
    const pool_mod = @import("pool.zig");
    var pool: pool_mod.Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .max_connections = 2,
        .conn = tls_insecure,
    });
    defer pool.deinit();

    const Sink = struct {
        var got: ?anyerror = null;
        fn cb(_: ?*anyopaque, result: pool_mod.Error!*Conn) void {
            _ = result catch |e| {
                got = e;
                return;
            };
        }
    };
    Sink.got = null;
    pool.acquire(Sink.cb, null);

    try pumpUntil(&loop, 10_000, &pool, struct {
        fn f(p: *pool_mod.Pool) bool {
            _ = p;
            return Sink.got != null;
        }
    }.f);

    try testing.expectEqual(@as(?anyerror, error.TlsAlert), Sink.got);
    // The slot came back, so a retry is not blocked by a ghost.
    try testing.expectEqual(@as(u32, 0), pool.openCount());
    try testing.expectEqual(before, fiber_mod.liveStacks());
}
