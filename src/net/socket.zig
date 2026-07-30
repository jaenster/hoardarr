//! TCP on top of the reactor.
//!
//! Two types, both intrusive `reactor.Source` wrappers:
//!
//!   * `Listener` — a bound, listening socket that hands accepted fds to
//!     a callback.
//!   * `Stream` — a connected socket with an outbound queue.
//!
//! ## Why the output queue exists
//!
//! Everything here is non-blocking, so `write` can accept fewer bytes
//! than offered whenever the peer's window is full. The wrong answers are
//! to loop until it all goes (which blocks the loop, and with it every
//! other connection) or to drop the remainder (which corrupts the
//! stream). So a short write parks the tail in `out` and registers write
//! interest; when the kernel says writable we drain and, once empty,
//! *deregister* write interest again.
//!
//! That last step is the one that matters for idle CPU. A connected TCP
//! socket with room in its send buffer is writable essentially always, so
//! leaving write interest registered means every single `poll` returns
//! immediately and the loop spins at 100% of a core doing nothing. The
//! reactor has a test pinning this exact behaviour
//! (`dropping write interest stops the writable storm`).
//!
//! ## Why callbacks rather than blocking reads
//!
//! A blocking-read API would need a thread or a coroutine per connection.
//! With 40 provider connections plus HTTP clients that's either 50 stacks
//! of committed memory or a scheduler we'd have to write. Callbacks keep
//! the whole daemon on one thread with one stack, which is also why the
//! idle number is what it is.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");

const Allocator = std.mem.Allocator;
const Fd = sys.Fd;
const IpAddress = std.Io.net.IpAddress;

pub const Error = Allocator.Error || sys.Error;

/// Default listen backlog. 128 is the conventional value and comfortably
/// more than a homelab UI plus a handful of API clients will ever queue.
pub const default_backlog: u31 = 128;

// ---------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------

pub const Listener = struct {
    source: reactor.Source,
    /// Called once per accepted connection. The callee owns `fd` and must
    /// close it — including on its own error paths, since the listener has
    /// already forgotten about it by then.
    on_accept: *const fn (self: *Listener, fd: Fd) void,
    /// Non-fatal accept failures land here (per-connection problems like
    /// ECONNABORTED, or EMFILE under fd pressure). Left null means "log
    /// nothing and carry on".
    on_error: ?*const fn (self: *Listener, err: sys.Error) void = null,
    /// User data, so a listener can find its owning server.
    context: ?*anyopaque = null,

    /// Bind and listen. Initialises in place, because the reactor stores
    /// `&self.source` and a by-value return would leave it dangling.
    pub fn listen(
        self: *Listener,
        addr: IpAddress,
        on_accept: *const fn (self: *Listener, fd: Fd) void,
        backlog: u31,
    ) sys.Error!void {
        var sa = sys.Sockaddr.fromIp(addr);
        const fd = try sys.socket(sa.family(), sys.SOCK_STREAM, 0);
        errdefer sys.close(fd);

        // Without SO_REUSEADDR a restart fails for the length of TIME_WAIT
        // on the old listener, which for a container that gets recreated
        // on every config change is a guaranteed bad first impression.
        try sys.setReuseAddr(fd);
        try sys.bind(fd, &sa);
        try sys.listen(fd, backlog);

        self.* = .{
            .source = .{ .fd = fd, .interest = .readable, .callback = onReady },
            .on_accept = on_accept,
        };
    }

    pub fn close(self: *Listener) void {
        sys.close(self.source.fd);
        self.source.fd = sys.invalid_fd;
    }

    /// The port actually bound. Only interesting when the caller passed
    /// port 0 and let the kernel choose.
    pub fn boundPort(self: *const Listener) sys.Error!u16 {
        const sa = try sys.getsockname(self.source.fd);
        return sa.port();
    }

    fn onReady(src: *reactor.Source, ready: reactor.Ready) void {
        const self: *Listener = @fieldParentPtr("source", src);
        if (!ready.read) return;

        // Drain the backlog rather than accepting one per wakeup.
        // Level-triggered readiness would bring us back either way, but
        // one accept per `poll` round means a burst of connections costs
        // a syscall pair each; this costs one extra `accept` that returns
        // EAGAIN. The loop is bounded so one very busy listener can't
        // starve the other sources in this tick.
        var budget: usize = 64;
        while (budget > 0) : (budget -= 1) {
            const fd = sys.accept(src.fd) catch |err| switch (err) {
                error.WouldBlock => return,
                // Connection died between the SYN and our accept. Common
                // and entirely uninteresting.
                error.ConnectionAborted => continue,
                else => {
                    if (self.on_error) |f| f(self, err);
                    return;
                },
            };
            self.on_accept(self, fd);
        }
    }
};

// ---------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------

/// What a `Stream` tells its owner.
pub const Handler = struct {
    /// Readable. Call `stream.read(buf)`; a return of 0 means the peer
    /// closed and the owner should tear the stream down.
    on_readable: *const fn (stream: *Stream) void,
    /// The outbound queue just went empty. Optional; useful for a
    /// half-close-after-response pattern.
    on_drained: ?*const fn (stream: *Stream) void = null,
    /// The peer hung up or the socket errored. The stream is unusable
    /// after this; the owner must call `deinit`.
    on_close: *const fn (stream: *Stream, err: ?Error) void,
    /// A pending non-blocking connect finished. Only used by `connect`.
    on_connected: ?*const fn (stream: *Stream, err: ?Error) void = null,
};

pub const Stream = struct {
    source: reactor.Source,
    loop: *reactor.Loop,
    gpa: Allocator,
    handler: *const Handler,

    /// Bytes accepted from the owner that the kernel wouldn't take yet.
    out: std.ArrayList(u8) = .empty,
    /// How far into `out` we've drained. Draining from the front by
    /// consuming this cursor avoids memmoving the remainder on every
    /// partial write; `out` is compacted only when the cursor catches up.
    out_pos: usize = 0,

    state: State = .open,
    context: ?*anyopaque = null,

    /// Points at a stack flag owned by the in-progress `onReady`, when
    /// there is one. `deinit` clears it, which is how dispatch learns that
    /// a handler destroyed this stream.
    ///
    /// A handler is allowed to tear its own connection down — the NNTP
    /// pool does exactly that when a provider answers 430 — and once it
    /// has, `self` is freed memory. Reading `self.state` afterwards to
    /// decide whether to continue is a use-after-free, and it was one:
    /// it segfaulted on the 430 path, which on Usenet is the *ordinary*
    /// path, not an error case. The flag lives on the dispatching
    /// frame's stack precisely so it survives the object it describes.
    alive_guard: ?*bool = null,

    pub const State = enum {
        /// Non-blocking connect in flight; waiting for writability.
        connecting,
        open,
        /// Peer closed or we errored. No further I/O.
        closed,
    };

    /// Cap on queued outbound bytes. Past this, `write` reports
    /// `error.SystemResources` rather than growing without bound — an
    /// unbounded queue in front of a peer that has stopped reading is how
    /// a daemon OOMs from a single stalled client.
    pub const max_queued: usize = 8 << 20;

    /// Adopt an already-connected fd, e.g. one from `Listener.on_accept`.
    /// Takes ownership: `deinit` closes it.
    pub fn initAccepted(
        self: *Stream,
        gpa: Allocator,
        loop: *reactor.Loop,
        fd: Fd,
        handler: *const Handler,
    ) Error!void {
        self.* = .{
            .source = .{ .fd = fd, .interest = .readable, .callback = onReady },
            .loop = loop,
            .gpa = gpa,
            .handler = handler,
            .state = .open,
        };
        try loop.add(&self.source);
    }

    /// Start a non-blocking connect. `handler.on_connected` fires with
    /// null on success or the error on failure.
    pub fn connect(
        self: *Stream,
        gpa: Allocator,
        loop: *reactor.Loop,
        addr: IpAddress,
        handler: *const Handler,
    ) Error!void {
        var sa = sys.Sockaddr.fromIp(addr);
        const fd = try sys.socket(sa.family(), sys.SOCK_STREAM, 0);
        errdefer sys.close(fd);

        // On a non-blocking socket this is expected to report EINPROGRESS;
        // anything else means it resolved immediately (loopback, usually).
        var connecting = true;
        sys.connect(fd, &sa) catch |err| switch (err) {
            error.InProgress, error.WouldBlock => {},
            error.AlreadyConnected => connecting = false,
            else => return err,
        };

        self.* = .{
            .source = .{
                .fd = fd,
                // Connect completion shows up as writability either way,
                // success or refusal; `sys.socketError` distinguishes them.
                .interest = if (connecting) .writable else .readable,
                .callback = onReady,
            },
            .loop = loop,
            .gpa = gpa,
            .handler = handler,
            .state = if (connecting) .connecting else .open,
        };
        try loop.add(&self.source);
    }

    pub fn deinit(self: *Stream) void {
        // Tell any dispatch frame above us that we are gone, before we
        // actually go.
        if (self.alive_guard) |g| {
            g.* = false;
            self.alive_guard = null;
        }
        if (self.source.isRegistered()) self.loop.remove(&self.source);
        if (self.source.fd != sys.invalid_fd) {
            sys.close(self.source.fd);
            self.source.fd = sys.invalid_fd;
        }
        self.out.deinit(self.gpa);
        self.state = .closed;
    }

    /// Read straight into the caller's buffer. Returns 0 on peer close.
    /// `error.WouldBlock` means the owner has drained everything available
    /// and should return to the loop.
    pub fn read(self: *Stream, buf: []u8) sys.Error!usize {
        if (self.state == .closed) return error.NotConnected;
        return sys.read(self.source.fd, buf);
    }

    /// Queue `bytes` for sending.
    ///
    /// Tries the socket directly first when nothing is already queued —
    /// the overwhelmingly common case is that it all goes, and that path
    /// costs one syscall and touches no heap. Only the remainder is
    /// copied into `out`, and only then do we ask about writability.
    pub fn write(self: *Stream, bytes: []const u8) Error!void {
        if (self.state != .open) return error.NotConnected;
        if (bytes.len == 0) return;

        var rest = bytes;
        if (self.pending() == 0) {
            // Nothing queued, so ordering lets us go straight to the
            // kernel. With a non-empty queue we must not, or this write
            // would overtake the bytes already waiting.
            const n = sys.write(self.source.fd, rest) catch |err| switch (err) {
                error.WouldBlock => 0,
                else => |e| return e,
            };
            rest = rest[n..];
            if (rest.len == 0) return;
        }

        if (self.pending() + rest.len > max_queued) return error.SystemResources;
        try self.out.appendSlice(self.gpa, rest);
        try self.loop.modify(&self.source, .both);
    }

    /// Queued-but-unsent byte count. Useful as a backpressure signal:
    /// a producer can stop generating while this is large.
    pub fn pending(self: *const Stream) usize {
        return self.out.items.len - self.out_pos;
    }

    /// Send a FIN once the queue drains, then keep reading. This is how
    /// an HTTP response with `Connection: close` ends without discarding
    /// a request the peer already sent.
    pub fn shutdownWrite(self: *Stream) void {
        if (self.pending() == 0) sys.shutdown(self.source.fd, .write);
    }

    fn onReady(src: *reactor.Source, ready: reactor.Ready) void {
        const self: *Stream = @fieldParentPtr("source", src);

        // Every handler below may destroy this stream. `alive` lives on
        // *this* frame, so it stays readable after `self` does not; the
        // rule for the rest of this function is that nothing touches
        // `self` again without checking it first.
        var alive = true;
        const outer_guard = self.alive_guard;
        self.alive_guard = &alive;
        defer if (alive) {
            self.alive_guard = outer_guard;
        };

        if (self.state == .connecting) {
            self.finishConnect(ready);
            return;
        }

        // Writability first: draining the queue may free the owner to
        // produce more, and doing it before the read keeps a
        // request/response exchange to one trip through the loop.
        if (ready.write) {
            self.drain() catch |err| {
                self.fail(err);
                return;
            };
            if (!alive) return;
            if (self.state == .closed) return;
        }

        if (ready.read) {
            self.handler.on_readable(self);
            if (!alive) return;
            if (self.state == .closed) return;
        }

        // Terminal conditions are handled last so any data the peer sent
        // before hanging up is delivered first. A socket can be readable
        // and hung up in the same event, and dropping the payload because
        // of the HUP loses the final response.
        if (ready.terminal()) {
            self.state = .closed;
            self.handler.on_close(self, if (ready.err) self.pendingError() else null);
        }
    }

    fn finishConnect(self: *Stream, ready: reactor.Ready) void {
        // A refused connect reports the socket writable too, so
        // readiness alone proves nothing; SO_ERROR is the answer.
        if (sys.socketError(self.source.fd)) |_| {
            self.state = .open;
            // Reads are what we want now; write interest comes back only
            // when there's something queued.
            self.loop.modify(&self.source, .readable) catch |err| {
                self.fail(err);
                return;
            };
            if (self.handler.on_connected) |f| f(self, null);
        } else |err| {
            self.state = .closed;
            if (self.handler.on_connected) |f| f(self, err) else self.handler.on_close(self, err);
        }
        _ = ready;
    }

    fn drain(self: *Stream) Error!void {
        while (self.pending() > 0) {
            const chunk = self.out.items[self.out_pos..];
            const n = sys.write(self.source.fd, chunk) catch |err| switch (err) {
                error.WouldBlock => return, // still full; stay registered
                else => |e| return e,
            };
            if (n == 0) return;
            self.out_pos += n;
        }

        // Queue empty. Reclaim the buffer and — the important part — stop
        // asking about writability, or the loop spins.
        self.out.clearRetainingCapacity();
        self.out_pos = 0;
        try self.loop.modify(&self.source, .readable);
        if (self.handler.on_drained) |f| f(self);
    }

    /// The pending socket error, or a generic reset if we can't tell.
    fn pendingError(self: *Stream) sys.Error {
        sys.socketError(self.source.fd) catch |err| return err;
        return error.ConnectionReset;
    }

    /// Takes the wide `Error` rather than `sys.Error`: the failure paths
    /// that route here include `loop.modify`, which allocates in the poll
    /// backend and so can report OutOfMemory.
    fn fail(self: *Stream, err: Error) void {
        self.state = .closed;
        self.handler.on_close(self, err);
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Loopback address on a kernel-chosen port. Binding to a fixed port in a
/// test races every other test and any process on the developer's machine.
fn loopbackAny() IpAddress {
    return IpAddress.parse("127.0.0.1", 0) catch unreachable;
}

/// A server that echoes everything it receives, used as the far end for
/// the client-side tests.
const EchoServer = struct {
    listener: Listener,
    loop: *reactor.Loop,
    gpa: Allocator,
    conns: std.ArrayList(*Conn) = .empty,
    accepted: usize = 0,

    const Conn = struct {
        stream: Stream,
        server: *EchoServer,
        buf: [4096]u8 = undefined,

        const handler: Handler = .{
            .on_readable = onReadable,
            .on_close = onClose,
        };

        fn onReadable(s: *Stream) void {
            const self: *Conn = @fieldParentPtr("stream", s);
            while (true) {
                const n = s.read(&self.buf) catch |err| switch (err) {
                    error.WouldBlock => return,
                    else => {
                        s.state = .closed;
                        return;
                    },
                };
                if (n == 0) {
                    s.state = .closed;
                    return;
                }
                s.write(self.buf[0..n]) catch {
                    s.state = .closed;
                    return;
                };
            }
        }

        fn onClose(s: *Stream, _: ?Error) void {
            s.state = .closed;
        }
    };

    fn start(self: *EchoServer, gpa: Allocator, loop: *reactor.Loop) !u16 {
        self.* = .{ .listener = undefined, .loop = loop, .gpa = gpa };
        try self.listener.listen(loopbackAny(), onAccept, default_backlog);
        self.listener.context = self;
        try loop.add(&self.listener.source);
        return self.listener.boundPort();
    }

    fn deinit(self: *EchoServer) void {
        for (self.conns.items) |c| {
            c.stream.deinit();
            self.gpa.destroy(c);
        }
        self.conns.deinit(self.gpa);
        self.loop.remove(&self.listener.source);
        self.listener.close();
    }

    fn onAccept(l: *Listener, fd: Fd) void {
        const self: *EchoServer = @ptrCast(@alignCast(l.context.?));
        self.accepted += 1;
        const conn = self.gpa.create(Conn) catch {
            sys.close(fd);
            return;
        };
        conn.* = .{ .stream = undefined, .server = self };
        conn.stream.initAccepted(self.gpa, self.loop, fd, &Conn.handler) catch {
            sys.close(fd);
            self.gpa.destroy(conn);
            return;
        };
        self.conns.append(self.gpa, conn) catch {
            conn.stream.deinit();
            self.gpa.destroy(conn);
        };
    }
};

/// A client that connects, sends a payload, and accumulates the reply.
const EchoClient = struct {
    stream: Stream,
    gpa: Allocator,
    got: std.ArrayList(u8) = .empty,
    connected: bool = false,
    connect_err: ?Error = null,
    closed: bool = false,
    buf: [4096]u8 = undefined,
    send_on_connect: []const u8 = "",

    const handler: Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
        .on_connected = onConnected,
    };

    fn deinit(self: *EchoClient) void {
        self.stream.deinit();
        self.got.deinit(self.gpa);
    }

    fn onConnected(s: *Stream, err: ?Error) void {
        const self: *EchoClient = @fieldParentPtr("stream", s);
        self.connect_err = err;
        if (err != null) return;
        self.connected = true;
        if (self.send_on_connect.len > 0) {
            s.write(self.send_on_connect) catch |e| {
                self.connect_err = e;
            };
        }
    }

    fn onReadable(s: *Stream) void {
        const self: *EchoClient = @fieldParentPtr("stream", s);
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

    fn onClose(s: *Stream, _: ?Error) void {
        const self: *EchoClient = @fieldParentPtr("stream", s);
        self.closed = true;
    }
};

/// Pump the loop until `done` or the deadline. Tests must never spin
/// forever on a bug, and must never depend on a fixed number of ticks —
/// how many wakeups an exchange takes is a kernel scheduling detail.
fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) {
            return error.TestTimeout;
        }
        _ = try loop.tick(5);
    }
}

test "listener reports the port the kernel chose" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    try testing.expect(port != 0);
}

test "connect, echo round-trip, and peer close" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    var client: EchoClient = .{ .stream = undefined, .gpa = gpa, .send_on_connect = "hello reactor" };
    try client.stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), &EchoClient.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *EchoClient) bool {
            return c.got.items.len >= "hello reactor".len or c.connect_err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.connect_err);
    try testing.expect(client.connected);
    try testing.expectEqualStrings("hello reactor", client.got.items);
    try testing.expectEqual(@as(usize, 1), server.accepted);
}

test "connect to a closed port surfaces ConnectionRefused, not a hang" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Bind and immediately release a port, then connect to it. Nothing is
    // listening, so the kernel must RST rather than leaving us pending —
    // this is the path that distinguishes "writable" from "connected".
    var probe: Listener = undefined;
    try probe.listen(loopbackAny(), struct {
        fn f(_: *Listener, fd: Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead_port = try probe.boundPort();
    probe.close();

    var client: EchoClient = .{ .stream = undefined, .gpa = gpa };
    try client.stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", dead_port), &EchoClient.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *EchoClient) bool {
            return c.connect_err != null or c.connected;
        }
    }.f);

    try testing.expect(!client.connected);
    try testing.expectEqual(@as(?Error, error.ConnectionRefused), client.connect_err);
}

test "a write larger than the socket buffer is queued and fully delivered" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    // 4 MiB is far beyond any socket send buffer, so this exercises the
    // short-write path repeatedly rather than incidentally.
    const size = 4 << 20;
    const payload = try gpa.alloc(u8, size);
    defer gpa.free(payload);
    var prng = std.Random.DefaultPrng.init(0xA11CE);
    prng.random().bytes(payload);

    var client: EchoClient = .{ .stream = undefined, .gpa = gpa, .send_on_connect = payload };
    try client.stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), &EchoClient.handler);
    defer client.deinit();

    try pumpUntil(&loop, 30_000, &client, struct {
        fn f(c: *EchoClient) bool {
            return c.got.items.len >= c.send_on_connect.len or c.connect_err != null;
        }
    }.f);

    try testing.expectEqual(@as(?Error, null), client.connect_err);
    try testing.expectEqual(size, client.got.items.len);
    // Byte-exact, because a queue that reorders or drops on a partial
    // write would still produce the right *length* under an echo.
    try testing.expect(std.mem.eql(u8, payload, client.got.items));
}

test "write interest is dropped once the queue drains" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    var client: EchoClient = .{ .stream = undefined, .gpa = gpa, .send_on_connect = "x" ** 64 };
    try client.stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), &EchoClient.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *EchoClient) bool {
            return c.got.items.len >= 64;
        }
    }.f);

    // This is the idle-CPU invariant at the socket level: with nothing
    // queued the stream must not be listening for writability, because a
    // connected socket is almost always writable and the loop would spin.
    try testing.expectEqual(@as(usize, 0), client.stream.pending());
    try testing.expect(!client.stream.source.interest.write);
    try testing.expect(client.stream.source.interest.read);
}

test "queue over the cap is refused rather than growing without bound" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // A listener that accepts and then never reads: the peer's window
    // fills, our writes start short, and the queue grows. That's the
    // stalled-client shape the cap exists for.
    var stalled: Listener = undefined;
    var held: Fd = sys.invalid_fd;
    const Holder = struct {
        var slot: *Fd = undefined;
        fn f(_: *Listener, fd: Fd) void {
            slot.* = fd;
        }
    };
    Holder.slot = &held;
    try stalled.listen(loopbackAny(), Holder.f, default_backlog);
    const port = try stalled.boundPort();
    try loop.add(&stalled.source);
    defer {
        loop.remove(&stalled.source);
        stalled.close();
        if (held != sys.invalid_fd) sys.close(held);
    }

    var client: EchoClient = .{ .stream = undefined, .gpa = gpa };
    try client.stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), &EchoClient.handler);
    defer client.deinit();

    try pumpUntil(&loop, 2000, &client, struct {
        fn f(c: *EchoClient) bool {
            return c.connected or c.connect_err != null;
        }
    }.f);
    try testing.expect(client.connected);

    // Push until the cap trips. Each write is a chunk well under the cap,
    // so tripping it proves the *cumulative* queue is bounded.
    const chunk = try gpa.alloc(u8, 1 << 20);
    defer gpa.free(chunk);
    @memset(chunk, 'q');

    var refused = false;
    for (0..64) |_| {
        client.stream.write(chunk) catch |err| {
            try testing.expectEqual(error.SystemResources, err);
            refused = true;
            break;
        };
    }
    try testing.expect(refused);
    try testing.expect(client.stream.pending() <= Stream.max_queued);
}

test "many concurrent connections are each routed to their own stream" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    // Distinct payload per client, so a routing bug shows up as the wrong
    // bytes rather than merely the wrong count.
    const n = 32;
    const clients = try gpa.alloc(EchoClient, n);
    defer gpa.free(clients);
    const payloads = try gpa.alloc([]u8, n);
    defer {
        for (payloads) |p| gpa.free(p);
        gpa.free(payloads);
    }

    for (clients, payloads, 0..) |*c, *p, i| {
        p.* = try std.fmt.allocPrint(gpa, "client-{d:0>4}-payload", .{i});
        c.* = .{ .stream = undefined, .gpa = gpa, .send_on_connect = p.* };
        try c.stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), &EchoClient.handler);
    }
    defer for (clients) |*c| c.deinit();

    const Ctx = struct { clients: []EchoClient, payloads: [][]u8 };
    var ctx = Ctx{ .clients = clients, .payloads = payloads };
    try pumpUntil(&loop, 30_000, &ctx, struct {
        fn f(c: *Ctx) bool {
            for (c.clients, c.payloads) |*cl, p| {
                if (cl.got.items.len < p.len) return false;
            }
            return true;
        }
    }.f);

    for (clients, payloads) |*c, p| {
        try testing.expectEqualStrings(p, c.got.items);
    }
    try testing.expectEqual(@as(usize, n), server.accepted);
}

test "listener drains a burst of connections without one accept per tick" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    // Queue several connections in the backlog *before* the loop runs,
    // then tick once. The accept loop should take them all in that single
    // wakeup.
    const n = 8;
    var fds: [n]Fd = undefined;
    const addr = sys.Sockaddr.fromIp(try IpAddress.parse("127.0.0.1", port));
    for (&fds) |*fd| {
        fd.* = try sys.socket(sys.AF_INET, sys.SOCK_STREAM, 0);
        sys.connect(fd.*, &addr) catch |err| switch (err) {
            error.InProgress, error.WouldBlock, error.AlreadyConnected => {},
            else => return err,
        };
    }
    defer for (fds) |fd| sys.close(fd);

    // Give the kernel a moment to complete the loopback handshakes.
    sys.sleep(50 * std.time.ns_per_ms);
    _ = try loop.tick(50);

    try testing.expectEqual(@as(usize, n), server.accepted);
}

test "a handler may destroy its own stream from inside the callback" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: EchoServer = undefined;
    const port = try server.start(gpa, &loop);
    defer server.deinit();

    // This is what the NNTP pool does when a provider answers 430: the
    // read handler decides the connection is finished and tears it down
    // right there. Everything after that callback in `onReady` is then
    // touching freed memory — which segfaulted on the 430 path, the
    // ordinary path on Usenet rather than an error case.
    const Suicidal = struct {
        stream: *Stream,
        gpa: Allocator,
        fired: usize = 0,
        buf: [256]u8 = undefined,

        const handler: Handler = .{
            .on_readable = onReadable,
            .on_close = onClose,
            .on_connected = onConnected,
        };

        fn onConnected(s: *Stream, err: ?Error) void {
            _ = err;
            s.write("trigger a reply") catch {};
        }

        fn onReadable(s: *Stream) void {
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            self.fired += 1;
            _ = s.read(&self.buf) catch {};
            // Destroy the stream from inside its own dispatch, then free
            // the storage it lived in — the strongest form of the hazard.
            s.deinit();
            self.gpa.destroy(s);
            self.stream = undefined;
        }

        fn onClose(s: *Stream, _: ?Error) void {
            s.state = .closed;
        }
    };

    const stream = try gpa.create(Stream);
    var owner = Suicidal{ .stream = stream, .gpa = gpa };
    try stream.connect(gpa, &loop, try IpAddress.parse("127.0.0.1", port), &Suicidal.handler);
    stream.context = &owner;

    try pumpUntil(&loop, 3000, &owner, struct {
        fn f(o: *Suicidal) bool {
            return o.fired > 0;
        }
    }.f);

    // Reaching here at all is the assertion: before the liveness guard
    // this dereferenced freed memory immediately after the callback.
    try testing.expectEqual(@as(usize, 1), owner.fired);

    // And the loop is still healthy afterwards.
    _ = try loop.tick(10);
}

test "a stream destroyed while both readable and hung up does not resurface" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // A socket can be readable and hung up in the same event. If the read
    // handler frees the stream, the terminal branch must not then run
    // against it — that is the second dereference in the same dispatch.
    const p = try sys.pipe();
    defer sys.close(p.read_end);

    const Freer = struct {
        stream: *Stream,
        gpa: Allocator,
        fired: usize = 0,

        const handler: Handler = .{
            .on_readable = onReadable,
            .on_close = onClose,
        };

        fn onReadable(s: *Stream) void {
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            self.fired += 1;
            var b: [64]u8 = undefined;
            _ = s.read(&b) catch {};
            s.deinit();
            self.gpa.destroy(s);
        }

        fn onClose(s: *Stream, _: ?Error) void {
            // Must never run: the stream was freed in on_readable.
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            self.fired += 1000;
        }
    };

    const stream = try gpa.create(Stream);
    var owner = Freer{ .stream = stream, .gpa = gpa };
    try stream.initAccepted(gpa, &loop, p.read_end, &Freer.handler);
    stream.context = &owner;

    // Write then close the far end, so the same event carries both.
    _ = try sys.write(p.write_end, "bye");
    sys.close(p.write_end);

    try pumpUntil(&loop, 3000, &owner, struct {
        fn f(o: *Freer) bool {
            return o.fired > 0;
        }
    }.f);

    // Exactly one dispatch, and on_close never fired against dead memory.
    try testing.expectEqual(@as(usize, 1), owner.fired);
}
