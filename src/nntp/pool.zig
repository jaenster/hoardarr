//! A bounded pool of NNTP connections to one provider.
//!
//! ## What the bound is for
//!
//! Providers sell connection slots and enforce the limit by refusing the
//! excess, so exceeding it doesn't get you more throughput — it gets you a
//! stream of "too many connections" errors that look like the provider is
//! broken. The cap is therefore a hard invariant here, counted across
//! idle, in-use *and* still-connecting connections. Counting only the
//! established ones is the classic bug: forty simultaneous acquires each
//! see "0 open, cap 8" and dial forty sockets.
//!
//! ## Why acquire is a callback rather than a return value
//!
//! Getting a connection can require dialing and a full handshake, which is
//! several network round trips. On a single-threaded reactor there is
//! nothing to block on, so `acquire` takes a continuation: it fires
//! immediately when an idle connection is available, and after the
//! handshake when one had to be created. Waiters queue in FIFO order so a
//! busy job can't starve a newer one indefinitely.
//!
//! ## Reaping
//!
//! Idle connections are closed after a TTL, because providers drop them
//! silently and a connection that has been idle for ten minutes is
//! frequently already dead — discovering that on the next fetch costs a
//! failed segment and a retry. The Go version ran a reaper goroutine with
//! a ticker; here one reactor timer is armed only while at least one
//! connection is idle, so an empty pool costs nothing at all.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const conn_mod = @import("conn.zig");

const Allocator = std.mem.Allocator;
const IpAddress = std.Io.net.IpAddress;
const Conn = conn_mod.Conn;

pub const Error = conn_mod.Error || error{
    /// `close` has been called; no further acquires are served.
    PoolClosed,
    /// More waiters than `max_waiters`. Refusing is better than an
    /// unbounded queue that turns provider downtime into an OOM.
    TooManyWaiters,
};

pub const Options = struct {
    /// Hard cap on connections, counted including those still connecting.
    /// This is the number the provider sold you.
    max_connections: u32 = 8,
    /// Close an idle connection after this long. Providers drop idle
    /// connections silently, so holding one for ten minutes usually means
    /// discovering it's dead on the next fetch.
    idle_ttl_ns: u64 = 60 * std.time.ns_per_s,
    /// How often to look for expired idle connections. Only armed while
    /// something is idle.
    reap_interval_ns: u64 = 15 * std.time.ns_per_s,
    /// Cap on queued acquire requests.
    max_waiters: u32 = 1024,
    conn: conn_mod.Config = .{},
};

/// Called when a connection becomes available, or when it can't be.
///
/// On success the callee owns the connection until it calls
/// `Pool.release`. Not releasing it leaks a slot, which is why release is
/// exercised on every path in the tests.
pub const AcquireFn = *const fn (ctx: ?*anyopaque, result: Error!*Conn) void;

const Waiter = struct {
    callback: AcquireFn,
    ctx: ?*anyopaque,
};

pub const Pool = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    addr: IpAddress,
    options: Options,

    /// Connections not currently checked out, most-recently-used last.
    /// LIFO on reuse: the most recently used connection is the one most
    /// likely to still be alive, so it gets picked first and the cold ones
    /// age out into the reaper.
    idle: std.ArrayList(*Entry) = .empty,
    /// Every live connection, idle or not, so `close` can tear down and
    /// the cap can be enforced.
    all: std.ArrayList(*Entry) = .empty,
    waiters: std.ArrayList(Waiter) = .empty,

    reap_timer: reactor.Timer,
    closed: bool = false,

    /// Connections dialed but not yet handshaken. Counted against the cap
    /// so a burst of acquires can't overshoot it.
    connecting: u32 = 0,

    const Entry = struct {
        conn: Conn,
        pool: *Pool,
        /// Monotonic nanos of the last release. Only meaningful while idle.
        idle_since: u64 = 0,
        /// True between acquire and release.
        checked_out: bool = false,
        /// The waiter to hand this to once the handshake completes.
        pending: ?Waiter = null,

        const handler: conn_mod.Handler = .{
            .on_ready = onReady,
            .on_body = onBody,
            .on_error = onError,
        };

        fn onReady(c: *Conn) void {
            const self: *Entry = @fieldParentPtr("conn", c);
            const pool = self.pool;
            pool.connecting -= 1;

            // Hand straight to whoever asked for it, without a trip
            // through the idle list — that would let a later acquire jump
            // the queue.
            if (self.pending) |w| {
                self.pending = null;
                self.checked_out = true;
                w.callback(w.ctx, &self.conn);
                return;
            }
            pool.parkIdle(self);
        }

        fn onBody(c: *Conn, payload: []const u8) void {
            // The pool doesn't consume bodies; the owner installs its own
            // handler expectations via the fetch API. This exists because
            // `Conn` requires a handler, and forwarding is the honest
            // no-op: a body arriving with nobody waiting for it is a bug
            // in the caller, not something to paper over.
            _ = c;
            _ = payload;
        }

        fn onError(c: *Conn, err: conn_mod.Error) void {
            const self: *Entry = @fieldParentPtr("conn", c);
            const pool = self.pool;
            if (!self.checked_out and self.pending != null) pool.connecting -= 1;

            const w = self.pending;
            self.pending = null;
            pool.discard(self);
            // Report to the waiter after the entry is gone, so a waiter
            // that immediately retries sees an accurate slot count.
            if (w) |waiter| waiter.callback(waiter.ctx, err);
            pool.pump();
        }
    };

    /// Initialise in place: the reactor stores `&self.reap_timer`, and
    /// each entry stores a `*Pool` back-pointer.
    pub fn init(
        self: *Pool,
        gpa: Allocator,
        loop: *reactor.Loop,
        addr: IpAddress,
        options: Options,
    ) void {
        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .addr = addr,
            .options = options,
            .reap_timer = .{ .callback = onReap },
        };
    }

    pub fn deinit(self: *Pool) void {
        self.close();
        self.idle.deinit(self.gpa);
        self.all.deinit(self.gpa);
        self.waiters.deinit(self.gpa);
    }

    /// Refuse further acquires and tear everything down. Waiters are
    /// failed rather than silently dropped, so a caller awaiting a
    /// connection during shutdown gets an answer.
    pub fn close(self: *Pool) void {
        if (self.closed) return;
        self.closed = true;
        if (self.reap_timer.isArmed()) self.loop.cancelTimer(&self.reap_timer);

        for (self.waiters.items) |w| w.callback(w.ctx, error.PoolClosed);
        self.waiters.clearRetainingCapacity();

        // Copy first: `deinit` on an entry mutates `all` via discard paths.
        const entries = self.all.toOwnedSlice(self.gpa) catch blk: {
            // Under allocation failure, walk in place instead of copying.
            while (self.all.items.len > 0) {
                const e = self.all.items[self.all.items.len - 1];
                _ = self.all.pop();
                e.conn.deinit();
                self.gpa.destroy(e);
            }
            break :blk &[_]*Entry{};
        };
        defer self.gpa.free(entries);
        for (entries) |e| {
            e.conn.deinit();
            self.gpa.destroy(e);
        }
        self.idle.clearRetainingCapacity();
    }

    /// Total connections against the cap: established plus in-flight.
    pub fn openCount(self: *const Pool) u32 {
        return @intCast(self.all.items.len);
    }

    pub fn idleCount(self: *const Pool) usize {
        return self.idle.items.len;
    }

    pub fn waiterCount(self: *const Pool) usize {
        return self.waiters.items.len;
    }

    /// Ask for a connection. `callback` fires either synchronously (an
    /// idle connection was available) or later (one had to be dialed).
    pub fn acquire(self: *Pool, callback: AcquireFn, ctx: ?*anyopaque) void {
        if (self.closed) {
            callback(ctx, error.PoolClosed);
            return;
        }

        if (self.idle.pop()) |e| {
            if (self.idle.items.len == 0 and self.reap_timer.isArmed()) {
                // Nothing left to reap, so stop waking up for it.
                self.loop.cancelTimer(&self.reap_timer);
            }
            e.checked_out = true;
            callback(ctx, &e.conn);
            return;
        }

        if (self.openCount() < self.options.max_connections) {
            self.dial(.{ .callback = callback, .ctx = ctx }) catch |err| {
                callback(ctx, err);
            };
            return;
        }

        if (self.waiters.items.len >= self.options.max_waiters) {
            callback(ctx, error.TooManyWaiters);
            return;
        }
        self.waiters.append(self.gpa, .{ .callback = callback, .ctx = ctx }) catch {
            callback(ctx, error.SystemResources);
        };
    }

    /// Withdraw an acquire that has not been answered yet.
    ///
    /// A caller that gives up — a cancelled download fiber, whose
    /// `ctx` is a frame on the stack it is unwinding — must not leave its
    /// address in the queue. The pool would otherwise hand a connection to
    /// something that no longer exists and, worse, count that connection
    /// as checked out for the rest of the process.
    ///
    /// A queued waiter is simply dropped. One whose connection is already
    /// dialling cannot be, because `Entry.onError` decides whether to
    /// un-count `connecting` by whether a waiter is attached; it is
    /// redirected instead at a stub that takes delivery and releases at
    /// once, which is also what makes the freshly dialled connection
    /// available to whoever asks next rather than wasted.
    pub fn cancelAcquire(self: *Pool, ctx: ?*anyopaque) void {
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            if (self.waiters.items[i].ctx == ctx) {
                _ = self.waiters.orderedRemove(i);
                continue;
            }
            i += 1;
        }
        for (self.all.items) |e| {
            const w = e.pending orelse continue;
            if (w.ctx != ctx) continue;
            e.pending = .{ .callback = onAbandoned, .ctx = e };
        }
    }

    /// Stand-in waiter installed by `cancelAcquire`.
    fn onAbandoned(ctx: ?*anyopaque, result: Error!*Conn) void {
        const e: *Entry = @ptrCast(@alignCast(ctx.?));
        const c = result catch return;
        e.pool.release(c, false);
    }

    /// Give a connection back.
    ///
    /// `failed` says whether the caller's last use of it errored. A failed
    /// connection is discarded rather than reused: after a protocol error
    /// the stream's framing is suspect, and handing it to the next caller
    /// converts one failed segment into a cascade.
    ///
    /// **Reentrancy**: when a waiter is queued, this hands the connection
    /// straight over and therefore calls that waiter's callback *before
    /// returning*. A caller must not be iterating a container that its own
    /// acquire callback appends to — the append can reallocate and the
    /// iteration is then walking freed memory. Snapshot first.
    pub fn release(self: *Pool, c: *Conn, failed: bool) void {
        const e: *Entry = @fieldParentPtr("conn", c);
        std.debug.assert(e.checked_out);
        e.checked_out = false;

        if (self.closed) {
            self.discard(e);
            return;
        }
        if (failed or !c.isReady()) {
            self.discard(e);
            self.pump();
            return;
        }

        // Prefer handing it straight to a waiter over parking it: a
        // round trip through the idle list would only add a timer arm.
        if (self.waiters.items.len > 0) {
            const w = self.waiters.orderedRemove(0);
            e.checked_out = true;
            w.callback(w.ctx, &e.conn);
            return;
        }
        self.parkIdle(e);
    }

    // -- internals ----------------------------------------------------

    fn dial(self: *Pool, waiter: Waiter) Error!void {
        const e = try self.gpa.create(Entry);
        errdefer self.gpa.destroy(e);

        e.* = .{ .conn = undefined, .pool = self, .pending = waiter };
        try self.all.append(self.gpa, e);
        errdefer _ = self.all.pop();

        self.connecting += 1;
        errdefer self.connecting -= 1;

        try e.conn.connect(self.gpa, self.loop, self.addr, self.options.conn, &Entry.handler);
    }

    fn parkIdle(self: *Pool, e: *Entry) void {
        e.idle_since = sys.monotonicNanos();
        self.idle.append(self.gpa, e) catch {
            // Can't track it as idle, so don't keep it. Losing a
            // connection is recoverable; losing track of one is not.
            self.discard(e);
            return;
        };
        self.armReaper();
    }

    /// Arm the reaper only when something is idle. An empty or fully
    /// checked-out pool has no timer, so it contributes nothing to the
    /// idle-CPU figure.
    fn armReaper(self: *Pool) void {
        if (self.reap_timer.isArmed()) return;
        if (self.idle.items.len == 0) return;
        self.loop.addTimer(&self.reap_timer, self.options.reap_interval_ns) catch {};
    }

    fn onReap(t: *reactor.Timer) void {
        const self: *Pool = @fieldParentPtr("reap_timer", t);
        if (self.closed) return;

        const now = sys.monotonicNanos();
        var i: usize = 0;
        while (i < self.idle.items.len) {
            const e = self.idle.items[i];
            if (now - e.idle_since >= self.options.idle_ttl_ns) {
                _ = self.idle.orderedRemove(i);
                self.discard(e);
                continue;
            }
            i += 1;
        }
        self.armReaper();
    }

    /// Remove an entry entirely and free it.
    fn discard(self: *Pool, e: *Entry) void {
        for (self.idle.items, 0..) |x, i| {
            if (x == e) {
                _ = self.idle.orderedRemove(i);
                break;
            }
        }
        for (self.all.items, 0..) |x, i| {
            if (x == e) {
                _ = self.all.swapRemove(i);
                break;
            }
        }
        e.conn.deinit();
        self.gpa.destroy(e);

        if (self.idle.items.len == 0 and self.reap_timer.isArmed()) {
            self.loop.cancelTimer(&self.reap_timer);
        }
    }

    /// A slot freed up; start work for the oldest waiter if the cap allows.
    fn pump(self: *Pool) void {
        while (self.waiters.items.len > 0 and
            self.openCount() < self.options.max_connections)
        {
            const w = self.waiters.orderedRemove(0);
            self.dial(w) catch |err| {
                w.callback(w.ctx, err);
            };
        }
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;
const StubServer = conn_mod.StubServer;

const greeting_ok = "200 news.example.invalid ready\r\n";
const handshake = [_]StubServer.Exchange{
    .{ .expect = "MODE READER", .reply = "200 reader\r\n" },
};

/// Collects acquire outcomes so a test can assert on order and count.
const Collector = struct {
    got: std.ArrayList(Result) = .empty,
    gpa: Allocator,

    const Result = union(enum) { ok: *Conn, err: anyerror };

    fn cb(ctx: ?*anyopaque, result: Error!*Conn) void {
        const self: *Collector = @ptrCast(@alignCast(ctx.?));
        if (result) |c| {
            self.got.append(self.gpa, .{ .ok = c }) catch {};
        } else |e| {
            self.got.append(self.gpa, .{ .err = e }) catch {};
        }
    }

    fn deinit(self: *Collector) void {
        self.got.deinit(self.gpa);
    }

    fn okCount(self: *const Collector) usize {
        var n: usize = 0;
        for (self.got.items) |r| if (r == .ok) {
            n += 1;
        };
        return n;
    }
};

/// Release every connection the collector holds.
///
/// Snapshotting first is mandatory, not tidiness: `release` hands a freed
/// connection straight to a queued waiter and calls its callback before
/// returning, which appends to `col.got` and can reallocate the very slice
/// a naive loop would be iterating. Getting this wrong segfaults only under
/// ReleaseFast, which is exactly why the suite runs in both modes.
fn releaseAll(pool: *Pool, col: *Collector) void {
    var snapshot: [64]*Conn = undefined;
    var n: usize = 0;
    for (col.got.items) |r| {
        if (r == .ok and n < snapshot.len) {
            snapshot[n] = r.ok;
            n += 1;
        }
    }
    // Track what the waiters get handed so those are released too, and
    // bound the loop so a bug here can't spin forever.
    var rounds: usize = 0;
    while (n > 0 and rounds < 16) : (rounds += 1) {
        const before = col.got.items.len;
        for (snapshot[0..n]) |c| pool.release(c, false);
        n = 0;
        for (col.got.items[before..]) |r| {
            if (r == .ok and n < snapshot.len) {
                snapshot[n] = r.ok;
                n += 1;
            }
        }
    }
}

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

/// A stub that serves the handshake to an unlimited number of connections.
fn startStub(gpa: Allocator, loop: *reactor.Loop, stub: *StubServer) !u16 {
    return stub.start(gpa, loop, greeting_ok, &handshake);
}

test "acquire dials, hands over after handshake, and reuses on release" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{ .max_connections = 4 });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    pool.acquire(Collector.cb, &col);
    try pumpUntil(&loop, 2000, &col, struct {
        fn f(c: *Collector) bool {
            return c.got.items.len > 0;
        }
    }.f);

    try testing.expectEqual(@as(usize, 1), col.okCount());
    const c1 = col.got.items[0].ok;
    try testing.expect(c1.isReady());
    try testing.expectEqual(@as(u32, 1), pool.openCount());

    // Release then re-acquire must reuse, not dial: connection setup is
    // several round trips and providers count slots.
    pool.release(c1, false);
    try testing.expectEqual(@as(usize, 1), pool.idleCount());

    col.got.clearRetainingCapacity();
    pool.acquire(Collector.cb, &col);
    // Reuse is synchronous, so the callback has already fired.
    try testing.expectEqual(@as(usize, 1), col.got.items.len);
    try testing.expectEqual(c1, col.got.items[0].ok);
    try testing.expectEqual(@as(usize, 1), stub.accepted);
    pool.release(c1, false);
}

test "the connection cap counts connections that are still connecting" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{ .max_connections = 3 });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    // Ten acquires in one go, before any handshake can finish. Counting
    // only *established* connections is the classic bug: every acquire
    // sees "0 open" and dials, and the provider starts refusing.
    for (0..10) |_| pool.acquire(Collector.cb, &col);

    try testing.expectEqual(@as(u32, 3), pool.openCount());
    try testing.expectEqual(@as(usize, 7), pool.waiterCount());

    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collector) bool {
            return c.okCount() >= 3;
        }
    }.f);

    // Never more sockets than the cap, no matter the burst.
    try testing.expect(stub.accepted <= 3);
    try testing.expectEqual(@as(u32, 3), pool.openCount());

    releaseAll(&pool, &col);
}

test "waiters are served in FIFO order as connections come back" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{ .max_connections = 1 });
    defer pool.deinit();

    // Distinct contexts so the order of service is observable.
    const Tagged = struct {
        order: *std.ArrayList(u8),
        gpa: Allocator,
        tag: u8,
        held: ?*Conn = null,

        fn cb(ctx: ?*anyopaque, result: Error!*Conn) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.order.append(self.gpa, self.tag) catch {};
            self.held = result catch null;
        }
    };

    var order: std.ArrayList(u8) = .empty;
    defer order.deinit(gpa);
    var tagged: [4]Tagged = undefined;
    for (&tagged, 0..) |*t, i| {
        t.* = .{ .order = &order, .gpa = gpa, .tag = @intCast(i) };
        pool.acquire(Tagged.cb, t);
    }

    try pumpUntil(&loop, 3000, &order, struct {
        fn f(o: *std.ArrayList(u8)) bool {
            return o.items.len >= 1;
        }
    }.f);

    // Hand the single connection round the queue; each release must go to
    // the longest-waiting acquirer, or a busy job starves a newer one.
    for (1..4) |_| {
        var holder: ?*Tagged = null;
        for (&tagged) |*t| {
            if (t.held) |_| holder = t;
        }
        const h = holder orelse break;
        const c = h.held.?;
        h.held = null;
        pool.release(c, false);
        _ = try loop.tick(5);
    }

    try testing.expectEqual(@as(usize, 4), order.items.len);
    try testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3 }, order.items);

    for (&tagged) |*t| {
        if (t.held) |c| {
            pool.release(c, false);
            t.held = null;
        }
    }
}

test "a failed connection is discarded rather than handed to the next caller" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{ .max_connections = 2 });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    pool.acquire(Collector.cb, &col);
    try pumpUntil(&loop, 2000, &col, struct {
        fn f(c: *Collector) bool {
            return c.okCount() >= 1;
        }
    }.f);

    // After a protocol error the stream's framing is suspect. Reusing it
    // turns one failed segment into a cascade, so the pool must drop it.
    const c = col.got.items[0].ok;
    pool.release(c, true);

    try testing.expectEqual(@as(usize, 0), pool.idleCount());
    try testing.expectEqual(@as(u32, 0), pool.openCount());
}

test "an idle connection is reaped after its TTL" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .max_connections = 2,
        // Short enough to test, and the mechanism is the same at 60 s.
        .idle_ttl_ns = 40 * std.time.ns_per_ms,
        .reap_interval_ns = 20 * std.time.ns_per_ms,
    });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    pool.acquire(Collector.cb, &col);
    try pumpUntil(&loop, 2000, &col, struct {
        fn f(c: *Collector) bool {
            return c.okCount() >= 1;
        }
    }.f);
    pool.release(col.got.items[0].ok, false);
    try testing.expectEqual(@as(usize, 1), pool.idleCount());

    // Providers drop idle connections silently, so holding one past its
    // welcome just moves the failure to the next fetch.
    try pumpUntil(&loop, 3000, &pool, struct {
        fn f(p: *Pool) bool {
            return p.idleCount() == 0;
        }
    }.f);
    try testing.expectEqual(@as(u32, 0), pool.openCount());
}

test "an empty pool arms no timer, so it costs nothing when idle" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", 1), .{});
    defer pool.deinit();

    // The Go version ran a reaper ticker for the pool's whole lifetime.
    // Here the timer is armed only while something is idle, which is what
    // keeps a queue-empty daemon at zero wakeups.
    try testing.expect(!pool.reap_timer.isArmed());

    const start = sys.monotonicNanos();
    _ = try loop.tick(30);
    try testing.expect(sys.monotonicNanos() - start >= 20 * std.time.ns_per_ms);
    try testing.expect(!pool.reap_timer.isArmed());
}

test "a dial failure is reported to the waiter, not swallowed" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Bind then release, so nothing is listening.
    const socket_mod = @import("../net/socket.zig");
    var probe: socket_mod.Listener = undefined;
    try probe.listen(try IpAddress.parse("127.0.0.1", 0), struct {
        fn f(_: *socket_mod.Listener, fd: sys.Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead = try probe.boundPort();
    probe.close();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", dead), .{ .max_connections = 2 });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    pool.acquire(Collector.cb, &col);
    try pumpUntil(&loop, 3000, &col, struct {
        fn f(c: *Collector) bool {
            return c.got.items.len > 0;
        }
    }.f);

    try testing.expectEqual(@as(usize, 1), col.got.items.len);
    try testing.expectEqual(@as(anyerror, error.ConnectionRefused), col.got.items[0].err);
    // And the slot is returned, so a retry isn't blocked by a ghost.
    try testing.expectEqual(@as(u32, 0), pool.openCount());
}

test "close fails pending waiters instead of dropping them" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{ .max_connections = 1 });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    for (0..3) |_| pool.acquire(Collector.cb, &col);
    try testing.expectEqual(@as(usize, 2), pool.waiterCount());

    // A caller awaiting a connection during shutdown has to get an answer;
    // silently dropping the callback leaves a job wedged forever.
    pool.close();
    try testing.expectEqual(@as(usize, 2), col.got.items.len);
    for (col.got.items) |r| {
        try testing.expectEqual(@as(anyerror, error.PoolClosed), r.err);
    }
    try testing.expectEqual(@as(u32, 0), pool.openCount());
}

test "acquire after close is refused rather than hanging" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", 1), .{});
    defer pool.deinit();
    pool.close();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();
    pool.acquire(Collector.cb, &col);

    try testing.expectEqual(@as(usize, 1), col.got.items.len);
    try testing.expectEqual(@as(anyerror, error.PoolClosed), col.got.items[0].err);
}

test "the waiter queue is bounded" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    const port = try startStub(gpa, &loop, &stub);
    defer stub.deinit();

    var pool: Pool = undefined;
    pool.init(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .max_connections = 1,
        .max_waiters = 4,
    });
    defer pool.deinit();

    var col: Collector = .{ .gpa = gpa };
    defer col.deinit();

    // An unbounded queue turns provider downtime into an OOM, so past the
    // cap an acquire is refused immediately rather than queued.
    for (0..10) |_| pool.acquire(Collector.cb, &col);

    try testing.expectEqual(@as(usize, 4), pool.waiterCount());
    var refused: usize = 0;
    for (col.got.items) |r| {
        if (r == .err and r.err == error.TooManyWaiters) refused += 1;
    }
    try testing.expectEqual(@as(usize, 5), refused);

    releaseAll(&pool, &col);
}
