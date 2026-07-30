//! Outbound notifications, wired onto the reactor.
//!
//! `app/notify` is written against a blocking `Transport`: `post` returns
//! a status and `deliver` sleeps between retries. That shape is the right
//! one for the retry policy — attempt, classify, back off — and it is the
//! part with the tests. What it cannot do is run on the loop thread,
//! where a blocking `post` stops every download and a `sleep` stops the
//! whole daemon for 600 ms per failed webhook.
//!
//! So the notify context gets the same treatment `bootstrap/runtime.zig`
//! gives the download orchestrator: **a fiber**. One dispatch runs on its
//! own stack; `post` dials, writes and reads by parking; `sleep` arms a
//! reactor timer and yields. From `deliver`'s point of view it blocked;
//! from the loop's point of view nothing did.
//!
//! ## What runs where
//!
//! ```
//!   bus handler        loop thread   copies the envelope, arms the pump
//!   pump timer         loop thread   starts the next dispatch fiber
//!   Service.onEvent    fiber         loads subs, renders, delivers
//!   post / sleep       fiber         parks; the loop is free throughout
//!   resolver / TLS cb  loop thread   records, arms the resume timer
//! ```
//!
//! **No callback ever enters the fiber.** That is `runtime.zig`'s rule
//! and it holds here for the same reason: the DNS resolver's callback
//! fires from inside its own socket dispatch, and `tls.Conn`'s
//! `on_finished` fires from inside the fiber machinery. Both record their
//! result and arm a zero-delay timer; the timer callback is the one and
//! only place this file switches into a dispatch fiber.
//!
//! ## One at a time
//!
//! Go ran a goroutine per (subscription, event). Here the queue is FIFO
//! and exactly one dispatch is live: a webhook is not a hot path — one
//! per finished download, times however many subscriptions — and a
//! single in-flight fiber means no locking anywhere and one stack of
//! address space rather than one per subscriber. A subscriber that
//! times out costs the queue its deadline, not the daemon its liveness.
//!
//! ## Database access from a fiber
//!
//! The dispatch fiber runs on the loop thread, so it may touch the
//! single-threaded `*sqlite.Conn` — but only *between* parks, never
//! across one. The enabled subscriptions are materialised in full before
//! the first network call, and an outcome is recorded after a delivery
//! has returned. Nothing here holds a statement open while parked, which
//! is what keeps the connection's single-threaded contract intact while
//! the loop runs other work.
//!
//! ## TLS
//!
//! `https://` is the normal case — Discord and Slack webhooks are
//! nothing else — so it is not optional. `net/http/client.zig` is
//! reactor-driven but has no TLS, so the exchange here is written
//! against `net/tls.zig` instead: `Conn` for `https://`, its bare
//! `Transport` (a parking Reader/Writer pair over one fd) for `http://`.
//! One `exchange` function serves both, because above the byte stream
//! HTTP/1.1 does not care.
//!
//! Trust anchors come from the same `CaStore` the NNTP providers use. A
//! build with no CA bundle cannot verify anybody, so an `https://`
//! subscription fails with `Tls` and says so in the log rather than
//! silently downgrading to plaintext.

const std = @import("std");

const log = @import("../core/log.zig");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const fiber_mod = @import("../posix/fiber.zig");
const tls = @import("../net/tls.zig");
const dns = @import("../net/dns.zig");
const http = @import("../net/http/client.zig");
const nntp_transport = @import("../nntp/transport.zig");

const sqlite = @import("../store/sqlite.zig");
const outbox = @import("../store/outbox.zig");
const repo_subscription = @import("../store/repo_subscription.zig");

const event = @import("../domain/event.zig");
const dnotify = @import("../domain/notify.zig");
const notify_service = @import("../app/notify/service.zig");
const ntransport = @import("../app/notify/transport.zig");

const Allocator = std.mem.Allocator;
const Fiber = fiber_mod.Fiber;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;
const IpAddress = std.Io.net.IpAddress;

/// Stack for a dispatch fiber.
///
/// A TLS handshake was measured at 148 KB in ReleaseFast and 506 KB in
/// Debug (`posix/fiber.zig`), and the outer fiber here does not run one —
/// `tls.Conn` brings its own stack for that. It renders a payload, walks
/// a subscription list and parks. The module default is more than it
/// needs and it is address space, not memory.
const dispatch_stack_size = fiber_mod.default_stack_size;

/// Whole-exchange wall clock, connect through status line. A webhook
/// endpoint that accepts the connection and then says nothing must not
/// be able to pin the queue.
const attempt_deadline_ns: u64 = 15 * std.time.ns_per_s;

/// Backlog ceiling. Past it the handler reports failure, which hands the
/// row back to the outbox's own retry machinery instead of dropping the
/// notification or growing without bound.
const max_queued: usize = 256;

/// The `std.Io` handed to the `CaStore`'s `RwLock`.
///
/// Copied from `nntp/transport.zig` for the reason stated there: the lock
/// is never contended, both of its uncontended paths return without
/// dereferencing this, and adopting an `std.Io` implementation to own a
/// lock nobody takes is the thing this project exists to avoid. It faults
/// rather than misbehaving if that ever stops being true.
const dead_io: std.Io = .{
    .userdata = null,
    .vtable = @ptrFromInt(@alignOf(std.Io.VTable)),
};

// =====================================================================
// Notifier
// =====================================================================

/// The notify context's composition root: a bus subscriber, a queue, and
/// one fiber at a time delivering from it.
///
/// Initialise in place — the loop stores `&self.pump`, and the dispatch
/// fibers hold a `*Notifier`.
pub const Notifier = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    db: *sqlite.Conn,
    logger: *log.Logger = &log.default,

    /// Hostname resolution. Null means only address literals work, which
    /// is what a build with no resolver wired gets.
    resolver: ?*dns.Resolver = null,
    /// Trust anchors for `https://`. Null, or empty, means an `https://`
    /// subscription cannot be delivered.
    ca: ?*nntp_transport.CaStore = null,

    now: *const fn () i64 = defaultNow,

    queue: std.ArrayList(*Pending) = .empty,
    current: ?*Dispatch = null,
    pump: reactor.Timer = .{ .callback = onPump },
    started: bool = false,
    stopping: bool = false,

    /// Diagnostics. Loop thread only.
    dispatched: usize = 0,
    refused: usize = 0,

    fn defaultNow() i64 {
        return @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_ms));
    }

    /// Subscription names, one per subscribable topic. Comptime because
    /// `outbox` keys a row on the name and a name that changed between
    /// builds would replay every event the previous name had consumed.
    const sub_names = blk: {
        var names: [notify_service.subscribable_topics.len][]const u8 = undefined;
        for (notify_service.subscribable_topics, 0..) |t, i| names[i] = "notify." ++ t;
        break :blk names;
    };

    /// Register for every topic `app/notify` considers subscribable.
    ///
    /// `Subscriber` is a duck-typed `*Runtime`: whatever owns the reactor
    /// plumbing for a bus subscription. Taking it as `anytype` keeps this
    /// file independent of `bootstrap/runtime.zig`, which owns a great
    /// deal more than notify needs.
    pub fn subscribeAll(self: *Notifier, engine: anytype) !void {
        self.started = true;
        inline for (notify_service.subscribable_topics, sub_names) |topic, name| {
            try engine.subscribe(name, topic, &onBusEvent, @ptrCast(self));
        }
    }

    /// Cancel whatever is in flight and drop the backlog.
    ///
    /// Must run before the database and the loop are closed: a dispatch
    /// fiber unwinding here releases a loaded subscription list and
    /// closes a socket, and both need their owners alive.
    pub fn deinit(self: *Notifier) void {
        self.stopping = true;
        if (self.pump.isArmed()) self.loop.cancelTimer(&self.pump);

        if (self.current) |d| {
            d.canceled = true;
            // Unwind rather than discard, so the fiber's own `defer`s run.
            // A fiber parked on a socket takes the `cancel`; one merely
            // yielded takes the `enter`. The guard bounds a body that
            // somehow refuses to finish — better a leaked stack than a
            // shutdown that hangs.
            var guard: usize = 0;
            while (!d.finished and guard < 64) : (guard += 1) {
                if (d.attempt) |a| a.conn.cancel();
                if (d.finished) break;
                d.fiber.cancel();
                if (d.finished) break;
                d.fiber.enter();
            }
            if (!d.finished) {
                self.logger.warn("notify: a delivery could not be unwound at shutdown", &.{});
            }
            d.deinit();
            self.gpa.destroy(d);
            self.current = null;
        }

        for (self.queue.items) |p| p.destroy(self.gpa);
        self.queue.deinit(self.gpa);
    }

    // -- the bus side --------------------------------------------------

    fn onBusEvent(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
        const self: *Notifier = @ptrCast(@alignCast(ctx.?));
        return self.enqueue(env);
    }

    /// Copy the envelope and hand it to the pump.
    ///
    /// The handler returns before the webhook has been contacted, and
    /// that is deliberate rather than convenient: `Service.onEvent`
    /// already refuses to propagate a delivery failure, because a bus
    /// redelivery would duplicate the deliveries that *did* succeed. So
    /// the outbox row is settled on "we accepted responsibility", and
    /// retry-within-a-dispatch is `transport.deliver`'s job. A full queue
    /// is the one case that does report failure — there the event has not
    /// been accepted at all, and the outbox's own backoff is exactly the
    /// right place for it to wait.
    fn enqueue(self: *Notifier, env: outbox.Envelope) outbox.HandlerResult {
        if (self.stopping) return .{ .failed = "shutting down" };
        if (self.queue.items.len >= max_queued) {
            self.refused += 1;
            return .{ .failed = "notify backlog full" };
        }

        const p = Pending.create(self.gpa, env) catch return .{ .failed = "out of memory" };
        self.queue.append(self.gpa, p) catch {
            p.destroy(self.gpa);
            return .{ .failed = "out of memory" };
        };
        self.armPump();
        return .ok;
    }

    fn armPump(self: *Notifier) void {
        if (self.stopping or self.pump.isArmed()) return;
        self.loop.addTimer(&self.pump, 0) catch {};
    }

    fn onPump(t: *reactor.Timer) void {
        const self: *Notifier = @fieldParentPtr("pump", t);
        self.advance();
    }

    /// Reap a finished dispatch, then start the next one.
    ///
    /// Reaping happens here rather than in the fiber's `on_finished`
    /// because that hook runs on the resumer's stack — freeing the fiber
    /// from inside the frame that just entered it would pull the ground
    /// out from under `Fiber.enter`'s own epilogue.
    fn advance(self: *Notifier) void {
        if (self.stopping) return;

        if (self.current) |d| {
            if (!d.finished) return;
            d.deinit();
            self.gpa.destroy(d);
            self.current = null;
        }
        if (self.queue.items.len == 0) return;

        const p = self.queue.orderedRemove(0);
        const d = self.gpa.create(Dispatch) catch {
            p.destroy(self.gpa);
            self.logger.err("notify: cannot allocate a dispatch", &.{});
            return;
        };
        d.* = .{ .n = self, .pending = p };
        d.fiber.init(self.gpa, self.loop, dispatch_stack_size, Dispatch.entry, @ptrCast(d)) catch |e| {
            self.logger.err("notify: cannot start a delivery", &.{log.errv("err", e)});
            p.destroy(self.gpa);
            self.gpa.destroy(d);
            return;
        };
        d.fiber.on_finished = Dispatch.onFiberFinished;
        self.current = d;
        self.dispatched += 1;
        d.fiber.enter();
        // The fiber may have finished without ever parking — a
        // subscription list with no match, for instance. Reaping is the
        // pump's job either way.
        if (d.finished) self.armPump();
    }

    /// Trust configuration for `https://`, or null when this build has no
    /// anchors to verify against.
    fn trust(self: *Notifier) ?tls.Trust {
        const store = self.ca orelse return null;
        if (store.count() == 0) return null;
        return .{ .ca_bundle = .{
            .gpa = self.gpa,
            .bundle = &store.bundle,
            .lock = &store.lock,
            .io = dead_io,
        } };
    }
};

// =====================================================================
// A queued envelope
// =====================================================================

/// An owned copy of one bus envelope.
///
/// `outbox.Envelope`'s slices borrow the dispatcher's row buffer and die
/// when the handler returns, and the delivery outlives the handler by
/// design, so everything is duplicated into one allocation.
const Pending = struct {
    buf: []u8,
    topic: []const u8,
    aggregate_id: []const u8,
    payload: []const u8,
    occurred_at: i64,
    id: event.Uuid,

    fn create(gpa: Allocator, env: outbox.Envelope) Allocator.Error!*Pending {
        const p = try gpa.create(Pending);
        errdefer gpa.destroy(p);

        const total = env.topic.len + env.aggregate_id.len + env.payload.len;
        const buf = try gpa.alloc(u8, total);
        @memcpy(buf[0..env.topic.len], env.topic);
        @memcpy(buf[env.topic.len..][0..env.aggregate_id.len], env.aggregate_id);
        @memcpy(buf[env.topic.len + env.aggregate_id.len ..][0..env.payload.len], env.payload);

        p.* = .{
            .buf = buf,
            .topic = buf[0..env.topic.len],
            .aggregate_id = buf[env.topic.len..][0..env.aggregate_id.len],
            .payload = buf[env.topic.len + env.aggregate_id.len ..][0..env.payload.len],
            .occurred_at = env.occurred_at_ms,
            .id = .{ .bytes = env.id },
        };
        return p;
    }

    fn destroy(self: *Pending, gpa: Allocator) void {
        gpa.free(self.buf);
        gpa.destroy(self);
    }

    fn envelope(self: *const Pending) event.Envelope {
        return .{
            .id = self.id,
            .topic = self.topic,
            .aggregate_id = self.aggregate_id,
            .occurred_at = self.occurred_at,
            .payload = self.payload,
        };
    }
};

// =====================================================================
// One dispatch, on its own stack
// =====================================================================

/// The fan-out of one event to every matching subscription.
///
/// Also the `Transport` the notify service is handed: the vtable's
/// context *is* this struct, which is what lets `post` know which fiber
/// to park without a "current fiber" global.
const Dispatch = struct {
    n: *Notifier,
    pending: *Pending,

    fiber: Fiber = undefined,
    /// Resume clock. One timer serves every reason this fiber waits —
    /// a callback answered, or a backoff is owed — because it is only
    /// ever waiting for one of them.
    timer: reactor.Timer = .{ .callback = onTimer },
    /// Per-attempt wall clock, armed around one `post`.
    deadline: reactor.Timer = .{ .callback = onDeadline },

    /// The fiber is yielded and something is expected to resume it.
    awaiting: bool = false,
    /// Whatever we were waiting for has answered. Checked rather than
    /// assumed, because a resolver hit its cache and a refused connect
    /// both answer before the call that started them returns.
    settled: bool = false,
    /// Shutdown. Checked after every wait; the fiber unwinds through its
    /// own `defer`s rather than being discarded.
    canceled: bool = false,
    /// The deadline fired. Distinguishes "we gave up" from "the peer
    /// hung up", which are different log lines and different retries.
    timed_out: bool = false,
    finished: bool = false,

    /// The `https://` attempt currently on its own fiber, if any. Held so
    /// a deadline or a shutdown can cancel the inner fiber rather than
    /// the outer one, which is merely yielded and cannot take a `cancel`.
    attempt: ?*TlsAttempt = null,

    resolved: ?IpAddress = null,
    resolve_err: ?anyerror = null,

    /// Set once the stack has been released. See `onFiberFinished`.
    stack_freed: bool = false,

    fn deinit(self: *Dispatch) void {
        if (self.timer.isArmed()) self.n.loop.cancelTimer(&self.timer);
        if (self.deadline.isArmed()) self.n.loop.cancelTimer(&self.deadline);
        if (!self.stack_freed) self.fiber.deinit();
        self.pending.destroy(self.n.gpa);
    }

    // -- fiber plumbing ------------------------------------------------

    fn entry(f: *Fiber, ctx: ?*anyopaque) void {
        _ = f;
        const self: *Dispatch = @ptrCast(@alignCast(ctx.?));
        self.body() catch |e| {
            self.n.logger.warn("notify: dispatch stopped", &.{
                log.str("topic", self.pending.topic),
                log.errv("err", e),
            });
        };
    }

    /// The stack goes back here rather than at the next pump.
    ///
    /// `Fiber.enter` calls this as its last act, once the body has
    /// returned and the reactor registration is gone, so freeing from
    /// here is explicitly safe — and a megabyte of mapping should not
    /// stay reserved for a delivery that is over. The `Dispatch` itself
    /// survives; the frame that entered the fiber still reads it.
    fn onFiberFinished(f: *Fiber) void {
        const self: *Dispatch = @fieldParentPtr("fiber", f);
        self.finished = true;
        self.fiber.deinit();
        self.stack_freed = true;
        self.n.armPump();
    }

    /// Switch back to the loop. Only the resume timer brings us back.
    fn park(self: *Dispatch) void {
        self.awaiting = true;
        self.fiber.yield();
        self.awaiting = false;
    }

    fn armTimer(self: *Dispatch, delay_ns: u64) void {
        if (self.timer.isArmed()) self.n.loop.cancelTimer(&self.timer);
        self.n.loop.addTimer(&self.timer, delay_ns) catch {
            // The heap could not grow, so nothing would ever resume this
            // fiber. Unwind it instead of stranding it.
            self.canceled = true;
            self.n.loop.addTimer(&self.timer, 0) catch {};
        };
    }

    fn onTimer(t: *reactor.Timer) void {
        const self: *Dispatch = @fieldParentPtr("timer", t);
        if (self.finished) return;
        self.fiber.enter();
    }

    /// Called by whichever callback answered. Never enters the fiber —
    /// see the module comment.
    fn settle(self: *Dispatch) void {
        self.settled = true;
        if (self.awaiting) self.armTimer(0);
    }

    fn onDeadline(t: *reactor.Timer) void {
        const self: *Dispatch = @fieldParentPtr("deadline", t);
        if (self.finished) return;
        self.timed_out = true;
        if (self.attempt) |a| {
            // The inner fiber is the one parked on the socket.
            a.conn.cancel();
            return;
        }
        // Resumes the outer fiber's `park` with `error.Canceled`, which
        // surfaces through `tls.Transport` as a read/write failure and
        // unwinds the exchange. A no-op if we are merely yielded, in
        // which case the resume timer is already armed.
        self.fiber.failWith(error.Canceled);
    }

    // -- the body ------------------------------------------------------

    fn body(self: *Dispatch) !void {
        const gpa = self.n.gpa;
        const repo = repo_subscription.SubscriptionRepo.init(gpa, self.n.db);

        // Materialised in full before anything parks: the connection is
        // single-threaded and the loop runs other work while we are away.
        var list = repo.listEnabled(gpa) catch |e| {
            self.n.logger.err("notify: cannot read subscriptions", &.{log.errv("err", e)});
            return;
        };
        defer list.deinit();
        if (list.items.items.len == 0) return;

        const active = try gpa.alloc(*const dnotify.Subscription, list.items.items.len);
        defer gpa.free(active);
        for (list.items.items, active) |*s, *slot| slot.* = s;

        var svc: notify_service.Service = .{
            .gpa = gpa,
            .transport = self.transport(),
            .now = self.n.now,
            .logger = self.n.logger,
            .outcome = self.outcomeSink(),
            .active = active,
        };
        try svc.onEvent(self.pending.envelope());
    }

    // -- the Transport port --------------------------------------------

    fn transport(self: *Dispatch) ntransport.Transport {
        return .{
            .ctx = @ptrCast(self),
            .postFn = &postFn,
            .sleepFn = &sleepFn,
        };
    }

    /// `Transport.sleep`, as a reactor timer. This is the line that used
    /// to stop the daemon for 600 ms per failing webhook.
    fn sleepFn(ctx: *anyopaque, nanos: u64) void {
        const self: *Dispatch = @ptrCast(@alignCast(ctx));
        if (self.canceled or self.n.stopping) return;
        self.armTimer(nanos);
        self.park();
    }

    fn postFn(ctx: *anyopaque, req: ntransport.Request) ntransport.Error!ntransport.Response {
        const self: *Dispatch = @ptrCast(@alignCast(ctx));
        return self.post(req);
    }

    fn post(self: *Dispatch, req: ntransport.Request) ntransport.Error!ntransport.Response {
        if (self.canceled or self.n.stopping) return error.Canceled;

        const url = http.Url.parse(req.url) catch {
            // Not a transport failure in the retryable sense, but the
            // port has no "you configured this wrong" member and a
            // subscription URL is validated when it is saved. Reported
            // as `Connect` so the retry budget still bounds it.
            self.n.logger.warn("notify: unusable subscription url", &.{
                log.str("host", ntransport.Target.safeUrl(req.url)),
            });
            return error.Connect;
        };
        if (url.tls and self.n.trust() == null) {
            self.n.logger.err("notify: https subscription cannot be delivered without CA roots", &.{
                log.str("host", url.host),
            });
            return error.Tls;
        }

        const addr = try self.resolve(url.host, url.port);

        self.timed_out = false;
        self.n.loop.addTimer(&self.deadline, attempt_deadline_ns) catch {};
        defer if (self.deadline.isArmed()) self.n.loop.cancelTimer(&self.deadline);

        const status = if (url.tls)
            try self.postTls(url, req, addr)
        else
            try self.postPlain(url, req, addr);

        return .{ .status = status };
    }

    /// Hostname to address, or the literal straight through.
    fn resolve(self: *Dispatch, host: []const u8, port: u16) ntransport.Error!IpAddress {
        const resolver = self.n.resolver orelse {
            // No resolver: a literal is still usable, a name is not.
            return IpAddress.parse(host, port) catch error.Connect;
        };

        self.settled = false;
        self.resolved = null;
        self.resolve_err = null;
        resolver.resolve(host, port, onResolved, @ptrCast(self));
        if (!self.settled) {
            self.park();
            if (self.canceled or self.n.stopping) return error.Canceled;
        }
        if (self.resolve_err) |e| {
            self.n.logger.warn("notify: cannot resolve subscriber", &.{
                log.str("host", host),
                log.errv("err", e),
            });
            return error.Connect;
        }
        return self.resolved orelse error.Connect;
    }

    fn onResolved(ctx: ?*anyopaque, result: dns.Error!IpAddress) void {
        const self: *Dispatch = @ptrCast(@alignCast(ctx.?));
        if (result) |addr| {
            self.resolved = addr;
        } else |e| {
            self.resolve_err = e;
        }
        self.settle();
    }

    /// `http://`: the fiber's own parking Reader/Writer pair over one fd.
    fn postPlain(
        self: *Dispatch,
        url: http.Url,
        req: ntransport.Request,
        addr: IpAddress,
    ) ntransport.Error!u16 {
        const fd = tls.dial(&self.fiber, addr) catch |e| return mapDialError(e);
        defer sys.close(fd);

        var read_buf: [8 << 10]u8 = undefined;
        var write_buf: [4 << 10]u8 = undefined;
        var tr = tls.Transport.init(&self.fiber, fd, &read_buf, &write_buf);

        return exchange(&tr.reader, &tr.writer, url, req) catch |e| {
            if (self.timed_out) return error.Timeout;
            if (self.canceled or tr.canceled) return error.Canceled;
            return e;
        };
    }

    /// `https://`: a `tls.Conn`, which brings its own fiber for the
    /// handshake and the exchange. This fiber yields until it is done.
    fn postTls(
        self: *Dispatch,
        url: http.Url,
        req: ntransport.Request,
        addr: IpAddress,
    ) ntransport.Error!u16 {
        const fd = tls.dial(&self.fiber, addr) catch |e| return mapDialError(e);
        // From here `Conn` owns the fd and closes it in `deinit`.

        var att: TlsAttempt = .{
            .d = self,
            .url = url,
            .req = req,
            .trust = self.n.trust().?,
        };
        att.conn.init(self.n.gpa, self.n.loop, fd, TlsAttempt.session, @ptrCast(&att), dispatch_stack_size) catch {
            sys.close(fd);
            return error.Io;
        };
        att.conn.on_finished = TlsAttempt.onFinished;

        self.settled = false;
        self.attempt = &att;
        defer self.attempt = null;

        att.conn.start();
        if (!self.settled) {
            self.park();
        }
        // Freed only once the session has returned; `start` guarantees a
        // park before it returns and `on_finished` guarantees the rest.
        defer att.conn.deinit();

        if (self.timed_out) return error.Timeout;
        if (self.canceled or self.n.stopping) return error.Canceled;
        if (att.err) |e| return e;
        return att.status;
    }

    // -- the OutcomeSink port ------------------------------------------

    fn outcomeSink(self: *Dispatch) notify_service.OutcomeSink {
        return .{ .ctx = @ptrCast(self), .recordFn = &recordOutcome };
    }

    /// Persist one delivery's verdict onto the subscription.
    ///
    /// Runs between parks — `deliver` has already returned — so the
    /// read-modify-write never straddles a yield and the connection is
    /// ours for its duration. `reason` comes from `Delivery.describe`,
    /// which is built from status codes only and can never carry a URL.
    fn recordOutcome(
        ctx: *anyopaque,
        id: dnotify.SubscriptionId,
        ok: bool,
        reason: []const u8,
        now: event.Timestamp,
    ) void {
        const self: *Dispatch = @ptrCast(@alignCast(ctx));
        const gpa = self.n.gpa;
        const repo = repo_subscription.SubscriptionRepo.init(gpa, self.n.db);

        var s = repo.byId(gpa, id) catch return;
        defer s.deinit();
        if (ok) {
            s.markDeliverySuccess(now) catch return;
        } else {
            s.markDeliveryFailure(reason, now) catch return;
        }
        repo.save(&s) catch |e| {
            self.n.logger.warn("notify: cannot record delivery outcome", &.{
                log.int("sub_id", id),
                log.errv("err", e),
            });
        };
    }
};

// =====================================================================
// The TLS attempt
// =====================================================================

/// One `https://` exchange, running on the `tls.Conn`'s own fiber.
///
/// Separate from `Dispatch` because `Conn` insists on owning the stack
/// its handshake runs on — which is the right call, a handshake is deep
/// and it should not share a stack with the code that started it.
const TlsAttempt = struct {
    d: *Dispatch,
    url: http.Url,
    req: ntransport.Request,
    trust: tls.Trust,

    conn: tls.Conn = undefined,
    status: u16 = 0,
    err: ?ntransport.Error = null,

    fn session(c: *tls.Conn) void {
        const self: *TlsAttempt = @ptrCast(@alignCast(c.context.?));
        self.run(c) catch |e| {
            self.err = e;
        };
    }

    fn run(self: *TlsAttempt, c: *tls.Conn) ntransport.Error!void {
        // Ciphertext buffers on this fiber's stack; the plaintext pair is
        // allocated by `handshake` from `gpa` and freed by `deinit`.
        var read_buf: [tls.min_ciphertext_buffer]u8 = undefined;
        var write_buf: [tls.min_ciphertext_buffer]u8 = undefined;

        c.handshake(.{
            .host = self.url.host,
            .trust = self.trust,
            .read_buffer = &read_buf,
            .write_buffer = &write_buf,
            .gpa = c.gpa,
        }) catch |e| {
            self.d.n.logger.warn("notify: TLS handshake failed", &.{
                log.str("host", self.url.host),
                log.errv("err", e),
            });
            return switch (e) {
                error.Canceled => error.Canceled,
                error.TransportFailed => error.Connect,
                error.OutOfMemory, error.Misconfigured => error.Io,
                else => error.Tls,
            };
        };
        defer c.close();

        self.status = try exchange(c.reader(), c.writer(), self.url, self.req);
    }

    /// Fires on the loop's stack once the session has returned. Records
    /// and arms; never enters the outer fiber directly.
    fn onFinished(c: *tls.Conn) void {
        const self: *TlsAttempt = @ptrCast(@alignCast(c.context.?));
        self.d.settle();
    }
};

// =====================================================================
// HTTP/1.1, above whichever byte stream
// =====================================================================

/// Largest status line plus header block we will read before giving up.
/// A webhook answers with a handful of headers; anything bigger is a
/// peer we do not want to keep reading from.
const max_head_bytes: usize = 8 << 10;

/// Send the request, return the status code.
///
/// The response body is deliberately not read. The retry policy in
/// `app/notify/transport.zig` keys on the status code and nothing else,
/// the connection is `Connection: close` so there is no pipeline to keep
/// consistent, and a subscriber that answers a 500 with a megabyte of
/// HTML should not get to spend our memory on it.
fn exchange(
    r: *Reader,
    w: *Writer,
    url: http.Url,
    req: ntransport.Request,
) ntransport.Error!u16 {
    writeHead(w, url, req) catch return error.Io;
    w.flush() catch return error.Io;

    var consumed: usize = 0;
    const status_line = r.takeDelimiterInclusive('\n') catch |e| return mapReadError(e);
    consumed += status_line.len;
    const status = parseStatus(status_line) orelse return error.Io;

    // Drain the header block so the peer sees a complete read rather than
    // a reset, but under a ceiling.
    while (consumed < max_head_bytes) {
        const line = r.takeDelimiterInclusive('\n') catch |e| switch (e) {
            // A peer that closes right after its head is answering, not
            // failing: we have the status, which is all we came for.
            error.EndOfStream => break,
            else => return mapReadError(e),
        };
        consumed += line.len;
        if (std.mem.trimEnd(u8, line, "\r\n").len == 0) break;
    }
    return status;
}

fn writeHead(w: *Writer, url: http.Url, req: ntransport.Request) Writer.Error!void {
    try w.writeAll(req.method);
    try w.writeByte(' ');
    try w.writeAll(url.path);
    try w.writeAll(" HTTP/1.1\r\nHost: ");
    try w.writeAll(url.host);
    // No pool here, so a kept-alive connection is one we would
    // immediately drop; saying `close` lets the peer release its side.
    try w.writeAll("\r\nConnection: close\r\n");
    for (req.headers) |h| {
        // These come from configuration and from a rendered template, so
        // a value carrying CRLF would inject a header. Skipped rather
        // than sent — the adapters produce fixed names and JSON values,
        // so this is a backstop, not a filter anyone relies on.
        if (containsCrlf(h.name) or containsCrlf(h.value)) continue;
        try w.writeAll(h.name);
        try w.writeAll(": ");
        try w.writeAll(h.value);
        try w.writeAll("\r\n");
    }
    try w.print("Content-Length: {d}\r\n\r\n", .{req.body.len});
    try w.writeAll(req.body);
}

fn containsCrlf(s: []const u8) bool {
    return std.mem.indexOfAny(u8, s, "\r\n") != null;
}

/// `HTTP/1.1 204 No Content` -> 204.
fn parseStatus(line: []const u8) ?u16 {
    if (!std.mem.startsWith(u8, line, "HTTP/1.")) return null;
    if (line.len < 12) return null;
    return std.fmt.parseInt(u16, line[9..12], 10) catch null;
}

fn mapReadError(e: anyerror) ntransport.Error {
    return switch (e) {
        error.EndOfStream => error.Io,
        error.StreamTooLong => error.Io,
        else => error.Io,
    };
}

fn mapDialError(e: anyerror) ntransport.Error {
    return switch (e) {
        error.Canceled => error.Canceled,
        error.OutOfMemory => error.Io,
        else => error.Connect,
    };
}

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const server_mod = @import("../net/http/server.zig");

test "the subscription names are stable and one per topic" {
    // The outbox keys delivery on the subscription name. A name that
    // changed shape between builds would replay every event the old name
    // had already consumed, which for a webhook means a duplicate storm.
    try testing.expectEqual(notify_service.subscribable_topics.len, Notifier.sub_names.len);
    try testing.expectEqualStrings("notify.deliver.complete", Notifier.sub_names[10]);
    for (notify_service.subscribable_topics, Notifier.sub_names) |t, n| {
        try testing.expect(std.mem.startsWith(u8, n, "notify."));
        try testing.expectEqualStrings(t, n["notify.".len..]);
    }
}

test "a status line parses, and anything else is refused" {
    try testing.expectEqual(@as(?u16, 204), parseStatus("HTTP/1.1 204 No Content\r\n"));
    try testing.expectEqual(@as(?u16, 200), parseStatus("HTTP/1.0 200 OK\n"));
    try testing.expectEqual(@as(?u16, 500), parseStatus("HTTP/1.1 500\r\n"));
    try testing.expectEqual(@as(?u16, null), parseStatus("HTTP/2 200 OK\r\n"));
    try testing.expectEqual(@as(?u16, null), parseStatus("garbage"));
    try testing.expectEqual(@as(?u16, null), parseStatus("HTTP/1.1 2xx OK\r\n"));
}

test "the request head carries Host, Content-Length and the body" {
    var buf: [512]u8 = undefined;
    var w: Writer = .fixed(&buf);
    try writeHead(&w, .{
        .host = "example.invalid",
        .port = 80,
        .path = "/hook?x=1",
        .tls = false,
    }, .{
        .url = "http://example.invalid/hook?x=1",
        .headers = &.{
            .{ .name = "Content-Type", .value = "application/json" },
            // A value that would inject a second header must not be sent.
            .{ .name = "X-Evil", .value = "a\r\nX-Injected: yes" },
        },
        .body = "{\"a\":1}",
    });
    const out = w.buffered();
    try testing.expect(std.mem.startsWith(u8, out, "POST /hook?x=1 HTTP/1.1\r\n"));
    try testing.expect(std.mem.indexOf(u8, out, "Host: example.invalid\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "Content-Length: 7\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "Content-Type: application/json\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "X-Injected") == null);
    try testing.expect(std.mem.endsWith(u8, out, "\r\n\r\n{\"a\":1}"));
}

/// A `Dispatch` needs a `Notifier`, a loop and a database. These two
/// tests want only the transport, so they build the smallest thing that
/// can run one: a fiber, a loop, and our own HTTP server as the far end.
const Harness = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    n: Notifier,
    d: Dispatch = undefined,
    pending: Pending = undefined,
    result: ?(ntransport.Error!ntransport.Response) = null,
    url_buf: [128]u8 = undefined,
    url: []const u8 = "",

    fn body(f: *Fiber, ctx: ?*anyopaque) void {
        _ = f;
        const self: *Harness = @ptrCast(@alignCast(ctx.?));
        self.result = self.d.post(.{ .url = self.url, .body = "{\"ok\":true}" });
    }

    fn done(self: *Harness) bool {
        return self.result != null or self.d.finished;
    }
};

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, f: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!f(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

fn handleHook(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.sendStatus(204);
}

fn handleBusted(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.send(500, "text/plain", "nope");
}

const hook_routes = [_]server_mod.Route{
    .{ .method = .post, .path = "/hook", .handler = handleHook, .access = .public },
    .{ .method = .post, .path = "/busted", .handler = handleBusted, .access = .public },
};

test "a plaintext webhook is delivered without blocking the loop" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    srv.init(gpa, &loop, .{}, &hook_routes);
    try srv.listen(try IpAddress.parse("127.0.0.1", 0));
    defer srv.deinit();
    const port = try srv.boundPort();

    var h: Harness = .{ .gpa = gpa, .loop = &loop, .n = undefined };
    h.n = .{ .gpa = gpa, .loop = &loop, .db = undefined };
    h.url = try std.fmt.bufPrint(&h.url_buf, "http://127.0.0.1:{d}/hook", .{port});
    h.pending = .{
        .buf = &.{},
        .topic = "deliver.complete",
        .aggregate_id = "1",
        .payload = "{}",
        .occurred_at = 0,
        .id = .nil,
    };
    h.d = .{ .n = &h.n, .pending = &h.pending };
    try h.d.fiber.init(gpa, &loop, dispatch_stack_size, Harness.body, @ptrCast(&h));
    defer h.d.fiber.deinit();
    defer if (h.d.timer.isArmed()) loop.cancelTimer(&h.d.timer);
    defer if (h.d.deadline.isArmed()) loop.cancelTimer(&h.d.deadline);

    h.d.fiber.enter();
    try pumpUntil(&loop, 5000, &h, Harness.done);

    // The whole exercise: a real socket, a real request, a real status,
    // and the loop was dispatching the server's own connection the
    // entire time this fiber was parked.
    const resp = try h.result.?;
    try testing.expectEqual(@as(u16, 204), resp.status);
}

test "a 5xx comes back as a status, not as a transport error" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    srv.init(gpa, &loop, .{}, &hook_routes);
    try srv.listen(try IpAddress.parse("127.0.0.1", 0));
    defer srv.deinit();
    const port = try srv.boundPort();

    var h: Harness = .{ .gpa = gpa, .loop = &loop, .n = undefined };
    h.n = .{ .gpa = gpa, .loop = &loop, .db = undefined };
    h.url = try std.fmt.bufPrint(&h.url_buf, "http://127.0.0.1:{d}/busted", .{port});
    h.pending = .{
        .buf = &.{},
        .topic = "deliver.failed",
        .aggregate_id = "1",
        .payload = "{}",
        .occurred_at = 0,
        .id = .nil,
    };
    h.d = .{ .n = &h.n, .pending = &h.pending };
    try h.d.fiber.init(gpa, &loop, dispatch_stack_size, Harness.body, @ptrCast(&h));
    defer h.d.fiber.deinit();
    defer if (h.d.timer.isArmed()) loop.cancelTimer(&h.d.timer);
    defer if (h.d.deadline.isArmed()) loop.cancelTimer(&h.d.deadline);

    h.d.fiber.enter();
    try pumpUntil(&loop, 5000, &h, Harness.done);

    // `transport.deliver` decides whether a 500 is worth retrying; it can
    // only do that if the status reaches it.
    const resp = try h.result.?;
    try testing.expectEqual(@as(u16, 500), resp.status);
}

test "an https subscription with no CA roots fails loudly instead of downgrading" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var h: Harness = .{ .gpa = gpa, .loop = &loop, .n = undefined };
    h.n = .{ .gpa = gpa, .loop = &loop, .db = undefined };
    var logger: log.Logger = .{};
    h.n.logger = &logger;
    h.url = "https://hooks.slack.com/services/T00/B00/secret";
    h.pending = .{
        .buf = &.{},
        .topic = "deliver.complete",
        .aggregate_id = "1",
        .payload = "{}",
        .occurred_at = 0,
        .id = .nil,
    };
    h.d = .{ .n = &h.n, .pending = &h.pending };
    try h.d.fiber.init(gpa, &loop, dispatch_stack_size, Harness.body, @ptrCast(&h));
    defer h.d.fiber.deinit();
    defer if (h.d.timer.isArmed()) loop.cancelTimer(&h.d.timer);
    defer if (h.d.deadline.isArmed()) loop.cancelTimer(&h.d.deadline);

    h.d.fiber.enter();
    try pumpUntil(&loop, 5000, &h, Harness.done);

    // Never a silent plaintext POST of a bearer-credential URL.
    try testing.expectError(error.Tls, h.result.?);
}
