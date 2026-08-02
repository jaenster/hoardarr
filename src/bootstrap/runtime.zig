//! The running half of the composition root: what actually turns a queued
//! job into bytes on disk.
//!
//! Everything below `bootstrap.zig` is either a pure decision (the
//! orchestrator's `Runner`, the download `Service`) or a callback-driven
//! machine on the reactor (`nntp/pool.zig`, `nntp/conn.zig`). Those two
//! halves do not compose on their own, and this file is the two bridges
//! that make them.
//!
//! ## Bridge one: a synchronous fetch on a callback transport
//!
//! `orchestrator.Runner` reaches the network through
//! `ArticleFetcher.fetch(msg_id) -> bytes`, which is a *blocking*
//! signature — the retry policy, the batching and the failure
//! classification are all written as straight-line code around it, and
//! they are the part of the download path that has historically had the
//! bugs. `nntp/pool.zig` hands a connection to a callback and
//! `nntp/conn.zig` hands a body to another one.
//!
//! `posix/fiber.zig` is the answer, for the same reason it exists for
//! TLS: give the blocking-shaped code its own stack. When a fetch needs
//! the network its fiber `yield`s, and the pool's or the connection's
//! callback resumes it. From the `Runner`'s point of view `fetch`
//! returned bytes; from the reactor's point of view nothing ever blocked.
//!
//! **One fiber per connection slot, not one per job.** A blocking-shaped
//! fetch holds its stack for a whole round trip, so a job driven by a
//! single fiber has a single article outstanding: against a provider 30ms
//! away that is one article per 30ms however fast the link is, and the
//! other connection slots the operator paid for stay empty. So a
//! `JobSlot` is a coordinator fiber plus a set of `Worker` fibers, all
//! drawing from one cursor over the batch the `Runner` handed back —
//! a slow article then costs the worker that drew it rather than a fixed
//! share of the batch.
//!
//! The provider's `Pool` is what actually bounds the sockets, so workers
//! belonging to different jobs contend for one provider's slots by
//! queueing rather than by overshooting its cap. A stack is 1 MiB of
//! *address space*, demand-paged; `max_fetch_workers` bounds how many of
//! them a job can ask for whatever its `max_conns` claims.
//!
//! ### Every fiber entry happens from a timer
//!
//! This is the load-bearing rule of the file, and it is worth stating on
//! its own because violating it is a use-after-free rather than a
//! failing test.
//!
//! `Pool.release` may hand the connection straight to a queued waiter and
//! call that waiter's callback before returning. `Conn` calls `on_body`
//! from the middle of `consumeBody`, with its input buffer half-drained
//! and its state machine mid-transition. If a fiber resumed *inside*
//! those callbacks it would run the next `fetchBody` on a connection that
//! is still inside its own read loop, and release a connection from
//! inside the dispatch that owns it.
//!
//! So a callback never enters a fiber. It records its result and arms a
//! zero-delay reactor timer; the fiber resumes from that timer's
//! callback, at the top of the loop's dispatch, with no other frame of
//! ours on the stack. That is one timer arm per segment — a `log n` heap
//! insert against a network round trip — and it makes the re-entrancy
//! question closed rather than argued.
//!
//! ## Bridge two: the outbox on the reactor
//!
//! `outbox.Bus.subscribe` starts a thread per subscription, each with its
//! own `*sqlite.Conn`. Every subscriber the pipeline needs — verify,
//! repair, extract, deliver, notify — has to touch the application's
//! connection and the application's services, which are single-threaded.
//! A thread could only hand the work back over `Loop.wake()`, which is a
//! queue plus a lock plus a second copy of every event, for nothing.
//!
//! `Bus.subscribeInline` (added for this) registers the handler with no
//! thread; `Dispatcher` below registers the subscription's waker fd as a
//! reactor source and pumps it when it fires. Same waker, same
//! at-least-once contract, no second connection. The one thing the
//! threaded loop did that a nudge cannot is notice that a *failed*
//! delivery's `next_retry_at` has come due, so a pump that reports
//! failures arms a retry timer — and only then. An idle daemon has no
//! dispatcher timer armed at all, which is the property the whole port
//! is for.
//!
//! The bus keeps exactly one thread: the pruner. It is not a dispatcher —
//! it opens its own connection, hands nothing to the loop, and sleeps
//! between batches on purpose so its first sweep over a million rows does
//! not hold the write lock. See `outbox.Bus.startPruner`.
//!
//! ## Getting to a provider
//!
//! Both halves of "connect to news.example.com:563 over TLS" are
//! callback-based like everything else here, and both are driven from the
//! job's fiber — ask, `park`, continue:
//!
//!   * **Hostnames** go through `net/dns.zig`'s resolver. The answer is
//!     latched on the pool, so a provider costs one round trip per
//!     process rather than one per segment.
//!   * **TLS** is `nntp/conn.zig`'s own `Security.tls`, which runs
//!     `std.crypto.tls.Client` on a fiber of its own. What this file
//!     supplies is the trust anchors, and that is the one thing an
//!     operator can find missing: see `CaRoots`.
//!
//! A server the build cannot reach — TLS with no roots to verify against
//! — is registered anyway so the System page shows it, and says why in
//! the log and in the probe result. It is never a fetch candidate.

const std = @import("std");

const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const fiber_mod = @import("../posix/fiber.zig");
const log = @import("../core/log.zig");

const sqlite = @import("../store/sqlite.zig");
const outbox = @import("../store/outbox.zig");
const repo_server = @import("../store/repo_server.zig");

const nntp_conn = @import("../nntp/conn.zig");
const nntp_pool = @import("../nntp/pool.zig");
const dns = @import("../net/dns.zig");

const app_ports = @import("../app/ports.zig");
const dl_ports = @import("../app/download/ports.zig");
const orchestrator = @import("../app/download/orchestrator.zig");
const dl_service = @import("../app/download/service.zig");
const tiered = @import("../app/download/tiered_fetcher.zig");
const bandwidth = @import("../app/download/bandwidth.zig");
const byte_accounter = @import("../app/download/byte_accounter.zig");

const rest_ports = @import("../api/rest/ports.zig");
const dserver = @import("../domain/server.zig");

const Allocator = std.mem.Allocator;
const Fiber = fiber_mod.Fiber;
const IpAddress = std.Io.net.IpAddress;
const JobId = dl_ports.JobId;
const ServerId = dserver.ServerId;

/// Stack for a job fiber.
///
/// `posix/fiber.zig` measured 148 KB for a TLS handshake in ReleaseFast
/// and 506 KB in Debug; a `Runner` loop is shallower than that but it
/// calls into SQLite, the yEnc decoder and the filesystem port, so the
/// module default is what it gets. It is address space, not memory — a
/// job touches the pages it uses.
const job_stack_size = fiber_mod.default_stack_size;

/// Ceiling on a job's fetch fibers, whatever its provider's `max_conns`
/// says.
///
/// Two reasons for a ceiling rather than trusting the row. One is that
/// `max_conns` is operator input and a typo must not turn into gigabytes
/// of mappings: the reservation is `max_concurrent_jobs × this`, and at
/// 32 that is 32 MiB of address space for the shipped single-job default.
/// The other is that it buys nothing past this — 32 articles in flight at
/// a 50ms round trip is several hundred MiB/s of headroom, which is
/// already far past the uplink of anything this daemon runs on.
const max_fetch_workers: u16 = 32;

/// Ceiling on how long the daemon stalls for one operator-initiated
/// connection test. See `Probe`.
const probe_deadline_ns: u64 = 8 * std.time.ns_per_s;

// =====================================================================
// Reactor-driven outbox dispatcher
// =====================================================================

/// One `outbox` subscription serviced by the event loop.
///
/// Two wake-ups, and only two: the subscription's waker fd going
/// readable (a publisher committed), and a retry timer that is armed
/// only while a delivery is actually owed a retry. Nothing polls.
pub const Dispatcher = struct {
    rt: *Runtime,
    sub: *outbox.Subscription,
    source: reactor.Source,
    retry: reactor.Timer,
    registered: bool = false,

    /// How long after a failed delivery to look again. The bus's own
    /// backoff decides whether the row is actually due; this only has to
    /// be short enough that a due row is not left sitting.
    const retry_interval_ns: u64 = std.time.ns_per_s;

    fn onReadable(src: *reactor.Source, ready: reactor.Ready) void {
        const self: *Dispatcher = @fieldParentPtr("source", src);
        if (!ready.read) return;
        self.run();
    }

    fn onRetry(t: *reactor.Timer) void {
        const self: *Dispatcher = @fieldParentPtr("retry", t);
        self.run();
    }

    fn run(self: *Dispatcher) void {
        if (self.rt.stopping) return;
        // Drain *before* pumping: a handler that publishes re-signals
        // this very fd, and draining afterwards would swallow the nudge
        // its own work produced. Draining first can only cost one extra
        // empty SELECT.
        self.sub.drainWake();
        const pumped = self.sub.pump(self.rt.db);
        if (pumped.failed > 0) self.armRetry();
    }

    fn armRetry(self: *Dispatcher) void {
        if (self.retry.isArmed()) return;
        self.rt.loop.addTimer(&self.retry, retry_interval_ns) catch {};
    }

    fn deinit(self: *Dispatcher) void {
        if (self.retry.isArmed()) self.rt.loop.cancelTimer(&self.retry);
        if (self.registered) {
            self.rt.loop.remove(&self.source);
            self.registered = false;
        }
    }
};

// =====================================================================
// Provider pools
// =====================================================================

/// One provider: its `nntp.Pool`, the credentials the connections use,
/// and the snapshot the tiered fetcher sorts on.
pub const ServerPool = struct {
    rt: *Runtime,
    id: ServerId,
    /// Owned. `info.name`, `conn_config`'s credentials and the resolver's
    /// query all borrow these for the pool's lifetime, so they cannot
    /// live in the row that produced them.
    name: []u8,
    host: []u8,
    username: []u8,
    password: []u8,
    port: u16,

    info: dl_ports.PoolInfo,
    pool: nntp_pool.Pool,
    /// False when this build cannot dial the row at all — currently only
    /// TLS. The pool is still registered so the System page shows the
    /// server; it is simply never a fetch candidate.
    dialable: bool,
    /// Why not, for the log and the probe. Static text.
    undialable_reason: []const u8 = "",
    /// `pool.addr` holds a real address. False for a hostname until the
    /// first fetch resolves it on a fiber.
    resolved: bool = false,
    /// A worker is asking the resolver right now. The others wait for its
    /// answer instead of each sending the same query.
    resolving: bool = false,

    fn deinit(self: *ServerPool) void {
        if (self.dialable) self.pool.deinit();
        const gpa = self.rt.gpa;
        gpa.free(self.name);
        gpa.free(self.host);
        gpa.free(self.username);
        gpa.free(self.password);
    }
};

/// What can be done with a row's `host`/`tls`.
///
/// One function so the log line, the probe and the fetch path cannot
/// disagree about what is supported.
pub const Dialability = union(enum) {
    /// An address literal: no resolution needed, ever.
    literal: IpAddress,
    /// A hostname. Resolved on a fiber at first use.
    needs_dns,
    /// Not reachable by this configuration at all.
    unsupported: []const u8,
};

pub fn dialability(host: []const u8, port: u16, tls: bool, have_roots: bool) Dialability {
    if (host.len == 0) return .{ .unsupported = "no host configured" };
    // Trust is not optional and it is not something to guess at: a
    // provider connection carries the account's password, and dialling
    // TLS with nothing to verify against is worse than refusing, because
    // it looks like it worked.
    if (tls and !have_roots) return .{
        .unsupported = "TLS needs CA roots and none were found; see the startup log",
    };
    var text = host;
    if (text.len >= 2 and text[0] == '[' and text[text.len - 1] == ']') text = text[1 .. text.len - 1];
    if (IpAddress.parse(text, port)) |ip| return .{ .literal = ip } else |_| {}
    return .needs_dns;
}

// =====================================================================
// Trust anchors
// =====================================================================

/// Where a Linux or macOS host keeps its concatenated PEM root bundle.
///
/// Tried in order; the first that reads wins. The shipping image is
/// `scratch` and has none of them, which is deliberate — an operator who
/// wants TLS providers mounts a bundle at one of these paths, and the
/// startup log says so when they have not.
pub const ca_bundle_paths = [_][:0]const u8{
    "/etc/ssl/certs/ca-certificates.crt", // Debian, Ubuntu, Alpine
    "/etc/pki/tls/certs/ca-bundle.crt", // RHEL, Fedora
    "/etc/ssl/cert.pem", // macOS, BSD, Alpine
};

/// A bundle no larger than this. The real ones are ~200 KiB; anything
/// past this is a mistake, not a certificate store.
const max_ca_bundle_bytes: u64 = 8 << 20;

/// Loads the system root bundle into a `CaStore`, or reports that there
/// isn't one.
pub const CaRoots = struct {
    store: nntp_conn.CaStore,
    /// Number of anchors indexed. Zero means TLS providers cannot be
    /// verified and so are not dialled.
    count: usize = 0,
    /// The file the anchors came from, for the startup log. Borrowed
    /// from `ca_bundle_paths`.
    source: []const u8 = "",

    pub fn init(gpa: Allocator) CaRoots {
        return .{ .store = .init(gpa) };
    }

    pub fn deinit(self: *CaRoots) void {
        self.store.deinit();
    }

    pub fn haveRoots(self: *const CaRoots) bool {
        return self.count > 0;
    }

    /// Reads the first bundle that exists. Failure is not an error: a
    /// container without one simply cannot use TLS providers, which the
    /// caller reports rather than crashing over.
    pub fn load(self: *CaRoots, gpa: Allocator, now_sec: i64) void {
        for (ca_bundle_paths) |path| {
            const pem = readWhole(gpa, path) catch continue;
            defer gpa.free(pem);
            const n = self.store.addPem(pem, now_sec) catch continue;
            if (n == 0) continue;
            self.count = self.store.count();
            self.source = path;
            return;
        }
    }

    fn readWhole(gpa: Allocator, path: [:0]const u8) ![]u8 {
        const fd = try sys.open(path, .{ .mode = .read_only });
        defer sys.close(fd);
        const size = try sys.fileSize(fd);
        if (size == 0 or size > max_ca_bundle_bytes) return error.Unexpected;

        const buf = try gpa.alloc(u8, @intCast(size));
        errdefer gpa.free(buf);
        // `fileSize` left the offset at the end.
        const fd2 = try sys.open(path, .{ .mode = .read_only });
        defer sys.close(fd2);
        var off: usize = 0;
        while (off < buf.len) {
            const n = try sys.read(fd2, buf[off..]);
            if (n == 0) break;
            off += n;
        }
        return buf[0..off];
    }
};

// =====================================================================
// One in-flight job
// =====================================================================

/// The state of the NNTP round trip a worker is currently inside.
///
/// Lives on the worker's stack for exactly the duration of one
/// `fetchOne`; the worker holds a pointer to it so the callbacks can find
/// it. Nothing here is touched after the fiber has returned from that
/// call.
const Request = struct {
    worker: *Worker,
    /// Allocator the body is duped with — the one the `Runner` passed
    /// into `fetch`, so the bytes belong to the caller's arena.
    a: Allocator,
    /// The pool the connection came from, so the fiber can hand it back.
    sp: *ServerPool,

    /// Set by whichever callback answered. The fiber only yields while
    /// this is false, which is what makes a synchronous callback (an
    /// idle connection was available) correct without a special case.
    settled: bool = false,
    conn: ?*nntp_conn.Conn = null,
    err: ?anyerror = null,
    body: ?[]u8 = null,
    addr: ?IpAddress = null,

    /// The pool destroyed the entry under us — `Pool.Entry.onError`
    /// discards a failed connection whether or not it is checked out. So
    /// the fiber must neither restore the handler nor release it.
    conn_dead: bool = false,
    /// The pool's own handler, restored on the way out.
    saved: ?*const nntp_conn.Handler = null,

    /// Handler installed on a checked-out connection.
    ///
    /// `Pool.Entry.handler.on_body` is a documented no-op — the pool does
    /// not consume bodies, it says the owner installs its own
    /// expectations — but there is no helper for doing so, and
    /// `Conn.handler` is a plain mutable field. Swapping it for the
    /// duration of one fetch is that installation. `on_error` forwards to
    /// the pool's handler afterwards so the pool's slot accounting still
    /// happens.
    const handler: nntp_conn.Handler = .{
        .on_ready = onReady,
        .on_body = onBody,
        .on_error = onError,
    };

    fn onReady(_: *nntp_conn.Conn) void {
        // A checked-out connection is already past its handshake. Reaching
        // here would mean the pool handed out a connecting one.
        unreachable;
    }

    fn onBody(c: *nntp_conn.Conn, payload: []const u8) void {
        const self: *Request = @ptrCast(@alignCast(c.context.?));
        // `payload` borrows the connection's buffer and is valid only for
        // this call, so the copy happens here rather than after the
        // fiber resumes.
        self.body = self.a.dupe(u8, payload) catch blk: {
            self.err = error.OutOfMemory;
            break :blk null;
        };
        self.settle();
    }

    fn onError(c: *nntp_conn.Conn, err: nntp_conn.Error) void {
        const self: *Request = @ptrCast(@alignCast(c.context.?));
        self.err = err;
        // The pool's handler frees the entry and the connection with it,
        // so nothing below may touch `c`. Flagging first is what stops
        // the fiber from restoring a handler on freed memory.
        self.conn_dead = true;
        if (self.saved) |h| h.on_error(c, err);
        self.settle();
    }

    fn settle(self: *Request) void {
        self.settled = true;
        // Never enter the fiber from here — see the module comment.
        if (self.worker.awaiting) self.worker.armTimer(0);
    }

    /// `dns.ResolveFn`. Fires before `resolve` returns for a literal or a
    /// cache hit, which is why `settled` is checked rather than assumed.
    fn onResolved(ctx: ?*anyopaque, result: dns.Error!IpAddress) void {
        const self: *Request = @ptrCast(@alignCast(ctx.?));
        if (result) |addr| {
            self.addr = addr;
        } else |e| {
            self.err = e;
        }
        self.settle();
    }
};

/// How long a worker waits before looking again at a provider another
/// worker is currently resolving. Runs once per provider per process, so
/// the granularity costs nothing measurable.
const resolve_poll_ns: u64 = 5 * std.time.ns_per_ms;

/// One of a job's fetch fibers: claim a segment, drive it to a verdict,
/// repeat until the batch is empty.
///
/// Everything that talks to a provider lives here rather than on the
/// `JobSlot`, because the state of a round trip — the `Request` the
/// callbacks write into, the fiber they resume — is per fetch, and a job
/// keeps several fetches outstanding at once.
const Worker = struct {
    slot: *JobSlot,
    /// Per worker, not per job. The `PoolSet` it holds has *this worker*
    /// as its context, which is what lets `fetchOne` park the right fiber
    /// without a "current fiber" global that a second worker would
    /// corrupt.
    fetcher: tiered.TieredFetcher = undefined,

    fiber: Fiber = undefined,
    fiber_ready: bool = false,
    /// Resume clock, exactly as the coordinator's: one timer serves both
    /// "the runner asked for a backoff" and "a callback answered,
    /// continue at the top of the next dispatch", because a worker is
    /// only ever waiting for one of them.
    timer: reactor.Timer = .{ .callback = onTimer },
    request: ?*Request = null,

    /// The fiber is yielded and something is expected to resume it.
    awaiting: bool = false,
    /// Parked with nothing in hand, waiting to be given the next batch.
    ///
    /// Only an idle worker may be woken by the coordinator. Waking one
    /// that is mid-round-trip would return it from `park` with its
    /// `Request` unsettled and the connection's callback still aimed at
    /// its stack.
    idle: bool = false,
    /// The fiber has been entered at least once, so there is something on
    /// its stack to unwind.
    entered: bool = false,
    /// The body has returned; the stack is idle.
    done: bool = false,

    // -- the fiber body -----------------------------------------------

    fn entry(f: *Fiber, ctx: ?*anyopaque) void {
        _ = f;
        const self: *Worker = @ptrCast(@alignCast(ctx.?));
        self.run() catch |e| {
            if (e == error.Canceled) return;
            // The segments this worker held stay pending and a later
            // dispatch re-takes them. What must not happen is the
            // coordinator waiting on a fetch nobody is going to make, so
            // the failure is recorded where it will surface.
            self.slot.fatal = e;
            self.slot.rt.logger.err("download: fetch worker stopped", &.{
                log.int("job_id", self.slot.job_id),
                log.errv("err", e),
            });
        };
    }

    fn run(self: *Worker) !void {
        const slot = self.slot;
        while (true) {
            if (slot.canceled) return error.Canceled;
            if (slot.claim()) |seg| {
                defer slot.resolveOne();
                try self.driveSegment(seg);
                continue;
            }
            // Nothing left in this batch. Workers outlive it so the next
            // one costs no `mmap`; only the end of the job retires them.
            if (slot.draining) return;
            self.idle = true;
            self.park();
            self.idle = false;
        }
    }

    /// One segment, from dispatch to a verdict, honouring every backoff
    /// the runner asks for as a reactor timer.
    fn driveSegment(self: *Worker, seg: *orchestrator.Segment) !void {
        const slot = self.slot;
        const runner = slot.runner orelse return;

        // An arena per worker, reset between attempts: a body is hundreds
        // of kilobytes and is dead the instant the segment resolves, so
        // one arena shared across the batch would hold every body of it
        // until the batch ended.
        var arena = std.heap.ArenaAllocator.init(slot.rt.gpa);
        defer arena.deinit();

        var task: orchestrator.SegmentTask = .{
            .segment_id = seg.id,
            .message_id = seg.message_id,
        };
        while (true) {
            if (slot.canceled) return error.Canceled;
            const now = slot.rt.clock.now();
            switch (runner.step(arena.allocator(), self.fetcher.fetcher(), &task, now)) {
                .resolved => |r| return runner.submit(r),
                .wait_until => |at| {
                    _ = arena.reset(.retain_capacity);
                    try self.sleepUntil(at);
                },
            }
        }
    }

    /// Park until `at` (Unix millis). Zero and past deadlines still go
    /// through the timer, so the loop gets a chance to dispatch between
    /// segments instead of one worker monopolising it.
    fn sleepUntil(self: *Worker, at: app_ports.Timestamp) !void {
        const now = self.slot.rt.clock.now();
        const delay_ms: u64 = if (at > now) @intCast(at - now) else 0;
        self.armTimer(delay_ms * std.time.ns_per_ms);
        self.park();
        if (self.slot.canceled) return error.Canceled;
    }

    /// Switch back to the loop. Only an explicit `enter` — from this
    /// worker's timer, and from nowhere else — brings the fiber back.
    fn park(self: *Worker) void {
        self.awaiting = true;
        self.fiber.yield();
        self.awaiting = false;
    }

    fn armTimer(self: *Worker, delay_ns: u64) void {
        const loop = self.slot.rt.loop;
        if (self.timer.isArmed()) loop.cancelTimer(&self.timer);
        loop.addTimer(&self.timer, delay_ns) catch {
            // The heap could not grow. The worker would otherwise wait
            // forever, so take the whole job down instead of stranding it.
            self.slot.canceled = true;
            loop.addTimer(&self.timer, 0) catch {};
        };
    }

    fn onTimer(t: *reactor.Timer) void {
        const self: *Worker = @fieldParentPtr("timer", t);
        self.enter();
    }

    /// The one place a worker fiber is switched into.
    fn enter(self: *Worker) void {
        if (self.done or !self.fiber_ready) return;
        if (self.fiber.isDone()) {
            self.retire();
            return;
        }
        self.entered = true;
        self.fiber.enter();
        if (self.fiber.isDone()) self.retire();
    }

    /// The fiber has returned. Nudge the coordinator, which is very
    /// likely parked waiting for exactly this — through its timer, never
    /// by entering it from here.
    fn retire(self: *Worker) void {
        if (self.done) return;
        self.done = true;
        self.slot.live -= 1;
        self.slot.armTimer(0);
    }

    fn deinit(self: *Worker) void {
        if (self.timer.isArmed()) self.slot.rt.loop.cancelTimer(&self.timer);
        if (self.fiber_ready) self.fiber.deinit();
        self.fiber_ready = false;
    }

    // -- the PoolSet the tiered fetcher sees --------------------------

    fn poolSet(self: *Worker) dl_ports.PoolSet {
        return .{
            .ctx = @ptrCast(self),
            .snapshotFn = &snapshotFn,
            .fetchOneFn = &fetchOneFn,
        };
    }

    fn snapshotFn(ctx: *anyopaque, a: Allocator) Allocator.Error![]dl_ports.PoolInfo {
        const self: *Worker = @ptrCast(@alignCast(ctx));
        return self.slot.rt.poolSnapshot(a);
    }

    fn fetchOneFn(
        ctx: *anyopaque,
        a: Allocator,
        id: ServerId,
        message_id: []const u8,
    ) dl_ports.FetchError![]u8 {
        const self: *Worker = @ptrCast(@alignCast(ctx));
        const sp = self.slot.rt.poolById(id) orelse return error.NoPoolsAvailable;
        return self.fetchOne(a, sp, message_id);
    }

    /// A blocking-shaped `BODY <id>` over a callback transport.
    ///
    /// Runs on the worker's stack. Every wait is a `park`; every resume
    /// comes from `Request.settle` arming this worker's timer.
    fn fetchOne(
        self: *Worker,
        a: Allocator,
        sp: *ServerPool,
        message_id: []const u8,
    ) dl_ports.FetchError![]u8 {
        const slot = self.slot;
        if (slot.canceled or slot.rt.stopping) return error.Canceled;

        var req: Request = .{ .worker = self, .a = a, .sp = sp };
        self.request = &req;
        defer self.request = null;

        try self.resolveProvider(sp, &req);

        // ---- a connection ----
        sp.pool.acquire(onAcquired, &req);
        while (!req.settled) {
            self.park();
            if (slot.canceled) {
                // The queue still holds this frame's address, and this
                // frame is about to stop existing.
                sp.pool.cancelAcquire(&req);
                return error.Canceled;
            }
        }
        if (req.err) |e| return mapPoolError(e);
        const c = req.conn orelse return error.Network;

        // ---- the body ----
        req.settled = false;
        req.saved = c.handler;
        c.context = @ptrCast(&req);
        c.handler = &Request.handler;

        c.fetchBody(message_id) catch |e| {
            // The write failed but the connection object is intact, so it
            // is ours to hand back — as failed, because the stream's
            // framing is now a guess.
            c.handler = req.saved.?;
            c.context = null;
            sp.pool.release(c, true);
            return mapConnError(e);
        };

        while (!req.settled) {
            self.park();
            if (slot.canceled) {
                // Shutdown mid-body. The connection is still registered
                // with the loop and still owned by the pool; giving it
                // back as failed is what stops it being handed to another
                // fiber with a half-consumed block in its buffer.
                if (!req.conn_dead) {
                    c.handler = req.saved.?;
                    c.context = null;
                    sp.pool.release(c, true);
                }
                return error.Canceled;
            }
        }

        if (!req.conn_dead) {
            c.handler = req.saved.?;
            c.context = null;
            sp.pool.release(c, req.err != null);
        }

        if (req.err) |e| return mapConnError(e);
        return req.body orelse error.Network;
    }

    /// Turn a provider's hostname into an address. Once per provider per
    /// process: `Pool.addr` is what every later dial uses, so a hostname
    /// costs one round trip here and nothing afterwards.
    ///
    /// Serialised on `sp.resolving`, because a job's workers all reach
    /// their first fetch in the same instant and N queries for one name
    /// would be N answers aimed at N stacks. The latecomers look again on
    /// a short timer rather than asking.
    fn resolveProvider(self: *Worker, sp: *ServerPool, req: *Request) dl_ports.FetchError!void {
        if (sp.resolved) return;
        const slot = self.slot;

        if (sp.resolving) {
            while (sp.resolving and !sp.resolved) {
                self.armTimer(resolve_poll_ns);
                self.park();
                if (slot.canceled) return error.Canceled;
            }
            return if (sp.resolved) {} else error.Network;
        }

        const resolver = slot.rt.resolver orelse return error.NoPoolsAvailable;
        sp.resolving = true;
        defer sp.resolving = false;

        resolver.resolve(sp.host, sp.port, Request.onResolved, @ptrCast(req));
        while (!req.settled) {
            self.park();
            if (slot.canceled) return error.Canceled;
        }
        if (req.err) |e| {
            slot.rt.logger.warn("nntp: cannot resolve provider", &.{
                log.str("server", sp.name),
                log.str("host", sp.host),
                log.errv("err", e),
            });
            return error.Network;
        }
        sp.pool.addr = req.addr orelse return error.Network;
        sp.resolved = true;
        req.settled = false;
    }

    fn onAcquired(ctx: ?*anyopaque, result: nntp_pool.Error!*nntp_conn.Conn) void {
        const req: *Request = @ptrCast(@alignCast(ctx.?));
        if (result) |c| {
            req.conn = c;
        } else |e| {
            req.err = e;
        }
        req.settle();
    }
};

/// One job being driven: the fiber that decides, and the fibers that
/// fetch.
///
/// The coordinator owns the `Runner` and therefore every decision —
/// which segments are eligible, when to flush, when the job is done. It
/// never touches the network itself; it hands a batch to the workers and
/// parks until they have resolved all of it.
pub const JobSlot = struct {
    rt: *Runtime,
    job_id: JobId,
    hint: dl_service.Hint,

    fiber: Fiber = undefined,
    fiber_ready: bool = false,
    /// Resume clock. One timer serves every reason a coordinator ever
    /// waits — a poll gap the runner asked for, or a worker reporting in —
    /// because it is only ever waiting for one of them.
    timer: reactor.Timer = .{ .callback = onTimer },

    /// The fiber is yielded and something is expected to resume it.
    awaiting: bool = false,
    /// Shutdown, pause or removal. Checked after every yield, by the
    /// coordinator and by every worker; each unwinds through its own
    /// defers rather than being discarded.
    canceled: bool = false,
    /// The body returned. The slot is reaped from a timer, never from
    /// inside the frame that entered the fiber.
    finished: bool = false,
    /// Set once the fiber has been entered at least once.
    entered: bool = false,

    /// The runner the workers step. Borrowed from `body`'s frame, which
    /// outlives every worker because `drainWorkers` runs before it
    /// returns.
    runner: ?*orchestrator.Runner = null,

    workers: std.ArrayList(*Worker) = .empty,
    /// Workers whose fibers have not returned yet.
    live: usize = 0,
    /// No more batches are coming: a worker that finds nothing to claim
    /// exits instead of parking for the next one.
    draining: bool = false,
    /// What killed a worker, surfaced by the coordinator so a job whose
    /// fetchers have all died fails rather than hangs.
    fatal: ?anyerror = null,

    /// The segments handed to the workers, the next one nobody has
    /// claimed, and how many are claimed but unresolved. A shared cursor
    /// rather than a slice each: one slow article then costs the worker
    /// that drew it, not a fixed share of the batch.
    batch: []*orchestrator.Segment = &.{},
    cursor: usize = 0,
    busy: usize = 0,
    /// Segments resolved since the last flush, and how many of them are
    /// worth one. A batch is several times the worker count so that a
    /// worker finishing early has the next segment waiting, but the
    /// aggregate only learns a segment resolved when the batch is
    /// applied — so progress, `done_bytes` and the outbox would
    /// otherwise move in steps of a whole batch. Flushing every
    /// `flush_every` completions keeps the write cadence, and what the
    /// UI reads, at the granularity of the connection count.
    since_flush: usize = 0,
    flush_every: usize = 1,

    // -- the fiber body -----------------------------------------------

    fn entry(f: *Fiber, ctx: ?*anyopaque) void {
        _ = f;
        const self: *JobSlot = @ptrCast(@alignCast(ctx.?));
        self.body() catch |e| {
            if (e == error.Canceled) {
                self.rt.logger.info("download: runner canceled", &.{
                    log.int("job_id", self.job_id),
                });
                return;
            }
            // A job that cannot be driven is a failed job, not a wedged
            // loop: the slot is freed by the reaper either way and the
            // segments stay pending for a later attempt.
            self.rt.logger.err("download: runner stopped", &.{
                log.int("job_id", self.job_id),
                log.errv("err", e),
            });
        };
    }

    /// The whole of one job: load the aggregate, drive its segments to a
    /// verdict, persist. Every wait in here is a yield, never a sleep.
    fn body(self: *JobSlot) !void {
        const rt = self.rt;

        const job = try rt.job_store.byId(null, self.job_id);
        defer rt.job_store.release(job);

        var opts = rt.orchestrator_opts;
        // One fetch fiber per connection slot the provider sold, because
        // a segment is a round trip and a job with one of them in flight
        // leaves the rest of what was bought idle.
        opts.workers = @max(@min(self.hint.max_conns, max_fetch_workers), 1);
        if (rt.settings_ratio) |k| {
            const raw = k.read();
            if (raw > 0) opts.fail_hopeless_ratio = @as(f64, @floatFromInt(raw)) / 100.0;
        }

        var runner = try orchestrator.Runner.init(.{
            .gpa = rt.gpa,
            .store = rt.job_store,
            .sink = rt.sink,
            .txm = rt.txm,
            .fs = rt.fs,
            .clock = rt.clock,
            .logger = rt.logger,
            .opts = opts,
            .hint_server = self.hint.server_id,
            .job = job,
            .incomplete_dir = rt.incomplete_dir,
        });
        defer runner.deinit();
        self.runner = &runner;

        // Declared after `runner.deinit` so it runs *before* it: a worker
        // holds the runner for as long as its fiber has a frame, and
        // tearing the runner down under a live worker is a use-after-free
        // rather than a failing test.
        defer self.drainWorkers();

        try runner.begin(rt.clock.now());

        while (true) {
            if (self.canceled) return error.Canceled;

            var arena = std.heap.ArenaAllocator.init(rt.gpa);
            defer arena.deinit();
            const a = arena.allocator();

            switch (try runner.verdict(a, rt.clock.now())) {
                .done => break,
                .wait_until => |at| try self.sleepUntil(at),
                .work => |ready| {
                    defer a.free(ready);
                    try self.runBatch(ready, opts.workers);
                    // Unconditional, not `flushIfDue`: the aggregate only
                    // learns a segment resolved when the batch is
                    // applied, so an unflushed batch would make the next
                    // `verdict` hand back the very segments just
                    // fetched. The batch is already capped by
                    // `Options.batchSize`, which is what keeps the write
                    // rate sane.
                    try runner.flush(rt.clock.now());
                },
            }
        }
        try runner.flush(rt.clock.now());
    }

    /// Hand `ready` to the workers and park until every segment of it has
    /// a verdict.
    fn runBatch(self: *JobSlot, ready: []*orchestrator.Segment, want: u16) !void {
        self.batch = ready;
        self.cursor = 0;
        defer {
            self.batch = &.{};
            self.cursor = 0;
        }

        self.flush_every = @max(want, 1);
        self.since_flush = 0;

        self.ensureWorkers(@min(ready.len, @as(usize, want)));
        if (self.live == 0) return error.NoFetchWorkers;
        // Only the idle ones: a worker still inside a round trip has a
        // callback aimed at its stack and must be left to its own timer.
        for (self.workers.items) |w| {
            if (w.idle) w.armTimer(0);
        }

        while (self.cursor < self.batch.len or self.busy > 0) {
            if (self.live == 0) break;
            self.park();
            if (self.canceled) return error.Canceled;
            if (self.since_flush >= self.flush_every) {
                self.since_flush = 0;
                // Safe with fetches outstanding: a worker only runs while
                // this fiber is parked, so the aggregate and the store see
                // one writer, and the segments still in flight are exactly
                // the ones not in the batch being applied.
                if (self.runner) |r| try r.flush(self.rt.clock.now());
            }
        }
        if (self.fatal) |e| return e;
    }

    /// Grow the worker set to `want`, which only happens on the first
    /// batch of a job unless a later one is wider.
    fn ensureWorkers(self: *JobSlot, want: usize) void {
        const rt = self.rt;
        while (self.workers.items.len < want) {
            const w = rt.gpa.create(Worker) catch break;
            w.* = .{ .slot = self };
            w.fetcher = .{
                .gpa = rt.gpa,
                .pools = w.poolSet(),
                .logger = rt.logger,
                .accounter = rt.accounter,
                .limiter = rt.limiter,
                .clock = rt.clock,
            };
            w.fiber.init(rt.gpa, rt.loop, job_stack_size, Worker.entry, w) catch {
                rt.gpa.destroy(w);
                break;
            };
            w.fiber_ready = true;
            self.workers.append(rt.gpa, w) catch {
                w.fiber.deinit();
                rt.gpa.destroy(w);
                break;
            };
            self.live += 1;
            w.armTimer(0);
        }
        if (self.workers.items.len < want) {
            // Fewer fetchers than connection slots is slower, not wrong,
            // and is the right answer to memory pressure — but it is not
            // something to discover from a throughput graph.
            rt.logger.warn("download: could not start every fetch worker", &.{
                log.int("job_id", self.job_id),
                log.uint("workers", self.workers.items.len),
                log.uint("wanted", want),
            });
        }
    }

    /// Stop every worker and wait for its fiber to return.
    ///
    /// Runs on every exit path, before the runner is torn down. Waking a
    /// worker that is mid-round-trip is safe here and only here: it
    /// resumes, sees `canceled` or `draining`, and unwinds through
    /// `fetchOne`'s own cancel handling, which is what gives its
    /// connection back.
    fn drainWorkers(self: *JobSlot) void {
        self.draining = true;
        for (self.workers.items) |w| {
            if (!w.done) w.armTimer(0);
        }
        // No guard on this loop on purpose: giving up would leave live
        // fibers pointing at a runner that is about to be destroyed, and
        // a wedged job is a better failure than that. `Runtime.cancelSlot`
        // is what guarantees progress when the loop is no longer running.
        while (self.live > 0) self.park();
    }

    /// The next unclaimed segment of the batch, if any.
    fn claim(self: *JobSlot) ?*orchestrator.Segment {
        if (self.cursor >= self.batch.len) return null;
        const seg = self.batch[self.cursor];
        self.cursor += 1;
        self.busy += 1;
        return seg;
    }

    /// A claimed segment reached a verdict.
    fn resolveOne(self: *JobSlot) void {
        self.busy -= 1;
        self.since_flush += 1;
        const batch_done = self.cursor >= self.batch.len and self.busy == 0;
        if (batch_done or self.since_flush >= self.flush_every) self.armTimer(0);
    }

    /// Park the fiber until `at` (Unix millis). Zero and past deadlines
    /// still go through the timer, so the loop gets a chance to dispatch
    /// instead of one job monopolising it.
    fn sleepUntil(self: *JobSlot, at: app_ports.Timestamp) !void {
        const now = self.rt.clock.now();
        const delay_ms: u64 = if (at > now) @intCast(at - now) else 0;
        self.armTimer(delay_ms * std.time.ns_per_ms);
        self.park();
        if (self.canceled) return error.Canceled;
    }

    /// Switch back to the loop. Only an explicit `enter` — from the
    /// slot's timer, and from nowhere else — brings the fiber back.
    fn park(self: *JobSlot) void {
        self.awaiting = true;
        self.fiber.yield();
        self.awaiting = false;
    }

    fn armTimer(self: *JobSlot, delay_ns: u64) void {
        if (self.timer.isArmed()) self.rt.loop.cancelTimer(&self.timer);
        self.rt.loop.addTimer(&self.timer, delay_ns) catch {
            // The heap could not grow. The fiber would otherwise wait
            // forever, so unwind it instead of stranding the slot.
            self.canceled = true;
            self.rt.loop.addTimer(&self.timer, 0) catch {};
        };
    }

    fn onTimer(t: *reactor.Timer) void {
        const self: *JobSlot = @fieldParentPtr("timer", t);
        self.enter();
    }

    /// The one place the coordinator fiber is switched into, outside
    /// shutdown.
    fn enter(self: *JobSlot) void {
        if (self.finished or !self.fiber_ready) return;
        if (self.fiber.isDone()) {
            self.markFinished();
            return;
        }
        self.entered = true;
        self.fiber.enter();
        if (self.fiber.isDone()) self.markFinished();
    }

    fn markFinished(self: *JobSlot) void {
        if (self.finished) return;
        self.finished = true;
        self.rt.armReaper();
    }

    fn deinit(self: *JobSlot) void {
        if (self.timer.isArmed()) self.rt.loop.cancelTimer(&self.timer);
        for (self.workers.items) |w| {
            w.deinit();
            self.rt.gpa.destroy(w);
        }
        self.workers.deinit(self.rt.gpa);
        if (self.fiber_ready) self.fiber.deinit();
        self.fiber_ready = false;
    }
};

/// Pool-level failures in the vocabulary the orchestrator classifies on.
fn mapPoolError(e: anyerror) dl_ports.FetchError {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.PoolClosed => error.Canceled,
        error.TooManyWaiters => error.TooManyConnections,
        else => mapConnError(e),
    };
}

/// Connection-level failures, ditto. The split between `ProtocolTransient`
/// and `ProtocolPermanent` is RFC 3977's and `nntp/conn.zig` has already
/// made it; this only renames.
fn mapConnError(e: anyerror) dl_ports.FetchError {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.ArticleMissing => error.ArticleMissing,
        error.TooManyConnections => error.TooManyConnections,
        error.AuthFailed => error.AuthFailed,
        error.Transient => error.ProtocolTransient,
        error.Permanent => error.ProtocolPermanent,
        error.ProtocolDesync => error.ProtocolTransient,
        error.InvalidMessageId, error.ControlCharacter, error.CommandTooLong, error.NoSpaceLeft => error.ProtocolPermanent,
        else => error.Network,
    };
}

// =====================================================================
// Runtime
// =====================================================================

/// Everything the download engine needs, supplied by `bootstrap.zig`.
/// Every field is borrowed and must outlive the `Runtime`.
pub const Deps = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    db: *sqlite.Conn,
    bus: *outbox.Bus,
    logger: *log.Logger = &log.default,

    job_store: dl_ports.JobStore,
    sink: orchestrator.Sink,
    txm: app_ports.Manager,
    fs: app_ports.Filesystem,
    clock: app_ports.Clock,
    incomplete_dir: []const u8,

    /// The scheduler: which jobs run, which wait for a slot, which are
    /// parked for lack of a server. Owned by the caller, and it owns the
    /// queue service it parks jobs through.
    scheduler: *dl_service.Service,
    /// Turns a provider's hostname into an address, from a fiber. Absent
    /// means IP literals only.
    resolver: ?*dns.Resolver = null,

    orchestrator_opts: orchestrator.Options = .{},
    /// `fail_hopeless` as a whole-number percentage, live from Settings.
    settings_ratio: ?app_ports.Knob = null,
    limiter: ?*bandwidth.Limiter = null,
    accounter: ?*byte_accounter.Accounter = null,
    /// Persists what the accounter staged. Absent means the counters are
    /// only ever written by the housekeeping tick; see `onReap`.
    byte_flusher: ?*byte_accounter.Flusher = null,
    /// Per-command deadline and read buffer for every provider
    /// connection.
    conn_defaults: nntp_conn.Config = .{},
    /// Connection cap fallback when a server row says zero.
    default_max_conns: u16 = 8,
};

pub const Runtime = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    db: *sqlite.Conn,
    bus: *outbox.Bus,
    logger: *log.Logger,

    job_store: dl_ports.JobStore,
    sink: orchestrator.Sink,
    txm: app_ports.Manager,
    fs: app_ports.Filesystem,
    clock: app_ports.Clock,
    incomplete_dir: []const u8,

    scheduler: *dl_service.Service,
    resolver: ?*dns.Resolver,

    orchestrator_opts: orchestrator.Options,
    settings_ratio: ?app_ports.Knob,
    limiter: ?*bandwidth.Limiter,
    accounter: ?*byte_accounter.Accounter,
    byte_flusher: ?*byte_accounter.Flusher,
    conn_defaults: nntp_conn.Config,
    default_max_conns: u16,

    /// Trust anchors for TLS providers, loaded once at `start`.
    ca_roots: CaRoots,

    pools: std.ArrayList(*ServerPool) = .empty,
    slots: std.ArrayList(*JobSlot) = .empty,
    dispatchers: std.ArrayList(*Dispatcher) = .empty,

    /// Fires after a slot finishes: frees it, then fills the freed
    /// concurrency slot from the backlog. Deferred to a timer so a slot
    /// is never freed from inside the frame that ran its fiber.
    reaper: reactor.Timer = .{ .callback = onReap },
    stopping: bool = false,
    started: bool = false,

    pub fn init(self: *Runtime, d: Deps) void {
        self.* = .{
            .gpa = d.gpa,
            .loop = d.loop,
            .db = d.db,
            .bus = d.bus,
            .logger = d.logger,
            .job_store = d.job_store,
            .sink = d.sink,
            .txm = d.txm,
            .fs = d.fs,
            .clock = d.clock,
            .incomplete_dir = d.incomplete_dir,
            .scheduler = d.scheduler,
            .resolver = d.resolver,
            .orchestrator_opts = d.orchestrator_opts,
            .settings_ratio = d.settings_ratio,
            .limiter = d.limiter,
            .accounter = d.accounter,
            .byte_flusher = d.byte_flusher,
            .conn_defaults = d.conn_defaults,
            .default_max_conns = d.default_max_conns,
            .ca_roots = .init(d.gpa),
        };
    }

    /// Tear down in the one order that is safe.
    ///
    /// A fiber parked on a socket the loop is about to close is a
    /// use-after-free, and a fiber discarded rather than unwound leaks
    /// whatever it held — the job aggregate, a checked-out connection,
    /// the runner's batch. So:
    ///
    ///   1. Refuse new work.
    ///   2. Cancel every fiber and let each unwind through its own
    ///      `defer`s. Pools are still alive here, which is what lets a
    ///      fiber hand its connection back.
    ///   3. Free the slots.
    ///   4. Close the pools, which closes the sockets and drops their
    ///      reactor registrations.
    ///   5. Unregister the dispatchers.
    ///
    /// Called before the bus and the database are closed, because step 2
    /// touches both.
    pub fn deinit(self: *Runtime) void {
        self.stopping = true;
        if (self.reaper.isArmed()) self.loop.cancelTimer(&self.reaper);

        for (self.slots.items) |slot| self.cancelSlot(slot);
        for (self.slots.items) |slot| {
            slot.deinit();
            self.gpa.destroy(slot);
        }
        self.slots.deinit(self.gpa);

        for (self.pools.items) |sp| {
            sp.deinit();
            self.gpa.destroy(sp);
        }
        self.pools.deinit(self.gpa);

        for (self.dispatchers.items) |d| {
            d.deinit();
            self.gpa.destroy(d);
        }
        self.dispatchers.deinit(self.gpa);

        // Last: a TLS connection torn down above was verifying against
        // this bundle.
        self.ca_roots.deinit();
    }

    /// Unwind one slot's fibers. `cancel` rather than `deinit`, so each
    /// body's `defer`s run: the job aggregate is released and any
    /// checked-out connection goes back to its pool.
    ///
    /// The loop is no longer dispatching by the time this is called, so
    /// the switching the timers would have done has to happen here — and
    /// in this order, because the coordinator's own unwind ends in
    /// `drainWorkers`, which parks until the last worker has returned.
    fn cancelSlot(self: *Runtime, slot: *JobSlot) void {
        _ = self;
        slot.canceled = true;
        if (!slot.fiber_ready or slot.finished) return;
        // Never entered: there is nothing on that stack to unwind, and
        // entering it now would run the whole job during shutdown.
        if (!slot.entered) return;
        // Same for a worker whose first entry never came: retiring it is
        // what lets `drainWorkers` reach zero rather than park forever.
        for (slot.workers.items) |w| {
            if (!w.entered) w.retire();
        }
        var guard: usize = 0;
        while (!slot.fiber.isDone() and guard < 1024) : (guard += 1) {
            for (slot.workers.items) |w| {
                if (!w.done) w.enter();
            }
            slot.enter();
        }
        slot.finished = true;
    }

    // -- pools ---------------------------------------------------------

    /// Build a pool for every row in `servers`, and register the usable
    /// ones with the scheduler.
    pub fn loadPools(self: *Runtime) !void {
        self.ca_roots.load(self.gpa, @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_s)));
        if (self.ca_roots.haveRoots()) {
            self.logger.info("nntp: TLS trust anchors loaded", &.{
                log.str("path", self.ca_roots.source),
                log.uint("anchors", self.ca_roots.count),
            });
        } else {
            self.logger.warn("nntp: no CA bundle found; TLS providers will not be dialled", &.{
                log.str("looked_in", ca_bundle_paths[0]),
            });
        }

        const repo = repo_server.ServerRepo.init(self.gpa, self.db);
        var list = repo.list(self.gpa) catch |e| {
            self.logger.err("nntp: cannot read the server list", &.{log.errv("err", e)});
            return;
        };
        defer list.deinit();

        for (list.items.items) |*row| try self.addServer(row.*);
    }

    /// Register one provider. Idempotent on the server id.
    pub fn addServer(self: *Runtime, row: dserver.UsenetServer) !void {
        self.removeServer(row.id);

        const sp = try self.gpa.create(ServerPool);
        errdefer self.gpa.destroy(sp);

        const name = try self.gpa.dupe(u8, row.name);
        errdefer self.gpa.free(name);
        const host = try self.gpa.dupe(u8, row.host);
        errdefer self.gpa.free(host);
        const username = try self.gpa.dupe(u8, row.username);
        errdefer self.gpa.free(username);
        const password = try self.gpa.dupe(u8, row.password);
        errdefer self.gpa.free(password);

        const max_conns: u16 = if (row.max_conns > 0) row.max_conns else self.default_max_conns;
        const how = dialability(row.host, row.port, row.tls, self.ca_roots.haveRoots());

        sp.* = .{
            .rt = self,
            .id = row.id,
            .name = name,
            .host = host,
            .username = username,
            .password = password,
            .port = row.port,
            .info = .{
                .id = row.id,
                .name = name,
                .priority = row.priority,
                .backup = row.backup,
                .metered = row.billing_mode != .flat,
                .enabled = row.enabled,
                .quota_exhausted = row.quota_bytes > 0 and row.used_bytes >= row.quota_bytes,
                .max_conns = max_conns,
            },
            .pool = undefined,
            .dialable = how != .unsupported,
            .undialable_reason = switch (how) {
                .unsupported => |why| why,
                else => "",
            },
            .resolved = how == .literal,
        };

        switch (how) {
            .unsupported => |why| self.logger.warn("nntp: server cannot be dialled by this build", &.{
                log.int("server_id", row.id),
                log.str("server", name),
                log.str("reason", why),
            }),
            else => {
                var cfg = self.conn_defaults;
                cfg.username = username;
                cfg.password = password;
                if (row.tls) cfg.security = .{
                    .tls = .{
                        // Borrowed for the pool's lifetime — hence the owned
                        // copy above rather than the row's slice.
                        .host = host,
                        .trust = .{ .ca_bundle = &self.ca_roots.store },
                    },
                };
                // A hostname gets a placeholder until the first fetch
                // resolves it; nothing dials before then, and `fetchOne`
                // rewrites `pool.addr` before it does.
                const addr = switch (how) {
                    .literal => |ip| ip,
                    else => IpAddress.parse("0.0.0.0", row.port) catch unreachable,
                };
                sp.pool.init(self.gpa, self.loop, addr, .{
                    .max_connections = max_conns,
                    .conn = cfg,
                });
                if (how == .needs_dns and self.resolver == null) {
                    self.logger.warn("nntp: no resolver wired; this server will not be dialled", &.{
                        log.str("server", name),
                        log.str("host", host),
                    });
                }
            },
        }

        try self.pools.append(self.gpa, sp);

        if (sp.dialable and sp.info.usable()) {
            const started = self.scheduler.onServerAddedOrEnabled(self.gpa, sp.info) catch |e| {
                self.logger.err("download: server registration failed", &.{log.errv("err", e)});
                return;
            };
            defer self.gpa.free(started);
            for (started) |id| try self.openSlot(id);
        }
    }

    pub fn removeServer(self: *Runtime, id: ServerId) void {
        for (self.pools.items, 0..) |sp, i| {
            if (sp.id != id) continue;
            _ = self.pools.orderedRemove(i);
            self.scheduler.onServerDisabledOrRemoved(id);
            sp.deinit();
            self.gpa.destroy(sp);
            return;
        }
    }

    pub fn poolById(self: *Runtime, id: ServerId) ?*ServerPool {
        for (self.pools.items) |sp| {
            if (sp.id == id and sp.dialable) return sp;
        }
        return null;
    }

    fn poolSnapshot(self: *Runtime, a: Allocator) Allocator.Error![]dl_ports.PoolInfo {
        var out: std.ArrayList(dl_ports.PoolInfo) = .empty;
        errdefer out.deinit(a);
        for (self.pools.items) |sp| {
            if (!sp.dialable) continue;
            try out.append(a, sp.info);
        }
        return out.toOwnedSlice(a);
    }

    // -- slots ---------------------------------------------------------

    fn findSlot(self: *Runtime, id: JobId) ?*JobSlot {
        for (self.slots.items) |s| {
            if (s.job_id == id and !s.finished) return s;
        }
        return null;
    }

    /// Build a fiber for a job the scheduler admitted.
    ///
    /// The fiber is *not* entered here: the first entry, like every
    /// later one, happens from the slot's timer.
    fn openSlot(self: *Runtime, id: JobId) !void {
        if (self.stopping) return;
        if (self.findSlot(id) != null) return;

        const slot = try self.gpa.create(JobSlot);
        errdefer self.gpa.destroy(slot);

        slot.* = .{
            .rt = self,
            .job_id = id,
            .hint = self.scheduler.dispatchHint(),
        };
        // Only the coordinator's fiber here. The fetch workers are made
        // when the first batch arrives, so a job that turns out to have
        // nothing to do never reserves a stack it does not use.
        try slot.fiber.init(self.gpa, self.loop, job_stack_size, JobSlot.entry, slot);
        slot.fiber_ready = true;
        errdefer slot.deinit();

        try self.slots.append(self.gpa, slot);
        slot.armTimer(0);

        self.logger.info("download: runner started", &.{
            log.int("job_id", id),
            log.int("server_id", slot.hint.server_id),
            log.uint("stacks", fiber_mod.liveStacks()),
        });
    }

    /// Cancel a running job's fiber — pause, removal, or a job the
    /// scheduler dropped.
    ///
    /// Deferred to the slot's own timer rather than unwound here, and for
    /// the reason the module comment gives: this runs inside a reactor
    /// source callback (the outbox dispatcher's), and an unwinding fiber
    /// releases its connection, which unregisters a source in the middle
    /// of the backend's dispatch over its own arrays. The timer fires
    /// after that batch.
    fn closeSlot(self: *Runtime, id: JobId) void {
        const slot = self.findSlot(id) orelse return;
        slot.canceled = true;
        slot.armTimer(0);
    }

    fn armReaper(self: *Runtime) void {
        if (self.stopping or self.reaper.isArmed()) return;
        self.loop.addTimer(&self.reaper, 0) catch {};
    }

    fn onReap(t: *reactor.Timer) void {
        const self: *Runtime = @fieldParentPtr("reaper", t);
        if (self.stopping) return;

        var i: usize = 0;
        var freed: usize = 0;
        while (i < self.slots.items.len) {
            const slot = self.slots.items[i];
            if (!slot.finished) {
                i += 1;
                continue;
            }
            _ = self.slots.orderedRemove(i);
            self.scheduler.finishRunner(slot.job_id);
            slot.deinit();
            self.gpa.destroy(slot);
            freed += 1;
        }
        if (freed == 0) return;

        // A runner that has exited will not charge another byte, so this
        // is the last moment its consumption is only in memory. The
        // ten-second housekeeping tick is the right cadence *while* a
        // download is running — writing `used_bytes` per article is pure
        // SQLite overhead — but it is the wrong one for the end of a
        // job: a daemon restarted right after a download finished would
        // otherwise forget the whole of it, and a metered block account
        // that under-counts is one the operator over-spends. One UPDATE
        // per finished job, not per article.
        if (self.byte_flusher) |f| f.flush(null, self.clock.now()) catch |e| {
            self.logger.warn("download: byte accounting flush failed", &.{log.errv("err", e)});
        };

        const promoted = self.scheduler.nudgePending(self.gpa) catch |e| {
            self.logger.err("download: backlog promotion failed", &.{log.errv("err", e)});
            return;
        };
        defer self.gpa.free(promoted);
        for (promoted) |id| self.openSlot(id) catch |e| {
            self.logger.err("download: cannot start runner", &.{
                log.int("job_id", id),
                log.errv("err", e),
            });
        };
    }

    // -- bus subscriptions --------------------------------------------

    /// Register `handler` for `topic` and drive it from the reactor.
    ///
    /// Public because the post-download pipeline's subscribers live in
    /// `bootstrap.zig` — they are compositions of application services,
    /// not runtime machinery — but every one of them needs the same
    /// reactor plumbing.
    pub fn subscribe(
        self: *Runtime,
        name: []const u8,
        topic: []const u8,
        handler: outbox.Handler,
        ctx: ?*anyopaque,
    ) !void {
        const sub = try self.bus.subscribeInline(name, topic, handler, ctx);

        const d = try self.gpa.create(Dispatcher);
        errdefer self.gpa.destroy(d);
        d.* = .{
            .rt = self,
            .sub = sub,
            .source = .{
                .fd = sub.wakeFd(),
                .interest = .readable,
                .callback = Dispatcher.onReadable,
            },
            .retry = .{ .callback = Dispatcher.onRetry },
        };
        try self.loop.add(&d.source);
        d.registered = true;
        errdefer d.deinit();

        try self.dispatchers.append(self.gpa, d);

        // A restart inherits whatever the previous process did not
        // deliver, and nothing will nudge those rows. One pass now is the
        // catch-up the threaded dispatcher got from its first tick.
        d.run();
    }

    /// Adopt every job the database says is alive, then start listening.
    pub fn start(self: *Runtime) !void {
        if (self.started) return;
        self.started = true;

        try self.subscribeDownloadTopics();

        const admitted = try self.scheduler.start(self.gpa);
        defer self.gpa.free(admitted);
        for (admitted) |id| try self.openSlot(id);
    }

    fn subscribeDownloadTopics(self: *Runtime) !void {
        const T = struct {
            name: []const u8,
            topic: []const u8,
            handler: outbox.Handler,
        };
        const table = [_]T{
            .{ .name = "runtime.job.created", .topic = "download.job.created", .handler = &onJobStartable },
            .{ .name = "runtime.job.resumed", .topic = "download.job.resumed", .handler = &onJobStartable },
            .{
                .name = "runtime.job.recovery_vols",
                .topic = "download.job.recovery_vols_requested",
                .handler = &onJobStartable,
            },
            .{ .name = "runtime.job.paused", .topic = "download.job.paused", .handler = &onJobStopped },
            .{ .name = "runtime.job.removed", .topic = "download.job.removed", .handler = &onJobStopped },
        };
        for (table) |t| try self.subscribe(t.name, t.topic, t.handler, @ptrCast(self));
    }

    fn onJobStartable(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
        const self: *Runtime = @ptrCast(@alignCast(ctx.?));
        const id = aggregateJobId(env) orelse return .{ .failed = "no job id" };
        const admission = self.scheduler.startRunner(id) catch |e| {
            self.logger.err("download: admission failed", &.{
                log.int("job_id", id),
                log.errv("err", e),
            });
            return .{ .failed = @errorName(e) };
        };
        if (admission == .started) {
            self.openSlot(id) catch |e| {
                self.logger.err("download: cannot start runner", &.{
                    log.int("job_id", id),
                    log.errv("err", e),
                });
                self.scheduler.finishRunner(id);
                return .{ .failed = @errorName(e) };
            };
        }
        return .ok;
    }

    fn onJobStopped(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
        const self: *Runtime = @ptrCast(@alignCast(ctx.?));
        const id = aggregateJobId(env) orelse return .{ .failed = "no job id" };
        _ = self.scheduler.stopRunner(id);
        self.closeSlot(id);
        return .ok;
    }

    // -- observability -------------------------------------------------

    pub const PoolOccupancy = struct {
        id: ServerId,
        open: u32,
        idle: usize,
        waiters: usize,
    };

    pub fn occupancy(self: *Runtime, id: ServerId) ?PoolOccupancy {
        const sp = self.poolById(id) orelse return null;
        return .{
            .id = id,
            .open = sp.pool.openCount(),
            .idle = sp.pool.idleCount(),
            .waiters = sp.pool.waiterCount(),
        };
    }

    /// Re-drive jobs parked for lack of a usable provider.
    ///
    /// Called after the server list changes. `kickIdleJobs` unparks
    /// anything in `waiting_for_server` and starts a runner for every
    /// non-paused, non-terminal job that lacks one, so a provider
    /// configured *after* an NZB was uploaded picks it up immediately
    /// rather than at the next restart.
    pub fn kickParked(self: *Runtime) void {
        const admitted = self.scheduler.kickIdleJobs(self.gpa) catch |err| {
            self.logger.warn("download: could not re-drive parked jobs", &.{
                log.errv("err", err),
            });
            return;
        };
        defer self.gpa.free(admitted);
        for (admitted) |id| self.openSlot(id) catch |err| {
            self.logger.warn("download: could not open a slot for an unparked job", &.{
                log.int("job_id", id),
                log.errv("err", err),
            });
        };
    }

    pub fn activeSlots(self: *const Runtime) usize {
        var n: usize = 0;
        for (self.slots.items) |s| {
            if (!s.finished) n += 1;
        }
        return n;
    }
};

/// The job id an envelope is about.
///
/// The payload's `job_id` is the only field that means this across every
/// context, so it is the only one consulted first. `aggregate_id` names
/// the *publishing* context's aggregate — a verify set, a repair attempt,
/// a delivery — and only the download context's aggregate happens to be a
/// job. Reading it as one routes `verify.ok` for job 7 at whatever job
/// shares a number with verify set 3, which is a different job the moment
/// the two counters drift apart: a stage then runs against a stranger, and
/// a `repair.ok` can re-verify and fail a job that is still downloading.
///
/// Every payload carries the field. The post-download contexts declare
/// `job_id` on each event, and `infra.Publisher` injects it for the
/// download context, whose job-level events spell it `id`. The fallback
/// is therefore for rows that predate that injection, and it is confined
/// to the topics whose aggregate really is the job.
pub fn aggregateJobId(env: outbox.Envelope) ?JobId {
    if (payloadJobId(env.payload)) |v| return v;
    if (!std.mem.startsWith(u8, env.topic, "download.")) return null;
    if (std.fmt.parseInt(JobId, env.aggregate_id, 10)) |v| {
        if (v != 0) return v;
    } else |_| {}
    return null;
}

/// `"job_id":<int>` out of a payload this process encoded.
///
/// A scan rather than a parse, and that is a deliberate limit: the
/// encoder in `bootstrap/infra.zig` emits one flat object per event with
/// no nesting and no string containing a quote, so the first match is the
/// field. It is not a general JSON reader and must not be used as one.
pub fn payloadJobId(payload: []const u8) ?JobId {
    const key = "\"job_id\":";
    const at = std.mem.indexOf(u8, payload, key) orelse return null;
    var i = at + key.len;
    while (i < payload.len and payload[i] == ' ') i += 1;
    const start = i;
    if (i < payload.len and payload[i] == '-') i += 1;
    while (i < payload.len and std.ascii.isDigit(payload[i])) i += 1;
    if (i == start) return null;
    return std.fmt.parseInt(JobId, payload[start..i], 10) catch null;
}

// =====================================================================
// Connection probe
// =====================================================================

/// `POST /servers/test`: dial a provider and report how far it got.
///
/// ## Why this stalls the loop, and why that is the honest answer
///
/// `ServerProbe.probe` is synchronous — an HTTP handler calls it and
/// writes the response — and the handler does not run on a fiber. There
/// is no way to suspend it. Running the dial on the *main* loop and
/// ticking it from inside a source callback would re-enter the backend's
/// arrays mid-dispatch, which is a genuine memory-safety problem rather
/// than a stylistic one.
///
/// So the probe builds its own `reactor.Loop`, dials on that, and ticks
/// it to completion. The daemon pauses for as long as the probe takes,
/// bounded by `probe_deadline_ns`. That is acceptable for a button an
/// operator presses; it would not be for anything on the download path,
/// and nothing on the download path does it.
pub const Probe = struct {
    gpa: Allocator,
    logger: *log.Logger = &log.default,
    /// Per-step deadline handed to the connection.
    timeout_ns: u64 = 5 * std.time.ns_per_s,
    /// The daemon's trust anchors. Shared rather than reloaded: a
    /// `CaStore` is memory with no loop affinity, and re-parsing 150
    /// certificates per button press would be absurd. Null means TLS
    /// probes report the same "no roots" answer a TLS server row does.
    ca_roots: ?*CaRoots = null,

    pub fn port(self: *Probe) rest_ports.ServerProbe {
        return .{ .ctx = self, .probeFn = &probe };
    }

    const State = struct {
        done: bool = false,
        ok: bool = false,
        err: ?nntp_conn.Error = null,

        const handler: nntp_conn.Handler = .{
            .on_ready = onReady,
            .on_body = onBody,
            .on_error = onError,
        };

        fn onReady(c: *nntp_conn.Conn) void {
            const self: *State = @ptrCast(@alignCast(c.context.?));
            self.ok = true;
            self.done = true;
        }

        fn onBody(_: *nntp_conn.Conn, _: []const u8) void {}

        fn onError(c: *nntp_conn.Conn, err: nntp_conn.Error) void {
            const self: *State = @ptrCast(@alignCast(c.context.?));
            self.err = err;
            self.done = true;
        }
    };

    const Lookup = struct {
        done: bool = false,
        addr: ?IpAddress = null,
        err: ?dns.Error = null,

        fn onResolved(ctx: ?*anyopaque, result: dns.Error!IpAddress) void {
            const self: *Lookup = @ptrCast(@alignCast(ctx.?));
            if (result) |a| {
                self.addr = a;
            } else |e| {
                self.err = e;
            }
            self.done = true;
        }
    };

    /// How far a failure implies the conversation got.
    ///
    /// Derived from the error rather than from `Conn.state`, because
    /// `Conn.fail` sets the state to `closed` *before* reporting, so by
    /// the time anything outside can look, the step that failed is gone.
    /// The sampled state is used only to upgrade `dial`, where a slow
    /// connect is the ambiguous case.
    const Steps = struct { dial: bool, greeted: bool, auth: bool };

    fn stepsFor(err: nntp_conn.Error) Steps {
        return switch (err) {
            // Nothing was ever established.
            error.ConnectionRefused,
            error.ConnectionReset,
            error.NetworkUnreachable,
            error.HostUnreachable,
            error.TimedOut,
            => .{ .dial = false, .greeted = false, .auth = false },
            // The socket opened and the server answered — with a refusal.
            error.TooManyConnections, error.ProtocolDesync => .{ .dial = true, .greeted = false, .auth = false },
            // The greeting was accepted and the credentials were not.
            error.AuthFailed => .{ .dial = true, .greeted = true, .auth = false },
            // A 4xx/5xx to MODE READER: everything before it worked.
            error.Transient, error.Permanent => .{ .dial = true, .greeted = true, .auth = true },
            else => .{ .dial = false, .greeted = false, .auth = false },
        };
    }

    fn probe(
        ctx: ?*anyopaque,
        arena: Allocator,
        p: rest_ports.ProbeParams,
    ) Allocator.Error!rest_ports.ProbeResult {
        const self: *Probe = @ptrCast(@alignCast(ctx.?));
        _ = arena;

        const started = sys.monotonicNanos();
        const have_roots = if (self.ca_roots) |r| r.haveRoots() else false;
        switch (dialability(p.host, p.port, p.tls, have_roots)) {
            .unsupported => |why| return .{ .ok = false, .err = why },
            else => {},
        }

        var loop: reactor.Loop = undefined;
        loop.init(self.gpa) catch |e| return .{ .ok = false, .err = @errorName(e) };
        defer loop.deinit();

        // Its own resolver on its own loop, for the same reason as the
        // loop itself: the daemon's resolver has in-flight queries
        // registered with the daemon's loop, and ticking that from here
        // would re-enter its dispatch.
        var resolver: dns.Resolver = undefined;
        resolver.initFromSystem(self.gpa, &loop);
        defer resolver.deinit();

        var lookup: Lookup = .{};
        resolver.resolve(p.host, p.port, Lookup.onResolved, @ptrCast(&lookup));
        while (!lookup.done) {
            if (sys.monotonicNanos() - started > probe_deadline_ns) {
                return .{ .ok = false, .err = "dns timed out", .elapsed_ms = elapsedMs(started) };
            }
            _ = loop.tick(5) catch break;
        }
        const addr = lookup.addr orelse return .{
            .ok = false,
            .err = if (lookup.err) |e| @errorName(e) else "cannot resolve host",
            .elapsed_ms = elapsedMs(started),
        };

        var state: State = .{};
        var c: nntp_conn.Conn = undefined;
        c.connect(self.gpa, &loop, addr, .{
            .username = p.username,
            .password = p.password,
            .timeout_ns = self.timeout_ns,
            // A probe never asks for an article, so a small buffer is
            // plenty and the allocation is not worth 64 KiB.
            .read_buf_size = 8 * 1024,
            .security = if (p.tls) .{ .tls = .{
                .host = p.host,
                .trust = .{ .ca_bundle = &self.ca_roots.?.store },
            } } else .plaintext,
        }, &State.handler) catch |e| {
            return .{ .ok = false, .err = @errorName(e), .elapsed_ms = elapsedMs(started) };
        };
        c.context = @ptrCast(&state);
        defer c.deinit();

        var ever_connected = false;
        while (!state.done) {
            if (sys.monotonicNanos() - started > probe_deadline_ns) {
                return .{
                    .ok = false,
                    .dial = ever_connected,
                    .err = "timed out",
                    .elapsed_ms = elapsedMs(started),
                };
            }
            _ = loop.tick(5) catch break;
            if (c.state != .connecting and c.state != .closed) ever_connected = true;
        }

        if (state.ok) {
            return .{
                .ok = true,
                .dial = true,
                .greeted = true,
                // No credentials means the auth step was skipped rather
                // than failed, and the handshake got past it either way.
                .auth = true,
                .mode_reader = true,
                // `nntp/conn.zig` never sends DATE, so claiming it would
                // be a lie on a page whose whole job is per-step truth.
                .date = false,
                .elapsed_ms = elapsedMs(started),
            };
        }

        const err = state.err orelse return .{
            .ok = false,
            .dial = ever_connected,
            .err = "no answer",
            .elapsed_ms = elapsedMs(started),
        };
        const steps = stepsFor(err);
        return .{
            .ok = false,
            .dial = steps.dial or ever_connected,
            .greeted = steps.greeted,
            .auth = steps.auth,
            .err = @errorName(err),
            .elapsed_ms = elapsedMs(started),
        };
    }

    fn elapsedMs(started: u64) i64 {
        return @intCast((sys.monotonicNanos() - started) / std.time.ns_per_ms);
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;

test "how a server gets dialled is decided in exactly one place" {
    // TLS without trust anchors has to be reported rather than
    // attempted. Dialling anyway and skipping verification would send
    // the account password to whoever answers, and it would look like it
    // worked — which is the worst of both.
    switch (dialability("127.0.0.1", 563, true, false)) {
        .unsupported => |why| try testing.expect(std.mem.indexOf(u8, why, "CA roots") != null),
        else => return error.UnverifiableTlsShouldBeRefused,
    }
    // With roots, the same row is dialled like any other.
    switch (dialability("127.0.0.1", 563, true, true)) {
        .literal => |a| try testing.expectEqual(@as(u16, 563), a.getPort()),
        else => return error.VerifiableTlsShouldDial,
    }
    // A literal never touches the resolver.
    switch (dialability("127.0.0.1", 119, false, false)) {
        .literal => |a| try testing.expectEqual(@as(u16, 119), a.getPort()),
        else => return error.LiteralShouldParse,
    }
    switch (dialability("[::1]", 119, false, false)) {
        .literal => |a| try testing.expect(a == .ip6),
        else => return error.BracketedLiteralShouldParse,
    }
    // A hostname is resolved on the fiber at first use, not refused.
    try testing.expect(dialability("news.example.invalid", 119, false, false) == .needs_dns);
    switch (dialability("", 119, false, false)) {
        .unsupported => {},
        else => return error.EmptyHostShouldBeRefused,
    }
}

test "the system CA bundle is found when there is one, and reported when there is not" {
    const gpa = testing.allocator;
    var roots: CaRoots = .init(gpa);
    defer roots.deinit();

    var have_bundle = false;
    for (ca_bundle_paths) |path| {
        const fd = sys.open(path, .{ .mode = .read_only }) catch continue;
        sys.close(fd);
        have_bundle = true;
        break;
    }

    roots.load(gpa, @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_s)));
    if (!have_bundle) {
        // The shipping `scratch` image, and the assertion that matters
        // there: no roots means TLS providers are refused, not dialled
        // unverified.
        try testing.expect(!roots.haveRoots());
        try testing.expect(dialability("news.example.com", 563, true, roots.haveRoots()) == .unsupported);
        return;
    }

    // A real host: a few hundred anchors, and the source named so an
    // operator can tell which file the daemon actually read.
    try testing.expect(roots.haveRoots());
    try testing.expect(roots.count > 10);
    try testing.expect(roots.source.len > 0);
    try testing.expect(dialability("news.example.com", 563, true, roots.haveRoots()) == .needs_dns);
}

test "the job id comes from the payload, and from the aggregate id only for download topics" {
    // A download event keys on the job itself, and the publisher injects
    // the same number into the payload.
    try testing.expectEqual(@as(?JobId, 42), aggregateJobId(.{
        .id = @splat(0),
        .topic = "download.job.created",
        .aggregate_id = "42",
        .occurred_at_ms = 0,
        .payload = "{\"job_id\":42}",
        .attempts = 1,
    }));

    // A row from before that injection still resolves, because the
    // aggregate of a `download.` topic is the job.
    try testing.expectEqual(@as(?JobId, 42), aggregateJobId(.{
        .id = @splat(0),
        .topic = "download.job.created",
        .aggregate_id = "42",
        .occurred_at_ms = 0,
        .payload = "{\"id\":42}",
        .attempts = 1,
    }));

    // The regression this exists for: a verify event keys on the *verify
    // set*, which is a different counter. With more than one job alive
    // the two diverge, and preferring the aggregate id sends the whole
    // post-download pipeline at a stranger — delivering one job's files
    // under another's name and failing a job that is still downloading.
    try testing.expectEqual(@as(?JobId, 7), aggregateJobId(.{
        .id = @splat(0),
        .topic = "verify.ok",
        .aggregate_id = "3",
        .occurred_at_ms = 0,
        .payload = "{\"id\":3,\"job_id\":7,\"state\":\"ok\"}",
        .attempts = 1,
    }));

    // And a post-download event with no job id in it resolves to nothing
    // rather than to its own aggregate.
    try testing.expectEqual(@as(?JobId, null), aggregateJobId(.{
        .id = @splat(0),
        .topic = "repair.ok",
        .aggregate_id = "3",
        .occurred_at_ms = 0,
        .payload = "{\"id\":3}",
        .attempts = 1,
    }));

    try testing.expectEqual(@as(?JobId, null), payloadJobId("{\"id\":3}"));
    try testing.expectEqual(@as(?JobId, 9), payloadJobId("{\"job_id\": 9}"));
    try testing.expectEqual(@as(?JobId, null), payloadJobId("{\"job_id\":}"));
}

test "pool and connection failures map onto the classifier's vocabulary" {
    // The orchestrator decides retry-or-fail by switching exhaustively
    // over `FetchError`, so a transport error that lands on the wrong
    // member is a segment retried forever or abandoned on the first try.
    try testing.expectEqual(@as(dl_ports.FetchError, error.Canceled), mapPoolError(error.PoolClosed));
    try testing.expectEqual(
        @as(dl_ports.FetchError, error.TooManyConnections),
        mapPoolError(error.TooManyWaiters),
    );
    try testing.expectEqual(
        @as(dl_ports.FetchError, error.ArticleMissing),
        mapConnError(error.ArticleMissing),
    );
    try testing.expectEqual(@as(dl_ports.FetchError, error.AuthFailed), mapConnError(error.AuthFailed));
    try testing.expectEqual(
        @as(dl_ports.FetchError, error.ProtocolTransient),
        mapConnError(error.Transient),
    );
    try testing.expectEqual(
        @as(dl_ports.FetchError, error.ProtocolPermanent),
        mapConnError(error.Permanent),
    );
    // A timeout is a network condition, and transient: the provider may
    // simply be busy.
    try testing.expectEqual(@as(dl_ports.FetchError, error.Network), mapConnError(error.Timeout));
    try testing.expect((orchestrator.Failure{ .fetch = mapConnError(error.Timeout) }).isTransient());
}

test "a probe against a closed port reports the failure instead of hanging" {
    const gpa = testing.allocator;

    // Bind and release, so the port is certainly not listening.
    const socket_mod = @import("../net/socket.zig");
    var listener: socket_mod.Listener = undefined;
    try listener.listen(try IpAddress.parse("127.0.0.1", 0), struct {
        fn f(_: *socket_mod.Listener, fd: sys.Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead = try listener.boundPort();
    listener.close();

    var p: Probe = .{ .gpa = gpa, .timeout_ns = std.time.ns_per_s };
    const result = try p.port().probe(gpa, .{
        .host = "127.0.0.1",
        .port = dead,
        .tls = false,
    });
    try testing.expect(!result.ok);
    try testing.expect(result.err.len > 0);
}

test "a probe against a real NNTP server reports every step green" {
    const gpa = testing.allocator;
    const testserver = @import("../testserver/nntp.zig");

    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: testserver.Server = undefined;
    const port = try server.start(gpa, &loop, .{ .username = "u", .password = "p" });
    defer server.deinit();

    // The probe runs its own loop — see the type's comment — so it cannot
    // also advance the stub's. Putting the probe on a thread and the stub
    // on this one is the only arrangement that exercises the real
    // handshake; in production the two ends are separate processes and
    // the question does not arise.
    const Driver = struct {
        p: Probe,
        port: u16,
        result: rest_ports.ProbeResult = .{},
        done: std.atomic.Value(bool) = .init(false),

        fn go(self: *@This()) void {
            self.result = self.p.port().probe(self.p.gpa, .{
                .host = "127.0.0.1",
                .port = self.port,
                .tls = false,
                .username = "u",
                .password = "p",
            }) catch .{};
            self.done.store(true, .release);
        }
    };

    var driver: Driver = .{
        .p = .{ .gpa = gpa, .timeout_ns = 2 * std.time.ns_per_s },
        .port = port,
    };
    const th = try std.Thread.spawn(.{}, Driver.go, .{&driver});

    const deadline = sys.monotonicNanos() + 10 * std.time.ns_per_s;
    while (!driver.done.load(.acquire)) {
        if (sys.monotonicNanos() > deadline) break;
        _ = try loop.tick(5);
    }
    th.join();

    try testing.expect(driver.result.ok);
    try testing.expect(driver.result.dial);
    try testing.expect(driver.result.greeted);
    try testing.expect(driver.result.auth);
    try testing.expect(driver.result.mode_reader);
    try testing.expectEqualStrings("", driver.result.err);
}

test "a probe with the wrong credentials fails at the auth step, not the dial" {
    const gpa = testing.allocator;
    const testserver = @import("../testserver/nntp.zig");

    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var server: testserver.Server = undefined;
    const port = try server.start(gpa, &loop, .{ .username = "u", .password = "right" });
    defer server.deinit();

    const Driver = struct {
        p: Probe,
        port: u16,
        result: rest_ports.ProbeResult = .{},
        done: std.atomic.Value(bool) = .init(false),

        fn go(self: *@This()) void {
            self.result = self.p.port().probe(self.p.gpa, .{
                .host = "127.0.0.1",
                .port = self.port,
                .tls = false,
                .username = "u",
                .password = "wrong",
            }) catch .{};
            self.done.store(true, .release);
        }
    };

    var driver: Driver = .{
        .p = .{ .gpa = gpa, .timeout_ns = 2 * std.time.ns_per_s },
        .port = port,
    };
    const th = try std.Thread.spawn(.{}, Driver.go, .{&driver});

    const deadline = sys.monotonicNanos() + 10 * std.time.ns_per_s;
    while (!driver.done.load(.acquire)) {
        if (sys.monotonicNanos() > deadline) break;
        _ = try loop.tick(5);
    }
    th.join();

    // The distinction is the whole point of the Settings page's per-step
    // ticks: "we reached your provider and it rejected the password" is a
    // different support ticket from "we could not reach it".
    try testing.expect(!driver.result.ok);
    try testing.expect(driver.result.dial);
    try testing.expect(driver.result.greeted);
    try testing.expect(!driver.result.auth);
}
