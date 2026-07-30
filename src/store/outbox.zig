//! The transactional outbox: a durable event bus backed by two SQLite
//! tables.
//!
//! `publish` writes each event to `outbox` **inside the caller's
//! transaction**, alongside the aggregate change that produced it. Both
//! commit or neither does, so there is no window in which a job is
//! marked complete but the "completed" event was lost, nor one in which
//! an event describes a state the database never reached.
//!
//! Alongside each event it writes one `outbox_subs` row per
//! *topic-matching* subscription. Each subscription has its own
//! dispatcher thread that claims its own rows, so a slow Discord webhook
//! cannot delay the extract worker. Delivery is **at-least-once**:
//! handlers must be idempotent.
//!
//! ## The wake/commit race
//!
//! A dispatcher sleeps until either its poll tick fires or `publish`
//! nudges it. The nudge is the fast path and the tick is a safety net —
//! at the production 5-second interval, missing the nudge means a
//! five-second stall.
//!
//! Nudging at the moment of `INSERT` is wrong, and subtly so: inside the
//! caller's transaction those rows are invisible to every other
//! connection. The woken dispatcher runs its `SELECT`, finds nothing,
//! and goes back to sleep — and it is now *less* likely to be awake when
//! the rows do appear. So the nudge is deferred to a post-commit hook
//! (`tx.onCommit`). Each matching subscription gets a `wake_pending`
//! flag set during `publish`; the hook, running after `COMMIT` returns,
//! converts flags into writes on the wakers. A rolled-back transaction
//! runs no hook, and the stale flags it leaves behind cost at most one
//! empty `SELECT` later.
//!
//! ## Retry, backoff, poison
//!
//! A failing handler bumps `attempts`, records `last_error`, and sets
//! `next_retry_at` to now plus an exponentially growing delay. Once
//! `attempts` reaches `max_delivery_attempts` the row stops being
//! selected: it is parked, not deleted, with its error text intact so an
//! operator can look at it. The dispatcher is never pinned retrying one
//! poisoned event.
//!
//! ## Pruning
//!
//! Without a reaper this design accretes one `outbox_subs` row per
//! (subscription, event) forever and one `outbox` row per event. The
//! production container reached 2.1M `outbox` rows in thirteen days —
//! a 670 MiB database file that every dispatcher `SELECT` had to join
//! into. `prune` runs three bounded passes; see `pruneOnce`.
//!
//! ## Threading
//!
//! `SQLITE_THREADSAFE=2` forbids sharing a connection between threads,
//! so each dispatcher thread and the pruner thread open their own
//! connection to the same file. That also means the bus cannot dispatch
//! against a `:memory:` database — every connection to `:memory:` gets
//! its own private database. `publish` alone works in memory, which is
//! all the publish-side tests need; the dispatch tests use a temp file.

const std = @import("std");
const builtin = @import("builtin");
const sqlite = @import("sqlite.zig");
const tx = @import("tx.zig");
const migrate = @import("migrate.zig");
const sys = @import("../posix/sys.zig");

const Conn = sqlite.Conn;
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------

/// An event on its way to the outbox.
///
/// The bus is deliberately ignorant of event *types*: `payload` is
/// already-encoded JSON, produced by whoever owns the schema for that
/// topic. Keeping serialization out of here means adding a new event is
/// a change in one bounded context, not in the bus.
pub const Event = struct {
    /// `context.aggregate.verb`, e.g. `download.job.created`.
    topic: []const u8,
    /// Stable identifier of the aggregate the event is about. Format is
    /// owned by the producing context.
    aggregate_id: []const u8,
    /// Unix milliseconds. Zero means "stamp it at publish time", which
    /// is what a domain event that carries no clock of its own wants.
    occurred_at_ms: i64 = 0,
    /// JSON body. Borrowed for the duration of the `publish` call.
    payload: []const u8,
};

/// A stored event handed to a subscriber.
///
/// Every slice borrows from the dispatcher's row buffer and is valid
/// only for the duration of the handler call. A handler that needs to
/// keep a value copies it.
pub const Envelope = struct {
    /// UUIDv7, 16 bytes. Time-ordered, so it doubles as the delivery
    /// cursor: `ORDER BY event_id` is chronological.
    id: [16]u8,
    topic: []const u8,
    aggregate_id: []const u8,
    occurred_at_ms: i64,
    payload: []const u8,
    /// 1-based attempt number for *this* delivery. A handler can use it
    /// to log differently on a retry, or to give up early.
    attempts: u32,
};

/// What a handler reports back.
///
/// A tagged union rather than a Zig error set because the failure needs
/// to carry text: `last_error` is what an operator reads when an event
/// is parked, and an error name alone ("error.Failed") tells them
/// nothing. The message is borrowed for the duration of the return.
pub const HandlerResult = union(enum) {
    ok,
    failed: []const u8,
};

/// A subscriber callback. `ctx` is whatever was passed to `subscribe`.
pub const Handler = *const fn (ctx: ?*anyopaque, env: Envelope) HandlerResult;

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

pub const Options = struct {
    /// How long a dispatcher parks before re-checking without a nudge.
    ///
    /// This is a safety net, not the delivery path: `publish` nudges the
    /// matching dispatchers as soon as its transaction commits. With
    /// ~14 live subscriptions a 250 ms tick was ~56 `SELECT`s/second
    /// against `outbox_subs` with nothing pending — 30% of process CPU
    /// at idle. Five seconds keeps the "missed nudge" and "a failed
    /// delivery's `next_retry_at` has come due" cases responsive without
    /// burning anything. Tests override it.
    poll_interval_ms: i32 = 5000,

    /// Rows claimed per dispatcher pass.
    batch_size: u32 = 64,

    /// Delay before the first retry; each subsequent attempt doubles.
    backoff_base_ms: i64 = 1000,
    /// Ceiling on the retry delay.
    backoff_max_ms: i64 = 10 * 60 * 1000,

    /// Attempts after which an event is parked rather than retried.
    max_delivery_attempts: u32 = 20,

    /// How long a delivered row is kept before the pruner reaps it.
    /// Long enough to debug recent activity, short enough to keep the
    /// tables lean.
    prune_retention_ms: i64 = 60 * 60 * 1000,
    /// How often the pruner runs.
    prune_interval_ms: i32 = 60 * 1000,

    /// Injectable clock, for tests that need to control backoff and
    /// retention windows without sleeping through them.
    now: *const fn () i64 = sqlite.nowMillis,

    /// Start the background pruner on the first `subscribe`. Tests that
    /// assert on row counts turn it off so a reaper cannot delete the
    /// rows out from under the assertion.
    prune: bool = true,
};

pub const Error = sqlite.Error || error{
    /// `publish` or `subscribe` after `close`.
    BusClosed,
    /// Two subscriptions with the same name. Names are the primary key
    /// of delivery state, so a collision would make two subscribers
    /// share — and steal — each other's rows.
    DuplicateSubscription,
    EmptyName,
    EmptyTopic,
    /// The dispatcher thread could not be started.
    SpawnFailed,
    /// A wake pipe / eventfd could not be created.
    WakerFailed,
};

// ---------------------------------------------------------------------
// Wake primitive
// ---------------------------------------------------------------------

/// A dispatcher's park-and-nudge primitive: one file descriptor plus a
/// `poll` timeout.
///
/// Both halves of the wait — "somebody published" and "the poll interval
/// elapsed" — collapse into a single `poll(2)` call, which is exactly the
/// shape the rest of this codebase uses and leaves an idle dispatcher
/// parked in one syscall consuming nothing. On Linux an `eventfd` is one
/// descriptor with counter semantics, so a burst of nudges coalesces into
/// one readable event; elsewhere a self-pipe does the same job with two.
const Waker = struct {
    read_fd: sys.Fd,
    write_fd: sys.Fd,

    fn init() Error!Waker {
        if (builtin.os.tag == .linux) {
            const fd = sys.eventfd() catch return error.WakerFailed;
            return .{ .read_fd = fd, .write_fd = fd };
        }
        const p = sys.pipe() catch return error.WakerFailed;
        return .{ .read_fd = p.read_end, .write_fd = p.write_end };
    }

    fn deinit(self: Waker) void {
        sys.close(self.read_fd);
        if (self.write_fd != self.read_fd) sys.close(self.write_fd);
    }

    /// Make the next `wait` return immediately. Safe from any thread,
    /// and safe to call when the fd is already readable — the write just
    /// fails with EAGAIN on a full pipe, which is indistinguishable from
    /// success for our purposes.
    fn signal(self: Waker) void {
        const one = std.mem.toBytes(@as(u64, 1));
        _ = sys.write(self.write_fd, &one) catch {};
    }

    /// Park until signalled or `timeout_ms` elapses, then drain.
    fn wait(self: Waker, timeout_ms: i32) void {
        var fds = [_]sys.pollfd{.{ .fd = self.read_fd, .events = sys.POLL.IN, .revents = 0 }};
        _ = sys.poll(&fds, timeout_ms) catch return;
        if (fds[0].revents & sys.POLL.IN == 0) return;
        // Drain fully so a single nudge cannot wake us twice.
        var buf: [64]u8 = undefined;
        while (true) {
            const n = sys.read(self.read_fd, &buf) catch return;
            if (n < buf.len) return;
        }
    }
};

// ---------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------

/// One registered subscription and the thread that services it.
pub const Subscription = struct {
    bus: *Bus,
    /// Owned. Also the `outbox_subs.subscription` primary-key component.
    name: []const u8,
    /// Owned. Only events on this exact topic are delivered.
    topic: []const u8,
    handler: Handler,
    handler_ctx: ?*anyopaque,

    waker: Waker,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),

    /// Set by `publish` for a topic match, drained by the post-commit
    /// hook. Atomic because `publish` runs on the producer's thread while
    /// the dispatcher may be reading nothing here at all — the flag is
    /// only ever set by publishers and cleared by the hook, both on the
    /// producer side, but making it atomic costs nothing and documents
    /// that it crosses a thread boundary in the general case.
    wake_pending: std.atomic.Value(bool) = .init(false),

    /// Deliveries this subscription has attempted. Test observability;
    /// also a natural metric.
    attempted: std.atomic.Value(u64) = .init(0),
    delivered: std.atomic.Value(u64) = .init(0),

    pub fn subscriptionName(self: *const Subscription) []const u8 {
        return self.name;
    }
    pub fn subscriptionTopic(self: *const Subscription) []const u8 {
        return self.topic;
    }
};

/// A spin lock over the subscription registry.
///
/// This is the one shared mutable structure in the store layer, and the
/// only place a lock belongs: connections are thread-owned, so nothing
/// else here is shared at all. The critical sections are a handful of
/// pointer comparisons over a list that changes only at `subscribe` /
/// `close` time, so parking a thread in a futex would cost more than the
/// work it guards. Yielding after a bounded spin keeps a preempted
/// holder from turning that into a livelock.
const Spin = struct {
    state: std.atomic.Value(bool) = .init(false),

    const spin_limit = 128;

    fn lock(self: *Spin) void {
        var spins: usize = 0;
        while (self.state.swap(true, .acquire)) {
            spins += 1;
            if (spins < spin_limit) {
                std.atomic.spinLoopHint();
            } else {
                spins = 0;
                std.Thread.yield() catch std.atomic.spinLoopHint();
            }
        }
    }

    fn unlock(self: *Spin) void {
        self.state.store(false, .release);
    }
};

// ---------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------

pub const Bus = struct {
    gpa: Allocator,
    /// Path the dispatcher and pruner threads open their own connections
    /// against. Owned.
    db_path: []const u8,
    opts: Options,

    registry: Spin = .{},
    subs: std.ArrayList(*Subscription) = .empty,

    stopping: std.atomic.Value(bool) = .init(false),
    pruner: ?std.Thread = null,
    pruner_waker: ?Waker = null,

    /// Bus-wide counters, useful in tests and as metrics.
    published: std.atomic.Value(u64) = .init(0),
    pruned_subs: std.atomic.Value(u64) = .init(0),
    pruned_events: std.atomic.Value(u64) = .init(0),

    /// Create a bus over the database at `db_path`.
    ///
    /// No threads start until the first `subscribe`, so a component that
    /// only publishes pays nothing.
    pub fn init(gpa: Allocator, db_path: []const u8, opts: Options) Allocator.Error!*Bus {
        const self = try gpa.create(Bus);
        errdefer gpa.destroy(self);
        self.* = .{
            .gpa = gpa,
            .db_path = try gpa.dupe(u8, db_path),
            .opts = opts,
        };
        return self;
    }

    /// Stop every dispatcher and the pruner, wait for them, free
    /// everything. Idempotent.
    pub fn deinit(self: *Bus) void {
        self.stopping.store(true, .release);

        // Signal first, join second: a dispatcher parked in `poll` needs
        // the nudge to notice the stop flag at all.
        self.registry.lock();
        for (self.subs.items) |sub| {
            sub.stopping.store(true, .release);
            sub.waker.signal();
        }
        const subs = self.subs.items;
        self.registry.unlock();

        for (subs) |sub| {
            if (sub.thread) |th| th.join();
            sub.waker.deinit();
            self.gpa.free(sub.name);
            self.gpa.free(sub.topic);
            self.gpa.destroy(sub);
        }
        self.subs.deinit(self.gpa);

        if (self.pruner_waker) |w| w.signal();
        if (self.pruner) |th| th.join();
        if (self.pruner_waker) |w| w.deinit();

        self.gpa.free(self.db_path);
        const gpa = self.gpa;
        gpa.destroy(self);
    }

    // -- publish -------------------------------------------------------

    /// Persist `events` and their per-subscriber delivery rows.
    ///
    /// When `conn` already has a transaction open, the writes join it and
    /// the dispatcher nudge is deferred to a post-commit hook. Otherwise
    /// `publish` opens its own transaction and nudges as soon as it
    /// commits. Application code should normally be inside a transaction
    /// already — that is the entire point of an outbox.
    pub fn publish(self: *Bus, conn: *Conn, events: []const Event) Error!void {
        if (events.len == 0) return;
        if (self.stopping.load(.acquire)) return error.BusClosed;

        // Flag the subscriptions this batch concerns before writing, so
        // the post-commit hook needs no allocation and no second pass
        // over the registry. Only topic matches: creating `outbox_subs`
        // rows for subscriptions that will never select them is pure
        // waste — the dispatcher query filters on `o.topic = sub.topic`,
        // so those rows sit undelivered forever. The earlier
        // fan-out-to-everyone version turned each published event into
        // ~70 garbage rows on the live container.
        self.registry.lock();
        for (self.subs.items) |sub| {
            for (events) |e| {
                if (std.mem.eql(u8, sub.topic, e.topic)) {
                    sub.wake_pending.store(true, .release);
                    break;
                }
            }
        }
        self.registry.unlock();

        const ctx = PublishCtx{ .bus = self, .events = events };
        if (tx.inTransaction(conn)) {
            try insertBatch(ctx, conn);
            try tx.onCommit(conn, self, wakeFlagged);
        } else {
            try tx.inTx(conn, ctx, insertBatch);
            // Standalone transaction: it has already committed, so the
            // rows are visible and waking now is correct.
            wakeFlagged(self);
        }
        _ = self.published.fetchAdd(events.len, .monotonic);
    }

    const PublishCtx = struct {
        bus: *Bus,
        events: []const Event,
    };

    fn insertBatch(ctx: PublishCtx, conn: *Conn) Error!void {
        const self = ctx.bus;
        const now = self.opts.now();
        for (ctx.events) |e| {
            const id = uuidV7(now);
            const occurred = if (e.occurred_at_ms == 0) now else e.occurred_at_ms;
            try conn.execute(
                "INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
                .{ sqlite.blob(&id), e.topic, e.aggregate_id, occurred, sqlite.blob(e.payload) },
            );

            // One row per listening subscription. With nobody listening
            // the event still lands in `outbox`, which is what makes the
            // per-job timeline query work for topics that predate any
            // subscriber.
            self.registry.lock();
            defer self.registry.unlock();
            for (self.subs.items) |sub| {
                if (!std.mem.eql(u8, sub.topic, e.topic)) continue;
                try conn.execute(
                    "INSERT INTO outbox_subs(subscription, event_id, attempts) VALUES (?, ?, 0)",
                    .{ sub.name, sqlite.blob(&id) },
                );
            }
        }
    }

    /// Post-commit hook: turn the `wake_pending` flags into nudges.
    fn wakeFlagged(p: ?*anyopaque) void {
        const self: *Bus = @ptrCast(@alignCast(p.?));
        self.registry.lock();
        defer self.registry.unlock();
        for (self.subs.items) |sub| {
            if (sub.wake_pending.swap(false, .acquire)) sub.waker.signal();
        }
    }

    // -- subscribe -----------------------------------------------------

    /// Register `handler` under `name` for `topic` and start its
    /// dispatcher thread.
    ///
    /// The returned subscription is owned by the bus and stays valid
    /// until `deinit`.
    pub fn subscribe(
        self: *Bus,
        name: []const u8,
        topic: []const u8,
        handler: Handler,
        handler_ctx: ?*anyopaque,
    ) Error!*Subscription {
        if (self.stopping.load(.acquire)) return error.BusClosed;
        if (name.len == 0) return error.EmptyName;
        if (topic.len == 0) return error.EmptyTopic;

        self.registry.lock();
        for (self.subs.items) |existing| {
            if (std.mem.eql(u8, existing.name, name)) {
                self.registry.unlock();
                return error.DuplicateSubscription;
            }
        }
        self.registry.unlock();

        const sub = self.gpa.create(Subscription) catch return error.OutOfMemory;
        errdefer self.gpa.destroy(sub);
        const owned_name = self.gpa.dupe(u8, name) catch return error.OutOfMemory;
        errdefer self.gpa.free(owned_name);
        const owned_topic = self.gpa.dupe(u8, topic) catch return error.OutOfMemory;
        errdefer self.gpa.free(owned_topic);
        const waker = try Waker.init();
        errdefer waker.deinit();

        sub.* = .{
            .bus = self,
            .name = owned_name,
            .topic = owned_topic,
            .handler = handler,
            .handler_ctx = handler_ctx,
            .waker = waker,
        };

        // Register before spawning: the thread reads its own
        // `Subscription`, and a publisher that races us should see the
        // subscription rather than silently skip it.
        self.registry.lock();
        self.subs.append(self.gpa, sub) catch {
            self.registry.unlock();
            return error.OutOfMemory;
        };
        self.registry.unlock();

        sub.thread = std.Thread.spawn(.{}, dispatchLoop, .{sub}) catch {
            // Leave the registration in place: `deinit` frees it, and a
            // subscription with no thread simply never delivers, which
            // is strictly better than freeing memory a racing publisher
            // may be reading.
            return error.SpawnFailed;
        };

        if (self.opts.prune and self.pruner == null) {
            // Lazily started so a bus that never subscribes has no
            // background threads at all.
            const w = try Waker.init();
            self.pruner_waker = w;
            self.pruner = std.Thread.spawn(.{}, pruneLoop, .{self}) catch {
                w.deinit();
                self.pruner_waker = null;
                return error.SpawnFailed;
            };
        }

        return sub;
    }

    // -- pruning -------------------------------------------------------

    fn pruneLoop(self: *Bus) void {
        var conn = Conn.open(self.gpa, self.db_path, .{}) catch return;
        defer conn.close();

        // One pass on entry clears whatever backlog a previous process
        // left before the ticker takes over.
        self.pruneOnce(conn);
        while (!self.stopping.load(.acquire)) {
            self.pruner_waker.?.wait(self.opts.prune_interval_ms);
            if (self.stopping.load(.acquire)) return;
            self.pruneOnce(conn);
        }
    }

    /// Three bounded passes.
    ///
    ///   1. `outbox_subs` rows delivered longer ago than the retention
    ///      window.
    ///   2. `outbox_subs` rows still undelivered but whose event is more
    ///      than ten minutes old. A legitimate dispatch completes in
    ///      milliseconds, so anything still pending after ten minutes is
    ///      orphaned — a subscription removed mid-flight, or a row from
    ///      the era when `publish` fanned out to non-matching topics.
    ///      The constant is fixed rather than derived from the retention
    ///      window because the two answer different questions: retention
    ///      is "how long to remember a delivery for debugging", stale is
    ///      "this will never be delivered".
    ///   3. `outbox` rows past retention with no `outbox_subs` row left
    ///      pointing at them. Passes 1 and 2 have already reaped the
    ///      references, and an event nobody subscribed to never had any.
    ///      Without this pass the `outbox` table grows forever — 2.1M
    ///      rows in thirteen days on the production container, a 670 MiB
    ///      file that every dispatcher `SELECT` joined into. The
    ///      `NOT EXISTS` probe rides the `outbox_subs_event_id` index, so
    ///      each candidate check is a single index seek.
    ///
    /// Every pass is `LIMIT`ed and loops, so the first sweep after this
    /// ships cannot take the write lock for seconds at a time. Pass 3
    /// additionally yields between batches: it is the one that may have
    /// millions of rows to work through on its first run, and the
    /// orchestrator's transactions must keep flowing while it does.
    /// SQLite is built without `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`, hence
    /// the rowid-subquery form rather than `DELETE ... LIMIT`.
    pub fn pruneOnce(self: *Bus, conn: *Conn) void {
        const now = self.opts.now();
        const cutoff = now - self.opts.prune_retention_ms;
        const stale_cutoff = now - 10 * std.time.ms_per_min;

        while (!self.stopping.load(.acquire)) {
            conn.execute(
                \\DELETE FROM outbox_subs
                \\WHERE rowid IN (
                \\    SELECT rowid FROM outbox_subs
                \\    WHERE delivered_at IS NOT NULL AND delivered_at < ?
                \\    LIMIT 5000
                \\)
            , .{cutoff}) catch return;
            const n = conn.changes();
            if (n == 0) break;
            _ = self.pruned_subs.fetchAdd(@intCast(n), .monotonic);
        }

        while (!self.stopping.load(.acquire)) {
            conn.execute(
                \\DELETE FROM outbox_subs
                \\WHERE rowid IN (
                \\    SELECT s.rowid FROM outbox_subs s
                \\    JOIN outbox o ON o.id = s.event_id
                \\    WHERE s.delivered_at IS NULL AND o.occurred_at < ?
                \\    LIMIT 5000
                \\)
            , .{stale_cutoff}) catch return;
            const n = conn.changes();
            if (n == 0) break;
            _ = self.pruned_subs.fetchAdd(@intCast(n), .monotonic);
        }

        while (!self.stopping.load(.acquire)) {
            conn.execute(
                \\DELETE FROM outbox
                \\WHERE id IN (
                \\    SELECT id FROM outbox
                \\    WHERE occurred_at < ?
                \\      AND NOT EXISTS (SELECT 1 FROM outbox_subs WHERE event_id = outbox.id)
                \\    LIMIT 5000
                \\)
            , .{cutoff}) catch return;
            const n = conn.changes();
            if (n == 0) break;
            _ = self.pruned_events.fetchAdd(@intCast(n), .monotonic);
            sys.sleep(50 * std.time.ns_per_ms);
        }
    }

    // -- per-job timeline ----------------------------------------------

    /// Every outbox event whose payload carries `job_id`, oldest first.
    ///
    /// This is the operator-facing "what happened to my job" timeline,
    /// not the per-subscriber delivery audit, so retry metadata is not
    /// included.
    ///
    /// `json_extract` means a scan of `outbox`, which is acceptable
    /// because pass 3 of the pruner keeps that table bounded and the
    /// query runs on a human's click, not in a loop. If it ever gets hot
    /// the fix is a denormalised `job_id` column, not an index on an
    /// expression.
    pub fn eventsByJob(self: *Bus, conn: *Conn, gpa: Allocator, job_id: i64) Error!Timeline {
        _ = self;
        var out: Timeline = .{ .gpa = gpa };
        errdefer out.deinit();

        var st = try conn.query(
            \\SELECT id, topic, aggregate_id, occurred_at, payload
            \\FROM outbox
            \\WHERE json_extract(payload, '$.job_id') = ?
            \\ORDER BY occurred_at ASC, id ASC
        , .{job_id});
        defer st.release();

        while (try st.step()) {
            var env: Envelope = .{
                .id = .{0} ** 16,
                .topic = "",
                .aggregate_id = "",
                .occurred_at_ms = st.int(3),
                .payload = "",
                .attempts = 0,
            };
            const raw_id = st.bytes(0);
            @memcpy(env.id[0..@min(16, raw_id.len)], raw_id[0..@min(16, raw_id.len)]);
            env.topic = st.textAlloc(gpa, 1) catch return error.OutOfMemory;
            env.aggregate_id = st.textAlloc(gpa, 2) catch return error.OutOfMemory;
            env.payload = st.bytesAlloc(gpa, 4) catch return error.OutOfMemory;
            out.items.append(gpa, env) catch return error.OutOfMemory;
        }
        return out;
    }

    /// Owned result of `eventsByJob`. The envelopes' slices are heap
    /// copies, unlike the borrowed ones a handler sees, because a
    /// timeline outlives the statement that produced it.
    pub const Timeline = struct {
        gpa: Allocator,
        items: std.ArrayList(Envelope) = .empty,

        pub fn deinit(self: *Timeline) void {
            for (self.items.items) |env| {
                self.gpa.free(env.topic);
                self.gpa.free(env.aggregate_id);
                self.gpa.free(env.payload);
            }
            self.items.deinit(self.gpa);
        }
    };
};

// ---------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------

fn dispatchLoop(sub: *Subscription) void {
    const bus = sub.bus;
    // Own connection: `SQLITE_THREADSAFE=2` forbids sharing the
    // publisher's. If it cannot be opened the subscription simply never
    // delivers, which is visible through its counters staying at zero.
    var conn = Conn.open(bus.gpa, bus.db_path, .{}) catch return;
    defer conn.close();

    while (!sub.stopping.load(.acquire) and !bus.stopping.load(.acquire)) {
        sub.waker.wait(bus.opts.poll_interval_ms);
        if (sub.stopping.load(.acquire) or bus.stopping.load(.acquire)) return;

        // Drain: a nudge means at least one row, and a batch that came
        // back full probably means more.
        while (true) {
            const n = processBatch(sub, conn) catch break;
            if (n == 0) break;
            if (sub.stopping.load(.acquire) or bus.stopping.load(.acquire)) return;
        }
    }
}

/// Claim up to `batch_size` due rows and deliver each. Returns the number
/// attempted, success or not.
///
/// The rows are copied out before any handler runs. That is not caution
/// for its own sake: a handler is free to query the database, and the
/// mark-delivered `UPDATE` below writes to the very table the `SELECT`
/// is walking. Materializing first makes the cursor's lifetime a closed
/// question.
fn processBatch(sub: *Subscription, conn: *Conn) sqlite.Error!usize {
    const bus = sub.bus;
    const opts = bus.opts;

    var arena_state = std.heap.ArenaAllocator.init(bus.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var batch: std.ArrayList(Envelope) = .empty;

    {
        var st = try conn.query(
            \\SELECT s.event_id, s.attempts, o.topic, o.aggregate_id, o.occurred_at, o.payload
            \\FROM outbox_subs s
            \\JOIN outbox o ON o.id = s.event_id
            \\WHERE s.subscription = ?
            \\  AND s.delivered_at IS NULL
            \\  AND s.attempts < ?
            \\  AND o.topic = ?
            \\  AND (s.next_retry_at IS NULL OR s.next_retry_at <= ?)
            \\ORDER BY s.event_id
            \\LIMIT ?
        , .{
            sub.name,
            @as(i64, opts.max_delivery_attempts),
            sub.topic,
            opts.now(),
            @as(i64, opts.batch_size),
        });
        defer st.release();

        while (try st.step()) {
            var env: Envelope = .{
                .id = .{0} ** 16,
                .topic = "",
                .aggregate_id = "",
                .occurred_at_ms = st.int(4),
                .payload = "",
                .attempts = @as(u32, @intCast(st.int(1))) + 1,
            };
            const raw_id = st.bytes(0);
            if (raw_id.len != 16) {
                // A corrupt id cannot be marked delivered (the UPDATE
                // keys on it) and cannot be handed to a handler that
                // expects a UUID. Skip it; the stale-undelivered pruner
                // pass reaps it.
                continue;
            }
            @memcpy(&env.id, raw_id);
            env.topic = arena.dupe(u8, st.text(2)) catch return error.OutOfMemory;
            env.aggregate_id = arena.dupe(u8, st.text(3)) catch return error.OutOfMemory;
            env.payload = arena.dupe(u8, st.bytes(5)) catch return error.OutOfMemory;
            batch.append(arena, env) catch return error.OutOfMemory;
        }
    }

    for (batch.items) |env| {
        _ = sub.attempted.fetchAdd(1, .monotonic);
        switch (sub.handler(sub.handler_ctx, env)) {
            .ok => {
                markDelivered(sub, conn, env.id);
                _ = sub.delivered.fetchAdd(1, .monotonic);
            },
            .failed => |msg| markFailure(sub, conn, env.id, env.attempts, msg),
        }
    }
    return batch.items.len;
}

/// Errors here are swallowed on purpose. `Busy` clears on the next pass,
/// and a failure to *record* a delivery is not a failure to deliver — the
/// handler already ran. Re-raising would abandon the rest of the batch
/// for a condition that resolves itself, and the at-least-once contract
/// already covers the redelivery this causes.
fn markDelivered(sub: *Subscription, conn: *Conn, id: [16]u8) void {
    conn.execute(
        \\UPDATE outbox_subs
        \\SET delivered_at = ?, last_error = NULL, next_retry_at = NULL
        \\WHERE subscription = ? AND event_id = ?
    , .{ sub.bus.opts.now(), sub.name, sqlite.blob(&id) }) catch {};
}

fn markFailure(sub: *Subscription, conn: *Conn, id: [16]u8, attempts: u32, message: []const u8) void {
    const delay = backoff(sub.bus.opts, attempts);
    conn.execute(
        \\UPDATE outbox_subs
        \\SET attempts = ?, last_error = ?, next_retry_at = ?
        \\WHERE subscription = ? AND event_id = ?
    , .{
        @as(i64, attempts),
        message,
        sub.bus.opts.now() + delay,
        sub.name,
        sqlite.blob(&id),
    }) catch {};
}

/// Retry delay for the given attempt count: the base delay doubled once
/// per prior attempt, capped. Attempt 0 and 1 both wait the base delay,
/// so a caller that has not yet counted its first try is not punished.
pub fn backoff(opts: Options, attempts: u32) i64 {
    const n = @max(attempts, 1);
    var d = opts.backoff_base_ms;
    var i: u32 = 1;
    while (i < n) : (i += 1) {
        d *= 2;
        if (d >= opts.backoff_max_ms) return opts.backoff_max_ms;
    }
    return d;
}

// ---------------------------------------------------------------------
// UUIDv7
// ---------------------------------------------------------------------

/// A UUID version 7: 48-bit big-endian unix-millisecond timestamp, then
/// version and variant bits, then random.
///
/// Time-ordered by construction, which is why the `outbox` primary key is
/// a 16-byte BLOB and not an autoincrement integer: `ORDER BY event_id`
/// is chronological, so the dispatcher's cursor and the index it walks
/// are the same thing, and two processes can mint ids without
/// coordinating.
pub fn uuidV7(now_ms: i64) [16]u8 {
    var out: [16]u8 = undefined;
    randomBytes(out[6..]);

    const ms: u64 = @bitCast(now_ms);
    out[0] = @truncate(ms >> 40);
    out[1] = @truncate(ms >> 32);
    out[2] = @truncate(ms >> 24);
    out[3] = @truncate(ms >> 16);
    out[4] = @truncate(ms >> 8);
    out[5] = @truncate(ms);

    out[6] = (out[6] & 0x0F) | 0x70; // version 7
    out[8] = (out[8] & 0x3F) | 0x80; // variant 10
    return out;
}

/// Per-thread PRNG for the random half of a UUIDv7.
///
/// Not a CSPRNG, and it does not need to be: these ids are never secrets
/// or capabilities, they are primary keys whose only requirement is that
/// two of them minted in the same millisecond do not collide. 74 random
/// bits from xoshiro256++ makes that vanishingly unlikely, and the
/// thread-local state means minting an id in a hot publish loop touches
/// no shared cache line. Seeded from both clocks and the thread id so two
/// threads starting simultaneously do not share a stream.
threadlocal var prng: ?std.Random.DefaultPrng = null;

fn randomBytes(dest: []u8) void {
    if (prng == null) {
        const seed = @as(u64, @bitCast(@as(i64, @truncate(sys.realtimeNanos())))) ^
            (sys.monotonicNanos() << 1) ^
            (@as(u64, std.Thread.getCurrentId()) << 32);
        prng = std.Random.DefaultPrng.init(seed);
    }
    prng.?.random().bytes(dest);
}

/// Milliseconds encoded in a UUIDv7's timestamp field.
pub fn uuidV7Millis(id: [16]u8) i64 {
    var ms: u64 = 0;
    for (id[0..6]) |b| ms = (ms << 8) | b;
    return @intCast(ms);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;
const support = sqlite.testing_support;

/// A file-backed migrated database. Dispatch needs one connection per
/// thread, and `:memory:` is private per connection.
const Fixture = struct {
    path: []u8,
    conn: *Conn,

    fn init() !Fixture {
        const path = try support.tempPath(t.allocator);
        const conn = sqlite.Conn.open(t.allocator, path, .{}) catch |e| {
            t.allocator.free(path);
            return e;
        };
        try migrate.migrate(conn);
        return .{ .path = path, .conn = conn };
    }

    fn deinit(self: *Fixture) void {
        self.conn.close();
        support.removeTempFile(t.allocator, self.path);
        t.allocator.free(self.path);
    }
};

/// Options tuned for tests: fast ticks, fast backoff, no pruner racing
/// the assertions.
fn fastOptions() Options {
    return .{
        .poll_interval_ms = 10,
        .backoff_base_ms = 1,
        .backoff_max_ms = 5,
        .prune = false,
    };
}

/// Spin until `pred` holds or the deadline passes. Polling beats a
/// fixed sleep: the fast case finishes in microseconds and the slow case
/// still fails rather than flaking.
fn waitFor(ctx: anytype, comptime pred: anytype, timeout_ms: u64) !void {
    const deadline = sys.monotonicNanos() + timeout_ms * std.time.ns_per_ms;
    while (sys.monotonicNanos() < deadline) {
        if (pred(ctx)) return;
        sys.sleep(std.time.ns_per_ms);
    }
    if (pred(ctx)) return;
    return error.TestTimeout;
}

test "publish persists the event with its topic, aggregate and timestamp" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();

    const occurred: i64 = 1_700_000_000_000;
    try bus.publish(conn, &.{.{
        .topic = "test.topic",
        .aggregate_id = "agg-1",
        .occurred_at_ms = occurred,
        .payload = "{\"v\":\"hello\"}",
    }});

    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM outbox", .{}));
    var st = try conn.queryRow("SELECT topic, aggregate_id, occurred_at, payload FROM outbox", .{});
    defer st.release();
    try t.expectEqualStrings("test.topic", st.text(0));
    try t.expectEqualStrings("agg-1", st.text(1));
    try t.expectEqual(occurred, st.int(2));
    try t.expectEqualStrings("{\"v\":\"hello\"}", st.text(3));
}

test "a zero occurred_at is stamped at publish time" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false, .now = frozenNow });
    defer bus.deinit();

    try bus.publish(conn, &.{.{ .topic = "t", .aggregate_id = "a", .payload = "{}" }});
    try t.expectEqual(frozen_ms, try conn.scalarInt("SELECT occurred_at FROM outbox", .{}));
}

const frozen_ms: i64 = 1_700_000_000_000;
fn frozenNow() i64 {
    return frozen_ms;
}

test "publish inside a rolled-back transaction leaves nothing behind" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();

    const body = struct {
        fn run(b: *Bus, cn: *Conn) !void {
            try b.publish(cn, &.{.{ .topic = "a.b.c", .aggregate_id = "x", .payload = "{}" }});
            return error.UserRollback;
        }
    }.run;

    try t.expectError(error.UserRollback, tx.inTx(conn, bus, body));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM outbox", .{}));
}

test "publishing an empty batch is a no-op" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();
    try bus.publish(conn, &.{});
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM outbox", .{}));
}

test "publish after deinit-in-progress is refused" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();
    bus.stopping.store(true, .release);
    try t.expectError(
        error.BusClosed,
        bus.publish(conn, &.{.{ .topic = "t", .aggregate_id = "a", .payload = "{}" }}),
    );
}

test "sub rows are created only for topic-matching subscriptions" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    defer bus.deinit();

    _ = try bus.subscribe("listener", "wanted.topic", noopHandler, null);
    _ = try bus.subscribe("bystander", "other.topic", noopHandler, null);

    try bus.publish(fx.conn, &.{.{ .topic = "wanted.topic", .aggregate_id = "a", .payload = "{}" }});

    // Exactly one fan-out row. The version that created a row per
    // subscription regardless of topic turned each event into dozens of
    // rows that no dispatcher would ever select.
    try t.expectEqual(@as(i64, 1), try fx.conn.scalarInt(
        "SELECT COUNT(*) FROM outbox_subs WHERE subscription = ?",
        .{"listener"},
    ));
    try t.expectEqual(@as(i64, 0), try fx.conn.scalarInt(
        "SELECT COUNT(*) FROM outbox_subs WHERE subscription = ?",
        .{"bystander"},
    ));
}

test "an event with no subscriber still lands in the outbox" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();

    try bus.publish(conn, &.{.{ .topic = "nobody.listening", .aggregate_id = "a", .payload = "{}" }});
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM outbox", .{}));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM outbox_subs", .{}));
}

fn noopHandler(_: ?*anyopaque, _: Envelope) HandlerResult {
    return .ok;
}

/// Records what a handler saw, for the delivery assertions.
const Recorder = struct {
    calls: std.atomic.Value(u32) = .init(0),
    /// Fail the first `fail_first` calls, then succeed.
    fail_first: u32 = 0,
    /// Fail forever.
    always_fail: bool = false,

    topic: [64]u8 = .{0} ** 64,
    topic_len: usize = 0,
    aggregate: [64]u8 = .{0} ** 64,
    aggregate_len: usize = 0,
    first_attempts: u32 = 0,
    id: [16]u8 = .{0} ** 16,

    fn handle(p: ?*anyopaque, env: Envelope) HandlerResult {
        const self: *Recorder = @ptrCast(@alignCast(p.?));
        const n = self.calls.fetchAdd(1, .monotonic) + 1;
        if (n == 1) {
            @memcpy(self.topic[0..env.topic.len], env.topic);
            self.topic_len = env.topic.len;
            @memcpy(self.aggregate[0..env.aggregate_id.len], env.aggregate_id);
            self.aggregate_len = env.aggregate_id.len;
            self.first_attempts = env.attempts;
            self.id = env.id;
        }
        if (self.always_fail) return .{ .failed = "always fails" };
        if (n <= self.fail_first) return .{ .failed = "transient" };
        return .ok;
    }

    fn calledAtLeast(self: *Recorder, n: u32) bool {
        return self.calls.load(.monotonic) >= n;
    }
};

fn atLeastOnce(r: *Recorder) bool {
    return r.calledAtLeast(1);
}

test "a subscriber receives a published event and the row is marked delivered" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    defer bus.deinit();

    var rec = Recorder{};
    const sub = try bus.subscribe("worker", "test.topic", Recorder.handle, &rec);

    try bus.publish(fx.conn, &.{.{
        .topic = "test.topic",
        .aggregate_id = "agg-7",
        .payload = "{\"v\":\"p\"}",
    }});

    try waitFor(&rec, atLeastOnce, 5000);
    try t.expectEqualStrings("test.topic", rec.topic[0..rec.topic_len]);
    try t.expectEqualStrings("agg-7", rec.aggregate[0..rec.aggregate_len]);
    // First delivery is attempt 1, not 0 — an operator reading
    // "attempt 0" in a log would have to be told what it means.
    try t.expectEqual(@as(u32, 1), rec.first_attempts);
    // Version 7, variant 10: the id is a real UUIDv7, not 16 random bytes.
    try t.expectEqual(@as(u8, 0x70), rec.id[6] & 0xF0);
    try t.expectEqual(@as(u8, 0x80), rec.id[8] & 0xC0);

    const Waiting = struct {
        fx: *Fixture,
        fn marked(self: *const @This()) bool {
            const n = self.fx.conn.scalarIntOr(
                "SELECT COUNT(*) FROM outbox_subs WHERE subscription = ? AND delivered_at IS NOT NULL",
                .{"worker"},
                0,
            ) catch 0;
            return n == 1;
        }
    };
    var waiting = Waiting{ .fx = &fx };
    try waitFor(&waiting, Waiting.marked, 5000);
    try t.expect(sub.delivered.load(.monotonic) >= 1);
}

test "a failing handler is retried until it succeeds" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    defer bus.deinit();

    var rec = Recorder{ .fail_first = 2 };
    _ = try bus.subscribe("worker", "topic", Recorder.handle, &rec);
    try bus.publish(fx.conn, &.{.{ .topic = "topic", .aggregate_id = "x", .payload = "{}" }});

    const thrice = struct {
        fn pred(r: *Recorder) bool {
            return r.calledAtLeast(3);
        }
    }.pred;
    try waitFor(&rec, thrice, 5000);

    // Third call succeeded, so the row is delivered and last_error is
    // cleared — a parked row and a recovered row must not look alike.
    const Waiting = struct {
        fx: *Fixture,
        fn clean(self: *const @This()) bool {
            const n = self.fx.conn.scalarIntOr(
                \\SELECT COUNT(*) FROM outbox_subs
                \\WHERE delivered_at IS NOT NULL AND last_error IS NULL AND next_retry_at IS NULL
            , .{}, 0) catch 0;
            return n == 1;
        }
    };
    var waiting = Waiting{ .fx = &fx };
    try waitFor(&waiting, Waiting.clean, 5000);
}

test "a permanently failing handler is parked after max_delivery_attempts" {
    var fx = try Fixture.init();
    defer fx.deinit();
    var opts = fastOptions();
    opts.poll_interval_ms = 5;
    opts.max_delivery_attempts = 3;
    const bus = try Bus.init(t.allocator, fx.path, opts);
    defer bus.deinit();

    var rec = Recorder{ .always_fail = true };
    _ = try bus.subscribe("poison", "topic", Recorder.handle, &rec);
    try bus.publish(fx.conn, &.{.{ .topic = "topic", .aggregate_id = "x", .payload = "{}" }});

    const three = struct {
        fn pred(r: *Recorder) bool {
            return r.calledAtLeast(3);
        }
    }.pred;
    try waitFor(&rec, three, 5000);

    // Several more poll intervals must produce no further attempts: the
    // dispatcher is not allowed to spin on a poisoned event.
    sys.sleep(200 * std.time.ns_per_ms);
    try t.expectEqual(@as(u32, 3), rec.calls.load(.monotonic));

    // Parked, not deleted, with the error text intact for an operator.
    var st = try fx.conn.queryRow(
        "SELECT attempts, last_error, delivered_at FROM outbox_subs WHERE subscription = ?",
        .{"poison"},
    );
    defer st.release();
    try t.expectEqual(@as(i64, 3), st.int(0));
    try t.expectEqualStrings("always fails", st.text(1));
    try t.expect(st.isNull(2));
}

test "backoff doubles from the base and stops at the cap" {
    const opts = Options{ .backoff_base_ms = 1000, .backoff_max_ms = 10_000 };
    try t.expectEqual(@as(i64, 1000), backoff(opts, 0));
    try t.expectEqual(@as(i64, 1000), backoff(opts, 1));
    try t.expectEqual(@as(i64, 2000), backoff(opts, 2));
    try t.expectEqual(@as(i64, 4000), backoff(opts, 3));
    try t.expectEqual(@as(i64, 8000), backoff(opts, 4));
    try t.expectEqual(@as(i64, 10_000), backoff(opts, 5));
    try t.expectEqual(@as(i64, 10_000), backoff(opts, 20));
}

test "a duplicate subscription name is rejected" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    defer bus.deinit();

    _ = try bus.subscribe("a", "t1", noopHandler, null);
    // Names are the primary key of delivery state; two subscribers under
    // one name would steal each other's rows.
    try t.expectError(error.DuplicateSubscription, bus.subscribe("a", "t2", noopHandler, null));
}

test "subscribe validates its arguments" {
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();
    try t.expectError(error.EmptyName, bus.subscribe("", "t", noopHandler, null));
    try t.expectError(error.EmptyTopic, bus.subscribe("n", "", noopHandler, null));
}

test "deinit stops the dispatchers promptly" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    _ = try bus.subscribe("a", "t", noopHandler, null);

    // The nudge-then-join order is what makes this bounded: a dispatcher
    // parked in `poll` for the whole interval would otherwise hold
    // shutdown for that long.
    const start = sys.monotonicNanos();
    bus.deinit();
    const elapsed_ms = (sys.monotonicNanos() - start) / std.time.ns_per_ms;
    try t.expect(elapsed_ms < 2000);
}

test "the pruner starts on the first subscribe and stops with the bus" {
    var fx = try Fixture.init();
    defer fx.deinit();
    var opts = fastOptions();
    opts.prune = true;
    opts.prune_interval_ms = 10;
    // Retention of zero makes every delivered row immediately reapable, so
    // the pruner's effect is observable without waiting an hour.
    opts.prune_retention_ms = 0;
    const bus = try Bus.init(t.allocator, fx.path, opts);

    // No threads before the first subscribe: a component that only
    // publishes pays nothing.
    try t.expect(bus.pruner == null);
    _ = try bus.subscribe("worker", "topic", noopHandler, null);
    try t.expect(bus.pruner != null);

    try bus.publish(fx.conn, &.{.{ .topic = "topic", .aggregate_id = "x", .payload = "{}" }});

    const Waiting = struct {
        bus: *Bus,
        fn reaped(self: *const @This()) bool {
            return self.bus.pruned_subs.load(.monotonic) >= 1;
        }
    };
    var waiting = Waiting{ .bus = bus };
    try waitFor(&waiting, Waiting.reaped, 5000);

    // And shutdown joins it rather than leaking the thread.
    bus.deinit();
}

test "a second subscribe does not start a second pruner" {
    var fx = try Fixture.init();
    defer fx.deinit();
    var opts = fastOptions();
    opts.prune = true;
    const bus = try Bus.init(t.allocator, fx.path, opts);
    defer bus.deinit();

    _ = try bus.subscribe("a", "t", noopHandler, null);
    const first = bus.pruner.?;
    _ = try bus.subscribe("b", "t2", noopHandler, null);
    try t.expectEqual(first.getHandle(), bus.pruner.?.getHandle());
}

test "the wake fires only after the enclosing transaction commits" {
    // The race this pins: nudging at INSERT time wakes a dispatcher that
    // cannot yet see the rows. With a deliberately long poll interval,
    // delivery inside the timeout is only possible via a post-commit
    // nudge — a pre-commit nudge would leave the dispatcher asleep until
    // the tick.
    var fx = try Fixture.init();
    defer fx.deinit();
    var opts = fastOptions();
    opts.poll_interval_ms = 30_000;
    const bus = try Bus.init(t.allocator, fx.path, opts);
    defer bus.deinit();

    var rec = Recorder{};
    _ = try bus.subscribe("worker", "topic", Recorder.handle, &rec);

    const body = struct {
        fn run(b: *Bus, cn: *Conn) !void {
            try b.publish(cn, &.{.{ .topic = "topic", .aggregate_id = "x", .payload = "{}" }});
            // Still uncommitted: a dispatcher woken now would find
            // nothing. Give it every chance to make that mistake.
            sys.sleep(20 * std.time.ns_per_ms);
        }
    }.run;
    try tx.inTx(fx.conn, bus, body);

    try waitFor(&rec, atLeastOnce, 5000);
}

test "wake flags are per-subscription, so an unrelated dispatcher is not woken" {
    var fx = try Fixture.init();
    defer fx.deinit();
    var opts = fastOptions();
    opts.poll_interval_ms = 30_000;
    const bus = try Bus.init(t.allocator, fx.path, opts);
    defer bus.deinit();

    var wanted = Recorder{};
    var other = Recorder{};
    _ = try bus.subscribe("wanted", "a", Recorder.handle, &wanted);
    _ = try bus.subscribe("other", "b", Recorder.handle, &other);

    try bus.publish(fx.conn, &.{.{ .topic = "a", .aggregate_id = "x", .payload = "{}" }});
    try waitFor(&wanted, atLeastOnce, 5000);
    // The "b" dispatcher has a 30 s tick and was never nudged, so it has
    // had no opportunity to run at all.
    try t.expectEqual(@as(u32, 0), other.calls.load(.monotonic));
}

test "each event is delivered once per matching subscription" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    defer bus.deinit();

    var a = Recorder{};
    var b = Recorder{};
    _ = try bus.subscribe("a", "shared", Recorder.handle, &a);
    _ = try bus.subscribe("b", "shared", Recorder.handle, &b);

    try bus.publish(fx.conn, &.{.{ .topic = "shared", .aggregate_id = "x", .payload = "{}" }});

    try waitFor(&a, atLeastOnce, 5000);
    try waitFor(&b, atLeastOnce, 5000);
    // At-least-once, but with nothing failing there is no reason for a
    // second attempt.
    sys.sleep(100 * std.time.ns_per_ms);
    try t.expectEqual(@as(u32, 1), a.calls.load(.monotonic));
    try t.expectEqual(@as(u32, 1), b.calls.load(.monotonic));
}

test "a batch of events all reach the subscriber" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const bus = try Bus.init(t.allocator, fx.path, fastOptions());
    defer bus.deinit();

    var rec = Recorder{};
    _ = try bus.subscribe("worker", "bulk", Recorder.handle, &rec);

    var events: [25]Event = undefined;
    for (&events) |*e| e.* = .{ .topic = "bulk", .aggregate_id = "x", .payload = "{}" };
    try bus.publish(fx.conn, &events);

    const all_seen = struct {
        fn pred(r: *Recorder) bool {
            return r.calledAtLeast(25);
        }
    }.pred;
    try waitFor(&rec, all_seen, 10_000);
}

test "prune reaps delivered rows past retention" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false, .now = frozenNow });
    defer bus.deinit();

    // One delivered row inside the window, one outside it.
    const id_old = uuidV7(frozen_ms);
    const id_new = uuidV7(frozen_ms);
    const long_ago = frozen_ms - 2 * std.time.ms_per_hour;
    try conn.execute(
        "INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
        .{ sqlite.blob(&id_old), "t", "a", long_ago, sqlite.blob("{}") },
    );
    try conn.execute(
        "INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
        .{ sqlite.blob(&id_new), "t", "a", frozen_ms, sqlite.blob("{}") },
    );
    try conn.execute(
        "INSERT INTO outbox_subs(subscription, event_id, attempts, delivered_at) VALUES (?, ?, 1, ?)",
        .{ "s", sqlite.blob(&id_old), long_ago },
    );
    try conn.execute(
        "INSERT INTO outbox_subs(subscription, event_id, attempts, delivered_at) VALUES (?, ?, 1, ?)",
        .{ "s", sqlite.blob(&id_new), frozen_ms },
    );

    bus.pruneOnce(conn);
    // Pass 1 reaps the stale delivery row; pass 3 then reaps the event it
    // referenced, since nothing points at it and it is past retention.
    // The recent pair is untouched.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM outbox_subs", .{}));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM outbox", .{}));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM outbox WHERE id = ?",
        .{sqlite.blob(&id_new)},
    ));
}

test "prune reaps stale-undelivered rows" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false, .now = frozenNow });
    defer bus.deinit();

    // An undelivered row whose event is older than the ten-minute stale
    // window: orphaned, and it would otherwise sit in every dispatcher's
    // scan forever.
    const id = uuidV7(frozen_ms);
    try conn.execute(
        "INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
        .{ sqlite.blob(&id), "t", "a", frozen_ms - 30 * std.time.ms_per_min, sqlite.blob("{}") },
    );
    try conn.execute(
        "INSERT INTO outbox_subs(subscription, event_id, attempts) VALUES (?, ?, 0)",
        .{ "gone", sqlite.blob(&id) },
    );

    bus.pruneOnce(conn);
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM outbox_subs", .{}));
}

test "prune reaps orphan outbox rows past retention but keeps referenced ones" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false, .now = frozenNow });
    defer bus.deinit();

    const orphan_old = uuidV7(frozen_ms);
    const orphan_recent = uuidV7(frozen_ms);
    const referenced = uuidV7(frozen_ms);
    const old_ts = frozen_ms - 2 * std.time.ms_per_hour;

    for ([_]struct { id: [16]u8, ts: i64 }{
        .{ .id = orphan_old, .ts = old_ts },
        .{ .id = orphan_recent, .ts = frozen_ms },
        .{ .id = referenced, .ts = old_ts },
    }) |row| {
        try conn.execute(
            "INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
            .{ sqlite.blob(&row.id), "t", "a", row.ts, sqlite.blob("{}") },
        );
    }
    // A pending row young enough to survive pass 2 keeps its event alive.
    try conn.execute(
        "INSERT INTO outbox_subs(subscription, event_id, attempts) VALUES (?, ?, 0)",
        .{ "live", sqlite.blob(&referenced) },
    );
    try conn.execute("UPDATE outbox SET occurred_at = ? WHERE id = ?", .{ frozen_ms, sqlite.blob(&referenced) });

    bus.pruneOnce(conn);

    try t.expectEqual(@as(i64, 0), try conn.scalarInt(
        "SELECT COUNT(*) FROM outbox WHERE id = ?",
        .{sqlite.blob(&orphan_old)},
    ));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM outbox WHERE id = ?",
        .{sqlite.blob(&orphan_recent)},
    ));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt(
        "SELECT COUNT(*) FROM outbox WHERE id = ?",
        .{sqlite.blob(&referenced)},
    ));
    try t.expect(bus.pruned_events.load(.monotonic) >= 1);
}

test "deleting an outbox row cascades to its sub rows" {
    const conn = try migrate.openMigrated();
    defer conn.close();

    const id = uuidV7(frozen_ms);
    try conn.execute(
        "INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
        .{ sqlite.blob(&id), "t", "a", frozen_ms, sqlite.blob("{}") },
    );
    try conn.execute(
        "INSERT INTO outbox_subs(subscription, event_id, attempts) VALUES (?, ?, 0)",
        .{ "s", sqlite.blob(&id) },
    );
    try conn.execute("DELETE FROM outbox WHERE id = ?", .{sqlite.blob(&id)});
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM outbox_subs", .{}));
}

test "eventsByJob returns the job's events oldest first" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();

    try bus.publish(conn, &.{
        .{ .topic = "download.job.created", .aggregate_id = "7", .occurred_at_ms = 300, .payload = "{\"job_id\":7}" },
        .{ .topic = "verify.set.ok", .aggregate_id = "9", .occurred_at_ms = 100, .payload = "{\"job_id\":7}" },
        .{ .topic = "download.job.created", .aggregate_id = "8", .occurred_at_ms = 200, .payload = "{\"job_id\":8}" },
    });

    var timeline = try bus.eventsByJob(conn, t.allocator, 7);
    defer timeline.deinit();

    try t.expectEqual(@as(usize, 2), timeline.items.items.len);
    try t.expectEqualStrings("verify.set.ok", timeline.items.items[0].topic);
    try t.expectEqualStrings("download.job.created", timeline.items.items[1].topic);
    try t.expectEqualStrings("{\"job_id\":7}", timeline.items.items[1].payload);
}

test "eventsByJob on an unknown job is empty, not an error" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const bus = try Bus.init(t.allocator, ":memory:", .{ .prune = false });
    defer bus.deinit();

    var timeline = try bus.eventsByJob(conn, t.allocator, 12345);
    defer timeline.deinit();
    try t.expectEqual(@as(usize, 0), timeline.items.items.len);
}

test "uuidV7 is time-ordered and carries its timestamp" {
    const a = uuidV7(1_700_000_000_000);
    const b = uuidV7(1_700_000_000_001);
    // Lexicographic order on the 16-byte BLOB is chronological order,
    // which is what makes `ORDER BY event_id` a valid delivery cursor.
    try t.expect(std.mem.order(u8, &a, &b) == .lt);
    try t.expectEqual(@as(i64, 1_700_000_000_000), uuidV7Millis(a));
    try t.expectEqual(@as(u8, 0x70), a[6] & 0xF0);
    try t.expectEqual(@as(u8, 0x80), a[8] & 0xC0);

    // Two ids minted in the same millisecond still differ.
    const c1 = uuidV7(frozen_ms);
    const c2 = uuidV7(frozen_ms);
    try t.expect(!std.mem.eql(u8, &c1, &c2));
}
