//! The post-download pipeline, off the reactor thread.
//!
//! ## The problem
//!
//! `verify` hashes every byte of a release and `extract` decompresses it.
//! Run inline in their bus handler — which is where they ran — a 40 GB
//! release holds the loop thread for minutes: the HTTP surface answers
//! nothing, the SSE streams go silent, and every other download stops.
//!
//! Two things have to happen and they are not the same thing:
//!
//!   1. **The CPU has to leave the loop thread.** That is `posix/pool.zig`
//!      — worker threads parked on a futex, costing nothing when idle.
//!   2. **The caller has to be able to wait without blocking.** A stage
//!      is straight-line code around `verifier.verify(...)`; it cannot
//!      "return later". That is a fiber, exactly as `bootstrap/runtime.zig`
//!      uses one to give the download orchestrator a blocking-shaped
//!      `fetch`.
//!
//! So: one stage runs on a fiber, its hashing runs on a worker, and the
//! fiber parks in between. The loop thread is free the whole time.
//!
//! ## What this costs, stated plainly
//!
//! The bus handler now returns as soon as the stage is *queued*, not once
//! it has finished. The outbox row is settled on "we accepted this",
//! which means a crash between the hand-off and the stage's own commit
//! loses that redelivery. Three things make that the right trade rather
//! than a regression:
//!
//!   * The stages are idempotent by construction — each loads or creates
//!     its aggregate, skips a terminal one, and re-runs cleanly. That is
//!     what makes the outbox's at-least-once safe in the first place, and
//!     it is what makes a missed delivery recoverable.
//!   * A failure *inside* a stage is already not redelivered: it is
//!     recorded on the aggregate as a failed pass. Only a crash in the
//!     window loses anything.
//!   * The alternative is a daemon that stops answering for the length of
//!     a verification, which is not a trade at all.
//!
//! ## Shutdown waits for the worker
//!
//! A CPU body running on a worker owns a slice of the stage fiber's
//! stack, so that stack cannot be freed until the worker has returned.
//! `deinit` therefore joins the pool *first* and only then unwinds the
//! fiber — which means a shutdown during a large verification waits it
//! out. Abandoning the thread instead would be a use-after-free on a
//! `munmap`'d stack, so this is deliberate: the pool is owned here,
//! rather than shared, precisely so that ordering cannot be got wrong
//! from outside.
//!
//! ## Rules a stage body must keep
//!
//!   * **Never park inside a transaction.** The fiber runs on the loop
//!     thread, so the loop services other work — including other SQLite
//!     work — while it is away. `app/verify.zig` and `app/extract.zig`
//!     both call their injected port outside `inTx`, which is what makes
//!     this legal; a stage that ever calls one from inside a unit of work
//!     would corrupt the transaction.
//!   * **Nothing but the CPU body runs on a worker.** No database, no SSE
//!     hub, no `*Conn`. See `posix/pool.zig`.

const std = @import("std");

const log = @import("../core/log.zig");
const reactor = @import("../posix/reactor.zig");
const fiber_mod = @import("../posix/fiber.zig");
const wpool = @import("../posix/pool.zig");

const Allocator = std.mem.Allocator;
const Fiber = fiber_mod.Fiber;

/// A stage fiber calls into SQLite, the filesystem port and the notify
/// renderer, but the heavy lifting is on a worker, so it is shallower
/// than a download runner. The module default anyway — it is address
/// space, not memory.
const stage_stack_size = fiber_mod.default_stack_size;

/// Backlog ceiling. Past it the handler reports failure and the outbox's
/// own backoff holds the event, which is the right place for it to wait.
const max_queued: usize = 512;

/// Which stage a queued item is. The bodies live in the composition root;
/// this is only the label the runner dispatches on.
pub const Stage = enum {
    verify,
    reverify,
    repair,
    extract,
    deliver,

    pub fn text(s: Stage) []const u8 {
        return switch (s) {
            .verify => "verify",
            .reverify => "verify-after-repair",
            .repair => "repair",
            .extract => "extract",
            .deliver => "deliver",
        };
    }
};

pub const OffloadError = error{
    /// No pool wired, or it refused the task. The caller runs inline.
    Unavailable,
    /// Shutdown, or the pool cancelled the task before it ran.
    Canceled,
};

// =====================================================================
// Runner
// =====================================================================

/// Queues stages, runs them one at a time on a fiber, and owns the
/// worker pool their CPU is handed to.
///
/// One at a time on purpose. Two concurrent verifications on a NAS
/// contend for the same disk and the same memory bandwidth and finish no
/// sooner; serialising them keeps exactly one aggregate loaded, one
/// stack live, and makes `current()` unambiguous for the ports below.
pub const Stages = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    logger: *log.Logger = &log.default,

    /// The composition root's stage bodies. `ctx` is its `*App`.
    ctx: *anyopaque,
    bodyFn: *const fn (ctx: *anyopaque, stage: Stage, job_id: i64) anyerror!void,

    pool: wpool.Pool = undefined,
    pool_ready: bool = false,

    queue: std.ArrayList(Item) = .empty,
    run: ?*Run = null,
    pump: reactor.Timer = .{ .callback = onPump },
    stopping: bool = false,

    /// Diagnostics, loop thread only.
    started: usize = 0,
    refused: usize = 0,

    pub const Item = struct { stage: Stage, job_id: i64 };

    /// Bring the pool up. Split from the struct literal so a caller that
    /// wants the graph without threads — a test that drives stages by
    /// hand — simply does not call it, and every stage then runs inline
    /// exactly as it did before this file existed.
    pub fn start(self: *Stages, options: wpool.Options) !void {
        if (self.pool_ready) return;
        try self.pool.init(self.gpa, self.loop, options);
        self.pool_ready = true;
    }

    /// Stop taking work, finish what a worker already started, unwind the
    /// fiber, free the backlog. See the module comment on the ordering.
    pub fn deinit(self: *Stages) void {
        self.stopping = true;
        if (self.pump.isArmed()) self.loop.cancelTimer(&self.pump);

        // First: the workers. A body mid-run holds a pointer into the
        // stage fiber's stack, so the stack cannot be freed until it has
        // returned. `Pool.deinit` joins, then delivers the completions it
        // owes — including the one that settles our run.
        if (self.pool_ready) {
            self.pool.deinit();
            self.pool_ready = false;
        }

        if (self.run) |r| {
            r.canceled = true;
            // Unwind rather than discard, so the body's `defer`s release
            // the aggregate and the arena.
            var guard: usize = 0;
            while (!r.finished and guard < 64) : (guard += 1) {
                r.enter();
            }
            if (!r.finished) {
                self.logger.warn("pipeline: a stage could not be unwound at shutdown", &.{
                    log.str("stage", r.stage.text()),
                    log.int("job_id", r.job_id),
                });
            }
            r.deinit();
            self.gpa.destroy(r);
            self.run = null;
        }

        self.queue.deinit(self.gpa);
    }

    /// Accept one stage invocation. Returns an error only when the
    /// backlog is full or memory is gone — both of which the caller
    /// should report to the bus so the event is retried rather than lost.
    pub fn enqueue(self: *Stages, stage: Stage, job_id: i64) error{ Full, OutOfMemory }!void {
        if (self.stopping) return error.Full;
        if (self.queue.items.len >= max_queued) {
            self.refused += 1;
            return error.Full;
        }
        try self.queue.append(self.gpa, .{ .stage = stage, .job_id = job_id });
        self.armPump();
    }

    /// The stage fiber currently executing, if any. This is what the
    /// `Verifier` and `Extractor` ports consult to find out whether they
    /// have somewhere to park; a null means "run inline".
    ///
    /// Unambiguous because exactly one run is live and because it is only
    /// non-null between `fiber.enter()` and the return from it.
    pub fn current(self: *Stages) ?*Run {
        const r = self.run orelse return null;
        if (!r.executing or r.finished) return null;
        if (!self.pool_ready) return null;
        return r;
    }

    pub fn busy(self: *const Stages) bool {
        return self.run != null or self.queue.items.len != 0;
    }

    fn armPump(self: *Stages) void {
        if (self.stopping or self.pump.isArmed()) return;
        self.loop.addTimer(&self.pump, 0) catch {};
    }

    fn onPump(t: *reactor.Timer) void {
        const self: *Stages = @fieldParentPtr("pump", t);
        self.advance();
    }

    /// Reap a finished run, then start the next.
    ///
    /// Reaping happens here rather than in the fiber's `on_finished`,
    /// which runs on the resumer's stack: freeing the fiber from inside
    /// the frame that just entered it would pull the ground out from
    /// under `Fiber.enter`'s own epilogue.
    fn advance(self: *Stages) void {
        if (self.stopping) return;

        if (self.run) |r| {
            if (!r.finished) return;
            r.deinit();
            self.gpa.destroy(r);
            self.run = null;
        }
        if (self.queue.items.len == 0) return;

        const item = self.queue.orderedRemove(0);
        const r = self.gpa.create(Run) catch {
            self.logger.err("pipeline: cannot allocate a stage runner", &.{
                log.str("stage", item.stage.text()),
                log.int("job_id", item.job_id),
            });
            return;
        };
        r.* = .{ .s = self, .stage = item.stage, .job_id = item.job_id };
        r.fiber.init(self.gpa, self.loop, stage_stack_size, Run.entry, @ptrCast(r)) catch |e| {
            self.logger.err("pipeline: cannot start a stage", &.{
                log.str("stage", item.stage.text()),
                log.errv("err", e),
            });
            self.gpa.destroy(r);
            return;
        };
        r.fiber.on_finished = Run.onFiberFinished;
        self.run = r;
        self.started += 1;
        r.enter();
        if (r.finished) self.armPump();
    }
};

/// One stage on its own stack.
pub const Run = struct {
    s: *Stages,
    stage: Stage,
    job_id: i64,

    fiber: Fiber = undefined,
    /// Resume clock. A stage waits for exactly one thing at a time — a
    /// worker finishing — so one timer serves.
    timer: reactor.Timer = .{ .callback = onTimer },

    /// True between `fiber.enter()` and the return from it. What makes
    /// `Stages.current` mean "the fiber you are standing on" rather than
    /// "some fiber exists".
    executing: bool = false,
    awaiting: bool = false,
    settled: bool = false,
    canceled: bool = false,
    finished: bool = false,

    /// Set once the stack has been released. See `onFiberFinished`.
    stack_freed: bool = false,

    fn deinit(self: *Run) void {
        if (self.timer.isArmed()) self.s.loop.cancelTimer(&self.timer);
        if (!self.stack_freed) self.fiber.deinit();
    }

    fn entry(f: *Fiber, ctx: ?*anyopaque) void {
        _ = f;
        const self: *Run = @ptrCast(@alignCast(ctx.?));
        self.s.bodyFn(self.s.ctx, self.stage, self.job_id) catch |e| {
            // A stage that fails is a recorded failure on its own
            // aggregate; this is the log line for the case where it could
            // not even get that far.
            self.s.logger.err("pipeline stage failed", &.{
                log.str("stage", self.stage.text()),
                log.int("job_id", self.job_id),
                log.errv("err", e),
            });
        };
    }

    /// The stack goes back here rather than in `deinit`.
    ///
    /// `Fiber.enter` calls this as its last act, once the body has
    /// returned and the reactor registration is gone, so freeing from
    /// here is explicitly safe. Doing it now rather than at the next
    /// pump matters: a megabyte of mapping per stage would otherwise
    /// stay reserved until the loop came back around, and "no fiber
    /// outlives the work it was made for" is a property the suite
    /// asserts on with `fiber.liveStacks()`.
    ///
    /// The `Run` itself survives — the frame that entered the fiber is
    /// still on the stack and still reads it.
    fn onFiberFinished(f: *Fiber) void {
        const self: *Run = @fieldParentPtr("fiber", f);
        self.finished = true;
        self.fiber.deinit();
        self.stack_freed = true;
        self.s.armPump();
    }

    fn enter(self: *Run) void {
        if (self.finished) return;
        self.executing = true;
        self.fiber.enter();
        // `fiber` may be gone by now; `self` is not.
        self.executing = false;
    }

    /// Switch back to the loop. Only the resume timer brings us back.
    fn park(self: *Run) void {
        self.awaiting = true;
        self.fiber.yield();
        self.awaiting = false;
    }

    fn armTimer(self: *Run, delay_ns: u64) void {
        if (self.timer.isArmed()) self.s.loop.cancelTimer(&self.timer);
        self.s.loop.addTimer(&self.timer, delay_ns) catch {
            self.canceled = true;
            self.s.loop.addTimer(&self.timer, 0) catch {};
        };
    }

    fn onTimer(t: *reactor.Timer) void {
        const self: *Run = @fieldParentPtr("timer", t);
        if (self.finished) return;
        self.enter();
    }

    /// Called from the pool's completion, on the loop thread. Records and
    /// arms; never enters the fiber, because a completion runs inside the
    /// pool's own drain loop.
    fn settle(self: *Run) void {
        self.settled = true;
        if (self.awaiting) self.armTimer(0);
    }
};

// =====================================================================
// The offload itself
// =====================================================================

/// Run `f(ctx)` on a worker thread, parking the stage fiber until it
/// returns.
///
/// `ctx` normally points into the fiber's own stack, which is what makes
/// the shutdown ordering in `Stages.deinit` load-bearing: the worker
/// writes through that pointer, so the stack must outlive the task.
///
/// Returns `Unavailable` when there is no pool or no room in it — the
/// caller then does the work inline, which is correct but slow, and is
/// exactly the behaviour of a build with no pool wired at all.
pub fn offload(
    comptime T: type,
    run: *Run,
    ctx: *T,
    comptime f: fn (*T) void,
) OffloadError!void {
    if (run.canceled or run.s.stopping) return error.Canceled;
    if (!run.s.pool_ready) return error.Unavailable;

    const Wrapper = struct {
        task: wpool.Task,
        run: *Run,
        ctx: *T,
        ran: bool = false,

        fn body(t: *wpool.Task) void {
            const w: *@This() = @fieldParentPtr("task", t);
            f(w.ctx);
            w.ran = true;
        }

        fn done(t: *wpool.Task, status: wpool.Status) void {
            const w: *@This() = @fieldParentPtr("task", t);
            _ = status;
            w.run.settle();
        }
    };

    var w: Wrapper = .{
        .task = .{ .run = Wrapper.body, .complete = Wrapper.done },
        .run = run,
        .ctx = ctx,
    };

    run.settled = false;
    run.s.pool.submit(&w.task) catch return error.Unavailable;

    // `submit` never completes synchronously — a worker has to pick the
    // task up — but the check costs nothing and makes the park correct
    // regardless of that.
    if (!run.settled) run.park();

    // `ran` first, and `canceled` only after it. A shutdown joins the
    // pool before it unwinds this fiber, so the common teardown case is
    // work that *did* complete — and throwing that result away would
    // make the stage record a failure it did not have, on an aggregate
    // that would then be terminal after the restart.
    if (w.ran) return;
    return error.Canceled;
}

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const sys = @import("../posix/sys.zig");

/// A composition root stand-in: records which stages ran, and burns CPU
/// through the pool while it does.
const Recorder = struct {
    stages: Stages = undefined,
    seen: std.ArrayList(Stages.Item) = .empty,
    gpa: Allocator,
    /// Nanoseconds of CPU each stage body spends on a worker.
    spin_ns: u64 = 0,
    offloaded: usize = 0,
    inline_runs: usize = 0,
    fail_on: ?Stage = null,

    const Work = struct {
        spin_ns: u64,
        ran_on: std.Thread.Id = 0,

        fn body(w: *Work) void {
            w.ran_on = std.Thread.getCurrentId();
            if (w.spin_ns == 0) return;
            const until = sys.monotonicNanos() + w.spin_ns;
            while (sys.monotonicNanos() < until) {}
        }
    };

    fn bodyFn(ctx: *anyopaque, stage: Stage, job_id: i64) anyerror!void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        try self.seen.append(self.gpa, .{ .stage = stage, .job_id = job_id });

        var work: Work = .{ .spin_ns = self.spin_ns };
        if (self.stages.current()) |run| {
            offload(Work, run, &work, Work.body) catch {
                Work.body(&work);
                self.inline_runs += 1;
                if (self.fail_on == stage) return error.StageFailed;
                return;
            };
            self.offloaded += 1;
            // The whole point, asserted where it is cheap to assert.
            try testing.expect(work.ran_on != std.Thread.getCurrentId());
        } else {
            Work.body(&work);
            self.inline_runs += 1;
        }
        if (self.fail_on == stage) return error.StageFailed;
    }

    fn deinit(self: *Recorder) void {
        self.seen.deinit(self.gpa);
    }
};

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

test "a queued stage runs on a fiber with its CPU on a worker" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var rec: Recorder = .{ .gpa = gpa, .spin_ns = 5 * std.time.ns_per_ms };
    defer rec.deinit();
    rec.stages = .{ .gpa = gpa, .loop = &loop, .ctx = @ptrCast(&rec), .bodyFn = &Recorder.bodyFn };
    try rec.stages.start(.{ .workers = 2 });
    defer rec.stages.deinit();

    try rec.stages.enqueue(.verify, 7);
    try rec.stages.enqueue(.extract, 9);

    const Waiter = struct {
        r: *Recorder,
        fn done(w: *const @This()) bool {
            return w.r.seen.items.len == 2 and !w.r.stages.busy();
        }
    };
    var w: Waiter = .{ .r = &rec };
    try pumpUntil(&loop, 10_000, &w, Waiter.done);

    try testing.expectEqual(@as(usize, 2), rec.offloaded);
    try testing.expectEqual(@as(usize, 0), rec.inline_runs);
    // FIFO: the pipeline's stages are ordered and a reordering would run
    // extract against a set verify had not finished with.
    try testing.expectEqual(Stage.verify, rec.seen.items[0].stage);
    try testing.expectEqual(@as(i64, 7), rec.seen.items[0].job_id);
    try testing.expectEqual(Stage.extract, rec.seen.items[1].stage);
}

test "with no pool the stage still runs, inline" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // `start` never called: the graph exists, the threads do not. This is
    // the shape a test that drives the pipeline by hand gets, and it must
    // behave exactly as the pipeline did before any of this.
    var rec: Recorder = .{ .gpa = gpa };
    defer rec.deinit();
    rec.stages = .{ .gpa = gpa, .loop = &loop, .ctx = @ptrCast(&rec), .bodyFn = &Recorder.bodyFn };
    defer rec.stages.deinit();

    try rec.stages.enqueue(.deliver, 3);

    const Waiter = struct {
        r: *Recorder,
        fn done(w: *const @This()) bool {
            return w.r.seen.items.len == 1 and !w.r.stages.busy();
        }
    };
    var w: Waiter = .{ .r = &rec };
    try pumpUntil(&loop, 5000, &w, Waiter.done);

    try testing.expectEqual(@as(usize, 1), rec.inline_runs);
    try testing.expectEqual(@as(usize, 0), rec.offloaded);
}

test "the loop answers an HTTP request while a stage burns 200ms of CPU" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // The actual user-visible symptom: the web UI going dead for the
    // length of a verification. A real server, a real client, and a stage
    // that would have held the loop thread for 200 ms.
    const server_mod = @import("../net/http/server.zig");
    const client = @import("../net/http/client.zig");
    const IpAddress = std.Io.net.IpAddress;

    const routes = [_]server_mod.Route{.{
        .method = .get,
        .path = "/healthz",
        .handler = struct {
            fn h(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
                try ctx.res.send(200, "application/json", "{\"ok\":true}");
            }
        }.h,
        .access = .public,
    }};

    var srv: server_mod.Server = undefined;
    srv.init(gpa, &loop, .{}, &routes);
    try srv.listen(try IpAddress.parse("127.0.0.1", 0));
    defer srv.deinit();
    const port = try srv.boundPort();

    var rec: Recorder = .{ .gpa = gpa, .spin_ns = 200 * std.time.ns_per_ms };
    defer rec.deinit();
    rec.stages = .{ .gpa = gpa, .loop = &loop, .ctx = @ptrCast(&rec), .bodyFn = &Recorder.bodyFn };
    try rec.stages.start(.{ .workers = 2 });
    defer rec.stages.deinit();

    try rec.stages.enqueue(.verify, 1);
    // One tick to get the stage onto a worker before the request goes out.
    _ = try loop.tick(5);
    _ = try loop.tick(5);

    const Collected = struct {
        result: ?(client.Error!client.Response) = null,
        fn cb(ctx: ?*anyopaque, result: client.Error!client.Response) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.result = result;
        }
        fn done(self: *const @This()) bool {
            return self.result != null;
        }
    };
    var col: Collected = .{};
    defer if (col.result) |r| {
        if (r) |resp| {
            var m = resp;
            m.deinit();
        } else |_| {}
    };

    var ex: client.Exchange = undefined;
    const start = sys.monotonicNanos();
    try ex.start(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .method = .get,
        .path = "/healthz",
        .host = "127.0.0.1",
    }, .{}, Collected.cb, &col);
    defer ex.deinit();

    try pumpUntil(&loop, 5000, &col, Collected.done);
    const elapsed = sys.monotonicNanos() - start;

    const resp = try col.result.?;
    try testing.expectEqual(@as(u16, 200), resp.status);
    // Inline, this round trip could not have completed in under 200 ms
    // because the loop would not have run at all. Half that is a wide
    // margin and still an order of magnitude below the stall.
    try testing.expect(elapsed < 100 * std.time.ns_per_ms);

    // And the stage is still owed a completion, which must arrive.
    const Waiter = struct {
        r: *Recorder,
        fn done(w: *const @This()) bool {
            return !w.r.stages.busy();
        }
    };
    var w: Waiter = .{ .r = &rec };
    try pumpUntil(&loop, 10_000, &w, Waiter.done);
    try testing.expectEqual(@as(usize, 1), rec.offloaded);
}

test "a full backlog is refused so the bus keeps the event" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var rec: Recorder = .{ .gpa = gpa };
    defer rec.deinit();
    rec.stages = .{ .gpa = gpa, .loop = &loop, .ctx = @ptrCast(&rec), .bodyFn = &Recorder.bodyFn };
    defer rec.stages.deinit();

    for (0..max_queued) |i| try rec.stages.enqueue(.verify, @intCast(i));
    try testing.expectError(error.Full, rec.stages.enqueue(.verify, 9999));
    try testing.expectEqual(@as(usize, 1), rec.stages.refused);
}

test "shutdown waits for the worker rather than freeing its stack" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var rec: Recorder = .{ .gpa = gpa, .spin_ns = 80 * std.time.ns_per_ms };
    defer rec.deinit();
    rec.stages = .{ .gpa = gpa, .loop = &loop, .ctx = @ptrCast(&rec), .bodyFn = &Recorder.bodyFn };
    try rec.stages.start(.{ .workers = 1 });

    try rec.stages.enqueue(.verify, 1);
    _ = try loop.tick(5);
    _ = try loop.tick(5);

    // Tearing down while a worker is inside the body. If `deinit` freed
    // the fiber first the worker would write through a `munmap`'d
    // pointer, which is a crash rather than a failing assertion — so this
    // test passing at all is the assertion.
    rec.stages.deinit();
    try testing.expectEqual(@as(usize, 1), rec.seen.items.len);
}
