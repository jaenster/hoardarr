//! A worker pool for the work the reactor thread must not do.
//!
//! ## Why this exists
//!
//! The loop thread is allowed to do two things: syscalls that cannot
//! block, and arithmetic that finishes in microseconds. Everything else
//! it does is time during which the HTTP surface answers nothing and no
//! download makes progress. Two jobs break that rule badly — PAR2
//! verification hashes every byte of a release, and RAR extraction
//! decompresses it — and on a 40 GB release "badly" means minutes.
//!
//! Neither of those is asynchronous work waiting on a descriptor, so a
//! fiber does not help: there is nothing to park on. They are CPU, and
//! CPU belongs on a thread.
//!
//! ## The constraint this pool is built around
//!
//! The project's headline number is 0.000% CPU while idle — one thread,
//! parked in one syscall. A pool that spins, or that polls a queue on a
//! timer, would destroy it. So:
//!
//!   * A worker with nothing to do is blocked in `futex`/`__ulock_wait`.
//!     Not spinning, not sleeping on a timeout, not waking to look. A
//!     parked thread costs exactly nothing.
//!   * The hand-off back to the loop is one write to one fd, coalesced
//!     behind an atomic flag the same way `Loop.wake` coalesces its own.
//!     A burst of finished tasks is one wakeup.
//!
//! `test "an idle pool consumes no measurable CPU"` asserts it rather
//! than describing it.
//!
//! ## What may run on a worker
//!
//! **Pure CPU and blocking filesystem syscalls. Nothing else.** The
//! database connection, the SSE hubs, the aggregates, every `*Conn` and
//! the reactor itself are single-threaded and have no locks, because
//! that is the design. A `Task.run` that touches any of them is a data
//! race, not a slow path.
//!
//! Which is why `Task.complete` is delivered on the **loop thread**,
//! from the pool's own reactor source: the result crosses back before
//! anything in the single-threaded world is touched. A worker never
//! calls a completion.
//!
//! ## Why not `core/log.zig`'s `Mutex`
//!
//! It is the same three-state futex lock, and reusing it would be the
//! right instinct anywhere else — but `core` imports `posix`, and this
//! file is `posix`. The primitive is fifty lines; the import cycle is
//! forever.

const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");
const reactor = @import("reactor.zig");

const Allocator = std.mem.Allocator;
const Fd = sys.Fd;
const linux = std.os.linux;

/// How a task ended. Handed to `Task.complete` so a caller can tell
/// "your work ran" from "the pool shut down before it did" without a
/// side channel.
pub const Status = enum {
    /// `run` was called and returned.
    done,
    /// The pool shut down while the task was still queued. `run` was
    /// never called.
    canceled,
};

/// One unit of work. Intrusive: the caller owns the storage and the
/// pool borrows a `next` pointer out of it, so submitting allocates
/// nothing and a queue full of work costs no memory beyond the tasks
/// themselves.
///
/// The storage must stay alive until `complete` has been called.
pub const Task = struct {
    /// Runs on a worker thread. See the module comment for what is and
    /// is not allowed in here. It may not fail: a task that can fail
    /// records its own error in the enclosing struct and lets
    /// `complete` read it.
    run: *const fn (t: *Task) void,

    /// Runs on the **loop thread** after `run` returns, or with
    /// `.canceled` if the pool shut down first. This is where the
    /// result is allowed to re-enter the single-threaded world.
    ///
    /// Free the task from here if it was heap-allocated — the pool has
    /// no further reference to it.
    complete: *const fn (t: *Task, status: Status) void,

    /// Owned by the pool between `submit` and `complete`.
    next: ?*Task = null,
    status: Status = .done,
};

pub const Options = struct {
    /// Zero means "pick one": `min(4, cpu_count)`.
    ///
    /// Four is the ceiling because the target is a NAS whose CPU is also
    /// running the rest of somebody's containers. Hashing is memory-bound
    /// well before it is core-bound, and a pool sized to a 16-thread
    /// desktop would make the box unusable for the eight minutes it takes
    /// to verify a large release.
    workers: usize = 0,

    /// Queue ceiling. Submitting past it fails rather than growing:
    /// unbounded queueing turns a burst into an out-of-memory kill, and
    /// a caller that is told "full" can defer, which is a strictly
    /// better outcome than the kernel choosing for us.
    queue_capacity: usize = 256,

    /// Hard ceiling on the auto-sized worker count.
    pub const default_max_workers: usize = 4;
};

pub const SubmitError = error{
    /// `queue_capacity` reached.
    QueueFull,
    /// `deinit` has begun. Nothing new is accepted.
    Stopped,
};

pub const InitError = Allocator.Error || sys.Error || std.Thread.SpawnError;

/// A fixed set of worker threads plus the fd that carries their results
/// back to the loop.
///
/// Initialise in place: the loop stores `&self.source` and the workers
/// are handed `self`, so a by-value return would hand both a pointer
/// into a dead temporary.
pub const Pool = struct {
    gpa: Allocator,
    loop: *reactor.Loop,

    threads: []std.Thread = &.{},

    /// Guards both queues and the counters. Held for pointer swaps
    /// only — never across `run`, never across a completion.
    mu: Mutex = .{},

    /// Submission FIFO. Head is the next task to run.
    q_head: ?*Task = null,
    q_tail: ?*Task = null,
    queued: usize = 0,
    capacity: usize,

    /// Tasks whose `run` has returned, waiting for the loop thread to
    /// call their completions.
    done_head: ?*Task = null,
    done_tail: ?*Task = null,

    /// Currently inside `Task.run` on some worker.
    in_flight: usize = 0,

    /// Bumped under `mu` on every state change a parked worker cares
    /// about (a push, or `stopping`). A worker reads it while holding
    /// the lock and then waits on that exact value, which is what makes
    /// the wakeup impossible to lose: any change between the read and
    /// the wait has already moved the counter, so the wait returns at
    /// once instead of sleeping through it.
    seq: std.atomic.Value(u32) = .init(0),

    stopping: std.atomic.Value(bool) = .init(false),

    /// Worker -> loop. An `eventfd` on Linux, a self-pipe elsewhere,
    /// exactly as `reactor.Loop` does it and for the same reason.
    wake_read: Fd = sys.invalid_fd,
    wake_write: Fd = sys.invalid_fd,
    source: reactor.Source = undefined,
    registered: bool = false,

    /// Set before a worker writes, cleared after the loop drains. A
    /// hundred tasks finishing at once cost one write and one wakeup.
    wake_pending: std.atomic.Value(bool) = .init(false),

    /// Total tasks whose completion has been delivered. Diagnostics
    /// only; loop thread only, so it needs no atomicity.
    completed: usize = 0,

    pub fn init(self: *Pool, gpa: Allocator, loop: *reactor.Loop, options: Options) InitError!void {
        const n = if (options.workers != 0)
            options.workers
        else
            @min(Options.default_max_workers, std.Thread.getCpuCount() catch 1);

        var wr: Fd = undefined;
        var ww: Fd = undefined;
        if (sys.is_linux) {
            const efd = try sys.eventfd();
            wr = efd;
            ww = efd;
        } else {
            const p = try sys.pipe();
            wr = p.read_end;
            ww = p.write_end;
        }
        errdefer {
            sys.close(wr);
            if (ww != wr) sys.close(ww);
        }

        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .capacity = @max(options.queue_capacity, 1),
            .wake_read = wr,
            .wake_write = ww,
            .source = .{
                .fd = wr,
                .interest = .readable,
                .callback = onWakeup,
            },
        };

        const threads = try gpa.alloc(std.Thread, @max(n, 1));
        errdefer gpa.free(threads);
        self.threads = threads[0..0];

        try loop.add(&self.source);
        errdefer loop.remove(&self.source);
        self.registered = true;

        // Grown one at a time so a spawn failure halfway through still
        // has an accurate list to join in `errdefer`.
        for (threads) |*t| {
            t.* = std.Thread.spawn(.{}, workerMain, .{self}) catch |e| {
                self.stopAndJoin();
                return e;
            };
            self.threads = threads[0 .. self.threads.len + 1];
        }
    }

    /// Stop, join, and settle every task deterministically.
    ///
    /// Must run on the loop thread: it delivers the completions that are
    /// still owed, and those are loop-thread callbacks.
    ///
    ///   * Queued but never started -> `complete(.canceled)`.
    ///   * In flight -> the join waits it out, then `complete(.done)`.
    ///   * Finished but not yet delivered -> `complete(.done)`.
    ///
    /// So a caller's task storage is never left with an outstanding
    /// completion, which is what lets `complete` be the one place a
    /// heap-allocated task is freed.
    pub fn deinit(self: *Pool) void {
        self.stopAndJoin();

        if (self.registered) {
            self.loop.remove(&self.source);
            self.registered = false;
        }

        // No worker is running, so the lists are ours without the lock —
        // taken anyway, because "obviously nobody else is looking" is how
        // this class of bug is written.
        self.mu.lock();
        const done = self.done_head;
        const queued = self.q_head;
        self.done_head = null;
        self.done_tail = null;
        self.q_head = null;
        self.q_tail = null;
        self.queued = 0;
        self.mu.unlock();

        var t = done;
        while (t) |task| {
            t = task.next;
            task.next = null;
            self.completed += 1;
            task.complete(task, task.status);
        }
        t = queued;
        while (t) |task| {
            t = task.next;
            task.next = null;
            task.status = .canceled;
            task.complete(task, .canceled);
        }

        self.gpa.free(self.threads.ptr[0..self.threadCapacity()]);
        self.threads = &.{};

        sys.close(self.wake_read);
        if (self.wake_write != self.wake_read) sys.close(self.wake_write);
        self.wake_read = sys.invalid_fd;
        self.wake_write = sys.invalid_fd;
    }

    /// `threads` is narrowed during `init` so a partial spawn can be
    /// joined; the allocation is always the full requested length.
    fn threadCapacity(self: *const Pool) usize {
        return self.threads.len;
    }

    fn stopAndJoin(self: *Pool) void {
        if (self.stopping.swap(true, .release)) {
            // Already stopped; the threads were joined by whoever did it.
            return;
        }
        self.mu.lock();
        _ = self.seq.fetchAdd(1, .monotonic);
        self.mu.unlock();
        futexWakeAll(&self.seq);

        for (self.threads) |t| t.join();
    }

    /// Hand `task` to a worker.
    ///
    /// Call from the loop thread. It is safe from any thread, but the
    /// completion always arrives on the loop's, so a caller that is not
    /// the loop has nowhere useful to receive the answer.
    pub fn submit(self: *Pool, task: *Task) SubmitError!void {
        if (self.stopping.load(.acquire)) return error.Stopped;

        self.mu.lock();
        if (self.stopping.load(.monotonic)) {
            self.mu.unlock();
            return error.Stopped;
        }
        if (self.queued >= self.capacity) {
            self.mu.unlock();
            return error.QueueFull;
        }
        task.next = null;
        task.status = .done;
        if (self.q_tail) |tail| {
            tail.next = task;
        } else {
            self.q_head = task;
        }
        self.q_tail = task;
        self.queued += 1;
        _ = self.seq.fetchAdd(1, .monotonic);
        self.mu.unlock();

        futexWake(&self.seq, 1);
    }

    /// True once `deinit` has begun. A long `run` should check it
    /// between chunks and return early — the join waits for it, and a
    /// shutdown that takes eight minutes is a shutdown nobody waits for.
    pub fn isStopping(self: *const Pool) bool {
        return self.stopping.load(.acquire);
    }

    pub fn workerCount(self: *const Pool) usize {
        return self.threads.len;
    }

    /// Queued plus in flight. Loop-thread diagnostics; a snapshot, not a
    /// synchronisation primitive.
    pub fn outstanding(self: *Pool) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.queued + self.in_flight;
    }

    // -- worker side ---------------------------------------------------

    fn workerMain(self: *Pool) void {
        while (self.take()) |task| {
            task.run(task);
            self.finish(task);
        }
    }

    /// Block until there is work, or until the pool is stopping.
    ///
    /// The `stopping` check comes *before* the pop on purpose: shutdown
    /// abandons the backlog rather than draining it. A queue holding a
    /// 40 GB verification would otherwise make `deinit` take minutes,
    /// and those tasks are settled as `.canceled` instead — which the
    /// caller can act on, unlike a hang.
    fn take(self: *Pool) ?*Task {
        while (true) {
            self.mu.lock();
            if (self.stopping.load(.monotonic)) {
                self.mu.unlock();
                return null;
            }
            if (self.q_head) |task| {
                self.q_head = task.next;
                if (self.q_head == null) self.q_tail = null;
                self.queued -= 1;
                self.in_flight += 1;
                self.mu.unlock();
                task.next = null;
                return task;
            }
            const observed = self.seq.load(.monotonic);
            self.mu.unlock();

            // Parked here, and this is the whole point of the file: no
            // timeout, no re-check, no cycles until somebody bumps `seq`.
            futexWait(&self.seq, observed);
        }
    }

    fn finish(self: *Pool, task: *Task) void {
        self.mu.lock();
        task.status = .done;
        task.next = null;
        if (self.done_tail) |tail| {
            tail.next = task;
        } else {
            self.done_head = task;
        }
        self.done_tail = task;
        self.in_flight -= 1;
        self.mu.unlock();

        self.signalLoop();
    }

    /// One byte, at most one in flight. Identical in shape to
    /// `Loop.wake`, and deliberately not *using* `Loop.wake`: that fd is
    /// drained by the loop's own internal source, which has nowhere to
    /// dispatch a third party's completions. This pool owns an fd so it
    /// owns a callback.
    fn signalLoop(self: *Pool) void {
        if (self.wake_pending.swap(true, .release)) return;
        const one: u64 = 1;
        const buf: []const u8 = if (sys.is_linux) std.mem.asBytes(&one) else "x";
        _ = sys.write(self.wake_write, buf) catch {};
    }

    // -- loop side -----------------------------------------------------

    fn onWakeup(src: *reactor.Source, ready: reactor.Ready) void {
        if (!ready.read) return;
        const self: *Pool = @fieldParentPtr("source", src);

        var buf: [64]u8 = undefined;
        while (true) {
            _ = sys.read(src.fd, &buf) catch break;
            if (sys.is_linux) break;
        }
        // Cleared before draining, not after: a worker finishing between
        // the drain and the clear would otherwise see a pending flag,
        // skip its write, and leave its completion sitting until the next
        // unrelated wakeup.
        self.wake_pending.store(false, .release);

        self.drainCompletions();
    }

    fn drainCompletions(self: *Pool) void {
        while (true) {
            self.mu.lock();
            const task = self.done_head orelse {
                self.mu.unlock();
                return;
            };
            self.done_head = task.next;
            if (self.done_head == null) self.done_tail = null;
            self.mu.unlock();

            task.next = null;
            self.completed += 1;
            // Outside the lock: a completion is allowed to submit the
            // next task, and holding `mu` across that is a self-deadlock.
            task.complete(task, task.status);
        }
    }
};

// ---------------------------------------------------------------------
// Futex primitives
// ---------------------------------------------------------------------

/// Three-state futex mutex. `unlocked -> locked` is one uncontended
/// compare-exchange and no syscall, which matters because the lock is
/// taken on every submit and every completion.
///
/// A copy of `core/log.zig`'s, for the layering reason in the module
/// comment. `std.Thread.Mutex` does not exist in 0.16 and `std.Io.Mutex`
/// needs an `Io` handle this project deliberately does not have.
pub const Mutex = struct {
    pub const State = enum(u32) { unlocked, locked, contended };

    state: std.atomic.Value(State) = .init(.unlocked),

    const spin_limit = 64;

    pub fn lock(m: *Mutex) void {
        if (m.state.cmpxchgWeak(.unlocked, .locked, .acquire, .monotonic) == null) {
            @branchHint(.likely);
            return;
        }
        m.lockSlow();
    }

    fn lockSlow(m: *Mutex) void {
        @branchHint(.cold);
        var spins: usize = 0;
        while (spins < spin_limit) : (spins += 1) {
            if (m.state.load(.monotonic) == .unlocked and
                m.state.cmpxchgWeak(.unlocked, .locked, .acquire, .monotonic) == null) return;
            std.atomic.spinLoopHint();
        }
        while (m.state.swap(.contended, .acquire) != .unlocked) {
            waitState(&m.state);
        }
    }

    pub fn unlock(m: *Mutex) void {
        switch (m.state.swap(.unlocked, .release)) {
            .unlocked => unreachable, // unlock without lock
            .locked => {},
            .contended => {
                @branchHint(.unlikely);
                wakeState(&m.state);
            },
        }
    }

    fn waitState(v: *std.atomic.Value(State)) void {
        if (sys.is_linux) {
            _ = linux.futex_4arg(
                &v.raw,
                .{ .cmd = .WAIT, .private = true },
                @intFromEnum(State.contended),
                null,
            );
        } else {
            _ = std.c.__ulock_wait(
                .{ .op = .COMPARE_AND_WAIT, .NO_ERRNO = true },
                &v.raw,
                @intFromEnum(State.contended),
                0,
            );
        }
    }

    fn wakeState(v: *std.atomic.Value(State)) void {
        if (sys.is_linux) {
            _ = linux.futex_3arg(&v.raw, .{ .cmd = .WAKE, .private = true }, 1);
        } else {
            _ = std.c.__ulock_wake(.{ .op = .COMPARE_AND_WAIT, .NO_ERRNO = true }, &v.raw, 0);
        }
    }
};

/// Sleep until `*v` differs from `expect`, or somebody wakes us.
/// Spurious returns are allowed and the caller re-checks — which is why
/// every call site is inside a `while (true)`.
fn futexWait(v: *std.atomic.Value(u32), expect: u32) void {
    if (sys.is_linux) {
        _ = linux.futex_4arg(&v.raw, .{ .cmd = .WAIT, .private = true }, expect, null);
    } else {
        _ = std.c.__ulock_wait(
            .{ .op = .COMPARE_AND_WAIT, .NO_ERRNO = true },
            &v.raw,
            expect,
            0,
        );
    }
}

fn futexWake(v: *std.atomic.Value(u32), n: u32) void {
    if (sys.is_linux) {
        _ = linux.futex_3arg(&v.raw, .{ .cmd = .WAKE, .private = true }, n);
    } else {
        _ = std.c.__ulock_wake(.{ .op = .COMPARE_AND_WAIT, .NO_ERRNO = true }, &v.raw, 0);
    }
}

fn futexWakeAll(v: *std.atomic.Value(u32)) void {
    if (sys.is_linux) {
        _ = linux.futex_3arg(&v.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
    } else {
        _ = std.c.__ulock_wake(
            .{ .op = .COMPARE_AND_WAIT, .WAKE_ALL = true, .NO_ERRNO = true },
            &v.raw,
            0,
        );
    }
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Process CPU time (user + system) in nanoseconds. Same measurement
/// `reactor.zig`'s idle test uses, so the two numbers are comparable.
fn cpuNanos() u64 {
    const ru = std.posix.getrusage(0); // RUSAGE_SELF
    const u = @as(u64, @intCast(ru.utime.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ru.utime.usec)) * std.time.ns_per_us;
    const s = @as(u64, @intCast(ru.stime.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ru.stime.usec)) * std.time.ns_per_us;
    return u + s;
}

/// Drive the loop until `done` or the deadline, whichever comes first.
fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

const Counter = struct {
    task: Task = .{ .run = runFn, .complete = completeFn },
    ran_on: std.Thread.Id = 0,
    completed_on: std.Thread.Id = 0,
    ran: bool = false,
    finished: bool = false,
    status: Status = .done,
    spin_ns: u64 = 0,

    fn runFn(t: *Task) void {
        const self: *Counter = @fieldParentPtr("task", t);
        self.ran_on = std.Thread.getCurrentId();
        if (self.spin_ns != 0) {
            const until = sys.monotonicNanos() + self.spin_ns;
            while (sys.monotonicNanos() < until) {}
        }
        self.ran = true;
    }

    fn completeFn(t: *Task, status: Status) void {
        const self: *Counter = @fieldParentPtr("task", t);
        self.completed_on = std.Thread.getCurrentId();
        self.status = status;
        self.finished = true;
    }

    fn isFinished(self: *Counter) bool {
        return self.finished;
    }
};

test "work runs off the loop thread and the completion runs on it" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{ .workers = 2 });
    defer pool.deinit();

    var c: Counter = .{};
    try pool.submit(&c.task);

    try pumpUntil(&loop, 3000, &c, Counter.isFinished);

    try testing.expect(c.ran);
    try testing.expectEqual(Status.done, c.status);
    // The property the single-threaded world depends on: the body ran
    // somewhere else, the completion ran here.
    try testing.expect(c.ran_on != std.Thread.getCurrentId());
    try testing.expectEqual(std.Thread.getCurrentId(), c.completed_on);
}

test "every task of a batch completes exactly once" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{ .workers = 4 });
    defer pool.deinit();

    const n = 200;
    const items = try gpa.alloc(Counter, n);
    defer gpa.free(items);
    for (items) |*c| c.* = .{};

    for (items) |*c| try pool.submit(&c.task);

    const Waiter = struct {
        p: *Pool,
        want: usize,
        fn done(w: *const @This()) bool {
            return w.p.completed >= w.want;
        }
    };
    var w: Waiter = .{ .p = &pool, .want = n };
    try pumpUntil(&loop, 10_000, &w, Waiter.done);

    for (items) |*c| {
        try testing.expect(c.ran);
        try testing.expect(c.finished);
        try testing.expectEqual(Status.done, c.status);
    }
    try testing.expectEqual(@as(usize, 0), pool.outstanding());
}

test "a full queue is refused rather than grown" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // One worker, capacity two: the third submission has nowhere to go.
    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{ .workers = 1, .queue_capacity = 2 });
    defer pool.deinit();

    // A blocker that holds the single worker until we release it.
    const Blocker = struct {
        task: Task = .{ .run = runFn, .complete = completeFn },
        started: std.atomic.Value(bool) = .init(false),
        gate: std.atomic.Value(bool) = .init(false),
        finished: bool = false,

        fn runFn(t: *Task) void {
            const self: *@This() = @fieldParentPtr("task", t);
            self.started.store(true, .release);
            while (!self.gate.load(.acquire)) sys.sleep(std.time.ns_per_ms);
        }
        fn completeFn(t: *Task, _: Status) void {
            const self: *@This() = @fieldParentPtr("task", t);
            self.finished = true;
        }
        fn done(self: *@This()) bool {
            return self.finished;
        }
    };

    var blocker: Blocker = .{};
    try pool.submit(&blocker.task);
    // Wait until the worker is actually inside the blocker, so the
    // capacity being tested is the queue's rather than the
    // queue-plus-worker's.
    while (!blocker.started.load(.acquire)) sys.sleep(std.time.ns_per_ms);

    var a: Counter = .{};
    var b: Counter = .{};
    var c: Counter = .{};
    try pool.submit(&a.task);
    try pool.submit(&b.task);
    try testing.expectError(error.QueueFull, pool.submit(&c.task));

    blocker.gate.store(true, .release);
    try pumpUntil(&loop, 5000, &b, Counter.isFinished);
    try pumpUntil(&loop, 5000, &blocker, Blocker.done);
}

test "shutdown cancels the backlog and lets in-flight work finish" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{ .workers = 1, .queue_capacity = 16 });

    const Slow = struct {
        task: Task = .{ .run = runFn, .complete = completeFn },
        started: std.atomic.Value(bool) = .init(false),
        ran: bool = false,
        status: Status = .canceled,
        finished: bool = false,

        fn runFn(t: *Task) void {
            const self: *@This() = @fieldParentPtr("task", t);
            self.started.store(true, .release);
            sys.sleep(60 * std.time.ns_per_ms);
            self.ran = true;
        }
        fn completeFn(t: *Task, status: Status) void {
            const self: *@This() = @fieldParentPtr("task", t);
            self.status = status;
            self.finished = true;
        }
    };

    var slow: Slow = .{};
    var queued: [4]Counter = @splat(.{});
    try pool.submit(&slow.task);
    while (!slow.started.load(.acquire)) sys.sleep(std.time.ns_per_ms);
    for (&queued) |*c| try pool.submit(&c.task);

    // Nothing new is taken once shutdown starts.
    var late: Counter = .{};
    pool.deinit();
    try testing.expectError(error.Stopped, pool.submit(&late.task));

    // In flight: ran to completion, settled as done.
    try testing.expect(slow.ran);
    try testing.expect(slow.finished);
    try testing.expectEqual(Status.done, slow.status);

    // Queued: never ran, settled as cancelled. Both halves matter — a
    // task that is neither run nor settled is a leak the caller cannot
    // see.
    for (&queued) |*c| {
        try testing.expect(!c.ran);
        try testing.expect(c.finished);
        try testing.expectEqual(Status.canceled, c.status);
    }
}

test "the loop keeps servicing its sources while a worker burns CPU" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{ .workers = 2 });
    defer pool.deinit();

    // A pipe standing in for any fd the loop owes an answer to.
    const Probe = struct {
        source: reactor.Source,
        pipe: sys.Pipe,
        reads: usize = 0,

        fn onReady(src: *reactor.Source, ready: reactor.Ready) void {
            const self: *@This() = @fieldParentPtr("source", src);
            if (!ready.read) return;
            var buf: [64]u8 = undefined;
            const n = sys.read(src.fd, &buf) catch 0;
            if (n > 0) self.reads += 1;
        }
    };

    const p = try sys.pipe();
    var probe: Probe = .{
        .source = .{ .fd = p.read_end, .interest = .readable, .callback = Probe.onReady },
        .pipe = p,
    };
    try loop.add(&probe.source);
    defer {
        loop.remove(&probe.source);
        sys.close(p.read_end);
        sys.close(p.write_end);
    }

    // 150 ms of solid CPU. Run inline on the loop thread this would be
    // 150 ms during which the probe is not answered at all — which is
    // exactly the symptom a large release produces today.
    var hog: Counter = .{ .spin_ns = 150 * std.time.ns_per_ms };
    try pool.submit(&hog.task);

    var served: usize = 0;
    while (!hog.finished and served < 20) {
        _ = try sys.write(p.write_end, "x");
        _ = try loop.tick(20);
        served = probe.reads;
    }
    try testing.expect(served >= 5);

    try pumpUntil(&loop, 5000, &hog, Counter.isFinished);
    try testing.expect(hog.ran);
}

test "an idle pool consumes no measurable CPU" {
    // The number this whole project is measured on. A pool that spins,
    // or that wakes on a timeout to look at its queue, shows up here as
    // CPU time proportional to wall time — which is the regression this
    // test exists to catch. Parked threads show microseconds.
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{ .workers = 4 });
    defer pool.deinit();

    // One task first, so the threads have been through the whole
    // take/run/park cycle: a pool that only ever sat in its initial park
    // would prove less than one that has actually been used.
    var warm: Counter = .{};
    try pool.submit(&warm.task);
    try pumpUntil(&loop, 3000, &warm, Counter.isFinished);

    const before = cpuNanos();
    const wall_start = sys.monotonicNanos();
    const idle_ns: u64 = std.time.ns_per_s;
    // The loop parks too — this measures the daemon at rest, workers and
    // reactor together, which is what the deployment number reports.
    while (sys.monotonicNanos() - wall_start < idle_ns) {
        _ = try loop.tick(@intCast((idle_ns - (sys.monotonicNanos() - wall_start)) / std.time.ns_per_ms + 1));
    }
    const wall = sys.monotonicNanos() - wall_start;
    const cpu = cpuNanos() - before;

    try testing.expect(wall >= 900 * std.time.ns_per_ms);
    // 1% of wall is three orders of magnitude above what parked threads
    // actually cost and still four orders below a spin.
    if (cpu >= wall / 100) {
        std.debug.print(
            "idle pool burned {d} ns of CPU over {d} ns of wall\n",
            .{ cpu, wall },
        );
        return error.IdlePoolBurnedCpu;
    }
}

test "auto sizing stays within the NAS ceiling" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var pool: Pool = undefined;
    try pool.init(gpa, &loop, .{});
    defer pool.deinit();

    try testing.expect(pool.workerCount() >= 1);
    try testing.expect(pool.workerCount() <= Options.default_max_workers);
}
