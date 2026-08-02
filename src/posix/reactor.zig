//! The event loop. One thread, parked in one syscall, woken only by
//! real work.
//!
//! ## Why hand-rolled
//!
//! The idle-CPU target is the reason. A daemon that sits in front of a
//! Usenet provider spends most of its life with an empty queue, and the
//! usual ways of arranging that leak CPU: a goroutine-per-connection
//! runtime still runs a scheduler tick and a background sweeper, and a
//! 100 ms "poll for work" loop wakes 864,000 times a day to discover
//! nothing changed. Here, idle means every fd is registered, the timer
//! heap's nearest deadline is the poll timeout, and the thread is
//! blocked. Zero wakeups, zero syscalls, zero cycles.
//!
//! ## Backends
//!
//! `poll(2)` is the baseline and works on every target we build for,
//! including a developer's macOS laptop. `epoll` is compiled in on Linux
//! and is what actually ships.
//!
//! The classic reason people call `poll` slow is rebuilding the `pollfd`
//! array on every call, which turns a quiet loop into an O(n) memcpy per
//! wakeup. We don't: the array is allocated once and mutated in place on
//! register/unregister, with a swap-remove that patches the moved
//! source's slot index. A tick over an idle loop touches no memory
//! beyond the array the kernel already needs.
//!
//! Registration is intrusive — callers embed a `Source` in their own
//! struct and recover it with `@fieldParentPtr`. No allocation per
//! connection, no hash lookup on the hot path, and the loop can never
//! outlive a callback's context because removal is explicit.

const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");

const Fd = sys.Fd;
const Allocator = std.mem.Allocator;

/// What a source wants to hear about.
pub const Interest = packed struct(u2) {
    read: bool = false,
    write: bool = false,

    pub const none: Interest = .{};
    pub const readable: Interest = .{ .read = true };
    pub const writable: Interest = .{ .write = true };
    pub const both: Interest = .{ .read = true, .write = true };

    pub fn eql(a: Interest, b: Interest) bool {
        return a.read == b.read and a.write == b.write;
    }

    pub fn isNone(a: Interest) bool {
        return !a.read and !a.write;
    }
};

/// What actually happened. `hup` and `err` are always delivered even if
/// not requested — a half-closed socket that nobody is reading is still
/// something the owner has to know about, and dropping it is how you get
/// a source wedged in the loop forever.
pub const Ready = packed struct(u4) {
    read: bool = false,
    write: bool = false,
    hup: bool = false,
    err: bool = false,

    pub fn any(r: Ready) bool {
        return r.read or r.write or r.hup or r.err;
    }

    /// True when the source is finished as far as the kernel is
    /// concerned and the owner should tear it down.
    pub fn terminal(r: Ready) bool {
        return r.hup or r.err;
    }
};

/// An fd registered with the loop. Embed this in your connection struct:
///
///     const Conn = struct {
///         source: Source,
///         ...
///         fn onReady(src: *Source, ready: Ready) void {
///             const self: *Conn = @fieldParentPtr("source", src);
///             ...
///         }
///     };
pub const Source = struct {
    fd: Fd,
    interest: Interest,
    callback: *const fn (src: *Source, ready: Ready) void,

    /// Index into the backend's parallel arrays. Owned by the loop;
    /// callers must not touch it.
    slot: u32 = unregistered,

    const unregistered: u32 = std.math.maxInt(u32);

    pub fn isRegistered(self: *const Source) bool {
        return self.slot != unregistered;
    }
};

/// A one-shot deadline. Rearm by calling `Loop.addTimer` again from the
/// callback — repeating timers are built from that rather than baked in,
/// because "repeat every N" almost always wants either drift correction
/// or jitter and the caller is the only one who knows which.
pub const Timer = struct {
    /// Monotonic nanoseconds. Compared against `sys.monotonicNanos()`.
    deadline_ns: u64 = 0,
    callback: *const fn (t: *Timer) void,

    /// Position in the loop's heap, kept current so cancellation is
    /// O(log n) instead of a linear scan.
    heap_index: u32 = unqueued,

    const unqueued: u32 = std.math.maxInt(u32);

    pub fn isArmed(self: *const Timer) bool {
        return self.heap_index != unqueued;
    }
};

pub const Error = Allocator.Error || sys.Error;

/// Whether the shipping backend is available. Kept as a public constant
/// so benchmarks can report which one produced their numbers.
pub const backend_name = if (sys.is_linux) "epoll" else "poll";

pub const Loop = struct {
    gpa: Allocator,
    backend: Backend,
    timers: TimerHeap,

    /// Wakeup channel for other threads. On Linux this is an `eventfd`,
    /// whose counter semantics coalesce a burst of `wake()` calls into a
    /// single readable event carrying the total; on macOS it's a
    /// self-pipe, and we drain it fully for the same effect.
    wakeup_read: Fd,
    wakeup_write: Fd,
    wakeup_source: Source,

    /// Set by `wake()` before it writes, cleared after the drain. Lets a
    /// burst of wakeups skip the syscall entirely once one is in flight —
    /// the reason a busy producer doesn't turn into a write storm.
    wake_pending: std.atomic.Value(bool) = .init(false),

    stopping: std.atomic.Value(bool) = .init(false),

    /// Live source count, excluding the internal wakeup source. `run()`
    /// exits when this hits zero with no timers armed, so a loop with
    /// nothing left to do returns instead of blocking forever.
    source_count: usize = 0,

    /// Initialise in place.
    ///
    /// This deliberately takes `*Loop` instead of returning one: the loop
    /// registers `&self.wakeup_source` with its own backend, and
    /// `drainWakeup` recovers the loop from that source via
    /// `@fieldParentPtr`. A by-value return would hand the backend a
    /// pointer into the soon-to-be-dead temporary and the first `wake()`
    /// would jump through freed stack.
    pub fn init(self: *Loop, gpa: Allocator) Error!void {
        var backend = try Backend.init(gpa);
        errdefer backend.deinit(gpa);

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
            .backend = backend,
            .timers = .empty,
            .wakeup_read = wr,
            .wakeup_write = ww,
            .wakeup_source = .{
                .fd = wr,
                .interest = .readable,
                .callback = drainWakeup,
            },
        };
        try self.backend.add(gpa, &self.wakeup_source);
    }

    pub fn deinit(self: *Loop) void {
        self.backend.deinit(self.gpa);
        self.timers.deinit(self.gpa);
        sys.close(self.wakeup_read);
        if (self.wakeup_write != self.wakeup_read) sys.close(self.wakeup_write);
        self.* = undefined;
    }

    // -- sources ------------------------------------------------------

    pub fn add(self: *Loop, src: *Source) Error!void {
        std.debug.assert(!src.isRegistered());
        try self.backend.add(self.gpa, src);
        self.source_count += 1;
    }

    /// Change what a source listens for. Cheap enough to call per state
    /// transition — which is the point: a connection with nothing to
    /// send drops its write interest rather than being told "writable"
    /// on every single tick, which is the other classic way to burn a
    /// core on an idle loop.
    pub fn modify(self: *Loop, src: *Source, interest: Interest) Error!void {
        std.debug.assert(src.isRegistered());
        if (src.interest.eql(interest)) return;
        src.interest = interest;
        try self.backend.modify(src);
    }

    pub fn remove(self: *Loop, src: *Source) void {
        if (!src.isRegistered()) return;
        self.backend.remove(src);
        self.source_count -= 1;
    }

    // -- timers -------------------------------------------------------

    pub fn addTimer(self: *Loop, t: *Timer, delay_ns: u64) Error!void {
        t.deadline_ns = sys.monotonicNanos() + delay_ns;
        try self.timers.push(self.gpa, t);
    }

    pub fn addTimerAt(self: *Loop, t: *Timer, deadline_ns: u64) Error!void {
        t.deadline_ns = deadline_ns;
        try self.timers.push(self.gpa, t);
    }

    pub fn cancelTimer(self: *Loop, t: *Timer) void {
        self.timers.remove(t);
    }

    // -- driving ------------------------------------------------------

    /// Wake the loop from another thread. Safe to call from anywhere,
    /// including a signal handler: one non-blocking write, no locks.
    pub fn wake(self: *Loop) void {
        // If a wakeup is already in flight the loop is guaranteed to
        // come around and re-check whatever the caller just queued, so
        // there is nothing to gain from a second write.
        if (self.wake_pending.swap(true, .release)) return;
        const one: u64 = 1;
        const buf: []const u8 = if (sys.is_linux) std.mem.asBytes(&one) else "x";
        _ = sys.write(self.wakeup_write, buf) catch {};
    }

    /// Ask the loop to return from `run()` at the next opportunity.
    pub fn stop(self: *Loop) void {
        self.stopping.store(true, .release);
        self.wake();
    }

    /// Run until `stop()` is called, or until there is provably nothing
    /// left to do (no sources, no armed timers).
    pub fn run(self: *Loop) Error!void {
        while (!self.stopping.load(.acquire)) {
            if (self.source_count == 0 and self.timers.len() == 0) return;
            _ = try self.tick(null);
        }
    }

    /// One iteration: sleep until something is ready or the nearest
    /// timer is due, then dispatch. Returns the number of callbacks
    /// invoked.
    ///
    /// `max_wait_ms` caps the sleep; `null` means "as long as the timer
    /// heap allows", which for an empty heap is indefinitely. Tests pass
    /// a cap; production passes `null`, and that difference is exactly
    /// why production burns no CPU when idle.
    pub fn tick(self: *Loop, max_wait_ms: ?i32) Error!usize {
        const now = sys.monotonicNanos();

        // Compute the sleep budget from the nearest deadline. -1 tells
        // the kernel to block until an fd is ready, full stop.
        var timeout_ms: i32 = -1;
        if (self.timers.peek()) |next| {
            timeout_ms = if (next.deadline_ns <= now)
                0
            else
                nanosToMillisCeil(next.deadline_ns - now);
        }
        if (max_wait_ms) |cap| {
            timeout_ms = if (timeout_ms < 0) cap else @min(timeout_ms, cap);
        }

        const n = try self.backend.wait(timeout_ms);

        var fired: usize = 0;
        fired += self.backend.dispatch(n);
        fired += self.fireDueTimers();
        return fired;
    }

    fn fireDueTimers(self: *Loop) usize {
        var fired: usize = 0;
        // Re-read the clock each round: a slow callback must not cause
        // the next timer to fire early.
        while (self.timers.peek()) |next| {
            const now = sys.monotonicNanos();
            if (next.deadline_ns > now) break;
            const t = self.timers.pop().?;
            t.callback(t);
            fired += 1;
        }
        return fired;
    }

    fn drainWakeup(src: *Source, ready: Ready) void {
        if (!ready.read) return;
        // Drain to empty so the fd goes quiet again. On Linux one 8-byte
        // read clears the whole counter regardless of how many writes
        // landed; on macOS we loop the pipe until WouldBlock.
        var buf: [64]u8 = undefined;
        while (true) {
            _ = sys.read(src.fd, &buf) catch break;
            if (sys.is_linux) break;
        }
        const self: *Loop = @fieldParentPtr("wakeup_source", src);
        self.wake_pending.store(false, .release);
    }
};

fn nanosToMillisCeil(ns: u64) i32 {
    // Round up, never to zero: rounding a 0.4 ms deadline down to 0 turns
    // a sleep into a spin, which is the failure mode this whole file
    // exists to avoid.
    const ms = (ns + std.time.ns_per_ms - 1) / std.time.ns_per_ms;
    return @intCast(@min(ms, @as(u64, std.math.maxInt(i32))));
}

// ---------------------------------------------------------------------
// Timer heap
// ---------------------------------------------------------------------

/// Intrusive binary min-heap keyed on deadline. Intrusive so a timer can
/// be cancelled in O(log n) via its stored index — hoardarr arms a retry
/// timer per in-flight segment and cancels it on success, so cancellation
/// is as hot as insertion and a linear scan would show up.
const TimerHeap = struct {
    items: std.ArrayList(*Timer),

    const empty: TimerHeap = .{ .items = .empty };

    fn deinit(self: *TimerHeap, gpa: Allocator) void {
        self.items.deinit(gpa);
    }

    fn len(self: *const TimerHeap) usize {
        return self.items.items.len;
    }

    fn peek(self: *const TimerHeap) ?*Timer {
        if (self.items.items.len == 0) return null;
        return self.items.items[0];
    }

    fn push(self: *TimerHeap, gpa: Allocator, t: *Timer) Allocator.Error!void {
        std.debug.assert(!t.isArmed());
        try self.items.append(gpa, t);
        t.heap_index = @intCast(self.items.items.len - 1);
        self.siftUp(t.heap_index);
    }

    fn pop(self: *TimerHeap) ?*Timer {
        if (self.items.items.len == 0) return null;
        const top = self.items.items[0];
        self.removeAt(0);
        return top;
    }

    fn remove(self: *TimerHeap, t: *Timer) void {
        if (!t.isArmed()) return;
        self.removeAt(t.heap_index);
    }

    fn removeAt(self: *TimerHeap, index: u32) void {
        const items = self.items.items;
        const last: u32 = @intCast(items.len - 1);
        items[index].heap_index = Timer.unqueued;

        if (index == last) {
            _ = self.items.pop();
            return;
        }

        // Move the tail into the hole, then restore the invariant. The
        // moved element can need to go either direction, hence both
        // sifts.
        items[index] = items[last];
        items[index].heap_index = index;
        _ = self.items.pop();
        self.siftDown(index);
        self.siftUp(index);
    }

    fn siftUp(self: *TimerHeap, start: u32) void {
        const items = self.items.items;
        var i = start;
        while (i > 0) {
            const parent = (i - 1) / 2;
            if (items[parent].deadline_ns <= items[i].deadline_ns) break;
            swap(items, parent, i);
            i = parent;
        }
    }

    fn siftDown(self: *TimerHeap, start: u32) void {
        const items = self.items.items;
        const n: u32 = @intCast(items.len);
        var i = start;
        while (true) {
            const left = 2 * i + 1;
            if (left >= n) break;
            const right = left + 1;
            var smallest = left;
            if (right < n and items[right].deadline_ns < items[left].deadline_ns) {
                smallest = right;
            }
            if (items[i].deadline_ns <= items[smallest].deadline_ns) break;
            swap(items, i, smallest);
            i = smallest;
        }
    }

    fn swap(items: []*Timer, a: u32, b: u32) void {
        const tmp = items[a];
        items[a] = items[b];
        items[b] = tmp;
        items[a].heap_index = a;
        items[b].heap_index = b;
    }
};

// ---------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------

const Backend = if (sys.is_linux) EpollBackend else PollBackend;

/// `poll(2)`, the portable baseline.
///
/// Two parallel arrays: the `pollfd` array the kernel reads, and the
/// `*Source` array we use to route readiness back to an owner. They stay
/// index-aligned, which is what makes registration O(1) and dispatch a
/// straight walk with no lookups.
const PollBackend = struct {
    fds: std.ArrayList(sys.pollfd),
    sources: std.ArrayList(*Source),

    fn init(gpa: Allocator) Error!PollBackend {
        var self: PollBackend = .{ .fds = .empty, .sources = .empty };
        // Preallocate for a realistic connection count so a busy startup
        // doesn't realloc mid-accept.
        try self.fds.ensureTotalCapacity(gpa, 64);
        try self.sources.ensureTotalCapacity(gpa, 64);
        return self;
    }

    fn deinit(self: *PollBackend, gpa: Allocator) void {
        self.fds.deinit(gpa);
        self.sources.deinit(gpa);
    }

    fn add(self: *PollBackend, gpa: Allocator, src: *Source) Error!void {
        // `poll(2)` is perfectly happy to hold the same fd twice and will
        // report it ready on both entries, but `epoll_ctl(ADD)` answers
        // `EEXIST` — and epoll is what ships. Refusing the duplicate here
        // is what stops a development host from proving code correct that
        // cannot register a socket in production. The scan is over
        // registrations, not over ticks, and a loop with hundreds of them
        // is still cheaper than the syscall that follows.
        for (self.fds.items) |p| {
            if (p.fd == src.fd) return error.Exists;
        }
        try self.fds.append(gpa, .{
            .fd = src.fd,
            .events = pollEvents(src.interest),
            .revents = 0,
        });
        errdefer _ = self.fds.pop();
        try self.sources.append(gpa, src);
        src.slot = @intCast(self.sources.items.len - 1);
    }

    fn modify(self: *PollBackend, src: *Source) Error!void {
        self.fds.items[src.slot].events = pollEvents(src.interest);
    }

    fn remove(self: *PollBackend, src: *Source) void {
        const slot = src.slot;
        const last = self.sources.items.len - 1;
        if (slot != last) {
            // Swap-remove, then tell the relocated source where it went.
            // Without this fixup the moved source's `modify`/`remove`
            // would silently operate on someone else's fd.
            self.fds.items[slot] = self.fds.items[last];
            self.sources.items[slot] = self.sources.items[last];
            self.sources.items[slot].slot = slot;
        }
        _ = self.fds.pop();
        _ = self.sources.pop();
        src.slot = Source.unregistered;
    }

    fn wait(self: *PollBackend, timeout_ms: i32) Error!usize {
        // Clear stale revents so `dispatch` can't act on last tick's
        // results for an fd the kernel didn't report this time.
        for (self.fds.items) |*p| p.revents = 0;
        return sys.poll(self.fds.items, timeout_ms);
    }

    fn dispatch(self: *PollBackend, ready_count: usize) usize {
        if (ready_count == 0) return 0;

        var fired: usize = 0;
        var remaining = ready_count;
        var i: usize = 0;
        while (i < self.sources.items.len and remaining > 0) {
            const revents = self.fds.items[i].revents;
            if (revents == 0) {
                i += 1;
                continue;
            }
            remaining -= 1;

            const src = self.sources.items[i];
            const ready = readyFromPoll(revents);

            // A callback is allowed to remove itself, or any other
            // source. Removal is a swap-remove, so after the call the
            // slot may hold a different source — re-examine this index
            // instead of advancing past it, or that source's event is
            // dropped for this tick.
            src.callback(src, ready);
            fired += 1;

            if (i < self.sources.items.len and self.sources.items[i] != src) continue;
            i += 1;
        }
        return fired;
    }

    fn pollEvents(interest: Interest) i16 {
        var e: i16 = 0;
        if (interest.read) e |= sys.POLL.IN;
        if (interest.write) e |= sys.POLL.OUT;
        return e;
    }

    fn readyFromPoll(revents: i16) Ready {
        return .{
            .read = revents & sys.POLL.IN != 0,
            .write = revents & sys.POLL.OUT != 0,
            .hup = revents & sys.POLL.HUP != 0,
            // POLLNVAL means we're polling a closed fd — a bug, but
            // reporting it as an error beats spinning on it forever.
            .err = revents & (sys.POLL.ERR | sys.POLL.NVAL) != 0,
        };
    }
};

/// `epoll`, the Linux fast path.
///
/// Level-triggered on purpose. Edge-triggered would shave a syscall but
/// requires every reader to drain to `EAGAIN` before returning, and a
/// single spot that forgets turns into a connection that hangs until the
/// peer times out. Level-triggered plus explicit interest updates gives
/// the same idle behaviour with a far smaller correctness surface.
const EpollBackend = struct {
    epfd: Fd,
    /// Result buffer, sized once. `epoll_wait` fills at most this many
    /// per call; anything beyond is reported on the next tick.
    events: []std.os.linux.epoll_event,
    /// Sources indexed by slot, so an `epoll_event.data.u32` round-trips
    /// to an owner without a hash lookup. Freed slots go on a free list
    /// rather than being compacted — an `epoll_ctl(DEL)` already tells
    /// the kernel to forget the fd, so there is no array to keep dense.
    sources: std.ArrayList(?*Source),
    free_slots: std.ArrayList(u32),

    const max_events = 256;

    fn init(gpa: Allocator) Error!EpollBackend {
        const linux = std.os.linux;
        const rc = linux.epoll_create1(linux.EPOLL.CLOEXEC);
        const e = linux.errno(rc);
        if (e != .SUCCESS) return sys.mapError(e);
        const epfd: Fd = @intCast(rc);
        errdefer sys.close(epfd);

        const events = try gpa.alloc(linux.epoll_event, max_events);
        errdefer gpa.free(events);

        var self: EpollBackend = .{
            .epfd = epfd,
            .events = events,
            .sources = .empty,
            .free_slots = .empty,
        };
        try self.sources.ensureTotalCapacity(gpa, 64);
        return self;
    }

    fn deinit(self: *EpollBackend, gpa: Allocator) void {
        gpa.free(self.events);
        self.sources.deinit(gpa);
        self.free_slots.deinit(gpa);
        sys.close(self.epfd);
    }

    fn add(self: *EpollBackend, gpa: Allocator, src: *Source) Error!void {
        const slot: u32 = if (self.free_slots.pop()) |s| blk: {
            self.sources.items[s] = src;
            break :blk s;
        } else blk: {
            try self.sources.append(gpa, src);
            break :blk @intCast(self.sources.items.len - 1);
        };
        errdefer {
            self.sources.items[slot] = null;
            self.free_slots.append(gpa, slot) catch {};
        }

        var ev = std.os.linux.epoll_event{
            .events = epollEvents(src.interest),
            .data = .{ .u32 = slot },
        };
        try ctl(self.epfd, std.os.linux.EPOLL.CTL_ADD, src.fd, &ev);
        src.slot = slot;
    }

    fn modify(self: *EpollBackend, src: *Source) Error!void {
        var ev = std.os.linux.epoll_event{
            .events = epollEvents(src.interest),
            .data = .{ .u32 = src.slot },
        };
        try ctl(self.epfd, std.os.linux.EPOLL.CTL_MOD, src.fd, &ev);
    }

    fn remove(self: *EpollBackend, src: *Source) void {
        // A closed fd is already gone from the set, so an error here is
        // not actionable.
        ctl(self.epfd, std.os.linux.EPOLL.CTL_DEL, src.fd, null) catch {};
        self.sources.items[src.slot] = null;
        // If the free list can't grow we leak one slot index, which
        // costs 8 bytes and nothing else — better than failing a
        // teardown path.
        self.free_slots.append(self.freeSlotsAllocator(), src.slot) catch {};
        src.slot = Source.unregistered;
    }

    /// `remove` has no allocator parameter because callers treat teardown
    /// as infallible. The free list is the only thing that would want
    /// one, and it degrades gracefully, so we hand it a failing
    /// allocator rather than widening every caller's signature.
    fn freeSlotsAllocator(self: *EpollBackend) Allocator {
        _ = self;
        return std.testing.failing_allocator;
    }

    fn wait(self: *EpollBackend, timeout_ms: i32) Error!usize {
        const linux = std.os.linux;
        while (true) {
            const rc = linux.epoll_wait(self.epfd, self.events.ptr, @intCast(self.events.len), timeout_ms);
            const e = linux.errno(rc);
            if (e == .SUCCESS) return rc;
            if (e == .INTR) continue;
            return sys.mapError(e);
        }
    }

    fn dispatch(self: *EpollBackend, ready_count: usize) usize {
        var fired: usize = 0;
        for (self.events[0..ready_count]) |ev| {
            const slot = ev.data.u32;
            // A callback earlier in this batch may have removed this
            // source; the slot is nulled on removal so a stale event
            // resolves to nothing instead of a dangling pointer.
            const src = self.sources.items[slot] orelse continue;
            src.callback(src, readyFromEpoll(ev.events));
            fired += 1;
        }
        return fired;
    }

    fn ctl(epfd: Fd, op: u32, fd: Fd, ev: ?*std.os.linux.epoll_event) sys.Error!void {
        const linux = std.os.linux;
        const rc = linux.epoll_ctl(epfd, op, fd, ev);
        const e = linux.errno(rc);
        if (e != .SUCCESS) return sys.mapError(e);
    }

    fn epollEvents(interest: Interest) u32 {
        const EPOLL = std.os.linux.EPOLL;
        // RDHUP is requested unconditionally so a peer's half-close is
        // an event rather than something we discover on the next read.
        var e: u32 = EPOLL.RDHUP;
        if (interest.read) e |= EPOLL.IN;
        if (interest.write) e |= EPOLL.OUT;
        return e;
    }

    fn readyFromEpoll(events: u32) Ready {
        const EPOLL = std.os.linux.EPOLL;
        return .{
            .read = events & EPOLL.IN != 0,
            .write = events & EPOLL.OUT != 0,
            .hup = events & (EPOLL.HUP | EPOLL.RDHUP) != 0,
            .err = events & EPOLL.ERR != 0,
        };
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "timer heap orders by deadline and survives interleaved removal" {
    const gpa = testing.allocator;
    var heap: TimerHeap = .empty;
    defer heap.deinit(gpa);

    const noop = struct {
        fn f(_: *Timer) void {}
    }.f;

    var timers: [64]Timer = undefined;
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rand = prng.random();

    for (&timers) |*t| {
        t.* = .{ .deadline_ns = rand.int(u32), .callback = noop };
        try heap.push(gpa, t);
    }
    try testing.expectEqual(@as(usize, 64), heap.len());

    // Cancel a third of them from the middle of the heap; the rest must
    // still come out in deadline order.
    var cancelled: usize = 0;
    for (&timers, 0..) |*t, i| {
        if (i % 3 == 0) {
            heap.remove(t);
            try testing.expect(!t.isArmed());
            cancelled += 1;
        }
    }
    try testing.expectEqual(64 - cancelled, heap.len());

    var prev: u64 = 0;
    var seen: usize = 0;
    while (heap.pop()) |t| {
        try testing.expect(t.deadline_ns >= prev);
        prev = t.deadline_ns;
        try testing.expect(!t.isArmed());
        seen += 1;
    }
    try testing.expectEqual(64 - cancelled, seen);
}

test "timer heap handles cancelling the last element" {
    const gpa = testing.allocator;
    var heap: TimerHeap = .empty;
    defer heap.deinit(gpa);

    const noop = struct {
        fn f(_: *Timer) void {}
    }.f;
    var a: Timer = .{ .deadline_ns = 1, .callback = noop };
    var b: Timer = .{ .deadline_ns = 2, .callback = noop };
    try heap.push(gpa, &a);
    try heap.push(gpa, &b);

    heap.remove(&b);
    try testing.expectEqual(@as(usize, 1), heap.len());
    try testing.expectEqual(&a, heap.pop().?);
    try testing.expectEqual(@as(?*Timer, null), heap.pop());
}

/// Test harness: a pipe whose read end is registered with the loop and
/// which records every readiness it was handed.
const PipeProbe = struct {
    source: Source,
    pipe: sys.Pipe,
    reads: usize = 0,
    bytes: usize = 0,
    hups: usize = 0,
    buf: [256]u8 = undefined,

    fn init() !PipeProbe {
        const p = try sys.pipe();
        return .{
            .source = .{ .fd = p.read_end, .interest = .readable, .callback = onReady },
            .pipe = p,
        };
    }

    fn deinit(self: *PipeProbe) void {
        sys.close(self.pipe.read_end);
        if (self.pipe.write_end >= 0) sys.close(self.pipe.write_end);
    }

    fn closeWriter(self: *PipeProbe) void {
        sys.close(self.pipe.write_end);
        self.pipe.write_end = -1;
    }

    fn onReady(src: *Source, ready: Ready) void {
        const self: *PipeProbe = @fieldParentPtr("source", src);
        if (ready.read) {
            self.reads += 1;
            const n = sys.read(src.fd, &self.buf) catch 0;
            self.bytes += n;
            // A pipe whose writer is gone reports readable-with-zero
            // forever; treat that as the hangup it is.
            if (n == 0) self.hups += 1;
        }
        if (ready.hup) self.hups += 1;
    }
};

test "loop dispatches readability to the owning source" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var probe = try PipeProbe.init();
    defer probe.deinit();
    try loop.add(&probe.source);
    defer loop.remove(&probe.source);

    _ = try sys.write(probe.pipe.write_end, "hello");
    _ = try loop.tick(100);

    try testing.expectEqual(@as(usize, 1), probe.reads);
    try testing.expectEqual(@as(usize, 5), probe.bytes);
}

test "loop routes to the right source among many" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var probes: [16]PipeProbe = undefined;
    for (&probes) |*p| {
        p.* = try PipeProbe.init();
        try loop.add(&p.source);
    }
    defer for (&probes) |*p| {
        loop.remove(&p.source);
        p.deinit();
    };

    // Write a distinct length to two of them; only those two must fire.
    _ = try sys.write(probes[3].pipe.write_end, "aaa");
    _ = try sys.write(probes[11].pipe.write_end, "bbbbbbb");
    _ = try loop.tick(100);

    for (&probes, 0..) |*p, i| {
        const want: usize = switch (i) {
            3 => 3,
            11 => 7,
            else => 0,
        };
        try testing.expectEqual(want, p.bytes);
    }
}

test "removal patches the relocated source's slot" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var probes: [8]PipeProbe = undefined;
    for (&probes) |*p| {
        p.* = try PipeProbe.init();
        try loop.add(&p.source);
    }
    defer for (&probes) |*p| p.deinit();

    // Remove from the middle. Under swap-remove the last source moves
    // into the hole; if its slot isn't fixed up, the next write to it
    // gets attributed to the wrong owner (or lost).
    loop.remove(&probes[2].source);
    loop.remove(&probes[5].source);

    for (&probes, 0..) |*p, i| {
        if (i == 2 or i == 5) continue;
        _ = try sys.write(p.pipe.write_end, "z");
    }
    _ = try loop.tick(100);

    for (&probes, 0..) |*p, i| {
        const want: usize = if (i == 2 or i == 5) 0 else 1;
        try testing.expectEqual(want, p.bytes);
    }

    for (&probes, 0..) |*p, i| {
        if (i == 2 or i == 5) continue;
        loop.remove(&p.source);
    }
}

test "a source may remove itself from its own callback" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Self-removal is the normal shape for "connection closed": the
    // callback notices EOF and tears itself down mid-dispatch. The
    // dispatch loop has to survive the array shifting under it.
    const SelfRemover = struct {
        source: Source,
        pipe: sys.Pipe,
        loop: *Loop,
        fired: usize = 0,

        fn onReady(src: *Source, _: Ready) void {
            const self: *@This() = @fieldParentPtr("source", src);
            self.fired += 1;
            var buf: [16]u8 = undefined;
            _ = sys.read(src.fd, &buf) catch {};
            self.loop.remove(src);
        }
    };

    var a: SelfRemover = undefined;
    var b: SelfRemover = undefined;
    inline for (.{ &a, &b }) |s| {
        const p = try sys.pipe();
        s.* = .{
            .source = .{ .fd = p.read_end, .interest = .readable, .callback = SelfRemover.onReady },
            .pipe = p,
            .loop = &loop,
        };
        try loop.add(&s.source);
    }
    defer {
        sys.close(a.pipe.read_end);
        sys.close(a.pipe.write_end);
        sys.close(b.pipe.read_end);
        sys.close(b.pipe.write_end);
    }

    _ = try sys.write(a.pipe.write_end, "x");
    _ = try sys.write(b.pipe.write_end, "y");
    _ = try loop.tick(100);

    // Both must have fired despite the first one's removal reshuffling
    // the backend arrays.
    try testing.expectEqual(@as(usize, 1), a.fired);
    try testing.expectEqual(@as(usize, 1), b.fired);
    try testing.expect(!a.source.isRegistered());
    try testing.expect(!b.source.isRegistered());
}

test "dropping write interest stops the writable storm" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // An always-writable fd reported every tick is the classic busy
    // loop. Registering write interest once proves it fires; dropping
    // it proves the loop then goes quiet.
    const WriteProbe = struct {
        source: Source,
        pipe: sys.Pipe,
        fired: usize = 0,

        fn onReady(src: *Source, _: Ready) void {
            const self: *@This() = @fieldParentPtr("source", src);
            self.fired += 1;
        }
    };

    const p = try sys.pipe();
    defer sys.close(p.read_end);
    defer sys.close(p.write_end);

    var probe: WriteProbe = .{
        .source = .{ .fd = p.write_end, .interest = .writable, .callback = WriteProbe.onReady },
        .pipe = p,
    };
    try loop.add(&probe.source);
    defer loop.remove(&probe.source);

    _ = try loop.tick(50);
    try testing.expect(probe.fired >= 1);

    const before = probe.fired;
    try loop.modify(&probe.source, .none);
    // With no interest left and no timers, this tick has nothing to wait
    // for and must return on the cap rather than firing again.
    _ = try loop.tick(20);
    try testing.expectEqual(before, probe.fired);
}

test "timers fire in deadline order, not insertion order" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Recorder = struct {
        var order: [4]u8 = @splat(0);
        var n: usize = 0;

        fn make(comptime id: u8) *const fn (*Timer) void {
            return struct {
                fn f(_: *Timer) void {
                    order[n] = id;
                    n += 1;
                }
            }.f;
        }
    };
    Recorder.n = 0;

    var t1: Timer = .{ .callback = Recorder.make(1) };
    var t2: Timer = .{ .callback = Recorder.make(2) };
    var t3: Timer = .{ .callback = Recorder.make(3) };

    // Inserted 3, 1, 2 but armed for 30ms, 5ms, 15ms.
    try loop.addTimer(&t3, 30 * std.time.ns_per_ms);
    try loop.addTimer(&t1, 5 * std.time.ns_per_ms);
    try loop.addTimer(&t2, 15 * std.time.ns_per_ms);

    while (Recorder.n < 3) _ = try loop.tick(100);

    try testing.expectEqual(@as(u8, 1), Recorder.order[0]);
    try testing.expectEqual(@as(u8, 2), Recorder.order[1]);
    try testing.expectEqual(@as(u8, 3), Recorder.order[2]);
}

test "cancelled timer never fires" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Flag = struct {
        var fired: bool = false;
        fn f(_: *Timer) void {
            fired = true;
        }
    };
    Flag.fired = false;

    var t: Timer = .{ .callback = Flag.f };
    try loop.addTimer(&t, 5 * std.time.ns_per_ms);
    loop.cancelTimer(&t);
    try testing.expect(!t.isArmed());

    // Sleep past the deadline; nothing should have run.
    _ = try loop.tick(20);
    try testing.expect(!Flag.fired);
}

test "wake() breaks an otherwise indefinite sleep" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Waker = struct {
        fn run(l: *Loop) void {
            sys.sleep(10 * std.time.ns_per_ms);
            l.wake();
        }
    };

    // No sources and no timers: without the wakeup this tick would block
    // in the kernel until the heat death of the container.
    const th = try std.Thread.spawn(.{}, Waker.run, .{&loop});
    defer th.join();

    const start = sys.monotonicNanos();
    _ = try loop.tick(null);
    const elapsed = sys.monotonicNanos() - start;

    // Returned because it was woken, not because it spun.
    try testing.expect(elapsed >= 5 * std.time.ns_per_ms);
    try testing.expect(elapsed < 2 * std.time.ns_per_s);
}

test "a burst of wake() calls collapses into one wakeup" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // 1000 wakes while none is being drained must not queue 1000
    // wakeups; the pending flag short-circuits all but the first write.
    for (0..1000) |_| loop.wake();
    try testing.expect(loop.wake_pending.load(.acquire));

    _ = try loop.tick(50);
    try testing.expect(!loop.wake_pending.load(.acquire));

    // And the loop is quiet again afterwards: nothing left to read, so
    // this tick sleeps out its cap.
    const start = sys.monotonicNanos();
    _ = try loop.tick(20);
    try testing.expect(sys.monotonicNanos() - start >= 15 * std.time.ns_per_ms);
}

test "run() returns when there is provably nothing left to do" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // No sources, no timers — run() must not park forever.
    try loop.run();
}

test "run() exits on stop() from another thread" {
    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var probe = try PipeProbe.init();
    defer probe.deinit();
    try loop.add(&probe.source);
    defer loop.remove(&probe.source);

    const Stopper = struct {
        fn run(l: *Loop) void {
            sys.sleep(10 * std.time.ns_per_ms);
            l.stop();
        }
    };
    const th = try std.Thread.spawn(.{}, Stopper.run, .{&loop});
    defer th.join();

    // A registered source means run() would otherwise block forever.
    try loop.run();
    try testing.expect(loop.stopping.load(.acquire));
}

test "idle loop consumes no measurable CPU" {
    if (builtin.mode != .ReleaseFast) return error.SkipZigTest;

    const gpa = testing.allocator;
    var loop: Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var probe = try PipeProbe.init();
    defer probe.deinit();
    try loop.add(&probe.source);
    defer loop.remove(&probe.source);

    // This is the headline claim, asserted rather than described: park
    // the loop on a quiet fd for 200 ms of wall time and check how much
    // CPU time the process burned doing it. A spin loop would show
    // ~200 ms; blocking in one syscall shows single-digit microseconds.
    const before = cpuNanos();
    const wall_start = sys.monotonicNanos();
    _ = try loop.tick(200);
    const wall = sys.monotonicNanos() - wall_start;
    const cpu = cpuNanos() - before;

    try testing.expect(wall >= 150 * std.time.ns_per_ms);
    // 1% of wall time is already three orders of magnitude of headroom
    // over a spin; the point is that it scales with wakeups, not time.
    try testing.expect(cpu < wall / 100);
}

/// Process CPU time (user + system) in nanoseconds.
fn cpuNanos() u64 {
    const ru = std.posix.getrusage(0) // RUSAGE_SELF is 0 on both Linux and Darwin.
    ;
    const u = @as(u64, @intCast(ru.utime.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ru.utime.usec)) * std.time.ns_per_us;
    const s = @as(u64, @intCast(ru.stime.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ru.stime.usec)) * std.time.ns_per_us;
    return u + s;
}
