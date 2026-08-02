//! Stackful fibers: the bridge between the reactor and code that insists
//! on blocking.
//!
//! ## Why this exists
//!
//! `std.crypto.tls.Client.init` runs an entire TLS handshake — several
//! network round trips — as one synchronous call. Our sockets are
//! non-blocking and driven by a single-threaded reactor, so there is no
//! way to call it directly: the reader it pulls from would have to return
//! "would block" halfway through, and `init` is not resumable.
//!
//! The alternatives were a thread per TLS connection (40 committed stacks,
//! and it throws away the single-threaded design that gives us our
//! idle-CPU number) or writing our own incremental TLS 1.3 record layer
//! (weeks of work, and a security-critical wheel to reinvent). This is the
//! third option: give the blocking code its own stack. When its socket
//! would block, the fiber registers reactor interest and switches back to
//! the event loop; when the reactor reports readiness it switches back in
//! and the blocking-looking code resumes exactly where it left off.
//! Synchronous-looking TLS, one thread, zero idle CPU.
//!
//! ## What a context switch actually costs
//!
//! `std.Io.fiber.contextSwitch` saves three words (`sp`, `fp`, `pc`) and
//! declares every other register clobbered, which makes the compiler spill
//! whatever it still needs before the switch and reload it after. There is
//! no kernel involvement and no scheduler: a park/unpark pair is two
//! switches, tens of nanoseconds. That is why parking on I/O is affordable
//! at the granularity of a single TLS record.
//!
//! The asm lives in exactly one non-inlined function, `switchContext`, and
//! that is not a style choice — see the comment on it before touching it.
//!
//! ## The dangerous part
//!
//! Everything below the API is raw CPU state. A mistake here is memory
//! corruption, not a failing assertion, so:
//!
//!   * The stack is `mmap`'d with an unmapped guard page beneath it, so a
//!     stack overflow is an immediate SIGSEGV instead of silently
//!     scribbling over whatever the allocator put there.
//!   * Every state transition is asserted. Re-entering a running fiber,
//!     or entering a finished one, trips an assert in Debug rather than
//!     jumping through a dead stack.
//!   * `stackHighWater` exists so tests can measure how much stack a real
//!     workload uses instead of us guessing.
//!
//! ## Lifetime rules
//!
//!   * A `Fiber` is initialised in place and must not move afterwards: the
//!     closure sitting at the top of its own stack holds a `*Fiber`, and
//!     the reactor holds `&fiber.source`.
//!   * There is no unwinding. `deinit` on a fiber that is still parked
//!     frees the stack without running the rest of the fiber's body, so
//!     anything the body acquired and had not yet released is leaked. To
//!     shut a fiber down cleanly, `cancel` it — that resumes it with
//!     `error.Canceled` out of `park`, and normal Zig error returns carry
//!     it out through the body's `defer`s.

const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");
const reactor = @import("reactor.zig");

const abi = std.Io.fiber;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const Fd = sys.Fd;
const assert = std.debug.assert;

pub const Interest = reactor.Interest;
pub const Ready = reactor.Ready;

comptime {
    if (!abi.supported) {
        @compileError("std.Io.fiber has no context-switch implementation for " ++
            @tagName(builtin.cpu.arch));
    }
    // `std.Io.fiber` also covers riscv64, but the naked entry trampoline
    // below is written per-architecture and we only ship these two. A
    // third target is a deliberate act, not something to inherit silently
    // from std.
    switch (builtin.cpu.arch) {
        .x86_64, .aarch64 => {},
        else => |arch| @compileError("hoardarr fibers support x86_64 and aarch64 only, not " ++
            @tagName(arch)),
    }
}

pub const Error = Allocator.Error || sys.Error;

/// What `park` can report. `Canceled` is never produced by the kernel —
/// it is injected by `cancel` so a parked fiber can be unwound.
pub const ParkError = Error || error{Canceled};

/// Default stack size, in bytes.
///
/// This is measured, not guessed. `net/tls.zig` has a test
/// ("handshake stack high-water") that paints a fiber's stack with a
/// canary, runs a real `std.crypto.tls.Client.init` over the parking
/// transport, and reports the high-water mark:
///
///   ReleaseSafe   144,040 bytes
///   ReleaseFast   148,000 bytes
///   Debug         505,936 bytes
///
/// Debug dominates because nothing is coalesced there, and the test suite
/// runs in Debug, so the number that has to fit is the largest one. The
/// bulk of it is `Client.init`'s own frame: it declares
/// `[2][16384]u8` of cleartext record buffers plus the handshake cipher
/// union, and Zig commits a function's whole frame in its prologue. Above
/// that sit certificate chain verification (not covered by the
/// measurement, since nothing in-tree can produce a chain to verify) and
/// whatever the caller's session function does.
///
/// 1 MiB is ~7x the shipping measurement and ~2x the Debug one. It is
/// affordable because it is *virtual* address space: the mapping is
/// anonymous and demand-paged, so a connection's real cost is the pages it
/// touches — about 37 in ReleaseFast — not the reservation. 40 provider
/// connections is 40 MiB of address space and ~6 MiB resident.
///
/// The failure mode for being wrong is a SIGSEGV (or SIGBUS on Darwin,
/// where the guard page is mapped-but-inaccessible rather than unmapped),
/// which is how the 256 KiB this file originally shipped with was found
/// out. Generous is cheap; wrong is a crash.
pub const default_stack_size: usize = 1024 * 1024;

/// Below this a fiber cannot reliably hold the closure plus one frame of
/// whatever the caller runs, so it is rejected rather than corrupted.
pub const min_stack_size: usize = 16 * 1024;

/// The initial stack pointer must satisfy the C ABI's stack alignment on
/// both architectures: aarch64 requires `sp % 16 == 0`, and x86_64's
/// SysV entry condition is `rsp % 16 == 8`, which the `- @sizeOf(usize)`
/// below produces from a 16-aligned closure address.
const stack_align: usize = 16;

/// Filled into the stack by `fillCanary`, checked by `stackHighWater`.
/// Eight bytes rather than one so a workload that legitimately writes
/// 0xA5 doesn't make the measurement lie.
const canary_word: u64 = 0xA5A5_5A5A_C0DE_F00D;

/// Live mapped stacks, process-wide.
///
/// `mmap`'d memory is invisible to `std.testing.allocator`, so without
/// this a leaked fiber shows up only as a slow climb in RSS that no test
/// can see. One atomic increment per fiber creation is not a hot path.
var live_stacks: std.atomic.Value(usize) = .init(0);

pub fn liveStacks() usize {
    return live_stacks.load(.acquire);
}

pub const Fiber = struct {
    gpa: Allocator,
    loop: *reactor.Loop,

    /// The whole reservation: one unmapped guard page followed by the
    /// writable stack. Freed as a single `munmap`.
    mapping: []align(std.heap.page_size_min) u8,
    /// The writable part of `mapping`. `mapping.ptr[0..guard_len]` below it
    /// is `PROT_NONE`.
    stack: []u8,
    /// Address of the closure at the very top of `stack`, which is also
    /// the fiber's initial stack pointer. Everything the fiber does lives
    /// in `[stack.ptr, frame_base)`.
    frame_base: usize,

    /// Where to resume whoever last switched *into* this fiber. Rewritten
    /// on every entry, because the resumer is usually a reactor callback
    /// and so a different frame each time.
    caller: abi.Context = undefined,
    /// Where to resume the fiber itself.
    inner: abi.Context,

    state: State = .ready,

    start_fn: *const fn (self: *Fiber, context: ?*anyopaque) void,
    context: ?*anyopaque,

    /// Registration used by `park`. Stays registered between parks with
    /// `.none` interest rather than being added and removed each time:
    /// churning the reactor's arrays inside its own dispatch loop is
    /// legal but needless.
    source: reactor.Source,
    /// Result handed back to the in-progress `park`.
    wake: Wake = .{ .ready = .{} },

    /// Called from `enter`, on the resumer's stack, once the fiber's body
    /// has returned. Safe to `deinit` and free the fiber from here: by
    /// then the fiber's own stack is idle and its reactor registration has
    /// been dropped.
    on_finished: ?*const fn (self: *Fiber) void = null,

    pub const State = enum {
        /// Initialised, never entered.
        ready,
        /// Currently executing on its own stack.
        running,
        /// Waiting for the reactor to report readiness.
        parked,
        /// Switched out voluntarily; only an explicit `enter` brings it
        /// back.
        yielded,
        /// Body returned. The stack is idle and may be freed.
        finished,
    };

    const Wake = union(enum) {
        ready: Ready,
        failed: ParkError,
    };

    /// Allocate a stack and point a context at `start_fn`.
    ///
    /// Initialises in place: the closure written to the top of the new
    /// stack stores `self`, and the reactor stores `&self.source`, so a
    /// by-value return would hand both a pointer into a dead temporary.
    ///
    /// The fiber does not run until `enter` is called.
    pub fn init(
        self: *Fiber,
        gpa: Allocator,
        loop: *reactor.Loop,
        stack_size: usize,
        start_fn: *const fn (self: *Fiber, context: ?*anyopaque) void,
        context: ?*anyopaque,
    ) Error!void {
        const page = std.heap.pageSize();
        const usable = std.mem.alignForward(usize, @max(stack_size, min_stack_size), page);

        // One unmapped page below the stack. The whole range is reserved
        // `PROT_NONE` first and the usable part then re-mapped read/write
        // over it with `MAP_FIXED`; that is the portable way to get a
        // guard page in Zig 0.16, which exposes `mmap`/`munmap` but not
        // `mprotect`.
        const region = posix.mmap(
            null,
            usable + page,
            .{},
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        ) catch return error.OutOfMemory;
        errdefer posix.munmap(region);

        const stack_start: [*]align(std.heap.page_size_min) u8 = @alignCast(region.ptr + page);
        _ = posix.mmap(
            stack_start,
            usable,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .FIXED = true },
            -1,
            0,
        ) catch return error.OutOfMemory;

        const stack = stack_start[0..usable];
        const top = @intFromPtr(stack.ptr) + stack.len;
        const closure_addr = std.mem.alignBackward(usize, top - @sizeOf(Closure), stack_align);
        assert(closure_addr > @intFromPtr(stack.ptr));

        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .mapping = region,
            .stack = stack,
            .frame_base = closure_addr,
            .inner = newContext(closure_addr),
            .start_fn = start_fn,
            .context = context,
            .source = .{
                .fd = sys.invalid_fd,
                .interest = .none,
                .callback = onReady,
            },
        };

        const closure: *Closure = @ptrFromInt(closure_addr);
        closure.* = .{ .fiber = self };

        _ = live_stacks.fetchAdd(1, .release);
    }

    /// Release the stack.
    ///
    /// Legal in any state. On a fiber that is still parked or yielded this
    /// discards the rest of its body without unwinding — see the lifetime
    /// note at the top of the file. It is *not* legal from inside the
    /// fiber itself; the stack being freed is the one running the call.
    pub fn deinit(self: *Fiber) void {
        assert(self.state != .running);
        if (self.source.isRegistered()) self.loop.remove(&self.source);
        posix.munmap(self.mapping);
        _ = live_stacks.fetchSub(1, .release);
        self.* = undefined;
    }

    pub fn isDone(self: *const Fiber) bool {
        return self.state == .finished;
    }

    /// Switch into the fiber. Returns when it parks, yields, or finishes.
    ///
    /// Named `enter` rather than `resume` only because `resume` is a Zig
    /// keyword; this is the resume half of the pair with `park`.
    pub fn enter(self: *Fiber) void {
        switch (self.state) {
            .ready, .parked, .yielded => {},
            // Re-entering a running fiber would overwrite the context it
            // is going to be resumed from; entering a finished one would
            // jump to the `unreachable` after its final switch.
            .running, .finished => unreachable,
        }
        self.state = .running;
        switchContext(&self.caller, &self.inner);

        // Back on the resumer's stack. Loop bookkeeping for a finished
        // fiber happens here rather than inside the fiber so that by the
        // time `on_finished` runs, nothing about the fiber is live.
        if (self.state == .finished) {
            if (self.source.isRegistered()) self.loop.remove(&self.source);
            if (self.on_finished) |f| f(self);
        }
    }

    /// Called *from inside the fiber*. Registers `fd` with the reactor,
    /// switches back to whoever entered us, and returns once readiness
    /// fires. This is the entire point of the module.
    ///
    /// The registration is dropped back to `.none` before returning, so a
    /// fiber that is doing CPU work between two parks does not make an
    /// always-writable socket spin the loop.
    pub fn park(self: *Fiber, fd: Fd, interest: Interest) ParkError!Ready {
        assert(self.state == .running);
        assert(!interest.isNone());

        try self.watch(fd, interest);
        self.wake = .{ .ready = .{} };
        self.state = .parked;
        switchContext(&self.inner, &self.caller);

        assert(self.state == .running);
        // `modify` only fails on the epoll backend, and only if the kernel
        // rejects the fd — by which point the fd is dead and the caller is
        // about to find out from its next read anyway. Dropping interest
        // is an optimisation, not a correctness requirement.
        if (self.source.isRegistered()) self.loop.modify(&self.source, .none) catch {};

        return switch (self.wake) {
            .ready => |r| r,
            .failed => |e| e,
        };
    }

    /// Drop this fiber's reactor registration entirely, releasing the fd it
    /// was watching.
    ///
    /// `park` deliberately leaves the source registered at `.none` so a
    /// fiber that parks repeatedly on one fd pays a single `epoll_ctl`, but
    /// that makes the fiber the fd's registered owner until it parks on a
    /// different one. A fd handed to *another* fiber has to be released
    /// first: exactly one thing may own readiness for an fd, and
    /// `epoll_ctl(ADD)` enforces it with `EEXIST`.
    ///
    /// The next `park` re-registers, so this costs one syscall and is never
    /// a correctness hazard for the fiber calling it.
    pub fn unwatch(self: *Fiber) void {
        if (self.source.isRegistered()) self.loop.remove(&self.source);
    }

    /// Called *from inside the fiber*. Switch back to the resumer without
    /// registering anything; only an explicit `enter` brings the fiber
    /// back. Used by cooperative drivers and by the tests.
    pub fn yield(self: *Fiber) void {
        assert(self.state == .running);
        self.state = .yielded;
        switchContext(&self.inner, &self.caller);
        assert(self.state == .running);
    }

    /// Resume a parked fiber and make its `park` return `error.Canceled`.
    ///
    /// This is the clean shutdown path: the error propagates out through
    /// the body's normal `try`/`defer`, so everything it acquired gets
    /// released. Compare `deinit` on a parked fiber, which does not.
    pub fn cancel(self: *Fiber) void {
        self.failWith(error.Canceled);
    }

    /// Resume a parked fiber and make its `park` return `err`. Lets the
    /// owner surface a deadline or a socket error into blocking-shaped
    /// code that has no other way to hear about it.
    pub fn failWith(self: *Fiber, err: ParkError) void {
        if (self.state != .parked) return;
        self.wake = .{ .failed = err };
        self.enter();
    }

    // -- stack accounting ---------------------------------------------

    /// Paint the unused stack with a known pattern so `stackHighWater`
    /// can tell what a workload touched.
    ///
    /// Only legal before the first `enter`, and only worth calling from
    /// tests: it faults in every page of the reservation, which is exactly
    /// the demand-paging behaviour production relies on not doing.
    pub fn fillCanary(self: *Fiber) void {
        assert(self.state == .ready);
        const len = self.frame_base - @intFromPtr(self.stack.ptr);
        const words = std.mem.bytesAsSlice(u64, self.stack[0 .. len - len % @sizeOf(u64)]);
        @memset(words, canary_word);
    }

    /// Bytes of stack the fiber has touched, measured from `frame_base`
    /// downwards. Requires a prior `fillCanary`.
    ///
    /// This is a lower bound: if the deepest frame happened to store the
    /// canary pattern itself we would stop short. Eight bytes of pattern
    /// makes that a 2^-64 coincidence per word, which is why the pattern
    /// is a word rather than a byte.
    pub fn stackHighWater(self: *const Fiber) usize {
        const len = self.frame_base - @intFromPtr(self.stack.ptr);
        const words = std.mem.bytesAsSlice(u64, self.stack[0 .. len - len % @sizeOf(u64)]);
        for (words, 0..) |w, i| {
            if (w != canary_word) return len - i * @sizeOf(u64);
        }
        return 0;
    }

    /// The guard region: unmapped, and the reason a stack overflow is a
    /// crash instead of corruption. Exposed so a test can prove the kernel
    /// really refuses to touch it.
    pub fn guardPage(self: *const Fiber) []const u8 {
        const len = @intFromPtr(self.stack.ptr) - @intFromPtr(self.mapping.ptr);
        return self.mapping[0..len];
    }

    // -- internals ----------------------------------------------------

    fn watch(self: *Fiber, fd: Fd, interest: Interest) Error!void {
        if (self.source.isRegistered() and self.source.fd != fd) {
            self.loop.remove(&self.source);
        }
        if (self.source.isRegistered()) {
            try self.loop.modify(&self.source, interest);
        } else {
            self.source.fd = fd;
            self.source.interest = interest;
            try self.loop.add(&self.source);
        }
    }

    fn onReady(src: *reactor.Source, ready: Ready) void {
        const self: *Fiber = @fieldParentPtr("source", src);
        // A source left registered at `.none` should never fire, but
        // level-triggered backends can hand us a stale event from the same
        // dispatch batch. Ignoring it is the only safe answer: switching
        // into a fiber that is not parked would corrupt its saved context.
        if (self.state != .parked) return;
        self.wake = .{ .ready = ready };
        // May run `on_finished`, which is allowed to free `self`. Nothing
        // below this line may touch it.
        self.enter();
    }
};

// ---------------------------------------------------------------------
// The ABI boundary
// ---------------------------------------------------------------------
//
// A new fiber has no stack frame to resume into, so instead we forge one:
// a `Closure` is written at the top of its stack, the initial stack
// pointer is aimed at that closure, and the initial program counter is a
// naked trampoline that moves the stack pointer into argument 0 and tail
// calls `Closure.call`. This is the contract `std/Io/Kqueue.zig` uses; it
// is not something to improvise, because the offsets differ per
// architecture and getting one wrong means jumping into a `*Fiber` read
// from the wrong slot.

const Closure = extern struct {
    fiber: *Fiber,

    comptime {
        // The forged stack pointer is 16-aligned (see `stack_align`), so
        // the C ABI's alignment requirement is met for `call` as long as
        // the closure itself needs no more than that.
        assert(@alignOf(Closure) <= stack_align);
    }

    /// The fiber's entry point, and the only function in the process that
    /// must never return.
    ///
    /// The second parameter is the `Switch` that the resumer passed to
    /// `contextSwitch`; it arrives in argument register 1 because that is
    /// where `contextSwitch` leaves its result. We have no bookkeeping to
    /// do with it, but it is declared so the signature matches what the
    /// trampoline actually hands over.
    fn call(closure: *Closure, incoming: *const abi.Switch) callconv(.c) noreturn {
        _ = incoming;
        const fiber = closure.fiber;
        assert(fiber.state == .running);

        fiber.start_fn(fiber, fiber.context);

        fiber.state = .finished;
        // One-way. `enter` refuses to switch into a finished fiber, so the
        // context this saves into `fiber.inner` is never restored and the
        // `unreachable` below is genuinely unreachable.
        switchContext(&fiber.inner, &fiber.caller);
        unreachable;
    }
};

/// The forged frame's return address, zeroed so a stack walk terminates.
///
/// This is not cosmetic. Anything that captures a backtrace from inside a
/// fiber — `std.heap.DebugAllocator` does it on every single allocation —
/// unwinds until it sees a return address of 0. Without this the unwinder
/// picks up whatever the resumer happened to leave in the link register
/// (aarch64) or in the word below the initial stack pointer (x86_64),
/// treats it as a real caller, and walks straight off the top of the
/// fiber's stack into a SIGSEGV. Found the hard way: the first version of
/// this file crashed inside the unwinder the moment a fiber body appended
/// to an `ArrayList` under the testing allocator.
fn fiberEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        // `rsp` was set to `closure - @sizeOf(usize)` so that the C ABI's
        // "return address occupies the word below the 16-aligned frame"
        // condition holds; the `+8` here undoes it to recover the closure,
        // and the store then puts a null return address in that slot.
        .x86_64 => asm volatile (
            \\ leaq 8(%%rsp), %%rdi
            \\ movq $0, (%%rsp)
            \\ jmp %[call:P]
            :
            : [call] "X" (&Closure.call),
        ),
        // `Closure.call`'s prologue spills the incoming x30; zeroing it
        // first is what makes that spill slot a stack-walk terminator.
        .aarch64 => asm volatile (
            \\ mov x0, sp
            \\ mov x30, xzr
            \\ b %[call]
            :
            : [call] "X" (&Closure.call),
        ),
        else => |arch| @compileError("unimplemented architecture: " ++ @tagName(arch)),
    }
}

fn newContext(closure_addr: usize) abi.Context {
    return switch (builtin.cpu.arch) {
        .aarch64 => .{
            .sp = closure_addr,
            .fp = 0,
            .pc = @intFromPtr(&fiberEntry),
        },
        .x86_64 => .{
            .rsp = closure_addr - @sizeOf(usize),
            .rbp = 0,
            .rip = @intFromPtr(&fiberEntry),
        },
        else => |arch| @compileError("unimplemented architecture: " ++ @tagName(arch)),
    };
}

/// Save the current CPU state into `save` and restore the state in
/// `restore`. Returns, on some later switch back, at the instruction
/// following the one that saved into `save`.
///
/// **The `noinline` is load-bearing. Do not remove it.**
///
/// `abi.Context` holds only `sp`, `fp` and `pc`; every other register is
/// meant to be preserved by `contextSwitch`'s clobber list forcing the
/// compiler to spill it. LLVM's AArch64 backend does not honour `x30` in
/// that list. Inlined into a caller at `-OReleaseFast` it will happily
/// keep a loop counter in `x30` across the switch, and the counter comes
/// back holding whatever return address the other fiber left behind. That
/// is not a theoretical concern: it is what the first version of this file
/// did, and the "locals survive every one of many switches" test caught it
/// as a 64-iteration loop that reported 4-billion-and-change iterations.
///
/// Isolating the asm in its own non-inlined function fixes it *by
/// construction* rather than by hoping the clobber list is respected.
/// Because the asm is the only thing in the body, the clobber list makes
/// every callee-saved register live across it, so the compiler emits a
/// prologue that spills all of them — including `x30` / the return address
/// — and an epilogue at the resume label that reloads them. That is
/// precisely a hand-written `swapcontext`, and from any caller's point of
/// view this is now an ordinary C call: callee-saved registers are
/// restored by the callee, caller-saved ones the caller already had to
/// assume were destroyed.
noinline fn switchContext(save: *abi.Context, restore: *abi.Context) void {
    const message: abi.Switch = .{ .old = save, .new = restore };
    _ = abi.contextSwitch(&message);
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Drive `loop` until `done`, with a wall-clock ceiling. A fiber bug
/// hangs rather than fails, so no test here may loop unbounded.
fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) {
            return error.TestTimeout;
        }
        _ = try loop.tick(5);
    }
}

fn emptyLoop(gpa: Allocator, loop: *reactor.Loop) !void {
    try loop.init(gpa);
}

test "a fiber's locals survive every one of many switches" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const rounds = 64;

    const Body = struct {
        /// Deliberately awkward locals: values the compiler must keep
        /// across the switch in callee-saved registers or spill slots. If
        /// `contextSwitch`'s clobber list were wrong, these are what would
        /// come back wrong.
        var observed_rounds: usize = 0;
        var checksum: u64 = 0;
        var wrong: usize = 0;

        fn run(f: *Fiber, _: ?*anyopaque) void {
            var a: u64 = 0x0123_4567_89ab_cdef;
            var b: u64 = 0xfedc_ba98_7654_3210;
            var c: u32 = 1;
            var buf: [32]u8 = undefined;
            for (&buf, 0..) |*x, i| x.* = @truncate(i);

            var i: usize = 0;
            while (i < rounds) : (i += 1) {
                const a_before = a;
                const b_before = b;
                const c_before = c;

                f.yield();

                if (a != a_before or b != b_before or c != c_before) wrong += 1;
                for (buf, 0..) |x, j| if (x != @as(u8, @truncate(j))) {
                    wrong += 1;
                };

                a = a *% 0x9e37_79b9_7f4a_7c15 +% 1;
                b ^= a >> 17;
                c = c *% 3;
                checksum ^= a ^ b ^ c;
                observed_rounds += 1;
            }
        }
    };
    Body.observed_rounds = 0;
    Body.checksum = 0;
    Body.wrong = 0;

    var f: Fiber = undefined;
    try f.init(gpa, &loop, default_stack_size, Body.run, null);
    defer f.deinit();

    var entries: usize = 0;
    while (!f.isDone()) {
        f.enter();
        entries += 1;
        try testing.expect(entries <= rounds + 2);
    }

    try testing.expectEqual(@as(usize, 0), Body.wrong);
    try testing.expectEqual(@as(usize, rounds), Body.observed_rounds);
    try testing.expectEqual(@as(usize, rounds + 1), entries);

    // Recompute the expected checksum on the loop's own stack. A fiber
    // that silently ran on the wrong stack would still count rounds
    // correctly but could not reproduce this.
    var a: u64 = 0x0123_4567_89ab_cdef;
    var b: u64 = 0xfedc_ba98_7654_3210;
    var c: u32 = 1;
    var want: u64 = 0;
    for (0..rounds) |_| {
        a = a *% 0x9e37_79b9_7f4a_7c15 +% 1;
        b ^= a >> 17;
        c = c *% 3;
        want ^= a ^ b ^ c;
    }
    try testing.expectEqual(want, Body.checksum);
}

test "many fibers interleaved in a scrambled order keep their own identity" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const count = 96;
    const rounds = 12;

    const Slot = struct {
        fiber: Fiber = undefined,
        id: usize = 0,
        /// Accumulated purely from the fiber's own locals.
        acc: u64 = 0,
        /// Set if the fiber ever saw a local that didn't belong to it.
        confused: bool = false,
        /// Stack range check: the fiber's frame must live in its own map.
        off_stack: bool = false,
        rounds: usize = 0,

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            var local_id = self.id;
            var acc: u64 = self.id *% 0x1000_0001;
            var probe: usize = 0;

            for (0..rounds) |_| {
                const addr = @intFromPtr(&probe);
                if (addr < @intFromPtr(f.stack.ptr) or addr >= f.frame_base) {
                    self.off_stack = true;
                }
                f.yield();
                if (local_id != self.id) self.confused = true;
                acc = acc *% 31 +% local_id;
                local_id = self.id;
                probe += 1;
                self.rounds += 1;
            }
            self.acc = acc;
        }
    };

    const slots = try gpa.alloc(Slot, count);
    defer gpa.free(slots);
    for (slots, 0..) |*s, i| {
        s.* = .{ .id = i };
        // Vary the stack size so a bug that only shows up at one
        // particular alignment or page count has somewhere to hide.
        const size = min_stack_size + (i % 5) * 8 * 1024;
        try s.fiber.init(gpa, &loop, size, Slot.run, s);
    }
    defer for (slots) |*s| s.fiber.deinit();

    // Scramble the order deterministically. Round-robin would let a
    // save/restore bug that only swaps neighbouring fibers pass.
    var prng = std.Random.DefaultPrng.init(0x5EED_1234);
    const rand = prng.random();

    var remaining: usize = count;
    var guard: usize = 0;
    while (remaining > 0) {
        guard += 1;
        // Useful entries are `count * (rounds + 1)`; the rest are picks
        // that landed on an already-finished fiber, which near the end
        // costs a coupon-collector tail of roughly `count * ln(count)`.
        // Four times the useful count covers it with room to spare while
        // still turning a genuine livelock into a failure.
        try testing.expect(guard < count * (rounds + 1) * 4);
        const i = rand.uintLessThan(usize, count);
        const s = &slots[i];
        if (s.fiber.isDone()) continue;
        s.fiber.enter();
        if (s.fiber.isDone()) remaining -= 1;
    }

    for (slots, 0..) |*s, i| {
        try testing.expect(!s.confused);
        try testing.expect(!s.off_stack);
        try testing.expectEqual(@as(usize, rounds), s.rounds);

        var want: u64 = i *% 0x1000_0001;
        for (0..rounds) |_| want = want *% 31 +% i;
        try testing.expectEqual(want, s.acc);
    }
}

test "a fiber parks on a real pipe and the reactor wakes it" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var p = try sys.pipe();
    defer sys.close(p.read_end);
    defer if (p.write_end != sys.invalid_fd) sys.close(p.write_end);

    const Body = struct {
        got: std.ArrayList(u8) = .empty,
        gpa: Allocator,
        fd: Fd,
        err: ?anyerror = null,
        parks: usize = 0,

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            var buf: [64]u8 = undefined;
            // Blocking-shaped: read until EOF, parking whenever the pipe
            // has nothing. This is the exact shape the TLS transport uses.
            while (true) {
                const n = sys.read(self.fd, &buf) catch |err| switch (err) {
                    error.WouldBlock => {
                        const ready = f.park(self.fd, .readable) catch |e| {
                            self.err = e;
                            return;
                        };
                        self.parks += 1;
                        if (!ready.read and ready.terminal()) return;
                        continue;
                    },
                    else => {
                        self.err = err;
                        return;
                    },
                };
                if (n == 0) return;
                self.got.appendSlice(self.gpa, buf[0..n]) catch |err| {
                    self.err = err;
                    return;
                };
            }
        }
    };

    var body: Body = .{ .gpa = gpa, .fd = p.read_end };
    defer body.got.deinit(gpa);

    var f: Fiber = undefined;
    try f.init(gpa, &loop, default_stack_size, Body.run, &body);
    defer f.deinit();

    // First entry runs until the pipe is empty and it parks.
    f.enter();
    try testing.expectEqual(Fiber.State.parked, f.state);
    try testing.expect(f.source.isRegistered());
    try testing.expectEqual(p.read_end, f.source.fd);

    // From here on the loop drives it. Several separate writes, so the
    // fiber has to park and be woken repeatedly rather than once.
    const chunks = [_][]const u8{ "one ", "two ", "three ", "four" };
    for (chunks) |chunk| {
        _ = try sys.write(p.write_end, chunk);
        const before = body.got.items.len;
        const Ctx = struct { b: *Body, want: usize };
        var ctx = Ctx{ .b = &body, .want = before + chunk.len };
        try pumpUntil(&loop, 2000, &ctx, struct {
            fn done(c: *Ctx) bool {
                return c.b.got.items.len >= c.want or c.b.err != null;
            }
        }.done);
    }

    try testing.expectEqual(@as(?anyerror, null), body.err);
    try testing.expectEqualStrings("one two three four", body.got.items);
    try testing.expect(body.parks >= chunks.len);
    try testing.expect(!f.isDone());

    // Closing the writer is EOF, which ends the body.
    sys.close(p.write_end);
    p.write_end = sys.invalid_fd;
    try pumpUntil(&loop, 2000, &f, struct {
        fn done(x: *Fiber) bool {
            return x.isDone();
        }
    }.done);
    try testing.expect(f.isDone());
    // A finished fiber must not leave a source behind, or `Loop.run` would
    // never conclude there is nothing left to do.
    try testing.expect(!f.source.isRegistered());
    try testing.expectEqual(@as(usize, 0), loop.source_count);
}

test "between parks the fiber holds no reactor interest" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const p = try sys.pipe();
    defer sys.close(p.read_end);
    defer sys.close(p.write_end);

    const Body = struct {
        fd: Fd,
        interest_after_park: Interest = .both,

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            _ = f.park(self.fd, .readable) catch return;
            // Sampled from inside the fiber, immediately after park
            // returns: this is the window in which a fiber doing CPU work
            // would otherwise be spinning the loop.
            self.interest_after_park = f.source.interest;
            f.yield();
        }
    };

    var body: Body = .{ .fd = p.read_end };
    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, &body);
    defer f.deinit();

    f.enter();
    _ = try sys.write(p.write_end, "x");
    try pumpUntil(&loop, 2000, &f, struct {
        fn done(x: *Fiber) bool {
            return x.state == .yielded;
        }
    }.done);

    try testing.expect(body.interest_after_park.isNone());
}

test "deep recursion runs on the fiber's own stack" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Body = struct {
        var sum: u64 = 0;
        var deepest: usize = 0;
        var outside: bool = false;
        var loop_stack_probe: usize = 0;

        const depth = 400;

        fn recurse(f: *Fiber, n: usize) u64 {
            // A local array per frame, so the frames are real and the
            // stack actually grows. `never_inline` on the recursive call
            // stops ReleaseFast turning this into a loop.
            var pad: [48]u8 = undefined;
            for (&pad, 0..) |*x, i| x.* = @truncate(n +% i);

            const addr = @intFromPtr(&pad);
            if (addr < @intFromPtr(f.stack.ptr) or addr >= f.frame_base) outside = true;
            if (addr < deepest or deepest == 0) deepest = addr;

            if (n == 0) return pad[0];
            const rest = @call(.never_inline, recurse, .{ f, n - 1 });
            return rest +% pad[0] +% pad[47];
        }

        fn run(f: *Fiber, _: ?*anyopaque) void {
            sum = recurse(f, depth);
            f.yield();
        }
    };
    Body.sum = 0;
    Body.deepest = 0;
    Body.outside = false;

    var f: Fiber = undefined;
    try f.init(gpa, &loop, default_stack_size, Body.run, null);
    defer f.deinit();

    // The loop's own stack, for comparison.
    var here: usize = 0;
    Body.loop_stack_probe = @intFromPtr(&here);

    f.enter();

    try testing.expect(!Body.outside);
    try testing.expect(Body.deepest != 0);
    // The recursion really descended: at least a few hundred bytes per
    // frame times the depth would be excessive, but it must be well more
    // than one frame's worth.
    try testing.expect(f.frame_base - Body.deepest > Body.depth * 16);
    // And it was nowhere near the caller's stack.
    const distance = @max(Body.loop_stack_probe, Body.deepest) - @min(Body.loop_stack_probe, Body.deepest);
    try testing.expect(distance > f.stack.len);
}

test "stack canary: the untouched region is intact and the high-water mark is sane" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Body = struct {
        const depth = 200;

        fn recurse(n: usize) u64 {
            var pad: [64]u8 = undefined;
            for (&pad, 0..) |*x, i| x.* = @truncate(n ^ i);
            if (n == 0) return pad[0];
            return @call(.never_inline, recurse, .{n - 1}) +% pad[0];
        }

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const out: *u64 = @ptrCast(@alignCast(ctx.?));
            out.* = recurse(depth);
            f.yield();
        }
    };

    var out: u64 = 0;
    var f: Fiber = undefined;
    try f.init(gpa, &loop, default_stack_size, Body.run, &out);
    defer f.deinit();

    f.fillCanary();
    try testing.expectEqual(@as(usize, 0), f.stackHighWater());

    f.enter();

    const high_water = f.stackHighWater();
    try testing.expect(high_water > 0);
    try testing.expect(high_water < f.stack.len);
    // Everything below the high-water mark must still hold the pattern;
    // if it doesn't, something wrote outside the fiber's frames.
    const untouched = f.stack[0 .. f.frame_base - @intFromPtr(f.stack.ptr) - high_water];
    var i: usize = 0;
    while (i + @sizeOf(u64) <= untouched.len) : (i += @sizeOf(u64)) {
        try testing.expectEqual(canary_word, std.mem.readInt(u64, untouched[i..][0..8], .little));
    }

    // Reported so a human reading test output can sanity-check
    // `default_stack_size` against a real workload.
    std.log.debug("fiber stack high-water for {d} recursive frames: {d} bytes", .{
        Body.depth, high_water,
    });
}

test "a fiber that finishes without ever parking" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Body = struct {
        fn run(_: *Fiber, ctx: ?*anyopaque) void {
            const out: *u64 = @ptrCast(@alignCast(ctx.?));
            var acc: u64 = 0;
            for (0..1000) |i| acc +%= i * i;
            out.* = acc;
        }
    };

    var out: u64 = 0;
    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, &out);
    defer f.deinit();

    try testing.expect(!f.isDone());
    f.enter();
    try testing.expect(f.isDone());

    var want: u64 = 0;
    for (0..1000) |i| want +%= i * i;
    try testing.expectEqual(want, out);
    try testing.expect(!f.source.isRegistered());
}

test "a fiber whose body returns immediately" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Body = struct {
        var ran: bool = false;
        fn run(_: *Fiber, _: ?*anyopaque) void {
            ran = true;
        }
    };
    Body.ran = false;

    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, null);
    defer f.deinit();

    f.enter();
    try testing.expect(Body.ran);
    try testing.expect(f.isDone());
}

test "on_finished fires once, after the fiber is safe to free" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // The realistic shape: the fiber is heap-allocated, and the thing that
    // frees it is the completion callback firing out of a reactor
    // dispatch. `testing.allocator` proves the free happened.
    const Owned = struct {
        fiber: Fiber = undefined,
        gpa: Allocator,
        finished: *usize,
        fd: Fd,

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            var buf: [8]u8 = undefined;
            while (true) {
                const n = sys.read(self.fd, &buf) catch |err| switch (err) {
                    error.WouldBlock => {
                        _ = f.park(self.fd, .readable) catch return;
                        continue;
                    },
                    else => return,
                };
                if (n == 0) return;
            }
        }

        fn onFinished(f: *Fiber) void {
            const self: *@This() = @fieldParentPtr("fiber", f);
            self.finished.* += 1;
            const g = self.gpa;
            f.deinit();
            g.destroy(self);
        }
    };

    const p = try sys.pipe();
    defer sys.close(p.read_end);

    var finished: usize = 0;
    const owned = try gpa.create(Owned);
    owned.* = .{ .gpa = gpa, .finished = &finished, .fd = p.read_end };
    try owned.fiber.init(gpa, &loop, min_stack_size, Owned.run, owned);
    owned.fiber.on_finished = Owned.onFinished;

    const before = liveStacks();
    owned.fiber.enter();
    try testing.expectEqual(Fiber.State.parked, owned.fiber.state);

    // EOF ends the body; the completion callback frees everything from
    // inside the reactor's dispatch.
    sys.close(p.write_end);
    try pumpUntil(&loop, 2000, &finished, struct {
        fn done(n: *usize) bool {
            return n.* > 0;
        }
    }.done);

    try testing.expectEqual(@as(usize, 1), finished);
    try testing.expectEqual(before - 1, liveStacks());
}

test "deinit of a parked fiber releases its stack and its registration" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const p = try sys.pipe();
    defer sys.close(p.read_end);
    defer sys.close(p.write_end);

    const Body = struct {
        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const fd: *Fd = @ptrCast(@alignCast(ctx.?));
            // Parks forever. Nothing will ever write to the pipe.
            _ = f.park(fd.*, .readable) catch return;
            unreachable;
        }
    };

    var fd = p.read_end;
    const before = liveStacks();

    // Heap-allocated so `testing.allocator` also has an opinion about the
    // fiber struct itself, not just the mapping.
    const f = try gpa.create(Fiber);
    try f.init(gpa, &loop, default_stack_size, Body.run, &fd);
    try testing.expectEqual(before + 1, liveStacks());

    f.enter();
    try testing.expectEqual(Fiber.State.parked, f.state);
    try testing.expect(f.source.isRegistered());
    try testing.expectEqual(@as(usize, 1), loop.source_count);

    f.deinit();
    gpa.destroy(f);

    try testing.expectEqual(before, liveStacks());
    // The abandoned registration must be gone too, or the loop dispatches
    // into freed memory on the next readable event.
    try testing.expectEqual(@as(usize, 0), loop.source_count);
    _ = try loop.tick(5);
}

test "cancel unwinds a parked fiber through its own defers" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const p = try sys.pipe();
    defer sys.close(p.read_end);
    defer sys.close(p.write_end);

    const Body = struct {
        fd: Fd,
        released: bool = false,
        saw: ?anyerror = null,

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            // The point of `cancel` over `deinit`: this defer runs.
            defer self.released = true;
            _ = f.park(self.fd, .readable) catch |err| {
                self.saw = err;
                return;
            };
        }
    };

    var body: Body = .{ .fd = p.read_end };
    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, &body);
    defer f.deinit();

    f.enter();
    try testing.expectEqual(Fiber.State.parked, f.state);

    f.cancel();

    try testing.expect(f.isDone());
    try testing.expect(body.released);
    try testing.expectEqual(@as(?anyerror, error.Canceled), body.saw);
    try testing.expect(!f.source.isRegistered());
}

test "failWith injects an arbitrary error into a park" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const p = try sys.pipe();
    defer sys.close(p.read_end);
    defer sys.close(p.write_end);

    const Body = struct {
        fd: Fd,
        saw: ?anyerror = null,

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            _ = f.park(self.fd, .readable) catch |err| {
                self.saw = err;
                return;
            };
        }
    };

    var body: Body = .{ .fd = p.read_end };
    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, &body);
    defer f.deinit();

    f.enter();
    // A deadline is the motivating case: the owner's timer fires and has
    // to get the news into code that only knows how to block.
    f.failWith(error.TimedOut);
    try testing.expectEqual(@as(?anyerror, error.TimedOut), body.saw);
    try testing.expect(f.isDone());

    // Injecting into a fiber that is not parked is a no-op, not a crash.
    f.failWith(error.TimedOut);
    try testing.expect(f.isDone());
}

test "the guard page below the stack is unreadable to the kernel" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Body = struct {
        fn run(_: *Fiber, _: ?*anyopaque) void {}
    };

    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, null);
    defer f.deinit();

    const guard = f.guardPage();
    try testing.expect(guard.len >= std.heap.pageSize());
    // Contiguous and immediately below the stack: an overflowing frame
    // walks straight into it rather than into a neighbouring allocation.
    try testing.expectEqual(@intFromPtr(f.stack.ptr), @intFromPtr(guard.ptr) + guard.len);

    // Proving the protection without crashing the test process: hand the
    // kernel the address and let *it* fault. A `write` whose source buffer
    // is unreadable fails with EFAULT, which `sys.mapError` surfaces as
    // `error.Unexpected`. The same call over the usable stack succeeds, so
    // this distinguishes protection from a bad fd.
    const p = try sys.pipe();
    defer sys.close(p.read_end);
    defer sys.close(p.write_end);

    // Last byte of the guard region: the one an overflow reaches first.
    try testing.expectError(error.Unexpected, sys.write(p.write_end, guard[guard.len - 1 ..]));
    try testing.expectEqual(@as(usize, 1), try sys.write(p.write_end, f.stack[0..1]));
}

test "a fiber can park on two different fds in turn" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const a = try sys.pipe();
    defer sys.close(a.read_end);
    defer sys.close(a.write_end);
    const b = try sys.pipe();
    defer sys.close(b.read_end);
    defer sys.close(b.write_end);

    const Body = struct {
        a: Fd,
        b: Fd,
        got: [2]u8 = .{ 0, 0 },
        err: ?anyerror = null,

        fn readOne(f: *Fiber, fd: Fd) !u8 {
            var buf: [1]u8 = undefined;
            while (true) {
                const n = sys.read(fd, &buf) catch |err| switch (err) {
                    error.WouldBlock => {
                        _ = try f.park(fd, .readable);
                        continue;
                    },
                    else => return err,
                };
                if (n == 0) return error.EndOfStream;
                return buf[0];
            }
        }

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.got[0] = readOne(f, self.a) catch |e| {
                self.err = e;
                return;
            };
            self.got[1] = readOne(f, self.b) catch |e| {
                self.err = e;
                return;
            };
        }
    };

    var body: Body = .{ .a = a.read_end, .b = b.read_end };
    var f: Fiber = undefined;
    try f.init(gpa, &loop, min_stack_size, Body.run, &body);
    defer f.deinit();

    f.enter();
    try testing.expectEqual(a.read_end, f.source.fd);

    // Once the first byte lands, the fiber moves on to the second pipe and
    // its registration must follow. A stale fd here means the loop is
    // watching the wrong thing and the second read would hang.
    _ = try sys.write(a.write_end, "A");
    const Probe = struct { f: *Fiber, want: Fd };
    var probe = Probe{ .f = &f, .want = b.read_end };
    try pumpUntil(&loop, 2000, &probe, struct {
        fn done(x: *Probe) bool {
            return x.f.isDone() or (x.f.state == .parked and x.f.source.fd == x.want);
        }
    }.done);
    try testing.expectEqual(b.read_end, f.source.fd);

    _ = try sys.write(b.write_end, "B");
    try pumpUntil(&loop, 2000, &f, struct {
        fn done(x: *Fiber) bool {
            return x.isDone();
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, null), body.err);
    try testing.expectEqual(@as(u8, 'A'), body.got[0]);
    try testing.expectEqual(@as(u8, 'B'), body.got[1]);
    try testing.expectEqual(@as(usize, 0), loop.source_count);
}

test "a fiber may drive another fiber" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Nesting works for free because a fiber stack is a real stack: the
    // inner fiber's `caller` context simply points into the outer fiber's
    // frame. Worth pinning, because it is the shape a TLS connection
    // running inside a fiber would take if it ever spawned a helper.
    const Inner = struct {
        var rounds: usize = 0;
        fn run(f: *Fiber, _: ?*anyopaque) void {
            for (0..3) |_| {
                rounds += 1;
                f.yield();
            }
        }
    };
    const Outer = struct {
        var done: bool = false;
        fn run(_: *Fiber, ctx: ?*anyopaque) void {
            const inner: *Fiber = @ptrCast(@alignCast(ctx.?));
            while (!inner.isDone()) inner.enter();
            done = true;
        }
    };
    Inner.rounds = 0;
    Outer.done = false;

    var inner: Fiber = undefined;
    try inner.init(gpa, &loop, min_stack_size, Inner.run, null);
    defer inner.deinit();

    var outer: Fiber = undefined;
    try outer.init(gpa, &loop, default_stack_size, Outer.run, &inner);
    defer outer.deinit();

    outer.enter();
    try testing.expect(Outer.done);
    try testing.expect(outer.isDone());
    try testing.expect(inner.isDone());
    try testing.expectEqual(@as(usize, 3), Inner.rounds);
}

test "stack sizes are rounded up to whole pages and floored at the minimum" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    const Body = struct {
        fn run(_: *Fiber, _: ?*anyopaque) void {}
    };

    const page = std.heap.pageSize();

    var tiny: Fiber = undefined;
    try tiny.init(gpa, &loop, 1, Body.run, null);
    defer tiny.deinit();
    try testing.expect(tiny.stack.len >= min_stack_size);
    try testing.expectEqual(@as(usize, 0), tiny.stack.len % page);

    var odd: Fiber = undefined;
    try odd.init(gpa, &loop, min_stack_size + 1, Body.run, null);
    defer odd.deinit();
    try testing.expectEqual(@as(usize, 0), odd.stack.len % page);
    try testing.expect(odd.stack.len >= min_stack_size + 1);

    // The forged stack pointer must be 16-aligned on both architectures.
    try testing.expectEqual(@as(usize, 0), odd.frame_base % stack_align);
    try testing.expectEqual(@as(usize, 0), tiny.frame_base % stack_align);
}
