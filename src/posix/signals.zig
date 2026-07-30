//! Signal delivery as a reactor source.
//!
//! In a container the daemon is PID 1, so `SIGTERM` on `docker stop` is
//! how every shutdown begins. Getting it wrong means a 10-second wait
//! followed by `SIGKILL` on every single stop, and a `SIGKILL` mid-write
//! is how a SQLite WAL ends up needing recovery.
//!
//! ## Why not a plain signal handler
//!
//! A handler runs on whatever stack the signal interrupted, and almost
//! nothing is safe to call from there — not the allocator, not the
//! logger's mutex, certainly not SQLite. The standard fix is for the
//! handler to set a flag, but a flag is invisible to a thread parked in
//! `poll`: `poll` returns `EINTR`, the loop goes round, and only then does
//! anyone notice. That works, but it makes signal handling depend on the
//! loop's error path.
//!
//! So signals become a file descriptor instead, and shutdown is just
//! another readable event dispatched on the reactor with the full run-time
//! environment available:
//!
//!   * **Linux** — `signalfd`. The signals are blocked process-wide first,
//!     so they are never delivered as interrupts at all; they queue and we
//!     read them as data.
//!   * **macOS** — no `signalfd`, so a minimal handler writes one byte to
//!     a self-pipe, which is async-signal-safe (`write` is on the POSIX
//!     list). The pipe's read end is the reactor source. Same shape,
//!     different plumbing.

const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");
const reactor = @import("reactor.zig");

const Fd = sys.Fd;

pub const Signal = enum {
    /// `docker stop`, `systemctl stop`, orchestrator eviction.
    term,
    /// Ctrl-C in a foreground shell.
    interrupt,
    /// Conventionally "reload configuration".
    hup,

    /// `std.posix.SIG` is a typed enum in Zig 0.16, and its numbering is
    /// per-OS, so go through it rather than hardcoding numbers.
    pub fn sig(s: Signal) std.posix.SIG {
        return switch (s) {
            .term => .TERM,
            .interrupt => .INT,
            .hup => .HUP,
        };
    }

    pub fn number(s: Signal) u32 {
        return @intFromEnum(s.sig());
    }

    pub fn fromNumber(n: u32) ?Signal {
        inline for (watched) |s| {
            if (s.number() == n) return s;
        }
        return null;
    }
};

pub const Error = sys.Error || error{SignalSetupFailed};

/// The set of signals a `Signals` instance handles. Everything else keeps
/// its default disposition — we deliberately don't touch `SIGPIPE` here
/// (see `ignoreSigpipe`), and nothing should be catching `SIGSEGV`.
pub const watched = [_]Signal{ .term, .interrupt, .hup };

/// A reactor source that turns signals into callbacks.
pub const Signals = struct {
    source: reactor.Source,
    /// Called once per signal received, on the reactor thread, with
    /// everything available. Safe to log, allocate, or begin a graceful
    /// shutdown from here.
    callback: *const fn (self: *Signals, sig: Signal) void,
    context: ?*anyopaque = null,

    /// Only used by the self-pipe backend.
    write_end: Fd = sys.invalid_fd,

    /// The self-pipe write end, as a process-global, because a signal
    /// handler receives no context pointer. Single-instance by
    /// construction — `init` asserts it isn't already set — so this can't
    /// silently become a race between two loops.
    var global_write_end: std.atomic.Value(Fd) = .init(sys.invalid_fd);

    /// Install the handlers and initialise in place. In place because the
    /// reactor stores `&self.source`.
    pub fn init(
        self: *Signals,
        callback: *const fn (self: *Signals, sig: Signal) void,
    ) Error!void {
        if (sys.is_linux) {
            // Block first. Blocking before creating the signalfd is what
            // guarantees a signal arriving in the window between the two
            // is queued rather than killing the process with its default
            // disposition.
            var set = std.posix.sigemptyset();
            for (watched) |s| std.posix.sigaddset(&set, s.sig());
            std.posix.sigprocmask(std.posix.SIG.BLOCK, &set, null);

            const SFD_NONBLOCK: u32 = 0o4000;
            const SFD_CLOEXEC: u32 = 0o2000000;
            const fd = std.posix.signalfd(-1, &set, SFD_NONBLOCK | SFD_CLOEXEC) catch
                return error.SignalSetupFailed;

            self.* = .{
                .source = .{ .fd = fd, .interest = .readable, .callback = onReady },
                .callback = callback,
            };
            return;
        }

        const p = try sys.pipe();
        errdefer {
            sys.close(p.read_end);
            sys.close(p.write_end);
        }

        // One instance per process: the handler has no way to find a
        // particular Signals, so a second one would overwrite the first's
        // pipe and the first would go deaf.
        if (global_write_end.cmpxchgStrong(sys.invalid_fd, p.write_end, .acq_rel, .acquire) != null) {
            return error.SignalSetupFailed;
        }

        for (watched) |s| {
            const act = std.posix.Sigaction{
                .handler = .{ .handler = handleSignal },
                .mask = std.posix.sigemptyset(),
                // Restart interrupted syscalls: with the handler doing
                // nothing but a pipe write, an EINTR surfacing in the
                // middle of a socket read would be pure noise.
                .flags = std.posix.SA.RESTART,
            };
            std.posix.sigaction(s.sig(), &act, null);
        }

        self.* = .{
            .source = .{ .fd = p.read_end, .interest = .readable, .callback = onReady },
            .callback = callback,
            .write_end = p.write_end,
        };
    }

    pub fn deinit(self: *Signals) void {
        sys.close(self.source.fd);
        if (self.write_end != sys.invalid_fd) {
            _ = global_write_end.cmpxchgStrong(self.write_end, sys.invalid_fd, .acq_rel, .acquire);
            sys.close(self.write_end);
            self.write_end = sys.invalid_fd;
        }
        self.source.fd = sys.invalid_fd;
    }

    /// Async-signal-safe by construction: one non-blocking `write` of one
    /// byte and nothing else. No allocation, no locks, no formatting.
    fn handleSignal(signo: std.posix.SIG) callconv(.c) void {
        const fd = global_write_end.load(.acquire);
        if (fd == sys.invalid_fd) return;
        const byte: [1]u8 = .{@intCast(@intFromEnum(signo) & 0xFF)};
        // A full pipe means a signal is already queued and unread, which
        // is indistinguishable from this one for our purposes. Dropping it
        // is correct; blocking in a handler would not be.
        _ = sys.write(fd, &byte) catch {};
    }

    fn onReady(src: *reactor.Source, ready: reactor.Ready) void {
        if (!ready.read) return;
        const self: *Signals = @fieldParentPtr("source", src);

        if (sys.is_linux) {
            // struct signalfd_siginfo is 128 bytes; the signal number is
            // the first u32. Read several at once so a burst costs one
            // syscall.
            var buf: [8 * 128]u8 = undefined;
            const n = sys.read(src.fd, &buf) catch return;
            var off: usize = 0;
            while (off + 128 <= n) : (off += 128) {
                const signo = std.mem.readInt(u32, buf[off..][0..4], builtin.cpu.arch.endian());
                if (Signal.fromNumber(signo)) |s| self.callback(self, s);
            }
            return;
        }

        var buf: [64]u8 = undefined;
        while (true) {
            const n = sys.read(src.fd, &buf) catch return;
            if (n == 0) return;
            for (buf[0..n]) |b| {
                if (Signal.fromNumber(b)) |s| self.callback(self, s);
            }
            if (n < buf.len) return;
        }
    }
};

/// Ignore `SIGPIPE` process-wide.
///
/// Its default action is to kill the process, and writing to a socket the
/// peer just closed is completely routine for a server — a browser tab
/// closing mid-response does it. With it ignored, the write returns
/// `EPIPE` instead and the connection is torn down like any other error.
///
/// Call this before the first socket exists.
pub fn ignoreSigpipe() void {
    const act = std.posix.Sigaction{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.PIPE, &act, null);
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Send a signal to our own process. `raise` targets the calling thread,
/// which is what we want: the reactor is single-threaded and the test runs
/// on that same thread.
fn raiseSelf(s: Signal) void {
    std.posix.raise(s.sig()) catch unreachable;
}

const Recorder = struct {
    signals: Signals = undefined,
    got: std.ArrayList(Signal) = .empty,
    gpa: std.mem.Allocator,

    fn onSignal(sig: *Signals, s: Signal) void {
        const self: *Recorder = @fieldParentPtr("signals", sig);
        self.got.append(self.gpa, s) catch {};
    }

    fn deinit(self: *Recorder) void {
        self.signals.deinit();
        self.got.deinit(self.gpa);
    }
};

test "SIGTERM arrives as a reactor event, not as a process death" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var rec: Recorder = .{ .gpa = gpa };
    try rec.signals.init(Recorder.onSignal);
    defer rec.deinit();
    try loop.add(&rec.signals.source);
    defer loop.remove(&rec.signals.source);

    // The whole point: the default disposition of SIGTERM is to terminate,
    // so reaching the assertion below at all proves it was intercepted.
    raiseSelf(.term);

    const deadline = sys.monotonicNanos() + 2 * std.time.ns_per_s;
    while (rec.got.items.len == 0 and sys.monotonicNanos() < deadline) {
        _ = try loop.tick(20);
    }

    try testing.expectEqual(@as(usize, 1), rec.got.items.len);
    try testing.expectEqual(Signal.term, rec.got.items[0]);
}

test "each watched signal is distinguished" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var rec: Recorder = .{ .gpa = gpa };
    try rec.signals.init(Recorder.onSignal);
    defer rec.deinit();
    try loop.add(&rec.signals.source);
    defer loop.remove(&rec.signals.source);

    // A reload (HUP) must not be mistaken for a shutdown (TERM), which is
    // the mix-up that would make `docker kill -s HUP` stop the container.
    raiseSelf(.hup);
    raiseSelf(.interrupt);
    raiseSelf(.term);

    const deadline = sys.monotonicNanos() + 3 * std.time.ns_per_s;
    while (rec.got.items.len < 3 and sys.monotonicNanos() < deadline) {
        _ = try loop.tick(20);
    }

    try testing.expectEqual(@as(usize, 3), rec.got.items.len);
    // Signal delivery order for distinct pending signals isn't specified,
    // so assert on the set rather than the sequence.
    var seen_hup = false;
    var seen_int = false;
    var seen_term = false;
    for (rec.got.items) |s| switch (s) {
        .hup => seen_hup = true,
        .interrupt => seen_int = true,
        .term => seen_term = true,
    };
    try testing.expect(seen_hup and seen_int and seen_term);
}

test "signals do not wake an idle loop spuriously" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var rec: Recorder = .{ .gpa = gpa };
    try rec.signals.init(Recorder.onSignal);
    defer rec.deinit();
    try loop.add(&rec.signals.source);
    defer loop.remove(&rec.signals.source);

    // Registering a signal source must not itself make the loop readable.
    // If it did, the idle-CPU property would be gone the moment shutdown
    // handling was wired up.
    const start = sys.monotonicNanos();
    _ = try loop.tick(30);
    const elapsed = sys.monotonicNanos() - start;

    try testing.expect(elapsed >= 20 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), rec.got.items.len);
}

test "a second Signals instance is refused rather than stealing delivery" {
    if (sys.is_linux) return error.SkipZigTest; // signalfd has no global state

    const gpa = testing.allocator;
    var rec: Recorder = .{ .gpa = gpa };
    try rec.signals.init(Recorder.onSignal);
    defer rec.deinit();

    // The self-pipe handler has no context pointer, so a second instance
    // would silently overwrite the first's pipe and the first would stop
    // hearing anything. Fail loudly instead.
    var second: Recorder = .{ .gpa = gpa };
    try testing.expectError(error.SignalSetupFailed, second.signals.init(Recorder.onSignal));
    second.got.deinit(gpa);
}

test "SIGPIPE is ignorable so a closed peer doesn't kill us" {
    ignoreSigpipe();

    const p = try sys.pipe();
    sys.close(p.read_end);
    defer sys.close(p.write_end);

    // Writing to a pipe with no reader raises SIGPIPE, whose default
    // action is to terminate. Reaching the assertion proves it's ignored,
    // and EPIPE is what a socket write sees when a browser tab closes
    // mid-response.
    try testing.expectError(error.BrokenPipe, sys.write(p.write_end, "x"));
}
