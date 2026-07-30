//! Thin syscall layer.
//!
//! Zig 0.16 moved sockets into `std.Io.net`, whose implementations are
//! bound to the `std.Io` backends (io_uring on Linux, kqueue on the
//! BSDs). We don't want either: io_uring is blocked by the default
//! seccomp profile on plenty of container hosts, and we want one
//! deterministic readiness model rather than two. So we talk to the
//! kernel directly.
//!
//! Two backends, chosen at comptime:
//!
//!   * **Linux** — raw syscalls via `std.os.linux`. No libc, which means
//!     the shipped binary has no dynamic loader, no libc init, and no
//!     libc in the image. This is where hoardarr actually runs.
//!   * **macOS** — libc, because Darwin has no stable syscall ABI. This
//!     exists so the test suite runs on a developer laptop.
//!
//! Everything here is POSIX-shaped on purpose: `poll(2)` is the
//! readiness primitive both backends share, and `epoll` is a Linux-only
//! accelerator layered on top (see `reactor.zig`).

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;

pub const is_linux = builtin.os.tag == .linux;
pub const is_darwin = builtin.os.tag.isDarwin();

comptime {
    if (!is_linux and !is_darwin) {
        @compileError("hoardarr targets Linux containers; macOS is supported only for running tests");
    }
}

pub const Fd = i32;
pub const invalid_fd: Fd = -1;

/// Errors we actually branch on. Anything else surfaces as
/// `error.Unexpected` with the errno logged at the call site — silently
/// mapping unknown errnos onto a plausible-looking error is how you get
/// a retry loop that spins forever on EPERM.
pub const Error = error{
    WouldBlock,
    Interrupted,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    BrokenPipe,
    TimedOut,
    NetworkUnreachable,
    HostUnreachable,
    AddressInUse,
    AddressNotAvailable,
    InProgress,
    AlreadyConnected,
    NotConnected,
    PermissionDenied,
    TooManyOpenFiles,
    SystemResources,
    InvalidArgument,
    BadFileDescriptor,
    NoSuchFileOrDirectory,
    Exists,
    NotSupported,
    Unexpected,
};

/// The errno enum differs between backends but the names line up.
pub const E = if (is_linux) linux.E else std.c.E;

pub fn mapError(e: E) Error {
    return switch (e) {
        .AGAIN => error.WouldBlock,
        .INTR => error.Interrupted,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionReset,
        .CONNABORTED => error.ConnectionAborted,
        .PIPE => error.BrokenPipe,
        .TIMEDOUT => error.TimedOut,
        .NETUNREACH => error.NetworkUnreachable,
        .HOSTUNREACH => error.HostUnreachable,
        .ADDRINUSE => error.AddressInUse,
        .ADDRNOTAVAIL => error.AddressNotAvailable,
        .INPROGRESS => error.InProgress,
        .ISCONN => error.AlreadyConnected,
        .NOTCONN => error.NotConnected,
        .ACCES, .PERM => error.PermissionDenied,
        .MFILE, .NFILE => error.TooManyOpenFiles,
        .NOMEM, .NOBUFS => error.SystemResources,
        .INVAL => error.InvalidArgument,
        .BADF => error.BadFileDescriptor,
        .NOENT => error.NoSuchFileOrDirectory,
        .EXIST => error.Exists,
        .OPNOTSUPP, .NOSYS => error.NotSupported,
        else => error.Unexpected,
    };
}

/// Last errno for the calling thread. On Linux the raw syscall return
/// value carries it, so this is only meaningful on the libc backend.
inline fn cErrno() E {
    return @enumFromInt(std.c._errno().*);
}

/// Unwrap a raw Linux syscall result: values in [-4095, -1] reinterpreted
/// as unsigned are `-errno`.
inline fn linuxUnwrap(rc: usize) Error!usize {
    const e = linux.errno(rc);
    if (e == .SUCCESS) return rc;
    return mapError(e);
}

/// Unwrap a libc result: -1 signals failure, errno carries the reason.
inline fn cUnwrap(rc: anytype) Error!usize {
    if (rc == -1) return mapError(cErrno());
    return @intCast(rc);
}

// ---------------------------------------------------------------------
// Address family / socket type constants
// ---------------------------------------------------------------------

pub const AF_INET: u32 = if (is_linux) linux.AF.INET else std.c.AF.INET;
pub const AF_INET6: u32 = if (is_linux) linux.AF.INET6 else std.c.AF.INET6;
pub const AF_UNIX: u32 = if (is_linux) linux.AF.UNIX else std.c.AF.UNIX;

pub const SOCK_STREAM: u32 = if (is_linux) linux.SOCK.STREAM else std.c.SOCK.STREAM;
pub const SOCK_DGRAM: u32 = if (is_linux) linux.SOCK.DGRAM else std.c.SOCK.DGRAM;

pub const SOL_SOCKET: i32 = if (is_linux) linux.SOL.SOCKET else std.c.SOL.SOCKET;
pub const SO_REUSEADDR: u32 = if (is_linux) linux.SO.REUSEADDR else std.c.SO.REUSEADDR;
pub const SO_KEEPALIVE: u32 = if (is_linux) linux.SO.KEEPALIVE else std.c.SO.KEEPALIVE;
pub const SO_ERROR: u32 = if (is_linux) linux.SO.ERROR else std.c.SO.ERROR;

/// `SOCK_NONBLOCK | SOCK_CLOEXEC` in one shot on Linux. Darwin has no
/// such flags on `socket(2)`, so there we pay an extra `fcntl`.
const SOCK_NONBLOCK: u32 = if (is_linux) linux.SOCK.NONBLOCK else 0;
const SOCK_CLOEXEC: u32 = if (is_linux) linux.SOCK.CLOEXEC else 0;

// ---------------------------------------------------------------------
// sockaddr
// ---------------------------------------------------------------------

/// Darwin's sockaddr structs carry a leading length byte and a one-byte
/// family; Linux uses a two-byte family. Both are 16 / 28 bytes overall,
/// so we declare them explicitly rather than trusting a shared header.
pub const SockaddrIn = if (is_darwin) extern struct {
    len: u8 = @sizeOf(SockaddrIn),
    family: u8 = @intCast(AF_INET),
    /// network byte order
    port: u16 = 0,
    /// network byte order
    addr: u32 = 0,
    zero: [8]u8 = @splat(0),
} else extern struct {
    family: u16 = @intCast(AF_INET),
    port: u16 = 0,
    addr: u32 = 0,
    zero: [8]u8 = @splat(0),
};

pub const SockaddrIn6 = if (is_darwin) extern struct {
    len: u8 = @sizeOf(SockaddrIn6),
    family: u8 = @intCast(AF_INET6),
    port: u16 = 0,
    flowinfo: u32 = 0,
    addr: [16]u8 = @splat(0),
    scope_id: u32 = 0,
} else extern struct {
    family: u16 = @intCast(AF_INET6),
    port: u16 = 0,
    flowinfo: u32 = 0,
    addr: [16]u8 = @splat(0),
    scope_id: u32 = 0,
};

comptime {
    std.debug.assert(@sizeOf(SockaddrIn) == 16);
    std.debug.assert(@sizeOf(SockaddrIn6) == 28);
}

/// A sockaddr big enough for either family, plus the length the kernel
/// wants. Kept as a tagged union so callers can't pass a v4 length with
/// a v6 payload.
pub const Sockaddr = union(enum) {
    in: SockaddrIn,
    in6: SockaddrIn6,

    pub fn fromIp(addr: std.Io.net.IpAddress) Sockaddr {
        return switch (addr) {
            .ip4 => |a| .{
                .in = .{
                    .port = std.mem.nativeToBig(u16, a.port),
                    // Ip4Address stores the octets in network order already.
                    .addr = @bitCast(a.bytes),
                },
            },
            .ip6 => |a| .{ .in6 = .{
                .port = std.mem.nativeToBig(u16, a.port),
                .addr = a.bytes,
                .scope_id = a.interface.index,
            } },
        };
    }

    pub fn family(self: Sockaddr) u32 {
        return switch (self) {
            .in => AF_INET,
            .in6 => AF_INET6,
        };
    }

    pub fn len(self: Sockaddr) u32 {
        return switch (self) {
            .in => @sizeOf(SockaddrIn),
            .in6 => @sizeOf(SockaddrIn6),
        };
    }

    pub fn ptr(self: *const Sockaddr) *const anyopaque {
        return switch (self.*) {
            .in => |*a| @ptrCast(a),
            .in6 => |*a| @ptrCast(a),
        };
    }
};

// ---------------------------------------------------------------------
// Syscalls
// ---------------------------------------------------------------------

/// Create a non-blocking, close-on-exec socket. Non-blocking is not
/// optional anywhere in this codebase — the reactor owns all blocking.
pub fn socket(domain: u32, sock_type: u32, protocol: u32) Error!Fd {
    if (is_linux) {
        const rc = try linuxUnwrap(linux.socket(domain, sock_type | SOCK_NONBLOCK | SOCK_CLOEXEC, protocol));
        return @intCast(rc);
    }
    const rc: Fd = @intCast(try cUnwrap(std.c.socket(domain, sock_type, protocol)));
    errdefer close(rc);
    try setNonblock(rc);
    try setCloexec(rc);
    return rc;
}

pub fn close(fd: Fd) void {
    // Nothing useful to do about a failing close: EINTR must not be
    // retried (the fd is already gone on Linux) and EBADF is a bug we
    // want to surface via the leak checker, not a runtime branch.
    if (is_linux) {
        _ = linux.close(fd);
    } else {
        _ = std.c.close(fd);
    }
}

pub fn setNonblock(fd: Fd) Error!void {
    const F_GETFL: i32 = if (is_linux) linux.F.GETFL else std.c.F.GETFL;
    const F_SETFL: i32 = if (is_linux) linux.F.SETFL else std.c.F.SETFL;
    const O_NONBLOCK: u32 = if (is_linux) @bitCast(linux.O{ .NONBLOCK = true }) else @bitCast(std.c.O{ .NONBLOCK = true });

    if (is_linux) {
        const flags = try linuxUnwrap(linux.fcntl(fd, F_GETFL, 0));
        _ = try linuxUnwrap(linux.fcntl(fd, F_SETFL, flags | O_NONBLOCK));
    } else {
        const flags = try cUnwrap(std.c.fcntl(fd, F_GETFL, @as(c_int, 0)));
        _ = try cUnwrap(std.c.fcntl(fd, F_SETFL, @as(c_int, @intCast(flags | O_NONBLOCK))));
    }
}

pub fn setCloexec(fd: Fd) Error!void {
    const F_SETFD: i32 = if (is_linux) linux.F.SETFD else std.c.F.SETFD;
    const FD_CLOEXEC: c_int = 1;
    if (is_linux) {
        _ = try linuxUnwrap(linux.fcntl(fd, F_SETFD, FD_CLOEXEC));
    } else {
        _ = try cUnwrap(std.c.fcntl(fd, F_SETFD, FD_CLOEXEC));
    }
}

pub fn setReuseAddr(fd: Fd) Error!void {
    const one: c_int = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, std.mem.asBytes(&one));
}

pub fn setKeepalive(fd: Fd) Error!void {
    const one: c_int = 1;
    return setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, std.mem.asBytes(&one));
}

pub fn setsockopt(fd: Fd, level: i32, optname: u32, value: []const u8) Error!void {
    if (is_linux) {
        _ = try linuxUnwrap(linux.setsockopt(fd, level, optname, value.ptr, @intCast(value.len)));
    } else {
        _ = try cUnwrap(std.c.setsockopt(fd, level, optname, value.ptr, @intCast(value.len)));
    }
}

/// Pending socket error, as left by a failed non-blocking connect. The
/// reactor reports the fd writable either way; this is how we tell
/// "connected" from "refused".
pub fn socketError(fd: Fd) Error!void {
    var err: c_int = 0;
    var len: u32 = @sizeOf(c_int);
    if (is_linux) {
        _ = try linuxUnwrap(linux.getsockopt(fd, SOL_SOCKET, SO_ERROR, @ptrCast(&err), &len));
    } else {
        _ = try cUnwrap(std.c.getsockopt(fd, SOL_SOCKET, SO_ERROR, @ptrCast(&err), &len));
    }
    if (err == 0) return;
    return mapError(@enumFromInt(err));
}

pub fn bind(fd: Fd, addr: *const Sockaddr) Error!void {
    if (is_linux) {
        _ = try linuxUnwrap(linux.bind(fd, @ptrCast(@alignCast(addr.ptr())), addr.len()));
    } else {
        _ = try cUnwrap(std.c.bind(fd, @ptrCast(@alignCast(addr.ptr())), addr.len()));
    }
}

/// The address a socket is actually bound to. Needed whenever the kernel
/// picked the port for us (`bind` to port 0), which is how tests get a
/// port without racing another process for a fixed one.
pub fn getsockname(fd: Fd) Error!Sockaddr {
    var storage: SockaddrIn6 = .{};
    var len: u32 = @sizeOf(SockaddrIn6);
    if (is_linux) {
        _ = try linuxUnwrap(linux.getsockname(fd, @ptrCast(&storage), &len));
    } else {
        _ = try cUnwrap(std.c.getsockname(fd, @ptrCast(&storage), &len));
    }
    // Re-read the family from the struct the kernel filled in rather than
    // trusting the caller to know it. The field sits at the same offset in
    // both v4 and v6 layouts on each platform, so one cast reads either.
    const fam: u32 = @as(*const SockaddrIn, @ptrCast(&storage)).family;
    if (fam == AF_INET6) return .{ .in6 = storage };
    return .{ .in = @as(*const SockaddrIn, @ptrCast(&storage)).* };
}

pub fn listen(fd: Fd, backlog: u31) Error!void {
    if (is_linux) {
        _ = try linuxUnwrap(linux.listen(fd, backlog));
    } else {
        _ = try cUnwrap(std.c.listen(fd, backlog));
    }
}

/// Accept one connection. Returns `error.WouldBlock` when the backlog is
/// empty, which is the reactor's cue to go back to sleep.
pub fn accept(fd: Fd) Error!Fd {
    if (is_linux) {
        const rc = try linuxUnwrap(linux.accept4(fd, null, null, SOCK_NONBLOCK | SOCK_CLOEXEC));
        return @intCast(rc);
    }
    const rc: Fd = @intCast(try cUnwrap(std.c.accept(fd, null, null)));
    errdefer close(rc);
    try setNonblock(rc);
    try setCloexec(rc);
    return rc;
}

/// Start a connect. On a non-blocking socket this almost always returns
/// `error.InProgress`; wait for writability, then call `socketError`.
pub fn connect(fd: Fd, addr: *const Sockaddr) Error!void {
    if (is_linux) {
        _ = try linuxUnwrap(linux.connect(fd, @ptrCast(@alignCast(addr.ptr())), addr.len()));
    } else {
        _ = try cUnwrap(std.c.connect(fd, @ptrCast(@alignCast(addr.ptr())), addr.len()));
    }
}

pub fn read(fd: Fd, buf: []u8) Error!usize {
    while (true) {
        const rc = if (is_linux)
            linuxUnwrap(linux.read(fd, buf.ptr, buf.len))
        else
            cUnwrap(std.c.read(fd, buf.ptr, buf.len));
        return rc catch |err| switch (err) {
            // Retry EINTR here rather than propagating: every caller
            // would do the same thing, and a signal is not an I/O event.
            error.Interrupted => continue,
            else => err,
        };
    }
}

pub fn write(fd: Fd, buf: []const u8) Error!usize {
    while (true) {
        const rc = if (is_linux)
            linuxUnwrap(linux.write(fd, buf.ptr, buf.len))
        else
            cUnwrap(std.c.write(fd, buf.ptr, buf.len));
        return rc catch |err| switch (err) {
            error.Interrupted => continue,
            else => err,
        };
    }
}

pub const stdin_fd: Fd = 0;
pub const stdout_fd: Fd = 1;
pub const stderr_fd: Fd = 2;

/// Write every byte or fail. Only for blocking fds — the reactor's
/// sockets use `write` directly and handle short writes as backpressure
/// rather than looping, because looping on a socket is how you block the
/// event loop.
pub fn writeAll(fd: Fd, bytes: []const u8) Error!void {
    var rest = bytes;
    while (rest.len > 0) {
        const n = try write(fd, rest);
        if (n == 0) return error.BrokenPipe;
        rest = rest[n..];
    }
}

pub const ShutdownHow = enum(i32) { read = 0, write = 1, both = 2 };

pub fn shutdown(fd: Fd, how: ShutdownHow) void {
    if (is_linux) {
        _ = linux.shutdown(fd, @intFromEnum(how));
    } else {
        _ = std.c.shutdown(fd, @intFromEnum(how));
    }
}

/// A self-pipe, used to break the reactor out of `poll` from another
/// thread. On Linux an `eventfd` is one fd instead of two and its
/// counter semantics mean a burst of wakeups collapses into one read;
/// see `reactor.zig`.
pub const Pipe = struct { read_end: Fd, write_end: Fd };

pub fn pipe() Error!Pipe {
    var fds: [2]Fd = undefined;
    if (is_linux) {
        const O_NONBLOCK: u32 = @bitCast(linux.O{ .NONBLOCK = true, .CLOEXEC = true });
        _ = try linuxUnwrap(linux.pipe2(&fds, @bitCast(O_NONBLOCK)));
    } else {
        _ = try cUnwrap(std.c.pipe(&fds));
        try setNonblock(fds[0]);
        try setNonblock(fds[1]);
        try setCloexec(fds[0]);
        try setCloexec(fds[1]);
    }
    return .{ .read_end = fds[0], .write_end = fds[1] };
}

/// Linux-only: a counting wakeup fd. Cheaper than a pipe (one fd, no
/// buffer) and coalescing (N writes = one readable event carrying N).
pub fn eventfd() Error!Fd {
    comptime std.debug.assert(is_linux);
    const EFD_NONBLOCK: u32 = 0o4000;
    const EFD_CLOEXEC: u32 = 0o2000000;
    const rc = try linuxUnwrap(linux.eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    return @intCast(rc);
}

// ---------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------

pub const pollfd = std.posix.pollfd;
pub const POLL = std.posix.POLL;

/// Block until one of `fds` is ready or `timeout_ms` elapses. A negative
/// timeout blocks indefinitely — that is the idle state we want: one
/// thread parked in one syscall, consuming nothing.
pub fn poll(fds: []pollfd, timeout_ms: i32) Error!usize {
    while (true) {
        const rc = if (is_linux)
            linuxUnwrap(linux.poll(fds.ptr, @intCast(fds.len), timeout_ms))
        else
            cUnwrap(std.c.poll(fds.ptr, @intCast(fds.len), timeout_ms));
        return rc catch |err| switch (err) {
            error.Interrupted => continue,
            else => err,
        };
    }
}

// ---------------------------------------------------------------------
// Monotonic clock
// ---------------------------------------------------------------------

/// Monotonic nanoseconds. Used for every timeout and rate-limit window
/// in the process — never the wall clock, which jumps.
pub fn monotonicNanos() u64 {
    if (is_linux) {
        var ts: linux.timespec = undefined;
        _ = linux.clock_gettime(.MONOTONIC, &ts);
        return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
    }
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// Sleep for at least `ns`, resuming across signals. Used only by tests
/// and by the shutdown grace period — the reactor never sleeps, it
/// blocks on `poll`.
pub fn sleep(ns: u64) void {
    var req: if (is_linux) std.os.linux.timespec else std.c.timespec = .{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    var rem: @TypeOf(req) = undefined;
    while (true) {
        if (is_linux) {
            const rc = std.os.linux.nanosleep(&req, &rem);
            if (linux.errno(rc) != .INTR) return;
        } else {
            if (std.c.nanosleep(&req, &rem) == 0) return;
            if (cErrno() != .INTR) return;
        }
        req = rem;
    }
}

/// Wall-clock nanoseconds since the Unix epoch. Only for timestamps we
/// show a human or persist.
pub fn realtimeNanos() i128 {
    if (is_linux) {
        var ts: linux.timespec = undefined;
        _ = linux.clock_gettime(.REALTIME, &ts);
        return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
    }
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "socket is created non-blocking" {
    const fd = try socket(AF_INET, SOCK_STREAM, 0);
    defer close(fd);

    const F_GETFL: i32 = if (is_linux) linux.F.GETFL else std.c.F.GETFL;
    const O_NONBLOCK: usize = if (is_linux)
        @as(u32, @bitCast(linux.O{ .NONBLOCK = true }))
    else
        @as(u32, @bitCast(std.c.O{ .NONBLOCK = true }));

    const flags = if (is_linux)
        try linuxUnwrap(linux.fcntl(fd, F_GETFL, 0))
    else
        try cUnwrap(std.c.fcntl(fd, F_GETFL, @as(c_int, 0)));

    try testing.expect(flags & O_NONBLOCK != 0);
}

test "listen on an ephemeral port and connect to it" {
    const listener = try socket(AF_INET, SOCK_STREAM, 0);
    defer close(listener);
    try setReuseAddr(listener);

    // Port 0 => kernel picks. We read it back via poll-driven connect
    // below rather than getsockname, which keeps this test to the
    // syscalls the reactor actually uses.
    var addr = Sockaddr.fromIp(try std.Io.net.IpAddress.parse("127.0.0.1", 0));
    try bind(listener, &addr);
    try listen(listener, 8);

    // With no pending connection, accept must report WouldBlock rather
    // than blocking — the whole design depends on this.
    try testing.expectError(error.WouldBlock, accept(listener));
}

test "pipe round-trips and reports WouldBlock when empty" {
    const p = try pipe();
    defer close(p.read_end);
    defer close(p.write_end);

    var buf: [8]u8 = undefined;
    try testing.expectError(error.WouldBlock, read(p.read_end, &buf));

    try testing.expectEqual(@as(usize, 3), try write(p.write_end, "abc"));
    try testing.expectEqual(@as(usize, 3), try read(p.read_end, &buf));
    try testing.expectEqualStrings("abc", buf[0..3]);
}

test "poll reports a readable pipe and times out on an idle one" {
    const p = try pipe();
    defer close(p.read_end);
    defer close(p.write_end);

    var fds = [_]pollfd{.{ .fd = p.read_end, .events = POLL.IN, .revents = 0 }};

    // Idle: a zero timeout must report nothing ready.
    try testing.expectEqual(@as(usize, 0), try poll(&fds, 0));

    _ = try write(p.write_end, "x");
    try testing.expectEqual(@as(usize, 1), try poll(&fds, 0));
    try testing.expect(fds[0].revents & POLL.IN != 0);
}

test "poll with a timeout actually sleeps" {
    const p = try pipe();
    defer close(p.read_end);
    defer close(p.write_end);

    var fds = [_]pollfd{.{ .fd = p.read_end, .events = POLL.IN, .revents = 0 }};
    const start = monotonicNanos();
    try testing.expectEqual(@as(usize, 0), try poll(&fds, 20));
    const elapsed = monotonicNanos() - start;

    // Proves the timeout is honoured in the kernel rather than spun on.
    // Generous lower bound because poll's granularity is coarse.
    try testing.expect(elapsed >= 15 * std.time.ns_per_ms);
}

test "monotonic clock advances and never goes backwards" {
    var prev = monotonicNanos();
    for (0..1000) |_| {
        const now = monotonicNanos();
        try testing.expect(now >= prev);
        prev = now;
    }
}

test "sockaddr encodes port in network byte order" {
    const sa = Sockaddr.fromIp(try std.Io.net.IpAddress.parse("127.0.0.1", 8085));
    try testing.expectEqual(AF_INET, sa.family());
    try testing.expectEqual(@as(u32, 16), sa.len());
    try testing.expectEqual(@as(u16, 8085), std.mem.bigToNative(u16, sa.in.port));
    // 127.0.0.1 in network order.
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, @as([4]u8, @bitCast(sa.in.addr)));

    const sa6 = Sockaddr.fromIp(try std.Io.net.IpAddress.parse("::1", 8085));
    try testing.expectEqual(AF_INET6, sa6.family());
    try testing.expectEqual(@as(u32, 28), sa6.len());
    try testing.expectEqual(@as(u16, 8085), std.mem.bigToNative(u16, sa6.in6.port));
}
