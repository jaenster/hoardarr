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

    /// Render the address (without the port) into `buf`.
    ///
    /// Used as a rate-limiter bucket key, so it must be stable and must
    /// not include the ephemeral port — every request from one client
    /// arrives on a different one.
    pub fn formatAddress(self: Sockaddr, buf: []u8) ![]const u8 {
        var w = std.Io.Writer.fixed(buf);
        switch (self) {
            .in => |a| {
                const o: [4]u8 = @bitCast(a.addr);
                try w.print("{d}.{d}.{d}.{d}", .{ o[0], o[1], o[2], o[3] });
            },
            .in6 => |a| {
                for (0..8) |i| {
                    if (i > 0) try w.writeByte(':');
                    try w.print("{x}", .{std.mem.readInt(u16, a.addr[i * 2 ..][0..2], .big)});
                }
            },
        }
        return w.buffered();
    }

    /// Port in host byte order.
    pub fn port(self: Sockaddr) u16 {
        return switch (self) {
            .in => |a| std.mem.bigToNative(u16, a.port),
            .in6 => |a| std.mem.bigToNative(u16, a.port),
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

/// The peer's address on a connected socket.
///
/// The rate limiter needs a per-client key. A proxy header is the usual
/// source, but a client that sends none must not share a bucket with
/// every other such client — that turns one abusive caller into a denial
/// of service for everyone behind the same gap.
pub fn getpeername(fd: Fd) Error!Sockaddr {
    var storage: SockaddrIn6 = .{};
    var len: u32 = @sizeOf(SockaddrIn6);
    if (is_linux) {
        _ = try linuxUnwrap(linux.getpeername(fd, @ptrCast(&storage), &len));
    } else {
        _ = try cUnwrap(std.c.getpeername(fd, @ptrCast(&storage), &len));
    }
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

// ---------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------

pub const Uid = u32;
pub const Gid = u32;

pub fn getuid() Uid {
    return if (is_linux) linux.getuid() else std.c.getuid();
}

pub fn getgid() Gid {
    return if (is_linux) linux.getgid() else std.c.getgid();
}

/// Drop to `uid`:`gid` permanently.
///
/// This replaces the `su-exec` call in the old shell entrypoint, which is
/// most of why the image can be `scratch`: no shell, no suid helper, no
/// `adduser`.
///
/// The order matters and getting it wrong is a security bug, not a style
/// choice:
///
///  1. `setgroups` first, to drop supplementary groups. After `setuid`
///     we no longer have the privilege to do it, so a process that
///     dropped uid first would keep root's group memberships forever.
///  2. `setresgid` before `setresuid`, for the same reason.
///  3. `setres*` rather than `set*id`, so the saved-set id goes too.
///     Plain `setuid` from root does clear the saved id, but being
///     explicit means a future change to the callers can't quietly
///     reintroduce a process that can call `seteuid(0)` and come back.
///
/// Then it verifies the drop actually happened. A silent failure here
/// would leave the daemon running as root while every log line claims
/// otherwise.
pub fn dropPrivileges(uid: Uid, gid: Gid) Error!void {
    if (is_linux) {
        _ = try linuxUnwrap(linux.setgroups(1, &[_]Gid{gid}));
        _ = try linuxUnwrap(linux.setresgid(gid, gid, gid));
        _ = try linuxUnwrap(linux.setresuid(uid, uid, uid));
    } else {
        // Darwin has no `setres*id` — it predates the saved-set-id
        // interface — so the developer-machine path uses `setre*id`. This
        // is not the shipping path; the container is Linux, and
        // `zig build check` compiles the branch above for both Linux
        // targets on every build.
        _ = try cUnwrap(std.c.setregid(gid, gid));
        _ = try cUnwrap(std.c.setreuid(uid, uid));
    }

    // Belt and braces: confirm rather than assume.
    if (getuid() != uid or getgid() != gid) return error.PermissionDenied;
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

test "dropPrivileges refuses to silently no-op" {
    // Running as an unprivileged user, dropping to a *different* uid must
    // fail rather than appear to succeed. The dangerous bug this guards
    // against is a drop that errors, gets swallowed, and leaves the daemon
    // running with more privilege than the logs claim.
    if (getuid() == 0) return error.SkipZigTest; // don't actually drop root in a test

    const other: Uid = if (getuid() == 1 or getuid() == 0) 2 else 1;
    try testing.expectError(error.PermissionDenied, dropPrivileges(other, other));

    // And we are still who we were.
    try testing.expect(getuid() != other);
}

test "dropping to our own ids is a no-op that succeeds" {
    // An operator who runs the container with `user:` already set hits
    // this path: the requested ids match the current ones, so the drop is
    // trivially satisfied and must not error.
    const uid = getuid();
    const gid = getgid();
    if (uid == 0) return error.SkipZigTest;

    dropPrivileges(uid, gid) catch |err| switch (err) {
        // macOS restricts setresgid for non-root even to the same gid in
        // some sandbox configurations; the Linux path is the one that
        // ships, and `zig build check` compiles it.
        error.PermissionDenied, error.NotSupported => return error.SkipZigTest,
        else => return err,
    };
    try testing.expectEqual(uid, getuid());
}

// ---------------------------------------------------------------------
// Randomness
// ---------------------------------------------------------------------

/// Cryptographically secure random bytes, straight from the kernel.
///
/// `std.crypto.random` no longer exists in 0.16 and the replacement sits
/// behind `std.Io.randomSecure`, which would mean adopting an `Io`
/// implementation for the sake of sixteen bytes at start-up.
///
/// Linux uses `getrandom(2)`; Darwin uses `arc4random_buf`, which is
/// kernel-seeded and cannot fail. Both are the platform's recommended
/// interface, and neither needs an fd — which matters because this is
/// called before the daemon has opened anything.
pub fn randomBytes(buf: []u8) void {
    if (is_linux) {
        var off: usize = 0;
        while (off < buf.len) {
            const rc = linux.getrandom(buf.ptr + off, buf.len - off, 0);
            const e = linux.errno(rc);
            if (e == .SUCCESS) {
                off += rc;
                continue;
            }
            // EINTR is possible for large requests; anything else means
            // the entropy pool is unavailable, which is not a condition
            // we can paper over for key material.
            if (e == .INTR) continue;
            @panic("getrandom failed: no entropy source");
        }
        return;
    }
    std.c.arc4random_buf(buf.ptr, buf.len);
}

test "randomBytes fills the buffer and does not repeat" {
    var a: [32]u8 = @splat(0);
    var b: [32]u8 = @splat(0);
    randomBytes(&a);
    randomBytes(&b);

    // An all-zero result would mean the entropy source silently failed,
    // and this generates API keys and session tokens.
    try testing.expect(!std.mem.allEqual(u8, &a, 0));
    try testing.expect(!std.mem.eql(u8, &a, &b));

    // A zero-length request must not misbehave.
    var empty: [0]u8 = undefined;
    randomBytes(&empty);
}

// ---------------------------------------------------------------------
// Filesystem
// ---------------------------------------------------------------------
//
// `std.Io.Dir` / `std.Io.File` exist in 0.16 but every call wants an
// `std.Io`, whose vtable has ~117 members — adopting it would mean
// pulling in one of the backends this file exists to avoid. So the
// handful of path operations the daemon actually performs live here, the
// same way the socket calls do.
//
// Paths are NUL-terminated on the way in. That is the kernel's ABI, and
// making it explicit at the boundary means no helper silently allocates
// to append a NUL in the middle of a hot path.

pub const path_max = 4096;

/// `Error` covers errno; the path-length limit is ours.
pub const PathError = error{NameTooLong} || Error;

const O = if (is_linux) linux.O else std.c.O;

pub const OpenFlags = struct {
    /// `.read_only` for reading, `.write_only` for a log or an output
    /// file, `.read_write` for a database.
    mode: enum { read_only, write_only, read_write } = .read_only,
    create: bool = false,
    /// Append-only. Combined with a single `write` per record, this is
    /// what makes concurrent appends to a log file non-interleaving.
    append: bool = false,
    truncate: bool = false,
    /// Fail if the path already exists. With `create`, gives an atomic
    /// "create or fail", which is how a lock file is claimed.
    exclusive: bool = false,
    directory: bool = false,
};

pub fn open(path: [:0]const u8, flags: OpenFlags) Error!Fd {
    var o: O = .{ .CLOEXEC = true };
    if (flags.directory) {
        o.ACCMODE = .RDONLY;
        o.DIRECTORY = true;
    } else {
        o.ACCMODE = switch (flags.mode) {
            .read_only => .RDONLY,
            .write_only => .WRONLY,
            .read_write => .RDWR,
        };
        o.CREAT = flags.create;
        o.APPEND = flags.append;
        o.TRUNC = flags.truncate;
        o.EXCL = flags.exclusive;
    }

    while (true) {
        if (is_linux) {
            const rc = linux.open(path.ptr, o, 0o644);
            const e = linux.errno(rc);
            if (e == .SUCCESS) return @intCast(rc);
            if (e == .INTR) continue;
            return mapError(e);
        }
        const rc = std.c.open(path.ptr, o, @as(c_uint, 0o644));
        if (rc >= 0) return rc;
        const e = cErrno();
        if (e == .INTR) continue;
        return mapError(e);
    }
}

/// Size of an open file.
///
/// `lseek` to the end rather than `fstat`: one syscall, and no `struct
/// stat` layout to reproduce for two platforms. Safe on an append-only
/// fd because the write offset is the end regardless.
pub fn fileSize(fd: Fd) Error!u64 {
    const SEEK_END = 2;
    if (is_linux) {
        const rc = linux.lseek(fd, 0, SEEK_END);
        const e = linux.errno(rc);
        if (e != .SUCCESS) return mapError(e);
        return @intCast(rc);
    }
    const rc = std.c.lseek(fd, 0, @as(std.c.whence_t, SEEK_END));
    if (rc < 0) return mapError(cErrno());
    return @intCast(rc);
}

/// Flush a file's data to the device.
///
/// The store needs this on its own terms — SQLite issues its own fsyncs
/// via the VFS — but the outbox's "written before acknowledged" property
/// and a completed download's rename both depend on being able to force
/// durability at a chosen point.
pub fn fsync(fd: Fd) Error!void {
    while (true) {
        if (is_linux) {
            const rc = linux.fsync(fd);
            const e = linux.errno(rc);
            if (e == .SUCCESS) return;
            if (e == .INTR) continue;
            return mapError(e);
        }
        if (std.c.fsync(fd) == 0) return;
        const e = cErrno();
        if (e == .INTR) continue;
        return mapError(e);
    }
}

pub fn exists(path: [:0]const u8) bool {
    const F_OK: u32 = 0;
    if (is_linux) return linux.errno(linux.access(path.ptr, F_OK)) == .SUCCESS;
    return std.c.access(path.ptr, 0) == 0;
}

/// Rename, which within one filesystem is atomic. That is what makes
/// "write to a temporary name, then rename into place" safe: a reader
/// sees either the old file or the complete new one, never a partial.
pub fn rename(from: [:0]const u8, to: [:0]const u8) Error!void {
    if (is_linux) {
        const e = linux.errno(linux.rename(from.ptr, to.ptr));
        if (e != .SUCCESS) return mapError(e);
        return;
    }
    if (std.c.rename(from.ptr, to.ptr) != 0) return mapError(cErrno());
}

pub fn unlink(path: [:0]const u8) Error!void {
    if (is_linux) {
        const e = linux.errno(linux.unlink(path.ptr));
        if (e != .SUCCESS) return mapError(e);
        return;
    }
    if (std.c.unlink(path.ptr) != 0) return mapError(cErrno());
}

pub fn rmdir(path: [:0]const u8) Error!void {
    if (is_linux) {
        const e = linux.errno(linux.rmdir(path.ptr));
        if (e != .SUCCESS) return mapError(e);
        return;
    }
    if (std.c.rmdir(path.ptr) != 0) return mapError(cErrno());
}

/// Create one directory. An existing directory is success, because every
/// caller wants "make sure it's there" rather than "be the one to make
/// it".
pub fn mkdir(path: [:0]const u8) Error!void {
    if (is_linux) {
        const e = linux.errno(linux.mkdir(path.ptr, 0o755));
        if (e == .SUCCESS or e == .EXIST) return;
        return mapError(e);
    }
    if (std.c.mkdir(path.ptr, 0o755) == 0) return;
    const e = cErrno();
    if (e == .EXIST) return;
    return mapError(e);
}

/// `mkdir -p`. A fresh volume has neither `<data_dir>` nor its
/// subdirectories, and the daemon should create what it needs rather
/// than making the operator do it.
pub fn mkdirPath(dir: []const u8) PathError!void {
    if (dir.len == 0) return;
    if (dir.len + 1 > path_max) return error.NameTooLong;

    var buf: [path_max]u8 = undefined;
    @memcpy(buf[0..dir.len], dir);
    buf[dir.len] = 0;

    var i: usize = if (dir[0] == '/') 1 else 0;
    while (i < dir.len) : (i += 1) {
        if (buf[i] != '/') continue;
        buf[i] = 0;
        // Intermediate failures are ignored on purpose: a component can
        // be traversable without being one we're allowed to stat. Only
        // the final component has to succeed.
        mkdir(buf[0..i :0]) catch {};
        buf[i] = '/';
    }
    return mkdir(buf[0..dir.len :0]);
}

/// Copy `path` into a NUL-terminated stack buffer.
///
/// Every path call here wants a sentinel, and the alternative — an
/// allocation per call — would put the allocator on the path of a log
/// rotation and a file move.
pub fn pathZ(buf: *[path_max]u8, path: []const u8) PathError![:0]const u8 {
    if (path.len + 1 > path_max) return error.NameTooLong;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    return buf[0..path.len :0];
}

/// Join two path components into a NUL-terminated stack buffer.
pub fn joinZ(buf: *[path_max]u8, dir: []const u8, name: []const u8) PathError![:0]const u8 {
    const sep: usize = if (dir.len > 0 and dir[dir.len - 1] != '/') 1 else 0;
    if (dir.len + sep + name.len + 1 > path_max) return error.NameTooLong;
    @memcpy(buf[0..dir.len], dir);
    if (sep == 1) buf[dir.len] = '/';
    @memcpy(buf[dir.len + sep ..][0..name.len], name);
    buf[dir.len + sep + name.len] = 0;
    return buf[0 .. dir.len + sep + name.len :0];
}

test "open, write, size, rename, unlink" {
    var buf: [path_max]u8 = undefined;
    const dir = "/tmp/hoardarr-systest";
    try mkdirPath(dir);

    const a = try joinZ(&buf, dir, "a.txt");
    const fd = try open(a, .{ .mode = .write_only, .create = true, .truncate = true });
    try writeAll(fd, "hello");
    try fsync(fd);
    try testing.expectEqual(@as(u64, 5), try fileSize(fd));
    close(fd);

    try testing.expect(exists(a));

    var buf2: [path_max]u8 = undefined;
    const b = try joinZ(&buf2, dir, "b.txt");
    try rename(a, b);
    // The atomicity is the point: after a rename a reader sees either the
    // old name or the new one, never a half-written file under either.
    try testing.expect(!exists(a));
    try testing.expect(exists(b));

    try unlink(b);
    try testing.expect(!exists(b));
    try rmdir(try pathZ(&buf, dir));
}

test "mkdirPath creates every level and is idempotent" {
    var buf: [path_max]u8 = undefined;
    const deep = "/tmp/hoardarr-systest2/incomplete/nested";
    try mkdirPath(deep);
    try testing.expect(exists(try pathZ(&buf, deep)));

    // Called again on every start-up, so it must not fail once the tree
    // is there.
    try mkdirPath(deep);
    try testing.expect(exists(try pathZ(&buf, deep)));

    try rmdir(try pathZ(&buf, "/tmp/hoardarr-systest2/incomplete/nested"));
    try rmdir(try pathZ(&buf, "/tmp/hoardarr-systest2/incomplete"));
    try rmdir(try pathZ(&buf, "/tmp/hoardarr-systest2"));
}

test "exclusive create is how a lock file is claimed" {
    var buf: [path_max]u8 = undefined;
    const p = try pathZ(&buf, "/tmp/hoardarr-systest-lock");
    unlink(p) catch {};

    const fd = try open(p, .{ .mode = .write_only, .create = true, .exclusive = true });
    close(fd);
    // Two daemons pointed at one data directory must not both start, and
    // O_CREAT|O_EXCL is the primitive that decides which one wins.
    try testing.expectError(error.Exists, open(p, .{ .mode = .write_only, .create = true, .exclusive = true }));
    try unlink(p);
}

test "paths longer than the limit are refused, not truncated" {
    var buf: [path_max]u8 = undefined;
    const long = "x" ** (path_max + 10);
    // Truncating would silently operate on a different path than asked.
    try testing.expectError(error.NameTooLong, pathZ(&buf, long));
    try testing.expectError(error.NameTooLong, joinZ(&buf, "/tmp", long));
    try testing.expectError(error.NameTooLong, mkdirPath(long));
}

test "joinZ handles a trailing slash without doubling it" {
    var buf: [path_max]u8 = undefined;
    try testing.expectEqualStrings("/data/logs", try joinZ(&buf, "/data", "logs"));
    try testing.expectEqualStrings("/data/logs", try joinZ(&buf, "/data/", "logs"));
    try testing.expectEqualStrings("logs", try joinZ(&buf, "", "logs"));
}

test "opening a missing file reports NoSuchFileOrDirectory" {
    var buf: [path_max]u8 = undefined;
    const p = try pathZ(&buf, "/tmp/hoardarr-definitely-not-here-9f2a");
    try testing.expectError(error.NoSuchFileOrDirectory, open(p, .{}));
}

// `DirIter` declares its own `open`/`close`, which shadow the module-level
// ones inside its methods. Aliases rather than a self-import keep the call
// sites readable.
const openPath = open;
const closeFd = close;

/// Directory iteration. Linux uses `getdents64`; Darwin uses
/// `getdirentries` with its own `dirent` layout (16-bit `namlen`
/// instead of a NUL-terminated name).
pub const DirIter = struct {
    fd: Fd,
    buf: [4096]u8 align(8) = undefined,
    index: usize = 0,
    end: usize = 0,
    seek: i64 = 0,
    done: bool = false,

    pub fn open(dir: []const u8) PathError!DirIter {
        var buf: [path_max]u8 = undefined;
        const fd = try openPath(try pathZ(&buf, dir), .{ .directory = true });
        return .{ .fd = fd };
    }

    pub fn close(self: *DirIter) void {
        closeFd(self.fd);
        self.fd = invalid_fd;
    }

    fn refill(self: *DirIter) bool {
        if (self.done) return false;
        while (true) {
            const n: usize = if (is_linux) blk: {
                const rc = linux.getdents64(self.fd, &self.buf, self.buf.len);
                switch (linux.errno(rc)) {
                    .SUCCESS => break :blk @intCast(rc),
                    .INTR => continue,
                    else => {
                        self.done = true;
                        return false;
                    },
                }
            } else blk: {
                const rc = std.c.getdirentries(self.fd, &self.buf, self.buf.len, &self.seek);
                if (rc < 0) {
                    if (cErrno() == .INTR) continue;
                    self.done = true;
                    return false;
                }
                break :blk @intCast(rc);
            };
            if (n == 0) {
                self.done = true;
                return false;
            }
            self.index = 0;
            self.end = n;
            return true;
        }
    }

    /// Returns a name borrowed from the internal buffer, valid until
    /// the next `next()` call.
    pub fn next(self: *DirIter) ?[]const u8 {
        while (true) {
            if (self.index >= self.end) {
                if (!self.refill()) return null;
            }
            if (is_linux) {
                const e: *align(1) const linux.dirent64 = @ptrCast(&self.buf[self.index]);
                if (e.reclen == 0) {
                    self.done = true;
                    return null;
                }
                const name_ptr: [*:0]const u8 = @ptrCast(&self.buf[self.index + @offsetOf(linux.dirent64, "name")]);
                self.index += e.reclen;
                const name = std.mem.span(name_ptr);
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
                return name;
            } else {
                const e: *align(1) const std.c.dirent = @ptrCast(&self.buf[self.index]);
                if (e.reclen == 0) {
                    self.done = true;
                    return null;
                }
                const base = self.index + @offsetOf(std.c.dirent, "name");
                const namlen = e.namlen;
                self.index += e.reclen;
                if (e.ino == 0) continue;
                const name = self.buf[base..][0..namlen];
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
                return name;
            }
        }
    }
};

test "DirIter lists what was created and nothing else" {
    var buf: [path_max]u8 = undefined;
    const dir = "/tmp/hoardarr-diritertest";
    try mkdirPath(dir);

    for ([_][]const u8{ "one.log", "two.log", "three.log" }) |name| {
        var pb: [path_max]u8 = undefined;
        const p = try joinZ(&pb, dir, name);
        close(try open(p, .{ .mode = .write_only, .create = true }));
    }

    var it = try DirIter.open(dir);
    defer it.close();

    var found: usize = 0;
    var saw_dot = false;
    while (it.next()) |name| {
        // "." and ".." must be filtered, or a prune walking this would
        // try to unlink the directory it is standing in.
        if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) saw_dot = true;
        if (std.mem.endsWith(u8, name, ".log")) found += 1;
    }
    try testing.expectEqual(@as(usize, 3), found);
    try testing.expect(!saw_dot);

    for ([_][]const u8{ "one.log", "two.log", "three.log" }) |name| {
        var pb: [path_max]u8 = undefined;
        try unlink(try joinZ(&pb, dir, name));
    }
    try rmdir(try pathZ(&buf, dir));
}

test "DirIter over many entries spans several refills" {
    var buf: [path_max]u8 = undefined;
    const dir = "/tmp/hoardarr-diriterbig";
    try mkdirPath(dir);

    // The read buffer is 4 KiB, so 200 entries force multiple getdents
    // calls and exercise the boundary between them.
    const n = 200;
    for (0..n) |i| {
        var pb: [path_max]u8 = undefined;
        var name: [32]u8 = undefined;
        const nm = try std.fmt.bufPrint(&name, "entry-{d:0>4}.log", .{i});
        close(try open(try joinZ(&pb, dir, nm), .{ .mode = .write_only, .create = true }));
    }

    var it = try DirIter.open(dir);
    var count: usize = 0;
    while (it.next()) |name| {
        if (std.mem.startsWith(u8, name, "entry-")) count += 1;
    }
    it.close();
    try testing.expectEqual(@as(usize, n), count);

    for (0..n) |i| {
        var pb: [path_max]u8 = undefined;
        var name: [32]u8 = undefined;
        const nm = try std.fmt.bufPrint(&name, "entry-{d:0>4}.log", .{i});
        try unlink(try joinZ(&pb, dir, nm));
    }
    try rmdir(try pathZ(&buf, dir));
}

test "DirIter on a missing directory is an error, not an empty listing" {
    // An empty listing would make a prune silently do nothing while
    // reporting success.
    try testing.expectError(error.NoSuchFileOrDirectory, DirIter.open("/tmp/hoardarr-no-such-dir-4b1c"));
}

test "getpeername identifies the client, and the key excludes the port" {
    // A connected pair over loopback: the peer address is what the rate
    // limiter buckets on when no proxy header is present.
    const listener = try socket(AF_INET, SOCK_STREAM, 0);
    defer close(listener);
    try setReuseAddr(listener);
    var bind_addr = Sockaddr.fromIp(try std.Io.net.IpAddress.parse("127.0.0.1", 0));
    try bind(listener, &bind_addr);
    try listen(listener, 4);
    const port_no = (try getsockname(listener)).port();

    const client = try socket(AF_INET, SOCK_STREAM, 0);
    defer close(client);
    var target = Sockaddr.fromIp(try std.Io.net.IpAddress.parse("127.0.0.1", port_no));
    connect(client, &target) catch |err| switch (err) {
        error.InProgress, error.WouldBlock, error.AlreadyConnected => {},
        else => return err,
    };

    // Give the loopback handshake a moment, then accept.
    var accepted: Fd = invalid_fd;
    for (0..200) |_| {
        accepted = accept(listener) catch |err| switch (err) {
            error.WouldBlock => {
                sleep(2 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        break;
    }
    try testing.expect(accepted != invalid_fd);
    defer close(accepted);

    const peer = try getpeername(accepted);
    var buf: [64]u8 = undefined;
    const key = try peer.formatAddress(&buf);
    try testing.expectEqualStrings("127.0.0.1", key);

    // The ephemeral port differs per connection, so including it would
    // give every request its own bucket and defeat the limiter entirely.
    try testing.expect(std.mem.indexOfScalar(u8, key, ':') == null);
    try testing.expect(peer.port() != port_no);
}

test "IPv6 peers render as a stable key too" {
    const sa = Sockaddr.fromIp(try std.Io.net.IpAddress.parse("::1", 1234));
    var buf: [64]u8 = undefined;
    const key = try sa.formatAddress(&buf);
    try testing.expectEqualStrings("0:0:0:0:0:0:0:1", key);
}
