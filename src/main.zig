const std = @import("std");
const hoardarr = @import("hoardarr");
const build_info = @import("build_info");

const usage =
    \\hoardarr — a Usenet downloader
    \\
    \\Usage: hoardarr <command> [options]
    \\
    \\Commands:
    \\  serve         Run the daemon
    \\  healthcheck   Probe a running daemon; exit 0 when healthy
    \\  version       Print version information
    \\
;

/// Takes `Init.Minimal` rather than the full `Init`: the full one builds
/// an arena, a GPA, an `Io` implementation and an environment map before
/// `main` is even entered, and we want none of those. Our allocator is
/// chosen below, and our I/O is the reactor.
pub fn main(init: std.process.Init.Minimal) !u8 {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer _ = debug_allocator.deinit();
    // Debug builds get leak checking; release builds get the page
    // allocator directly, since every long-lived allocation in the
    // daemon is arena- or pool-backed and a general-purpose free path
    // would only add bookkeeping.
    const gpa = if (@import("builtin").mode == .Debug)
        debug_allocator.allocator()
    else
        std.heap.page_allocator;

    // POSIX hands argv straight to the process; walking it in place costs
    // nothing and needs no allocator.
    var args = init.args.iterate();
    _ = args.next(); // argv[0]
    const cmd = args.next() orelse {
        try writeStderr(usage);
        return 2;
    };

    if (std.mem.eql(u8, cmd, "version")) return cmdVersion();
    if (std.mem.eql(u8, cmd, "serve")) return cmdServe(gpa, init.environ);
    if (std.mem.eql(u8, cmd, "healthcheck")) return cmdHealthcheck();

    try writeStderr(usage);
    return 2;
}

fn cmdVersion() !u8 {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try w.print("hoardarr {s} ({s}) built {s}\nreactor backend: {s}\n", .{
        build_info.version,
        build_info.commit,
        build_info.build_date,
        hoardarr.posix.reactor.backend_name,
    });
    try writeStdout(w.buffered());
    return 0;
}

fn cmdServe(gpa: std.mem.Allocator, env: std.process.Environ) !u8 {
    // Drop privileges before anything else touches the network or the
    // filesystem. This is what the old shell entrypoint used `su-exec`
    // for, and doing it in-process is most of why the image can be
    // `scratch`: no shell, no suid helper, no `adduser`.
    try dropPrivilegesFromEnv(env);

    // The daemon is one reactor thread. Subsystems register their fds and
    // timers with it during startup and the process then blocks here
    // until something happens or a signal arrives.
    var loop: hoardarr.posix.reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    try writeStderr("hoardarr: serve is not wired up yet\n");
    return 1;
}

/// Honour the linuxserver-style `PUID`/`PGID` convention.
///
/// The homelab expectation is that you bind-mount a host directory into
/// `/data` and the files come out owned by your host user, so the ids have
/// to be settable without rebuilding the image.
///
/// Doing nothing when we are already unprivileged is deliberate: an
/// operator who started the container with compose's `user:` has opted
/// into explicit-uid semantics, and none of this applies. Silently trying
/// and failing would be worse than not trying.
fn dropPrivilegesFromEnv(env: std.process.Environ) !void {
    if (sys.getuid() != 0) return;

    const uid = envInt(env, "PUID") orelse 1000;
    const gid = envInt(env, "PGID") orelse 1000;

    // Refuse to keep running as root when asked to. A daemon that parses
    // files off the internet should not be uid 0, and quietly continuing
    // as root because a setuid failed is exactly the outcome to avoid.
    sys.dropPrivileges(uid, gid) catch |err| {
        var buf: [160]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try w.print("hoardarr: refusing to run as root: could not drop to {d}:{d}: {s}\n", .{ uid, gid, @errorName(err) });
        try writeStderr(w.buffered());
        return error.PrivilegeDropFailed;
    };
}

fn envInt(env: std.process.Environ, name: []const u8) ?u32 {
    // getPosix rather than getAlloc: we're POSIX-only and the value is
    // already a NUL-terminated string in the process's environment block,
    // so there is nothing to allocate.
    const raw = env.getPosix(name) orelse return null;
    return std.fmt.parseInt(u32, std.mem.trim(u8, raw, " \t\r\n"), 10) catch null;
}

fn cmdHealthcheck() !u8 {
    try writeStderr("hoardarr: healthcheck is not wired up yet\n");
    return 1;
}

// Straight to the fd. `std.Io.File` would work but drags in the `Io`
// vtable for what is two `write(2)` calls over the process lifetime.
const sys = hoardarr.posix.sys;

fn writeStdout(bytes: []const u8) !void {
    try sys.writeAll(sys.stdout_fd, bytes);
}

fn writeStderr(bytes: []const u8) !void {
    try sys.writeAll(sys.stderr_fd, bytes);
}
