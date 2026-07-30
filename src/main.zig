const std = @import("std");
const hoardarr = @import("hoardarr");
const build_info = hoardarr.build_info;

const cli = hoardarr.cli;

const usage =
    \\hoardarr — a Usenet downloader
    \\
    \\Usage: hoardarr <command> [options]
    \\
    \\Commands:
    \\  serve         Run the daemon
    \\  download      Queue an NZB on a running daemon
    \\  server        Manage Usenet providers (add|list|rm)
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
    if (std.mem.eql(u8, cmd, "healthcheck")) return cli.healthcheck.run(gpa, init.environ);

    // The remaining subcommands take arguments. They parse a plain
    // `[][]const u8` rather than the iterator so that every flag grammar
    // is a pure function with tests; draining it here is the only place
    // that needs an allocator for argv.
    if (std.mem.eql(u8, cmd, "download") or std.mem.eql(u8, cmd, "server")) {
        const rest = try cli.api.collectArgs(gpa, &args);
        defer gpa.free(rest);
        if (std.mem.eql(u8, cmd, "download")) return cli.download.run(gpa, init.environ, rest);
        return cli.server.run(gpa, init.environ, rest);
    }

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
    // Everything about startup order, ownership and shutdown lives in the
    // composition root; `main` only decides which subcommand runs.
    return hoardarr.bootstrap.run(gpa, env);
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
