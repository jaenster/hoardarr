//! What the subcommands that talk to a running daemon need in common:
//! where it is, how to authenticate to it, and how to run one HTTP
//! exchange to completion from a process that has no event loop.
//!
//! # Why these subcommands are REST clients
//!
//! Go's `cmd_download` and `cmd_server` built a whole `bootstrap.App` and
//! went straight at SQLite. That was defensible when the CLI was the only
//! way to drive a download; it is not now. The daemon holds the same
//! database open, in WAL mode, with outbox dispatcher threads writing to
//! it — a second process opening it to insert one row buys lock
//! contention and a class of "database is locked" failures that only
//! appear under load. It would also mean duplicating the composition
//! root, since `App` owns the listener, the signal handler and the bus,
//! and there is no half of it that means "just the stores".
//!
//! So `download` and `server` are clients of the daemon's own REST API —
//! the one the UI and every *arr client already use. The visible
//! consequence is that they need the daemon to be running, where Go's
//! did not. That is the trade, and it is the right way round: the CLI is
//! an admin tool, and the daemon is the thing that owns the data.
//!
//! `healthcheck` deliberately does not come through here. It must work
//! in a container whose config file it may not be able to read, so it
//! takes its address from the environment alone.
//!
//! # Loopback only
//!
//! Every request goes to `127.0.0.1`, whatever `server.listen` says. A
//! listen address is a *bind* spec — `:8085` and `0.0.0.0:8085` name the
//! wildcard, which is not an address you can connect to — and the CLI
//! runs beside the daemon by definition. Only the port is taken from it.
//! That also means no DNS resolver is needed here.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const client = @import("../net/http/client.zig");
const config = @import("../core/config.zig");

const Allocator = std.mem.Allocator;
const IpAddress = std.Io.net.IpAddress;

/// Matches `config.Server.listen`'s default and the Dockerfile's
/// `HOARDARR_LISTEN`.
pub const default_listen = ":8085";

/// Matches `bootstrap.run`'s default, which is the container's mount.
pub const default_data_dir = "/data";

/// Largest `config.toml` we will read. The daemon applies the same cap.
const max_config_bytes = 1 << 20;

// ---------------------------------------------------------------------
// Process plumbing
// ---------------------------------------------------------------------

/// Drains the remaining arguments into a slice.
///
/// The iterator hands out `[:0]const u8` pointing at the process's own
/// argv, so nothing is copied — only the slice of slices is allocated,
/// which is what lets every `parseArgs` below be a pure function over
/// `[]const []const u8` and therefore testable without a process.
pub fn collectArgs(gpa: Allocator, it: *std.process.Args.Iterator) Allocator.Error![][]const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(gpa);
    while (it.next()) |a| try out.append(gpa, a);
    return out.toOwnedSlice(gpa);
}

/// Reports `fmt` on stderr and yields the exit code, so a subcommand can
/// `return api.fail(...)` in one statement.
///
/// Plain text rather than a `core/log.zig` record: these lines are read
/// by an operator at a terminal and by shell scripts, and a structured
/// record with a timestamp and a level is noise in both.
pub fn fail(comptime fmt: []const u8, args: anytype) u8 {
    var buf: [1024]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    w.writeAll("hoardarr: ") catch {};
    w.print(fmt, args) catch {};
    w.writeByte('\n') catch {};
    sys.writeAll(sys.stderr_fd, w.buffered()) catch {};
    return 1;
}

pub fn writeStdout(bytes: []const u8) void {
    sys.writeAll(sys.stdout_fd, bytes) catch {};
}

/// One result line on stdout. Message first, then `key=value` fields —
/// the same shape Go's `slog` text handler produced, minus the timestamp
/// and level that only made the output harder to read in a terminal.
pub fn report(comptime fmt: []const u8, args: anytype) void {
    var buf: [2048]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    w.print(fmt, args) catch {};
    w.writeByte('\n') catch {};
    writeStdout(w.buffered());
}

// ---------------------------------------------------------------------
// Endpoint
// ---------------------------------------------------------------------

/// Where to send a request and what to authenticate it with.
pub const Endpoint = struct {
    port: u16,
    /// `server.url_base`, either empty or `/prefix` with no trailing
    /// slash — `config.validate` guarantees that shape.
    url_base: []const u8 = "",
    /// Empty for the public routes; `healthcheck` uses it that way.
    api_key: []const u8 = "",
};

/// The port a `listen` spec binds, be it `:8085`, `0.0.0.0:8085` or
/// `[::]:8085`.
///
/// Splits on the *last* colon so an IPv6 literal's internal colons do
/// not confuse it, which is the same rule Go's healthcheck used.
pub fn listenPort(listen: []const u8) ?u16 {
    const colon = std.mem.lastIndexOfScalar(u8, listen, ':') orelse return null;
    return std.fmt.parseInt(u16, listen[colon + 1 ..], 10) catch null;
}

/// A resolved endpoint plus the config arena backing its strings.
pub const Resolved = struct {
    loaded: config.Loaded,
    endpoint: Endpoint,

    pub fn deinit(self: Resolved) void {
        self.loaded.deinit();
    }
};

pub const ResolveError = error{
    /// `server.listen` had no parseable port.
    BadListen,
    /// The config file exists but could not be read.
    ConfigUnreadable,
} || config.LoadError;

/// Finds the daemon the same way the daemon finds itself: `config.toml`
/// under `HOARDARR_DATA_DIR` (default `/data`), then `HOARDARR_*`
/// overrides on top.
///
/// The API key comes from exactly the same place, in the same order —
/// `HOARDARR_API_KEY` beats the file's `[auth] api_key`. No fallback key
/// is offered: `config.loadFromBytes` mints one when asked, and a CLI
/// minting a key would produce a fresh random string that authenticates
/// against nothing. An absent key therefore fails with `EmptyAPIKey`,
/// which is the truth.
pub fn resolve(
    gpa: Allocator,
    env: std.process.Environ,
    diag: *config.Diagnostic,
) ResolveError!Resolved {
    var env_map = env.createMap(gpa) catch return error.OutOfMemory;
    defer env_map.deinit();

    const data_dir = envOr(env, "HOARDARR_DATA_DIR", default_data_dir);

    var path_buf: [sys.path_max]u8 = undefined;
    const cfg_path = sys.joinZ(&path_buf, data_dir, "config.toml") catch
        return error.ConfigUnreadable;

    const raw = readConfigFile(gpa, cfg_path) catch return error.ConfigUnreadable;
    defer if (raw) |r| gpa.free(r);

    const loaded = try config.loadFromBytes(gpa, raw, data_dir, .{
        .env = &env_map,
        .diag = diag,
    });
    errdefer loaded.deinit();

    const port = listenPort(loaded.config.server.listen) orelse return error.BadListen;
    return .{
        .loaded = loaded,
        .endpoint = .{
            .port = port,
            .url_base = loaded.config.server.url_base,
            .api_key = loaded.config.auth.api_key,
        },
    };
}

pub fn envOr(env: std.process.Environ, name: []const u8, fallback: []const u8) []const u8 {
    const v = env.getPosix(name) orelse return fallback;
    return if (v.len == 0) fallback else v;
}

/// Absent is not an error — a daemon started purely from the environment
/// has no file, and every setting has a default.
fn readConfigFile(gpa: Allocator, path: [:0]const u8) !?[]u8 {
    return readFileAlloc(gpa, path, max_config_bytes) catch |err| switch (err) {
        error.NoSuchFileOrDirectory => null,
        else => err,
    };
}

pub const ReadFileError = error{FileTooBig} || sys.Error || Allocator.Error;

/// Reads a whole file into one allocation.
///
/// Sizing and reading get a descriptor each, deliberately. `sys.fileSize`
/// is an `lseek(SEEK_END)` and leaves the offset at the end of the file,
/// and `sys` exposes no seek to put it back — so a single descriptor
/// would size the file correctly and then read exactly zero bytes, which
/// is a silent empty result rather than an error. `bootstrap` reopens for
/// the same reason.
pub fn readFileAlloc(gpa: Allocator, path: [:0]const u8, max: usize) ReadFileError![]u8 {
    const size = size: {
        const fd = try sys.open(path, .{ .mode = .read_only });
        defer sys.close(fd);
        break :size try sys.fileSize(fd);
    };
    if (size > max) return error.FileTooBig;

    const fd = try sys.open(path, .{ .mode = .read_only });
    defer sys.close(fd);

    const buf = try gpa.alloc(u8, @intCast(size));
    errdefer gpa.free(buf);
    var off: usize = 0;
    while (off < buf.len) {
        const n = try sys.read(fd, buf[off..]);
        // Short of the sized length means the file shrank under us; the
        // truncated prefix is what there is.
        if (n == 0) break;
        off += n;
    }
    return buf[0..off];
}

// ---------------------------------------------------------------------
// One blocking exchange
// ---------------------------------------------------------------------

pub const CallError = client.Error || Allocator.Error || error{
    /// The reactor itself failed, which is not something a retry helps.
    LoopFailed,
};

pub const Call = struct {
    method: client.Method = .get,
    /// Appended to `Endpoint.url_base`; must begin with '/'.
    path: []const u8,
    body: []const u8 = "",
    content_type: []const u8 = "application/json",
    /// Whole-exchange budget. Short by default: the peer is a process on
    /// loopback, so anything slow is a hang, not latency.
    deadline_ns: u64 = 10 * std.time.ns_per_s,
};

/// Runs one request against `ep` and returns the response, which the
/// caller owns and must `deinit`.
///
/// The HTTP client is reactor-driven and completion-callback based, so
/// this spins a loop of its own and ticks it until the callback fires.
/// That is the whole adaptation a synchronous CLI needs: no second
/// client, no blocking socket path to keep in step with the real one.
pub fn call(gpa: Allocator, ep: Endpoint, c: Call) CallError!client.Response {
    std.debug.assert(c.path.len > 0 and c.path[0] == '/');

    const path = try std.mem.concat(gpa, u8, &.{ ep.url_base, c.path });
    defer gpa.free(path);

    var headers: [1]client.Header = undefined;
    var n_headers: usize = 0;
    if (ep.api_key.len != 0) {
        headers[0] = .{ .name = "X-Api-Key", .value = ep.api_key };
        n_headers = 1;
    }

    var loop: reactor.Loop = undefined;
    loop.init(gpa) catch return error.LoopFailed;
    defer loop.deinit();

    var col: Collector = .{};
    var ex: client.Exchange = undefined;
    try ex.start(
        gpa,
        &loop,
        IpAddress.parse("127.0.0.1", ep.port) catch unreachable,
        .{
            .method = c.method,
            .path = path,
            .host = "127.0.0.1",
            .headers = headers[0..n_headers],
            .body = c.body,
            .content_type = c.content_type,
        },
        .{ .deadline_ns = c.deadline_ns },
        Collector.onComplete,
        &col,
    );
    defer ex.deinit();

    // The exchange arms its own deadline timer, so this outer bound only
    // has to be generous enough never to fire first; it exists so a
    // reactor that somehow stops delivering cannot hang a container's
    // HEALTHCHECK forever.
    const started = sys.monotonicNanos();
    while (col.result == null) {
        if (sys.monotonicNanos() - started > c.deadline_ns + std.time.ns_per_s) {
            return error.Timeout;
        }
        _ = loop.tick(20) catch return error.LoopFailed;
    }
    return col.result.?;
}

const Collector = struct {
    result: ?(client.Error!client.Response) = null,

    fn onComplete(ctx: ?*anyopaque, result: client.Error!client.Response) void {
        const self: *Collector = @ptrCast(@alignCast(ctx.?));
        self.result = result;
    }
};

/// Turns a non-2xx into a one-line reason for stderr.
///
/// Prefers the API's own `{"error":"…"}` over the bare status, because
/// "name, host, port required" tells the operator what to fix and "400"
/// does not. Falls back to the status when the body is not ours.
pub fn errorText(resp: *const client.Response, buf: []u8) []const u8 {
    if (extractError(resp.body)) |msg| {
        const n = @min(msg.len, buf.len);
        @memcpy(buf[0..n], msg[0..n]);
        return buf[0..n];
    }
    return std.fmt.bufPrint(buf, "status {d}", .{resp.status}) catch "request failed";
}

/// Scans for a top-level `"error"` string without allocating, so this
/// works on an error path that may itself be out of memory.
fn extractError(body: []const u8) ?[]const u8 {
    const key = "\"error\"";
    const at = std.mem.indexOf(u8, body, key) orelse return null;
    var i = at + key.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':')) i += 1;
    if (i >= body.len or body[i] != '"') return null;
    i += 1;
    const start = i;
    while (i < body.len) : (i += 1) {
        if (body[i] == '\\') {
            i += 1;
            continue;
        }
        if (body[i] == '"') return body[start..i];
    }
    return null;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "listenPort accepts every shape a listen spec takes" {
    try testing.expectEqual(@as(?u16, 8085), listenPort(":8085"));
    try testing.expectEqual(@as(?u16, 8085), listenPort("0.0.0.0:8085"));
    try testing.expectEqual(@as(?u16, 8085), listenPort("127.0.0.1:8085"));
    // Last colon wins, so an IPv6 literal's own colons are not ports.
    try testing.expectEqual(@as(?u16, 9000), listenPort("[::1]:9000"));
    try testing.expectEqual(@as(?u16, null), listenPort("8085"));
    try testing.expectEqual(@as(?u16, null), listenPort(":"));
    try testing.expectEqual(@as(?u16, null), listenPort(""));
    // Out of range, not silently truncated.
    try testing.expectEqual(@as(?u16, null), listenPort(":70000"));
}

test "readFileAlloc returns the whole file, not an empty prefix" {
    const gpa = testing.allocator;
    var path_buf: [sys.path_max]u8 = undefined;
    const path = try sys.scratchDir(&path_buf, "cli-readfile");
    const want =
        \\[auth]
        \\api_key = "0123456789abcdef0123456789abcdef"
        \\
    ;
    {
        const fd = try sys.open(path, .{ .mode = .write_only, .create = true, .truncate = true });
        defer sys.close(fd);
        try sys.writeAll(fd, want);
    }
    defer sys.unlink(path) catch {};

    // Regression: sizing the file leaves the offset at EOF, so reading
    // through the same descriptor returned zero bytes — which read as an
    // empty config and lost the API key rather than failing.
    const got = try readFileAlloc(gpa, path, 1 << 20);
    defer gpa.free(got);
    try testing.expectEqualStrings(want, got);

    try testing.expectError(error.FileTooBig, readFileAlloc(gpa, path, 4));
    try testing.expectError(
        error.NoSuchFileOrDirectory,
        readFileAlloc(gpa, "/tmp/hoardarr-cli-definitely-absent", 1 << 20),
    );
}

test "errorText prefers the API's message over the status" {
    var buf: [128]u8 = undefined;
    var resp: client.Response = .{
        .status = 400,
        .headers = &.{},
        .body = @constCast("{\"error\":\"name, host, port required\"}"),
        .gpa = testing.allocator,
        .head_buf = &.{},
    };
    try testing.expectEqualStrings("name, host, port required", errorText(&resp, &buf));

    resp.body = @constCast("<html>gateway</html>");
    try testing.expectEqualStrings("status 400", errorText(&resp, &buf));

    // A truncated body must not be mistaken for a message.
    resp.body = @constCast("{\"error\":\"unterminated");
    try testing.expectEqualStrings("status 400", errorText(&resp, &buf));
}
