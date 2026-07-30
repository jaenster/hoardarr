//! Composition root.
//!
//! The only place that knows which adapter satisfies which port. Every
//! layer below takes its dependencies as parameters, which is what lets
//! them be tested without a database, a socket, or a clock; this file is
//! where the real ones get chosen and wired.
//!
//! ## Startup order, and why it is this order
//!
//! 1. **Drop privileges.** Before anything opens a file or a socket, so
//!    nothing is created owned by root that the runtime user then can't
//!    write.
//! 2. **Read config.** Needs the data directory to exist, so that comes
//!    first. Config decides the log level, so it precedes logging.
//! 3. **Start logging.** Everything after this point can report failure
//!    properly instead of writing to stderr and hoping.
//! 4. **Open and migrate the database.** A failed migration must stop
//!    start-up rather than leave the daemon running against a schema it
//!    doesn't understand.
//! 5. **Install signal handling** before the listener, so a `SIGTERM`
//!    arriving during start-up is still handled cleanly.
//! 6. **Listen**, then run the loop.
//!
//! Shutdown is the reverse, and is why signals are a reactor source
//! rather than a handler: the teardown runs with the full runtime
//! available, on the loop thread, with nothing else in flight.

const std = @import("std");
const build_info = @import("build_info");
const assets = @import("assets");

const sys = @import("posix/sys.zig");
const reactor = @import("posix/reactor.zig");
const signals = @import("posix/signals.zig");
const log = @import("core/log.zig");
const config = @import("core/config.zig");
const http = @import("net/http/server.zig");
const response = @import("net/http/response.zig");
const sqlite = @import("store/sqlite.zig");
const migrate = @import("store/migrate.zig");

const Allocator = std.mem.Allocator;

pub const Error = error{
    ConfigInvalid,
    DatabaseUnavailable,
    ListenFailed,
    PrivilegeDropFailed,
} || Allocator.Error || sys.Error;

/// Everything the daemon owns, in one place so shutdown is a single
/// `deinit` in a defined order rather than scattered defers.
pub const App = struct {
    gpa: Allocator,
    loop: reactor.Loop = undefined,
    db: *sqlite.Conn = undefined,
    server: http.Server = undefined,
    sigs: signals.Signals = undefined,
    cfg: config.Loaded = undefined,

    started: bool = false,

    pub fn deinit(self: *App) void {
        if (!self.started) return;
        self.started = false;

        // Order matters: stop accepting first so nothing new arrives, then
        // drop live connections, then close the database. Closing the
        // database first would leave in-flight handlers dereferencing it.
        self.server.deinit();
        self.loop.remove(&self.sigs.source);
        self.sigs.deinit();
        self.db.close();
        self.loop.deinit();
        self.cfg.deinit();
    }
};

/// Read the config file with our own syscalls.
///
/// `config.loadOrCreate` wants an `std.Io`, and adopting one would pull in
/// exactly the backends `posix/sys.zig` exists to avoid. Reading the file
/// here and handing bytes to `config.loadFromBytes` keeps that boundary.
fn readConfigFile(gpa: Allocator, path: [:0]const u8) !?[]u8 {
    const fd = sys.open(path, .{ .mode = .read_only }) catch |err| switch (err) {
        // A fresh container has no config.toml, which is not an error:
        // every setting has a default.
        error.NoSuchFileOrDirectory => return null,
        else => return err,
    };
    defer sys.close(fd);

    const size = try sys.fileSize(fd);
    if (size > 1 << 20) return error.ConfigInvalid;

    // lseek left the offset at the end; a fresh open would be simpler but
    // this avoids a second syscall pair.
    const buf = try gpa.alloc(u8, @intCast(size));
    errdefer gpa.free(buf);

    const fd2 = try sys.open(path, .{ .mode = .read_only });
    defer sys.close(fd2);
    var off: usize = 0;
    while (off < buf.len) {
        const n = try sys.read(fd2, buf[off..]);
        if (n == 0) break;
        off += n;
    }
    return buf[0..off];
}

/// A 32-character hex API key, for the first start when no config exists.
fn generateApiKey(out: *[32]u8) void {
    var raw: [16]u8 = undefined;
    sys.randomBytes(&raw);
    _ = std.fmt.bufPrint(out, "{x}", .{&raw}) catch unreachable;
}

pub fn run(gpa: Allocator, env: std.process.Environ) !u8 {
    // The config layer takes an environment map rather than the raw block,
    // so HOARDARR_* overrides can be injected in tests without mutating
    // the real environment.
    var env_map = env.createMap(gpa) catch |err| {
        return fatal("cannot read environment: {t}", .{err});
    };
    defer env_map.deinit();

    // ---- 1. privileges ----
    try dropPrivileges(env);

    // ---- 2. config ----
    const data_dir = envOr(env, "HOARDARR_DATA_DIR", "/data");
    sys.mkdirPath(data_dir) catch |err| {
        return fatal("cannot create data directory '{s}': {t}", .{ data_dir, err });
    };

    var path_buf: [sys.path_max]u8 = undefined;
    const cfg_path = sys.joinZ(&path_buf, data_dir, "config.toml") catch {
        return fatal("data directory path is too long", .{});
    };

    const raw = readConfigFile(gpa, cfg_path) catch |err| {
        return fatal("cannot read {s}: {t}", .{ cfg_path, err });
    };
    defer if (raw) |r| gpa.free(r);

    var key_buf: [32]u8 = undefined;
    generateApiKey(&key_buf);

    var diag: config.Diagnostic = .{};
    var loaded = config.loadFromBytes(gpa, raw, data_dir, .{
        .env = &env_map,
        .diag = &diag,
        .api_key_fallback = &key_buf,
    }) catch {
        return fatal("configuration rejected: {s}", .{diag.msg});
    };
    errdefer loaded.deinit();
    const cfg = loaded.config;

    // ---- 3. logging ----
    log.initDefault(parseLevel(cfg.server.log_level), .text);
    log.info("starting", &.{
        log.str("version", build_info.version),
        log.str("commit", build_info.commit),
        log.str("backend", reactor.backend_name),
        log.str("data_dir", cfg.server.data_dir),
    });

    var app: App = .{ .gpa = gpa, .cfg = loaded };
    try app.loop.init(gpa);
    errdefer app.loop.deinit();

    // ---- 4. database ----
    //
    // The resolved data directory can differ from the environment's — a
    // config file or an override may point elsewhere — so create the
    // resolved one rather than assuming the earlier mkdir covered it.
    sys.mkdirPath(cfg.server.data_dir) catch |err| {
        return fatal("cannot create data directory '{s}': {t}", .{ cfg.server.data_dir, err });
    };

    var db_buf: [sys.path_max]u8 = undefined;
    const db_path = sys.joinZ(&db_buf, cfg.server.data_dir, "hoardarr.db") catch {
        return fatal("data directory path is too long", .{});
    };
    app.db = sqlite.Conn.open(gpa, db_path, .{}) catch |err| {
        return fatal("cannot open database {s}: {t}", .{ db_path, err });
    };
    errdefer app.db.close();

    // A schema we don't understand is worse than not starting: the daemon
    // would write rows the next version can't read.
    migrate.migrate(app.db) catch |err| {
        return fatal("migration failed: {t}", .{err});
    };
    log.info("database ready", &.{
        log.str("path", db_path),
        log.uint("schema_version", migrate.currentVersion(app.db) catch 0),
    });

    // ---- 5. signals ----
    signals.ignoreSigpipe();
    try app.sigs.init(onSignal);
    app.sigs.context = &app;
    try app.loop.add(&app.sigs.source);
    errdefer app.loop.remove(&app.sigs.source);

    // ---- 6. listen ----
    app.server.init(gpa, &app.loop, .{}, &routes);
    app.server.app_ctx = &app;
    app.server.url_base = cfg.server.url_base;

    const addr = parseListen(cfg.server.listen) catch {
        return fatal("cannot parse listen address '{s}'", .{cfg.server.listen});
    };
    app.server.listen(addr) catch |err| {
        return fatal("cannot listen on {s}: {t}", .{ cfg.server.listen, err });
    };
    app.started = true;
    defer app.deinit();

    log.info("listening", &.{
        log.str("addr", cfg.server.listen),
        log.boolean("ui_embedded", assets.present),
    });

    try app.loop.run();
    log.info("stopped", &.{});
    return 0;
}

/// `SIGTERM` and `SIGINT` stop the loop, which unwinds through `App.deinit`
/// with the runtime fully available. `SIGHUP` is accepted and logged but
/// does not reload yet — silently ignoring it would look like a hang.
fn onSignal(s: *signals.Signals, sig: signals.Signal) void {
    const app: *App = @ptrCast(@alignCast(s.context.?));
    switch (sig) {
        .term, .interrupt => {
            log.info("shutting down", &.{log.str("signal", @tagName(sig))});
            app.loop.stop();
        },
        .hup => log.warn("SIGHUP received; configuration reload is not implemented", &.{}),
    }
}

fn dropPrivileges(env: std.process.Environ) !void {
    if (sys.getuid() != 0) return;
    const uid = envInt(env, "PUID") orelse 1000;
    const gid = envInt(env, "PGID") orelse 1000;
    sys.dropPrivileges(uid, gid) catch |err| {
        _ = fatal("refusing to run as root: cannot drop to {d}:{d}: {t}", .{ uid, gid, err }) catch {};
        return error.PrivilegeDropFailed;
    };
}

fn envOr(env: std.process.Environ, name: []const u8, fallback: []const u8) []const u8 {
    const v = env.getPosix(name) orelse return fallback;
    return if (v.len == 0) fallback else v;
}

fn envInt(env: std.process.Environ, name: []const u8) ?u32 {
    const raw = env.getPosix(name) orelse return null;
    return std.fmt.parseInt(u32, std.mem.trim(u8, raw, " \t\r\n"), 10) catch null;
}

fn parseLevel(text: []const u8) log.Level {
    if (std.ascii.eqlIgnoreCase(text, "debug")) return .debug;
    if (std.ascii.eqlIgnoreCase(text, "warn")) return .warn;
    if (std.ascii.eqlIgnoreCase(text, "error")) return .err;
    return .info;
}

/// Parse a `host:port` or `:port` listen string.
///
/// A bare `:port` binds all interfaces, matching the Go behaviour and what
/// every existing `config.toml` says.
pub fn parseListen(spec: []const u8) !std.Io.net.IpAddress {
    const colon = std.mem.lastIndexOfScalar(u8, spec, ':') orelse return error.InvalidListen;
    const host = spec[0..colon];
    const port = std.fmt.parseInt(u16, spec[colon + 1 ..], 10) catch return error.InvalidListen;
    if (host.len == 0) return std.Io.net.IpAddress.parse("0.0.0.0", port);
    // Strip brackets from an IPv6 literal.
    const bare = if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']')
        host[1 .. host.len - 1]
    else
        host;
    return std.Io.net.IpAddress.parse(bare, port);
}

/// Report a start-up failure to stderr and return a non-zero exit code.
///
/// Deliberately stderr rather than the logger: most of these fire before
/// logging is configured, and a container that dies at start-up should say
/// why on the console the operator is already looking at.
fn fatal(comptime fmt: []const u8, args: anytype) !u8 {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    w.print("hoardarr: " ++ fmt ++ "\n", args) catch {};
    sys.writeAll(sys.stderr_fd, w.buffered()) catch {};
    return 1;
}

// ---------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------

const routes = [_]http.Route{
    .{ .method = .get, .path = "/healthz", .handler = handleHealthz, .access = .public },
    .{ .method = .get, .path = "/api/v1/version", .handler = handleVersion, .access = .public },
    // Catch-all last: an exact match always wins, so this only sees what
    // nothing else claimed.
    .{ .path = "/", .kind = .prefix, .handler = handleAsset, .access = .public },
};

fn handleHealthz(ctx: *http.Ctx) http.HandlerError!void {
    try ctx.res.send(200, "application/json", "{\"status\":\"ok\"}");
}

fn handleVersion(ctx: *http.Ctx) http.HandlerError!void {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    // A fixed writer's only failure is a full buffer, which for build
    // metadata means someone set an absurd -Dversion. Report it rather
    // than mapping it onto an unrelated response error.
    w.print(
        "{{\"version\":\"{f}\",\"commit\":\"{f}\",\"backend\":\"{f}\"}}",
        .{
            std.zig.fmtString(build_info.version),
            std.zig.fmtString(build_info.commit),
            std.zig.fmtString(reactor.backend_name),
        },
    ) catch {
        try ctx.res.send(500, "application/json", "{\"error\":\"version metadata too long\"}");
        return;
    };
    try ctx.res.send(200, "application/json", w.buffered());
}

/// Serve the embedded frontend.
///
/// Assets were gzipped at build time, so a client that accepts gzip gets
/// the pre-compressed bytes and the daemon never runs a compressor. An
/// unknown path falls back to `index.html` rather than 404ing, because the
/// UI is a single-page app and its routes only exist client-side.
fn handleAsset(ctx: *http.Ctx) http.HandlerError!void {
    if (!assets.present) {
        try ctx.res.send(200, "text/html; charset=utf-8",
            \\<!doctype html><meta charset=utf-8><title>hoardarr</title>
            \\<p>No frontend bundle in this build. Rebuild with
            \\<code>zig build -Dembed-ui=true</code> after
            \\<code>cd frontend &amp;&amp; npm run build</code>.
        );
        return;
    }

    const req_path = ctx.req.path;
    const path = if (req_path.len == 0 or std.mem.eql(u8, req_path, "/")) "/index.html" else req_path;
    const asset = assets.find(path) orelse assets.find("/index.html") orelse {
        try ctx.res.send(404, "application/json", "{\"error\":\"not found\"}");
        return;
    };

    if (asset.gz) |gz| {
        if (acceptsGzip(ctx)) {
            try ctx.res.setHeader("Content-Encoding", "gzip");
            // Any cache in front of us must not serve these bytes to a
            // client that didn't ask for gzip.
            try ctx.res.setHeader("Vary", "Accept-Encoding");
            try ctx.res.send(200, asset.content_type, gz);
            return;
        }
    }
    try ctx.res.send(200, asset.content_type, asset.raw);
}

fn acceptsGzip(ctx: *http.Ctx) bool {
    const v = ctx.req.header("accept-encoding") orelse return false;
    return std.mem.indexOf(u8, v, "gzip") != null;
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "listen strings parse the way existing configs are written" {
    // ":8085" is what every config.toml in the wild says, and it has to
    // mean "all interfaces" rather than being rejected.
    const any = try parseListen(":8085");
    try testing.expectEqual(@as(u16, 8085), any.getPort());

    const local = try parseListen("127.0.0.1:9000");
    try testing.expectEqual(@as(u16, 9000), local.getPort());
    try testing.expect(local == .ip4);

    const v6 = try parseListen("[::1]:9000");
    try testing.expectEqual(@as(u16, 9000), v6.getPort());
    try testing.expect(v6 == .ip6);
}

test "a malformed listen string is refused, not defaulted" {
    // Defaulting would silently bind somewhere the operator didn't ask
    // for, which for a service with an API key is a security surprise.
    try testing.expectError(error.InvalidListen, parseListen("8085"));
    try testing.expectError(error.InvalidListen, parseListen(":notaport"));
    try testing.expectError(error.InvalidListen, parseListen(""));
}

test "log level parsing matches the config vocabulary" {
    try testing.expectEqual(log.Level.debug, parseLevel("debug"));
    try testing.expectEqual(log.Level.info, parseLevel("info"));
    try testing.expectEqual(log.Level.warn, parseLevel("warn"));
    try testing.expectEqual(log.Level.err, parseLevel("error"));
    // Normalisation lowercases, but an unrecognised value must not turn
    // logging off — it falls back to info.
    try testing.expectEqual(log.Level.info, parseLevel("LOUD"));
    try testing.expectEqual(log.Level.info, parseLevel(""));
}

test "generated api keys are 32 hex characters and differ" {
    var a: [32]u8 = undefined;
    var b: [32]u8 = undefined;
    generateApiKey(&a);
    generateApiKey(&b);

    for (a) |c| try testing.expect(std.ascii.isHex(c));
    // A fixed key across installs would be a default credential.
    try testing.expect(!std.mem.eql(u8, &a, &b));
}
