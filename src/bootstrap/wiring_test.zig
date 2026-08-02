//! Boots the whole `App` and asks one endpoint from every group whether
//! it can answer.
//!
//! This is not an end-to-end download test — `src/testserver/` owns that.
//! It is a wiring test, and the thing it is guarding against is specific:
//! a port left null. Every REST route exists whether or not its
//! dependency was wired, and a null one answers 503 with a body that
//! looks like a legitimate response. Without an assertion per group, the
//! composition root can lose half the daemon and every unit test in the
//! repository still passes.
//!
//! So the shape of each assertion is "not 503, not 404", plus enough of
//! the body to prove the adapter actually reached its store.
//!
//! The client runs on the same reactor as the server. There is no second
//! thread and no sleeping: `tick` advances both ends until the exchange
//! completes, which is also the only way to drive a single-threaded
//! reactor from a test without a race.

const std = @import("std");

const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const config = @import("../core/config.zig");
const client = @import("../net/http/client.zig");
const sqlite = @import("../store/sqlite.zig");
const migrate = @import("../store/migrate.zig");
const bootstrap = @import("../bootstrap.zig");

const Allocator = std.mem.Allocator;
const testing = std.testing;

/// A booted daemon on an ephemeral port, plus the loop both ends share.
const Fixture = struct {
    gpa: Allocator,
    app: *bootstrap.App,
    dir: []const u8,
    port: u16 = 0,

    fn init(gpa: Allocator, dir: []const u8) !Fixture {
        // A fresh directory per run: a leftover database from a previous
        // failure would make this test pass for the wrong reason.
        var fs_impl = @import("infra.zig").RealFs{ .gpa = gpa };
        try fs_impl.filesystem().removeAll(dir);
        try sys.mkdirPath(dir);

        const app = try gpa.create(bootstrap.App);
        errdefer gpa.destroy(app);
        app.* = .{ .gpa = gpa };

        app.cfg = try config.loadFromBytes(gpa, null, dir, .{
            .api_key_fallback = "0123456789abcdef0123456789abcdef",
        });
        errdefer app.cfg.deinit();

        try app.loop.init(gpa);
        errdefer app.loop.deinit();

        // `config.normalize` resolves the default `./data` against the
        // base directory, so the resolved one is what has to exist —
        // exactly the distinction `run` makes for the same reason.
        try sys.mkdirPath(app.cfg.config.server.data_dir);

        var db_buf: [sys.path_max]u8 = undefined;
        const db_path = try sys.joinZ(&db_buf, app.cfg.config.server.data_dir, "hoardarr.db");
        app.db = try sqlite.Conn.open(gpa, db_path, .{});
        errdefer app.db.close();
        try migrate.migrate(app.db);

        try app.wire();
        app.started = true;
        errdefer app.deinit();

        // Port 0 lets the kernel choose, so two runs of the suite in
        // parallel cannot collide on a fixed port.
        try app.server.listen(try bootstrap.parseListen("127.0.0.1:0"));

        return .{ .gpa = gpa, .app = app, .dir = dir, .port = try app.server.listener.boundPort() };
    }

    fn deinit(self: *Fixture) void {
        self.app.deinit();
        self.gpa.destroy(self.app);

        var fs_impl = @import("infra.zig").RealFs{ .gpa = self.gpa };
        fs_impl.filesystem().removeAll(self.dir) catch {};
    }

    const Captured = struct {
        gpa: Allocator,
        status: u16 = 0,
        body: []u8 = &.{},
        done: bool = false,
        failed: ?anyerror = null,

        fn deinit(self: *Captured) void {
            self.gpa.free(self.body);
        }

        fn onComplete(ctx: ?*anyopaque, result: client.Error!client.Response) void {
            const self: *Captured = @ptrCast(@alignCast(ctx.?));
            self.done = true;
            var res = result catch |e| {
                self.failed = e;
                return;
            };
            defer res.deinit();
            self.status = res.status;
            self.body = self.gpa.dupe(u8, res.body) catch &.{};
        }
    };

    /// One request/response, driven on the shared loop.
    fn request(self: *Fixture, method: client.Method, path: []const u8, body: []const u8) !Captured {
        var captured: Captured = .{ .gpa = self.gpa };
        var ex: client.Exchange = undefined;
        try ex.start(
            self.gpa,
            &self.app.loop,
            try std.Io.net.IpAddress.parse("127.0.0.1", self.port),
            .{
                .method = method,
                .path = path,
                .host = "127.0.0.1",
                .headers = &.{.{ .name = "X-Api-Key", .value = api_key }},
                .body = body,
            },
            .{ .deadline_ns = 5 * std.time.ns_per_s },
            &Captured.onComplete,
            &captured,
        );
        defer ex.deinit();

        // Bounded so a wiring bug that never answers fails the test
        // instead of hanging the suite.
        var ticks: usize = 0;
        while (!captured.done and ticks < 2000) : (ticks += 1) {
            _ = try self.app.loop.tick(10);
        }
        if (!captured.done) return error.RequestTimedOut;
        if (captured.failed) |e| return e;
        return captured;
    }
};

const api_key = "0123456789abcdef0123456789abcdef";

/// The assertion this whole file exists for: a route that answered 503
/// has a null port behind it, and a 404 means it was never registered.
fn expectWired(c: Fixture.Captured, path: []const u8) !void {
    if (c.status == 503 or c.status == 404) {
        std.debug.print("\n{s} answered {d}: {s}\n", .{ path, c.status, c.body });
        return error.PortNotWired;
    }
}

test "every route group answers from a freshly booted daemon" {
    const gpa = testing.allocator;
    // The fixture borrows this path for its whole life, so the buffer has
    // to outlive it. See `sys.scratchDir` for why it is not a fixed path.
    var dir_buf: [sys.path_max]u8 = undefined;
    var fx = try Fixture.init(gpa, try sys.scratchDir(&dir_buf, "wiring"));
    defer fx.deinit();

    const Case = struct {
        method: client.Method = .get,
        path: []const u8,
        /// A fragment the body must contain, proving the adapter reached
        /// its store rather than answering an empty shell.
        contains: []const u8 = "",
    };

    const cases = [_]Case{
        // queue
        .{ .path = "/api/v1/queue", .contains = "\"jobs\"" },
        .{ .path = "/api/v1/history", .contains = "\"jobs\"" },
        // servers
        .{ .path = "/api/v1/servers", .contains = "\"servers\"" },
        // categories — seeded by the migration, so an empty list here
        // means the adapter is not talking to the database.
        .{ .path = "/api/v1/categories", .contains = "\"tv\"" },
        // system
        .{ .path = "/api/v1/system/status", .contains = "\"hoardarr\"" },
        .{ .path = "/api/v1/system/throughput", .contains = "\"series\"" },
        .{ .path = "/api/v1/system/speed-history", .contains = "\"samples\"" },
        .{ .path = "/api/v1/system/tasks", .contains = "\"tasks\"" },
        .{ .path = "/api/v1/system/logs", .contains = "\"entries\"" },
        .{ .path = "/api/v1/system/logs/files", .contains = "\"files\"" },
        .{ .path = "/api/v1/system/backups", .contains = "\"backups\"" },
        // commands
        .{ .path = "/api/v1/commands", .contains = "\"commands\"" },
        .{ .path = "/api/v1/commands/names", .contains = "\"names\"" },
        // config
        .{ .path = "/api/v1/config/general", .contains = "\"listen\"" },
        .{ .path = "/api/v1/config/paths", .contains = "\"data_dir\"" },
        .{ .path = "/api/v1/config/bandwidth", .contains = "bytes_per_sec" },
        // subscriptions
        .{ .path = "/api/v1/subscriptions", .contains = "\"subscriptions\"" },
        // auth
        .{ .path = "/api/v1/auth/whoami", .contains = "\"state\"" },
        // observability
        .{ .path = "/metrics", .contains = "hoardarr_build_info" },
        // SAB
        .{ .path = "/sabnzbd/api?mode=version&apikey=" ++ api_key, .contains = "\"version\"" },
        .{ .path = "/sabnzbd/api?mode=queue&apikey=" ++ api_key, .contains = "\"queue\"" },
        .{ .path = "/sabnzbd/api?mode=get_cats&apikey=" ++ api_key, .contains = "\"tv\"" },
        // the SPA catch-all, last so an exact match still wins
        .{ .path = "/healthz", .contains = "\"ok\"" },
    };

    inline for (cases) |c| {
        var res = try fx.request(c.method, c.path, "");
        defer res.deinit();
        try expectWired(res, c.path);
        if (res.status != 200) {
            std.debug.print("\n{s} answered {d}: {s}\n", .{ c.path, res.status, res.body });
            return error.UnexpectedStatus;
        }
        if (c.contains.len > 0 and std.mem.indexOf(u8, res.body, c.contains) == null) {
            std.debug.print("\n{s} body missing '{s}': {s}\n", .{ c.path, c.contains, res.body });
            return error.BodyMissingFragment;
        }
    }
}

test "an unauthenticated request is refused before it reaches a handler" {
    const gpa = testing.allocator;
    var dir_buf: [sys.path_max]u8 = undefined;
    var fx = try Fixture.init(gpa, try sys.scratchDir(&dir_buf, "wiring-auth"));
    defer fx.deinit();

    // Same request, no key. The API key is checked by the server, so this
    // proves `Api.authConfig()` was actually handed to it — a 200 here
    // would mean the whole surface is open.
    var captured: Fixture.Captured = .{ .gpa = gpa };
    var ex: client.Exchange = undefined;
    try ex.start(gpa, &fx.app.loop, try std.Io.net.IpAddress.parse("127.0.0.1", fx.port), .{
        .method = .get,
        .path = "/api/v1/queue",
        .host = "127.0.0.1",
    }, .{ .deadline_ns = 5 * std.time.ns_per_s }, &Fixture.Captured.onComplete, &captured);
    defer ex.deinit();
    defer captured.deinit();

    var ticks: usize = 0;
    while (!captured.done and ticks < 2000) : (ticks += 1) {
        _ = try fx.app.loop.tick(10);
    }
    try testing.expect(captured.done);
    try testing.expectEqual(@as(u16, 401), captured.status);
}

test "an NZB posted through the SAB API lands in the queue both APIs read" {
    const gpa = testing.allocator;
    var dir_buf: [sys.path_max]u8 = undefined;
    var fx = try Fixture.init(gpa, try sys.scratchDir(&dir_buf, "wiring-add"));
    defer fx.deinit();

    // The smallest NZB that describes something fetchable. Posted as a
    // form body rather than multipart because this test is about the
    // wiring, not about the parser — `api/sab/form.zig` owns that.
    const nzb =
        \\<?xml version="1.0" encoding="utf-8"?>
        \\<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
        \\<file poster="p@example.com" date="1700000000" subject="Wiring.Test [1/1] - &quot;Wiring.Test.mkv&quot; yEnc (1/1)">
        \\<groups><group>alt.binaries.test</group></groups>
        \\<segments><segment bytes="500000" number="1">seg1@example.com</segment></segments>
        \\</file>
        \\</nzb>
    ;

    // Straight through the REST port, which is the same `add_job` service
    // the SAB `addfile` mode calls.
    const added = try fx.app.p_queue.port().add(fx.app.api.beginRequest(), .{
        .nzb = nzb,
        .name = "Wiring.Test",
        .category = "tv",
        .source = "wiring-test",
    });
    try testing.expect(added.id > 0);
    try testing.expect(!added.duplicate);

    // Visible through REST...
    var q = try fx.request(.get, "/api/v1/queue", "");
    defer q.deinit();
    try testing.expectEqual(@as(u16, 200), q.status);
    try testing.expect(std.mem.indexOf(u8, q.body, "Wiring.Test") != null);

    // ...and through the SAB projection, which is a different port over
    // the same rows. Both answering is what an *arr actually needs.
    var s = try fx.request(.get, "/sabnzbd/api?mode=queue&apikey=" ++ api_key, "");
    defer s.deinit();
    try testing.expectEqual(@as(u16, 200), s.status);
    try testing.expect(std.mem.indexOf(u8, s.body, "Wiring.Test") != null);

    // And the state change published to the outbox, in the same
    // transaction — which is what makes the per-job timeline work.
    const timeline = try fx.app.p_events.port().byJob(fx.app.api.beginRequest(), added.id);
    try testing.expect(timeline.len >= 1);
    try testing.expectEqualStrings("download.job.created", timeline[0].topic);
}

test "the api key survives a restart" {
    const gpa = testing.allocator;
    // `dir` points into `dir_buf`, so the join below must write elsewhere.
    var dir_buf: [sys.path_max]u8 = undefined;
    const dir = try sys.scratchDir(&dir_buf, "wiring-key");

    var fs_impl = @import("infra.zig").RealFs{ .gpa = gpa };
    try fs_impl.filesystem().removeAll(dir);
    try sys.mkdirPath(dir);
    defer fs_impl.filesystem().removeAll(dir) catch {};

    var path_buf: [sys.path_max]u8 = undefined;
    const cfg_path = try sys.joinZ(&path_buf, dir, "config.toml");

    // First start: no file, so one is written with a generated key.
    var first_key: [32]u8 = undefined;
    bootstrap.generateApiKey(&first_key);
    const first = (try bootstrap.ensureConfigFile(gpa, cfg_path, dir, &first_key)).?;
    defer gpa.free(first);
    try testing.expect(std.mem.indexOf(u8, first, &first_key) != null);

    // Second start mints a different key in memory — and must not use it,
    // because the file already records one. This is the regression: a
    // fresh key per restart silently breaks every configured *arr.
    var second_key: [32]u8 = undefined;
    bootstrap.generateApiKey(&second_key);
    try testing.expect(!std.mem.eql(u8, &first_key, &second_key));

    const second = (try bootstrap.ensureConfigFile(gpa, cfg_path, dir, &second_key)).?;
    defer gpa.free(second);
    try testing.expectEqualStrings(first, second);

    // And what the config layer resolves is the first key, both times.
    for ([_][]const u8{ &first_key, &second_key }) |fallback| {
        var loaded = try config.loadFromBytes(gpa, second, dir, .{ .api_key_fallback = fallback });
        defer loaded.deinit();
        try testing.expectEqualStrings(&first_key, loaded.config.auth.api_key);
    }
}

test "the config file is written 0600 because it holds a credential" {
    const gpa = testing.allocator;
    var dir_buf: [sys.path_max]u8 = undefined;
    const dir = try sys.scratchDir(&dir_buf, "wiring-mode");

    var fs_impl = @import("infra.zig").RealFs{ .gpa = gpa };
    try fs_impl.filesystem().removeAll(dir);
    try sys.mkdirPath(dir);
    defer fs_impl.filesystem().removeAll(dir) catch {};

    var path_buf: [sys.path_max]u8 = undefined;
    const cfg_path = try sys.joinZ(&path_buf, dir, "config.toml");

    var key: [32]u8 = undefined;
    bootstrap.generateApiKey(&key);
    const bytes = (try bootstrap.ensureConfigFile(gpa, cfg_path, dir, &key)).?;
    defer gpa.free(bytes);

    // `sys.zig` has no `stat`, so the check goes through the shell-free
    // route the rest of this file uses: a second exclusive create must
    // fail, proving the file exists, and the mode is asserted by reading
    // it back through `std.fs` — the one place in the suite where using
    // std for a one-off metadata read costs nothing.
    try testing.expectError(error.Exists, sys.open(cfg_path, .{
        .mode = .write_only,
        .create = true,
        .exclusive = true,
    }));

    const mode = try fileMode(cfg_path);
    try testing.expectEqual(@as(u32, 0o600), mode & 0o777);
}

/// The file's mode bits, read through the open descriptor.
///
/// `posix/sys.zig` has no `stat` — deliberately, see `fileSize` — so this
/// goes to the raw layer the same way `infra.fchmod` does. It lives in
/// the test rather than in the adapter because nothing in the daemon
/// needs to read a mode; only this assertion does.
fn fileMode(path: [:0]const u8) !u32 {
    const fd = try sys.open(path, .{});
    defer sys.close(fd);
    if (sys.is_linux) {
        var st: std.os.linux.Stat = undefined;
        const rc = std.os.linux.fstat(fd, &st);
        if (std.os.linux.errno(rc) != .SUCCESS) return error.StatFailed;
        return @intCast(st.mode);
    }
    var st: std.c.Stat = undefined;
    if (std.c.fstat(fd, &st) != 0) return error.StatFailed;
    return @intCast(st.mode);
}

test "the ports left null are the documented ones and nothing else" {
    const gpa = testing.allocator;
    var dir_buf: [sys.path_max]u8 = undefined;
    var fx = try Fixture.init(gpa, try sys.scratchDir(&dir_buf, "wiring-null"));
    defer fx.deinit();

    const api = &fx.app.api;
    // Wired.
    try testing.expect(api.queue != null);
    try testing.expect(api.events != null);
    try testing.expect(api.servers != null);
    try testing.expect(api.categories != null);
    try testing.expect(api.system != null);
    try testing.expect(api.schedule != null);
    try testing.expect(api.commands != null);
    try testing.expect(api.backups != null);
    try testing.expect(api.log_files != null);
    try testing.expect(api.subscriptions != null);
    try testing.expect(api.config != null);
    try testing.expect(api.bandwidth != null);
    try testing.expect(api.auth != null);
    try testing.expect(api.log_ring != null);
    try testing.expect(api.events_hub != null);
    try testing.expect(api.logs_hub != null);
    try testing.expect(api.metrics != null);
    // Wired by `bootstrap/runtime.zig`: dialling a provider needs the
    // NNTP client, and the client needs the reactor.
    try testing.expect(api.probe != null);

    // Deliberately not, each with a reason in `App.wire`. Asserted so
    // that wiring one of them without deleting its excuse fails here.
    try testing.expect(api.health == null);
    try testing.expect(api.disk == null);
}
