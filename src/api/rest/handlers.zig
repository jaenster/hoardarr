//! The REST API: the route table, and the end-to-end tests for it.
//!
//! `bootstrap` builds an `Api`, fills in the ports it has adapters for,
//! and hands `routes` and that `Api` to `net/http`. Nothing else is
//! needed to serve the whole surface.
//!
//! ## Routing without a pattern language
//!
//! Go's `ServeMux` matched `/api/v1/queue/{id}/pause` and handed the
//! handler a named path value. The Zig router has `exact` and `prefix`
//! and nothing else, which is deliberate: matching is a `memcmp` per
//! route with no allocation and no build step. So a parameterised path
//! is a prefix route whose handler splits `ctx.tail` — see
//! `respond.segments`. Precedence does the rest: an exact match beats
//! every prefix, so `/queue/stream` and `/queue/nzb` win over the
//! `/queue/` prefix that would otherwise swallow them.
//!
//! ## Access
//!
//! Three routes are `public`: `/auth/whoami`, `/auth/setup` and
//! `/auth/login`. Everything else takes the `Route.access` default,
//! which is `protected` — a route added here without a thought about
//! auth fails closed, and that is worth more than the brevity of a
//! whitelist.
//!
//! ## Endpoints that a missing port disables
//!
//! Go registered a route only when its dependency was non-nil, so an
//! endpoint the build did not have produced a 404 indistinguishable
//! from a typo. Here the route always exists and answers 503. The
//! success paths are identical; the difference is that the UI can tell
//! "this server has no scheduler" from "that task does not exist".

const std = @import("std");
const http = @import("../../net/http/server.zig");

const auth = @import("auth.zig");
const queue = @import("queue.zig");
const servers = @import("servers.zig");
const categories = @import("categories.zig");
const system = @import("system.zig");
const config = @import("config.zig");
const subscriptions = @import("subscriptions.zig");

pub const Api = @import("api.zig").Api;
pub const ports = @import("ports.zig");
pub const json = @import("json.zig");
pub const dto = @import("dto.zig");
pub const ratelimit = @import("ratelimit.zig");
pub const multipart = @import("multipart.zig");
pub const respond = @import("respond.zig");
pub const stream = @import("stream.zig");

const Route = http.Route;

/// Every route the REST API serves, in the order a reader wants them:
/// auth first, then the surfaces in the order the UI's navigation lists
/// them. Order does not affect matching — precedence is exact-beats-
/// prefix, longest-prefix-wins — so this is purely for the reader.
pub const routes = [_]Route{
    // -- auth ---------------------------------------------------------
    // The only three public routes in the whole API.
    .{ .method = .get, .path = "/api/v1/auth/whoami", .handler = auth.whoami, .access = .public },
    .{ .method = .post, .path = "/api/v1/auth/setup", .handler = auth.setup, .access = .public },
    .{ .method = .post, .path = "/api/v1/auth/login", .handler = auth.login, .access = .public },
    .{ .method = .post, .path = "/api/v1/auth/logout", .handler = auth.logout },
    .{ .method = .post, .path = "/api/v1/auth/change-password", .handler = auth.changePassword },
    .{ .method = .post, .path = "/api/v1/auth/rotate-api-key", .handler = auth.rotateApiKey },

    // -- queue --------------------------------------------------------
    .{ .method = .get, .path = "/api/v1/queue", .handler = queue.list },
    .{ .method = .post, .path = "/api/v1/queue/nzb", .handler = queue.addNzb },
    .{ .method = .post, .path = "/api/v1/queue/reorder", .handler = queue.reorder },
    // Both names for the same stream; see `queue.eventStream`.
    .{ .method = .get, .path = "/api/v1/queue/stream", .handler = queue.eventStream },
    .{ .method = .get, .path = "/api/v1/events", .handler = queue.eventStream },
    // {id} and {id}/events.
    .{ .method = .get, .path = "/api/v1/queue/", .kind = .prefix, .handler = queue.getByPath },
    // {id}/pause and {id}/resume.
    .{ .method = .post, .path = "/api/v1/queue/", .kind = .prefix, .handler = queue.postByPath },
    .{ .method = .delete, .path = "/api/v1/queue/", .kind = .prefix, .handler = queue.deleteByPath },
    .{ .method = .get, .path = "/api/v1/history", .handler = queue.history },

    // -- servers ------------------------------------------------------
    .{ .method = .get, .path = "/api/v1/servers", .handler = servers.list },
    .{ .method = .post, .path = "/api/v1/servers", .handler = servers.add },
    .{ .method = .post, .path = "/api/v1/servers/test", .handler = servers.testUnsaved },
    .{ .method = .patch, .path = "/api/v1/servers/", .kind = .prefix, .handler = servers.patch },
    .{ .method = .delete, .path = "/api/v1/servers/", .kind = .prefix, .handler = servers.remove },
    // {id}/test, {id}/enable, {id}/disable.
    .{ .method = .post, .path = "/api/v1/servers/", .kind = .prefix, .handler = servers.postByPath },

    // -- categories ---------------------------------------------------
    .{ .method = .get, .path = "/api/v1/categories", .handler = categories.list },
    .{ .method = .post, .path = "/api/v1/categories", .handler = categories.upsert },
    .{ .method = .delete, .path = "/api/v1/categories/", .kind = .prefix, .handler = categories.remove },

    // -- system -------------------------------------------------------
    .{ .method = .get, .path = "/api/v1/system/status", .handler = system.status },
    .{ .method = .get, .path = "/api/v1/system/throughput", .handler = system.throughput },
    .{ .method = .get, .path = "/api/v1/system/speed-history", .handler = system.speedHistory },
    .{ .method = .get, .path = "/api/v1/system/health", .handler = system.health },
    .{ .method = .post, .path = "/api/v1/system/health/refresh", .handler = system.healthRefresh },
    .{ .method = .get, .path = "/api/v1/system/tasks", .handler = system.tasks },
    .{ .method = .post, .path = "/api/v1/system/tasks/", .kind = .prefix, .handler = system.taskRunNow },
    .{ .method = .get, .path = "/api/v1/system/diskspace", .handler = system.diskSpace },
    .{ .method = .get, .path = "/api/v1/system/logs", .handler = system.logSnapshot },
    .{ .method = .get, .path = "/api/v1/system/logs/tail", .handler = system.logStream },
    .{ .method = .get, .path = "/api/v1/system/logs/stream", .handler = system.logStream },
    .{ .method = .get, .path = "/api/v1/system/logs/files", .handler = system.logFiles },
    .{ .method = .get, .path = "/api/v1/system/logs/files/", .kind = .prefix, .handler = system.logFileDownload },
    .{ .method = .get, .path = "/api/v1/system/backups", .handler = system.backups },
    .{ .method = .post, .path = "/api/v1/system/backups", .handler = system.runBackup },
    .{ .method = .get, .path = "/api/v1/system/backups/", .kind = .prefix, .handler = system.downloadBackup },

    // -- commands -----------------------------------------------------
    .{ .method = .get, .path = "/api/v1/commands", .handler = system.listCommands },
    .{ .method = .post, .path = "/api/v1/commands", .handler = system.submitCommand },
    .{ .method = .get, .path = "/api/v1/commands/names", .handler = system.commandNames },
    .{ .method = .get, .path = "/api/v1/commands/", .kind = .prefix, .handler = system.getCommand },

    // -- config -------------------------------------------------------
    .{ .method = .get, .path = "/api/v1/config/paths", .handler = config.paths },
    .{ .method = .get, .path = "/api/v1/config/general", .handler = config.getGeneral },
    .{ .method = .put, .path = "/api/v1/config/general", .handler = config.putGeneral },
    .{ .method = .get, .path = "/api/v1/config/bandwidth", .handler = config.getBandwidth },
    .{ .method = .put, .path = "/api/v1/config/bandwidth", .handler = config.setBandwidth },

    // -- subscriptions ------------------------------------------------
    .{ .method = .get, .path = "/api/v1/subscriptions", .handler = subscriptions.list },
    .{ .method = .post, .path = "/api/v1/subscriptions", .handler = subscriptions.add },
    .{ .method = .patch, .path = "/api/v1/subscriptions/", .kind = .prefix, .handler = subscriptions.patch },
    .{ .method = .delete, .path = "/api/v1/subscriptions/", .kind = .prefix, .handler = subscriptions.remove },
    .{ .method = .post, .path = "/api/v1/subscriptions/", .kind = .prefix, .handler = subscriptions.postByPath },

    // -- observability ------------------------------------------------
    // Protected: the label values name the operator's servers and the
    // counters describe their traffic.
    .{ .method = .get, .path = "/metrics", .handler = system.prometheus },
};

/// For a caller that wants to concatenate these with the SAB routes and
/// the frontend's catch-all.
pub fn routeSlice() []const Route {
    return &routes;
}

// =====================================================================
// Tests
//
// These run the real server over a real loopback socket with a real
// client: routing, the auth middleware, body reading and the handlers,
// exercised the way a browser exercises them. The ports underneath are
// fakes, so nothing here touches a database or the network.
// =====================================================================

const testing = std.testing;
const socket = @import("../../net/socket.zig");
const reactor = @import("../../posix/reactor.zig");
const sys = @import("../../posix/sys.zig");
const sse = @import("../sse.zig");
const metrics = @import("../metrics.zig");
const logring = @import("../../core/logring.zig");
const IpAddress = std.Io.net.IpAddress;
const Allocator = std.mem.Allocator;

const test_key = "testkey0123456789abcdef0123456789ab";

/// A raw client: writes a request, accumulates the bytes that come back.
/// No HTTP knowledge beyond finding the end of a response, so the tests
/// assert on what actually went on the wire.
const Client = struct {
    stream: socket.Stream = undefined,
    gpa: Allocator,
    got: std.ArrayList(u8) = .empty,
    closed: bool = false,
    send_on_connect: []const u8 = "",
    disconnected: bool = false,
    buf: [8192]u8 = undefined,

    const handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
        .on_connected = onConnected,
    };

    fn connect(self: *Client, loop: *reactor.Loop, port: u16) !void {
        try self.stream.connect(self.gpa, loop, try IpAddress.parse("127.0.0.1", port), &handler);
    }

    fn disconnect(self: *Client) void {
        if (self.disconnected) return;
        self.disconnected = true;
        self.stream.deinit();
    }

    fn destroy(self: *Client) void {
        const gpa = self.gpa;
        self.disconnect();
        self.got.deinit(gpa);
        gpa.destroy(self);
    }

    fn onConnected(s: *socket.Stream, err: ?socket.Error) void {
        const self: *Client = @fieldParentPtr("stream", s);
        if (err != null) return;
        if (self.send_on_connect.len > 0) s.write(self.send_on_connect) catch {};
    }

    fn onReadable(s: *socket.Stream) void {
        const self: *Client = @fieldParentPtr("stream", s);
        while (true) {
            const n = s.read(&self.buf) catch |err| switch (err) {
                error.WouldBlock => return,
                else => {
                    self.closed = true;
                    return;
                },
            };
            if (n == 0) {
                self.closed = true;
                return;
            }
            self.got.appendSlice(self.gpa, self.buf[0..n]) catch return;
        }
    }

    fn onClose(s: *socket.Stream, _: ?socket.Error) void {
        const self: *Client = @fieldParentPtr("stream", s);
        self.closed = true;
    }

    fn status(self: *const Client) ?u16 {
        if (self.got.items.len < 12) return null;
        return std.fmt.parseInt(u16, self.got.items[9..12], 10) catch null;
    }

    fn head(self: *const Client) []const u8 {
        const sep = std.mem.indexOf(u8, self.got.items, "\r\n\r\n") orelse return self.got.items;
        return self.got.items[0 .. sep + 4];
    }

    fn body(self: *const Client) []const u8 {
        const sep = std.mem.indexOf(u8, self.got.items, "\r\n\r\n") orelse return "";
        return self.got.items[sep + 4 ..];
    }

    /// Value of a response header, or null. Case-sensitive on the name
    /// because the server writes them in one fixed casing.
    fn header(self: *const Client, name: []const u8) ?[]const u8 {
        var it = std.mem.splitSequence(u8, self.head(), "\r\n");
        _ = it.next();
        while (it.next()) |line| {
            if (line.len == 0) break;
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[0..colon], " "), name)) continue;
            return std.mem.trim(u8, line[colon + 1 ..], " ");
        }
        return null;
    }

    fn complete(self: *const Client) bool {
        const items = self.got.items;
        const sep = std.mem.indexOf(u8, items, "\r\n\r\n") orelse return false;
        const h = items[0 .. sep + 4];
        const have = items.len - (sep + 4);
        const at = std.mem.indexOf(u8, h, "Content-Length: ") orelse return true;
        const rest = h[at + 16 ..];
        const end = std.mem.indexOf(u8, rest, "\r\n") orelse return false;
        const want = std.fmt.parseInt(usize, rest[0..end], 10) catch return false;
        return have >= want;
    }

    /// Parse the body as JSON into `arena`.
    fn parsed(self: *const Client, arena: Allocator) !std.json.Value {
        return std.json.parseFromSliceLeaky(std.json.Value, arena, self.body(), .{});
    }
};

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

fn pumpFor(loop: *reactor.Loop, ms: u64) !void {
    const start = sys.monotonicNanos();
    while (sys.monotonicNanos() - start < ms * std.time.ns_per_ms) {
        _ = try loop.tick(2);
    }
}

/// Server + loop + the real route table, wired to a caller-owned `Api`.
const Harness = struct {
    loop: reactor.Loop = undefined,
    server: http.Server = undefined,
    port: u16 = 0,
    api: *Api,

    fn start(self: *Harness, gpa: Allocator) !void {
        try self.loop.init(gpa);
        self.server.init(gpa, &self.loop, .{}, &routes);
        self.server.app_ctx = self.api;
        self.server.auth = self.api.authConfig();
        try self.server.listen(try IpAddress.parse("127.0.0.1", 0));
        self.port = try self.server.boundPort();
    }

    fn deinit(self: *Harness) void {
        self.server.deinit();
        self.loop.deinit();
    }

    /// One exchange, with the API key so the request is authenticated.
    fn call(self: *Harness, gpa: Allocator, method: []const u8, path: []const u8, body: ?[]const u8) !*Client {
        return self.callWith(gpa, method, path, body, "X-Api-Key: " ++ test_key ++ "\r\n");
    }

    /// One exchange with caller-supplied extra headers (each ending in
    /// CRLF), which is how the auth tests present or withhold a
    /// credential.
    fn callWith(
        self: *Harness,
        gpa: Allocator,
        method: []const u8,
        path: []const u8,
        body: ?[]const u8,
        extra: []const u8,
    ) !*Client {
        var raw: std.ArrayList(u8) = .empty;
        defer raw.deinit(gpa);
        try raw.appendSlice(gpa, method);
        try raw.append(gpa, ' ');
        try raw.appendSlice(gpa, path);
        try raw.appendSlice(gpa, " HTTP/1.1\r\nHost: h\r\nConnection: close\r\n");
        try raw.appendSlice(gpa, extra);
        if (body) |b| {
            var len_buf: [32]u8 = undefined;
            try raw.appendSlice(gpa, try std.fmt.bufPrint(&len_buf, "Content-Length: {d}\r\n", .{b.len}));
            try raw.appendSlice(gpa, "\r\n");
            try raw.appendSlice(gpa, b);
        } else {
            try raw.appendSlice(gpa, "\r\n");
        }
        return self.exchange(gpa, raw.items);
    }

    fn exchange(self: *Harness, gpa: Allocator, raw: []const u8) !*Client {
        const client = try gpa.create(Client);
        errdefer gpa.destroy(client);
        // The request must outlive the call, and `raw` is the caller's
        // buffer, so it is copied into the client.
        const owned = try gpa.dupe(u8, raw);
        client.* = .{ .gpa = gpa, .send_on_connect = owned };
        errdefer {
            gpa.free(owned);
            client.got.deinit(gpa);
        }
        try client.connect(&self.loop, self.port);
        try pumpUntil(&self.loop, 2000, client, struct {
            fn f(c: *Client) bool {
                return c.closed or c.complete();
            }
        }.f);
        gpa.free(owned);
        client.send_on_connect = "";
        return client;
    }

    /// Open a stream and leave it open. The caller pumps the loop and
    /// destroys the client itself.
    fn openStream(self: *Harness, gpa: Allocator, path: []const u8) !*Client {
        var raw: std.ArrayList(u8) = .empty;
        defer raw.deinit(gpa);
        try raw.appendSlice(gpa, "GET ");
        try raw.appendSlice(gpa, path);
        try raw.appendSlice(gpa, " HTTP/1.1\r\nHost: h\r\nX-Api-Key: " ++ test_key ++ "\r\n\r\n");

        const client = try gpa.create(Client);
        errdefer gpa.destroy(client);
        const owned = try gpa.dupe(u8, raw.items);
        client.* = .{ .gpa = gpa, .send_on_connect = owned };
        try client.connect(&self.loop, self.port);
        try pumpUntil(&self.loop, 2000, client, struct {
            fn f(c: *Client) bool {
                return c.closed or std.mem.indexOf(u8, c.got.items, "\r\n\r\n") != null;
            }
        }.f);
        gpa.free(owned);
        client.send_on_connect = "";
        return client;
    }
};

/// Everything a test might need, wired together. A test overrides the
/// fakes it cares about before `start`.
const Fixture = struct {
    gpa: Allocator,
    api: Api,
    harness: Harness = undefined,

    fake_auth: ports.FakeAuth = .{},
    fake_config: ports.FakeRuntimeConfig = .{ .api_key = test_key },
    fake_queue: ports.FakeQueue,
    fake_events: ports.FakeEvents = .{},
    fake_servers: ports.FakeServers = .{},
    fake_probe: ports.FakeProbe = .{},
    fake_categories: ports.FakeCategories = .{},
    fake_system: ports.FakeSystem = .{},
    fake_health: ports.FakeHealth = .{},
    fake_schedule: ports.FakeSchedule = .{},
    fake_commands: ports.FakeCommands = .{},
    fake_backups: ports.FakeBackups = .{},
    fake_log_files: ports.FakeLogFiles = .{},
    fake_disk: ports.FakeDiskSpace = .{},
    fake_subs: ports.FakeSubscriptions = .{},
    fake_bandwidth: ports.FakeBandwidth = .{},

    events_hub: sse.Hub,
    logs_hub: sse.Hub,
    registry: metrics.Registry,
    /// Copies of what the fakes were handed, so an assertion after the
    /// response does not read the request buffer back.
    recorder: ports.Recorder,

    /// Frozen clock, so the rate-limit tests are deterministic.
    var fake_now_ns: u64 = 1_000_000_000;
    fn fakeNow() u64 {
        return fake_now_ns;
    }

    fn create(gpa: Allocator) !*Fixture {
        const f = try gpa.create(Fixture);
        f.* = .{
            .gpa = gpa,
            .api = Api.init(gpa),
            .fake_queue = .{ .gpa = gpa },
            .events_hub = sse.Hub.init(gpa),
            .logs_hub = sse.Hub.init(gpa),
            .registry = metrics.Registry.init(gpa),
            .recorder = ports.Recorder.init(gpa),
        };
        return f;
    }

    /// Wire every port and start serving. Called after a test has
    /// adjusted the fakes it cares about.
    fn start(f: *Fixture) !void {
        fake_now_ns = 1_000_000_000;
        f.api.nowFn = fakeNow;
        f.fake_auth.rec = &f.recorder;
        f.fake_queue.rec = &f.recorder;
        f.fake_servers.rec = &f.recorder;
        f.fake_probe.rec = &f.recorder;
        f.fake_categories.rec = &f.recorder;
        f.fake_commands.rec = &f.recorder;
        f.fake_backups.rec = &f.recorder;
        f.fake_log_files.rec = &f.recorder;
        f.fake_subs.rec = &f.recorder;
        f.api.auth = f.fake_auth.port();
        f.api.config = f.fake_config.port();
        f.api.queue = f.fake_queue.port();
        f.api.events = f.fake_events.port();
        f.api.servers = f.fake_servers.port();
        f.api.probe = f.fake_probe.port();
        f.api.categories = f.fake_categories.port();
        f.api.system = f.fake_system.port();
        f.api.health = f.fake_health.port();
        f.api.schedule = f.fake_schedule.port();
        f.api.commands = f.fake_commands.port();
        f.api.backups = f.fake_backups.port();
        f.api.log_files = f.fake_log_files.port();
        f.api.disk = f.fake_disk.port();
        f.api.subscriptions = f.fake_subs.port();
        f.api.bandwidth = f.fake_bandwidth.port();
        f.api.events_hub = &f.events_hub;
        f.api.logs_hub = &f.logs_hub;
        f.api.metrics = &f.registry;

        f.harness = .{ .api = &f.api };
        try f.harness.start(f.gpa);
    }

    fn destroy(f: *Fixture) void {
        const gpa = f.gpa;
        // The server first: closing a connection unsubscribes it from a
        // hub, so the hubs must still be alive when it happens.
        f.harness.deinit();
        f.events_hub.deinit();
        f.logs_hub.deinit();
        f.registry.deinit();
        f.recorder.deinit();
        f.fake_queue.deinit();
        f.api.deinit();
        gpa.destroy(f);
    }
};

// ---------------------------------------------------------------------
// Auth: the security surface
// ---------------------------------------------------------------------

test "a protected endpoint refuses a request with no credentials" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const c = try f.harness.callWith(gpa, "GET", "/api/v1/queue", null, "");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 401), c.status());
    // And nothing about the queue leaked into the refusal.
    try testing.expect(std.mem.indexOf(u8, c.body(), "jobs") == null);
}

test "a protected endpoint refuses a wrong API key, in header and in query" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const wrong = try f.harness.callWith(gpa, "GET", "/api/v1/queue", null, "X-Api-Key: not-the-key\r\n");
    defer wrong.destroy();
    try testing.expectEqual(@as(?u16, 401), wrong.status());

    // Right length, one byte different: the compare is constant-time,
    // not a prefix match.
    const near = try f.harness.callWith(
        gpa,
        "GET",
        "/api/v1/queue",
        null,
        "X-Api-Key: testkey0123456789abcdef0123456789aB\r\n",
    );
    defer near.destroy();
    try testing.expectEqual(@as(?u16, 401), near.status());

    const q = try f.harness.callWith(gpa, "GET", "/api/v1/queue?apikey=wrong", null, "");
    defer q.destroy();
    try testing.expectEqual(@as(?u16, 401), q.status());
}

test "a correct API key is accepted in the header and in the query" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const h = try f.harness.call(gpa, "GET", "/api/v1/queue", null);
    defer h.destroy();
    try testing.expectEqual(@as(?u16, 200), h.status());

    // `?apikey=` exists for EventSource, which cannot set a header.
    const q = try f.harness.callWith(gpa, "GET", "/api/v1/queue?apikey=" ++ test_key, null, "");
    defer q.destroy();
    try testing.expectEqual(@as(?u16, 200), q.status());
}

test "a valid session cookie authenticates; an expired one does not" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.session_token = "live-session-token";
    defer f.destroy();
    try f.start();

    const good = try f.harness.callWith(
        gpa,
        "GET",
        "/api/v1/queue",
        null,
        "Cookie: hoardarr_session=live-session-token\r\n",
    );
    defer good.destroy();
    try testing.expectEqual(@as(?u16, 200), good.status());

    // The session expires. Same cookie, same request, now refused —
    // and refused without falling back to anything, because no key was
    // presented.
    f.fake_auth.session_expired = true;
    const expired = try f.harness.callWith(
        gpa,
        "GET",
        "/api/v1/queue",
        null,
        "Cookie: hoardarr_session=live-session-token\r\n",
    );
    defer expired.destroy();
    try testing.expectEqual(@as(?u16, 401), expired.status());

    // An unknown token is refused the same way.
    f.fake_auth.session_expired = false;
    const forged = try f.harness.callWith(
        gpa,
        "GET",
        "/api/v1/queue",
        null,
        "Cookie: hoardarr_session=forged\r\n",
    );
    defer forged.destroy();
    try testing.expectEqual(@as(?u16, 401), forged.status());
}

test "a stale cookie still lets a valid API key through" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.session_token = "";
    defer f.destroy();
    try f.start();

    const c = try f.harness.callWith(
        gpa,
        "GET",
        "/api/v1/queue",
        null,
        "Cookie: hoardarr_session=stale\r\nX-Api-Key: " ++ test_key ++ "\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
}

test "whoami, login and setup are reachable with no credentials" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const who = try f.harness.callWith(gpa, "GET", "/api/v1/auth/whoami", null, "");
    defer who.destroy();
    try testing.expectEqual(@as(?u16, 200), who.status());
    const v = try who.parsed(arena.allocator());
    try testing.expectEqualStrings("needs_login", v.object.get("state").?.string);

    const login = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        "{\"username\":\"admin\",\"password\":\"correct horse battery\"}",
        "Content-Type: application/json\r\n",
    );
    defer login.destroy();
    try testing.expectEqual(@as(?u16, 200), login.status());

    // Setup is public too, and answers 409 once an admin exists rather
    // than letting an unauthenticated caller create a second one.
    const setup = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/setup",
        "{\"username\":\"attacker\",\"password\":\"password123\"}",
        "Content-Type: application/json\r\n",
    );
    defer setup.destroy();
    try testing.expectEqual(@as(?u16, 409), setup.status());
}

test "an unconfigured API key does not open the server" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_config.api_key = "";
    f.fake_auth.session_token = "";
    defer f.destroy();
    try f.start();

    // No key configured, and a client presenting an empty one: still 401.
    // An empty expected key matching an empty provided key would turn
    // a misconfiguration into an open server.
    const none = try f.harness.callWith(gpa, "GET", "/api/v1/queue", null, "");
    defer none.destroy();
    try testing.expectEqual(@as(?u16, 401), none.status());

    const empty = try f.harness.callWith(gpa, "GET", "/api/v1/queue", null, "X-Api-Key: \r\n");
    defer empty.destroy();
    try testing.expectEqual(@as(?u16, 401), empty.status());
}

test "login sets an HttpOnly session cookie scoped to the URL base" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_config.url_base = "/hoardarr";
    defer f.destroy();
    try f.start();

    const c = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        "{\"username\":\"admin\",\"password\":\"correct horse battery\"}",
        "",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());

    const cookie = c.header("Set-Cookie") orelse return error.NoCookie;
    try testing.expect(std.mem.startsWith(u8, cookie, "hoardarr_session="));
    try testing.expect(std.mem.indexOf(u8, cookie, "; HttpOnly") != null);
    try testing.expect(std.mem.indexOf(u8, cookie, "; SameSite=Lax") != null);
    try testing.expect(std.mem.indexOf(u8, cookie, "Path=/hoardarr/") != null);
    try testing.expect(std.mem.indexOf(u8, cookie, "Max-Age=") != null);
}

test "login with bad credentials is a 401 that does not say which half was wrong" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const bad_user = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        "{\"username\":\"nobody\",\"password\":\"correct horse battery\"}",
        "",
    );
    defer bad_user.destroy();
    const bad_pass = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        "{\"username\":\"admin\",\"password\":\"wrong\"}",
        "",
    );
    defer bad_pass.destroy();

    try testing.expectEqual(@as(?u16, 401), bad_user.status());
    try testing.expectEqual(@as(?u16, 401), bad_pass.status());
    // Identical bodies: no username oracle.
    try testing.expectEqualStrings(bad_user.body(), bad_pass.body());
    try testing.expect(c_containsNo(bad_user.body(), "nobody"));
}

fn c_containsNo(haystack: []const u8, needle: []const u8) bool {
    return std.mem.indexOf(u8, haystack, needle) == null;
}

test "login is rate limited per source address" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const attempt = "{\"username\":\"admin\",\"password\":\"wrong\"}";
    var i: usize = 0;
    while (i < ratelimit.auth_max_attempts) : (i += 1) {
        const c = try f.harness.callWith(
            gpa,
            "POST",
            "/api/v1/auth/login",
            attempt,
            "X-Forwarded-For: 203.0.113.9\r\n",
        );
        defer c.destroy();
        try testing.expectEqual(@as(?u16, 401), c.status());
    }

    const blocked = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        attempt,
        "X-Forwarded-For: 203.0.113.9\r\n",
    );
    defer blocked.destroy();
    try testing.expectEqual(@as(?u16, 429), blocked.status());
    try testing.expect(blocked.header("Retry-After") != null);
    // The bcrypt path was not entered for the blocked attempt.
    try testing.expectEqual(@as(usize, ratelimit.auth_max_attempts), f.fake_auth.logins);

    // A different source address has its own quota.
    const other = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        attempt,
        "X-Forwarded-For: 198.51.100.2\r\n",
    );
    defer other.destroy();
    try testing.expectEqual(@as(?u16, 401), other.status());

    // And the window rolls off.
    Fixture.fake_now_ns += ratelimit.auth_window_ns + 1;
    const later = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/login",
        attempt,
        "X-Forwarded-For: 203.0.113.9\r\n",
    );
    defer later.destroy();
    try testing.expectEqual(@as(?u16, 401), later.status());
}

test "setup shares the login limiter" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.needs_setup = true;
    defer f.destroy();
    try f.start();

    var i: usize = 0;
    while (i < ratelimit.auth_max_attempts) : (i += 1) {
        const c = try f.harness.callWith(gpa, "POST", "/api/v1/auth/login", "{}", "X-Real-IP: 10.1.2.3\r\n");
        defer c.destroy();
    }
    const c = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/setup",
        "{\"username\":\"admin\",\"password\":\"password123\"}",
        "X-Real-IP: 10.1.2.3\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 429), c.status());
    try testing.expectEqual(@as(usize, 0), f.fake_auth.setups);
}

test "whoami reports the setup and authenticated states" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.needs_setup = true;
    defer f.destroy();
    try f.start();

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const setup_state = try f.harness.callWith(gpa, "GET", "/api/v1/auth/whoami", null, "");
    defer setup_state.destroy();
    try testing.expectEqualStrings("needs_setup", (try setup_state.parsed(a)).object.get("state").?.string);

    f.fake_auth.needs_setup = false;
    f.fake_auth.session_token = "tok";
    const authed = try f.harness.callWith(gpa, "GET", "/api/v1/auth/whoami", null, "Cookie: hoardarr_session=tok\r\n");
    defer authed.destroy();
    const v = try authed.parsed(a);
    try testing.expectEqualStrings("authenticated", v.object.get("state").?.string);
    try testing.expectEqualStrings("admin", v.object.get("user").?.object.get("username").?.string);
    try testing.expectEqualStrings("admin", v.object.get("user").?.object.get("role").?.string);
}

test "setup creates the admin, issues a session, and enforces the password floor" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.needs_setup = true;
    defer f.destroy();
    try f.start();

    const short = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/setup",
        "{\"username\":\"admin\",\"password\":\"short\"}",
        "",
    );
    defer short.destroy();
    try testing.expectEqual(@as(?u16, 400), short.status());
    try testing.expectEqual(@as(usize, 0), f.fake_auth.setups);

    const blank = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/setup",
        "{\"username\":\"   \",\"password\":\"password123\"}",
        "",
    );
    defer blank.destroy();
    try testing.expectEqual(@as(?u16, 400), blank.status());

    const ok = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/setup",
        "{\"username\":\"admin\",\"password\":\"password123\"}",
        "",
    );
    defer ok.destroy();
    try testing.expectEqual(@as(?u16, 201), ok.status());
    try testing.expectEqual(@as(usize, 1), f.fake_auth.setups);
    try testing.expect(ok.header("Set-Cookie") != null);
}

test "logout clears the cookie and is a 204 even with no session" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.session_token = "tok";
    defer f.destroy();
    try f.start();

    const c = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/logout",
        null,
        "Cookie: hoardarr_session=tok\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 204), c.status());
    const cookie = c.header("Set-Cookie") orelse return error.NoCookie;
    try testing.expect(std.mem.indexOf(u8, cookie, "Max-Age=0") != null);
    try testing.expectEqual(@as(usize, 1), f.fake_auth.logouts);

    // With no session at all — reached with the API key — still 204.
    const bare = try f.harness.call(gpa, "POST", "/api/v1/auth/logout", null);
    defer bare.destroy();
    try testing.expectEqual(@as(?u16, 204), bare.status());
}

test "change-password requires a session, not just an API key" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_auth.session_token = "tok";
    defer f.destroy();
    try f.start();

    const body = "{\"old_password\":\"correct horse battery\",\"new_password\":\"a-new-password\"}";

    // API key only: authenticated as the deployment, not as a user.
    const key_only = try f.harness.call(gpa, "POST", "/api/v1/auth/change-password", body);
    defer key_only.destroy();
    try testing.expectEqual(@as(?u16, 401), key_only.status());
    try testing.expectEqual(@as(usize, 0), f.fake_auth.password_changes);

    const with_session = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/change-password",
        body,
        "Cookie: hoardarr_session=tok\r\n",
    );
    defer with_session.destroy();
    try testing.expectEqual(@as(?u16, 204), with_session.status());
    try testing.expectEqual(@as(usize, 1), f.fake_auth.password_changes);
    try testing.expectEqualStrings("a-new-password", f.fake_auth.last_new_password);

    // Wrong current password.
    const wrong = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/auth/change-password",
        "{\"old_password\":\"nope\",\"new_password\":\"another-password\"}",
        "Cookie: hoardarr_session=tok\r\n",
    );
    defer wrong.destroy();
    try testing.expectEqual(@as(?u16, 401), wrong.status());
}

test "rotating the API key invalidates the old one on the next request" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const rot = try f.harness.call(gpa, "POST", "/api/v1/auth/rotate-api-key", null);
    defer rot.destroy();
    try testing.expectEqual(@as(?u16, 200), rot.status());
    const new_key = (try rot.parsed(arena.allocator())).object.get("api_key").?.string;
    try testing.expectEqualStrings(f.fake_config.rotated_key, new_key);

    const old = try f.harness.call(gpa, "GET", "/api/v1/queue", null);
    defer old.destroy();
    try testing.expectEqual(@as(?u16, 401), old.status());

    var hdr: std.ArrayList(u8) = .empty;
    defer hdr.deinit(gpa);
    try hdr.appendSlice(gpa, "X-Api-Key: ");
    try hdr.appendSlice(gpa, f.fake_config.rotated_key);
    try hdr.appendSlice(gpa, "\r\n");
    const fresh = try f.harness.callWith(gpa, "GET", "/api/v1/queue", null, hdr.items);
    defer fresh.destroy();
    try testing.expectEqual(@as(?u16, 200), fresh.status());
}

// ---------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------

fn makeJob(a: Allocator, id: i64, name: []const u8, state: ports.JobState) !*ports.Job {
    const j = try a.create(ports.Job);
    j.* = try ports.Job.hydrate(a, .{
        .id = id,
        .nzb_hash = "hash",
        .name = name,
        .category = "tv",
        .state = state,
        .total_bytes = 100,
        .done_bytes = 50,
        .added_at = 1_700_000_000_000,
        .files = &.{},
    });
    return j;
}

test "the queue lists active jobs, and everything with include=all" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const active = [_]*ports.Job{try makeJob(a, 1, "Active.Release", .downloading)};
    const all = [_]*ports.Job{
        active[0],
        try makeJob(a, 2, "Done.Release", .completed),
    };

    const f = try Fixture.create(gpa);
    f.fake_queue.active = &active;
    f.fake_queue.all = &all;
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(gpa, "GET", "/api/v1/queue", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqualStrings("application/json", c.header("Content-Type").?);
    var v = try c.parsed(a);
    try testing.expectEqual(@as(usize, 1), v.object.get("jobs").?.array.items.len);
    try testing.expectEqualStrings(
        "Active.Release",
        v.object.get("jobs").?.array.items[0].object.get("name").?.string,
    );

    const c2 = try f.harness.call(gpa, "GET", "/api/v1/queue?include=all", null);
    defer c2.destroy();
    v = try c2.parsed(a);
    try testing.expectEqual(@as(usize, 2), v.object.get("jobs").?.array.items.len);
}

test "a job detail, a missing job, and a non-numeric id" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const jobs = [_]*ports.Job{try makeJob(a, 7, "Release.Seven", .downloading)};
    const f = try Fixture.create(gpa);
    f.fake_queue.by_id = &jobs;
    defer f.destroy();
    try f.start();

    const found = try f.harness.call(gpa, "GET", "/api/v1/queue/7", null);
    defer found.destroy();
    try testing.expectEqual(@as(?u16, 200), found.status());
    const v = try found.parsed(a);
    try testing.expectEqualStrings("Release.Seven", v.object.get("job").?.object.get("name").?.string);

    const missing = try f.harness.call(gpa, "GET", "/api/v1/queue/999", null);
    defer missing.destroy();
    try testing.expectEqual(@as(?u16, 404), missing.status());

    const bad = try f.harness.call(gpa, "GET", "/api/v1/queue/not-a-number", null);
    defer bad.destroy();
    try testing.expectEqual(@as(?u16, 400), bad.status());

    // A path with a segment too many is a 404, not a mis-parse.
    const deep = try f.harness.call(gpa, "GET", "/api/v1/queue/7/files/3", null);
    defer deep.destroy();
    try testing.expectEqual(@as(?u16, 404), deep.status());
}

test "pause, resume, remove and reorder reach the port" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const p = try f.harness.call(gpa, "POST", "/api/v1/queue/3/pause", null);
    defer p.destroy();
    try testing.expectEqual(@as(?u16, 204), p.status());

    const r = try f.harness.call(gpa, "POST", "/api/v1/queue/3/resume", null);
    defer r.destroy();
    try testing.expectEqual(@as(?u16, 204), r.status());

    const d = try f.harness.call(gpa, "DELETE", "/api/v1/queue/4", null);
    defer d.destroy();
    try testing.expectEqual(@as(?u16, 204), d.status());

    const ro = try f.harness.call(gpa, "POST", "/api/v1/queue/reorder", "{\"ids\":[3,1,2]}");
    defer ro.destroy();
    try testing.expectEqual(@as(?u16, 204), ro.status());

    try testing.expectEqualSlices(ports.JobId, &.{3}, f.fake_queue.paused.items);
    try testing.expectEqualSlices(ports.JobId, &.{3}, f.fake_queue.resumed.items);
    try testing.expectEqualSlices(ports.JobId, &.{4}, f.fake_queue.removed.items);
    try testing.expectEqualSlices(ports.JobId, &.{ 3, 1, 2 }, f.fake_queue.reordered.items);

    // An unknown action under the prefix is a 404.
    const bogus = try f.harness.call(gpa, "POST", "/api/v1/queue/3/detonate", null);
    defer bogus.destroy();
    try testing.expectEqual(@as(?u16, 404), bogus.status());
}

test "reorder rejects a body that is not a list of ids" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const bad_json = try f.harness.call(gpa, "POST", "/api/v1/queue/reorder", "{not json");
    defer bad_json.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_json.status());

    const wrong_type = try f.harness.call(gpa, "POST", "/api/v1/queue/reorder", "{\"ids\":\"1,2,3\"}");
    defer wrong_type.destroy();
    try testing.expectEqual(@as(?u16, 400), wrong_type.status());

    const strings = try f.harness.call(gpa, "POST", "/api/v1/queue/reorder", "{\"ids\":[{},[]]}");
    defer strings.destroy();
    try testing.expectEqual(@as(?u16, 400), strings.status());

    // Empty is a no-op, which is what a drag that ended where it started
    // sends.
    const empty = try f.harness.call(gpa, "POST", "/api/v1/queue/reorder", "{\"ids\":[]}");
    defer empty.destroy();
    try testing.expectEqual(@as(?u16, 204), empty.status());
    try testing.expectEqual(@as(usize, 0), f.fake_queue.reordered.items.len);
}

test "an NZB upload is parsed out of the multipart body" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_queue.add_result = .{ .id = 12 };
    defer f.destroy();
    try f.start();

    const b = "----zigboundary";
    const body =
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"category\"\r\n\r\ntv\r\n" ++
        "--" ++ b ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"; filename=\"Release.Name.S01E01.nzb\"\r\n" ++
        "Content-Type: application/x-nzb\r\n\r\n" ++
        "<nzb><file/></nzb>\r\n" ++
        "--" ++ b ++ "--\r\n";

    const c = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/queue/nzb",
        body,
        "Content-Type: multipart/form-data; boundary=" ++ b ++ "\r\n" ++
            "User-Agent: Sonarr/4.0\r\nX-Api-Key: " ++ test_key ++ "\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 201), c.status());

    const add = f.fake_queue.last_add.?;
    try testing.expectEqualStrings("<nzb><file/></nzb>", add.nzb);
    try testing.expectEqualStrings("Release.Name.S01E01", add.name);
    try testing.expectEqualStrings("tv", add.category);
    try testing.expectEqualStrings("Sonarr/4.0", add.source);
}

test "a duplicate NZB is a 200 with the existing job, not an error" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const f = try Fixture.create(gpa);
    f.fake_queue.add_result = .{ .id = 5, .duplicate = true, .state = "completed", .name = "Old.Release" };
    defer f.destroy();
    try f.start();

    const b = "----zigboundary";
    const body = "--" ++ b ++ "\r\nContent-Disposition: form-data; name=\"nzb\"; filename=\"x.nzb\"\r\n\r\n<nzb/>\r\n--" ++ b ++ "--\r\n";
    const c = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/queue/nzb",
        body,
        "Content-Type: multipart/form-data; boundary=" ++ b ++ "\r\nX-Api-Key: " ++ test_key ++ "\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    const v = try c.parsed(arena.allocator());
    try testing.expectEqual(true, v.object.get("duplicate").?.bool);
    try testing.expectEqual(@as(i64, 5), v.object.get("job_id").?.integer);
    try testing.expectEqualStrings("completed", v.object.get("state").?.string);
}

test "an upload with no file, or the wrong content type, is a 400" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const json_body = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/queue/nzb",
        "{\"nzb\":\"...\"}",
        "Content-Type: application/json\r\nX-Api-Key: " ++ test_key ++ "\r\n",
    );
    defer json_body.destroy();
    try testing.expectEqual(@as(?u16, 400), json_body.status());

    const b = "----zigboundary";
    const no_file = "--" ++ b ++ "\r\nContent-Disposition: form-data; name=\"category\"\r\n\r\ntv\r\n--" ++ b ++ "--\r\n";
    const c = try f.harness.callWith(
        gpa,
        "POST",
        "/api/v1/queue/nzb",
        no_file,
        "Content-Type: multipart/form-data; boundary=" ++ b ++ "\r\nX-Api-Key: " ++ test_key ++ "\r\n",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 400), c.status());
    try testing.expect(f.fake_queue.last_add == null);
}

test "history passes its filters through and rejects malformed ones" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(
        gpa,
        "GET",
        "/api/v1/history?category=tv&state=completed&since=2023-11-14T22:13:20Z&limit=25",
        null,
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    const q = f.fake_queue.last_history.?;
    try testing.expectEqualStrings("tv", q.category);
    try testing.expectEqual(ports.JobState.completed, q.state.?);
    try testing.expectEqual(@as(i64, 1_700_000_000_000), q.since_ms.?);
    try testing.expectEqual(@as(i32, 25), q.limit);

    const bad_time = try f.harness.call(gpa, "GET", "/api/v1/history?since=yesterday", null);
    defer bad_time.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_time.status());

    const bad_state = try f.harness.call(gpa, "GET", "/api/v1/history?state=nonsense", null);
    defer bad_state.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_state.status());

    const bad_limit = try f.harness.call(gpa, "GET", "/api/v1/history?limit=abc", null);
    defer bad_limit.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_limit.status());

    // Out of range clamps rather than failing.
    const huge = try f.harness.call(gpa, "GET", "/api/v1/history?limit=100000", null);
    defer huge.destroy();
    try testing.expectEqual(@as(?u16, 200), huge.status());
    try testing.expectEqual(@as(i32, 500), f.fake_queue.last_history.?.limit);
}

test "a port failure becomes a 500 that says nothing about the port" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_queue.fail = error.Internal;
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(gpa, "GET", "/api/v1/queue", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 500), c.status());
    try testing.expectEqualStrings("{\"error\":\"internal error\"}", c.body());
}

test "a job event timeline comes back as envelopes" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const event = @import("../../domain/event.zig");
    const envelopes = [_]event.Envelope{
        .{
            .id = event.Uuid.v7(1_700_000_000_000, @splat(1)),
            .topic = "download.job.started",
            .aggregate_id = "7",
            .occurred_at = 1_700_000_000_000,
            .payload = "{\"job_id\":7}",
        },
    };

    const f = try Fixture.create(gpa);
    f.fake_events.envelopes = &envelopes;
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(gpa, "GET", "/api/v1/queue/7/events", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqual(@as(ports.JobId, 7), f.fake_events.last_job);
    const v = try c.parsed(a);
    const evs = v.object.get("events").?.array.items;
    try testing.expectEqualStrings("download.job.started", evs[0].object.get("Topic").?.string);
}

// ---------------------------------------------------------------------
// Servers, categories
// ---------------------------------------------------------------------

test "adding a server validates before it reaches the port" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const missing = try f.harness.call(gpa, "POST", "/api/v1/servers", "{\"name\":\"a\"}");
    defer missing.destroy();
    try testing.expectEqual(@as(?u16, 400), missing.status());
    try testing.expect(f.fake_servers.last_add == null);

    const bad_port = try f.harness.call(
        gpa,
        "POST",
        "/api/v1/servers",
        "{\"name\":\"a\",\"host\":\"h\",\"port\":99999}",
    );
    defer bad_port.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_port.status());

    const ok = try f.harness.call(
        gpa,
        "POST",
        "/api/v1/servers",
        "{\"name\":\" eweka \",\"host\":\"news.eweka.nl\",\"port\":563,\"tls\":true," ++
            "\"username\":\"u\",\"password\":\"p\",\"max_conns\":20,\"billing_mode\":\"metered\"}",
    );
    defer ok.destroy();
    try testing.expectEqual(@as(?u16, 201), ok.status());
    const cmd = f.fake_servers.last_add.?;
    try testing.expectEqualStrings("eweka", cmd.name);
    try testing.expectEqual(@as(u16, 563), cmd.port);
    try testing.expectEqual(true, cmd.tls.?);
    try testing.expectEqual(@as(i32, 20), cmd.max_conns);
    try testing.expectEqualStrings("metered", cmd.billing_mode);
}

test "a duplicate server name is a 409" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_servers.fail = error.Conflict;
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(
        gpa,
        "POST",
        "/api/v1/servers",
        "{\"name\":\"eweka\",\"host\":\"h\",\"port\":563}",
    );
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 409), c.status());
}

test "patching a server distinguishes absent from zero" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(gpa, "PATCH", "/api/v1/servers/3", "{\"priority\":0,\"backup\":false}");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 204), c.status());

    const cmd = f.fake_servers.last_update.?;
    try testing.expectEqual(@as(ports.ServerId, 3), cmd.id);
    // Present-and-zero is applied...
    try testing.expectEqual(@as(i32, 0), cmd.priority.?);
    try testing.expectEqual(false, cmd.backup.?);
    // ...and absent is left alone.
    try testing.expect(cmd.host == null);
    try testing.expect(cmd.port == null);
    try testing.expect(cmd.password == null);
}

test "server enable, disable, delete and the connection probes" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const server_domain = @import("../../domain/server.zig");
    var srv = try server_domain.UsenetServer.hydrate(a, .{
        .id = 3,
        .name = "eweka",
        .host = "news.eweka.nl",
        .port = 563,
        .tls = true,
        .username = "u",
        .password = "stored-password",
        .max_conns = 20,
        .enabled = true,
        .added_at = 1,
        .updated_at = 1,
    });
    const list = [_]*server_domain.UsenetServer{&srv};

    const f = try Fixture.create(gpa);
    f.fake_servers.servers = &list;
    defer f.destroy();
    try f.start();

    const en = try f.harness.call(gpa, "POST", "/api/v1/servers/3/enable", null);
    defer en.destroy();
    try testing.expectEqual(@as(?u16, 204), en.status());
    const dis = try f.harness.call(gpa, "POST", "/api/v1/servers/3/disable", null);
    defer dis.destroy();
    try testing.expectEqual(@as(?u16, 204), dis.status());
    try testing.expectEqual(@as(usize, 2), f.fake_servers.enabled_len);
    try testing.expectEqual(true, f.fake_servers.enabled_calls[0].enabled);
    try testing.expectEqual(false, f.fake_servers.enabled_calls[1].enabled);

    const del = try f.harness.call(gpa, "DELETE", "/api/v1/servers/3", null);
    defer del.destroy();
    try testing.expectEqual(@as(?u16, 204), del.status());
    try testing.expectEqual(@as(ports.ServerId, 3), f.fake_servers.removed.?);

    // Probing a saved server uses its stored credentials, so the
    // operator does not retype the password.
    const probe_saved = try f.harness.call(gpa, "POST", "/api/v1/servers/3/test", null);
    defer probe_saved.destroy();
    try testing.expectEqual(@as(?u16, 200), probe_saved.status());
    try testing.expectEqualStrings("stored-password", f.fake_probe.last.?.password);
    try testing.expectEqual(true, (try probe_saved.parsed(a)).object.get("ok").?.bool);

    // Probing unsaved credentials defaults to TLS on.
    const probe_new = try f.harness.call(
        gpa,
        "POST",
        "/api/v1/servers/test",
        "{\"host\":\"news.example\",\"port\":119}",
    );
    defer probe_new.destroy();
    try testing.expectEqual(@as(?u16, 200), probe_new.status());
    try testing.expectEqual(true, f.fake_probe.last.?.tls);
    try testing.expectEqualStrings("news.example", f.fake_probe.last.?.host);
}

test "categories list, upsert and delete, including a reserved one" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const cats = [_]ports.Category{
        .{ .name = "tv", .dir = "/data/tv", .priority = 1 },
        .{ .name = "movies", .dir = "/data/movies" },
    };
    const f = try Fixture.create(gpa);
    f.fake_categories.categories = &cats;
    defer f.destroy();
    try f.start();

    const list = try f.harness.call(gpa, "GET", "/api/v1/categories", null);
    defer list.destroy();
    const v = try list.parsed(a);
    try testing.expectEqual(@as(usize, 2), v.object.get("categories").?.array.items.len);

    const up = try f.harness.call(gpa, "POST", "/api/v1/categories", "{\"name\":\" anime \",\"dir\":\"/data/anime\"}");
    defer up.destroy();
    try testing.expectEqual(@as(?u16, 200), up.status());
    try testing.expectEqualStrings("anime", f.fake_categories.saved.?.name);

    const nameless = try f.harness.call(gpa, "POST", "/api/v1/categories", "{\"dir\":\"/data/x\"}");
    defer nameless.destroy();
    try testing.expectEqual(@as(?u16, 400), nameless.status());

    // A name with a space arrives percent-encoded and is decoded here.
    const del = try f.harness.call(gpa, "DELETE", "/api/v1/categories/my%20shows", null);
    defer del.destroy();
    try testing.expectEqual(@as(?u16, 204), del.status());
    try testing.expectEqualStrings("my shows", f.fake_categories.deleted.?);

    f.fake_categories.fail = error.Forbidden;
    const reserved = try f.harness.call(gpa, "DELETE", "/api/v1/categories/default", null);
    defer reserved.destroy();
    try testing.expectEqual(@as(?u16, 403), reserved.status());
}

// ---------------------------------------------------------------------
// System, config, subscriptions
// ---------------------------------------------------------------------

test "system status, throughput and speed history" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const f = try Fixture.create(gpa);
    f.fake_system.status_result = .{
        .version = "0.2.0",
        .os = "linux",
        .queue_active = 2,
        .queue_total = 9,
        .started_at_ms = 1_700_000_000_000,
    };
    f.fake_system.throughput_result = .{
        .window_seconds = 3,
        .series = &.{ 1, 2, 3 },
        .current_bytes_per_sec = 3,
    };
    f.fake_system.history_result = .{
        .resolution_seconds = 60,
        .samples = &.{.{ .at_ms = 1_700_000_000_000, .bytes_per_sec = 42 }},
    };
    f.fake_bandwidth.cap = 1_048_576;
    defer f.destroy();
    try f.start();

    const st = try f.harness.call(gpa, "GET", "/api/v1/system/status", null);
    defer st.destroy();
    var v = try st.parsed(a);
    try testing.expectEqualStrings("0.2.0", v.object.get("version").?.string);
    try testing.expectEqual(@as(i64, 2), v.object.get("queue").?.object.get("active").?.integer);

    const tp = try f.harness.call(gpa, "GET", "/api/v1/system/throughput", null);
    defer tp.destroy();
    v = try tp.parsed(a);
    try testing.expectEqual(@as(i64, 3), v.object.get("current_bytes_per_sec").?.integer);
    // The cap is reported alongside, which is what the graph draws.
    try testing.expectEqual(@as(i64, 1_048_576), v.object.get("global_cap_bytes_per_sec").?.integer);

    const sh = try f.harness.call(gpa, "GET", "/api/v1/system/speed-history?range=24h", null);
    defer sh.destroy();
    v = try sh.parsed(a);
    try testing.expectEqualStrings("24h", v.object.get("range").?.string);
    try testing.expectEqual(@as(i32, 86400), f.fake_system.last_span_seconds);

    const bad = try f.harness.call(gpa, "GET", "/api/v1/system/speed-history?range=30m", null);
    defer bad.destroy();
    try testing.expectEqual(@as(?u16, 400), bad.status());

    // No range means 5m, the Go default.
    const def = try f.harness.call(gpa, "GET", "/api/v1/system/speed-history", null);
    defer def.destroy();
    try testing.expectEqual(@as(i32, 300), f.fake_system.last_span_seconds);
}

test "health sorts errors first and refresh asks for a re-run" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const f = try Fixture.create(gpa);
    f.fake_health.snapshot_result = .{
        .issues = &.{
            .{ .source = "disk", .severity = .warning, .message = "low" },
            .{ .source = "servers", .severity = .err, .message = "none enabled" },
        },
        .last_run_ms = 1_700_000_000_000,
    };
    defer f.destroy();
    try f.start();

    const h = try f.harness.call(gpa, "GET", "/api/v1/system/health", null);
    defer h.destroy();
    const v = try h.parsed(a);
    try testing.expectEqualStrings("error", v.object.get("issues").?.array.items[0].object.get("severity").?.string);

    const r = try f.harness.call(gpa, "POST", "/api/v1/system/health/refresh", null);
    defer r.destroy();
    try testing.expectEqual(@as(?u16, 202), r.status());
    try testing.expectEqual(@as(usize, 1), f.fake_health.refreshes);
}

test "tasks list and run-now" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const schedule_domain = @import("../../domain/schedule.zig");
    var t = try schedule_domain.Task.hydrate(a, .{
        .id = 2,
        .name = "backup",
        .kind = .recurring,
        .cadence_ms = 3_600_000,
        .next_run_at = 1_700_000_000_000,
        .last_error = "",
        .created_at = 1,
        .updated_at = 1,
    });
    const tasks = [_]*schedule_domain.Task{&t};

    const f = try Fixture.create(gpa);
    f.fake_schedule.tasks = &tasks;
    defer f.destroy();
    try f.start();

    const list = try f.harness.call(gpa, "GET", "/api/v1/system/tasks", null);
    defer list.destroy();
    const v = try list.parsed(a);
    try testing.expectEqual(@as(i64, 3600), v.object.get("tasks").?.array.items[0].object.get("cadence_seconds").?.integer);

    const run = try f.harness.call(gpa, "POST", "/api/v1/system/tasks/2/run-now", null);
    defer run.destroy();
    try testing.expectEqual(@as(?u16, 200), run.status());
    try testing.expectEqual(@as(ports.TaskId, 2), f.fake_schedule.ran.?);

    const missing = try f.harness.call(gpa, "POST", "/api/v1/system/tasks/99/run-now", null);
    defer missing.destroy();
    try testing.expectEqual(@as(?u16, 404), missing.status());

    const wrong_action = try f.harness.call(gpa, "POST", "/api/v1/system/tasks/2/run", null);
    defer wrong_action.destroy();
    try testing.expectEqual(@as(?u16, 404), wrong_action.status());
}

test "commands: submit, poll, list and names" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const command_domain = @import("../../domain/command.zig");
    var cmd = try command_domain.Command.hydrate(a, .{
        .id = 1,
        .name = "RefreshHealth",
        .body = "{\"deep\":true}",
        .trigger = .manual,
        .status = .queued,
        .queued_at = 1_700_000_000_000,
    });
    const cmds = [_]*command_domain.Command{&cmd};
    const names = [_][]const u8{ "RefreshHealth", "Backup" };

    const f = try Fixture.create(gpa);
    f.fake_commands.commands = &cmds;
    f.fake_commands.name_list = &names;
    defer f.destroy();
    try f.start();

    const sub = try f.harness.call(
        gpa,
        "POST",
        "/api/v1/commands",
        "{\"name\":\"RefreshHealth\",\"body\":{\"deep\":true}}",
    );
    defer sub.destroy();
    try testing.expectEqual(@as(?u16, 202), sub.status());
    try testing.expectEqualStrings("RefreshHealth", f.fake_commands.last_submit_name);
    try testing.expectEqualStrings("{\"deep\":true}", f.fake_commands.last_submit_body);

    const nameless = try f.harness.call(gpa, "POST", "/api/v1/commands", "{\"body\":{}}");
    defer nameless.destroy();
    try testing.expectEqual(@as(?u16, 400), nameless.status());

    const one = try f.harness.call(gpa, "GET", "/api/v1/commands/1", null);
    defer one.destroy();
    try testing.expectEqual(@as(?u16, 200), one.status());

    const list = try f.harness.call(gpa, "GET", "/api/v1/commands?limit=10", null);
    defer list.destroy();
    try testing.expectEqual(@as(i32, 10), f.fake_commands.last_limit);

    // Out of range falls back to the default rather than 400ing.
    const bad_limit = try f.harness.call(gpa, "GET", "/api/v1/commands?limit=99999", null);
    defer bad_limit.destroy();
    try testing.expectEqual(@as(i32, 50), f.fake_commands.last_limit);

    // `/commands/names` is an exact route and must not be read as an id.
    const names_res = try f.harness.call(gpa, "GET", "/api/v1/commands/names", null);
    defer names_res.destroy();
    try testing.expectEqual(@as(?u16, 200), names_res.status());
    const v = try names_res.parsed(a);
    try testing.expectEqual(@as(usize, 2), v.object.get("names").?.array.items.len);
}

test "backups and log files download as attachments" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_backups.files = &.{.{ .name = "hoardarr-20231114.db", .size_bytes = 4096, .at_ms = 1_700_000_000_000 }};
    f.fake_log_files.files = &.{.{ .name = "hoardarr.log", .size_bytes = 10, .at_ms = 1_700_000_000_000, .active = true }};
    defer f.destroy();
    try f.start();

    const run = try f.harness.call(gpa, "POST", "/api/v1/system/backups", null);
    defer run.destroy();
    try testing.expectEqual(@as(?u16, 200), run.status());
    try testing.expectEqual(@as(usize, 1), f.fake_backups.runs);

    const dl = try f.harness.call(gpa, "GET", "/api/v1/system/backups/hoardarr-20231114.db", null);
    defer dl.destroy();
    try testing.expectEqual(@as(?u16, 200), dl.status());
    try testing.expectEqualStrings("sqlite-bytes", dl.body());
    try testing.expectEqualStrings(
        "attachment; filename=\"hoardarr-20231114.db\"",
        dl.header("Content-Disposition").?,
    );

    const logs = try f.harness.call(gpa, "GET", "/api/v1/system/logs/files", null);
    defer logs.destroy();
    try testing.expectEqual(@as(?u16, 200), logs.status());

    const log_dl = try f.harness.call(gpa, "GET", "/api/v1/system/logs/files/hoardarr.log", null);
    defer log_dl.destroy();
    try testing.expectEqualStrings("log text\n", log_dl.body());
    try testing.expectEqualStrings("text/plain; charset=utf-8", log_dl.header("Content-Type").?);

    // A traversal attempt is the port's refusal, surfaced as a 400.
    f.fake_log_files.read_fail = error.Invalid;
    const traversal = try f.harness.call(gpa, "GET", "/api/v1/system/logs/files/..%2F..%2Fetc%2Fpasswd", null);
    defer traversal.destroy();
    try testing.expectEqual(@as(?u16, 400), traversal.status());
}

test "the log snapshot renders the ring" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const log = @import("../../core/log.zig");
    var ring = try logring.Ring.init(gpa, 8);
    defer ring.deinit();
    var logger: log.Logger = .{};
    logger.setMirror(ring.mirror());
    logger.log(.warn, "nntp: article missing", &.{log.str("server", "news.example")});

    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();
    f.api.log_ring = &ring;

    const c = try f.harness.call(gpa, "GET", "/api/v1/system/logs", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    const v = try c.parsed(a);
    const entries = v.object.get("entries").?.array.items;
    try testing.expectEqual(@as(usize, 1), entries.len);
    try testing.expectEqualStrings("WARN", entries[0].object.get("level").?.string);
    try testing.expectEqualStrings("nntp: article missing", entries[0].object.get("message").?.string);
    try testing.expectEqualStrings("news.example", entries[0].object.get("attrs").?.object.get("server").?.string);
}

test "disk space, paths and the general panel" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const f = try Fixture.create(gpa);
    f.fake_disk.entries = &.{
        .{ .label = "complete", .path = "/data/complete", .free_bytes = 100, .total_bytes = 200, .used_bytes = 100 },
    };
    defer f.destroy();
    try f.start();

    const ds = try f.harness.call(gpa, "GET", "/api/v1/system/diskspace", null);
    defer ds.destroy();
    var v = try ds.parsed(a);
    try testing.expectEqualStrings("complete", v.object.get("entries").?.array.items[0].object.get("label").?.string);

    const p = try f.harness.call(gpa, "GET", "/api/v1/config/paths", null);
    defer p.destroy();
    v = try p.parsed(a);
    try testing.expectEqualStrings("/data/incomplete", v.object.get("incomplete_dir").?.string);
    try testing.expectEqual(false, v.object.get("runtime_mutable").?.bool);

    const g = try f.harness.call(gpa, "GET", "/api/v1/config/general", null);
    defer g.destroy();
    v = try g.parsed(a);
    try testing.expectEqualStrings(test_key, v.object.get("api_key").?.string);
    try testing.expectEqualStrings("0.0.0.0:8085", v.object.get("listen").?.string);
    try testing.expectEqual(@as(i64, 2), v.object.get("max_concurrent_jobs").?.integer);
}

test "putting general settings validates each field" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const ok = try f.harness.call(
        gpa,
        "PUT",
        "/api/v1/config/general",
        "{\"url_base\":\"/hoardarr\",\"max_concurrent_jobs\":4,\"delete_samples\":true}",
    );
    defer ok.destroy();
    try testing.expectEqual(@as(?u16, 204), ok.status());
    try testing.expectEqualStrings("/hoardarr", f.fake_config.url_base);
    try testing.expectEqual(@as(i32, 4), f.fake_config.max_concurrent_jobs);
    try testing.expectEqual(true, f.fake_config.delete_samples);

    const bad_base = try f.harness.call(gpa, "PUT", "/api/v1/config/general", "{\"url_base\":\"no-slash\"}");
    defer bad_base.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_base.status());

    const bad_ratio = try f.harness.call(gpa, "PUT", "/api/v1/config/general", "{\"fail_hopeless_ratio\":5}");
    defer bad_ratio.destroy();
    try testing.expectEqual(@as(?u16, 400), bad_ratio.status());

    const wrong_type = try f.harness.call(gpa, "PUT", "/api/v1/config/general", "{\"delete_samples\":\"yes\"}");
    defer wrong_type.destroy();
    try testing.expectEqual(@as(?u16, 400), wrong_type.status());
}

test "a read-only runtime config refuses mutation with 503" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    f.fake_config.writable = false;
    defer f.destroy();
    try f.start();

    const put = try f.harness.call(gpa, "PUT", "/api/v1/config/general", "{\"url_base\":\"\"}");
    defer put.destroy();
    try testing.expectEqual(@as(?u16, 503), put.status());

    const rot = try f.harness.call(gpa, "POST", "/api/v1/auth/rotate-api-key", null);
    defer rot.destroy();
    try testing.expectEqual(@as(?u16, 503), rot.status());
}

test "the bandwidth cap round-trips and refuses a negative" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const set = try f.harness.call(gpa, "PUT", "/api/v1/config/bandwidth", "{\"global_bytes_per_sec\":5242880}");
    defer set.destroy();
    try testing.expectEqual(@as(?u16, 200), set.status());
    try testing.expectEqual(@as(i64, 5_242_880), f.fake_bandwidth.cap);

    const get = try f.harness.call(gpa, "GET", "/api/v1/config/bandwidth", null);
    defer get.destroy();
    const v = try get.parsed(arena.allocator());
    try testing.expectEqual(@as(i64, 5_242_880), v.object.get("global_bytes_per_sec").?.integer);

    const neg = try f.harness.call(gpa, "PUT", "/api/v1/config/bandwidth", "{\"global_bytes_per_sec\":-1}");
    defer neg.destroy();
    try testing.expectEqual(@as(?u16, 400), neg.status());
    try testing.expectEqual(@as(i64, 5_242_880), f.fake_bandwidth.cap);
}

test "subscriptions: add, patch, enable, test and remove" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const notify_domain = @import("../../domain/notify.zig");
    var sub = try notify_domain.Subscription.hydrate(a, .{
        .id = 5,
        .name = "discord",
        .kind = .discord,
        .url = "https://discord.com/api/webhooks/1/secret-token",
        .topics = &.{"download.job.completed"},
        .secret = "hmac",
        .enabled = true,
        .last_error = "",
        .created_at = 1,
        .updated_at = 1,
    });
    const subs = [_]*notify_domain.Subscription{&sub};

    const f = try Fixture.create(gpa);
    f.fake_subs.subscriptions = &subs;
    defer f.destroy();
    try f.start();

    const list = try f.harness.call(gpa, "GET", "/api/v1/subscriptions", null);
    defer list.destroy();
    const v = try list.parsed(a);
    const row = v.object.get("subscriptions").?.array.items[0].object;
    try testing.expectEqual(true, row.get("has_secret").?.bool);
    try testing.expect(std.mem.indexOf(u8, list.body(), "hmac") == null);

    const add = try f.harness.call(
        gpa,
        "POST",
        "/api/v1/subscriptions",
        "{\"name\":\"ops\",\"url\":\"https://example/hook\",\"topics\":[\"a\",\"b\"]}",
    );
    defer add.destroy();
    try testing.expectEqual(@as(?u16, 201), add.status());
    try testing.expectEqual(@as(usize, 2), f.fake_subs.last_add.?.topics.len);

    const no_url = try f.harness.call(gpa, "POST", "/api/v1/subscriptions", "{\"name\":\"ops\"}");
    defer no_url.destroy();
    try testing.expectEqual(@as(?u16, 400), no_url.status());

    const patch = try f.harness.call(gpa, "PATCH", "/api/v1/subscriptions/5", "{\"enabled\":false}");
    defer patch.destroy();
    try testing.expectEqual(@as(?u16, 204), patch.status());
    try testing.expectEqual(false, f.fake_subs.last_update.?.enabled.?);

    const enable = try f.harness.call(gpa, "POST", "/api/v1/subscriptions/5/enable", null);
    defer enable.destroy();
    try testing.expectEqual(@as(?u16, 204), enable.status());
    try testing.expectEqual(true, f.fake_subs.last_enabled.?);

    const t = try f.harness.call(gpa, "POST", "/api/v1/subscriptions/5/test", null);
    defer t.destroy();
    try testing.expectEqual(@as(?u16, 204), t.status());

    // A subscriber that answers badly is a 502: their failure, not the
    // caller's.
    f.fake_subs.test_fail = error.Upstream;
    const failed = try f.harness.call(gpa, "POST", "/api/v1/subscriptions/5/test", null);
    defer failed.destroy();
    try testing.expectEqual(@as(?u16, 502), failed.status());

    const del = try f.harness.call(gpa, "DELETE", "/api/v1/subscriptions/5", null);
    defer del.destroy();
    try testing.expectEqual(@as(?u16, 204), del.status());
    try testing.expectEqual(@as(ports.SubscriptionId, 5), f.fake_subs.removed.?);
}

// ---------------------------------------------------------------------
// Streams and metrics
// ---------------------------------------------------------------------

test "the event stream stays open, receives events, and unsubscribes on close" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const c = try f.harness.openStream(gpa, "/api/v1/queue/stream");
    defer c.destroy();

    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqualStrings("text/event-stream", c.header("Content-Type").?);
    try testing.expectEqualStrings("no-cache", c.header("Cache-Control").?);
    try testing.expectEqualStrings("no", c.header("X-Accel-Buffering").?);
    try testing.expectEqual(@as(usize, 1), f.events_hub.count());
    // The greeting arrived.
    try testing.expect(std.mem.indexOf(u8, c.got.items, ": connected") != null);

    f.events_hub.publish("download.job.started", "{\"job_id\":7}");
    try pumpFor(&f.harness.loop, 60);
    try testing.expect(std.mem.indexOf(u8, c.got.items, "event: download.job.started") != null);
    try testing.expect(std.mem.indexOf(u8, c.got.items, "data: {\"job_id\":7}") != null);
    // Chunked framing, and the connection is still open.
    try testing.expectEqualStrings("chunked", c.header("Transfer-Encoding").?);
    try testing.expect(!c.closed);

    // The client goes away: the hub notices via the close hook.
    c.disconnect();
    try pumpUntil(&f.harness.loop, 2000, &f.events_hub, struct {
        fn done(h: *sse.Hub) bool {
            return h.count() == 0;
        }
    }.done);
    try testing.expectEqual(@as(usize, 0), f.events_hub.count());
}

test "the log tail opens with a ready event" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const c = try f.harness.openStream(gpa, "/api/v1/system/logs/tail");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expect(std.mem.indexOf(u8, c.got.items, "event: ready") != null);
    try testing.expectEqual(@as(usize, 1), f.logs_hub.count());

    // /stream is the same handler under the other name.
    const alias = try f.harness.openStream(gpa, "/api/v1/system/logs/stream");
    defer alias.destroy();
    try testing.expectEqual(@as(usize, 2), f.logs_hub.count());

    f.logs_hub.publish("log", "{\"level\":\"WARN\"}");
    try pumpFor(&f.harness.loop, 60);
    try testing.expect(std.mem.indexOf(u8, alias.got.items, "event: log") != null);

    c.disconnect();
    alias.disconnect();
    try pumpUntil(&f.harness.loop, 2000, &f.logs_hub, struct {
        fn done(h: *sse.Hub) bool {
            return h.count() == 0;
        }
    }.done);
}

test "a stream needs a credential like everything else" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const c = try f.harness.callWith(gpa, "GET", "/api/v1/queue/stream", null, "");
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 401), c.status());
    try testing.expectEqual(@as(usize, 0), f.events_hub.count());
}

test "metrics are served in the Prometheus text format, behind auth" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    f.registry.set(&f.registry.jobs, &.{"downloading"}, 3);

    const anon = try f.harness.callWith(gpa, "GET", "/metrics", null, "");
    defer anon.destroy();
    try testing.expectEqual(@as(?u16, 401), anon.status());

    const c = try f.harness.call(gpa, "GET", "/metrics", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    try testing.expectEqualStrings(metrics.content_type, c.header("Content-Type").?);
    try testing.expect(std.mem.indexOf(u8, c.body(), "hoardarr_jobs{state=\"downloading\"} 3") != null);
    try testing.expect(std.mem.indexOf(u8, c.body(), "# TYPE hoardarr_nntp_articles_fetched_total counter") != null);
}

// ---------------------------------------------------------------------
// The route table itself
// ---------------------------------------------------------------------

test "only the three bootstrap auth endpoints are public" {
    var public_count: usize = 0;
    for (routes) |r| {
        if (r.access != .public) continue;
        public_count += 1;
        const ok = std.mem.eql(u8, r.path, "/api/v1/auth/whoami") or
            std.mem.eql(u8, r.path, "/api/v1/auth/setup") or
            std.mem.eql(u8, r.path, "/api/v1/auth/login");
        if (!ok) {
            std.debug.print("unexpected public route: {s}\n", .{r.path});
            return error.UnexpectedPublicRoute;
        }
    }
    try testing.expectEqual(@as(usize, 3), public_count);
}

test "every route is under /api/v1 or is the metrics endpoint" {
    for (routes) |r| {
        const ok = std.mem.startsWith(u8, r.path, "/api/v1/") or std.mem.eql(u8, r.path, "/metrics");
        if (!ok) {
            std.debug.print("route outside the API namespace: {s}\n", .{r.path});
            return error.UnexpectedRoute;
        }
        // A prefix route's path must end in '/', or it would also match
        // a longer sibling's name — `/api/v1/queue` swallowing
        // `/api/v1/queuex`.
        if (r.kind == .prefix) try testing.expect(std.mem.endsWith(u8, r.path, "/"));
        // Every route names a method: a catch-all here would answer for
        // verbs nobody implemented.
        try testing.expect(r.method != null);
    }
}

test "an unknown path is a 404 and a wrong method is a 405" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();

    const missing = try f.harness.call(gpa, "GET", "/api/v1/nothing-here", null);
    defer missing.destroy();
    try testing.expectEqual(@as(?u16, 404), missing.status());

    const wrong_method = try f.harness.call(gpa, "DELETE", "/api/v1/servers", null);
    defer wrong_method.destroy();
    try testing.expectEqual(@as(?u16, 405), wrong_method.status());
}

test "an endpoint whose port is not wired answers 503, not 404" {
    const gpa = testing.allocator;
    const f = try Fixture.create(gpa);
    defer f.destroy();
    try f.start();
    // A build without a scheduler.
    f.api.schedule = null;

    const c = try f.harness.call(gpa, "GET", "/api/v1/system/tasks", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 503), c.status());
    try testing.expect(std.mem.indexOf(u8, c.body(), "unavailable") != null);
}

test "a hostile release name survives the whole round trip" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    // What an NZB off the internet can contain.
    const hostile = "Bad\"Name\\\n\x01<script>\xff.2160p";
    const jobs = [_]*ports.Job{try makeJob(a, 1, hostile, .downloading)};

    const f = try Fixture.create(gpa);
    f.fake_queue.active = &jobs;
    defer f.destroy();
    try f.start();

    const c = try f.harness.call(gpa, "GET", "/api/v1/queue", null);
    defer c.destroy();
    try testing.expectEqual(@as(?u16, 200), c.status());
    // The response still parses, and the name came back as one string
    // rather than as structure.
    const v = try c.parsed(a);
    const list = v.object.get("jobs").?.array.items;
    try testing.expectEqual(@as(usize, 1), list.len);
    try testing.expectEqual(@as(usize, 1), list[0].object.get("name").?.string.len -
        (list[0].object.get("name").?.string.len - 1));
    try testing.expect(v.object.get("admin") == null);
}
