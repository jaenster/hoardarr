//! `hoardarr healthcheck` — the binary probing itself.
//!
//! This is the container's `HEALTHCHECK` command. The image is
//! distroless: no shell, no `curl`, no `wget`, so re-invoking our own
//! binary is the only way for the container to ask whether it is up.
//! That sets three hard constraints, and every decision below is one of
//! them:
//!
//!   1. **It must not need the config file.** The probe may run as a
//!      different uid than the daemon, or before the file exists, or with
//!      `/data` not yet mounted. So the address comes from the
//!      environment alone — `HOARDARR_LISTEN` and `HOARDARR_URL_BASE`,
//!      both of which the Dockerfile sets — and never from `config.toml`.
//!      A probe that fails because it could not read a file would mark a
//!      perfectly healthy container unhealthy.
//!   2. **It must not hang.** Docker kills a probe that overruns, but a
//!      hung one still burns the interval. The HTTP client takes a
//!      deadline; three seconds, as Go's context did.
//!   3. **It must be cheap.** No database, no config parse, no JSON
//!      decode. The status line is the whole answer — a 2xx from
//!      `/healthz` means the listener is accepting and routing, which is
//!      what "healthy" means for this process.
//!
//! Exit 0 when healthy, 1 otherwise. Nothing else is a valid answer.

const std = @import("std");
const api = @import("api.zig");
const client = @import("../net/http/client.zig");

const Allocator = std.mem.Allocator;

/// Go's `context.WithTimeout(…, 3*time.Second)`.
pub const deadline_ns: u64 = 3 * std.time.ns_per_s;

/// Where the daemon publishes liveness. Registered by the composition
/// root as a public route, so no API key is needed — which matters,
/// because the probe has no way to learn one without the config file.
pub const path = "/healthz";

pub fn run(gpa: Allocator, env: std.process.Environ) u8 {
    const listen = api.envOr(env, "HOARDARR_LISTEN", api.default_listen);
    const port = api.listenPort(listen) orelse
        return api.fail("healthcheck: cannot parse HOARDARR_LISTEN '{s}'", .{listen});

    // A reverse-proxy install serves at `/prefix/healthz`, and the
    // daemon mounts its routes under the same base inside the container.
    // The trailing slash Go trimmed is trimmed here too: `config`
    // rejects one, but the environment is not validated.
    const url_base = std.mem.trimEnd(u8, api.envOr(env, "HOARDARR_URL_BASE", ""), "/");

    var resp = api.call(gpa, .{ .port = port, .url_base = url_base }, .{
        .method = .get,
        .path = path,
        .deadline_ns = deadline_ns,
    }) catch |err| return api.fail("healthcheck: {t}", .{err});
    defer resp.deinit();

    // Only the status matters. The body is a JSON blob the daemon may
    // change; parsing it would make the probe fail for reasons that have
    // nothing to do with health.
    if (!healthy(resp.status)) {
        return api.fail("healthcheck: status {d}", .{resp.status});
    }
    return 0;
}

/// Go's `resp.StatusCode/100 != 2`.
pub fn healthy(status: u16) bool {
    return status / 100 == 2;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;
const reactor = @import("../posix/reactor.zig");
const server_mod = @import("../net/http/server.zig");

test "only a 2xx counts as healthy" {
    try testing.expect(healthy(200));
    try testing.expect(healthy(204));
    try testing.expect(healthy(299));
    try testing.expect(!healthy(199));
    try testing.expect(!healthy(301));
    try testing.expect(!healthy(404));
    try testing.expect(!healthy(500));
    try testing.expect(!healthy(503));
}

fn handleHealthz(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.send(200, "application/json", "{\"status\":\"ok\"}");
}

fn handleSick(ctx: *server_mod.Ctx) server_mod.HandlerError!void {
    try ctx.res.send(503, "application/json", "{\"status\":\"down\"}");
}

const probe_routes = [_]server_mod.Route{
    .{ .method = .get, .path = "/healthz", .handler = handleHealthz, .access = .public },
    .{ .method = .get, .path = "/base/healthz", .handler = handleHealthz, .access = .public },
    .{ .method = .get, .path = "/sick/healthz", .handler = handleSick, .access = .public },
};

/// Probes a real listener rather than a fake, because the thing under
/// test is the whole path — loop, connect, request line, status parse —
/// and a fake would only assert that the fake works.
fn probe(url_base: []const u8) !u16 {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var srv: server_mod.Server = undefined;
    srv.init(gpa, &loop, .{}, &probe_routes);
    defer srv.deinit();
    try srv.listen(try std.Io.net.IpAddress.parse("127.0.0.1", 0));
    const port = try srv.boundPort();

    // The server and the client each drive their own loop, so the server
    // has to be pumped from a thread while the blocking call runs.
    const Pump = struct {
        loop: *reactor.Loop,
        stop: bool = false,
        fn go(self: *@This()) void {
            while (!@atomicLoad(bool, &self.stop, .acquire)) {
                _ = self.loop.tick(5) catch return;
            }
        }
    };
    var pump: Pump = .{ .loop = &loop };
    const t = try std.Thread.spawn(.{}, Pump.go, .{&pump});
    defer {
        @atomicStore(bool, &pump.stop, true, .release);
        t.join();
    }

    var resp = try api.call(
        gpa,
        .{ .port = port, .url_base = url_base },
        .{ .method = .get, .path = path, .deadline_ns = 3 * std.time.ns_per_s },
    );
    defer resp.deinit();
    return resp.status;
}

test "a live listener answers the probe with a 2xx" {
    try testing.expectEqual(@as(u16, 200), try probe(""));
}

test "url_base is prepended, so a proxied install still probes" {
    try testing.expectEqual(@as(u16, 200), try probe("/base"));
}

test "an unhealthy daemon is reported, not treated as absent" {
    const status = try probe("/sick");
    try testing.expectEqual(@as(u16, 503), status);
    try testing.expect(!healthy(status));
}

test "nothing listening is a connect failure, not a hang" {
    const gpa = testing.allocator;
    // Port 1 on loopback: privileged, and nothing in this test binds it.
    if (api.call(gpa, .{ .port = 1 }, .{
        .method = .get,
        .path = path,
        .deadline_ns = 2 * std.time.ns_per_s,
    })) |resp| {
        var m = resp;
        m.deinit();
        return error.ExpectedConnectFailure;
    } else |_| {}
}
