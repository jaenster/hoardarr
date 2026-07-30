//! Port of `internal/bootstrap/e2e_multiserver_test.go` —
//! `TestMultiServer_E2E_PriorityFailover`.
//!
//! Two providers, both real, both on the daemon's loop. The primary has
//! a lower priority number, so it is asked first — and it has none of
//! the articles, so it answers 430 to everything. The download has to
//! complete anyway, from the secondary, and the byte accounting has to
//! show it: all bytes from the secondary, none from the primary.
//!
//! `used_bytes` is the assertion that cannot be faked by a lenient
//! pipeline. A daemon that failed over correctly but attributed the
//! traffic to the wrong server bills the operator's block account for
//! downloads it never served, and nothing else in the suite would
//! notice.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const infra = @import("../bootstrap/infra.zig");
const dserver = @import("../domain/server.zig");
const repo_server = @import("../store/repo_server.zig");
const tsnntp = @import("../testserver/nntp.zig");
const tsfixture = @import("../testserver/fixture.zig");

const release_name = "Hoardarr.MultiServer.Release";

fn addServer(
    fx: *h.Harness,
    gpa: std.mem.Allocator,
    name: []const u8,
    port: u16,
    priority: i32,
) !void {
    var row = try dserver.UsenetServer.init(gpa, .{
        .name = name,
        .host = "127.0.0.1",
        .port = port,
        .tls = false,
        .max_conns = 4,
        .priority = priority,
    }, infra.nowMillis());
    defer row.deinit();
    try repo_server.ServerRepo.init(gpa, fx.app.db).save(&row);
}

fn usedBytes(fx: *h.Harness, name: []const u8) !i64 {
    return fx.app.db.scalarIntOr(
        "SELECT used_bytes FROM servers WHERE name = ?",
        .{name},
        -1,
    );
}

test "multiserver: a primary answering 430 fails over, and the bytes are billed to the one that served them" {
    const gpa = testing.allocator;

    var fx = try h.Harness.init(gpa, "multiserver", .{});
    defer fx.deinit();

    fx.release = try tsfixture.generate(gpa, .{
        .name = release_name,
        .file_count = 1,
        .file_size = 128 * 1024,
        .article_size = 32 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 4,
    });

    // The primary: started through the harness so it is torn down with
    // everything else, and given no corpus at all, so every BODY it
    // sees is a 430. That is the exact condition the tiered fetcher
    // treats as "not here, try the next" rather than as a fault.
    const primary_port = try fx.startNntp(.{});

    // The secondary is a second server on the same loop, owned by this
    // test because the harness tracks one.
    var secondary: tsnntp.Server = undefined;
    const secondary_port = try secondary.start(gpa, &fx.app.loop, .{});
    defer secondary.deinit();
    for (fx.release.?.articles) |a| try secondary.addArticle(a.message_id, a.body);

    // Lower priority number wins, so the empty one is asked first.
    try addServer(fx, gpa, "primary", primary_port, 0);
    try addServer(fx, gpa, "secondary", secondary_port, 10);
    try fx.startEngine();

    const id = try fx.addRelease(release_name);
    const final = try fx.runUntilTerminal(id, 60_000);
    if (final != .completed) {
        std.debug.print("\nfailover job ended in {s}\n", .{final.toString()});
        return error.FailoverDidNotComplete;
    }

    // The primary really was asked, and really did refuse — otherwise
    // this passes as a single-server test wearing two names.
    try testing.expect(fx.provider().missed > 0);
    try testing.expectEqual(@as(usize, 0), fx.provider().served);
    try testing.expect(secondary.served > 0);

    // The bytes match, so the failover delivered the real article and
    // not an empty body the pipeline papered over.
    try fx.expectDeliveredMatchesRelease(release_name);

    // And the accounting followed the traffic. This is the assertion a
    // metered block account depends on.
    const primary_used = try usedBytes(fx, "primary");
    const secondary_used = try usedBytes(fx, "secondary");
    if (primary_used != 0) {
        std.debug.print("\nprimary used_bytes = {d}; it only ever answered 430\n", .{primary_used});
        return error.BytesBilledToTheWrongServer;
    }
    if (secondary_used <= 0) {
        std.debug.print("\nsecondary used_bytes = {d}; it served the whole release\n", .{secondary_used});
        return error.ServedBytesNotAccounted;
    }
}

test "multiserver: a job added with no server parks instead of failing, and runs when one appears" {
    // The Go suite covers this as `e2e_hotwire_test.go`
    // (`TestE2E_HotwirePoolAfterServerAdd`). It is the first-run
    // experience: the operator uploads an NZB before configuring a
    // provider, and the job must wait rather than burn its retry budget
    // and go terminal while they are still typing.
    const gpa = testing.allocator;

    var fx = try h.Harness.init(gpa, "hotwire", .{ .engine = true });
    defer fx.deinit();

    fx.release = try tsfixture.generate(gpa, .{
        .name = release_name,
        .file_count = 1,
        .file_size = 64 * 1024,
        .article_size = 32 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 4,
    });

    const id = try fx.addRelease(release_name);

    // With nothing to fetch from, the job must stay alive. Ticking a
    // good while is the point: a retry budget being consumed would show
    // up here as a terminal state.
    {
        var i: usize = 0;
        while (i < 500) : (i += 1) _ = try fx.app.loop.tick(1);
    }
    const parked = try fx.jobState(id);
    if (parked.isTerminal()) {
        std.debug.print("\na job with no server went {s}; it should wait\n", .{parked.toString()});
        return error.JobFailedWithoutAServer;
    }

    // Now add one, through the REST surface the Settings page uses —
    // which is what has to unpark it.
    const port = try fx.startNntp(.{});
    try fx.serveRelease();
    {
        const body = try std.fmt.allocPrint(gpa,
            \\{{"name":"late","host":"127.0.0.1","port":{d},"tls":false,"max_conns":4,"priority":0}}
        , .{port});
        defer gpa.free(body);
        var r = try fx.request(.{ .method = .post, .path = "/api/v1/servers", .body = body });
        defer r.deinit();
        try h.expectStatus(&r, 201, "POST /api/v1/servers");
    }

    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));
    try fx.expectDeliveredMatchesRelease(release_name);
}
