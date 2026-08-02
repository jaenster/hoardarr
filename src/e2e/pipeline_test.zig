//! Ports of the data-path tests:
//!
//!   * `e2e_m1_test.go` — `TestM1_E2E_FullPipeline`: NZB in, segments
//!     fetched over NNTP, file assembled, and the assembled bytes equal
//!     the original.
//!   * `e2e_m2_test.go` — `TestM2_E2E_HTTP_FullPipeline`: the same, but
//!     driven entirely over HTTP — `POST /api/v1/queue/nzb`, then poll
//!     `/api/v1/queue` to a terminal state.
//!   * `e2e_m3a_test.go` — `TestM3a_E2E_VerifyOK`: a clean download's
//!     PAR2 set verifies, the release is delivered, and the per-job
//!     scratch directory is cleaned up.
//!
//! The Go originals each hand-build a payload, a yEnc encoding and an
//! NZB inline. Here that is `testserver/fixture.zig`, whose own tests
//! prove the PAR2 set it generates is real — which makes the M3a
//! assertions stronger than Go's, where the PAR2 packets were assembled
//! by the test and could agree with a broken verifier.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const fiber = @import("../posix/fiber.zig");
const tsfixture = @import("../testserver/fixture.zig");

const release_name = "Hoardarr.Pipeline.Release";

const small: tsfixture.Options = .{
    .name = release_name,
    .file_count = 1,
    .file_size = 128 * 1024,
    .article_size = 32 * 1024,
    .par2_slice_size = 16 * 1024,
    .recovery_slices = 4,
};

test "M1: an NZB in, segments fetched, and the assembled bytes are the original" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "m1", .{});
    defer fx.deinit();
    try fx.withRelease(small, .{});

    const id = try fx.addRelease(release_name);
    try testing.expect(id > 0);

    const final = try fx.runUntilTerminal(id, 60_000);
    if (final != .completed) {
        std.debug.print("\njob ended in {s}\n", .{final.toString()});
        return error.JobDidNotComplete;
    }

    // Every segment was actually fetched. A pipeline that "completed"
    // without asking the provider for anything would still reach the
    // state above if the delivery step were lenient enough.
    const segments = try fx.segmentCount(id);
    try testing.expectEqual(segments, try fx.segmentsIn(id, .done));
    try testing.expect(fx.provider().served >= @as(usize, @intCast(segments)));
    try testing.expect(fx.provider().accepted >= 1);

    // What the client *sent*, which the reply bytes cannot show. Every
    // article of the release was asked for by name, and nothing else
    // was: a fetcher that asked for the wrong ids and a provider that
    // served them anyway would be indistinguishable from a working one
    // by the assertions above.
    const provider = fx.provider();
    for (fx.release.?.articles) |a| {
        if (!provider.wasRequested(a.message_id)) {
            std.debug.print("\nthe client never asked for {s}\n", .{a.message_id});
            return error.ArticleNeverRequested;
        }
    }
    const asked = try provider.requestedIds(gpa);
    defer gpa.free(asked);
    try testing.expectEqual(fx.release.?.articles.len, asked.len);

    // The handshake: MODE READER on every connection the pool opened,
    // and no AUTHINFO at all, because the server row carries no
    // credentials. A client that authenticated anyway would be leaking
    // an empty username at a provider that never asked.
    try testing.expectEqual(provider.accepted, provider.countVerb("MODE READER"));
    try testing.expect(!provider.sawVerb("AUTHINFO"));
    // And nothing the client sent was refused as malformed.
    try testing.expectEqual(@as(usize, 0), provider.rejected);

    // The four contexts handed out four different ids, so the routing
    // assertions above mean something.
    try fx.expectIdsDistinct(id);

    // The assertion the whole file exists for.
    try fx.expectDeliveredMatchesRelease(release_name);

    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "M1: the outbox timeline shows every stage actually ran" {
    // Go asserts this by subscribing to three topics on the bus. The
    // per-job timeline is the same evidence read from where it is
    // durably kept, and it proves the *bus* carried the pipeline rather
    // than the services happening to be called in the right order.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "m1-timeline", .{});
    defer fx.deinit();
    try fx.withRelease(small, .{});

    const id = try fx.addRelease(release_name);
    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));

    var timeline = try fx.app.bus.eventsByJob(fx.app.db, gpa, id);
    defer timeline.deinit();

    for ([_][]const u8{
        "download.job.created",
        "download.job.started",
        "download.segment.completed",
        "download.file.completed",
        "download.job.download_complete",
        "verify.started",
        "verify.ok",
        "deliver.started",
        "deliver.complete",
        "download.job.completed",
    }) |topic| {
        var seen = false;
        for (timeline.items.items) |env| {
            if (std.mem.eql(u8, env.topic, topic)) {
                seen = true;
                break;
            }
        }
        if (!seen) {
            std.debug.print("\ntimeline is missing {s}; it has:\n", .{topic});
            for (timeline.items.items) |env| std.debug.print("  {s}\n", .{env.topic});
            return error.StageMissingFromTimeline;
        }
    }
}

test "the client authenticates, in order, on every connection a credentialled provider gets" {
    // The provider refuses BODY until AUTHINFO succeeds, so a client that
    // skipped the handshake could not download at all — but one that
    // authenticated on the first connection only, or that sent PASS
    // before USER, would still fetch *something* against a lenient fake.
    // This asserts the sequence per connection instead.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "auth-nntp", .{});
    defer fx.deinit();
    try fx.withRelease(small, .{ .username = "hoardarr", .password = "s3cret" });

    const id = try fx.addRelease(release_name);
    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));

    const provider = fx.provider();
    try testing.expect(provider.accepted > 0);
    try testing.expectEqual(@as(usize, 0), provider.rejected);

    var conn: usize = 1;
    while (conn <= provider.accepted) : (conn += 1) {
        const seq = try provider.commandSequence(gpa, conn);
        defer gpa.free(seq);
        if (seq.len == 0) continue;
        try testing.expect(seq.len >= 2);
        try testing.expectEqualStrings("AUTHINFO USER hoardarr", seq[0]);
        try testing.expectEqualStrings("AUTHINFO PASS s3cret", seq[1]);
        // One handshake per connection, not one per fetch.
        var users: usize = 0;
        for (seq) |line| {
            if (std.mem.startsWith(u8, line, "AUTHINFO USER")) users += 1;
        }
        try testing.expectEqual(@as(usize, 1), users);
    }
    try testing.expectEqual(provider.accepted, provider.countVerb("AUTHINFO USER"));
    try testing.expectEqual(provider.accepted, provider.countVerb("AUTHINFO PASS"));
}

test "M2: the same pipeline driven entirely over HTTP" {
    // Go's M2 is M1 with the job posted through `POST /api/v1/queue/nzb`
    // and the outcome read by polling `GET /api/v1/queue`. That
    // distinction matters: it is the path an operator's browser and an
    // *arr actually take, and it exercises the multipart parser, the
    // REST port and the queue projection rather than the service call.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "m2", .{});
    defer fx.deinit();
    try fx.withRelease(small, .{});

    const boundary = "----hoardarrm2boundary";
    const body = try std.fmt.allocPrint(gpa, "--" ++ boundary ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"nzb\"; filename=\"{s}.nzb\"\r\n" ++
        "Content-Type: application/octet-stream\r\n\r\n" ++
        "{s}\r\n" ++
        "--" ++ boundary ++ "--\r\n", .{ release_name, fx.release.?.nzb });
    defer gpa.free(body);

    var id: i64 = 0;
    {
        var r = try fx.request(.{
            .method = .post,
            .path = "/api/v1/queue/nzb",
            .body = body,
            .content_type = "multipart/form-data; boundary=" ++ boundary,
        });
        defer r.deinit();
        if (r.status != 200 and r.status != 201) {
            std.debug.print("\nPOST /queue/nzb answered {d}: {s}\n", .{ r.status, r.body });
            return error.UploadRejected;
        }
        id = try std.fmt.parseInt(i64, r.field("job_id") orelse return error.NoJobId, 10);
        try testing.expect(id > 0);
    }

    // The job is visible through the queue projection while it runs.
    {
        var r = try fx.get("/api/v1/queue");
        defer r.deinit();
        try testing.expect(r.contains(release_name));
    }

    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));

    // And it moved from the queue to the history, which is the
    // transition an *arr watches for.
    {
        var r = try fx.get("/api/v1/history");
        defer r.deinit();
        try testing.expect(r.contains(release_name));
    }

    try fx.expectDeliveredMatchesRelease(release_name);
}

test "M3a: a clean download verifies against its PAR2 set and is delivered" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "m3a", .{});
    defer fx.deinit();
    // Several files, so a verifier that only ever looks at the first
    // one cannot pass this.
    try fx.withRelease(.{
        .name = release_name,
        .file_count = 3,
        .file_size = 96 * 1024,
        .article_size = 32 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 4,
    }, .{});

    // A first job runs to completion before the one under test, so the
    // job counter and the verify/repair/delivery counters have already
    // moved apart by different amounts when the second job starts. A
    // post-download stage that keys on the publishing context's
    // aggregate would then work on the wrong job — with one job in the
    // database it works on the right one by accident.
    const first_name = release_name ++ ".First";
    var first_release = try tsfixture.generate(gpa, .{
        .name = first_name,
        .file_count = 1,
        .file_size = 32 * 1024,
        .article_size = 16 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 1,
    });
    defer first_release.deinit();
    try fx.serveFixture(&first_release);
    const first_id = try fx.addFixture(&first_release, first_name);
    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(first_id, 60_000));

    const id = try fx.addRelease(release_name);
    try testing.expect(id != first_id);
    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));

    // The two jobs' aggregates really are separate numbers, in both
    // directions: nothing of the second job's is reachable by reading
    // one of the first job's ids as a job id.
    try fx.expectIdsDistinct(first_id);
    try fx.expectIdsDistinct(id);
    const first_ids = try fx.aggregateIds(first_id);
    const second_ids = try fx.aggregateIds(id);
    try testing.expect(first_ids.verify_set != second_ids.verify_set);
    try testing.expect(first_ids.delivery != second_ids.delivery);
    try testing.expect(second_ids.verify_set != id);
    try testing.expect(second_ids.delivery != id);

    // Both releases landed, each under its own name — the failure a
    // mis-routed delivery produces is one job's files under another
    // job's directory.
    try fx.expectDeliveredMatches(&first_release, first_name);

    // Verify ran and said OK. Repair must not have been reached at all:
    // a clean set that still goes through reconstruction means the
    // verifier is wrong about something.
    var timeline = try fx.app.bus.eventsByJob(fx.app.db, gpa, id);
    defer timeline.deinit();
    var saw_started = false;
    var saw_ok = false;
    for (timeline.items.items) |env| {
        if (std.mem.eql(u8, env.topic, "verify.started")) saw_started = true;
        if (std.mem.eql(u8, env.topic, "verify.ok")) saw_ok = true;
        if (std.mem.eql(u8, env.topic, "verify.failed")) return error.VerifyFailedOnACleanSet;
        if (std.mem.eql(u8, env.topic, "repair.started")) return error.RepairRanOnACleanSet;
    }
    try testing.expect(saw_started);
    try testing.expect(saw_ok);

    try fx.expectDeliveredMatchesRelease(release_name);

    // The scratch directory is gone, so a finished job leaves nothing
    // for the next one to trip over — and nothing for a disk-full
    // report to be confused by.
    const job_dir = try std.fmt.allocPrint(gpa, "{s}/{d}", .{ fx.incompleteDir(), id });
    defer gpa.free(job_dir);
    try testing.expect(!h.fileExists(job_dir));
}
