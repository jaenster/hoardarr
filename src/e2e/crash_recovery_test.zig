//! Port of `internal/bootstrap/e2e_crash_recovery_test.go` —
//! `TestM2_E2E_CrashRecovery`.
//!
//! The question the project owner actually asked: **does it survive a
//! restart?** A restart test passes trivially when there is nothing in
//! flight to lose, so this one goes out of its way to have something to
//! lose.
//!
//! # Stronger than the Go original, deliberately
//!
//! Go stops and restarts the *orchestrator* inside a live process
//! (`app.Orchestrator.Stop()` / `.Start(ctx)`). The whole App — the
//! database handle, the outbox, the pools, the aggregates — stays in
//! memory across the "crash", so anything the second run knows it could
//! in principle have remembered rather than re-read.
//!
//! Here the entire `App` is torn down and a new one is booted over the
//! same data directory: new SQLite connection, new outbox, new pools,
//! new fibers, new everything. **Nothing carries over in memory**, so
//! whatever the second daemon knows, it read back off disk. That is the
//! only version of this test that answers the question honestly.
//!
//! # Why it cannot pass by accident
//!
//! Three assertions have to hold together, and no single bug satisfies
//! all three:
//!
//!   1. The crash happens with the download genuinely part-done —
//!      asserted against the `segments` table, not against a timer.
//!   2. The second daemon fetches **strictly fewer** articles than a
//!      from-scratch run would need. The provider is rebuilt on restart
//!      so its counters start at zero, which makes "how much did the
//!      second daemon have to do" directly measurable. A daemon that
//!      restarted from nothing would fetch all of them; a wedged one
//!      would fetch none and never finish.
//!   3. The delivered bytes match the fixture's originals exactly. A
//!      resume that re-fetched into the wrong offset, or skipped a
//!      segment it only *thought* was done, corrupts the output while
//!      leaving the job green.
//!
//! # No sleeps
//!
//! The provider is throttled with `setBytesPerSec` so the download takes
//! long enough to be interrupted, and the crash point is found by
//! polling the segments table on the loop — not by sleeping for "long
//! enough", which is the version of this test that flakes.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const sys = @import("../posix/sys.zig");
const fiber = @import("../posix/fiber.zig");
const tsfixture = @import("../testserver/fixture.zig");

const release_name = "Hoardarr.Crash.Release";

/// Big enough that a throttled fetch spans several of the orchestrator's
/// one-second flushes, so the crash lands between them rather than
/// before the first.
const release_opts: tsfixture.Options = .{
    .name = release_name,
    .file_count = 1,
    .file_size = 512 * 1024,
    .article_size = 32 * 1024,
    .par2_slice_size = 16 * 1024,
    .recovery_slices = 4,
};

test "crash recovery: a daemon killed mid-download resumes instead of restarting" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "crash", .{});
    defer fx.deinit();

    // Paced writes, so the download is still running when we pull the
    // plug. Fast enough that the whole test is a couple of seconds,
    // slow enough that it cannot finish before the first flush.
    try fx.withRelease(release_opts, .{ .bytes_per_sec = 512 * 1024 });

    const id = try fx.addRelease(release_name);
    try testing.expect(id > 0);

    const total_segments = try fx.segmentCount(id);
    try testing.expect(total_segments > 4);

    // Run until the download is genuinely part-done: at least one
    // segment persisted as `done`, and at least one still outstanding.
    // Read from the table, because what is in the table is precisely
    // what the next daemon will get to start from.
    {
        const deadline = sys.monotonicNanos() + 30 * std.time.ns_per_s;
        var reached = false;
        while (sys.monotonicNanos() < deadline) {
            _ = try fx.app.loop.tick(1);
            const done = try fx.segmentsIn(id, .done);
            if (done > 0 and done < total_segments) {
                reached = true;
                break;
            }
            if (done >= total_segments) break;
        }
        if (!reached) {
            std.debug.print(
                "\nnever caught the job part-done: {d} of {d} segments done\n",
                .{ try fx.segmentsIn(id, .done), total_segments },
            );
            return error.NoPartialProgressToLose;
        }
    }

    const done_before = try fx.segmentsIn(id, .done);
    const served_before = fx.provider().served;
    try testing.expect(served_before > 0);
    // The job must still be active — if it had already finished there
    // would be nothing in flight and the test would prove nothing.
    try testing.expect(!(try fx.jobState(id)).isTerminal());

    // ---- the crash ----
    //
    // The whole daemon goes. The provider is rebuilt on the same port,
    // with its counters back at zero, so everything it serves from here
    // is work the *second* daemon had to do.
    try fx.restart(.{});

    // Nothing was lost on the way down: the segments that were done
    // before the crash are still done, and none of the outstanding ones
    // was flipped to `failed` by the teardown. Go asserts the second
    // half of this as `verifySegmentNotFailed`; a cancelled in-flight
    // fetch must come back as `pending`, not as a permanent failure the
    // retry budget will not forgive.
    try testing.expectEqual(done_before, try fx.segmentsIn(id, .done));
    try testing.expectEqual(@as(i64, 0), try fx.segmentsIn(id, .failed));
    try testing.expectEqual(@as(i64, 0), try fx.segmentsIn(id, .inflight));

    // ---- the resume ----
    const final = try fx.runUntilTerminal(id, 60_000);
    if (final != .completed) {
        std.debug.print("\nafter the crash the job ended in {s}\n", .{final.toString()});
        return error.JobDidNotCompleteAfterCrash;
    }

    // It resumed rather than restarted. The second daemon fetched
    // strictly less than a from-scratch run would have needed, and the
    // shortfall is at least the segments that were already done.
    const served_after = fx.provider().served;
    if (served_after >= total_segments) {
        std.debug.print(
            "\nafter the crash the daemon fetched {d} articles; a resume needs fewer than {d}\n",
            .{ served_after, total_segments },
        );
        return error.RestartedFromZero;
    }

    // ---- and the bytes ----
    //
    // The assertion the whole test exists for: a resume that wrote a
    // re-fetched segment at the wrong offset, or skipped one it wrongly
    // believed done, leaves a green job and a corrupt file.
    try fx.expectDeliveredMatchesRelease(release_name);

    // No fiber left parked on a socket that no longer exists.
    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

/// A release big enough that verify has real work to do, so the window
/// between "the stage was queued" and "the stage committed" is wide
/// enough to land in on purpose rather than by luck.
const post_download_opts: tsfixture.Options = .{
    .name = release_name,
    .file_count = 3,
    .file_size = 512 * 1024,
    .article_size = 64 * 1024,
    .par2_slice_size = 16 * 1024,
    .recovery_slices = 4,
};

/// Advances the loop until `stop` says the daemon is at the point the
/// test wants to kill it at, then returns. Never sleeps; the condition
/// is read from the database and the outbox, which is exactly what the
/// next daemon will get to start from.
fn runUntilCrashPoint(
    fx: *h.Harness,
    id: i64,
    comptime stop: fn (*h.Harness, i64) anyerror!bool,
) !void {
    const deadline = sys.monotonicNanos() + 60 * std.time.ns_per_s;
    while (sys.monotonicNanos() < deadline) {
        _ = try fx.app.loop.tick(1);
        if (try stop(fx, id)) return;
        if ((try fx.jobState(id)).isTerminal()) {
            // The pipeline ran past the point we meant to kill it at, so
            // whatever the rest of the test asserts would be about a
            // different situation. Say that, rather than quietly
            // testing something easier.
            std.debug.print("\nthe crash window closed before we could use it: job is {s}\n", .{
                (try fx.jobState(id)).toString(),
            });
            return error.CrashWindowMissed;
        }
    }
    std.debug.print("\nthe crash window never opened: job is {s}\n", .{
        (try fx.jobState(id)).toString(),
    });
    return error.CrashWindowNeverOpened;
}

test "crash recovery: a crash between download_complete and verify still reaches completed" {
    // `bootstrap/offload.zig` settles the outbox row when a post-download
    // stage is *queued*, not when it commits. Its own doc states the
    // trade: "a crash between the hand-off and the stage's own commit
    // loses that redelivery", justified by the stages being idempotent.
    //
    // Idempotent only helps if something re-triggers the stage. This
    // test is the question that follows from that: after such a crash,
    // does anything drive the job again?
    //
    // The assertion is deliberately the end of the pipeline — completed,
    // with the delivered bytes matching. "The job is still in the
    // database" would pass while the job is permanently stuck at
    // `download_complete` with nothing left to nudge it, which is a
    // worse outcome than the redelivery loss the outbox exists to
    // prevent.
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "crash-postdl", .{});
    defer fx.deinit();
    try fx.withRelease(post_download_opts, .{});

    const id = try fx.addRelease(release_name);

    // The exact window: the download is committed as complete — so the
    // `download.job.download_complete` row is durable and was handed to
    // the pipeline — and verify has not yet recorded that it started.
    const At = struct {
        fn handedOff(f: *h.Harness, job: i64) anyerror!bool {
            if ((try f.jobState(job)) != .download_complete) return false;
            return !(try f.hasEvent(job, "verify.started"));
        }
    };
    try runUntilCrashPoint(fx, id, At.handedOff);

    // Nothing downstream had committed, so the second daemon genuinely
    // has to re-drive the pipeline rather than find it already done.
    try testing.expect(!(try fx.hasEvent(id, "verify.ok")));
    try testing.expect(!(try fx.hasEvent(id, "deliver.complete")));
    try testing.expectEqual(h.JobState.download_complete, try fx.jobState(id));

    // ---- the crash ----
    try fx.restart(.{});

    // The job must not be stranded. Everything it still needs — verify,
    // then deliver — has to happen without a human touching it.
    const final = fx.runUntilTerminal(id, 90_000) catch |e| {
        std.debug.print(
            "\nafter a crash at download_complete the job is stuck in {s}: nothing re-drove the pipeline\n",
            .{(fx.jobState(id) catch h.JobState.failed).toString()},
        );
        return e;
    };
    if (final != .completed) {
        std.debug.print("\nafter the crash the job ended in {s}\n", .{final.toString()});
        return error.PostDownloadCrashNotRecovered;
    }

    // The bytes first, because that is what the operator actually gets:
    // the recovered run has to produce the same files an uninterrupted
    // one would.
    try fx.expectDeliveredMatchesRelease(release_name);

    // And the job's durable timeline has to record what the second
    // daemon did. The per-job timeline is what the UI shows and what an
    // operator reads to find out what happened to a release; one that
    // stops dead at the crash says the job never finished, while the
    // history says it did.
    if (!(try fx.hasEvent(id, "verify.ok")) or !(try fx.hasEvent(id, "deliver.complete"))) {
        std.debug.print(
            "\nthe job reached {s} and delivered correctly, but its timeline has no record of it:\n",
            .{final.toString()},
        );
        var timeline = try fx.app.bus.eventsByJob(fx.app.db, gpa, id);
        defer timeline.deinit();
        for (timeline.items.items) |env| std.debug.print("  {s}\n", .{env.topic});
        return error.RecoveredRunLeftNoTimeline;
    }

    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "crash recovery: a crash during verify re-runs it rather than stranding the job" {
    // The window the offload change genuinely widened: verify is on a
    // worker, mid-hash, when the process dies. `App.deinit` joins the
    // pool and then cancels the stage fiber, so the CPU work finishes
    // but its commit never happens — which is precisely the state a
    // `SIGKILL` mid-verification leaves behind, reproduced deterministically.
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "crash-verify", .{});
    defer fx.deinit();
    try fx.withRelease(post_download_opts, .{});

    const id = try fx.addRelease(release_name);

    const At = struct {
        fn midVerify(f: *h.Harness, job: i64) anyerror!bool {
            if (!(try f.hasEvent(job, "verify.started"))) return false;
            return !(try f.hasEvent(job, "verify.ok")) and
                !(try f.hasEvent(job, "verify.failed"));
        }
    };
    try runUntilCrashPoint(fx, id, At.midVerify);

    // ---- the crash ----
    try fx.restart(.{});

    const final = fx.runUntilTerminal(id, 90_000) catch |e| {
        std.debug.print(
            "\nafter a crash during verify the job is stuck in {s}\n",
            .{(fx.jobState(id) catch h.JobState.failed).toString()},
        );
        return e;
    };
    if (final != .completed) {
        std.debug.print("\nafter a crash during verify the job ended in {s}\n", .{final.toString()});
        return error.VerifyCrashNotRecovered;
    }

    // A re-run verify must reach the same conclusion — the download was
    // clean — and the delivery must still be exact. A verify that
    // resumed from a half-written hash state would show up here.
    try testing.expect(try fx.hasEvent(id, "verify.ok"));
    try fx.expectDeliveredMatchesRelease(release_name);
    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "crash recovery: a job queued but never started survives and then runs" {
    // The other half of the restart story, and the cheaper one to get
    // wrong: a job added seconds before a container update must still be
    // there afterwards, and must actually run rather than sit in the
    // queue forever because nothing re-admitted it.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "crash-queued", .{});
    defer fx.deinit();

    // No engine yet, so nothing can pick the job up before the restart.
    fx.release = try tsfixture.generate(gpa, release_opts);

    const id = try fx.addRelease(release_name);
    try testing.expect(id > 0);
    try testing.expect(!(try fx.jobState(id)).isTerminal());

    // Restart, and only then give it somewhere to fetch from — which is
    // exactly the shape of "the operator added the NZB before the
    // provider was configured, then restarted".
    try fx.restart(.{});

    const port = try fx.startNntp(.{});
    try fx.serveRelease();
    {
        const dserver = @import("../domain/server.zig");
        const repo_server = @import("../store/repo_server.zig");
        const infra = @import("../bootstrap/infra.zig");
        var row = try dserver.UsenetServer.init(gpa, .{
            .name = "stub",
            .host = "127.0.0.1",
            .port = @intCast(port),
            .tls = false,
            .max_conns = 4,
        }, infra.nowMillis());
        defer row.deinit();
        try repo_server.ServerRepo.init(gpa, fx.app.db).save(&row);
    }
    try fx.startEngine();

    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));
    try fx.expectDeliveredMatchesRelease(release_name);
}
