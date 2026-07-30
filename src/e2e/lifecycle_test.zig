//! Port of `internal/bootstrap/e2e_lifecycle_test.go` —
//! `TestM2_E2E_PauseResume` and `TestM2_E2E_RemoveMidFlight`.
//!
//! Both are about what happens to a job that has segments *in flight*
//! when an operator clicks something. The interesting failures are not
//! "the button did nothing" — they are "the runner was cancelled and
//! left the outstanding segments marked failed", and "the row went away
//! but the half-written file did not".
//!
//! Go holds the download still with a gated NNTP stub that blocks each
//! body until the test releases it. `testserver/nntp.zig` has no gate,
//! but it has a rate limiter, which gets the same property with less
//! machinery: the provider is slowed to the point where the download is
//! reliably still running, and the test polls the segments table on the
//! loop for the moment it is part-done. Nothing sleeps either way.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const sys = @import("../posix/sys.zig");
const fiber = @import("../posix/fiber.zig");
const tsfixture = @import("../testserver/fixture.zig");

const release_name = "Hoardarr.Lifecycle.Release";

const release_opts: tsfixture.Options = .{
    .name = release_name,
    .file_count = 1,
    .file_size = 512 * 1024,
    .article_size = 32 * 1024,
    .par2_slice_size = 16 * 1024,
    .recovery_slices = 4,
};

/// Advances the loop until at least one segment of `id` is persisted as
/// done and at least one is not, so an operator action lands on a job
/// that genuinely has work outstanding.
fn runUntilPartDone(fx: *h.Harness, id: i64) !void {
    const total = try fx.segmentCount(id);
    const deadline = sys.monotonicNanos() + 30 * std.time.ns_per_s;
    while (sys.monotonicNanos() < deadline) {
        _ = try fx.app.loop.tick(1);
        const done = try fx.segmentsIn(id, .done);
        if (done > 0 and done < total) return;
        if (done >= total) break;
    }
    return error.NoWorkInFlight;
}

test "lifecycle: pause mid-flight leaves the outstanding segments pending, and resume finishes" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "pause", .{});
    defer fx.deinit();
    try fx.withRelease(release_opts, .{ .bytes_per_sec = 512 * 1024 });

    const id = try fx.addRelease(release_name);
    try runUntilPartDone(fx, id);
    const done_at_pause = try fx.segmentsIn(id, .done);

    // Through the public REST surface, which is the path the UI takes:
    // handler → queue service → bus event → the engine cancels the
    // runner. Calling the service directly would skip the half that
    // has the bugs.
    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/queue/{d}/pause", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .post, .path = path });
        defer r.deinit();
        if (r.status != 200 and r.status != 202 and r.status != 204) {
            std.debug.print("\npause answered {d}: {s}\n", .{ r.status, r.body });
            return error.PauseRejected;
        }
    }

    // The API returns before the runner has finished unwinding, so the
    // settled state is waited for on the loop rather than assumed.
    try fx.runUntilState(id, .paused, 30_000);

    // The assertion that matters: a cancelled in-flight fetch comes
    // back as pending, not as a permanent failure. Go's
    // `verifySegmentNotFailed` is this line.
    try testing.expectEqual(@as(i64, 0), try fx.segmentsIn(id, .failed));
    try testing.expectEqual(@as(i64, 0), try fx.segmentsIn(id, .inflight));
    // And nothing already fetched was thrown away.
    try testing.expect((try fx.segmentsIn(id, .done)) >= done_at_pause);

    // A paused job stays paused. Without this the test would pass
    // against an engine that ignored the pause and simply finished.
    {
        var i: usize = 0;
        while (i < 200) : (i += 1) _ = try fx.app.loop.tick(1);
        try testing.expectEqual(h.JobState.paused, try fx.jobState(id));
    }

    // Resume, and it finishes — with the delivered bytes intact, which
    // is what proves the resume picked up rather than started over.
    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/queue/{d}/resume", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .post, .path = path });
        defer r.deinit();
        if (r.status >= 300) {
            std.debug.print("\nresume answered {d}: {s}\n", .{ r.status, r.body });
            return error.ResumeRejected;
        }
    }

    // Unthrottle so the rest of the download does not spend the budget.
    fx.provider().setBytesPerSec(0);
    try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));
    try fx.expectDeliveredMatchesRelease(release_name);

    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "lifecycle: removing a job mid-flight cancels it and purges its scratch directory" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "remove", .{});
    defer fx.deinit();
    try fx.withRelease(release_opts, .{ .bytes_per_sec = 512 * 1024 });

    const id = try fx.addRelease(release_name);
    try runUntilPartDone(fx, id);

    // The half-written file has to exist first, or "it was purged"
    // proves nothing.
    const job_dir = try std.fmt.allocPrint(gpa, "{s}/{d}", .{ fx.incompleteDir(), id });
    defer gpa.free(job_dir);
    try testing.expect(h.fileExists(job_dir));

    {
        const path = try std.fmt.allocPrint(gpa, "/api/v1/queue/{d}", .{id});
        defer gpa.free(path);
        var r = try fx.request(.{ .method = .delete, .path = path });
        defer r.deinit();
        if (r.status >= 300) {
            std.debug.print("\ndelete answered {d}: {s}\n", .{ r.status, r.body });
            return error.DeleteRejected;
        }
    }

    // The row is gone, the slot came back, and the bytes on disk went
    // with it. A removal that leaves the scratch tree behind is how a
    // disk fills up with downloads nobody asked for any more.
    {
        const deadline = sys.monotonicNanos() + 30 * std.time.ns_per_s;
        var gone = false;
        while (sys.monotonicNanos() < deadline) {
            _ = try fx.app.loop.tick(1);
            const rows = try fx.app.db.scalarInt("SELECT count(*) FROM jobs WHERE id = ?", .{id});
            if (rows == 0 and !h.fileExists(job_dir)) {
                gone = true;
                break;
            }
        }
        if (!gone) {
            std.debug.print("\nafter delete: rows={d}, dir present={}\n", .{
                fx.app.db.scalarInt("SELECT count(*) FROM jobs WHERE id = ?", .{id}) catch -1,
                h.fileExists(job_dir),
            });
            return error.RemoveDidNotPurge;
        }
    }

    try testing.expectEqual(@as(usize, 0), fx.app.engine.activeSlots());
    try testing.expectEqual(stacks_before, fiber.liveStacks());

    // The loop still works afterwards, which is the "not wedged" half.
    _ = try fx.app.loop.tick(1);
}
