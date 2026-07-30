//! Port of `internal/bootstrap/e2e_m3b_test.go` —
//! `TestM3b_E2E_RepairAndDeliver`.
//!
//! The one test where PAR2 has to do actual work: articles genuinely go
//! missing, verify says the set is damaged, reconstruction runs, and the
//! delivered file is byte-identical to the original anyway.
//!
//! # How the damage is made, and why it is not a corruption
//!
//! Go posts a *wrong* second segment whose yEnc CRC matches the wrong
//! bytes, so the orchestrator accepts it and the damage only surfaces at
//! verify. That works, but it tests a case that does not happen: a real
//! provider does not serve you plausible-looking wrong bytes with a
//! matching checksum.
//!
//! What real providers do is not have the article. `setMissingFraction`
//! reproduces exactly that, and it is deterministic — the drop decision
//! is a hash of the message-id, so the same articles vanish on every
//! run and on every retry. `wouldDrop` is public, so the loss set is
//! computed up front and asserted rather than inferred from what the run
//! happened to fetch.
//!
//! The recovery slice count is sized above the worst-case loss, which is
//! the difference between "repair worked" and "we got lucky".

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const fiber = @import("../posix/fiber.zig");
const tsfixture = @import("../testserver/fixture.zig");

const release_name = "Hoardarr.Repair.Release";

test "M3b: articles genuinely go missing, PAR2 repairs, and the delivered bytes are exact" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try h.Harness.init(gpa, "m3b", .{});
    defer fx.deinit();

    // 8 data slices' worth of file with 8 recovery slices: enough parity
    // to survive losing any 8, which comfortably covers a 20% drop over
    // the articles below. Sized deliberately rather than generously —
    // a set with parity for everything would pass even if reconstruction
    // silently did nothing and the verifier were lenient.
    try fx.withRelease(.{
        .name = release_name,
        .file_count = 1,
        .file_size = 256 * 1024,
        .article_size = 32 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 8,
    }, .{ .missing_fraction = 0.2 });

    // The loss set is a pure function of the message-ids, so it is known
    // before the run. If the fixture and the salt happen to drop nothing
    // there is no repair to test, and the test says so rather than
    // passing.
    var will_drop: usize = 0;
    for (fx.release.?.articles) |a| {
        if (fx.provider().wouldDrop(a.message_id)) will_drop += 1;
    }
    if (will_drop == 0) {
        std.debug.print("\nthe missing fraction dropped nothing; there is no repair to test\n", .{});
        return error.NothingWouldBeMissing;
    }

    const id = try fx.addRelease(release_name);
    const final = try fx.runUntilTerminal(id, 120_000);
    if (final != .completed) {
        std.debug.print("\njob ended in {s}; want completed after repair\n", .{final.toString()});
        return error.RepairDidNotComplete;
    }

    // Articles really were missing — the provider counted the 430s, and
    // the segments table kept them as `missing` rather than quietly
    // reporting them done.
    try testing.expect(fx.provider().missed > 0);

    // Verify saw the damage and repair ran. Both halves are asserted:
    // a `verify.ok` here would mean the verifier never noticed, and a
    // completion without `repair.` would mean the file was delivered
    // damaged.
    var timeline = try fx.app.bus.eventsByJob(fx.app.db, gpa, id);
    defer timeline.deinit();
    var saw_repair = false;
    var saw_repair_ok = false;
    for (timeline.items.items) |env| {
        if (std.mem.startsWith(u8, env.topic, "repair.")) saw_repair = true;
        if (std.mem.eql(u8, env.topic, "repair.ok")) saw_repair_ok = true;
    }
    if (!saw_repair) {
        std.debug.print("\nthe job completed without repair running; timeline:\n", .{});
        for (timeline.items.items) |env| std.debug.print("  {s}\n", .{env.topic});
        return error.RepairNeverRan;
    }
    try testing.expect(saw_repair_ok);

    // The point of the whole exercise: reconstruction produced the
    // original bytes, not merely a file of the right length.
    try fx.expectDeliveredMatchesRelease(release_name);

    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "M3b: damage beyond the parity is reported, not delivered as if it were fine" {
    // The other half of repair, and the one that matters for
    // correctness: when there is not enough parity the job must fail.
    // A pipeline that delivers a half-repaired file is worse than one
    // that fails, because the operator finds out from a media player.
    const gpa = testing.allocator;

    var fx = try h.Harness.init(gpa, "m3b-hopeless", .{});
    defer fx.deinit();

    // One recovery slice against a 60% loss: no combination of
    // equations spans that.
    try fx.withRelease(.{
        .name = release_name,
        .file_count = 1,
        .file_size = 256 * 1024,
        .article_size = 32 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 1,
    }, .{ .missing_fraction = 0.6 });

    const id = try fx.addRelease(release_name);
    const final = try fx.runUntilTerminal(id, 120_000);
    if (final == .completed) {
        std.debug.print("\nan unrepairable release was reported completed\n", .{});
        return error.UnrepairableDeliveredAsComplete;
    }
    try testing.expectEqual(h.JobState.failed, final);

    // And the slot came back, so one hopeless job does not cost the
    // daemon a concurrency slot forever.
    try testing.expectEqual(@as(usize, 0), fx.app.engine.activeSlots());
}
