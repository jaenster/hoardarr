//! Startup reconciliation of the post-download pipeline.
//!
//! # Why anything has to sweep at all
//!
//! Every stage after the download is triggered by an outbox event, and
//! the outbox settles a row once its handler has *accepted* the work —
//! see `bootstrap/offload.zig`, which queues a stage and returns. That
//! makes the loop responsive and it makes exactly one window unsafe: a
//! process that dies between "the event was accepted" and "the stage
//! committed" loses the redelivery, because the bus already considers
//! that row consumed.
//!
//! The stages are idempotent, so re-running one is free. But idempotency
//! only helps if something re-runs it, and nothing does: the download
//! orchestrator re-admits jobs that still have segments to fetch, and a
//! job parked at `download_complete` has none. It sits there forever.
//!
//! # What this does instead
//!
//! One pass over the jobs the database says are alive, and for each one
//! in a post-download state, the stage its aggregates imply:
//!
//!   * no verify pass, or one still running → **verify**
//!   * `repair_needed` → **repair**, or **reverify** when a previous
//!     repair already succeeded and only the re-check was lost
//!   * `ok` → **extract** and **deliver**, which is exactly what a
//!     `verify.ok` event does: both subscribe, both apply the mirror
//!     predicate, and precisely one of them acts
//!   * `failed` → **verify** again, which finds its terminal set and
//!     makes sure the Job reflects it
//!
//! The previous version of this swept only for a *delivery*, which meant
//! a crash before verification handed an unverified release straight to
//! the mover. Delivering a release nobody checked is the one outcome
//! worse than not delivering it at all, so the verdict is now read
//! rather than assumed.
//!
//! # It only ever queues
//!
//! Nothing here runs a stage. The sweep hands ids to the same queue the
//! bus handlers use, so a hundred stranded jobs cost a hundred queue
//! entries and one pass over the aggregates rather than a hundred
//! verifications on the boot path.

const std = @import("std");

const log = @import("../core/log.zig");
const app_ports = @import("ports.zig");
const dl_ports = @import("download/ports.zig");
const dverify = @import("../domain/verify.zig");
const drepair = @import("../domain/repair.zig");
const dextract = @import("../domain/extract.zig");
const ddeliver = @import("../domain/deliver.zig");

const Allocator = std.mem.Allocator;

pub const JobId = dl_ports.JobId;

/// The post-download stages, in the one vocabulary the pipeline uses.
///
/// `bootstrap/offload.zig` dispatches on this rather than on a copy of
/// it: two parallel enums for the same five stages is precisely the sort
/// of pair that drifts once and then silently runs `extract` where the
/// caller meant `deliver`.
pub const Stage = enum {
    verify,
    /// Verification re-run after a successful repair; resets the stale
    /// `repair_needed` verdict first.
    reverify,
    repair,
    extract,
    deliver,

    pub fn text(s: Stage) []const u8 {
        return switch (s) {
            .verify => "verify",
            .reverify => "verify-after-repair",
            .repair => "repair",
            .extract => "extract",
            .deliver => "deliver",
        };
    }
};

pub const QueueError = error{ Full, OutOfMemory };

/// Where a re-driven stage goes. The same door the bus handlers use, so
/// a swept job is indistinguishable from an event-driven one from here
/// on.
pub const StageQueue = struct {
    ctx: *anyopaque,
    enqueueFn: *const fn (ctx: *anyopaque, stage: Stage, job_id: JobId) QueueError!void,

    pub fn enqueue(self: StageQueue, stage: Stage, job_id: JobId) QueueError!void {
        return self.enqueueFn(self.ctx, stage, job_id);
    }
};

pub const Error = dl_ports.RepoError || Allocator.Error;

pub const Reconciler = struct {
    gpa: Allocator,
    jobs: dl_ports.JobStore,
    verify_sets: app_ports.Repo(dverify.VerifySet),
    repairs: app_ports.Repo(drepair.Repair),
    queue: StageQueue,
    logger: *log.Logger = &log.default,

    /// Sweeps once. Returns how many jobs were re-driven.
    pub fn run(self: *Reconciler) Error!usize {
        const active = try self.jobs.active(self.gpa, null);
        defer self.gpa.free(active);

        var swept: usize = 0;
        for (active) |row| {
            switch (row.state) {
                // The states in which the download is over and some
                // post-download stage owes the job an answer.
                .download_complete, .verifying, .repairing, .unpacking => {},
                else => continue,
            }
            if (self.reviveOne(row.id)) swept += 1;
        }
        if (swept > 0) {
            self.logger.info("recovery: re-drove stranded pipeline stages", &.{
                log.uint("jobs", swept),
            });
        }
        return swept;
    }

    /// Queues whatever `job_id` still owes. Returns whether anything was
    /// queued; a failure to queue is logged rather than propagated,
    /// because one job the backlog has no room for must not stop the
    /// sweep for the rest.
    fn reviveOne(self: *Reconciler, job_id: JobId) bool {
        const stages = self.nextStages(job_id);
        var queued = false;
        for (stages.slice()) |stage| {
            self.queue.enqueue(stage, job_id) catch |e| {
                self.logger.warn("recovery: cannot queue a stage", &.{
                    log.int("job_id", job_id),
                    log.str("stage", stage.text()),
                    log.errv("err", e),
                });
                continue;
            };
            self.logger.info("recovery: re-driving a stranded stage", &.{
                log.int("job_id", job_id),
                log.str("stage", stage.text()),
            });
            queued = true;
        }
        return queued;
    }

    /// At most two, and two only for the `verify.ok` fan-out.
    const Stages = struct {
        buf: [2]Stage = undefined,
        n: usize = 0,

        fn add(self: *Stages, s: Stage) void {
            self.buf[self.n] = s;
            self.n += 1;
        }

        fn slice(self: *const Stages) []const Stage {
            return self.buf[0..self.n];
        }
    };

    fn nextStages(self: *Reconciler, job_id: JobId) Stages {
        var out: Stages = .{};

        // No verdict yet, or one that never finished: the verification
        // itself is what was lost.
        const vset = self.verify_sets.byJobId(null, job_id) catch {
            out.add(.verify);
            return out;
        };
        const vstate = vset.state;
        self.verify_sets.release(vset);

        switch (vstate) {
            .pending, .verifying => out.add(.verify),
            // Terminal, but the Job was never told. Re-running verify is
            // a no-op that ends in `already_terminal`, and that path is
            // what pulls the Job across to failed.
            .failed => out.add(.verify),
            .ok => {
                // Exactly the `verify.ok` fan-out: `app/extract` takes
                // archive jobs, `app/deliver` takes the rest, each by
                // running the same predicate. Queueing both here rather
                // than guessing keeps that decision in one place.
                out.add(.extract);
                out.add(.deliver);
            },
            .repair_needed => {
                const rep = self.repairs.byJobId(null, job_id) catch {
                    out.add(.repair);
                    return out;
                };
                const rstate = rep.state;
                self.repairs.release(rep);
                // A repair that already succeeded is owed the re-check
                // that `repair.ok` would have triggered; anything else
                // is owed the reconstruction pass itself.
                out.add(if (rstate == .ok) .reverify else .repair);
            },
        }
        return out;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const job_mod = @import("../domain/download/job.zig");
const ddevents = @import("../domain/download/events.zig");

const Recorder = struct {
    seen: std.ArrayList(struct { stage: Stage, job_id: JobId }) = .empty,
    gpa: Allocator,
    fail: ?QueueError = null,

    fn queue(self: *Recorder) StageQueue {
        return .{ .ctx = @ptrCast(self), .enqueueFn = &enqueue };
    }

    fn enqueue(ctx: *anyopaque, stage: Stage, job_id: JobId) QueueError!void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        if (self.fail) |e| return e;
        try self.seen.append(self.gpa, .{ .stage = stage, .job_id = job_id });
    }

    fn has(self: *const Recorder, stage: Stage, job_id: JobId) bool {
        for (self.seen.items) |s| {
            if (s.stage == stage and s.job_id == job_id) return true;
        }
        return false;
    }

    fn deinit(self: *Recorder) void {
        self.seen.deinit(self.gpa);
    }
};

const Harness = struct {
    jobs: dl_ports.FakeJobStore = undefined,
    sets: app_ports.FakeRepo(dverify.VerifySet) = undefined,
    repairs: app_ports.FakeRepo(drepair.Repair) = undefined,
    rec: Recorder = undefined,
    logger: log.Logger = .{},
    svc: Reconciler = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.jobs = dl_ports.FakeJobStore.init(testing.allocator);
        self.sets = app_ports.FakeRepo(dverify.VerifySet).init(testing.allocator);
        self.repairs = app_ports.FakeRepo(drepair.Repair).init(testing.allocator);
        self.rec = .{ .gpa = testing.allocator };
        self.svc = .{
            .gpa = testing.allocator,
            .jobs = self.jobs.store(),
            .verify_sets = self.sets.repo(),
            .repairs = self.repairs.repo(),
            .queue = self.rec.queue(),
            .logger = &self.logger,
        };
    }

    fn deinit(self: *Harness) void {
        self.rec.deinit();
        self.repairs.deinit();
        self.sets.deinit();
        self.jobs.deinit();
    }

    fn seedJob(self: *Harness, state: job_mod.JobState) !*job_mod.Job {
        const j = try testing.allocator.create(job_mod.Job);
        j.* = try job_mod.Job.init(testing.allocator, .{
            .nzb_hash = "h",
            .name = "rel",
            .files = &.{.{
                .filename = "movie.mkv",
                .size_bytes = 100,
                .segments = &.{.{ .seq_index = 1, .message_id = "d@h", .bytes = 100 }},
            }},
        }, 0);
        ddevents.deinitAll(testing.allocator, try j.pullEvents());
        j.state = state;
        try self.jobs.insert(j);
        return j;
    }

    fn seedVerify(self: *Harness, job_id: JobId, state: dverify.VerifyState) !void {
        const v = try testing.allocator.create(dverify.VerifySet);
        v.* = try dverify.VerifySet.init(testing.allocator, job_id, 1);
        devent.deinitAll(dverify.Event, testing.allocator, try v.pullEvents());
        v.state = state;
        try self.sets.insert(v);
    }

    fn seedRepair(self: *Harness, job_id: JobId, state: drepair.State) !void {
        const r = try testing.allocator.create(drepair.Repair);
        r.* = try drepair.Repair.init(testing.allocator, job_id, 1);
        devent.deinitAll(drepair.Event, testing.allocator, try r.pullEvents());
        r.state = state;
        try self.repairs.insert(r);
    }
};

const devent = @import("../domain/event.zig");

test "a job stranded at download_complete with no verdict is sent back to verify" {
    // The exact crash the offload's settle-on-queue trades away: the
    // event was consumed, the stage never committed, and nothing else in
    // the daemon would ever look at this job again.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seedJob(.download_complete);

    try testing.expectEqual(@as(usize, 1), try h.svc.run());
    try testing.expect(h.rec.has(.verify, j.id));
    try testing.expectEqual(@as(usize, 1), h.rec.seen.items.len);
}

test "a verified job is offered to both verify.ok subscribers, not just the mover" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seedJob(.download_complete);
    try h.seedVerify(j.id, .ok);

    _ = try h.svc.run();
    try testing.expect(h.rec.has(.extract, j.id));
    try testing.expect(h.rec.has(.deliver, j.id));
}

test "an unverified job is never handed to the mover" {
    // The bug this file replaces: the old sweep ran `deliver` for
    // anything sitting in a post-download state, which moved a release
    // nobody had checked into complete/.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seedJob(.download_complete);
    try h.seedVerify(j.id, .verifying);

    _ = try h.svc.run();
    try testing.expect(h.rec.has(.verify, j.id));
    try testing.expect(!h.rec.has(.deliver, j.id));
    try testing.expect(!h.rec.has(.extract, j.id));
}

test "damage with no repair pass yet goes to repair" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seedJob(.repairing);
    try h.seedVerify(j.id, .repair_needed);

    _ = try h.svc.run();
    try testing.expect(h.rec.has(.repair, j.id));
}

test "a repair that already succeeded is owed the re-check, not another pass" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seedJob(.repairing);
    try h.seedVerify(j.id, .repair_needed);
    try h.seedRepair(j.id, .ok);

    _ = try h.svc.run();
    try testing.expect(h.rec.has(.reverify, j.id));
    try testing.expect(!h.rec.has(.repair, j.id));
}

test "a terminal failed verdict is re-offered so the job can be taken terminal" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seedJob(.download_complete);
    try h.seedVerify(j.id, .failed);

    _ = try h.svc.run();
    try testing.expect(h.rec.has(.verify, j.id));
}

test "jobs that are still downloading or already terminal are left alone" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.seedJob(.downloading);
    _ = try h.seedJob(.queued);
    _ = try h.seedJob(.paused);

    try testing.expectEqual(@as(usize, 0), try h.svc.run());
    try testing.expectEqual(@as(usize, 0), h.rec.seen.items.len);
}

test "a full backlog is logged and the sweep keeps going" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    _ = try h.seedJob(.download_complete);
    _ = try h.seedJob(.download_complete);
    h.rec.fail = error.Full;

    try testing.expectEqual(@as(usize, 0), try h.svc.run());
}
