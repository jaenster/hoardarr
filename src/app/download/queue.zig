//! Operator actions on the download queue: pause, resume, remove,
//! reorder, force-complete, retry.
//!
//! Every mutator follows the same shape — open a transaction, load the
//! aggregate, ask the domain to transition, and *only if the domain
//! actually recorded an event* write the row and publish. That guard is
//! what makes each of these idempotent: pausing an already-paused job
//! returns success having touched neither the database nor the bus, so a
//! double-click in the UI cannot produce two `JobPaused` events and two
//! orchestrator stop-runner cycles.
//!
//! The read-side queries (`list`, `history`, the `*Shallow` variants)
//! that Go put on this service are deliberately absent. They were
//! straight pass-throughs to the repository with no application logic,
//! and the API layer talking to the read model directly is one less hop
//! and one less place to forget a `Shallow`.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const ports = @import("ports.zig");
const dtx = @import("../../domain/tx.zig");
const job_mod = @import("../../domain/download/job.zig");
const devents = @import("../../domain/download/events.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Job = job_mod.Job;
pub const JobId = ports.JobId;
pub const JobSummary = ports.JobSummary;
pub const Sink = app_ports.EventSink(devents.Event);

pub const Error = ports.RepoError || app_ports.PublishError || app_ports.TxError;

pub const Service = struct {
    gpa: Allocator,
    store: ports.JobStore,
    sink: Sink,
    txm: app_ports.Manager,
    clock: app_ports.Clock,
    fs: app_ports.Filesystem,
    logger: *log.Logger = &log.default,
    /// Root of `incomplete/<job id>/`, swept on remove. Empty disables
    /// the sweep, which is what a test that does not care about the
    /// filesystem wants.
    incomplete_dir: []const u8 = "",

    /// What a transition did to the aggregate.
    const Transition = enum { pause, resume_, mark_completed, resume_from_wait };

    /// downloading|queued → paused. The orchestrator observes `JobPaused`
    /// and stops dispatching.
    pub fn pauseJob(self: *Service, id: JobId) Error!void {
        return self.transition(id, .pause);
    }

    /// paused → queued|downloading.
    pub fn resumeJob(self: *Service, id: JobId) Error!void {
        return self.transition(id, .resume_);
    }

    /// Forces a job terminal-completed without touching the files.
    ///
    /// This is SAB's `mode=history&name=mark_as_completed`, which the
    /// *arr clients invoke after a human has manually re-imported a
    /// release hoardarr had marked failed. It flips database state and
    /// emits `JobCompleted` so history reflects reality; the bytes on
    /// disk are none of its business.
    pub fn markCompleted(self: *Service, id: JobId) Error!void {
        return self.transition(id, .mark_completed);
    }

    /// waiting_for_server → queued, for when a server appears.
    pub fn resumeFromWait(self: *Service, id: JobId) Error!void {
        return self.transition(id, .resume_from_wait);
    }

    fn transition(self: *Service, id: JobId, kind: Transition) Error!void {
        const Args = struct { svc: *Service, id: JobId, kind: Transition };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.store.byId(unit, args.id);
                defer s.store.release(job);
                const now = s.clock.now();
                const changed = switch (args.kind) {
                    .pause => try job.pause(now),
                    .resume_ => try job.resume_(now),
                    .mark_completed => try job.markCompleted(now),
                    .resume_from_wait => try job.resumeFromWait(now),
                };
                // No transition, no write, no event — which is what makes
                // a repeated click harmless.
                if (!changed) return;
                try s.store.save(unit, job);
                const events = try job.pullEvents();
                defer devents.deinitAll(s.gpa, events);
                try s.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .id = id, .kind = kind }, Body.run);
    }

    /// Parks a job because no usable server exists, recording why.
    pub fn parkWaitingForServer(self: *Service, id: JobId, reason: []const u8) Error!void {
        const Args = struct { svc: *Service, id: JobId, reason: []const u8 };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.store.byId(unit, args.id);
                defer s.store.release(job);
                if (!try job.markWaitingForServer(args.reason, s.clock.now())) return;
                try s.store.save(unit, job);
                const events = try job.pullEvents();
                defer devents.deinitAll(s.gpa, events);
                try s.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .id = id, .reason = reason }, Body.run);
    }

    /// Deletes the job and sweeps its temp directory.
    ///
    /// The row and the `JobRemoved` event commit together; the on-disk
    /// cleanup happens after. If the sweep fails the row is already gone,
    /// so the directory is an orphan a restart-time sweeper can prune —
    /// far better than failing the delete and leaving the operator with a
    /// job they cannot get rid of.
    pub fn removeJob(self: *Service, id: JobId) Error!void {
        const Args = struct { svc: *Service, id: JobId };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.store.byId(unit, args.id);
                var released = false;
                defer if (!released) s.store.release(job);

                try job.markRemoved(s.clock.now());
                const events = try job.pullEvents();
                defer devents.deinitAll(s.gpa, events);
                // Release before the delete: the store is about to
                // destroy the aggregate, and holding a borrow across
                // that is how a use-after-free gets written.
                s.store.release(job);
                released = true;
                try s.store.delete(unit, args.id);
                try s.sink.publish(unit, events);
            }
        };
        try dtx.inTx(Error, self.txm, Args{ .svc = self, .id = id }, Body.run);

        if (self.incomplete_dir.len == 0) return;
        var buf: [512]u8 = undefined;
        const dir = std.fmt.bufPrint(&buf, "{s}/{d}", .{ self.incomplete_dir, id }) catch return;
        self.fs.removeAll(dir) catch |e| {
            self.logger.warn("queue: failed to remove incomplete dir; orphan left for sweeper", &.{
                log.int("job_id", id),
                log.str("dir", dir),
                log.errv("err", e),
            });
        };
    }

    /// Rewrites `queue_order` so `ids` appear at the top of the queue in
    /// the given order.
    ///
    /// The new orders start at `min(existing) - len(ids)` rather than at
    /// zero, so "drag a job to the top" really puts it at the top without
    /// renumbering every other row. Terminal jobs in the input are
    /// skipped — they have no live queue position — but an unknown id is
    /// an error, because that is a stale client and worth seeing.
    pub fn reorder(self: *Service, ids: []const JobId) Error!void {
        if (ids.len == 0) return;
        const Args = struct { svc: *Service, ids: []const JobId };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const active = try s.store.active(s.gpa, unit);
                defer s.gpa.free(active);

                // Go seeded the minimum from a zero-valued variable, so
                // an all-positive queue always got base = -len. Same
                // here, deliberately: it keeps the reordered block below
                // every untouched row even when the queue was built from
                // millisecond timestamps.
                var min_order: i64 = 0;
                for (active) |row| min_order = @min(min_order, row.queue_order);
                const base = min_order - @as(i64, @intCast(args.ids.len));

                for (args.ids, 0..) |id, i| {
                    const job = try s.store.byId(unit, id);
                    defer s.store.release(job);
                    if (job.state.isTerminal()) continue;
                    job.setQueueOrder(base + @as(i64, @intCast(i)));
                    try s.store.save(unit, job);
                }
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .ids = ids }, Body.run);
    }

    /// Flips every failed and missing segment back to pending and revives
    /// a terminated job. The operator-facing meaning is "the release was
    /// temporarily unavailable; try again".
    ///
    /// Returns the number of segments reset. Zero means there was nothing
    /// to do, and nothing was written.
    pub fn retryFailedSegments(self: *Service, id: JobId) Error!usize {
        const Args = struct { svc: *Service, id: JobId, reset: *usize };
        var reset: usize = 0;
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.store.byId(unit, args.id);
                defer s.store.release(job);
                const n = job.resetFailedToPending();
                args.reset.* = n;
                if (n == 0) return;
                try s.store.save(unit, job);
                const events = try job.pullEvents();
                defer devents.deinitAll(s.gpa, events);
                try s.sink.publish(unit, events);
            }
        };
        try dtx.inTx(Error, self.txm, Args{ .svc = self, .id = id, .reset = &reset }, Body.run);
        if (reset > 0) {
            self.logger.info("queue: reset failed/missing segments", &.{
                log.int("job_id", id),
                log.uint("count", reset),
            });
        }
        return reset;
    }

    /// Pauses every job that is downloading or queued. Returns how many
    /// were paused; a per-job failure is logged and skipped so one bad
    /// row cannot abort a bulk action.
    pub fn pauseAll(self: *Service) Error!usize {
        return self.bulk(.pause);
    }

    /// The inverse: resumes everything paused.
    pub fn resumeAll(self: *Service) Error!usize {
        return self.bulk(.resume_);
    }

    fn bulk(self: *Service, kind: Transition) Error!usize {
        const active = try self.store.active(self.gpa, null);
        defer self.gpa.free(active);
        var n: usize = 0;
        for (active) |row| {
            const wanted = switch (kind) {
                .pause => row.state == .downloading or row.state == .queued,
                .resume_ => row.state == .paused,
                else => false,
            };
            if (!wanted) continue;
            self.transition(row.id, kind) catch |e| {
                self.logger.warn("queue: bulk transition skipped", &.{
                    log.int("job_id", row.id),
                    log.errv("err", e),
                });
                continue;
            };
            n += 1;
        }
        return n;
    }
};

// =====================================================================
// Tests — internal/app/download/reorder_test.go plus the operational
// paths Go left to the e2e suite.
// =====================================================================

const testing = std.testing;

const Harness = struct {
    store: ports.FakeJobStore = undefined,
    fs: app_ports.FakeFs = undefined,
    sink: app_ports.FakeSink(devents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 1_000 },
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.store = ports.FakeJobStore.init(testing.allocator);
        self.fs = app_ports.FakeFs.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .sink = self.sink.sink(),
            .txm = self.ftx.manager(),
            .clock = self.clock.clock(),
            .fs = self.fs.filesystem(),
            .logger = &self.logger,
            .incomplete_dir = "/inc",
        };
    }

    fn deinit(self: *Harness) void {
        self.fs.deinit();
        self.store.deinit();
    }

    /// A queued job with one segment, at the given queue position.
    fn seed(self: *Harness, name: []const u8, order: i64) !*Job {
        const j = try testing.allocator.create(Job);
        j.* = try Job.init(testing.allocator, .{
            .nzb_hash = name,
            .name = name,
            .category = "*",
            .queue_order = order,
            .nzb_blob = "<nzb/>",
            .files = &.{.{
                .filename = "f.bin",
                .size_bytes = 1,
                .segments = &.{.{ .seq_index = 1, .message_id = "s@h", .bytes = 1 }},
            }},
        }, 0);
        try self.store.insert(j);
        const events = try j.pullEvents();
        devents.deinitAll(testing.allocator, events);
        return j;
    }
};

test "reorder puts the requested ids at the top in the requested order" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    const j1 = try h.seed("alpha", 0);
    const j2 = try h.seed("bravo", 1);
    const j3 = try h.seed("charlie", 2);

    // Drag charlie to the top, then bravo, then alpha.
    try h.svc.reorder(&.{ j3.id, j2.id, j1.id });

    const active = try h.store.store().active(testing.allocator, null);
    defer testing.allocator.free(active);
    try testing.expectEqual(@as(usize, 3), active.len);
    try testing.expectEqual(j3.id, active[0].id);
    try testing.expectEqual(j2.id, active[1].id);
    try testing.expectEqual(j1.id, active[2].id);
    // The block landed strictly below the untouched minimum.
    try testing.expect(j3.queue_order < 0);
    try testing.expect(h.ftx.balanced());
}

test "reorder is a no-op for an empty list and never opens a transaction" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try h.svc.reorder(&.{});
    try testing.expectEqual(@as(u32, 0), h.ftx.begins);
}

test "reorder surfaces an unknown id and rolls the whole batch back" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j1 = try h.seed("alpha", 5);

    try testing.expectError(error.JobNotFound, h.svc.reorder(&.{ j1.id, 99_999 }));
    // The transaction rolled back, so a partial reorder is not visible.
    try testing.expectEqual(@as(u32, 1), h.ftx.rollbacks);
    try testing.expect(h.ftx.balanced());
    try testing.expect(h.store.leakFree());
}

test "reorder silently skips terminal jobs" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j1 = try h.seed("alpha", 0);
    const done = try h.seed("done", 1);
    _ = try done.markCompleted(1);
    const evts = try done.pullEvents();
    devents.deinitAll(testing.allocator, evts);
    const before = done.queue_order;

    try h.svc.reorder(&.{ done.id, j1.id });
    // A terminal job has no live queue position to set.
    try testing.expectEqual(before, done.queue_order);
    try testing.expect(j1.queue_order < 0);
}

test "pause and resume emit once and are idempotent afterwards" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);

    try h.svc.pauseJob(j.id);
    try testing.expectEqual(job_mod.JobState.paused, j.state);
    try testing.expectEqual(@as(usize, 1), h.sink.count("download.job.paused"));

    // A second click changes nothing: no row write, no second event, no
    // second stop-runner cycle in the orchestrator.
    const saves = h.store.saves;
    try h.svc.pauseJob(j.id);
    try testing.expectEqual(saves, h.store.saves);
    try testing.expectEqual(@as(usize, 1), h.sink.count("download.job.paused"));

    try h.svc.resumeJob(j.id);
    // Never started, so it goes back to queued rather than downloading.
    try testing.expectEqual(job_mod.JobState.queued, j.state);
    try testing.expectEqual(@as(usize, 1), h.sink.count("download.job.resumed"));
    try h.svc.resumeJob(j.id);
    try testing.expectEqual(@as(usize, 1), h.sink.count("download.job.resumed"));
    try testing.expect(h.store.leakFree());
}

test "pausing a terminal job is a no-op, not an error" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    _ = try j.markCompleted(1);
    const evts = try j.pullEvents();
    devents.deinitAll(testing.allocator, evts);

    try h.svc.pauseJob(j.id);
    try testing.expectEqual(job_mod.JobState.completed, j.state);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
}

test "markCompleted flips state and publishes without touching files" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    try h.fs.addFile("/inc/1/1.tmp", 10);

    try h.svc.markCompleted(j.id);
    try testing.expectEqual(job_mod.JobState.completed, j.state);
    try testing.expectEqual(@as(?Timestamp, h.clock.t), j.finished_at);
    try testing.expect(h.sink.has("download.job.completed"));
    // The bytes on disk are none of this operation's business.
    try testing.expect(h.fs.has("/inc/1/1.tmp"));
}

test "removeJob deletes the row, publishes, and sweeps the temp dir" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    try h.fs.addFile("/inc/1/1.tmp", 10);
    try h.fs.addFile("/inc/2/1.tmp", 10);

    try h.svc.removeJob(j.id);
    try testing.expectEqual(@as(usize, 0), h.store.len());
    try testing.expectEqual(@as(usize, 1), h.store.deletes);
    try testing.expect(h.sink.has("download.job.removed"));
    try testing.expect(!h.fs.has("/inc/1/1.tmp"));
    // Another job's directory is untouched.
    try testing.expect(h.fs.has("/inc/2/1.tmp"));
    try testing.expect(h.ftx.balanced());
}

test "a failed temp-dir sweep leaves an orphan rather than failing the delete" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    try h.fs.addFile("/inc/1/1.tmp", 10);
    h.fs.fail_next = error.Denied;

    // The row is gone and the event is out; the directory is a problem
    // for a sweeper, not for the operator trying to delete a job.
    try h.svc.removeJob(j.id);
    try testing.expectEqual(@as(usize, 0), h.store.len());
    try testing.expect(h.fs.has("/inc/1/1.tmp"));
}

test "removing an unknown job reports it and writes nothing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.JobNotFound, h.svc.removeJob(42));
    try testing.expectEqual(@as(usize, 0), h.store.deletes);
    try testing.expectEqual(@as(u32, 1), h.ftx.rollbacks);
}

test "retryFailedSegments revives a failed job and reports the count" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    try j.markSegmentMissing(j.files[0].segments[0].id, 10);
    const evts = try j.pullEvents();
    devents.deinitAll(testing.allocator, evts);
    try testing.expectEqual(job_mod.JobState.failed, j.state);

    const n = try h.svc.retryFailedSegments(j.id);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectEqual(ports.SegmentState.pending, j.files[0].segments[0].state);
    try testing.expectEqual(job_mod.JobState.queued, j.state);
    try testing.expectEqual(@as(?Timestamp, null), j.finished_at);
    try testing.expectEqualStrings("", j.errorMsg());
    try testing.expect(h.store.saves > 0);
}

test "retryFailedSegments on a healthy job writes nothing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    const saves = h.store.saves;
    try testing.expectEqual(@as(usize, 0), try h.svc.retryFailedSegments(j.id));
    try testing.expectEqual(saves, h.store.saves);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
}

test "pauseAll and resumeAll act on exactly the eligible states" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const queued = try h.seed("queued", 0);
    const downloading = try h.seed("downloading", 1);
    _ = try downloading.markStarted(1);
    const paused = try h.seed("paused", 2);
    _ = try paused.pause(1);
    const done = try h.seed("done", 3);
    _ = try done.markCompleted(1);
    for ([_]*Job{ downloading, paused, done }) |j| {
        const evts = try j.pullEvents();
        devents.deinitAll(testing.allocator, evts);
    }

    // queued + downloading pause; the already-paused and the terminal
    // one are left alone.
    try testing.expectEqual(@as(usize, 2), try h.svc.pauseAll());
    try testing.expectEqual(job_mod.JobState.paused, queued.state);
    try testing.expectEqual(job_mod.JobState.paused, downloading.state);
    try testing.expectEqual(job_mod.JobState.completed, done.state);

    // Now all three non-terminal jobs are paused, so all three resume.
    try testing.expectEqual(@as(usize, 3), try h.svc.resumeAll());
    try testing.expectEqual(job_mod.JobState.queued, queued.state);
    try testing.expectEqual(job_mod.JobState.downloading, downloading.state);
    try testing.expect(h.store.leakFree());
}

test "parkWaitingForServer records the reason and resumeFromWait undoes it" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);

    try h.svc.parkWaitingForServer(j.id, "no enabled usenet server");
    try testing.expectEqual(job_mod.JobState.waiting_for_server, j.state);
    try testing.expect(h.sink.has("download.job.waiting_for_server"));

    // A second park is a no-op: the state is already parked.
    const saves = h.store.saves;
    try h.svc.parkWaitingForServer(j.id, "still nothing");
    try testing.expectEqual(saves, h.store.saves);

    try h.svc.resumeFromWait(j.id);
    try testing.expectEqual(job_mod.JobState.queued, j.state);
    // Unparking reuses JobResumed so existing subscribers pick it up
    // without a new topic.
    try testing.expect(h.sink.has("download.job.resumed"));
}

test "a publish failure rolls the state change back" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed("alpha", 0);
    h.sink.fail = error.Backend;

    try testing.expectError(error.Backend, h.svc.pauseJob(j.id));
    try testing.expectEqual(@as(u32, 1), h.ftx.rollbacks);
    try testing.expect(h.ftx.balanced());
    try testing.expect(h.store.leakFree());
}
