//! The `deliver` bounded context: moving verified files out of
//! `incomplete/` into `complete/<category>/<release>/`.
//!
//! Aggregate root: `Delivery`, one per download `Job`.
//!
//!     pending ──▶ moving ──▶ complete
//!        │   │        │
//!        │   │        └──▶ failed ──┐
//!        │   └───────────▶ failed   │ (retry)
//!        │                          │
//!        │        ┌─────────────────┘
//!        │        ▼
//!        └──▶ skipped        moving
//!
//! Deliveries are durable rather than a stateless move because a crashed
//! move must be distinguishable from one that never started. A stateless
//! mover either leaves half-renamed files behind or delivers twice
//! across a restart.
//!
//! `failed → moving` is a deliberate edge: a previous attempt hit a
//! missing source file or a full disk, and the operator (or the startup
//! sweep) retries. The stale error is cleared on the way in, so the row
//! never shows a failure reason for an attempt that is currently
//! running.
//!
//! `skipped` is for a job that turns out to be an archive: the extract
//! context owns it from there, and this aggregate records that it
//! consciously stood down rather than silently doing nothing.
//!
//! # Ownership
//!
//! The aggregate owns `target_dir` and `err_msg`, so it gets
//! `init(allocator, ...)` / `deinit`.
//!
//! `DeliveryQueued.target_dir` and `DeliveryComplete.target_dir` are
//! **borrowed** from the aggregate — the target is fixed at construction
//! — and valid until `deinit`. `DeliveryFailed.err` is **owned**, since
//! a retry clears the aggregate's copy. Call `Event.deinit`, or
//! `event.deinitAll` on a batch.

const std = @import("std");
const event = @import("event.zig");
const download_state = @import("download/state.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identity of the upstream aggregate. Owned by the `download` context.
pub const JobId = download_state.JobId;

/// Identifies a `Delivery`. Allocated by the repository; 0 means "not yet
/// persisted".
pub const DeliveryId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "deliver.";

pub const State = enum {
    /// Queued; the mover hasn't started.
    pending,
    /// Files are being moved.
    moving,
    /// Every file landed in the target directory.
    complete,
    /// The mover errored; see `err_msg`. Retryable.
    failed,
    /// The job is an archive; the extract context owns it.
    skipped,

    pub fn toString(self: State) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?State {
        return std.meta.stringToEnum(State, s);
    }

    /// `failed` is *not* terminal here: `start` accepts it as a retry.
    /// That is the one place this context's state machine differs from
    /// its siblings, and the reason `start` carries its own guard rather
    /// than an `isTerminal` check.
    pub fn isTerminal(self: State) bool {
        return switch (self) {
            .complete, .skipped => true,
            else => false,
        };
    }
};

pub const DeliveryQueued = struct {
    id: DeliveryId,
    job_id: JobId,
    /// Borrowed from the aggregate.
    target_dir: []const u8,
    at: Timestamp,
};

pub const DeliveryStarted = struct {
    id: DeliveryId,
    job_id: JobId,
    at: Timestamp,
};

pub const DeliveryComplete = struct {
    id: DeliveryId,
    job_id: JobId,
    /// Borrowed from the aggregate.
    target_dir: []const u8,
    at: Timestamp,
};

pub const DeliveryFailed = struct {
    id: DeliveryId,
    job_id: JobId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

/// The job is an archive; the extract context takes over and emits its
/// own completion event.
pub const DeliverySkipped = struct {
    id: DeliveryId,
    job_id: JobId,
    at: Timestamp,
};

pub const Kind = enum {
    queued,
    started,
    complete,
    failed,
    skipped,
};

pub const Event = union(Kind) {
    queued: DeliveryQueued,
    started: DeliveryStarted,
    complete: DeliveryComplete,
    failed: DeliveryFailed,
    skipped: DeliverySkipped,

    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .queued => topic_prefix ++ "queued",
            .started => topic_prefix ++ "started",
            .complete => topic_prefix ++ "complete",
            .failed => topic_prefix ++ "failed",
            .skipped => topic_prefix ++ "skipped",
        };
    }

    pub fn aggregateId(self: Event) DeliveryId {
        return switch (self) {
            inline else => |e| e.id,
        };
    }

    pub fn jobId(self: Event) JobId {
        return switch (self) {
            inline else => |e| e.job_id,
        };
    }

    pub fn occurredAt(self: Event) Timestamp {
        return switch (self) {
            inline else => |e| e.at,
        };
    }

    pub fn deinit(self: Event, allocator: Allocator) void {
        switch (self) {
            .failed => |e| allocator.free(e.err),
            else => {},
        }
    }
};

pub const TransitionError = error{
    /// `start` from anything but `pending` or `failed`.
    NotStartable,
    /// `complete` from anything but `moving`.
    NotMoving,
    /// `fail` from anything but `pending` or `moving`.
    NotFailable,
    /// `skip` from anything but `pending` — standing down after files
    /// have already begun moving would leave them half-delivered with
    /// nobody owning the cleanup.
    NotPending,
};

pub const Error = TransitionError || Allocator.Error;

pub const HydrateParams = struct {
    id: DeliveryId,
    job_id: JobId,
    state: State,
    target_dir: []const u8 = "",
    err_msg: []const u8 = "",
    created_at: Timestamp,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,
};

pub const Delivery = struct {
    allocator: Allocator,

    id: DeliveryId = 0,
    job_id: JobId,
    state: State = .pending,
    /// Resolved at construction; owned.
    target_dir: []const u8,
    /// Empty unless `state == .failed`. Owned.
    err_msg: []const u8 = "",

    created_at: Timestamp,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,

    events: event.Queue(Event) = .empty,

    pub const InitError = error{JobIdRequired} || Allocator.Error;

    /// Creates a pending delivery and records `DeliveryQueued` with a
    /// placeholder id — `setId` patches it after the insert.
    pub fn init(
        allocator: Allocator,
        job_id: JobId,
        target_dir: []const u8,
        now: Timestamp,
    ) InitError!Delivery {
        if (job_id == 0) return error.JobIdRequired;

        const dir = try allocator.dupe(u8, target_dir);
        errdefer allocator.free(dir);
        const empty_err = try allocator.dupe(u8, "");
        errdefer allocator.free(empty_err);

        var d: Delivery = .{
            .allocator = allocator,
            .job_id = job_id,
            .target_dir = dir,
            .err_msg = empty_err,
            .created_at = now,
        };
        try d.events.record(allocator, .{ .queued = .{
            .id = 0,
            .job_id = job_id,
            .target_dir = d.target_dir,
            .at = now,
        } });
        return d;
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!Delivery {
        const dir = try allocator.dupe(u8, p.target_dir);
        errdefer allocator.free(dir);
        const msg = try allocator.dupe(u8, p.err_msg);
        return .{
            .allocator = allocator,
            .id = p.id,
            .job_id = p.job_id,
            .state = p.state,
            .target_dir = dir,
            .err_msg = msg,
            .created_at = p.created_at,
            .started_at = p.started_at,
            .finished_at = p.finished_at,
        };
    }

    pub fn deinit(self: *Delivery) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.target_dir);
        self.allocator.free(self.err_msg);
        self.* = undefined;
    }

    pub fn setId(self: *Delivery, id: DeliveryId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                inline else => |*payload| if (payload.id == 0) {
                    payload.id = id;
                },
            }
        }
    }

    /// pending | failed → moving. The `failed` edge is a retry; the
    /// stale error is cleared so the row can't show a reason for an
    /// attempt that is currently running.
    pub fn start(self: *Delivery, now: Timestamp) Error!void {
        if (self.state != .pending and self.state != .failed) return error.NotStartable;

        const cleared = try self.allocator.dupe(u8, "");
        errdefer self.allocator.free(cleared);
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);

        self.allocator.free(self.err_msg);
        self.err_msg = cleared;
        self.state = .moving;
        self.started_at = now;
        self.finished_at = null;
        self.events.items.appendAssumeCapacity(.{ .started = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
    }

    /// moving → complete.
    pub fn complete(self: *Delivery, now: Timestamp) Error!void {
        if (self.state != .moving) return error.NotMoving;
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);
        self.state = .complete;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .complete = .{
            .id = self.id,
            .job_id = self.job_id,
            .target_dir = self.target_dir,
            .at = now,
        } });
    }

    /// pending | moving → failed. Reachable from `pending` because a
    /// missing source directory is noticed before the first rename.
    pub fn fail(self: *Delivery, reason: []const u8, now: Timestamp) Error!void {
        if (self.state != .moving and self.state != .pending) return error.NotFailable;

        const mine = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(mine);
        const theirs = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(theirs);
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);

        self.allocator.free(self.err_msg);
        self.err_msg = mine;
        self.state = .failed;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .failed = .{
            .id = self.id,
            .job_id = self.job_id,
            .err = theirs,
            .at = now,
        } });
    }

    /// pending → skipped, when the job needs extraction rather than a
    /// direct move.
    pub fn skip(self: *Delivery, now: Timestamp) Error!void {
        if (self.state != .pending) return error.NotPending;
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);
        self.state = .skipped;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .skipped = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
    }

    pub fn pullEvents(self: *Delivery) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    pub fn pendingEvents(self: *const Delivery) []const Event {
        return self.events.view();
    }
};

/// Errors a `Delivery` repository raises that callers branch on.
pub const RepositoryError = error{
    DeliveryNotFound,
};

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no unit tests; these are written from the
// implementation and from how `app/deliver/service.go` drives it.
// ---------------------------------------------------------------------

const t = std.testing;

test "state classification and persisted form" {
    try t.expect(!State.pending.isTerminal());
    try t.expect(!State.moving.isTerminal());
    // failed is retryable, so deliberately not terminal.
    try t.expect(!State.failed.isTerminal());
    try t.expect(State.complete.isTerminal());
    try t.expect(State.skipped.isTerminal());

    try t.expectEqualStrings("skipped", State.skipped.toString());
    try t.expectEqual(State.moving, State.parse("moving").?);
    try t.expectEqual(@as(?State, null), State.parse("delivering"));
}

test "init queues a pending delivery carrying the target dir" {
    try t.expectError(error.JobIdRequired, Delivery.init(t.allocator, 0, "/x", 1));

    var d = try Delivery.init(t.allocator, 5, "/complete/tv/Show", 100);
    defer d.deinit();
    try t.expectEqual(State.pending, d.state);
    try t.expectEqualStrings("/complete/tv/Show", d.target_dir);
    try t.expectEqual(@as(Timestamp, 100), d.created_at);

    d.setId(9);
    const batch = try d.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("deliver.queued", batch[0].topic());
    try t.expectEqual(@as(DeliveryId, 9), batch[0].aggregateId());
    try t.expectEqual(@as(JobId, 5), batch[0].jobId());
    try t.expectEqualStrings("/complete/tv/Show", batch[0].queued.target_dir);
}

test "the happy path is queued, started, complete" {
    var d = try Delivery.init(t.allocator, 5, "/out", 1);
    defer d.deinit();
    d.setId(1);

    try d.start(10);
    try t.expectEqual(State.moving, d.state);
    try t.expectEqual(@as(?Timestamp, 10), d.started_at);

    try d.complete(20);
    try t.expectEqual(State.complete, d.state);
    try t.expectEqual(@as(?Timestamp, 20), d.finished_at);

    const batch = try d.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
    try t.expectEqualStrings("deliver.started", batch[1].topic());
    try t.expectEqualStrings("deliver.complete", batch[2].topic());
    // History is written from the completion event's directory.
    try t.expectEqualStrings("/out", batch[2].complete.target_dir);
}

test "a failed delivery can be retried and the stale error is cleared" {
    var d = try Delivery.init(t.allocator, 5, "/out", 1);
    defer d.deinit();
    d.setId(2);

    try d.start(10);
    try d.fail("rename: cross-device link", 11);
    try t.expectEqual(State.failed, d.state);
    try t.expectEqualStrings("rename: cross-device link", d.err_msg);
    try t.expectEqual(@as(?Timestamp, 11), d.finished_at);

    try d.start(20);
    try t.expectEqual(State.moving, d.state);
    try t.expectEqual(@as(usize, 0), d.err_msg.len);
    try t.expectEqual(@as(?Timestamp, 20), d.started_at);
    // A running attempt must not carry a finish time from the failed one.
    try t.expectEqual(@as(?Timestamp, null), d.finished_at);

    try d.complete(30);
    try t.expectEqual(State.complete, d.state);

    const batch = try d.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 5), batch.len);
    try t.expectEqualStrings("deliver.queued", batch[0].topic());
    try t.expectEqualStrings("deliver.started", batch[1].topic());
    try t.expectEqualStrings("deliver.failed", batch[2].topic());
    try t.expectEqualStrings("deliver.started", batch[3].topic());
    try t.expectEqualStrings("deliver.complete", batch[4].topic());
    // The failure event kept its own copy of a reason the retry cleared.
    try t.expectEqualStrings("rename: cross-device link", batch[2].failed.err);
}

test "start is rejected from moving, complete and skipped" {
    var moving = try Delivery.init(t.allocator, 1, "/out", 1);
    defer moving.deinit();
    try moving.start(2);
    try t.expectError(error.NotStartable, moving.start(3));
    try t.expectEqual(@as(?Timestamp, 2), moving.started_at);
    t.allocator.free(try moving.pullEvents());

    var done = try Delivery.init(t.allocator, 1, "/out", 1);
    defer done.deinit();
    try done.start(2);
    try done.complete(3);
    try t.expectError(error.NotStartable, done.start(4));
    t.allocator.free(try done.pullEvents());

    var skipped = try Delivery.init(t.allocator, 1, "/out", 1);
    defer skipped.deinit();
    try skipped.skip(2);
    try t.expectError(error.NotStartable, skipped.start(3));
    t.allocator.free(try skipped.pullEvents());
}

test "complete requires a move to be running" {
    var pending = try Delivery.init(t.allocator, 1, "/out", 1);
    defer pending.deinit();
    try t.expectError(error.NotMoving, pending.complete(2));
    try t.expectEqual(State.pending, pending.state);
    t.allocator.free(try pending.pullEvents());

    var skipped = try Delivery.init(t.allocator, 1, "/out", 1);
    defer skipped.deinit();
    try skipped.skip(2);
    try t.expectError(error.NotMoving, skipped.complete(3));
    t.allocator.free(try skipped.pullEvents());
}

test "fail works from pending as well as moving" {
    var early = try Delivery.init(t.allocator, 1, "/out", 1);
    defer early.deinit();
    early.setId(3);
    try early.fail("source directory vanished", 30);
    try t.expectEqual(State.failed, early.state);
    try t.expectEqualStrings("source directory vanished", early.err_msg);
    try t.expectEqual(@as(?Timestamp, null), early.started_at);

    const batch = try early.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
    try t.expectEqualStrings("deliver.failed", batch[1].topic());
}

test "fail is rejected from complete and skipped" {
    var done = try Delivery.init(t.allocator, 1, "/out", 1);
    defer done.deinit();
    try done.start(2);
    try done.complete(3);
    try t.expectError(error.NotFailable, done.fail("too late", 4));
    try t.expectEqual(State.complete, done.state);
    t.allocator.free(try done.pullEvents());

    var skipped = try Delivery.init(t.allocator, 1, "/out", 1);
    defer skipped.deinit();
    try skipped.skip(2);
    try t.expectError(error.NotFailable, skipped.fail("nope", 3));
    t.allocator.free(try skipped.pullEvents());
}

test "skip stands down from pending only" {
    var d = try Delivery.init(t.allocator, 1, "/out", 1);
    defer d.deinit();
    d.setId(4);

    try d.skip(40);
    try t.expectEqual(State.skipped, d.state);
    try t.expectEqual(@as(?Timestamp, 40), d.finished_at);
    try t.expectError(error.NotPending, d.skip(41));

    const batch = try d.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
    try t.expectEqualStrings("deliver.skipped", batch[1].topic());
    try t.expectEqual(@as(Timestamp, 40), batch[1].occurredAt());
}

test "skip is rejected once a move has started or failed" {
    var moving = try Delivery.init(t.allocator, 1, "/out", 1);
    defer moving.deinit();
    try moving.start(2);
    try t.expectError(error.NotPending, moving.skip(3));
    t.allocator.free(try moving.pullEvents());

    var failed = try Delivery.init(t.allocator, 1, "/out", 1);
    defer failed.deinit();
    try failed.fail("disk full", 2);
    try t.expectError(error.NotPending, failed.skip(3));
    const batch = try failed.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
}

test "hydrate restores a row without events and is resumable" {
    var d = try Delivery.hydrate(t.allocator, .{
        .id = 6,
        .job_id = 30,
        .state = .failed,
        .target_dir = "/complete/movies/Film",
        .err_msg = "previous attempt died",
        .created_at = 1,
        .started_at = 2,
        .finished_at = 3,
    });
    defer d.deinit();
    try t.expectEqual(State.failed, d.state);
    try t.expectEqualStrings("previous attempt died", d.err_msg);
    try t.expectEqual(@as(usize, 0), d.pendingEvents().len);

    // The startup sweep retries it.
    try d.start(10);
    try t.expectEqual(State.moving, d.state);
    try t.expectEqual(@as(usize, 0), d.err_msg.len);

    const batch = try d.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqual(@as(DeliveryId, 6), batch[0].aggregateId());
}

test "every event tag has a distinct prefixed topic" {
    var seen: [std.meta.fields(Kind).len][]const u8 = undefined;
    inline for (std.meta.fields(Kind), 0..) |f, i| {
        const e: Event = @unionInit(Event, f.name, std.mem.zeroes(@FieldType(Event, f.name)));
        const tp = e.topic();
        try t.expect(std.mem.startsWith(u8, tp, topic_prefix));
        for (seen[0..i]) |prev| try t.expect(!std.mem.eql(u8, prev, tp));
        seen[i] = tp;
    }
}

test "undrained events are freed with the aggregate" {
    var d = try Delivery.init(t.allocator, 1, "/out", 1);
    try d.fail("nope", 2);
    d.deinit();
}
