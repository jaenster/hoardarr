//! The `repair` bounded context: rebuilding damaged files from PAR2
//! recovery slices via Reed-Solomon over GF(2^16).
//!
//! Aggregate root: `Repair`, one per download `Job`.
//!
//!     pending ──▶ repairing ──▶ ok
//!        │             │
//!        └─────────────┴──────▶ failed
//!
//! The aggregate is durable so a crash mid-reconstruction resumes
//! cleanly: a row still in `repairing` at startup says "retry", not
//! "deliver". The GF(2^16) arithmetic itself lives in `codec/par2`; the
//! domain only gates the transitions and records what happened.
//!
//! `failed` is reachable straight from `pending` because "not enough
//! recovery slices exist" is decided before any reconstruction starts.
//!
//! # Ownership
//!
//! The aggregate owns its `err` string, so it gets
//! `init(allocator, ...)` / `deinit`. `RepairFailed.err` is an **owned**
//! copy — the aggregate's own `err` is replaced by a later transition,
//! which would dangle an already-pulled event. Everything else in an
//! event is a scalar. Call `Event.deinit`, or `event.deinitAll` on a
//! batch.

const std = @import("std");
const event = @import("event.zig");
const download_state = @import("download/state.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identity of the upstream aggregate. Owned by the `download` context.
pub const JobId = download_state.JobId;

/// Identifies a `Repair`. Allocated by the repository; 0 means "not yet
/// persisted".
pub const RepairId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "repair.";

pub const State = enum {
    pending,
    repairing,
    /// Reconstruction succeeded and the post-repair MD5s matched.
    ok,
    /// Not enough recovery slices, or the post-repair MD5 still
    /// disagrees.
    failed,

    pub fn toString(self: State) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?State {
        return std.meta.stringToEnum(State, s);
    }

    pub fn isTerminal(self: State) bool {
        return switch (self) {
            .ok, .failed => true,
            else => false,
        };
    }
};

pub const RepairQueued = struct {
    id: RepairId,
    job_id: JobId,
    at: Timestamp,
};

pub const RepairStarted = struct {
    id: RepairId,
    job_id: JobId,
    at: Timestamp,
};

/// Reconstruction and the post-repair MD5 check both succeeded for every
/// previously-damaged file. The verify context re-emits `VerifyOK` from
/// here so extract and deliver take over.
pub const RepairOk = struct {
    id: RepairId,
    job_id: JobId,
    at: Timestamp,
};

pub const RepairFailed = struct {
    id: RepairId,
    job_id: JobId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

pub const Kind = enum {
    queued,
    started,
    ok,
    failed,
};

pub const Event = union(Kind) {
    queued: RepairQueued,
    started: RepairStarted,
    ok: RepairOk,
    failed: RepairFailed,

    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .queued => topic_prefix ++ "queued",
            .started => topic_prefix ++ "started",
            .ok => topic_prefix ++ "ok",
            .failed => topic_prefix ++ "failed",
        };
    }

    pub fn aggregateId(self: Event) RepairId {
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
    /// `start` from anything but `pending`.
    NotPending,
    /// `markOk` from anything but `repairing` — declaring success for a
    /// reconstruction that never ran would send a corrupt release
    /// downstream as if it were good.
    NotRepairing,
    /// `markFailed` from an already-terminal state.
    AlreadyTerminal,
};

pub const Error = TransitionError || Allocator.Error;

pub const HydrateParams = struct {
    id: RepairId,
    job_id: JobId,
    state: State,
    err: []const u8 = "",
    created_at: Timestamp,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,
};

pub const Repair = struct {
    allocator: Allocator,

    id: RepairId = 0,
    job_id: JobId,
    state: State = .pending,
    /// Empty unless `state == .failed`. Owned.
    err: []const u8 = "",

    created_at: Timestamp,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,

    events: event.Queue(Event) = .empty,

    pub const InitError = error{JobIdRequired} || Allocator.Error;

    /// Creates a pending repair and records `RepairQueued` with a
    /// placeholder id — `setId` patches it after the insert.
    pub fn init(allocator: Allocator, job_id: JobId, now: Timestamp) InitError!Repair {
        if (job_id == 0) return error.JobIdRequired;
        var r: Repair = .{
            .allocator = allocator,
            .job_id = job_id,
            .created_at = now,
        };
        try r.events.record(allocator, .{ .queued = .{ .id = 0, .job_id = job_id, .at = now } });
        return r;
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!Repair {
        return .{
            .allocator = allocator,
            .id = p.id,
            .job_id = p.job_id,
            .state = p.state,
            .err = try allocator.dupe(u8, p.err),
            .created_at = p.created_at,
            .started_at = p.started_at,
            .finished_at = p.finished_at,
        };
    }

    pub fn deinit(self: *Repair) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.err);
        self.* = undefined;
    }

    pub fn setId(self: *Repair, id: RepairId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                inline else => |*payload| if (payload.id == 0) {
                    payload.id = id;
                },
            }
        }
    }

    /// pending → repairing.
    pub fn start(self: *Repair, now: Timestamp) Error!void {
        if (self.state != .pending) return error.NotPending;
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);
        self.state = .repairing;
        self.started_at = now;
        self.events.items.appendAssumeCapacity(.{ .started = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
    }

    /// repairing → ok.
    pub fn markOk(self: *Repair, now: Timestamp) Error!void {
        if (self.state != .repairing) return error.NotRepairing;
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);
        self.state = .ok;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .ok = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
    }

    /// pending | repairing → failed. Reachable from `pending` because
    /// "not enough recovery slices" is known before reconstruction
    /// starts.
    pub fn markFailed(self: *Repair, reason: []const u8, now: Timestamp) Error!void {
        if (self.state.isTerminal()) return error.AlreadyTerminal;

        const mine = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(mine);
        const theirs = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(theirs);
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);

        self.allocator.free(self.err);
        self.err = mine;
        self.state = .failed;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .failed = .{
            .id = self.id,
            .job_id = self.job_id,
            .err = theirs,
            .at = now,
        } });
    }

    pub fn pullEvents(self: *Repair) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    pub fn pendingEvents(self: *const Repair) []const Event {
        return self.events.view();
    }
};

/// Errors a `Repair` repository raises that callers branch on.
pub const RepositoryError = error{
    RepairNotFound,
};

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no unit tests; these are written from the
// implementation and from how `app/repair/service.go` drives it.
// ---------------------------------------------------------------------

const t = std.testing;

test "state classification and persisted form" {
    try t.expect(!State.pending.isTerminal());
    try t.expect(!State.repairing.isTerminal());
    try t.expect(State.ok.isTerminal());
    try t.expect(State.failed.isTerminal());
    try t.expectEqualStrings("repairing", State.repairing.toString());
    try t.expectEqual(State.failed, State.parse("failed").?);
    try t.expectEqual(@as(?State, null), State.parse("borked"));
}

test "init queues a pending repair and requires a job id" {
    try t.expectError(error.JobIdRequired, Repair.init(t.allocator, 0, 1));

    var r = try Repair.init(t.allocator, 12, 100);
    defer r.deinit();
    try t.expectEqual(State.pending, r.state);
    try t.expectEqual(@as(Timestamp, 100), r.created_at);
    try t.expectEqual(@as(?Timestamp, null), r.started_at);

    r.setId(4);
    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("repair.queued", batch[0].topic());
    try t.expectEqual(@as(RepairId, 4), batch[0].aggregateId());
    try t.expectEqual(@as(JobId, 12), batch[0].jobId());
}

test "the happy path is queued, started, ok" {
    var r = try Repair.init(t.allocator, 12, 1);
    defer r.deinit();
    r.setId(1);

    try r.start(10);
    try t.expectEqual(State.repairing, r.state);
    try t.expectEqual(@as(?Timestamp, 10), r.started_at);

    try r.markOk(20);
    try t.expectEqual(State.ok, r.state);
    try t.expectEqual(@as(?Timestamp, 20), r.finished_at);
    try t.expectEqual(@as(usize, 0), r.err.len);

    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
    try t.expectEqualStrings("repair.queued", batch[0].topic());
    try t.expectEqualStrings("repair.started", batch[1].topic());
    try t.expectEqualStrings("repair.ok", batch[2].topic());
}

test "start only works from pending" {
    var r = try Repair.init(t.allocator, 1, 1);
    defer r.deinit();
    try r.start(2);
    try t.expectError(error.NotPending, r.start(3)); // repairing
    try r.markOk(4);
    try t.expectError(error.NotPending, r.start(5)); // ok
    try t.expectEqual(@as(?Timestamp, 2), r.started_at);

    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
}

test "markOk requires a reconstruction to have started" {
    var pending = try Repair.init(t.allocator, 1, 1);
    defer pending.deinit();
    try t.expectError(error.NotRepairing, pending.markOk(2));
    try t.expectEqual(State.pending, pending.state);
    t.allocator.free(try pending.pullEvents());

    var done = try Repair.init(t.allocator, 1, 1);
    defer done.deinit();
    try done.start(2);
    try done.markOk(3);
    try t.expectError(error.NotRepairing, done.markOk(4));
    t.allocator.free(try done.pullEvents());
}

test "markFailed works from pending — not enough slices to even try" {
    var r = try Repair.init(t.allocator, 1, 1);
    defer r.deinit();
    r.setId(2);

    try r.markFailed("need 4 recovery slices, have 1", 50);
    try t.expectEqual(State.failed, r.state);
    try t.expectEqualStrings("need 4 recovery slices, have 1", r.err);
    try t.expectEqual(@as(?Timestamp, 50), r.finished_at);
    try t.expectEqual(@as(?Timestamp, null), r.started_at);

    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
    try t.expectEqualStrings("repair.failed", batch[1].topic());
    try t.expectEqualStrings("need 4 recovery slices, have 1", batch[1].failed.err);
}

test "markFailed works from repairing" {
    var r = try Repair.init(t.allocator, 1, 1);
    defer r.deinit();
    try r.start(2);
    try r.markFailed("post-repair md5 still wrong", 3);
    try t.expectEqual(State.failed, r.state);

    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
}

test "markFailed is rejected once terminal" {
    var okrepair = try Repair.init(t.allocator, 1, 1);
    defer okrepair.deinit();
    try okrepair.start(2);
    try okrepair.markOk(3);
    try t.expectError(error.AlreadyTerminal, okrepair.markFailed("too late", 4));
    try t.expectEqual(State.ok, okrepair.state);
    t.allocator.free(try okrepair.pullEvents());

    var bad = try Repair.init(t.allocator, 1, 1);
    defer bad.deinit();
    try bad.markFailed("first cause", 2);
    try t.expectError(error.AlreadyTerminal, bad.markFailed("second cause", 3));
    try t.expectEqualStrings("first cause", bad.err);
    const batch = try bad.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
}

test "an error string survives the aggregate moving on" {
    var r = try Repair.init(t.allocator, 1, 1);
    defer r.deinit();
    try r.markFailed("gf16 solve failed", 2);

    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    // Hydrate over the top: the aggregate's copy is gone, the event's is not.
    r.allocator.free(r.err);
    r.err = try r.allocator.dupe(u8, "");
    try t.expectEqualStrings("gf16 solve failed", batch[1].failed.err);
}

test "hydrate restores a row without events" {
    var r = try Repair.hydrate(t.allocator, .{
        .id = 6,
        .job_id = 30,
        .state = .repairing,
        .created_at = 1,
        .started_at = 2,
    });
    defer r.deinit();
    try t.expectEqual(State.repairing, r.state);
    try t.expectEqual(@as(usize, 0), r.pendingEvents().len);
    // A row found mid-repair at startup must be resumable, not restartable.
    try t.expectError(error.NotPending, r.start(3));
    try r.markOk(4);
    const batch = try r.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
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
    var r = try Repair.init(t.allocator, 1, 1);
    try r.markFailed("nope", 2);
    r.deinit();
}
