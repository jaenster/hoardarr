//! The `extract` bounded context: unpacking archive jobs. RAR3 and RAR5
//! today; ZIP or 7z would be another `Extractor` implementation, not a
//! change here.
//!
//! Aggregate root: `Extract`, one per download `Job` (UNIQUE(job_id) at
//! the database level).
//!
//!     pending ──▶ extracting ──▶ complete
//!        │             │
//!        └─────────────┴──────▶ failed
//!
//! The aggregate is durable so a crash mid-extract resumes cleanly: a
//! row still in `extracting` at startup means retry, rather than
//! delivering a half-unpacked directory.
//!
//! # Ownership
//!
//! The aggregate owns `target_dir` and `err_msg`, so it gets
//! `init(allocator, ...)` / `deinit`.
//!
//! `ExtractQueued.target_dir` and `ExtractComplete.target_dir` are
//! **borrowed** from the aggregate: the target directory is fixed at
//! construction and never rewritten, so there is nothing to copy and
//! the common path allocates nothing. They are valid until `deinit`, so
//! a pulled batch must be consumed before then.
//!
//! `ExtractFailed.err` is **owned** — the aggregate's `err_msg` is
//! replaced by a later transition, which would dangle an already-pulled
//! event. Call `Event.deinit`, or `event.deinitAll` on a batch.

const std = @import("std");
const event = @import("event.zig");
const download_state = @import("download/state.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identity of the upstream aggregate. Owned by the `download` context.
pub const JobId = download_state.JobId;

/// Identifies an `Extract`. Allocated by the repository; 0 means "not yet
/// persisted".
pub const ExtractId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "extract.";

pub const State = enum {
    pending,
    extracting,
    complete,
    failed,

    pub fn toString(self: State) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?State {
        return std.meta.stringToEnum(State, s);
    }

    pub fn isTerminal(self: State) bool {
        return switch (self) {
            .complete, .failed => true,
            else => false,
        };
    }
};

pub const ExtractQueued = struct {
    id: ExtractId,
    job_id: JobId,
    /// Borrowed from the aggregate.
    target_dir: []const u8,
    at: Timestamp,
};

pub const ExtractStarted = struct {
    id: ExtractId,
    job_id: JobId,
    at: Timestamp,
};

pub const ExtractComplete = struct {
    id: ExtractId,
    job_id: JobId,
    /// Borrowed from the aggregate.
    target_dir: []const u8,
    at: Timestamp,
};

pub const ExtractFailed = struct {
    id: ExtractId,
    job_id: JobId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

pub const Kind = enum {
    queued,
    started,
    complete,
    failed,
};

pub const Event = union(Kind) {
    queued: ExtractQueued,
    started: ExtractStarted,
    complete: ExtractComplete,
    failed: ExtractFailed,

    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .queued => topic_prefix ++ "queued",
            .started => topic_prefix ++ "started",
            .complete => topic_prefix ++ "complete",
            .failed => topic_prefix ++ "failed",
        };
    }

    pub fn aggregateId(self: Event) ExtractId {
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
    /// `complete` from anything but `extracting` — claiming a directory
    /// is unpacked when nothing ran would hand deliver an empty release.
    NotExtracting,
    /// `fail` from an already-terminal state.
    AlreadyTerminal,
};

pub const Error = TransitionError || Allocator.Error;

pub const HydrateParams = struct {
    id: ExtractId,
    job_id: JobId,
    state: State,
    target_dir: []const u8 = "",
    err_msg: []const u8 = "",
    created_at: Timestamp,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,
};

pub const Extract = struct {
    allocator: Allocator,

    id: ExtractId = 0,
    job_id: JobId,
    state: State = .pending,
    /// Where the entries land. Fixed at construction; owned.
    target_dir: []const u8,
    /// Empty unless `state == .failed`. Owned.
    err_msg: []const u8 = "",

    created_at: Timestamp,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,

    events: event.Queue(Event) = .empty,

    pub const InitError = error{JobIdRequired} || Allocator.Error;

    /// Creates a pending extract and records `ExtractQueued` with a
    /// placeholder id — `setId` patches it after the insert.
    pub fn init(
        allocator: Allocator,
        job_id: JobId,
        target_dir: []const u8,
        now: Timestamp,
    ) InitError!Extract {
        if (job_id == 0) return error.JobIdRequired;

        const dir = try allocator.dupe(u8, target_dir);
        errdefer allocator.free(dir);
        const empty_err = try allocator.dupe(u8, "");
        errdefer allocator.free(empty_err);

        var x: Extract = .{
            .allocator = allocator,
            .job_id = job_id,
            .target_dir = dir,
            .err_msg = empty_err,
            .created_at = now,
        };
        try x.events.record(allocator, .{ .queued = .{
            .id = 0,
            .job_id = job_id,
            .target_dir = x.target_dir,
            .at = now,
        } });
        return x;
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!Extract {
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

    pub fn deinit(self: *Extract) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.target_dir);
        self.allocator.free(self.err_msg);
        self.* = undefined;
    }

    pub fn setId(self: *Extract, id: ExtractId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                inline else => |*payload| if (payload.id == 0) {
                    payload.id = id;
                },
            }
        }
    }

    /// pending → extracting.
    pub fn start(self: *Extract, now: Timestamp) Error!void {
        if (self.state != .pending) return error.NotPending;
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);
        self.state = .extracting;
        self.started_at = now;
        self.events.items.appendAssumeCapacity(.{ .started = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
    }

    /// extracting → complete.
    pub fn complete(self: *Extract, now: Timestamp) Error!void {
        if (self.state != .extracting) return error.NotExtracting;
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

    /// pending | extracting → failed. Reachable from `pending` because
    /// "no archive volumes in this job" is known before unrar starts.
    pub fn fail(self: *Extract, reason: []const u8, now: Timestamp) Error!void {
        if (self.state.isTerminal()) return error.AlreadyTerminal;

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

    pub fn pullEvents(self: *Extract) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    pub fn pendingEvents(self: *const Extract) []const Event {
        return self.events.view();
    }
};

/// Errors an `Extract` repository raises that callers branch on.
pub const RepositoryError = error{
    ExtractNotFound,
    /// UNIQUE(job_id) rejected a second insert for the same Job.
    DuplicateJob,
};

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no unit tests; these are written from the
// implementation and from how `app/extract/service.go` drives it.
// ---------------------------------------------------------------------

const t = std.testing;

test "state classification and persisted form" {
    try t.expect(!State.pending.isTerminal());
    try t.expect(!State.extracting.isTerminal());
    try t.expect(State.complete.isTerminal());
    try t.expect(State.failed.isTerminal());
    try t.expectEqualStrings("extracting", State.extracting.toString());
    try t.expectEqual(State.complete, State.parse("complete").?);
    try t.expectEqual(@as(?State, null), State.parse("unpacking"));
}

test "init queues a pending extract carrying the target dir" {
    try t.expectError(error.JobIdRequired, Extract.init(t.allocator, 0, "/x", 1));

    var x = try Extract.init(t.allocator, 5, "/complete/tv/Show.S01E01", 100);
    defer x.deinit();
    try t.expectEqual(State.pending, x.state);
    try t.expectEqualStrings("/complete/tv/Show.S01E01", x.target_dir);
    try t.expectEqual(@as(Timestamp, 100), x.created_at);

    x.setId(9);
    const batch = try x.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("extract.queued", batch[0].topic());
    try t.expectEqual(@as(ExtractId, 9), batch[0].aggregateId());
    try t.expectEqual(@as(JobId, 5), batch[0].jobId());
    try t.expectEqualStrings("/complete/tv/Show.S01E01", batch[0].queued.target_dir);
}

test "the happy path is queued, started, complete" {
    var x = try Extract.init(t.allocator, 5, "/out", 1);
    defer x.deinit();
    x.setId(1);

    try x.start(10);
    try t.expectEqual(State.extracting, x.state);
    try t.expectEqual(@as(?Timestamp, 10), x.started_at);

    try x.complete(20);
    try t.expectEqual(State.complete, x.state);
    try t.expectEqual(@as(?Timestamp, 20), x.finished_at);

    const batch = try x.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
    try t.expectEqualStrings("extract.started", batch[1].topic());
    try t.expectEqualStrings("extract.complete", batch[2].topic());
    // Deliver needs the directory off the completion event.
    try t.expectEqualStrings("/out", batch[2].complete.target_dir);
}

test "start only works from pending" {
    var x = try Extract.init(t.allocator, 1, "/out", 1);
    defer x.deinit();
    try x.start(2);
    try t.expectError(error.NotPending, x.start(3));
    try x.complete(4);
    try t.expectError(error.NotPending, x.start(5));
    try t.expectEqual(@as(?Timestamp, 2), x.started_at);

    const batch = try x.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
}

test "complete requires an extract to be running" {
    var pending = try Extract.init(t.allocator, 1, "/out", 1);
    defer pending.deinit();
    try t.expectError(error.NotExtracting, pending.complete(2));
    try t.expectEqual(State.pending, pending.state);
    t.allocator.free(try pending.pullEvents());

    var failed = try Extract.init(t.allocator, 1, "/out", 1);
    defer failed.deinit();
    try failed.fail("bad crc in volume 3", 2);
    try t.expectError(error.NotExtracting, failed.complete(3));
    const batch = try failed.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
}

test "fail works from pending and from extracting" {
    var early = try Extract.init(t.allocator, 1, "/out", 1);
    defer early.deinit();
    early.setId(2);
    try early.fail("no rar volumes in job", 30);
    try t.expectEqual(State.failed, early.state);
    try t.expectEqualStrings("no rar volumes in job", early.err_msg);
    try t.expectEqual(@as(?Timestamp, 30), early.finished_at);
    try t.expectEqual(@as(?Timestamp, null), early.started_at);
    {
        const batch = try early.pullEvents();
        defer event.deinitAll(Event, t.allocator, batch);
        try t.expectEqual(@as(usize, 2), batch.len);
        try t.expectEqualStrings("extract.failed", batch[1].topic());
        try t.expectEqualStrings("no rar volumes in job", batch[1].failed.err);
    }

    var late = try Extract.init(t.allocator, 1, "/out", 1);
    defer late.deinit();
    try late.start(2);
    try late.fail("entry escapes target dir", 3);
    try t.expectEqual(State.failed, late.state);
    const batch = try late.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len);
}

test "fail is rejected once terminal and keeps the first cause" {
    var done = try Extract.init(t.allocator, 1, "/out", 1);
    defer done.deinit();
    try done.start(2);
    try done.complete(3);
    try t.expectError(error.AlreadyTerminal, done.fail("too late", 4));
    try t.expectEqual(State.complete, done.state);
    t.allocator.free(try done.pullEvents());

    var bad = try Extract.init(t.allocator, 1, "/out", 1);
    defer bad.deinit();
    try bad.fail("first cause", 2);
    try t.expectError(error.AlreadyTerminal, bad.fail("second cause", 3));
    try t.expectEqualStrings("first cause", bad.err_msg);
    const batch = try bad.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
}

test "hydrate restores a row without events and is resumable" {
    var x = try Extract.hydrate(t.allocator, .{
        .id = 6,
        .job_id = 30,
        .state = .extracting,
        .target_dir = "/complete/movies/Film",
        .created_at = 1,
        .started_at = 2,
    });
    defer x.deinit();
    try t.expectEqual(State.extracting, x.state);
    try t.expectEqualStrings("/complete/movies/Film", x.target_dir);
    try t.expectEqual(@as(usize, 0), x.pendingEvents().len);

    // Found mid-extract at startup: retry means finish it, not restart it.
    try t.expectError(error.NotPending, x.start(3));
    try x.complete(4);
    const batch = try x.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqualStrings("/complete/movies/Film", batch[0].complete.target_dir);
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
    var x = try Extract.init(t.allocator, 1, "/out", 1);
    try x.fail("nope", 2);
    x.deinit();
}
