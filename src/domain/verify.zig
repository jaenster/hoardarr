//! The `verify` bounded context: post-download integrity checking via
//! PAR2 (Parchive Volume 2).
//!
//! Aggregate root: `VerifySet` — one verification run for one download
//! `Job`.
//!
//!     pending ──▶ verifying ──▶ ok
//!                      │
//!                      ├──▶ repair_needed ──┐ (the repair context
//!                      │                    │  reconstructs, then)
//!                      └──▶ failed          └─▶ reset ──▶ pending
//!
//! `ok` means every file's MD5 matched. `repair_needed` means the
//! download itself is intact but at least one file's checksum
//! disagrees — recoverable. `failed` means we could not read the PAR2
//! metadata at all, which no amount of recovery slices fixes.
//!
//! The aggregate keeps only the coarse summary: the state, the failed
//! filenames, and an error string. The parsed PAR2 packets stay in the
//! codec layer and never cross into the domain — a `VerifySet` is small
//! enough to load and save on every transition.
//!
//! # Ownership
//!
//! The aggregate owns its `error_msg` and its `failed_files` strings,
//! so it gets `init(allocator, ...)` / `deinit`.
//!
//! `RepairNeeded` carries an **owned** copy of the failed-file list, and
//! `VerifyFailed` an **owned** copy of the reason. They cannot borrow:
//! `reset` frees both on the aggregate, which would leave an
//! already-pulled event pointing at released memory. Everything else in
//! an event is a scalar. Whoever holds an event must call
//! `Event.deinit`, or `event.deinitAll` on a whole batch.

const std = @import("std");
const event = @import("event.zig");
const download_state = @import("download/state.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identity of the upstream aggregate this context reacts to. One
/// definition, owned by the `download` context.
pub const JobId = download_state.JobId;

/// Identifies a `VerifySet`. Allocated by the repository; 0 means "not
/// yet persisted".
pub const VerifySetId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "verify.";

/// The lifecycle of one verification attempt.
pub const VerifyState = enum {
    pending,
    verifying,
    ok,
    repair_needed,
    failed,

    pub fn toString(self: VerifyState) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?VerifyState {
        return std.meta.stringToEnum(VerifyState, s);
    }

    /// True once this pass will not progress further on its own.
    /// `repair_needed` counts: the *verify* pass is over, and only an
    /// explicit `reset` after a repair reopens it.
    pub fn isTerminal(self: VerifyState) bool {
        return switch (self) {
            .ok, .repair_needed, .failed => true,
            else => false,
        };
    }
};

/// A worker moved the set from pending to verifying. UI subscribers
/// light up "Verifying".
pub const VerifyStarted = struct {
    id: VerifySetId,
    job_id: JobId,
    at: Timestamp,
};

/// Every file's checksum matched. The extract context picks this up.
pub const VerifyOk = struct {
    id: VerifySetId,
    job_id: JobId,
    at: Timestamp,
};

/// At least one checksum disagreed. The repair context starts the
/// GF(2^16) reconstruction from here.
pub const RepairNeeded = struct {
    id: VerifySetId,
    job_id: JobId,
    /// Owned by the event.
    failed_files: []const []const u8,
    at: Timestamp,
};

/// Verification could not run — the PAR2 files themselves are missing or
/// unparseable. Terminal.
pub const VerifyFailed = struct {
    id: VerifySetId,
    job_id: JobId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

pub const Kind = enum {
    started,
    ok,
    repair_needed,
    failed,
};

pub const Event = union(Kind) {
    started: VerifyStarted,
    ok: VerifyOk,
    repair_needed: RepairNeeded,
    failed: VerifyFailed,

    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .started => topic_prefix ++ "started",
            .ok => topic_prefix ++ "ok",
            .repair_needed => topic_prefix ++ "repair_needed",
            .failed => topic_prefix ++ "failed",
        };
    }

    pub fn aggregateId(self: Event) VerifySetId {
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

    /// Frees the strings this event owns. Scalar-only events are a
    /// no-op, so this is always safe to call.
    pub fn deinit(self: Event, allocator: Allocator) void {
        switch (self) {
            .repair_needed => |e| freeStrings(allocator, e.failed_files),
            .failed => |e| allocator.free(e.err),
            else => {},
        }
    }
};

pub const TransitionError = error{
    /// The pass has already reached `ok`, `repair_needed` or `failed`.
    /// Re-deciding a finished verification would publish a second,
    /// contradictory outcome to every downstream context.
    AlreadyTerminal,
    /// `reset` was called from a state other than `repair_needed`.
    NotRepairNeeded,
};

pub const Error = TransitionError || Allocator.Error;

pub const HydrateParams = struct {
    id: VerifySetId,
    job_id: JobId,
    state: VerifyState,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,
    error_msg: []const u8 = "",
    failed_files: []const []const u8 = &.{},
};

/// One verification run for one Job.
pub const VerifySet = struct {
    allocator: Allocator,

    id: VerifySetId = 0,
    job_id: JobId,
    state: VerifyState = .pending,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,
    /// Empty unless `state == .failed`. Owned.
    error_msg: []const u8 = "",
    /// Filenames whose checksum disagreed. Owned; empty unless
    /// `state == .repair_needed`.
    failed_files: []const []const u8 = &.{},

    events: event.Queue(Event) = .empty,

    pub const InitError = error{JobIdRequired} || Allocator.Error;

    /// Creates a pending set. No event: `VerifyStarted` fires when a
    /// worker actually picks the job up, which may be much later.
    ///
    /// Go panicked on a zero JobID. A panic in a domain constructor
    /// takes the process down over a caller's bug in a request handler;
    /// an error lets the handler answer 400.
    pub fn init(allocator: Allocator, job_id: JobId, _: Timestamp) InitError!VerifySet {
        if (job_id == 0) return error.JobIdRequired;
        return .{ .allocator = allocator, .job_id = job_id };
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!VerifySet {
        const msg = try allocator.dupe(u8, p.error_msg);
        errdefer allocator.free(msg);
        const files = try dupeStrings(allocator, p.failed_files);
        return .{
            .allocator = allocator,
            .id = p.id,
            .job_id = p.job_id,
            .state = p.state,
            .started_at = p.started_at,
            .finished_at = p.finished_at,
            .error_msg = msg,
            .failed_files = files,
        };
    }

    pub fn deinit(self: *VerifySet) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.error_msg);
        freeStrings(self.allocator, self.failed_files);
        self.* = undefined;
    }

    pub fn setId(self: *VerifySet, id: VerifySetId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                inline else => |*payload| if (payload.id == 0) {
                    payload.id = id;
                },
            }
        }
    }

    /// pending → verifying. Returns whether it actually moved.
    ///
    /// A set already in `verifying` is a crash mid-pass being retried:
    /// no transition, no second `VerifyStarted`, and the worker carries
    /// on. Reaching here from a terminal state is a bug in the caller,
    /// which is why that is an error rather than Go's silent return.
    pub fn markStarted(self: *VerifySet, now: Timestamp) Error!bool {
        if (self.state.isTerminal()) return error.AlreadyTerminal;
        if (self.state != .pending) return false;
        self.state = .verifying;
        self.started_at = now;
        try self.events.record(self.allocator, .{ .started = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
        return true;
    }

    /// → ok. Every checksum matched.
    pub fn markOk(self: *VerifySet, now: Timestamp) Error!void {
        if (self.state.isTerminal()) return error.AlreadyTerminal;
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);
        self.state = .ok;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .ok = .{
            .id = self.id,
            .job_id = self.job_id,
            .at = now,
        } });
    }

    /// → repair_needed, carrying the filenames that failed.
    ///
    /// Both the aggregate and the event get their own copy of the list:
    /// the aggregate's is freed by `reset`, so an event sharing it would
    /// dangle the moment the repair worker finishes.
    pub fn markRepairNeeded(
        self: *VerifySet,
        failed_files: []const []const u8,
        now: Timestamp,
    ) Error!void {
        if (self.state.isTerminal()) return error.AlreadyTerminal;

        const mine = try dupeStrings(self.allocator, failed_files);
        errdefer freeStrings(self.allocator, mine);
        const theirs = try dupeStrings(self.allocator, failed_files);
        errdefer freeStrings(self.allocator, theirs);
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);

        freeStrings(self.allocator, self.failed_files);
        self.failed_files = mine;
        self.state = .repair_needed;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .repair_needed = .{
            .id = self.id,
            .job_id = self.job_id,
            .failed_files = theirs,
            .at = now,
        } });
    }

    /// → failed, from any non-failed state. Used when the PAR2 metadata
    /// is itself unreadable, which can be discovered before verification
    /// properly starts.
    ///
    /// Idempotent: failing an already-failed set keeps the first reason
    /// and emits nothing, so a retry loop cannot bury the original cause
    /// under a cascade of downstream errors.
    pub fn markFailed(self: *VerifySet, reason: []const u8, now: Timestamp) Allocator.Error!void {
        if (self.state == .failed) return;

        const msg = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(msg);
        const copy = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(copy);
        try self.events.items.ensureUnusedCapacity(self.allocator, 1);

        self.allocator.free(self.error_msg);
        self.error_msg = msg;
        self.state = .failed;
        self.finished_at = now;
        self.events.items.appendAssumeCapacity(.{ .failed = .{
            .id = self.id,
            .job_id = self.job_id,
            .err = copy,
            .at = now,
        } });
    }

    /// repair_needed → pending, so a second pass can confirm the repair
    /// worker's output.
    ///
    /// Any other state is an error, deliberately: nobody should reset an
    /// `ok` set (it would re-verify for nothing) or a `failed` one (the
    /// PAR2 metadata is still unreadable). No event — the service emits
    /// `VerifyStarted` when it picks the set back up.
    pub fn reset(self: *VerifySet) TransitionError!void {
        if (self.state != .repair_needed) return error.NotRepairNeeded;
        self.state = .pending;
        freeStrings(self.allocator, self.failed_files);
        self.failed_files = &.{};
        self.allocator.free(self.error_msg);
        self.error_msg = "";
        self.started_at = null;
        self.finished_at = null;
    }

    pub fn pullEvents(self: *VerifySet) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    pub fn pendingEvents(self: *const VerifySet) []const Event {
        return self.events.view();
    }
};

/// The outcome for one file in a verify pass.
pub const FileResult = struct {
    /// Borrowed from whatever produced the result — the PAR2 verifier's
    /// own buffers. Copy it if you keep it past the pass.
    filename: []const u8,
    ok: bool,
    /// Populated only when `ok` is false. Borrowed, as `filename`.
    reason: []const u8 = "",
};

/// A whole verify pass, per file.
pub const Result = struct {
    files: []const FileResult = &.{},

    /// True when every file verified. Vacuously true for an empty set —
    /// the caller decides whether "no files" is itself a failure, and
    /// `app/verify` does exactly that by refusing to run without PAR2.
    pub fn allOk(self: Result) bool {
        for (self.files) |f| {
            if (!f.ok) return false;
        }
        return true;
    }

    /// How many files failed. Lets a caller size a buffer before
    /// collecting the names.
    pub fn failedCount(self: Result) usize {
        var n: usize = 0;
        for (self.files) |f| {
            if (!f.ok) n += 1;
        }
        return n;
    }

    /// Collects the failed filenames in iteration order. The returned
    /// slice is owned by the caller; the names inside it are still
    /// borrowed from `files`, so it feeds straight into
    /// `markRepairNeeded`, which copies.
    pub fn failedNames(self: Result, allocator: Allocator) Allocator.Error![]const []const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        errdefer out.deinit(allocator);
        try out.ensureTotalCapacityPrecise(allocator, self.failedCount());
        for (self.files) |f| {
            if (!f.ok) out.appendAssumeCapacity(f.filename);
        }
        return out.toOwnedSlice(allocator);
    }
};

/// Errors a `VerifySet` repository raises that callers branch on.
pub const RepositoryError = error{
    VerifySetNotFound,
};

// ---------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------

/// Deep-copies a list of strings. Unwinds cleanly on a mid-list OOM,
/// which is the whole reason this isn't inlined at the three call sites.
fn dupeStrings(allocator: Allocator, in: []const []const u8) Allocator.Error![]const []const u8 {
    const out = try allocator.alloc([]const u8, in.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |s| allocator.free(s);
        allocator.free(out);
    }
    for (in, 0..) |s, i| {
        out[i] = try allocator.dupe(u8, s);
        filled = i + 1;
    }
    return out;
}

fn freeStrings(allocator: Allocator, list: []const []const u8) void {
    for (list) |s| allocator.free(s);
    allocator.free(list);
}

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no unit tests; these are written from the
// implementation and from how `app/verify/service.go` drives it —
// every transition, every invariant, every illegal transition.
// ---------------------------------------------------------------------

const t = std.testing;

test "state classification and persisted form" {
    try t.expect(!VerifyState.pending.isTerminal());
    try t.expect(!VerifyState.verifying.isTerminal());
    try t.expect(VerifyState.ok.isTerminal());
    try t.expect(VerifyState.repair_needed.isTerminal());
    try t.expect(VerifyState.failed.isTerminal());

    try t.expectEqualStrings("repair_needed", VerifyState.repair_needed.toString());
    try t.expectEqual(VerifyState.repair_needed, VerifyState.parse("repair_needed").?);
    try t.expectEqual(@as(?VerifyState, null), VerifyState.parse("nonsense"));
}

test "init starts pending, silent, and requires a job id" {
    try t.expectError(error.JobIdRequired, VerifySet.init(t.allocator, 0, 1));

    var v = try VerifySet.init(t.allocator, 42, 1);
    defer v.deinit();
    try t.expectEqual(VerifyState.pending, v.state);
    try t.expectEqual(@as(JobId, 42), v.job_id);
    try t.expectEqual(@as(?Timestamp, null), v.started_at);
    try t.expectEqual(@as(usize, 0), v.pendingEvents().len);
}

test "markStarted moves pending to verifying once" {
    var v = try VerifySet.init(t.allocator, 42, 1);
    defer v.deinit();
    v.setId(3);

    try t.expect(try v.markStarted(10));
    try t.expectEqual(VerifyState.verifying, v.state);
    try t.expectEqual(@as(?Timestamp, 10), v.started_at);

    // A crash-retry finds it already verifying: no move, no second event.
    try t.expect(!try v.markStarted(20));
    try t.expectEqual(@as(?Timestamp, 10), v.started_at);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("verify.started", batch[0].topic());
    try t.expectEqual(@as(VerifySetId, 3), batch[0].aggregateId());
    try t.expectEqual(@as(JobId, 42), batch[0].jobId());
}

test "markStarted refuses to restart a finished pass" {
    var v = try VerifySet.init(t.allocator, 1, 1);
    defer v.deinit();
    _ = try v.markStarted(1);
    try v.markOk(2);
    try t.expectError(error.AlreadyTerminal, v.markStarted(3));

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
}

test "markOk finishes the pass and emits VerifyOK" {
    var v = try VerifySet.init(t.allocator, 7, 1);
    defer v.deinit();
    v.setId(2);
    _ = try v.markStarted(1);
    t.allocator.free(try v.pullEvents());

    try v.markOk(99);
    try t.expectEqual(VerifyState.ok, v.state);
    try t.expectEqual(@as(?Timestamp, 99), v.finished_at);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqualStrings("verify.ok", batch[0].topic());
    try t.expectEqual(@as(Timestamp, 99), batch[0].occurredAt());
}

test "markOk on a terminal set is rejected" {
    var v = try VerifySet.init(t.allocator, 1, 1);
    defer v.deinit();
    try v.markRepairNeeded(&.{"a.rar"}, 5);
    try t.expectError(error.AlreadyTerminal, v.markOk(6));
    try t.expectEqual(VerifyState.repair_needed, v.state);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
}

test "markRepairNeeded records the failed files on aggregate and event" {
    var v = try VerifySet.init(t.allocator, 8, 1);
    defer v.deinit();
    v.setId(4);
    _ = try v.markStarted(1);
    t.allocator.free(try v.pullEvents());

    try v.markRepairNeeded(&.{ "release.r00", "release.r01" }, 55);
    try t.expectEqual(VerifyState.repair_needed, v.state);
    try t.expectEqual(@as(usize, 2), v.failed_files.len);
    try t.expectEqualStrings("release.r00", v.failed_files[0]);
    try t.expectEqualStrings("release.r01", v.failed_files[1]);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqualStrings("verify.repair_needed", batch[0].topic());
    try t.expectEqual(@as(usize, 2), batch[0].repair_needed.failed_files.len);
    try t.expectEqualStrings("release.r01", batch[0].repair_needed.failed_files[1]);
}

test "an event's file list survives the aggregate being reset" {
    // The reason the event owns its copy: reset frees the aggregate's.
    var v = try VerifySet.init(t.allocator, 8, 1);
    defer v.deinit();
    try v.markRepairNeeded(&.{"damaged.rar"}, 5);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);

    try v.reset();
    try t.expectEqual(@as(usize, 0), v.failed_files.len);
    try t.expectEqualStrings("damaged.rar", batch[0].repair_needed.failed_files[0]);
}

test "markFailed captures the reason and is idempotent" {
    var v = try VerifySet.init(t.allocator, 9, 1);
    defer v.deinit();
    v.setId(5);

    try v.markFailed("no .par2 files in job", 30);
    try t.expectEqual(VerifyState.failed, v.state);
    try t.expectEqualStrings("no .par2 files in job", v.error_msg);
    try t.expectEqual(@as(?Timestamp, 30), v.finished_at);

    // A second failure keeps the first cause and stays silent.
    try v.markFailed("something downstream also broke", 40);
    try t.expectEqualStrings("no .par2 files in job", v.error_msg);
    try t.expectEqual(@as(?Timestamp, 30), v.finished_at);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("verify.failed", batch[0].topic());
    try t.expectEqualStrings("no .par2 files in job", batch[0].failed.err);
}

test "markFailed reaches failed from repair_needed too" {
    // app/verify does this when the repair worker gives up.
    var v = try VerifySet.init(t.allocator, 1, 1);
    defer v.deinit();
    try v.markRepairNeeded(&.{"x"}, 1);
    try v.markFailed("repair exhausted", 2);
    try t.expectEqual(VerifyState.failed, v.state);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
}

test "reset only works from repair_needed" {
    var v = try VerifySet.init(t.allocator, 1, 1);
    defer v.deinit();

    try t.expectError(error.NotRepairNeeded, v.reset()); // pending
    _ = try v.markStarted(1);
    try t.expectError(error.NotRepairNeeded, v.reset()); // verifying

    try v.markRepairNeeded(&.{"a"}, 2);
    try v.reset();
    try t.expectEqual(VerifyState.pending, v.state);
    try t.expectEqual(@as(usize, 0), v.failed_files.len);
    try t.expectEqual(@as(usize, 0), v.error_msg.len);
    try t.expectEqual(@as(?Timestamp, null), v.started_at);
    try t.expectEqual(@as(?Timestamp, null), v.finished_at);

    // And the reopened pass can start again.
    try t.expect(try v.markStarted(3));

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 3), batch.len); // started, repair_needed, started
}

test "reset is rejected after ok or failed" {
    var okset = try VerifySet.init(t.allocator, 1, 1);
    defer okset.deinit();
    try okset.markOk(1);
    try t.expectError(error.NotRepairNeeded, okset.reset());
    t.allocator.free(try okset.pullEvents());

    var failedset = try VerifySet.init(t.allocator, 1, 1);
    defer failedset.deinit();
    try failedset.markFailed("bad par2", 1);
    try t.expectError(error.NotRepairNeeded, failedset.reset());
    const batch = try failedset.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
}

test "setId patches every queued event's placeholder" {
    var v = try VerifySet.init(t.allocator, 1, 1);
    defer v.deinit();
    _ = try v.markStarted(1);
    try v.markRepairNeeded(&.{"a"}, 2);
    v.setId(77);

    const batch = try v.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    for (batch) |e| try t.expectEqual(@as(VerifySetId, 77), e.aggregateId());
}

test "hydrate restores the summary without events" {
    var v = try VerifySet.hydrate(t.allocator, .{
        .id = 3,
        .job_id = 12,
        .state = .repair_needed,
        .started_at = 5,
        .finished_at = 6,
        .failed_files = &.{ "a.r00", "b.r01" },
    });
    defer v.deinit();

    try t.expectEqual(VerifyState.repair_needed, v.state);
    try t.expectEqual(@as(usize, 2), v.failed_files.len);
    try t.expectEqualStrings("b.r01", v.failed_files[1]);
    try t.expectEqual(@as(usize, 0), v.pendingEvents().len);

    // The hydrated list is the aggregate's own copy.
    try v.reset();
    try t.expectEqual(@as(usize, 0), v.failed_files.len);
}

test "result reports all-ok and collects failed names in order" {
    const r: Result = .{ .files = &.{
        .{ .filename = "a.rar", .ok = true },
        .{ .filename = "b.r00", .ok = false, .reason = "md5 mismatch" },
        .{ .filename = "c.r01", .ok = true },
        .{ .filename = "d.r02", .ok = false, .reason = "short read" },
    } };

    try t.expect(!r.allOk());
    try t.expectEqual(@as(usize, 2), r.failedCount());

    const names = try r.failedNames(t.allocator);
    defer t.allocator.free(names);
    try t.expectEqual(@as(usize, 2), names.len);
    try t.expectEqualStrings("b.r00", names[0]);
    try t.expectEqualStrings("d.r02", names[1]);
}

test "an all-good result yields no names, and an empty one is vacuously ok" {
    const good: Result = .{ .files = &.{.{ .filename = "a", .ok = true }} };
    try t.expect(good.allOk());
    const names = try good.failedNames(t.allocator);
    defer t.allocator.free(names);
    try t.expectEqual(@as(usize, 0), names.len);

    const nothing: Result = .{};
    try t.expect(nothing.allOk());
    try t.expectEqual(@as(usize, 0), nothing.failedCount());
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
    var v = try VerifySet.init(t.allocator, 1, 1);
    try v.markRepairNeeded(&.{ "a", "b" }, 1);
    try v.markFailed("and then this", 2);
    // No pull: deinit owns both events' strings and the aggregate's.
    v.deinit();
}
