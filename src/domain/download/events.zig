//! Domain events emitted by the `download` aggregate.
//!
//! Events are plain data — a tagged union, no interfaces and no
//! vtables. The bus (a transactional outbox in the store layer) reads
//! `topic`, `aggregateId` and `occurredAt` off the union and serialises
//! the payload; it never calls back into the domain.
//!
//! # String ownership
//!
//! Two classes of string appear in these payloads, and `deinit` knows
//! statically which is which:
//!
//!   * **Borrowed** — `JobCreated.name`, `JobCreated.category`,
//!     `SegmentDispatched.message_id` and `FileCompleted.filename`
//!     point into the emitting `Job`'s own storage. Those strings are
//!     immutable for the lifetime of the aggregate, so there is nothing
//!     to copy and the hot dispatch/complete path allocates nothing at
//!     all. They are only valid until `Job.deinit`, so a pulled event
//!     batch must be consumed (or its strings duplicated) before the
//!     Job is destroyed.
//!
//!   * **Owned** — the error/reason strings
//!     (`JobWaitingForServer.reason`, `SegmentFailed.err`,
//!     `JobDownloadFailed.err`, `JobFailed.err`) are duplicated with
//!     the Job's allocator when the event is recorded. They cannot be
//!     borrowed: `Job.error_msg` and `Segment.last_error` are
//!     overwritten (and freed) by later transitions, which would leave
//!     an already-pulled event dangling. The event owns its copy and
//!     `Event.deinit` frees it.
//!
//! So: whoever holds an `Event` must eventually call `Event.deinit`
//! (or `deinitAll` on a batch) with the allocator the Job was built
//! with. `Job.deinit` does that for events still queued on the
//! aggregate.

const std = @import("std");
const state = @import("state.zig");

const Allocator = std.mem.Allocator;
const JobId = state.JobId;
const FileId = state.FileId;
const SegmentId = state.SegmentId;
const Timestamp = state.Timestamp;

/// Topic prefix for every event in this context.
pub const topic_prefix = "download.";

/// Emitted when a fresh Job is constructed. `id` is 0 until the
/// repository assigns one; `Job.setId` patches the queued event.
pub const JobCreated = struct {
    id: JobId,
    /// Borrowed from the Job.
    name: []const u8,
    /// Borrowed from the Job.
    category: []const u8,
    total_bytes: i64,
    at: Timestamp,
};

/// First transition from queued to downloading.
pub const JobStarted = struct {
    id: JobId,
    at: Timestamp,
};

/// The user paused the job.
pub const JobPaused = struct {
    id: JobId,
    at: Timestamp,
};

/// The user resumed the job, or a server became available again and
/// the orchestrator unparked it.
pub const JobResumed = struct {
    id: JobId,
    at: Timestamp,
};

/// The user deleted the job. Recorded on the aggregate so the bus
/// delivers it in the same transaction as the DELETE.
pub const JobRemoved = struct {
    id: JobId,
    at: Timestamp,
};

/// The orchestrator parked the job because no usable NNTP server is
/// configured / enabled / has quota. Counterpart to `JobResumed`.
pub const JobWaitingForServer = struct {
    job_id: JobId,
    /// Owned by the event.
    reason: []const u8,
    at: Timestamp,
};

/// The repair worker needs PAR2 recovery volumes that were deferred at
/// job-add time. The orchestrator re-enters the runner so the
/// previously hidden recovery-vol segments get dispatched.
pub const RecoveryVolsRequested = struct {
    job_id: JobId,
    at: Timestamp,
};

/// A worker accepted this segment for fetch. `message_id` rides along
/// so the UI can show "fetching <msg-id>" without a follow-up lookup.
pub const SegmentDispatched = struct {
    job_id: JobId,
    segment_id: SegmentId,
    /// Borrowed from the Job.
    message_id: []const u8,
    attempt: i32,
    at: Timestamp,
};

/// Segment fetched, decoded, and persisted to disk.
pub const SegmentCompleted = struct {
    job_id: JobId,
    file_id: FileId,
    segment_id: SegmentId,
    bytes: i64,
    at: Timestamp,
};

/// Every configured server returned 430 for this article.
pub const SegmentMissing = struct {
    job_id: JobId,
    file_id: FileId,
    segment_id: SegmentId,
    at: Timestamp,
};

/// Non-430 retry budget exhausted (network, decode, CRC).
pub const SegmentFailed = struct {
    job_id: JobId,
    file_id: FileId,
    segment_id: SegmentId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

/// Every segment of this file reached `done`.
pub const FileCompleted = struct {
    job_id: JobId,
    file_id: FileId,
    /// Borrowed from the Job.
    filename: []const u8,
    at: Timestamp,
};

/// Every segment in the job resolved (done, missing, or failed).
/// Hands the job to the verify worker.
pub const JobDownloadComplete = struct {
    job_id: JobId,
    missing_segments: u32,
    at: Timestamp,
};

/// Fatal error during the download phase (nothing retrieved, ratio of
/// missing bytes past the hopeless threshold).
pub const JobDownloadFailed = struct {
    job_id: JobId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

/// Terminal success: files verified and moved into
/// `complete/<category>/<release>/`.
pub const JobCompleted = struct {
    job_id: JobId,
    at: Timestamp,
};

/// Terminal failure. Distinct from `JobDownloadFailed`, which covers
/// download-phase errors only: `JobFailed` is any stage deciding the
/// Job will not recover. A download-phase abort emits both.
pub const JobFailed = struct {
    job_id: JobId,
    /// Owned by the event.
    err: []const u8,
    at: Timestamp,
};

/// Discriminant of `Event`. Also the switch key for `topic`.
pub const Kind = enum {
    job_created,
    job_started,
    job_paused,
    job_resumed,
    job_removed,
    job_waiting_for_server,
    recovery_vols_requested,
    segment_dispatched,
    segment_completed,
    segment_missing,
    segment_failed,
    file_completed,
    job_download_complete,
    job_download_failed,
    job_completed,
    job_failed,
};

/// One recorded domain event. Copyable; see the module comment for who
/// owns the strings inside.
pub const Event = union(Kind) {
    job_created: JobCreated,
    job_started: JobStarted,
    job_paused: JobPaused,
    job_resumed: JobResumed,
    job_removed: JobRemoved,
    job_waiting_for_server: JobWaitingForServer,
    recovery_vols_requested: RecoveryVolsRequested,
    segment_dispatched: SegmentDispatched,
    segment_completed: SegmentCompleted,
    segment_missing: SegmentMissing,
    segment_failed: SegmentFailed,
    file_completed: FileCompleted,
    job_download_complete: JobDownloadComplete,
    job_download_failed: JobDownloadFailed,
    job_completed: JobCompleted,
    job_failed: JobFailed,

    /// The bus topic. A comptime-known constant per tag, so this is a
    /// jump table returning pointers into .rodata — no formatting.
    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .job_created => topic_prefix ++ "job.created",
            .job_started => topic_prefix ++ "job.started",
            .job_paused => topic_prefix ++ "job.paused",
            .job_resumed => topic_prefix ++ "job.resumed",
            .job_removed => topic_prefix ++ "job.removed",
            .job_waiting_for_server => topic_prefix ++ "job.waiting_for_server",
            .recovery_vols_requested => topic_prefix ++ "job.recovery_vols_requested",
            .segment_dispatched => topic_prefix ++ "segment.dispatched",
            .segment_completed => topic_prefix ++ "segment.completed",
            .segment_missing => topic_prefix ++ "segment.missing",
            .segment_failed => topic_prefix ++ "segment.failed",
            .file_completed => topic_prefix ++ "file.completed",
            .job_download_complete => topic_prefix ++ "job.download_complete",
            .job_download_failed => topic_prefix ++ "job.download_failed",
            .job_completed => topic_prefix ++ "job.completed",
            .job_failed => topic_prefix ++ "job.failed",
        };
    }

    /// The aggregate this event belongs to. The Go version rendered
    /// this as a decimal string; keeping it numeric here means the
    /// domain does no formatting (and therefore no allocation) —
    /// `writeAggregateId` renders it when the bus needs text.
    pub fn aggregateId(self: Event) JobId {
        return switch (self) {
            .job_created => |e| e.id,
            .job_started => |e| e.id,
            .job_paused => |e| e.id,
            .job_resumed => |e| e.id,
            .job_removed => |e| e.id,
            .job_waiting_for_server => |e| e.job_id,
            .recovery_vols_requested => |e| e.job_id,
            .segment_dispatched => |e| e.job_id,
            .segment_completed => |e| e.job_id,
            .segment_missing => |e| e.job_id,
            .segment_failed => |e| e.job_id,
            .file_completed => |e| e.job_id,
            .job_download_complete => |e| e.job_id,
            .job_download_failed => |e| e.job_id,
            .job_completed => |e| e.job_id,
            .job_failed => |e| e.job_id,
        };
    }

    pub fn occurredAt(self: Event) Timestamp {
        return switch (self) {
            inline else => |e| e.at,
        };
    }

    /// Frees the strings this event owns. Borrowed strings (see the
    /// module comment) are left alone. Safe to call on any event.
    pub fn deinit(self: Event, allocator: Allocator) void {
        switch (self) {
            .job_waiting_for_server => |e| allocator.free(e.reason),
            .segment_failed => |e| allocator.free(e.err),
            .job_download_failed => |e| allocator.free(e.err),
            .job_failed => |e| allocator.free(e.err),
            else => {},
        }
    }
};

/// Big enough for any i64 rendered as decimal, sign included.
pub const AggregateIdBuf = [20]u8;

/// Renders `aggregateId` into caller-provided storage — the string form
/// the outbox stores in its `aggregate_id` column.
pub fn writeAggregateId(e: Event, buf: *AggregateIdBuf) []const u8 {
    return std.fmt.bufPrint(buf, "{d}", .{e.aggregateId()}) catch unreachable;
}

/// Frees a batch of events and the slice holding them. The counterpart
/// to `Job.pullEvents`, which transfers ownership of both to the
/// caller.
pub fn deinitAll(allocator: Allocator, batch: []Event) void {
    for (batch) |e| e.deinit(allocator);
    allocator.free(batch);
}

test "topics match the Go wire strings" {
    const t = std.testing;
    const e: Event = .{ .job_download_complete = .{ .job_id = 3, .missing_segments = 1, .at = 5 } };
    try t.expectEqualStrings("download.job.download_complete", e.topic());
    try t.expectEqual(@as(JobId, 3), e.aggregateId());
    try t.expectEqual(@as(Timestamp, 5), e.occurredAt());

    var buf: AggregateIdBuf = undefined;
    try t.expectEqualStrings("3", writeAggregateId(e, &buf));

    // Every tag must produce a distinct, prefixed topic — a copy-paste
    // slip in the switch above would otherwise route two event types to
    // one subscriber.
    var seen: [std.meta.fields(Kind).len][]const u8 = undefined;
    inline for (std.meta.fields(Kind), 0..) |f, i| {
        const ev: Event = @unionInit(Event, f.name, std.mem.zeroes(@FieldType(Event, f.name)));
        const tp = ev.topic();
        try t.expect(std.mem.startsWith(u8, tp, topic_prefix));
        for (seen[0..i]) |prev| try t.expect(!std.mem.eql(u8, prev, tp));
        seen[i] = tp;
    }
}

test "deinit frees only the owned strings" {
    const alloc = std.testing.allocator;
    const batch = try alloc.alloc(Event, 2);
    batch[0] = .{ .job_failed = .{ .job_id = 1, .err = try alloc.dupe(u8, "boom"), .at = 1 } };
    // Borrowed name: a static string the aggregate would have owned.
    batch[1] = .{ .job_created = .{ .id = 1, .name = "release", .category = "", .total_bytes = 0, .at = 1 } };
    deinitAll(alloc, batch);
}
