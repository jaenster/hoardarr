//! Identity and lifecycle vocabulary for the `download` bounded
//! context. Nothing here allocates, does I/O, or reads a clock.
//!
//! The three id types live here rather than next to their entities so
//! that `events.zig` can name them without importing `job.zig` —
//! Zig's imports must form a DAG, while Go's package-level scope let
//! events.go reference `JobID` declared in job.go.
//!
//! The enum tag names are the on-the-wire / on-disk representation:
//! `@tagName` produces exactly the strings the Go `JobState` etc.
//! carried (`download_complete`, `waiting_for_server`, ...), so the
//! persistence layer round-trips them with `toString` / `parse` and no
//! translation table.

const std = @import("std");

/// Unix milliseconds, UTC. The domain never reads a clock — every
/// mutator takes the instant as a parameter, which keeps the aggregate
/// pure and the tests deterministic.
///
/// Instants that may be absent (`started_at`, `next_retry_at`, ...) are
/// modelled as `?Timestamp`; `null` is Go's `time.Time{}.IsZero()`.
pub const Timestamp = i64;

/// Identifies a `Job` aggregate. Allocated by the persistence layer;
/// 0 means "not yet persisted".
pub const JobId = i64;

/// Identifies a `File` entity within a Job.
pub const FileId = i64;

/// Identifies a `Segment` entity within a File.
pub const SegmentId = i64;

/// The lifecycle of a download job.
///
/// Forward edges:
///
///     queued ─▶ downloading ─▶ download_complete ─▶ completed
///        │           │                 │
///        │           ▼                 ▼
///        ▼         paused            failed
///     aborted
///
/// The verify/repair/unpack states belong to later stages of the
/// pipeline; the aggregate accepts them so the post-download contexts
/// can park a Job here without a second state machine.
pub const JobState = enum {
    queued,
    downloading,
    paused,
    download_complete,
    verifying,
    repairing,
    unpacking,
    completed,
    failed,
    aborted,
    /// The job is alive but cannot fetch because no usable NNTP server
    /// is currently configured / enabled / has quota. The runner exits
    /// cleanly on this transition; the orchestrator flips the state
    /// back to `queued` when a server becomes available.
    waiting_for_server,

    pub fn toString(self: JobState) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?JobState {
        return std.meta.stringToEnum(JobState, s);
    }

    /// True for states where downloading or post-processing is in
    /// progress. `waiting_for_server` counts as active — the job is
    /// alive and resumes the moment a server appears.
    pub fn isActive(self: JobState) bool {
        return switch (self) {
            .queued, .downloading, .verifying, .repairing, .unpacking, .waiting_for_server => true,
            else => false,
        };
    }

    /// True for states from which no further transition is expected.
    pub fn isTerminal(self: JobState) bool {
        return switch (self) {
            .completed, .failed, .aborted => true,
            else => false,
        };
    }
};

/// The lifecycle of a single file within a Job.
pub const FileState = enum {
    pending,
    downloading,
    complete,
    failed,

    pub fn toString(self: FileState) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?FileState {
        return std.meta.stringToEnum(FileState, s);
    }
};

/// The lifecycle of a single segment (one Usenet article).
pub const SegmentState = enum {
    /// Not yet dispatched. Default state.
    pending,
    /// Handed to a worker; awaiting the fetch result. Reset to
    /// `pending` on orchestrator restart.
    inflight,
    /// Fetched, decoded, written to disk.
    done,
    /// Every configured server returned 430. Unrecoverable from
    /// Usenet; PAR2 may still rescue the file.
    missing,
    /// Non-430 errors exhausted the retry budget (network, parse, CRC).
    failed,

    pub fn toString(self: SegmentState) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?SegmentState {
        return std.meta.stringToEnum(SegmentState, s);
    }

    /// True once the segment will not change state again. This is the
    /// predicate the Job's unresolved-segment counters track; see
    /// `Job.adjustUnresolved`.
    pub fn isTerminal(self: SegmentState) bool {
        return switch (self) {
            .done, .missing, .failed => true,
            else => false,
        };
    }
};

test "state strings round-trip the persisted form" {
    const t = std.testing;
    try t.expectEqualStrings("download_complete", JobState.download_complete.toString());
    try t.expectEqualStrings("waiting_for_server", JobState.waiting_for_server.toString());
    try t.expectEqual(JobState.download_complete, JobState.parse("download_complete").?);
    try t.expectEqual(@as(?JobState, null), JobState.parse("nonsense"));
    try t.expectEqualStrings("inflight", SegmentState.inflight.toString());
    try t.expectEqual(SegmentState.missing, SegmentState.parse("missing").?);
    try t.expectEqual(FileState.complete, FileState.parse("complete").?);
}

test "active and terminal classification" {
    const t = std.testing;
    try t.expect(JobState.waiting_for_server.isActive());
    try t.expect(JobState.queued.isActive());
    try t.expect(!JobState.paused.isActive());
    try t.expect(!JobState.download_complete.isActive());
    try t.expect(JobState.failed.isTerminal());
    try t.expect(!JobState.downloading.isTerminal());

    try t.expect(SegmentState.done.isTerminal());
    try t.expect(SegmentState.missing.isTerminal());
    try t.expect(SegmentState.failed.isTerminal());
    try t.expect(!SegmentState.pending.isTerminal());
    try t.expect(!SegmentState.inflight.isTerminal());
}
