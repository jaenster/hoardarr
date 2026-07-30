//! Value types and sentinel errors shared between the `download`
//! aggregate and the adapters that persist it.
//!
//! The Go original also declared the `JobRepository` and
//! `ArticleFetcher` interfaces here — a hexagonal-architecture habit
//! that only pays off in a language where the interface is the
//! dependency-inversion mechanism. In Zig the caller composes concrete
//! types, and a domain-side vtable would drag `Allocator`-flavoured I/O
//! signatures (and an indirect call per segment) into a layer that is
//! meant to be pure. So the ports proper live with their
//! implementations in `store/` and `nntp/`; what stays here is the
//! vocabulary those implementations must agree on with the domain.

const std = @import("std");
const state = @import("state.zig");

const JobState = state.JobState;
const SegmentId = state.SegmentId;
const SegmentState = state.SegmentState;
const Timestamp = state.Timestamp;

/// Filters terminal-state jobs for the history views. Every field is
/// optional; a null/empty filter means "no constraint". The repository
/// clamps `limit` to a sane upper bound so a runaway client cannot drag
/// the whole history into memory.
pub const HistoryQuery = struct {
    /// `finished_at > since`.
    since: ?Timestamp = null,
    /// Exact-match category; empty means any.
    category: []const u8 = "",
    /// Restrict to one state; null means any terminal state.
    state: ?JobState = null,
    /// 0 → the repository default.
    limit: u32 = 0,
};

/// One segment mutation pushed by the orchestrator's completion
/// drainer. The repository applies a whole batch in a single
/// transaction, so the writes commit atomically with the events the
/// orchestrator publishes alongside them.
pub const SegmentUpdate = struct {
    segment_id: SegmentId,
    state: SegmentState,
    attempts: i32 = 0,
    /// Borrowed from the aggregate; valid until the Job is deinit'd or
    /// the segment's error is replaced. The repository copies it into
    /// the statement binding and keeps nothing.
    last_error: []const u8 = "",
    file_offset: i64 = 0,
    next_retry_at: ?Timestamp = null,
};

/// Errors a `Job` repository raises that callers branch on.
///
/// `DuplicateNzbHash` covers the race where two concurrent uploads of
/// identical NZB bytes both pass a hash pre-check before either
/// commits: the first INSERT wins, the second lands here, and the
/// application service turns it into "already queued" with the existing
/// job's id.
pub const RepositoryError = error{
    JobNotFound,
    DuplicateNzbHash,
};

test "history query defaults are the unconstrained query" {
    const q = HistoryQuery{};
    try std.testing.expectEqual(@as(?Timestamp, null), q.since);
    try std.testing.expectEqual(@as(?JobState, null), q.state);
    try std.testing.expectEqual(@as(usize, 0), q.category.len);
    try std.testing.expectEqual(@as(u32, 0), q.limit);
}
