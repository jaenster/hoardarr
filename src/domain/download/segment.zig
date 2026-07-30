//! `Segment` — one Usenet article, the smallest unit of work for the
//! download orchestrator.
//!
//! Segments are entities owned by their `File`, which is owned by the
//! `Job` aggregate. They hold no allocator: every string in here is
//! allocated and freed by the owning Job (see `job.zig`), which is what
//! keeps ownership auditable — there is exactly one `deinit` in this
//! whole context.
//!
//! Mutation is likewise the Job's business. The methods below are the
//! ones that cannot break an aggregate invariant (id plumbing after an
//! INSERT, offset bookkeeping); anything that changes `state` lives on
//! `Job` because it has to keep the unresolved-segment counters in
//! step.

const std = @import("std");
const state_mod = @import("state.zig");

const SegmentId = state_mod.SegmentId;
const FileId = state_mod.FileId;
const SegmentState = state_mod.SegmentState;
const Timestamp = state_mod.Timestamp;

/// Constructor input for a fresh, pending Segment. The strings are
/// borrowed for the duration of the call; the Job copies what it keeps.
pub const NewSegmentParams = struct {
    seq_index: i32 = 0,
    message_id: []const u8,
    bytes: i64 = 0,
};

/// Constructor input for a Segment being rebuilt from persistence.
/// Strings are borrowed for the duration of the call.
pub const HydrateSegmentParams = struct {
    id: SegmentId = 0,
    file_id: FileId = 0,
    seq_index: i32 = 0,
    message_id: []const u8,
    bytes: i64 = 0,
    state: SegmentState = .pending,
    attempts: i32 = 0,
    last_error: ?[]const u8 = null,
    file_offset: i64 = 0,
    next_retry_at: ?Timestamp = null,
};

pub const Segment = struct {
    /// Assigned by the persistence layer on INSERT; 0 until then.
    id: SegmentId = 0,
    file_id: FileId = 0,
    /// 1-based position within the parent file.
    seq_index: i32 = 0,
    /// Message-id without the surrounding angle brackets. Owned by the
    /// Job; immutable once constructed, which is why events may borrow
    /// it instead of copying.
    message_id: []const u8,
    /// Article size on the wire, yEnc overhead included.
    bytes: i64 = 0,
    /// Read-only from outside the aggregate: transition it through the
    /// `Job.markSegment*` methods so the resolved counters stay honest.
    state: SegmentState = .pending,
    attempts: i32 = 0,
    /// Last failure text, or null for "no error recorded". Owned by the
    /// Job, which frees the previous value before storing a new one.
    last_error: ?[]const u8 = null,
    /// 0-based byte offset within the assembled file where this
    /// segment's decoded bytes go. Set after the first successful yEnc
    /// decode (`=ypart begin - 1`) and persisted.
    file_offset: i64 = 0,
    /// Earliest instant this segment may be re-dispatched; null = ready
    /// now. Populated on transient retry so the back-off survives a
    /// restart instead of dying with an in-memory timer.
    next_retry_at: ?Timestamp = null,

    /// Go returned "" for "no error"; callers that want the same shape
    /// use this instead of unwrapping the optional.
    pub fn lastError(self: Segment) []const u8 {
        return self.last_error orelse "";
    }

    /// Called by the repository to assign a database id after INSERT.
    /// The Job's segment index keys on the id, so a caller doing this
    /// must follow up with `Job.rebuildSegmentIndex`.
    pub fn setId(self: *Segment, id: SegmentId) void {
        self.id = id;
    }

    /// Called when a fresh segment is associated with its newly-saved
    /// file row.
    pub fn setFileId(self: *Segment, id: FileId) void {
        self.file_id = id;
    }

    /// Records the 0-based byte offset within the assembled file. The
    /// orchestrator computes this after decoding the first segment of a
    /// file (single-part: 0; multi-part: `ypart.begin - 1`).
    pub fn setFileOffset(self: *Segment, off: i64) void {
        self.file_offset = off;
    }

    /// True when the segment is eligible for dispatch at `now`: pending
    /// and past any back-off window. Shared by `Job.pendingSegments`
    /// and `Job.nextRetryReadyAt` so the two can never disagree about
    /// what "ready" means.
    pub fn isReadyAt(self: Segment, now: Timestamp) bool {
        if (self.state != .pending) return false;
        const at = self.next_retry_at orelse return true;
        return at <= now;
    }

    /// True when the segment is pending but held back by its back-off
    /// window — the state `nextRetryReadyAt` reports on.
    pub fn isDeferredAt(self: Segment, now: Timestamp) bool {
        if (self.state != .pending) return false;
        const at = self.next_retry_at orelse return false;
        return at > now;
    }
};
