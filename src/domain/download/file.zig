//! `File` — one logical file of a release, split across one or more
//! segments. Files belong to a `Job` and inherit its lifecycle.
//!
//! Like `Segment`, a File carries no allocator: its `filename`,
//! `poster`, `groups` and `segments` are all allocated and freed by the
//! owning Job.

const std = @import("std");
const state_mod = @import("state.zig");
const segment_mod = @import("segment.zig");

const FileId = state_mod.FileId;
const JobId = state_mod.JobId;
const FileState = state_mod.FileState;
const Segment = segment_mod.Segment;
const NewSegmentParams = segment_mod.NewSegmentParams;

/// Constructor input for a fresh pending File plus its segments.
/// Strings and slices are borrowed for the duration of the call; the
/// Job copies what it keeps.
pub const NewFileParams = struct {
    filename: []const u8,
    poster: []const u8 = "",
    groups: []const []const u8 = &.{},
    size_bytes: i64 = 0,
    is_par2: bool = false,
    is_recovery_vol: bool = false,
    segments: []const NewSegmentParams,
};

/// Constructor input for a File being rebuilt from persistence.
pub const HydrateFileParams = struct {
    id: FileId = 0,
    job_id: JobId = 0,
    filename: []const u8,
    poster: []const u8 = "",
    groups: []const []const u8 = &.{},
    size_bytes: i64 = 0,
    state: FileState = .pending,
    /// Kept as a stored field rather than `segments.len` because the
    /// "shallow" repository queries hydrate a File's metadata without
    /// its segments, and the UI still wants the count.
    segment_count: i32 = 0,
    segments_done: i32 = 0,
    is_par2: bool = false,
    is_recovery_vol: bool = false,
    segments: []const segment_mod.HydrateSegmentParams = &.{},
};

pub const File = struct {
    /// Assigned by the persistence layer on INSERT; 0 until then.
    id: FileId = 0,
    job_id: JobId = 0,
    /// Owned by the Job; immutable once constructed, so `FileCompleted`
    /// borrows it rather than copying.
    filename: []const u8,
    /// Owned by the Job.
    poster: []const u8 = "",
    /// Owned by the Job — both the outer slice and each string.
    groups: []const []const u8 = &.{},
    size_bytes: i64 = 0,
    /// Read-only from outside the aggregate.
    state: FileState = .pending,
    segment_count: i32 = 0,
    segments_done: i32 = 0,
    is_par2: bool = false,
    /// True when the filename matched `<base>.vol###+##.par2`. While a
    /// Job has `fetch_recovery_vols = false` the orchestrator skips
    /// these files' segments entirely — see `Job.pendingSegments`. The
    /// small index PAR2 (no `.vol`) stays false here even though
    /// `is_par2` is true.
    is_recovery_vol: bool = false,
    /// Owned by the Job. Pointers into this slice are stable for the
    /// aggregate's lifetime: it is allocated once at construction and
    /// never resized, which is what lets the segment index cache
    /// `*Segment`.
    segments: []Segment = &.{},

    /// Called by the repository after INSERT. Pushes the new id down
    /// into the segments, as the Go version did.
    pub fn setId(self: *File, id: FileId) void {
        self.id = id;
        for (self.segments) |*s| s.setFileId(id);
    }

    /// Called when the file is associated with a newly-saved job row.
    pub fn setJobId(self: *File, id: JobId) void {
        self.job_id = id;
    }
};
