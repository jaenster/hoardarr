//! The `download` bounded context: NZB-driven download jobs.
//!
//! `Job` is the aggregate root. It owns `File` entities, which own
//! `Segment` entities. Every mutation goes through a method on `Job` so
//! the invariants hold and the domain events land on the aggregate,
//! where `pullEvents` hands them to the transactional outbox.
//!
//! The orchestrator consumes `JobCreated`, dispatches segment fetches
//! to the NNTP pool, and feeds results back in through
//! `markSegmentDone` / `markSegmentMissing` / `markSegmentFailed`.
//!
//! # Purity
//!
//! This module imports `std` and its siblings in this directory and
//! nothing else: no adapters, no database, no clock. Every mutator
//! takes the current instant as a parameter, which is both a DDD
//! requirement and what makes the tests below deterministic.
//!
//! # Ownership
//!
//! One allocator, held by the aggregate, owns everything reachable from
//! it: the `files` slice, each file's `segments` slice, and every string
//! in the tree. `File` and `Segment` are plain value types with no
//! allocator of their own — so there is exactly one `deinit` to audit,
//! and `std.testing.allocator` proves the tests leak nothing.
//!
//! Strings handed *in* (params, error messages) are borrowed for the
//! duration of the call and copied if kept. Strings handed *out* stay
//! owned by the Job, except in events — see `events.zig` for the
//! per-field rule there.
//!
//! # The unresolved-segment counters
//!
//! `allSegmentsResolved` runs on every segment transition. Scanning
//! file × segment for a non-terminal state made `SegmentState.isTerminal`
//! the hottest non-network frame in production profiling (a job with
//! ~110K segments at ~10 transitions/s is >1M predicate calls/s). It is
//! now two integer comparisons against counters maintained on the
//! terminal boundary by `adjustUnresolved`. Two buckets, not one, so
//! recovery-vol gating is honoured without a recount when
//! `requestRecoveryVols` flips the flag.
//!
//! A counter that drifts from reality is a silent correctness bug —
//! `recountUnresolved` exists to assert the two agree, and the
//! randomised test at the bottom of this file does exactly that.

const std = @import("std");
const state_mod = @import("state.zig");
const events = @import("events.zig");
const file_mod = @import("file.zig");
const segment_mod = @import("segment.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = state_mod.Timestamp;
pub const JobId = state_mod.JobId;
pub const FileId = state_mod.FileId;
pub const SegmentId = state_mod.SegmentId;
pub const JobState = state_mod.JobState;
pub const FileState = state_mod.FileState;
pub const SegmentState = state_mod.SegmentState;

pub const File = file_mod.File;
pub const NewFileParams = file_mod.NewFileParams;
pub const HydrateFileParams = file_mod.HydrateFileParams;
pub const Segment = segment_mod.Segment;
pub const NewSegmentParams = segment_mod.NewSegmentParams;
pub const HydrateSegmentParams = segment_mod.HydrateSegmentParams;

pub const Event = events.Event;

/// A segment together with its owning file — the pair every lookup
/// needs, cached so one map probe answers both questions.
pub const SegmentRef = struct {
    file: *File,
    seg: *Segment,
};

/// The outcome of one successful fetch attempt, fed back into the
/// aggregate by `markSegmentDone`.
pub const SegmentResult = struct {
    segment_id: SegmentId,
    /// Decoded bytes written to disk.
    bytes_on_disk: i64 = 0,
    file_offset: i64 = 0,
};

/// Inputs for a fresh Job. Strings and slices are borrowed for the
/// duration of the call.
pub const NewJobParams = struct {
    nzb_hash: []const u8,
    name: []const u8,
    category: []const u8 = "",
    priority: i32 = 0,
    queue_order: i64 = 0,
    /// The requesting client's user-agent (e.g. "Sonarr/4.0.5"), so the
    /// UI and webhook subscribers can attribute jobs to their *arr
    /// origin. Empty for manual uploads.
    source: []const u8 = "",
    /// The original NZB bytes. Persisted (small, tens of KB) so a job
    /// can be re-queued from history without the original file.
    nzb_blob: []const u8 = &.{},
    files: []const NewFileParams,
    /// Defers per-slice PAR2 recovery files (`<base>.vol###+##.par2`)
    /// until repair asks for them, i.e. constructs the Job with
    /// `fetch_recovery_vols = false`.
    defer_recovery_vols: bool = false,
};

/// Inputs for a Job being rebuilt from persistence.
pub const HydrateJobParams = struct {
    id: JobId = 0,
    nzb_hash: []const u8,
    name: []const u8,
    category: []const u8 = "",
    priority: i32 = 0,
    queue_order: i64 = 0,
    source: []const u8 = "",
    state: JobState = .queued,
    total_bytes: i64 = 0,
    done_bytes: i64 = 0,
    failed_bytes: i64 = 0,
    added_at: Timestamp = 0,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,
    error_msg: ?[]const u8 = null,
    nzb_blob: []const u8 = &.{},
    files: []const HydrateFileParams = &.{},
    fetch_recovery_vols: bool = true,
};

pub const InitError = error{
    NzbHashRequired,
    NameRequired,
    NoFiles,
} || Allocator.Error;

pub const SegmentError = error{
    /// The id is not part of this aggregate.
    SegmentNotInJob,
    /// Dispatch was attempted on a segment that is not `pending`.
    SegmentNotPending,
} || Allocator.Error;

pub const RecoveryVolsError = error{
    /// `fetch_recovery_vols` is already true.
    RecoveryVolsAlreadyRequested,
    /// Nothing to fetch — no recovery-vol segment is still pending.
    NoDeferredRecoveryVols,
    /// The Job already reached a terminal state.
    JobTerminal,
} || Allocator.Error;

/// Counts returned by `recountUnresolved`.
pub const UnresolvedCounts = struct {
    non_recovery_vol: usize,
    recovery_vol: usize,
};

/// Duplicates a string, treating empty as "nothing to allocate". Keeps
/// zero-length allocations out of the aggregate entirely, which makes
/// the partially-constructed states that `errdefer j.deinit()` has to
/// survive trivially safe.
fn dupeStr(allocator: Allocator, s: []const u8) Allocator.Error![]const u8 {
    if (s.len == 0) return "";
    return allocator.dupe(u8, s);
}

pub const Job = struct {
    /// Owns every allocation reachable from this aggregate.
    allocator: Allocator,

    /// Assigned by the persistence layer on INSERT; 0 until then.
    id: JobId = 0,
    /// Owned. All four are immutable for the aggregate's lifetime.
    nzb_hash: []const u8 = "",
    name: []const u8 = "",
    category: []const u8 = "",
    source: []const u8 = "",

    priority: i32 = 0,
    queue_order: i64 = 0,
    /// Read-only from outside: transition through the `mark*` methods.
    state: JobState = .queued,

    total_bytes: i64 = 0,
    done_bytes: i64 = 0,
    failed_bytes: i64 = 0,

    added_at: Timestamp = 0,
    started_at: ?Timestamp = null,
    finished_at: ?Timestamp = null,

    /// Owned; null when no failure has been recorded.
    error_msg: ?[]const u8 = null,
    /// Owned copy of the original NZB bytes.
    nzb_blob: []const u8 = "",

    /// Gates whether the orchestrator picks up recovery-volume PAR2
    /// files during normal download. While false, those files' segments
    /// are hidden from `pendingSegments` and excluded from the resolved
    /// check, so `JobDownloadComplete` fires once the data + index PAR2
    /// have landed. The repair worker flips it via
    /// `requestRecoveryVols` when reconstruction is short of slices.
    fetch_recovery_vols: bool = true,

    /// Owned. Allocated once at construction and never resized, so
    /// `*File` / `*Segment` pointers into the tree stay valid for the
    /// aggregate's lifetime — that is what lets `seg_index` cache them.
    files: []File = &.{},

    /// O(1) `segmentById`. Built lazily because segment ids only become
    /// real after the repository's INSERT, and the orchestrator runs
    /// many concurrent `segmentById` calls per Job — the original linear
    /// scan showed up plainly in production profiles.
    ///
    /// Capacity for every segment is reserved at construction, so both
    /// the build and the invalidate-and-rebuild are allocation-free and
    /// cannot fail. Duplicate ids (all-zero before INSERT) only shrink
    /// the entry count, never exceed the reservation.
    seg_index: std.AutoHashMapUnmanaged(SegmentId, SegmentRef) = .empty,
    seg_index_valid: bool = false,

    /// Segments in non-recovery-vol files that have NOT reached a
    /// terminal state, and the same for recovery-vol files. See the
    /// module comment for why there are two.
    unresolved_non_recovery_vol: usize = 0,
    unresolved_recovery_vol: usize = 0,

    /// Set whenever a field belonging to the "full UPDATE jobs" row has
    /// changed since the last save — anything other than the running
    /// `done_bytes` / `failed_bytes` counters. The orchestrator's flush
    /// path uses it to choose between a full save and a cheap
    /// counters-only update, which is the common steady state during an
    /// active download.
    state_dirty: bool = false,

    /// Events recorded but not yet pulled. `deinit` frees any left
    /// here, including the strings they own.
    event_log: std.ArrayList(Event) = .empty,

    /// Constructs a fresh queued Job and records `JobCreated` with a
    /// zero id; `setId` patches that event once the row exists.
    pub fn init(allocator: Allocator, p: NewJobParams, now: Timestamp) InitError!Job {
        if (p.nzb_hash.len == 0) return error.NzbHashRequired;
        if (p.name.len == 0) return error.NameRequired;
        if (p.files.len == 0) return error.NoFiles;

        var j = Job{
            .allocator = allocator,
            .state = .queued,
            .added_at = now,
            .fetch_recovery_vols = !p.defer_recovery_vols,
        };
        errdefer j.deinit();

        j.nzb_hash = try dupeStr(allocator, p.nzb_hash);
        j.name = try dupeStr(allocator, p.name);
        j.category = try dupeStr(allocator, p.category);
        j.source = try dupeStr(allocator, p.source);
        j.nzb_blob = try dupeStr(allocator, p.nzb_blob);
        j.priority = p.priority;
        j.queue_order = p.queue_order;

        try j.buildFiles(NewFileParams, p.files);

        // Every freshly-constructed segment is pending (non-terminal),
        // so the initial counts are just the segment counts bucketed by
        // recovery-vol status.
        for (j.files) |*f| {
            j.total_bytes += f.size_bytes;
            if (f.is_recovery_vol) {
                j.unresolved_recovery_vol += f.segments.len;
            } else {
                j.unresolved_non_recovery_vol += f.segments.len;
            }
        }

        try j.event_log.append(allocator, .{ .job_created = .{
            .id = 0,
            .name = j.name,
            .category = j.category,
            .total_bytes = j.total_bytes,
            .at = now,
        } });
        return j;
    }

    /// Reconstructs a Job from persistence. Emits no events and marks
    /// nothing dirty — the aggregate is by definition in sync with the
    /// row it came from.
    pub fn hydrate(allocator: Allocator, p: HydrateJobParams) Allocator.Error!Job {
        var j = Job{
            .allocator = allocator,
            .id = p.id,
            .priority = p.priority,
            .queue_order = p.queue_order,
            .state = p.state,
            .total_bytes = p.total_bytes,
            .done_bytes = p.done_bytes,
            .failed_bytes = p.failed_bytes,
            .added_at = p.added_at,
            .started_at = p.started_at,
            .finished_at = p.finished_at,
            .fetch_recovery_vols = p.fetch_recovery_vols,
        };
        errdefer j.deinit();

        j.nzb_hash = try dupeStr(allocator, p.nzb_hash);
        j.name = try dupeStr(allocator, p.name);
        j.category = try dupeStr(allocator, p.category);
        j.source = try dupeStr(allocator, p.source);
        j.nzb_blob = try dupeStr(allocator, p.nzb_blob);
        if (p.error_msg) |msg| j.error_msg = try dupeStr(allocator, msg);

        try j.buildFiles(HydrateFileParams, p.files);

        // Seed the resolved counters from the loaded states so a Job
        // resurrected mid-download answers `allSegmentsResolved`
        // correctly without ever scanning again.
        const counts = j.recountUnresolved();
        j.unresolved_non_recovery_vol = counts.non_recovery_vol;
        j.unresolved_recovery_vol = counts.recovery_vol;
        return j;
    }

    /// Copies the file/segment tree out of either params flavour. The
    /// two differ only in the persisted fields, which `@hasField`
    /// selects at comptime — one copy of the ownership-critical code is
    /// worth the small amount of comptime.
    fn buildFiles(self: *Job, comptime FP: type, params: []const FP) Allocator.Error!void {
        const a = self.allocator;
        const hydrating = @hasField(FP, "state");

        self.files = try a.alloc(File, params.len);
        // Skeletons first: `errdefer j.deinit()` in the callers may run
        // at any point below, and it walks the whole slice.
        for (self.files) |*f| f.* = .{ .filename = "" };

        var total_segments: usize = 0;
        for (params, self.files) |p, *f| {
            f.filename = try dupeStr(a, p.filename);
            f.poster = try dupeStr(a, p.poster);
            f.size_bytes = p.size_bytes;
            f.is_par2 = p.is_par2;
            f.is_recovery_vol = p.is_recovery_vol;

            if (p.groups.len != 0) {
                const groups = try a.alloc([]const u8, p.groups.len);
                @memset(groups, "");
                f.groups = groups;
                for (p.groups, groups) |src, *dst| dst.* = try dupeStr(a, src);
            }

            const segs = try a.alloc(Segment, p.segments.len);
            for (segs) |*s| s.* = .{ .message_id = "" };
            f.segments = segs;
            for (p.segments, segs) |sp, *s| {
                s.message_id = try dupeStr(a, sp.message_id);
                s.seq_index = sp.seq_index;
                s.bytes = sp.bytes;
                if (comptime hydrating) {
                    s.id = sp.id;
                    s.file_id = sp.file_id;
                    s.state = sp.state;
                    s.attempts = sp.attempts;
                    s.file_offset = sp.file_offset;
                    s.next_retry_at = sp.next_retry_at;
                    if (sp.last_error) |le| {
                        const dup = try dupeStr(a, le);
                        s.last_error = if (dup.len == 0) null else dup;
                    }
                }
            }

            if (comptime hydrating) {
                f.id = p.id;
                f.job_id = p.job_id;
                f.state = p.state;
                f.segment_count = p.segment_count;
                f.segments_done = p.segments_done;
            } else {
                f.state = .pending;
                f.segment_count = @intCast(p.segments.len);
            }
            total_segments += p.segments.len;
        }

        try self.seg_index.ensureTotalCapacity(a, @intCast(total_segments));
    }

    /// Releases the whole tree. Any event still queued on the aggregate
    /// is freed too, so a Job that is dropped without a final
    /// `pullEvents` does not leak.
    pub fn deinit(self: *Job) void {
        const a = self.allocator;
        for (self.event_log.items) |e| e.deinit(a);
        self.event_log.deinit(a);
        self.seg_index.deinit(a);

        for (self.files) |*f| {
            for (f.segments) |*s| {
                a.free(s.message_id);
                if (s.last_error) |le| a.free(le);
            }
            a.free(f.segments);
            a.free(f.filename);
            a.free(f.poster);
            for (f.groups) |g| a.free(g);
            a.free(f.groups);
        }
        a.free(self.files);

        a.free(self.nzb_hash);
        a.free(self.name);
        a.free(self.category);
        a.free(self.source);
        a.free(self.nzb_blob);
        if (self.error_msg) |msg| a.free(msg);
        self.* = undefined;
    }

    // ---- accessors -------------------------------------------------

    /// Go returned "" for "no error"; same shape here.
    pub fn errorMsg(self: *const Job) []const u8 {
        return self.error_msg orelse "";
    }

    /// Reports whether any non-counter field changed since the last
    /// full save. The counters change every flush during an active
    /// download and are persisted separately regardless of this bit.
    pub fn isStateDirty(self: *const Job) bool {
        return self.state_dirty;
    }

    /// Called by the repository after a successful full save.
    pub fn clearStateDirty(self: *Job) void {
        self.state_dirty = false;
    }

    fn markStateDirty(self: *Job) void {
        self.state_dirty = true;
    }

    /// Called by the repository after a successful insert: patches the
    /// pending `JobCreated` event with the real id and pushes the id
    /// down into the children.
    pub fn setId(self: *Job, id: JobId) void {
        self.id = id;
        for (self.event_log.items) |*e| switch (e.*) {
            .job_created => |*jc| {
                if (jc.id == 0) jc.id = id;
            },
            else => {},
        };
        for (self.files) |*f| f.setJobId(id);
    }

    /// Rewrites the queue-order position. No event: reorder is a
    /// UI-driven concern and the orchestrator picks the new ordering up
    /// on its next dispatch tick. Terminal jobs are not filtered here —
    /// the queue service is responsible for not feeding terminal ids in.
    pub fn setQueueOrder(self: *Job, order: i64) void {
        if (self.queue_order == order) return;
        self.queue_order = order;
        self.markStateDirty();
    }

    /// Returns the recorded events and clears the aggregate's log.
    /// Ownership of the slice *and* of the strings the events own moves
    /// to the caller, which must release both with
    /// `events.deinitAll`. Borrowed strings inside the events stay
    /// valid only until this Job is deinit'd.
    pub fn pullEvents(self: *Job) Allocator.Error![]Event {
        return self.event_log.toOwnedSlice(self.allocator);
    }

    fn record(self: *Job, e: Event) Allocator.Error!void {
        return self.event_log.append(self.allocator, e);
    }

    // ---- job lifecycle ---------------------------------------------

    /// queued → downloading. Returns false (no event, no mutation) when
    /// the job has already started or moved past that point — the Go
    /// version documented this as idempotent, and the orchestrator
    /// relies on being able to call it unconditionally.
    pub fn markStarted(self: *Job, now: Timestamp) Allocator.Error!bool {
        if (self.state != .queued) return false;
        self.state = .downloading;
        self.started_at = now;
        self.markStateDirty();
        try self.record(.{ .job_started = .{ .id = self.id, .at = now } });
        return true;
    }

    /// downloading|queued → paused. The orchestrator observes
    /// `JobPaused` and stops dispatching segments. Returns false when
    /// the job is already paused or terminal.
    pub fn pause(self: *Job, now: Timestamp) Allocator.Error!bool {
        switch (self.state) {
            .queued, .downloading => {},
            else => return false,
        }
        self.state = .paused;
        self.markStateDirty();
        try self.record(.{ .job_paused = .{ .id = self.id, .at = now } });
        return true;
    }

    /// paused → downloading, or → queued when no work had started yet.
    /// The orchestrator observes `JobResumed` and re-enables segment
    /// dispatch. Returns false when the job is not paused.
    ///
    /// Trailing underscore because `resume` is a Zig keyword; the
    /// alternative (`@"resume"`) would push the quoting onto every call
    /// site.
    pub fn resume_(self: *Job, now: Timestamp) Allocator.Error!bool {
        if (self.state != .paused) return false;
        self.state = if (self.started_at == null) .queued else .downloading;
        self.markStateDirty();
        try self.record(.{ .job_resumed = .{ .id = self.id, .at = now } });
        return true;
    }

    /// Parks the job because no usable NNTP server is configured. The
    /// runner exits on this transition; the orchestrator's
    /// server-added handler calls `resumeFromWait` once one appears.
    /// Returns false outside queued/downloading.
    pub fn markWaitingForServer(self: *Job, reason: []const u8, now: Timestamp) Allocator.Error!bool {
        switch (self.state) {
            .queued, .downloading => {},
            else => return false,
        }
        const owned = try dupeStr(self.allocator, reason);
        errdefer self.allocator.free(owned);
        try self.event_log.ensureUnusedCapacity(self.allocator, 1);
        self.state = .waiting_for_server;
        self.markStateDirty();
        self.event_log.appendAssumeCapacity(.{ .job_waiting_for_server = .{
            .job_id = self.id,
            .reason = owned,
            .at = now,
        } });
        return true;
    }

    /// Undoes `markWaitingForServer` when a usable server appears. Goes
    /// back to queued — the orchestrator's normal path flips to
    /// downloading once dispatch starts — and emits `JobResumed` so
    /// existing SSE / runner subscribers pick it up without a new topic.
    pub fn resumeFromWait(self: *Job, now: Timestamp) Allocator.Error!bool {
        if (self.state != .waiting_for_server) return false;
        self.state = .queued;
        self.markStateDirty();
        try self.record(.{ .job_resumed = .{ .id = self.id, .at = now } });
        return true;
    }

    /// Records `JobRemoved` so the bus delivers it in the same
    /// transaction as the DELETE the application service is about to
    /// issue.
    pub fn markRemoved(self: *Job, now: Timestamp) Allocator.Error!void {
        try self.record(.{ .job_removed = .{ .id = self.id, .at = now } });
    }

    /// Finalises the Job after delivery moved its files into
    /// `complete/`. Returns false if it was already completed.
    pub fn markCompleted(self: *Job, now: Timestamp) Allocator.Error!bool {
        if (self.state == .completed) return false;
        self.state = .completed;
        self.finished_at = now;
        self.markStateDirty();
        try self.record(.{ .job_completed = .{ .job_id = self.id, .at = now } });
        return true;
    }

    /// Terminal failure with a reason. Returns false if already failed.
    pub fn markFailed(self: *Job, reason: []const u8, now: Timestamp) Allocator.Error!bool {
        if (self.state == .failed) return false;
        const owned = try dupeStr(self.allocator, reason);
        errdefer self.allocator.free(owned);
        try self.event_log.ensureUnusedCapacity(self.allocator, 1);
        try self.setErrorMsg(reason);
        self.state = .failed;
        self.finished_at = now;
        self.markStateDirty();
        self.event_log.appendAssumeCapacity(.{ .job_failed = .{
            .job_id = self.id,
            .err = owned,
            .at = now,
        } });
        return true;
    }

    /// Transitions to failed when the failed-bytes ratio reaches
    /// `threshold` (0.0–1.0 exclusive; outside that range disables the
    /// check). SABnzbd's `fail_hopeless`: stop burning bandwidth on a
    /// release already beyond PAR2's reach. Returns true if it fired.
    ///
    /// Download phase only — once `download_complete` is reached the
    /// verify pipeline decides fate.
    pub fn abortIfHopeless(self: *Job, threshold: f64, now: Timestamp) Allocator.Error!bool {
        if (threshold <= 0 or threshold >= 1) return false;
        switch (self.state) {
            .queued, .downloading, .waiting_for_server => {},
            else => return false,
        }
        if (self.total_bytes <= 0) return false;
        const ratio = @as(f64, @floatFromInt(self.failed_bytes)) / @as(f64, @floatFromInt(self.total_bytes));
        if (ratio < threshold) return false;

        const a = self.allocator;
        // `+ 0.5` then truncate: round-half-up, matching the Go form.
        const pct_missing: i64 = @intFromFloat(ratio * 100 + 0.5);
        const pct_threshold: i64 = @intFromFloat(threshold * 100 + 0.5);
        const reason = try std.fmt.allocPrint(
            a,
            "download aborted: {d}% missing exceeds {d}% threshold",
            .{ pct_missing, pct_threshold },
        );
        defer a.free(reason);

        try self.event_log.ensureUnusedCapacity(a, 2);
        const err_a = try dupeStr(a, reason);
        errdefer a.free(err_a);
        const err_b = try dupeStr(a, reason);
        errdefer a.free(err_b);
        try self.setErrorMsg(reason);

        self.state = .failed;
        self.finished_at = now;
        self.markStateDirty();
        self.event_log.appendAssumeCapacity(.{ .job_download_failed = .{
            .job_id = self.id,
            .err = err_a,
            .at = now,
        } });
        self.event_log.appendAssumeCapacity(.{ .job_failed = .{
            .job_id = self.id,
            .err = err_b,
            .at = now,
        } });
        return true;
    }

    // ---- segment lookup --------------------------------------------

    /// Looks a segment up within the aggregate; null when the id is not
    /// part of this Job.
    ///
    /// The index is built on first use and stays valid until
    /// `rebuildSegmentIndex` invalidates it, which the repository must
    /// call after assigning fresh ids on the insert path. Neither the
    /// build nor the lookup allocates.
    pub fn segmentById(self: *Job, id: SegmentId) ?SegmentRef {
        if (!self.seg_index_valid) self.buildSegmentIndex();
        return self.seg_index.get(id);
    }

    /// Drops the `segmentById` cache so the next lookup rebuilds it.
    pub fn rebuildSegmentIndex(self: *Job) void {
        self.seg_index_valid = false;
    }

    fn buildSegmentIndex(self: *Job) void {
        self.seg_index.clearRetainingCapacity();
        for (self.files) |*f| {
            for (f.segments) |*s| {
                self.seg_index.putAssumeCapacity(s.id, .{ .file = f, .seg = s });
            }
        }
        self.seg_index_valid = true;
    }

    // ---- segment transitions ---------------------------------------

    /// Records a successful fetch + decode + write. `done_bytes` grows;
    /// if this was the file's last outstanding segment the file
    /// completes and emits `FileCompleted`; if it was the job's last
    /// unresolved segment the download phase closes.
    ///
    /// Idempotent for a segment already `done`.
    pub fn markSegmentDone(self: *Job, r: SegmentResult, now: Timestamp) SegmentError!void {
        const ref = self.segmentById(r.segment_id) orelse return error.SegmentNotInJob;
        const f = ref.file;
        const s = ref.seg;
        if (s.state == .done) return;

        // Room for every event this call can emit — SegmentCompleted,
        // FileCompleted, and the download-phase pair — reserved before
        // any mutation, so an OOM cannot leave the aggregate changed
        // with its events missing.
        try self.event_log.ensureUnusedCapacity(self.allocator, 4);

        // Defensive: only Done early-returns above, but a future
        // terminal state must not double-decrement the counter.
        const was_terminal = s.state.isTerminal();
        s.state = .done;
        if (!was_terminal) self.adjustUnresolved(f, -1);
        self.clearSegmentError(s);
        s.file_offset = r.file_offset;
        self.done_bytes += r.bytes_on_disk;
        f.segments_done += 1;

        self.event_log.appendAssumeCapacity(.{ .segment_completed = .{
            .job_id = self.id,
            .file_id = f.id,
            .segment_id = s.id,
            .bytes = r.bytes_on_disk,
            .at = now,
        } });

        if (f.segments_done >= f.segment_count and f.state != .complete) {
            f.state = .complete;
            self.event_log.appendAssumeCapacity(.{ .file_completed = .{
                .job_id = self.id,
                .file_id = f.id,
                .filename = f.filename,
                .at = now,
            } });
        }

        if (self.allSegmentsResolved()) try self.completeDownloadPhase(now);
    }

    /// Records "every server returned 430". Terminal for download
    /// purposes; PAR2 may still rescue the file. Idempotent for a
    /// segment that is already terminal.
    pub fn markSegmentMissing(self: *Job, seg_id: SegmentId, now: Timestamp) SegmentError!void {
        const ref = self.segmentById(seg_id) orelse return error.SegmentNotInJob;
        const f = ref.file;
        const s = ref.seg;
        if (s.state.isTerminal()) return;

        try self.event_log.ensureUnusedCapacity(self.allocator, 3);
        try self.setSegmentError(s, "article missing on all servers");
        s.state = .missing;
        self.adjustUnresolved(f, -1);
        self.failed_bytes += s.bytes;
        self.event_log.appendAssumeCapacity(.{ .segment_missing = .{
            .job_id = self.id,
            .file_id = f.id,
            .segment_id = s.id,
            .at = now,
        } });
        if (self.allSegmentsResolved()) try self.completeDownloadPhase(now);
    }

    /// Records a non-430 failure that exhausted the retry budget
    /// (network, parse, CRC). Terminal, like missing. Idempotent for a
    /// segment that is already terminal.
    pub fn markSegmentFailed(self: *Job, seg_id: SegmentId, err_msg: []const u8, now: Timestamp) SegmentError!void {
        const ref = self.segmentById(seg_id) orelse return error.SegmentNotInJob;
        const f = ref.file;
        const s = ref.seg;
        if (s.state.isTerminal()) return;

        try self.event_log.ensureUnusedCapacity(self.allocator, 3);
        const owned = try dupeStr(self.allocator, err_msg);
        errdefer self.allocator.free(owned);
        try self.setSegmentError(s, err_msg);

        s.state = .failed;
        self.adjustUnresolved(f, -1);
        self.failed_bytes += s.bytes;
        self.event_log.appendAssumeCapacity(.{ .segment_failed = .{
            .job_id = self.id,
            .file_id = f.id,
            .segment_id = s.id,
            .err = owned,
            .at = now,
        } });
        if (self.allSegmentsResolved()) try self.completeDownloadPhase(now);
    }

    /// Flips a pending segment to inflight and records the attempt.
    /// Errors when the segment is not pending — a double dispatch is a
    /// bug in the caller, not a no-op.
    pub fn markSegmentDispatched(self: *Job, seg_id: SegmentId, now: Timestamp) SegmentError!void {
        const ref = self.segmentById(seg_id) orelse return error.SegmentNotInJob;
        const s = ref.seg;
        if (s.state != .pending) return error.SegmentNotPending;

        try self.event_log.ensureUnusedCapacity(self.allocator, 1);
        s.state = .inflight;
        s.attempts += 1;
        self.event_log.appendAssumeCapacity(.{ .segment_dispatched = .{
            .job_id = self.id,
            .segment_id = s.id,
            .message_id = s.message_id,
            .attempt = s.attempts,
            .at = now,
        } });
    }

    /// Flips a segment back to pending with a future `next_retry_at`, so
    /// the poll loop skips it until `at` has passed — even across a
    /// restart. Used for transient failures (conn-limit, 5xx, network
    /// hiccup) that we would rather defer than charge to the segment's
    /// retry budget now.
    ///
    /// Terminal segments do not re-enter the retry queue; the call is a
    /// no-op for them (use `markSegmentMissing` / `markSegmentFailed`).
    pub fn markSegmentForRetry(self: *Job, seg_id: SegmentId, at: Timestamp, err_msg: []const u8) SegmentError!void {
        const ref = self.segmentById(seg_id) orelse return error.SegmentNotInJob;
        const s = ref.seg;
        if (s.state.isTerminal()) return;
        try self.setSegmentError(s, err_msg);
        s.state = .pending;
        s.next_retry_at = at;
        s.attempts += 1;
    }

    /// Resets segments left mid-fetch by a crash. No event — this is
    /// recovery hygiene, not a domain event. Inflight is non-terminal
    /// both before and after, so the counters do not move.
    pub fn resetInflightToPending(self: *Job) usize {
        var n: usize = 0;
        for (self.files) |*f| {
            for (f.segments) |*s| {
                if (s.state == .inflight) {
                    s.state = .pending;
                    n += 1;
                }
            }
        }
        return n;
    }

    /// Flips every failed/missing segment back to pending, clearing its
    /// error and attempt count so the orchestrator picks it up fresh.
    /// The operator-facing trigger is "the release was temporarily
    /// unavailable; try again".
    ///
    /// File state is left alone — the repo recomputes it from the
    /// segment counters on the next save. A terminated job is kicked
    /// back to queued so the orchestrator considers it again.
    ///
    /// Returns the number of segments reset. Allocation-free: it only
    /// releases strings.
    pub fn resetFailedToPending(self: *Job) usize {
        var n: usize = 0;
        for (self.files) |*f| {
            for (f.segments) |*s| {
                if (s.state == .failed or s.state == .missing) {
                    s.state = .pending;
                    // Terminal → non-terminal: back in the pending pool
                    // and counting as unresolved again.
                    self.adjustUnresolved(f, 1);
                    s.attempts = 0;
                    self.clearSegmentError(s);
                    n += 1;
                }
            }
        }
        switch (self.state) {
            .failed, .aborted => {
                self.state = .queued;
                self.finished_at = null;
                self.clearErrorMsg();
                self.markStateDirty();
            },
            else => {},
        }
        return n;
    }

    // ---- queries ---------------------------------------------------

    /// Segments awaiting dispatch whose retry window has elapsed.
    /// Deferred segments (a `next_retry_at` still in the future) stay
    /// `pending` in the database but are hidden here, so the worker pool
    /// does not burn capacity re-fetching articles a server just
    /// rejected. `nextRetryReadyAt` answers "is there deferred work,
    /// and when".
    ///
    /// While `fetch_recovery_vols` is false, recovery-vol files'
    /// segments are hidden entirely.
    ///
    /// The returned slice is the caller's to free; the `*Segment`
    /// pointers in it stay valid for the aggregate's lifetime.
    pub fn pendingSegments(self: *const Job, allocator: Allocator, now: Timestamp) Allocator.Error![]*Segment {
        var out: std.ArrayList(*Segment) = .empty;
        errdefer out.deinit(allocator);
        for (self.files) |*f| {
            if (f.is_recovery_vol and !self.fetch_recovery_vols) continue;
            for (f.segments) |*s| {
                if (s.isReadyAt(now)) try out.append(allocator, s);
            }
        }
        return out.toOwnedSlice(allocator);
    }

    /// The earliest instant at which a currently deferred pending
    /// segment becomes ready, or null when nothing is deferred — which
    /// the orchestrator reads as "no deferred work left, you can exit".
    /// Recovery-vol gating mirrors `pendingSegments` so the runner never
    /// sleeps waiting for vols it is not supposed to fetch.
    pub fn nextRetryReadyAt(self: *const Job, now: Timestamp) ?Timestamp {
        var earliest: ?Timestamp = null;
        for (self.files) |*f| {
            if (f.is_recovery_vol and !self.fetch_recovery_vols) continue;
            for (f.segments) |*s| {
                if (!s.isDeferredAt(now)) continue;
                const at = s.next_retry_at.?;
                if (earliest == null or at < earliest.?) earliest = at;
            }
        }
        return earliest;
    }

    /// Whether every segment has reached a terminal state. Two integer
    /// comparisons — see the module comment. Recovery-vol segments are
    /// ignored while the job has not opted in to fetching them, which
    /// mirrors `pendingSegments`.
    pub fn allSegmentsResolved(self: *const Job) bool {
        if (self.unresolved_non_recovery_vol > 0) return false;
        if (self.fetch_recovery_vols and self.unresolved_recovery_vol > 0) return false;
        return true;
    }

    /// Recomputes the unresolved counters the slow way, O(files ×
    /// segments). Not used on any hot path: it seeds `hydrate`, and it
    /// is the oracle the counter invariant is tested against. A repo or
    /// debug build can assert the two agree.
    pub fn recountUnresolved(self: *const Job) UnresolvedCounts {
        var counts = UnresolvedCounts{ .non_recovery_vol = 0, .recovery_vol = 0 };
        for (self.files) |*f| {
            for (f.segments) |*s| {
                if (s.state.isTerminal()) continue;
                if (f.is_recovery_vol) {
                    counts.recovery_vol += 1;
                } else {
                    counts.non_recovery_vol += 1;
                }
            }
        }
        return counts;
    }

    /// Moves the per-bucket unresolved counter when a segment in `f`
    /// crosses the terminal boundary: -1 on becoming terminal, +1 on
    /// becoming non-terminal again. Signed on purpose — an underflow
    /// here means a transition was double-counted, and the checked
    /// subtraction turns that silent corruption into a crash in debug
    /// builds.
    fn adjustUnresolved(self: *Job, f: *const File, delta: i2) void {
        const bucket = if (f.is_recovery_vol) &self.unresolved_recovery_vol else &self.unresolved_non_recovery_vol;
        if (delta < 0) {
            bucket.* -= 1;
        } else {
            bucket.* += 1;
        }
    }

    /// Whether the orchestrator should pick up recovery-volume
    /// segments. Set from the "defer recovery vols" knob (inverted) at
    /// creation; flipped to true by `requestRecoveryVols`.
    pub fn fetchRecoveryVols(self: *const Job) bool {
        return self.fetch_recovery_vols;
    }

    /// Whether the Job still has recovery-vol segments that were never
    /// fetched because `fetch_recovery_vols` is false. The repair worker
    /// uses it to decide whether on-demand fetching is even possible.
    pub fn hasDeferredRecoveryVols(self: *const Job) bool {
        if (self.fetch_recovery_vols) return false;
        for (self.files) |*f| {
            if (!f.is_recovery_vol) continue;
            for (f.segments) |*s| {
                if (s.state == .pending) return true;
            }
        }
        return false;
    }

    /// Flips `fetch_recovery_vols` on and reopens the download phase so
    /// the orchestrator picks up the now-visible segments. Emits
    /// `RecoveryVolsRequested`. Errors when there is nothing to fetch
    /// (the caller should fail the repair outright) or when the job is
    /// already terminal.
    pub fn requestRecoveryVols(self: *Job, now: Timestamp) RecoveryVolsError!void {
        if (self.fetch_recovery_vols) return error.RecoveryVolsAlreadyRequested;
        if (!self.hasDeferredRecoveryVols()) return error.NoDeferredRecoveryVols;
        switch (self.state) {
            .completed, .failed, .aborted => return error.JobTerminal,
            else => {},
        }
        try self.event_log.ensureUnusedCapacity(self.allocator, 1);

        self.fetch_recovery_vols = true;
        self.markStateDirty();
        // Reopen the active phase. Anything past download_complete
        // (verifying / repairing) goes back to downloading; earlier
        // states keep theirs.
        switch (self.state) {
            .download_complete, .verifying, .repairing => self.state = .downloading,
            else => {},
        }
        self.event_log.appendAssumeCapacity(.{ .recovery_vols_requested = .{
            .job_id = self.id,
            .at = now,
        } });
    }

    /// Closes the download phase.
    ///
    /// Nothing through at all means the download failed outright — PAR2
    /// cannot reconstruct from zero bytes — so the job goes straight to
    /// failed rather than sitting in `download_complete` looking
    /// misleadingly green. That covers "no enabled pools" and
    /// "every segment missing".
    ///
    /// Otherwise the verify worker decides fate from
    /// `download_complete`.
    fn completeDownloadPhase(self: *Job, now: Timestamp) Allocator.Error!void {
        const a = self.allocator;
        if (self.done_bytes == 0) {
            const reason = "download failed: no segments retrieved";
            try self.event_log.ensureUnusedCapacity(a, 2);
            const err_a = try dupeStr(a, reason);
            errdefer a.free(err_a);
            const err_b = try dupeStr(a, reason);
            errdefer a.free(err_b);
            try self.setErrorMsg(reason);

            self.state = .failed;
            self.finished_at = now;
            self.markStateDirty();
            self.event_log.appendAssumeCapacity(.{ .job_download_failed = .{
                .job_id = self.id,
                .err = err_a,
                .at = now,
            } });
            self.event_log.appendAssumeCapacity(.{ .job_failed = .{
                .job_id = self.id,
                .err = err_b,
                .at = now,
            } });
            return;
        }
        try self.event_log.ensureUnusedCapacity(a, 1);
        self.state = .download_complete;
        self.markStateDirty();
        self.event_log.appendAssumeCapacity(.{ .job_download_complete = .{
            .job_id = self.id,
            .missing_segments = self.countMissingSegments(),
            .at = now,
        } });
    }

    /// Segments that resolved to missing or failed. Only called once, at
    /// the end of the download phase, so the scan is free in practice.
    pub fn countMissingSegments(self: *const Job) u32 {
        var n: u32 = 0;
        for (self.files) |*f| {
            for (f.segments) |*s| {
                if (s.state == .missing or s.state == .failed) n += 1;
            }
        }
        return n;
    }

    // ---- owned-string plumbing -------------------------------------
    //
    // Job owns every string in the tree, so replacing one means freeing
    // the previous value. The new copy is made first: that keeps the
    // operation safe even if the caller passes a slice of the string
    // being replaced, and leaves the aggregate untouched on OOM.

    fn setSegmentError(self: *Job, s: *Segment, msg: []const u8) Allocator.Error!void {
        const dup = try dupeStr(self.allocator, msg);
        self.clearSegmentError(s);
        s.last_error = if (dup.len == 0) null else dup;
    }

    fn clearSegmentError(self: *Job, s: *Segment) void {
        if (s.last_error) |le| self.allocator.free(le);
        s.last_error = null;
    }

    fn setErrorMsg(self: *Job, msg: []const u8) Allocator.Error!void {
        const dup = try dupeStr(self.allocator, msg);
        self.clearErrorMsg();
        self.error_msg = if (dup.len == 0) null else dup;
    }

    fn clearErrorMsg(self: *Job) void {
        if (self.error_msg) |msg| self.allocator.free(msg);
        self.error_msg = null;
    }
};

// ---------------------------------------------------------------------
// Tests — a translation of internal/domain/download/job_test.go, plus
// the counter-invariant coverage the O(1) resolved check needs.
// ---------------------------------------------------------------------

const testing = std.testing;

/// Mirrors the Go `mkJob` helper: two files, three segments, 1200 bytes
/// total, id 7.
fn mkJob(allocator: Allocator) !Job {
    var j = try Job.init(allocator, .{
        .nzb_hash = "abcdef",
        .name = "release",
        .files = &.{
            .{
                .filename = "file.r00",
                .size_bytes = 1000,
                .segments = &.{
                    .{ .seq_index = 1, .message_id = "msg1@host", .bytes = 600 },
                    .{ .seq_index = 2, .message_id = "msg2@host", .bytes = 400 },
                },
            },
            .{
                .filename = "file.par2",
                .size_bytes = 200,
                .is_par2 = true,
                .segments = &.{
                    .{ .seq_index = 1, .message_id = "par2@host", .bytes = 200 },
                },
            },
        },
    }, 1);
    j.setId(7);
    return j;
}

/// What the repository's insert path does: hand out ids, then tell the
/// aggregate its index is stale.
fn assignSegmentIds(j: *Job, first: SegmentId) void {
    var next = first;
    for (j.files) |*f| {
        for (f.segments) |*s| {
            s.setId(next);
            next += 1;
        }
    }
    j.rebuildSegmentIndex();
}

fn expectKind(kind: events.Kind, e: Event) !void {
    try testing.expectEqual(kind, std.meta.activeTag(e));
}

fn drainEvents(j: *Job) !void {
    events.deinitAll(j.allocator, try j.pullEvents());
}

/// Asserts the O(1) counters agree with a full recount. This is the
/// invariant a fast counter can silently break.
fn expectCountersConsistent(j: *const Job) !void {
    const counts = j.recountUnresolved();
    try testing.expectEqual(counts.non_recovery_vol, j.unresolved_non_recovery_vol);
    try testing.expectEqual(counts.recovery_vol, j.unresolved_recovery_vol);
}

test "init rejects incomplete params" {
    const a = testing.allocator;
    const one_file: []const NewFileParams = &.{
        .{ .filename = "f.bin", .segments = &.{.{ .message_id = "m@host" }} },
    };
    try testing.expectError(error.NzbHashRequired, Job.init(a, .{
        .nzb_hash = "",
        .name = "release",
        .files = one_file,
    }, 1));
    try testing.expectError(error.NameRequired, Job.init(a, .{
        .nzb_hash = "abc",
        .name = "",
        .files = one_file,
    }, 1));
    try testing.expectError(error.NoFiles, Job.init(a, .{
        .nzb_hash = "abc",
        .name = "release",
        .files = &.{},
    }, 1));
}

test "init emits JobCreated and setId patches it" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);

    try testing.expectEqual(@as(usize, 1), batch.len);
    try expectKind(.job_created, batch[0]);
    const jc = batch[0].job_created;
    try testing.expectEqual(@as(JobId, 7), jc.id);
    try testing.expectEqual(@as(i64, 1200), jc.total_bytes);
    try testing.expectEqualStrings("release", jc.name);
    try testing.expectEqual(@as(i64, 1200), j.total_bytes);
    try testing.expectEqual(JobState.queued, j.state);
    // The params were stack temporaries; the Job kept its own copies.
    try testing.expectEqualStrings("file.r00", j.files[0].filename);
    try testing.expectEqualStrings("msg1@host", j.files[0].segments[0].message_id);
    try expectCountersConsistent(&j);
}

test "markSegmentDispatched flips pending to inflight once" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    const pending = try j.pendingSegments(testing.allocator, 1000);
    defer testing.allocator.free(pending);
    try testing.expectEqual(@as(usize, 3), pending.len);

    try j.markSegmentDispatched(100, 1000);
    try testing.expectEqual(SegmentState.inflight, j.files[0].segments[0].state);
    try testing.expectEqual(@as(i32, 1), j.files[0].segments[0].attempts);

    // Re-dispatching the same segment is an error, not a no-op.
    try testing.expectError(error.SegmentNotPending, j.markSegmentDispatched(100, 1000));
    // An id from another aggregate is likewise rejected.
    try testing.expectError(error.SegmentNotInJob, j.markSegmentDispatched(999, 1000));

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    try testing.expectEqual(@as(usize, 1), batch.len);
    try testing.expectEqualStrings("msg1@host", batch[0].segment_dispatched.message_id);
    try testing.expectEqual(@as(i32, 1), batch[0].segment_dispatched.attempt);
    // Inflight is non-terminal, so nothing moved.
    try expectCountersConsistent(&j);
}

test "markSegmentDone updates progress and closes the phases" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    const now: Timestamp = 5000;
    try j.markSegmentDone(.{ .segment_id = 100, .bytes_on_disk = 600 }, now);
    try testing.expectEqual(@as(i64, 600), j.done_bytes);
    // File 0 has two segments; one done → not complete yet.
    try testing.expect(j.files[0].state != .complete);

    try j.markSegmentDone(.{ .segment_id = 101, .bytes_on_disk = 400 }, now);
    try testing.expectEqual(FileState.complete, j.files[0].state);
    // The par2 file is still pending → the job is not complete.
    try testing.expect(j.state != .download_complete);

    try j.markSegmentDone(.{ .segment_id = 102, .bytes_on_disk = 200 }, now);
    try testing.expectEqual(JobState.download_complete, j.state);
    try testing.expect(j.allSegmentsResolved());
    try expectCountersConsistent(&j);
    try testing.expect(j.isStateDirty());

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    var completed: usize = 0;
    var files_done: usize = 0;
    var phase_done: usize = 0;
    for (batch) |e| switch (e) {
        .segment_completed => completed += 1,
        .file_completed => files_done += 1,
        .job_download_complete => |d| {
            phase_done += 1;
            try testing.expectEqual(@as(u32, 0), d.missing_segments);
        },
        else => {},
    };
    try testing.expectEqual(@as(usize, 3), completed);
    try testing.expectEqual(@as(usize, 2), files_done);
    try testing.expectEqual(@as(usize, 1), phase_done);

    // Re-reporting a done segment is idempotent: no double counting.
    try j.markSegmentDone(.{ .segment_id = 102, .bytes_on_disk = 200 }, now);
    try testing.expectEqual(@as(i64, 1200), j.done_bytes);
    try drainEvents(&j);
}

test "markSegmentMissing still completes the job" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    const now: Timestamp = 7000;
    try j.markSegmentDone(.{ .segment_id = 100, .bytes_on_disk = 600 }, now);
    try j.markSegmentDone(.{ .segment_id = 101, .bytes_on_disk = 400 }, now);
    try j.markSegmentMissing(102, now);

    try testing.expectEqual(JobState.download_complete, j.state);
    try testing.expectEqual(@as(i64, 200), j.failed_bytes);
    try expectCountersConsistent(&j);

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    var saw_phase = false;
    for (batch) |e| switch (e) {
        .job_download_complete => |d| {
            saw_phase = true;
            try testing.expectEqual(@as(u32, 1), d.missing_segments);
        },
        else => {},
    };
    try testing.expect(saw_phase);
}

test "markSegmentFailed records the error and is idempotent" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    try j.markSegmentFailed(100, "crc mismatch", 9000);
    const s = j.segmentById(100).?.seg;
    try testing.expectEqual(SegmentState.failed, s.state);
    try testing.expectEqualStrings("crc mismatch", s.lastError());
    try testing.expectEqual(@as(i64, 600), j.failed_bytes);

    // Already terminal → no second event, no second byte count.
    try j.markSegmentFailed(100, "other", 9001);
    try testing.expectEqual(@as(i64, 600), j.failed_bytes);
    try testing.expectEqualStrings("crc mismatch", s.lastError());
    try expectCountersConsistent(&j);

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    try testing.expectEqual(@as(usize, 1), batch.len);
    try testing.expectEqualStrings("crc mismatch", batch[0].segment_failed.err);
}

test "every segment missing fails the job outright" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    try j.markSegmentMissing(100, 1);
    try j.markSegmentMissing(101, 2);
    try j.markSegmentMissing(102, 3);

    try testing.expectEqual(JobState.failed, j.state);
    try testing.expectEqual(@as(?Timestamp, 3), j.finished_at);
    try testing.expectEqualStrings("download failed: no segments retrieved", j.errorMsg());

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    var saw_download_failed = false;
    var saw_failed = false;
    for (batch) |e| switch (e) {
        .job_download_failed => saw_download_failed = true,
        .job_failed => saw_failed = true,
        else => {},
    };
    try testing.expect(saw_download_failed);
    try testing.expect(saw_failed);
}

test "pendingSegments skips deferred recovery vols" {
    var j = try Job.init(testing.allocator, .{
        .nzb_hash = "deadbeef",
        .name = "release",
        .files = &.{
            .{
                .filename = "release.bin",
                .size_bytes = 1000,
                .segments = &.{.{ .seq_index = 1, .message_id = "data@host", .bytes = 1000 }},
            },
            .{
                .filename = "release.par2",
                .size_bytes = 200,
                .is_par2 = true,
                .segments = &.{.{ .seq_index = 1, .message_id = "idx@host", .bytes = 200 }},
            },
            .{
                .filename = "release.vol000+01.par2",
                .size_bytes = 500,
                .is_par2 = true,
                .is_recovery_vol = true,
                .segments = &.{.{ .seq_index = 1, .message_id = "vol0@host", .bytes = 500 }},
            },
            .{
                .filename = "release.vol001+02.par2",
                .size_bytes = 500,
                .is_par2 = true,
                .is_recovery_vol = true,
                .segments = &.{.{ .seq_index = 1, .message_id = "vol1@host", .bytes = 500 }},
            },
        },
        .defer_recovery_vols = true,
    }, 1);
    defer j.deinit();
    try drainEvents(&j);

    const pending = try j.pendingSegments(testing.allocator, 1000);
    defer testing.allocator.free(pending);
    try testing.expectEqual(@as(usize, 2), pending.len);
    try testing.expect(j.hasDeferredRecoveryVols());

    // The two vol segments are counted, but in the bucket the gated
    // resolved check ignores.
    try testing.expectEqual(@as(usize, 2), j.unresolved_recovery_vol);
    try testing.expectEqual(@as(usize, 2), j.unresolved_non_recovery_vol);
    try expectCountersConsistent(&j);
}

test "requestRecoveryVols reveals the hidden segments" {
    var j = try Job.init(testing.allocator, .{
        .nzb_hash = "deadbeef",
        .name = "release",
        .files = &.{
            .{
                .filename = "release.bin",
                .size_bytes = 1000,
                .segments = &.{.{ .seq_index = 1, .message_id = "data@host", .bytes = 1000 }},
            },
            .{
                .filename = "release.vol000+01.par2",
                .size_bytes = 500,
                .is_par2 = true,
                .is_recovery_vol = true,
                .segments = &.{.{ .seq_index = 1, .message_id = "vol0@host", .bytes = 500 }},
            },
        },
        .defer_recovery_vols = true,
    }, 1);
    defer j.deinit();
    j.setId(42);
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    // The data file lands; the gated check ignores the vol, so the
    // download phase closes with the vol still pending.
    try j.markSegmentDone(.{ .segment_id = 100, .bytes_on_disk = 1000 }, 10);
    try testing.expectEqual(JobState.download_complete, j.state);
    try drainEvents(&j);

    try j.requestRecoveryVols(20);
    try testing.expectEqual(JobState.downloading, j.state);
    // Flipping the flag must not need a recount to stay correct.
    try expectCountersConsistent(&j);
    try testing.expect(!j.allSegmentsResolved());

    const pending = try j.pendingSegments(testing.allocator, 1000);
    defer testing.allocator.free(pending);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expectEqualStrings("vol0@host", pending[0].message_id);

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    try testing.expectEqual(@as(usize, 1), batch.len);
    try expectKind(.recovery_vols_requested, batch[0]);

    // Second call refuses — the flag is already flipped.
    try testing.expectError(error.RecoveryVolsAlreadyRequested, j.requestRecoveryVols(30));

    // With the vol fetched the job resolves for real.
    try j.markSegmentDone(.{ .segment_id = 101, .bytes_on_disk = 500 }, 40);
    try testing.expect(j.allSegmentsResolved());
    try testing.expectEqual(JobState.download_complete, j.state);
    try expectCountersConsistent(&j);
    try drainEvents(&j);
}

test "requestRecoveryVols refuses when there is nothing deferred" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    // fetch_recovery_vols defaults to true.
    try testing.expectError(error.RecoveryVolsAlreadyRequested, j.requestRecoveryVols(1));

    var k = try Job.init(testing.allocator, .{
        .nzb_hash = "h",
        .name = "n",
        .files = &.{.{
            .filename = "f.bin",
            .size_bytes = 10,
            .segments = &.{.{ .message_id = "m@host", .bytes = 10 }},
        }},
        .defer_recovery_vols = true,
    }, 1);
    defer k.deinit();
    try drainEvents(&k);
    try testing.expectError(error.NoDeferredRecoveryVols, k.requestRecoveryVols(1));
}

test "resetInflightToPending" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    try j.markSegmentDispatched(100, 1);
    try j.markSegmentDispatched(101, 1);
    try drainEvents(&j);

    try testing.expectEqual(@as(usize, 2), j.resetInflightToPending());
    for (j.files[0].segments) |*s| {
        try testing.expectEqual(SegmentState.pending, s.state);
    }
    try expectCountersConsistent(&j);
}

test "resetFailedToPending revives a failed job" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    try j.markSegmentMissing(100, 1);
    try j.markSegmentFailed(101, "boom", 2);
    try j.markSegmentMissing(102, 3);
    try testing.expectEqual(JobState.failed, j.state);
    try drainEvents(&j);

    try testing.expectEqual(@as(usize, 3), j.resetFailedToPending());
    try testing.expectEqual(JobState.queued, j.state);
    try testing.expectEqual(@as(?Timestamp, null), j.finished_at);
    try testing.expectEqualStrings("", j.errorMsg());
    try testing.expect(!j.allSegmentsResolved());
    try expectCountersConsistent(&j);

    const s = j.segmentById(101).?.seg;
    try testing.expectEqual(@as(i32, 0), s.attempts);
    try testing.expectEqualStrings("", s.lastError());
    // No events: this is operator-triggered hygiene, not a transition
    // the bus cares about.
    try testing.expectEqual(@as(usize, 0), j.event_log.items.len);
}

test "pendingSegments filters by next_retry_at" {
    const t0: Timestamp = 1_000_000;
    var j = try Job.init(testing.allocator, .{
        .nzb_hash = "deadbeef",
        .name = "release",
        .files = &.{.{
            .filename = "f.bin",
            .size_bytes = 100,
            .segments = &.{
                .{ .seq_index = 1, .message_id = "ready@host", .bytes = 50 },
                .{ .seq_index = 2, .message_id = "deferred@host", .bytes = 50 },
            },
        }},
    }, t0);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    const minute: Timestamp = 60 * 1000;
    const future = t0 + minute;
    try j.markSegmentForRetry(101, future, "conn-limit");

    {
        const got = try j.pendingSegments(testing.allocator, t0);
        defer testing.allocator.free(got);
        try testing.expectEqual(@as(usize, 1), got.len);
        try testing.expectEqual(@as(SegmentId, 100), got[0].id);
    }
    {
        const got = try j.pendingSegments(testing.allocator, future + 1000);
        defer testing.allocator.free(got);
        try testing.expectEqual(@as(usize, 2), got.len);
    }

    try testing.expectEqual(@as(?Timestamp, future), j.nextRetryReadyAt(t0));
    try testing.expectEqual(@as(?Timestamp, null), j.nextRetryReadyAt(future + 1000));
    // Exactly at the boundary the segment is ready, not deferred.
    try testing.expectEqual(@as(?Timestamp, null), j.nextRetryReadyAt(future));
    try expectCountersConsistent(&j);
}

test "markSegmentForRetry increments attempts and persists the error" {
    const t0: Timestamp = 0;
    var j = try Job.init(testing.allocator, .{
        .nzb_hash = "x",
        .name = "x",
        .files = &.{.{
            .filename = "f.bin",
            .size_bytes = 10,
            .segments = &.{.{ .seq_index = 1, .message_id = "m@host", .bytes = 10 }},
        }},
    }, t0);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 1);

    const at = t0 + 30 * 1000;
    try j.markSegmentForRetry(1, at, "boom");

    const s = j.segmentById(1).?.seg;
    try testing.expectEqual(@as(i32, 1), s.attempts);
    try testing.expectEqualStrings("boom", s.lastError());
    try testing.expectEqual(@as(?Timestamp, at), s.next_retry_at);
    try testing.expectEqual(SegmentState.pending, s.state);

    // A terminal segment does not re-enter the retry queue.
    try j.markSegmentMissing(1, at);
    try drainEvents(&j);
    try j.markSegmentForRetry(1, at + 1, "again");
    try testing.expectEqual(SegmentState.missing, s.state);
    try testing.expectEqual(@as(i32, 1), s.attempts);
    try testing.expectError(error.SegmentNotInJob, j.markSegmentForRetry(999, at, "nope"));
}

test "job lifecycle transitions and their idempotence" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);

    try testing.expect(try j.markStarted(10));
    try testing.expectEqual(JobState.downloading, j.state);
    try testing.expectEqual(@as(?Timestamp, 10), j.started_at);
    try testing.expect(!try j.markStarted(11));

    try testing.expect(try j.pause(20));
    try testing.expectEqual(JobState.paused, j.state);
    try testing.expect(!try j.pause(21));

    try testing.expect(try j.resume_(30));
    // started_at was set, so it resumes into downloading.
    try testing.expectEqual(JobState.downloading, j.state);
    try testing.expect(!try j.resume_(31));

    try testing.expect(try j.markWaitingForServer("no usable server", 40));
    try testing.expectEqual(JobState.waiting_for_server, j.state);
    try testing.expect(!try j.markWaitingForServer("again", 41));
    try testing.expect(try j.resumeFromWait(50));
    try testing.expectEqual(JobState.queued, j.state);
    try testing.expect(!try j.resumeFromWait(51));

    try testing.expect(try j.markCompleted(60));
    try testing.expectEqual(JobState.completed, j.state);
    try testing.expect(!try j.markCompleted(61));
    // Terminal states reject the download-phase transitions.
    try testing.expect(!try j.pause(70));
    try testing.expect(!try j.markStarted(70));

    try j.markRemoved(80);

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    const want = [_]events.Kind{
        .job_started,
        .job_paused,
        .job_resumed,
        .job_waiting_for_server,
        .job_resumed,
        .job_completed,
        .job_removed,
    };
    try testing.expectEqual(want.len, batch.len);
    for (batch, want) |e, kind| try expectKind(kind, e);
    try testing.expectEqualStrings("no usable server", batch[3].job_waiting_for_server.reason);
}

test "resume of a never-started job goes back to queued" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);

    try testing.expect(try j.pause(10));
    try testing.expect(try j.resume_(20));
    try testing.expectEqual(JobState.queued, j.state);
    try drainEvents(&j);
}

test "markFailed sets the reason once" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);

    try testing.expect(try j.markFailed("disk full", 100));
    try testing.expectEqual(JobState.failed, j.state);
    try testing.expectEqualStrings("disk full", j.errorMsg());
    try testing.expect(!try j.markFailed("something else", 101));
    try testing.expectEqualStrings("disk full", j.errorMsg());

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    try testing.expectEqual(@as(usize, 1), batch.len);
    try testing.expectEqualStrings("disk full", batch[0].job_failed.err);
}

test "deinit releases events that were never pulled" {
    // A Job dropped mid-flight (shutdown, a failed transaction) must not
    // leak the strings its unpulled events own.
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    assignSegmentIds(&j, 100);

    try testing.expect(try j.markWaitingForServer("no usable server", 5));
    try testing.expect(try j.resumeFromWait(6));
    try j.markSegmentFailed(100, "crc mismatch", 7);
    try testing.expect(try j.markFailed("giving up", 8));
    // No pullEvents: JobCreated, JobWaitingForServer, JobResumed,
    // SegmentFailed and JobFailed are all still on the aggregate.
    try testing.expectEqual(@as(usize, 5), j.event_log.items.len);
}

test "abortIfHopeless fires past the threshold only" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 100);

    // Thresholds outside (0,1) disable the check entirely.
    try testing.expect(!try j.abortIfHopeless(0, 1));
    try testing.expect(!try j.abortIfHopeless(1, 1));

    // 400 of 1200 bytes missing → ratio 0.333.
    try j.markSegmentMissing(101, 10);
    try drainEvents(&j);
    try testing.expect(!try j.abortIfHopeless(0.5, 11));
    try testing.expectEqual(JobState.queued, j.state);

    try testing.expect(try j.abortIfHopeless(0.3, 12));
    try testing.expectEqual(JobState.failed, j.state);
    try testing.expectEqual(@as(?Timestamp, 12), j.finished_at);
    try testing.expectEqualStrings(
        "download aborted: 33% missing exceeds 30% threshold",
        j.errorMsg(),
    );

    const batch = try j.pullEvents();
    defer events.deinitAll(testing.allocator, batch);
    try testing.expectEqual(@as(usize, 2), batch.len);
    try expectKind(.job_download_failed, batch[0]);
    try expectKind(.job_failed, batch[1]);
    try testing.expectEqualStrings(
        "download aborted: 33% missing exceeds 30% threshold",
        batch[1].job_failed.err,
    );

    // Terminal now, so it cannot fire twice.
    try testing.expect(!try j.abortIfHopeless(0.3, 13));
}

test "setQueueOrder only dirties on a real change" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);

    j.clearStateDirty();
    j.setQueueOrder(0);
    try testing.expect(!j.isStateDirty());
    j.setQueueOrder(9);
    try testing.expect(j.isStateDirty());
    try testing.expectEqual(@as(i64, 9), j.queue_order);
    try testing.expectEqual(@as(usize, 0), j.event_log.items.len);
}

test "hydrate seeds the unresolved counters from the loaded states" {
    var j = try Job.hydrate(testing.allocator, .{
        .id = 3,
        .nzb_hash = "hash",
        .name = "release",
        .category = "tv",
        .state = .downloading,
        .total_bytes = 1500,
        .done_bytes = 600,
        .added_at = 1,
        .started_at = 2,
        .error_msg = "previous trouble",
        .nzb_blob = "<nzb/>",
        .fetch_recovery_vols = false,
        .files = &.{
            .{
                .id = 11,
                .job_id = 3,
                .filename = "f.bin",
                .groups = &.{ "alt.binaries.a", "alt.binaries.b" },
                .size_bytes = 1000,
                .segment_count = 2,
                .segments_done = 1,
                .segments = &.{
                    .{ .id = 100, .file_id = 11, .message_id = "a@host", .bytes = 600, .state = .done },
                    .{ .id = 101, .file_id = 11, .message_id = "b@host", .bytes = 400, .state = .pending, .attempts = 2, .last_error = "timeout" },
                },
            },
            .{
                .id = 12,
                .job_id = 3,
                .filename = "f.vol000+01.par2",
                .size_bytes = 500,
                .is_par2 = true,
                .is_recovery_vol = true,
                .segment_count = 1,
                .segments = &.{
                    .{ .id = 102, .file_id = 12, .message_id = "v@host", .bytes = 500, .state = .pending },
                },
            },
        },
    });
    defer j.deinit();

    try testing.expectEqual(@as(usize, 0), j.event_log.items.len);
    try testing.expect(!j.isStateDirty());
    try expectCountersConsistent(&j);
    try testing.expectEqual(@as(usize, 1), j.unresolved_non_recovery_vol);
    try testing.expectEqual(@as(usize, 1), j.unresolved_recovery_vol);
    // Recovery vols are gated off, so the one pending vol does not hold
    // the job open — but the data segment does.
    try testing.expect(!j.allSegmentsResolved());
    try testing.expectEqualStrings("timeout", j.files[0].segments[1].lastError());
    try testing.expectEqualStrings("alt.binaries.b", j.files[0].groups[1]);
    try testing.expectEqualStrings("previous trouble", j.errorMsg());
    try testing.expectEqualStrings("<nzb/>", j.nzb_blob);

    // The loaded ids index without a rebuild call.
    try testing.expectEqual(@as(FileId, 12), j.segmentById(102).?.file.id);
    try testing.expectEqual(@as(?SegmentRef, null), j.segmentById(999));

    // Finish the data segment: the gated check closes the phase.
    try j.markSegmentDone(.{ .segment_id = 101, .bytes_on_disk = 400 }, 50);
    try testing.expectEqual(JobState.download_complete, j.state);
    try expectCountersConsistent(&j);
    try drainEvents(&j);
}

test "rebuildSegmentIndex picks up ids assigned after insert" {
    var j = try mkJob(testing.allocator);
    defer j.deinit();
    try drainEvents(&j);

    // Before ids exist every segment keys on 0, so the index answers
    // with whichever one landed last — the repo must invalidate.
    try testing.expect(j.segmentById(100) == null);
    for (j.files) |*f| {
        for (f.segments, 0..) |*s, i| s.setId(@intCast(100 + i));
    }
    // Stale index: still keyed on the old (zero) ids.
    try testing.expect(j.segmentById(100) == null);
    j.rebuildSegmentIndex();
    try testing.expect(j.segmentById(100) != null);
    // Rebuilding is allocation-free; capacity was reserved up front.
    try testing.expectEqual(@as(SegmentId, 100), j.segmentById(100).?.seg.id);
}

test "counters survive a randomised transition sequence" {
    // A fast counter can silently diverge from the truth, and a
    // divergence only shows up as a job that never completes (or one
    // that completes early). Drive a sizeable aggregate through a
    // pseudo-random mix of every transition and assert after each step
    // that the counters and a full recount still agree.
    const a = testing.allocator;

    var seg_params: [40]NewSegmentParams = undefined;
    for (&seg_params, 0..) |*sp, i| sp.* = .{
        .seq_index = @intCast(i + 1),
        .message_id = "m@host",
        .bytes = 100,
    };

    var j = try Job.init(a, .{
        .nzb_hash = "seeded",
        .name = "release",
        .files = &.{
            .{ .filename = "a.bin", .size_bytes = 4000, .segments = seg_params[0..20] },
            .{ .filename = "b.par2", .size_bytes = 1000, .is_par2 = true, .segments = seg_params[20..25] },
            .{ .filename = "b.vol000+05.par2", .size_bytes = 1500, .is_par2 = true, .is_recovery_vol = true, .segments = seg_params[25..40] },
        },
        .defer_recovery_vols = true,
    }, 1);
    defer j.deinit();
    try drainEvents(&j);
    assignSegmentIds(&j, 1);

    var prng = std.Random.DefaultPrng.init(0x5EED_C0FFEE);
    const rand = prng.random();

    var gate_flips: usize = 0;
    var unresolved_steps: usize = 0;

    // The mix is weighted towards the transitions that *undo* terminal
    // state (retry, reset). An even mix saturates every segment as
    // terminal within a few hundred steps and then only exercises the
    // idempotent early-returns; this keeps the aggregate churning
    // through mixed states for the whole run.
    var i: usize = 0;
    while (i < 4000) : (i += 1) {
        const id: SegmentId = @intCast(rand.intRangeLessThan(usize, 1, 41));
        const now: Timestamp = @intCast(1000 + i);
        switch (rand.uintLessThan(u8, 12)) {
            0, 1 => j.markSegmentDispatched(id, now) catch |err| switch (err) {
                error.SegmentNotPending => {},
                else => return err,
            },
            2, 3 => try j.markSegmentDone(.{ .segment_id = id, .bytes_on_disk = 100 }, now),
            4 => try j.markSegmentMissing(id, now),
            5 => try j.markSegmentFailed(id, "transient", now),
            6, 7, 8 => try j.markSegmentForRetry(id, now + 500, "backoff"),
            9, 10 => _ = j.resetFailedToPending(),
            else => _ = j.resetInflightToPending(),
        }
        try expectCountersConsistent(&j);
        // The O(1) answer must match what the scan would have said.
        const counts = j.recountUnresolved();
        const scanned = counts.non_recovery_vol == 0 and
            (!j.fetch_recovery_vols or counts.recovery_vol == 0);
        try testing.expectEqual(scanned, j.allSegmentsResolved());

        // Flip the recovery-vol gate mid-run: the counters must stay
        // correct across the transition without a recount. Attempted
        // periodically because it needs a still-pending vol segment to
        // have something to reveal.
        if (i % 250 == 0 and j.hasDeferredRecoveryVols()) {
            gate_flips += 1;
            j.requestRecoveryVols(now) catch |err| switch (err) {
                error.JobTerminal => {},
                else => return err,
            };
            try expectCountersConsistent(&j);
        }
        if (!j.allSegmentsResolved()) unresolved_steps += 1;
        try drainEvents(&j);
    }

    // Guard the guard: if the sequence had degenerated into "everything
    // terminal, every call a no-op" the consistency assertions above
    // would be worthless.
    try testing.expect(gate_flips >= 1);
    try testing.expect(unresolved_steps > 1000);
}

test {
    // Keep the sibling modules' tests reachable from this file, so
    // `zig test src/domain/download/job.zig` covers the whole context.
    _ = state_mod;
    _ = events;
    _ = segment_mod;
    _ = file_mod;
    _ = @import("ports.zig");
}
