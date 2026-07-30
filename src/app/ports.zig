//! The seams every application service in this layer is wired through.
//!
//! Go expressed these as interfaces satisfied by `adapter/*` packages
//! and let the compiler find the implementation. Zig has no implicit
//! interface, so each port is a struct holding a `*anyopaque` context
//! plus function pointers — the same shape `app/notify/transport.zig`
//! established, for the same reason: a service stores one in a field for
//! the process lifetime, so it must be a concrete non-generic type.
//!
//! Everything that touches the outside world lives here:
//!
//!   * `Clock` — the current instant. Nothing below this file reads a
//!     real clock, and no service sleeps. Delays are expressed as
//!     "wake me at T" verdicts the caller (the reactor) honours, which
//!     is what makes every test in this layer deterministic.
//!   * `EventSink(E)` — publishing domain events. Generic over the
//!     context's event union so the type survives all the way to the
//!     outbox; `tx.Unit` rides along so the events commit in the same
//!     transaction as the state change that produced them.
//!   * `Filesystem` — the ten operations the download/deliver/extract
//!     pipeline actually performs. Narrow on purpose: a fake that
//!     implements ten calls is credible, one that implements a POSIX
//!     surface is not.
//!
//! The transaction port is `domain/tx.zig`'s `Manager` unchanged — it is
//! already a vtable with the right shape, and inventing a second one
//! here would mean the store adapter has to satisfy both.

const std = @import("std");
const event = @import("../domain/event.zig");
const tx = @import("../domain/tx.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = event.Timestamp;
pub const Millis = event.Millis;
pub const Manager = tx.Manager;
pub const Unit = tx.Unit;
pub const TxError = tx.Error;

// ---------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------

/// The current instant, in Unix milliseconds UTC.
///
/// Injected rather than read, because a service that calls
/// `std.time.milliTimestamp` cannot be tested for anything that depends
/// on elapsed time without sleeping — and a test that sleeps is a test
/// that flakes.
pub const Clock = struct {
    ctx: *anyopaque,
    nowFn: *const fn (ctx: *anyopaque) Timestamp,

    pub fn now(self: Clock) Timestamp {
        return self.nowFn(self.ctx);
    }
};

/// A clock a test drives by hand. `pub` because every service's tests
/// need one and three copies of it would drift.
pub const FakeClock = struct {
    t: Timestamp = 0,

    pub fn clock(self: *FakeClock) Clock {
        return .{ .ctx = @ptrCast(self), .nowFn = &read };
    }

    pub fn advance(self: *FakeClock, ms: Millis) void {
        self.t += ms;
    }

    pub fn set(self: *FakeClock, at: Timestamp) void {
        self.t = at;
    }

    fn read(ctx: *anyopaque) Timestamp {
        const self: *FakeClock = @ptrCast(@alignCast(ctx));
        return self.t;
    }
};

/// A live boolean the operator can change from the Settings UI.
///
/// Go passed `func() bool` closures for these (`DeleteSamples`,
/// `CollapseSingleFolder`, `DeferRecoveryVols`, …) so an edit took effect
/// on the next job without a restart. Same idea, same freshness, with a
/// context pointer instead of a captured environment.
pub const Toggle = struct {
    ctx: *anyopaque,
    readFn: *const fn (ctx: *anyopaque) bool,

    pub fn read(self: Toggle) bool {
        return self.readFn(self.ctx);
    }
};

/// A `Toggle` over a plain bool a test can flip between calls.
pub const FakeToggle = struct {
    on: bool = false,
    reads: usize = 0,

    pub fn toggle(self: *FakeToggle) Toggle {
        return .{ .ctx = @ptrCast(self), .readFn = &read };
    }

    fn read(ctx: *anyopaque) bool {
        const self: *FakeToggle = @ptrCast(@alignCast(ctx));
        self.reads += 1;
        return self.on;
    }
};

/// A live number the operator can change from the Settings UI — the
/// concurrency cap, the fail-hopeless ratio, a bandwidth limit.
pub const Knob = struct {
    ctx: *anyopaque,
    readFn: *const fn (ctx: *anyopaque) i64,

    pub fn read(self: Knob) i64 {
        return self.readFn(self.ctx);
    }
};

pub const FakeKnob = struct {
    v: i64 = 0,

    pub fn knob(self: *FakeKnob) Knob {
        return .{ .ctx = @ptrCast(self), .readFn = &read };
    }

    fn read(ctx: *anyopaque) i64 {
        const self: *FakeKnob = @ptrCast(@alignCast(ctx));
        return self.v;
    }
};

// ---------------------------------------------------------------------
// Event publication
// ---------------------------------------------------------------------

pub const PublishError = error{
    /// The outbox insert failed. The caller must roll the transaction
    /// back — publishing and the state change are one unit.
    Backend,
} || Allocator.Error;

/// Publishes a batch of domain events.
///
/// Generic over the event union so the sink keeps the context's own
/// type all the way down to the outbox serialiser: an `[]const Event`
/// cannot be handed to the wrong context's sink by accident, and no
/// `anyopaque` payload conversion happens in the app layer.
///
/// `unit` is the ambient transaction, or null when the caller has none
/// (which only the fakes and the admin paths do). Passing it explicitly
/// rather than through a thread-local is deliberate: on a single-threaded
/// reactor a thread-local ambient transaction is a footgun waiting for
/// the first `await` in the middle of a tx.
pub fn EventSink(comptime E: type) type {
    return struct {
        const Self = @This();

        ctx: *anyopaque,
        publishFn: *const fn (ctx: *anyopaque, unit: ?*Unit, events: []const E) PublishError!void,

        pub fn publish(self: Self, unit: ?*Unit, events: []const E) PublishError!void {
            if (events.len == 0) return;
            return self.publishFn(self.ctx, unit, events);
        }
    };
}

/// Records every event it is handed, so a test can assert on the topics
/// a use case emitted and in what order.
///
/// The events themselves are *not* owned — the recorder keeps only the
/// topic strings, which are comptime constants in `.rodata`. That keeps
/// the fake free of the "who frees the owned error string" question
/// that a real subscriber has to answer.
pub fn FakeSink(comptime E: type) type {
    return struct {
        const Self = @This();
        const Sink = EventSink(E);

        topics: [64][]const u8 = @splat(""),
        n: usize = 0,
        /// Number of `publish` calls (as opposed to events).
        batches: usize = 0,
        /// Set to make the next publish fail.
        fail: ?PublishError = null,

        pub fn sink(self: *Self) Sink {
            return .{ .ctx = @ptrCast(self), .publishFn = &record };
        }

        pub fn seen(self: *const Self) []const []const u8 {
            return self.topics[0..self.n];
        }

        pub fn reset(self: *Self) void {
            self.n = 0;
            self.batches = 0;
        }

        /// How many recorded events carry `topic`.
        pub fn count(self: *const Self, topic: []const u8) usize {
            var c: usize = 0;
            for (self.seen()) |t| {
                if (std.mem.eql(u8, t, topic)) c += 1;
            }
            return c;
        }

        pub fn has(self: *const Self, topic: []const u8) bool {
            return self.count(topic) > 0;
        }

        fn record(ctx: *anyopaque, _: ?*Unit, events: []const E) PublishError!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fail) |e| {
                self.fail = null;
                return e;
            }
            self.batches += 1;
            for (events) |ev| {
                if (self.n == self.topics.len) return;
                self.topics[self.n] = ev.topic();
                self.n += 1;
            }
        }
    };
}

// ---------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------

/// A transaction manager that counts boundaries and can be told to fail
/// at each one. `domain/tx.zig` has an equivalent for its own tests but
/// keeps it private, and the app-layer services need one too.
pub const FakeTx = struct {
    begins: u32 = 0,
    commits: u32 = 0,
    rollbacks: u32 = 0,
    fail_begin: bool = false,
    fail_commit: ?TxError = null,

    const unit_vtable: Unit.VTable = .{ .commit = commitFn, .rollback = rollbackFn };
    const manager_vtable: Manager.VTable = .{ .begin = beginFn };

    pub fn manager(self: *FakeTx) Manager {
        return .{ .ctx = @ptrCast(self), .vtable = &manager_vtable };
    }

    /// True when every transaction opened has been closed exactly once.
    pub fn balanced(self: *const FakeTx) bool {
        return self.begins == self.commits + self.rollbacks;
    }

    fn beginFn(ctx: *anyopaque) TxError!Unit {
        const self: *FakeTx = @ptrCast(@alignCast(ctx));
        if (self.fail_begin) return error.Begin;
        self.begins += 1;
        return .{ .ctx = ctx, .vtable = &unit_vtable };
    }

    fn commitFn(ctx: *anyopaque) TxError!void {
        const self: *FakeTx = @ptrCast(@alignCast(ctx));
        if (self.fail_commit) |e| {
            self.rollbacks += 1;
            return e;
        }
        self.commits += 1;
    }

    fn rollbackFn(ctx: *anyopaque) void {
        const self: *FakeTx = @ptrCast(@alignCast(ctx));
        self.rollbacks += 1;
    }
};

// ---------------------------------------------------------------------
// One-per-job aggregate persistence
// ---------------------------------------------------------------------

pub const AggregateError = error{
    /// No row for that job. Every caller in this layer treats it as
    /// "first time through", not as a failure.
    NotFound,
    Backend,
} || Allocator.Error;

/// Persistence for the four post-download aggregates — `VerifySet`,
/// `Repair`, `Extract`, `Delivery`.
///
/// They share a shape exactly: at most one row per job, looked up by job
/// id, written whole. Go declared four separate `Repository` interfaces
/// with identical method sets; one generic saves three copies of the port
/// and three copies of its test double, and the type parameter keeps a
/// `Repo(Delivery)` from being passed where a `Repo(Extract)` belongs.
///
/// `A` must expose `id`, `job_id`, `setId` and `deinit` — which the
/// domain aggregates already do.
pub fn Repo(comptime A: type) type {
    return struct {
        const Self = @This();

        ctx: *anyopaque,
        byJobIdFn: *const fn (ctx: *anyopaque, unit: ?*Unit, job_id: i64) AggregateError!*A,
        /// Hands a loaded aggregate back. Exactly once per successful
        /// load.
        releaseFn: *const fn (ctx: *anyopaque, a: *A) void,
        /// Inserts when `id == 0` (assigning one via `setId`), otherwise
        /// updates. Takes ownership on insert.
        saveFn: *const fn (ctx: *anyopaque, unit: ?*Unit, a: *A) AggregateError!void,

        pub fn byJobId(self: Self, unit: ?*Unit, job_id: i64) AggregateError!*A {
            return self.byJobIdFn(self.ctx, unit, job_id);
        }

        pub fn release(self: Self, a: *A) void {
            self.releaseFn(self.ctx, a);
        }

        pub fn save(self: Self, unit: ?*Unit, a: *A) AggregateError!void {
            return self.saveFn(self.ctx, unit, a);
        }
    };
}

/// An in-memory `Repo(A)`. Hands out the same aggregate on every load for
/// the same reason `FakeJobStore` does: re-hydration is the repository's
/// contract to test, not the service's.
pub fn FakeRepo(comptime A: type) type {
    return struct {
        const Self = @This();

        gpa: Allocator,
        items: std.ArrayList(*A) = .empty,
        next_id: i64 = 1,
        saves: usize = 0,
        loads: usize = 0,
        releases: usize = 0,
        fail_save: ?AggregateError = null,

        pub fn init(gpa: Allocator) Self {
            return .{ .gpa = gpa };
        }

        pub fn deinit(self: *Self) void {
            for (self.items.items) |it| {
                it.deinit();
                self.gpa.destroy(it);
            }
            self.items.deinit(self.gpa);
            self.* = undefined;
        }

        pub fn repo(self: *Self) Repo(A) {
            return .{
                .ctx = @ptrCast(self),
                .byJobIdFn = &byJobId,
                .releaseFn = &release,
                .saveFn = &save,
            };
        }

        /// Takes ownership, assigning an id the way an INSERT would.
        pub fn insert(self: *Self, item: *A) Allocator.Error!void {
            if (item.id == 0) {
                item.setId(self.next_id);
                self.next_id += 1;
            }
            try self.items.append(self.gpa, item);
        }

        pub fn get(self: *Self, job_id: i64) ?*A {
            for (self.items.items) |it| {
                if (it.job_id == job_id) return it;
            }
            return null;
        }

        pub fn len(self: *const Self) usize {
            return self.items.items.len;
        }

        pub fn leakFree(self: *const Self) bool {
            return self.loads == self.releases;
        }

        fn byJobId(ctx: *anyopaque, _: ?*Unit, job_id: i64) AggregateError!*A {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const it = self.get(job_id) orelse return error.NotFound;
            self.loads += 1;
            return it;
        }

        fn release(ctx: *anyopaque, _: *A) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.releases += 1;
        }

        fn save(ctx: *anyopaque, _: ?*Unit, item: *A) AggregateError!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fail_save) |e| {
                self.fail_save = null;
                return e;
            }
            if (item.id == 0) try self.insert(item);
            self.saves += 1;
        }
    };
}

// ---------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------

/// Maps a job's category onto the sub-directory its release lands in.
///
/// Go passed the concrete `*sqlite.CategoryRepo` into the extract and
/// deliver services and looped over `List()` on every delivery, which
/// coupled two app packages to an adapter type for a single string
/// lookup. This is that lookup.
pub const Categories = struct {
    ctx: *anyopaque,
    /// The `dir` column for `name`, or null when the category is unknown
    /// (which means "no sub-directory", not an error). The reserved `*`
    /// category deliberately maps to null.
    dirForFn: *const fn (ctx: *anyopaque, name: []const u8) ?[]const u8,

    pub fn dirFor(self: Categories, name: []const u8) ?[]const u8 {
        return self.dirForFn(self.ctx, name);
    }
};

pub const FakeCategories = struct {
    pub const Row = struct { name: []const u8, dir: []const u8 };

    rows: []const Row = &.{},

    pub fn categories(self: *FakeCategories) Categories {
        return .{ .ctx = @ptrCast(self), .dirForFn = &dirFor };
    }

    fn dirFor(ctx: *anyopaque, name: []const u8) ?[]const u8 {
        const self: *FakeCategories = @ptrCast(@alignCast(ctx));
        for (self.rows) |r| {
            if (std.mem.eql(u8, r.name, name)) return r.dir;
        }
        return null;
    }
};

// ---------------------------------------------------------------------
// Filesystem
// ---------------------------------------------------------------------

/// What a filesystem operation can go wrong with.
///
/// `NotFound` is the one every caller branches on: a `.tmp` that never
/// materialised because every one of its segments 430'd is normal, not
/// an operator problem, and the deliver/extract/verify paths all skip
/// rather than fail on it.
pub const FsError = error{
    NotFound,
    /// The destination already exists and the operation refuses to
    /// clobber it.
    Exists,
    Denied,
    NotDirectory,
    /// Anything the backend reported that is none of the above.
    Io,
} || Allocator.Error;

/// One directory entry, as `Filesystem.list` reports it.
pub const DirEntry = struct {
    /// Base name only, never a path. Borrowed from the allocator passed
    /// to `list`.
    name: []const u8,
    is_dir: bool,
    /// 0 for directories.
    size: i64 = 0,
};

/// The filesystem, narrowed to what this layer does with it.
///
/// `writeAt` collapses open + preallocate + pwrite + close into one
/// call. That is not a convenience: the orchestrator's segment write is
/// the hottest filesystem path in the process, and a port that hands
/// out file handles would either leak them on a cancelled fetch or need
/// a handle lifetime the reactor has to track.
pub const Filesystem = struct {
    ctx: *anyopaque,

    mkdirAllFn: *const fn (ctx: *anyopaque, path: []const u8) FsError!void,
    /// Writes `bytes` at `offset`, creating `path` (and nothing above
    /// it) if absent. When `min_size` is positive the file is grown to
    /// at least that length first, so a multi-part file is allocated
    /// once rather than extended per segment.
    writeAtFn: *const fn (ctx: *anyopaque, path: []const u8, offset: i64, bytes: []const u8, min_size: i64) FsError!void,
    /// Rename. Must fail with `NotFound` when `src` is absent — the
    /// pipeline relies on distinguishing that from a real failure.
    moveFn: *const fn (ctx: *anyopaque, src: []const u8, dst: []const u8) FsError!void,
    removeFn: *const fn (ctx: *anyopaque, path: []const u8) FsError!void,
    /// Recursive delete. Absent path is success, not `NotFound`.
    removeAllFn: *const fn (ctx: *anyopaque, path: []const u8) FsError!void,
    /// Size in bytes, or `NotFound`. `null` for a directory.
    sizeOfFn: *const fn (ctx: *anyopaque, path: []const u8) FsError!?i64,
    /// Direct children of `path`, in a stable order. The slice and every
    /// name in it belong to `a`.
    listFn: *const fn (ctx: *anyopaque, a: Allocator, path: []const u8) FsError![]DirEntry,

    pub fn mkdirAll(self: Filesystem, path: []const u8) FsError!void {
        return self.mkdirAllFn(self.ctx, path);
    }

    pub fn writeAt(
        self: Filesystem,
        path: []const u8,
        offset: i64,
        bytes: []const u8,
        min_size: i64,
    ) FsError!void {
        return self.writeAtFn(self.ctx, path, offset, bytes, min_size);
    }

    pub fn move(self: Filesystem, src: []const u8, dst: []const u8) FsError!void {
        return self.moveFn(self.ctx, src, dst);
    }

    pub fn remove(self: Filesystem, path: []const u8) FsError!void {
        return self.removeFn(self.ctx, path);
    }

    pub fn removeAll(self: Filesystem, path: []const u8) FsError!void {
        return self.removeAllFn(self.ctx, path);
    }

    pub fn sizeOf(self: Filesystem, path: []const u8) FsError!?i64 {
        return self.sizeOfFn(self.ctx, path);
    }

    pub fn list(self: Filesystem, a: Allocator, path: []const u8) FsError![]DirEntry {
        return self.listFn(self.ctx, a, path);
    }

    /// Convenience shared by verify, repair, deliver and extract, all of
    /// which want "is there anything at this path" and none of which
    /// care why not.
    pub fn exists(self: Filesystem, path: []const u8) bool {
        _ = self.sizeOf(path) catch return false;
        return true;
    }

    pub fn isDir(self: Filesystem, path: []const u8) bool {
        const s = self.sizeOf(path) catch return false;
        return s == null;
    }
};

/// An in-memory filesystem for tests.
///
/// Paths are stored whole and compared literally — no normalisation, no
/// symlinks, no `..`. Callers in this layer only ever build paths by
/// joining known components, so the simplification costs nothing and
/// keeps the fake small enough to trust.
///
/// `pub` because verify, repair, extract, deliver and the orchestrator
/// all need a filesystem double, and five of them would drift.
pub const FakeFs = struct {
    pub const Node = struct {
        /// Owned.
        path: []u8,
        is_dir: bool,
        /// Owned; empty for directories.
        data: std.ArrayList(u8) = .empty,
    };

    gpa: Allocator,
    nodes: std.ArrayList(Node) = .empty,
    /// Set to make the next mutating call fail.
    fail_next: ?FsError = null,
    writes: usize = 0,
    moves: usize = 0,

    pub fn init(gpa: Allocator) FakeFs {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeFs) void {
        for (self.nodes.items) |*n| {
            self.gpa.free(n.path);
            n.data.deinit(self.gpa);
        }
        self.nodes.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn filesystem(self: *FakeFs) Filesystem {
        return .{
            .ctx = @ptrCast(self),
            .mkdirAllFn = &mkdirAll,
            .writeAtFn = &writeAt,
            .moveFn = &move,
            .removeFn = &remove,
            .removeAllFn = &removeAll,
            .sizeOfFn = &sizeOf,
            .listFn = &list,
        };
    }

    // ---- test-side helpers ----------------------------------------

    /// Creates a file of `size` bytes (zero-filled) plus every parent
    /// directory. The content is what `sizeOf` reports, which is all the
    /// postprocess heuristics look at.
    pub fn addFile(self: *FakeFs, path: []const u8, size: usize) Allocator.Error!void {
        try self.ensureParents(path);
        const n = try self.upsert(path, false);
        n.data.clearRetainingCapacity();
        try n.data.appendNTimes(self.gpa, 0xAB, size);
    }

    pub fn addDir(self: *FakeFs, path: []const u8) Allocator.Error!void {
        try self.ensureParents(path);
        _ = try self.upsert(path, true);
    }

    pub fn has(self: *const FakeFs, path: []const u8) bool {
        return self.find(path) != null;
    }

    pub fn fileSize(self: *const FakeFs, path: []const u8) ?usize {
        const n = self.find(path) orelse return null;
        if (n.is_dir) return null;
        return n.data.items.len;
    }

    pub fn contents(self: *const FakeFs, path: []const u8) ?[]const u8 {
        const n = self.find(path) orelse return null;
        if (n.is_dir) return null;
        return n.data.items;
    }

    pub fn count(self: *const FakeFs) usize {
        return self.nodes.items.len;
    }

    // ---- internals -------------------------------------------------

    fn find(self: *const FakeFs, path: []const u8) ?*Node {
        for (self.nodes.items) |*n| {
            if (std.mem.eql(u8, n.path, path)) return n;
        }
        return null;
    }

    fn upsert(self: *FakeFs, path: []const u8, is_dir: bool) Allocator.Error!*Node {
        if (self.find(path)) |n| return n;
        const owned = try self.gpa.dupe(u8, path);
        errdefer self.gpa.free(owned);
        try self.nodes.append(self.gpa, .{ .path = owned, .is_dir = is_dir });
        return &self.nodes.items[self.nodes.items.len - 1];
    }

    fn ensureParents(self: *FakeFs, path: []const u8) Allocator.Error!void {
        var i: usize = 0;
        while (std.mem.indexOfScalarPos(u8, path, i, '/')) |slash| {
            if (slash > 0) _ = try self.upsert(path[0..slash], true);
            i = slash + 1;
        }
    }

    fn takeFailure(self: *FakeFs) FsError!void {
        if (self.fail_next) |e| {
            self.fail_next = null;
            return e;
        }
    }

    fn mkdirAll(ctx: *anyopaque, path: []const u8) FsError!void {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        try self.takeFailure();
        try self.ensureParents(path);
        _ = try self.upsert(path, true);
    }

    fn writeAt(
        ctx: *anyopaque,
        path: []const u8,
        offset: i64,
        bytes: []const u8,
        min_size: i64,
    ) FsError!void {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        try self.takeFailure();
        // A real open(2) does not create intermediate directories, and
        // neither does this: the orchestrator mkdirs the job dir once
        // and a missing parent here is a bug worth surfacing.
        if (std.fs.path.dirname(path)) |parent| {
            const p = self.find(parent) orelse return error.NotFound;
            if (!p.is_dir) return error.NotDirectory;
        }
        const n = try self.upsert(path, false);
        if (n.is_dir) return error.NotDirectory;
        const off: usize = @intCast(@max(offset, 0));
        const need = @max(off + bytes.len, @as(usize, @intCast(@max(min_size, 0))));
        if (n.data.items.len < need) {
            try n.data.appendNTimes(self.gpa, 0, need - n.data.items.len);
        }
        @memcpy(n.data.items[off..][0..bytes.len], bytes);
        self.writes += 1;
    }

    fn move(ctx: *anyopaque, src: []const u8, dst: []const u8) FsError!void {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        try self.takeFailure();
        const idx = blk: {
            for (self.nodes.items, 0..) |*n, i| {
                if (std.mem.eql(u8, n.path, src)) break :blk i;
            }
            return error.NotFound;
        };
        if (std.fs.path.dirname(dst)) |parent| {
            const p = self.find(parent) orelse return error.NotFound;
            if (!p.is_dir) return error.NotDirectory;
        }
        if (self.find(dst) != null) return error.Exists;
        const owned = try self.gpa.dupe(u8, dst);
        // Rename children too, so moving a directory keeps its subtree.
        const old = self.nodes.items[idx].path;
        if (self.nodes.items[idx].is_dir) {
            var i: usize = 0;
            while (i < self.nodes.items.len) : (i += 1) {
                if (i == idx) continue;
                const p = self.nodes.items[i].path;
                if (p.len > old.len and std.mem.startsWith(u8, p, old) and p[old.len] == '/') {
                    const joined = std.mem.concat(self.gpa, u8, &.{ dst, p[old.len..] }) catch |e| {
                        self.gpa.free(owned);
                        return e;
                    };
                    self.gpa.free(self.nodes.items[i].path);
                    self.nodes.items[i].path = joined;
                }
            }
        }
        self.gpa.free(old);
        self.nodes.items[idx].path = owned;
        self.moves += 1;
    }

    fn remove(ctx: *anyopaque, path: []const u8) FsError!void {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        try self.takeFailure();
        for (self.nodes.items, 0..) |*n, i| {
            if (!std.mem.eql(u8, n.path, path)) continue;
            self.gpa.free(n.path);
            n.data.deinit(self.gpa);
            _ = self.nodes.orderedRemove(i);
            return;
        }
        return error.NotFound;
    }

    fn removeAll(ctx: *anyopaque, path: []const u8) FsError!void {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        try self.takeFailure();
        var i: usize = 0;
        while (i < self.nodes.items.len) {
            const p = self.nodes.items[i].path;
            const under = std.mem.eql(u8, p, path) or
                (p.len > path.len and std.mem.startsWith(u8, p, path) and p[path.len] == '/');
            if (!under) {
                i += 1;
                continue;
            }
            self.gpa.free(self.nodes.items[i].path);
            self.nodes.items[i].data.deinit(self.gpa);
            _ = self.nodes.orderedRemove(i);
        }
    }

    fn sizeOf(ctx: *anyopaque, path: []const u8) FsError!?i64 {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        const n = self.find(path) orelse return error.NotFound;
        if (n.is_dir) return null;
        return @intCast(n.data.items.len);
    }

    fn list(ctx: *anyopaque, a: Allocator, path: []const u8) FsError![]DirEntry {
        const self: *FakeFs = @ptrCast(@alignCast(ctx));
        const dir = self.find(path) orelse return error.NotFound;
        if (!dir.is_dir) return error.NotDirectory;

        var out: std.ArrayList(DirEntry) = .empty;
        errdefer out.deinit(a);
        for (self.nodes.items) |*n| {
            if (n.path.len <= path.len + 1) continue;
            if (!std.mem.startsWith(u8, n.path, path)) continue;
            if (n.path[path.len] != '/') continue;
            const rest = n.path[path.len + 1 ..];
            // Direct children only.
            if (std.mem.indexOfScalar(u8, rest, '/') != null) continue;
            try out.append(a, .{
                .name = try a.dupe(u8, rest),
                .is_dir = n.is_dir,
                .size = if (n.is_dir) 0 else @intCast(n.data.items.len),
            });
        }
        return out.toOwnedSlice(a);
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "the fake clock only moves when a test moves it" {
    var fc: FakeClock = .{ .t = 1000 };
    const c = fc.clock();
    try testing.expectEqual(@as(Timestamp, 1000), c.now());
    try testing.expectEqual(@as(Timestamp, 1000), c.now());
    fc.advance(250);
    try testing.expectEqual(@as(Timestamp, 1250), c.now());
    fc.set(7);
    try testing.expectEqual(@as(Timestamp, 7), c.now());
}

const TinyEvent = union(enum) {
    a: struct { at: Timestamp = 0 },
    b: struct { at: Timestamp = 0 },

    pub fn topic(self: TinyEvent) []const u8 {
        return switch (self) {
            .a => "t.a",
            .b => "t.b",
        };
    }
};

test "a sink records topics in order and counts batches" {
    var fake: FakeSink(TinyEvent) = .{};
    const s = fake.sink();
    try s.publish(null, &.{ .{ .a = .{} }, .{ .b = .{} } });
    try s.publish(null, &.{.{ .a = .{} }});
    // An empty batch never reaches the adapter — the Go code guarded
    // this at every call site; here the port does it once.
    try s.publish(null, &.{});

    try testing.expectEqual(@as(usize, 2), fake.batches);
    try testing.expectEqualDeep(@as([]const []const u8, &.{ "t.a", "t.b", "t.a" }), fake.seen());
    try testing.expectEqual(@as(usize, 2), fake.count("t.a"));
    try testing.expect(fake.has("t.b"));
    try testing.expect(!fake.has("t.c"));
}

test "a sink failure surfaces once and then clears" {
    var fake: FakeSink(TinyEvent) = .{ .fail = error.Backend };
    const s = fake.sink();
    try testing.expectError(error.Backend, s.publish(null, &.{.{ .a = .{} }}));
    try s.publish(null, &.{.{ .a = .{} }});
    try testing.expectEqual(@as(usize, 1), fake.n);
}

test "the fake transaction manager balances its boundaries" {
    var ftx: FakeTx = .{};
    var u = try ftx.manager().begin();
    try u.commit();
    var second = try ftx.manager().begin();
    second.rollback();
    try testing.expectEqual(@as(u32, 2), ftx.begins);
    try testing.expectEqual(@as(u32, 1), ftx.commits);
    try testing.expectEqual(@as(u32, 1), ftx.rollbacks);
    try testing.expect(ftx.balanced());
}

test "writeAt grows the file and honours the preallocation floor" {
    var fs = FakeFs.init(testing.allocator);
    defer fs.deinit();
    const f = fs.filesystem();

    try f.mkdirAll("/inc/7");
    try f.writeAt("/inc/7/1.tmp", 4, "abcd", 16);
    try testing.expectEqual(@as(?usize, 16), fs.fileSize("/inc/7/1.tmp"));
    try testing.expectEqualStrings("abcd", fs.contents("/inc/7/1.tmp").?[4..8]);
    // A second segment lands at its own offset without disturbing the
    // first — this is the idempotent-WriteAt property crash recovery
    // depends on.
    try f.writeAt("/inc/7/1.tmp", 0, "ZZZZ", 16);
    try testing.expectEqualStrings("ZZZZabcd", fs.contents("/inc/7/1.tmp").?[0..8]);
    try testing.expectEqual(@as(?usize, 16), fs.fileSize("/inc/7/1.tmp"));
}

test "writeAt refuses to create a file under a missing directory" {
    var fs = FakeFs.init(testing.allocator);
    defer fs.deinit();
    try testing.expectError(error.NotFound, fs.filesystem().writeAt("/nope/1.tmp", 0, "x", 0));
}

test "move renames a whole subtree and refuses to clobber" {
    var fs = FakeFs.init(testing.allocator);
    defer fs.deinit();
    const f = fs.filesystem();
    try fs.addFile("/a/inner/x.mkv", 10);
    try fs.addFile("/a/inner/y.nfo", 2);

    try f.move("/a/inner", "/a/outer");
    try testing.expect(fs.has("/a/outer/x.mkv"));
    try testing.expect(!fs.has("/a/inner"));

    try fs.addFile("/a/taken", 1);
    try testing.expectError(error.Exists, f.move("/a/outer/x.mkv", "/a/taken"));
    try testing.expectError(error.NotFound, f.move("/a/ghost", "/a/whatever"));
}

test "list reports direct children only, removeAll takes the subtree" {
    var fs = FakeFs.init(testing.allocator);
    defer fs.deinit();
    const f = fs.filesystem();
    try fs.addFile("/r/top.mkv", 5);
    try fs.addFile("/r/sub/nested.mkv", 6);

    const kids = try f.list(testing.allocator, "/r");
    defer {
        for (kids) |k| testing.allocator.free(k.name);
        testing.allocator.free(kids);
    }
    try testing.expectEqual(@as(usize, 2), kids.len);
    var saw_file = false;
    var saw_dir = false;
    for (kids) |k| {
        if (std.mem.eql(u8, k.name, "top.mkv")) {
            saw_file = true;
            try testing.expectEqual(@as(i64, 5), k.size);
        }
        if (std.mem.eql(u8, k.name, "sub")) {
            saw_dir = true;
            try testing.expect(k.is_dir);
        }
    }
    try testing.expect(saw_file and saw_dir);

    try f.removeAll("/r/sub");
    try testing.expect(!fs.has("/r/sub/nested.mkv"));
    try testing.expect(fs.has("/r/top.mkv"));
    // Removing something absent is success, matching os.RemoveAll.
    try f.removeAll("/r/sub");
}

test "exists and isDir answer without leaking the error" {
    var fs = FakeFs.init(testing.allocator);
    defer fs.deinit();
    const f = fs.filesystem();
    try fs.addFile("/d/f", 1);
    try testing.expect(f.exists("/d/f"));
    try testing.expect(!f.exists("/d/ghost"));
    try testing.expect(f.isDir("/d"));
    try testing.expect(!f.isDir("/d/f"));
    try testing.expect(!f.isDir("/d/ghost"));
}

test "a toggle is read fresh on every call" {
    var ft: FakeToggle = .{};
    const t = ft.toggle();
    try testing.expect(!t.read());
    ft.on = true;
    try testing.expect(t.read());
    try testing.expectEqual(@as(usize, 2), ft.reads);

    var fk: FakeKnob = .{ .v = 3 };
    const k = fk.knob();
    try testing.expectEqual(@as(i64, 3), k.read());
    fk.v = 0;
    try testing.expectEqual(@as(i64, 0), k.read());
}

/// A minimal aggregate for exercising the generic repository.
const TinyAggregate = struct {
    gpa: Allocator,
    id: i64 = 0,
    job_id: i64,

    fn setId(self: *TinyAggregate, id: i64) void {
        self.id = id;
    }

    fn deinit(self: *TinyAggregate) void {
        self.* = undefined;
    }
};

test "the generic repository assigns ids, finds by job and reports misses" {
    var fake = FakeRepo(TinyAggregate).init(testing.allocator);
    defer fake.deinit();
    const r = fake.repo();

    const item = try testing.allocator.create(TinyAggregate);
    item.* = .{ .gpa = testing.allocator, .job_id = 7 };
    try r.save(null, item);
    try testing.expectEqual(@as(i64, 1), item.id);
    try testing.expectEqual(@as(usize, 1), fake.len());

    const found = try r.byJobId(null, 7);
    try testing.expectEqual(item, found);
    r.release(found);
    try testing.expectError(error.NotFound, r.byJobId(null, 8));
    try testing.expect(fake.leakFree());

    // A second save of the same aggregate updates rather than inserting.
    try r.save(null, item);
    try testing.expectEqual(@as(usize, 1), fake.len());
    try testing.expectEqual(@as(usize, 2), fake.saves);
}

test "category lookup answers null for unknown and reserved names" {
    var fc: FakeCategories = .{ .rows = &.{
        .{ .name = "tv", .dir = "TV" },
        .{ .name = "movies", .dir = "" },
    } };
    const c = fc.categories();
    try testing.expectEqualStrings("TV", c.dirFor("tv").?);
    // A known category with an empty dir still resolves — it just means
    // "straight into complete/".
    try testing.expectEqualStrings("", c.dirFor("movies").?);
    try testing.expectEqual(@as(?[]const u8, null), c.dirFor("*"));
    try testing.expectEqual(@as(?[]const u8, null), c.dirFor("nope"));
}
