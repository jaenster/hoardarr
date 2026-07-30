//! Unit of work: the contract that binds a set of repository writes and
//! the domain events they produced into one atomic commit.
//!
//! This is the load-bearing half of the transactional-outbox pattern.
//! An aggregate's state change and the events it emitted must land in
//! the same transaction, or a crash between the two leaves the system
//! with a state nobody was ever told about (or a notification for
//! something that didn't happen).
//!
//! # Why a vtable here, and nowhere else in the domain
//!
//! `download/ports.zig` argues — correctly — that domain-side vtables
//! are a Go habit that costs an indirect call per operation for no
//! benefit. The exception is a boundary crossed *once per transaction*:
//! the indirect call is amortised over every write inside it, and the
//! app layer genuinely must not know whether it is talking to SQLite,
//! Postgres, or the in-memory double used by tests. So `Manager` erases
//! its backend, and `Unit` is the handle the repositories thread
//! through instead of Go's `context.Context`.
//!
//! Explicit threading is the real gain over the Go original. There, the
//! transaction rode invisibly on a context key, so "did this repository
//! call join the transaction?" was answered at runtime, by whether the
//! caller happened to pass the right ctx. Here `*Unit` is a parameter:
//! a write that forgot the transaction does not compile.
//!
//! # Errors
//!
//! Backends have their own error sets, and a public signature must not
//! be `anyerror` (PORT.md). So the vtable speaks this fixed set and the
//! adapter maps into it, logging the backend detail on the way past.

const std = @import("std");

/// Everything a transaction boundary can go wrong with.
///
/// `Conflict` is the one callers branch on: a serialisation failure or
/// busy-timeout expiry that the caller may legitimately retry, as
/// opposed to `Backend`, which means the adapter has already logged
/// something the operator has to look at.
pub const Error = error{
    /// BEGIN failed — no transaction was started, nothing to roll back.
    Begin,
    /// COMMIT failed. The transaction is finished and the writes were
    /// discarded; the backend has already rolled back.
    Commit,
    /// A lock/serialisation conflict. Retryable.
    Conflict,
    /// Anything else the backend reported.
    Backend,
};

/// An open transaction. Repositories take `*Unit` and hand it to their
/// backend; the domain itself never looks inside.
///
/// Exactly one of `commit` or `rollback` must be called, and neither
/// may be called twice — `finished` makes the second call a caught
/// programmer error rather than backend-defined behaviour.
pub const Unit = struct {
    ctx: *anyopaque,
    vtable: *const VTable,
    /// Set by the first `commit`/`rollback`. Guards double-finish.
    finished: bool = false,
    /// Incremented by `join`. A nested `inTx` on the same unit does not
    /// open a savepoint (v0.1 keeps nesting flat, as Go did); it just
    /// makes the inner scope a no-op at commit time.
    depth: u8 = 0,

    pub const VTable = struct {
        commit: *const fn (ctx: *anyopaque) Error!void,
        /// Rollback cannot fail usefully — if it does, the connection is
        /// poisoned and the adapter's job is to log and drop it.
        rollback: *const fn (ctx: *anyopaque) void,
    };

    pub fn commit(self: *Unit) Error!void {
        std.debug.assert(!self.finished);
        self.finished = true;
        return self.vtable.commit(self.ctx);
    }

    pub fn rollback(self: *Unit) void {
        std.debug.assert(!self.finished);
        self.finished = true;
        self.vtable.rollback(self.ctx);
    }

    pub fn isFinished(self: *const Unit) bool {
        return self.finished;
    }
};

/// Opens transactions. One per backend; the app layer holds it for the
/// process lifetime.
pub const Manager = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        begin: *const fn (ctx: *anyopaque) Error!Unit,
    };

    pub fn begin(self: Manager) Error!Unit {
        return self.vtable.begin(self.ctx);
    }
};

/// Runs `body` inside a fresh transaction: commit on success, rollback
/// on any error or on an early `return`.
///
/// `body` is comptime-known, so there is no closure and no allocation;
/// its error set is a parameter rather than `anyerror`, so the composite
/// `BodyError || Error` stays exhaustive and a caller can still `switch`
/// on its own errors after the transaction. Go's version collapsed
/// everything into the bare `error` interface and lost that.
pub fn inTx(
    comptime BodyError: type,
    m: Manager,
    args: anytype,
    comptime body: fn (unit: *Unit, args: @TypeOf(args)) BodyError!void,
) (BodyError || Error)!void {
    var unit = try m.begin();
    body(&unit, args) catch |err| {
        unit.rollback();
        return err;
    };
    return unit.commit();
}

/// Marks a nested scope as joining the transaction already in flight,
/// rather than opening a second one. Returns true when this call is the
/// outermost scope and therefore owns the commit.
///
/// Nesting is flat: no savepoints in v0.1, matching the Go behaviour.
/// An inner scope that fails must propagate the error so the outer
/// scope rolls the whole thing back.
pub fn join(unit: *Unit) bool {
    const outermost = unit.depth == 0;
    unit.depth += 1;
    return outermost;
}

pub fn leave(unit: *Unit) void {
    std.debug.assert(unit.depth > 0);
    unit.depth -= 1;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// In-memory backend that records what it was asked to do. Stands in
/// for Go's `adapter/eventbus/memory` no-op TransactionManager, except
/// it can also be told to fail at each boundary.
const FakeBackend = struct {
    begins: u32 = 0,
    commits: u32 = 0,
    rollbacks: u32 = 0,
    /// Writes applied inside the current transaction. Cleared on
    /// rollback so a test can assert "nothing was observable".
    writes: u32 = 0,
    committed_writes: u32 = 0,

    fail_begin: bool = false,
    fail_commit: ?Error = null,

    fn beginFn(ctx: *anyopaque) Error!Unit {
        const self: *FakeBackend = @ptrCast(@alignCast(ctx));
        if (self.fail_begin) return error.Begin;
        self.begins += 1;
        self.writes = 0;
        return .{ .ctx = ctx, .vtable = &unit_vtable };
    }

    fn commitFn(ctx: *anyopaque) Error!void {
        const self: *FakeBackend = @ptrCast(@alignCast(ctx));
        if (self.fail_commit) |e| {
            self.rollbacks += 1;
            self.writes = 0;
            return e;
        }
        self.commits += 1;
        self.committed_writes += self.writes;
        self.writes = 0;
    }

    fn rollbackFn(ctx: *anyopaque) void {
        const self: *FakeBackend = @ptrCast(@alignCast(ctx));
        self.rollbacks += 1;
        self.writes = 0;
    }

    const unit_vtable: Unit.VTable = .{ .commit = commitFn, .rollback = rollbackFn };
    const manager_vtable: Manager.VTable = .{ .begin = beginFn };

    fn manager(self: *FakeBackend) Manager {
        return .{ .ctx = self, .vtable = &manager_vtable };
    }

    /// Simulates a repository write joining the ambient transaction.
    fn write(self: *FakeBackend, unit: *Unit) void {
        std.debug.assert(!unit.isFinished());
        self.writes += 1;
    }
};

test "inTx commits when the body succeeds" {
    const t = std.testing;
    var be: FakeBackend = .{};

    const Body = struct {
        fn run(unit: *Unit, backend: *FakeBackend) error{}!void {
            backend.write(unit);
            backend.write(unit);
        }
    };
    try inTx(error{}, be.manager(), &be, Body.run);

    try t.expectEqual(@as(u32, 1), be.begins);
    try t.expectEqual(@as(u32, 1), be.commits);
    try t.expectEqual(@as(u32, 0), be.rollbacks);
    try t.expectEqual(@as(u32, 2), be.committed_writes);
}

test "inTx rolls back and preserves the body's error type" {
    const t = std.testing;
    var be: FakeBackend = .{};

    const Body = struct {
        const E = error{DomainInvariantViolated};
        fn run(unit: *Unit, backend: *FakeBackend) E!void {
            backend.write(unit);
            return error.DomainInvariantViolated;
        }
    };
    try t.expectError(error.DomainInvariantViolated, inTx(Body.E, be.manager(), &be, Body.run));

    try t.expectEqual(@as(u32, 1), be.begins);
    try t.expectEqual(@as(u32, 0), be.commits);
    try t.expectEqual(@as(u32, 1), be.rollbacks);
    // The load-bearing assertion: no partial state is observable.
    try t.expectEqual(@as(u32, 0), be.committed_writes);
}

test "inTx surfaces a failed begin without touching the body" {
    const t = std.testing;
    var be: FakeBackend = .{ .fail_begin = true };

    const Body = struct {
        fn run(unit: *Unit, backend: *FakeBackend) error{}!void {
            backend.write(unit);
        }
    };
    try t.expectError(error.Begin, inTx(error{}, be.manager(), &be, Body.run));
    try t.expectEqual(@as(u32, 0), be.begins);
    try t.expectEqual(@as(u32, 0), be.writes);
    try t.expectEqual(@as(u32, 0), be.rollbacks);
}

test "a failed commit discards the writes" {
    const t = std.testing;
    var be: FakeBackend = .{ .fail_commit = error.Conflict };

    const Body = struct {
        fn run(unit: *Unit, backend: *FakeBackend) error{}!void {
            backend.write(unit);
        }
    };
    try t.expectError(error.Conflict, inTx(error{}, be.manager(), &be, Body.run));
    try t.expectEqual(@as(u32, 0), be.committed_writes);
}

test "a unit reports itself finished exactly once" {
    const t = std.testing;
    var be: FakeBackend = .{};
    var unit = try be.manager().begin();
    try t.expect(!unit.isFinished());
    try unit.commit();
    try t.expect(unit.isFinished());
}

test "nested joins are flat and only the outermost owns the commit" {
    const t = std.testing;
    var be: FakeBackend = .{};
    var unit = try be.manager().begin();

    try t.expect(join(&unit)); // outer
    try t.expect(!join(&unit)); // inner joins, does not commit
    leave(&unit);
    leave(&unit);
    try t.expectEqual(@as(u8, 0), unit.depth);

    try unit.commit();
    try t.expectEqual(@as(u32, 1), be.commits);
}
