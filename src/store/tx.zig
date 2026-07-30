//! Transactions: `BEGIN IMMEDIATE`, nesting-by-joining, post-commit
//! hooks, and replay on lock contention.
//!
//! ## Why the connection carries the transaction
//!
//! The Go build threaded the active `*sql.Tx` through a
//! `context.Context` so a repository method could join whatever
//! transaction its caller had open. Zig has no ambient context, and it
//! does not need one: a connection is owned by a single thread
//! (`sqlite.zig`), so "the transaction open on this connection" *is*
//! the ambient transaction. `Conn.tx_depth` holds it, and a repository
//! that writes through the connection automatically participates.
//!
//! ## Nesting
//!
//! An inner `inTx` joins the outer one — it runs the body with no SQL
//! of its own, and the outermost frame decides commit or rollback.
//! There are no savepoints: a nested rollback in this system always
//! means "abandon the whole unit of work", and partial rollback would
//! let a caller commit half an aggregate.
//!
//! ## Post-commit hooks
//!
//! `onCommit` defers a side-effect until after `COMMIT` returns. This
//! is not a convenience — it is a correctness fix. The outbox bus
//! nudges its dispatcher threads when it publishes an event, and until
//! the outer transaction commits those `INSERT`s are invisible to every
//! other connection. A dispatcher woken too early queries, sees
//! nothing, and goes back to sleep until its next poll tick — which at
//! the 5-second production interval turned a sub-millisecond handoff
//! into a five-second stall, and tripped the end-to-end suite about two
//! runs in three. Hooks run only on success; a rolled-back transaction
//! runs none.
//!
//! ## Replay
//!
//! Two result codes mean "the transaction did not happen, try again":
//!
//!   * `SQLITE_BUSY` (5) — lost the race for the write lock after
//!     `busy_timeout` expired.
//!   * `SQLITE_BUSY_SNAPSHOT` (517) — a read snapshot went stale under
//!     us. `busy_timeout` never clears this one; only rolling back and
//!     replaying does.
//!
//! `BEGIN IMMEDIATE` makes the second case rare (the write lock is
//! claimed before any read), but "rare" is not "never" when several
//! writer threads share one file, so `inTx` replays either way.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const sys = @import("../posix/sys.zig");

const Conn = sqlite.Conn;

/// Cap on replays before `inTx` gives up with the last error.
///
/// Twenty attempts with the backoff below stagger over ~2.5 s in the
/// worst case. That absorbs the bursty pattern seen when the
/// orchestrator's drainer, the outbox dispatchers and a handful of
/// mutating API requests all want the write lock inside the same few
/// milliseconds, and it stays far below the outer bound that actually
/// matters — the HTTP client's timeout.
pub const max_retries: usize = 20;

const backoff_base_ns: u64 = std.time.ns_per_ms;
const backoff_cap_ns: u64 = 250 * std.time.ns_per_ms;

/// Open a transaction, or join the one already open on `conn`.
///
/// `BEGIN IMMEDIATE`, never a bare `BEGIN`: the default deferred
/// behaviour opens a read snapshot and only upgrades to a write lock on
/// the first mutation, at which point a concurrent commit surfaces as
/// `SQLITE_BUSY_SNAPSHOT` — a code `busy_timeout` cannot wait out.
/// Claiming the lock upfront turns writer contention back into the
/// bounded wait that `busy_timeout` is designed for.
pub fn begin(conn: *Conn) sqlite.Error!void {
    if (conn.tx_depth > 0) {
        conn.tx_depth += 1;
        return;
    }
    // `execute` rather than `exec`: it goes through the statement cache, so
    // opening a transaction costs a `sqlite3_reset` instead of a parse plus
    // a NUL-terminated copy of the SQL. The orchestrator's drainer opens
    // one of these every few hundred milliseconds and every repository
    // write is wrapped in one, so this is a hot path.
    try conn.execute("BEGIN IMMEDIATE", .{});
    conn.tx_depth = 1;
}

/// Close the current nesting level. The outermost level commits and
/// then runs the queued post-commit hooks.
pub fn commit(conn: *Conn) sqlite.Error!void {
    std.debug.assert(conn.tx_depth > 0);
    if (conn.tx_depth > 1) {
        conn.tx_depth -= 1;
        return;
    }
    try conn.execute("COMMIT", .{});
    conn.tx_depth = 0;
    runHooks(conn);
}

/// Abandon the whole transaction regardless of nesting depth, and drop
/// every queued hook unrun.
///
/// Errors are swallowed deliberately: this is the unwind path, the
/// caller already has the error that brought it here, and a failing
/// `ROLLBACK` means the transaction is gone anyway.
pub fn rollback(conn: *Conn) void {
    if (conn.tx_depth == 0) return;
    conn.tx_depth = 0;
    conn.execute("ROLLBACK", .{}) catch {};
    conn.commit_hooks.clearRetainingCapacity();
}

/// Register `run(ctx)` to fire after the outermost transaction commits.
/// With no transaction open it fires immediately, which keeps callers
/// from having to care whether they are inside one.
pub fn onCommit(
    conn: *Conn,
    ctx: ?*anyopaque,
    run: *const fn (?*anyopaque) void,
) std.mem.Allocator.Error!void {
    if (conn.tx_depth == 0) {
        run(ctx);
        return;
    }
    try conn.commit_hooks.append(conn.gpa, .{ .ctx = ctx, .run = run });
}

fn runHooks(conn: *Conn) void {
    // Snapshot the length first: a hook may open its own transaction and
    // register more hooks, and those belong to that transaction, not to
    // the one just committed.
    const n = conn.commit_hooks.items.len;
    for (conn.commit_hooks.items[0..n]) |hook| hook.run(hook.ctx);
    conn.commit_hooks.replaceRange(conn.gpa, 0, n, &.{}) catch {
        conn.commit_hooks.clearRetainingCapacity();
    };
}

/// Run `body(ctx, conn)` inside a transaction, replaying it on lock
/// contention.
///
/// `body` returning an error rolls back and the error propagates
/// unchanged — a domain error must not be mistaken for a storage
/// failure. When `conn` already has a transaction open, `body` simply
/// joins it: no replay (the outer frame owns the outcome), and the error
/// propagates so the outer frame can roll back.
///
/// The error set is inferred because it is the union of `sqlite.Error`
/// and whatever `body` returns.
pub fn inTx(conn: *Conn, ctx: anytype, comptime body: anytype) !void {
    if (conn.tx_depth > 0) {
        // Joined: count the level so a nested commit is symmetric with
        // its begin, but let the outer frame decide the outcome.
        try begin(conn);
        errdefer rollback(conn);
        try body(ctx, conn);
        try commit(conn);
        return;
    }

    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        const failure = attempt: {
            begin(conn) catch |e| break :attempt e;
            body(ctx, conn) catch |e| {
                rollback(conn);
                break :attempt e;
            };
            commit(conn) catch |e| {
                rollback(conn);
                break :attempt e;
            };
            return;
        };

        // Only storage-level contention is replayable. Anything else —
        // a constraint violation, an I/O error, a domain error from the
        // body — is returned as-is.
        const retryable = switch (@as(anyerror, failure)) {
            error.Busy, error.BusySnapshot, error.Locked => true,
            else => false,
        };
        if (!retryable or attempt + 1 >= max_retries) return failure;

        // Exponential backoff, capped. A fixed 1 ms spin (the shape this
        // started as) does not give the other writer room to finish on a
        // loaded machine, and the collision then just repeats.
        const shift: u6 = @intCast(@min(attempt, 8));
        sys.sleep(@min(backoff_base_ns << shift, backoff_cap_ns));
    }
}

/// True when `conn` has a transaction open. The Zig equivalent of Go's
/// `TxFromContext(ctx) != nil`.
pub fn inTransaction(conn: *const Conn) bool {
    return conn.tx_depth > 0;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;
const support = sqlite.testing_support;

fn scratch() !*Conn {
    const conn = try support.openMemory();
    try conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)");
    return conn;
}

fn insertOne(_: void, conn: *Conn) !void {
    try conn.execute("INSERT INTO t(v) VALUES (?)", .{"x"});
}

test "a committed transaction persists its writes" {
    const conn = try scratch();
    defer conn.close();

    try inTx(conn, {}, insertOne);
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM t", .{}));
    try t.expect(!inTransaction(conn));
}

test "a body error rolls back and propagates unchanged" {
    const conn = try scratch();
    defer conn.close();

    const failing = struct {
        fn run(_: void, cn: *Conn) !void {
            try cn.execute("INSERT INTO t(v) VALUES (?)", .{"x"});
            return error.CallerChangedTheirMind;
        }
    }.run;

    try t.expectError(error.CallerChangedTheirMind, inTx(conn, {}, failing));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM t", .{}));
    try t.expect(!inTransaction(conn));
}

test "a nested transaction joins the outer one" {
    const conn = try scratch();
    defer conn.close();

    const outer = struct {
        fn run(_: void, cn: *Conn) !void {
            try t.expect(inTransaction(cn));
            try inTx(cn, {}, insertOne);
            // Still inside the outer transaction after the inner one
            // "commits" — the inner commit only decremented the depth.
            try t.expect(inTransaction(cn));
        }
    }.run;

    try inTx(conn, {}, outer);
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM t", .{}));
}

test "an inner error rolls the whole outer transaction back" {
    const conn = try scratch();
    defer conn.close();

    const nest = struct {
        fn inner(_: void, cn: *Conn) !void {
            try cn.execute("INSERT INTO t(v) VALUES (?)", .{"inner"});
            return error.Nope;
        }
        fn outer(_: void, cn: *Conn) !void {
            try cn.execute("INSERT INTO t(v) VALUES (?)", .{"outer"});
            try inTx(cn, {}, inner);
        }
    };

    try t.expectError(error.Nope, inTx(conn, {}, nest.outer));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM t", .{}));
    try t.expect(!inTransaction(conn));
}

test "begin uses IMMEDIATE so the write lock is claimed upfront" {
    // Two connections onto one file. The first claims the write lock in
    // BEGIN itself, so the second's BEGIN fails with BUSY *before* it
    // has read anything — which is the whole point of IMMEDIATE over the
    // deferred default, where the failure would instead land on the
    // first write as BUSY_SNAPSHOT.
    const path = try support.tempPath(t.allocator);
    defer t.allocator.free(path);
    defer support.removeTempFile(t.allocator, path);

    const a = try sqlite.Conn.open(t.allocator, path, .{ .busy_timeout_ms = 0 });
    defer a.close();
    try a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    const b = try sqlite.Conn.open(t.allocator, path, .{ .busy_timeout_ms = 0 });
    defer b.close();

    try begin(a);
    try a.execute("INSERT INTO t(v) VALUES (?)", .{"first"});
    try t.expectError(error.Busy, begin(b));
    try commit(a);

    // Lock released: b can now take it.
    try begin(b);
    try commit(b);
}

test "inTx gives up after max_retries against a permanently held lock" {
    const path = try support.tempPath(t.allocator);
    defer t.allocator.free(path);
    defer support.removeTempFile(t.allocator, path);

    const a = try sqlite.Conn.open(t.allocator, path, .{ .busy_timeout_ms = 0 });
    defer a.close();
    try a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)");

    const b = try sqlite.Conn.open(t.allocator, path, .{ .busy_timeout_ms = 0 });
    defer b.close();

    try begin(a);
    try a.execute("INSERT INTO t(v) VALUES (?)", .{"held"});

    // The assertion is that it terminates with the retryable error
    // rather than hanging. The backoff sums to ~2.5 s worst case, which
    // is real but bounded.
    try t.expectError(error.Busy, inTx(b, {}, insertOne));
    try t.expect(!inTransaction(b));
    try commit(a);
}

test "post-commit hooks run after COMMIT, in registration order" {
    const conn = try scratch();
    defer conn.close();

    const Observer = struct {
        conn: *Conn,
        order: [3]u8 = .{ 0, 0, 0 },
        n: usize = 0,
        rows_when_first_ran: i64 = -1,

        fn note(self: *@This(), tag: u8) void {
            if (self.n == 0) {
                // The row must already be visible: that is the property
                // the whole hook mechanism exists to guarantee.
                self.rows_when_first_ran =
                    self.conn.scalarInt("SELECT COUNT(*) FROM t", .{}) catch -1;
            }
            self.order[self.n] = tag;
            self.n += 1;
        }
        fn a(p: ?*anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(p.?))).note('a');
        }
        fn b(p: ?*anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(p.?))).note('b');
        }
        fn c(p: ?*anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(p.?))).note('c');
        }
    };
    var obs = Observer{ .conn = conn };

    const body = struct {
        fn run(o: *Observer, cn: *Conn) !void {
            try cn.execute("INSERT INTO t(v) VALUES (?)", .{"x"});
            try onCommit(cn, o, Observer.a);
            try onCommit(cn, o, Observer.b);
            try onCommit(cn, o, Observer.c);
            // Nothing has fired yet — the rows are still invisible.
            try t.expectEqual(@as(usize, 0), o.n);
        }
    }.run;

    try inTx(conn, &obs, body);
    try t.expectEqual(@as(usize, 3), obs.n);
    try t.expectEqualSlices(u8, "abc", &obs.order);
    try t.expectEqual(@as(i64, 1), obs.rows_when_first_ran);
}

test "a rolled-back transaction runs no hooks" {
    const conn = try scratch();
    defer conn.close();

    const Flag = struct {
        fired: bool = false,
        fn set(p: ?*anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(p.?))).fired = true;
        }
    };
    var flag = Flag{};

    const body = struct {
        fn run(f: *Flag, cn: *Conn) !void {
            try onCommit(cn, f, Flag.set);
            return error.Abandon;
        }
    }.run;

    try t.expectError(error.Abandon, inTx(conn, &flag, body));
    try t.expect(!flag.fired);
    try t.expectEqual(@as(usize, 0), conn.commit_hooks.items.len);
}

test "onCommit outside a transaction runs immediately" {
    const conn = try scratch();
    defer conn.close();

    const Flag = struct {
        fired: bool = false,
        fn set(p: ?*anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(p.?))).fired = true;
        }
    };
    var flag = Flag{};
    try onCommit(conn, &flag, Flag.set);
    try t.expect(flag.fired);
}

test "hooks registered by a hook belong to their own transaction" {
    const conn = try scratch();
    defer conn.close();

    const Nested = struct {
        conn: *Conn,
        outer_ran: bool = false,
        inner_ran: bool = false,

        fn outerHook(p: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(p.?));
            self.outer_ran = true;
            // No transaction is open by now, so this runs immediately
            // rather than being left dangling in the outer queue.
            onCommit(self.conn, p, innerHook) catch {};
        }
        fn innerHook(p: ?*anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(p.?))).inner_ran = true;
        }
    };
    var n = Nested{ .conn = conn };

    const body = struct {
        fn run(s: *Nested, cn: *Conn) !void {
            try onCommit(cn, s, Nested.outerHook);
        }
    }.run;

    try inTx(conn, &n, body);
    try t.expect(n.outer_ran);
    try t.expect(n.inner_ran);
    try t.expectEqual(@as(usize, 0), conn.commit_hooks.items.len);
}

test "a transaction is invisible to another connection until it commits" {
    const path = try support.tempPath(t.allocator);
    defer t.allocator.free(path);
    defer support.removeTempFile(t.allocator, path);

    const w = try sqlite.Conn.open(t.allocator, path, .{});
    defer w.close();
    try w.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    const r = try sqlite.Conn.open(t.allocator, path, .{});
    defer r.close();

    try begin(w);
    try w.execute("INSERT INTO t(v) VALUES (?)", .{"pending"});
    // WAL: the reader keeps its own snapshot and sees nothing.
    try t.expectEqual(@as(i64, 0), try r.scalarInt("SELECT COUNT(*) FROM t", .{}));
    try commit(w);
    try t.expectEqual(@as(i64, 1), try r.scalarInt("SELECT COUNT(*) FROM t", .{}));
}
