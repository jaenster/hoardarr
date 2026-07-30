//! Ergonomic SQLite wrapper: connections, pragmas, prepared-statement
//! caching, typed bind/column helpers, and result-code → error-set
//! mapping.
//!
//! ## Threading
//!
//! The amalgamation is compiled with `SQLITE_THREADSAFE=2`
//! (multi-thread, *not* serialized): the library is safe to use from
//! several threads, but **a single connection must never be touched by
//! two threads**. That is modelled directly here — a `Conn` is owned by
//! the thread that opened it, and in Debug builds every entry point
//! asserts the caller is that owner. There is deliberately no pool and
//! no per-call mutex: a thread that needs the database opens its own
//! connection and keeps it for its lifetime, which is what the compile
//! flag is *for*. Handing connections between threads would mean a
//! mutex on the hot path and would buy nothing, because WAL already
//! lets the readers run concurrently with the writer.
//!
//! The one place this layer does need a lock is the outbox's
//! subscription registry, which several dispatcher threads read while
//! `Publish` writes — see `outbox.zig`. The mutex lives there, not
//! here.
//!
//! ## Pragmas
//!
//! WAL, `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=5000`,
//! `cache_size=-65536` (64 MiB). These are load-bearing, not
//! decoration:
//!
//!   * WAL is what allows the API's readers to run while the
//!     orchestrator's drainer and the outbox dispatchers write.
//!   * `busy_timeout` turns writer contention into a bounded wait
//!     instead of an immediate `SQLITE_BUSY`.
//!   * every `BEGIN` is a `BEGIN IMMEDIATE` (see `tx.zig`), so a
//!     transaction claims the write lock upfront rather than opening a
//!     read snapshot and discovering on its first write that another
//!     connection moved the database on — which surfaces as
//!     `SQLITE_BUSY_SNAPSHOT` (517), a code `busy_timeout` cannot help
//!     with. The Go build got this from the driver's
//!     `_txlock=immediate` DSN parameter; in C there is no DSN, so the
//!     statement text carries it.
//!
//! Changing any of them silently causes `SQLITE_BUSY` storms under
//! load, so `Options` defaults to the documented set and the tests pin
//! it.
//!
//! ## Statement cache
//!
//! Preparing a statement is the single biggest avoidable cost in a
//! SQLite-backed hot loop — `sqlite3_prepare_v3` parses SQL and runs
//! the query planner, which for the orchestrator's per-segment UPDATE
//! dwarfs the actual write. Every `prepare` here goes through a
//! per-connection cache keyed by SQL text; `Stmt.release` does
//! `sqlite3_reset` + `sqlite3_clear_bindings` and hands the handle back
//! to the cache instead of finalizing it.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const c = @import("sqlite_c.zig").c;
const sys = @import("../posix/sys.zig");

/// Wall-clock milliseconds since the Unix epoch.
///
/// Every timestamp column in the schema is unix-milliseconds, so the
/// nanosecond→millisecond conversion lives at this one boundary instead
/// of in each repository. Wall clock, not monotonic: these values are
/// persisted and shown to humans.
pub fn nowMillis() i64 {
    return @intCast(@divTrunc(sys.realtimeNanos(), std.time.ns_per_ms));
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Every failure mode this layer surfaces, split so callers can react
/// rather than just log.
///
/// The retryable/permanent split is the important one. `Busy`,
/// `Locked` and `BusySnapshot` mean "try again"; a constraint violation
/// means "the caller's data is wrong and retrying will fail
/// identically"; `IoFailure` / `Corrupt` mean the disk is in trouble.
/// `tx.zig` replays transactions on exactly the first group.
pub const Error = error{
    /// SQLITE_BUSY — lost a race for the write lock after
    /// `busy_timeout` expired. Retryable.
    Busy,
    /// SQLITE_BUSY_SNAPSHOT (517) — this transaction opened a read
    /// snapshot and then tried to write after another connection
    /// committed. `busy_timeout` never clears it; only rolling back and
    /// replaying does. Retryable.
    BusySnapshot,
    /// SQLITE_LOCKED — a conflict inside the same database handle
    /// (table locked by another statement). Retryable.
    Locked,

    /// SQLITE_CONSTRAINT with no more specific extended code.
    Constraint,
    /// UNIQUE (or PRIMARY KEY) violation. Callers map this onto their
    /// own "already exists" sentinels.
    ConstraintUnique,
    ConstraintForeignKey,
    ConstraintNotNull,
    ConstraintCheck,

    /// SQLITE_IOERR family — the filesystem failed.
    IoFailure,
    /// SQLITE_CORRUPT / SQLITE_NOTADB — the file is not a usable
    /// database.
    Corrupt,
    /// Disk or database is full.
    Full,
    /// The database (or a table) is read-only.
    ReadOnly,
    /// Could not open the file at all.
    CantOpen,
    /// Datatype mismatch, range error, or a bad bind index — all
    /// programming errors in the calling code.
    Mismatch,
    /// SQLITE_MISUSE — this wrapper used the C API wrongly. A bug here,
    /// never in the caller's data.
    Misuse,
    /// SQLITE_ERROR: SQL syntax error, missing table, unknown column.
    Sql,
    /// A result code we do not model. Kept distinct from `Sql` so an
    /// unexpected code is visibly unexpected in a log.
    Unexpected,

    /// The statement returned no rows where the caller required one.
    /// Not a SQLite result code — the wrapper's equivalent of Go's
    /// `sql.ErrNoRows`.
    NoRows,

    OutOfMemory,
};

/// True for the codes that clear on a retry. `tx.zig` uses this to
/// decide whether to replay a transaction body.
pub fn isRetryable(e: Error) bool {
    return switch (e) {
        error.Busy, error.BusySnapshot, error.Locked => true,
        else => false,
    };
}

/// True for the codes that mean "the caller's data violates the
/// schema". Retrying is pointless; the caller should translate to a
/// domain-level error.
pub fn isConstraint(e: Error) bool {
    return switch (e) {
        error.Constraint,
        error.ConstraintUnique,
        error.ConstraintForeignKey,
        error.ConstraintNotNull,
        error.ConstraintCheck,
        => true,
        else => false,
    };
}

/// Map a SQLite result code (primary or extended) onto `Error`.
///
/// Extended result codes are enabled on every connection we open, so
/// the low byte is the primary code and the high bits refine it. We
/// switch on the extended value first for the cases where the
/// refinement changes the caller's decision (BUSY vs BUSY_SNAPSHOT,
/// which flavour of CONSTRAINT), then fall back to the primary code.
pub fn mapResult(rc: c_int) Error {
    switch (rc) {
        c.SQLITE_BUSY_SNAPSHOT => return error.BusySnapshot,
        c.SQLITE_CONSTRAINT_UNIQUE, c.SQLITE_CONSTRAINT_PRIMARYKEY => return error.ConstraintUnique,
        c.SQLITE_CONSTRAINT_FOREIGNKEY => return error.ConstraintForeignKey,
        c.SQLITE_CONSTRAINT_NOTNULL => return error.ConstraintNotNull,
        c.SQLITE_CONSTRAINT_CHECK => return error.ConstraintCheck,
        else => {},
    }
    // Primary code is the low 8 bits of any extended code.
    return switch (rc & 0xFF) {
        c.SQLITE_BUSY => error.Busy,
        c.SQLITE_LOCKED => error.Locked,
        c.SQLITE_CONSTRAINT => error.Constraint,
        c.SQLITE_IOERR => error.IoFailure,
        c.SQLITE_CORRUPT, c.SQLITE_NOTADB => error.Corrupt,
        c.SQLITE_FULL => error.Full,
        c.SQLITE_READONLY => error.ReadOnly,
        c.SQLITE_CANTOPEN => error.CantOpen,
        c.SQLITE_MISMATCH, c.SQLITE_RANGE, c.SQLITE_TOOBIG => error.Mismatch,
        c.SQLITE_MISUSE => error.Misuse,
        c.SQLITE_NOMEM => error.OutOfMemory,
        c.SQLITE_ERROR => error.Sql,
        else => error.Unexpected,
    };
}

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

/// Connection tuning. The zero value is the shipping configuration —
/// see the module comment for why each number is what it is.
pub const Options = struct {
    /// `PRAGMA busy_timeout`, milliseconds. How long a writer waits for
    /// the lock before returning `error.Busy`.
    busy_timeout_ms: i32 = 5000,

    /// `PRAGMA cache_size`. Negative means kibibytes (SQLite's
    /// convention), so -65536 is 64 MiB of page cache.
    cache_size_kb: i64 = -65536,

    /// `PRAGMA foreign_keys`. On, so `ON DELETE CASCADE` in the schema
    /// actually fires.
    foreign_keys: bool = true,

    /// Attempt `PRAGMA journal_mode=WAL`. Skipped automatically for
    /// `:memory:`, which has no journal file to write.
    wal: bool = true,

    /// Run `PRAGMA optimize` on open. Cheap, idempotent, and without it
    /// the planner can pick the primary-key index over
    /// `outbox_subs_pending` for the dispatcher query — which on the
    /// live container pinned a core at 250% for queries that should
    /// have been free.
    optimize_on_open: bool = true,

    /// Upper bound on cached prepared statements per connection. Past
    /// this, `prepare` still works but hands back an uncached statement
    /// that is finalized on release, so a pathological query generator
    /// cannot grow the cache without bound.
    max_cached_stmts: usize = 256,
};

// ---------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------

/// One cached prepared statement.
///
/// `in_use` exists because the cache is keyed by SQL text and the same
/// text can legitimately be live twice at once — a loop stepping rows
/// from a query that, per row, runs the *same* query on a child table.
/// Handing out the one handle twice would reset the outer cursor
/// mid-iteration, so the second request gets a private uncached
/// statement instead.
const Cached = struct {
    handle: *c.sqlite3_stmt,
    in_use: bool,
};

/// A SQLite connection, owned by exactly one thread.
pub const Conn = struct {
    gpa: Allocator,
    handle: *c.sqlite3,
    /// Path this connection was opened against. Owned; kept for
    /// diagnostics and because `checkpoint` is a no-op for `:memory:`.
    path: []const u8,
    in_memory: bool,
    max_cached: usize,

    /// SQL text → prepared statement. Keys are owned copies: most call
    /// sites pass string literals, but the history query composes its
    /// WHERE clause at runtime and that buffer does not outlive the
    /// call.
    cache: std.StringHashMapUnmanaged(Cached) = .empty,

    /// Transaction nesting depth. Non-zero means a `BEGIN IMMEDIATE` is
    /// outstanding on this connection; `tx.zig` owns the bookkeeping.
    /// It lives here rather than in a separate object because a
    /// connection is single-threaded, which makes the connection itself
    /// the natural carrier for "the ambient transaction" — the role Go
    /// gave to `context.Context`.
    tx_depth: usize = 0,
    /// Post-commit hooks for the outermost transaction. Run only after
    /// `COMMIT` succeeds, so observers cannot look for rows that are
    /// still invisible. See `tx.onCommit`.
    commit_hooks: std.ArrayList(Hook) = .empty,

    /// Owning thread. Debug-only: the whole point of `SQLITE_THREADSAFE=2`
    /// is that this never changes, and a stray cross-thread call is a
    /// silent memory-corruption bug otherwise.
    owner: std.Thread.Id,

    /// Number of `sqlite3_prepare_v3` calls made. Exposed so the cache
    /// can be tested for what it claims to do rather than assumed.
    prepare_count: usize = 0,
    /// Number of `prepare` calls served from the cache.
    cache_hit_count: usize = 0,

    /// A post-commit callback: a function plus its context pointer.
    /// Zig has no closures, so the pair is explicit.
    pub const Hook = struct {
        ctx: ?*anyopaque,
        run: *const fn (?*anyopaque) void,
    };

    pub const OpenError = Error || error{EmptyPath};

    /// Open (or create) the database at `path` and apply the standard
    /// pragmas. `path` may be `":memory:"`.
    ///
    /// The caller is responsible for creating parent directories; this
    /// wrapper does not touch the filesystem itself, so the store layer
    /// stays free of the `Io` plumbing that `std.Io.Dir` would require.
    pub fn open(gpa: Allocator, path: []const u8, opts: Options) OpenError!*Conn {
        if (path.len == 0) return error.EmptyPath;

        const path_z = try gpa.dupeZ(u8, path);
        defer gpa.free(path_z);

        var handle: ?*c.sqlite3 = null;
        // NOMUTEX matches SQLITE_THREADSAFE=2: no per-connection mutex,
        // because we guarantee single-thread ownership instead of paying
        // for it on every call. URI parsing stays on so an operator can
        // pass `file:...?mode=ro` if they ever need to.
        const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE |
            c.SQLITE_OPEN_NOMUTEX | c.SQLITE_OPEN_URI;
        const rc = c.sqlite3_open_v2(path_z.ptr, &handle, flags, null);
        if (rc != c.SQLITE_OK) {
            // sqlite3_open_v2 can hand back a handle even on failure,
            // and it must still be closed to release the memory.
            if (handle) |h| _ = c.sqlite3_close(h);
            return mapResult(rc);
        }
        const h = handle.?;
        errdefer _ = c.sqlite3_close(h);

        // Extended codes are how `BUSY` gets distinguished from
        // `BUSY_SNAPSHOT` and how a UNIQUE violation gets distinguished
        // from a CHECK violation. Without this the error set collapses
        // to the primary codes and the retry logic loses its input.
        _ = c.sqlite3_extended_result_codes(h, 1);
        _ = c.sqlite3_busy_timeout(h, opts.busy_timeout_ms);

        const self = try gpa.create(Conn);
        errdefer gpa.destroy(self);
        const owned_path = try gpa.dupe(u8, path);
        errdefer gpa.free(owned_path);

        self.* = .{
            .gpa = gpa,
            .handle = h,
            .path = owned_path,
            .in_memory = std.mem.eql(u8, path, ":memory:"),
            .max_cached = opts.max_cached_stmts,
            .owner = std.Thread.getCurrentId(),
        };
        errdefer self.cache.deinit(gpa);

        try self.applyPragmas(opts);
        return self;
    }

    /// Finalize every cached statement, close the handle, free the
    /// connection. Safe to call once.
    pub fn close(self: *Conn) void {
        self.assertOwner();
        var it = self.cache.iterator();
        while (it.next()) |entry| {
            _ = c.sqlite3_finalize(entry.value_ptr.handle);
            self.gpa.free(entry.key_ptr.*);
        }
        self.cache.deinit(self.gpa);
        self.commit_hooks.deinit(self.gpa);
        self.gpa.free(self.path);
        // sqlite3_close fails with BUSY if anything is unfinalized;
        // close_v2 defers the teardown instead of leaking, which is the
        // behaviour we want on a shutdown path that must not fail.
        _ = c.sqlite3_close_v2(self.handle);
        const gpa = self.gpa;
        gpa.destroy(self);
    }

    inline fn assertOwner(self: *const Conn) void {
        if (builtin.mode == .Debug) {
            std.debug.assert(self.owner == std.Thread.getCurrentId());
        }
    }

    fn applyPragmas(self: *Conn, opts: Options) Error!void {
        var buf: [128]u8 = undefined;

        // WAL only exists for file-backed databases; asking an
        // in-memory database for it returns "memory" and, on some
        // builds, an error.
        if (opts.wal and !self.in_memory) {
            try self.exec("PRAGMA journal_mode = WAL");
        }
        try self.exec("PRAGMA synchronous = NORMAL");
        try self.exec(if (opts.foreign_keys)
            "PRAGMA foreign_keys = ON"
        else
            "PRAGMA foreign_keys = OFF");

        // busy_timeout was already set through the C API; the pragma
        // keeps `PRAGMA busy_timeout` readable for the tests and for an
        // operator poking at the file.
        try self.exec(std.fmt.bufPrintZ(&buf, "PRAGMA busy_timeout = {d}", .{opts.busy_timeout_ms}) catch
            return error.Misuse);
        try self.exec(std.fmt.bufPrintZ(&buf, "PRAGMA cache_size = {d}", .{opts.cache_size_kb}) catch
            return error.Misuse);

        if (opts.optimize_on_open) try self.exec("PRAGMA optimize");
    }

    /// Run one or more statements with no parameters and no results.
    /// This is the migration / pragma path; everything else goes
    /// through `prepare` so it gets the statement cache.
    pub fn exec(self: *Conn, sql: []const u8) Error!void {
        self.assertOwner();
        const sql_z = self.gpa.dupeZ(u8, sql) catch return error.OutOfMemory;
        defer self.gpa.free(sql_z);
        const rc = c.sqlite3_exec(self.handle, sql_z.ptr, null, null, null);
        if (rc != c.SQLITE_OK) return mapResult(c.sqlite3_extended_errcode(self.handle));
    }

    /// Prepare `sql`, preferring a cached handle.
    ///
    /// The returned statement must be released with `Stmt.release`,
    /// which resets it and hands it back to the cache. Forgetting to
    /// release leaks the handle until `close`.
    pub fn prepare(self: *Conn, sql: []const u8) Error!Stmt {
        self.assertOwner();

        if (self.cache.getEntry(sql)) |entry| {
            if (!entry.value_ptr.in_use) {
                entry.value_ptr.in_use = true;
                self.cache_hit_count += 1;
                // A cached handle is reset on release, but resetting
                // again is free and makes the invariant local.
                _ = c.sqlite3_reset(entry.value_ptr.handle);
                _ = c.sqlite3_clear_bindings(entry.value_ptr.handle);
                // The key is the cache's own copy, so it stays valid for
                // as long as the entry does — which is what lets
                // `release` find the entry again and clear `in_use`.
                return .{ .conn = self, .handle = entry.value_ptr.handle, .key = entry.key_ptr.* };
            }
            // Already live on an outer cursor — fall through and give
            // this caller a private statement.
            return .{ .conn = self, .handle = try self.rawPrepare(sql, false), .key = null };
        }

        if (self.cache.count() >= self.max_cached) {
            return .{ .conn = self, .handle = try self.rawPrepare(sql, false), .key = null };
        }

        const handle = try self.rawPrepare(sql, true);
        const key = self.gpa.dupe(u8, sql) catch {
            _ = c.sqlite3_finalize(handle);
            return error.OutOfMemory;
        };
        self.cache.put(self.gpa, key, .{ .handle = handle, .in_use = true }) catch {
            self.gpa.free(key);
            _ = c.sqlite3_finalize(handle);
            return error.OutOfMemory;
        };
        return .{ .conn = self, .handle = handle, .key = key };
    }

    fn rawPrepare(self: *Conn, sql: []const u8, persistent: bool) Error!*c.sqlite3_stmt {
        var stmt: ?*c.sqlite3_stmt = null;
        // SQLITE_PREPARE_PERSISTENT tells SQLite the statement will be
        // reused many times, which changes how it allocates the VDBE
        // program — exactly right for a cached statement, wrong for a
        // one-shot.
        const flags: c_uint = if (persistent) c.SQLITE_PREPARE_PERSISTENT else 0;
        self.prepare_count += 1;
        const rc = c.sqlite3_prepare_v3(
            self.handle,
            sql.ptr,
            @intCast(sql.len),
            flags,
            &stmt,
            null,
        );
        if (rc != c.SQLITE_OK) {
            if (stmt) |s| _ = c.sqlite3_finalize(s);
            return mapResult(c.sqlite3_extended_errcode(self.handle));
        }
        return stmt orelse error.Sql; // empty / comment-only SQL
    }

    /// Prepare, bind `args`, step to completion. The workhorse for
    /// INSERT / UPDATE / DELETE.
    pub fn execute(self: *Conn, sql: []const u8, args: anytype) Error!void {
        var st = try self.prepare(sql);
        defer st.release();
        try st.bindAll(args);
        _ = try st.step();
    }

    /// Prepare, bind, and position on the first row. Returns
    /// `error.NoRows` if the query matched nothing.
    ///
    /// The caller owns the returned statement and must `release` it —
    /// any borrowed column slices die at that point.
    pub fn queryRow(self: *Conn, sql: []const u8, args: anytype) Error!Stmt {
        var st = try self.prepare(sql);
        errdefer st.release();
        try st.bindAll(args);
        if (!try st.step()) return error.NoRows;
        return st;
    }

    /// Prepare and bind, leaving the statement before its first row.
    /// Iterate with `Stmt.step`.
    pub fn query(self: *Conn, sql: []const u8, args: anytype) Error!Stmt {
        var st = try self.prepare(sql);
        errdefer st.release();
        try st.bindAll(args);
        return st;
    }

    /// Single-column, single-row integer query. The count / MAX / EXISTS
    /// shape, which is most of what the repos ask for.
    pub fn scalarInt(self: *Conn, sql: []const u8, args: anytype) Error!i64 {
        var st = try self.queryRow(sql, args);
        defer st.release();
        return st.int(0);
    }

    /// As `scalarInt`, but a missing row yields `dflt` instead of
    /// `error.NoRows`.
    pub fn scalarIntOr(self: *Conn, sql: []const u8, args: anytype, dflt: i64) Error!i64 {
        return self.scalarInt(sql, args) catch |e| switch (e) {
            error.NoRows => dflt,
            else => e,
        };
    }

    /// Rows changed by the most recent statement. `sqlite3_changes64`
    /// so a bulk DELETE past 2^31 rows still reports honestly.
    pub fn changes(self: *Conn) i64 {
        self.assertOwner();
        return c.sqlite3_changes64(self.handle);
    }

    /// Rowid assigned by the most recent successful INSERT.
    pub fn lastInsertRowid(self: *Conn) i64 {
        self.assertOwner();
        return c.sqlite3_last_insert_rowid(self.handle);
    }

    /// The most recent error message from this connection. Borrowed;
    /// valid until the next call into SQLite. For log lines only —
    /// control flow goes through `Error`.
    pub fn lastErrorMessage(self: *Conn) []const u8 {
        const msg = c.sqlite3_errmsg(self.handle);
        if (msg == null) return "";
        return std.mem.span(@as([*:0]const u8, @ptrCast(msg)));
    }

    pub const CheckpointResult = struct {
        busy: i64,
        log_pages: i64,
        checkpointed: i64,
    };

    /// `PRAGMA wal_checkpoint(TRUNCATE)` — force the WAL back to zero
    /// bytes.
    ///
    /// SQLite's auto-checkpoint counter is per-connection, and with one
    /// connection per thread no single counter reliably crosses the
    /// threshold, so under sustained write load the WAL grows without
    /// bound unless something checkpoints explicitly.
    pub fn checkpoint(self: *Conn) Error!CheckpointResult {
        if (self.in_memory) return .{ .busy = 0, .log_pages = 0, .checkpointed = 0 };
        var st = try self.queryRow("PRAGMA wal_checkpoint(TRUNCATE)", .{});
        defer st.release();
        return .{
            .busy = st.int(0),
            .log_pages = st.int(1),
            .checkpointed = st.int(2),
        };
    }
};

// ---------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------

/// Marks a byte slice as a BLOB rather than TEXT.
///
/// `[]const u8` is both "string" and "bytes" in Zig, and SQLite cares
/// about the difference: a BLOB never gets a collation or a text
/// affinity conversion. Binding defaults to TEXT (far more common) and
/// this wrapper opts into BLOB explicitly.
pub const Blob = struct { bytes: []const u8 };

/// Wrap a byte slice so `bindAll` binds it as a BLOB.
pub fn blob(bytes: []const u8) Blob {
    return .{ .bytes = bytes };
}

/// A prepared statement in the middle of its lifecycle: bind, step,
/// read columns, release.
pub const Stmt = struct {
    conn: *Conn,
    handle: *c.sqlite3_stmt,
    /// Cache key when this statement is cached, `null` when it is a
    /// private one-shot that `release` must finalize.
    key: ?[]const u8,
    /// Next 1-based parameter index for `bind`.
    next_param: c_int = 1,

    /// Reset and return the statement to the cache (or finalize it if
    /// it was never cached). Every column slice handed out becomes
    /// invalid here.
    pub fn release(self: *Stmt) void {
        // `reset` reports the error of the last `step`, which the caller
        // has already seen. Discard it.
        _ = c.sqlite3_reset(self.handle);
        _ = c.sqlite3_clear_bindings(self.handle);
        if (self.key) |k| {
            if (self.conn.cache.getPtr(k)) |entry| entry.in_use = false;
        } else {
            _ = c.sqlite3_finalize(self.handle);
        }
        self.* = undefined;
    }

    /// Bind one value at the next parameter index, inferring the SQLite
    /// type from the Zig type:
    ///
    ///   * integers, `bool`, enums → INTEGER
    ///   * floats → REAL
    ///   * `[]const u8`, `[:0]const u8`, string literals → TEXT
    ///   * `Blob` → BLOB
    ///   * `null` and `?T` → NULL / the payload
    pub fn bind(self: *Stmt, value: anytype) Error!void {
        const idx = self.next_param;
        self.next_param += 1;
        try self.bindAt(idx, value);
    }

    /// Bind every field of a tuple (or struct) in declaration order,
    /// starting at parameter 1.
    pub fn bindAll(self: *Stmt, args: anytype) Error!void {
        const T = @TypeOf(args);
        if (T == void) return;
        const info = @typeInfo(T);
        if (info != .@"struct") @compileError("bindAll expects a tuple or struct, got " ++ @typeName(T));
        inline for (info.@"struct".fields) |f| {
            try self.bind(@field(args, f.name));
        }
    }

    fn bindAt(self: *Stmt, idx: c_int, value: anytype) Error!void {
        const T = @TypeOf(value);
        const rc = switch (@typeInfo(T)) {
            .null => c.sqlite3_bind_null(self.handle, idx),
            .optional => {
                if (value) |v| return self.bindAt(idx, v);
                return self.bindAt(idx, null);
            },
            .bool => c.sqlite3_bind_int(self.handle, idx, if (value) 1 else 0),
            .int, .comptime_int => c.sqlite3_bind_int64(self.handle, idx, @intCast(value)),
            .float, .comptime_float => c.sqlite3_bind_double(self.handle, idx, @floatCast(value)),
            .@"enum" => c.sqlite3_bind_int64(self.handle, idx, @intFromEnum(value)),
            .pointer, .array => blk: {
                if (T == Blob) unreachable; // handled below by struct branch
                const slice: []const u8 = value;
                if (slice.len == 0) {
                    // A zero-length TEXT must not be bound from a
                    // possibly-dangling pointer; bind the empty string
                    // explicitly, and with a static lifetime since the
                    // literal outlives everything.
                    break :blk bindTextRaw(self.handle, idx, "", 0, null);
                }
                break :blk bindTextRaw(self.handle, idx, slice.ptr, @intCast(slice.len), transient);
            },
            .@"struct" => blk: {
                if (T != Blob) @compileError("cannot bind " ++ @typeName(T));
                if (value.bytes.len == 0) {
                    break :blk c.sqlite3_bind_zeroblob(self.handle, idx, 0);
                }
                break :blk bindBlobRaw(
                    self.handle,
                    idx,
                    @ptrCast(value.bytes.ptr),
                    @intCast(value.bytes.len),
                    transient,
                );
            },
            else => @compileError("cannot bind " ++ @typeName(T)),
        };
        if (rc != c.SQLITE_OK) return mapResult(rc);
    }

    /// Advance the statement. Returns `true` when a row is available,
    /// `false` when the statement is done.
    pub fn step(self: *Stmt) Error!bool {
        const rc = c.sqlite3_step(self.handle);
        return switch (rc) {
            c.SQLITE_ROW => true,
            c.SQLITE_DONE => false,
            // The extended code carries the detail (which constraint,
            // BUSY vs BUSY_SNAPSHOT); `step` itself only returns the
            // primary code in some builds.
            else => mapResult(c.sqlite3_extended_errcode(c.sqlite3_db_handle(self.handle))),
        };
    }

    /// Step once, requiring a row. `error.NoRows` when the statement is
    /// already done.
    pub fn stepRow(self: *Stmt) Error!void {
        if (!try self.step()) return error.NoRows;
    }

    // -- column readers ------------------------------------------------
    //
    // Column indices are 0-based (SQLite's convention for reads, unlike
    // the 1-based parameter indices for binds — a wart of the C API
    // that is not worth papering over, because every other SQLite
    // reference the reader will consult has the same asymmetry).

    pub fn isNull(self: *Stmt, i: usize) bool {
        return c.sqlite3_column_type(self.handle, @intCast(i)) == c.SQLITE_NULL;
    }

    pub fn int(self: *Stmt, i: usize) i64 {
        return c.sqlite3_column_int64(self.handle, @intCast(i));
    }

    pub fn optInt(self: *Stmt, i: usize) ?i64 {
        if (self.isNull(i)) return null;
        return self.int(i);
    }

    pub fn boolean(self: *Stmt, i: usize) bool {
        return self.int(i) != 0;
    }

    pub fn float(self: *Stmt, i: usize) f64 {
        return c.sqlite3_column_double(self.handle, @intCast(i));
    }

    /// TEXT column, borrowed. Valid until the next `step` or `release`
    /// on this statement. A NULL column reads as `""` — matching Go's
    /// `sql.NullString.String`, which every repo here relied on.
    pub fn text(self: *Stmt, i: usize) []const u8 {
        const ptr = c.sqlite3_column_text(self.handle, @intCast(i));
        if (ptr == null) return "";
        const len = c.sqlite3_column_bytes(self.handle, @intCast(i));
        return @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    }

    /// TEXT column, distinguishing NULL from the empty string.
    pub fn optText(self: *Stmt, i: usize) ?[]const u8 {
        if (self.isNull(i)) return null;
        return self.text(i);
    }

    /// BLOB column, borrowed with the same lifetime as `text`.
    pub fn bytes(self: *Stmt, i: usize) []const u8 {
        const ptr = c.sqlite3_column_blob(self.handle, @intCast(i));
        const len = c.sqlite3_column_bytes(self.handle, @intCast(i));
        if (ptr == null or len == 0) return "";
        return @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    }

    /// TEXT column copied into caller-owned memory. Use when the value
    /// outlives the statement.
    pub fn textAlloc(self: *Stmt, gpa: Allocator, i: usize) Allocator.Error![]u8 {
        return gpa.dupe(u8, self.text(i));
    }

    /// BLOB column copied into caller-owned memory.
    pub fn bytesAlloc(self: *Stmt, gpa: Allocator, i: usize) Allocator.Error![]u8 {
        return gpa.dupe(u8, self.bytes(i));
    }

    /// Number of columns in the result set.
    pub fn columnCount(self: *Stmt) usize {
        return @intCast(c.sqlite3_column_count(self.handle));
    }
};

// -- SQLITE_TRANSIENT ------------------------------------------------
//
// `SQLITE_TRANSIENT` is `((sqlite3_destructor_type)-1)`: not a real
// function pointer, a sentinel meaning "copy the bytes, I am not
// keeping them alive". Zig will not let an all-ones address be typed as
// a `*const fn`, because the type carries an alignment the address
// obviously does not satisfy — and rightly so, since it is not a
// function at all.
//
// So the two binds that take a destructor are re-declared via `@extern`
// with the last parameter typed `?*const anyopaque`, which has
// alignment 1 and accepts the sentinel. Same symbol, same ABI, honest
// types.
//
// Copying is the right default: bound values are routinely stack
// temporaries or slices from a caller's arena, and the alternative is a
// use-after-free that only shows up under load.

const transient: ?*const anyopaque = @ptrFromInt(std.math.maxInt(usize));

const bindTextRaw = @extern(*const fn (
    ?*c.sqlite3_stmt,
    c_int,
    ?[*]const u8,
    c_int,
    ?*const anyopaque,
) callconv(.c) c_int, .{ .name = "sqlite3_bind_text" });

const bindBlobRaw = @extern(*const fn (
    ?*c.sqlite3_stmt,
    c_int,
    ?*const anyopaque,
    c_int,
    ?*const anyopaque,
) callconv(.c) c_int, .{ .name = "sqlite3_bind_blob" });

// ---------------------------------------------------------------------
// Column encodings shared by the repositories
// ---------------------------------------------------------------------

/// Bind an empty string as SQL NULL.
///
/// The aggregates model "no value" as `""` because a Zig optional string
/// costs a branch at every read site for no benefit inside the domain.
/// The schema models it as nullable TEXT, which is what keeps
/// `WHERE err_msg IS NOT NULL` a meaningful query and stops an index from
/// filling with empty strings. This is the conversion.
pub fn nullIfEmpty(s: []const u8) ?[]const u8 {
    return if (s.len == 0) null else s;
}

/// A JSON array of strings stored in one TEXT column.
///
/// Three tables do this — `files.groups`, `par2_sets.failed_files`,
/// `subscriptions.topics` — because in each case the list is short, read
/// as a whole, and never queried by element. A junction table would add
/// a join to every read to support a query nobody makes.
///
/// This lives in the SQLite adapter rather than in a codec module because
/// it *is* a storage encoding: the domain deals in `[]const []const u8`
/// and does not know that one of its fields is spelled as JSON on disk.
pub const string_array = struct {
    /// Append the JSON encoding of `items` to `buf`.
    pub fn encode(
        gpa: Allocator,
        buf: *std.ArrayList(u8),
        items: []const []const u8,
    ) Allocator.Error!void {
        try buf.append(gpa, '[');
        for (items, 0..) |item, i| {
            if (i > 0) try buf.append(gpa, ',');
            try buf.append(gpa, '"');
            // Values reach us from NZB files and from operator input, so
            // they get escaped rather than trusted.
            for (item) |ch| switch (ch) {
                '"', '\\' => {
                    try buf.append(gpa, '\\');
                    try buf.append(gpa, ch);
                },
                '\n' => try buf.appendSlice(gpa, "\\n"),
                '\r' => try buf.appendSlice(gpa, "\\r"),
                '\t' => try buf.appendSlice(gpa, "\\t"),
                0...0x08, 0x0B, 0x0C, 0x0E...0x1F => try buf.print(gpa, "\\u{x:0>4}", .{ch}),
                else => try buf.append(gpa, ch),
            };
            try buf.append(gpa, '"');
        }
        try buf.append(gpa, ']');
    }

    /// One-shot encode into freshly allocated memory. Caller frees.
    pub fn encodeAlloc(gpa: Allocator, items: []const []const u8) Allocator.Error![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(gpa);
        try encode(gpa, &buf, items);
        return buf.toOwnedSlice(gpa);
    }

    pub const DecodeError = error{MalformedJsonArray} || Allocator.Error;

    /// Decode into `arena`. An empty column decodes to an empty list
    /// rather than an error: a row written before the column existed has
    /// nothing to say, and refusing to load it would strand the row.
    pub fn decode(arena: Allocator, json: []const u8) DecodeError![]const []const u8 {
        const trimmed = std.mem.trim(u8, json, " \t\r\n");
        if (trimmed.len == 0) return &.{};
        return std.json.parseFromSliceLeaky(
            []const []const u8,
            arena,
            trimmed,
            .{ .allocate = .alloc_always },
        ) catch |e| switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.MalformedJsonArray,
        };
    }
};

// ---------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------

/// Test-only fixtures. Kept in the module under test so `outbox.zig`
/// and the repo tests share one definition of "a database to test
/// against", exactly as the Go tests shared `openTestDB`.
pub const testing_support = struct {

    /// An in-memory connection with the shipping pragmas.
    ///
    /// `:memory:` is private per connection and this build has
    /// `SQLITE_OMIT_SHARED_CACHE`, so a test that needs two
    /// connections onto one database must use `openTempFile`.
    pub fn openMemory() !*Conn {
        return Conn.open(std.testing.allocator, ":memory:", .{});
    }

    /// A unique path under `.zig-cache/tmp/`. Caller frees the returned
    /// slice and calls `removeTempFile` when done.
    pub fn tempPath(gpa: Allocator) ![]u8 {
        const dir = std.Io.Dir.cwd();
        dir.createDirPath(std.testing.io, ".zig-cache/tmp") catch {};
        var rnd: [8]u8 = undefined;
        std.testing.io.random(&rnd);
        return std.fmt.allocPrint(gpa, ".zig-cache/tmp/hoardarr-{x}.db", .{std.fmt.bytesToHex(rnd, .lower)});
    }

    /// A file-backed connection, so WAL and cross-connection visibility
    /// are actually exercised.
    pub fn openTempFile(path: []const u8) !*Conn {
        return Conn.open(std.testing.allocator, path, .{});
    }

    /// Delete a temp database along with its WAL sidecars.
    pub fn removeTempFile(gpa: Allocator, path: []const u8) void {
        const dir = std.Io.Dir.cwd();
        dir.deleteFile(std.testing.io, path) catch {};
        for ([_][]const u8{ "-wal", "-shm" }) |suffix| {
            const p = std.fmt.allocPrint(gpa, "{s}{s}", .{ path, suffix }) catch continue;
            defer gpa.free(p);
            dir.deleteFile(std.testing.io, p) catch {};
        }
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "open rejects an empty path" {
    try t.expectError(error.EmptyPath, Conn.open(t.allocator, "", .{}));
}

test "open applies the documented pragmas" {
    const conn = try testing_support.openMemory();
    defer conn.close();

    try t.expectEqual(@as(i64, 1), try conn.scalarInt("PRAGMA foreign_keys", .{}));
    try t.expectEqual(@as(i64, 5000), try conn.scalarInt("PRAGMA busy_timeout", .{}));
    // cache_size is reported in pages when positive, in the negative
    // kibibyte form when set that way — SQLite echoes back what we set.
    try t.expectEqual(@as(i64, -65536), try conn.scalarInt("PRAGMA cache_size", .{}));
    // NORMAL is 1.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("PRAGMA synchronous", .{}));
}

test "a file-backed database lands in WAL mode" {
    const path = try testing_support.tempPath(t.allocator);
    defer t.allocator.free(path);
    defer testing_support.removeTempFile(t.allocator, path);

    const conn = try testing_support.openTempFile(path);
    defer conn.close();

    var st = try conn.queryRow("PRAGMA journal_mode", .{});
    defer st.release();
    try t.expectEqualStrings("wal", st.text(0));
}

test "bind and read every column type" {
    const conn = try testing_support.openMemory();
    defer conn.close();

    try conn.exec("CREATE TABLE t(i INTEGER, r REAL, s TEXT, b BLOB, n INTEGER, f INTEGER)");
    try conn.execute(
        "INSERT INTO t(i, r, s, b, n, f) VALUES (?, ?, ?, ?, ?, ?)",
        .{ @as(i64, -9007199254740993), @as(f64, 0.075), "hello", blob(&[_]u8{ 0, 1, 2, 0xFF }), null, true },
    );

    var st = try conn.queryRow("SELECT i, r, s, b, n, f FROM t", .{});
    defer st.release();
    try t.expectEqual(@as(i64, -9007199254740993), st.int(0));
    try t.expectEqual(@as(f64, 0.075), st.float(1));
    try t.expectEqualStrings("hello", st.text(2));
    try t.expectEqualSlices(u8, &[_]u8{ 0, 1, 2, 0xFF }, st.bytes(3));
    try t.expect(st.isNull(4));
    try t.expectEqual(@as(?i64, null), st.optInt(4));
    try t.expectEqualStrings("", st.text(4)); // NULL reads as ""
    try t.expectEqual(@as(?[]const u8, null), st.optText(4));
    try t.expect(st.boolean(5));
}

test "optional binds map to NULL and to the payload" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a TEXT, b INTEGER)");

    const some: ?[]const u8 = "x";
    const none: ?[]const u8 = null;
    const some_i: ?i64 = 7;
    const none_i: ?i64 = null;
    try conn.execute("INSERT INTO t VALUES (?, ?)", .{ some, some_i });
    try conn.execute("INSERT INTO t VALUES (?, ?)", .{ none, none_i });

    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM t WHERE a IS NOT NULL", .{}));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM t WHERE b IS NULL", .{}));
}

test "a repeated query reuses the cached statement" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER)");

    const sql = "INSERT INTO t(a) VALUES (?)";
    const before = conn.prepare_count;
    for (0..50) |i| {
        try conn.execute(sql, .{@as(i64, @intCast(i))});
    }
    // One prepare for the first call, then 49 cache hits. If the cache
    // ever regresses to prepare-per-call this is the test that notices.
    try t.expectEqual(before + 1, conn.prepare_count);
    try t.expectEqual(@as(usize, 49), conn.cache_hit_count);
    try t.expectEqual(@as(i64, 50), try conn.scalarInt("SELECT COUNT(*) FROM t", .{}));
}

test "release clears bindings so a reused statement cannot inherit them" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER, b INTEGER)");

    try conn.execute("INSERT INTO t VALUES (?, ?)", .{ @as(i64, 1), @as(i64, 2) });
    // Same SQL, but bind only the first parameter: an unbound parameter
    // is NULL. If clear_bindings were skipped, b would still be 2.
    var st = try conn.prepare("INSERT INTO t VALUES (?, ?)");
    try st.bind(@as(i64, 3));
    _ = try st.step();
    st.release();

    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM t WHERE b IS NULL", .{}));
}

test "the same SQL live twice gets a private statement for the inner cursor" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER)");
    for (0..3) |i| try conn.execute("INSERT INTO t VALUES (?)", .{@as(i64, @intCast(i))});

    // Outer cursor holds the cached handle; the inner loop asks for the
    // same SQL and must not reset the outer one out from under us.
    var outer = try conn.query("SELECT a FROM t ORDER BY a", .{});
    defer outer.release();
    var seen: usize = 0;
    while (try outer.step()) {
        var inner = try conn.query("SELECT a FROM t ORDER BY a", .{});
        defer inner.release();
        var inner_rows: usize = 0;
        while (try inner.step()) inner_rows += 1;
        try t.expectEqual(@as(usize, 3), inner_rows);
        seen += 1;
    }
    try t.expectEqual(@as(usize, 3), seen);
}

test "the statement cache is bounded" {
    const conn = try Conn.open(t.allocator, ":memory:", .{ .max_cached_stmts = 4 });
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)");

    // Five distinct statements, cache capped at four: the fifth is
    // prepared uncached and finalized on release rather than growing
    // the map.
    for ([_][]const u8{
        "SELECT a FROM t", "SELECT b FROM t", "SELECT c FROM t",
        "SELECT d FROM t", "SELECT e FROM t",
    }) |sql| {
        var st = try conn.prepare(sql);
        st.release();
    }
    try t.expectEqual(@as(usize, 4), conn.cache.count());
}

test "result codes map to distinguishable errors" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL UNIQUE, c INTEGER CHECK (c > 0))");

    try conn.execute("INSERT INTO t VALUES (?, ?, ?)", .{ @as(i64, 1), "x", @as(i64, 1) });

    // UNIQUE is distinguishable from NOT NULL is distinguishable from
    // CHECK — the repos map each onto a different domain error.
    try t.expectError(
        error.ConstraintUnique,
        conn.execute("INSERT INTO t VALUES (?, ?, ?)", .{ @as(i64, 2), "x", @as(i64, 1) }),
    );
    try t.expectError(
        error.ConstraintNotNull,
        conn.execute("INSERT INTO t VALUES (?, ?, ?)", .{ @as(i64, 3), null, @as(i64, 1) }),
    );
    try t.expectError(
        error.ConstraintCheck,
        conn.execute("INSERT INTO t VALUES (?, ?, ?)", .{ @as(i64, 4), "y", @as(i64, 0) }),
    );
    // And a real failure is not a constraint at all.
    try t.expectError(error.Sql, conn.exec("SELECT * FROM no_such_table"));
}

test "retryable and constraint predicates agree with the error set" {
    try t.expect(isRetryable(error.Busy));
    try t.expect(isRetryable(error.BusySnapshot));
    try t.expect(isRetryable(error.Locked));
    try t.expect(!isRetryable(error.ConstraintUnique));
    try t.expect(!isRetryable(error.IoFailure));

    try t.expect(isConstraint(error.ConstraintUnique));
    try t.expect(isConstraint(error.ConstraintForeignKey));
    try t.expect(!isConstraint(error.Busy));
    try t.expect(!isConstraint(error.IoFailure));
}

test "mapResult keeps extended codes distinct from their primary code" {
    try t.expectEqual(Error.BusySnapshot, mapResult(c.SQLITE_BUSY_SNAPSHOT));
    try t.expectEqual(Error.Busy, mapResult(c.SQLITE_BUSY));
    try t.expectEqual(Error.ConstraintUnique, mapResult(c.SQLITE_CONSTRAINT_UNIQUE));
    try t.expectEqual(Error.Constraint, mapResult(c.SQLITE_CONSTRAINT));
    try t.expectEqual(Error.IoFailure, mapResult(c.SQLITE_IOERR_READ));
    try t.expectEqual(Error.Unexpected, mapResult(0x7F));
}

test "a missing row is NoRows, not a zero value" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER)");
    try t.expectError(error.NoRows, conn.scalarInt("SELECT a FROM t", .{}));
    try t.expectEqual(@as(i64, -1), try conn.scalarIntOr("SELECT a FROM t", .{}, -1));
    // COUNT(*) always has a row, so it is 0 rather than NoRows.
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM t", .{}));
}

test "changes and lastInsertRowid track the last statement" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)");

    try conn.execute("INSERT INTO t(a) VALUES (?)", .{@as(i64, 1)});
    try t.expectEqual(@as(i64, 1), conn.lastInsertRowid());
    try conn.execute("INSERT INTO t(a) VALUES (?)", .{@as(i64, 2)});
    try t.expectEqual(@as(i64, 2), conn.lastInsertRowid());

    try conn.execute("UPDATE t SET a = 9", .{});
    try t.expectEqual(@as(i64, 2), conn.changes());
    try conn.execute("DELETE FROM t WHERE a = ?", .{@as(i64, 999)});
    try t.expectEqual(@as(i64, 0), conn.changes());
}

test "foreign keys cascade because the pragma is on" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec(
        \\CREATE TABLE parent(id INTEGER PRIMARY KEY);
        \\CREATE TABLE child(id INTEGER PRIMARY KEY,
        \\    parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE);
    );
    try conn.execute("INSERT INTO parent(id) VALUES (?)", .{@as(i64, 1)});
    try conn.execute("INSERT INTO child(parent_id) VALUES (?)", .{@as(i64, 1)});
    try conn.execute("DELETE FROM parent WHERE id = ?", .{@as(i64, 1)});
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM child", .{}));
}

test "checkpoint truncates the WAL and no-ops in memory" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    const r = try conn.checkpoint();
    try t.expectEqual(@as(i64, 0), r.log_pages);

    const path = try testing_support.tempPath(t.allocator);
    defer t.allocator.free(path);
    defer testing_support.removeTempFile(t.allocator, path);
    const file_conn = try testing_support.openTempFile(path);
    defer file_conn.close();
    try file_conn.exec("CREATE TABLE t(a INTEGER)");
    for (0..200) |i| try file_conn.execute("INSERT INTO t VALUES (?)", .{@as(i64, @intCast(i))});
    const fr = try file_conn.checkpoint();
    try t.expectEqual(@as(i64, 0), fr.busy);
}

test "blob round-trips including embedded NULs and empty" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)");

    const payload = [_]u8{ 0xDE, 0x00, 0xAD, 0x00, 0xBE, 0xEF };
    try conn.execute("INSERT INTO t(id, b) VALUES (?, ?)", .{ @as(i64, 1), blob(&payload) });
    try conn.execute("INSERT INTO t(id, b) VALUES (?, ?)", .{ @as(i64, 2), blob("") });

    var st = try conn.queryRow("SELECT b FROM t WHERE id = ?", .{@as(i64, 1)});
    try t.expectEqualSlices(u8, &payload, st.bytes(0));
    st.release();

    var st2 = try conn.queryRow("SELECT b FROM t WHERE id = ?", .{@as(i64, 2)});
    defer st2.release();
    try t.expectEqual(@as(usize, 0), st2.bytes(0).len);
}

test "text with embedded UTF-8 and quotes survives binding" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(s TEXT)");
    const tricky = "O'Brien \"quoted\" — ünïcödé \\ backslash";
    try conn.execute("INSERT INTO t VALUES (?)", .{tricky});
    var st = try conn.queryRow("SELECT s FROM t", .{});
    defer st.release();
    try t.expectEqualStrings(tricky, st.text(0));
}

test "textAlloc outlives the statement" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(s TEXT)");
    try conn.execute("INSERT INTO t VALUES (?)", .{"durable"});

    var st = try conn.queryRow("SELECT s FROM t", .{});
    const copy = try st.textAlloc(t.allocator, 0);
    defer t.allocator.free(copy);
    st.release();
    try t.expectEqualStrings("durable", copy);
}

test "nullIfEmpty distinguishes absent from empty in the row" {
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT)");
    try conn.execute("INSERT INTO t(id, s) VALUES (?, ?)", .{ @as(i64, 1), nullIfEmpty("") });
    try conn.execute("INSERT INTO t(id, s) VALUES (?, ?)", .{ @as(i64, 2), nullIfEmpty("x") });
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM t WHERE s IS NULL", .{}));
}

test "a string array round-trips through its TEXT column" {
    var arena_state = std.heap.ArenaAllocator.init(t.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_][]const u8{
        "alt.binaries.test",
        "has \"quotes\"",
        "back\\slash",
        "tab\there and newline\n",
        "",
    };
    const encoded = try string_array.encodeAlloc(t.allocator, &items);
    defer t.allocator.free(encoded);

    const decoded = try string_array.decode(arena, encoded);
    try t.expectEqual(items.len, decoded.len);
    for (items, decoded) |want, got| try t.expectEqualStrings(want, got);
}

test "an empty string array column decodes to an empty list, garbage to an error" {
    var arena_state = std.heap.ArenaAllocator.init(t.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    try t.expectEqual(@as(usize, 0), (try string_array.decode(arena, "")).len);
    try t.expectEqual(@as(usize, 0), (try string_array.decode(arena, "   ")).len);
    try t.expectEqual(@as(usize, 0), (try string_array.decode(arena, "[]")).len);
    try t.expectError(error.MalformedJsonArray, string_array.decode(arena, "{\"not\":\"an array\"}"));
    try t.expectError(error.MalformedJsonArray, string_array.decode(arena, "[1,2,3]"));
}

test "double-quoted identifiers stay rejected through the wrapper" {
    // SQLITE_DQS=0 is a build flag; assert it survives the wrapper so a
    // typo'd column name can never become a silent string literal.
    const conn = try testing_support.openMemory();
    defer conn.close();
    try conn.exec("CREATE TABLE t(a INTEGER)");
    try t.expectError(error.Sql, conn.exec("SELECT \"nope\" FROM t"));
}
