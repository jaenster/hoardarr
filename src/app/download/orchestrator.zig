//! Drives one `Job` from queued to download_complete.
//!
//! # Why this is a state machine and not a goroutine pool
//!
//! The Go original spawned, per batch of ready segments, N worker
//! goroutines plus a producer plus a drainer, coordinated by three
//! channels and a `WaitGroup`, with `time.After` sleeps inside the
//! workers for retry backoff. The Zig daemon is single-threaded on a
//! reactor, so that shape does not translate — and would not be worth
//! translating if it did, because the sleeps are what made the Go tests
//! wall-clock-dependent (`TestOrchestrator_DurableRetry_RespectsNextRetryAt`
//! asserts "at least 80ms elapsed" and skips the rest on slow CI).
//!
//! Here the retry policy is a pure verdict function and the runner is a
//! set of steps a caller drives:
//!
//!     begin(now)                  → reset crash leftovers, mark started
//!     takeReady(a, now, max)      → the segments eligible right now
//!     step(a, task, now)          → one fetch attempt, or "wake me at T"
//!     submit(result)              → hand a resolved segment to the batch
//!     flushDue(now) / flush(now)  → persist + publish one batch
//!     verdict(now)                → work / wait_until / done
//!
//! `step` never blocks and never sleeps: when a backoff is owed it
//! returns `.wait_until`, and the composition root turns that into a
//! reactor timer. Every test below therefore runs in zero wall-clock
//! time and asserts the *exact* delay schedule rather than a lower
//! bound on elapsed time.
//!
//! # Retry: two budgets, not one
//!
//! Both of Go's budgets are preserved because they answer different
//! questions.
//!
//!   * **In-process** (`max_attempts`, `base_backoff_ms`) — "the
//!     connection hiccuped, ask again in 200ms". Costs nothing durable
//!     and does not touch the aggregate.
//!   * **Durable** (`max_durable_attempts`, `durable_backoff_*`) — "the
//!     provider is unhappy; come back in 30 seconds". Written to the
//!     segment row as `next_retry_at`, so it survives a restart. This is
//!     the only thing that increments `Segment.attempts`, which is why
//!     the ported tests assert `attempts == 1` after one durable retry
//!     even though six fetches happened.
//!
//! `NoPoolsAvailable` consumes neither budget. A job uploaded before any
//! server was configured must not burn its retries waiting for the
//! operator to finish typing in the Settings UI.
//!
//! # The two persist paths
//!
//! `flushAggregate` chooses between a full `save` and a two-column
//! `updateCounters` on the aggregate's `state_dirty` bit. During an
//! active download nothing but `done_bytes` moves for tens of
//! consecutive flushes, and the full 12-column UPDATE was still visible
//! in the production SQLite profile after every other optimisation.
//! `Job` maintains the bit; this file only reads it.

const std = @import("std");
const log = @import("../../core/log.zig");
const yenc = @import("../../codec/yenc.zig");
const app_ports = @import("../ports.zig");
const ports = @import("ports.zig");
const dtx = @import("../../domain/tx.zig");
const job_mod = @import("../../domain/download/job.zig");
const dsegment = @import("../../domain/download/segment.zig");
const devents = @import("../../domain/download/events.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const Job = job_mod.Job;
pub const Segment = dsegment.Segment;
pub const SegmentId = ports.SegmentId;
pub const JobId = ports.JobId;
pub const ServerId = ports.ServerId;
pub const SegmentUpdate = ports.SegmentUpdate;
pub const FetchError = ports.FetchError;

pub const Sink = app_ports.EventSink(devents.Event);
pub const FakeSink = app_ports.FakeSink(devents.Event);

/// Everything a step of the runner can fail with. Deliberately explicit:
/// no `anyerror` in a public signature (PORT.md).
pub const Error = ports.RepoError || app_ports.PublishError ||
    app_ports.FsError || app_ports.TxError;

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

/// Runtime tuning. Every zero means "use the default", exactly as Go's
/// `OrchestratorOptions` did, so a caller can supply a partial literal.
pub const Options = struct {
    /// Cap on concurrently in-flight fetches. The pool is the real
    /// bound; this is the upper bound on outstanding `SegmentTask`s.
    workers: u16 = 0,
    /// How often the drainer flushes a batch to the store and the bus.
    ///
    /// 1s, not Go's original 100ms: on the low-power hardware this ships
    /// to, SQLite's VDBE was already the dominant CPU consumer during a
    /// download and 10Hz drove it ten times harder than necessary. A
    /// crash loses at most one second of segment progress, and those
    /// segments re-dispatch cleanly from their persisted `pending`
    /// state.
    flush_interval_ms: Millis = 0,
    /// Forces a flush at this batch size regardless of the interval.
    flush_batch_max: usize = 0,
    /// Total in-process attempts per dispatch. 1 disables in-process
    /// retry.
    max_attempts: u8 = 0,
    /// First in-process retry delay; doubles per attempt.
    base_backoff_ms: Millis = 0,
    /// Cap on total dispatches per segment across the job's lifetime.
    /// Once `Segment.attempts` reaches this, the next failure is
    /// terminal instead of earning another durable retry.
    max_durable_attempts: i32 = 0,
    durable_backoff_base_ms: Millis = 0,
    durable_backoff_max_ms: Millis = 0,
    /// Ceiling on how long the runner asks to sleep between polls. Small
    /// so operator actions (pause, remove) are noticed promptly rather
    /// than after a 30-minute durable backoff.
    max_poll_gap_ms: Millis = 0,
    /// Wait between re-checks while no pool is available. Larger than
    /// `base_backoff_ms` on purpose: pool availability changes on
    /// operator action, not on the millisecond timescale.
    pool_wait_ms: Millis = 0,
    /// SABnzbd's `fail_hopeless`, as a fraction. 0 disables.
    fail_hopeless_ratio: f64 = 0,

    pub const defaults: Options = .{
        .workers = 1,
        .flush_interval_ms = 1000,
        .flush_batch_max = 1024,
        .max_attempts = 3,
        .base_backoff_ms = 200,
        .max_durable_attempts = 10,
        .durable_backoff_base_ms = 30 * std.time.ms_per_s,
        .durable_backoff_max_ms = 30 * std.time.ms_per_min,
        .max_poll_gap_ms = 60 * std.time.ms_per_s,
        .pool_wait_ms = 5 * std.time.ms_per_s,
        .fail_hopeless_ratio = 0,
    };

    /// Fills every zero field from `defaults`.
    pub fn normalized(self: Options) Options {
        var o = self;
        const d = defaults;
        if (o.workers == 0) o.workers = d.workers;
        if (o.flush_interval_ms == 0) o.flush_interval_ms = d.flush_interval_ms;
        if (o.flush_batch_max == 0) o.flush_batch_max = d.flush_batch_max;
        if (o.max_attempts == 0) o.max_attempts = d.max_attempts;
        if (o.base_backoff_ms == 0) o.base_backoff_ms = d.base_backoff_ms;
        if (o.max_durable_attempts == 0) o.max_durable_attempts = d.max_durable_attempts;
        if (o.durable_backoff_base_ms == 0) o.durable_backoff_base_ms = d.durable_backoff_base_ms;
        if (o.durable_backoff_max_ms == 0) o.durable_backoff_max_ms = d.durable_backoff_max_ms;
        if (o.max_poll_gap_ms == 0) o.max_poll_gap_ms = d.max_poll_gap_ms;
        if (o.pool_wait_ms == 0) o.pool_wait_ms = d.pool_wait_ms;
        return o;
    }

    /// In-process backoff before the attempt following `attempt`
    /// (1-based): `base << (attempt-1)`, saturating.
    pub fn inProcessBackoffMs(self: Options, attempt: u8) Millis {
        if (attempt == 0) return 0;
        const shift: u6 = @intCast(@min(attempt - 1, 40));
        const base: u64 = @intCast(@max(self.base_backoff_ms, 0));
        const scaled = std.math.shlExact(u64, base, shift) catch
            return std.math.maxInt(i32);
        return @intCast(@min(scaled, @as(u64, std.math.maxInt(i32))));
    }

    /// Delay for a segment that has already been durably retried
    /// `attempts` times: base, 2×base, 4×base, … capped.
    ///
    /// The doubling loop is Go's, quirk included — it multiplies while
    /// `d < max`, so it takes one step past the cap and then clamps,
    /// which is why `attempts = 1` already yields 2× the base.
    pub fn durableBackoffMs(self: Options, attempts: i32) Millis {
        var d = self.durable_backoff_base_ms;
        if (attempts < 1) return d;
        var i: i32 = 0;
        while (i < attempts and d < self.durable_backoff_max_ms) : (i += 1) {
            d *|= 2;
        }
        return @min(d, self.durable_backoff_max_ms);
    }
};

// ---------------------------------------------------------------------
// Failure classification
// ---------------------------------------------------------------------

/// Why one fetch attempt did not put bytes on disk.
///
/// This replaces Go's `isTransientFetchErr`, which decided policy partly
/// by `errors.Is` and partly by `strings.Contains(err.Error(), "yenc
/// decode")`. A string match is a silent dependency on a wrap prefix:
/// renaming the message turns a permanent failure into an unbounded
/// retry loop. Here the switch is exhaustive and the compiler enforces
/// that a new variant gets a decision.
pub const Failure = union(enum) {
    /// The article never arrived.
    fetch: FetchError,
    /// The body arrived and was not valid yEnc, or its CRC disagreed.
    /// Permanent: the same bytes will fail the same way.
    decode: DecodeCause,
    /// A local filesystem error. Permanent — the disk is the disk, and
    /// an indefinite retry hides the problem from the operator instead
    /// of surfacing it in history.
    write: app_ports.FsError,
    /// The segment id is no longer part of the aggregate. A bug, not a
    /// transient condition.
    not_in_job,

    pub const DecodeCause = enum { malformed, crc_mismatch, out_of_memory };

    /// Whether the segment should go back to pending with a durable
    /// backoff (true) or straight to terminal `failed` (false). The bias
    /// is Go's: when in doubt, retry.
    pub fn isTransient(self: Failure) bool {
        return switch (self) {
            .fetch => |e| switch (e) {
                // The provider just told us we are over our slot count.
                error.TooManyConnections => true,
                // The pool may hand out a different connection next time.
                error.AuthRequired => true,
                // Bad credentials do not fix themselves.
                error.AuthFailed => false,
                error.UnexpectedGreeting => true,
                // RFC 3977: 4xx is a transient negative, 5xx is not.
                error.ProtocolTransient => true,
                error.ProtocolPermanent => false,
                error.Network => true,
                // Never reaches the classifier — handled as `missing`
                // and as "nobody to ask" respectively — but a default
                // here would hide a future routing bug.
                error.ArticleMissing => false,
                error.NoPoolsAvailable => true,
                // Shutdown: the segment stays pending for the next run.
                error.Canceled => true,
                error.OutOfMemory => true,
            },
            .decode => |c| switch (c) {
                // Memory pressure is a property of this moment, not of
                // the article.
                .out_of_memory => true,
                .malformed, .crc_mismatch => false,
            },
            .write => false,
            .not_in_job => false,
        };
    }

    /// Widest output of `describe`.
    pub const DescribeBuf = [96]u8;

    /// The reason string persisted in `segments.last_error` and carried
    /// on `SegmentFailed`. Built from error names only — no paths, no
    /// message-ids, nothing an operator would have to redact.
    pub fn describe(self: Failure, buf: *DescribeBuf) []const u8 {
        return switch (self) {
            .fetch => |e| std.fmt.bufPrint(buf, "fetch: {t}", .{e}) catch unreachable,
            .decode => |c| std.fmt.bufPrint(buf, "yenc decode: {t}", .{c}) catch unreachable,
            .write => |e| std.fmt.bufPrint(buf, "write: {t}", .{e}) catch unreachable,
            .not_in_job => "segment not in job aggregate",
        };
    }
};

/// Maps a yEnc decode failure onto the three cases that matter for
/// retry policy.
pub fn decodeCause(e: yenc.DecodeError) Failure.DecodeCause {
    return switch (e) {
        error.OutOfMemory => .out_of_memory,
        error.CrcMismatch => .crc_mismatch,
        else => .malformed,
    };
}

// ---------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------

/// What became of one segment dispatch.
pub const Outcome = union(enum) {
    done: struct { bytes_on_disk: i64, file_offset: i64 },
    /// Every server answered 430.
    missing,
    /// The in-process budget is spent.
    failed: Failure,
    /// Shutdown interrupted the dispatch. The drainer skips these
    /// entirely, so the segment stays `pending` and a restart re-fetches
    /// it — safe because writes are idempotent at known offsets.
    cancelled,
};

/// One resolved segment, on its way to the drainer.
pub const Result = struct {
    segment_id: SegmentId,
    outcome: Outcome,
    /// Which server served it, for byte accounting. 0 when nothing was
    /// fetched.
    server_id: ServerId = 0,
};

// ---------------------------------------------------------------------
// The per-segment state machine
// ---------------------------------------------------------------------

/// One segment's in-flight dispatch. Replaces the goroutine that Go's
/// `processSegment` ran in: the same loop, turned inside out so the
/// waiting happens in the caller's reactor rather than in a thread.
pub const SegmentTask = struct {
    segment_id: SegmentId,
    /// Borrowed from the aggregate, which owns it for its lifetime.
    message_id: []const u8,
    /// In-process attempts consumed so far.
    attempt: u8 = 0,
    /// Earliest instant the next attempt may run.
    ready_at: Timestamp = 0,

    pub fn isReady(self: SegmentTask, now: Timestamp) bool {
        return self.ready_at <= now;
    }
};

/// What `step` wants the caller to do next.
pub const Step = union(enum) {
    /// The segment reached a verdict; hand it to `submit`.
    resolved: Result,
    /// Nothing to do until this instant. Call `step` again then.
    wait_until: Timestamp,
};

/// What `verdict` says about the job as a whole.
pub const Verdict = union(enum) {
    /// These segments are eligible for dispatch now. Slice belongs to
    /// the caller.
    work: []*Segment,
    /// Everything pending is retry-gated; re-poll at this instant.
    wait_until: Timestamp,
    /// Nothing pending and nothing deferred — the runner is finished.
    done,
};

// ---------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------

pub const Runner = struct {
    gpa: Allocator,
    store: ports.JobStore,
    fetcher: ports.ArticleFetcher,
    sink: Sink,
    txm: app_ports.Manager,
    fs: app_ports.Filesystem,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    opts: Options,
    /// Label only — the tiered fetcher picks the real server.
    hint_server: ServerId = 0,

    /// Borrowed. The caller loaded it through the store and releases it.
    job: *Job,
    /// `<incomplete>/<job id>`. Owned.
    job_dir: []u8,

    /// Resolved segments awaiting a flush.
    batch: std.ArrayList(Result) = .empty,
    /// When the current batch window opened. `flushDue` compares against
    /// it so the flush cadence is driven by the injected clock.
    window_opened_at: Timestamp = 0,
    /// Set once `begin` has run.
    started: bool = false,
    /// Which server served the most recent successful fetch. Read by
    /// `step` when it builds the `Result`; the byte accounter needs it,
    /// and threading it through `Outcome` would put transport detail in
    /// a domain-shaped type.
    last_server_id: ServerId = 0,

    pub const InitParams = struct {
        gpa: Allocator,
        store: ports.JobStore,
        fetcher: ports.ArticleFetcher,
        sink: Sink,
        txm: app_ports.Manager,
        fs: app_ports.Filesystem,
        clock: app_ports.Clock,
        logger: *log.Logger = &log.default,
        opts: Options = .{},
        hint_server: ServerId = 0,
        job: *Job,
        /// Root under which per-job temp directories live.
        incomplete_dir: []const u8,
    };

    pub fn init(p: InitParams) Allocator.Error!Runner {
        const dir = try std.fmt.allocPrint(p.gpa, "{s}/{d}", .{ p.incomplete_dir, p.job.id });
        return .{
            .gpa = p.gpa,
            .store = p.store,
            .fetcher = p.fetcher,
            .sink = p.sink,
            .txm = p.txm,
            .fs = p.fs,
            .clock = p.clock,
            .logger = p.logger,
            .opts = p.opts.normalized(),
            .hint_server = p.hint_server,
            .job = p.job,
            .job_dir = dir,
        };
    }

    pub fn deinit(self: *Runner) void {
        self.batch.deinit(self.gpa);
        self.gpa.free(self.job_dir);
        self.* = undefined;
    }

    /// Prepares the job for dispatch: clears segments a previous crash
    /// left `inflight`, transitions queued → downloading, and creates
    /// the temp directory.
    pub fn begin(self: *Runner, now: Timestamp) Error!void {
        const reset = self.job.resetInflightToPending();
        if (reset > 0) {
            self.logger.info("orchestrator: reset inflight segments", &.{
                log.int("job_id", self.job.id),
                log.uint("count", reset),
            });
            try self.flushAggregate(&.{});
        }
        if (try self.job.markStarted(now)) {
            try self.persist(true, &.{});
        }
        try self.fs.mkdirAll(self.job_dir);
        self.started = true;
        self.window_opened_at = now;
    }

    /// The segments eligible for dispatch right now, capped at `max`
    /// (pass `opts.workers` for the configured concurrency).
    ///
    /// The returned pointers stay valid for the aggregate's lifetime;
    /// the slice belongs to `a`.
    pub fn takeReady(self: *Runner, a: Allocator, now: Timestamp, max: usize) Allocator.Error![]*Segment {
        const all = try self.job.pendingSegments(a, now);
        if (all.len <= max) return all;
        // Shrink in place rather than reallocating: `pendingSegments`
        // already owns exactly one allocation and the caller frees it.
        if (a.resize(all, max)) return all[0..max];
        defer a.free(all);
        return a.dupe(*Segment, all[0..max]);
    }

    /// What to do next at the job level. Mirrors Go's outer `Run` loop.
    pub fn verdict(self: *Runner, a: Allocator, now: Timestamp) Allocator.Error!Verdict {
        const ready = try self.takeReady(a, now, self.opts.workers);
        if (ready.len > 0) return .{ .work = ready };
        a.free(ready);

        const next = self.job.nextRetryReadyAt(now) orelse return .done;
        const gap = @min(@max(next - now, 0), self.opts.max_poll_gap_ms);
        return .{ .wait_until = now + gap };
    }

    /// One turn of a segment's dispatch: at most one fetch attempt.
    ///
    /// Returns `.wait_until` for a backoff the caller must honour before
    /// calling again, and `.resolved` once the segment has a verdict.
    /// Never sleeps.
    pub fn step(self: *Runner, a: Allocator, task: *SegmentTask, now: Timestamp) Step {
        if (!task.isReady(now)) return .{ .wait_until = task.ready_at };

        task.attempt += 1;
        const out = self.attempt(a, task.*, now);
        switch (out) {
            .done, .missing, .cancelled => return .{ .resolved = .{
                .segment_id = task.segment_id,
                .outcome = out,
                .server_id = self.last_server_id,
            } },
            .failed => |f| {
                // Nobody to ask is not the segment's fault: wait for a
                // server to appear and do not consume the budget.
                if (f == .fetch and f.fetch == error.NoPoolsAvailable) {
                    task.attempt -= 1;
                    task.ready_at = now + self.opts.pool_wait_ms;
                    self.logger.info("orchestrator: no pools available; waiting", &.{
                        log.int("job_id", self.job.id),
                        log.int("segment_id", task.segment_id),
                    });
                    return .{ .wait_until = task.ready_at };
                }
                if (task.attempt < self.opts.max_attempts) {
                    task.ready_at = now + self.opts.inProcessBackoffMs(task.attempt);
                    var buf: Failure.DescribeBuf = undefined;
                    self.logger.debug("orchestrator: retrying segment", &.{
                        log.int("job_id", self.job.id),
                        log.int("segment_id", task.segment_id),
                        log.uint("attempt", task.attempt),
                        log.str("err", f.describe(&buf)),
                    });
                    return .{ .wait_until = task.ready_at };
                }
                return .{ .resolved = .{ .segment_id = task.segment_id, .outcome = out } };
            },
        }
    }

    /// One fetch + decode + write, with no retry of its own.
    pub fn attempt(self: *Runner, a: Allocator, task: SegmentTask, _: Timestamp) Outcome {
        self.last_server_id = 0;
        const body = self.fetcher.fetch(a, self.hint_server, task.message_id) catch |e| {
            if (e == error.ArticleMissing) return .missing;
            if (e == error.Canceled) return .cancelled;
            return .{ .failed = .{ .fetch = e } };
        };
        defer a.free(body.bytes);
        self.last_server_id = body.server_id;

        var article = yenc.decode(a, body.bytes) catch |e|
            return .{ .failed = .{ .decode = decodeCause(e) } };
        defer article.deinit(a);

        const ref = self.job.segmentById(task.segment_id) orelse
            return .{ .failed = .not_in_job };

        // yEnc multi-part carries a 1-based `=ypart begin`; single-part
        // articles have begin = 0 and land at offset 0.
        const offset: i64 = if (article.header.begin > 0) article.header.begin - 1 else 0;

        var buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&buf, "{s}/{d}.tmp", .{ self.job_dir, ref.file.id }) catch
            return .{ .failed = .{ .write = error.Io } };

        self.fs.writeAt(path, offset, article.payload, article.header.size) catch |e|
            return .{ .failed = .{ .write = e } };

        return .{ .done = .{
            .bytes_on_disk = @intCast(article.payload.len),
            .file_offset = offset,
        } };
    }

    /// Queues a resolved segment for the next flush.
    pub fn submit(self: *Runner, r: Result) Allocator.Error!void {
        try self.batch.append(self.gpa, r);
    }

    /// True when the batch has reached the size cap or the flush window
    /// has elapsed — the two conditions Go's `select` on ticker-vs-cap
    /// expressed.
    pub fn flushDue(self: *const Runner, now: Timestamp) bool {
        if (self.batch.items.len == 0) return false;
        if (self.batch.items.len >= self.opts.flush_batch_max) return true;
        return now - self.window_opened_at >= self.opts.flush_interval_ms;
    }

    /// Applies the queued results to the aggregate and persists them,
    /// with the segment rows, the job row and the events all in one
    /// transaction.
    pub fn flush(self: *Runner, now: Timestamp) Error!void {
        defer {
            self.batch.clearRetainingCapacity();
            self.window_opened_at = now;
        }
        if (self.batch.items.len == 0) return;

        // Ids that actually mutated the aggregate; a cancelled result
        // touches nothing and must not produce a segment UPDATE.
        var touched: std.ArrayList(SegmentId) = .empty;
        defer touched.deinit(self.gpa);
        try touched.ensureTotalCapacity(self.gpa, self.batch.items.len);

        for (self.batch.items) |r| {
            switch (r.outcome) {
                .cancelled => continue,
                .done => |d| self.job.markSegmentDone(.{
                    .segment_id = r.segment_id,
                    .bytes_on_disk = d.bytes_on_disk,
                    .file_offset = d.file_offset,
                }, now) catch |e| {
                    if (e == error.OutOfMemory) return error.OutOfMemory;
                    self.logger.err("orchestrator: markSegmentDone", &.{
                        log.int("segment_id", r.segment_id),
                        log.errv("err", e),
                    });
                    continue;
                },
                .missing => self.job.markSegmentMissing(r.segment_id, now) catch |e| {
                    if (e == error.OutOfMemory) return error.OutOfMemory;
                    self.logger.err("orchestrator: markSegmentMissing", &.{
                        log.int("segment_id", r.segment_id),
                        log.errv("err", e),
                    });
                    continue;
                },
                .failed => |f| try self.resolveFailure(r.segment_id, f, now),
            }
            touched.appendAssumeCapacity(r.segment_id);
        }

        // fail_hopeless: stop burning bandwidth on a release already
        // past PAR2's reach. Opt-in, matching SABnzbd.
        if (self.opts.fail_hopeless_ratio > 0) {
            _ = try self.job.abortIfHopeless(self.opts.fail_hopeless_ratio, now);
        }

        try self.flushAggregate(touched.items);
    }

    /// Flushes when `flushDue` says so. Convenience for a caller that
    /// polls; `flush` is the unconditional form.
    pub fn flushIfDue(self: *Runner, now: Timestamp) Error!void {
        if (self.flushDue(now)) try self.flush(now);
    }

    /// Durable retry versus terminal failure — the decision Go made in
    /// the `default` arm of `flushBatch`'s switch.
    fn resolveFailure(self: *Runner, id: SegmentId, f: Failure, now: Timestamp) Error!void {
        var buf: Failure.DescribeBuf = undefined;
        const msg = f.describe(&buf);
        const ref = self.job.segmentById(id) orelse {
            self.logger.err("orchestrator: failure for unknown segment", &.{
                log.int("segment_id", id),
            });
            return;
        };
        const attempts = ref.seg.attempts;
        if (f.isTransient() and attempts < self.opts.max_durable_attempts) {
            const backoff = self.opts.durableBackoffMs(attempts);
            self.job.markSegmentForRetry(id, now + backoff, msg) catch |e| {
                if (e == error.OutOfMemory) return error.OutOfMemory;
                self.logger.err("orchestrator: markSegmentForRetry", &.{
                    log.int("segment_id", id),
                    log.errv("err", e),
                });
                return;
            };
            self.logger.info("orchestrator: durable retry scheduled", &.{
                log.int("job_id", self.job.id),
                log.int("segment_id", id),
                log.int("attempts", attempts + 1),
                log.int("next_at", now + backoff),
                log.str("err", msg),
            });
            return;
        }
        self.job.markSegmentFailed(id, msg, now) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory;
            self.logger.err("orchestrator: markSegmentFailed", &.{
                log.int("segment_id", id),
                log.errv("err", e),
            });
        };
    }

    /// Persists the aggregate and publishes its queued events in one
    /// transaction, plus the segment rows named by `touched`.
    fn flushAggregate(self: *Runner, touched: []const SegmentId) Error!void {
        const updates = try self.buildSegmentUpdates(touched);
        defer self.gpa.free(updates);
        return self.persist(self.job.isStateDirty(), updates);
    }

    /// One transaction: segment rows, then either a full job save or the
    /// two-column counter update, then the events.
    fn persist(self: *Runner, full_save: bool, updates: []const SegmentUpdate) Error!void {
        const Args = struct {
            runner: *Runner,
            full_save: bool,
            updates: []const SegmentUpdate,
        };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const r = args.runner;
                try r.store.updateSegmentBatch(unit, args.updates);
                if (args.full_save) {
                    try r.store.save(unit, r.job);
                } else {
                    try r.store.updateCounters(unit, r.job);
                }
                const events = try r.job.pullEvents();
                defer devents.deinitAll(r.gpa, events);
                try r.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{
            .runner = self,
            .full_save = full_save,
            .updates = updates,
        }, Body.run);
    }

    /// Reads the post-mutation state of every touched segment. Duplicate
    /// ids collapse: a batch that resolved the same segment twice (which
    /// a double-submit would produce) writes one row, not two.
    fn buildSegmentUpdates(self: *Runner, touched: []const SegmentId) Allocator.Error![]SegmentUpdate {
        if (touched.len == 0) return &.{};
        var out: std.ArrayList(SegmentUpdate) = .empty;
        errdefer out.deinit(self.gpa);
        try out.ensureTotalCapacity(self.gpa, touched.len);
        for (touched) |id| {
            var already = false;
            for (out.items) |u| {
                if (u.segment_id == id) {
                    already = true;
                    break;
                }
            }
            if (already) continue;
            const ref = self.job.segmentById(id) orelse continue;
            out.appendAssumeCapacity(.{
                .segment_id = ref.seg.id,
                .state = ref.seg.state,
                .attempts = ref.seg.attempts,
                .last_error = ref.seg.lastError(),
                .file_offset = ref.seg.file_offset,
                .next_retry_at = ref.seg.next_retry_at,
            });
        }
        return out.toOwnedSlice(self.gpa);
    }
};

// =====================================================================
// Tests — a translation of internal/app/download/orchestrator_test.go,
// plus the coverage the batched-flush and dirty-bit paths need.
// =====================================================================

const testing = std.testing;

/// A runner wired entirely to fakes. Built in place because the port
/// vtables it hands out capture `&self.<field>`.
const Harness = struct {
    fs: app_ports.FakeFs = undefined,
    store: ports.FakeJobStore = undefined,
    fetcher: ports.FakeFetcher = undefined,
    sink: FakeSink = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 1 },
    logger: log.Logger = .{},
    runner: Runner = undefined,
    job: *Job = undefined,

    const incomplete = "/inc";

    fn init(
        self: *Harness,
        bodies: []const ports.FakeFetcher.Canned,
        fail_first: u32,
        opts: Options,
    ) !void {
        self.* = .{};
        self.fs = app_ports.FakeFs.init(testing.allocator);
        self.store = ports.FakeJobStore.init(testing.allocator);
        self.fetcher = .{ .bodies = bodies, .fail_first = fail_first };
        // No sinks on the logger keeps the suite quiet; the runner still
        // exercises every log call site.
        self.logger.setLevel(.debug);

        self.job = try ports.testJob(testing.allocator, "h", bodies[0].message_id, 64);
        try self.store.insert(self.job);

        self.runner = try Runner.init(.{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .fetcher = self.fetcher.fetcher(),
            .sink = self.sink.sink(),
            .txm = self.ftx.manager(),
            .fs = self.fs.filesystem(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
            .opts = opts,
            .job = self.job,
            .incomplete_dir = incomplete,
        });
    }

    /// Swaps in a wider aggregate than the default one-segment job.
    /// Takes ownership and rebuilds the runner so its temp-directory
    /// path matches the new job's id.
    fn useJob(self: *Harness, job: *Job) !void {
        try self.store.insert(job);
        // Read the options out first: `deinit` poisons the runner.
        const opts = self.runner.opts;
        self.runner.deinit();
        self.job = job;
        self.runner = try Runner.init(.{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .fetcher = self.fetcher.fetcher(),
            .sink = self.sink.sink(),
            .txm = self.ftx.manager(),
            .fs = self.fs.filesystem(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
            .opts = opts,
            .job = job,
            .incomplete_dir = incomplete,
        });
    }

    fn deinit(self: *Harness) void {
        self.runner.deinit();
        self.store.deinit();
        self.fs.deinit();
    }

    fn now(self: *const Harness) Timestamp {
        return self.clock.t;
    }

    fn segId(self: *const Harness) SegmentId {
        return self.job.files[0].segments[0].id;
    }

    fn seg(self: *const Harness) *Segment {
        return &self.job.files[0].segments[0];
    }

    fn task(self: *const Harness) SegmentTask {
        const s = self.seg();
        return .{ .segment_id = s.id, .message_id = s.message_id };
    }

    /// Drives one segment to a verdict the way the reactor would: step,
    /// honour the requested wake-up by moving the fake clock, repeat.
    /// Records every delay so a test can assert the schedule.
    fn drive(self: *Harness, t: *SegmentTask, waits: *std.ArrayList(Millis)) !Result {
        for (0..64) |_| {
            switch (self.runner.step(testing.allocator, t, self.clock.t)) {
                .resolved => |r| return r,
                .wait_until => |at| {
                    try waits.append(testing.allocator, at - self.clock.t);
                    self.clock.set(at);
                },
            }
        }
        return error.StepLoopDidNotConverge;
    }
};

fn yencBody(a: Allocator, payload: []const u8) ![]u8 {
    return yenc.encodeForTest(a, .{ .name = "f.bin", .payload = payload });
}

// ---- options --------------------------------------------------------

test "zero options normalise to the Go defaults" {
    const o = (Options{}).normalized();
    try testing.expectEqual(@as(u16, 1), o.workers);
    try testing.expectEqual(@as(Millis, 1000), o.flush_interval_ms);
    try testing.expectEqual(@as(usize, 1024), o.flush_batch_max);
    try testing.expectEqual(@as(u8, 3), o.max_attempts);
    try testing.expectEqual(@as(Millis, 200), o.base_backoff_ms);
    try testing.expectEqual(@as(i32, 10), o.max_durable_attempts);
    try testing.expectEqual(@as(Millis, 30_000), o.durable_backoff_base_ms);
    try testing.expectEqual(@as(Millis, 1_800_000), o.durable_backoff_max_ms);
    try testing.expectEqual(@as(Millis, 60_000), o.max_poll_gap_ms);
    try testing.expectEqual(@as(Millis, 5_000), o.pool_wait_ms);
    // fail_hopeless stays off: SABnzbd treats it as opt-in and so do we.
    try testing.expectEqual(@as(f64, 0), o.fail_hopeless_ratio);
    // An explicit value survives normalisation.
    const custom = (Options{ .max_attempts = 1, .workers = 8 }).normalized();
    try testing.expectEqual(@as(u8, 1), custom.max_attempts);
    try testing.expectEqual(@as(u16, 8), custom.workers);
}

test "in-process backoff doubles per attempt and saturates" {
    const o = (Options{}).normalized();
    try testing.expectEqual(@as(Millis, 0), o.inProcessBackoffMs(0));
    try testing.expectEqual(@as(Millis, 200), o.inProcessBackoffMs(1));
    try testing.expectEqual(@as(Millis, 400), o.inProcessBackoffMs(2));
    try testing.expectEqual(@as(Millis, 800), o.inProcessBackoffMs(3));
    try testing.expectEqual(@as(Millis, std.math.maxInt(i32)), o.inProcessBackoffMs(200));
}

test "durable backoff grows past the base and clamps at the ceiling" {
    const o = (Options{}).normalized();
    // Go's loop multiplies while d < max, so a segment with zero prior
    // durable retries waits the base and one prior retry already waits
    // double. Preserved deliberately — the constant tuning downstream
    // was done against these numbers.
    try testing.expectEqual(@as(Millis, 30_000), o.durableBackoffMs(0));
    try testing.expectEqual(@as(Millis, 60_000), o.durableBackoffMs(1));
    try testing.expectEqual(@as(Millis, 120_000), o.durableBackoffMs(2));
    try testing.expectEqual(@as(Millis, 1_800_000), o.durableBackoffMs(20));
    // A squeezed configuration (what the fast tests use) never exceeds
    // its own ceiling.
    const tight = (Options{ .durable_backoff_base_ms = 1, .durable_backoff_max_ms = 1 }).normalized();
    try testing.expectEqual(@as(Millis, 1), tight.durableBackoffMs(0));
    try testing.expectEqual(@as(Millis, 1), tight.durableBackoffMs(9));
}

// ---- classification -------------------------------------------------

test "transient classification matches the Go table" {
    const cases = [_]struct { Failure, bool }{
        .{ .{ .fetch = error.TooManyConnections }, true },
        .{ .{ .fetch = error.AuthRequired }, true },
        .{ .{ .fetch = error.AuthFailed }, false },
        .{ .{ .fetch = error.UnexpectedGreeting }, true },
        .{ .{ .fetch = error.ProtocolTransient }, true },
        .{ .{ .fetch = error.ProtocolPermanent }, false },
        .{ .{ .fetch = error.Network }, true },
        .{ .{ .fetch = error.NoPoolsAvailable }, true },
        .{ .{ .fetch = error.Canceled }, true },
        .{ .{ .fetch = error.OutOfMemory }, true },
        // yEnc failures are permanent: the same bytes decode the same
        // way. Go reached this verdict by string-matching the wrap
        // prefix; here it is a tag.
        .{ .{ .decode = .malformed }, false },
        .{ .{ .decode = .crc_mismatch }, false },
        .{ .{ .decode = .out_of_memory }, true },
        // Local IO is permanent — the disk is the disk.
        .{ .{ .write = error.Denied }, false },
        .{ .{ .write = error.Io }, false },
        .{ .not_in_job, false },
    };
    for (cases) |c| try testing.expectEqual(c[1], c[0].isTransient());
}

test "a failure describes itself without paths or message ids" {
    var buf: Failure.DescribeBuf = undefined;
    try testing.expectEqualStrings(
        "fetch: TooManyConnections",
        (Failure{ .fetch = error.TooManyConnections }).describe(&buf),
    );
    try testing.expectEqualStrings(
        "yenc decode: crc_mismatch",
        (Failure{ .decode = .crc_mismatch }).describe(&buf),
    );
    try testing.expectEqualStrings("write: Denied", (Failure{ .write = error.Denied }).describe(&buf));
    try testing.expectEqualStrings("segment not in job aggregate", (Failure{ .not_in_job = {} }).describe(&buf));
}

test "decode errors map onto the three retry-relevant causes" {
    try testing.expectEqual(Failure.DecodeCause.out_of_memory, decodeCause(error.OutOfMemory));
    try testing.expectEqual(Failure.DecodeCause.crc_mismatch, decodeCause(error.CrcMismatch));
    try testing.expectEqual(Failure.DecodeCause.malformed, decodeCause(error.MissingYEnd));
    try testing.expectEqual(Failure.DecodeCause.malformed, decodeCause(error.DanglingEscape));
}

// ---- one segment, end to end ----------------------------------------

test "a clean fetch decodes, writes at the right offset and completes the job" {
    const payload = "hello hoardarr, twelve dozen bytes of payload here ok";
    const body = try yencBody(testing.allocator, payload);
    defer testing.allocator.free(body);

    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = body }}, 0, .{});
    defer h.deinit();

    try h.runner.begin(h.now());
    // begin() saved the started transition; JobStarted is on the bus.
    try testing.expect(h.sink.has("download.job.started"));

    var t = h.task();
    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);
    const r = try h.drive(&t, &waits);

    try testing.expectEqual(@as(usize, 0), waits.items.len);
    try testing.expectEqual(@as(usize, 1), h.fetcher.calls);
    try testing.expectEqual(@as(i64, @intCast(payload.len)), r.outcome.done.bytes_on_disk);
    try testing.expectEqual(@as(i64, 0), r.outcome.done.file_offset);

    try h.runner.submit(r);
    try h.runner.flush(h.now());

    try testing.expectEqual(ports.SegmentState.done, h.seg().state);
    try testing.expectEqual(job_mod.JobState.download_complete, h.job.state);
    try testing.expectEqual(@as(i64, @intCast(payload.len)), h.job.done_bytes);
    // The payload really landed on disk at offset 0.
    const disk = h.fs.contents("/inc/1/1.tmp").?;
    try testing.expectEqualStrings(payload, disk[0..payload.len]);
    // Segment row, file completion and the download-phase close all
    // rode out on the bus.
    try testing.expect(h.sink.has("download.segment.completed"));
    try testing.expect(h.sink.has("download.file.completed"));
    try testing.expect(h.sink.has("download.job.download_complete"));
    try testing.expectEqual(@as(usize, 1), h.store.segment_rows);
    try testing.expect(h.ftx.balanced());
}

test "transient failures are retried in process on the doubling schedule" {
    // Ports TestOrchestrator_RetriesTransientFailures. Go asserted the
    // fetch count; we assert the count *and* the delay schedule, which
    // Go could not do without sleeping.
    const body = try yencBody(testing.allocator, "hello!");
    defer testing.allocator.free(body);

    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = body }}, 2, .{ .max_attempts = 3 });
    defer h.deinit();
    try h.runner.begin(h.now());

    var t = h.task();
    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);
    const r = try h.drive(&t, &waits);

    try testing.expectEqual(@as(usize, 3), h.fetcher.calls);
    try testing.expectEqualSlices(Millis, &.{ 200, 400 }, waits.items);
    try testing.expect(r.outcome == .done);

    try h.runner.submit(r);
    try h.runner.flush(h.now());
    try testing.expectEqual(ports.SegmentState.done, h.seg().state);
    // In-process retries are invisible to the aggregate.
    try testing.expectEqual(@as(i32, 0), h.seg().attempts);
}

test "the in-process budget exhausts into a durable retry, not a failure" {
    // Ports the first half of TestOrchestrator_GivesUpAfterMaxAttempts:
    // three in-process attempts, then a deferred re-dispatch rather
    // than a terminal state.
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "unused" }}, 99, .{
        .max_attempts = 3,
        .max_durable_attempts = 1,
        .durable_backoff_base_ms = 50,
        .durable_backoff_max_ms = 50,
    });
    defer h.deinit();
    try h.runner.begin(h.now());

    var t = h.task();
    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);
    const r = try h.drive(&t, &waits);

    try testing.expectEqual(@as(usize, 3), h.fetcher.calls);
    try testing.expectEqualSlices(Millis, &.{ 200, 400 }, waits.items);
    try testing.expect(r.outcome == .failed);

    const at = h.now();
    try h.runner.submit(r);
    try h.runner.flush(at);

    // Back to pending with a future gate, one attempt on the clock.
    try testing.expectEqual(ports.SegmentState.pending, h.seg().state);
    try testing.expectEqual(@as(i32, 1), h.seg().attempts);
    try testing.expectEqual(@as(?Timestamp, at + 50), h.seg().next_retry_at);
    // Nothing terminal was published and the job is still alive.
    try testing.expect(!h.sink.has("download.segment.failed"));
    try testing.expectEqual(job_mod.JobState.downloading, h.job.state);
    // The gate is honoured: no work is ready, but the runner is not done.
    switch (try h.runner.verdict(testing.allocator, at)) {
        .wait_until => |w| try testing.expectEqual(at + 50, w),
        else => return error.ExpectedWait,
    }
}

test "the durable budget exhausts into a terminal segment failure" {
    // Second half of TestOrchestrator_GivesUpAfterMaxAttempts: with
    // max_durable_attempts = 1, the segment's second dispatch is its
    // last. Total fetches = max_attempts × (1 + 1 durable retry) = 6,
    // exactly the number Go asserted.
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "unused" }}, 99, .{
        .max_attempts = 3,
        .max_durable_attempts = 1,
        .durable_backoff_base_ms = 1,
        .durable_backoff_max_ms = 1,
        .max_poll_gap_ms = 1,
    });
    defer h.deinit();
    try h.runner.begin(h.now());

    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);

    // Dispatch 1 → durable retry.
    var t1 = h.task();
    try h.runner.submit(try h.drive(&t1, &waits));
    try h.runner.flush(h.now());
    try testing.expectEqual(ports.SegmentState.pending, h.seg().state);

    // Honour the gate, then dispatch 2 → terminal.
    const gate = h.seg().next_retry_at.?;
    h.clock.set(gate);
    const ready = try h.runner.verdict(testing.allocator, h.now());
    try testing.expectEqual(@as(usize, 1), ready.work.len);
    testing.allocator.free(ready.work);

    var t2 = h.task();
    try h.runner.submit(try h.drive(&t2, &waits));
    try h.runner.flush(h.now());

    try testing.expectEqual(@as(usize, 6), h.fetcher.calls);
    try testing.expectEqual(ports.SegmentState.failed, h.seg().state);
    try testing.expect(h.sink.has("download.segment.failed"));
    // Nothing was retrieved at all, so the download phase fails outright
    // rather than sitting in download_complete looking green.
    try testing.expectEqual(job_mod.JobState.failed, h.job.state);
    try testing.expect(h.sink.has("download.job.download_failed"));
    try testing.expect(h.sink.has("download.job.failed"));
    // And the runner reports itself finished.
    try testing.expectEqual(Verdict.done, try h.runner.verdict(testing.allocator, h.now()));
}

test "a durable retry that succeeds on a later dispatch completes the job" {
    // Ports TestOrchestrator_DurableRetry_TransientThenSuccess. Five
    // failures: three in dispatch 1, two in dispatch 2, success on the
    // sixth call — exactly one durable retry on the clock.
    const body = try yencBody(testing.allocator, "hello!");
    defer testing.allocator.free(body);

    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = body }}, 5, .{
        .max_attempts = 3,
        .max_durable_attempts = 5,
        .durable_backoff_base_ms = 1,
        .durable_backoff_max_ms = 1,
    });
    defer h.deinit();
    try h.runner.begin(h.now());

    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);

    var t1 = h.task();
    try h.runner.submit(try h.drive(&t1, &waits));
    try h.runner.flush(h.now());
    try testing.expectEqual(ports.SegmentState.pending, h.seg().state);

    h.clock.set(h.seg().next_retry_at.?);
    var t2 = h.task();
    try h.runner.submit(try h.drive(&t2, &waits));
    try h.runner.flush(h.now());

    try testing.expectEqual(@as(usize, 6), h.fetcher.calls);
    try testing.expectEqual(ports.SegmentState.done, h.seg().state);
    try testing.expectEqual(@as(i32, 1), h.seg().attempts);
    try testing.expectEqual(job_mod.JobState.download_complete, h.job.state);
}

test "no pools available waits without consuming the retry budget" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 99, .{
        .max_attempts = 3,
        .pool_wait_ms = 5000,
    });
    defer h.deinit();
    h.fetcher.err = error.NoPoolsAvailable;
    try h.runner.begin(h.now());

    var t = h.task();
    const start = h.now();
    // Three turns, three pool waits, and the attempt counter never
    // moves — otherwise a job queued before the operator configured a
    // server would burn its whole budget waiting.
    for (0..3) |_| {
        switch (h.runner.step(testing.allocator, &t, h.clock.t)) {
            .wait_until => |at| {
                try testing.expectEqual(h.clock.t + 5000, at);
                h.clock.set(at);
            },
            .resolved => return error.ShouldNotResolve,
        }
    }
    try testing.expectEqual(@as(u8, 0), t.attempt);
    try testing.expectEqual(@as(usize, 3), h.fetcher.calls);
    try testing.expectEqual(start + 15_000, h.now());
}

test "a 430 from every server is terminal on the first attempt" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "unused" }}, 0, .{});
    defer h.deinit();
    // Asking for a message the script does not know is a 430 everywhere.
    h.job.files[0].segments[0].state = .pending;
    try h.runner.begin(h.now());

    var t: SegmentTask = .{ .segment_id = h.segId(), .message_id = "ghost@h" };
    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);
    const r = try h.drive(&t, &waits);

    // No retry: re-asking the same servers cannot conjure the article.
    try testing.expectEqual(@as(usize, 1), h.fetcher.calls);
    try testing.expectEqual(@as(usize, 0), waits.items.len);
    try testing.expect(r.outcome == .missing);

    try h.runner.submit(r);
    try h.runner.flush(h.now());
    try testing.expectEqual(ports.SegmentState.missing, h.seg().state);
    try testing.expect(h.sink.has("download.segment.missing"));
    try testing.expectEqual(@as(i64, 64), h.job.failed_bytes);
}

test "a malformed body fails terminally without a durable retry" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "not yenc at all" }}, 0, .{ .max_attempts = 1 });
    defer h.deinit();
    try h.runner.begin(h.now());

    var t = h.task();
    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);
    const r = try h.drive(&t, &waits);
    try testing.expect(r.outcome == .failed);
    try testing.expect(!r.outcome.failed.isTransient());

    try h.runner.submit(r);
    try h.runner.flush(h.now());
    // Straight to failed — no next_retry_at, no second dispatch.
    try testing.expectEqual(ports.SegmentState.failed, h.seg().state);
    try testing.expectEqual(@as(?Timestamp, null), h.seg().next_retry_at);
    try testing.expectEqualStrings("yenc decode: malformed", h.seg().lastError());
}

test "a cancelled dispatch leaves the segment pending and writes nothing" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "unused" }}, 99, .{});
    defer h.deinit();
    h.fetcher.err = error.Canceled;
    try h.runner.begin(h.now());
    h.store.segment_rows = 0;

    var t = h.task();
    var waits: std.ArrayList(Millis) = .empty;
    defer waits.deinit(testing.allocator);
    const r = try h.drive(&t, &waits);
    try testing.expect(r.outcome == .cancelled);

    try h.runner.submit(r);
    try h.runner.flush(h.now());
    // The crash-recovery path: still pending, no segment UPDATE, so a
    // restart re-dispatches it cleanly.
    try testing.expectEqual(ports.SegmentState.pending, h.seg().state);
    try testing.expectEqual(@as(usize, 0), h.store.segment_rows);
    try testing.expectEqual(job_mod.JobState.downloading, h.job.state);
}

// ---- the flush path -------------------------------------------------

test "a steady-state flush updates counters instead of the whole row" {
    // The behaviour 023c051 introduced: while the job's non-counter
    // fields are clean, the flush must not run the 12-column UPDATE.
    const body = try yencBody(testing.allocator, "abc");
    defer testing.allocator.free(body);

    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = body }}, 0, .{});
    defer h.deinit();
    try h.runner.begin(h.now());

    // begin() did a full save for the started transition, which is a
    // state change and therefore correct.
    const saves_after_begin = h.store.saves;
    try testing.expect(saves_after_begin >= 1);
    try testing.expect(!h.job.isStateDirty());

    // A pure counter move: done_bytes grows, no transition.
    h.job.done_bytes += 4096;
    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .cancelled });
    try h.runner.flush(h.now());
    try testing.expectEqual(saves_after_begin, h.store.saves);
    try testing.expectEqual(@as(usize, 1), h.store.counter_updates);

    // A transition dirties the aggregate, so the next flush pays for a
    // full save — and clears the bit again.
    _ = try h.job.pause(h.now());
    try testing.expect(h.job.isStateDirty());
    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .cancelled });
    try h.runner.flush(h.now());
    try testing.expectEqual(saves_after_begin + 1, h.store.saves);
    try testing.expectEqual(@as(usize, 1), h.store.counter_updates);
    try testing.expect(!h.job.isStateDirty());
}

test "flushDue fires on the batch cap and on the window, never when empty" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{
        .flush_interval_ms = 1000,
        .flush_batch_max = 2,
    });
    defer h.deinit();
    try h.runner.begin(h.now());
    const t0 = h.now();

    // Empty batch never flushes, however long the window has been open.
    try testing.expect(!h.runner.flushDue(t0 + 10_000));

    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .cancelled });
    try testing.expect(!h.runner.flushDue(t0));
    try testing.expect(!h.runner.flushDue(t0 + 999));
    try testing.expect(h.runner.flushDue(t0 + 1000));

    // The cap wins before the window elapses.
    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .cancelled });
    try testing.expect(h.runner.flushDue(t0));

    // Flushing reopens the window.
    try h.runner.flush(t0 + 1000);
    try testing.expect(!h.runner.flushDue(t0 + 1500));
}

test "flushIfDue is a no-op below the threshold" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{ .flush_interval_ms = 1000 });
    defer h.deinit();
    try h.runner.begin(h.now());
    const before = h.store.counter_updates;
    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .cancelled });
    try h.runner.flushIfDue(h.now());
    try testing.expectEqual(before, h.store.counter_updates);
    try h.runner.flushIfDue(h.now() + 1000);
    try testing.expectEqual(before + 1, h.store.counter_updates);
}

test "a duplicate submit for one segment writes a single row" {
    const body = try yencBody(testing.allocator, "abc");
    defer testing.allocator.free(body);
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = body }}, 0, .{});
    defer h.deinit();
    try h.runner.begin(h.now());
    h.store.segment_rows = 0;

    const r: Result = .{ .segment_id = h.segId(), .outcome = .{ .done = .{ .bytes_on_disk = 3, .file_offset = 0 } } };
    try h.runner.submit(r);
    try h.runner.submit(r);
    try h.runner.flush(h.now());

    try testing.expectEqual(@as(usize, 1), h.store.segment_rows);
    // markSegmentDone is idempotent, so the byte counter moved once.
    try testing.expectEqual(@as(i64, 3), h.job.done_bytes);
}

test "a failed save rolls the transaction back and publishes nothing" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{});
    defer h.deinit();
    try h.runner.begin(h.now());
    h.sink.reset();

    h.store.fail_save = error.Backend;
    _ = try h.job.pause(h.now()); // dirties the aggregate → full save path
    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .cancelled });
    try testing.expectError(error.Backend, h.runner.flush(h.now()));

    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expectEqual(@as(u32, 1), h.ftx.rollbacks);
    try testing.expect(h.ftx.balanced());
}

test "fail_hopeless aborts mid-download once the threshold is crossed" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{ .fail_hopeless_ratio = 0.05 });
    defer h.deinit();

    // Three segments so the job is still mid-download when the ratio
    // trips: one delivered, one lost, one untouched. If every segment
    // had resolved, `completeDownloadPhase` would have decided the
    // job's fate first and fail_hopeless would have nothing to do —
    // which is exactly the Go ordering.
    var job = try testing.allocator.create(Job);
    job.* = try Job.init(testing.allocator, .{
        .nzb_hash = "hopeless",
        .name = "hopeless",
        .files = &.{.{
            .filename = "h.bin",
            .size_bytes = 100,
            .segments = &.{
                .{ .seq_index = 1, .message_id = "a@h", .bytes = 30 },
                .{ .seq_index = 2, .message_id = "b@h", .bytes = 30 },
                .{ .seq_index = 3, .message_id = "c@h", .bytes = 40 },
            },
        }},
    }, 0);
    try h.useJob(job);
    try h.runner.begin(h.now());

    const segs = job.files[0].segments;
    try h.runner.submit(.{
        .segment_id = segs[0].id,
        .outcome = .{ .done = .{ .bytes_on_disk = 30, .file_offset = 0 } },
    });
    try h.runner.submit(.{ .segment_id = segs[1].id, .outcome = .missing });
    try h.runner.flush(h.now());

    // 30 of 100 bytes lost is well past 5%: stop burning bandwidth on
    // a release PAR2 cannot rescue.
    try testing.expectEqual(job_mod.JobState.failed, job.state);
    try testing.expect(h.sink.has("download.job.download_failed"));
    try testing.expect(h.sink.has("download.job.failed"));
    try testing.expect(std.mem.indexOf(u8, job.errorMsg(), "exceeds") != null);
    try testing.expect(std.mem.indexOf(u8, job.errorMsg(), "30%") != null);
}

test "fail_hopeless stays out of the way when disabled" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{});
    defer h.deinit();
    try h.runner.begin(h.now());
    try h.runner.submit(.{ .segment_id = h.segId(), .outcome = .missing });
    try h.runner.flush(h.now());
    // Every segment resolved with nothing retrieved, so the download
    // phase fails — but on the "no bytes at all" rule, not the ratio.
    try testing.expectEqualStrings("download failed: no segments retrieved", h.job.errorMsg());
}

// ---- begin, poll and concurrency ------------------------------------

test "begin resets crash leftovers, marks started and creates the job dir" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{});
    defer h.deinit();

    // Simulate a crash mid-fetch.
    h.job.files[0].segments[0].state = .inflight;
    try h.runner.begin(h.now());

    try testing.expectEqual(ports.SegmentState.pending, h.seg().state);
    try testing.expectEqual(job_mod.JobState.downloading, h.job.state);
    try testing.expect(h.fs.has("/inc/1"));
    try testing.expect(h.sink.has("download.job.started"));
    // The reset flush plus the started save: two transactions, both
    // closed.
    try testing.expectEqual(@as(u32, 2), h.ftx.begins);
    try testing.expect(h.ftx.balanced());
}

test "begin on an already-started job emits nothing new" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{});
    defer h.deinit();
    _ = try h.job.markStarted(1);
    const events = try h.job.pullEvents();
    devents.deinitAll(testing.allocator, events);

    try h.runner.begin(h.now());
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expectEqual(@as(u32, 0), h.ftx.begins);
}

test "takeReady honours the worker cap and the retry gate" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{ .workers = 2 });
    defer h.deinit();

    // Widen the job to four segments so the cap is observable.
    var job = try testing.allocator.create(Job);
    job.* = try Job.init(testing.allocator, .{
        .nzb_hash = "wide",
        .name = "wide",
        .files = &.{.{
            .filename = "w.bin",
            .segments = &.{
                .{ .seq_index = 1, .message_id = "a@h" },
                .{ .seq_index = 2, .message_id = "b@h" },
                .{ .seq_index = 3, .message_id = "c@h" },
                .{ .seq_index = 4, .message_id = "d@h" },
            },
        }},
    }, 0);
    try h.useJob(job);

    const two = try h.runner.takeReady(testing.allocator, 100, 2);
    defer testing.allocator.free(two);
    try testing.expectEqual(@as(usize, 2), two.len);

    const all = try h.runner.takeReady(testing.allocator, 100, 99);
    defer testing.allocator.free(all);
    try testing.expectEqual(@as(usize, 4), all.len);

    // Gate one segment into the future; it drops out of the ready set
    // but keeps the runner alive.
    try job.markSegmentForRetry(job.files[0].segments[0].id, 500, "later");
    for (job.files[0].segments[1..]) |*s| try job.markSegmentMissing(s.id, 100);
    const gated = try h.runner.takeReady(testing.allocator, 100, 99);
    defer testing.allocator.free(gated);
    try testing.expectEqual(@as(usize, 0), gated.len);
    switch (try h.runner.verdict(testing.allocator, 100)) {
        .wait_until => |w| try testing.expectEqual(@as(Timestamp, 500), w),
        else => return error.ExpectedWait,
    }
}

test "the poll gap is clamped so operator actions are noticed promptly" {
    var h: Harness = undefined;
    try h.init(&.{.{ .message_id = "m@h", .body = "x" }}, 0, .{ .max_poll_gap_ms = 1000 });
    defer h.deinit();
    // A 30-minute durable gate must not turn into a 30-minute sleep.
    try h.job.markSegmentForRetry(h.segId(), 1_800_000, "later");
    switch (try h.runner.verdict(testing.allocator, 0)) {
        .wait_until => |w| try testing.expectEqual(@as(Timestamp, 1000), w),
        else => return error.ExpectedWait,
    }
}

test "two segments interleave through one runner without corrupting state" {
    // The single-threaded answer to Go's worker pool: two tasks in
    // flight, stepped alternately, resolving out of order. The invariant
    // is that the aggregate's counters and the persisted rows agree with
    // the recount regardless of interleaving.
    // Multi-part articles, so the two writes land at different offsets
    // inside one temp file — the property that makes cancel-and-restart
    // safe in the first place.
    const body_a = try yenc.encodeForTest(testing.allocator, .{
        .name = "p.bin",
        .payload = "first part",
        .part = 1,
        .total = 2,
        .begin = 1,
        .end = 10,
    });
    defer testing.allocator.free(body_a);
    const body_b = try yenc.encodeForTest(testing.allocator, .{
        .name = "p.bin",
        .payload = "second part!",
        .part = 2,
        .total = 2,
        .begin = 11,
        .end = 22,
    });
    defer testing.allocator.free(body_b);

    var h: Harness = undefined;
    try h.init(&.{
        .{ .message_id = "a@h", .body = body_a },
        .{ .message_id = "b@h", .body = body_b },
    }, 1, .{ .workers = 2, .max_attempts = 2 });
    defer h.deinit();

    var job = try testing.allocator.create(Job);
    job.* = try Job.init(testing.allocator, .{
        .nzb_hash = "pair",
        .name = "pair",
        .files = &.{.{
            .filename = "p.bin",
            .size_bytes = 22,
            .segments = &.{
                .{ .seq_index = 1, .message_id = "a@h", .bytes = 10 },
                .{ .seq_index = 2, .message_id = "b@h", .bytes = 12 },
            },
        }},
    }, 0);
    try h.useJob(job);
    try h.runner.begin(h.now());

    var t_a: SegmentTask = .{ .segment_id = job.files[0].segments[0].id, .message_id = "a@h" };
    var t_b: SegmentTask = .{ .segment_id = job.files[0].segments[1].id, .message_id = "b@h" };

    // Both fail their first attempt and ask for the same 200ms backoff.
    const s1 = h.runner.step(testing.allocator, &t_a, h.now());
    const s2 = h.runner.step(testing.allocator, &t_b, h.now());
    try testing.expectEqual(h.now() + 200, s1.wait_until);
    try testing.expectEqual(h.now() + 200, s2.wait_until);

    // Time passes once for both — they share the reactor, not a thread
    // each. B resolves first, A second: the reverse of dispatch order.
    h.clock.advance(200);
    const r_b = h.runner.step(testing.allocator, &t_b, h.now()).resolved;
    const r_a = h.runner.step(testing.allocator, &t_a, h.now()).resolved;
    try testing.expect(r_b.outcome == .done);
    try testing.expect(r_a.outcome == .done);

    try h.runner.submit(r_b);
    try h.runner.submit(r_a);
    try h.runner.flush(h.now());

    const counts = job.recountUnresolved();
    try testing.expectEqual(@as(usize, 0), counts.non_recovery_vol);
    try testing.expectEqual(counts.non_recovery_vol, job.unresolved_non_recovery_vol);
    try testing.expectEqual(job_mod.JobState.download_complete, job.state);
    try testing.expectEqual(@as(i64, 22), job.done_bytes);
    try testing.expectEqual(@as(usize, 2), h.store.segment_rows);
    // Both parts landed at their own offsets inside one temp file.
    try testing.expectEqualStrings("first partsecond part!", h.fs.contents("/inc/2/2.tmp").?);
    try testing.expect(h.ftx.balanced());
}
