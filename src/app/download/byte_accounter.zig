//! Per-server byte accounting: stage deltas in memory, persist them on a
//! tick.
//!
//! The fetcher reports bytes on every completed article, which for a busy
//! job is hundreds per second across N servers. Persisting each delta
//! would be correct and wasteful: the operator-visible granularity of
//! "how much of my block account is gone" is megabytes and minutes, not
//! bytes and milliseconds.
//!
//! On a clean shutdown `flush` runs once more, so nothing staged is lost.
//! On a hard crash a few seconds' worth of consumption is forgotten,
//! which is the right trade for a counter whose only consumer is a quota
//! display.
//!
//! Go guarded the map with a mutex and ran the flush on its own
//! goroutine with a ticker. Single-threaded reactor, so: no mutex, and
//! `dueAt` reports when the next flush is owed instead of a goroutine
//! sleeping on a ticker.

const std = @import("std");
const log = @import("../../core/log.zig");
const app_ports = @import("../ports.zig");
const dserver = @import("../../domain/server.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Millis = app_ports.Millis;
pub const ServerId = dserver.ServerId;

/// Default flush cadence, matching Go.
pub const default_interval_ms: Millis = 10 * std.time.ms_per_s;

pub const StoreError = error{Backend} || Allocator.Error;

/// The slice of the server repository the accounter needs. Narrow so
/// nothing here depends on the shape of the rest of the server context.
pub const ServerByteStore = struct {
    ctx: *anyopaque,
    incrementFn: *const fn (
        ctx: *anyopaque,
        unit: ?*app_ports.Unit,
        id: ServerId,
        n: i64,
    ) StoreError!void,

    pub fn increment(
        self: ServerByteStore,
        unit: ?*app_ports.Unit,
        id: ServerId,
        n: i64,
    ) StoreError!void {
        return self.incrementFn(self.ctx, unit, id, n);
    }
};

/// One staged (server, bytes) pair.
pub const Delta = struct { id: ServerId, bytes: i64 };

/// Staged per-server byte totals.
pub const Accounter = struct {
    gpa: Allocator,
    deltas: std.ArrayList(Delta) = .empty,
    /// Fires on every `add` with the byte count only. The throughput
    /// tracker subscribes here, which is how bytes/sec reaches the
    /// System page without the fetcher knowing the system context
    /// exists.
    observer: ?Observer = null,

    pub const Observer = struct {
        ctx: *anyopaque,
        onBytesFn: *const fn (ctx: *anyopaque, n: i64) void,

        pub fn onBytes(self: Observer, n: i64) void {
            self.onBytesFn(self.ctx, n);
        }
    };

    pub fn init(gpa: Allocator) Accounter {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Accounter) void {
        self.deltas.deinit(self.gpa);
        self.* = undefined;
    }

    /// Stages `n` bytes against `id`. Non-positive counts are ignored, so
    /// a fetch that produced nothing cannot make a quota look better.
    pub fn add(self: *Accounter, id: ServerId, n: i64) Allocator.Error!void {
        if (n <= 0) return;
        for (self.deltas.items) |*d| {
            if (d.id == id) {
                d.bytes += n;
                if (self.observer) |o| o.onBytes(n);
                return;
            }
        }
        try self.deltas.append(self.gpa, .{ .id = id, .bytes = n });
        if (self.observer) |o| o.onBytes(n);
    }

    pub fn staged(self: *const Accounter, id: ServerId) i64 {
        for (self.deltas.items) |d| {
            if (d.id == id) return d.bytes;
        }
        return 0;
    }

    pub fn isEmpty(self: *const Accounter) bool {
        return self.deltas.items.len == 0;
    }

    /// Hands the staged deltas to the caller and resets the staging area.
    /// The slice belongs to `a`.
    pub fn drain(self: *Accounter, a: Allocator) Allocator.Error![]Delta {
        const out = try a.dupe(Delta, self.deltas.items);
        self.deltas.clearRetainingCapacity();
        return out;
    }
};

/// Moves staged deltas into the persistent `used_bytes` counters.
pub const Flusher = struct {
    gpa: Allocator,
    accounter: *Accounter,
    store: ServerByteStore,
    interval_ms: Millis = default_interval_ms,
    logger: *log.Logger = &log.default,
    /// When the last flush happened.
    last_flush_at: Timestamp = 0,
    flushes: usize = 0,

    /// When the next flush is owed. The reactor arms a timer for this;
    /// there is no goroutine and no ticker.
    pub fn dueAt(self: *const Flusher) Timestamp {
        return self.last_flush_at + self.interval_ms;
    }

    pub fn isDue(self: *const Flusher, now: Timestamp) bool {
        return now >= self.dueAt();
    }

    /// Persists everything staged. A per-server failure is logged and
    /// that server's delta is dropped rather than retried: the counter is
    /// advisory, and holding a growing delta across an outage that lasts
    /// hours would eventually report a nonsense jump.
    pub fn flush(self: *Flusher, unit: ?*app_ports.Unit, now: Timestamp) Allocator.Error!void {
        self.last_flush_at = now;
        if (self.accounter.isEmpty()) return;
        const deltas = try self.accounter.drain(self.gpa);
        defer self.gpa.free(deltas);
        self.flushes += 1;
        for (deltas) |d| {
            self.store.increment(unit, d.id, d.bytes) catch |e| {
                if (e == error.OutOfMemory) return error.OutOfMemory;
                self.logger.warn("byte flusher: increment failed; delta dropped", &.{
                    log.int("server_id", d.id),
                    log.int("delta", d.bytes),
                    log.errv("err", e),
                });
            };
        }
    }

    pub fn flushIfDue(self: *Flusher, unit: ?*app_ports.Unit, now: Timestamp) Allocator.Error!void {
        if (self.isDue(now)) try self.flush(unit, now);
    }
};

// =====================================================================
// Test double
// =====================================================================

/// Records what reached the persistent counters.
pub const FakeByteStore = struct {
    totals: std.ArrayList(Delta) = .empty,
    gpa: Allocator,
    calls: usize = 0,
    /// Set to fail the next increment.
    fail_next: ?StoreError = null,

    pub fn init(gpa: Allocator) FakeByteStore {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeByteStore) void {
        self.totals.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn store(self: *FakeByteStore) ServerByteStore {
        return .{ .ctx = @ptrCast(self), .incrementFn = &increment };
    }

    pub fn total(self: *const FakeByteStore, id: ServerId) i64 {
        for (self.totals.items) |d| {
            if (d.id == id) return d.bytes;
        }
        return 0;
    }

    fn increment(ctx: *anyopaque, _: ?*app_ports.Unit, id: ServerId, n: i64) StoreError!void {
        const self: *FakeByteStore = @ptrCast(@alignCast(ctx));
        if (self.fail_next) |e| {
            self.fail_next = null;
            return e;
        }
        self.calls += 1;
        for (self.totals.items) |*d| {
            if (d.id == id) {
                d.bytes += n;
                return;
            }
        }
        try self.totals.append(self.gpa, .{ .id = id, .bytes = n });
    }
};

/// Counts what an observer was told, standing in for the throughput
/// tracker.
pub const FakeObserver = struct {
    total: i64 = 0,
    calls: usize = 0,

    pub fn observer(self: *FakeObserver) Accounter.Observer {
        return .{ .ctx = @ptrCast(self), .onBytesFn = &onBytes };
    }

    fn onBytes(ctx: *anyopaque, n: i64) void {
        const self: *FakeObserver = @ptrCast(@alignCast(ctx));
        self.total += n;
        self.calls += 1;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "adds aggregate per server and drain resets the staging area" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();

    try acc.add(1, 100);
    try acc.add(2, 50);
    try acc.add(1, 25);
    try testing.expectEqual(@as(i64, 125), acc.staged(1));
    try testing.expectEqual(@as(i64, 50), acc.staged(2));
    try testing.expectEqual(@as(i64, 0), acc.staged(3));

    const drained = try acc.drain(testing.allocator);
    defer testing.allocator.free(drained);
    try testing.expectEqual(@as(usize, 2), drained.len);
    try testing.expect(acc.isEmpty());
    try testing.expectEqual(@as(i64, 0), acc.staged(1));
}

test "non-positive byte counts are ignored entirely" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();
    var obs: FakeObserver = .{};
    acc.observer = obs.observer();

    try acc.add(1, 0);
    try acc.add(1, -100);
    try testing.expect(acc.isEmpty());
    // Not even the observer hears about them, so a bogus read cannot
    // show up as a spike on the throughput graph.
    try testing.expectEqual(@as(usize, 0), obs.calls);
}

test "the observer sees every add, including into an existing bucket" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();
    var obs: FakeObserver = .{};
    acc.observer = obs.observer();

    try acc.add(1, 10);
    try acc.add(1, 20);
    try acc.add(2, 30);
    try testing.expectEqual(@as(usize, 3), obs.calls);
    try testing.expectEqual(@as(i64, 60), obs.total);
}

test "the flusher moves staged deltas into the persistent counters" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();
    var fake = FakeByteStore.init(testing.allocator);
    defer fake.deinit();
    var f: Flusher = .{ .gpa = testing.allocator, .accounter = &acc, .store = fake.store() };

    try acc.add(1, 1000);
    try acc.add(2, 2000);
    try f.flush(null, 0);

    try testing.expectEqual(@as(i64, 1000), fake.total(1));
    try testing.expectEqual(@as(i64, 2000), fake.total(2));
    try testing.expect(acc.isEmpty());
    // A second flush with nothing staged writes nothing.
    try f.flush(null, 0);
    try testing.expectEqual(@as(usize, 2), fake.calls);
    try testing.expectEqual(@as(usize, 1), f.flushes);
}

test "the flush cadence is a deadline, not a ticker" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();
    var fake = FakeByteStore.init(testing.allocator);
    defer fake.deinit();
    var f: Flusher = .{
        .gpa = testing.allocator,
        .accounter = &acc,
        .store = fake.store(),
        .interval_ms = 10_000,
        .last_flush_at = 1_000,
    };

    try testing.expectEqual(@as(Timestamp, 11_000), f.dueAt());
    try testing.expect(!f.isDue(10_999));
    try testing.expect(f.isDue(11_000));

    try acc.add(1, 5);
    try f.flushIfDue(null, 10_999);
    try testing.expectEqual(@as(i64, 0), fake.total(1));
    try f.flushIfDue(null, 11_000);
    try testing.expectEqual(@as(i64, 5), fake.total(1));
    // The deadline moved with the flush.
    try testing.expectEqual(@as(Timestamp, 21_000), f.dueAt());
}

test "a backend failure drops that server's delta and keeps the rest" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();
    var fake = FakeByteStore.init(testing.allocator);
    defer fake.deinit();
    var logger: log.Logger = .{};
    var f: Flusher = .{
        .gpa = testing.allocator,
        .accounter = &acc,
        .store = fake.store(),
        .logger = &logger,
    };

    try acc.add(1, 100);
    try acc.add(2, 200);
    fake.fail_next = error.Backend;
    try f.flush(null, 0);

    // Server 1's delta was dropped; server 2's still landed. Holding the
    // failed delta would eventually report a nonsense jump.
    try testing.expectEqual(@as(i64, 0), fake.total(1));
    try testing.expectEqual(@as(i64, 200), fake.total(2));
    try testing.expect(acc.isEmpty());
}

test "an empty flush still advances the deadline" {
    var acc = Accounter.init(testing.allocator);
    defer acc.deinit();
    var fake = FakeByteStore.init(testing.allocator);
    defer fake.deinit();
    var f: Flusher = .{ .gpa = testing.allocator, .accounter = &acc, .store = fake.store() };
    try f.flush(null, 5_000);
    try testing.expectEqual(@as(Timestamp, 5_000 + default_interval_ms), f.dueAt());
}
