//! Vocabulary shared by every bounded context's event set.
//!
//! The Go original declared `event.Event` as an interface with
//! `Topic()`, `AggregateID()` and `OccurredAt()`, plus a `Bus` and a
//! `Handler`. Zig has no cross-package interface worth paying a vtable
//! for here: each context's events are a closed set, so they become a
//! tagged union in that context's own module with `topic()`,
//! `aggregateId()` and `occurredAt()` as switch-driven jump tables. No
//! dynamic dispatch, no boxing, and the compiler catches a forgotten
//! arm.
//!
//! What genuinely *is* shared, and therefore lives here:
//!
//!   * `Timestamp` — the one instant representation the whole domain
//!     agrees on.
//!   * `Uuid` — the envelope identifier the bus stamps on publish.
//!   * `Envelope` — the delivery record handlers see.
//!   * `Queue` — the pending-event buffer every aggregate root carries.
//!
//! The `Bus` and `Handler` ports do not: they are I/O, and they live
//! with their implementation in `store/outbox.zig`.

const std = @import("std");

const Allocator = std.mem.Allocator;

/// Unix milliseconds, UTC.
///
/// The domain never reads a clock — every mutator takes the instant as
/// a parameter. That is what makes the aggregates deterministic under
/// test, and it is also the correct shape: "when did this happen" is an
/// input to a state transition, not something the transition discovers.
///
/// Instants that may be absent (`started_at`, `finished_at`, ...) are
/// `?Timestamp`; `null` is Go's `time.Time{}.IsZero()`.
pub const Timestamp = i64;

/// Milliseconds in the units the domain talks about. Go used
/// `time.Duration` (nanoseconds); the persisted columns were always
/// unix-ms, so the domain speaks ms and drops the unit mismatch.
pub const Millis = i64;

pub const second_ms: Millis = 1000;
pub const minute_ms: Millis = 60 * second_ms;
pub const hour_ms: Millis = 60 * minute_ms;
pub const day_ms: Millis = 24 * hour_ms;

/// A UUIDv7: 48 bits of big-endian unix-ms followed by 74 bits of
/// randomness, with the version and variant nibbles pinned. Time-
/// ordered, so the outbox's primary key sorts in publish order and
/// B-tree inserts stay at the right edge of the index.
///
/// Both the timestamp and the random bytes are parameters — no clock
/// and no global CSPRNG reach into the domain. The bus supplies them.
pub const Uuid = struct {
    bytes: [16]u8,

    /// Length of the canonical 8-4-4-4-12 hex form.
    pub const text_len = 36;
    pub const TextBuf = [text_len]u8;

    pub const nil: Uuid = .{ .bytes = @splat(0) };

    /// `ms` is truncated to its low 48 bits, which is good until the
    /// year 10889. `rand` supplies the remaining entropy; only 74 of
    /// its 80 bits survive (the version and variant fields overwrite
    /// the rest).
    pub fn v7(ms: i64, rand: [10]u8) Uuid {
        var u: Uuid = .{ .bytes = undefined };
        const t: u64 = @bitCast(ms);
        u.bytes[0] = @truncate(t >> 40);
        u.bytes[1] = @truncate(t >> 32);
        u.bytes[2] = @truncate(t >> 24);
        u.bytes[3] = @truncate(t >> 16);
        u.bytes[4] = @truncate(t >> 8);
        u.bytes[5] = @truncate(t);
        @memcpy(u.bytes[6..16], &rand);
        // Version 7 in the high nibble of octet 6.
        u.bytes[6] = (u.bytes[6] & 0x0F) | 0x70;
        // RFC 4122 variant (0b10) in the two high bits of octet 8.
        u.bytes[8] = (u.bytes[8] & 0x3F) | 0x80;
        return u;
    }

    /// Renders the canonical lowercase hex form into caller storage.
    /// Returns a slice of `buf` so the result can be passed straight to
    /// a statement binding without an allocation.
    pub fn writeText(self: Uuid, buf: *TextBuf) []const u8 {
        const hex = "0123456789abcdef";
        var o: usize = 0;
        for (self.bytes, 0..) |b, i| {
            if (i == 4 or i == 6 or i == 8 or i == 10) {
                buf[o] = '-';
                o += 1;
            }
            buf[o] = hex[b >> 4];
            buf[o + 1] = hex[b & 0x0F];
            o += 2;
        }
        return buf[0..text_len];
    }

    pub const ParseError = error{InvalidUuid};

    pub fn parse(text: []const u8) ParseError!Uuid {
        if (text.len != text_len) return error.InvalidUuid;
        var u: Uuid = .{ .bytes = undefined };
        var o: usize = 0;
        var i: usize = 0;
        while (i < 16) : (i += 1) {
            if (i == 4 or i == 6 or i == 8 or i == 10) {
                if (text[o] != '-') return error.InvalidUuid;
                o += 1;
            }
            const hi = std.fmt.charToDigit(text[o], 16) catch return error.InvalidUuid;
            const lo = std.fmt.charToDigit(text[o + 1], 16) catch return error.InvalidUuid;
            u.bytes[i] = (@as(u8, hi) << 4) | lo;
            o += 2;
        }
        return u;
    }

    /// The embedded unix-ms. Only meaningful for a v7 uuid.
    pub fn timestampMs(self: Uuid) i64 {
        var t: u64 = 0;
        for (self.bytes[0..6]) |b| t = (t << 8) | b;
        return @intCast(t);
    }

    pub fn eql(a: Uuid, b: Uuid) bool {
        return std.mem.eql(u8, &a.bytes, &b.bytes);
    }
};

/// Widest decimal rendering of an i64, sign included.
pub const AggregateIdBuf = [20]u8;

/// Renders a numeric aggregate id as the decimal string the outbox
/// stores in its `aggregate_id` column.
///
/// The domain keeps aggregate ids numeric — Go's `AggregateID() string`
/// forced every event constructor through `strconv`, which is an
/// allocation per event on a hot path for no benefit. Text is produced
/// once, here, by whoever is about to write it out.
pub fn writeAggregateId(id: i64, buf: *AggregateIdBuf) []const u8 {
    return std.fmt.bufPrint(buf, "{d}", .{id}) catch unreachable;
}

/// One delivery attempt of one event, as a handler sees it.
///
/// # String ownership
///
/// Every slice here is **borrowed** from the bus, which owns the row it
/// read and keeps it alive for the duration of the handler call. A
/// handler that needs the payload past its return must copy it.
pub const Envelope = struct {
    /// Assigned by the bus at publish time; time-ordered.
    id: Uuid,
    /// Mirrors the event's own `topic()` so the bus can index and route
    /// without deserialising the payload.
    topic: []const u8,
    /// Decimal rendering of the producing aggregate's id.
    aggregate_id: []const u8,
    occurred_at: Timestamp,
    /// Serialised event body. Opaque to the bus.
    payload: []const u8,
    /// 1-based. First delivery is 1; incremented on retry, so a handler
    /// can tell "this is a redelivery, I may have half-applied it".
    attempts: u32 = 1,
};

/// The pending-event buffer an aggregate root carries between a state
/// transition and the commit that publishes it.
///
/// Go accumulated into a `[]event.Event` on the struct and drained it
/// with `PullEvents`. Same shape here, with the ownership made
/// explicit: the queue never owns the allocator, the aggregate does,
/// and it hands it in on every call.
///
/// `E` may declare `pub fn deinit(self: E, allocator: Allocator) void`
/// to release strings the event owns; `Queue` calls it for events still
/// buffered at `deinit` time. Events handed out by `pull` become the
/// caller's problem — see `deinitAll`.
pub fn Queue(comptime E: type) type {
    const has_deinit = @hasDecl(E, "deinit");
    return struct {
        const Self = @This();

        items: std.ArrayList(E) = .empty,

        pub const empty: Self = .{};

        /// Buffers one event. On failure the event is *not* recorded, so
        /// a caller that already mutated aggregate state must treat OOM
        /// as fatal to the transaction — which it is: the tx rolls back
        /// and the state change is discarded with the event.
        pub fn record(self: *Self, allocator: Allocator, e: E) Allocator.Error!void {
            try self.items.append(allocator, e);
        }

        /// Transfers the buffered events, and the slice holding them, to
        /// the caller. The queue is left empty.
        pub fn pull(self: *Self, allocator: Allocator) Allocator.Error![]E {
            return self.items.toOwnedSlice(allocator);
        }

        /// Read-only view for assertions and for a bus that publishes
        /// without taking ownership. Invalidated by `record`.
        pub fn view(self: *const Self) []const E {
            return self.items.items;
        }

        /// Mutable view. The one legitimate use is patching a queued
        /// event's placeholder id after the repository assigns the real
        /// one (`setId` on every aggregate here).
        pub fn slice(self: *Self) []E {
            return self.items.items;
        }

        pub fn len(self: *const Self) usize {
            return self.items.items.len;
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            if (has_deinit) {
                for (self.items.items) |e| e.deinit(allocator);
            }
            self.items.deinit(allocator);
        }
    };
}

/// Frees a batch taken from `Queue.pull`, including the slice itself.
pub fn deinitAll(comptime E: type, allocator: Allocator, batch: []E) void {
    if (@hasDecl(E, "deinit")) {
        for (batch) |e| e.deinit(allocator);
    }
    allocator.free(batch);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

test "uuid v7 pins version and variant and keeps the timestamp" {
    const t = std.testing;
    const ms: i64 = 0x0192_3456_789A;
    const u = Uuid.v7(ms, .{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

    try t.expectEqual(@as(u8, 0x7F), u.bytes[6]);
    try t.expectEqual(@as(u8, 0xBF), u.bytes[8]);
    try t.expectEqual(ms, u.timestampMs());
}

test "uuid v7 is time-ordered under byte comparison" {
    const t = std.testing;
    const zeros: [10]u8 = @splat(0);
    const a = Uuid.v7(1_700_000_000_000, zeros);
    const b = Uuid.v7(1_700_000_000_001, zeros);
    try t.expect(std.mem.lessThan(u8, &a.bytes, &b.bytes));
}

test "uuid text round-trips" {
    const t = std.testing;
    const u = Uuid.v7(1_700_000_000_000, .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
    var buf: Uuid.TextBuf = undefined;
    const text = u.writeText(&buf);

    try t.expectEqual(@as(usize, 36), text.len);
    try t.expectEqual(@as(u8, '-'), text[8]);
    try t.expectEqual(@as(u8, '-'), text[13]);
    try t.expectEqual(@as(u8, '-'), text[18]);
    try t.expectEqual(@as(u8, '-'), text[23]);
    try t.expect((try Uuid.parse(text)).eql(u));
}

test "uuid parse rejects malformed input" {
    const t = std.testing;
    try t.expectError(error.InvalidUuid, Uuid.parse("too-short"));
    // Right length, dash in the wrong place.
    try t.expectError(error.InvalidUuid, Uuid.parse("0192345-6789a7fff-bfffffffffffffff"));
    // Right length and layout, non-hex digit.
    try t.expectError(error.InvalidUuid, Uuid.parse("g1923456-789a-7fff-bfff-ffffffffffff"));
}

test "aggregate id renders as decimal" {
    const t = std.testing;
    var buf: AggregateIdBuf = undefined;
    try t.expectEqualStrings("0", writeAggregateId(0, &buf));
    try t.expectEqualStrings("42", writeAggregateId(42, &buf));
    try t.expectEqualStrings("-9223372036854775808", writeAggregateId(std.math.minInt(i64), &buf));
}

/// Stand-in event with an owned string, to prove `Queue` releases what
/// it holds and stops caring once the batch is pulled.
const TestEvent = struct {
    id: i64,
    owned: []const u8,

    pub fn deinit(self: TestEvent, allocator: Allocator) void {
        allocator.free(self.owned);
    }
};

test "queue buffers, pulls and transfers ownership" {
    const t = std.testing;
    const alloc = t.allocator;

    var q: Queue(TestEvent) = .empty;
    defer q.deinit(alloc);

    try t.expectEqual(@as(usize, 0), q.len());
    try q.record(alloc, .{ .id = 1, .owned = try alloc.dupe(u8, "one") });
    try q.record(alloc, .{ .id = 2, .owned = try alloc.dupe(u8, "two") });
    try t.expectEqual(@as(usize, 2), q.len());

    // Patch a placeholder id in place — the `setId` pattern.
    for (q.slice()) |*e| {
        if (e.id == 1) e.id = 99;
    }
    try t.expectEqual(@as(i64, 99), q.view()[0].id);

    const batch = try q.pull(alloc);
    // Pull drains: a second pull yields nothing, and the queue no
    // longer owns the strings.
    try t.expectEqual(@as(usize, 0), q.len());
    try t.expectEqual(@as(usize, 2), batch.len);
    deinitAll(TestEvent, alloc, batch);

    const empty_batch = try q.pull(alloc);
    try t.expectEqual(@as(usize, 0), empty_batch.len);
    deinitAll(TestEvent, alloc, empty_batch);
}

test "queue deinit releases undrained events" {
    const t = std.testing;
    const alloc = t.allocator;

    var q: Queue(TestEvent) = .empty;
    try q.record(alloc, .{ .id = 1, .owned = try alloc.dupe(u8, "leak me") });
    // No pull: deinit must free both the event's string and the list.
    q.deinit(alloc);
}

test "queue works for events with no owned strings" {
    const t = std.testing;
    const alloc = t.allocator;
    const Plain = struct { at: Timestamp };

    var q: Queue(Plain) = .empty;
    defer q.deinit(alloc);
    try q.record(alloc, .{ .at = 7 });
    try t.expectEqual(@as(Timestamp, 7), q.view()[0].at);
}

test "duration constants line up" {
    const t = std.testing;
    try t.expectEqual(@as(Millis, 604_800_000), 7 * day_ms);
    try t.expectEqual(@as(Millis, 3_600_000), hour_ms);
}
