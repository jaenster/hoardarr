//! The `notify` bounded context: outbound notifications — generic
//! webhooks, Discord, Slack.
//!
//! Aggregate root: `Subscription`, one consumer's interest in a set of
//! event topics. The notify service tails the bus, fans each envelope out
//! to the subscriptions whose topic patterns match, and a per-`kind`
//! sender does the delivery. The outbox provides at-least-once across
//! restarts; the per-delivery retry guards against a subscriber that is
//! merely having a bad minute.
//!
//! # Topic matching
//!
//! A topic pattern is either an exact topic or a prefix ending in `*`:
//! `deliver.*` matches every topic starting with `deliver.`. There is no
//! mid-pattern wildcard, deliberately — the event names are hierarchical
//! and a prefix is the only cut anyone has needed.
//!
//! Patterns are normalised on the way in: trimmed, empties dropped,
//! deduplicated, sorted. Sorting means two subscriptions to the same set
//! compare equal byte for byte, which is what lets the settings UI show
//! "no changes" honestly.
//!
//! # Ownership
//!
//! The aggregate owns `name`, `url`, `secret`, `last_error` and every
//! string in `topics`, so it gets `init(allocator, ...)` / `deinit`.
//!
//! `SubscriptionAdded` owns its `url` and `topics` copies, because
//! `update` replaces the aggregate's, which would dangle an
//! already-pulled event. Its `name`, and `SubscriptionRemoved.name`, are
//! **borrowed**: the name is the identity of the subscription and there
//! is no rename path. Call `Event.deinit`, or `event.deinitAll` on a
//! batch, before the aggregate goes away.
//!
//! # Deviation from Go
//!
//! `Update` in Go emitted `SubscriptionUpdated` and bumped `updated_at`
//! on every call, even one that changed nothing — so a settings page
//! that PUT the unchanged form woke every listener. Here the event is
//! gated on an actual change, matching `server.UsenetServer.update`.
//! Validation also runs over the whole parameter set before the first
//! field moves, so a rejected update is a no-op rather than a partial
//! one.

const std = @import("std");
const event = @import("event.zig");

const Allocator = std.mem.Allocator;
const Timestamp = event.Timestamp;

/// Identifies a `Subscription`. Allocated by the repository; 0 means "not
/// yet persisted".
pub const SubscriptionId = i64;

/// Topic prefix for every event in this context.
pub const topic_prefix = "notify.";

/// Longest subscription name we store.
pub const max_name_len = 128;

/// Which delivery adapter handles the subscription. Adding a provider is
/// a new sender plus a variant here — no schema change, since the column
/// stores the tag name.
pub const Kind = enum {
    webhook,
    discord,
    slack,

    pub fn toString(self: Kind) []const u8 {
        return @tagName(self);
    }

    pub fn parse(s: []const u8) ?Kind {
        return std.meta.stringToEnum(Kind, s);
    }
};

pub const SubscriptionAdded = struct {
    id: SubscriptionId,
    /// Borrowed from the aggregate.
    name: []const u8,
    kind: Kind,
    /// Owned by the event.
    url: []const u8,
    /// Owned by the event, strings included.
    topics: []const []const u8,
    at: Timestamp,
};

/// Fires after one or more editable fields actually changed. The payload
/// is lean on purpose: a listener that cares which field re-reads the
/// row.
pub const SubscriptionUpdated = struct {
    id: SubscriptionId,
    at: Timestamp,
};

pub const SubscriptionEnabled = struct {
    id: SubscriptionId,
    at: Timestamp,
};

pub const SubscriptionDisabled = struct {
    id: SubscriptionId,
    at: Timestamp,
};

pub const SubscriptionRemoved = struct {
    id: SubscriptionId,
    /// Borrowed from the aggregate — valid only until `deinit`.
    name: []const u8,
    at: Timestamp,
};

pub const EventKind = enum {
    added,
    updated,
    enabled,
    disabled,
    removed,
};

pub const Event = union(EventKind) {
    added: SubscriptionAdded,
    updated: SubscriptionUpdated,
    enabled: SubscriptionEnabled,
    disabled: SubscriptionDisabled,
    removed: SubscriptionRemoved,

    pub fn topic(self: Event) []const u8 {
        return switch (self) {
            .added => topic_prefix ++ "subscription.added",
            .updated => topic_prefix ++ "subscription.updated",
            .enabled => topic_prefix ++ "subscription.enabled",
            .disabled => topic_prefix ++ "subscription.disabled",
            .removed => topic_prefix ++ "subscription.removed",
        };
    }

    pub fn aggregateId(self: Event) SubscriptionId {
        return switch (self) {
            inline else => |e| e.id,
        };
    }

    pub fn occurredAt(self: Event) Timestamp {
        return switch (self) {
            inline else => |e| e.at,
        };
    }

    pub fn deinit(self: Event, allocator: Allocator) void {
        switch (self) {
            .added => |e| {
                allocator.free(e.url);
                freeStrings(allocator, e.topics);
            },
            else => {},
        }
    }
};

pub const ValidationError = error{
    NameRequired,
    NameTooLong,
    UrlRequired,
    /// `std.Uri` could not parse it at all.
    UrlInvalid,
    /// Only http and https reach a sender. A `file:` or `gopher:` target
    /// would be an SSRF surface with no upside.
    UrlSchemeUnsupported,
    UrlHostRequired,
    /// An empty topic list would make the subscription a no-op row that
    /// the settings UI shows as active.
    TopicsRequired,
};

pub const InitError = ValidationError || Allocator.Error;

pub const NewParams = struct {
    name: []const u8,
    kind: Kind = .webhook,
    url: []const u8,
    /// At least one non-empty entry. Normalised on the way in.
    topics: []const []const u8,
    /// Optional HMAC key. Empty means "sign nothing".
    secret: []const u8 = "",
};

/// The snapshot the repository hands back. Trusted.
pub const HydrateParams = struct {
    id: SubscriptionId,
    name: []const u8,
    kind: Kind,
    url: []const u8,
    topics: []const []const u8 = &.{},
    secret: []const u8 = "",
    enabled: bool,
    last_success_at: ?Timestamp = null,
    last_error_at: ?Timestamp = null,
    last_error: []const u8 = "",
    created_at: Timestamp,
    updated_at: Timestamp,
};

/// Which fields an `update` should touch. `null` means "leave alone".
/// `name` is absent on purpose: it is the subscription's identity, and an
/// edit goes through delete + recreate.
pub const UpdateParams = struct {
    url: ?[]const u8 = null,
    topics: ?[]const []const u8 = null,
    /// An empty string clears the secret.
    secret: ?[]const u8 = null,
    enabled: ?bool = null,
};

pub const Subscription = struct {
    allocator: Allocator,

    id: SubscriptionId = 0,
    /// Owned. Immutable once constructed.
    name: []const u8,
    kind: Kind,
    /// Owned.
    url: []const u8,
    /// Owned, strings included. Normalised: trimmed, deduplicated,
    /// sorted, never empty.
    topics: []const []const u8,
    /// Owned. HMAC key; empty means unsigned.
    secret: []const u8,
    enabled: bool = true,

    /// Operational telemetry for the settings UI. Moved by the `mark*`
    /// methods so it stays inside the aggregate boundary, and
    /// deliberately event-free — "the webhook worked again" is not
    /// something another context reacts to.
    last_success_at: ?Timestamp = null,
    last_error_at: ?Timestamp = null,
    /// Owned.
    last_error: []const u8,

    created_at: Timestamp,
    updated_at: Timestamp,

    events: event.Queue(Event) = .empty,

    /// Validates, normalises, copies, and records `SubscriptionAdded`
    /// with a placeholder id — `setId` patches it after the insert.
    pub fn init(allocator: Allocator, p: NewParams, now: Timestamp) InitError!Subscription {
        const name = trim(p.name);
        if (name.len == 0) return error.NameRequired;
        if (name.len > max_name_len) return error.NameTooLong;
        try validateUrl(p.url);

        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);
        const url_copy = try allocator.dupe(u8, p.url);
        errdefer allocator.free(url_copy);
        const secret_copy = try allocator.dupe(u8, p.secret);
        errdefer allocator.free(secret_copy);
        const err_copy = try allocator.dupe(u8, "");
        errdefer allocator.free(err_copy);

        const topics = try normaliseTopics(allocator, p.topics);
        errdefer freeStrings(allocator, topics);

        // The event's own copies.
        const event_url = try allocator.dupe(u8, p.url);
        errdefer allocator.free(event_url);
        const event_topics = try dupeStrings(allocator, topics);
        errdefer freeStrings(allocator, event_topics);

        var s: Subscription = .{
            .allocator = allocator,
            .name = name_copy,
            .kind = p.kind,
            .url = url_copy,
            .topics = topics,
            .secret = secret_copy,
            .last_error = err_copy,
            .created_at = now,
            .updated_at = now,
        };
        try s.events.record(allocator, .{ .added = .{
            .id = 0,
            .name = s.name,
            .kind = p.kind,
            .url = event_url,
            .topics = event_topics,
            .at = now,
        } });
        return s;
    }

    pub fn hydrate(allocator: Allocator, p: HydrateParams) Allocator.Error!Subscription {
        const name = try allocator.dupe(u8, p.name);
        errdefer allocator.free(name);
        const url = try allocator.dupe(u8, p.url);
        errdefer allocator.free(url);
        const secret = try allocator.dupe(u8, p.secret);
        errdefer allocator.free(secret);
        const last_error = try allocator.dupe(u8, p.last_error);
        errdefer allocator.free(last_error);
        const topics = try dupeStrings(allocator, p.topics);
        return .{
            .allocator = allocator,
            .id = p.id,
            .name = name,
            .kind = p.kind,
            .url = url,
            .topics = topics,
            .secret = secret,
            .enabled = p.enabled,
            .last_success_at = p.last_success_at,
            .last_error_at = p.last_error_at,
            .last_error = last_error,
            .created_at = p.created_at,
            .updated_at = p.updated_at,
        };
    }

    pub fn deinit(self: *Subscription) void {
        self.events.deinit(self.allocator);
        self.allocator.free(self.name);
        self.allocator.free(self.url);
        self.allocator.free(self.secret);
        self.allocator.free(self.last_error);
        freeStrings(self.allocator, self.topics);
        self.* = undefined;
    }

    pub fn setId(self: *Subscription, id: SubscriptionId) void {
        self.id = id;
        for (self.events.slice()) |*e| {
            switch (e.*) {
                inline else => |*payload| if (payload.id == 0) {
                    payload.id = id;
                },
            }
        }
    }

    /// Whether this subscription wants the given event topic. Exact match
    /// or trailing-`*` prefix match.
    pub fn matchesTopic(self: *const Subscription, ev_topic: []const u8) bool {
        for (self.topics) |pattern| {
            if (std.mem.eql(u8, pattern, ev_topic)) return true;
            if (pattern.len > 0 and pattern[pattern.len - 1] == '*' and
                std.mem.startsWith(u8, ev_topic, pattern[0 .. pattern.len - 1])) return true;
        }
        return false;
    }

    /// Whether the service should deliver to this subscription at all.
    pub fn wants(self: *const Subscription, ev_topic: []const u8) bool {
        return self.enabled and self.matchesTopic(ev_topic);
    }

    /// Applies the supplied fields. Validation completes before the first
    /// mutation; a rejected update changes nothing.
    ///
    /// An `enabled` flip records `SubscriptionEnabled`/`Disabled` *and*
    /// `SubscriptionUpdated`, as Go did — a listener may subscribe to
    /// either the specific or the generic topic, and dropping one would
    /// silently break whichever it chose.
    pub fn update(self: *Subscription, p: UpdateParams, now: Timestamp) InitError!void {
        // --- validate ---------------------------------------------------
        if (p.url) |raw| try validateUrl(raw);
        if (p.topics) |list| {
            if (countNonEmpty(list) == 0) return error.TopicsRequired;
        }

        // --- allocate ---------------------------------------------------
        var url_copy: ?[]u8 = null;
        var secret_copy: ?[]u8 = null;
        var topics_copy: ?[]const []const u8 = null;
        errdefer {
            if (url_copy) |c| self.allocator.free(c);
            if (secret_copy) |c| self.allocator.free(c);
            if (topics_copy) |c| freeStrings(self.allocator, c);
        }
        if (p.url) |raw| {
            if (!std.mem.eql(u8, raw, self.url)) url_copy = try self.allocator.dupe(u8, raw);
        }
        if (p.secret) |raw| {
            if (!std.mem.eql(u8, raw, self.secret)) secret_copy = try self.allocator.dupe(u8, raw);
        }
        if (p.topics) |list| {
            const normalised = try normaliseTopics(self.allocator, list);
            if (stringsEql(normalised, self.topics)) {
                freeStrings(self.allocator, normalised);
            } else {
                topics_copy = normalised;
            }
        }
        // Room for the enable/disable event plus the update event.
        try self.events.items.ensureUnusedCapacity(self.allocator, 2);

        // --- apply ------------------------------------------------------
        var changed = false;
        if (url_copy) |c| {
            self.allocator.free(self.url);
            self.url = c;
            changed = true;
        }
        if (secret_copy) |c| {
            self.allocator.free(self.secret);
            self.secret = c;
            changed = true;
        }
        if (topics_copy) |c| {
            freeStrings(self.allocator, self.topics);
            self.topics = c;
            changed = true;
        }
        if (p.enabled) |v| {
            if (v != self.enabled) {
                self.enabled = v;
                changed = true;
                self.events.items.appendAssumeCapacity(if (v)
                    .{ .enabled = .{ .id = self.id, .at = now } }
                else
                    .{ .disabled = .{ .id = self.id, .at = now } });
            }
        }

        if (changed) {
            self.updated_at = now;
            self.events.items.appendAssumeCapacity(.{ .updated = .{ .id = self.id, .at = now } });
        }
    }

    /// Flips the active flag on its own. A no-op when unchanged, so a
    /// repeated toggle doesn't spam subscribers.
    pub fn setEnabled(self: *Subscription, enabled: bool, now: Timestamp) Allocator.Error!void {
        if (self.enabled == enabled) return;
        self.enabled = enabled;
        self.updated_at = now;
        try self.events.record(self.allocator, if (enabled)
            .{ .enabled = .{ .id = self.id, .at = now } }
        else
            .{ .disabled = .{ .id = self.id, .at = now } });
    }

    /// Bookkeeping after a delivery landed. Clears the stale error so the
    /// settings UI doesn't show a failure next to a working webhook.
    pub fn markDeliverySuccess(self: *Subscription, now: Timestamp) Allocator.Error!void {
        const cleared = try self.allocator.dupe(u8, "");
        self.allocator.free(self.last_error);
        self.last_error = cleared;
        self.last_success_at = now;
        self.updated_at = now;
    }

    /// Bookkeeping after a delivery failed. `last_success_at` is left
    /// alone: "worked at 09:00, failing since 11:00" is exactly what the
    /// operator needs to see.
    pub fn markDeliveryFailure(
        self: *Subscription,
        reason: []const u8,
        now: Timestamp,
    ) Allocator.Error!void {
        const copy = try self.allocator.dupe(u8, reason);
        self.allocator.free(self.last_error);
        self.last_error = copy;
        self.last_error_at = now;
        self.updated_at = now;
    }

    /// Records `SubscriptionRemoved` immediately before the row is
    /// deleted, so the event commits in the same transaction as the
    /// DELETE.
    pub fn markRemoved(self: *Subscription, now: Timestamp) Allocator.Error!void {
        try self.events.record(self.allocator, .{
            .removed = .{ .id = self.id, .name = self.name, .at = now },
        });
    }

    pub fn pullEvents(self: *Subscription) Allocator.Error![]Event {
        return self.events.pull(self.allocator);
    }

    pub fn pendingEvents(self: *const Subscription) []const Event {
        return self.events.view();
    }
};

/// Errors a `Subscription` repository raises that callers branch on.
pub const RepositoryError = error{
    SubscriptionNotFound,
    /// `name` is unique; a second insert under the same name lands here.
    DuplicateName,
};

// ---------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------

fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, &std.ascii.whitespace);
}

/// http/https with a non-empty host. Anything `std.Uri` cannot parse is
/// rejected outright rather than passed to a sender to discover.
fn validateUrl(raw: []const u8) ValidationError!void {
    if (trim(raw).len == 0) return error.UrlRequired;
    const uri = std.Uri.parse(raw) catch return error.UrlInvalid;
    if (!std.mem.eql(u8, uri.scheme, "http") and !std.mem.eql(u8, uri.scheme, "https")) {
        return error.UrlSchemeUnsupported;
    }
    const host = uri.host orelse return error.UrlHostRequired;
    if (host.isEmpty()) return error.UrlHostRequired;
}

fn countNonEmpty(in: []const []const u8) usize {
    var n: usize = 0;
    for (in) |s| {
        if (trim(s).len != 0) n += 1;
    }
    return n;
}

/// Trims, drops empties, deduplicates and sorts. Rejects a list with
/// nothing usable in it.
///
/// Deduplication is a linear scan rather than a hash set: topic lists are
/// a handful of entries, and a `StringHashMap` here would allocate a
/// table and hash every string to save comparisons that never happen.
fn normaliseTopics(
    allocator: Allocator,
    in: []const []const u8,
) InitError![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (out.items) |s| allocator.free(s);
        out.deinit(allocator);
    }
    try out.ensureTotalCapacity(allocator, in.len);

    outer: for (in) |raw| {
        const v = trim(raw);
        if (v.len == 0) continue;
        for (out.items) |seen| {
            if (std.mem.eql(u8, seen, v)) continue :outer;
        }
        out.appendAssumeCapacity(try allocator.dupe(u8, v));
    }
    if (out.items.len == 0) return error.TopicsRequired;

    std.mem.sort([]const u8, out.items, {}, lessThanString);
    return out.toOwnedSlice(allocator);
}

fn lessThanString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn stringsEql(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!std.mem.eql(u8, x, y)) return false;
    }
    return true;
}

fn dupeStrings(allocator: Allocator, in: []const []const u8) Allocator.Error![]const []const u8 {
    const out = try allocator.alloc([]const u8, in.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |s| allocator.free(s);
        allocator.free(out);
    }
    for (in, 0..) |s, i| {
        out[i] = try allocator.dupe(u8, s);
        filled = i + 1;
    }
    return out;
}

fn freeStrings(allocator: Allocator, list: []const []const u8) void {
    for (list) |s| allocator.free(s);
    allocator.free(list);
}

// ---------------------------------------------------------------------
// Tests
//
// The Go package had no unit tests of its own; these are written from
// the implementation and from how `app/notify` drives it.
// ---------------------------------------------------------------------

const t = std.testing;

/// Pulls and releases the buffered events. `SubscriptionAdded` owns its
/// url and topic copies, so freeing only the slice would leak them.
fn drain(s: *Subscription) !void {
    const batch = try s.pullEvents();
    event.deinitAll(Event, t.allocator, batch);
}

fn newSub(topics: []const []const u8) !Subscription {
    return Subscription.init(t.allocator, .{
        .name = "ops",
        .url = "https://hooks.example.com/abc",
        .topics = topics,
    }, 1_000);
}

test "init normalises topics and records SubscriptionAdded" {
    var s = try newSub(&.{ "deliver.complete", "  download.job.failed  ", "deliver.complete", "" });
    defer s.deinit();

    try t.expectEqualStrings("ops", s.name);
    try t.expectEqual(Kind.webhook, s.kind);
    try t.expect(s.enabled);
    try t.expectEqual(@as(?Timestamp, null), s.last_success_at);

    // Deduplicated, trimmed, empties dropped, sorted.
    try t.expectEqual(@as(usize, 2), s.topics.len);
    try t.expectEqualStrings("deliver.complete", s.topics[0]);
    try t.expectEqualStrings("download.job.failed", s.topics[1]);

    s.setId(4);
    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("notify.subscription.added", batch[0].topic());
    try t.expectEqual(@as(SubscriptionId, 4), batch[0].aggregateId());
    try t.expectEqualStrings("ops", batch[0].added.name);
    try t.expectEqualStrings("https://hooks.example.com/abc", batch[0].added.url);
    try t.expectEqual(@as(usize, 2), batch[0].added.topics.len);
}

test "init rejects a bad name" {
    try t.expectError(error.NameRequired, Subscription.init(t.allocator, .{
        .name = "   ",
        .url = "https://h/x",
        .topics = &.{"a"},
    }, 1));
    try t.expectError(error.NameTooLong, Subscription.init(t.allocator, .{
        .name = "n" ** (max_name_len + 1),
        .url = "https://h/x",
        .topics = &.{"a"},
    }, 1));

    var ok = try Subscription.init(t.allocator, .{
        .name = "n" ** max_name_len,
        .url = "https://h/x",
        .topics = &.{"a"},
    }, 1);
    defer ok.deinit();
    try t.expectEqual(@as(usize, max_name_len), ok.name.len);
}

test "init rejects urls a sender could not use" {
    const cases = [_]struct { url: []const u8, want: anyerror }{
        .{ .url = "", .want = error.UrlRequired },
        .{ .url = "   ", .want = error.UrlRequired },
        .{ .url = "not a url", .want = error.UrlInvalid },
        .{ .url = "ftp://example.com/x", .want = error.UrlSchemeUnsupported },
        .{ .url = "file:///etc/passwd", .want = error.UrlSchemeUnsupported },
        .{ .url = "http:///no-host", .want = error.UrlHostRequired },
    };
    for (cases) |c| {
        try t.expectError(c.want, Subscription.init(t.allocator, .{
            .name = "n",
            .url = c.url,
            .topics = &.{"a"},
        }, 1));
    }

    // Both accepted schemes, with a port and a path.
    for ([_][]const u8{ "http://localhost:8080/hook", "https://example.com/a/b?c=d" }) |url| {
        var s = try Subscription.init(t.allocator, .{ .name = "n", .url = url, .topics = &.{"a"} }, 1);
        defer s.deinit();
        try t.expectEqualStrings(url, s.url);
    }
}

test "init rejects an empty topic set" {
    try t.expectError(error.TopicsRequired, Subscription.init(t.allocator, .{
        .name = "n",
        .url = "https://h/x",
        .topics = &.{},
    }, 1));
    try t.expectError(error.TopicsRequired, Subscription.init(t.allocator, .{
        .name = "n",
        .url = "https://h/x",
        .topics = &.{ "", "   " },
    }, 1));
}

test "matchesTopic handles exact matches and trailing wildcards" {
    var s = try newSub(&.{ "download.job.completed", "deliver.*" });
    defer s.deinit();
    try drain(&s);

    try t.expect(s.matchesTopic("download.job.completed"));
    try t.expect(!s.matchesTopic("download.job.failed"));
    try t.expect(!s.matchesTopic("download.job.completed.extra"));

    try t.expect(s.matchesTopic("deliver.complete"));
    try t.expect(s.matchesTopic("deliver.failed"));
    try t.expect(s.matchesTopic("deliver."));
    try t.expect(!s.matchesTopic("delive"));
    try t.expect(!s.matchesTopic("verify.ok"));
}

test "a bare star matches everything" {
    var s = try newSub(&.{"*"});
    defer s.deinit();
    try drain(&s);
    try t.expect(s.matchesTopic("anything.at.all"));
    try t.expect(s.matchesTopic(""));
}

test "wants also honours the enabled flag" {
    var s = try newSub(&.{"deliver.*"});
    defer s.deinit();
    try drain(&s);

    try t.expect(s.wants("deliver.complete"));
    try s.setEnabled(false, 2);
    try t.expect(!s.wants("deliver.complete"));
    // Interest itself is unchanged — only delivery is suspended.
    try t.expect(s.matchesTopic("deliver.complete"));

    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqualStrings("notify.subscription.disabled", batch[0].topic());
}

test "setEnabled is silent when unchanged" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    s.setId(1);
    try drain(&s);

    try s.setEnabled(true, 999);
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
    try t.expectEqual(@as(Timestamp, 1_000), s.updated_at);

    try s.setEnabled(false, 2_000);
    try s.setEnabled(true, 3_000);
    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
    try t.expectEqualStrings("notify.subscription.disabled", batch[0].topic());
    try t.expectEqualStrings("notify.subscription.enabled", batch[1].topic());
}

test "update applies url, topics and secret and emits once" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    s.setId(2);
    try drain(&s);

    try s.update(.{
        .url = "https://other.example.com/hook",
        .topics = &.{ "verify.ok", "deliver.*" },
        .secret = "shhh",
    }, 5_000);

    try t.expectEqualStrings("https://other.example.com/hook", s.url);
    try t.expectEqualStrings("shhh", s.secret);
    try t.expectEqual(@as(usize, 2), s.topics.len);
    try t.expectEqualStrings("deliver.*", s.topics[0]); // sorted
    try t.expectEqualStrings("verify.ok", s.topics[1]);
    try t.expectEqual(@as(Timestamp, 5_000), s.updated_at);

    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 1), batch.len);
    try t.expectEqualStrings("notify.subscription.updated", batch[0].topic());
    try t.expectEqual(@as(SubscriptionId, 2), batch[0].aggregateId());
}

test "an update that changes nothing is silent" {
    // Go emitted SubscriptionUpdated unconditionally here.
    var s = try newSub(&.{ "b", "a" });
    defer s.deinit();
    s.setId(2);
    try drain(&s);

    try s.update(.{
        .url = "https://hooks.example.com/abc",
        // Same set, different order and with a duplicate: normalisation
        // makes it byte-identical, so nothing changed.
        .topics = &.{ "a", "b", "a" },
        .secret = "",
        .enabled = true,
    }, 9_000);

    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
    try t.expectEqual(@as(Timestamp, 1_000), s.updated_at);
}

test "an enable flip inside update emits both the specific and generic event" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    s.setId(3);
    try drain(&s);

    try s.update(.{ .enabled = false }, 5_000);
    try t.expect(!s.enabled);

    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
    try t.expectEqualStrings("notify.subscription.disabled", batch[0].topic());
    try t.expectEqualStrings("notify.subscription.updated", batch[1].topic());
}

test "an empty secret clears a previously set one" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    try drain(&s);

    try s.update(.{ .secret = "key" }, 2_000);
    try t.expectEqualStrings("key", s.secret);
    try s.update(.{ .secret = "" }, 3_000);
    try t.expectEqual(@as(usize, 0), s.secret.len);

    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqual(@as(usize, 2), batch.len);
}

test "a rejected update leaves the aggregate untouched" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    s.setId(1);
    try drain(&s);

    try t.expectError(error.UrlSchemeUnsupported, s.update(.{
        .url = "ftp://nope/x",
        .topics = &.{"would-have-applied"},
    }, 9_000));
    try t.expectError(error.TopicsRequired, s.update(.{ .topics = &.{"  "} }, 9_000));

    try t.expectEqualStrings("https://hooks.example.com/abc", s.url);
    try t.expectEqual(@as(usize, 1), s.topics.len);
    try t.expectEqualStrings("a", s.topics[0]);
    try t.expectEqual(@as(Timestamp, 1_000), s.updated_at);
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
}

test "the added event keeps its own url and topics across an update" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    s.setId(1);

    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);

    try s.update(.{ .url = "https://moved.example.com/x", .topics = &.{"z"} }, 2_000);
    try t.expectEqualStrings("https://hooks.example.com/abc", batch[0].added.url);
    try t.expectEqualStrings("a", batch[0].added.topics[0]);
    try drain(&s);
}

test "delivery telemetry moves without emitting events" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    try drain(&s);

    try s.markDeliveryFailure("502 Bad Gateway", 2_000);
    try t.expectEqualStrings("502 Bad Gateway", s.last_error);
    try t.expectEqual(@as(?Timestamp, 2_000), s.last_error_at);
    try t.expectEqual(@as(?Timestamp, null), s.last_success_at);
    try t.expectEqual(@as(Timestamp, 2_000), s.updated_at);

    try s.markDeliverySuccess(3_000);
    try t.expectEqual(@as(usize, 0), s.last_error.len);
    try t.expectEqual(@as(?Timestamp, 3_000), s.last_success_at);
    // The failure instant is kept: "worked at X, was failing at Y".
    try t.expectEqual(@as(?Timestamp, 2_000), s.last_error_at);

    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
}

test "markRemoved records the tombstone with the aggregate's name" {
    var s = try newSub(&.{"a"});
    defer s.deinit();
    s.setId(6);
    try drain(&s);

    try s.markRemoved(7_000);
    const batch = try s.pullEvents();
    defer event.deinitAll(Event, t.allocator, batch);
    try t.expectEqualStrings("notify.subscription.removed", batch[0].topic());
    try t.expectEqualStrings("ops", batch[0].removed.name);
    try t.expectEqual(@as(Timestamp, 7_000), batch[0].occurredAt());
}

test "hydrate restores a row without events" {
    var s = try Subscription.hydrate(t.allocator, .{
        .id = 8,
        .name = "discord-ops",
        .kind = .discord,
        .url = "https://discord.com/api/webhooks/1/2",
        .topics = &.{ "deliver.*", "verify.failed" },
        .secret = "k",
        .enabled = false,
        .last_success_at = 10,
        .last_error_at = 20,
        .last_error = "429",
        .created_at = 1,
        .updated_at = 20,
    });
    defer s.deinit();

    try t.expectEqual(Kind.discord, s.kind);
    try t.expect(!s.enabled);
    try t.expect(s.matchesTopic("deliver.complete"));
    try t.expect(!s.wants("deliver.complete")); // disabled
    try t.expectEqualStrings("429", s.last_error);
    try t.expectEqual(@as(usize, 0), s.pendingEvents().len);
}

test "every event tag has a distinct prefixed topic" {
    var seen: [std.meta.fields(EventKind).len][]const u8 = undefined;
    inline for (std.meta.fields(EventKind), 0..) |f, i| {
        const e: Event = @unionInit(Event, f.name, std.mem.zeroes(@FieldType(Event, f.name)));
        const tp = e.topic();
        try t.expect(std.mem.startsWith(u8, tp, topic_prefix));
        for (seen[0..i]) |prev| try t.expect(!std.mem.eql(u8, prev, tp));
        seen[i] = tp;
    }
}

test "kind round-trips its persisted form" {
    try t.expectEqualStrings("webhook", Kind.webhook.toString());
    try t.expectEqualStrings("slack", Kind.slack.toString());
    try t.expectEqual(Kind.discord, Kind.parse("discord").?);
    try t.expectEqual(@as(?Kind, null), Kind.parse("pushover"));
}

test "undrained events are freed with the aggregate" {
    var s = try newSub(&.{ "a", "b" });
    try s.markRemoved(2);
    s.deinit();
}
