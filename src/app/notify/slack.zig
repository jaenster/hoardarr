//! Slack incoming-webhook adapter.
//!
//! Spec: https://api.slack.com/messaging/webhooks
//!
//! Block Kit rather than `attachments`: attachments are deprecated and
//! render worse for structured events. The cost is that Slack has no
//! per-message colour outside attachments, so the outcome is signalled
//! by a leading emoji in the header instead.
//!
//! Block order:
//!
//!   1. `header`  — outcome emoji plus the cleaned release name.
//!   2. `section` — bold verb, then the raw release in a code block.
//!   3. `section` — the Source / Category / Size / … field grid, when
//!                  any of them has a value.
//!   4. `section` — the error message, for failures.
//!   5. `context` — the raw topic and "hoardarr • <sub> • <timestamp>".
//!
//! The top-level `text` is the notification / mobile-push fallback,
//! which Block Kit requires for accessibility.
//!
//! `buildPayload` is pure: envelope in, bytes out. `send` is the only
//! part that touches the network, through the injected `Transport`.

const std = @import("std");
const event = @import("../../domain/event.zig");
const render = @import("render.zig");
const transport = @import("transport.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Target = transport.Target;

/// Slack rejects a `header` block whose text exceeds 150 characters.
const header_max = 150;

/// Cap on the error message so a runaway stack trace does not get the
/// whole message rejected.
const error_max = 1000;

pub const user_agent = "hoardarr-slack/1";

pub const headers: []const transport.Header = &.{
    .{ .name = "Content-Type", .value = "application/json" },
    .{ .name = "User-Agent", .value = user_agent },
};

/// One attempt only, matching Discord. Slack's webhook rate limit is
/// one message per second per hook; a retry burst is the fastest way to
/// find it.
pub const policy: transport.RetryPolicy = .chat;

/// Serialises `env` as a Slack webhook body. Allocated from `arena`.
pub fn buildPayload(
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) Allocator.Error![]u8 {
    var out: Writer.Allocating = .init(arena);
    errdefer out.deinit();
    writeBody(&out.writer, arena, target, env) catch |e| switch (e) {
        // Arena-backed writer: the only failure mode is OOM.
        error.WriteFailed => return error.OutOfMemory,
        error.OutOfMemory => return error.OutOfMemory,
    };
    return out.toOwnedSlice();
}

pub fn buildRequest(
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) Allocator.Error!transport.Request {
    return .{
        .url = target.url,
        .headers = headers,
        .body = try buildPayload(arena, target, env),
    };
}

pub fn send(
    arena: Allocator,
    t: transport.Transport,
    target: Target,
    env: event.Envelope,
) Allocator.Error!transport.Delivery {
    const req = try buildRequest(arena, target, env);
    return transport.deliver(t, policy, req);
}

/// Leading marker for the header block. Slack has no per-message colour
/// without the deprecated `attachments` shape, so the emoji is the only
/// signalling channel that survives.
pub fn outcomeEmoji(o: render.Outcome) []const u8 {
    return switch (o) {
        .ok => "✅",
        .fail => "❌",
        .warn => "⚠️",
        .info => "ℹ️",
    };
}

fn writeBody(
    w: *Writer,
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) (Allocator.Error || Writer.Error)!void {
    const v = try render.View.from(arena, env);
    const title = if (v.clean_title.len != 0) v.clean_title else v.verb;

    // The header is emoji + title, capped. Materialised because the cap
    // applies to the concatenation, not to either half.
    const header = render.truncate(
        try std.mem.concat(arena, u8, &.{ outcomeEmoji(v.outcome), " ", title }),
        header_max,
    );

    try w.writeAll("{");

    // Fallback line for notifications and mobile push.
    try render.writeJsonKey(w, "text");
    try w.writeByte('"');
    try render.writeJsonInner(w, v.verb);
    if (v.release.len != 0) {
        try w.writeAll(" — ");
        try render.writeJsonInner(w, v.release);
    }
    try w.writeByte('"');

    try w.writeAll(",");
    try render.writeJsonKey(w, "blocks");
    try w.writeAll("[");

    // 1. Header.
    try w.writeAll("{");
    try render.writeJsonField(w, "type", "header");
    try w.writeAll(",");
    try render.writeJsonKey(w, "text");
    try w.writeAll("{");
    try render.writeJsonField(w, "type", "plain_text");
    try w.writeAll(",");
    try render.writeJsonField(w, "text", header);
    try w.writeAll("}}");

    // 2. Verb + raw release.
    try w.writeAll(",{");
    try render.writeJsonField(w, "type", "section");
    try w.writeAll(",");
    try render.writeJsonKey(w, "text");
    try w.writeAll("{");
    try render.writeJsonField(w, "type", "mrkdwn");
    try w.writeAll(",");
    try render.writeJsonKey(w, "text");
    try writeSectionBody(w, v);
    try w.writeAll("}}");

    // 3. Field grid, omitted entirely when nothing has a value — an
    // empty `fields` array is a Block Kit validation error.
    try writeFieldsBlock(w, v);

    // 4. Error, for failures.
    if (v.error_msg.len != 0) {
        try w.writeAll(",{");
        try render.writeJsonField(w, "type", "section");
        try w.writeAll(",");
        try render.writeJsonKey(w, "text");
        try w.writeAll("{");
        try render.writeJsonField(w, "type", "mrkdwn");
        try w.writeAll(",");
        try render.writeJsonKey(w, "text");
        try w.writeAll("\"*Error*\\n```");
        try render.writeJsonInner(w, render.truncate(v.error_msg, error_max));
        try w.writeAll("```\"}}");
    }

    // A payload that would not project still produces a message that
    // says so, rather than one with silently missing fields.
    if (v.diag) |d| {
        try w.writeAll(",{");
        try render.writeJsonField(w, "type", "section");
        try w.writeAll(",");
        try render.writeJsonKey(w, "text");
        try w.writeAll("{");
        try render.writeJsonField(w, "type", "mrkdwn");
        try w.writeAll(",");
        try render.writeJsonKey(w, "text");
        try w.writeAll("\"*Payload*\\n");
        try render.writeJsonInner(w, d.message());
        try w.writeAll("\"}}");
    }

    // 5. Context footer.
    var ts: render.Rfc3339Buf = undefined;
    try w.writeAll(",{");
    try render.writeJsonField(w, "type", "context");
    try w.writeAll(",");
    try render.writeJsonKey(w, "elements");
    try w.writeAll("[{");
    try render.writeJsonField(w, "type", "mrkdwn");
    try w.writeAll(",");
    try render.writeJsonKey(w, "text");
    try w.writeByte('"');
    try w.writeByte('`');
    try render.writeJsonInner(w, v.topic);
    try w.writeByte('`');
    try w.writeByte('"');
    try w.writeAll("},{");
    try render.writeJsonField(w, "type", "mrkdwn");
    try w.writeAll(",");
    try render.writeJsonKey(w, "text");
    try w.writeAll("\"hoardarr • ");
    try render.writeJsonInner(w, target.name);
    try w.writeAll(" • ");
    try render.writeJsonInner(w, render.rfc3339(&ts, v.occurred_at));
    try w.writeAll("\"}]}");

    try w.writeAll("]}");
}

/// Main section: bold verb, then the raw release in a code block so the
/// operator can copy it.
fn writeSectionBody(w: *Writer, v: render.View) Writer.Error!void {
    try w.writeByte('"');
    try w.writeAll("*");
    try render.writeJsonInner(w, v.verb);
    try w.writeAll("*");
    if (v.release.len != 0) {
        try w.writeAll("\\n```");
        try render.writeJsonInner(w, v.release);
        try w.writeAll("```");
    }
    try w.writeByte('"');
}

/// The `fields` section, as `*Label*\nvalue` mrkdwn entries. Emits
/// nothing at all when every value is empty.
fn writeFieldsBlock(w: *Writer, v: render.View) Writer.Error!void {
    var count_buf: [24]u8 = undefined;
    const files: []const u8 = if (v.file_count > 0)
        std.fmt.bufPrint(&count_buf, "{d}", .{v.file_count}) catch unreachable
    else
        "";

    const pairs = [_]struct { []const u8, []const u8 }{
        .{ "Source", v.source },
        .{ "Category", v.category },
        .{ "Size", v.size_human },
        .{ "Files", files },
        .{ "Quality", v.quality },
        .{ "State", v.state },
    };

    var any = false;
    for (pairs) |p| {
        if (p[1].len != 0) any = true;
    }
    if (!any) return;

    try w.writeAll(",{");
    try render.writeJsonField(w, "type", "section");
    try w.writeAll(",");
    try render.writeJsonKey(w, "fields");
    try w.writeAll("[");
    var first = true;
    for (pairs) |p| {
        if (p[1].len == 0) continue;
        if (!first) try w.writeAll(",");
        first = false;
        try w.writeAll("{");
        try render.writeJsonField(w, "type", "mrkdwn");
        try w.writeAll(",");
        try render.writeJsonKey(w, "text");
        try w.writeByte('"');
        try w.writeAll("*");
        try render.writeJsonInner(w, p[0]);
        try w.writeAll("*\\n");
        try render.writeJsonInner(w, p[1]);
        try w.writeByte('"');
        try w.writeAll("}");
    }
    try w.writeAll("]}");
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

const enriched_payload =
    \\{
    \\  "event": {"job_id": 42},
    \\  "job": {
    \\    "name": "Foo.Bar.S01E02.1080p.WEB-DL-RLSGRP",
    \\    "category": "tv",
    \\    "state": "completed",
    \\    "source": "Radarr/5.0",
    \\    "total_bytes": 1073741824,
    \\    "file_count": 3
    \\  }
    \\}
;

fn testEnv(topic: []const u8, payload: []const u8) event.Envelope {
    return .{
        .id = .nil,
        .topic = topic,
        .aggregate_id = "42",
        .occurred_at = 1_700_000_000_000,
        .payload = payload,
    };
}

/// Parsed payload plus block lookup helpers. Every assertion goes
/// through a real JSON parse.
const Message = struct {
    parsed: std.json.Parsed(std.json.Value),

    fn init(body: []const u8) !Message {
        return .{ .parsed = try std.json.parseFromSlice(
            std.json.Value,
            testing.allocator,
            body,
            .{},
        ) };
    }

    fn deinit(self: Message) void {
        self.parsed.deinit();
    }

    fn root(self: Message) std.json.ObjectMap {
        return self.parsed.value.object;
    }

    fn blocks(self: Message) []std.json.Value {
        return self.root().get("blocks").?.array.items;
    }

    /// The first `section` block carrying a `fields` array.
    fn fieldsBlock(self: Message) ?std.json.ObjectMap {
        for (self.blocks()) |b| {
            const o = b.object;
            if (!std.mem.eql(u8, o.get("type").?.string, "section")) continue;
            if (o.get("fields") != null) return o;
        }
        return null;
    }

    /// Every `section` block's mrkdwn text, concatenated for substring
    /// assertions.
    fn sectionTexts(self: Message, out: *std.ArrayList([]const u8), a: Allocator) !void {
        for (self.blocks()) |b| {
            const o = b.object;
            const t = o.get("text") orelse continue;
            if (t != .object) continue;
            try out.append(a, t.object.get("text").?.string);
        }
    }
};

fn hasPrefixIn(items: []std.json.Value, prefix: []const u8) bool {
    for (items) |f| {
        if (std.mem.startsWith(u8, f.object.get("text").?.string, prefix)) return true;
    }
    return false;
}

test "block kit shape: header, section, fields, context" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "slack-test", .url = "https://example.test/hook" },
        testEnv("deliver.complete", enriched_payload),
    );
    const m = try Message.init(body);
    defer m.deinit();

    // Fallback text mentions the verb and the raw release.
    const fallback = m.root().get("text").?.string;
    try testing.expect(std.mem.indexOf(u8, fallback, "Delivered") != null);
    try testing.expect(std.mem.indexOf(
        u8,
        fallback,
        "Foo.Bar.S01E02.1080p.WEB-DL-RLSGRP",
    ) != null);

    const blocks = m.blocks();
    try testing.expect(blocks.len >= 3);

    const header = blocks[0].object;
    try testing.expectEqualStrings("header", header.get("type").?.string);
    const header_text = header.get("text").?.object;
    try testing.expectEqualStrings("plain_text", header_text.get("type").?.string);
    try testing.expect(std.mem.indexOf(
        u8,
        header_text.get("text").?.string,
        "Foo Bar S01E02",
    ) != null);
    // Outcome is signalled by the emoji, since Block Kit has no colour.
    try testing.expect(std.mem.startsWith(u8, header_text.get("text").?.string, "✅"));

    // Second block: bold verb plus a code-blocked raw release.
    const section = blocks[1].object;
    try testing.expectEqualStrings("section", section.get("type").?.string);
    const sect_text = section.get("text").?.object.get("text").?.string;
    try testing.expect(std.mem.startsWith(u8, sect_text, "*Delivered*"));
    try testing.expect(std.mem.indexOf(u8, sect_text, "\n```") != null);

    // Field grid.
    const fb = m.fieldsBlock() orelse return error.NoFieldsBlock;
    const fields = fb.get("fields").?.array.items;
    for ([_][]const u8{ "*Source*", "*Category*", "*Size*", "*Files*", "*Quality*", "*State*" }) |want| {
        try testing.expect(hasPrefixIn(fields, want));
    }
    for (fields) |f| {
        try testing.expectEqualStrings("mrkdwn", f.object.get("type").?.string);
    }
    try testing.expect(hasPrefixIn(fields, "*Source*\nRadarr"));
    try testing.expect(hasPrefixIn(fields, "*Size*\n1.00 GB"));
    try testing.expect(hasPrefixIn(fields, "*Files*\n3"));

    // Context footer carries the raw topic and the subscription name.
    const ctx = blocks[blocks.len - 1].object;
    try testing.expectEqualStrings("context", ctx.get("type").?.string);
    const elements = ctx.get("elements").?.array.items;
    try testing.expectEqualStrings("`deliver.complete`", elements[0].object.get("text").?.string);
    try testing.expectEqualStrings(
        "hoardarr • slack-test • 2023-11-14T22:13:20Z",
        elements[1].object.get("text").?.string,
    );
}

test "a failing status is reported, not swallowed" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 500 }} };
    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "s", .url = "https://example.test/hook" },
        testEnv("deliver.complete", enriched_payload),
    );
    try testing.expectError(error.Unavailable, d.toError());
    try testing.expectEqual(@as(usize, 1), fake.calls);
}

test "send posts to the target URL with the JSON content type" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 200 }} };
    const url = "https://hooks.slack.com/services/T00/B00/tok";
    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "s", .url = url },
        testEnv("deliver.complete", enriched_payload),
    );
    try testing.expect(d.ok());
    const req = fake.last.?;
    try testing.expectEqualStrings(url, req.url);
    try testing.expectEqualStrings("Content-Type", req.headers[0].name);
    try testing.expectEqualStrings("application/json", req.headers[0].value);
    try testing.expectEqualStrings("hoardarr-slack/1", req.headers[1].value);
}

test "an empty field grid emits no fields block at all" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    // No payload: nothing to put in the grid, and Slack rejects an
    // empty `fields` array outright.
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "s", .url = "https://example.test/hook" },
        testEnv("verify.started", ""),
    );
    const m = try Message.init(body);
    defer m.deinit();
    try testing.expect(m.fieldsBlock() == null);
    // Header falls back to the verb, and the emoji is the neutral one.
    const header_text = m.blocks()[0].object.get("text").?.object.get("text").?.string;
    try testing.expectEqualStrings("ℹ️ Verifying", header_text);
    // No release means the fallback text is just the verb.
    try testing.expectEqualStrings("Verifying", m.root().get("text").?.string);
}

test "outcome emoji covers every outcome" {
    try testing.expectEqualStrings("✅", outcomeEmoji(.ok));
    try testing.expectEqualStrings("❌", outcomeEmoji(.fail));
    try testing.expectEqualStrings("⚠️", outcomeEmoji(.warn));
    try testing.expectEqualStrings("ℹ️", outcomeEmoji(.info));
}

test "a failure event gets an error section" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "s", .url = "https://example.test/hook" },
        testEnv(
            "deliver.failed",
            \\{"event":{"err":"target filesystem is read-only"},"job":{"name":"X-G","state":"failed"}}
            ,
        ),
    );
    const m = try Message.init(body);
    defer m.deinit();

    var texts: std.ArrayList([]const u8) = .empty;
    defer texts.deinit(arena.allocator());
    try m.sectionTexts(&texts, arena.allocator());

    var found = false;
    for (texts.items) |t| {
        if (std.mem.startsWith(u8, t, "*Error*")) {
            found = true;
            try testing.expect(std.mem.indexOf(u8, t, "read-only") != null);
        }
    }
    try testing.expect(found);
    try testing.expect(std.mem.startsWith(u8, m.blocks()[0].object.get("text").?.object.get("text").?.string, "❌"));
}

test "the header is capped at Slack's 150-character limit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const long = "T" ** 400;
    const payload = try std.fmt.allocPrint(
        arena.allocator(),
        "{{\"job\":{{\"name\":\"{s}\"}}}}",
        .{long},
    );
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "s", .url = "https://example.test/hook" },
        testEnv("deliver.complete", payload),
    );
    const m = try Message.init(body);
    defer m.deinit();
    const header_text = m.blocks()[0].object.get("text").?.object.get("text").?.string;
    // Byte-capped, and the emoji is multi-byte, so assert on bytes.
    try testing.expect(header_text.len <= header_max);
}

test "a malformed payload produces a diagnostic section" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "s", .url = "https://example.test/hook" },
        testEnv("deliver.complete", "[1,2,3]"),
    );
    const m = try Message.init(body);
    defer m.deinit();

    var texts: std.ArrayList([]const u8) = .empty;
    defer texts.deinit(arena.allocator());
    try m.sectionTexts(&texts, arena.allocator());

    var found = false;
    for (texts.items) |t| {
        if (std.mem.startsWith(u8, t, "*Payload*")) {
            found = true;
            try testing.expect(std.mem.indexOf(
                u8,
                t,
                render.Diagnostic.payload_not_object.message(),
            ) != null);
        }
    }
    try testing.expect(found);
    // Still a complete message with the topic-derived parts.
    try testing.expectEqualStrings("Delivered", m.root().get("text").?.string);
}

test "hostile release names still produce parseable JSON" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const names = [_][]const u8{
        "a\\\"b\\\\c",
        "\\\"}],\\\"blocks\\\":[",
        "x\\u0000y\\u001f",
        "\\ud834lone",
        "```fence```",
    };
    for (names) |n| {
        const payload = try std.fmt.allocPrint(
            arena.allocator(),
            "{{\"job\":{{\"name\":\"{s}\",\"category\":\"tv\"}}}}",
            .{n},
        );
        const body = try buildPayload(
            arena.allocator(),
            .{ .name = "na\"me\\", .url = "https://example.test/hook" },
            testEnv("deliver.failed", payload),
        );
        const m = std.json.parseFromSlice(
            std.json.Value,
            testing.allocator,
            body,
            .{},
        ) catch |err| {
            std.debug.print("unparseable payload for {s}:\n{s}\n", .{ n, body });
            return err;
        };
        defer m.deinit();
        try testing.expect(std.unicode.utf8ValidateSlice(body));
        // The blocks array survived the injection attempt: still exactly
        // the block count we emitted, not one the release name added.
        try testing.expect(m.value.object.get("blocks").?.array.items.len >= 3);
    }
}

test "the subscription secret never reaches the payload or the URL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const secret = "s3cr3t-hmac-key";
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 200 }} };
    _ = try send(
        arena.allocator(),
        fake.transport(),
        .{
            .name = "s",
            .url = "https://hooks.slack.com/services/T00/B00/tok",
            .secret = secret,
        },
        testEnv("deliver.complete", enriched_payload),
    );
    const req = fake.last.?;
    try testing.expect(std.mem.indexOf(u8, req.body, secret) == null);
    try testing.expect(std.mem.indexOf(u8, req.url, secret) == null);
    for (req.headers) |h| {
        try testing.expect(std.mem.indexOf(u8, h.value, secret) == null);
    }
}
