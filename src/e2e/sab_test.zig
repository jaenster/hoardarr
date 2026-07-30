//! Port of `internal/bootstrap/e2e_sab_test.go` —
//! `TestSAB_E2E_PhaseOneModes`.
//!
//! Pins the `/sabnzbd/api` JSON shapes an *arr talks to. Sonarr and
//! Radarr parse these by field name, so a change here is a change in
//! somebody's automation; the point of the test is that such a change
//! cannot happen silently.
//!
//! One departure from Go: Go's subtests each get their own `t.Run`
//! scope but share one daemon. Zig has no subtests, so the modes are
//! sections of one test against one daemon — same coverage, same
//! ordering dependency (addfile before queue before delete).

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");

const base = "/sabnzbd/api";
const key_q = "&apikey=" ++ h.api_key;

/// The smallest NZB that describes something fetchable. Nothing fetches
/// it — there is no server configured — so the job sits in `queued`,
/// which is exactly what the queue-shape assertions want.
const nzb =
    \\<?xml version="1.0" encoding="UTF-8"?>
    \\<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
    \\  <head><meta type="title">sab-test</meta></head>
    \\  <file poster="t" date="1700000000" subject='[1/1] - "x.bin" yEnc'>
    \\    <groups><group>alt.binaries.test</group></groups>
    \\    <segments>
    \\      <segment bytes="64" number="1">sab-test-seg1@local</segment>
    \\    </segments>
    \\  </file>
    \\</nzb>
;

const boundary = "----hoardarre2eboundary";

/// A `multipart/form-data` body with the NZB in a part named `name`,
/// which is the part the *arr suite actually posts.
fn addFileBody(gpa: std.mem.Allocator, filename: []const u8, xml: []const u8) ![]u8 {
    return std.fmt.allocPrint(gpa, "--" ++ boundary ++ "\r\n" ++
        "Content-Disposition: form-data; name=\"name\"; filename=\"{s}\"\r\n" ++
        "Content-Type: application/octet-stream\r\n\r\n" ++
        "{s}\r\n" ++
        "--" ++ boundary ++ "--\r\n", .{ filename, xml });
}

test "sab: the modes an *arr calls, in the shapes it parses" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "sab", .{});
    defer fx.deinit();

    // A wrong key is refused. The SAB surface authenticates itself from
    // the form rather than through the server's middleware, so this is
    // a different code path from the REST 401 and needs its own
    // assertion.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=version&apikey=nope", .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 401, "sab with a wrong apikey");
    }

    // version — must claim 3.x or *arr refuses to talk to us at all.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=version" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=version");
        const v = r.field("version") orelse return error.NoVersionField;
        if (!std.mem.startsWith(u8, v, "3.")) {
            std.debug.print("\nsab version = '{s}'; want 3.x\n", .{v});
            return error.WrongSabVersion;
        }
    }

    // get_cats — the catch-all category has to be in the list, because
    // it is what an *arr picks when its category is unset.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=get_cats" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=get_cats");
        try testing.expect(r.contains("\"*\""));
    }

    // get_config — `misc.complete_dir` is what an *arr reads to decide
    // where to look for the finished download.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=get_config" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=get_config");
        try testing.expect(r.contains("\"misc\""));
        const cd = r.field("complete_dir") orelse return error.NoCompleteDir;
        try testing.expect(cd.len > 0);
    }

    // An empty queue reports zero slots and Idle, not an error.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=queue" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=queue (empty)");
        try r.expectField("noofslots", "0");
        try r.expectField("status", "Idle");
    }

    // addfile → the queue → delete.
    var nzo: []u8 = &.{};
    defer gpa.free(nzo);
    {
        const body = try addFileBody(gpa, "sab-test.nzb", nzb);
        defer gpa.free(body);

        var r = try fx.request(.{
            .method = .post,
            .path = base ++ "?mode=addfile&cat=*" ++ key_q,
            .body = body,
            .content_type = "multipart/form-data; boundary=" ++ boundary,
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=addfile");
        try r.expectField("status", "true");

        // The nzo id is the handle an *arr keeps; its prefix is part of
        // the contract, because *arr string-matches on it.
        const id = r.field("nzo_ids") orelse return error.NoNzoIds;
        // `nzo_ids` is an array, so the scanner stops at the `]`; the
        // first quoted element is what we want.
        const start = std.mem.indexOfScalar(u8, id, '"') orelse return error.NoNzoId;
        const rest = id[start + 1 ..];
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse return error.NoNzoId;
        nzo = try gpa.dupe(u8, rest[0..end]);
        if (!std.mem.startsWith(u8, nzo, "SABnzbd_nzo_")) {
            std.debug.print("\nnzo_id = '{s}'; want a SABnzbd_nzo_ prefix\n", .{nzo});
            return error.WrongNzoIdShape;
        }
    }

    // The queue now has exactly one slot, named after the upload with
    // the .nzb trimmed — the label an *arr shows the user.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=queue" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=queue (one slot)");
        try r.expectField("noofslots", "1");
        try r.expectField("filename", "sab-test");
        try testing.expect(r.contains(nzo));
    }

    // pause, then delete, both through the form-encoded queue mode.
    {
        const body = try std.fmt.allocPrint(
            gpa,
            "mode=queue&name=pause&value={s}&apikey=" ++ h.api_key,
            .{nzo},
        );
        defer gpa.free(body);
        var r = try fx.request(.{
            .method = .post,
            .path = base,
            .body = body,
            .content_type = "application/x-www-form-urlencoded",
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=queue&name=pause");
    }
    {
        const body = try std.fmt.allocPrint(
            gpa,
            "mode=queue&name=delete&value={s}&apikey=" ++ h.api_key,
            .{nzo},
        );
        defer gpa.free(body);
        var r = try fx.request(.{
            .method = .post,
            .path = base,
            .body = body,
            .content_type = "application/x-www-form-urlencoded",
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=queue&name=delete");
    }

    // And the queue is empty again — the delete reached the store, not
    // just the handler.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=queue" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=queue (after delete)");
        try r.expectField("noofslots", "0");
    }

    // history, empty.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=history" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=history");
        try r.expectField("noofslots", "0");
    }

    // An unimplemented mode is a 400, not a 200 with an empty body: an
    // *arr that gets a 200 assumes the call worked.
    {
        var r = try fx.request(.{ .path = base ++ "?mode=reorder" ++ key_q, .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 400, "mode=reorder");
    }
}

test "sab: a queued job is still queued after a restart" {
    // Not in the Go suite. An *arr polls the queue every few seconds
    // and treats a disappearing nzo_id as a failed grab, so losing the
    // queue across a container restart is a user-visible bug even
    // before any bytes have been fetched.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "sab-restart", .{});
    defer fx.deinit();

    const body = try addFileBody(gpa, "restart-me.nzb", nzb);
    defer gpa.free(body);

    var nzo: []u8 = &.{};
    defer gpa.free(nzo);
    {
        var r = try fx.request(.{
            .method = .post,
            .path = base ++ "?mode=addfile&cat=*" ++ key_q,
            .body = body,
            .content_type = "multipart/form-data; boundary=" ++ boundary,
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "mode=addfile");
        const ids = r.field("nzo_ids") orelse return error.NoNzoIds;
        const start = std.mem.indexOfScalar(u8, ids, '"') orelse return error.NoNzoId;
        const rest = ids[start + 1 ..];
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse return error.NoNzoId;
        nzo = try gpa.dupe(u8, rest[0..end]);
    }

    try fx.restart(.{});

    var r = try fx.request(.{ .path = base ++ "?mode=queue" ++ key_q, .api_key = "" });
    defer r.deinit();
    try h.expectStatus(&r, 200, "mode=queue after a restart");
    try r.expectField("noofslots", "1");
    try r.expectField("filename", "restart-me");
    // The same handle, not merely a job with the same name: an *arr
    // tracks the id and a re-issued one is a lost download to it.
    if (!r.contains(nzo)) {
        std.debug.print("\nnzo_id '{s}' not in the post-restart queue: {s}\n", .{ nzo, r.body });
        return error.NzoIdChangedAcrossRestart;
    }
}
