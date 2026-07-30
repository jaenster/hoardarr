//! Port of `internal/bootstrap/bootstrap_test.go` — `TestM05_FullBootstrap`.
//!
//! The acceptance test for the composition root: config loads, SQLite
//! opens and migrates, the outbox schema is there, the directory tree
//! exists, the server listens, the public endpoint is public, the
//! protected one is protected, and shutdown returns rather than hanging.
//!
//! Two deliberate departures from the Go original, both because the Zig
//! daemon genuinely behaves differently rather than because the
//! assertion was inconvenient:
//!
//!   * Go's `config.LoadOrCreate` **writes** `config.toml` on first run
//!     and the test asserts the file appeared. `bootstrap.run` reads a
//!     config file if one is there and otherwise runs on defaults; it
//!     never writes one (`readConfigFile` returns `null` for a missing
//!     file and that is the end of it). So there is no file to assert
//!     on, and asserting one would be asserting a feature that does not
//!     exist.
//!   * Go cancels a context to shut the daemon down and waits for
//!     `Run` to return. Here the loop is driven by the test, so the
//!     equivalent is `loop.stop()` — asserted to make `run` return.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const sys = @import("../posix/sys.zig");

test "M0.5: a freshly built daemon has its schema, its directories and its auth" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "m05", .{});
    defer fx.deinit();

    const cfg = fx.app.cfg.config;

    // The API key must exist before anything is served; a daemon that
    // starts with an empty key is a daemon with no auth at all.
    try testing.expect(fx.app.runtime.apiKey().len > 0);

    // The database file exists where the config says it does.
    {
        const db_path = try fx.dataPath(gpa, &.{"hoardarr.db"});
        defer gpa.free(db_path);
        try testing.expect(h.fileExists(db_path));
    }

    // The outbox schema is present. Without these three the daemon runs
    // and every write that publishes an event fails at the first commit.
    for ([_][]const u8{ "outbox", "outbox_subs", "schema_migrations" }) |name| {
        const n = fx.app.db.scalarInt(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name = ?",
            .{name},
        ) catch |e| {
            std.debug.print("\nquerying for table '{s}': {t}\n", .{ name, e });
            return error.SchemaQueryFailed;
        };
        if (n != 1) {
            std.debug.print("\ntable '{s}' not present\n", .{name});
            return error.TableMissing;
        }
    }

    // The directory tree a download needs before the first segment
    // lands. Created at start-up so a permissions problem surfaces
    // while the operator is watching, not eight minutes in.
    for ([_][]const u8{ cfg.server.data_dir, cfg.paths.incomplete_dir, cfg.paths.complete_dir }) |d| {
        if (!h.fileExists(d)) {
            std.debug.print("\nexpected directory '{s}' to exist\n", .{d});
            return error.DirectoryMissing;
        }
    }

    // Public health endpoint, no key.
    {
        var r = try fx.request(.{ .path = "/healthz", .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 200, "/healthz");
        try r.expectField("status", "ok");
    }

    // Protected endpoint without a key → 401. Go asks `/api/v1/whoami`;
    // this tree's equivalent protected read is the queue, and
    // `/api/v1/auth/whoami` is deliberately public here (it is what the
    // UI asks *before* it has credentials, to find out whether setup is
    // needed).
    {
        var r = try fx.request(.{ .path = "/api/v1/queue", .api_key = "" });
        defer r.deinit();
        try h.expectStatus(&r, 401, "/api/v1/queue without a key");
    }

    // With the key in the header → 200.
    {
        var r = try fx.get("/api/v1/queue");
        defer r.deinit();
        try testing.expect(r.contains("\"jobs\""));
    }

    // And with the key as a query parameter, which is how the SAB
    // surface authenticates and how a browser tab can be opened by hand.
    {
        var r = try fx.request(.{
            .path = "/sabnzbd/api?mode=version&apikey=" ++ h.api_key,
            .api_key = "",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "sab version by query key");
        try testing.expect(r.contains("\"version\""));
    }

    // A wrong key is refused rather than ignored.
    {
        var r = try fx.request(.{ .path = "/api/v1/queue", .api_key = "not-the-key" });
        defer r.deinit();
        try h.expectStatus(&r, 401, "/api/v1/queue with a wrong key");
    }
}

test "M0.5: the loop stops when asked, with connections still open" {
    // Go asserts `Run` returns within 5s of a context cancel. The Zig
    // equivalent is that `loop.run` returns after `loop.stop`, and the
    // interesting case is stopping while a client is attached rather
    // than from an idle loop.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "m05-stop", .{ .timers = true });
    defer fx.deinit();

    var r = try fx.get("/healthz");
    r.deinit();

    fx.app.loop.stop();
    try fx.app.loop.run();
}

test "M0.5: the generated API key is persisted, so it survives a restart" {
    // The whole point of `persistFirstRunApiKey`: an *arr configured
    // yesterday must still authenticate today. A key regenerated per
    // start would make every integration silently break on restart.
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "m05-key", .{});
    defer fx.deinit();

    const before = try gpa.dupe(u8, fx.app.runtime.apiKey());
    defer gpa.free(before);
    try testing.expectEqualStrings(h.api_key, before);

    // A *different* fallback on the second boot. Without it this test
    // would pass even with nothing persisted, because the fallback
    // alone would produce the same key twice.
    try fx.restart(.{ .api_key_fallback = "ffffffffffffffffffffffffffffffff" });

    try testing.expectEqualStrings(before, fx.app.runtime.apiKey());

    var r = try fx.get("/api/v1/queue");
    defer r.deinit();
}
