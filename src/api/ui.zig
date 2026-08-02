//! Mounting the embedded single-page app at whatever path it is served on.
//!
//! Vite cannot know the mount path at build time, so `frontend/vite.config.ts`
//! bakes a sentinel into every asset URL it emits:
//!
//!     base: "/__HOARDARR_BASE__/"
//!
//! and the server replaces that sentinel with the configured `url_base`
//! plus a slash — or a bare `/` when no base is set. One binary then works
//! behind any reverse-proxy mount with no rebuild. The sentinel lands in
//! `index.html`, and in any chunk that reads `import.meta.env.BASE_URL`.
//!
//! Serving the embedded bytes unmodified is not a cosmetic mistake: the
//! browser is told to fetch `/__HOARDARR_BASE__/assets/index-*.js`, which
//! 404s, and the page renders an empty root element. It answers 200 to
//! every request while displaying nothing, which is why "the asset route
//! works" was never evidence that the UI did.
//!
//! ## Why the rewrite is materialised rather than done per request
//!
//! The assets ship gzipped from `tools/embed_assets.zig`, and substitution
//! changes length, so the pre-compressed copy cannot survive it. Patching
//! bytes per request would therefore mean either re-gzipping the whole
//! bundle on every page load or giving up compression on its largest file.
//! Both are paid per request for a value that changes approximately never.
//! So the substituted copies — raw and gzip — are built once and reused
//! until `url_base` actually changes, which is the only event that can
//! invalidate them.

const std = @import("std");
const assets = @import("assets");
const log = @import("../core/log.zig");

const Allocator = std.mem.Allocator;

/// What Vite bakes in. Includes both slashes, so replacing it with the
/// base plus one slash yields `/hoardarr/assets/...` for a base of
/// `/hoardarr` and `/assets/...` for an empty one, with no special case.
pub const sentinel = "/__HOARDARR_BASE__/";

/// Substituted copies of the assets that mention the sentinel.
///
/// Only those: an asset without it is served straight from the binary, so
/// a bundle whose fonts and CSS carry no base reference costs nothing here.
pub const Rewriter = struct {
    gpa: Allocator,

    /// The bundle to rewrite. Injectable so the tests can exercise this
    /// against a table that definitely contains the sentinel: a build
    /// without `-Dembed-ui=true` has no assets at all, and a test that
    /// skips there is exactly the hole the original bug went through.
    table: []const assets.Asset = &assets.assets,

    /// The `url_base` `entries` was built for. Owned. Empty and
    /// `!materialised` before the first call, which is distinct from
    /// "built for the empty base".
    base: []u8 = &.{},
    materialised: bool = false,
    entries: std.ArrayList(Entry) = .empty,

    pub const Entry = struct {
        /// Borrowed from the generated table; those live in .rodata.
        path: []const u8,
        content_type: []const u8,
        /// Owned.
        raw: []u8,
        /// Owned. Null when gzip did not pay for itself, matching the
        /// threshold the build-time step applies.
        gz: ?[]u8,
    };

    pub fn deinit(self: *Rewriter) void {
        self.release();
        self.gpa.free(self.base);
        self.base = &.{};
    }

    fn release(self: *Rewriter) void {
        for (self.entries.items) |e| {
            self.gpa.free(e.raw);
            if (e.gz) |g| self.gpa.free(g);
        }
        self.entries.clearAndFree(self.gpa);
        self.materialised = false;
    }

    /// The rewritten asset for `path` under `base`, or null when that
    /// asset does not mention the sentinel and can be served as embedded.
    ///
    /// Rebuilds when `base` has changed since the last call, because
    /// `url_base` is a runtime setting: an operator moving the mount point
    /// through the API must not have to restart to see the UI work.
    pub fn find(self: *Rewriter, base: []const u8, path: []const u8) ?*const Entry {
        self.ensure(base) catch |err| {
            // A failure here means the UI cannot be served correctly, and
            // serving the sentinel instead would look like a blank page
            // with a 200. Report it and fall back to the embedded bytes so
            // the API stays up.
            log.warn("ui: could not rewrite the asset base", &.{
                log.str("base", base),
                log.str("error", @errorName(err)),
            });
            return null;
        };
        for (self.entries.items) |*e| {
            if (std.mem.eql(u8, e.path, path)) return e;
        }
        return null;
    }

    fn ensure(self: *Rewriter, base: []const u8) Allocator.Error!void {
        if (self.materialised and std.mem.eql(u8, self.base, base)) return;

        self.release();
        const owned_base = try self.gpa.dupe(u8, base);
        self.gpa.free(self.base);
        self.base = owned_base;

        // `sentinel` carries both slashes, so the replacement is the base
        // with one slash appended. An empty base collapses to `/`.
        const replacement = try std.fmt.allocPrint(self.gpa, "{s}/", .{base});
        defer self.gpa.free(replacement);

        var rewritten: usize = 0;
        for (self.table) |*a| {
            if (std.mem.indexOf(u8, a.raw, sentinel) == null) continue;

            const raw = try replaceAlloc(self.gpa, a.raw, sentinel, replacement);
            errdefer self.gpa.free(raw);

            // Same 10% rule the build-time step uses: a second copy has to
            // earn its place.
            var gz: ?[]u8 = null;
            if (gzip(self.gpa, raw)) |z| {
                if (z.len + z.len / 10 < raw.len) gz = z else self.gpa.free(z);
            } else |_| {}

            try self.entries.append(self.gpa, .{
                .path = a.path,
                .content_type = a.content_type,
                .raw = raw,
                .gz = gz,
            });
            rewritten += 1;
        }

        self.materialised = true;
        log.debug("ui: asset base applied", &.{
            log.str("base", if (base.len == 0) "/" else base),
            log.uint("rewritten", rewritten),
        });
    }
};

/// `std.mem.replaceOwned`, except the needle and replacement differ in
/// length and the count is not known up front, which is the case
/// `replacementSize` handles.
fn replaceAlloc(
    gpa: Allocator,
    input: []const u8,
    needle: []const u8,
    replacement: []const u8,
) Allocator.Error![]u8 {
    const size = std.mem.replacementSize(u8, input, needle, replacement);
    const out = try gpa.alloc(u8, size);
    errdefer gpa.free(out);
    _ = std.mem.replace(u8, input, needle, replacement, out);
    return out;
}

fn gzip(gpa: Allocator, input: []const u8) ![]u8 {
    // `Compress.init` asserts the output writer has a non-empty buffer and
    // a default Allocating starts with none, so the capacity is given up
    // front. Sized off the input because gzip output is almost always
    // smaller, which usually avoids a regrow entirely.
    var out: std.Io.Writer.Allocating = try .initCapacity(gpa, @max(input.len, 4096));
    errdefer out.deinit();

    const window = try gpa.alloc(u8, std.compress.flate.max_window_len);
    defer gpa.free(window);

    var comp = try std.compress.flate.Compress.init(&out.writer, window, .gzip, .best);
    try comp.writer.writeAll(input);
    try comp.writer.flush();
    return out.toOwnedSlice();
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "the sentinel becomes the mount path, with exactly one slash" {
    const gpa = testing.allocator;

    const src = "<script src=\"" ++ sentinel ++ "assets/app.js\"></script>";

    const at_root = try replaceAlloc(gpa, src, sentinel, "/");
    defer gpa.free(at_root);
    try testing.expectEqualStrings(
        "<script src=\"/assets/app.js\"></script>",
        at_root,
    );

    const mounted = try replaceAlloc(gpa, src, sentinel, "/hoardarr/");
    defer gpa.free(mounted);
    try testing.expectEqualStrings(
        "<script src=\"/hoardarr/assets/app.js\"></script>",
        mounted,
    );

    const nested = try replaceAlloc(gpa, src, sentinel, "/deeply/nested/mount/");
    defer gpa.free(nested);
    try testing.expectEqualStrings(
        "<script src=\"/deeply/nested/mount/assets/app.js\"></script>",
        nested,
    );
}

test "every occurrence is replaced, not just the first" {
    const gpa = testing.allocator;
    const src = sentinel ++ "a " ++ sentinel ++ "b " ++ sentinel ++ "c";
    const out = try replaceAlloc(gpa, src, sentinel, "/base/");
    defer gpa.free(out);
    try testing.expectEqualStrings("/base/a /base/b /base/c", out);
}

/// A bundle shaped like the one Vite emits, built here rather than taken
/// from the binary.
///
/// The embedded table is empty unless the build ran with
/// `-Dembed-ui=true`, and a test that skips in the default build is
/// exactly what let the missing substitution ship: the suite was green on
/// every machine that had never built the frontend. These run everywhere.
const fake_bundle = [_]assets.Asset{
    .{
        .path = "/index.html",
        .content_type = "text/html; charset=utf-8",
        .raw = "<!doctype html><script src=\"" ++ sentinel ++ "assets/app.js\"></script>" ++
            "<link rel=stylesheet href=\"" ++ sentinel ++ "assets/app.css\">" ++
            "<link rel=icon href=\"" ++ sentinel ++ "favicon.svg\">",
        .gz = null,
    },
    .{
        .path = "/assets/app.js",
        .content_type = "text/javascript; charset=utf-8",
        // A chunk that reads `import.meta.env.BASE_URL` ends up carrying
        // the sentinel too, which is why the rewrite cannot be limited to
        // the HTML.
        .raw = "export const base=\"" ++ sentinel ++ "\";fetch(base+\"api/v1/queue\");",
        .gz = null,
    },
    .{
        .path = "/assets/app.css",
        .content_type = "text/css; charset=utf-8",
        .raw = ":root{--x:1}",
        .gz = null,
    },
    .{
        .path = "/favicon.svg",
        .content_type = "image/svg+xml",
        .raw = "<svg/>",
        .gz = null,
    },
};

fn fakeFind(path: []const u8) ?*const assets.Asset {
    for (&fake_bundle) |*a| {
        if (std.mem.eql(u8, a.path, path)) return a;
    }
    return null;
}

test "nothing the daemon would serve still carries the sentinel" {
    // Walks *every* asset rather than the ones known to carry it today, so
    // a new chunk referencing `import.meta.env.BASE_URL` is covered the
    // moment it exists.
    const gpa = testing.allocator;

    for ([_][]const u8{ "", "/hoardarr", "/deeply/nested/mount" }) |base| {
        var rw: Rewriter = .{ .gpa = gpa, .table = &fake_bundle };
        defer rw.deinit();

        for (&fake_bundle) |*a| {
            const served = if (rw.find(base, a.path)) |e| e.raw else a.raw;
            if (std.mem.indexOf(u8, served, sentinel)) |at| {
                std.debug.print(
                    "\n{s} still carries the base sentinel at byte {d} under base \"{s}\"\n",
                    .{ a.path, at, base },
                );
                return error.SentinelSurvivedIntoServedBytes;
            }
        }
    }
}

test "a rewritten URL names an asset that actually exists" {
    // A substitution that produces a well-formed but wrong path fails the
    // same way no substitution does — a 404 behind a 200, and a blank
    // page — so the result is resolved against the table rather than
    // eyeballed.
    const gpa = testing.allocator;

    for ([_][]const u8{ "", "/hoardarr" }) |base| {
        var rw: Rewriter = .{ .gpa = gpa, .table = &fake_bundle };
        defer rw.deinit();

        const index = rw.find(base, "/index.html") orelse return error.IndexWasNotRewritten;

        var checked: usize = 0;
        var it = std.mem.splitScalar(u8, index.raw, '"');
        while (it.next()) |field| {
            if (field.len == 0 or field[0] != '/') continue;
            if (base.len != 0 and !std.mem.startsWith(u8, field, base)) continue;
            // The server sees the path with its mount prefix stripped.
            const path = field[base.len..];
            if (fakeFind(path) == null) {
                std.debug.print("\nindex references {s}, which is not an asset\n", .{field});
                return error.RewrittenUrlDoesNotResolve;
            }
            checked += 1;
        }
        // The bundle always names at least a script, a stylesheet and an
        // icon; finding none would mean this test checked nothing.
        try testing.expect(checked >= 3);
    }
}

test "a changed base rebuilds rather than serving the previous mount" {
    const gpa = testing.allocator;
    var rw: Rewriter = .{ .gpa = gpa, .table = &fake_bundle };
    defer rw.deinit();

    const first = rw.find("/one", "/index.html") orelse return error.IndexWasNotRewritten;
    try testing.expect(std.mem.indexOf(u8, first.raw, "/one/assets/") != null);

    const second = rw.find("/two", "/index.html") orelse return error.IndexWasNotRewritten;
    try testing.expect(std.mem.indexOf(u8, second.raw, "/two/assets/") != null);
    try testing.expect(std.mem.indexOf(u8, second.raw, "/one/assets/") == null);
}

test "an asset with no sentinel is left to be served from the binary" {
    // The rewriter must not copy what it does not need to change: a font
    // or an image would otherwise be duplicated in memory and, worse,
    // re-gzipped on every base change.
    const gpa = testing.allocator;
    var rw: Rewriter = .{ .gpa = gpa, .table = &fake_bundle };
    defer rw.deinit();

    try testing.expect(rw.find("/hoardarr", "/assets/app.css") == null);
    try testing.expect(rw.find("/hoardarr", "/favicon.svg") == null);
    try testing.expect(rw.find("/hoardarr", "/index.html") != null);
    try testing.expectEqual(@as(usize, 2), rw.entries.items.len);
}

test "the real embedded bundle, when this build has one" {
    // The fake bundle proves the mechanism; only the real one proves it
    // against what Vite actually emitted. Skipped rather than absent so a
    // build with `-Dembed-ui=true` covers the shipping artefact.
    if (!assets.present) return error.SkipZigTest;

    const gpa = testing.allocator;
    var rw: Rewriter = .{ .gpa = gpa };
    defer rw.deinit();

    var rewritten: usize = 0;
    for ([_][]const u8{ "", "/hoardarr" }) |base| {
        for (&assets.assets) |*a| {
            const served = if (rw.find(base, a.path)) |e| blk: {
                rewritten += 1;
                break :blk e.raw;
            } else a.raw;
            if (std.mem.indexOf(u8, served, sentinel) != null) {
                std.debug.print("\n{s} still carries the sentinel under \"{s}\"\n", .{ a.path, base });
                return error.SentinelSurvivedIntoServedBytes;
            }
        }
    }
    // Vite always bakes the sentinel into index.html, so a run that
    // rewrote nothing means the bundle was not built the way this code
    // assumes and the check above proved nothing.
    try testing.expect(rewritten > 0);
}
