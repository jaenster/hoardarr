//! Build-time frontend asset embedder.
//!
//! Walks the built React bundle and emits a Zig source file the daemon
//! `@embedFile`s, so the shipped binary needs no filesystem for the UI and
//! the container needs no `/app` directory.
//!
//! The important part is that every asset is **gzipped here, at build
//! time, at level 9**. The daemon then serves the pre-compressed bytes
//! directly with `Content-Encoding: gzip` and never runs a compressor.
//! That is worth doing for two reasons: compressing the same unchanging
//! JS bundle on every page load is pure waste, and level 9 at build time
//! is both smaller and free, whereas a runtime compressor has to trade
//! ratio against latency.
//!
//! Assets whose gzip output isn't actually smaller (already-compressed
//! PNGs, tiny files where the 18-byte gzip envelope dominates) keep only
//! their identity encoding — shipping both would be bytes in the image for
//! nothing.
//!
//! Usage: embed_assets <input-dir> <output-dir>
//!
//! Writes `<output-dir>/assets.zig` plus a `blobs/` directory beside it.
//! The generated file references the blobs with relative `@embedFile`
//! paths, so the whole thing relocates with the build cache.

const std = @import("std");

const Asset = struct {
    /// URL path, always with a leading slash and forward slashes.
    url: []const u8,
    /// Blob filename for the identity encoding.
    raw_blob: []const u8,
    raw_len: usize,
    /// Blob filename for the gzip encoding, when it's worth shipping.
    gz_blob: ?[]const u8,
    gz_len: usize,
    content_type: []const u8,
};

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;

    var args = init.minimal.args.iterate();
    _ = args.next();
    const in_path = args.next() orelse return usage();
    const out_path = args.next() orelse return usage();

    var out_dir = try std.Io.Dir.cwd().createDirPathOpen(io, out_path, .{});
    defer out_dir.close(io);
    var blob_dir = try out_dir.createDirPathOpen(io, "blobs", .{});
    defer blob_dir.close(io);

    var assets: std.ArrayList(Asset) = .empty;
    defer assets.deinit(gpa);

    // An empty or missing input directory is not an error: a developer
    // running `zig build` without having built the frontend should get a
    // working daemon that serves a "run npm build" placeholder, which is
    // what the Go version did too.
    var in_dir = std.Io.Dir.cwd().openDir(io, in_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            try emit(gpa, io, out_dir, assets.items, false);
            return 0;
        },
        else => return err,
    };
    defer in_dir.close(io);

    var walker = try in_dir.walk(gpa);
    defer walker.deinit();

    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;

        const contents = try entry.dir.readFileAlloc(io, entry.basename, gpa, .limited(64 << 20));

        // Normalise to a URL: forward slashes, leading slash. The walker
        // already gives POSIX separators on our targets, but being
        // explicit costs nothing and documents the contract.
        const url = try std.fmt.allocPrint(gpa, "/{s}", .{entry.path});
        for (url) |*ch| {
            if (ch.* == '\\') ch.* = '/';
        }

        const raw_blob = try mangle(gpa, entry.path, "");
        try blob_dir.writeFile(io, .{ .sub_path = raw_blob, .data = contents });

        const gz = try gzip(gpa, contents);
        var gz_blob: ?[]const u8 = null;
        // Only keep gzip when it genuinely wins. A 10% threshold rather
        // than "any smaller at all", because saving 40 bytes on a 4 KB
        // file isn't worth a second copy in the image.
        if (gz.len + gz.len / 10 < contents.len) {
            gz_blob = try mangle(gpa, entry.path, ".gz");
            try blob_dir.writeFile(io, .{ .sub_path = gz_blob.?, .data = gz });
        }

        try assets.append(gpa, .{
            .url = url,
            .raw_blob = raw_blob,
            .raw_len = contents.len,
            .gz_blob = gz_blob,
            .gz_len = gz.len,
            .content_type = contentType(entry.basename),
        });
    }

    // Sort by URL so the generated file is byte-identical across runs.
    // Directory iteration order is not stable, and an unstable generated
    // file busts the build cache on every single build.
    std.mem.sort(Asset, assets.items, {}, struct {
        fn lessThan(_: void, a: Asset, b: Asset) bool {
            return std.mem.order(u8, a.url, b.url) == .lt;
        }
    }.lessThan);

    try emit(gpa, io, out_dir, assets.items, assets.items.len > 0);
    return 0;
}

/// Read a whole file.
///
/// Deliberately not `Dir.readFileAlloc`: that hands its `File.Reader` a
/// zero-length buffer, and `allocRemaining` on an unbuffered reader fails
/// with OutOfMemory on Zig 0.16.0 even for a 512-byte file. Giving the
/// reader a real buffer avoids the whole question.
fn readWholeFile(
    gpa: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    sub_path: []const u8,
) ![]u8 {
    var file = try dir.openFile(io, sub_path, .{});
    defer file.close(io);

    var buf: [64 * 1024]u8 = undefined;
    var reader = file.reader(io, &buf);

    var out: std.Io.Writer.Allocating = .init(gpa);
    errdefer out.deinit();
    _ = try reader.interface.streamRemaining(&out.writer);
    return out.toOwnedSlice();
}

fn usage() u8 {
    std.log.err("usage: embed_assets <input-dir> <output-dir>", .{});
    return 2;
}

/// Turn a relative path into a flat, filesystem-safe blob name. Keeping
/// the original path in the name (with separators replaced) makes the
/// cache directory readable when something goes wrong.
fn mangle(gpa: std.mem.Allocator, path: []const u8, suffix: []const u8) ![]const u8 {
    const name = try std.fmt.allocPrint(gpa, "{s}{s}", .{ path, suffix });
    for (name) |*ch| {
        switch (ch.*) {
            'a'...'z', 'A'...'Z', '0'...'9', '.', '-', '_' => {},
            else => ch.* = '_',
        }
    }
    return name;
}

fn gzip(gpa: std.mem.Allocator, input: []const u8) ![]u8 {
    // Compress.init asserts the output writer has a non-empty buffer, and a
    // default Allocating starts with none, so give it a real capacity up
    // front. The input length is a fine starting guess: gzip output is
    // almost always smaller, so this typically avoids any regrow at all.
    var out: std.Io.Writer.Allocating = try .initCapacity(gpa, @max(input.len, 4096));
    errdefer out.deinit();

    const window = try gpa.alloc(u8, std.compress.flate.max_window_len);
    defer gpa.free(window);

    var comp = try std.compress.flate.Compress.init(&out.writer, window, .gzip, .best);
    try comp.writer.writeAll(input);
    try comp.writer.flush();
    return out.toOwnedSlice();
}

/// Content types for what a Vite bundle actually emits. Unknown
/// extensions get `application/octet-stream` rather than a guess — a
/// wrong `Content-Type` on a script is a silently broken page.
fn contentType(name: []const u8) []const u8 {
    const ext = std.fs.path.extension(name);
    const map = .{
        .{ ".html", "text/html; charset=utf-8" },
        .{ ".js", "text/javascript; charset=utf-8" },
        .{ ".mjs", "text/javascript; charset=utf-8" },
        .{ ".css", "text/css; charset=utf-8" },
        .{ ".json", "application/json; charset=utf-8" },
        .{ ".svg", "image/svg+xml" },
        .{ ".png", "image/png" },
        .{ ".jpg", "image/jpeg" },
        .{ ".jpeg", "image/jpeg" },
        .{ ".gif", "image/gif" },
        .{ ".webp", "image/webp" },
        .{ ".ico", "image/x-icon" },
        .{ ".woff", "font/woff" },
        .{ ".woff2", "font/woff2" },
        .{ ".ttf", "font/ttf" },
        .{ ".map", "application/json; charset=utf-8" },
        .{ ".txt", "text/plain; charset=utf-8" },
        .{ ".webmanifest", "application/manifest+json" },
    };
    inline for (map) |pair| {
        if (std.mem.eql(u8, ext, pair[0])) return pair[1];
    }
    return "application/octet-stream";
}

fn emit(
    gpa: std.mem.Allocator,
    io: std.Io,
    out_dir: std.Io.Dir,
    assets: []const Asset,
    present: bool,
) !void {
    var src: std.Io.Writer.Allocating = .init(gpa);
    defer src.deinit();
    const w = &src.writer;

    try w.writeAll(
        \\//! Generated by tools/embed_assets.zig. Do not edit.
        \\//!
        \\//! Every asset is gzipped at build time at level 9, so the daemon
        \\//! serves pre-compressed bytes and never runs a compressor. `gz` is
        \\//! null where gzip wasn't at least 10% smaller than the original.
        \\
        \\/// True when a frontend bundle was present at build time. When
        \\/// false the server serves a placeholder explaining how to build it.
        \\pub const present: bool =
    );
    try w.print(" {};\n\n", .{present});

    try w.writeAll(
        \\pub const Asset = struct {
        \\    /// Request path, with a leading slash.
        \\    path: []const u8,
        \\    content_type: []const u8,
        \\    /// Identity encoding.
        \\    raw: []const u8,
        \\    /// Pre-gzipped bytes, or null when gzip wasn't worth shipping.
        \\    gz: ?[]const u8,
        \\};
        \\
        \\pub const assets = [_]Asset{
        \\
    );

    for (assets) |a| {
        try w.print("    .{{\n        .path = \"{f}\",\n", .{std.zig.fmtString(a.url)});
        try w.print("        .content_type = \"{f}\",\n", .{std.zig.fmtString(a.content_type)});
        try w.print("        .raw = @embedFile(\"blobs/{f}\"),\n", .{std.zig.fmtString(a.raw_blob)});
        if (a.gz_blob) |g| {
            try w.print("        .gz = @embedFile(\"blobs/{f}\"),\n", .{std.zig.fmtString(g)});
        } else {
            try w.writeAll("        .gz = null,\n");
        }
        try w.writeAll("    },\n");
    }

    try w.writeAll(
        \\};
        \\
        \\/// Linear scan over a handful of entries, resolved at comptime for
        \\/// literal paths. A Vite bundle is a dozen files, so a hash map
        \\/// would cost more in setup than it saves per lookup.
        \\pub fn find(path: []const u8) ?*const Asset {
        \\    for (&assets) |*a| {
        \\        if (@import("std").mem.eql(u8, a.path, path)) return a;
        \\    }
        \\    return null;
        \\}
        \\
    );

    try out_dir.writeFile(io, .{ .sub_path = "assets.zig", .data = src.written() });
}
