const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast });

    const version = b.option([]const u8, "version", "version string") orelse "dev";
    const commit = b.option([]const u8, "commit", "git commit") orelse "unknown";
    const build_date = b.option([]const u8, "build-date", "build timestamp") orelse "unknown";
    const embed_ui = b.option(bool, "embed-ui", "embed frontend/dist into the binary") orelse false;
    // Symbols are ~90% of a ReleaseSmall binary and buy us nothing in a
    // container: a crash there is diagnosed from the structured log, not
    // from a backtrace nobody can symbolise anyway.
    const strip = b.option(bool, "strip", "strip debug symbols") orelse false;

    const build_info = b.addOptions();
    build_info.addOption([]const u8, "version", version);
    build_info.addOption([]const u8, "commit", commit);
    build_info.addOption([]const u8, "build_date", build_date);
    build_info.addOption(bool, "embed_ui", embed_ui);

    const hoardarr = b.addModule("hoardarr", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    hoardarr.addOptions("build_info", build_info);
    addSqlite(b, hoardarr, target, optimize);
    addAssets(b, hoardarr, embed_ui);

    const exe = b.addExecutable(.{
        .name = "hoardarr",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "hoardarr", .module = hoardarr }},
            .strip = strip,
        }),
    });
    exe.root_module.addOptions("build_info", build_info);
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    b.step("run", "Run hoardarr").dependOn(&run.step);

    // ---- tests ----
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    tests.root_module.addOptions("build_info", build_info);
    addSqlite(b, tests.root_module, target, optimize);
    addAssets(b, tests.root_module, embed_ui);
    const run_tests = b.addRunArtifact(tests);
    b.step("test", "Run unit tests").dependOn(&run_tests.step);

    // ---- cross-target compile check ----
    //
    // The Linux backends (epoll, eventfd, raw syscalls) can't run on a
    // macOS dev box, and a backend that only ever gets compiled in CI is
    // a backend that's broken. This step type-checks every target we
    // ship without needing to execute anything.
    const check = b.step("check", "Type-check every shipping target");
    for ([_]std.Target.Query{
        .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl },
        .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .musl },
    }) |query| {
        const resolved = b.resolveTargetQuery(query);
        const check_mod = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = resolved,
            .optimize = .ReleaseFast,
        });
        const check_hoardarr = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = resolved,
            .optimize = .ReleaseFast,
        });
        check_hoardarr.addOptions("build_info", build_info);
        addSqlite(b, check_hoardarr, resolved, .ReleaseFast);
        addAssets(b, check_hoardarr, embed_ui);
        check_mod.addOptions("build_info", build_info);
        check_mod.addImport("hoardarr", check_hoardarr);
        const obj = b.addObject(.{
            .name = b.fmt("check-{s}-{s}", .{ @tagName(query.cpu_arch.?), @tagName(query.abi.?) }),
            .root_module = check_mod,
        });
        check.dependOn(&obj.step);
    }

    // ---- benchmarks ----
    //
    // The library gets its own ReleaseFast module rather than reusing
    // `hoardarr` above: that one is built at the user-selected optimize
    // level, and linking a Debug library into a ReleaseFast harness
    // measures the bounds checks instead of the code.
    const hoardarr_fast = b.addModule("hoardarr_fast", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hoardarr_fast.addOptions("build_info", build_info);
    addSqlite(b, hoardarr_fast, target, .ReleaseFast);
    addAssets(b, hoardarr_fast, embed_ui);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/main.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{.{ .name = "hoardarr", .module = hoardarr_fast }},
        }),
    });
    b.installArtifact(bench);
    const run_bench = b.addRunArtifact(bench);
    if (b.args) |args| run_bench.addArgs(args);
    b.step("bench", "Run microbenchmarks").dependOn(&run_bench.step);
}

/// Compile the vendored SQLite amalgamation and attach it to `mod`.
///
/// SQLite stays as the persistence adapter rather than being rewritten.
/// It is C, not Go, so it satisfies the "no Go" requirement, and Zig
/// compiles C natively — there is no FFI boundary and no cgo-equivalent
/// call overhead. Writing a storage engine from scratch would be weeks of
/// work whose failure mode is losing somebody's download history, which
/// is a bad trade against a database that ships on every phone on earth.
/// `store/` keeps it behind the same port the Go version used, so a
/// future Postgres adapter remains a peer rather than a rewrite.
fn addSqlite(
    b: *std.Build,
    mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const lib = b.addLibrary(.{
        .name = "sqlite3",
        .linkage = .static,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
        }),
    });
    lib.root_module.addCSourceFile(.{
        .file = b.path("c/sqlite3/sqlite3.c"),
        .flags = &sqlite_cflags,
    });
    lib.root_module.addIncludePath(b.path("c/sqlite3"));
    // The amalgamation needs a libc. This is the one place we do: the
    // Zig code itself goes straight to syscalls, but SQLite's VFS is
    // written against POSIX stdio and pthreads.
    lib.root_module.link_libc = true;

    mod.linkLibrary(lib);
    mod.addIncludePath(b.path("c/sqlite3"));
}

/// Build flags, chosen for footprint and for the guarantees the store
/// layer relies on.
const sqlite_cflags = [_][]const u8{
    // We use one connection per thread with an explicit mutex around
    // writes, never one connection shared across threads, so SQLite's own
    // per-connection mutexes are pure overhead.
    "-DSQLITE_THREADSAFE=2",
    // WAL is how we get concurrent readers alongside a writer.
    "-DSQLITE_ENABLE_JSON1",
    "-DSQLITE_DQS=0", // reject double-quoted string literals; they hide typos
    "-DSQLITE_DEFAULT_MEMSTATUS=0",
    "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
    "-DSQLITE_LIKE_DOESNT_MATCH_BLOBS",
    "-DSQLITE_MAX_EXPR_DEPTH=0",
    "-DSQLITE_OMIT_DEPRECATED",
    "-DSQLITE_OMIT_SHARED_CACHE",
    "-DSQLITE_OMIT_PROGRESS_CALLBACK",
    "-DSQLITE_USE_ALLOCA",
    // Everything below is a feature we never call, and each one is bytes
    // in the image and attack surface in a process that parses files off
    // the internet.
    "-DSQLITE_OMIT_LOAD_EXTENSION",
    "-DSQLITE_OMIT_AUTHORIZATION",
    "-DSQLITE_OMIT_COMPLETE",
    "-DSQLITE_OMIT_TCL_VARIABLE",
    "-DSQLITE_OMIT_UTF16",
    "-DSQLITE_UNTESTABLE",
};

/// Generate the embedded-frontend module and attach it to `mod` as
/// `assets`. Runs `tools/embed_assets.zig` over `frontend/dist`.
///
/// The generator runs at build time so gzip happens once at level 9 rather
/// than per request at whatever level fits a latency budget. It also means
/// the container needs no directory for the UI.
fn addAssets(
    b: *std.Build,
    mod: *std.Build.Module,
    embed_ui: bool,
) void {
    // The generator is a build-time tool, so it targets the host and is
    // built for speed of the build rather than of the product.
    const tool = b.addExecutable(.{
        .name = "embed_assets",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/embed_assets.zig"),
            .target = b.graph.host,
            .optimize = .ReleaseFast,
        }),
    });

    const run = b.addRunArtifact(tool);
    // Without -Dembed-ui the generator is pointed at a path that doesn't
    // exist, which it treats as "no bundle" and emits a stub for. That
    // keeps one code path in the server instead of a compile-time fork.
    run.addArg(if (embed_ui) "frontend/dist" else "frontend/dist-absent");
    const out_dir = run.addOutputDirectoryArg("assets");

    mod.addAnonymousImport("assets", .{
        .root_source_file = out_dir.path(b, "assets.zig"),
    });
}
