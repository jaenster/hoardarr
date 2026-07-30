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
