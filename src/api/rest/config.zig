//! `/api/v1/config/*` — paths, the General panel, and the bandwidth cap.
//!
//! Three tiers of mutability, and the endpoints are shaped by which tier
//! a setting is in:
//!
//!   * **Paths** are read-only at runtime. Changing `incomplete_dir`
//!     while jobs hold open files under it is not a settings change, it
//!     is a migration; it needs `config.toml` and a restart.
//!   * **General** is a mix. `url_base` and the orchestrator knobs are
//!     runtime-mutable and persisted; `listen` and `log_level` are
//!     reported so the UI can show them, and are not settable here.
//!   * **Bandwidth** is fully runtime: the token bucket reconfigures in
//!     place, mid-download.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const json = @import("json.zig");
const respond = @import("respond.zig");
const Api = @import("api.zig").Api;

const Error = respond.Error;

/// GET /api/v1/config/paths
pub fn paths(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cfg = api.config orelse return respond.unavailable(ctx, "runtime config");
    const arena = api.beginRequest();

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.strField("data_dir", cfg.data_dir);
    try w.strField("incomplete_dir", cfg.incomplete_dir);
    try w.strField("complete_dir", cfg.complete_dir);
    try w.boolField("runtime_mutable", false);
    try w.boolField("requires_restart", true);
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// GET /api/v1/config/general
///
/// The mutable fields are read through the port on every request rather
/// than from a startup snapshot, so the panel reflects an edit made in
/// another tab — and so the API key shown is the live one after a
/// rotation.
pub fn getGeneral(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cfg = api.config orelse return respond.unavailable(ctx, "runtime config");
    const arena = api.beginRequest();

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.strField("listen", cfg.listen);
    try w.strField("api_key", cfg.apiKey());
    try w.strField("log_level", cfg.log_level);
    try w.strField("sab_base", cfg.sab_base);
    try w.strField("url_base", cfg.urlBase());
    try w.intField("max_concurrent_jobs", cfg.maxConcurrentJobs());
    try w.floatField("fail_hopeless_ratio", cfg.failHopelessRatio());
    try w.boolField("defer_recovery_vols", cfg.deferRecoveryVols());
    try w.boolField("delete_samples", cfg.deleteSamples());
    try w.boolField("collapse_single_folder", cfg.collapseSingleFolder());
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// PUT /api/v1/config/general
///
/// Every field is optional and applied only when present. Each setter
/// validates and persists; the first rejection stops the rest, so a
/// request is not half-applied past the field that failed. (Fields
/// before it *are* applied — that is what the Go handler did, and making
/// it atomic would mean a transaction across a config file and a live
/// token bucket for no operator-visible benefit.)
pub fn putGeneral(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const cfg = api.config orelse return respond.unavailable(ctx, "runtime config");
    const writer = cfg.writer orelse return respond.fail(ctx, 503, "runtime config is read-only");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    if (body.has("url_base")) {
        const v = body.string("url_base") orelse
            return respond.fail(ctx, 400, "url_base must be a string");
        writer.setUrlBaseFn(writer.ctx, v) catch
            return respond.fail(ctx, 400, "url_base must be empty or start with / and not end with /");
    }
    if (body.has("max_concurrent_jobs")) {
        const v = body.int("max_concurrent_jobs") orelse
            return respond.fail(ctx, 400, "max_concurrent_jobs must be a number");
        writer.setMaxConcurrentJobsFn(writer.ctx, saturate(v)) catch
            return respond.fail(ctx, 400, "max_concurrent_jobs is out of range");
    }
    if (body.has("fail_hopeless_ratio")) {
        const v = body.float("fail_hopeless_ratio") orelse
            return respond.fail(ctx, 400, "fail_hopeless_ratio must be a number");
        writer.setFailHopelessRatioFn(writer.ctx, v) catch
            return respond.fail(ctx, 400, "fail_hopeless_ratio must be between 0 and 1");
    }
    if (body.has("defer_recovery_vols")) {
        const v = body.boolean("defer_recovery_vols") orelse
            return respond.fail(ctx, 400, "defer_recovery_vols must be a boolean");
        writer.setDeferRecoveryVolsFn(writer.ctx, v) catch
            return respond.fail(ctx, 400, "defer_recovery_vols could not be applied");
    }
    if (body.has("delete_samples")) {
        const v = body.boolean("delete_samples") orelse
            return respond.fail(ctx, 400, "delete_samples must be a boolean");
        writer.setDeleteSamplesFn(writer.ctx, v) catch
            return respond.fail(ctx, 400, "delete_samples could not be applied");
    }
    if (body.has("collapse_single_folder")) {
        const v = body.boolean("collapse_single_folder") orelse
            return respond.fail(ctx, 400, "collapse_single_folder must be a boolean");
        writer.setCollapseSingleFolderFn(writer.ctx, v) catch
            return respond.fail(ctx, 400, "collapse_single_folder could not be applied");
    }
    return respond.noContent(ctx);
}

/// GET /api/v1/config/bandwidth
pub fn getBandwidth(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const bw = api.bandwidth orelse return respond.unavailable(ctx, "bandwidth control");
    const arena = api.beginRequest();

    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.intField("global_bytes_per_sec", bw.globalCap());
    try w.endObject();
    return respond.ok(ctx, &w);
}

/// PUT /api/v1/config/bandwidth — `{"global_bytes_per_sec": n}`, where
/// 0 means uncapped.
pub fn setBandwidth(ctx: *http.Ctx) Error!void {
    const api = ctx.app(Api);
    const bw = api.bandwidth orelse return respond.unavailable(ctx, "bandwidth control");
    const arena = api.beginRequest();
    const body = try respond.bodyObject(ctx, arena) orelse return;

    const v = body.int("global_bytes_per_sec") orelse
        return respond.fail(ctx, 400, "global_bytes_per_sec must be a number");
    if (v < 0) return respond.fail(ctx, 400, "global_bytes_per_sec must be >= 0");

    bw.setGlobalCap(v);
    var w = json.Writer.init(arena);
    try w.beginObject();
    try w.intField("global_bytes_per_sec", v);
    try w.endObject();
    return respond.ok(ctx, &w);
}

fn saturate(v: i64) i32 {
    return @intCast(std.math.clamp(v, std.math.minInt(i32), std.math.maxInt(i32)));
}
