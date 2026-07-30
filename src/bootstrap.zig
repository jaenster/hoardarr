//! Composition root.
//!
//! The only place that knows which adapter satisfies which port. Every
//! layer below takes its dependencies as parameters, which is what lets
//! them be tested without a database, a socket, or a clock; this file is
//! where the real ones get chosen and wired.
//!
//! ## Startup order, and why it is this order
//!
//! 1. **Drop privileges.** Before anything opens a file or a socket, so
//!    nothing is created owned by root that the runtime user then can't
//!    write.
//! 2. **Read config.** Needs the data directory to exist, so that comes
//!    first. Config decides the log level, so it precedes logging.
//! 3. **Start logging.** Everything after this point can report failure
//!    properly instead of writing to stderr and hoping.
//! 4. **Open and migrate the database.** A failed migration must stop
//!    start-up rather than leave the daemon running against a schema it
//!    doesn't understand.
//! 5. **Build the object graph** — stores, services, ports, API. Nothing
//!    here touches the network, so a failure is still a clean exit.
//! 6. **Install signal handling** before the listener, so a `SIGTERM`
//!    arriving during start-up is still handled cleanly.
//! 7. **Listen**, then run the loop.
//!
//! Shutdown is the reverse, and is why signals are a reactor source
//! rather than a handler: the teardown runs with the full runtime
//! available, on the loop thread, with nothing else in flight.
//!
//! ## What a missing dependency means
//!
//! Two different things, deliberately distinguished:
//!
//!   * A **broken** dependency — an unreadable config, a failed
//!     migration, a port already bound — stops start-up with a message on
//!     stderr. Starting half-working is worse than not starting.
//!   * A **missing optional subsystem** — no Usenet servers configured
//!     yet, no frontend bundle in this build — is a normal first run. The
//!     daemon starts and serves the UI so the operator can fix it.
//!
//! ## The one pointer the HTTP layer gets
//!
//! `Server.app_ctx` is a single `*anyopaque`, and the REST handlers cast
//! it to `Api`. The SAB handler needs its own struct, so rather than give
//! the server a second context, `app_ctx` points at `App.api` and the SAB
//! glue walks back with `@fieldParentPtr`. That keeps the REST layer's
//! `ctx.app(Api)` exactly as its own tests exercise it.

const std = @import("std");
const build_info = @import("build_info");
const assets = @import("assets");

const sys = @import("posix/sys.zig");
const reactor = @import("posix/reactor.zig");
const signals = @import("posix/signals.zig");
const log = @import("core/log.zig");
const logring = @import("core/logring.zig");
const config = @import("core/config.zig");
const http = @import("net/http/server.zig");
const response = @import("net/http/response.zig");
const sqlite = @import("store/sqlite.zig");
const migrate = @import("store/migrate.zig");
const outbox = @import("store/outbox.zig");
const repo_server = @import("store/repo_server.zig");
const repo_speed_history = @import("store/repo_speed_history.zig");

const devents = @import("domain/download/events.zig");
const dauth_domain = @import("domain/auth.zig");
const dserver = @import("domain/server.zig");
const dverify = @import("domain/verify.zig");
const drepair = @import("domain/repair.zig");
const dextract = @import("domain/extract.zig");
const ddeliver = @import("domain/deliver.zig");

const app_ports = @import("app/ports.zig");
const queue_svc = @import("app/download/queue.zig");
const add_job_svc = @import("app/download/add_job.zig");
const bandwidth = @import("app/download/bandwidth.zig");
const byte_accounter = @import("app/download/byte_accounter.zig");
const dl_service = @import("app/download/service.zig");
const throughput_mod = @import("app/system/throughput.zig");
const system_svc = @import("app/system/service.zig");
const command_svc = @import("app/command/service.zig");
const auth_svc = @import("app/auth.zig");
const verify_app = @import("app/verify.zig");
const repair_app = @import("app/repair.zig");
const extract_app = @import("app/extract.zig");
const deliver_app = @import("app/deliver/service.zig");
const recovery_app = @import("app/recovery.zig");
const dns = @import("net/dns.zig");

const sse = @import("api/sse.zig");
const metrics = @import("api/metrics.zig");
const rest_handlers = @import("api/rest/handlers.zig");
const rest_api = @import("api/rest/api.zig");
const sab_handler = @import("api/sab/handler.zig");

const infra = @import("bootstrap/infra.zig");
const stores = @import("bootstrap/stores.zig");
const settings = @import("bootstrap/settings.zig");
const rest_ports = @import("bootstrap/rest.zig");
const sab_ports = @import("bootstrap/sab.zig");
const files = @import("bootstrap/files.zig");
const pipeline = @import("bootstrap/pipeline.zig");
const offload_mod = @import("bootstrap/offload.zig");
const runtime_mod = @import("bootstrap/runtime.zig");

const Allocator = std.mem.Allocator;
const Api = rest_api.Api;

/// The toolchain this binary was built with. Reported on the System page
/// where Go used to report `runtime.Version()`.
pub const zig_version = @import("builtin").zig_version_string;

pub const Error = error{
    ConfigInvalid,
    DatabaseUnavailable,
    ListenFailed,
    PrivilegeDropFailed,
} || Allocator.Error || sys.Error;

/// How often the SSE hubs emit a comment frame, so a proxy in the middle
/// does not reap an idle stream.
const sse_heartbeat_ns: u64 = 15 * std.time.ns_per_s;

/// How often the periodic work runs: the throughput history flush, the
/// retention purge, and the queue snapshot pushed to `/api/v1/events`.
const tick_interval_ns: u64 = std.time.ns_per_s;

/// Everything the daemon owns, in one place so shutdown is a single
/// `deinit` in a defined order rather than scattered defers.
///
/// The field order is the dependency order: infrastructure, then stores,
/// then services, then the ports over them, then the API. Reading it top
/// to bottom is reading the graph.
pub const App = struct {
    gpa: Allocator,
    loop: reactor.Loop = undefined,
    cfg: config.Loaded = undefined,
    db: *sqlite.Conn = undefined,
    bus: *outbox.Bus = undefined,
    server: http.Server = undefined,
    sigs: signals.Signals = undefined,
    /// Whether `sigs` was initialised. A test boots the graph without
    /// signal handling, and tearing down an uninitialised `Signals` would
    /// hand the reactor a garbage registration slot.
    signals_installed: bool = false,

    // -- infrastructure ------------------------------------------------

    fs: infra.RealFs = undefined,
    txm: infra.TxManager = undefined,
    download_publisher: infra.Publisher(devents.Event, "job_id") = undefined,
    runtime: settings.Runtime = undefined,
    categories: infra.CategoryLookup = undefined,

    /// Turns a provider's hostname into an address. Callback-based on the
    /// reactor, driven from a job's fiber.
    resolver: dns.Resolver = undefined,
    resolver_ready: bool = false,

    /// The four post-download contexts publish on their own event
    /// unions, so each needs its own publisher. `aggregate_key` is null
    /// for all four: their payloads already carry `job_id`, which is what
    /// the per-job timeline and the pipeline's routing both key on.
    verify_publisher: infra.Publisher(dverify.Event, null) = undefined,
    repair_publisher: infra.Publisher(drepair.Event, null) = undefined,
    extract_publisher: infra.Publisher(dextract.Event, null) = undefined,
    deliver_publisher: infra.Publisher(ddeliver.Event, null) = undefined,

    log_ring: logring.Ring = undefined,
    registry: metrics.Registry = undefined,
    events_hub: sse.Hub = undefined,
    logs_hub: sse.Hub = undefined,
    heartbeat: reactor.Timer = .{ .callback = onHeartbeat },
    ticker: reactor.Timer = .{ .callback = onTick },

    throughput: throughput_mod.Throughput = .{},
    limiter: bandwidth.Limiter = undefined,

    // -- stores --------------------------------------------------------

    job_store: stores.JobStore = undefined,
    user_store: stores.UserStore = undefined,
    session_store: stores.SessionStore = undefined,
    command_store: stores.CommandStore = undefined,
    verify_store: pipeline.VerifyStore = undefined,
    repair_store: pipeline.RepairStore = undefined,
    extract_store: pipeline.ExtractStore = undefined,
    deliver_store: pipeline.DeliverStore = undefined,

    // -- services ------------------------------------------------------

    queue_service: queue_svc.Service = undefined,
    add_job_service: add_job_svc.Service = undefined,
    auth_service: auth_svc.Service = undefined,
    system_service: system_svc.Service = undefined,
    command_service: command_svc.Service = undefined,
    pool_stats: PoolStats = undefined,
    speed_history: SpeedHistory = undefined,

    // -- the download engine and the pipeline over it ------------------

    verifier: pipeline.Verifier = undefined,
    repairer: pipeline.Repairer = undefined,
    extractor: pipeline.Extractor = undefined,

    verify_service: verify_app.Service = undefined,
    repair_service: repair_app.Service = undefined,
    extract_service: extract_app.Service = undefined,
    deliver_service: deliver_app.Service = undefined,
    /// The startup sweep for jobs whose trigger event was consumed by a
    /// process that then died. See `app/recovery.zig`.
    reconciler: recovery_app.Reconciler = undefined,

    accounter: byte_accounter.Accounter = undefined,
    server_bytes: ServerBytes = undefined,
    byte_flusher: byte_accounter.Flusher = undefined,

    scheduler: dl_service.Service = undefined,
    engine: runtime_mod.Runtime = undefined,
    engine_ready: bool = false,
    probe: runtime_mod.Probe = undefined,
    notifier: pipeline.Notifier = undefined,
    notifier_ready: bool = false,
    /// Runs the post-download stages on a fiber with their CPU on a
    /// worker pool. See `bootstrap/offload.zig`.
    stages: offload_mod.Stages = undefined,
    stages_ready: bool = false,

    // -- REST ports ----------------------------------------------------

    p_queue: rest_ports.Queue = undefined,
    p_events: rest_ports.Events = undefined,
    p_servers: rest_ports.Servers = undefined,
    p_categories: rest_ports.Categories = undefined,
    p_system: rest_ports.System = undefined,
    p_schedule: rest_ports.Schedule = undefined,
    p_commands: rest_ports.Commands = undefined,
    p_subscriptions: rest_ports.Subscriptions = undefined,
    p_config: rest_ports.RuntimeConfig = undefined,
    p_bandwidth: rest_ports.Bandwidth = undefined,
    p_auth: rest_ports.Auth = undefined,
    p_backups: files.Backups = undefined,
    p_log_files: files.LogFiles = undefined,

    // -- SAB ports -----------------------------------------------------

    s_queue: sab_ports.Queue = undefined,
    s_categories: sab_ports.Categories = undefined,
    s_add_job: sab_ports.AddJob = undefined,
    s_api_key: sab_ports.ApiKey = undefined,
    s_throughput: sab_ports.Throughput = undefined,

    // -- the two things the HTTP layer talks to ------------------------

    /// `app_ctx` points here. See the module comment.
    api: Api = undefined,
    sab: sab_handler.Handler = undefined,

    /// Owned copies of the two derived paths, because the ports hand them
    /// out as borrowed slices for the process lifetime.
    backups_dir: []u8 = &.{},
    logs_dir: []u8 = &.{},
    sab_base: []u8 = &.{},

    started: bool = false,

    pub fn deinit(self: *App) void {
        if (!self.started) return;
        self.started = false;

        // Order matters: stop accepting first so nothing new arrives,
        // then drop live connections and streams, then tear down the
        // graph, and only then close the database. Closing it earlier
        // would leave an in-flight handler dereferencing it.
        self.server.deinit();
        self.events_hub.closeAll(.shutdown);
        self.logs_hub.closeAll(.shutdown);

        // The download engine comes down before anything it touches.
        //
        // Its fibers hold a loaded job aggregate and, mid-fetch, a
        // checked-out provider connection; `Runtime.deinit` cancels each
        // one so it unwinds through its own `defer`s and gives both back
        // — which needs the database, the bus and the loop still alive.
        // Freeing a parked fiber instead, or closing the sockets first,
        // is a leak and a use-after-free respectively.
        // Before everything else in the graph: a stage fiber may be
        // parked on a worker that is writing into its stack, so the pool
        // has to be joined before anything that stack points at goes
        // away. `Stages.deinit` owns that ordering.
        if (self.stages_ready) {
            self.stages.deinit();
            self.stages_ready = false;
        }

        // Before the engine, because a delivery in flight is parked on a
        // socket the loop owns and holds a subscription list read through
        // the database — both of which are still alive at this point, and
        // neither of which would be a step later.
        if (self.notifier_ready) {
            self.notifier.deinit();
            self.notifier_ready = false;
        }
        if (self.engine_ready) {
            self.engine.deinit();
            self.engine_ready = false;
        }
        // After the engine, because a fiber unwinding mid-resolve is a
        // query this has to answer.
        if (self.resolver_ready) {
            self.resolver.deinit();
            self.resolver_ready = false;
        }
        self.scheduler.deinit();

        if (self.heartbeat.isArmed()) self.loop.cancelTimer(&self.heartbeat);
        if (self.ticker.isArmed()) self.loop.cancelTimer(&self.ticker);
        if (self.signals_installed) {
            self.loop.remove(&self.sigs.source);
            self.sigs.deinit();
            self.signals_installed = false;
        }

        // The bus still owns the pruner thread, which holds its own
        // connection to the same database file. Joining it before the
        // database closes is the whole reason this is not a `defer`.
        // The dispatchers are reactor sources, and the engine's teardown
        // above has already dropped them.
        self.bus.deinit();

        self.api.deinit();
        self.verify_store.deinit();
        self.repair_store.deinit();
        self.extract_store.deinit();
        self.deliver_store.deinit();
        self.job_store.deinit();
        self.command_service.deinit();
        self.accounter.deinit();
        self.limiter.deinit();
        self.events_hub.deinit();
        self.logs_hub.deinit();

        metrics.uninstall();
        self.registry.deinit();
        log.default.setMirror(null);
        self.log_ring.deinit();

        self.gpa.free(self.backups_dir);
        self.gpa.free(self.logs_dir);
        self.gpa.free(self.sab_base);

        self.db.close();
        self.loop.deinit();
        self.cfg.deinit();
    }

    /// Builds the whole object graph over an already-open, already-migrated
    /// database.
    ///
    /// Split out of `run` so the integration test can boot the same graph
    /// on an ephemeral port without the process-level parts — privilege
    /// dropping, signal handling and the blocking loop.
    pub fn wire(self: *App) !void {
        const cfg = self.cfg.config;
        const gpa = self.gpa;

        // ---- observability ----
        self.log_ring = try logring.Ring.init(gpa, logring.Ring.default_capacity);
        errdefer self.log_ring.deinit();
        log.default.setMirror(self.log_ring.mirror());

        self.registry = metrics.Registry.init(gpa);
        errdefer self.registry.deinit();
        metrics.install(&self.registry);
        metrics.setBuildInfo(build_info.version, build_info.commit, zig_version);

        self.events_hub = sse.Hub.init(gpa);
        errdefer self.events_hub.deinit();
        self.logs_hub = sse.Hub.init(gpa);
        errdefer self.logs_hub.deinit();

        // ---- infrastructure ----
        self.fs = .{ .gpa = gpa };
        self.txm = .{ .conn = self.db };

        var db_path_buf: [sys.path_max]u8 = undefined;
        const db_path = sys.joinZ(&db_path_buf, cfg.server.data_dir, "hoardarr.db") catch
            return error.ConfigInvalid;
        self.bus = try outbox.Bus.init(gpa, db_path, .{});
        errdefer self.bus.deinit();

        self.download_publisher = .{ .gpa = gpa, .bus = self.bus, .conn = self.db };

        self.sab_base = try std.fmt.allocPrint(gpa, "http://localhost{s}{s}", .{
            cfg.server.listen,
            sab_handler.mount_path,
        });
        errdefer gpa.free(self.sab_base);

        self.runtime = .{
            .gpa = gpa,
            .conn = self.db,
            .file = cfg,
            .sab_base = self.sab_base,
        };
        self.runtime.refresh();
        try self.persistFirstRunApiKey();

        self.limiter = bandwidth.Limiter.init(gpa, self.runtime.bandwidthGlobal(), infra.nowMillis());
        errdefer self.limiter.deinit();

        // ---- stores ----
        self.job_store = .{ .gpa = gpa, .conn = self.db };
        self.user_store = .{ .gpa = gpa, .conn = self.db };
        self.session_store = .{ .conn = self.db };
        self.command_store = .{ .gpa = gpa, .conn = self.db };

        // ---- services ----
        const clock = infra.SystemClock.clock();

        self.queue_service = .{
            .gpa = gpa,
            .store = self.job_store.port(),
            .sink = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .clock = clock,
            .fs = self.fs.filesystem(),
            .incomplete_dir = cfg.paths.incomplete_dir,
        };

        self.add_job_service = .{
            .gpa = gpa,
            .store = self.job_store.port(),
            .sink = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .clock = clock,
            .defer_recovery_vols = self.runtime.toggle(settings.keys.defer_recovery_vols),
        };

        self.auth_service = .{
            .gpa = gpa,
            .users = self.user_store.port(),
            .sessions = self.session_store.port(),
            // Auth events are published through their own sink so the
            // topic namespace stays the auth context's. It shares the
            // bus and the connection, so a login still commits with its
            // session row.
            .sink = authSink(self),
            .txm = self.txm.manager(),
            .clock = clock,
        };

        self.pool_stats = .{ .gpa = gpa, .conn = self.db, .engine = &self.engine };
        self.speed_history = .{ .conn = self.db };

        self.system_service = .{
            .gpa = gpa,
            .jobs = self.job_store.port(),
            .pools = self.pool_stats.port(),
            .clock = clock,
            .throughput = &self.throughput,
            .version = build_info.version,
            .commit = build_info.commit,
            .build_date = build_info.build_date,
            .migration_version = migrate.currentVersion(self.db) catch 0,
            .started_at = infra.nowMillis(),
            .history = self.speed_history.port(),
        };
        self.system_service.scheduleHistory(infra.nowMillis());

        self.command_service = .{
            .gpa = gpa,
            .store = self.command_store.port(),
            .clock = clock,
        };
        errdefer self.command_service.deinit();

        // ---- the download engine ----
        //
        // Everything from here to the end of this block is what turns a
        // queued job into bytes on disk. `bootstrap/runtime.zig` owns the
        // two bridges that make it possible — a fiber per job so the
        // orchestrator's synchronous fetch can run on a callback
        // transport, and the outbox dispatchers on the reactor instead of
        // on threads. Read that file's header before changing any of it.

        self.resolver.initFromSystem(gpa, &self.loop);
        self.resolver_ready = true;
        errdefer {
            self.resolver.deinit();
            self.resolver_ready = false;
        }

        self.categories = .{ .gpa = gpa, .conn = self.db };

        self.verify_publisher = .{ .gpa = gpa, .bus = self.bus, .conn = self.db };
        self.repair_publisher = .{ .gpa = gpa, .bus = self.bus, .conn = self.db };
        self.extract_publisher = .{ .gpa = gpa, .bus = self.bus, .conn = self.db };
        self.deliver_publisher = .{ .gpa = gpa, .bus = self.bus, .conn = self.db };

        self.verify_store = .{ .gpa = gpa, .conn = self.db };
        self.repair_store = .{ .gpa = gpa, .conn = self.db };
        self.extract_store = .{ .gpa = gpa, .conn = self.db };
        self.deliver_store = .{ .gpa = gpa, .conn = self.db };

        self.stages = .{
            .gpa = gpa,
            .loop = &self.loop,
            .ctx = @ptrCast(self),
            .bodyFn = &runStageBody,
        };
        self.stages_ready = true;

        self.verifier = .{ .gpa = gpa, .logger = &log.default, .stages = &self.stages };
        self.repairer = .{ .logger = &log.default, .stages = &self.stages };
        self.extractor = .{ .gpa = gpa, .logger = &log.default, .stages = &self.stages };

        self.verify_service = .{
            .gpa = gpa,
            .jobs = self.job_store.port(),
            .store = self.verify_store.port(),
            .verifier = self.verifier.port(),
            .sink = self.verify_publisher.sink(),
            .downloads = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .fs = self.fs.filesystem(),
            .clock = clock,
            .incomplete_dir = cfg.paths.incomplete_dir,
        };

        self.repair_service = .{
            .gpa = gpa,
            .jobs = self.job_store.port(),
            .store = self.repair_store.port(),
            .repairer = self.repairer.port(),
            .sink = self.repair_publisher.sink(),
            .downloads = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .fs = self.fs.filesystem(),
            .clock = clock,
            .incomplete_dir = cfg.paths.incomplete_dir,
        };

        self.extract_service = .{
            .gpa = gpa,
            .jobs = self.job_store.port(),
            .store = self.extract_store.port(),
            .extractor = self.extractor.port(),
            .categories = self.categories.categories(),
            .sink = self.extract_publisher.sink(),
            .downloads = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .fs = self.fs.filesystem(),
            .clock = clock,
            .incomplete_dir = cfg.paths.incomplete_dir,
            .complete_dir = cfg.paths.complete_dir,
        };

        self.deliver_service = .{
            .gpa = gpa,
            .jobs = self.job_store.port(),
            .store = self.deliver_store.port(),
            .categories = self.categories.categories(),
            .sink = self.deliver_publisher.sink(),
            .downloads = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .fs = self.fs.filesystem(),
            .clock = clock,
            .incomplete_dir = cfg.paths.incomplete_dir,
            .complete_dir = cfg.paths.complete_dir,
            .delete_samples = self.runtime.toggle(settings.keys.delete_samples),
            .collapse_single_folder = self.runtime.toggle(settings.keys.collapse_single_folder),
        };

        self.reconciler = .{
            .gpa = gpa,
            .jobs = self.job_store.port(),
            .verify_sets = self.verify_store.port(),
            .repairs = self.repair_store.port(),
            .queue = stageQueue(self),
        };

        // Per-server byte totals are staged in memory and written on the
        // housekeeping tick, not per article: `used_bytes = used_bytes + ?`
        // once per segment was measured as pure SQLite overhead on a
        // download that already writes a segment row per batch. The
        // engine also flushes when a runner exits, so a job's
        // consumption is durable the moment it stops accruing — see
        // `bootstrap/runtime.zig`'s `onReap`.
        self.accounter = byte_accounter.Accounter.init(gpa);
        errdefer self.accounter.deinit();
        self.server_bytes = .{ .gpa = gpa, .conn = self.db };
        self.byte_flusher = .{
            .gpa = gpa,
            .accounter = &self.accounter,
            .store = self.server_bytes.port(),
        };

        self.scheduler = .{
            .gpa = gpa,
            .store = self.job_store.port(),
            .queue = &self.queue_service,
            .clock = clock,
            .concurrency_cap = self.runtime.knob(settings.keys.max_concurrent_jobs),
        };
        errdefer self.scheduler.deinit();

        self.engine.init(.{
            .gpa = gpa,
            .loop = &self.loop,
            .db = self.db,
            .bus = self.bus,
            .job_store = self.job_store.port(),
            .sink = self.download_publisher.sink(),
            .txm = self.txm.manager(),
            .fs = self.fs.filesystem(),
            .clock = clock,
            .incomplete_dir = cfg.paths.incomplete_dir,
            .scheduler = &self.scheduler,
            .resolver = &self.resolver,
            .limiter = &self.limiter,
            .accounter = &self.accounter,
            .byte_flusher = &self.byte_flusher,
        });
        self.engine_ready = true;
        errdefer {
            self.engine.deinit();
            self.engine_ready = false;
        }

        // The probe shares the engine's trust anchors rather than
        // reloading them: a `CaStore` is memory with no loop affinity,
        // and the engine is what owns its lifetime.
        self.probe = .{ .gpa = gpa, .ca_roots = &self.engine.ca_roots };

        // Same anchors again for the notifier: an `https://` webhook is
        // verified against exactly what a TLS provider is.
        self.notifier = .{
            .gpa = gpa,
            .loop = &self.loop,
            .db = self.db,
            .resolver = &self.resolver,
            .ca = &self.engine.ca_roots.store,
        };
        self.notifier_ready = true;

        // ---- REST ports ----
        self.p_queue = .{
            .gpa = gpa,
            .conn = self.db,
            .commands = &self.queue_service,
            .adder = &self.add_job_service,
        };
        self.p_events = .{ .conn = self.db, .bus = self.bus };
        self.p_servers = .{ .gpa = gpa, .conn = self.db };
        self.p_categories = .{ .gpa = gpa, .conn = self.db };
        self.p_system = .{ .gpa = gpa, .conn = self.db, .svc = &self.system_service };
        self.p_schedule = .{ .gpa = gpa, .conn = self.db };
        self.p_commands = .{ .gpa = gpa, .conn = self.db, .svc = &self.command_service };
        self.p_subscriptions = .{ .gpa = gpa, .conn = self.db };
        self.p_config = .{ .rt = &self.runtime };
        self.p_bandwidth = .{ .limiter = &self.limiter, .rt = &self.runtime };
        self.p_auth = .{ .svc = &self.auth_service };

        self.backups_dir = try std.fs.path.join(gpa, &.{ cfg.server.data_dir, "backups" });
        errdefer gpa.free(self.backups_dir);
        self.logs_dir = try std.fs.path.join(gpa, &.{ cfg.server.data_dir, "logs" });
        errdefer gpa.free(self.logs_dir);

        self.p_backups = .{ .gpa = gpa, .conn = self.db, .dir = self.backups_dir };
        self.p_log_files = .{ .dir = self.logs_dir };

        // ---- the API object ----
        self.api = Api.init(gpa);
        errdefer self.api.deinit();

        self.api.queue = self.p_queue.port();
        self.api.events = self.p_events.port();
        self.api.servers = self.p_servers.port();
        // Without this the hook is permanently null and `notifyChanged` is
        // a no-op, which breaks the first-run flow outright: upload an NZB,
        // then configure a provider, and the job stays parked in
        // `waiting_for_server` until the daemon is restarted. The e2e
        // hotwire test is what caught it.
        self.p_servers.on_change = .{ .ctx = @ptrCast(self), .changedFn = onServersChanged };
        self.api.categories = self.p_categories.port();
        self.api.system = self.p_system.port();
        self.api.schedule = self.p_schedule.port();
        self.api.commands = self.p_commands.port();
        self.api.subscriptions = self.p_subscriptions.port();
        self.api.config = self.p_config.port();
        self.api.bandwidth = self.p_bandwidth.port();
        self.api.auth = self.p_auth.port();
        self.api.backups = self.p_backups.port();
        self.api.log_files = self.p_log_files.port();

        self.api.probe = self.probe.port();

        // Deliberately null, and each for a reason the operator can act
        // on rather than a gap they have to guess at:
        //
        //   * `health` — there is no health-check service in the app
        //     layer to wire; the port has no implementation to point at.
        //   * `disk` — `statfs` is in neither `posix/sys.zig` nor Zig's
        //     `std.posix`, and reproducing `struct statfs` for two
        //     platforms belongs in the syscall layer, not here.
        self.api.health = null;
        self.api.disk = null;

        self.api.log_ring = &self.log_ring;
        self.api.events_hub = &self.events_hub;
        self.api.logs_hub = &self.logs_hub;
        self.api.metrics = &self.registry;

        // ---- SAB ----
        self.s_queue = .{ .gpa = gpa, .conn = self.db, .commands = &self.queue_service };
        self.s_categories = .{ .conn = self.db };
        self.s_add_job = .{ .svc = &self.add_job_service };
        self.s_api_key = .{ .rt = &self.runtime };
        self.s_throughput = .{ .svc = &self.system_service };

        self.sab = .{
            .api_key = self.s_api_key.port(),
            .queue = self.s_queue.port(),
            .categories = self.s_categories.port(),
            .add_job = self.s_add_job.port(),
            // `mode=addurl` needs an HTTP client with DNS, which the
            // async client does not do yet; that mode answers 502 rather
            // than pretending to have fetched an NZB.
            .fetch = null,
            .throughput = self.s_throughput.port(),
            .complete_dir = cfg.paths.complete_dir,
            .logger = &log.default,
        };

        // ---- HTTP ----
        self.server.init(gpa, &self.loop, .{}, &routes);
        self.server.app_ctx = &self.api;
        self.server.url_base = self.runtime.urlBase();
        self.server.auth = self.api.authConfig();
    }

    /// Arms the two periodic timers. Separate from `wire` because a test
    /// that drives the loop by hand wants the graph without the clocks.
    pub fn startTimers(self: *App) !void {
        try self.loop.addTimer(&self.heartbeat, sse_heartbeat_ns);
        try self.loop.addTimer(&self.ticker, tick_interval_ns);
    }

    /// Bring the download pipeline up: pools, subscribers, and whatever
    /// the database says was in flight when the last process stopped.
    ///
    /// Separate from `wire` because `wire` is pure graph construction —
    /// no sockets, no threads, no side effects — and this is the point at
    /// which the daemon starts doing things. The e2e suite calls both.
    ///
    /// Order is not arbitrary. Pools first, so a job admitted by the
    /// sweep below finds somewhere to fetch from instead of parking
    /// itself in `waiting_for_server`. Subscribers second, so no event
    /// published by the sweep is lost — an `outbox_subs` row is only
    /// written for a subscription that already exists. The sweep last.
    pub fn startEngine(self: *App) !void {
        // Threads before subscribers: a stage queued by the sweep below
        // must find somewhere to put its hashing rather than falling back
        // to the reactor thread.
        try self.stages.start(.{});
        try self.engine.loadPools();
        try self.subscribePipeline();
        try self.engine.start();

        // Jobs whose trigger event was consumed by a previous process
        // before it wrote the row that would have advanced them. The bus
        // will not redeliver those, so somebody has to go looking — and
        // has to re-drive the stage the job's own verdict implies rather
        // than assume it is the last one. See `app/recovery.zig`.
        _ = self.reconciler.run() catch |err| {
            log.warn("pipeline: startup recovery failed", &.{log.str("error", @errorName(err))});
        };
    }

    /// A server was added, edited, enabled, disabled or removed through the
    /// API. Rebuild the pools so a newly configured provider can pick up
    /// jobs that are parked waiting for one, and so a removed provider
    /// loses its connections rather than being dialled again.
    ///
    /// Failure is logged rather than propagated: the REST call itself
    /// succeeded and the row is written, so reporting an error to the
    /// operator would be misleading. The next restart reloads regardless.
    fn onServersChanged(ctx: *anyopaque) void {
        const self: *App = @ptrCast(@alignCast(ctx));
        if (!self.engine_ready) return;
        self.engine.loadPools() catch |err| {
            log.warn("servers changed but pools could not be rebuilt", &.{
                log.str("error", @errorName(err)),
            });
            return;
        };
        self.engine.kickParked();
    }

    /// The post-download half of `docs/architecture.md`'s event flow,
    /// as a table.
    ///
    /// `verify.ok` has two subscribers on purpose: `app/extract` takes
    /// archive jobs and `app/deliver` takes everything else, both by
    /// running the same predicate over the same aggregate. Exactly one of
    /// them acts, and neither has to know about the other.
    fn subscribePipeline(self: *App) !void {
        const Sub = struct {
            name: []const u8,
            topic: []const u8,
            handler: outbox.Handler,
        };
        const table = [_]Sub{
            .{ .name = "verify.on_download_complete", .topic = "download.job.download_complete", .handler = &onVerify },
            .{ .name = "verify.on_repair_ok", .topic = "repair.ok", .handler = &onReverify },
            .{ .name = "repair.on_repair_needed", .topic = "verify.repair_needed", .handler = &onRepair },
            .{ .name = "extract.on_verify_ok", .topic = "verify.ok", .handler = &onExtract },
            .{ .name = "deliver.on_verify_ok", .topic = "verify.ok", .handler = &onDeliver },
        };
        for (table) |s| try self.engine.subscribe(s.name, s.topic, s.handler, @ptrCast(self));

        // Notify comes last and subscribes to the *outcome* topics the
        // five above publish, so a webhook fires on what the pipeline
        // decided rather than on what it was asked to do.
        try self.notifier.subscribeAll(&self.engine);
    }

    /// Mirror the resolved API key into the settings table when it has
    /// none.
    ///
    /// There are two durable records of the key and they answer different
    /// questions. `config.toml` is what this install *minted* — written
    /// by `ensureConfigFile` on the first start, readable by the operator,
    /// and the file Go wrote for the same reason. The settings table is
    /// what the operator has *rotated to* since, and it wins, because a
    /// rotation from the UI that a config file could silently override
    /// would be a rotation that did not work.
    ///
    /// This mirrors the file's key into the table on the first start so
    /// that every entry point agrees — including the ones that build the
    /// graph directly rather than through `run`, which never see the file
    /// at all and would otherwise mint a fresh key per boot.
    fn persistFirstRunApiKey(self: *App) !void {
        const repo = @import("store/repo_settings.zig").SettingsRepo.init(self.db);
        const existing = repo.get(self.gpa, settings.keys.api_key) catch null;
        if (existing) |v| {
            self.gpa.free(v);
            return;
        }
        const key = self.runtime.apiKey();
        if (key.len == 0) return;
        repo.set(settings.keys.api_key, key) catch |err| {
            log.warn("cannot persist the api key", &.{log.str("error", @errorName(err))});
        };
    }

    /// The directories a download needs before the first segment lands.
    ///
    /// Created at start-up rather than lazily: a permissions problem
    /// should surface while the operator is watching the container come
    /// up, not eight minutes into a download.
    pub fn ensureDirs(self: *App) !void {
        const cfg = self.cfg.config;
        for ([_][]const u8{
            cfg.paths.incomplete_dir,
            cfg.paths.complete_dir,
            self.logs_dir,
        }) |dir| {
            if (dir.len == 0) continue;
            sys.mkdirPath(dir) catch |err| {
                log.warn("cannot create directory", &.{
                    log.str("path", dir),
                    log.str("error", @errorName(err)),
                });
            };
        }
    }
};

// ---------------------------------------------------------------------
// The post-download bus handlers
// ---------------------------------------------------------------------
//
// Each is a one-liner over an application service, and each runs inline
// on the reactor thread — see `bootstrap/pipeline.zig` on what that
// costs. Returning `.failed` hands the row back to the outbox's own
// retry-and-park machinery rather than losing it, which is why none of
// them swallow an error.

/// Queue one stage and return.
///
/// The handler no longer *runs* the stage: verification hashes every byte
/// of the release and extraction decompresses it, and doing that here
/// held the reactor thread for as long as it took — no HTTP, no SSE, no
/// other download. `offload.Stages` runs it on a fiber with the CPU on a
/// worker pool instead, and that file's module comment states what
/// settling the outbox row at hand-off rather than at completion costs.
///
/// A refusal is still reported, because a stage that was never queued is
/// one the bus should hold on to and offer again.
fn runStage(ctx: ?*anyopaque, env: outbox.Envelope, stage: offload_mod.Stage) outbox.HandlerResult {
    const app: *App = @ptrCast(@alignCast(ctx.?));
    const id = runtime_mod.aggregateJobId(env) orelse return .{ .failed = "no job id in the event" };
    app.stages.enqueue(stage, id) catch |err| {
        log.err("pipeline stage could not be queued", &.{
            log.str("stage", stage.text()),
            log.int("job_id", id),
            log.str("error", @errorName(err)),
        });
        return .{ .failed = @errorName(err) };
    };
    return .ok;
}

/// `recovery.StageQueue` over the same backlog the bus handlers push
/// into, so a job the startup sweep found is indistinguishable from one
/// an event delivered — same fiber, same worker pool, same ordering.
fn stageQueue(app: *App) recovery_app.StageQueue {
    const Impl = struct {
        fn enqueue(
            ctx: *anyopaque,
            stage: recovery_app.Stage,
            job_id: i64,
        ) recovery_app.QueueError!void {
            const a: *App = @ptrCast(@alignCast(ctx));
            return a.stages.enqueue(stage, job_id);
        }
    };
    return .{ .ctx = @ptrCast(app), .enqueueFn = &Impl.enqueue };
}

/// The stage bodies, run on the stage fiber. One switch rather than five
/// closures, because `offload.Stages` dispatches on a value.
fn runStageBody(ctx: *anyopaque, stage: offload_mod.Stage, id: i64) anyerror!void {
    const app: *App = @ptrCast(@alignCast(ctx));
    switch (stage) {
        .verify => _ = try app.verify_service.run(id),
        .reverify => _ = try app.verify_service.onRepairOk(id),
        .repair => _ = try app.repair_service.run(id),
        .extract => _ = try app.extract_service.run(id),
        .deliver => _ = try app.deliver_service.run(id),
    }
}

fn onVerify(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
    return runStage(ctx, env, .verify);
}

fn onReverify(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
    return runStage(ctx, env, .reverify);
}

fn onRepair(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
    return runStage(ctx, env, .repair);
}

fn onExtract(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
    return runStage(ctx, env, .extract);
}

fn onDeliver(ctx: ?*anyopaque, env: outbox.Envelope) outbox.HandlerResult {
    return runStage(ctx, env, .deliver);
}

/// `byte_accounter.ServerByteStore` over the servers table.
pub const ServerBytes = struct {
    gpa: Allocator,
    conn: *sqlite.Conn,

    pub fn port(self: *ServerBytes) byte_accounter.ServerByteStore {
        return .{ .ctx = @ptrCast(self), .incrementFn = &increment };
    }

    fn increment(
        ctx: *anyopaque,
        _: ?*app_ports.Unit,
        id: dserver.ServerId,
        n: i64,
    ) byte_accounter.StoreError!void {
        const self: *ServerBytes = @ptrCast(@alignCast(ctx));
        const repo = repo_server.ServerRepo.init(self.gpa, self.conn);
        repo.incrementUsedBytes(id, n) catch return error.Backend;
    }
};

/// The auth context publishes on its own event union, so it needs its own
/// sink instance. It is a single value with no per-call state, so it is
/// built on demand rather than stored.
fn authSink(app: *App) app_ports.EventSink(dauth_domain.Event) {
    const Impl = struct {
        fn publish(
            ctx: *anyopaque,
            _: ?*app_ports.Unit,
            events: []const dauth_domain.Event,
        ) app_ports.PublishError!void {
            const a: *App = @ptrCast(@alignCast(ctx));
            var p: infra.Publisher(dauth_domain.Event, null) = .{
                .gpa = a.gpa,
                .bus = a.bus,
                .conn = a.db,
            };
            return p.sink().publish(null, events);
        }
    };
    return .{ .ctx = @ptrCast(app), .publishFn = &Impl.publish };
}

/// `system_svc.PoolStats` over the servers table, plus live occupancy.
///
/// The configured shape comes from the row and the in-use / idle counts
/// come from the `nntp.Pool` the engine built for it. A row with no pool
/// — TLS with no trust anchors, or a server added since the last restart
/// — reports zero occupancy rather than being hidden, because "you added
/// it and nothing is using it" is the answer the System page exists to
/// give.
pub const PoolStats = struct {
    gpa: Allocator,
    conn: *sqlite.Conn,
    /// Null in a graph built without the download engine.
    engine: ?*runtime_mod.Runtime = null,

    pub fn port(self: *PoolStats) system_svc.PoolStats {
        return .{ .ctx = @ptrCast(self), .snapshotFn = &snapshot };
    }

    fn snapshot(ctx: *anyopaque, a: Allocator) Allocator.Error![]system_svc.PoolStatus {
        const self: *PoolStats = @ptrCast(@alignCast(ctx));
        const repo = repo_server.ServerRepo.init(a, self.conn);
        const list = repo.list(a) catch return &.{};
        const out = try a.alloc(system_svc.PoolStatus, list.items.items.len);
        for (list.items.items, 0..) |*s, i| {
            out[i] = .{
                .server_id = s.id,
                .server_name = s.name,
                .max_conns = s.max_conns,
                .enabled = s.enabled,
                .backup = s.backup,
                .metered = s.billing_mode != .flat,
                .quota_bytes = s.quota_bytes,
                .used_bytes = s.used_bytes,
            };
            const engine = self.engine orelse continue;
            const live = engine.occupancy(s.id) orelse continue;
            out[i].idle = @intCast(@min(live.idle, std.math.maxInt(u16)));
            // `openCount` is everything against the provider's cap,
            // established or still connecting; what is not parked is
            // being used.
            out[i].in_use = @intCast(live.open -| @as(u32, @intCast(live.idle)));
        }
        return out;
    }
};

/// `system_svc.SpeedHistory` over the minute-bucket table.
pub const SpeedHistory = struct {
    conn: *sqlite.Conn,

    pub fn port(self: *SpeedHistory) system_svc.SpeedHistory {
        return .{
            .ctx = @ptrCast(self),
            .appendFn = &append,
            .purgeFn = &purge,
            .savePeakFn = &savePeak,
        };
    }

    fn repo(self: *SpeedHistory) repo_speed_history.SpeedHistoryRepo {
        return repo_speed_history.SpeedHistoryRepo.init(self.conn);
    }

    fn append(ctx: *anyopaque, s: system_svc.SpeedSample) system_svc.HistoryError!void {
        const self: *SpeedHistory = @ptrCast(@alignCast(ctx));
        self.repo().append(.{
            .at_seconds = @divFloor(s.at, std.time.ms_per_s),
            .bytes_per_sec = s.bytes_per_sec,
        }) catch return error.Backend;
    }

    fn purge(ctx: *anyopaque, cutoff: i64) system_svc.HistoryError!usize {
        const self: *SpeedHistory = @ptrCast(@alignCast(ctx));
        const n = self.repo().purge(@divFloor(cutoff, std.time.ms_per_s)) catch
            return error.Backend;
        return @intCast(@max(n, 0));
    }

    fn savePeak(ctx: *anyopaque, v: i64) system_svc.HistoryError!void {
        const self: *SpeedHistory = @ptrCast(@alignCast(ctx));
        const settings_repo = @import("store/repo_settings.zig").SettingsRepo.init(self.conn);
        settings_repo.setInt("system.all_time_peak_bytes_per_sec", v) catch return error.Backend;
    }
};

// ---------------------------------------------------------------------
// Periodic work
// ---------------------------------------------------------------------

/// Keeps proxies from reaping an idle SSE stream. A comment frame, not an
/// event, so a client that is only listening for named topics sees
/// nothing at all.
fn onHeartbeat(t: *reactor.Timer) void {
    const app: *App = @fieldParentPtr("heartbeat", t);
    app.events_hub.heartbeat();
    app.logs_hub.heartbeat();
    app.loop.addTimer(&app.heartbeat, sse_heartbeat_ns) catch {};
}

/// One second of housekeeping: flush the throughput minute bucket when it
/// is due, run the retention purge when that is due.
///
/// Both are "when due" rather than "every tick" because the service owns
/// the schedule — aligning the flush to a wall-clock minute is what lets
/// the API concatenate the in-memory window with the persisted rows.
fn onTick(t: *reactor.Timer) void {
    const app: *App = @fieldParentPtr("ticker", t);
    const now = infra.nowMillis();

    app.system_service.flushHistory(now) catch |err| {
        log.default.warn("throughput history flush failed", &.{log.str("error", @errorName(err))});
    };
    _ = app.system_service.purgeHistory(now) catch |err| {
        log.default.warn("throughput history purge failed", &.{log.str("error", @errorName(err))});
    };

    // Staged per-server byte totals, written at most once every ten
    // seconds rather than once per article. The counter is what a metered
    // account's quota is measured against, so it is flushed on a clock
    // rather than only at shutdown.
    app.byte_flusher.flushIfDue(null, now) catch |err| {
        log.default.warn("byte accounting flush failed", &.{log.str("error", @errorName(err))});
    };

    app.loop.addTimer(&app.ticker, tick_interval_ns) catch {};
}

/// Read the config file with our own syscalls.
///
/// `config.loadOrCreate` wants an `std.Io`, and adopting one would pull in
/// exactly the backends `posix/sys.zig` exists to avoid. Reading the file
/// here and handing bytes to `config.loadFromBytes` keeps that boundary.
fn readConfigFile(gpa: Allocator, path: [:0]const u8) !?[]u8 {
    const fd = sys.open(path, .{ .mode = .read_only }) catch |err| switch (err) {
        // A fresh container has no config.toml, which is not an error:
        // every setting has a default.
        error.NoSuchFileOrDirectory => return null,
        else => return err,
    };
    defer sys.close(fd);

    const size = try sys.fileSize(fd);
    if (size > 1 << 20) return error.ConfigInvalid;

    // lseek left the offset at the end; a fresh open would be simpler but
    // this avoids a second syscall pair.
    const buf = try gpa.alloc(u8, @intCast(size));
    errdefer gpa.free(buf);

    const fd2 = try sys.open(path, .{ .mode = .read_only });
    defer sys.close(fd2);
    var off: usize = 0;
    while (off < buf.len) {
        const n = try sys.read(fd2, buf[off..]);
        if (n == 0) break;
        off += n;
    }
    return buf[0..off];
}

/// A 32-character hex API key, for the first start when no config exists.
pub fn generateApiKey(out: *[32]u8) void {
    var raw: [16]u8 = undefined;
    sys.randomBytes(&raw);
    _ = std.fmt.bufPrint(out, "{x}", .{&raw}) catch unreachable;
}

/// Mode of `config.toml`. It holds the API key, so it is not world- or
/// group-readable.
const config_mode: u32 = 0o600;

/// Write `config.toml` on the first start, and return its bytes.
///
/// Without this the generated key lives only in memory: a fresh one is
/// minted on every start, so every *arr configured yesterday stops
/// authenticating after a restart, and the operator has no file to read
/// the current one out of.
///
/// The ordering matters and matches the Go original. The file is rendered
/// from the config *before* environment overrides are folded in, so what
/// it records is the generated key even when `HOARDARR_API_KEY` is also
/// set — the file is the durable record of what this install minted, and
/// the environment is a per-run override on top of it.
///
/// Exclusive create is how two daemons racing on one data directory are
/// resolved: exactly one writes, and the loser re-reads rather than
/// clobbering the winner's key.
pub fn ensureConfigFile(
    gpa: Allocator,
    path: [:0]const u8,
    data_dir: []const u8,
    generated_key: []const u8,
) !?[]u8 {
    if (readConfigFile(gpa, path)) |existing| {
        if (existing) |bytes| return bytes;
    } else |err| return err;

    // Defaults plus the generated key, with no environment applied.
    var seed = config.loadFromBytes(gpa, null, data_dir, .{
        .api_key_fallback = generated_key,
    }) catch return error.ConfigInvalid;
    defer seed.deinit();

    const rendered = try config.render(gpa, seed.config);
    defer gpa.free(rendered);

    const fd = sys.open(path, .{
        .mode = .write_only,
        .create = true,
        .exclusive = true,
    }) catch |err| switch (err) {
        // Somebody else got there first. Their key is the real one.
        error.Exists => return readConfigFile(gpa, path),
        else => return err,
    };
    defer sys.close(fd);

    // Tighten before writing, so the key never exists world-readable.
    infra.fchmod(fd, config_mode) catch |err| {
        log.warn("cannot restrict config file permissions", &.{
            log.str("path", path),
            log.str("error", @errorName(err)),
        });
    };
    try sys.writeAll(fd, rendered);
    try sys.fsync(fd);

    log.info("wrote initial configuration", &.{
        log.str("path", path),
        log.str("note", "contains the generated api key"),
    });
    return try gpa.dupe(u8, rendered);
}

pub fn run(gpa: Allocator, env: std.process.Environ) !u8 {
    // The config layer takes an environment map rather than the raw block,
    // so HOARDARR_* overrides can be injected in tests without mutating
    // the real environment.
    var env_map = env.createMap(gpa) catch |err| {
        return fatal("cannot read environment: {t}", .{err});
    };
    defer env_map.deinit();

    // ---- 1. privileges ----
    try dropPrivileges(env);

    // ---- 2. config ----
    const data_dir = envOr(env, "HOARDARR_DATA_DIR", "/data");
    sys.mkdirPath(data_dir) catch |err| {
        return fatal("cannot create data directory '{s}': {t}", .{ data_dir, err });
    };

    var path_buf: [sys.path_max]u8 = undefined;
    const cfg_path = sys.joinZ(&path_buf, data_dir, "config.toml") catch {
        return fatal("data directory path is too long", .{});
    };

    var key_buf: [32]u8 = undefined;
    generateApiKey(&key_buf);

    // Reads the file, or writes it with a freshly generated key on the
    // very first start. Either way `raw` is what is on disk afterwards,
    // so the key survives restarts.
    const raw = ensureConfigFile(gpa, cfg_path, data_dir, &key_buf) catch |err| {
        return fatal("cannot read or create {s}: {t}", .{ cfg_path, err });
    };
    defer if (raw) |r| gpa.free(r);

    var diag: config.Diagnostic = .{};
    var loaded = config.loadFromBytes(gpa, raw, data_dir, .{
        .env = &env_map,
        .diag = &diag,
        .api_key_fallback = &key_buf,
    }) catch {
        return fatal("configuration rejected: {s}", .{diag.msg});
    };
    errdefer loaded.deinit();
    const cfg = loaded.config;

    // ---- 3. logging ----
    log.initDefault(parseLevel(cfg.server.log_level), .text);
    log.info("starting", &.{
        log.str("version", build_info.version),
        log.str("commit", build_info.commit),
        log.str("backend", reactor.backend_name),
        log.str("data_dir", cfg.server.data_dir),
    });

    var app: App = .{ .gpa = gpa };
    app.cfg = loaded;
    try app.loop.init(gpa);
    errdefer app.loop.deinit();

    // ---- 4. database ----
    //
    // The resolved data directory can differ from the environment's — a
    // config file or an override may point elsewhere — so create the
    // resolved one rather than assuming the earlier mkdir covered it.
    sys.mkdirPath(cfg.server.data_dir) catch |err| {
        return fatal("cannot create data directory '{s}': {t}", .{ cfg.server.data_dir, err });
    };

    var db_buf: [sys.path_max]u8 = undefined;
    const db_path = sys.joinZ(&db_buf, cfg.server.data_dir, "hoardarr.db") catch {
        return fatal("data directory path is too long", .{});
    };
    app.db = sqlite.Conn.open(gpa, db_path, .{}) catch |err| {
        return fatal("cannot open database {s}: {t}", .{ db_path, err });
    };
    errdefer app.db.close();

    // A schema we don't understand is worse than not starting: the daemon
    // would write rows the next version can't read.
    migrate.migrate(app.db) catch |err| {
        return fatal("migration failed: {t}", .{err});
    };
    log.info("database ready", &.{
        log.str("path", db_path),
        log.uint("schema_version", migrate.currentVersion(app.db) catch 0),
    });

    // ---- 5. the object graph ----
    app.wire() catch |err| {
        return fatal("cannot wire the application: {t}", .{err});
    };
    app.started = true;
    defer app.deinit();
    try app.ensureDirs();

    // Not having a server yet is a normal first run, not a failure: the
    // operator adds one from the UI we are about to serve.
    logServerCount(&app);

    // ---- 6. signals ----
    signals.ignoreSigpipe();
    try app.sigs.init(onSignal);
    app.sigs.context = &app;
    try app.loop.add(&app.sigs.source);
    app.signals_installed = true;

    // ---- 7. listen ----
    const addr = parseListen(cfg.server.listen) catch {
        return fatal("cannot parse listen address '{s}'", .{cfg.server.listen});
    };
    app.server.listen(addr) catch |err| {
        return fatal("cannot listen on {s}: {t}", .{ cfg.server.listen, err });
    };
    try app.startTimers();

    // ---- 8. downloads ----
    app.startEngine() catch |err| {
        return fatal("cannot start the download engine: {t}", .{err});
    };

    log.info("listening", &.{
        log.str("addr", cfg.server.listen),
        log.boolean("ui_embedded", assets.present),
        log.uint("routes", routes.len),
    });

    try app.loop.run();
    log.info("stopped", &.{});
    return 0;
}

fn logServerCount(app: *App) void {
    const repo = repo_server.ServerRepo.init(app.gpa, app.db);
    var list = repo.list(app.gpa) catch return;
    defer list.deinit();
    if (list.items.items.len == 0) {
        log.warn("no usenet servers configured; add one from Settings before queueing", &.{});
    } else {
        log.info("usenet servers loaded", &.{log.uint("count", list.items.items.len)});
    }
}

/// `SIGTERM` and `SIGINT` stop the loop, which unwinds through `App.deinit`
/// with the runtime fully available. `SIGHUP` is accepted and logged but
/// does not reload yet — silently ignoring it would look like a hang.
fn onSignal(s: *signals.Signals, sig: signals.Signal) void {
    const app: *App = @ptrCast(@alignCast(s.context.?));
    switch (sig) {
        .term, .interrupt => {
            log.info("shutting down", &.{log.str("signal", @tagName(sig))});
            app.loop.stop();
        },
        .hup => log.warn("SIGHUP received; configuration reload is not implemented", &.{}),
    }
}

fn dropPrivileges(env: std.process.Environ) !void {
    if (sys.getuid() != 0) return;
    const uid = envInt(env, "PUID") orelse 1000;
    const gid = envInt(env, "PGID") orelse 1000;
    sys.dropPrivileges(uid, gid) catch |err| {
        _ = fatal("refusing to run as root: cannot drop to {d}:{d}: {t}", .{ uid, gid, err }) catch {};
        return error.PrivilegeDropFailed;
    };
}

fn envOr(env: std.process.Environ, name: []const u8, fallback: []const u8) []const u8 {
    const v = env.getPosix(name) orelse return fallback;
    return if (v.len == 0) fallback else v;
}

fn envInt(env: std.process.Environ, name: []const u8) ?u32 {
    const raw = env.getPosix(name) orelse return null;
    return std.fmt.parseInt(u32, std.mem.trim(u8, raw, " \t\r\n"), 10) catch null;
}

fn parseLevel(text: []const u8) log.Level {
    if (std.ascii.eqlIgnoreCase(text, "debug")) return .debug;
    if (std.ascii.eqlIgnoreCase(text, "warn")) return .warn;
    if (std.ascii.eqlIgnoreCase(text, "error")) return .err;
    return .info;
}

/// Parse a `host:port` or `:port` listen string.
///
/// A bare `:port` binds all interfaces, matching the Go behaviour and what
/// every existing `config.toml` says.
pub fn parseListen(spec: []const u8) !std.Io.net.IpAddress {
    const colon = std.mem.lastIndexOfScalar(u8, spec, ':') orelse return error.InvalidListen;
    const host = spec[0..colon];
    const port = std.fmt.parseInt(u16, spec[colon + 1 ..], 10) catch return error.InvalidListen;
    if (host.len == 0) return std.Io.net.IpAddress.parse("0.0.0.0", port);
    // Strip brackets from an IPv6 literal.
    const bare = if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']')
        host[1 .. host.len - 1]
    else
        host;
    return std.Io.net.IpAddress.parse(bare, port);
}

/// Report a start-up failure to stderr and return a non-zero exit code.
///
/// Deliberately stderr rather than the logger: most of these fire before
/// logging is configured, and a container that dies at start-up should say
/// why on the console the operator is already looking at.
fn fatal(comptime fmt: []const u8, args: anytype) !u8 {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    w.print("hoardarr: " ++ fmt ++ "\n", args) catch {};
    sys.writeAll(sys.stderr_fd, w.buffered()) catch {};
    return 1;
}

// ---------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------

/// The whole served surface: this file's two, the REST table, the two SAB
/// routes, and the SPA catch-all.
///
/// Order does not affect matching — precedence is exact-beats-prefix,
/// longest-prefix-wins — but the catch-all is last so a reader can see
/// that nothing hides behind it.
pub const routes = local_routes ++ rest_handlers.routes ++ sab_routes ++ [_]http.Route{
    .{ .path = "/", .kind = .prefix, .handler = handleAsset, .access = .public },
};

const local_routes = [_]http.Route{
    .{ .method = .get, .path = "/healthz", .handler = handleHealthz, .access = .public },
    .{ .method = .get, .path = "/api/v1/version", .handler = handleVersion, .access = .public },
    // An unknown path under /api/ must be a JSON 404, not the SPA. The
    // catch-all would otherwise hand an *arr an HTML page with a 200 on
    // it, which is far harder to diagnose than a 404. Longest-prefix-wins
    // keeps this from shadowing the real prefix routes, every one of
    // which is longer.
    .{ .path = "/api/", .kind = .prefix, .handler = handleUnknownApi },
};

/// The SAB surface authenticates itself — `apikey=` in the form body,
/// checked in constant time by the handler — so both routes are `.public`
/// as far as the server's own auth is concerned, and both accept every
/// method because clients POST `addfile` and GET everything else.
const sab_routes = [_]http.Route{
    .{ .method = null, .path = sab_handler.mount_path, .handler = handleSab, .access = .public },
    .{
        .method = null,
        .path = sab_handler.mount_prefix,
        .kind = .prefix,
        .handler = handleSab,
        .access = .public,
    },
};

/// `api/sab/handler.zig` ships a `routes(App, field)` helper, but it
/// resolves its handler with `ctx.app(App)` — and `app_ctx` has to be the
/// `Api` the REST handlers expect. Walking back from that one pointer with
/// `@fieldParentPtr` is the cost of the two layers sharing a server.
fn handleSab(ctx: *http.Ctx) http.HandlerError!void {
    const api = ctx.app(Api);
    const app: *App = @fieldParentPtr("api", api);

    var arena: std.heap.ArenaAllocator = .init(ctx.server.gpa);
    defer arena.deinit();

    const result = try app.sab.dispatch(arena.allocator(), sab_handler.inputFromRequest(ctx.req));
    try ctx.res.send(result.status, "application/json", result.body);
}

fn handleUnknownApi(ctx: *http.Ctx) http.HandlerError!void {
    try ctx.res.send(404, "application/json", "{\"error\":\"not found\"}");
}

fn handleHealthz(ctx: *http.Ctx) http.HandlerError!void {
    try ctx.res.send(200, "application/json", "{\"status\":\"ok\"}");
}

fn handleVersion(ctx: *http.Ctx) http.HandlerError!void {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    // A fixed writer's only failure is a full buffer, which for build
    // metadata means someone set an absurd -Dversion. Report it rather
    // than mapping it onto an unrelated response error.
    w.print(
        "{{\"version\":\"{f}\",\"commit\":\"{f}\",\"backend\":\"{f}\"}}",
        .{
            std.zig.fmtString(build_info.version),
            std.zig.fmtString(build_info.commit),
            std.zig.fmtString(reactor.backend_name),
        },
    ) catch {
        try ctx.res.send(500, "application/json", "{\"error\":\"version metadata too long\"}");
        return;
    };
    try ctx.res.send(200, "application/json", w.buffered());
}

/// Serve the embedded frontend.
///
/// Assets were gzipped at build time, so a client that accepts gzip gets
/// the pre-compressed bytes and the daemon never runs a compressor. An
/// unknown path falls back to `index.html` rather than 404ing, because the
/// UI is a single-page app and its routes only exist client-side.
fn handleAsset(ctx: *http.Ctx) http.HandlerError!void {
    if (!assets.present) {
        try ctx.res.send(200, "text/html; charset=utf-8",
            \\<!doctype html><meta charset=utf-8><title>hoardarr</title>
            \\<p>No frontend bundle in this build. Rebuild with
            \\<code>zig build -Dembed-ui=true</code> after
            \\<code>cd frontend &amp;&amp; npm run build</code>.
        );
        return;
    }

    const req_path = ctx.req.path;
    const path = if (req_path.len == 0 or std.mem.eql(u8, req_path, "/")) "/index.html" else req_path;
    const asset = assets.find(path) orelse assets.find("/index.html") orelse {
        try ctx.res.send(404, "application/json", "{\"error\":\"not found\"}");
        return;
    };

    if (asset.gz) |gz| {
        if (acceptsGzip(ctx)) {
            try ctx.res.setHeader("Content-Encoding", "gzip");
            // Any cache in front of us must not serve these bytes to a
            // client that didn't ask for gzip.
            try ctx.res.setHeader("Vary", "Accept-Encoding");
            try ctx.res.send(200, asset.content_type, gz);
            return;
        }
    }
    try ctx.res.send(200, asset.content_type, asset.raw);
}

fn acceptsGzip(ctx: *http.Ctx) bool {
    const v = ctx.req.header("accept-encoding") orelse return false;
    return std.mem.indexOf(u8, v, "gzip") != null;
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "listen strings parse the way existing configs are written" {
    // ":8085" is what every config.toml in the wild says, and it has to
    // mean "all interfaces" rather than being rejected.
    const any = try parseListen(":8085");
    try testing.expectEqual(@as(u16, 8085), any.getPort());

    const local = try parseListen("127.0.0.1:9000");
    try testing.expectEqual(@as(u16, 9000), local.getPort());
    try testing.expect(local == .ip4);

    const v6 = try parseListen("[::1]:9000");
    try testing.expectEqual(@as(u16, 9000), v6.getPort());
    try testing.expect(v6 == .ip6);
}

test "a malformed listen string is refused, not defaulted" {
    // Defaulting would silently bind somewhere the operator didn't ask
    // for, which for a service with an API key is a security surprise.
    try testing.expectError(error.InvalidListen, parseListen("8085"));
    try testing.expectError(error.InvalidListen, parseListen(":notaport"));
    try testing.expectError(error.InvalidListen, parseListen(""));
}

test "log level parsing matches the config vocabulary" {
    try testing.expectEqual(log.Level.debug, parseLevel("debug"));
    try testing.expectEqual(log.Level.info, parseLevel("info"));
    try testing.expectEqual(log.Level.warn, parseLevel("warn"));
    try testing.expectEqual(log.Level.err, parseLevel("error"));
    // Normalisation lowercases, but an unrecognised value must not turn
    // logging off — it falls back to info.
    try testing.expectEqual(log.Level.info, parseLevel("LOUD"));
    try testing.expectEqual(log.Level.info, parseLevel(""));
}

test "generated api keys are 32 hex characters and differ" {
    var a: [32]u8 = undefined;
    var b: [32]u8 = undefined;
    generateApiKey(&a);
    generateApiKey(&b);

    for (a) |c| try testing.expect(std.ascii.isHex(c));
    // A fixed key across installs would be a default credential.
    try testing.expect(!std.mem.eql(u8, &a, &b));
}

test "every route the daemon serves is reachable and nothing shadows another" {
    // The catch-all is a prefix on "/", so an exact route added after it
    // still wins — but only because matching is exact-beats-prefix. This
    // asserts the table itself is sane: no duplicate method+path pair,
    // and the SPA fallback is the only bare-"/" prefix.
    var spa_catchalls: usize = 0;
    for (routes, 0..) |r, i| {
        if (r.kind == .prefix and std.mem.eql(u8, r.path, "/")) spa_catchalls += 1;
        for (routes[i + 1 ..]) |other| {
            if (r.kind != other.kind) continue;
            if (!std.mem.eql(u8, r.path, other.path)) continue;
            if (r.method != other.method) continue;
            std.debug.print("duplicate route: {s}\n", .{r.path});
            return error.DuplicateRoute;
        }
    }
    try testing.expectEqual(@as(usize, 1), spa_catchalls);

    // The three public bootstrap endpoints, plus SAB, plus the auth
    // triple. Everything else must be protected or an unauthenticated
    // client could read the queue.
    var public: usize = 0;
    for (routes) |r| {
        if (r.access == .public) public += 1;
    }
    try testing.expectEqual(@as(usize, 8), public);
}
