// Package bootstrap is hoardarr's composition root.
//
// It is the only place that imports concrete adapter packages and wires
// them to domain ports. main() does nothing but parse flags, load config,
// install signal handlers, and call into bootstrap.
//
// The split exists so command-line entry stays tiny and testable, and so
// alternative entry points (e.g. an in-process integration-test harness)
// can wire the same App without duplicating logic.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/bcrypt"
	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/api/sab"
	"github.com/jaenster/hoardarr/internal/api/sse"
	appauth "github.com/jaenster/hoardarr/internal/app/auth"
	appdeliver "github.com/jaenster/hoardarr/internal/app/deliver"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appextract "github.com/jaenster/hoardarr/internal/app/extract"
	appnotify "github.com/jaenster/hoardarr/internal/app/notify"
	apprepair "github.com/jaenster/hoardarr/internal/app/repair"
	appschedule "github.com/jaenster/hoardarr/internal/app/schedule"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	appsystem "github.com/jaenster/hoardarr/internal/app/system"
	appverify "github.com/jaenster/hoardarr/internal/app/verify"
	adapterfs "github.com/jaenster/hoardarr/internal/adapter/fs"
	adapterdiscord "github.com/jaenster/hoardarr/internal/adapter/notify/discord"
	adapterrouter "github.com/jaenster/hoardarr/internal/adapter/notify/router"
	adapterslack "github.com/jaenster/hoardarr/internal/adapter/notify/slack"
	adapternotify "github.com/jaenster/hoardarr/internal/adapter/notify/webhook"
	adapterrar "github.com/jaenster/hoardarr/internal/adapter/rar"

	"github.com/jaenster/hoardarr/internal/domain/extract"
	"github.com/jaenster/hoardarr/internal/domain/notify"
	"github.com/jaenster/hoardarr/internal/loghub"
	"github.com/jaenster/hoardarr/internal/metrics"
	"github.com/jaenster/hoardarr/internal/config"
	domainschedule "github.com/jaenster/hoardarr/internal/domain/schedule"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
	"github.com/jaenster/hoardarr/internal/server"
)

// buildSABBase derives the SAB-compat URL the operator pastes into
// Sonarr/Radarr. cfg.Server.Listen may be ":8085" (any-interface) or
// "127.0.0.1:8085" (explicit); the second form is the better hint to
// the operator. We leave both as-is and prepend http:// — TLS belongs
// to a reverse proxy in front of hoardarr today.
func buildSABBase(listen string) string {
	if listen == "" {
		return ""
	}
	host := listen
	if len(host) > 0 && host[0] == ':' {
		host = "localhost" + host
	}
	return "http://" + host + "/sabnzbd/api"
}

// defaultBuildVersion is the fallback version string used when the
// caller doesn't pass build metadata via WithBuildInfo. Real binaries
// are built with -ldflags injecting main.version, which cmd/hoardarr
// forwards via WithBuildInfo. Tests and `go run` end up here.
const defaultBuildVersion = "dev"

// App is the wired-together hoardarr runtime. Construct via Build, run
// via Run, tear down via Shutdown.
type App struct {
	Cfg    config.Config
	Logger *slog.Logger

	DB    *sqlite.DB
	TxMgr *sqlite.TxManager
	Bus   *sqlite.OutboxBus

	ServerRepo *sqlite.ServerRepo
	JobRepo    *sqlite.JobRepo

	ServerService *appserver.Service
	AddJobService *appdownload.AddJobService
	QueueService  *appdownload.QueueService
	AuthService   *appauth.Service
	SystemService *appsystem.Service

	StartedAt time.Time

	Pools        map[domainserver.ServerID]*nntp.Pool
	Orchestrator *appdownload.OrchestratorService
	Verify       *appverify.Service
	Repair       *apprepair.Service
	Deliver      *appdeliver.Service
	Extract      *appextract.Service
	Notify       *appnotify.Service
	Scheduler    *appschedule.Scheduler
	ByteFlusher  *appdownload.ByteFlusher
	LogHub       *loghub.Hub

	LiveHub *sse.Hub

	HTTP       *server.Server
	HTTPServer *http.Server

	dataDirLock *fileLock

	shutdownOnce sync.Once
	shutdownErr  error
}

// BuildOption configures Build. Used by tests / alternative entry
// points (e.g. cassette replay) without bloating Build's signature.
type BuildOption func(*buildOptions)

type buildOptions struct {
	nntpDialer nntp.Dialer
	extractor  extract.Extractor
	logHub     *loghub.Hub
	configPath string
	version    string
	commit     string
	buildDate  string
}

// WithNNTPDialer overrides the default network dialer used by all
// NNTP pools. Tests use this to plug in recording / replay wrappers.
func WithNNTPDialer(d nntp.Dialer) BuildOption {
	return func(o *buildOptions) { o.nntpDialer = d }
}

// WithExtractor overrides the RAR-backed extract.Extractor with a
// substitute. Tests use this to bypass real RAR parsing while still
// exercising the deliver/extract orchestration flow.
func WithExtractor(e extract.Extractor) BuildOption {
	return func(o *buildOptions) { o.extractor = e }
}

// WithLogHub plumbs the process-global log hub (created in main) into
// the App so the REST API can expose the recent-logs snapshot and
// live tail.
func WithLogHub(h *loghub.Hub) BuildOption {
	return func(o *buildOptions) { o.logHub = h }
}

// WithConfigPath tells bootstrap where the live config.toml lives so
// runtime settings mutations (URL_BASE from the UI) can persist back
// to disk. Tests that don't care about persistence can omit this.
func WithConfigPath(path string) BuildOption {
	return func(o *buildOptions) { o.configPath = path }
}

// WithBuildInfo plumbs the binary's identification (version, commit,
// build date) through to the System status endpoint and the eventual
// /metrics build_info gauge. cmd/hoardarr passes the values injected
// via -ldflags; tests can leave these empty (they default to "dev" /
// "unknown" / "unknown").
func WithBuildInfo(version, commit, buildDate string) BuildOption {
	return func(o *buildOptions) {
		o.version = version
		o.commit = commit
		o.buildDate = buildDate
	}
}

// Build wires the runtime: ensures data directories exist, opens the
// SQLite database, applies migrations, constructs the transaction
// manager and outbox event bus, builds NNTP pools for enabled servers,
// constructs the orchestrator service, and prepares the HTTP server.
//
// On error, all partially-initialised resources are released before
// returning. Callers may pass the returned App to Run.
func Build(ctx context.Context, cfg config.Config, frontendFS fs.FS, logger *slog.Logger, opts ...BuildOption) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}
	bo := buildOptions{}
	for _, o := range opts {
		o(&bo)
	}
	if bo.version == "" {
		bo.version = defaultBuildVersion
	}
	metrics.SetBuildInfo(bo.version, bo.commit, runtime.Version())

	if err := ensureDirs(cfg); err != nil {
		return nil, err
	}

	lock, err := acquireDataDirLock(cfg.Server.DataDir)
	if err != nil {
		return nil, err
	}

	db, err := openDB(ctx, cfg)
	if err != nil {
		_ = lock.Release()
		return nil, err
	}

	if err := db.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	txm := sqlite.NewTxManager(db)
	bus := sqlite.NewOutboxBus(db, sqlite.OutboxOptions{Logger: logger})

	serverRepo := sqlite.NewServerRepo(db)
	jobRepo := sqlite.NewJobRepo(db)

	serverService := appserver.New(serverRepo, bus, txm, nil)
	addJobService := appdownload.NewAddJobService(jobRepo, bus, txm, nil)
	queueService := appdownload.NewQueueService(appdownload.QueueServiceParams{
		Repo:          jobRepo,
		Bus:           bus,
		TxManager:     txm,
		Logger:        logger,
		IncompleteDir: cfg.Paths.IncompleteDir,
	})

	pools, err := buildPools(ctx, serverRepo, logger, bo.nntpDialer)
	if err != nil {
		_ = bus.Close()
		_ = db.Close()
		return nil, fmt.Errorf("build pools: %w", err)
	}

	throughput := appsystem.NewThroughput()
	byteAccounter := appdownload.NewByteAccounter().WithObserver(func(n int64) {
		throughput.Add(n)
	})
	byteFlusher := appdownload.NewByteFlusher(byteAccounter, serverRepo, 10*time.Second, logger)

	settingsRepo := sqlite.NewSettingsRepo(db)
	runtime, err := server.NewRuntime(ctx, settingsRepo, cfg, logger)
	if err != nil {
		_ = bus.Close()
		_ = db.Close()
		return nil, fmt.Errorf("build runtime: %w", err)
	}

	// Bandwidth limiter: global cap from the runtime (which seeded
	// from cfg + SQLite settings), per-server caps from the registry.
	// Per-server caps applied via SetServerCap for every enabled
	// server.
	bandwidthLimiter := appdownload.NewLimiter(runtime.BandwidthGlobalCap())
	for id, p := range pools {
		bandwidthLimiter.SetServerCap(id, p.Server().BandwidthBytesPerSec())
	}
	// Live: when the operator changes the global cap from Settings,
	// fan it out to the in-memory token bucket so the next dispatch
	// honours the new rate.
	runtime.OnBandwidthGlobalChange(func(v int64) { bandwidthLimiter.SetGlobalCap(v) })
	orch := appdownload.NewOrchestratorService(appdownload.OrchestratorServiceParams{
		Repo:          jobRepo,
		Bus:           bus,
		TxManager:     txm,
		Pools:         pools,
		Accounter:     byteAccounter,
		Limiter:       bandwidthLimiter,
		IncompleteDir: cfg.Paths.IncompleteDir,
		Logger:        logger,
		PoolFactory: &poolFactory{
			repo:   serverRepo,
			logger: logger,
			dialer: bo.nntpDialer,
		},
		ConcurrencyCap:    runtime.MaxConcurrentJobs,
		FailHopelessRatio: runtime.FailHopelessRatio,
	})
	// Drain pending when the operator raises the cap from Settings.
	runtime.OnMaxConcurrentJobsChange(func(_ int) { orch.NudgePending() })

	// On-demand PAR2 recovery-vol fetching: AddJob reads the live knob
	// so toggling defer_recovery_vols from Settings → General takes
	// effect on the next upload.
	addJobService.WithDeferRecoveryVols(runtime.DeferRecoveryVols)

	verifyRepo := sqlite.NewVerifyRepo(db)
	verifySvc := appverify.New(appverify.ServiceParams{
		JobRepo:       jobRepo,
		VerifyRepo:    verifyRepo,
		Bus:           bus,
		TxManager:     txm,
		IncompleteDir: cfg.Paths.IncompleteDir,
		Logger:        logger,
	})

	deliveryRepo := sqlite.NewDeliveryRepo(db)
	categoryRepoForDeliver := sqlite.NewCategoryRepo(db)
	deliverSvc := appdeliver.New(appdeliver.ServiceParams{
		JobRepo:              jobRepo,
		DeliveryRepo:         deliveryRepo,
		CategoryRepo:         categoryRepoForDeliver,
		FS:                   adapterfs.Default,
		Bus:                  bus,
		TxManager:            txm,
		IncompleteDir:        cfg.Paths.IncompleteDir,
		CompleteDir:          cfg.Paths.CompleteDir,
		Logger:               logger,
		DeleteSamples:        runtime.DeleteSamples,
		CollapseSingleFolder: runtime.CollapseSingleFolder,
	})

	repairRepo := sqlite.NewRepairRepo(db)
	repairSvc := apprepair.New(apprepair.ServiceParams{
		JobRepo:       jobRepo,
		Repo:          repairRepo,
		Bus:           bus,
		TxManager:     txm,
		IncompleteDir: cfg.Paths.IncompleteDir,
		Logger:        logger,
	})

	extractRepo := sqlite.NewExtractRepo(db)
	categoryRepoForExtract := sqlite.NewCategoryRepo(db)
	extractor := bo.extractor
	if extractor == nil {
		extractor = adapterrar.Extractor{}
	}
	extractSvc := appextract.New(appextract.ServiceParams{
		JobRepo:       jobRepo,
		ExtractRepo:   extractRepo,
		CategoryRepo:  categoryRepoForExtract,
		Extractor:     extractor,
		Bus:           bus,
		TxManager:     txm,
		IncompleteDir: cfg.Paths.IncompleteDir,
		CompleteDir:   cfg.Paths.CompleteDir,
		Logger:        logger,
	})

	userRepo := sqlite.NewUserRepo(db)
	sessionRepo := sqlite.NewSessionRepo(db)
	hasher := &bcrypt.Hasher{}
	authSvc := appauth.New(appauth.ServiceParams{
		Users:     userRepo,
		Sessions:  sessionRepo,
		Hasher:    hasher,
		Bus:       bus,
		TxManager: txm,
	})

	categoryRepo := sqlite.NewCategoryRepo(db)

	subscriptionRepo := sqlite.NewSubscriptionRepo(db)
	notifyAdmin := appnotify.NewAdmin(subscriptionRepo, bus, txm, nil)
	notifyRouter := adapterrouter.New(map[notify.Kind]notify.Sender{
		notify.KindWebhook: adapternotify.New(),
		notify.KindDiscord: adapterdiscord.New(),
		notify.KindSlack:   adapterslack.New(),
	})
	notifySvc := appnotify.New(appnotify.ServiceParams{
		Repo:      subscriptionRepo,
		Sender:    notifyRouter,
		Bus:       bus,
		TxManager: txm,
		Logger:    logger,
		// Enable payload enrichment: webhook events carrying a job_id
		// get a "job" snapshot stapled on (name, source, bytes, etc.)
		// so consumers don't need a follow-up GET.
		Jobs: jobRepo,
	})
	notifyFacade := &notifyFacade{admin: notifyAdmin, svc: notifySvc}

	scheduleRepo := sqlite.NewScheduleRepo(db)
	scheduler := appschedule.New(scheduleRepo, appschedule.Config{
		Tick:    5 * time.Second,
		Workers: 4,
		Log:     logger,
	})
	// Built-in maintenance task: nudge SQLite's query planner with
	// PRAGMA optimize on a slow cadence. Hourly is more than enough —
	// optimize is incremental + cheap.
	scheduler.Register("sqlite.optimize", func(ctx context.Context, _ []byte) error {
		_, err := db.ExecCtx(ctx, "PRAGMA optimize")
		return err
	})
	if _, err := scheduler.EnsureTask(ctx, domainschedule.NewParams{
		Name:     "sqlite.optimize",
		Kind:     domainschedule.KindRecurring,
		Cadence:  time.Hour,
		FirstRun: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		logger.Warn("schedule: ensure sqlite.optimize", "err", err)
	}

	startedAt := time.Now().UTC()
	systemSvc := appsystem.New(appsystem.Params{
		Version:   bo.version,
		StartedAt: startedAt,
		Jobs:      jobRepo,
		// Read the live pool map every time — captures hot-wired
		// servers added at runtime so /api/v1/system/status reflects
		// reality instead of just startup state.
		PoolsSource: orch.PoolsSnapshot,
		Servers:     serverRepo,
		Throughput:  throughput,
		// Bus drives the periodic system.throughput + system.pools
		// SSE pushes so the frontend doesn't poll those endpoints.
		Bus:    bus,
		Logger: logger,
	})

	liveHub, err := sse.NewHub(bus, sse.DefaultTopics, logger)
	if err != nil {
		closePools(pools)
		_ = bus.Close()
		_ = db.Close()
		return nil, fmt.Errorf("live hub: %w", err)
	}

	srv := server.New(cfg, runtime, logger, frontendFS)
	srv.SetSessionAuthenticator(authSvc)
	srv.MountREST(&rest.Handlers{
		Queue:      queueService,
		AddJob:     addJobService,
		Servers:    serverService,
		Categories: categoryRepo,
		Auth:       authSvc,
		System:     systemSvc,
		Subscriptions: notifyFacade,
		Outbox:        bus,
		Paths: &rest.PathsView{
			DataDir:       cfg.Server.DataDir,
			IncompleteDir: cfg.Paths.IncompleteDir,
			CompleteDir:   cfg.Paths.CompleteDir,
		},
		General: &rest.GeneralView{
			Listen:   cfg.Server.Listen,
			APIKey:   cfg.Auth.APIKey,
			LogLevel: cfg.Server.LogLevel,
			SABBase:  buildSABBase(cfg.Server.Listen),
			URLBase:  cfg.Server.URLBase,
		},
		Bandwidth: bandwidthAdmin{runtime: runtime},
		LogHub:    bo.logHub,
		Logger:    logger,
		Runtime:   runtime,
	})
	srv.MountSAB(&sab.Handler{
		APIKey:      cfg.Auth.APIKey,
		Queue:       queueService,
		AddJob:      addJobService,
		Categories:  categoryRepo,
		Logger:      logger,
		CompleteDir: cfg.Paths.CompleteDir,
		// Wire the throughput tracker so SAB queue responses surface
		// kbpersec / timeleft / per-slot eta. *arr suites import jobs
		// faster when they get a real ETA instead of "unknown".
		// Use the 10-second average rather than the instantaneous
		// per-second bucket so kbpersec / timeleft don't jitter with
		// every successful fetch.
		Throughput: func() int64 {
			s := throughput.Sample()
			if s.Avg10sBytesPerSec > 0 {
				return s.Avg10sBytesPerSec
			}
			return s.CurrentBytesPerSec
		},
	})
	srv.MountSSE(liveHub)
	httpSrv := &http.Server{
		Addr:    cfg.Server.Listen,
		Handler: srv,
		// Slow-loris defence on header reads.
		ReadHeaderTimeout: 10 * time.Second,
		// Recycle idle keep-alive conns. SSE connections aren't
		// "idle" — the heartbeat (~15s) keeps them active — so this
		// cap only catches actual zombie conns.
		IdleTimeout: 90 * time.Second,
		// Deliberately unset: ReadTimeout would kill slow large
		// multipart NZB uploads; WriteTimeout would kill long-lived
		// SSE streams. We use per-handler context for timeouts.
	}

	return &App{
		Cfg:           cfg,
		Logger:        logger,
		DB:            db,
		TxMgr:         txm,
		Bus:           bus,
		ServerRepo:    serverRepo,
		JobRepo:       jobRepo,
		ServerService: serverService,
		AddJobService: addJobService,
		QueueService:  queueService,
		Pools:         pools,
		Orchestrator:  orch,
		Verify:        verifySvc,
		Repair:        repairSvc,
		Deliver:       deliverSvc,
		Extract:       extractSvc,
		Notify:        notifySvc,
		Scheduler:     scheduler,
		ByteFlusher:   byteFlusher,
		LogHub:        bo.logHub,
		AuthService:   authSvc,
		SystemService: systemSvc,
		StartedAt:     startedAt,
		LiveHub:       liveHub,
		HTTP:          srv,
		HTTPServer:    httpSrv,
		dataDirLock:   lock,
	}, nil
}

// bandwidthAdmin adapts *server.Runtime to rest.BandwidthAdmin. The
// REST handler asks "what's the cap" / "set the cap"; Runtime owns
// persistence and fans changes out to the in-memory limiter via its
// OnBandwidthGlobalChange listener.
type bandwidthAdmin struct {
	runtime *server.Runtime
}

func (a bandwidthAdmin) GlobalCap() int64 {
	return a.runtime.BandwidthGlobalCap()
}

func (a bandwidthAdmin) SetGlobalCap(v int64) {
	_, _ = a.runtime.SetBandwidthGlobalCap(v)
}

func closePools(pools map[domainserver.ServerID]*nntp.Pool) {
	for _, p := range pools {
		_ = p.Close()
	}
}

// Run starts the orchestrator service and the HTTP listener, blocking
// until ctx is cancelled or HTTP fails.
//
// On ctx cancellation, Run calls Shutdown and returns its error.
// On HTTP failure (other than ErrServerClosed), Run returns the
// failure; the caller decides whether to call Shutdown.
func (a *App) Run(ctx context.Context) error {
	a.Logger.Info(
		"hoardarr starting",
		"listen", a.Cfg.Server.Listen,
		"data_dir", a.Cfg.Server.DataDir,
		"db", a.Cfg.Storage.SQLite.Path,
	)

	if err := a.Orchestrator.Start(ctx); err != nil {
		return fmt.Errorf("start orchestrator: %w", err)
	}
	if err := a.Verify.Start(ctx); err != nil {
		return fmt.Errorf("start verify: %w", err)
	}
	if err := a.Repair.Start(ctx); err != nil {
		return fmt.Errorf("start repair: %w", err)
	}
	if err := a.Deliver.Start(ctx); err != nil {
		return fmt.Errorf("start deliver: %w", err)
	}
	if err := a.Extract.Start(ctx); err != nil {
		return fmt.Errorf("start extract: %w", err)
	}
	if err := a.Notify.Start(ctx); err != nil {
		return fmt.Errorf("start notify: %w", err)
	}
	if err := a.SystemService.Start(ctx); err != nil {
		return fmt.Errorf("start system: %w", err)
	}
	if err := a.Scheduler.Start(ctx); err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}
	a.ByteFlusher.Start(ctx)

	errCh := make(chan error, 1)
	go func() {
		if err := a.HTTPServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http listen: %w", err)
	case <-ctx.Done():
		return a.Shutdown()
	}
}

// Shutdown stops in this order: HTTP server, orchestrator service,
// NNTP pools, event bus, database. The first error encountered is
// recorded and returned, but all steps are attempted.
//
// Idempotent: only the first call performs work; later calls return
// the same error.
func (a *App) Shutdown() error {
	a.shutdownOnce.Do(func() {
		a.Logger.Info("hoardarr stopping")

		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := a.HTTPServer.Shutdown(sctx); err != nil {
			a.shutdownErr = fmt.Errorf("http shutdown: %w", err)
		}
		if a.LiveHub != nil {
			if err := a.LiveHub.Close(); err != nil && a.shutdownErr == nil {
				a.shutdownErr = fmt.Errorf("live hub close: %w", err)
			}
		}
		a.ByteFlusher.Stop()
		if err := a.Scheduler.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("stop scheduler: %w", err)
		}
		if err := a.SystemService.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("stop system: %w", err)
		}
		if err := a.Notify.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("notify stop: %w", err)
		}
		if err := a.Extract.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("extract stop: %w", err)
		}
		if err := a.Deliver.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("deliver stop: %w", err)
		}
		if err := a.Repair.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("repair stop: %w", err)
		}
		if err := a.Verify.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("verify stop: %w", err)
		}
		if err := a.Orchestrator.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("orchestrator stop: %w", err)
		}
		// Use the orchestrator's live pool snapshot — a.Pools is the
		// frozen startup set and misses anything hot-wired via UI.
		// Without this, conns from runtime-added servers linger on
		// the provider until their idle timeout (60-120s on Eweka),
		// which trips the max-connections limit during quick restarts.
		live := a.Orchestrator.PoolsSnapshot()
		for id, p := range live {
			if err := p.Close(); err != nil && a.shutdownErr == nil {
				a.shutdownErr = fmt.Errorf("pool close (server %d): %w", id, err)
			}
		}
		if err := a.Bus.Close(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("bus close: %w", err)
		}
		if err := a.DB.Close(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("db close: %w", err)
		}
		if err := a.dataDirLock.Release(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("release data-dir lock: %w", err)
		}
	})
	return a.shutdownErr
}

func ensureDirs(cfg config.Config) error {
	dirs := []string{
		cfg.Server.DataDir,
		cfg.Paths.IncompleteDir,
		cfg.Paths.CompleteDir,
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("create dir %q: %w", d, err)
		}
	}
	return nil
}

func openDB(ctx context.Context, cfg config.Config) (*sqlite.DB, error) {
	switch cfg.Storage.Backend {
	case "sqlite":
		db, err := sqlite.Open(ctx, cfg.Storage.SQLite.Path, sqlite.Options{})
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		return db, nil
	default:
		return nil, fmt.Errorf("unsupported storage.backend %q", cfg.Storage.Backend)
	}
}

// buildPools opens an nntp.Pool for every enabled server in the
// registry. Empty result is fine — orchestrator tolerates "no servers
// yet" and just sits idle.
//
// dialer overrides the network dialer for every pool. Pass nil for
// production (DefaultDialer); tests pass recording / replay wrappers.
func buildPools(ctx context.Context, repo *sqlite.ServerRepo, logger *slog.Logger, dialer nntp.Dialer) (map[domainserver.ServerID]*nntp.Pool, error) {
	servers, err := repo.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[domainserver.ServerID]*nntp.Pool, len(servers))
	for _, s := range servers {
		out[s.ID()] = nntp.NewPool(s, nntp.PoolOptions{
			Logger: logger,
			Dialer: dialer,
		})
	}
	return out, nil
}

// poolFactory implements appdownload.PoolFactory by loading the server
// aggregate from the repo and constructing a pool with the orchestrator-
// owned dialer + logger.
//
// Returns (nil, nil) when the server exists but is disabled — the
// caller treats that as "drop the existing pool for this id".
type poolFactory struct {
	repo   *sqlite.ServerRepo
	logger *slog.Logger
	dialer nntp.Dialer
}

func (f *poolFactory) BuildPool(ctx context.Context, id domainserver.ServerID) (*nntp.Pool, error) {
	srv, err := f.repo.ByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !srv.Enabled() {
		return nil, nil
	}
	return nntp.NewPool(srv, nntp.PoolOptions{
		Logger: f.logger,
		Dialer: f.dialer,
	}), nil
}
