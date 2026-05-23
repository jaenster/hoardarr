package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntptest"
	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/app/backup"
	"github.com/jaenster/hoardarr/internal/app/diskspace"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	"github.com/jaenster/hoardarr/internal/logfile"
	appnotify "github.com/jaenster/hoardarr/internal/app/notify"
	"github.com/jaenster/hoardarr/internal/domain/event"
	domaincommand "github.com/jaenster/hoardarr/internal/domain/command"
	domainhealth "github.com/jaenster/hoardarr/internal/domain/health"
	domainschedule "github.com/jaenster/hoardarr/internal/domain/schedule"
	"github.com/jaenster/hoardarr/internal/loghub"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	appsystem "github.com/jaenster/hoardarr/internal/app/system"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/notify"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// Handlers groups dependencies for the REST API surface.
type Handlers struct {
	Queue         *appdownload.QueueService
	AddJob        *appdownload.AddJobService
	Servers       *appserver.Service
	Categories    *sqlite.CategoryRepo
	Auth          Auther         // optional; nil disables /api/v1/auth/*
	System        SystemStatuser   // optional; nil disables /api/v1/system/status
	Health        HealthSnapshotter // optional; nil disables /api/v1/system/health
	Schedule      ScheduleAdmin    // optional; nil disables /api/v1/system/tasks
	DiskSources   []diskspace.Source // empty disables /api/v1/system/diskspace
	LogDir        string             // empty disables /api/v1/system/logs/files
	Commands      CommandAdmin       // optional; nil disables /api/v1/commands
	Backup        BackupAdmin        // optional; nil disables /api/v1/system/backups
	Paths         *PathsView     // optional; nil disables /api/v1/config/paths
	General       *GeneralView   // optional; nil disables /api/v1/config/general
	Bandwidth     BandwidthAdmin // optional; nil disables /api/v1/config/bandwidth
	Subscriptions Subscriptions  // optional; nil disables /api/v1/subscriptions
	Outbox        EventReader    // optional; nil disables /api/v1/queue/{id}/events
	LogHub        *loghub.Hub    // optional; nil disables /api/v1/system/logs*
	Logger        *slog.Logger
	// Runtime is the runtime-mutable config view. Currently provides
	// URLBase (live-editable from Settings) so session cookies and
	// the General response stay in sync after the operator changes
	// the URL base from the UI. May be nil for tests that don't
	// care; in that case the cookie path defaults to "/".
	Runtime URLBaseReader
}

// URLBaseReader is the slice of *server.Runtime that REST needs.
// Defined here as an interface so the API package doesn't depend on
// internal/server (which would be a cycle).
type URLBaseReader interface {
	URLBase() string
	MaxConcurrentJobs() int
	FailHopelessRatio() float64
	DeferRecoveryVols() bool
	DeleteSamples() bool
	CollapseSingleFolder() bool
	APIKey() string
}

// URLBaseWriter is implemented by *server.Runtime and exposes the
// mutation side of the runtime config to the Settings handler. The
// Handlers field uses URLBaseReader for the common path; the
// Settings handler type-asserts to URLBaseWriter when it needs to
// mutate.
type URLBaseWriter interface {
	URLBaseReader
	SetURLBase(v string) (string, error)
	SetMaxConcurrentJobs(v int) (int, error)
	SetFailHopelessRatio(v float64) (float64, error)
	SetDeferRecoveryVols(v bool) (bool, error)
	SetDeleteSamples(v bool) (bool, error)
	SetCollapseSingleFolder(v bool) (bool, error)
	RotateAPIKey() (string, error)
}

// sessionCookiePath returns the Path attribute for the session
// cookie. Always trailing-slash terminated so the browser includes
// every URL under the prefix.
func (h *Handlers) sessionCookiePath() string {
	base := ""
	if h.Runtime != nil {
		base = h.Runtime.URLBase()
	}
	if base == "" {
		return "/"
	}
	return base + "/"
}

// EventReader is the slice of the outbox bus that the per-job
// timeline endpoint needs.
type EventReader interface {
	EventsByJob(ctx context.Context, jobID int64) ([]event.Envelope, error)
}

// BandwidthAdmin is the slice of the download.Limiter that REST needs
// to expose runtime control of the global cap.
type BandwidthAdmin interface {
	GlobalCap() int64
	SetGlobalCap(bytesPerSec int64)
}

// Subscriptions is the slice of app/notify the REST handler needs.
type Subscriptions interface {
	List(ctx context.Context) ([]*notify.Subscription, error)
	Add(ctx context.Context, cmd appnotify.AddCmd) (notify.SubscriptionID, error)
	Update(ctx context.Context, id notify.SubscriptionID, cmd appnotify.UpdateCmd) error
	Remove(ctx context.Context, id notify.SubscriptionID) error
	SetEnabled(ctx context.Context, id notify.SubscriptionID, enabled bool) error
	Test(ctx context.Context, id notify.SubscriptionID) error
}

// PathsView exposes the resolved data and category dirs for read-only
// display in the UI. Mutation lives at the config.toml layer (M6 will
// promote it to runtime once we have a safe drain strategy).
type PathsView struct {
	DataDir       string
	IncompleteDir string
	CompleteDir   string
}

// GeneralView surfaces the slice of runtime config the UI needs to
// render a meaningful General settings panel and the SAB-compat tab.
// The API key is read live from Runtime (rotated via Settings →
// Authentication) so the response always reflects the current value.
type GeneralView struct {
	Listen   string
	LogLevel string
	SABBase  string // e.g. "http://hoardarr:8085/sabnzbd/api"
	URLBase  string // reverse-proxy mount prefix; empty when at root
}

// SystemStatuser is the slice of app/system.Service that the REST
// handler needs. Defined here as an interface so tests can pass a
// fake without dragging the full service in.
type SystemStatuser interface {
	Status(ctx context.Context) (appsystem.Status, error)
	Throughput() *appsystem.Throughput
	History() appsystem.SpeedHistoryStore
}

// BackupAdmin is the slice of app/backup.Service the REST handler
// needs. List + on-demand Run + SafePath enforcement for downloads.
type BackupAdmin interface {
	List() []backup.FileInfo
	Run(ctx context.Context) error
	SafePath(name string) (string, error)
}

// CommandAdmin is the slice of app/command.Service the REST handler
// needs. Defined as an interface so a fake can be plugged in tests.
type CommandAdmin interface {
	Submit(ctx context.Context, name string, body []byte, trigger domaincommand.Trigger) (domaincommand.CommandID, error)
	List(ctx context.Context, limit int) ([]*domaincommand.Command, error)
	ByID(ctx context.Context, id domaincommand.CommandID) (*domaincommand.Command, error)
	Names() []string
}

// ScheduleAdmin is the slice of the schedule repo + service the REST
// handler needs to surface the Tasks page.
type ScheduleAdmin interface {
	List(ctx context.Context) ([]*domainschedule.Task, error)
	ByID(ctx context.Context, id domainschedule.TaskID) (*domainschedule.Task, error)
	Save(ctx context.Context, t *domainschedule.Task) error
}

// HealthSnapshotter is the slice of app/health.Service the REST
// handler needs. Returns the current issue snapshot + the timestamp
// of the last check run (so the UI can show "checked Xs ago" and
// detect a stuck check loop).
type HealthSnapshotter interface {
	Snapshot() ([]domainhealth.Issue, time.Time)
	Refresh()
}

// Mount registers the /api/v1/* routes on mux. The caller is responsible
// for wrapping individual routes with the api-key middleware (the
// `protect` helper passed in). /api/v1/health is NOT registered here —
// that lives in the server package as a public liveness probe.
//
// The /api/v1/auth/* endpoints are registered when Handlers.Auth is
// non-nil. setup, login, and whoami are public; logout is protected.
func (h *Handlers) Mount(mux *http.ServeMux, protect func(http.Handler) http.Handler) {
	register := func(method, pattern string, fn http.HandlerFunc) {
		mux.Handle(method+" "+pattern, protect(fn))
	}

	// Auth (mostly public — see mountAuth).
	if h.Auth != nil {
		h.mountAuth(mux, protect)
	}

	// Queue.
	register("GET", "/api/v1/queue", h.listQueue)
	register("GET", "/api/v1/queue/{id}", h.getJob)
	register("POST", "/api/v1/queue/nzb", h.addNZB)
	register("POST", "/api/v1/queue/{id}/pause", h.pauseJob)
	register("POST", "/api/v1/queue/{id}/resume", h.resumeJob)
	register("POST", "/api/v1/queue/reorder", h.reorderQueue)
	register("DELETE", "/api/v1/queue/{id}", h.removeJob)
	if h.Outbox != nil {
		register("GET", "/api/v1/queue/{id}/events", h.jobEvents)
	}

	// History.
	register("GET", "/api/v1/history", h.listHistory)

	// Servers.
	register("GET", "/api/v1/servers", h.listServers)
	register("POST", "/api/v1/servers", h.addServer)
	register("PATCH", "/api/v1/servers/{id}", h.patchServer)
	register("DELETE", "/api/v1/servers/{id}", h.removeServer)
	register("POST", "/api/v1/servers/test", h.testServer)
	register("POST", "/api/v1/servers/{id}/test", h.testExistingServer)
	register("POST", "/api/v1/servers/{id}/enable", h.enableServer)
	register("POST", "/api/v1/servers/{id}/disable", h.disableServer)

	// Categories.
	register("GET", "/api/v1/categories", h.listCategories)
	register("POST", "/api/v1/categories", h.upsertCategory)
	register("DELETE", "/api/v1/categories/{name}", h.removeCategory)

	// System status.
	if h.System != nil {
		register("GET", "/api/v1/system/status", h.systemStatus)
		register("GET", "/api/v1/system/throughput", h.systemThroughput)
		register("GET", "/api/v1/system/speed-history", h.systemSpeedHistory)
	}
	if h.Health != nil {
		register("GET", "/api/v1/system/health", h.systemHealth)
		register("POST", "/api/v1/system/health/refresh", h.systemHealthRefresh)
	}
	if h.Schedule != nil {
		register("GET", "/api/v1/system/tasks", h.systemTasks)
		register("POST", "/api/v1/system/tasks/{id}/run-now", h.systemTaskRunNow)
	}
	if len(h.DiskSources) > 0 {
		register("GET", "/api/v1/system/diskspace", h.systemDiskspace)
	}
	if h.LogDir != "" {
		register("GET", "/api/v1/system/logs/files", h.systemLogFiles)
		register("GET", "/api/v1/system/logs/files/{name}", h.systemLogFileDownload)
	}
	if h.Commands != nil {
		register("GET", "/api/v1/commands", h.listCommands)
		register("POST", "/api/v1/commands", h.submitCommand)
		register("GET", "/api/v1/commands/{id}", h.getCommand)
		register("GET", "/api/v1/commands/names", h.commandNames)
	}
	if h.Backup != nil {
		register("GET", "/api/v1/system/backups", h.listBackups)
		register("POST", "/api/v1/system/backups", h.runBackup)
		register("GET", "/api/v1/system/backups/{name}", h.downloadBackup)
	}
	if h.LogHub != nil {
		register("GET", "/api/v1/system/logs", h.systemLogsSnapshot)
		// /tail (preferred) and /stream (back-compat). Adblock filter
		// lists frequently match "stream"; the SSE handler is the same.
		register("GET", "/api/v1/system/logs/tail", h.systemLogsStream)
		register("GET", "/api/v1/system/logs/stream", h.systemLogsStream)
	}

	// Paths (read-only). Edits require a config.toml change + restart;
	// runtime mutation is unsafe while jobs hold open files in incomplete/.
	if h.Paths != nil {
		register("GET", "/api/v1/config/paths", h.getPaths)
	}

	// General config view: listen addr, API key, log level. Read-only;
	// mutation lives at config.toml + restart, same as paths.
	if h.General != nil {
		register("GET", "/api/v1/config/general", h.getGeneral)
		register("PUT", "/api/v1/config/general", h.putGeneral)
	}

	// Bandwidth global cap is runtime-mutable (token bucket reconfigures
	// in place).
	if h.Bandwidth != nil {
		register("GET", "/api/v1/config/bandwidth", h.getBandwidth)
		register("PUT", "/api/v1/config/bandwidth", h.setBandwidth)
	}

	// Subscriptions (webhooks).
	if h.Subscriptions != nil {
		register("GET", "/api/v1/subscriptions", h.listSubscriptions)
		register("POST", "/api/v1/subscriptions", h.addSubscription)
		register("PATCH", "/api/v1/subscriptions/{id}", h.patchSubscription)
		register("DELETE", "/api/v1/subscriptions/{id}", h.removeSubscription)
		register("POST", "/api/v1/subscriptions/{id}/test", h.testSubscription)
		register("POST", "/api/v1/subscriptions/{id}/enable", h.enableSubscription)
		register("POST", "/api/v1/subscriptions/{id}/disable", h.disableSubscription)
	}
}

// --- queue ----------------------------------------------------------

func (h *Handlers) listQueue(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	includeAll := r.URL.Query().Get("include") == "all"
	var jobs []*download.Job
	var err error
	// JobsOnly loads — no files, no segments. The wire DTO doesn't
	// emit per-file rows in the list response anyway (the UI only
	// reads them on the job-detail page, which uses /api/v1/queue/{id}
	// with full hydration). Skipping the N file queries per call is
	// the dominant win under Sonarr/Radarr polling pressure.
	if includeAll {
		jobs, err = h.Queue.ListJobsOnly(ctx)
	} else {
		jobs, err = h.Queue.ActiveJobsOnly(ctx)
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]JobDTO, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToDTO(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (h *Handlers) addNZB(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// 256 MiB is generous for any real NZB. The largest releases I've
	// seen ship NZBs around 5-10 MB; 256 MiB leaves a wide safety
	// margin without exposing us to memory exhaustion via a single
	// hostile request.
	const maxNZBBytes = 256 << 20
	if err := r.ParseMultipartForm(maxNZBBytes); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("parse form: %w", err))
		return
	}
	file, header, err := r.FormFile("nzb")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("nzb file required: %w", err))
		return
	}
	defer file.Close()

	// Display name = the uploaded filename minus the ".nzb" suffix.
	// Operators upload "Release.Name.S01E01.1080p.WEB-DL.nzb"; that's
	// what they want to see in the queue, not an arbitrary internal
	// file ID picked out of the NZB's <file> list.
	var displayName string
	if header != nil {
		displayName = strings.TrimSuffix(header.Filename, ".nzb")
		displayName = strings.TrimSuffix(displayName, ".NZB")
	}

	id, err := h.AddJob.AddJob(ctx, appdownload.AddJobCmd{
		NZB:      file,
		Name:     displayName,
		Category: r.FormValue("category"),
		Source:   r.UserAgent(),
	})
	if err != nil {
		switch {
		case errors.Is(err, appdownload.ErrDuplicateNZB):
			// Look up the existing job's state so the UI can phrase
			// the toast — "already in queue" vs "already completed".
			// Fall back to just the id if the lookup fails.
			out := map[string]any{
				"job_id":    int64(id),
				"duplicate": true,
			}
			if j, lerr := h.Queue.Get(ctx, id); lerr == nil && j != nil {
				out["state"] = string(j.State())
				out["name"] = j.Name()
			}
			writeJSON(w, http.StatusOK, out)
			return
		default:
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"job_id": int64(id)})
}

// listHistory returns terminal-state jobs (completed/failed/aborted)
// ordered by finished_at DESC. Query params:
//
//	?since=<RFC3339>   only jobs finished after this instant
//	?category=<name>   exact-match filter
//	?state=<terminal>  one of completed|failed|aborted
//	?limit=<n>         clamped server-side to [1, 500]
func (h *Handlers) listHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	hq := download.HistoryQuery{
		Category: strings.TrimSpace(q.Get("category")),
	}
	if s := strings.TrimSpace(q.Get("state")); s != "" {
		hq.State = download.JobState(s)
	}
	if s := strings.TrimSpace(q.Get("since")); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, fmt.Errorf("since: %w", err))
			return
		}
		hq.Since = &t
	}
	if s := strings.TrimSpace(q.Get("limit")); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, fmt.Errorf("limit: %w", err))
			return
		}
		hq.Limit = n
	}
	jobs, err := h.Queue.HistoryJobsOnly(r.Context(), hq)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]JobDTO, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToDTO(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

// jobEvents returns the bus envelopes (download.*, verify.*, repair.*,
// deliver.*, extract.*) touching one job, ordered by occurred_at.
// Used by the UI's per-job timeline view.
func (h *Handlers) jobEvents(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	events, err := h.Outbox.EventsByJob(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	// We surface the envelope shape directly; the UI knows the
	// internal layout because it consumes the same shapes via SSE.
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// getJob returns a single job with full file + segment hydration —
// the queue list path was changed to JobsOnly for perf and no longer
// embeds files, so the JobDetail page relies on this endpoint to
// render the per-file breakdown.
func (h *Handlers) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	job, err := h.Queue.Get(r.Context(), download.JobID(id))
	if err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": jobToDTO(job)})
}

func (h *Handlers) pauseJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Queue.PauseJob(r.Context(), download.JobID(id)); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) resumeJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Queue.ResumeJob(r.Context(), download.JobID(id)); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type reorderQueueReq struct {
	IDs []int64 `json:"ids"`
}

func (h *Handlers) reorderQueue(w http.ResponseWriter, r *http.Request) {
	var req reorderQueueReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if len(req.IDs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	ids := make([]download.JobID, len(req.IDs))
	for i, id := range req.IDs {
		ids[i] = download.JobID(id)
	}
	if err := h.Queue.Reorder(r.Context(), ids); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) removeJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Queue.RemoveJob(r.Context(), download.JobID(id)); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- servers --------------------------------------------------------

func (h *Handlers) listServers(w http.ResponseWriter, r *http.Request) {
	servers, err := h.Servers.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]ServerDTO, 0, len(servers))
	for _, s := range servers {
		out = append(out, serverToDTO(s))
	}
	writeJSON(w, http.StatusOK, map[string]any{"servers": out})
}

type addServerReq struct {
	Name                 string `json:"name"`
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	TLS                  *bool  `json:"tls,omitempty"`
	Username             string `json:"username,omitempty"`
	Password             string `json:"password,omitempty"`
	MaxConns             int    `json:"max_conns,omitempty"`
	Priority             int    `json:"priority,omitempty"`
	Backup               bool   `json:"backup,omitempty"`
	BillingMode          string `json:"billing_mode,omitempty"` // "flat" | "metered"; empty => flat
	QuotaBytes           int64  `json:"quota_bytes,omitempty"`
	BandwidthBytesPerSec int64  `json:"bandwidth_bytes_per_sec,omitempty"`
}

func (h *Handlers) addServer(w http.ResponseWriter, r *http.Request) {
	var req addServerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.Host) == "" || req.Port == 0 {
		h.writeError(w, http.StatusBadRequest, errors.New("name, host, port required"))
		return
	}
	id, err := h.Servers.Add(r.Context(), appserver.AddCmd{
		Name:                 req.Name,
		Host:                 req.Host,
		Port:                 req.Port,
		TLS:                  req.TLS,
		Username:             req.Username,
		Password:             req.Password,
		MaxConns:             req.MaxConns,
		Priority:             req.Priority,
		Backup:               req.Backup,
		BillingMode:          domainserver.BillingMode(req.BillingMode),
		QuotaBytes:           req.QuotaBytes,
		BandwidthBytesPerSec: req.BandwidthBytesPerSec,
	})
	if err != nil {
		switch {
		case errors.Is(err, appserver.ErrNameTaken):
			h.writeError(w, http.StatusConflict, err)
		default:
			h.writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": int64(id)})
}

// patchServerReq mirrors UpdateCmd — every field is a pointer so
// "absent" is distinguishable from "set to zero value". The JSON
// decoder leaves nil pointers nil when the key isn't present.
type patchServerReq struct {
	Host                 *string `json:"host,omitempty"`
	Port                 *int    `json:"port,omitempty"`
	TLS                  *bool   `json:"tls,omitempty"`
	Username             *string `json:"username,omitempty"`
	Password             *string `json:"password,omitempty"`
	MaxConns             *int    `json:"max_conns,omitempty"`
	Priority             *int    `json:"priority,omitempty"`
	Backup               *bool   `json:"backup,omitempty"`
	BillingMode          *string `json:"billing_mode,omitempty"`
	QuotaBytes           *int64  `json:"quota_bytes,omitempty"`
	BandwidthBytesPerSec *int64  `json:"bandwidth_bytes_per_sec,omitempty"`
}

func (h *Handlers) patchServer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	var req patchServerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	cmd := appserver.UpdateCmd{
		ID:                   domainserver.ServerID(id),
		Host:                 req.Host,
		Port:                 req.Port,
		TLS:                  req.TLS,
		Username:             req.Username,
		Password:             req.Password,
		MaxConns:             req.MaxConns,
		Priority:             req.Priority,
		Backup:               req.Backup,
		QuotaBytes:           req.QuotaBytes,
		BandwidthBytesPerSec: req.BandwidthBytesPerSec,
	}
	if req.BillingMode != nil {
		bm := domainserver.BillingMode(*req.BillingMode)
		cmd.BillingMode = &bm
	}
	if err := h.Servers.Update(r.Context(), cmd); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) enableServer(w http.ResponseWriter, r *http.Request) {
	h.setServerEnabled(w, r, true)
}
func (h *Handlers) disableServer(w http.ResponseWriter, r *http.Request) {
	h.setServerEnabled(w, r, false)
}
func (h *Handlers) setServerEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Servers.SetEnabled(r.Context(), domainserver.ServerID(id), enabled); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Test connection ------------------------------------------------

type testServerReq struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	TLS      *bool  `json:"tls,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type testServerResp struct {
	OK         bool   `json:"ok"`
	Dial       bool   `json:"dial"`
	Greeted    bool   `json:"greeted"`
	Auth       bool   `json:"auth"`
	ModeRdr    bool   `json:"mode_reader"`
	Date       bool   `json:"date"`
	ServerDate string `json:"server_date,omitempty"`
	Err        string `json:"err,omitempty"`
	ElapsedMs  int64  `json:"elapsed_ms"`
}

func (h *Handlers) testServer(w http.ResponseWriter, r *http.Request) {
	var req testServerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if strings.TrimSpace(req.Host) == "" || req.Port == 0 {
		h.writeError(w, http.StatusBadRequest, errors.New("host + port required"))
		return
	}
	tls := true
	if req.TLS != nil {
		tls = *req.TLS
	}
	result := nntptest.Probe(r.Context(), nntptest.Params{
		Host: req.Host, Port: req.Port, TLS: tls,
		Username: req.Username, Password: req.Password,
	})
	writeJSON(w, http.StatusOK, probeResultToResp(result))
}

// testExistingServer probes a saved server using its stored creds.
// Useful from the per-row Test button — operator doesn't have to
// re-type the password.
func (h *Handlers) testExistingServer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	srv, err := h.Servers.Get(r.Context(), domainserver.ServerID(id))
	if err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	result := nntptest.Probe(r.Context(), nntptest.Params{
		Host: srv.Host(), Port: srv.Port(), TLS: srv.TLS(),
		Username: srv.Username(), Password: srv.Password(),
	})
	writeJSON(w, http.StatusOK, probeResultToResp(result))
}

func probeResultToResp(r nntptest.Result) testServerResp {
	return testServerResp{
		OK:         r.OK,
		Dial:       r.Dial,
		Greeted:    r.Greeted,
		Auth:       r.Auth,
		ModeRdr:    r.ModeReader,
		Date:       r.Date,
		ServerDate: r.ServerDate,
		Err:        r.Err,
		ElapsedMs:  r.Elapsed.Milliseconds(),
	}
}

func (h *Handlers) removeServer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Servers.Remove(r.Context(), domainserver.ServerID(id)); err != nil {
		h.writeError(w, statusFor(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- system throughput ----------------------------------------------

func (h *Handlers) systemThroughput(w http.ResponseWriter, _ *http.Request) {
	cap := int64(0)
	if h.Bandwidth != nil {
		cap = h.Bandwidth.GlobalCap()
	}
	tp := h.System.Throughput()
	if tp == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"window_seconds":             appsystem.DefaultSampleSeconds,
			"series":                     []int64{},
			"total_bytes":                0,
			"current_bytes_per_sec":      0,
			"avg10s_bytes_per_sec":       0,
			"avg60s_bytes_per_sec":       0,
			"peak_window_bytes_per_sec":  0,
			"peak_alltime_bytes_per_sec": 0,
			"global_cap_bytes_per_sec":   cap,
		})
		return
	}
	s := tp.Sample()
	writeJSON(w, http.StatusOK, map[string]any{
		"window_seconds":             s.WindowSeconds,
		"series":                     s.Series,
		"total_bytes":                s.Total,
		"current_bytes_per_sec":      s.CurrentBytesPerSec,
		"avg10s_bytes_per_sec":       s.Avg10sBytesPerSec,
		"avg60s_bytes_per_sec":       s.Avg60sBytesPerSec,
		"peak_window_bytes_per_sec":  s.WindowPeakBytesPerSec,
		"peak_alltime_bytes_per_sec": tp.AllTimePeak(),
		"global_cap_bytes_per_sec":   cap,
	})
}

// --- system speed history -------------------------------------------

// speedHistoryRanges enumerates the allowed ?range values and their
// total span in seconds. The handler picks an in-memory vs DB source
// based on whether the span fits inside the throughput ring.
var speedHistoryRanges = map[string]time.Duration{
	"5m":  5 * time.Minute,
	"1h":  time.Hour,
	"6h":  6 * time.Hour,
	"24h": 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
}

func (h *Handlers) systemSpeedHistory(w http.ResponseWriter, r *http.Request) {
	rangeKey := r.URL.Query().Get("range")
	if rangeKey == "" {
		rangeKey = "5m"
	}
	span, ok := speedHistoryRanges[rangeKey]
	if !ok {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("range %q not one of 5m, 1h, 6h, 24h, 7d", rangeKey))
		return
	}

	cap := int64(0)
	if h.Bandwidth != nil {
		cap = h.Bandwidth.GlobalCap()
	}

	tp := h.System.Throughput()
	now := time.Now().UTC()

	// In-memory ring covers up to 1 hour at 1-second resolution.
	// Anything longer falls back to the persistent 1-minute store.
	if span <= time.Duration(appsystem.WindowSize)*time.Second && tp != nil {
		seconds := int(span / time.Second)
		s := tp.SampleRange(seconds)
		samples := make([]appsystem.SpeedSample, len(s.Series))
		for i, v := range s.Series {
			samples[i] = appsystem.SpeedSample{
				At:          now.Add(-time.Duration(len(s.Series)-1-i) * time.Second).Truncate(time.Second),
				BytesPerSec: v,
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"range":                      rangeKey,
			"resolution_seconds":         1,
			"samples":                    samples,
			"peak_window_bytes_per_sec":  s.WindowPeakBytesPerSec,
			"peak_alltime_bytes_per_sec": tp.AllTimePeak(),
			"global_cap_bytes_per_sec":   cap,
		})
		return
	}

	// DB-backed range. Returns 1-minute resolution samples.
	store := h.System.History()
	var samples []appsystem.SpeedSample
	if store != nil {
		from := now.Add(-span).Truncate(time.Minute)
		to := now.Truncate(time.Minute)
		got, err := store.Range(r.Context(), from, to)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, err)
			return
		}
		samples = got
	}

	var windowPeak int64
	for _, s := range samples {
		if s.BytesPerSec > windowPeak {
			windowPeak = s.BytesPerSec
		}
	}
	var allTimePeak int64
	if tp != nil {
		allTimePeak = tp.AllTimePeak()
	}
	if samples == nil {
		samples = []appsystem.SpeedSample{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"range":                      rangeKey,
		"resolution_seconds":         60,
		"samples":                    samples,
		"peak_window_bytes_per_sec":  windowPeak,
		"peak_alltime_bytes_per_sec": allTimePeak,
		"global_cap_bytes_per_sec":   cap,
	})
}

// --- system logs ----------------------------------------------------

// systemLogsSnapshot returns the current ring contents oldest -> newest.
// Used by the System page on first load. The client then opens the
// SSE stream below for live tail.
func (h *Handlers) systemLogsSnapshot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"entries": h.LogHub.Snapshot(),
	})
}

// systemLogsStream is an SSE endpoint that flushes one event per log
// record. The Subscribe channel is drained per ctx cancel.
func (h *Handlers) systemLogsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	ch, cancel := h.LogHub.Subscribe()
	defer cancel()

	// Send a tiny initial event so the client knows the stream is alive.
	_, _ = fmt.Fprintf(w, "event: ready\ndata: {}\n\n")
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			b, err := json.Marshal(e)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "event: log\ndata: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// --- system status --------------------------------------------------

func (h *Handlers) systemStatus(w http.ResponseWriter, r *http.Request) {
	st, err := h.System.Status(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	pools := make([]map[string]any, 0, len(st.Pools))
	for _, p := range st.Pools {
		pools = append(pools, map[string]any{
			"server_id":    int64(p.ServerID),
			"server_name":  p.ServerName,
			"host":         p.Host,
			"port":         p.Port,
			"max_conns":    p.MaxConns,
			"in_use":       p.InUse,
			"idle":         p.Idle,
			"enabled":      p.Enabled,
			"backup":       p.Backup,
			"billing_mode": p.BillingMode,
			"quota_bytes":  p.QuotaBytes,
			"used_bytes":   p.UsedBytes,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service":           st.Service,
		"version":           st.Version,
		"commit":            st.Commit,
		"build_date":        st.BuildDate,
		"runtime_version":   st.RuntimeVersion,
		"os":                st.OS,
		"arch":              st.Arch,
		"is_docker":         st.IsDocker,
		"database_type":     st.DatabaseType,
		"migration_version": st.MigrationVersion,
		"started_at":        st.StartedAt.Format(time.RFC3339),
		"uptime_ms":         st.Uptime.Milliseconds(),
		"queue": map[string]any{
			"active": st.QueueActive,
			"total":  st.QueueTotal,
		},
		"pools": pools,
	})
}

// --- bandwidth ------------------------------------------------------

func (h *Handlers) getBandwidth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"global_bytes_per_sec": h.Bandwidth.GlobalCap(),
	})
}

type setBandwidthReq struct {
	GlobalBytesPerSec int64 `json:"global_bytes_per_sec"`
}

func (h *Handlers) setBandwidth(w http.ResponseWriter, r *http.Request) {
	var req setBandwidthReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if req.GlobalBytesPerSec < 0 {
		h.writeError(w, http.StatusBadRequest, errors.New("global_bytes_per_sec must be >= 0"))
		return
	}
	h.Bandwidth.SetGlobalCap(req.GlobalBytesPerSec)
	writeJSON(w, http.StatusOK, map[string]any{
		"global_bytes_per_sec": req.GlobalBytesPerSec,
	})
}

// --- general config (read-only) -------------------------------------

func (h *Handlers) getGeneral(w http.ResponseWriter, _ *http.Request) {
	// URLBase + MaxConcurrentJobs are runtime-mutable; prefer the
	// Runtime view over the frozen snapshot in GeneralView so the
	// response reflects any edits applied since startup.
	urlBase := h.General.URLBase
	maxConcurrent := 0
	failHopeless := 0.0
	deferVols := false
	delSamples := false
	collapse := false
	if h.Runtime != nil {
		urlBase = h.Runtime.URLBase()
		maxConcurrent = h.Runtime.MaxConcurrentJobs()
		failHopeless = h.Runtime.FailHopelessRatio()
		deferVols = h.Runtime.DeferRecoveryVols()
		delSamples = h.Runtime.DeleteSamples()
		collapse = h.Runtime.CollapseSingleFolder()
	}
	apiKey := ""
	if h.Runtime != nil {
		apiKey = h.Runtime.APIKey()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"listen":                 h.General.Listen,
		"api_key":                apiKey,
		"log_level":              h.General.LogLevel,
		"sab_base":               h.General.SABBase,
		"url_base":               urlBase,
		"max_concurrent_jobs":    maxConcurrent,
		"fail_hopeless_ratio":    failHopeless,
		"defer_recovery_vols":    deferVols,
		"delete_samples":         delSamples,
		"collapse_single_folder": collapse,
	})
}

type putGeneralReq struct {
	URLBase              *string  `json:"url_base,omitempty"`
	MaxConcurrentJobs    *int     `json:"max_concurrent_jobs,omitempty"`
	FailHopelessRatio    *float64 `json:"fail_hopeless_ratio,omitempty"`
	DeferRecoveryVols    *bool    `json:"defer_recovery_vols,omitempty"`
	DeleteSamples        *bool    `json:"delete_samples,omitempty"`
	CollapseSingleFolder *bool    `json:"collapse_single_folder,omitempty"`
}

// putGeneral applies runtime-mutable General settings. Currently only
// url_base is editable; future fields land here without changing the
// route. Persists to config.toml so the change survives restart.
func (h *Handlers) putGeneral(w http.ResponseWriter, r *http.Request) {
	if h.Runtime == nil {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("runtime config unavailable"))
		return
	}
	writer, ok := h.Runtime.(URLBaseWriter)
	if !ok {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("runtime config is read-only"))
		return
	}
	var req putGeneralReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if req.URLBase != nil {
		if _, err := writer.SetURLBase(*req.URLBase); err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	if req.MaxConcurrentJobs != nil {
		if _, err := writer.SetMaxConcurrentJobs(*req.MaxConcurrentJobs); err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	if req.FailHopelessRatio != nil {
		if _, err := writer.SetFailHopelessRatio(*req.FailHopelessRatio); err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	if req.DeferRecoveryVols != nil {
		if _, err := writer.SetDeferRecoveryVols(*req.DeferRecoveryVols); err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	if req.DeleteSamples != nil {
		if _, err := writer.SetDeleteSamples(*req.DeleteSamples); err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	if req.CollapseSingleFolder != nil {
		if _, err := writer.SetCollapseSingleFolder(*req.CollapseSingleFolder); err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- subscriptions / webhooks ---------------------------------------

func (h *Handlers) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := h.Subscriptions.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]SubscriptionDTO, 0, len(subs))
	for _, s := range subs {
		out = append(out, subscriptionToDTO(s))
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

type addSubscriptionReq struct {
	Name   string   `json:"name"`
	Kind   string   `json:"kind,omitempty"` // empty -> "webhook"
	URL    string   `json:"url"`
	Topics []string `json:"topics"`
	Secret string   `json:"secret,omitempty"`
}

func (h *Handlers) addSubscription(w http.ResponseWriter, r *http.Request) {
	var req addSubscriptionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	kind := notify.Kind(req.Kind)
	if kind == "" {
		kind = notify.KindWebhook
	}
	id, err := h.Subscriptions.Add(r.Context(), appnotify.AddCmd{
		Name:   req.Name,
		Kind:   kind,
		URL:    req.URL,
		Topics: req.Topics,
		Secret: req.Secret,
	})
	if err != nil {
		switch {
		case errors.Is(err, appnotify.ErrNameTaken):
			h.writeError(w, http.StatusConflict, err)
		default:
			h.writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": int64(id)})
}

type patchSubscriptionReq struct {
	URL     *string   `json:"url,omitempty"`
	Topics  *[]string `json:"topics,omitempty"`
	Secret  *string   `json:"secret,omitempty"`
	Enabled *bool     `json:"enabled,omitempty"`
}

func (h *Handlers) patchSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	var req patchSubscriptionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if err := h.Subscriptions.Update(r.Context(), notify.SubscriptionID(id), appnotify.UpdateCmd{
		URL:     req.URL,
		Topics:  req.Topics,
		Secret:  req.Secret,
		Enabled: req.Enabled,
	}); err != nil {
		if errors.Is(err, notify.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, err)
			return
		}
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) removeSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Subscriptions.Remove(r.Context(), notify.SubscriptionID(id)); err != nil {
		if errors.Is(err, notify.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) testSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Subscriptions.Test(r.Context(), notify.SubscriptionID(id)); err != nil {
		h.writeError(w, http.StatusBadGateway, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) enableSubscription(w http.ResponseWriter, r *http.Request) {
	h.setSubscriptionEnabled(w, r, true)
}

func (h *Handlers) disableSubscription(w http.ResponseWriter, r *http.Request) {
	h.setSubscriptionEnabled(w, r, false)
}

func (h *Handlers) setSubscriptionEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Subscriptions.SetEnabled(r.Context(), notify.SubscriptionID(id), enabled); err != nil {
		if errors.Is(err, notify.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- paths (read-only) ----------------------------------------------

func (h *Handlers) getPaths(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"data_dir":           h.Paths.DataDir,
		"incomplete_dir":     h.Paths.IncompleteDir,
		"complete_dir":       h.Paths.CompleteDir,
		"runtime_mutable":    false,
		"requires_restart":   true,
	})
}

// --- categories -----------------------------------------------------

func (h *Handlers) listCategories(w http.ResponseWriter, r *http.Request) {
	cats, err := h.Categories.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]CategoryDTO, 0, len(cats))
	for _, c := range cats {
		out = append(out, categoryToDTO(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"categories": out})
}

type upsertCategoryReq struct {
	Name     string `json:"name"`
	Dir      string `json:"dir,omitempty"`
	Priority int    `json:"priority,omitempty"`
}

func (h *Handlers) upsertCategory(w http.ResponseWriter, r *http.Request) {
	var req upsertCategoryReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	c := sqlite.Category{
		Name:     strings.TrimSpace(req.Name),
		Dir:      strings.TrimSpace(req.Dir),
		Priority: req.Priority,
	}
	if err := h.Categories.Save(r.Context(), c); err != nil {
		switch {
		case errors.Is(err, sqlite.ErrCategoryNameInvalid):
			h.writeError(w, http.StatusBadRequest, err)
		default:
			h.writeError(w, http.StatusInternalServerError, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, categoryToDTO(c))
}

func (h *Handlers) removeCategory(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, errors.New("name required"))
		return
	}
	if err := h.Categories.Delete(r.Context(), name); err != nil {
		switch {
		case errors.Is(err, sqlite.ErrCategoryNotFound):
			h.writeError(w, http.StatusNotFound, err)
		case errors.Is(err, sqlite.ErrCategoryReserved):
			h.writeError(w, http.StatusForbidden, err)
		default:
			h.writeError(w, http.StatusInternalServerError, err)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- helpers --------------------------------------------------------

func pathID(r *http.Request, name string) (int64, error) {
	raw := r.PathValue(name)
	if raw == "" {
		return 0, fmt.Errorf("%s: missing path value", name)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, download.ErrJobNotFound),
		errors.Is(err, domainserver.ErrNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (h *Handlers) writeError(w http.ResponseWriter, status int, err error) {
	if status >= 500 && h.Logger != nil {
		// Don't ERROR-log client disconnects mid-query. context.Canceled
		// percolates up from the DB driver when the browser tab closed
		// or the user hit refresh; that's not a server fault. Surface
		// them at DEBUG so operators can still find them if needed.
		if isClientDisconnect(err) {
			h.Logger.Debug("rest handler: client disconnected", "status", status, "err", err)
		} else {
			h.Logger.Error("rest handler", "status", status, "err", err)
		}
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

// isClientDisconnect reports whether err is the kind of "context
// cancelled" / "broken pipe" that comes from the client going away,
// as opposed to a real server-side fault.
func isClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// SQLite driver wraps context.Canceled as a string in some paths.
	s := err.Error()
	return strings.Contains(s, "context canceled") ||
		strings.Contains(s, "context deadline exceeded")
}

// --- health ---------------------------------------------------------

// systemHealth returns the current Issue snapshot from the health
// service. Snapshot is sub-millisecond — no need to gate behind a
// long polling interval. Sort errors before warnings so the UI can
// render them in priority order.
func (h *Handlers) systemHealth(w http.ResponseWriter, _ *http.Request) {
	issues, lastRun := h.Health.Snapshot()
	errs := make([]domainhealth.Issue, 0, len(issues))
	warns := make([]domainhealth.Issue, 0, len(issues))
	for _, i := range issues {
		if i.Severity == domainhealth.SeverityError {
			errs = append(errs, i)
		} else {
			warns = append(warns, i)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"issues":   append(errs, warns...),
		"last_run": lastRun.UTC().Format(time.RFC3339),
	})
}

// systemHealthRefresh requests the service to re-run checks now and
// then returns the next snapshot. Useful as the "I fixed the thing,
// recheck now" button in the UI rather than waiting for the tick.
func (h *Handlers) systemHealthRefresh(w http.ResponseWriter, r *http.Request) {
	h.Health.Refresh()
	// Brief settle window so the re-run lands before we read the
	// snapshot back. Bounded so a slow checker can't stall the
	// request indefinitely.
	select {
	case <-time.After(200 * time.Millisecond):
	case <-r.Context().Done():
		return
	}
	h.systemHealth(w, r)
}

// --- tasks (scheduled jobs) -----------------------------------------

// systemTasks lists every recurring + oneshot task with the fields
// operators care about: when it last ran, when it's next due, the
// most recent error if any, and whether it's currently running.
func (h *Handlers) systemTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := h.Schedule.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, taskToDTO(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": out})
}

// systemTaskRunNow pulls the task's next-run-at forward to now so the
// scheduler's next tick (≤ 1s away) picks it up. Doesn't actually run
// the handler inline — that would block the HTTP request on a
// possibly-long-running task and bypass the claim model. The
// scheduler's normal dispatch handles concurrency + retries cleanly.
func (h *Handlers) systemTaskRunNow(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("bad id: %w", err))
		return
	}
	t, err := h.Schedule.ByID(r.Context(), domainschedule.TaskID(id))
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	if t == nil {
		h.writeError(w, http.StatusNotFound, fmt.Errorf("task %d not found", id))
		return
	}
	now := time.Now().UTC()
	t.Reschedule(now, now)
	if err := h.Schedule.Save(r.Context(), t); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": taskToDTO(t)})
}

// --- commands -------------------------------------------------------

// listCommands returns the N most recent commands, newest-first.
// Default and max page size are baked in; we don't need cursor
// pagination for an operator-driven feed.
func (h *Handlers) listCommands(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	cmds, err := h.Commands.List(r.Context(), limit)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, commandToDTO(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": out})
}

// submitCommand body: {name, body?}. Returns the queued command's
// dto so the caller can poll it.
func (h *Handlers) submitCommand(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string          `json:"name"`
		Body json.RawMessage `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	id, err := h.Commands.Submit(r.Context(), req.Name, req.Body, domaincommand.TriggerManual)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	c, err := h.Commands.ByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"command": commandToDTO(c)})
}

// getCommand returns a single command by id — used by the UI to
// poll progress on a recently-submitted command.
func (h *Handlers) getCommand(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("bad id: %w", err))
		return
	}
	c, err := h.Commands.ByID(r.Context(), domaincommand.CommandID(id))
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	if c == nil {
		h.writeError(w, http.StatusNotFound, fmt.Errorf("command %d not found", id))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"command": commandToDTO(c)})
}

// commandNames lists every registered handler name so the UI can
// populate a "trigger command" dropdown.
func (h *Handlers) commandNames(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"names": h.Commands.Names()})
}

func commandToDTO(c *domaincommand.Command) map[string]any {
	dto := map[string]any{
		"id":        int64(c.ID()),
		"name":      c.Name(),
		"trigger":   string(c.Trigger()),
		"status":    string(c.Status()),
		"queued_at": c.QueuedAt().UTC().Format(time.RFC3339),
	}
	if !c.StartedAt().IsZero() {
		dto["started_at"] = c.StartedAt().UTC().Format(time.RFC3339)
	}
	if !c.EndedAt().IsZero() {
		dto["ended_at"] = c.EndedAt().UTC().Format(time.RFC3339)
	}
	if c.Result() != "" {
		dto["result"] = string(c.Result())
	}
	if c.Error() != "" {
		dto["error"] = c.Error()
	}
	if d := c.Duration(); d > 0 {
		dto["duration_ms"] = d.Milliseconds()
	}
	if len(c.Body()) > 0 {
		dto["body"] = json.RawMessage(c.Body())
	}
	return dto
}

// --- backups --------------------------------------------------------

func (h *Handlers) listBackups(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"backups": h.Backup.List()})
}

// runBackup runs the backup synchronously. SQLite VACUUM INTO is
// usually sub-second on hoardarr-sized databases (≤ tens of MB) so
// blocking the request is fine; if the operator is staring at the UI
// they get immediate confirmation. For larger DBs in the future,
// route this through Commands.
func (h *Handlers) runBackup(w http.ResponseWriter, r *http.Request) {
	if err := h.Backup.Run(r.Context()); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"backups": h.Backup.List()})
}

func (h *Handlers) downloadBackup(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	path, err := h.Backup.SafePath(name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			h.writeError(w, http.StatusNotFound, err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, name))
	if _, err := io.Copy(w, f); err != nil {
		h.Logger.Warn("backup: copy to client failed", "name", name, "err", err)
	}
}

// --- log files ------------------------------------------------------

// systemLogFiles lists every log file in LogDir, newest-first.
func (h *Handlers) systemLogFiles(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"files": logfile.List(h.LogDir),
	})
}

// systemLogFileDownload streams the named log file as text/plain.
// logfile.SafePath enforces the "must be a hoardarr log filename, no
// traversal" rule so the path parameter can't be used to read
// arbitrary files under LogDir's parent.
func (h *Handlers) systemLogFileDownload(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	path, err := logfile.SafePath(h.LogDir, name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			h.writeError(w, http.StatusNotFound, err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, name))
	if _, err := io.Copy(w, f); err != nil {
		h.Logger.Warn("logfile: copy to client failed", "name", name, "err", err)
	}
}

// --- diskspace ------------------------------------------------------

// systemDiskspace runs statfs(2) against each configured path and
// returns the entries verbatim. Sub-millisecond; no caching layer.
func (h *Handlers) systemDiskspace(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"entries": diskspace.Snapshot(h.DiskSources),
	})
}

// taskToDTO renders a schedule.Task into the wire shape the frontend
// expects. Durations are emitted in seconds because frontend code
// formats them with humaniseDuration.
func taskToDTO(t *domainschedule.Task) map[string]any {
	dto := map[string]any{
		"id":                   int64(t.ID()),
		"name":                 t.Name(),
		"kind":                 string(t.Kind()),
		"cadence_seconds":      int64(t.Cadence().Seconds()),
		"next_run_at":          t.NextRunAt().UTC().Format(time.RFC3339),
		"enabled":              t.Enabled(),
		"status":               string(t.Status()),
		"consecutive_failures": t.ConsecutiveFailures(),
	}
	if !t.LastRunAt().IsZero() {
		dto["last_run_at"] = t.LastRunAt().UTC().Format(time.RFC3339)
	}
	if t.LastError() != "" {
		dto["last_error"] = t.LastError()
	}
	if !t.ClaimedAt().IsZero() {
		dto["claimed_at"] = t.ClaimedAt().UTC().Format(time.RFC3339)
	}
	return dto
}
