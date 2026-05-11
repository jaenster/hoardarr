package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntptest"
	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appnotify "github.com/jaenster/hoardarr/internal/app/notify"
	"github.com/jaenster/hoardarr/internal/domain/event"
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
	System        SystemStatuser // optional; nil disables /api/v1/system/status
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
// API key is included so admins can copy it into *arr clients; we
// only return it to authenticated requests.
type GeneralView struct {
	Listen   string
	APIKey   string
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
	}
	if h.LogHub != nil {
		register("GET", "/api/v1/system/logs", h.systemLogsSnapshot)
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
	// Shallow loads — file metadata only, no per-segment hydration.
	// The DTO drops segments anyway and skipping the N+M repo queries
	// turns a multi-second response on big releases into milliseconds.
	if includeAll {
		jobs, err = h.Queue.ListShallow(ctx)
	} else {
		jobs, err = h.Queue.ActiveShallow(ctx)
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
	})
	if err != nil {
		switch {
		case errors.Is(err, appdownload.ErrDuplicateNZB):
			writeJSON(w, http.StatusOK, map[string]any{
				"job_id":    int64(id),
				"duplicate": true,
			})
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
	jobs, err := h.Queue.History(r.Context(), hq)
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
	tp := h.System.Throughput()
	if tp == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"window_seconds":       appsystem.WindowSize,
			"series":               []int64{},
			"total_bytes":          0,
			"current_bytes_per_sec": 0,
			"avg10s_bytes_per_sec":  0,
			"avg60s_bytes_per_sec":  0,
		})
		return
	}
	s := tp.Sample()
	writeJSON(w, http.StatusOK, map[string]any{
		"window_seconds":       appsystem.WindowSize,
		"series":               s.Series,
		"total_bytes":          s.Total,
		"current_bytes_per_sec": s.CurrentBytesPerSec,
		"avg10s_bytes_per_sec":  s.Avg10sBytesPerSec,
		"avg60s_bytes_per_sec":  s.Avg60sBytesPerSec,
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
		"service":    st.Service,
		"version":    st.Version,
		"started_at": st.StartedAt.Format(time.RFC3339),
		"uptime_ms":  st.Uptime.Milliseconds(),
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
	if h.Runtime != nil {
		urlBase = h.Runtime.URLBase()
		maxConcurrent = h.Runtime.MaxConcurrentJobs()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"listen":              h.General.Listen,
		"api_key":             h.General.APIKey,
		"log_level":           h.General.LogLevel,
		"sab_base":            h.General.SABBase,
		"url_base":            urlBase,
		"max_concurrent_jobs": maxConcurrent,
	})
}

type putGeneralReq struct {
	URLBase           *string `json:"url_base,omitempty"`
	MaxConcurrentJobs *int    `json:"max_concurrent_jobs,omitempty"`
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
