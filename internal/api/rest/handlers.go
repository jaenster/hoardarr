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

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	appsystem "github.com/jaenster/hoardarr/internal/app/system"
	"github.com/jaenster/hoardarr/internal/domain/download"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// Handlers groups dependencies for the REST API surface.
type Handlers struct {
	Queue      *appdownload.QueueService
	AddJob     *appdownload.AddJobService
	Servers    *appserver.Service
	Categories *sqlite.CategoryRepo
	Auth       Auther         // optional; nil disables /api/v1/auth/*
	System     SystemStatuser // optional; nil disables /api/v1/system/status
	Paths      *PathsView     // optional; nil disables /api/v1/config/paths
	General    *GeneralView   // optional; nil disables /api/v1/config/general
	Logger     *slog.Logger
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
	Listen    string
	APIKey    string
	LogLevel  string
	SABBase   string // e.g. "http://hoardarr:8085/sabnzbd/api"
}

// SystemStatuser is the slice of app/system.Service that the REST
// handler needs. Defined here as an interface so tests can pass a
// fake without dragging the full service in.
type SystemStatuser interface {
	Status(ctx context.Context) (appsystem.Status, error)
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
	register("DELETE", "/api/v1/queue/{id}", h.removeJob)

	// History.
	register("GET", "/api/v1/history", h.listHistory)

	// Servers.
	register("GET", "/api/v1/servers", h.listServers)
	register("POST", "/api/v1/servers", h.addServer)
	register("DELETE", "/api/v1/servers/{id}", h.removeServer)

	// Categories.
	register("GET", "/api/v1/categories", h.listCategories)
	register("POST", "/api/v1/categories", h.upsertCategory)
	register("DELETE", "/api/v1/categories/{name}", h.removeCategory)

	// System status.
	if h.System != nil {
		register("GET", "/api/v1/system/status", h.systemStatus)
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
	}
}

// --- queue ----------------------------------------------------------

func (h *Handlers) listQueue(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	includeAll := r.URL.Query().Get("include") == "all"
	var jobs []*download.Job
	var err error
	if includeAll {
		jobs, err = h.Queue.List(ctx)
	} else {
		jobs, err = h.Queue.Active(ctx)
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
	file, _, err := r.FormFile("nzb")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("nzb file required: %w", err))
		return
	}
	defer file.Close()

	id, err := h.AddJob.AddJob(ctx, appdownload.AddJobCmd{
		NZB:      file,
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
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	TLS      *bool  `json:"tls,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	MaxConns int    `json:"max_conns,omitempty"`
	Priority int    `json:"priority,omitempty"`
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
		Name:     req.Name,
		Host:     req.Host,
		Port:     req.Port,
		TLS:      req.TLS,
		Username: req.Username,
		Password: req.Password,
		MaxConns: req.MaxConns,
		Priority: req.Priority,
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
			"server_id":   int64(p.ServerID),
			"server_name": p.ServerName,
			"host":        p.Host,
			"port":        p.Port,
			"max_conns":   p.MaxConns,
			"in_use":      p.InUse,
			"idle":        p.Idle,
			"enabled":     p.Enabled,
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

// --- general config (read-only) -------------------------------------

func (h *Handlers) getGeneral(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"listen":    h.General.Listen,
		"api_key":   h.General.APIKey,
		"log_level": h.General.LogLevel,
		"sab_base":  h.General.SABBase,
	})
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
		h.Logger.Error("rest handler", "status", status, "err", err)
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
