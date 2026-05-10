package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	"github.com/jaenster/hoardarr/internal/domain/download"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// Handlers groups dependencies for the REST API surface.
type Handlers struct {
	Queue      *appdownload.QueueService
	AddJob     *appdownload.AddJobService
	Servers    *appserver.Service
	Categories *sqlite.CategoryRepo
	Logger     *slog.Logger
}

// Mount registers the /api/v1/* routes on mux. The caller is responsible
// for wrapping individual routes with the api-key middleware (the
// `protect` helper passed in). /api/v1/health is NOT registered here —
// that lives in the server package as a public liveness probe.
func (h *Handlers) Mount(mux *http.ServeMux, protect func(http.Handler) http.Handler) {
	register := func(method, pattern string, fn http.HandlerFunc) {
		mux.Handle(method+" "+pattern, protect(fn))
	}

	// Queue.
	register("GET", "/api/v1/queue", h.listQueue)
	register("POST", "/api/v1/queue/nzb", h.addNZB)
	register("POST", "/api/v1/queue/{id}/pause", h.pauseJob)
	register("POST", "/api/v1/queue/{id}/resume", h.resumeJob)
	register("DELETE", "/api/v1/queue/{id}", h.removeJob)

	// Servers.
	register("GET", "/api/v1/servers", h.listServers)
	register("POST", "/api/v1/servers", h.addServer)
	register("DELETE", "/api/v1/servers/{id}", h.removeServer)

	// Categories.
	register("GET", "/api/v1/categories", h.listCategories)
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
	if err := r.ParseMultipartForm(32 << 20); err != nil {
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
