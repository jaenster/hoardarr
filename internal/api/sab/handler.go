// Package sab implements the SABnzbd API surface that Sonarr / Radarr /
// Lidarr / Readarr / Prowlarr expect from a download client. This is an
// anti-corruption layer: requests in SAB shapes are translated into
// hoardarr domain operations, responses are encoded in the JSON shapes
// the *arr suite reads.
//
// Design choices that aren't obvious:
//
//   - mode= dispatch follows real SAB's URL pattern. POST /sabnzbd/api
//     with form-encoded body (or query params) carrying mode= plus
//     mode-specific fields. We accept both POST and GET; *arr uses POST
//     for addfile and GET for everything else.
//
//   - nzo_id format: "SABnzbd_nzo_<base32(jobID)>". Opaque to consumers,
//     reversible by us. *arr uses these as primary keys for queue
//     identity, so they must be stable for the life of a Job.
//
//   - "version" lies to clients: we report a recent SAB version so
//     consumers don't refuse to talk to us. The *arr suite gates a few
//     features on version; reporting >=3.0.0 is the practical floor.
//
//   - apikey: SAB takes apikey as a query/form param, not a header.
//     Browser-cookie auth is not supported here; this surface is for
//     headless *arr clients exclusively.
package sab

import (
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	"github.com/jaenster/hoardarr/internal/domain/download"
)

// reportedVersion is the SAB version we claim to be. *arr clients check
// version >= 3.0.0; lying as 3.7.2 is what most SABnzbd-replacement
// projects do, and matches what an out-of-the-box SAB ships today.
const reportedVersion = "3.7.2"

// Handler is the SAB API entry point. Mount at /sabnzbd/api (and
// /sabnzbd/ for the few clients that path-prefix without /api).
type Handler struct {
	APIKey     string
	Queue      *appdownload.QueueService
	AddJob     *appdownload.AddJobService
	Categories *sqlite.CategoryRepo
	Logger     *slog.Logger
	CompleteDir string
}

// ServeHTTP dispatches on mode=.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(256 << 20); err != nil {
		// Not multipart — try plain form. Either is fine; we just need
		// values.
		if err := r.ParseForm(); err != nil {
			h.writeError(w, http.StatusBadRequest, fmt.Errorf("parse form: %w", err))
			return
		}
	}

	if !constantTimeStringEq(formGet(r, "apikey"), h.APIKey) {
		h.writeError(w, http.StatusUnauthorized, errors.New("invalid apikey"))
		return
	}

	mode := formGet(r, "mode")
	switch mode {
	case "version":
		h.modeVersion(w, r)
	case "get_config":
		h.modeGetConfig(w, r)
	case "get_cats":
		h.modeGetCats(w, r)
	case "addfile":
		h.modeAddFile(w, r)
	case "queue":
		h.modeQueue(w, r)
	case "history":
		h.modeHistory(w, r)
	case "":
		h.writeError(w, http.StatusBadRequest, errors.New("mode= required"))
	default:
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("mode=%q not implemented", mode))
	}
}

// --- modes -----------------------------------------------------------

func (h *Handler) modeVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"version": reportedVersion})
}

// modeGetConfig returns the slice of SAB config that *arr clients
// actually parse. Real SAB returns a vast nested structure; we return
// only what's load-bearing (misc.complete_dir, categories), which is
// what consumers gate behaviour on.
func (h *Handler) modeGetConfig(w http.ResponseWriter, r *http.Request) {
	cats, err := h.Categories.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	catList := make([]map[string]any, 0, len(cats))
	for _, c := range cats {
		catList = append(catList, map[string]any{
			"name":     c.Name,
			"order":    c.Priority,
			"pp":       "",
			"script":   "None",
			"dir":      c.Dir,
			"newzbin":  "",
			"priority": c.Priority,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"config": map[string]any{
			"misc": map[string]any{
				"complete_dir": h.CompleteDir,
				"version":      reportedVersion,
			},
			"categories": catList,
		},
	})
}

// modeGetCats returns the names list. SAB's shape is `{"categories":["*","movies",...]}`.
func (h *Handler) modeGetCats(w http.ResponseWriter, r *http.Request) {
	cats, err := h.Categories.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	names := make([]string, 0, len(cats))
	for _, c := range cats {
		names = append(names, c.Name)
	}
	writeJSON(w, http.StatusOK, map[string]any{"categories": names})
}

// modeAddFile accepts a multipart NZB upload. SAB's addfile takes
// "name" as the file part name (real SAB) and "nzbfile" (some forks).
// We accept both.
func (h *Handler) modeAddFile(w http.ResponseWriter, r *http.Request) {
	file, _, err := firstFormFile(r, "name", "nzbfile")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	defer file.Close()

	cat := formGet(r, "cat")
	id, err := h.AddJob.AddJob(r.Context(), appdownload.AddJobCmd{
		NZB:      file,
		Category: cat,
	})
	if err != nil && !errors.Is(err, appdownload.ErrDuplicateNZB) {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  true,
		"nzo_ids": []string{nzoID(id)},
	})
}

// modeQueue is overloaded: bare ?mode=queue lists, ?name=pause/resume/delete
// mutates. Real SAB also has reorder/setpriority — out of phase-1 scope.
func (h *Handler) modeQueue(w http.ResponseWriter, r *http.Request) {
	switch formGet(r, "name") {
	case "":
		h.modeQueueList(w, r)
	case "pause":
		h.modeQueueAction(w, r, "pause")
	case "resume":
		h.modeQueueAction(w, r, "resume")
	case "delete":
		h.modeQueueAction(w, r, "delete")
	default:
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("queue.name=%q not implemented", formGet(r, "name")))
	}
}

func (h *Handler) modeQueueList(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.Queue.Active(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	slots := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		slots = append(slots, jobToSABSlot(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"queue": map[string]any{
			"version":   reportedVersion,
			"paused":    false,
			"speed":     "0 B/s",
			"speedlimit": "0",
			"size":       totalSizeHuman(jobs),
			"sizeleft":   sizeLeftHuman(jobs),
			"mb":         totalBytesMB(jobs),
			"mbleft":     bytesLeftMB(jobs),
			"noofslots":  len(slots),
			"start":      0,
			"limit":      len(slots),
			"finish":     len(slots),
			"slots":      slots,
			"status":     queueStatus(jobs),
		},
	})
}

func (h *Handler) modeQueueAction(w http.ResponseWriter, r *http.Request, action string) {
	rawIDs := formGet(r, "value")
	ids := strings.Split(rawIDs, ",")
	results := make([]string, 0, len(ids))
	for _, raw := range ids {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		id, err := jobIDFromNZO(raw)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, fmt.Errorf("nzo_id %q: %w", raw, err))
			return
		}
		switch action {
		case "pause":
			err = h.Queue.PauseJob(r.Context(), id)
		case "resume":
			err = h.Queue.ResumeJob(r.Context(), id)
		case "delete":
			err = h.Queue.RemoveJob(r.Context(), id)
		}
		if err != nil {
			h.writeError(w, http.StatusBadRequest, err)
			return
		}
		results = append(results, raw)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  true,
		"nzo_ids": results,
	})
}

func (h *Handler) modeHistory(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := formGet(r, "limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	jobs, err := h.Queue.History(r.Context(), download.HistoryQuery{Limit: limit})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	slots := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		slots = append(slots, jobToSABHistorySlot(j, h.CompleteDir))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"history": map[string]any{
			"noofslots":      len(slots),
			"slots":          slots,
			"version":        reportedVersion,
			"total_size":     totalSizeHuman(jobs),
			"month_size":     "0 B",
			"week_size":      "0 B",
			"day_size":       "0 B",
		},
	})
}

// --- helpers ---------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, err error) {
	if status >= 500 && h.Logger != nil {
		h.Logger.Error("sab handler", "status", status, "err", err)
	}
	writeJSON(w, status, map[string]any{
		"status": false,
		"error":  err.Error(),
	})
}

func formGet(r *http.Request, key string) string {
	// PostForm wins (multipart form fields), then URL query, then plain Form.
	if v := r.PostFormValue(key); v != "" {
		return v
	}
	return r.FormValue(key)
}

// firstFormFile tries each name and returns the first that hits.
func firstFormFile(r *http.Request, names ...string) (file interface {
	Read([]byte) (int, error)
	Close() error
}, filename string, err error) {
	for _, name := range names {
		f, hdr, ferr := r.FormFile(name)
		if ferr == nil {
			return f, hdr.Filename, nil
		}
	}
	return nil, "", errors.New("no nzb file part (expected name= or nzbfile=)")
}

// nzoID encodes a JobID into SAB's opaque "SABnzbd_nzo_<base32>" format.
// We use base32 (no padding) because SAB nzo_ids are typed and pasted
// freely; '+' / '/' from base64 would cause URL-encoding hassles.
func nzoID(id download.JobID) string {
	b := []byte(strconv.FormatInt(int64(id), 10))
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
	return "SABnzbd_nzo_" + enc
}

// jobIDFromNZO is the inverse of nzoID. Returns an error for inputs
// that don't match the expected prefix or fail to decode.
func jobIDFromNZO(s string) (download.JobID, error) {
	const prefix = "SABnzbd_nzo_"
	if !strings.HasPrefix(s, prefix) {
		return 0, errors.New("bad nzo_id prefix")
	}
	enc := s[len(prefix):]
	b, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(enc)
	if err != nil {
		return 0, fmt.Errorf("base32: %w", err)
	}
	id, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse: %w", err)
	}
	return download.JobID(id), nil
}

// constantTimeStringEq matches the auth-middleware helper. Pulled
// inline so the SAB package doesn't import server internals.
func constantTimeStringEq(a, b string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
