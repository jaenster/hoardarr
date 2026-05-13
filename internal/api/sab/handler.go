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
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	APIKey      string
	Queue       *appdownload.QueueService
	AddJob      *appdownload.AddJobService
	Categories  *sqlite.CategoryRepo
	Logger      *slog.Logger
	CompleteDir string
	// Throughput returns current overall download rate in bytes/sec.
	// Used to populate queue.kbpersec / queue.timeleft and per-slot
	// eta/timeleft. May be nil; the SAB API then reports 0 / unknown
	// (existing behaviour, but *arr clients prefer numbers).
	Throughput func() int64
	// fetchClient handles mode=addurl downloads. Lazily constructed so
	// tests can inject a stubbed transport without touching the global
	// http.DefaultClient.
	fetchClient *http.Client
}

// FetchClient returns the (lazily-constructed) http.Client used by
// mode=addurl. Exposed so tests can replace its Transport.
func (h *Handler) FetchClient() *http.Client {
	if h.fetchClient == nil {
		h.fetchClient = &http.Client{Timeout: 30 * time.Second}
	}
	return h.fetchClient
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
	case "addurl":
		h.modeAddURL(w, r)
	case "queue":
		h.modeQueue(w, r)
	case "history":
		h.modeHistory(w, r)
	case "get_files":
		h.modeGetFiles(w, r)
	case "eval_sort":
		h.modeEvalSort(w, r)
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
	file, filename, err := firstFormFile(r, "name", "nzbfile")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	defer file.Close()

	// Display name from the multipart filename so *arr clients
	// uploading "Release.Name.S01E01.nzb" show up under that label
	// in the queue.
	displayName := strings.TrimSuffix(filename, ".nzb")
	displayName = strings.TrimSuffix(displayName, ".NZB")

	cat := formGet(r, "cat")
	id, err := h.AddJob.AddJob(r.Context(), appdownload.AddJobCmd{
		NZB:      file,
		Name:     displayName,
		Category: cat,
		Source:   r.UserAgent(),
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
	jobs, err := h.Queue.ActiveJobsOnly(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	var rate int64
	if h.Throughput != nil {
		rate = h.Throughput()
	}
	// Total bytes left across the active queue. We share the
	// throughput evenly across non-paused jobs for per-slot ETA
	// since hoardarr fetches them concurrently (one runner per job).
	var totalLeft int64
	activeCount := 0
	for _, j := range jobs {
		if j.State() == download.JobStatePaused {
			continue
		}
		left := j.TotalBytes() - j.DoneBytes()
		if left > 0 {
			totalLeft += left
			activeCount++
		}
	}
	perJobRate := rate
	if activeCount > 1 && rate > 0 {
		perJobRate = rate / int64(activeCount)
	}

	slots := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		slots = append(slots, jobToSABSlotWithETA(j, perJobRate))
	}

	queueTimeLeft := "0:00:00"
	if rate > 0 && totalLeft > 0 {
		queueTimeLeft = formatHMS(totalLeft / rate)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queue": map[string]any{
			"version":         reportedVersion,
			"paused":          false,
			"speed":           formatBytesPerSec(rate),
			"kbpersec":        fmt.Sprintf("%.2f", float64(rate)/1024),
			"speedlimit":      "0",
			"speedlimit_abs":  "",
			"size":            totalSizeHuman(jobs),
			"sizeleft":        sizeLeftHuman(jobs),
			"mb":              totalBytesMB(jobs),
			"mbleft":          bytesLeftMB(jobs),
			"noofslots":       len(slots),
			"noofslots_total": len(slots),
			"start":           0,
			"limit":           len(slots),
			"finish":          len(slots),
			"slots":           slots,
			"status":          queueStatus(jobs),
			"timeleft":        queueTimeLeft,
			"diskspace1":      "0",
			"diskspace2":      "0",
			"diskspacetotal1": "0",
			"diskspacetotal2": "0",
		},
	})
}

// formatBytesPerSec renders bytes/sec the way SAB's "speed" field
// does it: "<value> <unit>/s" where unit is human-readable.
func formatBytesPerSec(bps int64) string {
	if bps <= 0 {
		return "0 B/s"
	}
	const k = 1024
	if bps < k {
		return fmt.Sprintf("%d B/s", bps)
	}
	units := []string{"K", "M", "G", "T"}
	v := float64(bps) / float64(k)
	i := 0
	for v >= k && i < len(units)-1 {
		v /= float64(k)
		i++
	}
	return fmt.Sprintf("%.1f %sB/s", v, units[i])
}

// formatHMS renders a duration in seconds as "h:mm:ss" (SAB format).
func formatHMS(seconds int64) string {
	if seconds < 0 {
		seconds = 0
	}
	h := seconds / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	return fmt.Sprintf("%d:%02d:%02d", h, m, s)
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
	switch formGet(r, "name") {
	case "":
		h.modeHistoryList(w, r)
		return
	case "delete":
		// History delete uses the same RemoveJob plumbing as queue delete:
		// the underlying repo doesn't distinguish, and removing a
		// completed/failed row is just an UPDATE that nulls history.
		h.modeQueueAction(w, r, "delete")
		return
	case "mark_as_completed":
		h.modeMarkCompleted(w, r)
		return
	default:
		h.writeError(w, http.StatusBadRequest,
			fmt.Errorf("history.name=%q not implemented", formGet(r, "name")))
		return
	}
}

func (h *Handler) modeHistoryList(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := formGet(r, "limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	jobs, err := h.Queue.HistoryJobsOnly(r.Context(), download.HistoryQuery{Limit: limit})
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

// modeAddURL fetches an NZB by URL and routes the body through the
// existing addfile pipeline. Sonarr's "Send NZB by URL" button hits
// this; before this lands the button silently fails and the job never
// makes it into hoardarr's queue.
func (h *Handler) modeAddURL(w http.ResponseWriter, r *http.Request) {
	rawURL := strings.TrimSpace(formGet(r, "name"))
	if rawURL == "" {
		h.writeError(w, http.StatusBadRequest, errors.New("name= (url) required"))
		return
	}
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		h.writeError(w, http.StatusBadRequest, errors.New("url must be http:// or https://"))
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, rawURL, nil)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("build request: %w", err))
		return
	}
	req.Header.Set("User-Agent", "hoardarr/sab-shim")
	resp, err := h.FetchClient().Do(req)
	if err != nil {
		h.writeError(w, http.StatusBadGateway, fmt.Errorf("fetch nzb: %w", err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		h.writeError(w, http.StatusBadGateway, fmt.Errorf("fetch nzb: upstream %d", resp.StatusCode))
		return
	}

	// Display name precedence: explicit nzbname= > Content-Disposition >
	// derived from URL path > a placeholder. Indexers usually send a
	// Content-Disposition: attachment; filename="Release.Name.nzb" header.
	displayName := strings.TrimSpace(formGet(r, "nzbname"))
	if displayName == "" {
		if cd := resp.Header.Get("Content-Disposition"); cd != "" {
			if _, params, perr := mime.ParseMediaType(cd); perr == nil {
				if fn := params["filename"]; fn != "" {
					displayName = strings.TrimSuffix(fn, ".nzb")
					displayName = strings.TrimSuffix(displayName, ".NZB")
				}
			}
		}
	}
	if displayName == "" {
		displayName = filenameFromURL(rawURL)
	}
	if displayName == "" {
		displayName = "addurl-job"
	}

	id, err := h.AddJob.AddJob(r.Context(), appdownload.AddJobCmd{
		NZB:      resp.Body,
		Name:     displayName,
		Category: formGet(r, "cat"),
		Source:   r.UserAgent() + " (addurl)",
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

// modeGetFiles returns the per-file list for a job. Sonarr's queue
// detail view in some versions enumerates these; the data is already
// hydrated by QueueService.Get (issue #125), this is just a SAB-shaped
// wrapper.
func (h *Handler) modeGetFiles(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimSpace(formGet(r, "value"))
	if raw == "" {
		h.writeError(w, http.StatusBadRequest, errors.New("value= (nzo_id) required"))
		return
	}
	id, err := jobIDFromNZO(raw)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("nzo_id %q: %w", raw, err))
		return
	}
	j, err := h.Queue.Get(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, err)
		return
	}
	files := make([]map[string]any, 0, len(j.Files()))
	for _, f := range j.Files() {
		total := f.SizeBytes()
		var doneBytes int64
		if cnt := f.SegmentCount(); cnt > 0 {
			doneBytes = total * int64(f.SegmentsDone()) / int64(cnt)
		}
		files = append(files, map[string]any{
			"filename": f.Filename(),
			"mb":       fmt.Sprintf("%.2f", float64(total)/(1024*1024)),
			"mbleft":   fmt.Sprintf("%.2f", float64(total-doneBytes)/(1024*1024)),
			"bytes":    total,
			"set":      "",
			"easy_id":  int(f.ID()),
			"status":   sabFileStatus(f),
			"nzf_id":   fmt.Sprintf("nzf_%d", int(f.ID())),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"files": files})
}

// modeEvalSort renders a SAB-style sort template. *arr clients call
// this to preview where an import will land before submitting it; a
// 4xx response makes them refuse the download client entirely.
func (h *Handler) modeEvalSort(w http.ResponseWriter, r *http.Request) {
	template := formGet(r, "name")
	if template == "" {
		h.writeError(w, http.StatusBadRequest, errors.New("name= (template) required"))
		return
	}
	ctx := buildSortContext(func(k string) string { return formGet(r, k) })
	writeJSON(w, http.StatusOK, map[string]any{
		"status": true,
		"result": evalSort(template, ctx),
	})
}

// modeMarkCompleted flips a Failed job to Completed. SAB exposes this
// from its web UI's "mark as completed" right-click action and *arr
// clients sometimes call it after a manual re-import. Files on disk
// are untouched; only DB state + history view change.
func (h *Handler) modeMarkCompleted(w http.ResponseWriter, r *http.Request) {
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
		if err := h.Queue.MarkCompleted(r.Context(), id); err != nil {
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

// sabFileStatus maps hoardarr's per-file state to a single-word SAB
// status string. *arr clients tend to just display whatever they get,
// so the exact vocabulary is less important than being non-empty.
func sabFileStatus(f *download.File) string {
	switch f.State() {
	case download.FileStateComplete:
		return "Finished"
	case download.FileStateDownloading:
		return "Active"
	case download.FileStatePending:
		return "Queued"
	case download.FileStateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// filenameFromURL extracts a reasonable display name from a URL like
// https://indexer.example/getnzb?id=abc.nzb&apikey=… by stripping the
// query and any trailing .nzb suffix.
func filenameFromURL(u string) string {
	// Strip query.
	if i := strings.Index(u, "?"); i >= 0 {
		u = u[:i]
	}
	// Last path component.
	if i := strings.LastIndex(u, "/"); i >= 0 {
		u = u[i+1:]
	}
	u = strings.TrimSuffix(u, ".nzb")
	u = strings.TrimSuffix(u, ".NZB")
	return u
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
