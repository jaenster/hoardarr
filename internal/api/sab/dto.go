package sab

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// jobToSABSlot maps a hoardarr Job to a SAB queue-slot map. Field
// naming follows real SAB v3.7.x output. Numeric fields that SAB
// emits as strings (mb / mbleft) stay strings here too — we mimic
// the wire shape exactly so consumers don't need to special-case us.
func jobToSABSlot(j *download.Job) map[string]any {
	return jobToSABSlotWithETA(j, 0)
}

// jobToSABSlotWithETA is jobToSABSlot with a per-job rate estimate
// (bytes/sec). When > 0, timeleft / eta are populated; when 0, they
// fall back to SAB's "unknown" sentinels (real SAB does the same
// during early download).
func jobToSABSlotWithETA(j *download.Job, perJobBytesPerSec int64) map[string]any {
	totalMB := bytesToMBString(j.TotalBytes())
	doneMB := bytesToMBString(j.DoneBytes())
	leftMB := bytesToMBString(j.TotalBytes() - j.DoneBytes())
	pct := 0
	if j.TotalBytes() > 0 {
		pct = int((j.DoneBytes() * 100) / j.TotalBytes())
	}

	timeLeft := "0:00:00"
	etaStr := "unknown"
	bytesLeft := j.TotalBytes() - j.DoneBytes()
	if perJobBytesPerSec > 0 && bytesLeft > 0 && j.State() == download.JobStateDownloading {
		secs := bytesLeft / perJobBytesPerSec
		timeLeft = formatSABHMS(secs)
		etaStr = time.Now().Add(time.Duration(secs) * time.Second).Format("Mon 15:04")
	}

	return map[string]any{
		"index":         0,
		"nzo_id":        nzoID(j.ID()),
		"unpackopts":    "3",
		"priority":      priorityToSAB(j.Priority()),
		"script":        "None",
		"filename":      j.Name(),
		"cat":           catOrStar(j.Category()),
		"mbleft":        leftMB,
		"mb":            totalMB,
		"size":          formatBytesHuman(j.TotalBytes()),
		"sizeleft":      formatBytesHuman(bytesLeft),
		"percentage":    fmt.Sprintf("%d", pct),
		"mbmissing":     "0.00",
		"status":        stateToSABStatus(j.State()),
		"timeleft":      timeLeft,
		"avg_age":       "0d",
		"eta":           etaStr,
		"missing":       0,
		// SAB v3.7.x parity. *arr ignores these; SAB-mobile reads them:
		"labels":        []string{},
		"password":      "",
		"direct_unpack": nil, // SAB: int progress or null
		"time_added":    formatTimeISO(j.AddedAt()),
		"_doneMB":       doneMB,
	}
}

// formatSABHMS renders a duration in seconds as SAB's "h:mm:ss".
func formatSABHMS(seconds int64) string {
	if seconds < 0 {
		seconds = 0
	}
	h := seconds / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	return fmt.Sprintf("%d:%02d:%02d", h, m, s)
}

// jobToSABHistorySlot maps a terminal Job to a SAB history-slot map.
// Real SAB has fields like storage / status / completed (unix-seconds).
func jobToSABHistorySlot(j *download.Job, completeDir string) map[string]any {
	storage := ""
	if j.State() == download.JobStateCompleted {
		// Best-effort: <complete>/<release>/. Category subdir not
		// resolved here (would need the category list); the consumer
		// (Sonarr / Radarr) actually looks at the import path it
		// configured, not this string, so a near-miss is fine.
		storage = filepath.Join(completeDir, j.Name())
	}
	return map[string]any{
		"id":            int64(j.ID()),
		"nzo_id":        nzoID(j.ID()),
		"name":          j.Name(),
		"nzb_name":      j.Name() + ".nzb",
		"category":      catOrStar(j.Category()),
		"pp":            "X", // post-process opts encoded; we always do verify+extract+deliver
		"size":          formatBytesHuman(j.TotalBytes()),
		"bytes":         j.TotalBytes(),
		"storage":       storage,
		"completed":     unixOrZero(j.FinishedAt()),
		"status":        historyStateToSAB(j.State()),
		"fail_message":  j.ErrorMsg(),
		"path":          storage,
		"script":        "None",
		"download_time": 0,
		"postproc_time": 0,
		"stage_log":     []any{},
		"action_line":   "",
		// SAB v3.7.x parity. Provide defaults so consumers don't NPE.
		"report":         "",
		"url":            "",
		"url_info":       "",
		"script_line":    "",
		"downloaded":     j.TotalBytes(),
		"completeness":   nil,
		"meta":           nil,
		"series":         "",
		"duplicate_key":  "",
		"md5sum":         "",
		"password":       "",
		"loaded":         false,
		"retry":          false,
		"archive":        false,
		"time_added":     formatTimeISO(j.AddedAt()),
	}
}

func priorityToSAB(p int) string {
	// SAB ints: -100=Default, -3=Stop, -2=Repair, -1=Force, 0=Normal,
	// 1=High, 2=High++. Our scale matches the standard normal/high split
	// directly enough for *arr's default priority=normal.
	switch {
	case p < 0:
		return "-1"
	case p > 0:
		return "1"
	default:
		return "0"
	}
}

func catOrStar(c string) string {
	if c == "" {
		return "*"
	}
	return c
}

func stateToSABStatus(s download.JobState) string {
	switch s {
	case download.JobStateQueued:
		return "Queued"
	case download.JobStateDownloading:
		return "Downloading"
	case download.JobStatePaused:
		return "Paused"
	case download.JobStateDownloadComplete:
		return "Verifying"
	case download.JobStateVerifying:
		return "Verifying"
	case download.JobStateRepairing:
		return "Repairing"
	case download.JobStateUnpacking:
		return "Extracting"
	case download.JobStateCompleted:
		return "Completed"
	case download.JobStateFailed:
		return "Failed"
	case download.JobStateAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

func historyStateToSAB(s download.JobState) string {
	switch s {
	case download.JobStateCompleted:
		return "Completed"
	case download.JobStateFailed:
		return "Failed"
	case download.JobStateAborted:
		return "Failed"
	default:
		return "Unknown"
	}
}

func queueStatus(jobs []*download.Job) string {
	for _, j := range jobs {
		if j.State() == download.JobStateDownloading {
			return "Downloading"
		}
	}
	if len(jobs) == 0 {
		return "Idle"
	}
	return "Paused"
}

// formatBytesHuman is SAB-style: "12.34 MB", "1.2 GB" etc. SAB uses
// 1024-base (MiB-as-MB) which is what's expected here despite the
// notation.
func formatBytesHuman(n int64) string {
	if n < 0 {
		n = 0
	}
	const k = 1024
	switch {
	case n < k:
		return fmt.Sprintf("%d B", n)
	case n < k*k:
		return fmt.Sprintf("%.2f KB", float64(n)/k)
	case n < k*k*k:
		return fmt.Sprintf("%.2f MB", float64(n)/(k*k))
	default:
		return fmt.Sprintf("%.2f GB", float64(n)/(k*k*k))
	}
}

func bytesToMBString(n int64) string {
	if n < 0 {
		n = 0
	}
	mb := float64(n) / (1024 * 1024)
	s := fmt.Sprintf("%.2f", mb)
	return strings.TrimSuffix(s, ".00")
}

func totalSizeHuman(jobs []*download.Job) string {
	var t int64
	for _, j := range jobs {
		t += j.TotalBytes()
	}
	return formatBytesHuman(t)
}

func sizeLeftHuman(jobs []*download.Job) string {
	var t int64
	for _, j := range jobs {
		t += j.TotalBytes() - j.DoneBytes()
	}
	return formatBytesHuman(t)
}

func totalBytesMB(jobs []*download.Job) string {
	var t int64
	for _, j := range jobs {
		t += j.TotalBytes()
	}
	return bytesToMBString(t)
}

func bytesLeftMB(jobs []*download.Job) string {
	var t int64
	for _, j := range jobs {
		t += j.TotalBytes() - j.DoneBytes()
	}
	return bytesToMBString(t)
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// formatTimeISO emits an RFC3339 string for SAB's `time_added` slot
// field. Returns an empty string for the zero time so consumers can
// distinguish "never set".
func formatTimeISO(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
