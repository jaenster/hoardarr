// Package rest mounts hoardarr's first-party HTTP/JSON API at /api/v1.
//
// The shapes here are independent of domain types: this package owns
// the wire contract. Translations are in dto.go (domain → DTO; never
// the reverse — request bodies decode to ad-hoc structs in the handler
// and are converted to domain commands via app/* services).
package rest

import (
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/notify"
	"github.com/jaenster/hoardarr/internal/domain/server"
)

// JobDTO is the JSON shape returned by /api/v1/queue endpoints.
type JobDTO struct {
	ID          int64     `json:"id"`
	NZBHash     string    `json:"nzb_hash"`
	Name        string    `json:"name"`
	Category    string    `json:"category"`
	Priority    int       `json:"priority"`
	State       string    `json:"state"`
	Source      string    `json:"source,omitempty"`
	TotalBytes  int64     `json:"total_bytes"`
	DoneBytes   int64     `json:"done_bytes"`
	FailedBytes int64     `json:"failed_bytes"`
	AddedAt    time.Time  `json:"added_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error       string    `json:"error,omitempty"`
	Files       []FileDTO `json:"files"`
}

// FileDTO is the per-file slice attached to a JobDTO.
type FileDTO struct {
	ID            int64  `json:"id"`
	Filename      string `json:"filename"`
	SizeBytes     int64  `json:"size_bytes"`
	State         string `json:"state"`
	SegmentCount  int    `json:"segment_count"`
	SegmentsDone  int    `json:"segments_done"`
	IsPar2        bool   `json:"is_par2"`
}

// ServerDTO is the JSON shape returned by /api/v1/servers. Passwords
// are intentionally omitted from responses.
type ServerDTO struct {
	ID                   int64     `json:"id"`
	Name                 string    `json:"name"`
	Host                 string    `json:"host"`
	Port                 int       `json:"port"`
	TLS                  bool      `json:"tls"`
	Username             string    `json:"username,omitempty"`
	MaxConns             int       `json:"max_conns"`
	Priority             int       `json:"priority"`
	Enabled              bool      `json:"enabled"`
	Backup               bool      `json:"backup"`
	BillingMode          string    `json:"billing_mode"`
	QuotaBytes           int64     `json:"quota_bytes"`
	UsedBytes            int64     `json:"used_bytes"`
	BandwidthBytesPerSec int64     `json:"bandwidth_bytes_per_sec"`
	AddedAt              time.Time `json:"added_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// CategoryDTO is the JSON shape for /api/v1/categories.
type CategoryDTO struct {
	Name     string `json:"name"`
	Dir      string `json:"dir"`
	Priority int    `json:"priority"`
}

// SubscriptionDTO is the JSON shape for /api/v1/subscriptions.
// Secret is intentionally omitted from list responses; if a future
// "show secret" UI flow is needed it can use a separate endpoint that
// returns it once.
type SubscriptionDTO struct {
	ID            int64      `json:"id"`
	Name          string     `json:"name"`
	Kind          string     `json:"kind"`
	URL           string     `json:"url"`
	Topics        []string   `json:"topics"`
	HasSecret     bool       `json:"has_secret"`
	Enabled       bool       `json:"enabled"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastErrorAt   *time.Time `json:"last_error_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func subscriptionToDTO(s *notify.Subscription) SubscriptionDTO {
	dto := SubscriptionDTO{
		ID:        int64(s.ID()),
		Name:      s.Name(),
		Kind:      string(s.Kind()),
		URL:       s.URL(),
		Topics:    s.Topics(),
		HasSecret: s.Secret() != "",
		Enabled:   s.Enabled(),
		LastError: s.LastError(),
		CreatedAt: s.CreatedAt(),
		UpdatedAt: s.UpdatedAt(),
	}
	if t := s.LastSuccessAt(); !t.IsZero() {
		dto.LastSuccessAt = &t
	}
	if t := s.LastErrorAt(); !t.IsZero() {
		dto.LastErrorAt = &t
	}
	return dto
}

// jobToDTO converts a Job aggregate to its wire shape.
func jobToDTO(j *download.Job) JobDTO {
	dto := JobDTO{
		ID:          int64(j.ID()),
		NZBHash:     j.NZBHash(),
		Name:        j.Name(),
		Category:    j.Category(),
		Priority:    j.Priority(),
		State:       string(j.State()),
		Source:      j.Source(),
		TotalBytes:  j.TotalBytes(),
		DoneBytes:   j.DoneBytes(),
		FailedBytes: j.FailedBytes(),
		AddedAt:     j.AddedAt(),
		Error:       j.ErrorMsg(),
	}
	if t := j.StartedAt(); !t.IsZero() {
		dto.StartedAt = &t
	}
	if t := j.FinishedAt(); !t.IsZero() {
		dto.FinishedAt = &t
	}
	files := j.Files()
	dto.Files = make([]FileDTO, 0, len(files))
	for _, f := range files {
		dto.Files = append(dto.Files, FileDTO{
			ID:           int64(f.ID()),
			Filename:     f.Filename(),
			SizeBytes:    f.SizeBytes(),
			State:        string(f.State()),
			SegmentCount: f.SegmentCount(),
			SegmentsDone: f.SegmentsDone(),
			IsPar2:       f.IsPar2(),
		})
	}
	return dto
}

func serverToDTO(s *server.UsenetServer) ServerDTO {
	return ServerDTO{
		ID:                   int64(s.ID()),
		Name:                 s.Name(),
		Host:                 s.Host(),
		Port:                 s.Port(),
		TLS:                  s.TLS(),
		Username:             s.Username(),
		MaxConns:             s.MaxConns(),
		Priority:             s.Priority(),
		Enabled:              s.Enabled(),
		Backup:               s.Backup(),
		BillingMode:          string(s.BillingMode()),
		QuotaBytes:           s.QuotaBytes(),
		UsedBytes:            s.UsedBytes(),
		BandwidthBytesPerSec: s.BandwidthBytesPerSec(),
		AddedAt:              s.AddedAt(),
		UpdatedAt:            s.UpdatedAt(),
	}
}

func categoryToDTO(c sqlite.Category) CategoryDTO {
	return CategoryDTO{
		Name:     c.Name,
		Dir:      c.Dir,
		Priority: c.Priority,
	}
}
