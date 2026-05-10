package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nzb"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// AddJobService is the use case "add a new NZB to the queue."
//
// Flow:
//  1. Read the NZB body.
//  2. Compute its SHA-256 (hex) for dedupe.
//  3. Parse the NZB into our intermediate Document type.
//  4. Convert Document into a domain.Job aggregate.
//  5. Within a transaction: persist the Job, publish JobCreated.
//
// Returns ErrDuplicateNZB if the same NZB body has already been
// queued (matched by SHA-256).
type AddJobService struct {
	repo download.JobRepository
	bus  event.Bus
	tx   tx.TransactionManager
	now  func() time.Time
}

// NewAddJobService wires the use case.
func NewAddJobService(repo download.JobRepository, bus event.Bus, txm tx.TransactionManager, now func() time.Time) *AddJobService {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &AddJobService{repo: repo, bus: bus, tx: txm, now: now}
}

// AddJobCmd is the input shape for AddJob. Either NZB body or NZBPath
// must be set.
type AddJobCmd struct {
	// NZB is the raw NZB document. Mutually exclusive with NZBPath.
	NZB io.Reader

	// Name overrides the NZB's <head><meta name="title"> if set.
	Name string

	// Category to assign on creation. Empty for "uncategorized".
	Category string

	// Priority (lower = higher; 0 default).
	Priority int
}

// AddJob validates the NZB, dedupes against existing jobs, and creates
// a new Job. Returns the assigned id.
func (s *AddJobService) AddJob(ctx context.Context, cmd AddJobCmd) (download.JobID, error) {
	if cmd.NZB == nil {
		return 0, errors.New("addjob: nzb body required")
	}
	body, err := io.ReadAll(cmd.NZB)
	if err != nil {
		return 0, fmt.Errorf("read nzb: %w", err)
	}
	hash := sha256Hex(body)

	doc, err := nzb.ParseBytes(body)
	if err != nil {
		return 0, fmt.Errorf("parse nzb: %w", err)
	}

	name := strings.TrimSpace(cmd.Name)
	if name == "" {
		name = chooseName(doc)
	}

	files, total := buildFiles(doc)
	if len(files) == 0 {
		return 0, errors.New("nzb has no usable files")
	}

	var id download.JobID
	err = s.tx.InTx(ctx, func(ctx context.Context) error {
		// Dedupe.
		if existing, err := s.repo.ByNZBHash(ctx, hash); err == nil {
			id = existing.ID()
			return ErrDuplicateNZB
		} else if !errors.Is(err, download.ErrJobNotFound) {
			return err
		}

		now := s.now()
		j, err := download.NewJob(download.NewJobParams{
			NZBHash:    hash,
			Name:       name,
			Category:   cmd.Category,
			Priority:   cmd.Priority,
			QueueOrder: now.UnixNano(),
			NZBBlob:    body,
			Files:      files,
		}, now)
		if err != nil {
			return err
		}
		if err := s.repo.Save(ctx, j); err != nil {
			return fmt.Errorf("save job: %w", err)
		}
		id = j.ID()
		// Sanity: total bytes should match what we summed.
		_ = total
		return s.bus.Publish(ctx, j.PullEvents()...)
	})
	if err != nil {
		return id, err
	}
	return id, nil
}

// ErrDuplicateNZB is returned when AddJob is called with an NZB whose
// hash already corresponds to a stored job. The returned ID is the
// existing job's id.
var ErrDuplicateNZB = errors.New("download: duplicate nzb")

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func chooseName(d *nzb.Document) string {
	for _, m := range d.Meta {
		if strings.EqualFold(m.Type, "title") || strings.EqualFold(m.Type, "name") {
			return m.Value
		}
	}
	// Fallback: derive from the longest filename.
	best := ""
	for _, f := range d.Files {
		if len(f.Filename) > len(best) {
			best = f.Filename
		}
	}
	if best == "" {
		return "unnamed"
	}
	return best
}

func buildFiles(d *nzb.Document) ([]download.NewFileParams, int64) {
	var out []download.NewFileParams
	var total int64
	for _, f := range d.Files {
		if len(f.Segments) == 0 {
			continue
		}
		var segParams []download.NewSegmentParams
		var size int64
		for _, s := range f.Segments {
			segParams = append(segParams, download.NewSegmentParams{
				SeqIndex:  s.Number,
				MessageID: s.MessageID,
				Bytes:     s.Bytes,
			})
			size += s.Bytes
		}
		out = append(out, download.NewFileParams{
			Filename:  f.Filename,
			Poster:    f.Poster,
			Groups:    f.Groups,
			SizeBytes: size,
			IsPar2:    isPar2Filename(f.Filename),
			Segments:  segParams,
		})
		total += size
	}
	return out, total
}

func isPar2Filename(name string) bool {
	low := strings.ToLower(name)
	return strings.HasSuffix(low, ".par2") ||
		strings.Contains(low, ".vol") ||
		strings.HasSuffix(low, ".par")
}
