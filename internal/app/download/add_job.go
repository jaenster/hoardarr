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
		// Fast-path dedupe by hash.
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
			// Race safety net: the pre-check passed but a concurrent
			// upload won the INSERT first. Map UNIQUE-violation back
			// to ErrDuplicateNZB and look up the winner's id.
			if errors.Is(err, download.ErrDuplicateNZBHash) {
				if winner, lerr := s.repo.ByNZBHash(ctx, hash); lerr == nil {
					id = winner.ID()
				}
				return ErrDuplicateNZB
			}
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
	// Prefer a non-PAR2 filename. PAR2 fragments like
	// "release.vol00+01.par2" make terrible job labels because they
	// expose internal release naming + the volume suffix; pick a
	// data filename if any exist.
	var dataNames []string
	var allNames []string
	for _, f := range d.Files {
		allNames = append(allNames, f.Filename)
		if !isPar2Filename(f.Filename) {
			dataNames = append(dataNames, f.Filename)
		}
	}
	if name := releaseFromFilenames(dataNames); name != "" {
		return name
	}
	if name := releaseFromFilenames(allNames); name != "" {
		return name
	}
	return "unnamed"
}

// releaseFromFilenames derives a release-name string from a set of
// filenames by stripping the common "this is part N of M" suffix:
//
//	release.part001.rar → release
//	release.r00, release.r01 → release
//	release.001, release.002 → release
//
// If no suffix pattern matches, returns the first filename verbatim.
func releaseFromFilenames(names []string) string {
	if len(names) == 0 {
		return ""
	}
	for _, n := range names {
		if base := stripRelaseSuffix(n); base != "" && base != n {
			return base
		}
	}
	return names[0]
}

// stripRelaseSuffix strips the common multi-part suffix from a
// filename. Returns "" when nothing matched. Recognised:
//
//	".partNNN.rar"   → "release"   (most modern releases)
//	".rNN"           → "release"   (legacy splits)
//	".NNN"           → "release"   (split archives)
//	".vol*.par2"     → "release"   (PAR2 volume — kept as defence in
//	                                depth though chooseName filters)
func stripRelaseSuffix(name string) string {
	low := strings.ToLower(name)
	// .partNNN.rar
	if i := strings.LastIndex(low, ".part"); i >= 0 && strings.HasSuffix(low, ".rar") {
		return name[:i]
	}
	// .rNN  (.r00, .r01, …)
	if i := strings.LastIndex(low, ".r"); i >= 0 && len(low)-i == 4 && allDigits(low[i+2:]) {
		return name[:i]
	}
	// .NNN (numeric split)
	if i := strings.LastIndex(low, "."); i >= 0 && len(low)-i == 4 && allDigits(low[i+1:]) {
		return name[:i]
	}
	// .vol*+*.par2
	if i := strings.LastIndex(low, ".vol"); i >= 0 && strings.HasSuffix(low, ".par2") {
		return name[:i]
	}
	if i := strings.LastIndex(low, "."); i >= 0 && (strings.HasSuffix(low, ".rar") || strings.HasSuffix(low, ".par2")) {
		return name[:i]
	}
	return ""
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
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
