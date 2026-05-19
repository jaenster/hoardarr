package download

import "time"

// SegmentID identifies a Segment. Allocated by the persistence layer.
type SegmentID int64

// Segment is one Usenet article — the smallest unit of work for the
// download orchestrator. Each segment maps to a Message-ID, a byte
// range within the parent file, and a per-attempt retry count.
//
// Segments are entities owned by their File, not aggregates. Mutation
// happens through methods on the parent Job (which owns the File).
type Segment struct {
	id        SegmentID
	fileID    FileID
	seqIndex  int    // 1-based within the parent file
	messageID string // without surrounding angle brackets
	bytes     int64  // article size on the wire (overhead included)
	state     SegmentState
	attempts  int
	lastError string
	// fileOffset is the 0-based byte offset within the assembled file
	// where this segment's decoded bytes go. Set after the first
	// successful yEnc decode (from =ypart begin - 1) and persisted.
	fileOffset int64
	// nextRetryAt is the earliest time this segment is eligible for
	// re-dispatch. Zero = ready now. Populated on transient retry so
	// the back-off survives a restart instead of being lost with the
	// in-memory goroutine that was sleeping on time.After.
	nextRetryAt time.Time
}

// NewSegmentParams constructs a fresh pending Segment.
type NewSegmentParams struct {
	SeqIndex  int
	MessageID string
	Bytes     int64
}

func newSegment(p NewSegmentParams) *Segment {
	return &Segment{
		seqIndex:  p.SeqIndex,
		messageID: p.MessageID,
		bytes:     p.Bytes,
		state:     SegmentStatePending,
	}
}

// HydrateSegmentParams is what the repository hands back when loading
// a row.
type HydrateSegmentParams struct {
	ID          SegmentID
	FileID      FileID
	SeqIndex    int
	MessageID   string
	Bytes       int64
	State       SegmentState
	Attempts    int
	LastError   string
	FileOffset  int64
	NextRetryAt time.Time
}

// HydrateSegment is the adapter-side constructor that reconstructs a
// Segment from persistence. No events are emitted.
func HydrateSegment(p HydrateSegmentParams) *Segment {
	return &Segment{
		id:          p.ID,
		fileID:      p.FileID,
		seqIndex:    p.SeqIndex,
		messageID:   p.MessageID,
		bytes:       p.Bytes,
		state:       p.State,
		attempts:    p.Attempts,
		lastError:   p.LastError,
		fileOffset:  p.FileOffset,
		nextRetryAt: p.NextRetryAt,
	}
}

// Accessors.
func (s *Segment) ID() SegmentID       { return s.id }
func (s *Segment) FileID() FileID      { return s.fileID }
func (s *Segment) SeqIndex() int       { return s.seqIndex }
func (s *Segment) MessageID() string   { return s.messageID }
func (s *Segment) Bytes() int64        { return s.bytes }
func (s *Segment) State() SegmentState { return s.state }
func (s *Segment) Attempts() int       { return s.attempts }
func (s *Segment) LastError() string   { return s.lastError }
func (s *Segment) FileOffset() int64   { return s.fileOffset }

// NextRetryAt returns the earliest instant this segment is eligible
// for re-dispatch. Zero = ready now.
func (s *Segment) NextRetryAt() time.Time { return s.nextRetryAt }

// MarkPendingRetry flips a transient-failed (inflight/failed) segment
// back to pending with a durable backoff. After saving, the
// orchestrator's pending-segment query (filtered by next_retry_at)
// will skip this segment until `at` passes — even across a process
// restart.
//
// Records the error string so the per-job timeline + UI surface what
// went wrong on the previous attempt without callers having to track
// it separately.
func (s *Segment) MarkPendingRetry(at time.Time, errMsg string) {
	s.state = SegmentStatePending
	s.nextRetryAt = at.UTC()
	s.attempts++
	s.lastError = errMsg
}

// SetID is called by the repository to assign a database id after
// insert.
func (s *Segment) SetID(id SegmentID) { s.id = id }

// SetFileID is called when a fresh segment is associated with its
// (newly-saved) file row.
func (s *Segment) SetFileID(id FileID) { s.fileID = id }

// SetFileOffset records the 0-based byte offset within the assembled
// file. The orchestrator computes this after decoding the first
// segment of a file (single-part: 0; multi-part: ypart.begin - 1).
func (s *Segment) SetFileOffset(off int64) { s.fileOffset = off }
