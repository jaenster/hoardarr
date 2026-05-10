package download

// FileID identifies a File entity within a Job.
type FileID int64

// File is one logical file split across one or more segments. Files
// belong to a Job and inherit its lifecycle.
type File struct {
	id           FileID
	jobID        JobID
	filename     string
	poster       string
	groups       []string
	sizeBytes    int64
	state        FileState
	segmentCount int
	segmentsDone int
	isPar2       bool
	segments     []*Segment
}

// NewFileParams constructs a fresh pending File along with its
// segments.
type NewFileParams struct {
	Filename  string
	Poster    string
	Groups    []string
	SizeBytes int64
	IsPar2    bool
	Segments  []NewSegmentParams
}

func newFile(p NewFileParams) *File {
	f := &File{
		filename:     p.Filename,
		poster:       p.Poster,
		groups:       append([]string(nil), p.Groups...),
		sizeBytes:    p.SizeBytes,
		state:        FileStatePending,
		segmentCount: len(p.Segments),
		isPar2:       p.IsPar2,
	}
	for _, sp := range p.Segments {
		f.segments = append(f.segments, newSegment(sp))
	}
	return f
}

// HydrateFileParams is what the repository hands back when loading.
type HydrateFileParams struct {
	ID           FileID
	JobID        JobID
	Filename     string
	Poster       string
	Groups       []string
	SizeBytes    int64
	State        FileState
	SegmentCount int
	SegmentsDone int
	IsPar2       bool
	Segments     []*Segment
}

// HydrateFile is the constructor adapters use to reconstruct a File
// from persistence. No events are emitted.
func HydrateFile(p HydrateFileParams) *File {
	return &File{
		id:           p.ID,
		jobID:        p.JobID,
		filename:     p.Filename,
		poster:       p.Poster,
		groups:       append([]string(nil), p.Groups...),
		sizeBytes:    p.SizeBytes,
		state:        p.State,
		segmentCount: p.SegmentCount,
		segmentsDone: p.SegmentsDone,
		isPar2:       p.IsPar2,
		segments:     p.Segments,
	}
}

// Accessors.
func (f *File) ID() FileID         { return f.id }
func (f *File) JobID() JobID       { return f.jobID }
func (f *File) Filename() string   { return f.filename }
func (f *File) Poster() string     { return f.poster }
func (f *File) Groups() []string   { return append([]string(nil), f.groups...) }
func (f *File) SizeBytes() int64   { return f.sizeBytes }
func (f *File) State() FileState   { return f.state }
func (f *File) SegmentCount() int  { return f.segmentCount }
func (f *File) SegmentsDone() int  { return f.segmentsDone }
func (f *File) IsPar2() bool       { return f.isPar2 }
func (f *File) Segments() []*Segment {
	out := make([]*Segment, len(f.segments))
	copy(out, f.segments)
	return out
}

// SetID is called by the repository after insert.
func (f *File) SetID(id FileID) {
	f.id = id
	for _, s := range f.segments {
		s.SetFileID(id)
	}
}

// SetJobID is called when the file is associated with a (newly-saved)
// job row.
func (f *File) SetJobID(id JobID) { f.jobID = id }
