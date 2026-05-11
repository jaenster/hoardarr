// Package fixture generates a realistic Usenet release: multiple data
// files + a PAR2 recovery set + yEnc-encoded articles + an NZB pointing
// at all of them.
//
// Used by tests that want to exercise the *full* hoardarr pipeline —
// download, verify, repair, deliver — against a slow / lossy fake NNTP
// server. The "missing articles" knob on testserver/nntp combined with
// a PAR2 set with enough redundancy lets you intentionally drop ~N%
// of articles and assert that hoardarr repairs the result.
//
// Output:
//   - NZB:     the .nzb XML bytes ready to drop on /api/v1/queue/nzb
//   - Files:   map filename → raw bytes (for assertion of final output)
//   - Articles: map msgID → yEnc-encoded body (register on Server.AddArticle)
package fixture

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"hash/crc32"

	"github.com/jaenster/hoardarr/internal/adapter/par2"
	testnntp "github.com/jaenster/hoardarr/internal/testserver/nntp"
)

// Options controls fixture shape.
type Options struct {
	// Name appears as the NZB / par-set base name (e.g. "movie.s01e01").
	Name string

	// FileCount is how many data files the release contains. Each
	// file gets FileSize bytes of random data.
	FileCount int

	// FileSize is the size of each data file in bytes.
	FileSize int

	// ArticleSize controls the yEnc segment size (i.e. how much of a
	// file fits in one Usenet article). Typical real values are
	// around 768 KiB; for tests, use whatever exercises the
	// multi-segment path.
	ArticleSize int

	// PAR2SliceSize is the PAR2 recovery slice size in bytes. Must be
	// even and a divisor of FileSize for clean math; the generator
	// rounds up otherwise.
	PAR2SliceSize int

	// RecoverySlices is how many recovery slices the PAR2 set
	// contains. Sets the maximum missing slices we can repair.
	// Set this above the worst-case losses you'll intentionally
	// induce via testserver.MissingFraction.
	RecoverySlices int
}

// Fixture is the generated material.
type Fixture struct {
	// NZB bytes — POST to /api/v1/queue/nzb.
	NZB []byte
	// Files is filename → raw bytes (no yEnc, no PAR2). Useful for
	// assertions that delivered output matches expected.
	Files map[string][]byte
	// Articles is msg-id → yEnc-encoded body. Register each on the
	// testserver via Server.AddArticle.
	Articles map[string][]byte
	// FileCount + RecoveryFileCount echo back what was generated.
	FileCount         int
	RecoveryFileCount int
}

// Generate builds a fixture according to opts.
//
// PAR2 layout: ONE recovery file per recovery slice (par2cmdline's
// "one slice per .par2 file" mode). This keeps the generator simple
// and matches what most real releases look like for small slice
// counts. For larger redundancy a real producer would bundle
// multiple slices per file; not needed here.
func Generate(opts Options) (*Fixture, error) {
	if opts.Name == "" {
		opts.Name = "release"
	}
	if opts.FileCount <= 0 {
		opts.FileCount = 1
	}
	if opts.FileSize <= 0 {
		opts.FileSize = 1 << 20
	}
	if opts.ArticleSize <= 0 {
		opts.ArticleSize = 256 * 1024
	}
	if opts.PAR2SliceSize <= 0 {
		opts.PAR2SliceSize = 64 * 1024
	}
	if opts.PAR2SliceSize%2 != 0 {
		opts.PAR2SliceSize++
	}
	if opts.RecoverySlices < 0 {
		opts.RecoverySlices = 0
	}

	// --- generate random data files ----------------------------------
	files := make(map[string][]byte, opts.FileCount)
	fileNames := make([]string, 0, opts.FileCount)
	for i := 0; i < opts.FileCount; i++ {
		buf := make([]byte, opts.FileSize)
		if _, err := rand.Read(buf); err != nil {
			return nil, fmt.Errorf("rand: %w", err)
		}
		name := fmt.Sprintf("%s.part%03d", opts.Name, i+1)
		files[name] = buf
		fileNames = append(fileNames, name)
	}

	// --- build PAR2 recovery set -------------------------------------
	// Slice every file into PAR2SliceSize chunks. The recovery slices
	// are linear combinations of ALL slices across all files, in the
	// order Main declares them.
	type fileEntry struct {
		name    string
		bytes   []byte
		fileID  [16]byte
		fullMD5 [16]byte
		md5_16k [16]byte
		slices  [][]byte
		checks  []par2.SliceCheck
	}
	entries := make([]*fileEntry, 0, opts.FileCount)
	for _, name := range fileNames {
		body := files[name]
		fe := &fileEntry{name: name, bytes: body}
		fe.fullMD5 = md5.Sum(body)
		first16k := body
		if len(first16k) > 16384 {
			first16k = first16k[:16384]
		}
		fe.md5_16k = md5.Sum(first16k)
		fe.fileID = computeFileID(fe.fullMD5, fe.md5_16k, uint64(len(body)), name)
		fe.slices = par2.SplitIntoSlices(body, opts.PAR2SliceSize)
		fe.checks = make([]par2.SliceCheck, len(fe.slices))
		for i, s := range fe.slices {
			fe.checks[i].MD5 = md5.Sum(s)
			fe.checks[i].CRC32 = crc32.ChecksumIEEE(s)
		}
		entries = append(entries, fe)
	}

	// Concatenate all data slices in main-declared file order — this
	// is the slice array the recovery slices are computed over.
	var allSlices [][]byte
	for _, fe := range entries {
		allSlices = append(allSlices, fe.slices...)
	}

	// SetID is derived from the file IDs the same way par2cmdline
	// does it; for tests, a stable hash of the name + file count is
	// fine.
	var setID [16]byte
	{
		h := md5.New()
		h.Write([]byte(opts.Name))
		for _, fe := range entries {
			h.Write(fe.fileID[:])
		}
		copy(setID[:], h.Sum(nil))
	}

	// --- assemble the .par2 index file (Main + FileDesc + IFSC) ------
	fileIDs := make([][16]byte, len(entries))
	for i, fe := range entries {
		fileIDs[i] = fe.fileID
	}
	indexPar2 := par2.EncodeMain(setID, uint64(opts.PAR2SliceSize), fileIDs)
	for _, fe := range entries {
		indexPar2 = append(indexPar2,
			par2.EncodeFileDesc(setID, fe.fileID, fe.fullMD5, fe.md5_16k, uint64(len(fe.bytes)), fe.name)...)
		indexPar2 = append(indexPar2, par2.EncodeIFSC(setID, fe.fileID, fe.checks)...)
	}
	indexPar2 = append(indexPar2, par2.EncodeCreator(setID, "hoardarr-fixture-gen")...)

	indexName := opts.Name + ".par2"
	files[indexName] = indexPar2

	// --- one recovery-slice file per recovery slice ------------------
	for r := 0; r < opts.RecoverySlices; r++ {
		exponent := uint16(r + 1) // exponents start at 1 per PAR2 spec
		body := par2.EncodeRecoverySlice(allSlices, exponent)
		// Each recovery file is the Main + FileDesc + IFSC index PLUS
		// one Recv packet. Real producers include the full index in
		// every .par2 file so any single one is "complete" — that's
		// what hoardarr's verifier expects.
		fileBody := append([]byte(nil), indexPar2...)
		fileBody = append(fileBody, par2.EncodeRecvSlc(setID, exponent, body)...)
		recName := fmt.Sprintf("%s.vol%03d+01.par2", opts.Name, r)
		files[recName] = fileBody
	}

	// --- yEnc-encode every file as multi-part articles + build NZB ---
	articles := make(map[string][]byte)
	nzbFiles := make([]testnntp.FileSpec, 0, len(files))
	for _, name := range orderedNames(opts.Name, opts.FileCount, opts.RecoverySlices) {
		body, ok := files[name]
		if !ok {
			continue
		}
		segCount := (len(body) + opts.ArticleSize - 1) / opts.ArticleSize
		if segCount < 1 {
			segCount = 1
		}
		segs := make([]testnntp.SegmentSpec, 0, segCount)
		for i := 0; i < segCount; i++ {
			start := i * opts.ArticleSize
			end := start + opts.ArticleSize
			if end > len(body) {
				end = len(body)
			}
			msgID := fmt.Sprintf("%s.%03d@hoardarr-fixture", name, i+1)
			begin1 := int64(start + 1)
			end1 := int64(end)
			encoded := testnntp.EncodeArticlePart(name, body[start:end], begin1, end1, int64(len(body)), i+1, segCount)
			articles[msgID] = encoded
			segs = append(segs, testnntp.SegmentSpec{MessageID: msgID, Bytes: int64(end - start)})
		}
		nzbFiles = append(nzbFiles, testnntp.FileSpec{Filename: name, Segments: segs})
	}

	return &Fixture{
		NZB:               testnntp.BuildNZB(nzbFiles),
		Files:             files,
		Articles:          articles,
		FileCount:         opts.FileCount,
		RecoveryFileCount: opts.RecoverySlices,
	}, nil
}

// computeFileID matches the PAR2 file-id derivation:
// MD5(md5_16k || size_le || name).
func computeFileID(_ [16]byte, md516k [16]byte, size uint64, name string) [16]byte {
	h := md5.New()
	h.Write(md516k[:])
	var sb [8]byte
	for i := uint(0); i < 8; i++ {
		sb[i] = byte(size >> (8 * i))
	}
	h.Write(sb[:])
	h.Write([]byte(name))
	var id [16]byte
	copy(id[:], h.Sum(nil))
	return id
}

// orderedNames returns the canonical order to walk the file set
// (data files first, then PAR2 index, then recovery files).
func orderedNames(base string, dataCount, recCount int) []string {
	out := make([]string, 0, dataCount+recCount+1)
	for i := 0; i < dataCount; i++ {
		out = append(out, fmt.Sprintf("%s.part%03d", base, i+1))
	}
	out = append(out, base+".par2")
	for r := 0; r < recCount; r++ {
		out = append(out, fmt.Sprintf("%s.vol%03d+01.par2", base, r))
	}
	return out
}
