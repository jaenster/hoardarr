// Package par2 parses PAR2 files (Parity Archive Volume 2) per the
// reference spec at parchive.sourceforge.net.
//
// Scope:
//   - Packet parser (Main / FileDesc / IFSC / Creator); RecoverySlice
//     packets are recognised but only their metadata is retained
//     (the actual recovery data is needed only by M3b's repair path,
//     which lives in a sibling module).
//   - Recovery-set assembly: dedupes repeated packets across multiple
//     .par2 files (index + vol files all share the same set_id and
//     repeat the descriptive packets).
//   - Per-packet MD5 verification — defends against partial / corrupt
//     PAR2 file downloads.
//
// Out of scope (M3b):
//   - GF(2^16) Reed-Solomon math.
//   - Slice reconstruction.
//
// The verify path (M3a) only needs the parsed structures here plus
// MD5 / CRC32 computed over the assembled output files.
package par2

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// magic is the 8-byte PAR2 packet preamble.
var magic = []byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}

// Packet types (16-byte right-padded ASCII identifiers).
var (
	typeMain     = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'M', 'a', 'i', 'n', 0, 0, 0, 0}
	typeFileDesc = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'F', 'i', 'l', 'e', 'D', 'e', 's', 'c'}
	typeIFSC     = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'I', 'F', 'S', 'C', 0, 0, 0, 0}
	typeRecvSlc  = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'R', 'e', 'c', 'v', 'S', 'l', 'i', 'c'}
	typeCreator  = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'C', 'r', 'e', 'a', 't', 'o', 'r', 0}
)

// RecoverySet is the consolidated view across all .par2 files of a
// single recovery set (matched by SetID).
type RecoverySet struct {
	SetID         [16]byte
	SliceSize     uint64
	RecoveryFiles [][16]byte // file IDs that will be reconstructed
	Files         []*ParFile

	// Creator is informational — the producing client's name.
	Creator string

	// RecoverySliceCount is the total recovery slice count observed
	// across input files; used by M3b to decide whether enough vol
	// files have been fetched. M3a doesn't consume it.
	RecoverySliceCount int
}

// ParFile is one recoverable file's descriptor + per-slice checksums.
type ParFile struct {
	ID     [16]byte
	Name   string
	Size   uint64
	MD5    [16]byte // MD5 of full file
	MD516k [16]byte // MD5 of first 16384 bytes (or full file if smaller)
	Slices []SliceCheck
}

// SliceCheck is one slice's integrity bundle from an IFSC packet.
type SliceCheck struct {
	MD5   [16]byte
	CRC32 uint32
}

// Packet is a parsed but type-dispatched PAR2 packet.
type Packet struct {
	SetID [16]byte
	Type  [16]byte
	Body  []byte
}

// ParseDir scans dir for .par2 files and builds the consolidated
// RecoverySet. Returns ErrNoPar2 if no PAR2 files found.
func ParseDir(dir string) (*RecoverySet, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(e.Name()), ".par2") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	if len(paths) == 0 {
		return nil, ErrNoPar2
	}
	sort.Strings(paths)

	set := &RecoverySet{}
	seenSetID := false
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", p, err)
		}
		err = parsePackets(bufio.NewReader(f), func(pkt Packet) error {
			if !seenSetID {
				set.SetID = pkt.SetID
				seenSetID = true
			} else if set.SetID != pkt.SetID {
				return fmt.Errorf("par2: file %q has set_id %x; expected %x", p, pkt.SetID, set.SetID)
			}
			return set.consume(pkt)
		})
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", p, err)
		}
	}
	return set, nil
}

// Parse reads PAR2 packets from r and returns the consolidated set.
func Parse(r io.Reader) (*RecoverySet, error) {
	set := &RecoverySet{}
	seen := false
	br := bufio.NewReader(r)
	err := parsePackets(br, func(pkt Packet) error {
		if !seen {
			set.SetID = pkt.SetID
			seen = true
		} else if set.SetID != pkt.SetID {
			return fmt.Errorf("par2: mixed set_ids: %x vs %x", set.SetID, pkt.SetID)
		}
		return set.consume(pkt)
	})
	return set, err
}

// ErrNoPar2 is returned by ParseDir when the directory contains no
// .par2 files.
var ErrNoPar2 = errors.New("par2: no .par2 files found")

// parsePackets reads all PAR2 packets from r, validating each
// packet's body MD5 against the header's claimed digest. Invalid
// packets are silently skipped — that's per the spec, since a PAR2
// stream may legitimately contain non-PAR2 prefix/suffix bytes
// (e.g. when concatenated with other content).
//
// The callback is invoked for each verified packet. Return non-nil
// from cb to abort.
func parsePackets(r *bufio.Reader, cb func(Packet) error) error {
	for {
		// Find the next magic.
		if err := readUntilMagic(r); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		// Read 64-byte header (we already consumed the magic).
		var hdr [64 - 8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read header: %w", err)
		}

		length := binary.LittleEndian.Uint64(hdr[0:8])
		if length < 64 {
			// Invalid; resync.
			continue
		}
		var declaredMD5 [16]byte
		copy(declaredMD5[:], hdr[8:24])
		var setID [16]byte
		copy(setID[:], hdr[24:40])
		var pktType [16]byte
		copy(pktType[:], hdr[40:56])

		bodyLen := length - 64
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return fmt.Errorf("read body: %w", err)
		}

		// MD5 covers set_id + type + body (everything after the
		// declared MD5 field).
		h := md5.New()
		h.Write(setID[:])
		h.Write(pktType[:])
		h.Write(body)
		var actual [16]byte
		copy(actual[:], h.Sum(nil))
		if actual != declaredMD5 {
			// Skip — corrupt packet. Keep scanning for more.
			continue
		}

		if err := cb(Packet{SetID: setID, Type: pktType, Body: body}); err != nil {
			return err
		}
	}
}

// readUntilMagic advances r past the next occurrence of the PAR2
// magic bytes. Returns io.EOF if the stream ends without a magic.
//
// Implementation note: we read byte-at-a-time after the first match
// of the first byte. Inefficient on huge non-PAR2 prefixes, but real
// inputs are .par2 files with the magic at offset 0.
func readUntilMagic(r *bufio.Reader) error {
	matched := 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b == magic[matched] {
			matched++
			if matched == len(magic) {
				return nil
			}
		} else {
			// Reset; if the mismatched byte is the first magic byte,
			// keep partial-match state.
			if b == magic[0] {
				matched = 1
			} else {
				matched = 0
			}
		}
	}
}

func (s *RecoverySet) consume(pkt Packet) error {
	switch pkt.Type {
	case typeMain:
		return s.consumeMain(pkt.Body)
	case typeFileDesc:
		return s.consumeFileDesc(pkt.Body)
	case typeIFSC:
		return s.consumeIFSC(pkt.Body)
	case typeRecvSlc:
		s.RecoverySliceCount++
		return nil
	case typeCreator:
		// Free-form ASCII string, possibly NUL-padded.
		s.Creator = strings.TrimRight(string(pkt.Body), "\x00 \r\n\t")
		return nil
	default:
		// Unknown packet type — spec says ignore.
		return nil
	}
}

func (s *RecoverySet) consumeMain(body []byte) error {
	if len(body) < 12 {
		return errors.New("par2: main packet too short")
	}
	s.SliceSize = binary.LittleEndian.Uint64(body[0:8])
	numFiles := binary.LittleEndian.Uint32(body[8:12])
	off := 12
	if int(numFiles)*16 > len(body)-off {
		return errors.New("par2: main packet too short for file list")
	}
	s.RecoveryFiles = make([][16]byte, numFiles)
	for i := uint32(0); i < numFiles; i++ {
		copy(s.RecoveryFiles[i][:], body[off:off+16])
		off += 16
	}
	return nil
}

func (s *RecoverySet) consumeFileDesc(body []byte) error {
	if len(body) < 56 {
		return errors.New("par2: filedesc too short")
	}
	var id [16]byte
	copy(id[:], body[0:16])

	if s.fileByID(id) != nil {
		// Already seen; FileDesc repeats across .par2 files. Skip.
		return nil
	}

	f := &ParFile{ID: id}
	copy(f.MD5[:], body[16:32])
	copy(f.MD516k[:], body[32:48])
	f.Size = binary.LittleEndian.Uint64(body[48:56])
	// Filename runs from offset 56 to end of body, NUL-padded to a
	// 4-byte boundary.
	name := body[56:]
	name = bytes.TrimRight(name, "\x00")
	f.Name = string(name)
	s.Files = append(s.Files, f)
	return nil
}

func (s *RecoverySet) consumeIFSC(body []byte) error {
	if len(body) < 16 {
		return errors.New("par2: ifsc too short")
	}
	var id [16]byte
	copy(id[:], body[0:16])
	rest := body[16:]
	if len(rest)%20 != 0 {
		return fmt.Errorf("par2: ifsc remainder %d not multiple of 20", len(rest))
	}
	f := s.fileByID(id)
	if f == nil {
		// IFSC arrived before FileDesc — create a stub.
		f = &ParFile{ID: id}
		s.Files = append(s.Files, f)
	}
	if len(f.Slices) > 0 {
		// Already populated from a different .par2 file; trust the
		// first one (they should agree by spec).
		return nil
	}
	count := len(rest) / 20
	f.Slices = make([]SliceCheck, count)
	for i := 0; i < count; i++ {
		off := i * 20
		copy(f.Slices[i].MD5[:], rest[off:off+16])
		f.Slices[i].CRC32 = binary.LittleEndian.Uint32(rest[off+16 : off+20])
	}
	return nil
}

func (s *RecoverySet) fileByID(id [16]byte) *ParFile {
	for _, f := range s.Files {
		if f.ID == id {
			return f
		}
	}
	return nil
}
