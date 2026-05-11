package par2

// PAR2 packet encoder — synthesises spec-conformant packets without
// needing par2cmdline on the test runner.
//
// All Encode* functions return complete packet bytes (header + body)
// with a valid MD5 over set_id + type + body, so streams produced
// here are indistinguishable from those a real PAR2 producer would
// emit.
//
// Used by:
//   - in-package parser tests (round-trip)
//   - downstream e2e tests (e.g. internal/bootstrap/e2e_*) that
//     synthesise fixtures
//   - a future rebuild-par2 CLI, if we ever add one

import (
	"crypto/md5"
	"encoding/binary"
)

// EncodePacket builds one PAR2 packet (header + body) with a valid MD5.
func EncodePacket(setID [16]byte, pktType [16]byte, body []byte) []byte {
	h := md5.New()
	h.Write(setID[:])
	h.Write(pktType[:])
	h.Write(body)
	bodyMD5 := h.Sum(nil)

	totalLen := uint64(64 + len(body))
	out := make([]byte, 0, totalLen)
	out = append(out, magic...)

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], totalLen)
	out = append(out, lenBuf[:]...)

	out = append(out, bodyMD5...)
	out = append(out, setID[:]...)
	out = append(out, pktType[:]...)
	out = append(out, body...)
	return out
}

// EncodeMain builds a Main packet listing the recoverable file IDs +
// the slice size.
func EncodeMain(setID [16]byte, sliceSize uint64, fileIDs [][16]byte) []byte {
	body := make([]byte, 0, 12+len(fileIDs)*16)

	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], sliceSize)
	body = append(body, sb[:]...)

	var nf [4]byte
	binary.LittleEndian.PutUint32(nf[:], uint32(len(fileIDs)))
	body = append(body, nf[:]...)

	for _, id := range fileIDs {
		body = append(body, id[:]...)
	}
	return EncodePacket(setID, typeMain, body)
}

// EncodeFileDesc builds a FileDesc packet describing one input file.
func EncodeFileDesc(setID, fileID, md5full, md516k [16]byte, size uint64, name string) []byte {
	body := make([]byte, 0, 56+len(name)+4)
	body = append(body, fileID[:]...)
	body = append(body, md5full[:]...)
	body = append(body, md516k[:]...)
	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], size)
	body = append(body, sb[:]...)
	body = append(body, []byte(name)...)
	for len(body)%4 != 0 {
		body = append(body, 0)
	}
	return EncodePacket(setID, typeFileDesc, body)
}

// EncodeIFSC builds an Input File Slice Checksum packet — per-slice
// MD5+CRC32 pairs, in slice order.
func EncodeIFSC(setID, fileID [16]byte, slices []SliceCheck) []byte {
	body := make([]byte, 0, 16+len(slices)*20)
	body = append(body, fileID[:]...)
	for _, s := range slices {
		body = append(body, s.MD5[:]...)
		var cb [4]byte
		binary.LittleEndian.PutUint32(cb[:], s.CRC32)
		body = append(body, cb[:]...)
	}
	return EncodePacket(setID, typeIFSC, body)
}

// EncodeRecvSlc builds a Recovery Slice packet: 4-byte exponent then
// the slice body. The exponent is a 16-bit value in PAR2 but the wire
// format uses 4 bytes (the high two are zero), matching what par2cmdline
// emits.
func EncodeRecvSlc(setID [16]byte, exponent uint16, body []byte) []byte {
	out := make([]byte, 0, 4+len(body))
	var eb [4]byte
	binary.LittleEndian.PutUint32(eb[:], uint32(exponent))
	out = append(out, eb[:]...)
	out = append(out, body...)
	return EncodePacket(setID, typeRecvSlc, out)
}

// EncodeCreator builds a Creator packet — a free-form ASCII string
// identifying the producing client.
func EncodeCreator(setID [16]byte, name string) []byte {
	body := []byte(name)
	for len(body)%4 != 0 {
		body = append(body, 0)
	}
	return EncodePacket(setID, typeCreator, body)
}
