package par2

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestParse_RoundTrip(t *testing.T) {
	setID := mustHexBytes16("00112233445566778899aabbccddeeff")
	fileID := mustHexBytes16("ff112233445566778899aabbccddeeff")

	payload := []byte("hello par2 verify world — twelve dozen test bytes")
	fileMD5 := md5.Sum(payload)
	first16k := payload
	if len(first16k) > 16384 {
		first16k = first16k[:16384]
	}
	first16kMD5 := md5.Sum(first16k)

	// One slice covering the whole payload.
	sliceMD5 := md5.Sum(payload)
	sliceCRC := crc32.ChecksumIEEE(payload)
	slices := []SliceCheck{{MD5: sliceMD5, CRC32: sliceCRC}}

	stream := bytes.Buffer{}
	stream.Write(EncodeMain(setID, uint64(len(payload)), [][16]byte{fileID}))
	stream.Write(EncodeFileDesc(setID, fileID, fileMD5, first16kMD5, uint64(len(payload)), "hello.bin"))
	stream.Write(EncodeIFSC(setID, fileID, slices))
	stream.Write(EncodeCreator(setID, "hoardarr-test"))

	got, err := Parse(&stream)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.SetID != setID {
		t.Errorf("SetID = %x; want %x", got.SetID, setID)
	}
	if got.SliceSize != uint64(len(payload)) {
		t.Errorf("SliceSize = %d; want %d", got.SliceSize, len(payload))
	}
	if len(got.RecoveryFiles) != 1 || got.RecoveryFiles[0] != fileID {
		t.Errorf("RecoveryFiles = %v", got.RecoveryFiles)
	}
	if len(got.Files) != 1 {
		t.Fatalf("Files = %d; want 1", len(got.Files))
	}
	f := got.Files[0]
	if f.Name != "hello.bin" {
		t.Errorf("Name = %q; want hello.bin", f.Name)
	}
	if f.Size != uint64(len(payload)) {
		t.Errorf("Size = %d; want %d", f.Size, len(payload))
	}
	if f.MD5 != fileMD5 {
		t.Errorf("MD5 mismatch")
	}
	if len(f.Slices) != 1 {
		t.Fatalf("Slices = %d; want 1", len(f.Slices))
	}
	if f.Slices[0].MD5 != sliceMD5 || f.Slices[0].CRC32 != sliceCRC {
		t.Errorf("Slice check mismatch")
	}
	if got.Creator != "hoardarr-test" {
		t.Errorf("Creator = %q", got.Creator)
	}
}

// PAR2 bodies whose MD5 doesn't match the header are corrupt and must
// be silently skipped (per spec — the stream may also legitimately
// contain non-PAR2 framing bytes).
func TestParse_SkipsCorruptPacket(t *testing.T) {
	setID := mustHexBytes16("00112233445566778899aabbccddeeff")

	good := EncodeCreator(setID, "good")
	corrupt := EncodeCreator(setID, "corrupt")
	// Flip one byte of the corrupt packet's body to invalidate its MD5.
	corrupt[len(corrupt)-1] ^= 0xff

	var buf bytes.Buffer
	buf.Write(corrupt)
	buf.Write(good)

	got, err := Parse(&buf)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.Creator != "good" {
		t.Errorf("Creator = %q; want good (corrupt should have been skipped)", got.Creator)
	}
}

func TestParseDir_FindsAllPar2Files(t *testing.T) {
	dir := t.TempDir()
	setID := mustHexBytes16("aa112233445566778899aabbccddeeff")
	fileID := mustHexBytes16("bb112233445566778899aabbccddeeff")

	// .par2 (index): Main + FileDesc + Creator
	indexBuf := bytes.Buffer{}
	zeroMD5 := [16]byte{}
	indexBuf.Write(EncodeMain(setID, 1024, [][16]byte{fileID}))
	indexBuf.Write(EncodeFileDesc(setID, fileID, zeroMD5, zeroMD5, 0, "release.bin"))
	indexBuf.Write(EncodeCreator(setID, "test"))
	if err := os.WriteFile(filepath.Join(dir, "release.par2"), indexBuf.Bytes(), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	// .vol00+01.par2: same descriptive packets + IFSC.
	slices := []SliceCheck{{MD5: zeroMD5, CRC32: 0}}
	volBuf := bytes.Buffer{}
	volBuf.Write(EncodeMain(setID, 1024, [][16]byte{fileID}))
	volBuf.Write(EncodeFileDesc(setID, fileID, zeroMD5, zeroMD5, 0, "release.bin"))
	volBuf.Write(EncodeIFSC(setID, fileID, slices))
	if err := os.WriteFile(filepath.Join(dir, "release.vol00+01.par2"), volBuf.Bytes(), 0o644); err != nil {
		t.Fatalf("write vol: %v", err)
	}

	got, err := ParseDir(dir)
	if err != nil {
		t.Fatalf("ParseDir: %v", err)
	}
	if got.SetID != setID {
		t.Errorf("SetID = %x", got.SetID)
	}
	if len(got.Files) != 1 {
		t.Errorf("Files = %d; want 1 (deduped across files)", len(got.Files))
	}
	if len(got.Files[0].Slices) != 1 {
		t.Errorf("Slices = %d; want 1 (from IFSC)", len(got.Files[0].Slices))
	}
}

func TestParseDir_NoPar2Files(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "not_par2.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := ParseDir(dir)
	if err != ErrNoPar2 {
		t.Errorf("err = %v; want ErrNoPar2", err)
	}
}

// PAR2 streams from different recovery sets (different set_ids) must
// be rejected — they're not the same set and merging would be wrong.
func TestParse_MixedSetIDs(t *testing.T) {
	setA := mustHexBytes16("00112233445566778899aabbccddeeff")
	setB := mustHexBytes16("ff000000000000000000000000000000")

	var buf bytes.Buffer
	buf.Write(EncodeCreator(setA, "set a"))
	buf.Write(EncodeCreator(setB, "set b"))

	_, err := Parse(&buf)
	if err == nil {
		t.Fatal("expected error for mixed set_ids")
	}
}

func mustHexBytes16(s string) [16]byte {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		panic("bad hex16")
	}
	var out [16]byte
	copy(out[:], b)
	return out
}
