package par2

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// TestRepair_SingleSliceMissing is the foundational happy-path test:
// 1. Synthesise a clean payload split into 4 slices of 16 bytes.
// 2. Build the IFSC/FileDesc + 2 RecvSlc packets via the encode helpers.
// 3. Damage one slice in the on-disk file.
// 4. Run Repair.
// 5. Assert the file matches the original byte-for-byte.
func TestRepair_SingleSliceMissing(t *testing.T) {
	const sliceSize = 16
	original := make([]byte, sliceSize*4-3) // exercise the zero-pad case (not a multiple of sliceSize)
	if _, err := rand.Read(original); err != nil {
		t.Fatalf("rand: %v", err)
	}

	tmp := t.TempDir()
	dataPath := filepath.Join(tmp, "release.bin")
	par2Path := filepath.Join(tmp, "release.par2")

	dataMD5 := md5.Sum(original)

	// Compute per-slice IFSC entries.
	slices := SplitIntoSlices(original, sliceSize)
	checks := make([]SliceCheck, len(slices))
	for i, s := range slices {
		checks[i].MD5 = md5.Sum(s)
		checks[i].CRC32 = crc32.ChecksumIEEE(s)
	}

	setID := [16]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F}
	fileID := [16]byte{0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F}

	// Two recovery slices is enough to repair one missing — we use
	// two to also exercise the case where we have more RS than damage.
	exps := []uint16{1, 2}

	par2Buf := bytes.Buffer{}
	par2Buf.Write(EncodeMain(setID, sliceSize, [][16]byte{fileID}))
	par2Buf.Write(EncodeFileDesc(setID, fileID, dataMD5, dataMD5, uint64(len(original)), "release.bin"))
	par2Buf.Write(EncodeIFSC(setID, fileID, checks))
	for _, e := range exps {
		recv := EncodeRecoverySlice(slices, e)
		par2Buf.Write(EncodeRecvSlc(setID, e, recv))
	}
	par2Buf.Write(EncodeCreator(setID, "hoardarr-repair-test"))

	if err := os.WriteFile(dataPath, original, 0o644); err != nil {
		t.Fatalf("write data: %v", err)
	}
	if err := os.WriteFile(par2Path, par2Buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write par2: %v", err)
	}

	// Corrupt slice index 1 (middle of the file).
	corrupt, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	for j := sliceSize; j < 2*sliceSize; j++ {
		corrupt[j] = 0xFF
	}
	if err := os.WriteFile(dataPath, corrupt, 0o644); err != nil {
		t.Fatalf("rewrite corrupted: %v", err)
	}

	// Sanity: file MD5 no longer matches before repair.
	if h := mustFileMD5(t, dataPath); h == dataMD5 {
		t.Fatal("expected pre-repair MD5 mismatch")
	}

	res, err := Repair(context.Background(), RepairInput{
		Par2Paths: []string{par2Path},
		DataPaths: map[string]string{"release.bin": dataPath},
	})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if len(res.Failed) != 0 {
		t.Errorf("Failed entries: %+v", res.Failed)
	}
	if len(res.Repaired) != 1 {
		t.Fatalf("Repaired count = %d; want 1", len(res.Repaired))
	}
	if res.Repaired[0].Filename != "release.bin" {
		t.Errorf("repaired name = %q", res.Repaired[0].Filename)
	}

	// Byte-equivalence post-repair.
	got, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read post: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("post-repair bytes differ from original (got %d, want %d)",
			len(got), len(original))
	}
}

// TestRepair_AlreadyOK is a no-op repair on an undamaged file.
func TestRepair_AlreadyOK(t *testing.T) {
	const sliceSize = 16
	original := make([]byte, sliceSize*3)
	if _, err := rand.Read(original); err != nil {
		t.Fatalf("rand: %v", err)
	}
	tmp := t.TempDir()
	dataPath := filepath.Join(tmp, "x.bin")
	par2Path := filepath.Join(tmp, "x.par2")

	dataMD5 := md5.Sum(original)
	slices := SplitIntoSlices(original, sliceSize)
	checks := make([]SliceCheck, len(slices))
	for i, s := range slices {
		checks[i].MD5 = md5.Sum(s)
		checks[i].CRC32 = crc32.ChecksumIEEE(s)
	}

	setID := [16]byte{0x42}
	fileID := [16]byte{0x77}
	par2Buf := bytes.Buffer{}
	par2Buf.Write(EncodeMain(setID, sliceSize, [][16]byte{fileID}))
	par2Buf.Write(EncodeFileDesc(setID, fileID, dataMD5, dataMD5, uint64(len(original)), "x.bin"))
	par2Buf.Write(EncodeIFSC(setID, fileID, checks))
	par2Buf.Write(EncodeRecvSlc(setID, 1, EncodeRecoverySlice(slices, 1)))
	par2Buf.Write(EncodeCreator(setID, "test"))

	_ = os.WriteFile(dataPath, original, 0o644)
	_ = os.WriteFile(par2Path, par2Buf.Bytes(), 0o644)

	res, err := Repair(context.Background(), RepairInput{
		Par2Paths: []string{par2Path},
		DataPaths: map[string]string{"x.bin": dataPath},
	})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if len(res.Repaired) != 0 {
		t.Errorf("expected 0 Repaired; got %+v", res.Repaired)
	}
	if len(res.AlreadyOK) != 1 || res.AlreadyOK[0] != "x.bin" {
		t.Errorf("AlreadyOK = %v", res.AlreadyOK)
	}
}

// TestRepair_TooFewRecoverySlices: corrupt 2 slices but provide only 1
// recovery slice. Expect ErrUnrecoverableSet plus the file reported as
// Failed.
func TestRepair_TooFewRecoverySlices(t *testing.T) {
	const sliceSize = 16
	original := make([]byte, sliceSize*4)
	if _, err := rand.Read(original); err != nil {
		t.Fatalf("rand: %v", err)
	}
	tmp := t.TempDir()
	dataPath := filepath.Join(tmp, "x.bin")
	par2Path := filepath.Join(tmp, "x.par2")

	dataMD5 := md5.Sum(original)
	slices := SplitIntoSlices(original, sliceSize)
	checks := make([]SliceCheck, len(slices))
	for i, s := range slices {
		checks[i].MD5 = md5.Sum(s)
		checks[i].CRC32 = crc32.ChecksumIEEE(s)
	}

	setID := [16]byte{0x42}
	fileID := [16]byte{0x77}
	par2Buf := bytes.Buffer{}
	par2Buf.Write(EncodeMain(setID, sliceSize, [][16]byte{fileID}))
	par2Buf.Write(EncodeFileDesc(setID, fileID, dataMD5, dataMD5, uint64(len(original)), "x.bin"))
	par2Buf.Write(EncodeIFSC(setID, fileID, checks))
	par2Buf.Write(EncodeRecvSlc(setID, 1, EncodeRecoverySlice(slices, 1)))
	par2Buf.Write(EncodeCreator(setID, "test"))

	_ = os.WriteFile(dataPath, original, 0o644)
	_ = os.WriteFile(par2Path, par2Buf.Bytes(), 0o644)

	// Corrupt 2 slices.
	corrupt, _ := os.ReadFile(dataPath)
	for j := 0; j < sliceSize; j++ {
		corrupt[j] = 0xAA
		corrupt[j+sliceSize] = 0xBB
	}
	_ = os.WriteFile(dataPath, corrupt, 0o644)

	res, err := Repair(context.Background(), RepairInput{
		Par2Paths: []string{par2Path},
		DataPaths: map[string]string{"x.bin": dataPath},
	})
	if err != ErrUnrecoverableSet {
		t.Fatalf("err = %v; want ErrUnrecoverableSet", err)
	}
	if len(res.Failed) != 1 || res.Failed[0].Filename != "x.bin" {
		t.Errorf("Failed = %+v", res.Failed)
	}
}

func mustFileMD5(t *testing.T, path string) [16]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return md5.Sum(b)
}
