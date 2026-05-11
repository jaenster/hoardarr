package bootstrap_test

// M3b end-to-end: drives a corrupted-data scenario through the full
// pipeline and asserts that the repair worker reconstructs the
// damaged slices, verify re-runs successfully, and the file is
// delivered byte-equivalent to the original.
//
// Flow under test:
//
//   NZB has two yEnc articles for one data file (one CORRECT segment,
//   one WRONG segment with matching yEnc CRC so the decoder accepts
//   it), plus a PAR2 file with IFSC+RecvSlc packets generated against
//   the *clean* payload.
//
//   download → file on disk has correct first half + corrupt second half
//   verify   → IFSC slice MD5s don't match → RepairNeeded fires
//   repair   → reconstructs the bad slice from RS → RepairOK fires
//   verify   → re-runs, this time AllOK → VerifyOK fires
//   deliver  → moves file to complete/<release>/<filename>
//
// Assertions:
//   - delivered file == original payload
//   - job state ends at "completed"

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/par2"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestM3b_E2E_RepairAndDeliver(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", mustFreePort(t)))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// 4096 bytes split into 4 slices of 1024 each. Two yEnc segments
	// of 2048 each map cleanly to two slices per segment.
	const sliceSize = 1024
	const segmentSize = 2048 // = 2 slices per segment
	const totalSize = sliceSize * 4
	original := make([]byte, totalSize)
	_, _ = rand.Read(original)
	dataMD5 := md5.Sum(original)

	cleanSegments := [][]byte{
		original[0:segmentSize],
		original[segmentSize : segmentSize*2],
	}
	// Wrong bytes for the second segment — same length, so the file
	// layout is preserved. yEnc CRC matches the WRONG bytes so the
	// orchestrator accepts the segment.
	wrongSecond := bytes.Repeat([]byte{0xAA}, segmentSize)

	dataName := "release.bin"
	dataMsg1 := "seg1@m3b.hoardarr.test"
	dataMsg2 := "seg2@m3b.hoardarr.test"
	dataEnc1 := yencEncode(dataName, cleanSegments[0], 1, 2, 1, segmentSize, totalSize)
	dataEnc2 := yencEncode(dataName, wrongSecond, 2, 2, segmentSize+1, segmentSize*2, totalSize)

	// PAR2 packets built against CLEAN payload.
	slicesClean := par2.SplitIntoSlices(original, sliceSize)
	checks := make([]par2.SliceCheck, len(slicesClean))
	for i, s := range slicesClean {
		checks[i].MD5 = md5.Sum(s)
		checks[i].CRC32 = crc32.ChecksumIEEE(s)
	}
	setID := [16]byte{0x3B, 0x3B, 0x3B, 0x3B}
	fileID := [16]byte{0xAB, 0xAB, 0xAB, 0xAB}
	par2Buf := bytes.Buffer{}
	par2Buf.Write(par2.EncodeMain(setID, sliceSize, [][16]byte{fileID}))
	par2Buf.Write(par2.EncodeFileDesc(setID, fileID, dataMD5, dataMD5, uint64(totalSize), dataName))
	par2Buf.Write(par2.EncodeIFSC(setID, fileID, checks))
	// 2 recovery slices — only need 2 since the corruption is in
	// slices 2 and 3 (the second segment covers them both).
	for _, e := range []uint16{1, 2} {
		par2Buf.Write(par2.EncodeRecvSlc(setID, e, par2.EncodeRecoverySlice(slicesClean, e)))
	}
	par2Buf.Write(par2.EncodeCreator(setID, "hoardarr-m3b-test"))
	par2Bytes := par2Buf.Bytes()
	par2Msg := "seg1@m3b.par2.hoardarr.test"
	par2Enc := yencEncode("release.par2", par2Bytes, 0, 0, 0, 0, int64(len(par2Bytes)))

	// 2-segment NZB for the data file + 1-segment NZB for the par2.
	nzbXML := buildM3bNZB("m3b-release", dataName, []string{dataMsg1, dataMsg2}, []int{len(dataEnc1), len(dataEnc2)},
		"release.par2", par2Msg, len(par2Enc))

	stub := newStubNNTP(t)
	stub.addArticle(dataMsg1, dataEnc1)
	stub.addArticle(dataMsg2, dataEnc2)
	stub.addArticle(par2Msg, par2Enc)
	defer stub.Close()

	host, portStr, _ := net.SplitHostPort(stub.Addr())
	stubPort, _ := strconv.Atoi(portStr)
	preseedServer(t, cfg, host, stubPort)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	apiKey := cfg.Auth.APIKey
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	jobID := uploadNZB(t, base, apiKey, "m3b.nzb", nzbXML)

	final := waitForJobState(t, app, jobID, "completed", 20*time.Second)
	if final == nil {
		t.Fatalf("job did not reach completed state")
	}

	// Delivered file must equal the original — the whole point of the test.
	delivered := filepath.Join(cfg.Paths.CompleteDir, "m3b-release", dataName)
	got, err := os.ReadFile(delivered)
	if err != nil {
		t.Fatalf("read delivered: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("delivered bytes differ from original (got=%d, want=%d)",
			len(got), len(original))
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s")
	}
}

// buildM3bNZB builds an NZB with one multi-segment data file plus a
// one-segment par2 file. We don't reuse buildM3aNZB because that
// helper hard-codes one segment per file.
func buildM3bNZB(release, dataName string, dataMsgs []string, dataLens []int, par2Name, par2Msg string, par2Len int) string {
	var sb bytes.Buffer
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	sb.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	sb.WriteString(`  <head><meta type="title">` + release + `</meta></head>` + "\n")

	sb.WriteString(`  <file poster="t" date="1700000000" subject='[1/2] - "` + dataName + `" yEnc'>` + "\n")
	sb.WriteString(`    <groups><group>alt.binaries.test</group></groups>` + "\n")
	sb.WriteString(`    <segments>` + "\n")
	for i, m := range dataMsgs {
		sb.WriteString(`      <segment bytes="` + strconv.Itoa(dataLens[i]) + `" number="` + strconv.Itoa(i+1) + `">` + m + `</segment>` + "\n")
	}
	sb.WriteString(`    </segments>` + "\n")
	sb.WriteString(`  </file>` + "\n")

	sb.WriteString(`  <file poster="t" date="1700000000" subject='[2/2] - "` + par2Name + `" yEnc'>` + "\n")
	sb.WriteString(`    <groups><group>alt.binaries.test</group></groups>` + "\n")
	sb.WriteString(`    <segments>` + "\n")
	sb.WriteString(`      <segment bytes="` + strconv.Itoa(par2Len) + `" number="1">` + par2Msg + `</segment>` + "\n")
	sb.WriteString(`    </segments>` + "\n")
	sb.WriteString(`  </file>` + "\n")

	sb.WriteString(`</nzb>` + "\n")
	return sb.String()
}
