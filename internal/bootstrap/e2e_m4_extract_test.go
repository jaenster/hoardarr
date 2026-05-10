package bootstrap_test

// M4 extract end-to-end (with stub Extractor):
//
// Drives an NZB whose data file is named release.rar through the full
// pipeline. Verify uses real synthesised PAR2 (so it actually OKs the
// download); extract is wired with a stub that pretends to unpack —
// it writes a synthetic "extracted/contents.bin" file under the
// target dir and returns. We assert:
//
//   1. extract service ran (was given the right archive paths)
//   2. extracted file lives at <complete>/<release>/extracted/contents.bin
//   3. job state ends at "completed"
//   4. incomplete dir cleaned up
//
// A real-RAR fixture e2e is a separate follow-up — committing a tiny
// sample RAR is the cleanest path and does not need to live in this
// flow-orchestration test.

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/par2"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/domain/extract"
)

// stubExtractor satisfies extract.Extractor without parsing RAR. It
// records every Extract call and writes a known marker file into
// targetDir so the test can assert the full pipeline ran.
type stubExtractor struct {
	calls atomic.Int32
	last  atomic.Pointer[stubExtractCall]
}

type stubExtractCall struct {
	archivePaths []string
	targetDir    string
}

func (s *stubExtractor) Extract(_ context.Context, archivePaths []string, targetDir string) ([]string, error) {
	s.calls.Add(1)
	s.last.Store(&stubExtractCall{archivePaths: archivePaths, targetDir: targetDir})
	dir := filepath.Join(targetDir, "extracted")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	out := filepath.Join(dir, "contents.bin")
	if err := os.WriteFile(out, []byte("stub-extracted"), 0o644); err != nil {
		return nil, err
	}
	return []string{"extracted/contents.bin"}, nil
}

// Compile-time port check.
var _ extract.Extractor = (*stubExtractor)(nil)

func TestM4_E2E_ExtractWithStub(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	httpPort := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", httpPort))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// Pretend the data file is a single-volume RAR.
	dataPayload := make([]byte, 4096)
	_, _ = rand.Read(dataPayload)
	const dataName = "release.rar"
	dataMD5 := md5.Sum(dataPayload)

	setID := [16]byte{0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42}
	fileID := [16]byte{0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77}

	par2Buf := bytes.Buffer{}
	par2Buf.Write(par2.EncodeMain(setID, uint64(len(dataPayload)), [][16]byte{fileID}))
	par2Buf.Write(par2.EncodeFileDesc(setID, fileID, dataMD5, dataMD5, uint64(len(dataPayload)), dataName))
	par2Buf.Write(par2.EncodeCreator(setID, "hoardarr-test"))
	par2Bytes := par2Buf.Bytes()

	dataEnc := yencEncode(dataName, dataPayload, 0, 0, 0, 0, int64(len(dataPayload)))
	par2Enc := yencEncode("release.par2", par2Bytes, 0, 0, 0, 0, int64(len(par2Bytes)))

	dataMsg := "seg1@m4ext.hoardarr.test"
	par2Msg := "seg1@m4ext.par2.hoardarr.test"

	nzbXML := buildM3aNZB("m4-rar-release",
		nzbEntry{filename: dataName, msgID: dataMsg, encoded: dataEnc},
		nzbEntry{filename: "release.par2", msgID: par2Msg, encoded: par2Enc},
	)

	stub := newStubNNTP(t)
	stub.addArticle(dataMsg, dataEnc)
	stub.addArticle(par2Msg, par2Enc)
	defer stub.Close()

	host, portStr, _ := net.SplitHostPort(stub.Addr())
	stubPort, _ := strconv.Atoi(portStr)
	preseedServer(t, cfg, host, stubPort)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stubX := &stubExtractor{}
	app, err := bootstrap.Build(ctx, cfg, nil, nil, bootstrap.WithExtractor(stubX))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	apiKey := cfg.Auth.APIKey
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	jobID := uploadNZB(t, base, apiKey, "m4-rar.nzb", nzbXML)

	final := waitForJobState(t, app, jobID, "completed", 15*time.Second)
	if final == nil {
		t.Fatalf("job did not reach completed state")
	}

	if stubX.calls.Load() != 1 {
		t.Errorf("stub extractor calls = %d; want 1", stubX.calls.Load())
	}
	call := stubX.last.Load()
	if call == nil || len(call.archivePaths) != 1 {
		t.Fatalf("stub call invalid: %+v", call)
	}
	// archivePaths should point at the staged real-name file inside
	// incomplete/<jobid>/release.rar (not the .tmp shadow).
	if filepath.Base(call.archivePaths[0]) != "release.rar" {
		t.Errorf("archivePaths[0] basename = %q; want release.rar",
			filepath.Base(call.archivePaths[0]))
	}

	// The stub wrote contents.bin under <complete>/m4-rar-release/extracted/.
	delivered := filepath.Join(cfg.Paths.CompleteDir, "m4-rar-release", "extracted", "contents.bin")
	if got, err := os.ReadFile(delivered); err != nil {
		t.Errorf("expected extracted file at %s: %v", delivered, err)
	} else if string(got) != "stub-extracted" {
		t.Errorf("extracted file body = %q; want stub-extracted", got)
	}

	// Incomplete dir cleaned up.
	jobDir := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(jobID, 10))
	if _, err := os.Stat(jobDir); !os.IsNotExist(err) {
		t.Errorf("expected incomplete dir %s removed; got err=%v", jobDir, err)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s")
	}
}
