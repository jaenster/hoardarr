package bootstrap_test

// M3a end-to-end: download → verify → VerifyOK.
//
// Builds a synthetic data file + matching PAR2 metadata + an NZB
// pointing at both, served by the existing in-process NNTP stub.
// The orchestrator downloads, the verify worker fires on the
// JobDownloadComplete event, runs PAR2 verification, and emits
// VerifyOK. The test asserts that event was delivered.

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/par2"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

func TestM3a_E2E_VerifyOK(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	httpPort := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", httpPort))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// Random data file payload + matching PAR2 metadata.
	dataPayload := make([]byte, 4096)
	_, _ = rand.Read(dataPayload)
	const dataName = "release.bin"
	dataMD5 := md5.Sum(dataPayload)

	setID := [16]byte{0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42}
	fileID := [16]byte{0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77}

	par2Buf := bytes.Buffer{}
	par2Buf.Write(par2.EncodeMain(setID, uint64(len(dataPayload)), [][16]byte{fileID}))
	par2Buf.Write(par2.EncodeFileDesc(setID, fileID, dataMD5, dataMD5, uint64(len(dataPayload)), dataName))
	par2Buf.Write(par2.EncodeCreator(setID, "hoardarr-test"))
	par2Bytes := par2Buf.Bytes()

	// Encode each file as ONE yEnc article (single-part).
	dataEnc := yencEncode(dataName, dataPayload, 0, 0, 0, 0, int64(len(dataPayload)))
	par2Enc := yencEncode("release.par2", par2Bytes, 0, 0, 0, 0, int64(len(par2Bytes)))

	dataMsg := "seg1@m3a.hoardarr.test"
	par2Msg := "seg1@m3a.par2.hoardarr.test" // distinct file → seg index reset

	// Hand-build NZB so each file has exactly one segment.
	nzbXML := buildM3aNZB("m3a-release",
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	// Subscribe to verify events on the bus before kicking the run
	// so we don't miss anything. Multiple distinct subscription names
	// are required (memory-bus contract: unique per Bus).
	var (
		verifyStarted atomic.Int32
		verifyOK      atomic.Int32
		verifyFailed  atomic.Int32
	)
	subs := []struct {
		name, topic string
		fn          event.Handler
	}{
		{"e2e-m3a-started", "verify.started", func(_ context.Context, _ event.Envelope) error {
			verifyStarted.Add(1)
			return nil
		}},
		{"e2e-m3a-ok", "verify.ok", func(_ context.Context, _ event.Envelope) error {
			verifyOK.Add(1)
			return nil
		}},
		{"e2e-m3a-failed", "verify.failed", func(_ context.Context, _ event.Envelope) error {
			verifyFailed.Add(1)
			return nil
		}},
	}
	for _, s := range subs {
		if _, err := app.Bus.Subscribe(s.name, s.topic, s.fn); err != nil {
			t.Fatalf("subscribe %s: %v", s.topic, err)
		}
	}

	apiKey := cfg.Auth.APIKey
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	jobID := uploadNZB(t, base, apiKey, "m3a.nzb", nzbXML)

	// Wait for ALL relevant events to settle. Each topic has its own
	// dispatcher goroutine, so VerifyOK can land before our
	// VerifyStarted handler has been called for the same job.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if verifyStarted.Load() > 0 && verifyOK.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if verifyStarted.Load() < 1 {
		t.Errorf("VerifyStarted events = %d; want ≥1", verifyStarted.Load())
	}
	if verifyOK.Load() < 1 {
		t.Errorf("VerifyOK events = %d; want ≥1 (download was clean; PAR2 should match)", verifyOK.Load())
	}
	if verifyFailed.Load() != 0 {
		t.Errorf("VerifyFailed events = %d; want 0", verifyFailed.Load())
	}

	// Final state should be download_complete.
	final, err := app.QueueService.Get(ctx, finalJobID(jobID))
	if err != nil {
		t.Fatalf("Get final: %v", err)
	}
	if string(final.State()) != "download_complete" {
		t.Errorf("job state = %s; want download_complete", final.State())
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s")
	}
}

// nzbEntry is a per-file shape for the M3a fixture builder.
type nzbEntry struct {
	filename string
	msgID    string
	encoded  []byte
}

// buildM3aNZB builds an NZB with one file per nzbEntry, each file
// containing one segment. Differs from buildNZB in that it supports
// multiple files (each with its own subject/filename).
func buildM3aNZB(name string, entries ...nzbEntry) string {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	sb.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	sb.WriteString(`  <head><meta type="title">` + name + `</meta></head>` + "\n")
	for _, e := range entries {
		sb.WriteString(`  <file poster="t" date="1700000000" subject='[1/1] - "` + e.filename + `" yEnc'>` + "\n")
		sb.WriteString(`    <groups><group>alt.binaries.test</group></groups>` + "\n")
		sb.WriteString(`    <segments>` + "\n")
		sb.WriteString(`      <segment bytes="` + strconv.Itoa(len(e.encoded)) + `" number="1">` + e.msgID + `</segment>` + "\n")
		sb.WriteString(`    </segments>` + "\n")
		sb.WriteString(`  </file>` + "\n")
	}
	sb.WriteString(`</nzb>` + "\n")
	return sb.String()
}
