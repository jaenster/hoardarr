package bootstrap_test

// M1 end-to-end test.
//
// Drives the full pipeline against an in-process NNTP stub:
//
//	test crafts payload + yEnc-encoded segments + NZB
//	         │
//	         ▼
//	stub NNTP server holds {message-id → encoded body} map
//	         │
//	         ▼
//	bootstrap.Build wires App (DB, outbox bus, server registry, ...)
//	         │
//	         ▼
//	add a server (pointing at the stub), add the NZB, run orchestrator
//	         │
//	         ▼
//	assert: incomplete/<jobid>/<fileid>.tmp matches original payload,
//	        outbox holds JobCreated + SegmentCompleted + JobDownloadComplete
//
// No mocks above the NNTP wire boundary — real SQLite, real outbox,
// real yEnc decoder, real assemble-on-disk logic.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

func TestM1_E2E_FullPipeline(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:0")
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// 1. Build payload + encode + craft NZB.
	payload := make([]byte, 8192)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	const partSize = 4096
	totalSize := int64(len(payload))
	segs := []e2eSegment{
		{
			MessageID: "seg1@hoardarr.test",
			Begin:     1, End: partSize,
			Encoded: yencEncode("file.bin", payload[:partSize], 1, 2, 1, partSize, totalSize),
		},
		{
			MessageID: "seg2@hoardarr.test",
			Begin:     partSize + 1, End: 2 * partSize,
			Encoded: yencEncode("file.bin", payload[partSize:], 2, 2, partSize+1, 2*partSize, totalSize),
		},
	}
	nzbXML := buildNZB("test-release", segs)

	// 2. Start stub NNTP listener.
	stub := newStubNNTP(t)
	for _, s := range segs {
		stub.addArticle(s.MessageID, s.Encoded)
	}
	stubAddr := stub.Addr()
	defer stub.Close()

	// 3. Build App.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	// Subscribe to download events so we can assert delivery.
	var (
		mu              sync.Mutex
		gotJobCreated   int
		gotSegCompleted int
		gotJobDLComplete int
	)
	subBus := app.Bus
	if _, err := subBus.Subscribe("e2e-job-created", "download.job.created", func(_ context.Context, _ event.Envelope) error {
		mu.Lock()
		gotJobCreated++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("subscribe job.created: %v", err)
	}
	if _, err := subBus.Subscribe("e2e-seg-completed", "download.segment.completed", func(_ context.Context, _ event.Envelope) error {
		mu.Lock()
		gotSegCompleted++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("subscribe segment.completed: %v", err)
	}
	if _, err := subBus.Subscribe("e2e-job-dlc", "download.job.download_complete", func(_ context.Context, _ event.Envelope) error {
		mu.Lock()
		gotJobDLComplete++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("subscribe job.download_complete: %v", err)
	}

	// 4. Add server pointing at the stub.
	host, portStr, _ := net.SplitHostPort(stubAddr)
	var port int
	fmt.Sscan(portStr, &port)
	tlsOff := false
	srvID, err := app.ServerService.Add(ctx, appserver.AddCmd{
		Name: "stub", Host: host, Port: port,
		TLS: &tlsOff,
		Username: "u", Password: "p",
		MaxConns: 2, Priority: 0,
	})
	if err != nil {
		t.Fatalf("server add: %v", err)
	}

	// 5. AddJob.
	jobID, err := app.AddJobService.AddJob(ctx, appdownload.AddJobCmd{
		NZB:      bytes.NewReader([]byte(nzbXML)),
		Category: "test",
	})
	if err != nil {
		t.Fatalf("AddJob: %v", err)
	}

	// 6. Run orchestrator.
	srv, err := app.ServerService.Get(ctx, srvID)
	if err != nil {
		t.Fatalf("Get server: %v", err)
	}
	pool := nntp.NewPool(srv, nntp.PoolOptions{})
	defer pool.Close()
	fetcher := appdownload.NewPoolFetcher(pool)
	orch := appdownload.NewOrchestrator(
		app.JobRepo, fetcher, app.Bus, app.TxMgr,
		srv.ID(), srv.MaxConns(), cfg.Paths.IncompleteDir,
		appdownload.OrchestratorOptions{},
	)
	if err := orch.Run(ctx, jobID); err != nil {
		t.Fatalf("orchestrator.Run: %v", err)
	}

	// 7. Wait briefly for outbox dispatchers to deliver.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := gotJobCreated >= 1 && gotSegCompleted >= 2 && gotJobDLComplete >= 1
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	if gotJobCreated < 1 {
		t.Errorf("JobCreated events = %d; want >= 1", gotJobCreated)
	}
	if gotSegCompleted < 2 {
		t.Errorf("SegmentCompleted events = %d; want >= 2", gotSegCompleted)
	}
	if gotJobDLComplete < 1 {
		t.Errorf("JobDownloadComplete events = %d; want >= 1", gotJobDLComplete)
	}
	mu.Unlock()

	// 8. Verify the assembled file matches the original payload.
	final, err := app.JobRepo.ByID(ctx, jobID)
	if err != nil {
		t.Fatalf("load final job: %v", err)
	}
	if final.State().IsActive() {
		t.Errorf("final state = %s; want non-active", final.State())
	}
	files := final.Files()
	if len(files) != 1 {
		t.Fatalf("files = %d; want 1", len(files))
	}
	tmpPath := filepath.Join(cfg.Paths.IncompleteDir, fmt.Sprintf("%d", int64(jobID)), fmt.Sprintf("%d.tmp", int64(files[0].ID())))
	got, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatalf("read assembled file %s: %v", tmpPath, err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("assembled bytes differ from original (len=%d/%d)", len(got), len(payload))
	}
}

// SubscribableBus is exported on App so e2e tests can wire subscribers
// directly. Defined in a small helper file alongside this test.

// ---------------------------------------------------------------------
// Test helpers below: NZB builder, yEnc encoder, stub NNTP server.
// ---------------------------------------------------------------------

type e2eSegment struct {
	MessageID string
	Begin     int64
	End       int64
	Encoded   []byte // full article body including =ybegin/=ypart/=yend
}

// buildNZB renders a minimal valid NZB document referencing one file
// composed of the given segments.
func buildNZB(name string, segs []e2eSegment) string {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	sb.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	sb.WriteString(`  <head><meta type="title">` + name + `</meta></head>` + "\n")
	sb.WriteString(`  <file poster="t" date="1700000000" subject='[1/1] - "file.bin" yEnc'>` + "\n")
	sb.WriteString(`    <groups><group>alt.binaries.test</group></groups>` + "\n")
	sb.WriteString(`    <segments>` + "\n")
	for _, s := range segs {
		fmt.Fprintf(&sb, `      <segment bytes="%d" number="%d">%s</segment>`+"\n",
			len(s.Encoded), seqIndexFromMessageID(s.MessageID), s.MessageID)
	}
	sb.WriteString(`    </segments>` + "\n")
	sb.WriteString(`  </file>` + "\n")
	sb.WriteString(`</nzb>` + "\n")
	return sb.String()
}

func seqIndexFromMessageID(mid string) int {
	// "segN@..." → N
	if len(mid) < 4 || !strings.HasPrefix(mid, "seg") {
		return 1
	}
	var n int
	if _, err := fmt.Sscanf(mid, "seg%d@", &n); err != nil {
		return 1
	}
	return n
}

// yencEncode produces a yEnc-encoded article body. Adapted from the
// adapter/yenc test helper. We re-implement here to keep this test
// independent of the decoder under test (catch encoding bugs on either
// side).
//
// totalFileSize is the size of the WHOLE assembled file (yEnc spec:
// =ybegin size= is the total file size, not the segment size). The
// =yend size= field IS the segment size.
func yencEncode(name string, payload []byte, part, total int, begin, end int64, totalFileSize int64) []byte {
	var buf bytes.Buffer
	if total > 0 {
		fmt.Fprintf(&buf, "=ybegin part=%d total=%d line=128 size=%d name=%s\r\n", part, total, totalFileSize, name)
		fmt.Fprintf(&buf, "=ypart begin=%d end=%d\r\n", begin, end)
	} else {
		fmt.Fprintf(&buf, "=ybegin line=128 size=%d name=%s\r\n", len(payload), name)
	}
	col := 0
	for _, b := range payload {
		out := byte(b + 42)
		critical := out == 0x00 || out == 0x0A || out == 0x0D || out == '='
		if critical || (col == 0 && (out == '\t' || out == ' ' || out == '.')) {
			buf.WriteByte('=')
			buf.WriteByte(out + 64)
			col += 2
		} else {
			buf.WriteByte(out)
			col++
		}
		if col >= 128 {
			buf.WriteString("\r\n")
			col = 0
		}
	}
	if col > 0 {
		buf.WriteString("\r\n")
	}
	crc := crc32.ChecksumIEEE(payload)
	if total > 0 {
		fmt.Fprintf(&buf, "=yend size=%d part=%d pcrc32=%08x\r\n", end-begin+1, part, crc)
	} else {
		fmt.Fprintf(&buf, "=yend size=%d crc32=%08x\r\n", len(payload), crc)
	}
	return buf.Bytes()
}

// --- stub NNTP server ---

type stubNNTPServer struct {
	t        *testing.T
	listener net.Listener
	mu       sync.Mutex
	articles map[string][]byte
	wg       sync.WaitGroup
	done     chan struct{}
}

func newStubNNTP(t *testing.T) *stubNNTPServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &stubNNTPServer{
		t:        t,
		listener: l,
		articles: map[string][]byte{},
		done:     make(chan struct{}),
	}
	go s.acceptLoop()
	return s
}

func (s *stubNNTPServer) Addr() string { return s.listener.Addr().String() }

func (s *stubNNTPServer) addArticle(mid string, body []byte) {
	s.mu.Lock()
	s.articles[mid] = body
	s.mu.Unlock()
}

func (s *stubNNTPServer) Close() {
	close(s.done)
	_ = s.listener.Close()
	s.wg.Wait()
}

func (s *stubNNTPServer) acceptLoop() {
	for {
		c, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(c)
		}()
	}
}

func (s *stubNNTPServer) handle(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(c)

	// Greeting.
	if _, err := c.Write([]byte("200 hoardarr-stub ready\r\n")); err != nil {
		return
	}

	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(strings.ToUpper(cmd), "AUTHINFO USER "):
			c.Write([]byte("381 password required\r\n"))
		case strings.HasPrefix(strings.ToUpper(cmd), "AUTHINFO PASS "):
			c.Write([]byte("281 authenticated\r\n"))
		case strings.EqualFold(cmd, "MODE READER"):
			c.Write([]byte("200 reader\r\n"))
		case strings.EqualFold(cmd, "DATE"):
			c.Write([]byte("111 20260510120000\r\n"))
		case strings.HasPrefix(strings.ToUpper(cmd), "BODY <") && strings.HasSuffix(cmd, ">"):
			mid := strings.TrimSuffix(strings.TrimPrefix(cmd, "BODY <"), ">")
			s.mu.Lock()
			body, ok := s.articles[mid]
			s.mu.Unlock()
			if !ok {
				c.Write([]byte("430 no such article\r\n"))
				continue
			}
			c.Write([]byte(fmt.Sprintf("222 0 <%s>\r\n", mid)))
			// Apply dot-stuffing.
			ds := dotStuff(body)
			c.Write(ds)
			c.Write([]byte(".\r\n"))
		case strings.EqualFold(cmd, "QUIT"):
			c.Write([]byte("205 closing\r\n"))
			return
		default:
			c.Write([]byte("500 unknown\r\n"))
		}
	}
}

func dotStuff(body []byte) []byte {
	var out bytes.Buffer
	r := bufio.NewReader(bytes.NewReader(body))
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if len(line) > 0 && line[0] == '.' {
				out.WriteByte('.')
			}
			out.Write(line)
		}
		if err != nil {
			break
		}
	}
	return out.Bytes()
}

// silence unused import check during partial WIP.
var _ = json.Marshal
var _ = io.Discard
