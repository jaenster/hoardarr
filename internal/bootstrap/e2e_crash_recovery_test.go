package bootstrap_test

// Crash-recovery end-to-end test.
//
// The most load-bearing invariant in hoardarr's design: kill the
// orchestrator mid-download and a fresh start picks up exactly where
// it left off, with the same final bytes on disk.
//
// Strategy: use a stub NNTP whose body delivery is gated per-article.
// The test:
//
//   1. Posts an NZB with two segments.
//   2. Releases segment 1; waits for it to be persisted as done.
//   3. Calls App.Orchestrator.Stop() — segment 2 is still gated, so
//      its in-flight worker is cancelled while the body is unread.
//   4. Calls App.Orchestrator.Start(...) — restarts runners for any
//      active jobs in the DB.
//   5. Releases segment 2; waits for completion.
//   6. Asserts the assembled file matches the original payload.
//
// This exercises both halves of the recovery story: (a) cancelled-mid-
// fetch segments stay pending (don't get marked as failed), (b) the
// orchestrator service finds them on restart and re-dispatches.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/domain/download"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

func TestM2_E2E_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	httpPort := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", httpPort))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// Build payload + NZB.
	payload := make([]byte, 8192)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	const partSize = 4096
	totalSize := int64(len(payload))
	segs := []e2eSegment{
		{
			MessageID: "seg1@crash.hoardarr.test",
			Begin:     1, End: partSize,
			Encoded: yencEncode("crash.bin", payload[:partSize], 1, 2, 1, partSize, totalSize),
		},
		{
			MessageID: "seg2@crash.hoardarr.test",
			Begin:     partSize + 1, End: 2 * partSize,
			Encoded: yencEncode("crash.bin", payload[partSize:], 2, 2, partSize+1, 2*partSize, totalSize),
		},
	}
	nzbXML := buildNZB("crash-release", segs)

	// Gated stub: body delivery for each message-id waits for an
	// explicit Release() call from the test. This makes the timing
	// deterministic — no flaky sleeps.
	stub := newGatedStubNNTP(t)
	for _, s := range segs {
		stub.add(s.MessageID, s.Encoded)
	}
	defer stub.Close()

	// Pre-seed servers row pointing at stub so bootstrap builds a pool.
	host, portStr, _ := net.SplitHostPort(stub.Addr())
	stubPort, _ := strconv.Atoi(portStr)
	preseedServer(t, cfg, host, stubPort)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
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

	// Submit NZB.
	jobID := uploadNZB(t, base, apiKey, "crash.nzb", nzbXML)

	// 1. Release segment 1; wait until it's done in the DB.
	stub.release(segs[0].MessageID)
	waitSegmentState(t, app, jobID, segs[0].MessageID, "done", 5*time.Second)

	// 2. Stop the orchestrator while segment 2 is still gated. The
	//    worker holding the body reader gets cancelled; segment 2
	//    must remain pending (never flipped to failed).
	if err := app.Orchestrator.Stop(); err != nil {
		t.Fatalf("Orchestrator.Stop: %v", err)
	}
	verifySegmentNotFailed(t, app, jobID, segs[1].MessageID)

	// 3. Restart the orchestrator. It should find the active job in
	//    the DB and re-dispatch the pending segment 2.
	if err := app.Orchestrator.Start(ctx); err != nil {
		t.Fatalf("Orchestrator.Start (resume): %v", err)
	}

	// 4. Release segment 2; wait for the job to reach
	//    download_complete.
	stub.release(segs[1].MessageID)
	waitJobComplete(t, app, jobID, 5*time.Second)

	// 5. Assert assembled file.
	final, err := app.QueueService.Get(ctx, finalJobID(jobID))
	if err != nil {
		t.Fatalf("Get final: %v", err)
	}
	files := final.Files()
	if len(files) != 1 {
		t.Fatalf("file count = %d", len(files))
	}
	tmp := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(jobID, 10),
		strconv.FormatInt(int64(files[0].ID()), 10)+".tmp")
	got, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("read assembled: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("assembled bytes differ from original (got=%d want=%d)", len(got), len(payload))
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s after cancel")
	}
}

// --- gated stub NNTP -------------------------------------------------

// gatedStubNNTP is like stubNNTPServer but per-article delivery waits
// on a release channel. The test calls release(messageID) to let the
// body flow.
type gatedStubNNTP struct {
	t        *testing.T
	listener net.Listener
	mu       sync.Mutex
	articles map[string][]byte
	gates    map[string]chan struct{}
	wg       sync.WaitGroup
}

func newGatedStubNNTP(t *testing.T) *gatedStubNNTP {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &gatedStubNNTP{
		t:        t,
		listener: l,
		articles: map[string][]byte{},
		gates:    map[string]chan struct{}{},
	}
	go s.acceptLoop()
	return s
}

func (s *gatedStubNNTP) Addr() string { return s.listener.Addr().String() }

func (s *gatedStubNNTP) add(mid string, body []byte) {
	s.mu.Lock()
	s.articles[mid] = body
	s.gates[mid] = make(chan struct{})
	s.mu.Unlock()
}

// release lifts the gate for messageID, allowing any waiting handler
// to send the body.
func (s *gatedStubNNTP) release(mid string) {
	s.mu.Lock()
	gate := s.gates[mid]
	s.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (s *gatedStubNNTP) Close() {
	_ = s.listener.Close()
	s.wg.Wait()
}

func (s *gatedStubNNTP) acceptLoop() {
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

func (s *gatedStubNNTP) handle(c net.Conn) {
	defer c.Close()
	// Long deadline so the test exercises the orchestrator's
	// ctx-cancel-closes-conn path, not the stub timing out.
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	br := bufio.NewReader(c)

	if _, err := c.Write([]byte("200 hoardarr-gated ready\r\n")); err != nil {
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
			_, _ = c.Write([]byte("381 password required\r\n"))
		case strings.HasPrefix(strings.ToUpper(cmd), "AUTHINFO PASS "):
			_, _ = c.Write([]byte("281 authenticated\r\n"))
		case strings.EqualFold(cmd, "MODE READER"):
			_, _ = c.Write([]byte("200 reader\r\n"))
		case strings.EqualFold(cmd, "DATE"):
			_, _ = c.Write([]byte("111 20260510120000\r\n"))
		case strings.HasPrefix(strings.ToUpper(cmd), "BODY <") && strings.HasSuffix(cmd, ">"):
			mid := strings.TrimSuffix(strings.TrimPrefix(cmd, "BODY <"), ">")
			s.mu.Lock()
			body, ok := s.articles[mid]
			gate := s.gates[mid]
			s.mu.Unlock()
			if !ok {
				_, _ = c.Write([]byte("430 no such article\r\n"))
				continue
			}
			// Wait for the test to release the gate (or for the
			// connection to die).
			select {
			case <-gate:
			case <-readerClosedNotify(br):
				return
			}
			if _, err := c.Write([]byte(fmt.Sprintf("222 0 <%s>\r\n", mid))); err != nil {
				return
			}
			ds := dotStuff(body)
			if _, err := c.Write(ds); err != nil {
				return
			}
			if _, err := c.Write([]byte(".\r\n")); err != nil {
				return
			}
		case strings.EqualFold(cmd, "QUIT"):
			_, _ = c.Write([]byte("205 closing\r\n"))
			return
		default:
			_, _ = c.Write([]byte("500 unknown\r\n"))
		}
	}
}

// readerClosedNotify returns a channel closed when br's underlying
// reader returns EOF or error. Probes by Peek(1). Used so a gated
// handler can abort if the orchestrator closed its connection while
// we were waiting on the gate.
func readerClosedNotify(br *bufio.Reader) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		// Peek with no actual read advance, in a tight loop.
		for {
			if _, err := br.Peek(1); err != nil {
				close(done)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	return done
}

// --- helpers ---------------------------------------------------------

func waitSegmentState(t *testing.T, app *bootstrap.App, jobID int64, messageID string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := app.QueueService.Get(context.Background(), finalJobID(jobID))
		if err == nil {
			for _, f := range j.Files() {
				for _, s := range f.Segments() {
					if s.MessageID() == messageID {
						if string(s.State()) == want {
							return
						}
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("segment %s never reached state %q", messageID, want)
}

func verifySegmentNotFailed(t *testing.T, app *bootstrap.App, jobID int64, messageID string) {
	t.Helper()
	j, err := app.QueueService.Get(context.Background(), finalJobID(jobID))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for _, f := range j.Files() {
		for _, s := range f.Segments() {
			if s.MessageID() == messageID {
				st := string(s.State())
				if st == "failed" {
					t.Fatalf("segment %s flipped to failed during stop; should stay pending/inflight", messageID)
				}
				return
			}
		}
	}
	t.Fatalf("segment %s not found", messageID)
}

func waitJobComplete(t *testing.T, app *bootstrap.App, jobID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := app.QueueService.Get(context.Background(), finalJobID(jobID))
		if err == nil {
			st := string(j.State())
			if st == "download_complete" || st == "completed" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %d never reached download_complete", jobID)
}

// finalJobID is a small bridge: the e2e helpers in e2e_m2_test.go return
// an int64 jobID, but QueueService.Get expects download.JobID. Rather
// than thread the import everywhere, this helper does the cast.
func finalJobID(id int64) download.JobID { return download.JobID(id) }

var _ = sqlite.NewServerRepo // keep imports
var _ = domainserver.ServerID(0)
