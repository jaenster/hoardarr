// Command testserver-nntpd runs the fake NNTP server (from
// internal/testserver/nntp) plus an HTTP control plane on a sibling
// port. Playwright + manual UI poking both drive it via the HTTP API.
//
// HTTP control plane:
//   POST /seed-nzb     — synthesize articles + return an NZB body
//   POST /options      — update bps / latency / missing knobs at runtime
//   POST /reset        — clear all registered articles
//   GET  /addr         — return the NNTP listener address
//
// All requests/responses are JSON unless otherwise noted. The NZB
// endpoint returns application/xml.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	testnntp "github.com/jaenster/hoardarr/internal/testserver/nntp"
)

func main() {
	var (
		nntpAddr  = flag.String("nntp", "127.0.0.1:0", "address the fake NNTP server listens on")
		httpAddr  = flag.String("http", "127.0.0.1:0", "address the control-plane HTTP server listens on")
		user      = flag.String("user", "", "AUTHINFO USER; empty = no auth")
		pass      = flag.String("pass", "", "AUTHINFO PASS")
		bps       = flag.Int64("bps", 0, "bytes-per-second cap on BODY/ARTICLE responses; 0 = no cap")
		latencyMs = flag.Int("latency-ms", 0, "fixed per-article latency in milliseconds")
		missing   = flag.Float64("missing-fraction", 0, "fraction [0,1) of articles to return 430 for")
		addrFile  = flag.String("addr-file", "", "if non-empty, write JSON {nntp, http} addresses here on startup")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	srv, err := testnntp.Start(testnntp.Options{
		Listen:          *nntpAddr,
		Username:        *user,
		Password:        *pass,
		BytesPerSec:     *bps,
		ArticleLatency:  time.Duration(*latencyMs) * time.Millisecond,
		MissingFraction: *missing,
		Logger:          logger,
	})
	if err != nil {
		log.Fatalf("testserver-nntpd: %v", err)
	}
	defer srv.Stop()

	logger.Info("testserver-nntpd started", "nntp", srv.Addr())

	ctl := &controlPlane{srv: srv, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/seed-nzb", ctl.seedNZB)
	mux.HandleFunc("/options", ctl.options)
	mux.HandleFunc("/reset", ctl.reset)
	mux.HandleFunc("/addr", ctl.addr)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	httpL, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		log.Fatalf("testserver-nntpd http listen: %v", err)
	}
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpServer.Serve(httpL); err != nil && err != http.ErrServerClosed {
			log.Printf("http serve: %v", err)
		}
	}()
	logger.Info("control plane listening", "http", httpL.Addr().String())

	if *addrFile != "" {
		body, _ := json.MarshalIndent(map[string]string{
			"nntp": srv.Addr(),
			"http": httpL.Addr().String(),
		}, "", "  ")
		if err := os.WriteFile(*addrFile, body, 0o644); err != nil {
			log.Fatalf("write addr file: %v", err)
		}
	}

	// Block until signalled.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("shutting down")
	_ = httpServer.Close()
	_ = httpL.Close()
}

// --- control plane --------------------------------------------------

type controlPlane struct {
	srv    *testnntp.Server
	logger *slog.Logger

	mu sync.Mutex // serialises /options + /reset against each other
}

type seedNZBReq struct {
	// JobName is informational — used as the default for any
	// FileName that's blank.
	JobName string `json:"job_name"`
	Files   []struct {
		Filename string `json:"filename"`
		Segments []struct {
			MessageID string `json:"msg_id"`
			SizeBytes int    `json:"size_bytes"`
		} `json:"segments"`
	} `json:"files"`
}

// seedNZB synthesizes payloads for every segment, registers them with
// the fake server, and returns the NZB XML body. The caller posts the
// returned bytes to hoardarr's /api/v1/queue/nzb.
func (c *controlPlane) seedNZB(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req seedNZBReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Files) == 0 {
		http.Error(w, "no files", http.StatusBadRequest)
		return
	}
	files := make([]testnntp.FileSpec, 0, len(req.Files))
	for _, f := range req.Files {
		fn := f.Filename
		if fn == "" {
			fn = req.JobName
		}
		segs := make([]testnntp.SegmentSpec, 0, len(f.Segments))
		for _, s := range f.Segments {
			if s.SizeBytes <= 0 {
				http.Error(w, "size_bytes required", http.StatusBadRequest)
				return
			}
			_, spec := c.srv.SynthesizeAndRegister(s.MessageID, fn, s.SizeBytes)
			segs = append(segs, spec)
		}
		files = append(files, testnntp.FileSpec{
			Filename: fn,
			Segments: segs,
		})
	}
	body := testnntp.BuildNZB(files)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(body)
}

type optionsReq struct {
	BytesPerSec     *int64   `json:"bytes_per_sec,omitempty"`
	LatencyMs       *int     `json:"latency_ms,omitempty"`
	MissingFraction *float64 `json:"missing_fraction,omitempty"`
}

func (c *controlPlane) options(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req optionsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.BytesPerSec != nil {
		c.srv.SetBytesPerSec(*req.BytesPerSec)
	}
	if req.LatencyMs != nil {
		c.srv.SetArticleLatency(time.Duration(*req.LatencyMs) * time.Millisecond)
	}
	if req.MissingFraction != nil {
		c.srv.SetMissingFraction(*req.MissingFraction)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *controlPlane) reset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.srv.Reset()
	w.WriteHeader(http.StatusNoContent)
}

func (c *controlPlane) addr(w http.ResponseWriter, _ *http.Request) {
	host, port := c.srv.Host(), c.srv.Port()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"host": host,
		"port": port,
	})
}
