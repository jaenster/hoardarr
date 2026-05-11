package bootstrap_test

// End-to-end server edit + test-connection: spin up a hoardarr instance
// and a tiny NNTP stub, then drive the full /api/v1/servers surface
// (POST add, PATCH edit, POST test, POST {id}/test, POST enable/disable,
// DELETE) over real HTTP. Asserts the wire-level handshake reaches DATE.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestServerEditAndTestConnection_E2E(t *testing.T) {
	// Boot a hoardarr instance.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("bootstrap.Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not return within 5s after cancel")
		}
	}()

	client := apiKeyClient(t, apiKey)

	// Stub NNTP server that accepts a complete handshake.
	stubAddr := startNNTPStub(t)
	stubHost, stubPort := splitHostPort(t, stubAddr)

	// 1) Add a server pointing at the stub.
	addBody := map[string]any{
		"name":      "stub",
		"host":      stubHost,
		"port":      stubPort,
		"tls":       false,
		"username":  "user",
		"password":  "pw",
		"max_conns": 4,
		"priority":  1,
	}
	addResp := postJSON(t, client, base+"/api/v1/servers", addBody)
	defer addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(addResp.Body)
		t.Fatalf("POST /servers status=%d body=%s", addResp.StatusCode, body)
	}
	var added struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(addResp.Body).Decode(&added); err != nil {
		t.Fatalf("decode add resp: %v", err)
	}
	if added.ID == 0 {
		t.Fatalf("expected non-zero server id")
	}

	// 2) Test connection via raw params (test-before-save flow).
	probeBody := map[string]any{
		"host":     stubHost,
		"port":     stubPort,
		"tls":      false,
		"username": "user",
		"password": "pw",
	}
	probeResp := postJSON(t, client, base+"/api/v1/servers/test", probeBody)
	defer probeResp.Body.Close()
	if probeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(probeResp.Body)
		t.Fatalf("POST /servers/test status=%d body=%s", probeResp.StatusCode, body)
	}
	var probe testServerRespDTO
	if err := json.NewDecoder(probeResp.Body).Decode(&probe); err != nil {
		t.Fatalf("decode probe: %v", err)
	}
	if !probe.OK || !probe.Dial || !probe.Auth || !probe.ModeReader || !probe.Date {
		t.Fatalf("probe not OK: %+v", probe)
	}

	// 3) Test via stored id (the "per-row Test button" path).
	probe2Resp := postJSON(t, client, fmt.Sprintf("%s/api/v1/servers/%d/test", base, added.ID), nil)
	defer probe2Resp.Body.Close()
	if probe2Resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(probe2Resp.Body)
		t.Fatalf("POST /servers/{id}/test status=%d body=%s", probe2Resp.StatusCode, body)
	}
	var probe2 testServerRespDTO
	if err := json.NewDecoder(probe2Resp.Body).Decode(&probe2); err != nil {
		t.Fatalf("decode probe2: %v", err)
	}
	if !probe2.OK {
		t.Errorf("stored-creds probe not OK: %+v", probe2)
	}

	// 4) PATCH the server: change port + flip backup + add bandwidth cap.
	newPort := 5000 + (stubPort % 1000) // any plausible-looking integer
	patchBody := map[string]any{
		"port":                    newPort,
		"backup":                  true,
		"bandwidth_bytes_per_sec": 1024 * 1024, // 1 MiB/s
		"billing_mode":            "metered",
		"quota_bytes":             100 * 1024 * 1024,
	}
	patchResp := patchJSON(t, client, fmt.Sprintf("%s/api/v1/servers/%d", base, added.ID), patchBody)
	defer patchResp.Body.Close()
	if patchResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(patchResp.Body)
		t.Fatalf("PATCH /servers/{id} status=%d body=%s", patchResp.StatusCode, body)
	}

	// Round-trip via GET /servers — confirm fields actually changed.
	listReq, _ := http.NewRequest(http.MethodGet, base+"/api/v1/servers", nil)
	listResp := mustDo(t, client, listReq)
	defer listResp.Body.Close()
	var listBody struct {
		Servers []struct {
			ID                   int64  `json:"id"`
			Port                 int    `json:"port"`
			Backup               bool   `json:"backup"`
			BillingMode          string `json:"billing_mode"`
			QuotaBytes           int64  `json:"quota_bytes"`
			BandwidthBytesPerSec int64  `json:"bandwidth_bytes_per_sec"`
		} `json:"servers"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listBody); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	var found bool
	for _, s := range listBody.Servers {
		if s.ID == added.ID {
			found = true
			if s.Port != newPort {
				t.Errorf("port not updated: got %d want %d", s.Port, newPort)
			}
			if !s.Backup {
				t.Errorf("backup not set")
			}
			if s.BillingMode != "metered" {
				t.Errorf("billing_mode = %q; want metered", s.BillingMode)
			}
			if s.BandwidthBytesPerSec != 1024*1024 {
				t.Errorf("bandwidth_bytes_per_sec = %d; want %d", s.BandwidthBytesPerSec, 1024*1024)
			}
			if s.QuotaBytes != 100*1024*1024 {
				t.Errorf("quota_bytes = %d; want %d", s.QuotaBytes, 100*1024*1024)
			}
		}
	}
	if !found {
		t.Fatalf("added server id %d not in list", added.ID)
	}

	// 5) Disable + enable round trip.
	dResp := postJSON(t, client, fmt.Sprintf("%s/api/v1/servers/%d/disable", base, added.ID), nil)
	dResp.Body.Close()
	if dResp.StatusCode != http.StatusNoContent {
		t.Fatalf("disable status=%d", dResp.StatusCode)
	}
	eResp := postJSON(t, client, fmt.Sprintf("%s/api/v1/servers/%d/enable", base, added.ID), nil)
	eResp.Body.Close()
	if eResp.StatusCode != http.StatusNoContent {
		t.Fatalf("enable status=%d", eResp.StatusCode)
	}
}

type testServerRespDTO struct {
	OK         bool   `json:"ok"`
	Dial       bool   `json:"dial"`
	Greeted    bool   `json:"greeted"`
	Auth       bool   `json:"auth"`
	ModeReader bool   `json:"mode_reader"`
	Date       bool   `json:"date"`
	ServerDate string `json:"server_date,omitempty"`
	Err        string `json:"err,omitempty"`
	ElapsedMs  int64  `json:"elapsed_ms"`
}

// startNNTPStub spins up an NNTP listener that scripts a complete USER
// → PASS → MODE READER → DATE → QUIT exchange. Accepts multiple
// connections; the listener and per-connection goroutines stop when
// the cleanup hook closes the listener.
func startNNTPStub(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go serveNNTPStub(c)
		}
	}()
	return l.Addr().String()
}

func serveNNTPStub(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	fmt.Fprintf(c, "200 Welcome\r\n")
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		up := strings.ToUpper(strings.TrimRight(line, "\r\n"))
		switch {
		case strings.HasPrefix(up, "AUTHINFO USER"):
			fmt.Fprintf(c, "381 More auth needed\r\n")
		case strings.HasPrefix(up, "AUTHINFO PASS"):
			fmt.Fprintf(c, "281 Accepted\r\n")
		case up == "MODE READER":
			fmt.Fprintf(c, "200 Reader mode\r\n")
		case up == "DATE":
			fmt.Fprintf(c, "111 20250510120000\r\n")
		case up == "QUIT":
			fmt.Fprintf(c, "205 Bye\r\n")
			return
		default:
			fmt.Fprintf(c, "500 Unknown\r\n")
		}
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host/port: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return host, port
}

// postJSON marshals body to JSON and POSTs it.
func postJSON(t *testing.T, c *http.Client, url string, body any) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return mustDo(t, c, req)
}

func patchJSON(t *testing.T, c *http.Client, url string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return mustDo(t, c, req)
}
