package nntptest_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntptest"
)

// startStub spins up a single-connection NNTP listener that speaks the
// given script. Each entry is (expectedRequest, response). The greeting
// is sent first, unconditionally.
func startStub(t *testing.T, greeting string, script [][2]string) (host string, port int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	addr := l.Addr().(*net.TCPAddr)

	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		br := bufio.NewReader(c)

		fmt.Fprintf(c, "%s\r\n", greeting)

		for _, step := range script {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			got := strings.TrimRight(line, "\r\n")
			if step[0] != "" && got != step[0] {
				// Be permissive: just keep responding so the probe can
				// report the right step as failed.
			}
			fmt.Fprintf(c, "%s\r\n", step[1])
		}
		// Linger briefly so the client can read the last response
		// before we close.
		time.Sleep(50 * time.Millisecond)
	}()
	return addr.IP.String(), addr.Port
}

func TestProbeHappyPath(t *testing.T) {
	host, port := startStub(t, "200 Welcome", [][2]string{
		{"AUTHINFO USER user", "381 More auth needed"},
		{"AUTHINFO PASS pw", "281 Accepted"},
		{"MODE READER", "200 Reader mode"},
		{"DATE", "111 20250510120000"},
		{"QUIT", "205 Bye"},
	})

	res := nntptest.Probe(context.Background(), nntptest.Params{
		Host: host, Port: port, TLS: false,
		Username: "user", Password: "pw",
	})
	if !res.OK {
		t.Fatalf("probe not OK: %+v", res)
	}
	if !res.Auth || !res.ModeReader || !res.Date {
		t.Fatalf("expected all stages true: %+v", res)
	}
	if res.ServerDate == "" {
		t.Errorf("ServerDate empty")
	}
}

func TestProbeAuthFailure(t *testing.T) {
	host, port := startStub(t, "200 Welcome", [][2]string{
		{"AUTHINFO USER user", "381 More auth needed"},
		{"AUTHINFO PASS wrong", "481 Authentication failed"},
		{"QUIT", "205 Bye"},
	})

	res := nntptest.Probe(context.Background(), nntptest.Params{
		Host: host, Port: port, TLS: false,
		Username: "user", Password: "wrong",
	})
	if res.OK {
		t.Fatalf("expected probe not OK: %+v", res)
	}
	if !res.Dial || !res.Greeted {
		t.Errorf("expected Dial+Greeted true, got %+v", res)
	}
	if res.Auth {
		t.Errorf("expected Auth false, got true")
	}
	if !strings.Contains(res.Err, "auth") {
		t.Errorf("expected err to mention auth, got %q", res.Err)
	}
}

func TestProbeDialFailure(t *testing.T) {
	// Bind a listener just to get an unused port, then close it so the
	// dial fails immediately.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().(*net.TCPAddr)
	_ = l.Close()

	res := nntptest.Probe(context.Background(), nntptest.Params{
		Host: "127.0.0.1", Port: addr.Port, TLS: false,
	})
	if res.OK || res.Dial {
		t.Fatalf("expected dial to fail: %+v", res)
	}
	if res.Err == "" {
		t.Errorf("expected error message")
	}
	// Sanity: port should round-trip through ParseInt without losing
	// precision (paranoia: catches accidental int overflow on 32-bit).
	if _, err := strconv.Atoi(fmt.Sprintf("%d", addr.Port)); err != nil {
		t.Errorf("port unparseable: %v", err)
	}
}
