package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// cmdHealthcheck is the binary's self-probe used by the Dockerfile
// HEALTHCHECK directive. Distroless has no shell or curl, so the
// container's only viable way to probe its own readiness is to invoke
// the binary again with this subcommand.
//
// Reads HOARDARR_LISTEN (set by the Dockerfile ENV) to find the local
// HTTP port, GETs /healthz, exits 0 on 2xx and 1 on anything else.
// No JSON parsing — the response body doesn't matter, only the status.
func cmdHealthcheck(_ []string) error {
	listen := os.Getenv("HOARDARR_LISTEN")
	if listen == "" {
		listen = ":8085"
	}
	// listen looks like ":8085" or "0.0.0.0:8085". Always probe via
	// 127.0.0.1 — the healthcheck runs inside the same container.
	port := listen
	if i := strings.LastIndex(listen, ":"); i >= 0 {
		port = listen[i:]
	}
	url := "http://127.0.0.1" + port + "/healthz"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	return nil
}
