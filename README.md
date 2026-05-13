# hoardarr

Go-based SABnzbd alternative with a Sonarr/Radarr-style UI. Drop-in
replacement for the SAB API that Sonarr / Radarr / Lidarr / Readarr /
Prowlarr expect — point them at hoardarr's `/sabnzbd/api` and they
don't know the difference.

Single binary. Pure-Go SQLite (no cgo). Frontend is embedded via
`go:embed`, so deployment is "scp the binary, give it a writable data
dir, run it." Designed for the homelab / Synology / Unraid use case
where SABnzbd's 2010-era UI sits next to your modern *arr stack.

## What it does

- NNTP fetch + yEnc decode + NZB parse — written from scratch.
- PAR2 verify + Reed-Solomon repair over GF(2^16) — no shell-out to
  `par2cmdline`, no external math library.
- Multi-server pool with per-server caps, metered/quota tracking, and
  hot-wire registration (add a server in the UI, the orchestrator
  starts using it without a restart).
- RAR extraction (multi-part, RAR3 + RAR5) — via `nwaples/rardecode`,
  the one place we still depend on third-party code.
- SAB API shim at `/sabnzbd/api` for *arr-suite compatibility.
- Webhook + Discord + Slack notifications, HMAC-signed.
- Settings UI for servers, categories, paths, bandwidth, auth.
- Live activity, history, per-job timeline + segment-level file
  explorer.

## Build & run

```bash
make build            # frontend bundle + go build -tags embed
./hoardarr serve      # default: listens on :8085, data dir ./data
```

Or with Docker:

```bash
docker build -t hoardarr:latest .
docker run -p 8085:8085 -v /path/to/data:/data hoardarr:latest
```

First-run flow: open `http://localhost:8085`, create an admin account,
add a Usenet server, drop an NZB. Point Sonarr/Radarr at
`http://hoardarr:8085/sabnzbd` with the API key from `Settings →
Authentication`.

## Configuration

Bootstrap-time config lives in `config.toml` (or `HOARDARR_*` env vars)
— things like the listen address, data dir, SQLite path, and log
level that must be known before the database is open. Everything else
(URL base, max concurrent jobs, bandwidth caps, recovery-vol
deferral, etc.) is runtime-mutable from `Settings → General` and
persists in SQLite.

Env vars override the file:

| Variable                  | Default              |
|-|-|
| `HOARDARR_LISTEN`         | `:8085`              |
| `HOARDARR_DATA_DIR`       | `./data`             |
| `HOARDARR_SQLITE_PATH`    | `<data>/hoardarr.db` |
| `HOARDARR_INCOMPLETE_DIR` | `<data>/incomplete`  |
| `HOARDARR_COMPLETE_DIR`   | `<data>/complete`    |
| `HOARDARR_LOG_LEVEL`      | `info`               |
| `HOARDARR_API_KEY`        | auto-generated       |

## Architecture

DDD bounded contexts coupled only by domain events on a transactional
outbox bus. Top-level layout:

```
internal/
  domain/           # aggregates + ports, no framework deps
  app/              # use cases — wire ports to domain logic
  adapter/          # nntp / yenc / nzb / par2 / sqlite / rar / fs
  api/{rest,sab,sse}
  bootstrap/        # composition root
frontend/           # React + TS + Vite, embedded via go:embed
cmd/                # hoardarr (main) + testserver-nntpd (e2e)
```

## Tests

```bash
make test           # go test ./...
cd frontend && npx playwright test   # browser e2e (boots real binary)
```

The Go e2e suite covers the full pipeline against an in-process
NNTP stub (`internal/testserver/nntp`). Playwright specs drive the
UI against a real hoardarr binary + the testserver, so the SAB shim,
SSE updates, drag-reorder, and per-job detail flows are exercised
under a real browser.

## License

MIT — see [`LICENSE`](LICENSE).

## Status

Pre-1.0. The drop-in SAB compatibility works in production (Sonarr,
Radarr, Lidarr, Readarr, Prowlarr have all been observed running
against it). API and DB schema may still shift; expect breaking
changes before 1.0 is tagged.
