# hoardarr

[![ci](https://github.com/jaenster/hoardarr/actions/workflows/ci.yml/badge.svg)](https://github.com/jaenster/hoardarr/actions/workflows/ci.yml)
[![docker](https://github.com/jaenster/hoardarr/actions/workflows/docker.yml/badge.svg)](https://github.com/jaenster/hoardarr/actions/workflows/docker.yml)
[![release](https://img.shields.io/github/v/release/jaenster/hoardarr?include_prereleases&sort=semver)](https://github.com/jaenster/hoardarr/releases)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

**hoardarr** is a Zig SABnzbd alternative with a Sonarr/Radarr-style UI.
Point Sonarr / Radarr / Lidarr / Readarr / Prowlarr at `/sabnzbd/api`
and they don't know the difference.

## What it does

- **Drop-in SABnzbd replacement.** Implements the SAB v3 API the *arr
  suite calls. Change the host in Sonarr's download-client settings
  and you're done; no other client-side config changes.
- **Sonarr/Radarr-style UI.** Dark theme, drag-reorder queue, live
  per-segment progress over SSE, per-job timeline, per-file explorer.
  No SABnzbd 2010-era page reloads.
- **Wire-level from scratch.** NNTP, yEnc, NZB parsing, PAR2 (verify +
  Reed-Solomon repair over GF(2^16)) and RAR are all written here, with
  SIMD on the hot paths. The only third-party code in the binary is
  SQLite.
- **Nothing running when nothing is happening.** One thread parked in one
  syscall; no polling interval anywhere. Measured against the previous
  release on the same machine, both idle: **0.000% CPU and 2.8 MB
  resident**, against 3.25% of a core and 43.9 MB.
- **A 2 MB container.** Runtime stage is `scratch` — no libc, no shell,
  no `ca-certificates`, no `tzdata`. The frontend is embedded and
  gzipped at build time, so the daemon never runs a compressor.
- **Multi-server with priority tiers + metered providers.** Block
  accounts kick in only after a missing-article 430 from primaries;
  byte counters persist across restarts so monthly caps are honoured.
- **Operator-friendly.** Health-check banner, scheduled tasks, on-demand
  Commands, durable scheduled backups, log-file rotation + download,
  per-path disk-space surface. Sonarr's `Settings → System` feature set
  on day one.
- **Webhooks + notifications.** Outbox-backed event bus delivers to
  Discord / Slack / Pushover / generic webhook with HMAC + retry.
- **Reverse-proxy aware.** URL-base sentinel rewrite means one binary
  works at `/`, `/hoardarr`, or any other mount path without a rebuild.
- **Verifiable releases.** Every image and tarball is signed with
  sigstore-keyless via GitHub Actions provenance (SLSA build level 3).

![Settings](docs/img/settings-servers.png)
![Job detail](docs/img/job-detail.png)
![Activity](docs/img/activity.png)
![History](docs/img/history.png)

## How does it compare to SABnzbd and NZBGet?

Honest positioning — pick the row that matches what you actually care about.

| | hoardarr | SABnzbd | NZBGet |
|-|-|-|-|
| Language | Zig (no libc on Linux) | Python | C++ |
| Container image | ~2 MB (`scratch`) | ~50 MB + Python runtime | ~5 MB |
| Memory at idle | ~3 MB | ~80 MB | ~20 MB |
| CPU at idle | 0.000% | polling loop | low |
| UI | Sonarr/Radarr-style, dark, live SSE | Original 2010s template | Bootstrap, dated |
| SAB API drop-in | Yes (`/sabnzbd/api`) | Native | Compat shim |
| Sonarr/Radarr | Drop-in | Native | Drop-in |
| PAR2 verify + repair | Native Go | par2cmdline (external) | Built-in C++ |
| RAR extraction | Built-in (stored/multi-volume) | `unrar` (external) | Built-in C++ |
| Per-segment retry budget | Yes (durable across restart) | Global queue retry | Per-job retry |
| Multi-server priority + backup | Yes | Yes | Yes |
| Metered providers (byte caps) | Yes (persisted) | No | No |
| Webhooks | Native (HMAC + outbox retry) | Notification scripts | RPC |
| Auth | Bcrypt sessions + API key + CSRF + rate-limit | Username/password | Username/password |
| Reverse-proxy URL base | Live-editable from UI | Restart required | Restart required |
| Database | SQLite (WAL, transactional outbox) | SQLite | SQLite |
| Backups | Built-in scheduled `VACUUM INTO` + UI | Manual | Manual |
| Health checks | Sonarr-style banner | None | None |
| Release signing | Sigstore-keyless (SLSA L3) | None | None |
| License | MIT | GPL-2.0 | GPL-2.0 |
| Maturity | Pre-1.0, production-tested against *arr suite | Mature, since 2007 | Mature, since 2004 |

Pick **SABnzbd** if you want the most mature option with the largest
script ecosystem and you don't mind the Python deploy footprint.
Pick **NZBGet** if you want minimal RAM/CPU above all else and the
older UI doesn't bother you.
Pick **hoardarr** if you want a modern *arr-aesthetic UI, a one-binary
container deploy with no external tools, and don't need third-party
post-processing scripts (yet — script hooks are on the roadmap).

## Supported architectures

The image is built for both `linux/amd64` and `linux/arm64`. The
appropriate manifest is selected automatically by your Docker engine.

| Architecture | Tag |
|-|-|
| amd64 | `ghcr.io/jaenster/hoardarr:latest` |
| arm64 | `ghcr.io/jaenster/hoardarr:latest` |

Version tags: `:latest` tracks the most recent release. `:0.1.2`,
`:0.1`, `:0` follow semver for pinning.

## Usage

Copy this `docker-compose.yml`, adjust `PUID`/`PGID`/`TZ` + volumes
for your host, and `docker compose up -d`:

```yaml
services:
  hoardarr:
    image: ghcr.io/jaenster/hoardarr:latest
    container_name: hoardarr
    restart: unless-stopped
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Etc/UTC
    volumes:
      - ./data:/data
      - /srv/media/incomplete:/data/incomplete
      - /srv/media/complete:/data/complete
    ports:
      - "8085:8085"
```

Open `http://localhost:8085`, create the admin account, add a Usenet
server in `Settings → Servers`, then point Sonarr / Radarr at
`http://hoardarr:8085/sabnzbd` with the API key from
`Settings → Authentication`. See
[Coming from SABnzbd](#coming-from-sabnzbd) at the bottom for the
*arr-side download-client config (with screenshots).

### docker run

```bash
docker run -d \
  --name=hoardarr \
  -e PUID=1000 -e PGID=1000 -e TZ=Etc/UTC \
  -p 8085:8085 \
  -v $(pwd)/data:/data \
  -v /srv/media/incomplete:/data/incomplete \
  -v /srv/media/complete:/data/complete \
  --restart unless-stopped \
  ghcr.io/jaenster/hoardarr:latest
```

## Parameters

### Environment variables

| Variable | Default | Notes |
|-|-|-|
| `PUID` | `1000` | UID the binary drops to. Match your host user so bind-mount files end up owned by you. |
| `PGID` | `1000` | GID. Same idea. |
| `TZ` | `Etc/UTC` | IANA zone (e.g. `Europe/Amsterdam`). Used by slog timestamps + webhook payloads. |
| `HOARDARR_LISTEN` | `:8085` | Address the HTTP server binds to. |
| `HOARDARR_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error`. |
| `HOARDARR_URL_BASE` | unset | Path prefix when behind a reverse proxy (e.g. `/hoardarr`). |

Runtime settings (bandwidth caps, max-concurrent jobs, sample-file
removal, recovery-vol deferral, etc.) live in `Settings → General` in
the UI and persist in the SQLite database. Only the bootstrap-time
knobs above are env-configurable.

### Volumes

| Path | Purpose |
|-|-|
| `/data` | Canonical state dir: `config.toml` (auto-generated), SQLite DB, sessions, logs. |
| `/data/incomplete` | In-flight job data. Survives container restarts. |
| `/data/complete` | Finished releases. Point your *arr stack to read from the same path. |

### Ports

| Port | Purpose |
|-|-|
| `8085/tcp` | Web UI + REST + SAB API (`/sabnzbd/api`) + Prometheus `/metrics`. |

## Updating

```bash
docker compose pull
docker compose up -d
```

The SQLite migrations are forward-only and run automatically on
startup. Downgrading after an upgrade is not supported; back up
`/data` before updating if you're nervous.

## Verifying the release

Every container image and binary tarball is signed with
sigstore-keyless via GitHub Actions provenance attestations (SLSA
build level 3). No PGP key to manage, no service to trust beyond
GitHub + the public Rekor transparency log.

```bash
# Container:
gh attestation verify oci://ghcr.io/jaenster/hoardarr:0.1.2 \
  --repo jaenster/hoardarr

# Source tarball:
gh attestation verify hoardarr_0.1.2_linux_amd64.tar.gz \
  --repo jaenster/hoardarr
```

A pass means the artifact was built by hoardarr's own GitHub Actions
workflow from the matching git tag.

## REST API

The full `/api/v1/*` surface (auth, queue, history, servers, system,
commands, webhooks, SSE topics) is documented at
[`docs/api.md`](docs/api.md). The SAB-compat shim at `/sabnzbd/api` is
covered there too.

## Reverse proxy

hoardarr speaks plain HTTP by design. For HTTPS, put nginx / Caddy /
Traefik in front. See [`docs/reverse-proxy.md`](docs/reverse-proxy.md)
for snippets covering hostname mounts (`hoardarr.example.com`) and
path-prefix mounts (`example.com/hoardarr`), plus the SSE-buffering
gotcha that breaks live progress under every proxy by default.

## Observability

`/metrics` is a Prometheus scrape endpoint behind the same API-key
auth as the rest of the API. See
[`docs/observability.md`](docs/observability.md) for scrape config,
the metric reference, and a starter alert ruleset.

## Backup

Stop hoardarr and copy `/data`. SQLite WAL is checkpointed
periodically and on shutdown, so the bytes on disk are consistent.

```bash
docker compose stop hoardarr
tar -czf hoardarr-backup-$(date +%F).tar.gz data/
docker compose start hoardarr
```

Finished releases in `complete/` are unaffected if you only restore
the `data/` subtree — in-progress jobs in `incomplete/` start over on
next boot.

## Coming from SABnzbd

In Sonarr / Radarr / Lidarr / Readarr / Prowlarr,
`Settings → Download Clients` → `+` → **SABnzbd**. Toggle
**`Show Advanced`** in the top toolbar *before* filling the form —
the one field most people miss is `URL Base`, and it only renders
after that toggle:

![Sonarr SABnzbd form with Show Advanced expanded; URL Base = /sabnzbd](docs/img/sonarr-sab-form-advanced.png)

For a default install, set `URL Base = /sabnzbd`. If you put hoardarr
behind a reverse proxy at `/hoardarr`, it's `/hoardarr/sabnzbd`. An
empty URL Base makes the `Test` button fail with a misleading
"Sabnzbd authentication failed" — that's almost always this.

Full walk-through (per-app categories, common test-failure diagnostics,
running side-by-side with SABnzbd, etc.):
[**`docs/coming-from-sabnzbd.md`**](docs/coming-from-sabnzbd.md).

## FAQ

**Will Sonarr / Radarr / Lidarr / Readarr / Prowlarr actually work with this?**
Yes — the SAB v3 API surface they use (`addfile`, `queue`, `history`,
`get_config`, `get_cats`, `addurl`, `eval_sort`, etc.) is implemented at
`/sabnzbd/api`. The end-to-end test suite drives the real *arr clients
against a live hoardarr in CI.

**Can I import my SABnzbd config?**
Not automatically. You'll need to re-add usenet servers and categories
in the UI (or via the REST API). NZB queue/history is provider-state,
not transferable.

**Can I run hoardarr alongside SABnzbd?**
Yes — they bind to different ports by default (`8085` vs `8080`). Useful
for evaluation: point one Sonarr instance at hoardarr, leave the other
*arrs on SAB, switch once you're confident.

**Does it support post-processing scripts?**
Not yet. SAB's hook script protocol is on the roadmap as an outbox event
subscriber. In the meantime, the webhook subscribers (Discord / Slack /
Pushover / generic HMAC POST) cover the notification half; full
"transform the release before delivery" hooks land later.

**Is auth required?**
Yes. First boot prompts for an admin password. Sessions are bcrypt +
HTTP-only cookies, rate-limited, with CSRF on mutating endpoints. The
*arr suite authenticates via API key (rotatable from Settings).

**No libc means…?**
On Linux the binary talks to the kernel directly, so it is statically
linked with no dynamic loader and cross-compiles to x86_64 and aarch64
from any host. No glibc dependency, no `apt install par2cmdline`, no
`unrar` on the host. The container's runtime stage is `scratch`: the
binary and two empty directories. `PUID`/`PGID` still work — the daemon
drops privileges itself rather than needing `su-exec` and a shell.

**Where does state live?**
Everything except the actual downloaded bytes lives in
`<data_dir>/hoardarr.db` (SQLite with WAL). Jobs, segments, servers,
categories, sessions, scheduled tasks, commands, webhook subscribers,
the outbox — one file. Back up `data/` and you've backed up hoardarr.

**Does it work behind a reverse proxy at a subpath?**
Yes — set `HOARDARR_URL_BASE=/hoardarr` (or change it live from
`Settings → General` after first boot). The URL base is a sentinel
replaced in every served asset, so one binary works at `/`, `/hoardarr`,
or any other prefix without a rebuild. See `docs/reverse-proxy.md`.

**Is the SQLite single-writer a problem at scale?**
No, for the scale hoardarr targets. WAL + a 100 ms batched-commit
drainer keeps writes far under SQLite's contention floor. The
persistence layer is adapter-pluggable so a Postgres adapter could
land later, but the bottleneck in practice is NNTP throughput, not
database I/O.

## Status

Pre-1.0. The SAB-API drop-in works in production against Sonarr /
Radarr / Lidarr / Readarr / Prowlarr. The Go-side API and DB schema
may still shift before 1.0; expect occasional breaking changes (and
backwards-compatible migrations) until then.

## Building from source

You generally don't need to; the Docker image is the recommended path.
Contributors / developers see [CONTRIBUTING.md](CONTRIBUTING.md) for
the dev loop and [docs/architecture.md](docs/architecture.md) for the
DDD + outbox + bounded-contexts layout.

## License

MIT — see [`LICENSE`](LICENSE).
