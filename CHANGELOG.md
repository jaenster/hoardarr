# Changelog

All notable changes to hoardarr are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Full-width download-speed chart on the System page, with the
  configured global cap drawn as a dashed reference line, window-
  peak marker, and an all-time-peak hint line. Range picker switches
  the view between 5 min / 1 h / 6 h / 24 h / 7 d.
- Persistent throughput history (`speed_history` table, one row per
  minute, 30-day retention) backs the longer ranges; the in-memory
  ring widens to one hour and serves the sub-hour ranges at 1-second
  resolution. New endpoint:
  `GET /api/v1/system/speed-history?range=5m|1h|6h|24h|7d`.
- All-time observed throughput peak persists across restarts (new
  `system.throughput_all_time_peak_bps` setting). `GET /api/v1/system/
  throughput` now also returns `peak_window_bytes_per_sec`,
  `peak_alltime_bytes_per_sec`, and `global_cap_bytes_per_sec`.
- Quick throttle slider on the System page: set the global cap
  without a trip to `Settings → Bandwidth`.

## [0.2.0] — operator panels + segment retry persistence + SAB compat polish

### Added

- SAB API parity: `mode=addurl`, `mode=eval_sort`, `mode=get_files`,
  `mode=history&name=delete`, `mode=history&name=mark_as_completed`.
  Closes the silently-broken Sonarr "Send NZB" button and the path-
  template preview *arr clients call before importing.
- Deliver-side post-processing borrowed from SABnzbd:
  - Deobfuscation fallback rename for releases that arrive without
    PAR2 and have an obfuscated largest file (32-hex names etc.).
    Multiple safety guards keep this from firing on hand-named
    releases.
  - Sample/proof file removal after a successful delivery, with a
    100%-match safeguard so genuine `Sample.Pack` releases survive.
    Opt-out via `Settings → General`.
  - Single-folder collapse: flattens a redundant inner directory
    around the release. Opt-out via `Settings → General`.
- Two new live-editable settings (`delete_samples`,
  `collapse_single_folder`), persisted in SQLite and defaulting ON to
  match SABnzbd's out-of-the-box behaviour.

### Documentation

- Public-readiness scaffolding: `README.md`, `LICENSE`,
  `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`,
  `CHANGELOG.md`, GitHub issue templates, and the CI/Docker workflows
  under `.github/workflows/`.

## [0.1.0] — initial public release

Pre-1.0 footprint:

- Pure-Go NNTP / yEnc / NZB / PAR2 stack (no cgo).
- DDD + transactional-outbox architecture with bounded contexts for
  download / verify / repair / extract / deliver / server / notify /
  auth.
- Multi-server pool with priority tiers, backup flag, metered/quota
  tracking, per-server bandwidth caps.
- PAR2 verify + repair over GF(2^16) Reed-Solomon, with on-demand
  fetch of `.vol*` recovery volumes ("SAB smart par2").
- RAR3 + RAR5 extraction via `nwaples/rardecode` (the one third-party
  data-path dependency).
- SABnzbd API shim at `/sabnzbd/api` for *arr-suite compatibility:
  Sonarr, Radarr, Lidarr, Readarr, and Prowlarr all drop in.
- Sonarr/Radarr-style React UI: Activity / History / Settings / System
  pages, drag-reorder queue, live SSE progress, per-job file +
  segment drilldown, drag-reorder, server card grid, webhook +
  Discord + Slack + Pushover notification providers.
- Live-editable runtime config in SQLite (URL base, concurrency cap,
  bandwidth, recovery-vol deferral, sample handling, folder
  collapse).
- Durable scheduled tasks subsystem that survives restart.

[Unreleased]: https://github.com/jaenster/hoardarr/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jaenster/hoardarr/releases/tag/v0.2.0
[0.1.0]: https://github.com/jaenster/hoardarr/releases/tag/v0.1.0
