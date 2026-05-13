# Contributing to hoardarr

Thanks for your interest. hoardarr is pre-1.0 and developed in the
open. PRs, issues, and ideas are welcome — but please read this short
guide first so we don't end up making churn for each other.

## Before you start

For anything bigger than a typo or one-file bug fix, **open an issue
first** describing the change. Especially for:

- New SAB API modes / endpoints.
- New domain bounded contexts.
- Schema migrations.
- New runtime-mutable settings or REST endpoints.
- Frontend additions outside an existing page.

The architecture is intentionally event-driven + DDD-shaped (see the
implementation plan in `~/.claude/plans/` for the deep version, or the
top-level layout note in `README.md`). Cross-cutting changes that
ignore the bounded-context split tend to need a rewrite — saves both
of us time to align on shape first.

## Development setup

```bash
# Backend tests
go test ./...

# Backend race tests
go test -race ./...

# Vet (CI gates on this)
go vet ./...

# Production build (single binary, embedded frontend)
make build

# Dev mode — frontend live-reload on :5173, backend on :8085
cd frontend && npm install && npm run dev    # terminal 1
go run ./cmd/hoardarr serve                   # terminal 2
```

End-to-end tests against a real Usenet provider are env-gated:

```bash
HOARDARR_USENET_TEST=1 go test -tags integration ./internal/adapter/nntp/...
```

Playwright drives the frontend against the real binary:

```bash
make build && cd frontend && npx playwright test
```

CI runs the non-integration suite + Playwright on every PR. Real-provider
tests are skipped on CI.

## Code style

- **slog, not fmt.Print.** Structured logging everywhere. `fmt.Errorf`
  is only for error wrapping.
- **No comments saying "phase 1" / "stage" / "according to the plan"** —
  the plan is meta; the code should stand on its own. Comments explain
  *why*, not *what*.
- **Never delete `.skip` / xfail / disabled tests** without writing a
  one-line follow-up issue (or task in the local list).
- **Co-author trailers are forbidden in commit messages.** No
  `Co-Authored-By:` for tooling.
- **No `git add .`** in scripts / docs — always stage explicit paths.

Go formatting is `gofmt`/`goimports` by default. Frontend uses the
project's `tsc` strict settings; if `tsc --noEmit` is unhappy with
your change, fix it before opening the PR.

## Commit messages

Format: `area: brief description`, lowercase area, imperative mood.
Examples from the recent log:

```
deliver: SAB-style post-processing (deobfuscate, samples, collapse)
par2: on-demand fetch of recovery volumes (SAB's smart par2)
nntp: backoff on 'too many connections' instead of failing
```

Bodies (when present) explain *why* the change is correct, not what
the diff already says.

## DCO

By submitting a contribution you assert that:

- The work is yours (or you have permission to contribute it under
  the project's MIT license).
- You're licensing the contribution under the same MIT terms.

We don't currently require a `Signed-off-by:` line, but if you want to
add one it won't be rejected.

## Reporting bugs

Use the `.github/ISSUE_TEMPLATE/bug_report.yml` form. Please include:

- Version (output of `./hoardarr --version` once that's wired; for now
  the git SHA from `git rev-parse HEAD`).
- Whether you're running the binary directly or the Docker image.
- The relevant log lines (slog text format is fine; please don't
  include API keys or credentials).
- If applicable: a minimal NZB or fixture that reproduces it.

## Security issues

See [`SECURITY.md`](SECURITY.md). Do not open public issues for
suspected vulnerabilities.

## Code of conduct

This project follows the [Contributor Covenant
2.1](CODE_OF_CONDUCT.md).
