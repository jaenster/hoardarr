#
# hoardarr — make targets.
#
# The build itself is `build.zig`; this file is a set of shorthands for
# the combinations people actually type, plus the frontend step, which
# Zig doesn't own.
#
# Default: `make build` produces a single `./hoardarr` with the frontend
# embedded (gzipped at build time). That is what runs in production.
#
# For frontend iteration use `make dev`: Vite live-reloads on :5173 and
# the daemon serves the API on :8085, without the embed step.
#

SHELL := /bin/bash
ZIG ?= zig
NPM ?= npm
BIN := zig-out/bin/hoardarr

# Baked into the binary via -Doption. VERSION is the closest git tag, or
# "dev" outside one; COMMIT is the short SHA; BUILD_DATE is RFC3339 UTC.
# Override on the command line; CI and the Dockerfile set them explicitly.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

STAMP := -Dversion="$(VERSION)" -Dcommit="$(COMMIT)" -Dbuild-date="$(BUILD_DATE)"

FRONTEND_SRC := $(shell find frontend/src frontend/public 2>/dev/null) \
                frontend/package.json frontend/tsconfig.json \
                frontend/vite.config.ts frontend/index.html

.PHONY: all
all: build

# ---------------------------------------------------------------------
# Production build: frontend bundle + embedded binary.
# ---------------------------------------------------------------------
.PHONY: build
build: frontend/dist/index.html
	@echo "==> zig build (embedded) → $(BIN) ($(VERSION) $(COMMIT))"
	@$(ZIG) build --release=fast -Dembed-ui=true -Dstrip=true $(STAMP)

frontend/dist/index.html: $(FRONTEND_SRC)
	@echo "==> npm install (if needed)"
	@cd frontend && [ -d node_modules ] || $(NPM) install
	@echo "==> npm run build"
	@cd frontend && $(NPM) run build

.PHONY: frontend
frontend: frontend/dist/index.html

# ---------------------------------------------------------------------
# Dev: no embed step, so the binary rebuilds in a second. Run
# `cd frontend && npm run dev` in another terminal for the UI.
# ---------------------------------------------------------------------
.PHONY: dev
dev:
	@$(ZIG) build run -- serve

# ---------------------------------------------------------------------
# Tests.
#
# Both optimisation modes, because they catch different things: Debug
# has the safety checks, ReleaseFast has the optimiser. The reactor and
# the SIMD codecs have each had a bug that only showed up optimised.
# ---------------------------------------------------------------------
.PHONY: test
test:
	@$(ZIG) build test --summary all

.PHONY: test-release
test-release:
	@$(ZIG) build test --release=fast --summary all

.PHONY: check
check:
	@echo "==> type-checking every shipping target"
	@$(ZIG) build check

.PHONY: fmt
fmt:
	@$(ZIG) fmt src/ bench/ tools/ build.zig

.PHONY: fmt-check
fmt-check:
	@$(ZIG) fmt --check src/ bench/ tools/ build.zig

# Everything CI runs, so a green `make ci` locally means a green CI.
.PHONY: ci
ci: fmt-check test test-release check

# ---------------------------------------------------------------------
# Benchmarks. `bench/run.sh` also runs the Go baseline where one still
# exists, so both columns come off the same machine.
# ---------------------------------------------------------------------
.PHONY: bench
bench:
	@$(ZIG) build bench

.PHONY: bench-compare
bench-compare:
	@./bench/run.sh

# ---------------------------------------------------------------------
# Container. Runtime stage is `scratch`; see Dockerfile.zig for why
# nothing else is needed in the image.
# ---------------------------------------------------------------------
.PHONY: docker
docker:
	@docker build -f Dockerfile.zig -t hoardarr:dev \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) .
	@docker images hoardarr:dev --format '==> image size: {{.Size}}'

# ---------------------------------------------------------------------
# Run the built binary.
# ---------------------------------------------------------------------
.PHONY: run
run: build
	@$(BIN) serve

# ---------------------------------------------------------------------
# Housekeeping.
# ---------------------------------------------------------------------
.PHONY: clean
clean:
	@echo "==> removing build artifacts"
	@rm -rf zig-out .zig-cache frontend/dist
