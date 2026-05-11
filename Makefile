#
# hoardarr — make targets.
#
# Default: `make build` produces a single-binary `./hoardarr` with the
# frontend baked in via go:embed. This is what you run in production.
#
# For day-to-day frontend iteration prefer `make dev` (Vite live-reload
# on :5173 + Go daemon serving the API on :8080).
#
# Variables you can override on the command line:
#     make build  BIN=./hoardarr.test     # change output path
#     make build  GOFLAGS=-trimpath -ldflags='-s -w'  # release flags
#

SHELL := /bin/bash
BIN ?= ./hoardarr
GOFLAGS ?=
NPM ?= npm
GO ?= go

# Source-listing helpers used as Make dependency targets so we only
# re-run the corresponding step when the inputs actually change.
FRONTEND_SRC := $(shell find frontend/src frontend/public 2>/dev/null) \
                frontend/package.json frontend/tsconfig.json \
                frontend/vite.config.ts frontend/index.html
GO_SRC := $(shell find . -name '*.go' -not -path './frontend/*' 2>/dev/null) go.mod go.sum

.PHONY: all
all: build

# ---------------------------------------------------------------------
# Production build: frontend bundle + embedded binary.
# ---------------------------------------------------------------------
.PHONY: build
build: $(BIN)

$(BIN): frontend/dist/index.html $(GO_SRC)
	@echo "==> go build (embedded) → $(BIN)"
	@$(GO) build $(GOFLAGS) -tags embed -o $(BIN) ./cmd/hoardarr

frontend/dist/index.html: $(FRONTEND_SRC)
	@echo "==> npm install (if needed)"
	@cd frontend && [ -d node_modules ] || $(NPM) install
	@echo "==> npm run build"
	@cd frontend && $(NPM) run build

# ---------------------------------------------------------------------
# Dev: assumes you run `cd frontend && npm run dev` in another terminal.
# The Go daemon here is built WITHOUT -tags embed; the frontend is
# served by Vite directly.
# ---------------------------------------------------------------------
.PHONY: dev
dev:
	@$(GO) run ./cmd/hoardarr serve

# ---------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------
.PHONY: test
test:
	@$(GO) test -count=1 ./...

.PHONY: race
race:
	@$(GO) test -count=1 -race ./...

# Frontend type-check + bundle without re-running Go.
.PHONY: frontend
frontend: frontend/dist/index.html

# ---------------------------------------------------------------------
# Run the built binary. Builds first if stale.
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
	@rm -f $(BIN) hoardarr.new
	@rm -rf frontend/dist

.PHONY: tidy
tidy:
	@$(GO) mod tidy
	@cd frontend && $(NPM) install
