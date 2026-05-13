# syntax=docker/dockerfile:1.6
#
# Two-stage build:
#   builder — golang:alpine compiles the frontend (Node) + the Go binary
#             with -tags embed so frontend/dist is baked in.
#   runtime — distroless static. We need ca-certificates for TLS
#             dialing to Usenet providers; the static-debian image
#             ships them. No shell, no package manager; just our
#             binary + a non-root user.
#
# Image is intentionally minimal because the data dir is mounted from
# the host. Everything that needs to persist lives outside the image.

# Pin the frontend stage to the build platform so buildx doesn't run
# npm under QEMU on arm64 — npm install + vite build under emulation
# is brutally slow (we observed >90 min on a free runner before
# cancelling). Frontend output is arch-neutral JS/CSS so this is safe.
FROM --platform=$BUILDPLATFORM node:22-alpine AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install --no-fund --no-audit
COPY frontend/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend /src/frontend/dist ./frontend/dist

# Build args injected by `docker buildx build` for multi-arch builds.
# TARGETOS / TARGETARCH come from the buildx platform list (linux/amd64,
# linux/arm64, ...). VERSION / COMMIT / BUILD_DATE land in -ldflags so
# `hoardarr version` reports correct identification per-image.
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -tags embed \
        -trimpath \
        -ldflags="-s -w \
          -X main.version=${VERSION} \
          -X main.commit=${COMMIT} \
          -X main.buildDate=${BUILD_DATE}" \
        -o /out/hoardarr ./cmd/hoardarr

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/jaenster/hoardarr"
LABEL org.opencontainers.image.title="hoardarr"
LABEL org.opencontainers.image.description="Go-based SABnzbd alternative with a Sonarr/Radarr-style UI"

# Distroless nonroot is uid/gid 65532. The compose file overrides
# this with the operator's PUID/PGID via `user:` so the container
# writes files with the right ownership against the host bind mount.
USER nonroot:nonroot

# Working dir is also the data dir by default; compose mounts a host
# folder on top of /data so state survives container recreation.
WORKDIR /data

# Bind to all interfaces by default. The compose port-publish picks
# the host-facing port.
ENV HOARDARR_LISTEN=:8085
ENV HOARDARR_DATA_DIR=/data

COPY --from=builder /out/hoardarr /usr/local/bin/hoardarr

EXPOSE 8085

# Distroless has no shell or curl, so the only way to self-probe is
# to invoke the binary's own `healthcheck` subcommand. It reads the
# HOARDARR_LISTEN env above to find the port and GETs /healthz.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/hoardarr", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/hoardarr"]
CMD ["serve"]
