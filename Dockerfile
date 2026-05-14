# syntax=docker/dockerfile:1.6
#
# Three-stage build:
#   frontend — node, builds the React bundle. Pinned to BUILDPLATFORM
#              so arm64 builds don't run npm under QEMU emulation.
#   builder  — golang:alpine cross-compiles the Go binary with the
#              embedded frontend (-tags embed).
#   runtime  — alpine + a tiny entrypoint shim. We pick alpine over
#              distroless so we can support the linuxserver-style
#              PUID/PGID/TZ env vars homelab users expect; the cost
#              is ~20MB of additional image size.

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

FROM alpine:3.20
LABEL org.opencontainers.image.source="https://github.com/jaenster/hoardarr"
LABEL org.opencontainers.image.title="hoardarr"
LABEL org.opencontainers.image.description="Go-based SABnzbd alternative with a Sonarr/Radarr-style UI"

# ca-certificates → TLS to Usenet providers + indexers.
# su-exec → drop privileges to PUID:PGID in the entrypoint without
#           a fat suid-tool / s6-overlay.
# tzdata  → so $TZ resolves against /usr/share/zoneinfo.
RUN apk add --no-cache ca-certificates su-exec tzdata

COPY --from=builder /out/hoardarr /usr/local/bin/hoardarr
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# /data is the canonical mount point — config.toml, the SQLite DB,
# sessions, incomplete/, and complete/ all live here. WORKDIR matches
# the entrypoint's `cd $HOARDARR_DATA_DIR` so a `docker run` without
# the shim (e.g. `docker run ... hoardarr version` for ad-hoc probes)
# still picks up the right cwd.
WORKDIR /data
ENV HOARDARR_LISTEN=:8085
ENV HOARDARR_DATA_DIR=/data
VOLUME ["/data"]

EXPOSE 8085

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/hoardarr", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["serve"]
