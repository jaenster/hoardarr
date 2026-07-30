# syntax=docker/dockerfile:1.6
#
# Two stages, and the runtime stage is `scratch`.
#
# The Go image was alpine + ca-certificates + su-exec + tzdata, ~20 MB of
# base before the binary. None of that is needed here:
#
#   * **No libc.** On Linux the Zig code goes straight to syscalls (see
#     src/posix/sys.zig), so there is no dynamic loader and nothing to
#     link against at runtime. Only the vendored SQLite wants a libc, and
#     musl is linked statically into the binary.
#   * **No su-exec.** Dropping to PUID:PGID was the entrypoint shim's job
#     because Go can't setuid reliably from a multithreaded runtime. We
#     call setgid/setgroups/setuid ourselves before starting the reactor,
#     which also removes the shell from the image.
#   * **The CA bundle, and nothing else from a distro.** Every Usenet
#     provider is TLS on 563, so trust anchors are not optional — a
#     `scratch` image without them refuses to dial any real provider, which
#     is exactly what the first deployment of this image did. The single
#     `ca-certificates.crt` is copied out of the builder, so it tracks the
#     base image's bundle rather than a vendored copy that silently goes
#     stale. That is ~230 KB and the only file in the image besides the
#     binary.
#   * **No tzdata.** Timestamps are stored and logged in UTC and rendered
#     in the browser's zone, which is where a user's timezone actually
#     lives.
#   * **No frontend directory.** The bundle is embedded, gzipped, at build
#     time.
#
# What's left is one static binary and two empty directories, so the image
# is the binary plus a few hundred bytes of metadata.

FROM --platform=$BUILDPLATFORM node:22-alpine AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install --no-fund --no-audit
COPY frontend/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM alpine:3.20 AS builder

# The official tarball rather than the distro package: Alpine 3.20 ships a
# Zig old enough to reject build.zig.zon's syntax, and pinning the exact
# compiler with its checksum is what makes this build reproducible.
ARG ZIG_VERSION=0.16.0
RUN set -eux; \
    apk add --no-cache curl xz ca-certificates; \
    case "$(uname -m)" in \
      x86_64)  ZARCH=x86_64;  ZSHA=70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00 ;; \
      aarch64) ZARCH=aarch64; ZSHA=ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17 ;; \
      *) echo "unsupported build arch: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSLO "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZARCH}-linux-${ZIG_VERSION}.tar.xz"; \
    echo "${ZSHA}  zig-${ZARCH}-linux-${ZIG_VERSION}.tar.xz" | sha256sum -c -; \
    tar -xJf "zig-${ZARCH}-linux-${ZIG_VERSION}.tar.xz" -C /opt; \
    mv "/opt/zig-${ZARCH}-linux-${ZIG_VERSION}" /opt/zig; \
    rm "zig-${ZARCH}-linux-${ZIG_VERSION}.tar.xz"
ENV PATH="/opt/zig:$PATH"

WORKDIR /src
COPY build.zig build.zig.zon ./
COPY src/ ./src/
COPY c/ ./c/
COPY tools/ ./tools/
COPY --from=frontend /src/frontend/dist ./frontend/dist

ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# ReleaseSmall rather than ReleaseFast for the image: the hot paths are the
# SIMD codecs, and those are dominated by their vector loops rather than by
# anything the size/speed tradeoff touches. Measure before changing this —
# bench/run.sh is the tool.
RUN case "$TARGETARCH" in \
      amd64) ZTARGET=x86_64-linux-musl ;; \
      arm64) ZTARGET=aarch64-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac && \
    zig build \
      --release=small \
      -Dtarget=$ZTARGET \
      -Dstrip=true \
      -Dembed-ui=true \
      -Dversion="$VERSION" \
      -Dcommit="$COMMIT" \
      -Dbuild-date="$BUILD_DATE" \
      --prefix /out

FROM scratch
LABEL org.opencontainers.image.source="https://github.com/jaenster/hoardarr"
LABEL org.opencontainers.image.title="hoardarr"
LABEL org.opencontainers.image.description="A Usenet downloader with a Sonarr/Radarr-style UI"

COPY --from=builder /out/bin/hoardarr /hoardarr

# The path the daemon already looks in first; see `ca_bundle_paths` in
# src/bootstrap/runtime.zig. Without this it logs "no CA bundle found; TLS
# providers will not be dialled" and every `tls = true` server is registered
# but never contacted.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# `scratch` has no filesystem at all, so the mount points have to be
# created here. A bind mount would create them implicitly, but a named
# volume or a plain `docker run` would not.
WORKDIR /data
VOLUME ["/data"]

ENV HOARDARR_LISTEN=:8085
ENV HOARDARR_DATA_DIR=/data

EXPOSE 8085

# No shell in the image, so this must be the exec form. The binary probes
# itself over the loopback listener.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/hoardarr", "healthcheck"]

ENTRYPOINT ["/hoardarr"]
CMD ["serve"]
