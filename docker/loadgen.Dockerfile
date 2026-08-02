# syntax=docker/dockerfile:1.6
#
# The load generator, built the same way as the daemon and shipped the
# same way: cross-compiled in a pinned Zig builder, run from `scratch`.
#
# It is deliberately even barer than the root image:
#
#   * **No CA bundle.** loadgen speaks plaintext NNTP to whoever connects
#     to it. It dials nothing, so it trusts nothing.
#   * **No frontend and no SQLite.** The binary links neither; see the
#     `loadgen` step in build.zig, which gives it a library module without
#     either. That is also why there is no node stage here.
#   * **No volume.** The only thing it writes is the NZB, and the whole
#     point of that file is to be picked up from a bind mount by whatever
#     POSTs it to the daemon.
#
# Run it beside the daemon on the same host you are measuring, so the
# offered load crosses a loopback or a bridge rather than the link you
# are trying to characterise.

FROM --platform=$BUILDPLATFORM alpine:3.20 AS builder

# Same pinned tarball and checksums as the root Dockerfile: the two
# images must be built by the same compiler or a measurement taken
# against one says nothing about the other.
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
COPY tools/ ./tools/

ARG TARGETARCH

# `zig build loadgen` rather than the default step: the default one is
# the daemon, which needs the C amalgamation this image does not copy in.
# The optimize mode is fixed in build.zig — a load generator built at
# anything but ReleaseFast measures itself.
RUN case "$TARGETARCH" in \
      amd64) ZTARGET=x86_64-linux-musl ;; \
      arm64) ZTARGET=aarch64-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac && \
    zig build loadgen \
      -Dtarget=$ZTARGET \
      -Dstrip=true \
      --prefix /out

FROM scratch
LABEL org.opencontainers.image.source="https://github.com/jaenster/hoardarr"
LABEL org.opencontainers.image.title="hoardarr-loadgen"
LABEL org.opencontainers.image.description="Synthetic Usenet release served over NNTP, for load-testing hoardarr"

COPY --from=builder /out/bin/loadgen /loadgen

# Where the NZB lands. Bind-mount over it to get the file out; without a
# mount it stays inside the container's writable layer, which is fine for
# a run driven by `docker exec`-free tooling that only needs the port.
WORKDIR /out

EXPOSE 1119

ENTRYPOINT ["/loadgen"]
CMD ["--listen", "0.0.0.0:1119", "--nzb", "/out/release.nzb"]
