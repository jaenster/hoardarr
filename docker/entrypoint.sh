#!/bin/sh
#
# hoardarr container entrypoint.
#
# Reads three optional env vars in the linuxserver-image style and
# drops privileges to the corresponding uid/gid before exec'ing the
# real binary:
#
#   PUID — numeric uid the binary should run as. Default 1000.
#   PGID — numeric gid. Default 1000.
#   TZ   — IANA zone (e.g. Europe/Amsterdam). Default Etc/UTC.
#
# Passing PUID/PGID matters because the homelab convention is to
# bind-mount a host directory into /data and expect files to be
# owned by your host user. Without this shim the operator would
# have to chown every host directory to match whatever uid the
# image bakes in.

set -eu

PUID=${PUID:-1000}
PGID=${PGID:-1000}
TZ=${TZ:-Etc/UTC}

# Set the container timezone so Go's `time` package picks it up,
# slog timestamps render correctly, and webhook payloads ship
# operator-local times.
if [ -f "/usr/share/zoneinfo/$TZ" ]; then
    cp "/usr/share/zoneinfo/$TZ" /etc/localtime
    echo "$TZ" > /etc/timezone
else
    echo "entrypoint: unknown TZ '$TZ', falling back to UTC" >&2
fi

# Materialise a group + user with the requested numeric ids if
# they're not already present in the image.
if ! getent group "$PGID" >/dev/null 2>&1; then
    addgroup -g "$PGID" hoardarr 2>/dev/null || true
fi
if ! getent passwd "$PUID" >/dev/null 2>&1; then
    adduser -D -H -u "$PUID" -G hoardarr -s /sbin/nologin hoardarr 2>/dev/null || true
fi

# Ensure the data dir is writable by the runtime user. Bind-mounted
# hosts dirs come in with whatever ownership the host gave them;
# this lets a freshly-created `./data` work without a separate
# host-side chown.
if [ -d "$HOARDARR_DATA_DIR" ]; then
    chown -R "$PUID:$PGID" "$HOARDARR_DATA_DIR" 2>/dev/null || true
fi

# cd into the data dir so hoardarr's default `./config.toml` lookup
# lands inside the bind-mounted volume rather than at /. Without this
# the binary errors with `permission denied` on first run because the
# unprivileged PUID/PGID user can't write to the container root.
cd "$HOARDARR_DATA_DIR"

# su-exec is suid-less — uses setresuid/setresgid directly, no fork
# overhead, no signal-forwarding wrapper. PID 1 is the hoardarr
# binary, which is what we want for graceful SIGTERM handling.
exec su-exec "$PUID:$PGID" /usr/local/bin/hoardarr "$@"
