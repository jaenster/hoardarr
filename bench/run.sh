#!/usr/bin/env bash
#
# Run the Zig benchmarks and the Go baseline back to back, so both
# columns of bench/REPORT.md come off the same machine in the same
# thermal state. Comparing a number you measured today against one
# somebody posted last year is how benchmark tables end up lying.
#
# Usage:
#   bench/run.sh              # everything
#   bench/run.sh yenc         # only benchmarks whose name contains "yenc"
#
set -euo pipefail
cd "$(dirname "$0")/.."

filter="${1:-}"

echo "=============================================================="
echo " machine"
echo "=============================================================="
if [[ "$(uname -s)" == "Darwin" ]]; then
    sysctl -n machdep.cpu.brand_string
    echo "cores: $(sysctl -n hw.ncpu)"
else
    grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//'
    echo "cores: $(nproc)"
fi
echo "zig:   $(zig version)"
echo "go:    $(go version | awk '{print $3}')"
echo

echo "=============================================================="
echo " zig"
echo "=============================================================="
# --release=fast on the harness; build.zig additionally forces the
# library module to ReleaseFast, because linking a Debug library into a
# ReleaseFast harness measures bounds checks rather than the code.
zig build bench -- ${filter}
echo

echo "=============================================================="
echo " go baseline"
echo "=============================================================="
# -benchtime=2s so each case gets enough iterations to settle; Go's
# default 1s leaves the fast cases noisy.
if [[ -n "$filter" ]]; then
    go test -run '^$' -bench "$filter" -benchtime=2s ./bench/go/
else
    go test -run '^$' -bench . -benchtime=2s ./bench/go/
fi
