#!/usr/bin/env bash
#
# Run the Zig benchmarks.
#
# The Go baseline used to run here too, back to back, so both columns came
# off the same machine in the same thermal state — comparing a number you
# measured today against one somebody posted last year is how benchmark
# tables end up lying.
#
# Usage:
#   bench/run.sh              # everything
#   bench/run.sh yenc         # only benchmarks whose name contains "yenc"
#
# This used to run the Go implementation alongside, so both columns of
# bench/REPORT.md came off one machine in one sitting. The Go tree is gone;
# the numbers it produced are recorded there.
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

# The Go baseline is gone with the Go tree. Its numbers are preserved in
# bench/REPORT.md, measured on the same machine at the same time as the Zig
# column — which is the only way that comparison was ever meaningful. Do not
# re-add a column here that was measured on a different day or a different
# box; a table like that reads as a comparison and isn't one.
echo "Go baseline: see bench/REPORT.md (the Go tree was removed once the"
echo "parity suite passed; its numbers were taken alongside these)."
