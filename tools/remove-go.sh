#!/usr/bin/env bash
#
# Remove the Go implementation.
#
# Run this only once the parity gate is green — `internal/` is the source
# for the end-to-end tests, so deleting it earlier means porting the
# remainder from memory. `PORT.md` records the sequencing.
#
# What survives, and why:
#
#   testdata/repair-bug-job38/   a real ParPar index used as the PAR2
#                                oracle by the Zig tests. It is the only
#                                repo-relative path those tests open —
#                                verified by grep, not assumed. Note it is
#                                *gitignored*, so it lives on developer
#                                machines only and the tests that use it
#                                skip in CI.
#   frontend/                    unchanged; it was never Go.
#   docs/, README.md, CHANGELOG  already rewritten for the Zig build.
#
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -n "$(git status --porcelain)" ]]; then
    echo "working tree is dirty; commit or stash first" >&2
    exit 1
fi

echo "==> removing the Go tree"
git rm -r -q --ignore-unmatch \
    internal \
    cmd \
    go.mod \
    go.sum \
    assets.go \
    assets_dev.go \
    assets_embed.go \
    bench/go \
    docker/entrypoint.sh

# The Go Dockerfile's runtime stage is alpine and runs `apk add`, which is
# why docker.yml still emulates arm64. The Zig one cross-compiles in the
# builder and its runtime stage is `scratch`, so nothing foreign is ever
# executed and QEMU can go with it.
echo "==> Dockerfile.zig -> Dockerfile"
git rm -q --ignore-unmatch Dockerfile
git mv Dockerfile.zig Dockerfile

echo "==> checking nothing still references Go"
leftovers=$(grep -rlniE '\bgo\.mod\b|golang|goroutine|CGO_ENABLED|GOOS|modernc' \
    --include='*.zig' --include='*.yml' --include='*.yaml' \
    --include='Makefile' --include='Dockerfile' --include='*.md' \
    . 2>/dev/null | grep -v '^\./PORT.md$' | grep -v '^\./CHANGELOG.md$' || true)
if [[ -n "$leftovers" ]]; then
    echo "still referencing Go (PORT.md and CHANGELOG.md are allowed to):" >&2
    echo "$leftovers" >&2
    exit 1
fi

echo "==> verifying the Zig build is unaffected"
zig build test --release=fast --summary all
zig build check
zig build --release=fast -Dembed-ui=true

echo
echo "Go removed. Remaining tree:"
git ls-files | sed 's|/.*||' | sort -u | tr '\n' ' '
echo
