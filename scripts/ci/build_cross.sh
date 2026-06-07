#!/usr/bin/env bash
#
# Cross-compile the nram server binary for every supported OS/arch and,
# optionally, package each build into a release archive alongside a combined
# SHA256SUMS manifest.
#
# A single host can build every target: the binary embeds the web UI
# (internal/ui/dist via go:embed) and uses the pure-Go SQLite driver
# (modernc.org/sqlite), so cross-compilation needs no C toolchain or per-OS
# runner — just CGO_ENABLED=0 with GOOS/GOARCH.
#
# Build identity (commit, dirty flag, build time) is stamped automatically by
# the Go toolchain's VCS embedding (runtime/debug); no ldflags are required, so
# this script deliberately sets none.
#
# Requirements on PATH: bash, go, tar; zip (only when PACKAGE=1); npm (only when
# the embedded UI is rebuilt, i.e. SKIP_UI != 1). sha256 is taken from
# sha256sum or, as a fallback, shasum -a 256.
#
# Environment:
#   VERSION     label embedded in artifact filenames (default: git short SHA)
#   PACKAGE     "1" => tar.gz/zip each binary and emit dist/SHA256SUMS;
#               otherwise raw binaries are copied into the output directory
#   OUTPUT_DIR  destination directory, wiped and recreated (default: <repo>/dist)
#   SKIP_UI     "1" => do not run `make build-ui` (assumes dist already built)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/dist}"
PACKAGE="${PACKAGE:-0}"
SKIP_UI="${SKIP_UI:-0}"

if [ -z "${VERSION:-}" ]; then
  VERSION="$(git rev-parse --short HEAD 2>/dev/null || echo dev)"
fi

# OS/arch matrix. Pure-Go build => every entry compiles from one host.
TARGETS=(
  linux/amd64
  linux/arm64
  darwin/amd64
  darwin/arm64
  windows/amd64
  windows/arm64
)

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  else
    shasum -a 256 "$@"
  fi
}

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

if [ "$SKIP_UI" != "1" ]; then
  echo "build_cross: building embedded UI (make build-ui)"
  make build-ui
fi

echo "build_cross: version=$VERSION package=$PACKAGE output=$OUTPUT_DIR"

for t in "${TARGETS[@]}"; do
  goos="${t%/*}"
  goarch="${t#*/}"
  binext=""
  [ "$goos" = "windows" ] && binext=".exe"

  stage="$(mktemp -d)"
  binpath="$stage/nram${binext}"

  echo "build_cross: compiling $goos/$goarch"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build -trimpath -o "$binpath" ./cmd/server

  name="nram_${VERSION}_${goos}_${goarch}"
  if [ "$PACKAGE" = "1" ]; then
    if [ "$goos" = "windows" ]; then
      (cd "$stage" && zip -q "$OUTPUT_DIR/${name}.zip" "nram${binext}")
    else
      tar -czf "$OUTPUT_DIR/${name}.tar.gz" -C "$stage" "nram${binext}"
    fi
  else
    cp "$binpath" "$OUTPUT_DIR/${name}${binext}"
  fi
  rm -rf "$stage"
done

if [ "$PACKAGE" = "1" ]; then
  echo "build_cross: writing SHA256SUMS"
  # Sum only the artifacts (nram_*) so the manifest never lists itself.
  (cd "$OUTPUT_DIR" && sha256 nram_* > SHA256SUMS)
fi

echo "build_cross: done"
ls -l "$OUTPUT_DIR"
