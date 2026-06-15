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
source "$REPO_ROOT/scripts/ci/version.sh"

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

# Pinned, pure-Go packaging tool versions, fetched with a @version suffix so
# go.mod is never modified. goversioninfo (run inside gen_winres.sh) emits the
# Windows icon/version resource; nfpm builds the Linux .deb/.rpm.
NFPM_VERSION="v2.46.3"

# Package-safe version for .deb/.rpm and macOS CFBundleShortVersionString. deb/rpm
# versions must start with a digit, so a non-numeric VERSION (e.g. a git short SHA
# on a local build) is given a synthetic 0.0.0+ prefix; '-' becomes '~' (valid
# pre-release separator for both formats) and any other stray char becomes '_'.
PKGVER="${VERSION#v}"
case "$PKGVER" in [0-9]*) ;; *) PKGVER="0.0.0+${PKGVER}" ;; esac
PKGVER="$(printf '%s' "$PKGVER" | tr '-' '~' | tr -c 'A-Za-z0-9.+~' '_')"

# Single EXIT trap cleans up the build-time artifacts/tools the steps below set.
TOOLBIN=""
cleanup() {
  rm -f "$REPO_ROOT"/cmd/server/resource_windows_*.syso
  [ -n "$TOOLBIN" ] && rm -rf "$TOOLBIN"
}
trap cleanup EXIT

# Windows targets embed an icon + file metadata via resource_windows_<arch>.syso
# files placed next to main() in cmd/server/ (build artifacts, never committed;
# the trap removes them).
if printf '%s\n' "${TARGETS[@]}" | grep -q '^windows/'; then
  echo "build_cross: generating Windows resources"
  VERSION="$VERSION" bash "$REPO_ROOT/scripts/ci/gen_winres.sh"
fi

# Build nfpm once into a temp GOBIN, rather than paying the `go run` link cost on
# every per-arch, per-packager (deb/rpm) invocation in the loop below.
NFPM_BIN=""
if printf '%s\n' "${TARGETS[@]}" | grep -q '^linux/'; then
  TOOLBIN="$(mktemp -d)"
  echo "build_cross: installing nfpm $NFPM_VERSION"
  GOBIN="$TOOLBIN" go install "github.com/goreleaser/nfpm/v2/cmd/nfpm@${NFPM_VERSION}"
  NFPM_BIN="$TOOLBIN/nfpm"
fi

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
    case "$goos" in
      windows)
        # Single .exe (icon + metadata already embedded via the .syso).
        (cd "$stage" && zip -q "$OUTPUT_DIR/${name}.zip" "nram${binext}")
        ;;
      darwin)
        # nram is a terminal server on macOS; ship the raw binary in a tarball.
        # No .app bundle: an unsigned, un-notarized bundle is blocked by Gatekeeper
        # on download, and the binary is run from a terminal regardless.
        tar -czf "$OUTPUT_DIR/${name}.tar.gz" -C "$stage" "nram${binext}"
        ;;
      linux)
        # Raw-binary tarball (unchanged), plus native packages that install a
        # desktop launcher + hicolor icons for real menu integration. Render the
        # nfpm config with sed (the binary path and arch/version vary per build;
        # nfpm's own env expansion does not cover the contents glob). The
        # desktop/icon `src` paths stay relative and resolve from $REPO_ROOT (cwd).
        tar -czf "$OUTPUT_DIR/${name}.tar.gz" -C "$stage" "nram${binext}"
        nfpmcfg="$stage/nfpm.yaml"
        sed -e "s|\${ARCH}|${goarch}|g" \
            -e "s|\${VERSION}|${PKGVER}|g" \
            -e "s|\${BINARY}|${binpath}|g" \
          "$REPO_ROOT/packaging/linux/nfpm.yaml" > "$nfpmcfg"
        for pkg in deb rpm; do
          "$NFPM_BIN" package --config "$nfpmcfg" --packager "$pkg" \
            --target "$OUTPUT_DIR/${name}.${pkg}"
        done
        ;;
    esac
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
