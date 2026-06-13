#!/usr/bin/env bash
#
# Generate per-arch Windows resource files (resource_windows_<arch>.syso) into
# cmd/server/ so that `go build` with GOOS=windows automatically embeds the
# Neural Ram icon and file/version metadata into nram.exe.
#
# goversioninfo is pure Go and is run via `go run pkg@version`, so this never
# modifies go.mod/go.sum. Go's filename build constraints ensure each
# resource_windows_<arch>.syso links only into the matching GOOS/GOARCH build;
# non-windows builds ignore them entirely.
#
# The generated .syso files are build artifacts, not committed sources;
# build_cross.sh removes them after cross-compiling.
#
# Environment:
#   VERSION  version string (default 0.0.0); the leading numeric major.minor.patch
#            is parsed for the binary FixedFileInfo, and the full string is used
#            for the human-readable File/Product version fields.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$REPO_ROOT/scripts/ci/version.sh"

VERSION="${VERSION:-0.0.0}"
GOVERSIONINFO_VERSION="v1.4.0"

# Leading numeric major.minor.patch for the binary FixedFileInfo; the full
# VERSION string is used for the human-readable File/Product version fields.
read -r major minor patch < <(parse_version_numeric "$VERSION")

echo "gen_winres: VERSION=$VERSION -> numeric $major.$minor.$patch"

# `-platform-specific` writes resource_windows_<arch>.syso into the current
# working directory and ignores -o, so run it from cmd/server/ (where main() is)
# with absolute paths to the icon and config.
ICON="$REPO_ROOT/packaging/appicon/icon.ico"
JSON="$REPO_ROOT/packaging/appicon/versioninfo.json"

( cd "$REPO_ROOT/cmd/server" && \
	go run "github.com/josephspurrier/goversioninfo/cmd/goversioninfo@${GOVERSIONINFO_VERSION}" \
		-platform-specific=true \
		-icon "$ICON" \
		-file-version "$VERSION" \
		-product-version "$VERSION" \
		-ver-major "$major" -ver-minor "$minor" -ver-patch "$patch" \
		-product-ver-major "$major" -product-ver-minor "$minor" -product-ver-patch "$patch" \
		"$JSON" )

echo "gen_winres: generated:"
ls -1 "$REPO_ROOT"/cmd/server/resource_windows_*.syso
