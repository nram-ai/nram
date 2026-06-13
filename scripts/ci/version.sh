#!/usr/bin/env bash
#
# Side-effect-free version helpers shared by the build/packaging scripts
# (build_cross.sh, gen_winres.sh). Unlike lib.sh this installs no EXIT trap and
# starts no processes, so it is safe to source from scripts that manage their
# own trap:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/version.sh"

# parse_version_numeric VERSION
# Echoes "MAJOR MINOR PATCH": strips a leading "v" and any "-prerelease" suffix,
# splits on dots, and forces every missing or non-numeric component to 0 (e.g.
# when VERSION is a git short SHA on a local build).
parse_version_numeric() {
  local ver="${1#v}"; ver="${ver%%-*}"
  local maj min pat _rest
  IFS='.' read -r maj min pat _rest <<EOF
$ver
EOF
  case "${maj:-}" in *[!0-9]*|'') maj=0 ;; esac
  case "${min:-}" in *[!0-9]*|'') min=0 ;; esac
  case "${pat:-}" in *[!0-9]*|'') pat=0 ;; esac
  echo "$maj $min $pat"
}
