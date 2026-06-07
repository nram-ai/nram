#!/usr/bin/env bash
#
# Shared helpers for the nram black-box CI scripts (smoke.sh, migrate_admin_e2e.sh).
# Source it after `set -euo pipefail`:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#
# Provides REPO_ROOT and BIN, a CI_PREFIX-tagged fail(), the server lifecycle
# (start_server / wait_for_ready with an EXIT-trap cleanup), and HTTP seeding
# helpers. Set CI_PREFIX before the first fail()/echo so log lines are tagged.
#
# The seeding helpers echo their result and are meant to be captured with
# $(...). That runs them in a subshell, but bash resets the EXIT trap in
# subshells, so a captured call never triggers the server cleanup.
#
# Requirements on PATH: bash, curl, jq.

# Resolve the repo root from this file's own location (scripts/ci/lib.sh).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${NRAM_BIN:-$REPO_ROOT/bin/nram}"
CI_PREFIX="${CI_PREFIX:-ci}"

fail() { echo "$CI_PREFIX: FAIL: $*" >&2; exit 1; }

# Server lifecycle. start_server records SERVER_PID; the EXIT trap terminates it.
SERVER_PID=""
_ci_cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap _ci_cleanup EXIT

# start_server WORKDIR PORT [DATABASE_URL]
# Launches the binary with cwd=WORKDIR (so SQLite's nram.db lands there) and no
# providers configured. DATABASE_URL empty => SQLite, set => Postgres.
start_server() {
  local workdir="$1" port="$2" dburl="${3:-}"
  (
    cd "$workdir"
    exec env PORT="$port" LOG_LEVEL=error DATABASE_URL="$dburl" "$BIN"
  ) &
  SERVER_PID=$!
}

# wait_for_ready BASE — polls BASE/v1/health for up to ~60s, failing if the
# server process dies first.
wait_for_ready() {
  local base="$1" ready=""
  for _ in $(seq 1 120); do
    kill -0 "$SERVER_PID" 2>/dev/null || fail "server exited during startup"
    if curl -fsS "$base/v1/health" >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.5
  done
  [ -n "$ready" ] || fail "server did not become ready within 60s"
}

# setup_admin BASE EMAIL PASS — completes first-admin setup, echoes bearer token.
setup_admin() {
  local base="$1" email="$2" pass="$3" resp token
  resp="$(curl -fsS -X POST "$base/v1/admin/setup" -H 'content-type: application/json' \
    -d "{\"email\":\"$email\",\"password\":\"$pass\"}")"
  token="$(echo "$resp" | jq -r '.token // empty')"
  [ -n "$token" ] || fail "no token from setup: $resp"
  echo "$token"
}

# create_project BASE TOKEN NAME SLUG — echoes the new project id.
create_project() {
  local base="$1" token="$2" name="$3" slug="$4" resp id
  resp="$(curl -fsS -X POST "$base/v1/me/projects" -H "authorization: Bearer $token" \
    -H 'content-type: application/json' -d "{\"name\":\"$name\",\"slug\":\"$slug\"}")"
  id="$(echo "$resp" | jq -r '.id // empty')"
  [ -n "$id" ] || fail "no project id: $resp"
  echo "$id"
}

# store_memory BASE TOKEN PROJECT_ID CONTENT [TAGS_JSON] — echoes the memory id.
# TAGS_JSON defaults to ["ci"].
store_memory() {
  local base="$1" token="$2" project_id="$3" content="$4" tags="${5:-[\"ci\"]}" resp id
  resp="$(curl -fsS -X POST "$base/v1/projects/$project_id/memories" -H "authorization: Bearer $token" \
    -H 'content-type: application/json' -d "{\"content\":\"$content\",\"tags\":$tags}")"
  id="$(echo "$resp" | jq -r '.id // empty')"
  [ -n "$id" ] || fail "no memory id: $resp"
  echo "$id"
}
