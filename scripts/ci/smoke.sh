#!/usr/bin/env bash
#
# Black-box HTTP smoke test for the nram server binary.
#
# Boots the compiled binary with NO embedding/inference providers configured and
# drives it over HTTP end to end: health -> setup -> create project -> store
# memory -> recall. With no embedder, recall exercises the keyword/recency
# fallback path rather than vector similarity.
#
# Backend is selected by the DATABASE_URL passed through to the server:
#   unset / empty -> SQLite (nram.db is created in the work dir, left intact)
#   set           -> Postgres
#
# Reused by two CI jobs:
#   - e2e-http:      run with DATABASE_URL set to drive the binary against Postgres.
#   - migration-e2e: run without DATABASE_URL to seed a real nram.db, which the
#                    migrate-to-postgres CLI then consumes (hence SMOKE_WORKDIR is
#                    honoured so the caller knows where nram.db lands).
#
# Requirements on PATH: bash, curl, jq. The binary must already be built.
#
# Environment overrides:
#   NRAM_BIN       path to the server binary   (default: <repo>/nram, what make build writes)
#   SMOKE_PORT     listen port                 (default: 18675)
#   SMOKE_WORKDIR  server working directory     (default: a fresh mktemp dir)
#   DATABASE_URL   Postgres DSN; empty => SQLite

set -euo pipefail

CI_PREFIX="smoke"
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${SMOKE_PORT:-18675}"
WORKDIR="${SMOKE_WORKDIR:-$(mktemp -d)}"
BASE="http://127.0.0.1:${PORT}"

ADMIN_EMAIL="admin@ci.local"
ADMIN_PASS="CiPassword123!"
MARKER="ci-smoke-$$-$(date +%s)"

EXPECT_BACKEND="sqlite"
[ -n "${DATABASE_URL:-}" ] && EXPECT_BACKEND="postgres"

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN (build it first)"
mkdir -p "$WORKDIR"

echo "smoke: backend=$EXPECT_BACKEND port=$PORT workdir=$WORKDIR"
start_server "$WORKDIR" "$PORT" "${DATABASE_URL:-}"
wait_for_ready "$BASE"

# --- Health ---------------------------------------------------------------
health="$(curl -fsS "$BASE/v1/health")"
echo "$health" | jq -e '.status == "ok"' >/dev/null \
  || fail "health status not ok: $health"
echo "$health" | jq -e --arg b "$EXPECT_BACKEND" '.backend == $b' >/dev/null \
  || fail "health backend != $EXPECT_BACKEND: $health"
echo "$health" | jq -e '.providers.embedding.status == "not_configured"' >/dev/null \
  || fail "embedding provider should be not_configured: $health"
echo "smoke: health ok (backend=$EXPECT_BACKEND, providers unconfigured)"

# --- Setup -> project -> memory -------------------------------------------
token="$(setup_admin "$BASE" "$ADMIN_EMAIL" "$ADMIN_PASS")"
echo "smoke: setup complete"

project_id="$(create_project "$BASE" "$token" "CI Smoke" "ci-smoke")"
echo "smoke: project created ($project_id)"

memory_id="$(store_memory "$BASE" "$token" "$project_id" "$MARKER recall smoke memory" '["ci","smoke"]')"
echo "smoke: memory stored ($memory_id)"

# --- Recall (keyword/recency fallback, no embedder) -----------------------
recall="$(curl -fsS -X POST "$BASE/v1/projects/$project_id/memories/recall" \
  -H "authorization: Bearer $token" -H 'content-type: application/json' \
  -d "{\"query\":\"$MARKER\",\"limit\":10}")"
echo "$recall" | jq -e --arg id "$memory_id" 'any(.memories[]?; .id == $id)' >/dev/null \
  || fail "recall did not return the stored memory ($memory_id): $recall"
echo "smoke: recall returned the stored memory"

echo "smoke: PASS (backend=$EXPECT_BACKEND, nram.db in $WORKDIR if sqlite)"
