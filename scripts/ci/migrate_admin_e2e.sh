#!/usr/bin/env bash
#
# End-to-end test for the admin-driven SQLite -> Postgres migration
# (POST /v1/admin/database/migrate, backed by the DataMigrator).
#
# Boots the binary on a fresh SQLite database, completes setup, stores a memory,
# then asks the running server to migrate itself into the Postgres instance named
# by MIGRATE_TARGET_URL. The endpoint is asynchronous (returns 202 status="started"
# and streams progress over SSE), so this asserts the terminal db_migration.completed
# event arrives on /v1/events.
#
# Requirements on PATH: bash, curl, jq. The binary must already be built.
#
# Environment:
#   MIGRATE_TARGET_URL  (required) destination Postgres DSN, must be empty + have
#                       the pgvector extension enabled.
#   NRAM_BIN            path to the server binary (default: <repo>/nram, what make build writes)
#   SMOKE_PORT          listen port (default: 18686)

set -euo pipefail

CI_PREFIX="admin-migrate"
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PORT="${SMOKE_PORT:-18686}"
WORKDIR="$(mktemp -d)"
BASE="http://127.0.0.1:${PORT}"

ADMIN_EMAIL="admin@ci.local"
ADMIN_PASS="CiPassword123!"
MARKER="ci-admin-migrate-$$-$(date +%s)"

[ -n "${MIGRATE_TARGET_URL:-}" ] || fail "MIGRATE_TARGET_URL is required"
[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"

echo "admin-migrate: booting sqlite server in $WORKDIR (port $PORT)"
start_server "$WORKDIR" "$PORT" ""
wait_for_ready "$BASE"

# Seed: setup admin, create project, store a memory so there is data to migrate.
token="$(setup_admin "$BASE" "$ADMIN_EMAIL" "$ADMIN_PASS")"
project_id="$(create_project "$BASE" "$token" "CI Admin Migrate" "ci-admin-migrate")"
store_memory "$BASE" "$token" "$project_id" "$MARKER admin migrate memory" >/dev/null
echo "admin-migrate: seeded (project + memory)"

# The migrate endpoint is asynchronous: it returns 202 {"status":"started"} and
# streams the terminal result over SSE on /v1/events (scope db-migration). Open the
# stream *after* triggering, using a sentinel last_event_id so the replay buffer
# re-emits the terminal event even if the migration already finished (race-free).
resp="$(curl -fsS -X POST "$BASE/v1/admin/database/migrate" \
  -H "authorization: Bearer $token" -H 'content-type: application/json' \
  -d "{\"url\":\"$MIGRATE_TARGET_URL\"}")"
status="$(echo "$resp" | jq -r '.status // empty')"
echo "admin-migrate: endpoint returned status=$status"
[ "$status" = "started" ] || fail "expected status=started from async endpoint: $resp"

events_log="$WORKDIR/migration-events.sse"
curl -fsS --no-buffer --max-time 90 \
  -H "authorization: Bearer $token" \
  "$BASE/v1/events?scope=db-migration&last_event_id=ci-await-migration" \
  >"$events_log" 2>/dev/null &
sse_pid=$!

result=""
deadline=$((SECONDS + 75))
while [ "$SECONDS" -lt "$deadline" ]; do
  if grep -q '^event: db_migration.completed' "$events_log" 2>/dev/null; then
    result="complete"; break
  fi
  if grep -q '^event: db_migration.failed' "$events_log" 2>/dev/null; then
    result="failed"; break
  fi
  kill -0 "$sse_pid" 2>/dev/null || break
  sleep 0.5
done

kill "$sse_pid" 2>/dev/null || true
wait "$sse_pid" 2>/dev/null || true

if [ "$result" = "failed" ]; then
  fail "migration reported failure: $(grep '^data:' "$events_log" | tail -1)"
fi
[ "$result" = "complete" ] || fail "migration did not complete within timeout; events:
$(cat "$events_log" 2>/dev/null)"

echo "admin-migrate: PASS"
