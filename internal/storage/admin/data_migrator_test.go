package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/nram-ai/nram/internal/migration"
)

// openSQLiteInMemory creates an in-memory SQLite database with the full nram schema.
func openSQLiteInMemory(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	mg, err := migration.NewMigrator(db, "sqlite")
	if err != nil {
		t.Fatalf("create sqlite migrator: %v", err)
	}
	if err := mg.Up(); err != nil {
		t.Fatalf("apply sqlite migrations: %v", err)
	}
	_ = mg.Close()
	return db
}

// seedSQLite inserts comprehensive test rows covering every migrated table.
// Each table gets multiple rows testing: all-nullable-populated, all-nullable-NULL,
// special characters, unicode, edge cases.
func seedSQLite(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed: %s\nargs: %v\nerr: %v", query, args, err)
		}
	}

	// ── namespaces (root already inserted by migration) ────────────────────
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000001', 'TestOrg', 'testorg', 'organization',
		        '00000000-0000-0000-0000-000000000000', 'testorg', 1, '{"key":"value"}',
		        '2025-01-01T00:01:00Z', '2025-01-01T00:01:00Z')`)

	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000002', 'TestUser', 'testuser', 'user',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'testorg/testuser', 2, '{}',
		        '2025-01-01T00:02:00Z', '2025-01-01T00:02:00Z')`)

	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000003', 'TestProject', 'testproject', 'project',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'testorg/testproject', 2,
		        '2025-01-01T00:03:00Z', '2025-01-01T00:03:00Z')`)

	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000004', 'SecondOrg', 'secondorg', 'organization',
		        '00000000-0000-0000-0000-000000000000', 'secondorg', 1,
		        '2025-01-01T00:04:00Z', '2025-01-01T00:04:00Z')`)

	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, created_at, updated_at)
		VALUES ('aaaaaaaa-0000-0000-0000-000000000005', 'EmptyProject', 'emptyproject', 'project',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'testorg/emptyproject', 2,
		        '2025-01-01T00:05:00Z', '2025-01-01T00:05:00Z')`)

	// ── organizations ──────────────────────────────────────────────────────
	mustExec(`INSERT INTO organizations (id, namespace_id, name, slug, settings, created_at, updated_at)
		VALUES ('bbbbbbbb-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000001', 'TestOrg', 'testorg', '{}',
		        '2025-01-01T01:00:00Z', '2025-01-01T01:00:00Z')`)

	mustExec(`INSERT INTO organizations (id, namespace_id, name, slug, settings, created_at, updated_at)
		VALUES ('bbbbbbbb-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000004', 'SecondOrg', 'secondorg',
		        '{"feature_flags":{"beta":true}}',
		        '2025-01-01T01:01:00Z', '2025-01-01T01:01:00Z')`)

	// ── users ──────────────────────────────────────────────────────────────
	// User 1: all fields populated
	mustExec(`INSERT INTO users (id, email, display_name, password_hash, org_id, namespace_id, role,
	                             settings, last_login, disabled_at, created_at, updated_at)
		VALUES ('cccccccc-0000-0000-0000-000000000001',
		        'test@example.com', 'Test User', '$2a$10$hash',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002', 'administrator',
		        '{"theme":"dark"}', '2025-06-15T09:00:00Z', '2025-07-01T00:00:00Z',
		        '2025-01-02T00:00:00Z', '2025-01-02T00:00:00Z')`)

	// User 2: nullable fields NULL, Unicode display_name
	mustExec(`INSERT INTO users (id, email, display_name, org_id, namespace_id, role, created_at, updated_at)
		VALUES ('cccccccc-0000-0000-0000-000000000002',
		        'unicode@example.com', 'Ünïcödé Üser',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002', 'member',
		        '2025-01-02T00:01:00Z', '2025-01-02T00:01:00Z')`)

	// ── api_keys ───────────────────────────────────────────────────────────
	// Key 1: empty scopes, no last_used/expires_at
	mustExec(`INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes, created_at)
		VALUES ('dddddddd-0000-0000-0000-000000000001',
		        'cccccccc-0000-0000-0000-000000000001',
		        'nram_k_', 'hashvalue123', 'empty-scopes-key', '[]',
		        '2025-01-03T00:00:00Z')`)

	// Key 2: populated scopes (UUIDs), last_used and expires_at set
	mustExec(`INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at)
		VALUES ('dddddddd-0000-0000-0000-000000000002',
		        'cccccccc-0000-0000-0000-000000000001',
		        'nram_k_', 'hashvalue456', 'scoped-key',
		        '["eeeeeeee-0000-0000-0000-000000000001"]',
		        '2025-05-10T12:00:00Z', '2026-05-10T12:00:00Z',
		        '2025-01-03T00:01:00Z')`)

	// Key 3: multiple scope UUIDs
	mustExec(`INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes, created_at)
		VALUES ('dddddddd-0000-0000-0000-000000000003',
		        'cccccccc-0000-0000-0000-000000000002',
		        'nram_k_', 'hashvalue789', 'multi-scope-key',
		        '["eeeeeeee-0000-0000-0000-000000000001","eeeeeeee-0000-0000-0000-000000000002"]',
		        '2025-01-03T00:02:00Z')`)

	// ── projects ───────────────────────────────────────────────────────────
	// Project 1: tags with special chars, description set
	mustExec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, default_tags, created_at, updated_at)
		VALUES ('eeeeeeee-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000001',
		        'TestProject', 'testproject', 'A project for testing',
		        '["tag with spaces","tag-with-dashes","日本語"]',
		        '2025-01-04T00:00:00Z', '2025-01-04T00:00:00Z')`)

	// Project 2: no description, empty tags
	mustExec(`INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, default_tags, created_at, updated_at)
		VALUES ('eeeeeeee-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000005',
		        'aaaaaaaa-0000-0000-0000-000000000001',
		        'EmptyProject', 'emptyproject', '[]',
		        '2025-01-04T00:01:00Z', '2025-01-04T00:01:00Z')`)

	// ── settings ───────────────────────────────────────────────────────────
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('string_val', '"hello"', 'global', '2025-01-05T00:00:00Z')`)
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('number_val', '42', 'global', '2025-01-05T00:01:00Z')`)
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('object_val', '{"nested":true}', 'global', '2025-01-05T00:02:00Z')`)
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('array_val', '[1,2,3]', 'global', '2025-01-05T00:03:00Z')`)
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('bool_val', 'true', 'global', '2025-01-05T00:04:00Z')`)
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('null_val', 'null', 'global', '2025-01-05T00:05:00Z')`)
	mustExec(`INSERT INTO settings (key, value, scope, updated_at) VALUES ('org_setting', '"org_value"', 'org:bbbbbbbb-0000-0000-0000-000000000001', '2025-01-05T00:06:00Z')`)

	// ── system_meta ────────────────────────────────────────────────────────
	mustExec(`INSERT OR IGNORE INTO system_meta (key, value, created_at, updated_at) VALUES ('test_meta', 'value1', '2025-01-06T00:00:00Z', '2025-01-06T00:00:00Z')`)
	mustExec(`INSERT OR IGNORE INTO system_meta (key, value, created_at, updated_at) VALUES ('another_meta', 'value2', '2025-01-06T00:01:00Z', '2025-01-06T00:01:00Z')`)

	// ── memories ───────────────────────────────────────────────────────────
	// Memory 1: all nullable fields set, enriched=1, long content with Unicode
	mustExec(`INSERT INTO memories (id, namespace_id, content, embedding_dim, source, tags,
	                                confidence, importance, access_count, last_accessed, expires_at,
	                                superseded_by, enriched, metadata, created_at, updated_at,
	                                deleted_at, purge_after)
		VALUES ('ffffffff-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'A comprehensive test memory with Unicode: 你好世界 and symbols: ñ é ü ö',
		        384, 'api',
		        '["tag1","tag2","alpha","beta","gamma"]',
		        0.95, 0.8, 5,
		        '2025-03-15T10:30:00Z', '2026-12-31T23:59:59Z',
		        NULL, 1,
		        '{"source_file":"test.md","line":42}',
		        '2025-01-15T10:30:00Z', '2025-03-15T10:30:00Z',
		        '2025-06-01T00:00:00Z', '2025-09-01T00:00:00Z')`)

	// Memory 2: all nullable fields NULL, enriched=0, empty tags
	mustExec(`INSERT INTO memories (id, namespace_id, content, tags, confidence, importance,
	                                created_at, updated_at)
		VALUES ('ffffffff-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Minimal memory', '[]', 0.5, 0.3,
		        '2025-02-01T08:00:00Z', '2025-02-01T08:00:00Z')`)

	// Memory 3: special chars: newlines, quotes, backslashes, emoji
	mustExec(`INSERT INTO memories (id, namespace_id, content, tags, confidence, importance,
	                                created_at, updated_at)
		VALUES ('ffffffff-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Line1' || char(10) || 'Line2 with "quotes" and back\slash and emoji 🎉',
		        '["special"]', 0.7, 0.6,
		        '2025-03-01T09:00:00Z', '2025-03-01T09:00:00Z')`)

	// Set superseded_by on memory 3 to point to memory 1 (which was inserted first,
	// so FK order is satisfied during migration since rows are scanned by id).
	mustExec(`UPDATE memories SET superseded_by = 'ffffffff-0000-0000-0000-000000000001'
		WHERE id = 'ffffffff-0000-0000-0000-000000000003'`)

	// ── entities ───────────────────────────────────────────────────────────
	// Entity 1: with properties and metadata
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, properties, metadata,
	                                created_at, updated_at)
		VALUES ('11111111-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Alice', 'alice', 'person',
		        '{"role":"antagonist","age":42}',
		        '{"source":"manual"}',
		        '2025-01-15T10:30:00Z', '2025-01-15T10:30:00Z')`)

	// Entity 2: without metadata, Unicode name
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type,
	                                created_at, updated_at)
		VALUES ('11111111-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Böb Müller', 'bob-muller', 'person',
		        '2025-01-16T10:30:00Z', '2025-01-16T10:30:00Z')`)

	// Entity 3: with embedding_dim
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim,
	                                created_at, updated_at)
		VALUES ('11111111-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'Acme Corp', 'acme-corp', 'organization', 768,
		        '2025-01-17T10:30:00Z', '2025-01-17T10:30:00Z')`)

	// ── entity_aliases ─────────────────────────────────────────────────────
	mustExec(`INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type, created_at)
		VALUES ('22222222-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000001', 'Ali', 'nickname',
		        '2025-01-15T10:30:00Z')`)

	mustExec(`INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type, created_at)
		VALUES ('22222222-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000001', 'Алиса', 'translation',
		        '2025-01-16T10:30:00Z')`)

	mustExec(`INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type, created_at)
		VALUES ('22222222-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000002', 'Bob', 'shortname',
		        '2025-01-16T10:30:00Z')`)

	// ── relationships ──────────────────────────────────────────────────────
	// Relationship 1: all nullable fields populated
	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight,
	                                     properties, valid_from, valid_until, source_memory, created_at)
		VALUES ('33333333-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000001',
		        '11111111-0000-0000-0000-000000000002',
		        'knows', 0.85,
		        '{"since":"2020"}',
		        '2025-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
		        'ffffffff-0000-0000-0000-000000000001',
		        '2025-01-15T10:30:00Z')`)

	// Relationship 2: nullable fields NULL, default weight
	mustExec(`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation,
	                                     valid_from, created_at)
		VALUES ('33333333-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000002',
		        '11111111-0000-0000-0000-000000000003',
		        'works_at',
		        '2025-01-16T00:00:00Z',
		        '2025-01-16T10:30:00Z')`)

	// ── memory_lineage ─────────────────────────────────────────────────────
	// Lineage 1: parent_id NULL (root origin)
	mustExec(`INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context, created_at)
		VALUES ('44444444-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'ffffffff-0000-0000-0000-000000000001', NULL, 'origin', '{}',
		        '2025-01-15T10:30:00Z')`)

	// Lineage 2: parent_id set, context JSON
	mustExec(`INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context, created_at)
		VALUES ('44444444-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'ffffffff-0000-0000-0000-000000000002',
		        'ffffffff-0000-0000-0000-000000000001',
		        'derived', '{"reason":"summarized"}',
		        '2025-02-01T08:00:00Z')`)

	// ── ingestion_log ──────────────────────────────────────────────────────
	// Log 1: multiple memory_ids, content_hash set
	mustExec(`INSERT INTO ingestion_log (id, namespace_id, source, content_hash, raw_content,
	                                     memory_ids, status, created_at)
		VALUES ('55555555-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'api', 'sha256:abc123', 'raw content here',
		        '["ffffffff-0000-0000-0000-000000000001","ffffffff-0000-0000-0000-000000000002"]',
		        'completed',
		        '2025-01-15T10:30:00Z')`)

	// Log 2: error status with error text
	mustExec(`INSERT INTO ingestion_log (id, namespace_id, source, raw_content,
	                                     memory_ids, status, error, created_at)
		VALUES ('55555555-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'file', 'failed raw content',
		        '[]',
		        'error', 'parsing failed: unexpected token',
		        '2025-01-16T10:30:00Z')`)

	// Log 3: single memory_id, no content_hash
	mustExec(`INSERT INTO ingestion_log (id, namespace_id, source, raw_content,
	                                     memory_ids, status, created_at)
		VALUES ('55555555-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'webhook', 'webhook payload',
		        '["ffffffff-0000-0000-0000-000000000003"]',
		        'completed',
		        '2025-01-17T10:30:00Z')`)

	// ── enrichment_queue ───────────────────────────────────────────────────
	// Queue 1: pending, unclaimed
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, steps_completed,
	                                        created_at, updated_at)
		VALUES ('66666666-0000-0000-0000-000000000001',
		        'ffffffff-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'pending', '[]',
		        '2025-01-15T10:30:00Z', '2025-01-15T10:30:00Z')`)

	// Queue 2: claimed with steps completed
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, claimed_at,
	                                        claimed_by, steps_completed, created_at, updated_at)
		VALUES ('66666666-0000-0000-0000-000000000002',
		        'ffffffff-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'processing', '2025-02-01T09:00:00Z', 'worker-1',
		        '["embedding","entity_extraction"]',
		        '2025-02-01T08:00:00Z', '2025-02-01T09:00:00Z')`)

	// Queue 3: completed with last_error from a previous attempt
	mustExec(`INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, attempts,
	                                        last_error, steps_completed, completed_at,
	                                        created_at, updated_at)
		VALUES ('66666666-0000-0000-0000-000000000003',
		        'ffffffff-0000-0000-0000-000000000003',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'completed', 2,
		        'timeout on first attempt',
		        '["embedding","entity_extraction","summarization"]',
		        '2025-03-01T10:00:00Z',
		        '2025-03-01T09:00:00Z', '2025-03-01T10:00:00Z')`)

	// ── webhooks ───────────────────────────────────────────────────────────
	// Webhook 1: active with secret, last_fired and last_status set
	mustExec(`INSERT INTO webhooks (id, url, secret, events, scope, active, last_fired, last_status,
	                                failure_count, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000001',
		        'https://example.com/hook', 'whsec_supersecret',
		        '["memory.created","memory.deleted"]', 'global', 1,
		        '2025-06-01T12:00:00Z', 200, 0,
		        '2025-01-15T10:30:00Z', '2025-06-01T12:00:00Z')`)

	// Webhook 2: inactive, no secret, no last_fired
	mustExec(`INSERT INTO webhooks (id, url, events, scope, active, created_at, updated_at)
		VALUES ('77777777-0000-0000-0000-000000000002',
		        'https://other.example.com/hook',
		        '["memory.updated"]', 'ns:aaaaaaaa-0000-0000-0000-000000000002', 0,
		        '2025-02-01T10:30:00Z', '2025-02-01T10:30:00Z')`)

	// ── memory_shares ──────────────────────────────────────────────────────
	// Share 1: all nullable fields set
	mustExec(`INSERT INTO memory_shares (id, source_ns_id, target_ns_id, permission, created_by,
	                                     expires_at, revoked_at, created_at)
		VALUES ('88888888-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000001',
		        'recall', 'cccccccc-0000-0000-0000-000000000001',
		        '2026-12-31T23:59:59Z', '2025-08-01T00:00:00Z',
		        '2025-01-15T10:30:00Z')`)

	// Share 2: all nullable fields NULL
	mustExec(`INSERT INTO memory_shares (id, source_ns_id, target_ns_id, permission, created_at)
		VALUES ('88888888-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'read',
		        '2025-02-01T10:30:00Z')`)

	// ── token_usage ────────────────────────────────────────────────────────
	// Token usage 1: all nullable FKs set
	mustExec(`INSERT INTO token_usage (id, org_id, user_id, project_id, namespace_id, operation,
	                                   provider, model, tokens_input, tokens_output, memory_id,
	                                   api_key_id, latency_ms, created_at)
		VALUES ('aabbccdd-0000-0000-0000-000000000001',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'cccccccc-0000-0000-0000-000000000001',
		        'eeeeeeee-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'embed', 'openai', 'text-embedding-3-small',
		        150, 0,
		        'ffffffff-0000-0000-0000-000000000001',
		        'dddddddd-0000-0000-0000-000000000001',
		        42,
		        '2025-03-15T10:30:00Z')`)

	// Token usage 2: nullable FKs NULL
	mustExec(`INSERT INTO token_usage (id, namespace_id, operation, provider, model,
	                                   tokens_input, tokens_output, created_at)
		VALUES ('aabbccdd-0000-0000-0000-000000000002',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'recall', 'anthropic', 'claude-3-opus',
		        200, 500,
		        '2025-03-16T10:30:00Z')`)

	// Token usage 3: partial FKs
	mustExec(`INSERT INTO token_usage (id, org_id, namespace_id, operation, provider, model,
	                                   tokens_input, tokens_output, latency_ms, created_at)
		VALUES ('aabbccdd-0000-0000-0000-000000000003',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'aaaaaaaa-0000-0000-0000-000000000002',
		        'enrich', 'openai', 'gpt-4o',
		        300, 600, 1250,
		        '2025-03-17T10:30:00Z')`)

	// ── oauth_clients ──────────────────────────────────────────────────────
	// Client 1: with secret and org_id, auto_registered=0
	mustExec(`INSERT INTO oauth_clients (id, client_id, client_secret, name, redirect_uris,
	                                     grant_types, org_id, auto_registered, created_at)
		VALUES ('99999999-0000-0000-0000-000000000001',
		        'test-client-id', 'secret123', 'Test App',
		        '["https://app.example.com/callback","https://app.example.com/callback2"]',
		        '["authorization_code","refresh_token"]',
		        'bbbbbbbb-0000-0000-0000-000000000001', 0,
		        '2025-01-15T10:30:00Z')`)

	// Client 2: no secret, no org_id, auto_registered=1
	mustExec(`INSERT INTO oauth_clients (id, client_id, name, redirect_uris, grant_types,
	                                     auto_registered, created_at)
		VALUES ('99999999-0000-0000-0000-000000000002',
		        'auto-client-id', 'Auto Client',
		        '["https://dynamic.example.com/callback"]',
		        '["authorization_code"]',
		        1,
		        '2025-02-01T10:30:00Z')`)

	// ── oauth_authorization_codes ──────────────────────────────────────────
	// Code 1: with PKCE and resource
	mustExec(`INSERT INTO oauth_authorization_codes
		(code, client_id, user_id, redirect_uri, scope, code_challenge,
		 code_challenge_method, expires_at, created_at, resource)
		VALUES ('testcode123', 'test-client-id',
		        'cccccccc-0000-0000-0000-000000000001',
		        'https://app.example.com/callback', 'read write',
		        'E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM', 'S256',
		        '2025-06-01T12:10:00Z', '2025-06-01T12:00:00Z',
		        'https://api.example.com/')`)

	// Code 2: without PKCE, without resource
	mustExec(`INSERT INTO oauth_authorization_codes
		(code, client_id, user_id, redirect_uri, scope, expires_at, created_at)
		VALUES ('testcode456', 'auto-client-id',
		        'cccccccc-0000-0000-0000-000000000002',
		        'https://dynamic.example.com/callback', 'read',
		        '2025-06-02T12:10:00Z', '2025-06-02T12:00:00Z')`)

	// ── oauth_refresh_tokens ───────────────────────────────────────────────
	// Token 1: with expires_at, not revoked
	mustExec(`INSERT INTO oauth_refresh_tokens
		(token_hash, client_id, user_id, scope, expires_at, created_at)
		VALUES ('refreshhash123', 'test-client-id',
		        'cccccccc-0000-0000-0000-000000000001', 'read write',
		        '2026-06-01T12:00:00Z',
		        '2025-06-01T12:00:00Z')`)

	// Token 2: revoked, no expires_at
	mustExec(`INSERT INTO oauth_refresh_tokens
		(token_hash, client_id, user_id, scope, revoked_at, created_at)
		VALUES ('refreshhash456', 'test-client-id',
		        'cccccccc-0000-0000-0000-000000000001', 'read',
		        '2025-07-01T00:00:00Z',
		        '2025-06-15T12:00:00Z')`)

	// ── oauth_idp_configs ──────────────────────────────────────────────────
	// Config 1: with issuer_url, auto_provision=1
	mustExec(`INSERT INTO oauth_idp_configs (id, org_id, provider_type, client_id, client_secret,
	                                         issuer_url, allowed_domains, auto_provision,
	                                         default_role, created_at, updated_at)
		VALUES ('aaaaaaaa-1111-0000-0000-000000000001',
		        'bbbbbbbb-0000-0000-0000-000000000001',
		        'google', 'google-client-id', 'google-client-secret',
		        'https://accounts.google.com',
		        '["example.com","test.com"]', 1, 'member',
		        '2025-01-15T10:30:00Z', '2025-01-15T10:30:00Z')`)

	// Config 2: without issuer_url, auto_provision=0, single domain
	mustExec(`INSERT INTO oauth_idp_configs (id, org_id, provider_type, client_id, client_secret,
	                                         allowed_domains, auto_provision, default_role,
	                                         created_at, updated_at)
		VALUES ('aaaaaaaa-1111-0000-0000-000000000002',
		        'bbbbbbbb-0000-0000-0000-000000000002',
		        'github', 'github-client-id', 'github-client-secret',
		        '["github.example.com"]', 0, 'readonly',
		        '2025-02-01T10:30:00Z', '2025-02-01T10:30:00Z')`)
}

// cleanPostgres truncates all migrated tables in reverse dependency order so
// the test can be run multiple times without leftover data.
func cleanPostgres(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	// Reverse of migratedTables order to respect FK constraints.
	tables := []string{
		"dream_project_dirty",
		"dream_log_summaries",
		"dream_logs",
		"dream_cycles",
		"webauthn_credentials",
		"oauth_idp_configs",
		"oauth_refresh_tokens",
		"oauth_authorization_codes",
		"oauth_clients",
		"token_usage",
		"memory_shares",
		"webhooks",
		"enrichment_queue",
		"ingestion_log",
		"memory_lineage",
		"relationships",
		"entity_aliases",
		"entities",
		"memories",
		"system_meta",
		"settings",
		"projects",
		"api_keys",
		"users",
		"organizations",
		"namespaces",
	}
	for _, table := range tables {
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table+" CASCADE"); err != nil {
			t.Logf("cleanup truncate %s: %v", table, err)
		}
	}
	// Re-insert the root namespace that the migrations expect.
	_, _ = db.ExecContext(ctx, `
		INSERT INTO namespaces (id, name, slug, kind, path, depth)
		VALUES ('00000000-0000-0000-0000-000000000000', 'root', 'root', 'root', '', 0)
		ON CONFLICT DO NOTHING
	`)
}

// verifyRows is a test helper that queries all rows from a Postgres table and
// compares them field-by-field against expected values. Each expected row is a
// map of column-name to expected value. Values are compared as strings after
// normalization (NULL -> "<NULL>", bool -> "true"/"false", etc.).
func verifyRows(t *testing.T, pgDB *sql.DB, table string, expectedRows []map[string]interface{}) {
	t.Helper()
	ctx := context.Background()

	rows, err := pgDB.QueryContext(ctx, "SELECT * FROM "+table+" ORDER BY 1")
	if err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns %s: %v", table, err)
	}

	var actualRows []map[string]string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		row := make(map[string]string, len(cols))
		for i, col := range cols {
			row[col] = pgValToString(vals[i])
		}
		actualRows = append(actualRows, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error %s: %v", table, err)
	}

	if len(actualRows) != len(expectedRows) {
		t.Errorf("%s: expected %d rows, got %d", table, len(expectedRows), len(actualRows))
		return
	}

	for i, expected := range expectedRows {
		if i >= len(actualRows) {
			break
		}
		actual := actualRows[i]
		for col, wantVal := range expected {
			wantStr := expectedValToString(wantVal)
			gotStr, ok := actual[col]
			if !ok {
				t.Errorf("%s row %d: column %q not found in result", table, i, col)
				continue
			}
			if !valuesMatch(wantStr, gotStr) {
				t.Errorf("%s row %d col %q:\n  want: %s\n  got:  %s", table, i, col, wantStr, gotStr)
			}
		}
	}
}

// pgValToString normalizes a Postgres scan result to a comparable string.
func pgValToString(v interface{}) string {
	if v == nil {
		return "<NULL>"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		// Avoid trailing zeros for clean comparison.
		s := fmt.Sprintf("%g", val)
		return s
	case []byte:
		return string(val)
	case time.Time:
		return val.UTC().Format("2006-01-02T15:04:05Z")
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// expectedValToString normalizes an expected test value to a comparable string.
func expectedValToString(v interface{}) string {
	if v == nil {
		return "<NULL>"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// valuesMatch compares want vs got strings, handling timestamp format differences
// and Postgres array format differences.
func valuesMatch(want, got string) bool {
	if want == "<ANY>" {
		return true
	}
	if want == got {
		return true
	}
	// Postgres returns timestamps as time.Time which we format as 2006-01-02T15:04:05Z.
	// Normalize both sides.
	wantNorm := normalizeTimestamp(want)
	gotNorm := normalizeTimestamp(got)
	if wantNorm == gotNorm {
		return true
	}
	// Compare JSON values for equivalence.
	if jsonEquivalent(want, got) {
		return true
	}
	// Postgres arrays: {a,b} format.
	if strings.HasPrefix(want, "{") && strings.HasPrefix(got, "{") {
		return normalizeArray(want) == normalizeArray(got)
	}
	return false
}

func normalizeTimestamp(s string) string {
	// Try to parse as time and re-format.
	for _, layout := range []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05+00",
		"2006-01-02 15:04:05.000000+00",
		time.RFC3339,
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format("2006-01-02T15:04:05Z")
		}
	}
	return s
}

func jsonEquivalent(a, b string) bool {
	var va, vb interface{}
	if json.Unmarshal([]byte(a), &va) != nil {
		return false
	}
	if json.Unmarshal([]byte(b), &vb) != nil {
		return false
	}
	ja, _ := json.Marshal(va)
	jb, _ := json.Marshal(vb)
	return string(ja) == string(jb)
}

func normalizeArray(s string) string {
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.Trim(parts[i], `"`)
	}
	return strings.Join(parts, ",")
}

func TestDataMigrator_SQLiteToPostgres(t *testing.T) {
	ctx := context.Background()

	// Build source SQLite database with test data.
	srcDB := openSQLiteInMemory(t)
	defer srcDB.Close()
	seedSQLite(t, srcDB)

	// Open the pgvector-capable Postgres target and clean it before the test.
	pgDB, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer pgDB.Close()
	cleanPostgres(t, pgDB)
	pgDB.Close() // DataMigrator will open its own connection.

	// Run the migration.
	dm, err := newDataMigrator(ctx, srcDB, resolvedPostgresURL)
	if err != nil {
		t.Fatalf("newDataMigrator: %v", err)
	}
	defer dm.Close()

	if err := dm.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Reopen Postgres for verification.
	pgConn, err := sql.Open("pgx", resolvedPostgresURL)
	if err != nil {
		t.Fatalf("open postgres for verification: %v", err)
	}
	defer pgConn.Close()

	// ── Row count verification ─────────────────────────────────────────────
	t.Run("row_counts", func(t *testing.T) {
		for _, table := range migratedTables {
			var srcCount, dstCount int
			if err := srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&srcCount); err != nil {
				t.Logf("  SKIP %s (not in sqlite): %v", table, err)
				continue
			}
			if err := pgConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&dstCount); err != nil {
				t.Errorf("  FAIL count %s in postgres: %v", table, err)
				continue
			}
			expectedDst := srcCount
			if table == "system_meta" {
				expectedDst = srcCount + 1 // storage_backend marker
			}
			if dstCount != expectedDst {
				t.Errorf("%s: sqlite=%d postgres=%d (expected %d)", table, srcCount, dstCount, expectedDst)
			}
		}
	})

	// ── Storage backend marker ─────────────────────────────────────────────
	t.Run("storage_backend_marker", func(t *testing.T) {
		var val string
		if err := pgConn.QueryRowContext(ctx,
			"SELECT value FROM system_meta WHERE key = 'storage_backend'",
		).Scan(&val); err != nil {
			t.Fatalf("read storage_backend: %v", err)
		}
		if val != "postgres" {
			t.Errorf("storage_backend = %q, want %q", val, "postgres")
		}
	})

	// ── Per-table content verification ─────────────────────────────────────

	t.Run("namespaces", func(t *testing.T) {
		// Skip root namespace (inserted by migration with dynamic timestamps).
		// Verify 5 seeded rows ordered by id.
		verifyRows(t, pgConn, "namespaces WHERE id != '00000000-0000-0000-0000-000000000000'", []map[string]interface{}{
			{
				"id": "aaaaaaaa-0000-0000-0000-000000000001", "name": "TestOrg", "slug": "testorg",
				"kind": "organization", "parent_id": "00000000-0000-0000-0000-000000000000",
				"path": "testorg", "depth": int(1), "metadata": `{"key":"value"}`,
				"created_at": "2025-01-01T00:01:00Z", "updated_at": "2025-01-01T00:01:00Z",
			},
			{
				"id": "aaaaaaaa-0000-0000-0000-000000000002", "name": "TestUser", "slug": "testuser",
				"kind": "user", "parent_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"path": "testorg/testuser", "depth": int(2), "metadata": `{}`,
				"created_at": "2025-01-01T00:02:00Z", "updated_at": "2025-01-01T00:02:00Z",
			},
			{
				"id": "aaaaaaaa-0000-0000-0000-000000000003", "name": "TestProject", "slug": "testproject",
				"kind": "project", "parent_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"path": "testorg/testproject", "depth": int(2), "metadata": `{}`,
				"created_at": "2025-01-01T00:03:00Z", "updated_at": "2025-01-01T00:03:00Z",
			},
			{
				"id": "aaaaaaaa-0000-0000-0000-000000000004", "name": "SecondOrg", "slug": "secondorg",
				"kind": "organization", "parent_id": "00000000-0000-0000-0000-000000000000",
				"path": "secondorg", "depth": int(1), "metadata": `{}`,
				"created_at": "2025-01-01T00:04:00Z", "updated_at": "2025-01-01T00:04:00Z",
			},
			{
				"id": "aaaaaaaa-0000-0000-0000-000000000005", "name": "EmptyProject", "slug": "emptyproject",
				"kind": "project", "parent_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"path": "testorg/emptyproject", "depth": int(2), "metadata": `{}`,
				"created_at": "2025-01-01T00:05:00Z", "updated_at": "2025-01-01T00:05:00Z",
			},
		})
		// Verify root namespace parent_id is NULL.
		var parentID sql.NullString
		pgConn.QueryRowContext(ctx,
			"SELECT parent_id FROM namespaces WHERE id = $1",
			"00000000-0000-0000-0000-000000000000",
		).Scan(&parentID)
		if parentID.Valid {
			t.Errorf("root namespace parent_id should be NULL, got %q", parentID.String)
		}
	})

	t.Run("organizations", func(t *testing.T) {
		verifyRows(t, pgConn, "organizations", []map[string]interface{}{
			{
				"id": "bbbbbbbb-0000-0000-0000-000000000001", "namespace_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"name": "TestOrg", "slug": "testorg", "settings": `{}`,
				"created_at": "2025-01-01T01:00:00Z", "updated_at": "2025-01-01T01:00:00Z",
			},
			{
				"id": "bbbbbbbb-0000-0000-0000-000000000002", "namespace_id": "aaaaaaaa-0000-0000-0000-000000000004",
				"name": "SecondOrg", "slug": "secondorg", "settings": `{"feature_flags":{"beta":true}}`,
				"created_at": "2025-01-01T01:01:00Z", "updated_at": "2025-01-01T01:01:00Z",
			},
		})
	})

	t.Run("users", func(t *testing.T) {
		verifyRows(t, pgConn, "users", []map[string]interface{}{
			{
				"id": "cccccccc-0000-0000-0000-000000000001", "email": "test@example.com",
				"display_name": "Test User", "password_hash": "$2a$10$hash",
				"org_id": "bbbbbbbb-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"role": "administrator", "settings": `{"theme":"dark"}`,
				"last_login": "2025-06-15T09:00:00Z", "disabled_at": "2025-07-01T00:00:00Z",
				"created_at": "2025-01-02T00:00:00Z", "updated_at": "2025-01-02T00:00:00Z",
			},
			{
				"id": "cccccccc-0000-0000-0000-000000000002", "email": "unicode@example.com",
				"display_name": "Ünïcödé Üser", "password_hash": nil,
				"org_id": "bbbbbbbb-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"role": "member", "settings": `{}`,
				"last_login": nil, "disabled_at": nil,
				"created_at": "2025-01-02T00:01:00Z", "updated_at": "2025-01-02T00:01:00Z",
			},
		})
	})

	t.Run("api_keys", func(t *testing.T) {
		verifyRows(t, pgConn, "api_keys", []map[string]interface{}{
			{
				"id": "dddddddd-0000-0000-0000-000000000001",
				"user_id": "cccccccc-0000-0000-0000-000000000001",
				"key_prefix": "nram_k_", "key_hash": "hashvalue123",
				"name": "empty-scopes-key", "scopes": "{}",
				"last_used": nil, "expires_at": nil,
				"created_at": "2025-01-03T00:00:00Z",
			},
			{
				"id": "dddddddd-0000-0000-0000-000000000002",
				"user_id": "cccccccc-0000-0000-0000-000000000001",
				"key_prefix": "nram_k_", "key_hash": "hashvalue456",
				"name": "scoped-key",
				"scopes": `{"eeeeeeee-0000-0000-0000-000000000001"}`,
				"last_used": "2025-05-10T12:00:00Z", "expires_at": "2026-05-10T12:00:00Z",
				"created_at": "2025-01-03T00:01:00Z",
			},
			{
				"id": "dddddddd-0000-0000-0000-000000000003",
				"user_id": "cccccccc-0000-0000-0000-000000000002",
				"key_prefix": "nram_k_", "key_hash": "hashvalue789",
				"name": "multi-scope-key",
				"scopes": `{"eeeeeeee-0000-0000-0000-000000000001","eeeeeeee-0000-0000-0000-000000000002"}`,
				"last_used": nil, "expires_at": nil,
				"created_at": "2025-01-03T00:02:00Z",
			},
		})
	})

	t.Run("projects", func(t *testing.T) {
		verifyRows(t, pgConn, "projects", []map[string]interface{}{
			{
				"id": "eeeeeeee-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000003",
				"owner_namespace_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"name": "TestProject", "slug": "testproject",
				"description": "A project for testing",
				"default_tags": `{"tag with spaces","tag-with-dashes","日本語"}`,
				"settings": "{}",
				"created_at": "2025-01-04T00:00:00Z", "updated_at": "2025-01-04T00:00:00Z",
			},
			{
				"id": "eeeeeeee-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000005",
				"owner_namespace_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"name": "EmptyProject", "slug": "emptyproject",
				"description": "",
				"default_tags": "{}",
				"settings": "{}",
				"created_at": "2025-01-04T00:01:00Z", "updated_at": "2025-01-04T00:01:00Z",
			},
		})
	})

	t.Run("settings", func(t *testing.T) {
		verifyRows(t, pgConn, "settings", []map[string]interface{}{
			{"key": "array_val", "value": "[1,2,3]", "scope": "global", "updated_by": nil, "updated_at": "2025-01-05T00:03:00Z"},
			{"key": "bool_val", "value": "true", "scope": "global", "updated_by": nil, "updated_at": "2025-01-05T00:04:00Z"},
			{"key": "null_val", "value": "null", "scope": "global", "updated_by": nil, "updated_at": "2025-01-05T00:05:00Z"},
			{"key": "number_val", "value": "42", "scope": "global", "updated_by": nil, "updated_at": "2025-01-05T00:01:00Z"},
			{"key": "object_val", "value": `{"nested":true}`, "scope": "global", "updated_by": nil, "updated_at": "2025-01-05T00:02:00Z"},
			{"key": "org_setting", "value": `"org_value"`, "scope": "org:bbbbbbbb-0000-0000-0000-000000000001", "updated_by": nil, "updated_at": "2025-01-05T00:06:00Z"},
			{"key": "string_val", "value": `"hello"`, "scope": "global", "updated_by": nil, "updated_at": "2025-01-05T00:00:00Z"},
		})
	})

	t.Run("system_meta", func(t *testing.T) {
		// Filter to only seeded rows (exclude storage_backend inserted by migrator).
		verifyRows(t, pgConn, "system_meta WHERE key NOT IN ('storage_backend')", []map[string]interface{}{
			{"key": "another_meta", "value": "value2", "created_at": "2025-01-06T00:01:00Z", "updated_at": "2025-01-06T00:01:00Z"},
			{"key": "test_meta", "value": "value1", "created_at": "2025-01-06T00:00:00Z", "updated_at": "2025-01-06T00:00:00Z"},
		})
	})

	t.Run("memories", func(t *testing.T) {
		verifyRows(t, pgConn, "memories", []map[string]interface{}{
			{
				"id":            "ffffffff-0000-0000-0000-000000000001",
				"namespace_id":  "aaaaaaaa-0000-0000-0000-000000000002",
				"content":       "A comprehensive test memory with Unicode: 你好世界 and symbols: ñ é ü ö",
				"embedding_dim": int(384), "source": "api",
				"tags":          `{"tag1","tag2","alpha","beta","gamma"}`,
				"confidence":    0.95, "importance": 0.8,
				"access_count":  int(5),
				"last_accessed": "2025-03-15T10:30:00Z",
				"expires_at":    "2026-12-31T23:59:59Z",
				"superseded_by": nil,
				"enriched":      true,
				"metadata":      `{"source_file":"test.md","line":42}`,
				"created_at":    "2025-01-15T10:30:00Z",
				"updated_at":    "2025-03-15T10:30:00Z",
				"deleted_at":    "2025-06-01T00:00:00Z",
				"purge_after":   "2025-09-01T00:00:00Z",
			},
			{
				"id":            "ffffffff-0000-0000-0000-000000000002",
				"namespace_id":  "aaaaaaaa-0000-0000-0000-000000000002",
				"content":       "Minimal memory",
				"embedding_dim": nil, "source": nil,
				"tags":          "{}",
				"confidence":    0.5, "importance": 0.3,
				"access_count":  int(0),
				"last_accessed": nil, "expires_at": nil, "superseded_by": nil,
				"enriched":      false,
				"metadata":      "{}",
				"created_at":    "2025-02-01T08:00:00Z",
				"updated_at":    "2025-02-01T08:00:00Z",
				"deleted_at":    nil, "purge_after": nil,
			},
			{
				"id":            "ffffffff-0000-0000-0000-000000000003",
				"namespace_id":  "aaaaaaaa-0000-0000-0000-000000000002",
				"content":       "Line1\nLine2 with \"quotes\" and back\\slash and emoji 🎉",
				"embedding_dim": nil, "source": nil,
				"tags":          `{"special"}`,
				"confidence":    0.7, "importance": 0.6,
				"access_count":  int(0),
				"last_accessed": nil, "expires_at": nil,
				"superseded_by": "ffffffff-0000-0000-0000-000000000001",
				"enriched":      false,
				"metadata":      "{}",
				"created_at":    "2025-03-01T09:00:00Z",
				"updated_at":    "2025-03-01T09:00:00Z",
				"deleted_at":    nil, "purge_after": nil,
			},
		})
	})

	t.Run("entities", func(t *testing.T) {
		verifyRows(t, pgConn, "entities", []map[string]interface{}{
			{
				"id": "11111111-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"name": "Alice", "canonical": "alice", "entity_type": "person",
				"embedding_dim": nil,
				"properties":    `{"role":"antagonist","age":42}`,
				"mention_count": int(1),
				"metadata":      `{"source":"manual"}`,
				"created_at":    "2025-01-15T10:30:00Z", "updated_at": "2025-01-15T10:30:00Z",
			},
			{
				"id": "11111111-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"name": "Böb Müller", "canonical": "bob-muller", "entity_type": "person",
				"embedding_dim": nil,
				"properties":    "{}",
				"mention_count": int(1),
				"metadata":      "{}",
				"created_at":    "2025-01-16T10:30:00Z", "updated_at": "2025-01-16T10:30:00Z",
			},
			{
				"id": "11111111-0000-0000-0000-000000000003",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"name": "Acme Corp", "canonical": "acme-corp", "entity_type": "organization",
				"embedding_dim": int(768),
				"properties":    "{}",
				"mention_count": int(1),
				"metadata":      "{}",
				"created_at":    "2025-01-17T10:30:00Z", "updated_at": "2025-01-17T10:30:00Z",
			},
		})
	})

	t.Run("entity_aliases", func(t *testing.T) {
		verifyRows(t, pgConn, "entity_aliases", []map[string]interface{}{
			{
				"id": "22222222-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"entity_id": "11111111-0000-0000-0000-000000000001",
				"alias": "Ali", "alias_type": "nickname",
				"created_at": "2025-01-15T10:30:00Z",
			},
			{
				"id": "22222222-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"entity_id": "11111111-0000-0000-0000-000000000001",
				"alias": "Алиса", "alias_type": "translation",
				"created_at": "2025-01-16T10:30:00Z",
			},
			{
				"id": "22222222-0000-0000-0000-000000000003",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"entity_id": "11111111-0000-0000-0000-000000000002",
				"alias": "Bob", "alias_type": "shortname",
				"created_at": "2025-01-16T10:30:00Z",
			},
		})
	})

	t.Run("relationships", func(t *testing.T) {
		verifyRows(t, pgConn, "relationships", []map[string]interface{}{
			{
				"id": "33333333-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"source_id": "11111111-0000-0000-0000-000000000001",
				"target_id": "11111111-0000-0000-0000-000000000002",
				"relation": "knows", "weight": 0.85,
				"properties":    `{"since":"2020"}`,
				"valid_from":    "2025-01-01T00:00:00Z",
				"valid_until":   "2026-01-01T00:00:00Z",
				"source_memory": "ffffffff-0000-0000-0000-000000000001",
				"created_at":    "2025-01-15T10:30:00Z",
			},
			{
				"id": "33333333-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"source_id": "11111111-0000-0000-0000-000000000002",
				"target_id": "11111111-0000-0000-0000-000000000003",
				"relation": "works_at", "weight": 1.0,
				"properties":    "{}",
				"valid_from":    "2025-01-16T00:00:00Z",
				"valid_until":   nil,
				"source_memory": nil,
				"created_at":    "2025-01-16T10:30:00Z",
			},
		})
	})

	t.Run("memory_lineage", func(t *testing.T) {
		verifyRows(t, pgConn, "memory_lineage", []map[string]interface{}{
			{
				"id": "44444444-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"memory_id": "ffffffff-0000-0000-0000-000000000001",
				"parent_id": nil, "relation": "origin",
				"context":    "{}",
				"created_at": "2025-01-15T10:30:00Z",
			},
			{
				"id": "44444444-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"memory_id": "ffffffff-0000-0000-0000-000000000002",
				"parent_id": "ffffffff-0000-0000-0000-000000000001",
				"relation":  "derived",
				"context":   `{"reason":"summarized"}`,
				"created_at": "2025-02-01T08:00:00Z",
			},
		})
	})

	t.Run("ingestion_log", func(t *testing.T) {
		verifyRows(t, pgConn, "ingestion_log", []map[string]interface{}{
			{
				"id": "55555555-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"source": "api", "content_hash": "sha256:abc123",
				"raw_content": "raw content here",
				"memory_ids":  `{"ffffffff-0000-0000-0000-000000000001","ffffffff-0000-0000-0000-000000000002"}`,
				"status":      "completed",
				"error":       nil,
				"metadata":    "{}",
				"created_at":  "2025-01-15T10:30:00Z",
			},
			{
				"id": "55555555-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"source": "file", "content_hash": nil,
				"raw_content": "failed raw content",
				"memory_ids":  "{}",
				"status":      "error",
				"error":       `"parsing failed: unexpected token"`,
				"metadata":    "{}",
				"created_at":  "2025-01-16T10:30:00Z",
			},
			{
				"id": "55555555-0000-0000-0000-000000000003",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"source": "webhook", "content_hash": nil,
				"raw_content": "webhook payload",
				"memory_ids":  `{"ffffffff-0000-0000-0000-000000000003"}`,
				"status":      "completed",
				"error":       nil,
				"metadata":    "{}",
				"created_at":  "2025-01-17T10:30:00Z",
			},
		})
	})

	t.Run("enrichment_queue", func(t *testing.T) {
		verifyRows(t, pgConn, "enrichment_queue", []map[string]interface{}{
			{
				"id": "66666666-0000-0000-0000-000000000001",
				"memory_id": "ffffffff-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"status": "pending", "priority": int(0),
				"claimed_at": nil, "claimed_by": nil,
				"attempts": int(0), "max_attempts": int(3),
				"last_error": nil,
				"steps_completed": "[]",
				"completed_at":    nil,
				"created_at": "2025-01-15T10:30:00Z", "updated_at": "2025-01-15T10:30:00Z",
			},
			{
				// Seeded as 'processing' with worker-1 owning it; finalizeStuckJobs
				// resets it to pending with cleared claim fields and bumps updated_at
				// so the Postgres deployment doesn't wait on a worker that's gone.
				"id": "66666666-0000-0000-0000-000000000002",
				"memory_id": "ffffffff-0000-0000-0000-000000000002",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"status": "pending", "priority": int(0),
				"claimed_at": nil, "claimed_by": nil,
				"attempts": int(0), "max_attempts": int(3),
				"last_error": nil,
				"steps_completed": `["embedding","entity_extraction"]`,
				"completed_at":    nil,
				"created_at": "2025-02-01T08:00:00Z", "updated_at": "<ANY>",
			},
			{
				"id": "66666666-0000-0000-0000-000000000003",
				"memory_id": "ffffffff-0000-0000-0000-000000000003",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"status": "completed", "priority": int(0),
				"claimed_at": nil, "claimed_by": nil,
				"attempts": int(2), "max_attempts": int(3),
				"last_error":      `"timeout on first attempt"`,
				"steps_completed": `["embedding","entity_extraction","summarization"]`,
				"completed_at":    "2025-03-01T10:00:00Z",
				"created_at": "2025-03-01T09:00:00Z", "updated_at": "2025-03-01T10:00:00Z",
			},
		})
	})

	t.Run("webhooks", func(t *testing.T) {
		verifyRows(t, pgConn, "webhooks", []map[string]interface{}{
			{
				"id": "77777777-0000-0000-0000-000000000001",
				"url": "https://example.com/hook", "secret": "whsec_supersecret",
				"events": `{"memory.created","memory.deleted"}`,
				"scope":  "global", "active": true,
				"last_fired":    "2025-06-01T12:00:00Z",
				"last_status":   int(200),
				"failure_count": int(0),
				"created_at":    "2025-01-15T10:30:00Z", "updated_at": "2025-06-01T12:00:00Z",
			},
			{
				"id": "77777777-0000-0000-0000-000000000002",
				"url": "https://other.example.com/hook", "secret": nil,
				"events": `{"memory.updated"}`,
				"scope":  "ns:aaaaaaaa-0000-0000-0000-000000000002", "active": false,
				"last_fired":    nil,
				"last_status":   nil,
				"failure_count": int(0),
				"created_at":    "2025-02-01T10:30:00Z", "updated_at": "2025-02-01T10:30:00Z",
			},
		})
	})

	t.Run("memory_shares", func(t *testing.T) {
		verifyRows(t, pgConn, "memory_shares", []map[string]interface{}{
			{
				"id": "88888888-0000-0000-0000-000000000001",
				"source_ns_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"target_ns_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"permission": "recall",
				"created_by": "cccccccc-0000-0000-0000-000000000001",
				"expires_at": "2026-12-31T23:59:59Z",
				"revoked_at": "2025-08-01T00:00:00Z",
				"created_at": "2025-01-15T10:30:00Z",
			},
			{
				"id": "88888888-0000-0000-0000-000000000002",
				"source_ns_id": "aaaaaaaa-0000-0000-0000-000000000001",
				"target_ns_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"permission": "read",
				"created_by": nil, "expires_at": nil, "revoked_at": nil,
				"created_at": "2025-02-01T10:30:00Z",
			},
		})
	})

	t.Run("token_usage", func(t *testing.T) {
		verifyRows(t, pgConn, "token_usage", []map[string]interface{}{
			{
				"id": "aabbccdd-0000-0000-0000-000000000001",
				"org_id": "bbbbbbbb-0000-0000-0000-000000000001",
				"user_id": "cccccccc-0000-0000-0000-000000000001",
				"project_id": "eeeeeeee-0000-0000-0000-000000000001",
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"operation": "embed", "provider": "openai", "model": "text-embedding-3-small",
				"tokens_input": int(150), "tokens_output": int(0),
				"memory_id":  "ffffffff-0000-0000-0000-000000000001",
				"api_key_id": "dddddddd-0000-0000-0000-000000000001",
				"latency_ms": int(42),
				"created_at": "2025-03-15T10:30:00Z",
			},
			{
				"id": "aabbccdd-0000-0000-0000-000000000002",
				"org_id": nil, "user_id": nil, "project_id": nil,
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"operation": "recall", "provider": "anthropic", "model": "claude-3-opus",
				"tokens_input": int(200), "tokens_output": int(500),
				"memory_id": nil, "api_key_id": nil, "latency_ms": nil,
				"created_at": "2025-03-16T10:30:00Z",
			},
			{
				"id": "aabbccdd-0000-0000-0000-000000000003",
				"org_id": "bbbbbbbb-0000-0000-0000-000000000001",
				"user_id": nil, "project_id": nil,
				"namespace_id": "aaaaaaaa-0000-0000-0000-000000000002",
				"operation": "enrich", "provider": "openai", "model": "gpt-4o",
				"tokens_input": int(300), "tokens_output": int(600),
				"memory_id": nil, "api_key_id": nil,
				"latency_ms": int(1250),
				"created_at": "2025-03-17T10:30:00Z",
			},
		})
	})

	t.Run("oauth_clients", func(t *testing.T) {
		verifyRows(t, pgConn, "oauth_clients", []map[string]interface{}{
			{
				"id": "99999999-0000-0000-0000-000000000001",
				"client_id": "test-client-id", "client_secret": "secret123",
				"name": "Test App",
				"redirect_uris":  `{"https://app.example.com/callback","https://app.example.com/callback2"}`,
				"grant_types":    `{"authorization_code","refresh_token"}`,
				"org_id":         "bbbbbbbb-0000-0000-0000-000000000001",
				"auto_registered": false,
				"created_at":     "2025-01-15T10:30:00Z",
			},
			{
				"id": "99999999-0000-0000-0000-000000000002",
				"client_id": "auto-client-id", "client_secret": nil,
				"name": "Auto Client",
				"redirect_uris":  `{"https://dynamic.example.com/callback"}`,
				"grant_types":    `{"authorization_code"}`,
				"org_id":         nil,
				"auto_registered": true,
				"created_at":     "2025-02-01T10:30:00Z",
			},
		})
	})

	t.Run("oauth_authorization_codes", func(t *testing.T) {
		verifyRows(t, pgConn, "oauth_authorization_codes", []map[string]interface{}{
			{
				"code":      "testcode123",
				"client_id": "test-client-id",
				"user_id":   "cccccccc-0000-0000-0000-000000000001",
				"redirect_uri":         "https://app.example.com/callback",
				"scope":                "read write",
				"code_challenge":       "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
				"code_challenge_method": "S256",
				"expires_at":           "2025-06-01T12:10:00Z",
				"created_at":           "2025-06-01T12:00:00Z",
				"resource":             "https://api.example.com/",
			},
			{
				"code":      "testcode456",
				"client_id": "auto-client-id",
				"user_id":   "cccccccc-0000-0000-0000-000000000002",
				"redirect_uri":         "https://dynamic.example.com/callback",
				"scope":                "read",
				"code_challenge":       nil,
				"code_challenge_method": "S256",
				"expires_at":           "2025-06-02T12:10:00Z",
				"created_at":           "2025-06-02T12:00:00Z",
				"resource":             nil,
			},
		})
	})

	t.Run("oauth_refresh_tokens", func(t *testing.T) {
		verifyRows(t, pgConn, "oauth_refresh_tokens", []map[string]interface{}{
			{
				"token_hash": "refreshhash123",
				"client_id":  "test-client-id",
				"user_id":    "cccccccc-0000-0000-0000-000000000001",
				"scope":      "read write",
				"expires_at": "2026-06-01T12:00:00Z",
				"revoked_at": nil,
				"created_at": "2025-06-01T12:00:00Z",
			},
			{
				"token_hash": "refreshhash456",
				"client_id":  "test-client-id",
				"user_id":    "cccccccc-0000-0000-0000-000000000001",
				"scope":      "read",
				"expires_at": nil,
				"revoked_at": "2025-07-01T00:00:00Z",
				"created_at": "2025-06-15T12:00:00Z",
			},
		})
	})

	t.Run("oauth_idp_configs", func(t *testing.T) {
		verifyRows(t, pgConn, "oauth_idp_configs", []map[string]interface{}{
			{
				"id": "aaaaaaaa-1111-0000-0000-000000000001",
				"org_id": "bbbbbbbb-0000-0000-0000-000000000001",
				"provider_type": "google",
				"client_id": "google-client-id", "client_secret": "google-client-secret",
				"issuer_url":      "https://accounts.google.com",
				"allowed_domains": `{"example.com","test.com"}`,
				"auto_provision":  true,
				"default_role":    "member",
				"created_at":      "2025-01-15T10:30:00Z", "updated_at": "2025-01-15T10:30:00Z",
			},
			{
				"id": "aaaaaaaa-1111-0000-0000-000000000002",
				"org_id": "bbbbbbbb-0000-0000-0000-000000000002",
				"provider_type": "github",
				"client_id": "github-client-id", "client_secret": "github-client-secret",
				"issuer_url":      nil,
				"allowed_domains": `{"github.example.com"}`,
				"auto_provision":  false,
				"default_role":    "readonly",
				"created_at":      "2025-02-01T10:30:00Z", "updated_at": "2025-02-01T10:30:00Z",
			},
		})
	})

	// Cleanup Postgres after test.
	cleanPostgres(t, pgConn)
}

func TestDataMigrator_ArrayConversions(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`[]`, `{}`},
		{`["a"]`, `{"a"}`},
		{`["a","b","c"]`, `{"a","b","c"}`},
		{`["tag1","tag2"]`, `{"tag1","tag2"}`},
		{`["has \"quote\""]`, `{"has \"quote\""}`},
		{``, `{}`},
		{`null`, `{}`},
	}
	for _, tt := range tests {
		got, err := jsonArrayToPostgresTextArray(tt.input)
		if err != nil {
			t.Errorf("input=%q err=%v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("input=%q got=%q want=%q", tt.input, got, tt.want)
		}
	}
}

func TestDatabaseAdminStore_TestConnection(t *testing.T) {
	srcDB, err := sql.Open("sqlite", "file::memory:?cache=shared&_test_conn=1")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer srcDB.Close()

	// We need a storage.DB to build the store; use a minimal wrapper via the
	// package-level openSQLiteInMemory helper.
	sqliteDB := openSQLiteInMemory(t)
	defer sqliteDB.Close()

	// Wrap in a dbWrapper so we can build the store.
	store := &DatabaseAdminStore{db: &testSQLiteDB{db: sqliteDB}}

	ctx := context.Background()
	result, err := store.TestConnection(ctx, resolvedPostgresURL)
	if err != nil {
		t.Fatalf("TestConnection returned error: %v", err)
	}
	if !result.Success {
		t.Errorf("TestConnection.Success = false: %s", result.Message)
	}
	if result.LatencyMs < 0 {
		t.Errorf("unexpected negative latency: %d", result.LatencyMs)
	}
	t.Logf("pgvector installed: %v, latency: %dms", result.PgvectorInstalled, result.LatencyMs)
}

func TestDatabaseAdminStore_TriggerMigration_RejectsNonSQLite(t *testing.T) {
	store := &DatabaseAdminStore{db: &testPostgresDB{}}
	ctx := context.Background()
	status, err := store.TriggerMigration(ctx, resolvedPostgresURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.Status != "error" {
		t.Errorf("expected status=error for postgres source, got %q", status.Status)
	}
}

// ── minimal storage.DB stubs for unit tests ───────────────────────────────────

type testSQLiteDB struct {
	db *sql.DB
}

func (d *testSQLiteDB) Backend() string { return "sqlite" }
func (d *testSQLiteDB) Ping(ctx context.Context) error { return d.db.PingContext(ctx) }
func (d *testSQLiteDB) Close() error                  { return d.db.Close() }
func (d *testSQLiteDB) DB() *sql.DB                   { return d.db }
func (d *testSQLiteDB) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, q, args...)
}
func (d *testSQLiteDB) Query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, q, args...)
}
func (d *testSQLiteDB) QueryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, q, args...)
}
func (d *testSQLiteDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.db.BeginTx(ctx, opts)
}
func (d *testSQLiteDB) WriteDB() *sql.DB { return d.db }

type testPostgresDB struct{}

func (d *testPostgresDB) Backend() string { return "postgres" }
func (d *testPostgresDB) Ping(_ context.Context) error { return nil }
func (d *testPostgresDB) Close() error                 { return nil }
func (d *testPostgresDB) DB() *sql.DB                  { return nil }
func (d *testPostgresDB) Exec(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, fmt.Errorf("not implemented")
}
func (d *testPostgresDB) Query(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("not implemented")
}
func (d *testPostgresDB) QueryRow(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}
func (d *testPostgresDB) BeginTx(_ context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	return nil, fmt.Errorf("not implemented")
}
func (d *testPostgresDB) WriteDB() *sql.DB { return nil }
