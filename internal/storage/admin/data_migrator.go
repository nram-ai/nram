package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx stdlib driver
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
	pgvector "github.com/pgvector/pgvector-go"
)

// DataMigrator handles copying all data from a SQLite source DB to a Postgres target.
//
// The migrator tracks every row successfully inserted into Postgres in the
// `inserted` map so child-table migrations can skip rows whose FK parent was
// dropped (either because the parent itself was orphan, or because SQLite lost
// the parent row but retained a child reference with FKs disabled).
//
// For each FK relationship, rows that reference a missing parent are recorded
// in `skipped` (keyed as "table.column") rather than aborting the migration —
// matching the user-approved policy to drop orphans while preserving rich
// history. The full skip breakdown is returned alongside a successful run so
// the operator can see exactly what was dropped.
type DataMigrator struct {
	src *sql.DB
	dst *sql.DB

	// inserted[table][id] = true if row id was successfully inserted into Postgres.
	// Consulted by child-table migrations to perform orphan-drop FK checks.
	inserted map[string]map[string]bool

	// insertedByClientID tracks oauth_clients.client_id specifically (TEXT FK),
	// separate from the UUID-keyed inserted["oauth_clients"].
	insertedByClientID map[string]bool

	// skipped["table.column"] = count of ROWS skipped due to orphan FK.
	// These reduce the row count of the target table vs. source.
	skipped map[string]int

	// skippedUpdates["table.column"] = count of column-level UPDATEs skipped
	// due to an orphan FK. Does not change row counts — the row was already
	// inserted with a NULL/blank value on the first pass.
	skippedUpdates map[string]int

	// tableCounts["table"] = total rows inserted into that table (for stats).
	tableCounts map[string]int
}

// markInserted records that (table, id) was successfully written to Postgres.
func (m *DataMigrator) markInserted(table, id string) {
	if m.inserted[table] == nil {
		m.inserted[table] = make(map[string]bool)
	}
	m.inserted[table][id] = true
	m.tableCounts[table]++
}

// hasInserted returns true if (table, id) was successfully inserted earlier.
// Used by child migrations to check FK parent existence before attempting an insert.
func (m *DataMigrator) hasInserted(table, id string) bool {
	return m.inserted[table][id]
}

// markInsertedAnon bumps the per-table inserted count for tables whose PK is
// not a UUID we need to track for FK checks (e.g. oauth tokens keyed by
// TEXT hashes, dream_project_dirty keyed by project_id, webauthn rows where
// the id column may be absent).
func (m *DataMigrator) markInsertedAnon(table string) {
	m.tableCounts[table]++
}

// sourceTableExists returns true if the given table exists in the SQLite source.
// Dream and webauthn tables were added in later migrations; a source from a
// stale deployment may pre-date them, in which case the migrator should skip
// rather than abort.
func (m *DataMigrator) sourceTableExists(ctx context.Context, name string) (bool, error) {
	var got string
	err := m.src.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name=$1", name,
	).Scan(&got)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == name, nil
}

// skipOrphan records that one row was skipped because its FK parent is missing.
// The key is "childTable.childColumn" — callers use the column that would have
// produced the FK failure (e.g. "relationships.source_memory").
func (m *DataMigrator) skipOrphan(table, column string) {
	m.skipped[table+"."+column]++
}

// skipOrphanUpdate records that one column-update was skipped because the FK
// target is missing. Used for second-pass updates (like memories.superseded_by)
// where the row itself exists but a specific column cannot be populated.
// Does not affect row-count validation.
func (m *DataMigrator) skipOrphanUpdate(table, column string) {
	m.skippedUpdates[table+"."+column]++
}

// Stats returns a snapshot of the migrator's outcome: per-table inserted counts
// and per-FK orphan-skip counts.
func (m *DataMigrator) Stats() MigrationStats {
	inserted := make(map[string]int, len(m.tableCounts))
	for k, v := range m.tableCounts {
		inserted[k] = v
	}
	skipped := make(map[string]int, len(m.skipped))
	for k, v := range m.skipped {
		skipped[k] = v
	}
	updates := make(map[string]int, len(m.skippedUpdates))
	for k, v := range m.skippedUpdates {
		updates[k] = v
	}
	return MigrationStats{
		Inserted:       inserted,
		SkippedOrphans: skipped,
		SkippedUpdates: updates,
	}
}

// MigrationStats describes what a DataMigrator.Run accomplished.
type MigrationStats struct {
	// Inserted[table] = number of rows successfully inserted into Postgres.
	Inserted map[string]int `json:"inserted,omitempty"`
	// SkippedOrphans["table.column"] = number of rows skipped because their FK
	// parent was missing from the source (or itself skipped transitively).
	SkippedOrphans map[string]int `json:"skipped_orphans,omitempty"`
	// SkippedUpdates["table.column"] = number of column-level updates skipped
	// due to orphan FK (row itself was inserted, but a self-ref column such
	// as memories.superseded_by could not be populated).
	SkippedUpdates map[string]int `json:"skipped_updates,omitempty"`
}

// newDataMigrator opens a Postgres connection to targetURL, runs schema migrations,
// and returns a DataMigrator ready to copy data.
func newDataMigrator(ctx context.Context, src *sql.DB, targetURL string) (*DataMigrator, error) {
	dst, err := sql.Open("pgx", targetURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	dst.SetMaxOpenConns(10)
	dst.SetConnMaxLifetime(5 * time.Minute)

	if err := dst.PingContext(ctx); err != nil {
		dst.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	// Run schema migrations on the target database.
	mg, err := migration.NewMigrator(dst, storage.BackendPostgres)
	if err != nil {
		dst.Close()
		return nil, fmt.Errorf("create postgres migrator: %w", err)
	}
	if err := mg.Up(); err != nil {
		_ = mg.Close()
		dst.Close()
		return nil, fmt.Errorf("postgres migrations: %w", err)
	}
	_ = mg.Close()

	return &DataMigrator{
		src:                src,
		dst:                dst,
		inserted:           make(map[string]map[string]bool),
		insertedByClientID: make(map[string]bool),
		skipped:            make(map[string]int),
		skippedUpdates:     make(map[string]int),
		tableCounts:        make(map[string]int),
	}, nil
}

// Close releases the target database connection.
func (m *DataMigrator) Close() error {
	return m.dst.Close()
}

// migratedTables lists the tables copied during Run, in migration order.
// This drives both the copy loop and the post-copy validation.
var migratedTables = []string{
	"namespaces",
	"organizations",
	"users",
	"api_keys",
	"projects",
	"settings",
	"system_meta",
	"memories",
	"entities",
	"entity_aliases",
	"relationships",
	"memory_lineage",
	"ingestion_log",
	"enrichment_queue",
	"webhooks",
	"memory_shares",
	"token_usage",
	"oauth_clients",
	"oauth_authorization_codes",
	"oauth_refresh_tokens",
	"oauth_idp_configs",
	"webauthn_credentials",
	"dream_cycles",
	"dream_logs",
	"dream_log_summaries",
	"dream_project_dirty",
}

// Run copies all tables from SQLite to Postgres in dependency order.
// It returns validation errors if row counts do not match.
func (m *DataMigrator) Run(ctx context.Context) error {
	type tableTask struct {
		name string
		fn   func(context.Context) error
	}
	tasks := []tableTask{
		{"namespaces", m.migrateNamespaces},
		{"organizations", m.migrateOrganizations},
		{"users", m.migrateUsers},
		{"api_keys", m.migrateAPIKeys},
		{"projects", m.migrateProjects},
		{"settings", m.migrateSettings},
		{"system_meta", m.migrateSystemMeta},
		{"memories", m.migrateMemories},
		{"memory_vectors", m.migrateMemoryVectors},
		{"entities", m.migrateEntities},
		{"entity_aliases", m.migrateEntityAliases},
		{"relationships", m.migrateRelationships},
		{"memory_lineage", m.migrateMemoryLineage},
		{"ingestion_log", m.migrateIngestionLog},
		{"enrichment_queue", m.migrateEnrichmentQueue},
		{"webhooks", m.migrateWebhooks},
		{"memory_shares", m.migrateMemoryShares},
		{"token_usage", m.migrateTokenUsage},
		{"oauth_clients", m.migrateOAuthClients},
		{"oauth_authorization_codes", m.migrateOAuthAuthorizationCodes},
		{"oauth_refresh_tokens", m.migrateOAuthRefreshTokens},
		{"oauth_idp_configs", m.migrateOAuthIDPConfigs},
		{"webauthn_credentials", m.migrateWebauthnCredentials},
		{"dream_cycles", m.migrateDreamCycles},
		{"dream_logs", m.migrateDreamLogs},
		{"dream_log_summaries", m.migrateDreamLogSummaries},
		{"dream_project_dirty", m.migrateDreamProjectDirty},
		// Second pass: set memories.superseded_by now that all memories are inserted.
		{"memories:superseded_by_pass2", m.migrateMemoriesSupersededByPass2},
	}

	for _, t := range tasks {
		if err := t.fn(ctx); err != nil {
			return fmt.Errorf("migrate %s: %w", t.name, err)
		}
	}

	// Validate row counts before writing the extra system_meta marker so the
	// counts stay comparable.
	if err := m.validateCounts(ctx); err != nil {
		return err
	}

	// Mark the destination as the new canonical backend.
	_, err := m.dst.ExecContext(ctx, `
		INSERT INTO system_meta (key, value) VALUES ('storage_backend', 'postgres')
		ON CONFLICT (key) DO UPDATE SET value = 'postgres', updated_at = now()
	`)
	if err != nil {
		return fmt.Errorf("set storage_backend: %w", err)
	}

	return nil
}

// validateCounts checks that source and destination row counts match for each
// table, accounting for rows the migrator deliberately skipped as orphans.
//
// Expected: postgres_count + orphan_skips_for_table == sqlite_count.
// A mismatch beyond that indicates a real migrator bug (e.g. silent ON CONFLICT
// drop, failed insert without a skip record).
func (m *DataMigrator) validateCounts(ctx context.Context) error {
	for _, name := range migratedTables {
		var srcCount, dstCount int
		if err := m.src.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+name).Scan(&srcCount); err != nil {
			srcCount = 0
		}
		if err := m.dst.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+name).Scan(&dstCount); err != nil {
			return fmt.Errorf("count %s in postgres: %w", name, err)
		}
		// Sum all orphan skips recorded against this table.
		var skipped int
		for key, n := range m.skipped {
			if strings.HasPrefix(key, name+".") {
				skipped += n
			}
		}
		if dstCount+skipped != srcCount {
			return fmt.Errorf("row count mismatch for %s: sqlite=%d postgres=%d orphans_skipped=%d (expected postgres=%d)",
				name, srcCount, dstCount, skipped, srcCount-skipped)
		}
	}

	// Validate vector counts: SQLite memory_vectors → sum of Postgres memory_vectors_* tables,
	// plus any orphan skips (memory_id pointed at a skipped memory) or unsupported-dimension skips.
	var srcVectors int
	if err := m.src.QueryRowContext(ctx, "SELECT COUNT(*) FROM memory_vectors").Scan(&srcVectors); err != nil {
		srcVectors = 0
	}
	if srcVectors > 0 {
		var dstVectors int
		for _, table := range vectorDimensionTables {
			var count int
			if err := m.dst.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				return fmt.Errorf("count %s in postgres: %w", table, err)
			}
			dstVectors += count
		}
		var skippedVectors int
		for key, n := range m.skipped {
			if strings.HasPrefix(key, "memory_vectors.") {
				skippedVectors += n
			}
		}
		if dstVectors+skippedVectors != srcVectors {
			return fmt.Errorf("row count mismatch for memory_vectors: sqlite=%d postgres(sum)=%d skipped=%d",
				srcVectors, dstVectors, skippedVectors)
		}
	}

	return nil
}

// vectorDimensionTables and supportedVectorDimensions are derived from
// storage.SupportedVectorDimensions so adding a new dimension in one place
// automatically flows through migration, validation, and reset.
var (
	vectorDimensionTables    = buildVectorDimensionTables()
	supportedVectorDimensions = buildSupportedVectorDimensions()
)

func buildVectorDimensionTables() []string {
	dims := make([]int, 0, len(storage.SupportedVectorDimensions))
	for d := range storage.SupportedVectorDimensions {
		dims = append(dims, d)
	}
	sort.Ints(dims)
	out := make([]string, len(dims))
	for i, d := range dims {
		out[i] = fmt.Sprintf("memory_vectors_%d", d)
	}
	return out
}

func buildSupportedVectorDimensions() map[int]string {
	out := make(map[int]string, len(storage.SupportedVectorDimensions))
	for d := range storage.SupportedVectorDimensions {
		out[d] = fmt.Sprintf("memory_vectors_%d", d)
	}
	return out
}

// ── per-table copy helpers ────────────────────────────────────────────────────

func (m *DataMigrator) migrateNamespaces(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at
		FROM namespaces
		ORDER BY depth ASC, id ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// The root row is inserted by migration; skip it to avoid a conflict.
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, name, slug, kind string
			parentID             sql.NullString
			path                 string
			depth                int
			metadata             string
			createdAt, updatedAt string
		)
		if err := rows.Scan(&id, &name, &slug, &kind, &parentID, &path, &depth, &metadata, &createdAt, &updatedAt); err != nil {
			return err
		}
		var pgParent interface{}
		if parentID.Valid {
			if !m.hasInserted("namespaces", parentID.String) {
				// Orphan parent (SQLite FKs were disabled at some point and the
				// parent namespace got purged). Drop the child rather than abort.
				m.skipOrphan("namespaces", "parent_id")
				continue
			}
			pgParent = parentID.String
		}
		if _, err := stmt.ExecContext(ctx, id, name, slug, kind, pgParent, path, depth, metadata, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert namespace %s: %w", id, err)
		}
		m.markInserted("namespaces", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateOrganizations(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, name, slug, settings, created_at, updated_at
		FROM organizations
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO organizations (id, namespace_id, name, slug, settings, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var id, nsID, name, slug, settings, createdAt, updatedAt string
		if err := rows.Scan(&id, &nsID, &name, &slug, &settings, &createdAt, &updatedAt); err != nil {
			return err
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("organizations", "namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx, id, nsID, name, slug, settings, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert org %s: %w", id, err)
		}
		m.markInserted("organizations", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateUsers(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, email, display_name, password_hash, org_id, namespace_id, role,
		       settings, created_at, updated_at, last_login, disabled_at
		FROM users
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO users (id, email, display_name, password_hash, org_id, namespace_id, role,
		                   settings, created_at, updated_at, last_login, disabled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, email, displayName    string
			passwordHash              sql.NullString
			orgID, nsID, role         string
			settings, createdAt, updatedAt string
			lastLogin, disabledAt     sql.NullString
		)
		if err := rows.Scan(&id, &email, &displayName, &passwordHash, &orgID, &nsID, &role,
			&settings, &createdAt, &updatedAt, &lastLogin, &disabledAt); err != nil {
			return err
		}
		if !m.hasInserted("organizations", orgID) {
			m.skipOrphan("users", "org_id")
			continue
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("users", "namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx, id, email, displayName,
			nullStringToInterface(passwordHash),
			orgID, nsID, role, settings, createdAt, updatedAt,
			nullStringToInterface(lastLogin),
			nullStringToInterface(disabledAt),
		); err != nil {
			return fmt.Errorf("insert user %s: %w", id, err)
		}
		m.markInserted("users", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateAPIKeys(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at
		FROM api_keys
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO api_keys (id, user_id, key_prefix, key_hash, name, scopes, last_used, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6::uuid[], $7, $8, $9)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, userID, keyPrefix, keyHash, name string
			scopesJSON                            string
			lastUsed, expiresAt                  sql.NullString
			createdAt                            string
		)
		if err := rows.Scan(&id, &userID, &keyPrefix, &keyHash, &name, &scopesJSON, &lastUsed, &expiresAt, &createdAt); err != nil {
			return err
		}
		pgScopes, err := jsonArrayToPostgresUUIDArray(scopesJSON)
		if err != nil {
			return fmt.Errorf("convert scopes for api_key %s: %w", id, err)
		}
		if !m.hasInserted("users", userID) {
			m.skipOrphan("api_keys", "user_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx, id, userID, keyPrefix, keyHash, name, pgScopes,
			nullStringToInterface(lastUsed),
			nullStringToInterface(expiresAt),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert api_key %s: %w", id, err)
		}
		m.markInserted("api_keys", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateProjects(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, owner_namespace_id, name, slug, description,
		       default_tags, settings, created_at, updated_at
		FROM projects
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description,
		                      default_tags, settings, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::text[], $8, $9, $10)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, ownerNsID       string
			name, slug, description   string
			defaultTagsJSON, settings string
			createdAt, updatedAt      string
		)
		if err := rows.Scan(&id, &nsID, &ownerNsID, &name, &slug, &description,
			&defaultTagsJSON, &settings, &createdAt, &updatedAt); err != nil {
			return err
		}
		pgTags, err := jsonArrayToPostgresTextArray(defaultTagsJSON)
		if err != nil {
			return fmt.Errorf("convert default_tags for project %s: %w", id, err)
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("projects", "namespace_id")
			continue
		}
		if !m.hasInserted("namespaces", ownerNsID) {
			m.skipOrphan("projects", "owner_namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx, id, nsID, ownerNsID, name, slug, description,
			pgTags, settings, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert project %s: %w", id, err)
		}
		m.markInserted("projects", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateSettings(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT key, value, scope, updated_by, updated_at
		FROM settings
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO settings (key, value, scope, updated_by, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			key, value, scope string
			updatedBy         sql.NullString
			updatedAt         string
		)
		if err := rows.Scan(&key, &value, &scope, &updatedBy, &updatedAt); err != nil {
			return err
		}
		// updated_by is NULL-able; if set, must reference a real user.
		if updatedBy.Valid && !m.hasInserted("users", updatedBy.String) {
			m.skipOrphan("settings", "updated_by")
			continue
		}
		if _, err := stmt.ExecContext(ctx, key, value, scope,
			nullStringToInterface(updatedBy), updatedAt); err != nil {
			return fmt.Errorf("insert setting %s/%s: %w", key, scope, err)
		}
		m.markInsertedAnon("settings")
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateSystemMeta(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT key, value, created_at, updated_at FROM system_meta
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO system_meta (key, value, created_at, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var key, value, createdAt, updatedAt string
		if err := rows.Scan(&key, &value, &createdAt, &updatedAt); err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, key, value, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert system_meta %s: %w", key, err)
		}
		m.markInsertedAnon("system_meta")
	}
	return tx.Commit()
}

// migrateMemories performs PASS 1 of the two-pass memory migration.
// It inserts every memory with superseded_by set to NULL, regardless of what
// the source had. The self-referential superseded_by column is populated in a
// second pass (migrateMemoriesSupersededByPass2) after every memory is written,
// which avoids FK-order violations without requiring DEFERRABLE constraints.
// Rows whose namespace_id points at a missing namespace are skipped.
func (m *DataMigrator) migrateMemories(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, content, embedding_dim, source, tags, confidence, importance,
		       access_count, last_accessed, expires_at, superseded_by, enriched, metadata,
		       created_at, updated_at, deleted_at, purge_after
		FROM memories
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO memories (id, namespace_id, content, embedding_dim, source, tags, confidence,
		                      importance, access_count, last_accessed, expires_at,
		                      enriched, metadata, created_at, updated_at, deleted_at, purge_after)
		VALUES ($1, $2, $3, $4, $5, $6::text[], $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, content    string
			embeddingDim         sql.NullInt64
			source               sql.NullString
			tagsJSON             string
			confidence           float64
			importance           float64
			accessCount          int
			lastAccessed         sql.NullString
			expiresAt            sql.NullString
			supersededBy         sql.NullString
			enriched             int
			metadata             string
			createdAt, updatedAt string
			deletedAt            sql.NullString
			purgeAfter           sql.NullString
		)
		if err := rows.Scan(&id, &nsID, &content, &embeddingDim, &source, &tagsJSON,
			&confidence, &importance, &accessCount, &lastAccessed, &expiresAt, &supersededBy,
			&enriched, &metadata, &createdAt, &updatedAt, &deletedAt, &purgeAfter); err != nil {
			return err
		}
		// Orphan check: namespace_id must reference a namespace we inserted.
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("memories", "namespace_id")
			continue
		}
		pgTags, err := jsonArrayToPostgresTextArray(tagsJSON)
		if err != nil {
			return fmt.Errorf("convert tags for memory %s: %w", id, err)
		}
		var pgEnriched bool
		if enriched != 0 {
			pgEnriched = true
		}
		// Note: superseded_by is deliberately omitted here — populated in pass 2.
		if _, err := stmt.ExecContext(ctx,
			id, nsID, content,
			nullInt64ToInterface(embeddingDim),
			nullStringToInterface(source),
			pgTags,
			confidence, importance, accessCount,
			nullStringToInterface(lastAccessed),
			nullStringToInterface(expiresAt),
			pgEnriched,
			metadata, createdAt, updatedAt,
			nullStringToInterface(deletedAt),
			nullStringToInterface(purgeAfter),
		); err != nil {
			return fmt.Errorf("insert memory %s: %w", id, err)
		}
		m.markInserted("memories", id)
	}
	return tx.Commit()
}

// migrateMemoriesSupersededByPass2 is the second pass of memory migration.
// It walks the source once more and UPDATEs memories.superseded_by for each
// row where both the memory and its superseded_by target were successfully
// inserted in pass 1. Rows where either end is missing (orphan) are skipped
// and recorded against the memories.superseded_by counter.
func (m *DataMigrator) migrateMemoriesSupersededByPass2(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, superseded_by FROM memories WHERE superseded_by IS NOT NULL
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx,
		`UPDATE memories SET superseded_by = $1 WHERE id = $2`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var id, supersededBy string
		if err := rows.Scan(&id, &supersededBy); err != nil {
			return err
		}
		if !m.hasInserted("memories", id) || !m.hasInserted("memories", supersededBy) {
			// Either the memory or its superseded_by target was skipped.
			// Record as a column-update skip: the row still exists in Postgres
			// with superseded_by NULL, so row counts are not affected.
			m.skipOrphanUpdate("memories", "superseded_by")
			continue
		}
		if _, err := stmt.ExecContext(ctx, supersededBy, id); err != nil {
			return fmt.Errorf("update superseded_by for memory %s: %w", id, err)
		}
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateMemoryVectors(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT memory_id, namespace_id, dimension, embedding FROM memory_vectors
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Group vectors by target dimension table so we can batch-insert per table in a transaction.
	type vectorRow struct {
		memoryID string
		floats   []float32
	}
	byTable := make(map[string][]vectorRow)
	var skipped int

	for rows.Next() {
		var (
			memoryID    string
			namespaceID string
			dimension   int
			embedding   []byte
		)
		if err := rows.Scan(&memoryID, &namespaceID, &dimension, &embedding); err != nil {
			return err
		}

		// Orphan check: memory_id must reference a memory we successfully inserted.
		// Otherwise the Postgres memory_vectors_*.memory_id FK would fail.
		if !m.hasInserted("memories", memoryID) {
			m.skipOrphan("memory_vectors", "memory_id")
			continue
		}

		table, ok := supportedVectorDimensions[dimension]
		if !ok {
			log.Printf("migrateMemoryVectors: skipping memory %s with unsupported dimension %d", memoryID, dimension)
			m.skipOrphan("memory_vectors", "unsupported_dimension")
			skipped++
			continue
		}

		floats, err := hnsw.DecodeVector(embedding)
		if err != nil {
			return fmt.Errorf("decode vector for memory %s: %w", memoryID, err)
		}

		byTable[table] = append(byTable[table], vectorRow{memoryID: memoryID, floats: floats})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if skipped > 0 {
		log.Printf("migrateMemoryVectors: skipped %d vectors with unsupported dimensions", skipped)
	}

	// Insert vectors into each Postgres dimension table.
	for table, vectors := range byTable {
		tx, err := m.dst.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck

		stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (memory_id, embedding) VALUES ($1, $2)
			ON CONFLICT (memory_id) DO UPDATE SET embedding = EXCLUDED.embedding
		`, table))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, v := range vectors {
			if _, err := stmt.ExecContext(ctx, v.memoryID, pgvector.NewVector(v.floats)); err != nil {
				return fmt.Errorf("insert vector into %s for memory %s: %w", table, v.memoryID, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit vectors for %s: %w", table, err)
		}
	}

	return nil
}

func (m *DataMigrator) migrateEntities(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, name, canonical, entity_type, embedding_dim, properties,
		       mention_count, metadata, created_at, updated_at
		FROM entities
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim,
		                      properties, mention_count, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, name, canonical, entityType string
			embeddingDim                          sql.NullInt64
			properties                            string
			mentionCount                          int
			metadata                              string
			createdAt, updatedAt                  string
		)
		if err := rows.Scan(&id, &nsID, &name, &canonical, &entityType, &embeddingDim,
			&properties, &mentionCount, &metadata, &createdAt, &updatedAt); err != nil {
			return err
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("entities", "namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id, nsID, name, canonical, entityType,
			nullInt64ToInterface(embeddingDim),
			properties, mentionCount, metadata, createdAt, updatedAt,
		); err != nil {
			return fmt.Errorf("insert entity %s: %w", id, err)
		}
		m.markInserted("entities", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateEntityAliases(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, entity_id, alias, alias_type, created_at FROM entity_aliases
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var id, namespaceID, entityID, alias, aliasType, createdAt string
		if err := rows.Scan(&id, &namespaceID, &entityID, &alias, &aliasType, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("entities", entityID) {
			m.skipOrphan("entity_aliases", "entity_id")
			continue
		}
		if !m.hasInserted("namespaces", namespaceID) {
			m.skipOrphan("entity_aliases", "namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx, id, namespaceID, entityID, alias, aliasType, createdAt); err != nil {
			return fmt.Errorf("insert entity_alias %s: %w", id, err)
		}
		m.markInserted("entity_aliases", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateRelationships(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, source_id, target_id, relation, weight, properties,
		       valid_from, valid_until, source_memory, created_at
		FROM relationships
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight,
		                           properties, valid_from, valid_until, source_memory, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, srcID, tgtID, relation string
			weight                           float64
			properties                       string
			validFrom                        string
			validUntil, sourceMemory         sql.NullString
			createdAt                        string
		)
		if err := rows.Scan(&id, &nsID, &srcID, &tgtID, &relation, &weight, &properties,
			&validFrom, &validUntil, &sourceMemory, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("relationships", "namespace_id")
			continue
		}
		if !m.hasInserted("entities", srcID) {
			m.skipOrphan("relationships", "source_id")
			continue
		}
		if !m.hasInserted("entities", tgtID) {
			m.skipOrphan("relationships", "target_id")
			continue
		}
		if sourceMemory.Valid && !m.hasInserted("memories", sourceMemory.String) {
			m.skipOrphan("relationships", "source_memory")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id, nsID, srcID, tgtID, relation, weight, properties, validFrom,
			nullStringToInterface(validUntil),
			nullStringToInterface(sourceMemory),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert relationship %s: %w", id, err)
		}
		m.markInserted("relationships", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateMemoryLineage(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, memory_id, parent_id, relation, context, created_at FROM memory_lineage
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO memory_lineage (id, namespace_id, memory_id, parent_id, relation, context, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, namespaceID, memoryID, relation string
			parentID                             sql.NullString
			context                              string
			createdAt                            string
		)
		if err := rows.Scan(&id, &namespaceID, &memoryID, &parentID, &relation, &context, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("namespaces", namespaceID) {
			m.skipOrphan("memory_lineage", "namespace_id")
			continue
		}
		if !m.hasInserted("memories", memoryID) {
			m.skipOrphan("memory_lineage", "memory_id")
			continue
		}
		if parentID.Valid && !m.hasInserted("memories", parentID.String) {
			m.skipOrphan("memory_lineage", "parent_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx, id, namespaceID, memoryID,
			nullStringToInterface(parentID),
			relation, context, createdAt,
		); err != nil {
			return fmt.Errorf("insert memory_lineage %s: %w", id, err)
		}
		m.markInserted("memory_lineage", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateIngestionLog(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, namespace_id, source, content_hash, raw_content, memory_ids,
		       status, error, metadata, created_at
		FROM ingestion_log
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ingestion_log (id, namespace_id, source, content_hash, raw_content,
		                           memory_ids, status, error, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6::uuid[], $7, $8, $9, $10)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, source  string
			contentHash       sql.NullString
			rawContent        string
			memoryIDsJSON     string
			status            string
			errText           sql.NullString
			metadata          string
			createdAt         string
		)
		if err := rows.Scan(&id, &nsID, &source, &contentHash, &rawContent, &memoryIDsJSON,
			&status, &errText, &metadata, &createdAt); err != nil {
			return err
		}
		pgMemoryIDs, err := jsonArrayToPostgresUUIDArray(memoryIDsJSON)
		if err != nil {
			return fmt.Errorf("convert memory_ids for ingestion_log %s: %w", id, err)
		}
		pgErr, err := textToJSONB(errText)
		if err != nil {
			return fmt.Errorf("encode error for ingestion_log %s: %w", id, err)
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("ingestion_log", "namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id, nsID, source,
			nullStringToInterface(contentHash),
			rawContent, pgMemoryIDs, status, pgErr, metadata, createdAt,
		); err != nil {
			return fmt.Errorf("insert ingestion_log %s: %w", id, err)
		}
		m.markInserted("ingestion_log", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateEnrichmentQueue(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, memory_id, namespace_id, status, priority, claimed_at, claimed_by,
		       attempts, max_attempts, last_error, steps_completed, completed_at,
		       created_at, updated_at
		FROM enrichment_queue
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, priority, claimed_at,
		                              claimed_by, attempts, max_attempts, last_error,
		                              steps_completed, completed_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, memoryID, nsID, status string
			priority                   int
			claimedAt                  sql.NullString
			claimedBy                  sql.NullString
			attempts, maxAttempts      int
			lastError                  sql.NullString
			stepsCompleted             string
			completedAt                sql.NullString
			createdAt, updatedAt       string
		)
		if err := rows.Scan(&id, &memoryID, &nsID, &status, &priority, &claimedAt, &claimedBy,
			&attempts, &maxAttempts, &lastError, &stepsCompleted, &completedAt,
			&createdAt, &updatedAt); err != nil {
			return err
		}
		pgLastError, err := textToJSONB(lastError)
		if err != nil {
			return fmt.Errorf("encode last_error for enrichment_queue %s: %w", id, err)
		}
		if !m.hasInserted("memories", memoryID) {
			m.skipOrphan("enrichment_queue", "memory_id")
			continue
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("enrichment_queue", "namespace_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id, memoryID, nsID, status, priority,
			nullStringToInterface(claimedAt),
			nullStringToInterface(claimedBy),
			attempts, maxAttempts, pgLastError, stepsCompleted,
			nullStringToInterface(completedAt),
			createdAt, updatedAt,
		); err != nil {
			return fmt.Errorf("insert enrichment_queue %s: %w", id, err)
		}
		m.markInserted("enrichment_queue", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateWebhooks(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, url, secret, events, scope, active, last_fired, last_status, failure_count,
		       created_at, updated_at
		FROM webhooks
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO webhooks (id, url, secret, events, scope, active, last_fired, last_status,
		                      failure_count, created_at, updated_at)
		VALUES ($1, $2, $3, $4::text[], $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, url         string
			secret          sql.NullString
			eventsJSON      string
			scope           string
			active          int
			lastFired       sql.NullString
			lastStatus      sql.NullInt64
			failureCount    int
			createdAt, updatedAt string
		)
		if err := rows.Scan(&id, &url, &secret, &eventsJSON, &scope, &active, &lastFired,
			&lastStatus, &failureCount, &createdAt, &updatedAt); err != nil {
			return err
		}
		pgEvents, err := jsonArrayToPostgresTextArray(eventsJSON)
		if err != nil {
			return fmt.Errorf("convert events for webhook %s: %w", id, err)
		}
		pgActive := active != 0
		if _, err := stmt.ExecContext(ctx,
			id, url,
			nullStringToInterface(secret),
			pgEvents, scope, pgActive,
			nullStringToInterface(lastFired),
			nullInt64ToInterface(lastStatus),
			failureCount, createdAt, updatedAt,
		); err != nil {
			return fmt.Errorf("insert webhook %s: %w", id, err)
		}
		m.markInserted("webhooks", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateMemoryShares(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, source_ns_id, target_ns_id, permission, created_by, expires_at, revoked_at, created_at
		FROM memory_shares
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO memory_shares (id, source_ns_id, target_ns_id, permission, created_by,
		                           expires_at, revoked_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, srcNsID, tgtNsID, permission string
			createdBy, expiresAt, revokedAt  sql.NullString
			createdAt                        string
		)
		if err := rows.Scan(&id, &srcNsID, &tgtNsID, &permission, &createdBy,
			&expiresAt, &revokedAt, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("namespaces", srcNsID) {
			m.skipOrphan("memory_shares", "source_ns_id")
			continue
		}
		if !m.hasInserted("namespaces", tgtNsID) {
			m.skipOrphan("memory_shares", "target_ns_id")
			continue
		}
		if createdBy.Valid && !m.hasInserted("users", createdBy.String) {
			m.skipOrphan("memory_shares", "created_by")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id, srcNsID, tgtNsID, permission,
			nullStringToInterface(createdBy),
			nullStringToInterface(expiresAt),
			nullStringToInterface(revokedAt),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert memory_share %s: %w", id, err)
		}
		m.markInserted("memory_shares", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateTokenUsage(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, org_id, user_id, project_id, namespace_id, operation, provider, model,
		       tokens_input, tokens_output, memory_id, api_key_id, latency_ms, created_at
		FROM token_usage
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO token_usage (id, org_id, user_id, project_id, namespace_id, operation, provider,
		                         model, tokens_input, tokens_output, memory_id, api_key_id,
		                         latency_ms, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, operation, provider, model string
			orgID, userID, projectID             sql.NullString
			tokensInput, tokensOutput            int
			memoryID, apiKeyID                   sql.NullString
			latencyMs                            sql.NullInt64
			createdAt                            string
		)
		if err := rows.Scan(&id, &orgID, &userID, &projectID, &nsID, &operation, &provider,
			&model, &tokensInput, &tokensOutput, &memoryID, &apiKeyID, &latencyMs, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("token_usage", "namespace_id")
			continue
		}
		if orgID.Valid && !m.hasInserted("organizations", orgID.String) {
			m.skipOrphan("token_usage", "org_id")
			continue
		}
		if userID.Valid && !m.hasInserted("users", userID.String) {
			m.skipOrphan("token_usage", "user_id")
			continue
		}
		if projectID.Valid && !m.hasInserted("projects", projectID.String) {
			m.skipOrphan("token_usage", "project_id")
			continue
		}
		if memoryID.Valid && !m.hasInserted("memories", memoryID.String) {
			m.skipOrphan("token_usage", "memory_id")
			continue
		}
		if apiKeyID.Valid && !m.hasInserted("api_keys", apiKeyID.String) {
			m.skipOrphan("token_usage", "api_key_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id,
			nullStringToInterface(orgID),
			nullStringToInterface(userID),
			nullStringToInterface(projectID),
			nsID, operation, provider, model,
			tokensInput, tokensOutput,
			nullStringToInterface(memoryID),
			nullStringToInterface(apiKeyID),
			nullInt64ToInterface(latencyMs),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert token_usage %s: %w", id, err)
		}
		m.markInserted("token_usage", id)
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateOAuthClients(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, client_id, client_secret, name, redirect_uris, grant_types, org_id,
		       auto_registered, created_at
		FROM oauth_clients
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO oauth_clients (id, client_id, client_secret, name, redirect_uris, grant_types,
		                           org_id, auto_registered, created_at)
		VALUES ($1, $2, $3, $4, $5::text[], $6::text[], $7, $8, $9)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, clientID         string
			clientSecret         sql.NullString
			name                 string
			redirectURIsJSON     string
			grantTypesJSON       string
			orgID                sql.NullString
			autoRegistered       int
			createdAt            string
		)
		if err := rows.Scan(&id, &clientID, &clientSecret, &name, &redirectURIsJSON,
			&grantTypesJSON, &orgID, &autoRegistered, &createdAt); err != nil {
			return err
		}
		pgRedirectURIs, err := jsonArrayToPostgresTextArray(redirectURIsJSON)
		if err != nil {
			return fmt.Errorf("convert redirect_uris for oauth_client %s: %w", id, err)
		}
		pgGrantTypes, err := jsonArrayToPostgresTextArray(grantTypesJSON)
		if err != nil {
			return fmt.Errorf("convert grant_types for oauth_client %s: %w", id, err)
		}
		pgAutoRegistered := autoRegistered != 0
		if orgID.Valid && !m.hasInserted("organizations", orgID.String) {
			m.skipOrphan("oauth_clients", "org_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id, clientID,
			nullStringToInterface(clientSecret),
			name, pgRedirectURIs, pgGrantTypes,
			nullStringToInterface(orgID),
			pgAutoRegistered, createdAt,
		); err != nil {
			return fmt.Errorf("insert oauth_client %s: %w", id, err)
		}
		m.markInserted("oauth_clients", id)
		m.insertedByClientID[clientID] = true
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateOAuthAuthorizationCodes(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT code, client_id, user_id, redirect_uri, scope, code_challenge,
		       code_challenge_method, expires_at, created_at, resource
		FROM oauth_authorization_codes
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO oauth_authorization_codes (code, client_id, user_id, redirect_uri, scope,
		                                       code_challenge, code_challenge_method,
		                                       expires_at, created_at, resource)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			code, clientID, userID, redirectURI string
			scope                               string
			codeChallenge                       sql.NullString
			codeChallengeMethod                 string
			expiresAt, createdAt                string
			resource                            sql.NullString
		)
		if err := rows.Scan(&code, &clientID, &userID, &redirectURI, &scope,
			&codeChallenge, &codeChallengeMethod, &expiresAt, &createdAt, &resource); err != nil {
			return err
		}
		if !m.insertedByClientID[clientID] {
			m.skipOrphan("oauth_authorization_codes", "client_id")
			continue
		}
		if !m.hasInserted("users", userID) {
			m.skipOrphan("oauth_authorization_codes", "user_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			code, clientID, userID, redirectURI, scope,
			nullStringToInterface(codeChallenge),
			codeChallengeMethod, expiresAt, createdAt,
			nullStringToInterface(resource),
		); err != nil {
			return fmt.Errorf("insert oauth_authorization_code %s: %w", code, err)
		}
		m.markInsertedAnon("oauth_authorization_codes")
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateOAuthRefreshTokens(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT token_hash, client_id, user_id, scope, expires_at, revoked_at, created_at
		FROM oauth_refresh_tokens
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO oauth_refresh_tokens (token_hash, client_id, user_id, scope, expires_at,
		                                  revoked_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			tokenHash, clientID, userID, scope string
			expiresAt, revokedAt               sql.NullString
			createdAt                          string
		)
		if err := rows.Scan(&tokenHash, &clientID, &userID, &scope,
			&expiresAt, &revokedAt, &createdAt); err != nil {
			return err
		}
		if !m.insertedByClientID[clientID] {
			m.skipOrphan("oauth_refresh_tokens", "client_id")
			continue
		}
		if !m.hasInserted("users", userID) {
			m.skipOrphan("oauth_refresh_tokens", "user_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			tokenHash, clientID, userID, scope,
			nullStringToInterface(expiresAt),
			nullStringToInterface(revokedAt),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert oauth_refresh_token %s: %w", tokenHash, err)
		}
		m.markInsertedAnon("oauth_refresh_tokens")
	}
	return tx.Commit()
}

func (m *DataMigrator) migrateOAuthIDPConfigs(ctx context.Context) error {
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, org_id, provider_type, client_id, client_secret, issuer_url,
		       allowed_domains, auto_provision, default_role, created_at, updated_at
		FROM oauth_idp_configs
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO oauth_idp_configs (id, org_id, provider_type, client_id, client_secret,
		                               issuer_url, allowed_domains, auto_provision, default_role,
		                               created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::text[], $8, $9, $10, $11)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id                string
			orgID             sql.NullString
			providerType      string
			clientID          string
			clientSecret      string
			issuerURL         sql.NullString
			allowedDomainsJSON string
			autoProvision     int
			defaultRole       string
			createdAt, updatedAt string
		)
		if err := rows.Scan(&id, &orgID, &providerType, &clientID, &clientSecret, &issuerURL,
			&allowedDomainsJSON, &autoProvision, &defaultRole, &createdAt, &updatedAt); err != nil {
			return err
		}
		pgAllowedDomains, err := jsonArrayToPostgresTextArray(allowedDomainsJSON)
		if err != nil {
			return fmt.Errorf("convert allowed_domains for oauth_idp_config %s: %w", id, err)
		}
		pgAutoProvision := autoProvision != 0
		if orgID.Valid && !m.hasInserted("organizations", orgID.String) {
			m.skipOrphan("oauth_idp_configs", "org_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			id,
			nullStringToInterface(orgID),
			providerType, clientID, clientSecret,
			nullStringToInterface(issuerURL),
			pgAllowedDomains, pgAutoProvision, defaultRole, createdAt, updatedAt,
		); err != nil {
			return fmt.Errorf("insert oauth_idp_config %s: %w", id, err)
		}
		m.markInserted("oauth_idp_configs", id)
	}
	return tx.Commit()
}

// ── new tables: dreaming + webauthn ──────────────────────────────────────────

// migrateDreamCycles copies the dream_cycles table. Skips rows whose project_id
// or namespace_id references a missing parent. No-op if the source predates
// the dreaming schema (SQLite migration 16).
func (m *DataMigrator) migrateDreamCycles(ctx context.Context) error {
	if ok, err := m.sourceTableExists(ctx, "dream_cycles"); err != nil {
		return err
	} else if !ok {
		return nil
	}
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, project_id, namespace_id, status, phase, tokens_used, token_budget,
		       phase_summary, error, started_at, completed_at, created_at, updated_at
		FROM dream_cycles
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO dream_cycles (id, project_id, namespace_id, status, phase, tokens_used,
		                          token_budget, phase_summary, error, started_at, completed_at,
		                          created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, projectID, nsID, status string
			phase                       sql.NullString
			tokensUsed, tokenBudget     sql.NullInt64
			phaseSummary                sql.NullString
			errorText                   sql.NullString
			startedAt, completedAt      sql.NullString
			createdAt, updatedAt        sql.NullString
		)
		if err := rows.Scan(&id, &projectID, &nsID, &status, &phase, &tokensUsed, &tokenBudget,
			&phaseSummary, &errorText, &startedAt, &completedAt, &createdAt, &updatedAt); err != nil {
			return err
		}
		if !m.hasInserted("projects", projectID) {
			m.skipOrphan("dream_cycles", "project_id")
			continue
		}
		if !m.hasInserted("namespaces", nsID) {
			m.skipOrphan("dream_cycles", "namespace_id")
			continue
		}
		pgPhaseSummary := "{}"
		if phaseSummary.Valid && phaseSummary.String != "" {
			pgPhaseSummary = phaseSummary.String
		}
		if _, err := stmt.ExecContext(ctx,
			id, projectID, nsID, status,
			nullStringToInterface(phase),
			nullInt64ToInterface(tokensUsed),
			nullInt64ToInterface(tokenBudget),
			pgPhaseSummary,
			nullStringToInterface(errorText),
			nullStringToInterface(startedAt),
			nullStringToInterface(completedAt),
			nullStringToInterface(createdAt),
			nullStringToInterface(updatedAt),
		); err != nil {
			return fmt.Errorf("insert dream_cycle %s: %w", id, err)
		}
		m.markInserted("dream_cycles", id)
	}
	return tx.Commit()
}

// migrateDreamLogs copies the dream_logs table. No-op if the source predates
// the dreaming schema.
func (m *DataMigrator) migrateDreamLogs(ctx context.Context) error {
	if ok, err := m.sourceTableExists(ctx, "dream_logs"); err != nil {
		return err
	} else if !ok {
		return nil
	}
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, cycle_id, project_id, phase, operation, target_type, target_id,
		       before_state, after_state, created_at
		FROM dream_logs
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO dream_logs (id, cycle_id, project_id, phase, operation, target_type, target_id,
		                        before_state, after_state, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, cycleID, projectID, phase, operation, targetType, targetID string
			beforeState, afterState                                        sql.NullString
			createdAt                                                      sql.NullString
		)
		if err := rows.Scan(&id, &cycleID, &projectID, &phase, &operation, &targetType, &targetID,
			&beforeState, &afterState, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("dream_cycles", cycleID) {
			m.skipOrphan("dream_logs", "cycle_id")
			continue
		}
		if !m.hasInserted("projects", projectID) {
			m.skipOrphan("dream_logs", "project_id")
			continue
		}
		before := "{}"
		if beforeState.Valid && beforeState.String != "" {
			before = beforeState.String
		}
		after := "{}"
		if afterState.Valid && afterState.String != "" {
			after = afterState.String
		}
		if _, err := stmt.ExecContext(ctx,
			id, cycleID, projectID, phase, operation, targetType, targetID,
			before, after,
			nullStringToInterface(createdAt),
		); err != nil {
			return fmt.Errorf("insert dream_log %s: %w", id, err)
		}
		m.markInserted("dream_logs", id)
	}
	return tx.Commit()
}

// migrateDreamLogSummaries copies the dream_log_summaries table. No-op if the
// source predates the dreaming schema.
func (m *DataMigrator) migrateDreamLogSummaries(ctx context.Context) error {
	if ok, err := m.sourceTableExists(ctx, "dream_log_summaries"); err != nil {
		return err
	} else if !ok {
		return nil
	}
	rows, err := m.src.QueryContext(ctx, `
		SELECT id, cycle_id, project_id, summary, created_at FROM dream_log_summaries
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO dream_log_summaries (id, cycle_id, project_id, summary, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, cycleID, projectID string
			summary                sql.NullString
			createdAt              sql.NullString
		)
		if err := rows.Scan(&id, &cycleID, &projectID, &summary, &createdAt); err != nil {
			return err
		}
		if !m.hasInserted("dream_cycles", cycleID) {
			m.skipOrphan("dream_log_summaries", "cycle_id")
			continue
		}
		if !m.hasInserted("projects", projectID) {
			m.skipOrphan("dream_log_summaries", "project_id")
			continue
		}
		summaryJSON := "{}"
		if summary.Valid && summary.String != "" {
			summaryJSON = summary.String
		}
		if _, err := stmt.ExecContext(ctx,
			id, cycleID, projectID, summaryJSON,
			nullStringToInterface(createdAt),
		); err != nil {
			return fmt.Errorf("insert dream_log_summary %s: %w", id, err)
		}
		m.markInserted("dream_log_summaries", id)
	}
	return tx.Commit()
}

// migrateDreamProjectDirty copies the dream_project_dirty table (keyed by project_id).
// No-op if the source predates the dreaming schema.
func (m *DataMigrator) migrateDreamProjectDirty(ctx context.Context) error {
	if ok, err := m.sourceTableExists(ctx, "dream_project_dirty"); err != nil {
		return err
	} else if !ok {
		return nil
	}
	rows, err := m.src.QueryContext(ctx, `
		SELECT project_id, dirty_since, last_dream_at FROM dream_project_dirty
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO dream_project_dirty (project_id, dirty_since, last_dream_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (project_id) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			projectID                string
			dirtySince, lastDreamAt  sql.NullString
		)
		if err := rows.Scan(&projectID, &dirtySince, &lastDreamAt); err != nil {
			return err
		}
		if !m.hasInserted("projects", projectID) {
			m.skipOrphan("dream_project_dirty", "project_id")
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			projectID,
			nullStringToInterface(dirtySince),
			nullStringToInterface(lastDreamAt),
		); err != nil {
			return fmt.Errorf("insert dream_project_dirty %s: %w", projectID, err)
		}
		m.markInsertedAnon("dream_project_dirty")
	}
	return tx.Commit()
}

// migrateWebauthnCredentials copies the webauthn_credentials table. Skips rows
// whose user_id references a missing user.
func (m *DataMigrator) migrateWebauthnCredentials(ctx context.Context) error {
	// Probe the SQLite schema for column names; the v1 webauthn table was added
	// in migration 15 with a minimal shape, and the production schema may differ.
	// Use SELECT * to tolerate column-order drift between deployments; only the
	// intersection of columns with Postgres is actually written below.
	cols, err := sqliteColumnList(ctx, m.src, "webauthn_credentials")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		// Table not present on this source — nothing to migrate.
		return nil
	}

	// Core columns we expect; missing ones become NULL.
	selectList := strings.Join(cols, ", ")
	rows, err := m.src.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM webauthn_credentials", selectList))
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := m.dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	pgCols, err := postgresColumnList(ctx, m.dst, "webauthn_credentials")
	if err != nil {
		return err
	}
	commonCols := intersectCols(cols, pgCols)
	if len(commonCols) == 0 {
		return nil
	}
	placeholders := make([]string, len(commonCols))
	for i := range commonCols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO webauthn_credentials (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		strings.Join(commonCols, ", "), strings.Join(placeholders, ", "),
	)
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Map src column name → index in SELECT list.
	srcIndex := make(map[string]int, len(cols))
	for i, c := range cols {
		srcIndex[c] = i
	}

	rawDest := make([]any, len(cols))
	raw := make([]sql.NullString, len(cols))
	for i := range raw {
		rawDest[i] = &raw[i]
	}

	userIDIdx, hasUserID := srcIndex["user_id"]
	idIdx, hasID := srcIndex["id"]

	for rows.Next() {
		if err := rows.Scan(rawDest...); err != nil {
			return err
		}
		if hasUserID {
			userID := raw[userIDIdx].String
			if !m.hasInserted("users", userID) {
				m.skipOrphan("webauthn_credentials", "user_id")
				continue
			}
		}
		args := make([]any, len(commonCols))
		for i, c := range commonCols {
			idx := srcIndex[c]
			if raw[idx].Valid {
				args[i] = raw[idx].String
			} else {
				args[i] = nil
			}
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			rowID := "?"
			if hasID {
				rowID = raw[idIdx].String
			}
			return fmt.Errorf("insert webauthn_credential %s: %w", rowID, err)
		}
		if hasID {
			m.markInserted("webauthn_credentials", raw[idIdx].String)
		} else {
			m.markInsertedAnon("webauthn_credentials")
		}
	}
	return tx.Commit()
}

// sqliteColumnList returns the ordered column names for a SQLite table, or nil
// if the table does not exist.
func sqliteColumnList(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", identQuoteSQLite(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		// PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// postgresColumnList returns the ordered column names for a Postgres table in the current schema.
func postgresColumnList(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// intersectCols returns the columns in `src` that also appear in `dst`, preserving src order.
func intersectCols(src, dst []string) []string {
	have := make(map[string]bool, len(dst))
	for _, c := range dst {
		have[c] = true
	}
	var out []string
	for _, c := range src {
		if have[c] {
			out = append(out, c)
		}
	}
	return out
}

// ── conversion helpers ────────────────────────────────────────────────────────

// jsonArrayToPostgresTextArray converts a JSON array string like `["a","b"]`
// to the Postgres array literal `{a,b}` suitable for use with ::text[].
func jsonArrayToPostgresTextArray(jsonStr string) (string, error) {
	jsonStr = strings.TrimSpace(jsonStr)
	if jsonStr == "" || jsonStr == "null" {
		return "{}", nil
	}
	var items []string
	if err := json.Unmarshal([]byte(jsonStr), &items); err != nil {
		// If it is already a Postgres array literal, pass through.
		if strings.HasPrefix(jsonStr, "{") {
			return jsonStr, nil
		}
		return "{}", fmt.Errorf("parse json array %q: %w", jsonStr, err)
	}
	if len(items) == 0 {
		return "{}", nil
	}
	// Quote each element using Postgres quoting rules.
	quoted := make([]string, len(items))
	for i, s := range items {
		quoted[i] = `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
	}
	return "{" + strings.Join(quoted, ",") + "}", nil
}

// jsonArrayToPostgresUUIDArray converts a JSON array of UUID strings to Postgres
// array literal format suitable for use with ::uuid[].
func jsonArrayToPostgresUUIDArray(jsonStr string) (string, error) {
	return jsonArrayToPostgresTextArray(jsonStr)
}

// nullStringToInterface returns nil when the NullString is invalid, or the
// string value otherwise. This allows database/sql to transmit NULL correctly.
func nullStringToInterface(ns sql.NullString) interface{} {
	if !ns.Valid {
		return nil
	}
	return ns.String
}

// nullInt64ToInterface returns nil when the NullInt64 is invalid.
func nullInt64ToInterface(ni sql.NullInt64) interface{} {
	if !ni.Valid {
		return nil
	}
	return ni.Int64
}

// textToJSONB converts a nullable SQLite TEXT column into a value safe to send
// to a Postgres JSONB column.
//
// Returns nil for SQL NULL and empty strings (Postgres treats empty JSONB as
// invalid). If the stored text parses as JSON, it is passed through verbatim
// so structured errors survive round-tripping. Otherwise the text is marshalled
// as a JSON string literal — using json.Marshal, not manual quoting, so
// backslashes, newlines, and control characters are escaped correctly
// (SQLSTATE 22P02 bait).
func textToJSONB(ns sql.NullString) (interface{}, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	// Postgres JSONB rejects \u0000 even when validly JSON-escaped — strip null
	// bytes defensively. Other control chars are fine once json.Marshal escapes them.
	s := strings.ReplaceAll(ns.String, "\x00", "")
	if s == "" {
		return nil, nil
	}
	// If the value is already valid JSON, pass it through.
	var probe json.RawMessage
	if err := json.Unmarshal([]byte(s), &probe); err == nil {
		return s, nil
	}
	// Otherwise wrap as a JSON string.
	encoded, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}
