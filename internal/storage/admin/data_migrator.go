package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx stdlib driver
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
	pgvector "github.com/pgvector/pgvector-go"
)

// DataMigrator handles copying all data from a SQLite source DB to a Postgres target.
type DataMigrator struct {
	src *sql.DB
	dst *sql.DB
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

	return &DataMigrator{src: src, dst: dst}, nil
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

// validateCounts checks that source and destination row counts match for each table.
func (m *DataMigrator) validateCounts(ctx context.Context) error {
	for _, name := range migratedTables {
		var srcCount, dstCount int
		if err := m.src.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+name).Scan(&srcCount); err != nil {
			srcCount = 0
		}
		if err := m.dst.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+name).Scan(&dstCount); err != nil {
			return fmt.Errorf("count %s in postgres: %w", name, err)
		}
		if srcCount != dstCount {
			return fmt.Errorf("row count mismatch for %s: sqlite=%d postgres=%d", name, srcCount, dstCount)
		}
	}

	// Validate vector counts: SQLite memory_vectors → sum of Postgres memory_vectors_* tables.
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
		if srcVectors != dstVectors {
			return fmt.Errorf("row count mismatch for memory_vectors: sqlite=%d postgres(sum)=%d", srcVectors, dstVectors)
		}
	}

	return nil
}

// vectorDimensionTables maps supported dimensions to their Postgres table names.
var vectorDimensionTables = []string{
	"memory_vectors_384",
	"memory_vectors_512",
	"memory_vectors_768",
	"memory_vectors_1024",
	"memory_vectors_1536",
	"memory_vectors_3072",
}

// supportedVectorDimensions maps dimension → Postgres table name for vector migration.
var supportedVectorDimensions = map[int]string{
	384:  "memory_vectors_384",
	512:  "memory_vectors_512",
	768:  "memory_vectors_768",
	1024: "memory_vectors_1024",
	1536: "memory_vectors_1536",
	3072: "memory_vectors_3072",
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
			pgParent = parentID.String
		}
		if _, err := stmt.ExecContext(ctx, id, name, slug, kind, pgParent, path, depth, metadata, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert namespace %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, id, nsID, name, slug, settings, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert org %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, id, email, displayName,
			nullStringToInterface(passwordHash),
			orgID, nsID, role, settings, createdAt, updatedAt,
			nullStringToInterface(lastLogin),
			nullStringToInterface(disabledAt),
		); err != nil {
			return fmt.Errorf("insert user %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, id, userID, keyPrefix, keyHash, name, pgScopes,
			nullStringToInterface(lastUsed),
			nullStringToInterface(expiresAt),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert api_key %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, id, nsID, ownerNsID, name, slug, description,
			pgTags, settings, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert project %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, key, value, scope,
			nullStringToInterface(updatedBy), updatedAt); err != nil {
			return fmt.Errorf("insert setting %s/%s: %w", key, scope, err)
		}
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
	}
	return tx.Commit()
}

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
		                      importance, access_count, last_accessed, expires_at, superseded_by,
		                      enriched, metadata, created_at, updated_at, deleted_at, purge_after)
		VALUES ($1, $2, $3, $4, $5, $6::text[], $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var (
			id, nsID, content  string
			embeddingDim       sql.NullInt64
			source             sql.NullString
			tagsJSON           string
			confidence         float64
			importance         float64
			accessCount        int
			lastAccessed       sql.NullString
			expiresAt          sql.NullString
			supersededBy       sql.NullString
			enriched           int
			metadata           string
			createdAt, updatedAt string
			deletedAt          sql.NullString
			purgeAfter         sql.NullString
		)
		if err := rows.Scan(&id, &nsID, &content, &embeddingDim, &source, &tagsJSON,
			&confidence, &importance, &accessCount, &lastAccessed, &expiresAt, &supersededBy,
			&enriched, &metadata, &createdAt, &updatedAt, &deletedAt, &purgeAfter); err != nil {
			return err
		}
		pgTags, err := jsonArrayToPostgresTextArray(tagsJSON)
		if err != nil {
			return fmt.Errorf("convert tags for memory %s: %w", id, err)
		}
		var pgEnriched bool
		if enriched != 0 {
			pgEnriched = true
		}
		if _, err := stmt.ExecContext(ctx,
			id, nsID, content,
			nullInt64ToInterface(embeddingDim),
			nullStringToInterface(source),
			pgTags,
			confidence, importance, accessCount,
			nullStringToInterface(lastAccessed),
			nullStringToInterface(expiresAt),
			nullStringToInterface(supersededBy),
			pgEnriched,
			metadata, createdAt, updatedAt,
			nullStringToInterface(deletedAt),
			nullStringToInterface(purgeAfter),
		); err != nil {
			return fmt.Errorf("insert memory %s: %w", id, err)
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

		table, ok := supportedVectorDimensions[dimension]
		if !ok {
			log.Printf("migrateMemoryVectors: skipping memory %s with unsupported dimension %d", memoryID, dimension)
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
		if _, err := stmt.ExecContext(ctx,
			id, nsID, name, canonical, entityType,
			nullInt64ToInterface(embeddingDim),
			properties, mentionCount, metadata, createdAt, updatedAt,
		); err != nil {
			return fmt.Errorf("insert entity %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, id, namespaceID, entityID, alias, aliasType, createdAt); err != nil {
			return fmt.Errorf("insert entity_alias %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx,
			id, nsID, srcID, tgtID, relation, weight, properties, validFrom,
			nullStringToInterface(validUntil),
			nullStringToInterface(sourceMemory),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert relationship %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx, id, namespaceID, memoryID,
			nullStringToInterface(parentID),
			relation, context, createdAt,
		); err != nil {
			return fmt.Errorf("insert memory_lineage %s: %w", id, err)
		}
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
		// Postgres error column is JSONB; wrap plain text in a JSON string if present.
		var pgErr interface{}
		if errText.Valid && errText.String != "" {
			pgErr = `"` + strings.ReplaceAll(errText.String, `"`, `\"`) + `"`
		}
		if _, err := stmt.ExecContext(ctx,
			id, nsID, source,
			nullStringToInterface(contentHash),
			rawContent, pgMemoryIDs, status, pgErr, metadata, createdAt,
		); err != nil {
			return fmt.Errorf("insert ingestion_log %s: %w", id, err)
		}
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
		// last_error is JSONB in Postgres; wrap plain text.
		var pgLastError interface{}
		if lastError.Valid && lastError.String != "" {
			pgLastError = `"` + strings.ReplaceAll(lastError.String, `"`, `\"`) + `"`
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
		if _, err := stmt.ExecContext(ctx,
			id, srcNsID, tgtNsID, permission,
			nullStringToInterface(createdBy),
			nullStringToInterface(expiresAt),
			nullStringToInterface(revokedAt),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert memory_share %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx,
			id, clientID,
			nullStringToInterface(clientSecret),
			name, pgRedirectURIs, pgGrantTypes,
			nullStringToInterface(orgID),
			pgAutoRegistered, createdAt,
		); err != nil {
			return fmt.Errorf("insert oauth_client %s: %w", id, err)
		}
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
		if _, err := stmt.ExecContext(ctx,
			code, clientID, userID, redirectURI, scope,
			nullStringToInterface(codeChallenge),
			codeChallengeMethod, expiresAt, createdAt,
			nullStringToInterface(resource),
		); err != nil {
			return fmt.Errorf("insert oauth_authorization_code %s: %w", code, err)
		}
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
		if _, err := stmt.ExecContext(ctx,
			tokenHash, clientID, userID, scope,
			nullStringToInterface(expiresAt),
			nullStringToInterface(revokedAt),
			createdAt,
		); err != nil {
			return fmt.Errorf("insert oauth_refresh_token %s: %w", tokenHash, err)
		}
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
		if _, err := stmt.ExecContext(ctx,
			id,
			nullStringToInterface(orgID),
			providerType, clientID, clientSecret,
			nullStringToInterface(issuerURL),
			pgAllowedDomains, pgAutoProvision, defaultRole, createdAt, updatedAt,
		); err != nil {
			return fmt.Errorf("insert oauth_idp_config %s: %w", id, err)
		}
	}
	return tx.Commit()
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
