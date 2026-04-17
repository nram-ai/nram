package admin

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// fkRelation describes one foreign-key relationship in the SQLite schema that
// the Postgres migration will later enforce. Any row in child whose fk column
// points at a non-existent parent row is an orphan that would abort the
// migration with a FK constraint error on insert.
type fkRelation struct {
	ChildTable  string
	ChildColumn string
	ParentTable string
	ParentCol   string
}

// String returns "child.column → parent.column" for display.
func (r fkRelation) String() string {
	return fmt.Sprintf("%s.%s → %s.%s", r.ChildTable, r.ChildColumn, r.ParentTable, r.ParentCol)
}

// sqliteFKRelations lists every FK in the SQLite baseline schema (migrations 1–17)
// that the Postgres schema also declares. Self-referential FKs (e.g. memories.superseded_by)
// are included and handled correctly by the audit query. Ordering is stable for
// deterministic output.
var sqliteFKRelations = []fkRelation{
	{"memories", "namespace_id", "namespaces", "id"},
	{"memories", "superseded_by", "memories", "id"},
	{"entities", "namespace_id", "namespaces", "id"},
	{"entity_aliases", "entity_id", "entities", "id"},
	{"entity_aliases", "namespace_id", "namespaces", "id"},
	{"relationships", "namespace_id", "namespaces", "id"},
	{"relationships", "source_id", "entities", "id"},
	{"relationships", "target_id", "entities", "id"},
	{"relationships", "source_memory", "memories", "id"},
	{"memory_lineage", "memory_id", "memories", "id"},
	{"memory_lineage", "parent_id", "memories", "id"},
	{"memory_lineage", "namespace_id", "namespaces", "id"},
	{"ingestion_log", "namespace_id", "namespaces", "id"},
	{"enrichment_queue", "memory_id", "memories", "id"},
	{"enrichment_queue", "namespace_id", "namespaces", "id"},
	{"memory_shares", "source_ns_id", "namespaces", "id"},
	{"memory_shares", "target_ns_id", "namespaces", "id"},
	{"memory_shares", "created_by", "users", "id"},
	{"token_usage", "org_id", "organizations", "id"},
	{"token_usage", "user_id", "users", "id"},
	{"token_usage", "project_id", "projects", "id"},
	{"token_usage", "namespace_id", "namespaces", "id"},
	{"token_usage", "memory_id", "memories", "id"},
	{"token_usage", "api_key_id", "api_keys", "id"},
	{"api_keys", "user_id", "users", "id"},
	{"users", "org_id", "organizations", "id"},
	{"users", "namespace_id", "namespaces", "id"},
	{"organizations", "namespace_id", "namespaces", "id"},
	{"projects", "namespace_id", "namespaces", "id"},
	{"projects", "owner_namespace_id", "namespaces", "id"},
	{"settings", "updated_by", "users", "id"},
	{"oauth_clients", "org_id", "organizations", "id"},
	{"oauth_clients", "user_id", "users", "id"},
	{"oauth_authorization_codes", "client_id", "oauth_clients", "client_id"},
	{"oauth_authorization_codes", "user_id", "users", "id"},
	{"oauth_refresh_tokens", "client_id", "oauth_clients", "client_id"},
	{"oauth_refresh_tokens", "user_id", "users", "id"},
	{"oauth_idp_configs", "org_id", "organizations", "id"},
	{"webauthn_credentials", "user_id", "users", "id"},
	{"dream_cycles", "project_id", "projects", "id"},
	{"dream_cycles", "namespace_id", "namespaces", "id"},
	{"dream_logs", "cycle_id", "dream_cycles", "id"},
	{"dream_logs", "project_id", "projects", "id"},
	{"dream_log_summaries", "cycle_id", "dream_cycles", "id"},
	{"dream_log_summaries", "project_id", "projects", "id"},
	{"dream_project_dirty", "project_id", "projects", "id"},
}

// MigrationAudit scans the SQLite source DB and reports orphan-row counts per
// FK relationship. It is a read-only operation — SQLite is never mutated.
// Rejects the request if the current backend is not SQLite, since orphans only
// matter for a SQLite→Postgres migration.
func (s *DatabaseAdminStore) MigrationAudit(ctx context.Context) (*api.MigrationAudit, error) {
	if s.db.Backend() != storage.BackendSQLite {
		return nil, fmt.Errorf("migration audit is only meaningful from SQLite; current backend is %s", s.db.Backend())
	}

	audit := &api.MigrationAudit{Backend: s.db.Backend()}

	sqliteDB := s.db.DB()
	tables, err := existingSQLiteTables(ctx, sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("enumerate sqlite tables: %w", err)
	}

	for _, rel := range sqliteFKRelations {
		if !tables[rel.ChildTable] {
			audit.Errors = append(audit.Errors, api.AuditError{
				Table:   rel.ChildTable,
				Column:  rel.ChildColumn,
				Message: "source table missing — skipped",
			})
			continue
		}
		if !tables[rel.ParentTable] {
			audit.Errors = append(audit.Errors, api.AuditError{
				Table:   rel.ChildTable,
				Column:  rel.ChildColumn,
				Message: fmt.Sprintf("parent table %q missing — skipped", rel.ParentTable),
			})
			continue
		}

		count, err := countOrphans(ctx, sqliteDB, rel)
		if err != nil {
			audit.Errors = append(audit.Errors, api.AuditError{
				Table:   rel.ChildTable,
				Column:  rel.ChildColumn,
				Message: err.Error(),
			})
			continue
		}
		if count == 0 {
			continue
		}
		audit.Orphans = append(audit.Orphans, api.OrphanCount{
			Table:      rel.ChildTable,
			Column:     rel.ChildColumn,
			References: fmt.Sprintf("%s.%s", rel.ParentTable, rel.ParentCol),
			Count:      count,
		})
		audit.TotalOrphans += count
	}

	// Sort orphans by count desc, then by table.column for stability.
	sort.Slice(audit.Orphans, func(i, j int) bool {
		if audit.Orphans[i].Count != audit.Orphans[j].Count {
			return audit.Orphans[i].Count > audit.Orphans[j].Count
		}
		if audit.Orphans[i].Table != audit.Orphans[j].Table {
			return audit.Orphans[i].Table < audit.Orphans[j].Table
		}
		return audit.Orphans[i].Column < audit.Orphans[j].Column
	})

	return audit, nil
}

// countOrphans returns the number of rows in rel.ChildTable whose rel.ChildColumn
// is non-null but does not match any rel.ParentCol value in rel.ParentTable.
//
// Identifiers come from the static sqliteFKRelations list — never from user input —
// so direct interpolation is safe. We still sanitize via identQuoteSQLite for clarity.
func countOrphans(ctx context.Context, db *sql.DB, rel fkRelation) (int, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s c
		WHERE c.%s IS NOT NULL
		  AND NOT EXISTS (SELECT 1 FROM %s p WHERE p.%s = c.%s)
	`,
		identQuoteSQLite(rel.ChildTable),
		identQuoteSQLite(rel.ChildColumn),
		identQuoteSQLite(rel.ParentTable),
		identQuoteSQLite(rel.ParentCol),
		identQuoteSQLite(rel.ChildColumn),
	)
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("query %s: %w", rel, err)
	}
	return count, nil
}

// existingSQLiteTables returns a set of table names present in the SQLite DB.
func existingSQLiteTables(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables[name] = true
	}
	return tables, rows.Err()
}

// identQuoteSQLite is an alias for quoteIdent; SQLite and Postgres share
// the same double-quote identifier-escaping rule.
func identQuoteSQLite(ident string) string {
	return quoteIdent(ident)
}
