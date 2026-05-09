package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// setupTestDB opens a fresh SQLite database in a temp directory and applies
// every migration. Returns the storage.DB; the *testing.T cleanup handler
// closes the connection and restores the working directory.
func setupTestDB(t *testing.T) storage.DB {
	t.Helper()
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return db
}

// fixtures bundles the dependencies the cascade needs and exposes the IDs of
// the seeded project, its namespace, and the corresponding global project.
type fixtures struct {
	db              storage.DB
	svc             *ProjectDeleteService
	projectRepo     *storage.ProjectRepo
	memoryRepo      *storage.MemoryRepo
	lineageRepo     *storage.MemoryLineageRepo
	entityRepo      *storage.EntityRepo
	entityAliasRepo *storage.EntityAliasRepo
	relRepo         *storage.RelationshipRepo
	nsRepo          *storage.NamespaceRepo
	tokenRepo       *storage.TokenUsageRepo
	dreamCycleRepo  *storage.DreamCycleRepo
	target          *model.Project
	global          *model.Project
}

// seedProject creates the namespaces and projects needed for a cascade test
// (target project plus its owner's "global" project). All repos are wired
// into a real ProjectDeleteService.
func seedProject(t *testing.T) *fixtures {
	t.Helper()
	ctx := context.Background()
	db := setupTestDB(t)

	nsRepo := storage.NewNamespaceRepo(db)
	projectRepo := storage.NewProjectRepo(db)
	memoryRepo := storage.NewMemoryRepo(db)
	lineageRepo := storage.NewMemoryLineageRepo(db)
	entityRepo := storage.NewEntityRepo(db)
	entityAliasRepo := storage.NewEntityAliasRepo(db)
	relRepo := storage.NewRelationshipRepo(db)
	enrichRepo := storage.NewEnrichmentQueueRepo(db)
	tokenRepo := storage.NewTokenUsageRepo(db)
	dreamCycleRepo := storage.NewDreamCycleRepo(db)
	ingestRepo := storage.NewIngestionLogRepo(db)
	shareRepo := storage.NewMemoryShareRepo(db)

	// Owner namespace (kind=user); target and global project namespaces hang
	// off it. Migrations seed the root (zero-UUID) namespace, so the org slot
	// would be the parent of the owner; for cascade purposes a flat layout is
	// enough — the cascade only deletes the target project's own namespace.
	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	ownerID := uuid.New()
	owner := &model.Namespace{
		ID:       ownerID,
		Name:     "owner",
		Slug:     "owner-" + uuid.NewString()[:8],
		Kind:     "user",
		ParentID: &rootID,
		Path:     ownerID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, owner); err != nil {
		t.Fatalf("create owner ns: %v", err)
	}

	targetNSID := uuid.New()
	targetNS := &model.Namespace{
		ID:       targetNSID,
		Name:     "target",
		Slug:     "target-" + uuid.NewString()[:8],
		Kind:     "project",
		ParentID: &ownerID,
		Path:     ownerID.String() + "/" + targetNSID.String(),
		Depth:    2,
	}
	if err := nsRepo.Create(ctx, targetNS); err != nil {
		t.Fatalf("create target ns: %v", err)
	}

	globalNSID := uuid.New()
	globalNS := &model.Namespace{
		ID:       globalNSID,
		Name:     "global",
		Slug:     "global-" + uuid.NewString()[:8],
		Kind:     "project",
		ParentID: &ownerID,
		Path:     ownerID.String() + "/" + globalNSID.String(),
		Depth:    2,
	}
	if err := nsRepo.Create(ctx, globalNS); err != nil {
		t.Fatalf("create global ns: %v", err)
	}

	target := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      targetNSID,
		OwnerNamespaceID: ownerID,
		Slug:             "target",
		Name:             "Target",
	}
	if err := projectRepo.Create(ctx, target); err != nil {
		t.Fatalf("create target project: %v", err)
	}

	global := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      globalNSID,
		OwnerNamespaceID: ownerID,
		Slug:             "global",
		Name:             "Global",
	}
	if err := projectRepo.Create(ctx, global); err != nil {
		t.Fatalf("create global project: %v", err)
	}

	svc := NewProjectDeleteService(
		db,
		projectRepo, projectRepo,
		memoryRepo, lineageRepo, memoryRepo,
		nil, // vector store is post-commit best-effort; not exercised here
		entityAliasRepo, entityRepo, relRepo, enrichRepo,
		tokenRepo, ingestRepo, shareRepo,
		nil, // hnsw deleter is exercised in a dedicated test
		nsRepo,
		nil,
	)

	return &fixtures{
		db:              db,
		svc:             svc,
		projectRepo:     projectRepo,
		memoryRepo:      memoryRepo,
		lineageRepo:     lineageRepo,
		entityRepo:      entityRepo,
		entityAliasRepo: entityAliasRepo,
		relRepo:         relRepo,
		nsRepo:          nsRepo,
		tokenRepo:       tokenRepo,
		dreamCycleRepo:  dreamCycleRepo,
		target:          target,
		global:          global,
	}
}

// countRows runs a parameterized SELECT COUNT(*) and returns the result.
func countRows(t *testing.T, db storage.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// TestProjectDelete_CascadeWithFKBlockers populates a project with the exact
// FK shape that broke the original cascade: entities are referenced by
// relationships (relationships.source_id / target_id REFERENCE entities(id)
// with no ON DELETE action). With the cascade in the wrong order the entity
// delete raised a FK violation and left orphans behind. This test asserts
// that one Delete call empties every relevant table.
func TestProjectDelete_CascadeWithFKBlockers(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	memID := uuid.New()
	mem := &model.Memory{
		ID:          memID,
		NamespaceID: fx.target.NamespaceID,
		Content:     "test memory",
		Tags:        []string{"t"},
	}
	if err := fx.memoryRepo.Create(ctx, mem); err != nil {
		t.Fatalf("create memory: %v", err)
	}

	entA := &model.Entity{
		ID:          uuid.New(),
		NamespaceID: fx.target.NamespaceID,
		Name:        "A",
		Canonical:   "a",
		EntityType:  "thing",
	}
	entB := &model.Entity{
		ID:          uuid.New(),
		NamespaceID: fx.target.NamespaceID,
		Name:        "B",
		Canonical:   "b",
		EntityType:  "thing",
	}
	if err := fx.entityRepo.Create(ctx, entA); err != nil {
		t.Fatalf("create entity A: %v", err)
	}
	if err := fx.entityRepo.Create(ctx, entB); err != nil {
		t.Fatalf("create entity B: %v", err)
	}

	rel := &model.Relationship{
		ID:          uuid.New(),
		NamespaceID: fx.target.NamespaceID,
		SourceID:    entA.ID,
		TargetID:    entB.ID,
		Relation:    "knows",
		Weight:      1.0,
	}
	if err := fx.relRepo.Create(ctx, rel); err != nil {
		t.Fatalf("create relationship: %v", err)
	}

	resp, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp.DeletedMemories != 1 {
		t.Errorf("expected 1 memory in response, got %d", resp.DeletedMemories)
	}
	if resp.ProjectSlug != "target" {
		t.Errorf("expected slug target, got %s", resp.ProjectSlug)
	}

	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM memories WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected 0 memories left, got %d", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM entities WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected 0 entities left, got %d (entity-before-relationships ordering bug?)", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM relationships WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected 0 relationships left, got %d", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM projects WHERE id = ?", fx.target.ID.String()); n != 0 {
		t.Errorf("expected target project gone, found %d rows", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM namespaces WHERE id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected target namespace gone, found %d rows", n)
	}

	// Global project must still exist.
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM projects WHERE id = ?", fx.global.ID.String()); n != 1 {
		t.Errorf("expected global project intact, found %d rows", n)
	}
}

// TestProjectDelete_TokenUsageReassigned verifies that token_usage rows
// pointing at the deleted project are redirected to the owner's global
// project rather than orphaned (token_usage.project_id has no ON DELETE
// action and would otherwise block the project row delete).
func TestProjectDelete_TokenUsageReassigned(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	// Insert a token_usage row directly. The repo does not expose a method
	// for the test-relevant fields, so use a raw SQL insert with the minimum
	// required columns.
	usageID := uuid.New()
	if _, err := fx.db.Exec(ctx,
		`INSERT INTO token_usage (id, project_id, namespace_id, operation, provider, model)
			VALUES (?, ?, ?, ?, ?, ?)`,
		usageID.String(), fx.target.ID.String(), fx.target.NamespaceID.String(),
		"store", "test", "test-model",
	); err != nil {
		t.Fatalf("insert token_usage: %v", err)
	}

	if _, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var projectID, namespaceID string
	if err := fx.db.QueryRow(ctx,
		`SELECT project_id, namespace_id FROM token_usage WHERE id = ?`,
		usageID.String(),
	).Scan(&projectID, &namespaceID); err != nil {
		t.Fatalf("read token_usage: %v", err)
	}
	if projectID != fx.global.ID.String() {
		t.Errorf("expected token_usage.project_id = global %s, got %s", fx.global.ID, projectID)
	}
	if namespaceID != fx.global.NamespaceID.String() {
		t.Errorf("expected token_usage.namespace_id = global ns %s, got %s", fx.global.NamespaceID, namespaceID)
	}
}

// TestProjectDelete_CascadeWithDreamCycleTokenUsage — the project-delete
// cascade drops the project row, which CASCADE-deletes any dream_cycles
// rows. Without ON DELETE SET NULL on token_usage.cycle_id, that
// CASCADE-delete fails when token_usage rows still reference the cycle,
// rolling the whole delete back. The fix lives in postgres migration 000031
// and sqlite migration 000034. This test seeds the exact shape that broke
// the cascade and asserts the row survives with cycle_id cleared.
func TestProjectDelete_CascadeWithDreamCycleTokenUsage(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	cycle := &model.DreamCycle{
		ProjectID:   fx.target.ID,
		NamespaceID: fx.target.NamespaceID,
		Status:      model.DreamStatusCompleted,
	}
	if err := fx.dreamCycleRepo.Create(ctx, cycle); err != nil {
		t.Fatalf("create dream_cycle: %v", err)
	}

	usageID := uuid.New()
	if _, err := fx.db.Exec(ctx,
		`INSERT INTO token_usage (id, project_id, namespace_id, operation, provider, model, cycle_id)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
		usageID.String(), fx.target.ID.String(), fx.target.NamespaceID.String(),
		"dream", "test", "test-model", cycle.ID.String(),
	); err != nil {
		t.Fatalf("insert token_usage: %v", err)
	}

	if _, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var projectID, namespaceID string
	var cycleIDOut sql.NullString
	if err := fx.db.QueryRow(ctx,
		`SELECT project_id, namespace_id, cycle_id FROM token_usage WHERE id = ?`,
		usageID.String(),
	).Scan(&projectID, &namespaceID, &cycleIDOut); err != nil {
		t.Fatalf("read token_usage: %v", err)
	}
	if projectID != fx.global.ID.String() {
		t.Errorf("expected token_usage.project_id = global %s, got %s", fx.global.ID, projectID)
	}
	if namespaceID != fx.global.NamespaceID.String() {
		t.Errorf("expected token_usage.namespace_id = global ns %s, got %s", fx.global.NamespaceID, namespaceID)
	}
	if cycleIDOut.Valid {
		t.Errorf("expected token_usage.cycle_id NULL after dream_cycle CASCADE-delete, got %s", cycleIDOut.String)
	}

	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM dream_cycles WHERE id = ?", cycle.ID.String()); n != 0 {
		t.Errorf("expected dream_cycle gone, found %d rows", n)
	}
}

// TestProjectDelete_CascadeWithOrphanNamespaceTokenUsage reproduces the
// production failure shape from 2026-05-09: a token_usage row whose
// namespace_id points at the doomed namespace but whose project_id is NULL
// or points at a different project. ReassignProjectTx (keyed on project_id)
// did not catch these rows, and the namespace row delete then failed with
// SQLSTATE 23503 on token_usage_namespace_id_fkey. ReassignNamespaceTx is
// the second sweep that closes the gap.
func TestProjectDelete_CascadeWithOrphanNamespaceTokenUsage(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	// Row A: project_id NULL, namespace_id matches the doomed namespace.
	// Row B: project_id points at the global project (any non-target project),
	// namespace_id still matches the doomed namespace. Both must survive the
	// cascade with project_id and namespace_id rewritten to global.
	usageA := uuid.New()
	if _, err := fx.db.Exec(ctx,
		`INSERT INTO token_usage (id, project_id, namespace_id, operation, provider, model)
			VALUES (?, NULL, ?, ?, ?, ?)`,
		usageA.String(), fx.target.NamespaceID.String(),
		"recall", "test", "test-model",
	); err != nil {
		t.Fatalf("insert token_usage A: %v", err)
	}

	usageB := uuid.New()
	if _, err := fx.db.Exec(ctx,
		`INSERT INTO token_usage (id, project_id, namespace_id, operation, provider, model)
			VALUES (?, ?, ?, ?, ?, ?)`,
		usageB.String(), fx.global.ID.String(), fx.target.NamespaceID.String(),
		"recall", "test", "test-model",
	); err != nil {
		t.Fatalf("insert token_usage B: %v", err)
	}

	if _, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	for _, id := range []uuid.UUID{usageA, usageB} {
		var projectID, namespaceID string
		if err := fx.db.QueryRow(ctx,
			`SELECT project_id, namespace_id FROM token_usage WHERE id = ?`,
			id.String(),
		).Scan(&projectID, &namespaceID); err != nil {
			t.Fatalf("read token_usage %s: %v", id, err)
		}
		if projectID != fx.global.ID.String() {
			t.Errorf("row %s: expected project_id = global %s, got %s", id, fx.global.ID, projectID)
		}
		if namespaceID != fx.global.NamespaceID.String() {
			t.Errorf("row %s: expected namespace_id = global ns %s, got %s", id, fx.global.NamespaceID, namespaceID)
		}
	}

	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM namespaces WHERE id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected target namespace gone, found %d rows", n)
	}
}

// TestProjectDelete_CascadeWithMemoryLineage seeds a memory_lineage row in
// the doomed namespace and asserts the cascade clears it. Without the
// explicit DeleteByNamespaceTx step, memory_lineage.namespace_id (FK to
// namespaces with no ON DELETE action) would block the namespace row delete
// for any row whose namespace_id is not schema-guaranteed to match its
// parent memory's namespace_id.
func TestProjectDelete_CascadeWithMemoryLineage(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	mem := &model.Memory{
		ID:          uuid.New(),
		NamespaceID: fx.target.NamespaceID,
		Content:     "lineage parent",
	}
	if err := fx.memoryRepo.Create(ctx, mem); err != nil {
		t.Fatalf("create memory: %v", err)
	}

	lineage := &model.MemoryLineage{
		NamespaceID: fx.target.NamespaceID,
		MemoryID:    mem.ID,
		Relation:    model.LineageExtractedFact,
	}
	if err := fx.lineageRepo.Create(ctx, lineage); err != nil {
		t.Fatalf("create lineage: %v", err)
	}

	if _, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM memory_lineage WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected 0 lineage rows left, got %d", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM namespaces WHERE id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected target namespace gone, found %d rows", n)
	}
}

// TestProjectDelete_CascadeWithEntityAliases seeds an entity_aliases row in
// the doomed namespace and asserts the cascade clears it via the explicit
// DeleteByNamespaceTx step. The entity_id FK CASCADE would handle aliases
// whose namespace_id matches their parent entity's, but explicit clearing
// covers the no-schema-guarantee case for entity_aliases.namespace_id.
func TestProjectDelete_CascadeWithEntityAliases(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	ent := &model.Entity{
		ID:          uuid.New(),
		NamespaceID: fx.target.NamespaceID,
		Name:        "Aliased",
		Canonical:   "aliased",
		EntityType:  "thing",
	}
	if err := fx.entityRepo.Create(ctx, ent); err != nil {
		t.Fatalf("create entity: %v", err)
	}

	alias := &model.EntityAlias{
		NamespaceID: fx.target.NamespaceID,
		EntityID:    ent.ID,
		Alias:       "ali",
		AliasType:   "name",
	}
	if err := fx.entityAliasRepo.Create(ctx, alias); err != nil {
		t.Fatalf("create alias: %v", err)
	}

	if _, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM entity_aliases WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected 0 alias rows left, got %d", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM namespaces WHERE id = ?", fx.target.NamespaceID.String()); n != 0 {
		t.Errorf("expected target namespace gone, found %d rows", n)
	}
}

// TestProjectDelete_RejectsGlobal — deleting the global project itself is
// always refused, since the cascade redirects token_usage to global.
func TestProjectDelete_RejectsGlobal(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	_, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.global.ID})
	if err == nil {
		t.Fatal("expected error for global project")
	}
	if err.Error() != "the global project cannot be deleted" {
		t.Errorf("unexpected error: %v", err)
	}
	// Both projects must still exist.
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM projects WHERE id IN (?, ?)",
		fx.target.ID.String(), fx.global.ID.String()); n != 2 {
		t.Errorf("expected both projects intact, found %d rows", n)
	}
}

// TestProjectDelete_NoGlobalProject — if the owner has no global project
// to receive reassigned token_usage, the cascade refuses up front rather
// than orphaning billing data.
func TestProjectDelete_NoGlobalProject(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	// Drop the global project to simulate the missing-global condition. With
	// FK enforcement on, deleting the global project row also requires
	// cleaning up its namespace-scoped rows; for this test no such rows
	// exist, so the bare DELETE works.
	if _, err := fx.db.Exec(ctx, `DELETE FROM projects WHERE id = ?`, fx.global.ID.String()); err != nil {
		t.Fatalf("drop global project: %v", err)
	}

	_, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID})
	if err == nil {
		t.Fatal("expected error when global project is missing")
	}
	if !errors.Is(err, ErrNoGlobalProject) {
		t.Errorf("expected ErrNoGlobalProject, got %v", err)
	}

	// The cascade must not have run.
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM projects WHERE id = ?", fx.target.ID.String()); n != 1 {
		t.Errorf("expected target project intact, found %d rows", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM namespaces WHERE id = ?", fx.target.NamespaceID.String()); n != 1 {
		t.Errorf("expected target namespace intact, found %d rows", n)
	}
}

// TestProjectDelete_ProjectNotFound — bogus project ID returns an error
// without touching any rows.
func TestProjectDelete_ProjectNotFound(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	_, err := fx.svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: uuid.New()})
	if err == nil {
		t.Fatal("expected error for missing project")
	}
}

// TestProjectDelete_TxRollsBackOnFailure — when a deleter inside the cascade
// errors, the transaction must roll back. Nothing the cascade touched should
// remain in a half-deleted state.
func TestProjectDelete_TxRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	// Seed a memory and an entity so the cascade has visible work to do.
	memID := uuid.New()
	if err := fx.memoryRepo.Create(ctx, &model.Memory{
		ID:          memID,
		NamespaceID: fx.target.NamespaceID,
		Content:     "x",
	}); err != nil {
		t.Fatalf("create memory: %v", err)
	}
	if err := fx.entityRepo.Create(ctx, &model.Entity{
		ID:          uuid.New(),
		NamespaceID: fx.target.NamespaceID,
		Name:        "X",
		Canonical:   "x",
		EntityType:  "thing",
	}); err != nil {
		t.Fatalf("create entity: %v", err)
	}

	// Wrap the project repo so DeleteTx fails, simulating any late-cascade
	// error. The wrapper proxies every other call to the real repo.
	failProjectDeleter := &failingProjectDeleter{inner: fx.projectRepo}

	enrichRepo := storage.NewEnrichmentQueueRepo(fx.db)
	ingestRepo := storage.NewIngestionLogRepo(fx.db)
	shareRepo := storage.NewMemoryShareRepo(fx.db)
	svc := NewProjectDeleteService(
		fx.db,
		fx.projectRepo, failProjectDeleter,
		fx.memoryRepo, fx.lineageRepo, fx.memoryRepo,
		nil,
		fx.entityAliasRepo, fx.entityRepo, fx.relRepo, enrichRepo,
		fx.tokenRepo, ingestRepo, shareRepo,
		nil, fx.nsRepo, nil,
	)

	_, err := svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID})
	if err == nil {
		t.Fatal("expected error from failing project deleter")
	}

	// Roll-back assertion: every row the cascade would have removed is still
	// there. Without the transaction wrap, memories and entities would have
	// already been deleted by this point.
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM memories WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 1 {
		t.Errorf("expected memory preserved by rollback, got %d remaining", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM entities WHERE namespace_id = ?", fx.target.NamespaceID.String()); n != 1 {
		t.Errorf("expected entity preserved by rollback, got %d remaining", n)
	}
	if n := countRows(t, fx.db, "SELECT COUNT(*) FROM projects WHERE id = ?", fx.target.ID.String()); n != 1 {
		t.Errorf("expected project preserved by rollback, got %d", n)
	}
}

// failingProjectDeleter wraps a real *storage.ProjectRepo and unconditionally
// fails DeleteTx so the cascade exercises its rollback path.
type failingProjectDeleter struct {
	inner *storage.ProjectRepo
}

func (f *failingProjectDeleter) DeleteTx(_ context.Context, _ *sql.Tx, _ uuid.UUID) error {
	return fmt.Errorf("synthetic failure")
}

// TestProjectDelete_EntityVectorsCleanedFromQdrant verifies that when a
// project with embedded entities is deleted, the entity points are removed
// from Qdrant. Pre-fix, ProjectDeleteService only iterated memory IDs after
// commit; entity vectors stayed leaked indefinitely on Qdrant deployments.
// SQLite is unaffected (entity_vectors_* SQL CASCADE handles it), so this
// test specifically exercises the Qdrant code path.
func TestProjectDelete_EntityVectorsCleanedFromQdrant(t *testing.T) {
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant project-delete integration test")
	}

	ctx := context.Background()
	fx := seedProject(t)

	qstore, err := storage.NewQdrantStore(storage.QdrantConfig{Addr: addr})
	if err != nil {
		t.Fatalf("NewQdrantStore: %v", err)
	}
	t.Cleanup(func() { qstore.Close() })
	if err := qstore.EnsureCollections(ctx); err != nil {
		t.Fatalf("EnsureCollections: %v", err)
	}

	// Two entities in the target namespace, both with vectors in Qdrant.
	entA := &model.Entity{
		ID: uuid.New(), NamespaceID: fx.target.NamespaceID,
		Name: "A", Canonical: "qdrant_proj_a_" + uuid.NewString()[:8], EntityType: "thing",
	}
	entB := &model.Entity{
		ID: uuid.New(), NamespaceID: fx.target.NamespaceID,
		Name: "B", Canonical: "qdrant_proj_b_" + uuid.NewString()[:8], EntityType: "thing",
	}
	if err := fx.entityRepo.Create(ctx, entA); err != nil {
		t.Fatalf("create entity A: %v", err)
	}
	if err := fx.entityRepo.Create(ctx, entB); err != nil {
		t.Fatalf("create entity B: %v", err)
	}

	dim := 384
	emb := make([]float32, dim)
	emb[0] = 1.0
	for i := 1; i < dim; i++ {
		emb[i] = 0.01
	}
	for _, id := range []uuid.UUID{entA.ID, entB.ID} {
		if err := qstore.Upsert(ctx, storage.VectorKindEntity, id, fx.target.NamespaceID, emb, dim); err != nil {
			t.Fatalf("upsert vector %s: %v", id, err)
		}
	}
	t.Cleanup(func() {
		// Belt-and-suspenders if delete cascade doesn't reach this far.
		_ = qstore.Delete(context.Background(), storage.VectorKindEntity, entA.ID)
		_ = qstore.Delete(context.Background(), storage.VectorKindEntity, entB.ID)
	})

	// Rebuild ProjectDeleteService with the real Qdrant store wired in. The
	// fixture's svc was built with vectorStore=nil for the SQL-only tests.
	enrichRepo := storage.NewEnrichmentQueueRepo(fx.db)
	ingestRepo := storage.NewIngestionLogRepo(fx.db)
	shareRepo := storage.NewMemoryShareRepo(fx.db)
	svc := NewProjectDeleteService(
		fx.db,
		fx.projectRepo, fx.projectRepo,
		fx.memoryRepo, fx.lineageRepo, fx.memoryRepo,
		qstore,
		fx.entityAliasRepo, fx.entityRepo, fx.relRepo, enrichRepo,
		fx.tokenRepo, ingestRepo, shareRepo,
		nil, fx.nsRepo, nil,
	)

	if _, err := svc.Delete(ctx, &ProjectDeleteRequest{ProjectID: fx.target.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got, err := qstore.GetByIDs(ctx, storage.VectorKindEntity, []uuid.UUID{entA.ID, entB.ID}, dim)
	if err != nil {
		t.Fatalf("GetByIDs post-delete: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 entity vectors in Qdrant after project delete, got %d (%v)", len(got), got)
	}
}
