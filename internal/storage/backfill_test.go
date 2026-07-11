package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestEnqueueUncoveredMemories_OnlyNeverEnriched verifies the durable-coverage
// contract: "uncovered" is keyed on the memories.enriched flag, NOT on the
// presence of an enrichment_queue row. This is the regression guard for the
// whole-corpus re-queue bug where a completed memory whose queue row had been
// cleared read as uncovered and got re-enqueued every dream cycle.
//
//  1. A never-enriched memory (enriched = false) with no job gets one enqueued.
//  2. An enriched memory (enriched = true) with NO queue row at all (its
//     completed row cleared by the admin clear-completed endpoint) is NOT
//     re-enqueued — the durable flag survives clearing the queue.
//  3. An enriched memory with a completed queue row still present is NOT
//     re-enqueued either.
//  4. Never-enriched memories whose only job is pending or in-flight
//     (processing) are NOT double-enqueued.
//  5. Soft-deleted memories are skipped regardless of state.
//  6. Running the backfill twice is a no-op the second time (idempotent).
//  7. Backfill jobs land at priority -1 so the worker drains them after any
//     newly-stored memories (priority 0 or higher).
func TestEnqueueUncoveredMemories_OnlyNeverEnriched(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		queueRepo := NewEnrichmentQueueRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		var ids struct {
			plainUnenriched, pendingUnenriched, processingUnenriched,
			enrichedCleared, enrichedCompleted, softDeleted uuid.UUID
		}

		// a. Never-enriched, no job → the only memory that should be enqueued.
		memA := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memA); err != nil {
			t.Fatalf("create plain unenriched memory: %v", err)
		}
		ids.plainUnenriched = memA.ID

		// b. Never-enriched, already-pending job → skipped (active job).
		memB := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memB); err != nil {
			t.Fatalf("create memory-with-pending: %v", err)
		}
		ids.pendingUnenriched = memB.ID
		existingPending := &model.EnrichmentJob{MemoryID: memB.ID, NamespaceID: nsID}
		if _, err := queueRepo.Enqueue(ctx, existingPending); err != nil {
			t.Fatalf("seed pending job: %v", err)
		}

		// c. Never-enriched, in-flight (processing) job → skipped (active job).
		//    Regression guard for the dedup predicate: the claimed status is
		//    'processing', not 'running'.
		memC := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memC); err != nil {
			t.Fatalf("create in-flight memory: %v", err)
		}
		ids.processingUnenriched = memC.ID
		processingJob := &model.EnrichmentJob{MemoryID: memC.ID, NamespaceID: nsID}
		if _, err := queueRepo.Enqueue(ctx, processingJob); err != nil {
			t.Fatalf("seed in-flight job: %v", err)
		}
		setProcessing := `UPDATE enrichment_queue SET status = 'processing' WHERE id = ?`
		if db.Backend() == BackendPostgres {
			setProcessing = `UPDATE enrichment_queue SET status = 'processing' WHERE id = $1`
		}
		if _, err := db.Exec(ctx, setProcessing, processingJob.ID.String()); err != nil {
			t.Fatalf("mark processing: %v", err)
		}

		// d. Enriched, NO queue row (completed row cleared) → must NOT re-enqueue.
		//    This is the core regression: durable coverage survives clearing the
		//    queue.
		memD := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memD); err != nil {
			t.Fatalf("create enriched-cleared memory: %v", err)
		}
		ids.enrichedCleared = memD.ID
		if err := memRepo.MarkEnriched(ctx, memD.ID, nsID, nil, nil, nil, nil, nil); err != nil {
			t.Fatalf("mark enriched (cleared): %v", err)
		}

		// e. Enriched, completed queue row still present → must NOT re-enqueue.
		memE := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memE); err != nil {
			t.Fatalf("create enriched-completed memory: %v", err)
		}
		ids.enrichedCompleted = memE.ID
		completedJob := &model.EnrichmentJob{MemoryID: memE.ID, NamespaceID: nsID}
		if _, err := queueRepo.Enqueue(ctx, completedJob); err != nil {
			t.Fatalf("seed completed job: %v", err)
		}
		if err := queueRepo.Complete(ctx, completedJob.ID, ""); err != nil {
			t.Fatalf("mark completed: %v", err)
		}
		if err := memRepo.MarkEnriched(ctx, memE.ID, nsID, nil, nil, nil, nil, nil); err != nil {
			t.Fatalf("mark enriched (completed): %v", err)
		}

		// f. Soft-deleted (never-enriched) → skipped regardless.
		memF := newTestMemory(nsID)
		if err := memRepo.Create(ctx, memF); err != nil {
			t.Fatalf("create soft-deleted memory: %v", err)
		}
		ids.softDeleted = memF.ID
		if err := memRepo.SoftDelete(ctx, memF.ID, nsID); err != nil {
			t.Fatalf("soft-delete memory: %v", err)
		}

		// Run the backfill.
		enqueued, err := EnqueueUncoveredMemories(ctx, db)
		if err != nil {
			t.Fatalf("backfill failed: %v", err)
		}
		// Expect exactly 1 new job: only plainUnenriched. The enriched memories
		// (cleared and completed) are not re-enqueued; the pending and in-flight
		// memories already have a live job; the soft-deleted one is excluded.
		if enqueued != 1 {
			t.Fatalf("first backfill: expected 1 new job, got %d", enqueued)
		}

		// Idempotency: running again must insert zero rows. The job created in
		// the first pass is pending and satisfies the LEFT JOIN ... IS NULL guard.
		enqueuedAgain, err := EnqueueUncoveredMemories(ctx, db)
		if err != nil {
			t.Fatalf("second backfill failed: %v", err)
		}
		if enqueuedAgain != 0 {
			t.Fatalf("idempotency broken: second backfill enqueued %d jobs, expected 0", enqueuedAgain)
		}

		// Verify which memories actually got jobs.
		type jobRow struct {
			memID    uuid.UUID
			priority int
			status   string
		}

		queryAll := `SELECT memory_id, priority, status FROM enrichment_queue WHERE namespace_id = ?`
		if db.Backend() == BackendPostgres {
			queryAll = `SELECT memory_id, priority, status FROM enrichment_queue WHERE namespace_id = $1`
		}
		rows, err := db.Query(ctx, queryAll, nsID.String())
		if err != nil {
			t.Fatalf("query jobs: %v", err)
		}
		defer func() { _ = rows.Close() }()

		byMem := map[uuid.UUID][]jobRow{}
		for rows.Next() {
			var memIDStr string
			var j jobRow
			if err := rows.Scan(&memIDStr, &j.priority, &j.status); err != nil {
				t.Fatalf("scan: %v", err)
			}
			mid, err := uuid.Parse(memIDStr)
			if err != nil {
				t.Fatalf("parse memory_id: %v", err)
			}
			j.memID = mid
			byMem[mid] = append(byMem[mid], j)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}

		// plainUnenriched: exactly one new backfill job at priority -1, pending.
		if got := byMem[ids.plainUnenriched]; len(got) != 1 {
			t.Errorf("plainUnenriched: expected 1 job, got %d (%v)", len(got), got)
		} else if got[0].priority != -1 || got[0].status != "pending" {
			t.Errorf("plainUnenriched: expected priority=-1 status=pending, got %+v", got[0])
		}

		// pendingUnenriched: still exactly its original priority-0 pending job.
		if got := byMem[ids.pendingUnenriched]; len(got) != 1 {
			t.Errorf("pendingUnenriched: expected 1 job (no duplicate), got %d (%v)", len(got), got)
		} else if got[0].priority != 0 || got[0].status != "pending" {
			t.Errorf("pendingUnenriched: expected original priority=0 status=pending, got %+v", got[0])
		}

		// processingUnenriched: still exactly the in-flight job (no duplicate).
		if got := byMem[ids.processingUnenriched]; len(got) != 1 {
			t.Errorf("processingUnenriched: expected 1 job (no duplicate), got %d (%v)", len(got), got)
		} else if got[0].status != "processing" {
			t.Errorf("processingUnenriched: expected the in-flight job to remain, got %+v", got[0])
		}

		// enrichedCleared: no jobs at all — the durable flag kept it out even
		// with the queue empty.
		if got := byMem[ids.enrichedCleared]; len(got) != 0 {
			t.Errorf("enrichedCleared: expected 0 jobs (durable coverage), got %d (%v)", len(got), got)
		}

		// enrichedCompleted: still exactly its original completed job, no
		// re-enqueue.
		if got := byMem[ids.enrichedCompleted]; len(got) != 1 {
			t.Errorf("enrichedCompleted: expected 1 job (no re-enqueue), got %d (%v)", len(got), got)
		} else if got[0].status != "completed" {
			t.Errorf("enrichedCompleted: expected the completed job to remain, got %+v", got[0])
		}

		// softDeleted: no jobs at all.
		if got := byMem[ids.softDeleted]; len(got) != 0 {
			t.Errorf("softDeleted: expected 0 jobs, got %d (%v)", len(got), got)
		}
	})
}

// TestNormalizeMemoryTags exercises the one-shot tag-cleaning backfill.
// Seeds rows with quoted tags via direct SQL (bypassing the repo, which
// now normalizes on write), runs the backfill, and verifies the dirty
// rows are cleaned, the already-clean rows are not rewritten, and a
// second pass is a no-op.
func TestNormalizeMemoryTags(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		// NormalizeMemoryTags is a whole-DB scan with no namespace filter, so
		// any rows other tests left behind in the shared Postgres schema
		// would be counted alongside this test's own rows.
		truncateAllForTest(t, db)
		ctx := context.Background()
		nsID := createTestNamespace(t, ctx, db)
		backend := db.Backend()

		insertSQL := `INSERT INTO memories (id, namespace_id, content, tags, confidence, importance, metadata, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if backend == BackendPostgres {
			insertSQL = `INSERT INTO memories (id, namespace_id, content, tags, confidence, importance, metadata, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
		}

		now := "2026-05-09T00:00:00Z"
		dirtyID := uuid.New()
		cleanID := uuid.New()
		mixedID := uuid.New()

		// Dirty: every tag wrapped in literal quote characters.
		dirtyEncoded := encodeStringArray(backend, []string{
			`"behavioral contract"`,
			`"failure modes"`,
		})
		// Clean: already in normalized form.
		cleanEncoded := encodeStringArray(backend, []string{"alpha", "beta"})
		// Mixed: some clean, some dirty, plus a duplicate after normalization.
		mixedEncoded := encodeStringArray(backend, []string{
			"behavioral-contract",
			`"behavioral contract"`,
			"Claude",
			`"Claude"`,
		})

		for _, row := range []struct {
			id      uuid.UUID
			tags    string
			content string
		}{
			{dirtyID, dirtyEncoded, "dirty"},
			{cleanID, cleanEncoded, "clean"},
			{mixedID, mixedEncoded, "mixed"},
		} {
			if _, err := db.Exec(ctx, insertSQL,
				row.id.String(), nsID.String(), row.content, row.tags,
				1.0, 0.5, "{}", now, now,
			); err != nil {
				t.Fatalf("seed %s: %v", row.content, err)
			}
		}

		updated, err := NormalizeMemoryTags(ctx, db)
		if err != nil {
			t.Fatalf("backfill: %v", err)
		}
		// dirty + mixed both change; clean does not.
		if updated != 2 {
			t.Fatalf("first pass: want 2 rows updated, got %d", updated)
		}

		// Second pass is a no-op.
		updatedAgain, err := NormalizeMemoryTags(ctx, db)
		if err != nil {
			t.Fatalf("backfill (idempotent run): %v", err)
		}
		if updatedAgain != 0 {
			t.Fatalf("second pass: want 0 rows updated (idempotent), got %d", updatedAgain)
		}

		// Verify each row's stored tags.
		repo := NewMemoryRepo(db)
		dirtyMem, err := repo.GetByID(ctx, dirtyID, nsID)
		if err != nil {
			t.Fatalf("reload dirty: %v", err)
		}
		if !reflect.DeepEqual(dirtyMem.Tags, []string{"behavioral contract", "failure modes"}) {
			t.Errorf("dirty tags: got %v, want [behavioral contract, failure modes]", dirtyMem.Tags)
		}

		cleanMem, err := repo.GetByID(ctx, cleanID, nsID)
		if err != nil {
			t.Fatalf("reload clean: %v", err)
		}
		if !reflect.DeepEqual(cleanMem.Tags, []string{"alpha", "beta"}) {
			t.Errorf("clean tags: got %v, want [alpha, beta]", cleanMem.Tags)
		}

		mixedMem, err := repo.GetByID(ctx, mixedID, nsID)
		if err != nil {
			t.Fatalf("reload mixed: %v", err)
		}
		if !reflect.DeepEqual(mixedMem.Tags, []string{"behavioral-contract", "behavioral contract", "Claude"}) {
			t.Errorf("mixed tags: got %v, want [behavioral-contract, behavioral contract, Claude]", mixedMem.Tags)
		}
	})
}

// TestEnqueueAllLiveMemories_EnqueuesEveryLiveMemory exercises the
// model-switch cascade entry point. Unlike EnqueueUncoveredMemories (which
// dedups against existing pending or in-flight jobs), the force-reembed path
// enqueues every live memory that does not already hold an unclaimed-pending
// job: the partial unique index dedups those (the pending job will re-embed it
// against the wiped vector store), while a memory whose only job is in-flight
// ('processing') still gets a fresh pending row.
func TestEnqueueAllLiveMemories_EnqueuesEveryLiveMemory(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		// EnqueueAllLiveMemories is a whole-DB scan; under the shared
		// Postgres schema, prior tests' live memories would inflate the count.
		truncateAllForTest(t, db)
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		queueRepo := NewEnrichmentQueueRepo(db)

		nsID := createTestNamespace(t, ctx, db)

		// Three live memories (one with a pre-existing pending job),
		// one soft-deleted (must be skipped).
		memA := newTestMemory(nsID)
		memB := newTestMemory(nsID)
		memC := newTestMemory(nsID)
		memD := newTestMemory(nsID)
		for _, m := range []*model.Memory{memA, memB, memC, memD} {
			if err := memRepo.Create(ctx, m); err != nil {
				t.Fatalf("create memory: %v", err)
			}
		}
		// Pre-existing pending job on memB; backfill must still enqueue.
		if _, err := queueRepo.Enqueue(ctx, &model.EnrichmentJob{MemoryID: memB.ID, NamespaceID: nsID}); err != nil {
			t.Fatalf("seed pending: %v", err)
		}
		if err := memRepo.SoftDelete(ctx, memD.ID, nsID); err != nil {
			t.Fatalf("soft-delete: %v", err)
		}

		enqueued, err := EnqueueAllLiveMemories(ctx, db)
		if err != nil {
			t.Fatalf("force re-embed enqueue: %v", err)
		}
		// Two newly-enqueued jobs (memA, memC). memB is skipped because it
		// already holds an unclaimed-pending job (the partial unique index
		// dedups it); memD is skipped as soft-deleted. Every live memory still
		// ends up with a pending job.
		if enqueued != 2 {
			t.Fatalf("expected 2 force-enqueued jobs, got %d", enqueued)
		}

		// Verify per-memory job counts. memB keeps exactly its original pending
		// job (force-all skipped it via ON CONFLICT); memA/memC each get one.
		query := `SELECT memory_id, COUNT(*) FROM enrichment_queue WHERE namespace_id = ? GROUP BY memory_id`
		if db.Backend() == BackendPostgres {
			query = `SELECT memory_id, COUNT(*) FROM enrichment_queue WHERE namespace_id = $1 GROUP BY memory_id`
		}
		rows, err := db.Query(ctx, query, nsID.String())
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		defer func() { _ = rows.Close() }()
		got := map[uuid.UUID]int{}
		for rows.Next() {
			var idStr string
			var n int
			if err := rows.Scan(&idStr, &n); err != nil {
				t.Fatalf("scan: %v", err)
			}
			id, _ := uuid.Parse(idStr)
			got[id] = n
		}
		if got[memA.ID] != 1 {
			t.Errorf("memA: want 1 job, got %d", got[memA.ID])
		}
		if got[memB.ID] != 1 {
			t.Errorf("memB: want 1 job (original pending; force skipped via dedup), got %d", got[memB.ID])
		}
		if got[memC.ID] != 1 {
			t.Errorf("memC: want 1 job, got %d", got[memC.ID])
		}
		if _, ok := got[memD.ID]; ok {
			t.Errorf("memD soft-deleted must not be enqueued, got %d jobs", got[memD.ID])
		}
	})
}

// TestListDreamEntityBackfillCandidates pins the candidate gate that drives the
// consolidation-entity backfill: a consolidation dream is a candidate only when
// it has NEITHER an entity_extracted_at stamp NOR a relationship sourced by it.
// The stamp is the convergence gate — without it an entity-only synthesis
// (entities but no relationships) would be re-selected and re-extracted forever.
func TestListDreamEntityBackfillCandidates(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		memRepo := NewMemoryRepo(db)
		entityRepo := NewEntityRepo(db)
		relRepo := NewRelationshipRepo(db)
		nsID := createTestNamespace(t, ctx, db)

		// newConsolidationDream creates an origin=dream memory carrying a
		// non-empty source_memory_ids metadata array (the discriminator).
		newConsolidationDream := func(t *testing.T) *model.Memory {
			t.Helper()
			m := newTestMemory(nsID)
			m.Origin = model.OriginDream
			m.Metadata = json.RawMessage(fmt.Sprintf(`{"%s":["%s"]}`, model.DreamMetaSourceMemoryIDs, uuid.New().String()))
			if err := memRepo.Create(ctx, m); err != nil {
				t.Fatalf("create consolidation dream: %v", err)
			}
			return m
		}

		// a. Uncovered consolidation dream: NULL stamp, no relationship → the
		//    only memory that should be selected.
		uncovered := newConsolidationDream(t)

		// b. Stamped dream: entity extraction has run (entity_extracted_at set)
		//    but produced no relationship — the entity-only case. Must NOT be
		//    selected; this is what closes the re-extraction loop.
		stampedOnly := newConsolidationDream(t)
		now := time.Now().UTC()
		if err := memRepo.MarkEnriched(ctx, stampedOnly.ID, nsID, nil, nil, nil, nil, &now); err != nil {
			t.Fatalf("stamp entity_extracted_at: %v", err)
		}

		// c. Covered-by-relationship dream: NULL stamp but a relationship is
		//    sourced by it (e.g. a manual-runbook recovery predating the column).
		//    Must NOT be selected.
		withRel := newConsolidationDream(t)
		entA := &model.Entity{NamespaceID: nsID, Name: "Alice", Canonical: "alice", EntityType: "person"}
		if err := entityRepo.Upsert(ctx, entA); err != nil {
			t.Fatalf("upsert entity A: %v", err)
		}
		entB := &model.Entity{NamespaceID: nsID, Name: "Acme", Canonical: "acme", EntityType: "organization"}
		if err := entityRepo.Upsert(ctx, entB); err != nil {
			t.Fatalf("upsert entity B: %v", err)
		}
		relMemID := withRel.ID
		if err := relRepo.Create(ctx, &model.Relationship{
			ID:           uuid.New(),
			NamespaceID:  nsID,
			SourceID:     entA.ID,
			TargetID:     entB.ID,
			Relation:     "member of",
			Weight:       1,
			SourceMemory: &relMemID,
			ValidFrom:    now,
			CreatedAt:    now,
		}); err != nil {
			t.Fatalf("create relationship: %v", err)
		}

		// d. Non-consolidation dream: origin=dream but NO source_memory_ids.
		//    Excluded by the discriminator.
		plainDream := newTestMemory(nsID)
		plainDream.Origin = model.OriginDream
		plainDream.Metadata = json.RawMessage(`{"nram_kind":"project_description"}`)
		if err := memRepo.Create(ctx, plainDream); err != nil {
			t.Fatalf("create non-consolidation dream: %v", err)
		}

		// e. Superseded consolidation dream: live filter excludes it.
		superseded := newConsolidationDream(t)
		survivor := newConsolidationDream(t) // also a valid candidate; see assertion
		if err := memRepo.MarkSupersededBy(ctx, superseded.ID, nsID, survivor.ID); err != nil {
			t.Fatalf("supersede dream: %v", err)
		}

		got, err := memRepo.ListDreamEntityBackfillCandidates(ctx, []uuid.UUID{nsID}, 0)
		if err != nil {
			t.Fatalf("ListDreamEntityBackfillCandidates: %v", err)
		}

		gotIDs := make(map[uuid.UUID]bool, len(got))
		for _, c := range got {
			gotIDs[c.ID] = true
		}

		// uncovered and survivor are the two valid candidates; everything else
		// is excluded by exactly one predicate.
		want := map[uuid.UUID]string{
			uncovered.ID: "uncovered",
			survivor.ID:  "survivor",
		}
		exclude := map[uuid.UUID]string{
			stampedOnly.ID: "stamped (entity_extracted_at set, no rel)",
			withRel.ID:     "covered by relationship",
			plainDream.ID:  "non-consolidation dream",
			superseded.ID:  "superseded",
		}
		for id, label := range want {
			if !gotIDs[id] {
				t.Errorf("expected %s dream %s to be a candidate, but it was excluded", label, id)
			}
		}
		for id, label := range exclude {
			if gotIDs[id] {
				t.Errorf("dream %s (%s) must NOT be a candidate, but it was selected", id, label)
			}
		}
		if len(got) != len(want) {
			t.Errorf("got %d candidates, want exactly %d (uncovered + survivor)", len(got), len(want))
		}
	})
}
