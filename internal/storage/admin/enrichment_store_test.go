package admin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// seedEnrichmentFixture wires up org → user namespace → project namespace +
// project row + memory + enrichment_queue rows. Returns the user namespace
// ID (passed to SelfQueueStatus), the org ID (passed to OrgQueueStatus),
// the project ID (asserted on items), and the project name (asserted on
// self-tier responses, expected empty on org and admin-tier responses).
func seedEnrichmentFixture(t *testing.T, db storage.DB, ctx context.Context) (
	userNsID uuid.UUID, orgID uuid.UUID, projectID uuid.UUID, projectName string,
) {
	t.Helper()
	var orgNsID uuid.UUID
	orgID, orgNsID = insertOrgWithNamespace(t, db, ctx)

	userNsID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userNsID.String(), "alice", userNsID.String(), "user",
		"test-org/"+userNsID.String(), 1, orgNsID.String())

	projNsID := uuid.New()
	projectName = "alice-proj"
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), projectName, projectName, "project",
		"test-org/"+userNsID.String()+"/"+projectName, 2, userNsID.String())

	projectID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		projectID.String(), projectName, projectName, projNsID.String(), userNsID.String())

	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), projNsID.String(), "x")

	for _, status := range []string{"pending", "failed"} {
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status) VALUES (?, ?, ?, ?)",
			uuid.New().String(), memID.String(), projNsID.String(), status)
	}
	return
}

// TestEnrichment_SelfQueueStatus_PopulatesProjectFields asserts the
// JOIN-based self path emits both project_id and project_name on each item.
// Only self-tier callers see project names (they own every returned
// project); org and system tiers leave the field empty. The field powers
// the Project column on the user-facing enrichment queue.
func TestEnrichment_SelfQueueStatus_PopulatesProjectFields(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	userNsID, _, projectID, projectName := seedEnrichmentFixture(t, db, ctx)

	resp, err := store.SelfQueueStatus(ctx, userNsID, api.QueueListParams{})
	if err != nil {
		t.Fatalf("SelfQueueStatus: %v", err)
	}
	if resp == nil || len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %+v", resp)
	}
	for i, item := range resp.Items {
		if item.ProjectID == nil {
			t.Errorf("item[%d].ProjectID is nil; want %s", i, projectID)
			continue
		}
		if *item.ProjectID != projectID {
			t.Errorf("item[%d].ProjectID: got %s want %s", i, *item.ProjectID, projectID)
		}
		if item.ProjectName != projectName {
			t.Errorf("item[%d].ProjectName: got %q want %q", i, item.ProjectName, projectName)
		}
	}
}

// TestEnrichment_QueueStatus_AttachesPhaseMetrics asserts the read-time join
// against token_usage hydrates per-phase latency/token metrics onto each queue
// item, restricted to the canonical phase operations, in canonical order, and
// excluding non-phase operations.
func TestEnrichment_QueueStatus_AttachesPhaseMetrics(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	_, nsID := insertOrgWithNamespace(t, db, ctx)

	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), nsID.String(), "x")
	execSeed(t, db, ctx,
		"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status) VALUES (?, ?, ?, ?)",
		uuid.New().String(), memID.String(), nsID.String(), "completed")

	tokenRepo := storage.NewTokenUsageRepo(db)
	lat := 12345
	rec := func(op string, in, out int) {
		t.Helper()
		id := memID
		l := lat
		if err := tokenRepo.Record(ctx, &model.TokenUsage{
			NamespaceID:  nsID,
			Operation:    op,
			Provider:     "ollama",
			Model:        "qwen3:8b-extract",
			TokensInput:  in,
			TokensOutput: out,
			// Half the prompt served from cache, which is the realistic shape
			// for enrichment: every phase reuses one long system prompt.
			TokensCacheRead: in / 2,
			MemoryID:        &id,
			LatencyMs:       &l,
			Success:         true,
		}); err != nil {
			t.Fatalf("record %s: %v", op, err)
		}
	}
	// Recorded out of canonical order; the helper must reorder them.
	rec("entity_extraction", 580, 90)
	rec("fact_extraction", 600, 120)
	rec("ingestion_decision", 200, 30)
	rec("memorize", 1, 1) // non-phase op; must not surface

	resp, err := store.QueueStatus(ctx, api.QueueListParams{})
	if err != nil {
		t.Fatalf("QueueStatus: %v", err)
	}
	if resp == nil || len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %+v", resp)
	}

	pm := resp.Items[0].PhaseMetrics
	wantOrder := []string{"ingestion_decision", "fact_extraction", "entity_extraction"}
	if len(pm) != len(wantOrder) {
		t.Fatalf("expected %d phase metrics, got %d: %+v", len(wantOrder), len(pm), pm)
	}
	for i, w := range wantOrder {
		if pm[i].Operation != w {
			t.Fatalf("phase_metrics[%d].Operation = %q, want %q", i, pm[i].Operation, w)
		}
		if pm[i].Operation == "memorize" {
			t.Fatalf("non-phase op surfaced")
		}
	}
	if pm[1].PromptTokens != 600 || pm[1].CompletionTokens != 120 {
		t.Fatalf("fact_extraction tokens: got %d/%d want 600/120",
			pm[1].PromptTokens, pm[1].CompletionTokens)
	}
	// EnrichmentPhaseMetric is a second projection of a token_usage row,
	// separate from UsageGroup/UsageTotals; the cache columns have to be
	// carried across this mapping too.
	if pm[1].CacheReadTokens != 300 {
		t.Fatalf("fact_extraction cache_read_tokens: got %d want 300", pm[1].CacheReadTokens)
	}
	if pm[1].CacheWriteTokens != 0 {
		t.Fatalf("fact_extraction cache_write_tokens: got %d want 0", pm[1].CacheWriteTokens)
	}
	if pm[1].LatencyMs == nil || *pm[1].LatencyMs != lat {
		t.Fatalf("fact_extraction latency: got %v want %d", pm[1].LatencyMs, lat)
	}

	// Serialization guard: the UI consumes snake_case JSON keys, so confirm the
	// struct tags marshal as the TS interface and OpenAPI schema expect.
	raw, err := json.Marshal(resp.Items[0])
	if err != nil {
		t.Fatalf("marshal item: %v", err)
	}
	for _, key := range []string{
		`"phase_metrics"`, `"operation"`, `"prompt_tokens"`,
		`"completion_tokens"`, `"cache_read_tokens"`, `"cache_write_tokens"`,
		`"latency_ms"`,
	} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("marshaled item missing JSON key %s: %s", key, raw)
		}
	}
}

// TestEnrichment_QueueStatus_PhaseMetricsPreferRunKey asserts the read-time
// join scopes metrics to the exact run via token_usage.request_id (the
// model.EnrichmentRunKey stamped by the worker) rather than the timestamp
// window. A prior run of the same job wrote a fact_extraction row INSIDE the 5s
// slack window; the current run's row must win purely on the run key, and the
// prior run's tokens must not surface. The nil-request_id timestamp fallback is
// covered by TestEnrichment_QueueStatus_AttachesPhaseMetrics.
func TestEnrichment_QueueStatus_PhaseMetricsPreferRunKey(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	_, nsID := insertOrgWithNamespace(t, db, ctx)

	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), nsID.String(), "x")

	// The job is on its second attempt (attempts=1): attempt 0 failed, attempt 1
	// succeeded. The read side reconstructs EnrichmentRunKey(jobID, 1).
	jobID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, attempts) VALUES (?, ?, ?, ?, ?)",
		jobID.String(), memID.String(), nsID.String(), "completed", 1)

	currentKey := model.EnrichmentRunKey(jobID, 1)
	priorKey := model.EnrichmentRunKey(jobID, 0)

	tokenRepo := storage.NewTokenUsageRepo(db)
	rec := func(op, reqID string, in, out int) {
		t.Helper()
		id := memID
		r := reqID
		l := 100
		if err := tokenRepo.Record(ctx, &model.TokenUsage{
			NamespaceID:  nsID,
			Operation:    op,
			Provider:     "ollama",
			Model:        "qwen3:8b-extract",
			TokensInput:  in,
			TokensOutput: out,
			MemoryID:     &id,
			LatencyMs:    &l,
			Success:      true,
			RequestID:    &r,
		}); err != nil {
			t.Fatalf("record %s: %v", op, err)
		}
	}
	// Prior run (attempt 0) and current run (attempt 1) rows are recorded in the
	// same second, so both sit inside the 5s timestamp window; only the run key
	// distinguishes them.
	rec("fact_extraction", priorKey, 999, 999)   // stale run; must be excluded
	rec("fact_extraction", currentKey, 600, 120) // current run; must win
	rec("entity_extraction", currentKey, 580, 90)

	resp, err := store.QueueStatus(ctx, api.QueueListParams{})
	if err != nil {
		t.Fatalf("QueueStatus: %v", err)
	}
	if resp == nil || len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %+v", resp)
	}

	pm := resp.Items[0].PhaseMetrics
	wantOrder := []string{"fact_extraction", "entity_extraction"}
	if len(pm) != len(wantOrder) {
		t.Fatalf("expected %d phase metrics, got %d: %+v", len(wantOrder), len(pm), pm)
	}
	for i, w := range wantOrder {
		if pm[i].Operation != w {
			t.Fatalf("phase_metrics[%d].Operation = %q, want %q", i, pm[i].Operation, w)
		}
	}
	// The current run's fact_extraction tokens win; the prior run's 999/999 must
	// not appear despite falling inside the 5s window.
	if pm[0].PromptTokens != 600 || pm[0].CompletionTokens != 120 {
		t.Fatalf("fact_extraction tokens: got %d/%d want 600/120 (prior run bled through)",
			pm[0].PromptTokens, pm[0].CompletionTokens)
	}
}

// TestEnrichment_SetPaused_InvalidatesResolverCache guards the production
// wiring gap: workers and the SSE tick read enrichment.paused through the
// cached SettingsService resolver, but SetPaused writes via settingsRepo
// directly. Without an explicit cache eviction the resolver would keep serving
// the stale value until the TTL (~30s) elapsed, so pause/resume would visibly
// lag. The test primes the cache the way a worker's first poll would, flips
// the flag, and asserts the resolver observes it immediately.
func TestEnrichment_SetPaused_InvalidatesResolverCache(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	settingsSvc := service.NewSettingsService(settingsRepo)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, settingsSvc, db)

	// Prime the resolver cache with the unset (false) value.
	if settingsSvc.ResolveBool(ctx, service.SettingEnrichmentPaused, "global") {
		t.Fatal("precondition: enrichment.paused should resolve false when unset")
	}

	if err := store.SetPaused(ctx, true); err != nil {
		t.Fatalf("SetPaused(true): %v", err)
	}
	if !settingsSvc.ResolveBool(ctx, service.SettingEnrichmentPaused, "global") {
		t.Error("resolver still reports unpaused after SetPaused(true): cache not invalidated")
	}
	if paused, err := store.IsPaused(ctx); err != nil {
		t.Fatalf("IsPaused: %v", err)
	} else if !paused {
		t.Error("IsPaused = false after SetPaused(true)")
	}

	// Resume path has the same invalidation requirement.
	if err := store.SetPaused(ctx, false); err != nil {
		t.Fatalf("SetPaused(false): %v", err)
	}
	if settingsSvc.ResolveBool(ctx, service.SettingEnrichmentPaused, "global") {
		t.Error("resolver still reports paused after SetPaused(false): cache not invalidated")
	}
}

// TestEnrichment_QueueStatus_AdminEmitsProjectIDOnly asserts the admin
// (system-tier) path populates project_id but leaves project_name empty,
// matching the privacy posture: cross-tenant admin views show UUIDs only.
func TestEnrichment_QueueStatus_AdminEmitsProjectIDOnly(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	_, _, projectID, _ := seedEnrichmentFixture(t, db, ctx)

	resp, err := store.QueueStatus(ctx, api.QueueListParams{})
	if err != nil {
		t.Fatalf("QueueStatus: %v", err)
	}
	if resp == nil || len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %+v", resp)
	}
	for i, item := range resp.Items {
		if item.ProjectID == nil {
			t.Errorf("item[%d].ProjectID is nil; admin tier should still surface project_id", i)
			continue
		}
		if *item.ProjectID != projectID {
			t.Errorf("item[%d].ProjectID: got %s want %s", i, *item.ProjectID, projectID)
		}
		if item.ProjectName != "" {
			t.Errorf("item[%d].ProjectName: got %q, expected empty (admin tier emits UUID only)", i, item.ProjectName)
		}
	}
}

// seedSecondOrgEnrichmentFixture mirrors seedEnrichmentFixture but takes
// caller-supplied slugs/names so two orgs can coexist in the same test DB
// without colliding on UNIQUE(parent_id, slug) on namespaces or UNIQUE on
// organizations.slug. statuses controls how many queue rows are seeded
// (one per status string). Returns the org ID and project ID so the test
// can scope assertions to each tenant.
func seedSecondOrgEnrichmentFixture(
	t *testing.T, db storage.DB, ctx context.Context,
	orgSlug, userSlug, projectSlug string, statuses []string,
) (orgID uuid.UUID, projectID uuid.UUID) {
	t.Helper()
	orgNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth) VALUES (?, ?, ?, ?, ?, ?)",
		orgNsID.String(), orgSlug, orgSlug, "org", orgSlug, 0)
	orgID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO organizations (id, name, slug, namespace_id) VALUES (?, ?, ?, ?)",
		orgID.String(), orgSlug, orgSlug, orgNsID.String())

	userNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userNsID.String(), userSlug, userNsID.String(), "user",
		orgSlug+"/"+userNsID.String(), 1, orgNsID.String())

	projNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), projectSlug, projectSlug, "project",
		orgSlug+"/"+userNsID.String()+"/"+projectSlug, 2, userNsID.String())

	projectID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		projectID.String(), projectSlug, projectSlug, projNsID.String(), userNsID.String())

	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), projNsID.String(), "x")

	for _, status := range statuses {
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status) VALUES (?, ?, ?, ?)",
			uuid.New().String(), memID.String(), projNsID.String(), status)
	}
	return
}

// TestEnrichment_OrgQueueStatus_EmitsProjectIDOnly asserts the org-tier path
// populates project_id but leaves project_name empty. An org_owner browsing
// the org enrichment queue must not learn the names of projects owned by
// other users in the org; only the project UUID surfaces. Mirrors the
// system-tier privacy posture covered by
// TestEnrichment_QueueStatus_AdminEmitsProjectIDOnly above.
//
// Cross-tenant control: a second org is seeded with three additional queue
// rows under a distinct project. The test asserts OrgQueueStatus on the
// first org returns exactly the first org's rows (no second-org leakage)
// and that resp.Counts is scoped to the first org's subtree (not the full
// DB). The counts loop runs separate queries from the items SELECT, so
// asserting both guards both code paths against a scoping regression.
//
// Runs against both sqlite and Postgres via adminTestBackends so the
// $1/$2-placeholder branch of OrgQueueStatus also gets coverage.
func TestEnrichment_OrgQueueStatus_EmitsProjectIDOnly(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()

			queueRepo := storage.NewEnrichmentQueueRepo(db)
			settingsRepo := storage.NewSettingsRepo(db)
			store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

			_, orgAID, orgAProjectID, orgAProjectName := seedEnrichmentFixture(t, db, ctx)

			// Cross-tenant control data: a separate org with its own
			// project and queue rows. None of these must surface in
			// the orgA query.
			orgBID, orgBProjectID := seedSecondOrgEnrichmentFixture(
				t, db, ctx,
				"other-org", "bob", "bob-proj",
				[]string{"pending", "processing", "completed"},
			)

			resp, err := store.OrgQueueStatus(ctx, orgAID, api.QueueListParams{})
			if err != nil {
				t.Fatalf("OrgQueueStatus(orgA): %v", err)
			}
			if resp == nil || len(resp.Items) != 2 {
				t.Fatalf("orgA: expected 2 items, got %+v", resp)
			}
			for i, item := range resp.Items {
				if item.ProjectID == nil {
					t.Errorf("orgA item[%d].ProjectID is nil; org tier should still surface project_id", i)
					continue
				}
				if *item.ProjectID == orgBProjectID {
					t.Errorf("orgA item[%d].ProjectID leaked orgB's project %s", i, orgBProjectID)
					continue
				}
				if *item.ProjectID != orgAProjectID {
					t.Errorf("orgA item[%d].ProjectID: got %s want %s", i, *item.ProjectID, orgAProjectID)
				}
				if item.ProjectName != "" {
					t.Errorf("orgA item[%d].ProjectName: got %q, expected empty (org tier emits UUID only; fixture name was %q)", i, item.ProjectName, orgAProjectName)
				}
			}

			// Counts must be scoped to orgA's subtree. seedEnrichmentFixture
			// inserts one pending and one failed row; orgB's three rows
			// (pending+processing+completed) must not bleed in.
			if resp.Counts.Pending != 1 {
				t.Errorf("orgA Counts.Pending: got %d want 1 (leak from orgB's pending row would push this to 2)", resp.Counts.Pending)
			}
			if resp.Counts.Failed != 1 {
				t.Errorf("orgA Counts.Failed: got %d want 1", resp.Counts.Failed)
			}
			if resp.Counts.Processing != 0 {
				t.Errorf("orgA Counts.Processing: got %d want 0 (orgB has 1 processing row that must not leak)", resp.Counts.Processing)
			}
			if resp.Counts.Completed != 0 {
				t.Errorf("orgA Counts.Completed: got %d want 0 (orgB has 1 completed row that must not leak)", resp.Counts.Completed)
			}

			// Sanity check the other direction: querying orgB returns
			// only its 3 rows, none of orgA's.
			respB, err := store.OrgQueueStatus(ctx, orgBID, api.QueueListParams{})
			if err != nil {
				t.Fatalf("OrgQueueStatus(orgB): %v", err)
			}
			if respB == nil || len(respB.Items) != 3 {
				t.Fatalf("orgB: expected 3 items, got %+v", respB)
			}
			for i, item := range respB.Items {
				if item.ProjectID == nil {
					continue
				}
				if *item.ProjectID == orgAProjectID {
					t.Errorf("orgB item[%d].ProjectID leaked orgA's project %s", i, orgAProjectID)
				}
				if item.ProjectName != "" {
					t.Errorf("orgB item[%d].ProjectName: got %q, expected empty", i, item.ProjectName)
				}
			}
		})
	}
}

// TestEnrichment_SelfQueueStatus_Pagination exercises the limit/offset/sort/
// status params and the deterministic ordering tiebreaker. The 5 pending rows
// share one created_at: the exact case where, before the (created_at, id)
// tiebreaker, the DB was free to return tied rows in a different order on each
// query, making the UI table jump. The test asserts paging covers every row
// without overlap and that repeated identical queries return an identical
// ordering.
func TestEnrichment_SelfQueueStatus_Pagination(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	_, orgNsID := insertOrgWithNamespace(t, db, ctx)
	userNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userNsID.String(), "alice", userNsID.String(), "user",
		"test-org/"+userNsID.String(), 1, orgNsID.String())
	projNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), "p", "p", "project",
		"test-org/"+userNsID.String()+"/p", 2, userNsID.String())
	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), projNsID.String(), "x")

	// 5 pending rows sharing one created_at, plus 2 failed. Each pending row
	// needs its own memory: the partial unique index forbids two pending jobs
	// for one memory. The 2 failed rows can share the original memID (failed
	// rows do not participate in the pending-uniqueness constraint).
	const tied = "2026-01-01T00:00:00.000Z"
	pendingIDs := map[string]bool{}
	for i := range 5 {
		pmemID := uuid.New()
		execSeed(t, db, ctx,
			"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
			pmemID.String(), projNsID.String(), "x")
		id := uuid.New().String()
		pendingIDs[id] = true
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, attempts, created_at) VALUES (?, ?, ?, ?, ?, ?)",
			id, pmemID.String(), projNsID.String(), "pending", i, tied)
	}
	for range 2 {
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status, attempts, created_at) VALUES (?, ?, ?, ?, ?, ?)",
			uuid.New().String(), memID.String(), projNsID.String(), "failed", 0, tied)
	}

	// status filter scopes items to pending; counts still report all states.
	pendingPage, err := store.SelfQueueStatus(ctx, userNsID, api.QueueListParams{Status: "pending", Limit: 50})
	if err != nil {
		t.Fatalf("SelfQueueStatus(pending): %v", err)
	}
	if len(pendingPage.Items) != 5 {
		t.Fatalf("status=pending: got %d items, want 5", len(pendingPage.Items))
	}
	for _, it := range pendingPage.Items {
		if it.Status != "pending" {
			t.Errorf("status filter leaked a %q row", it.Status)
		}
	}
	if pendingPage.Counts.Pending != 5 || pendingPage.Counts.Failed != 2 {
		t.Errorf("counts must ignore the filter: got pending=%d failed=%d, want 5/2",
			pendingPage.Counts.Pending, pendingPage.Counts.Failed)
	}

	// Paging by limit/offset covers every pending row exactly once.
	seen := map[string]int{}
	for offset := 0; offset < 6; offset += 2 {
		page, err := store.SelfQueueStatus(ctx, userNsID,
			api.QueueListParams{Status: "pending", Limit: 2, Offset: offset})
		if err != nil {
			t.Fatalf("SelfQueueStatus(offset=%d): %v", offset, err)
		}
		for _, it := range page.Items {
			seen[it.ID.String()]++
		}
	}
	if len(seen) != 5 {
		t.Errorf("paging covered %d distinct pending rows, want 5 (overlap or gaps mean unstable ordering)", len(seen))
	}
	for id, n := range seen {
		if !pendingIDs[id] {
			t.Errorf("paging returned unexpected id %s", id)
		}
		if n != 1 {
			t.Errorf("id %s appeared %d times across pages, want 1", id, n)
		}
	}

	// Deterministic ordering: identical queries over the tied-timestamp rows
	// must return the same id order every time.
	order := func() []string {
		page, err := store.SelfQueueStatus(ctx, userNsID,
			api.QueueListParams{Status: "pending", Limit: 50, Sort: "created_at", Dir: "desc"})
		if err != nil {
			t.Fatalf("SelfQueueStatus(order): %v", err)
		}
		ids := make([]string, len(page.Items))
		for i, it := range page.Items {
			ids[i] = it.ID.String()
		}
		return ids
	}
	first, second := order(), order()
	if len(first) != 5 {
		t.Fatalf("ordering page: got %d items, want 5", len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("tied-timestamp ordering not deterministic at index %d: %s vs %s", i, first[i], second[i])
		}
	}

	// Sort by attempts ascending must be monotonic.
	asc, err := store.SelfQueueStatus(ctx, userNsID,
		api.QueueListParams{Status: "pending", Limit: 50, Sort: "attempts", Dir: "asc"})
	if err != nil {
		t.Fatalf("SelfQueueStatus(attempts asc): %v", err)
	}
	for i := 1; i < len(asc.Items); i++ {
		if asc.Items[i].Attempts < asc.Items[i-1].Attempts {
			t.Errorf("attempts asc not sorted: item[%d]=%d < item[%d]=%d",
				i, asc.Items[i].Attempts, i-1, asc.Items[i-1].Attempts)
		}
	}

	// Zero-value params resolve to the default page across all statuses.
	all, err := store.SelfQueueStatus(ctx, userNsID, api.QueueListParams{})
	if err != nil {
		t.Fatalf("SelfQueueStatus(default): %v", err)
	}
	if len(all.Items) != 7 {
		t.Errorf("default params: got %d items, want 7 (5 pending + 2 failed)", len(all.Items))
	}
}

// TestEnrichment_SelfQueueStatus_ScopedCounts asserts the single GROUP BY count
// path is scoped to the caller's namespace subtree (a second org's rows never
// leak in) and still counts rows whose memory was soft-deleted, matching the old
// join's behavior (which had no deleted_at filter). Runs on both backends so the
// $N/? placeholder branch of scopedCountByStatus is covered.
func TestEnrichment_SelfQueueStatus_ScopedCounts(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()
			queueRepo := storage.NewEnrichmentQueueRepo(db)
			settingsRepo := storage.NewSettingsRepo(db)
			store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

			// Caller: 1 pending + 1 failed (seedEnrichmentFixture).
			userNsID, _, _, _ := seedEnrichmentFixture(t, db, ctx)
			// Cross-tenant control: another org with every status.
			seedSecondOrgEnrichmentFixture(t, db, ctx, "other-org", "bob", "bob-proj",
				[]string{"pending", "processing", "completed", "failed"})

			resp, err := store.SelfQueueStatus(ctx, userNsID, api.QueueListParams{})
			if err != nil {
				t.Fatalf("SelfQueueStatus: %v", err)
			}
			if resp.Counts.Pending != 1 || resp.Counts.Failed != 1 ||
				resp.Counts.Processing != 0 || resp.Counts.Completed != 0 {
				t.Fatalf("counts not scoped to caller: %+v (want pending=1 failed=1, rest 0)", resp.Counts)
			}

			// Soft-delete the caller's memory; its queue rows must still count
			// (the count keys on enrichment_queue.namespace_id, and the old join
			// applied no deleted_at filter either).
			execSeed(t, db, ctx,
				"UPDATE memories SET deleted_at = ? WHERE namespace_id IN (SELECT id FROM namespaces WHERE path LIKE ?)",
				"2020-01-01T00:00:00Z", "test-org/"+userNsID.String()+"/%")

			resp2, err := store.SelfQueueStatus(ctx, userNsID, api.QueueListParams{})
			if err != nil {
				t.Fatalf("SelfQueueStatus after soft-delete: %v", err)
			}
			if resp2.Counts.Pending != 1 || resp2.Counts.Failed != 1 {
				t.Fatalf("soft-deleted memory should still count: %+v (want pending=1 failed=1)", resp2.Counts)
			}
		})
	}
}

// TestEnrichment_ClearFailed_ScopedAndGlobal asserts the self-tier clear deletes
// only the caller's failed rows (other tenants and non-failed statuses intact),
// and the global admin clear deletes every remaining failed row while leaving
// pending rows untouched. Runs on both backends.
func TestEnrichment_ClearFailed_ScopedAndGlobal(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()
			queueRepo := storage.NewEnrichmentQueueRepo(db)
			settingsRepo := storage.NewSettingsRepo(db)
			store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

			// Caller: 1 pending + 1 failed. Control org: 1 pending + 1 failed.
			userNsID, _, _, _ := seedEnrichmentFixture(t, db, ctx)
			seedSecondOrgEnrichmentFixture(t, db, ctx, "other-org", "bob", "bob-proj",
				[]string{"pending", "failed"})

			userPath := "test-org/" + userNsID.String()

			// Self clear: only the caller's failed row (1) is removed.
			n, err := store.SelfClearFailed(ctx, userPath, 0)
			if err != nil {
				t.Fatalf("SelfClearFailed: %v", err)
			}
			if n != 1 {
				t.Fatalf("SelfClearFailed deleted %d, want 1", n)
			}
			resp, err := store.SelfQueueStatus(ctx, userNsID, api.QueueListParams{})
			if err != nil {
				t.Fatalf("SelfQueueStatus: %v", err)
			}
			if resp.Counts.Failed != 0 || resp.Counts.Pending != 1 {
				t.Fatalf("after self clear, caller counts=%+v, want failed=0 pending=1", resp.Counts)
			}

			// The control org's failed row is untouched (system still sees 1).
			sys, err := store.QueueStatus(ctx, api.QueueListParams{})
			if err != nil {
				t.Fatalf("QueueStatus: %v", err)
			}
			if sys.Counts.Failed != 1 {
				t.Fatalf("other org's failed row should remain, system failed=%d want 1", sys.Counts.Failed)
			}

			// Global clear removes the remaining failed row; pending untouched.
			gn, err := store.ClearFailedJobs(ctx, 0)
			if err != nil {
				t.Fatalf("ClearFailedJobs: %v", err)
			}
			if gn != 1 {
				t.Fatalf("ClearFailedJobs deleted %d, want 1", gn)
			}
			sys2, err := store.QueueStatus(ctx, api.QueueListParams{})
			if err != nil {
				t.Fatalf("QueueStatus after global clear: %v", err)
			}
			if sys2.Counts.Failed != 0 {
				t.Fatalf("after global clear, system failed=%d want 0", sys2.Counts.Failed)
			}
			if sys2.Counts.Pending != 2 {
				t.Fatalf("pending rows must be untouched, system pending=%d want 2", sys2.Counts.Pending)
			}
		})
	}
}
