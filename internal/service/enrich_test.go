package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Enrich-specific mock implementations ---

type enrichMemoryReader struct {
	memories map[uuid.UUID]*model.Memory
	nsList   []model.Memory
}

func (m *enrichMemoryReader) GetByID(_ context.Context, id uuid.UUID, _ uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *enrichMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID, namespaces []uuid.UUID) ([]model.Memory, error) {
	allowed := make(map[uuid.UUID]bool, len(namespaces))
	for _, ns := range namespaces {
		allowed[ns] = true
	}
	var result []model.Memory
	for _, id := range ids {
		if mem, ok := m.memories[id]; ok && allowed[mem.NamespaceID] {
			result = append(result, *mem)
		}
	}
	return result, nil
}

func (m *enrichMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
	if offset >= len(m.nsList) {
		return nil, nil
	}
	end := min(offset+limit, len(m.nsList))
	return m.nsList[offset:end], nil
}

func (m *enrichMemoryReader) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, filters storage.MemoryListFilters, limit, offset int) ([]model.Memory, error) {
	rows := m.nsList
	if filters.HideSuperseded {
		filtered := make([]model.Memory, 0, len(rows))
		for _, mem := range rows {
			if mem.SupersededBy != nil {
				continue
			}
			filtered = append(filtered, mem)
		}
		rows = filtered
	}
	if offset >= len(rows) {
		return nil, nil
	}
	end := min(offset+limit, len(rows))
	return rows[offset:end], nil
}

type enrichProjectRepo struct {
	projects map[uuid.UUID]*model.Project
}

func (m *enrichProjectRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := m.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found")
	}
	return p, nil
}

func (m *enrichProjectRepo) GetByNamespaceID(_ context.Context, namespaceID uuid.UUID) (*model.Project, error) {
	for _, p := range m.projects {
		if p.NamespaceID == namespaceID {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found for namespace")
}

type enrichQueueRepo struct {
	jobs []*model.EnrichmentJob
}

func (m *enrichQueueRepo) Enqueue(_ context.Context, item *model.EnrichmentJob) (bool, error) {
	m.jobs = append(m.jobs, item)
	return true, nil
}

type enrichLineageQuerier struct {
	children map[uuid.UUID]uuid.UUID // child → parent
}

func (m *enrichLineageQuerier) FindParentIDs(_ context.Context, _ uuid.UUID, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	result := make(map[uuid.UUID]uuid.UUID)
	for _, id := range memoryIDs {
		if pid, ok := m.children[id]; ok {
			result[id] = pid
		}
	}
	return result, nil
}

func (m *enrichLineageQuerier) FindChildIDsByRelation(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ []string) ([]uuid.UUID, error) {
	return nil, nil
}

// --- Test helpers ---

func setupEnrichFixtures() (uuid.UUID, uuid.UUID, *enrichProjectRepo) {
	projectID := uuid.New()
	namespaceID := uuid.New()

	projects := &enrichProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projectID: {
				ID:          projectID,
				NamespaceID: namespaceID,
				Name:        "test-project",
				Slug:        "test-project",
			},
		},
	}

	return projectID, namespaceID, projects
}

func makeEnrichMemory(id uuid.UUID, nsID uuid.UUID, enriched bool) *model.Memory {
	return &model.Memory{
		ID:          id,
		NamespaceID: nsID,
		Content:     "memory content " + id.String(),
		Enriched:    enriched,
		Confidence:  1.0,
		Importance:  0.5,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

// --- Tests ---

func TestEnrich_SpecificIDs_MixedEnriched(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()

	id1 := uuid.New() // not enriched
	id2 := uuid.New() // enriched
	id3 := uuid.New() // not enriched

	mem1 := makeEnrichMemory(id1, nsID, false)
	mem2 := makeEnrichMemory(id2, nsID, true)
	mem3 := makeEnrichMemory(id3, nsID, false)

	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: mem1,
			id2: mem2,
			id3: mem3,
		},
	}

	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{id1, id2, id3},
		Priority:  5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Queued != 2 {
		t.Errorf("expected queued=2, got %d", resp.Queued)
	}
	if resp.Skipped != 1 {
		t.Errorf("expected skipped=1, got %d", resp.Skipped)
	}
	if len(queue.jobs) != 2 {
		t.Fatalf("expected 2 enqueued jobs, got %d", len(queue.jobs))
	}
	for _, job := range queue.jobs {
		if job.Status != "pending" {
			t.Errorf("expected status=pending, got %s", job.Status)
		}
		if job.MaxAttempts != 3 {
			t.Errorf("expected max_attempts=3, got %d", job.MaxAttempts)
		}
		if job.Priority != 5 {
			t.Errorf("expected priority=5, got %d", job.Priority)
		}
		if job.NamespaceID != nsID {
			t.Errorf("expected namespace_id=%s, got %s", nsID, job.NamespaceID)
		}
	}
}

func TestEnrich_AllUnEnriched(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	reader := &enrichMemoryReader{
		nsList: []model.Memory{
			*makeEnrichMemory(id1, nsID, false),
			*makeEnrichMemory(id2, nsID, true),
			*makeEnrichMemory(id3, nsID, false),
		},
	}

	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		All:       true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Queued != 2 {
		t.Errorf("expected queued=2, got %d", resp.Queued)
	}
	if resp.Skipped != 1 {
		t.Errorf("expected skipped=1, got %d", resp.Skipped)
	}
	if len(queue.jobs) != 2 {
		t.Fatalf("expected 2 enqueued jobs, got %d", len(queue.jobs))
	}
}

func TestEnrich_AllAlreadyEnriched(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()

	id1 := uuid.New()
	id2 := uuid.New()

	reader := &enrichMemoryReader{
		nsList: []model.Memory{
			*makeEnrichMemory(id1, nsID, true),
			*makeEnrichMemory(id2, nsID, true),
		},
	}

	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		All:       true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Queued != 0 {
		t.Errorf("expected queued=0, got %d", resp.Queued)
	}
	if resp.Skipped != 2 {
		t.Errorf("expected skipped=2, got %d", resp.Skipped)
	}
	if len(queue.jobs) != 0 {
		t.Errorf("expected 0 enqueued jobs, got %d", len(queue.jobs))
	}
}

func TestEnrich_ProjectNotFound(t *testing.T) {
	projects := &enrichProjectRepo{
		projects: map[uuid.UUID]*model.Project{},
	}

	reader := &enrichMemoryReader{}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	_, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: uuid.New(),
		All:       true,
	})
	if err == nil {
		t.Fatal("expected error for missing project")
	}
}

func TestEnrich_NoFilterError(t *testing.T) {
	projectID, _, projects := setupEnrichFixtures()

	reader := &enrichMemoryReader{}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	_, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
	})
	if err == nil {
		t.Fatal("expected error when neither memory_ids nor all is specified")
	}
}

func TestEnrich_MemoriesInWrongNamespaceSkipped(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()
	otherNS := uuid.New()

	id1 := uuid.New() // correct namespace
	id2 := uuid.New() // wrong namespace

	mem1 := makeEnrichMemory(id1, nsID, false)
	mem2 := makeEnrichMemory(id2, otherNS, false)

	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: mem1,
			id2: mem2,
		},
	}

	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{id1, id2},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Queued != 1 {
		t.Errorf("expected queued=1, got %d", resp.Queued)
	}
	// mem2 is not in namespace so it's silently filtered, not counted as skipped
	if len(queue.jobs) != 1 {
		t.Fatalf("expected 1 enqueued job, got %d", len(queue.jobs))
	}
	if queue.jobs[0].MemoryID != id1 {
		t.Errorf("expected job for memory %s, got %s", id1, queue.jobs[0].MemoryID)
	}
}

func TestEnrich_LatencyTracked(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()

	id1 := uuid.New()
	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: makeEnrichMemory(id1, nsID, false),
		},
	}

	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{id1},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestEnrich_ProjectIDRequired(t *testing.T) {
	reader := &enrichMemoryReader{}
	projects := &enrichProjectRepo{projects: map[uuid.UUID]*model.Project{}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	_, err := svc.Enrich(context.Background(), &EnrichRequest{
		All: true,
	})
	if err == nil {
		t.Fatal("expected error for missing project_id")
	}
}

func TestEnrich_SpecificIDs_SkipsSupersededByDefault(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()
	winnerID := uuid.New()
	loserID := uuid.New()
	loser := makeEnrichMemory(loserID, nsID, false)
	loser.SupersededBy = &winnerID

	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			winnerID: makeEnrichMemory(winnerID, nsID, false),
			loserID:  loser,
		},
	}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{winnerID, loserID},
	})
	if err != nil {
		t.Fatalf("default enrich: %v", err)
	}
	if resp.Queued != 1 {
		t.Fatalf("expected only the winner queued; got %d", resp.Queued)
	}
	if len(queue.jobs) != 1 || queue.jobs[0].MemoryID != winnerID {
		t.Fatalf("expected enqueued memory=%s; got %+v", winnerID, queue.jobs)
	}
}

func TestEnrich_All_SkipsSupersededByDefault(t *testing.T) {
	projectID, nsID, projects := setupEnrichFixtures()
	winnerID := uuid.New()
	loserID := uuid.New()
	loser := *makeEnrichMemory(loserID, nsID, false)
	loser.SupersededBy = &winnerID

	reader := &enrichMemoryReader{
		nsList: []model.Memory{
			*makeEnrichMemory(winnerID, nsID, false),
			loser,
		},
	}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	resp, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID: projectID,
		All:       true,
	})
	if err != nil {
		t.Fatalf("default enrich all: %v", err)
	}
	if resp.Queued != 1 {
		t.Fatalf("expected only the winner queued; got %d", resp.Queued)
	}

	// With IncludeSuperseded the loser is enrolled too.
	queue.jobs = nil
	respIncl, err := svc.Enrich(context.Background(), &EnrichRequest{
		ProjectID:         projectID,
		All:               true,
		IncludeSuperseded: true,
	})
	if err != nil {
		t.Fatalf("include enrich all: %v", err)
	}
	if respIncl.Queued != 2 {
		t.Fatalf("expected both rows queued with IncludeSuperseded; got %d", respIncl.Queued)
	}
}

// stubAugLister returns a fixed list of candidate IDs to exercise both
// dry-run (no enqueue) and the real backfill path without touching SQL.
type stubAugLister struct {
	ids []uuid.UUID
}

func (s *stubAugLister) ListAugmentationBackfillCandidates(_ context.Context, _ []uuid.UUID, _ int) ([]storage.BackfillCandidate, error) {
	cands := make([]storage.BackfillCandidate, len(s.ids))
	for i, id := range s.ids {
		cands[i] = storage.BackfillCandidate{ID: id}
	}
	return cands, nil
}

// stubMVLister returns a fixed list of candidate memory IDs for the
// multi-vector backfill.
type stubMVLister struct {
	ids []uuid.UUID
}

func (s *stubMVLister) ListMultiVectorBackfillCandidates(_ context.Context, _ []uuid.UUID, _ int) ([]storage.BackfillCandidate, error) {
	cands := make([]storage.BackfillCandidate, len(s.ids))
	for i, id := range s.ids {
		cands[i] = storage.BackfillCandidate{ID: id}
	}
	return cands, nil
}

func TestBackfillMultiVector_DryRunDoesNotEnqueue(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{
		id1: makeEnrichMemory(id1, nsID, true),
		id2: makeEnrichMemory(id2, nsID, true),
	}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachMultiVectorLister(&stubMVLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillMultiVector(context.Background(), &BackfillMultiVectorRequest{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if resp.CandidateCount != 2 || resp.Enqueued != 0 {
		t.Fatalf("dry run expected 2 candidates / 0 enqueued; got %d/%d", resp.CandidateCount, resp.Enqueued)
	}
	if len(queue.jobs) != 0 {
		t.Fatalf("dry run enqueued %d jobs; expected 0", len(queue.jobs))
	}
}

func TestBackfillMultiVector_Enqueues(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{
		id1: makeEnrichMemory(id1, nsID, true),
		id2: makeEnrichMemory(id2, nsID, true),
	}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachMultiVectorLister(&stubMVLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillMultiVector(context.Background(), &BackfillMultiVectorRequest{})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if resp.Enqueued != 2 || len(queue.jobs) != 2 {
		t.Fatalf("expected 2 enqueued/2 jobs; got %d/%d", resp.Enqueued, len(queue.jobs))
	}
	gotIDs := map[uuid.UUID]bool{queue.jobs[0].MemoryID: true, queue.jobs[1].MemoryID: true}
	if !gotIDs[id1] || !gotIDs[id2] {
		t.Fatalf("enqueued job memory IDs %v don't cover both candidates", gotIDs)
	}

	// Each enqueued job must carry the multi-vector sentinel in StepsCompleted
	// so the worker routes ONLY to the lean facet sweep (no SGLang, no
	// whole-memory re-embed).
	for i, job := range queue.jobs {
		var steps []string
		if err := json.Unmarshal(job.StepsCompleted, &steps); err != nil {
			t.Fatalf("job[%d] StepsCompleted is not a JSON array: %v (%s)", i, err, string(job.StepsCompleted))
		}
		found := false
		for _, s := range steps {
			if s == model.JobMarkerOnlyMultiVector {
				found = true
			}
		}
		if !found {
			t.Errorf("job[%d] missing sentinel %q in steps_completed %v",
				i, model.JobMarkerOnlyMultiVector, steps)
		}
	}
}

// TestBackfillAugmentation_EnqueuesNoMarker pins that the augmentation backfill
// shares runBackfill but enqueues plain full-pipeline jobs: the marker param is
// empty, so StepsCompleted stays nil and the worker runs the re-embed path.
func TestBackfillAugmentation_EnqueuesNoMarker(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1 := uuid.New()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{
		id1: makeEnrichMemory(id1, nsID, true),
	}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachAugmentationLister(&stubAugLister{ids: []uuid.UUID{id1}})

	if _, err := svc.BackfillAugmentation(context.Background(), &BackfillAugmentationRequest{}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if len(queue.jobs) != 1 {
		t.Fatalf("expected 1 job; got %d", len(queue.jobs))
	}
	if queue.jobs[0].StepsCompleted != nil {
		t.Errorf("augmentation job should carry no marker; got StepsCompleted %s",
			string(queue.jobs[0].StepsCompleted))
	}
}

type stubMissingEmbLister struct {
	ids []uuid.UUID
}

func (s *stubMissingEmbLister) ListMissingEmbeddingCandidates(_ context.Context, _ []uuid.UUID, _ int) ([]storage.BackfillCandidate, error) {
	cands := make([]storage.BackfillCandidate, len(s.ids))
	for i, id := range s.ids {
		cands[i] = storage.BackfillCandidate{ID: id}
	}
	return cands, nil
}

// TestBackfillMissingEmbeddings_EnqueuesNoMarker pins that the missing-embedding
// repair enqueues plain full-pipeline jobs (no sentinel): the worker skips
// extraction for an already-enriched memory and runs the re-embed + finalize
// path, restoring the vector.
func TestBackfillMissingEmbeddings_EnqueuesNoMarker(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{
		id1: makeEnrichMemory(id1, nsID, true),
		id2: makeEnrichMemory(id2, nsID, true),
	}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachMissingEmbeddingLister(&stubMissingEmbLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillMissingEmbeddings(context.Background(), &BackfillMissingEmbeddingsRequest{})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if resp.Enqueued != 2 || len(queue.jobs) != 2 {
		t.Fatalf("expected 2 enqueued/2 jobs; got %d/%d", resp.Enqueued, len(queue.jobs))
	}
	for i, job := range queue.jobs {
		if job.StepsCompleted != nil {
			t.Errorf("missing-embedding job[%d] should carry no marker; got StepsCompleted %s",
				i, string(job.StepsCompleted))
		}
	}
}

func TestBackfillMissingEmbeddings_DryRunDoesNotEnqueue(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1 := uuid.New()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{
		id1: makeEnrichMemory(id1, nsID, true),
	}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachMissingEmbeddingLister(&stubMissingEmbLister{ids: []uuid.UUID{id1}})

	resp, err := svc.BackfillMissingEmbeddings(context.Background(), &BackfillMissingEmbeddingsRequest{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if resp.CandidateCount != 1 || resp.Enqueued != 0 || len(queue.jobs) != 0 {
		t.Fatalf("dry run expected 1 candidate / 0 enqueued / 0 jobs; got %d/%d/%d",
			resp.CandidateCount, resp.Enqueued, len(queue.jobs))
	}
}

func TestBackfillMissingEmbeddings_NoListerReturnsError(t *testing.T) {
	_, _, projects := setupEnrichFixtures()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	if _, err := svc.BackfillMissingEmbeddings(context.Background(), &BackfillMissingEmbeddingsRequest{}); err == nil {
		t.Fatal("expected error when missing-embedding lister is not attached")
	}
}

func TestBackfillMultiVector_NoListerReturnsError(t *testing.T) {
	_, _, projects := setupEnrichFixtures()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	if _, err := svc.BackfillMultiVector(context.Background(), &BackfillMultiVectorRequest{}); err == nil {
		t.Fatal("expected error when multi-vector lister is unwired")
	}
}

func TestBackfillAugmentation_DryRunDoesNotEnqueue(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: makeEnrichMemory(id1, nsID, true),
			id2: makeEnrichMemory(id2, nsID, true),
		},
	}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachAugmentationLister(&stubAugLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillAugmentation(context.Background(), &BackfillAugmentationRequest{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if resp.CandidateCount != 2 || resp.Enqueued != 0 {
		t.Fatalf("dry run expected 2 candidates / 0 enqueued; got %d/%d", resp.CandidateCount, resp.Enqueued)
	}
	if len(queue.jobs) != 0 {
		t.Fatalf("dry run enqueued %d jobs; expected 0", len(queue.jobs))
	}
}

func TestBackfillAugmentation_EnqueuesEvenWhenEnriched(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	// Both memories are already enriched. The point of the backfill is to
	// re-embed them with augmentation; per-step idempotency in the worker
	// will skip fact/entity extraction but still re-run embedding.
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: makeEnrichMemory(id1, nsID, true),
			id2: makeEnrichMemory(id2, nsID, true),
		},
	}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachAugmentationLister(&stubAugLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillAugmentation(context.Background(), &BackfillAugmentationRequest{})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if resp.Enqueued != 2 {
		t.Fatalf("expected 2 enqueued; got %d", resp.Enqueued)
	}
	if len(queue.jobs) != 2 {
		t.Fatalf("expected 2 jobs in queue; got %d", len(queue.jobs))
	}
	// Spot-check the enqueued job points at one of the candidate memories.
	gotIDs := map[uuid.UUID]bool{queue.jobs[0].MemoryID: true, queue.jobs[1].MemoryID: true}
	if !gotIDs[id1] || !gotIDs[id2] {
		t.Fatalf("enqueued job memory IDs %v don't cover both candidates", gotIDs)
	}
}

func TestBackfillAugmentation_NoListerReturnsError(t *testing.T) {
	_, _, projects := setupEnrichFixtures()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	_, err := svc.BackfillAugmentation(context.Background(), &BackfillAugmentationRequest{})
	if err == nil {
		t.Fatalf("expected error when lister is unwired; backfill silently no-oping would mask deployment bugs")
	}
}

// stubParaphraseLister returns a fixed list of candidate parent IDs.
type stubParaphraseLister struct {
	ids []uuid.UUID
}

func (s *stubParaphraseLister) ListEnrichedParentsWithExtractedChildren(_ context.Context, _ []uuid.UUID, _ int) ([]storage.BackfillCandidate, error) {
	cands := make([]storage.BackfillCandidate, len(s.ids))
	for i, id := range s.ids {
		cands[i] = storage.BackfillCandidate{ID: id}
	}
	return cands, nil
}

func TestBackfillExtractedFactParaphrase_DryRunDoesNotEnqueue(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: makeEnrichMemory(id1, nsID, true),
			id2: makeEnrichMemory(id2, nsID, true),
		},
	}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachParaphraseCandidateLister(&stubParaphraseLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillExtractedFactParaphrase(context.Background(), &BackfillExtractedFactParaphraseRequest{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if resp.CandidateCount != 2 || resp.Enqueued != 0 {
		t.Fatalf("dry run expected 2 candidates / 0 enqueued; got %d/%d", resp.CandidateCount, resp.Enqueued)
	}
	if len(queue.jobs) != 0 {
		t.Fatalf("dry run enqueued %d jobs; expected 0", len(queue.jobs))
	}
}

func TestBackfillExtractedFactParaphrase_EnqueuesMarkerOnly(t *testing.T) {
	_, nsID, projects := setupEnrichFixtures()
	id1, id2 := uuid.New(), uuid.New()
	reader := &enrichMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: makeEnrichMemory(id1, nsID, true),
			id2: makeEnrichMemory(id2, nsID, true),
		},
	}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})
	svc.AttachParaphraseCandidateLister(&stubParaphraseLister{ids: []uuid.UUID{id1, id2}})

	resp, err := svc.BackfillExtractedFactParaphrase(context.Background(), &BackfillExtractedFactParaphraseRequest{})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if resp.Enqueued != 2 {
		t.Fatalf("expected 2 enqueued; got %d", resp.Enqueued)
	}
	if len(queue.jobs) != 2 {
		t.Fatalf("expected 2 jobs in queue; got %d", len(queue.jobs))
	}

	// Each enqueued job must carry the paraphrase-guard sentinel in
	// StepsCompleted so the worker routes ONLY to the sweep handler.
	for i, job := range queue.jobs {
		var steps []string
		if err := json.Unmarshal(job.StepsCompleted, &steps); err != nil {
			t.Fatalf("job[%d] StepsCompleted is not a JSON array: %v (%s)", i, err, string(job.StepsCompleted))
		}
		found := false
		for _, s := range steps {
			if s == model.JobMarkerOnlyParaphraseGuard {
				found = true
			}
		}
		if !found {
			t.Errorf("job[%d] missing sentinel %q in steps_completed %v",
				i, model.JobMarkerOnlyParaphraseGuard, steps)
		}
	}
}

func TestBackfillExtractedFactParaphrase_NoListerReturnsError(t *testing.T) {
	_, _, projects := setupEnrichFixtures()
	reader := &enrichMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	queue := &enrichQueueRepo{}
	svc := NewEnrichService(reader, projects, queue, &enrichLineageQuerier{children: map[uuid.UUID]uuid.UUID{}})

	_, err := svc.BackfillExtractedFactParaphrase(context.Background(), &BackfillExtractedFactParaphraseRequest{})
	if err == nil {
		t.Fatalf("expected error when paraphrase lister is unwired")
	}
}
