package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- Enrich-specific mock implementations ---

type enrichMemoryReader struct {
	memories map[uuid.UUID]*model.Memory
	nsList   []model.Memory
}

func (m *enrichMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *enrichMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var result []model.Memory
	for _, id := range ids {
		if mem, ok := m.memories[id]; ok {
			result = append(result, *mem)
		}
	}
	return result, nil
}

func (m *enrichMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
	if offset >= len(m.nsList) {
		return nil, nil
	}
	end := offset + limit
	if end > len(m.nsList) {
		end = len(m.nsList)
	}
	return m.nsList[offset:end], nil
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

type enrichQueueRepo struct {
	jobs []*model.EnrichmentJob
}

func (m *enrichQueueRepo) Enqueue(_ context.Context, item *model.EnrichmentJob) error {
	m.jobs = append(m.jobs, item)
	return nil
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
