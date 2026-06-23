package dreaming

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

type fakeProjectReader struct {
	proj *model.Project
	err  error
}

func (f *fakeProjectReader) GetByNamespaceID(_ context.Context, _ uuid.UUID) (*model.Project, error) {
	return f.proj, f.err
}

type fakeDescLister struct {
	mems  []model.Memory
	calls int
}

func (f *fakeDescLister) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, _, _ int) ([]model.Memory, error) {
	f.calls++
	return f.mems, nil
}

type recordingDescWriter struct {
	created []*model.Memory
	deleted []uuid.UUID
}

func (w *recordingDescWriter) Create(_ context.Context, mem *model.Memory) error {
	w.created = append(w.created, mem)
	return nil
}

func (w *recordingDescWriter) SoftDelete(_ context.Context, id, _ uuid.UUID) error {
	w.deleted = append(w.deleted, id)
	return nil
}

type recordingPurger struct{ purged []uuid.UUID }

func (p *recordingPurger) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	p.purged = append(p.purged, id)
	return nil
}

func descBackingMemory(ns, projID uuid.UUID, content, sourceStamp string) model.Memory {
	meta, _ := json.Marshal(map[string]any{
		metadataKindKey:          metadataKindValue,
		metadataProjectIDKey:     projID.String(),
		metadataSourceUpdatedKey: sourceStamp,
	})
	return model.Memory{
		ID:          uuid.New(),
		NamespaceID: ns,
		Content:     content,
		Origin:      model.OriginDream,
		Tags:        []string{descriptionTag},
		Metadata:    json.RawMessage(meta),
	}
}

// runDescPhase wires the phase against fakes and runs one cycle.
func runDescPhase(t *testing.T, proj *model.Project, existing []model.Memory) (*recordingDescWriter, *enqueueRecorder, *recordingPurger) {
	t.Helper()
	reader := &fakeProjectReader{proj: proj}
	lister := &fakeDescLister{mems: existing}
	writer := &recordingDescWriter{}
	queue := &enqueueRecorder{}
	purger := &recordingPurger{}

	phase := NewProjectDescriptionPhase(reader, lister, writer, queue, nil)
	phase.AttachVectorPurger(purger)

	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: proj.ID, NamespaceID: proj.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return writer, queue, purger
}

func testProject(slug, desc string) *model.Project {
	return &model.Project{
		ID:          uuid.New(),
		NamespaceID: uuid.New(),
		Slug:        slug,
		Description: desc,
		UpdatedAt:   time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC),
	}
}

// First cycle with a substantive description backfills exactly one tagged,
// dream-origin backing memory and enqueues it for enrichment (embedding).
func TestProjectDescriptionPhase_Backfill(t *testing.T) {
	proj := testProject("ranshaw", "C++17 elliptic-curve library")
	writer, queue, purger := runDescPhase(t, proj, nil)

	if len(writer.created) != 1 {
		t.Fatalf("created %d memories, want 1", len(writer.created))
	}
	mem := writer.created[0]
	if mem.Content != proj.Description {
		t.Errorf("content = %q, want %q", mem.Content, proj.Description)
	}
	if mem.Origin != model.OriginDream {
		t.Errorf("origin = %q, want dream", mem.Origin)
	}
	if !mem.Enriched {
		t.Errorf("backing memory must be Enriched=true (dream-recursion guard)")
	}
	if len(mem.Tags) != 1 || mem.Tags[0] != descriptionTag {
		t.Errorf("tags = %v, want [%s]", mem.Tags, descriptionTag)
	}
	if !isProjectDescription(mem) {
		t.Errorf("created memory must carry the project_description marker")
	}
	meta := decodeMetadata(mem.Metadata)
	if got, _ := meta[metadataProjectIDKey].(string); got != proj.ID.String() {
		t.Errorf("metadata project_id = %q, want %q", got, proj.ID.String())
	}
	if stamp, _ := meta[metadataSourceUpdatedKey].(string); stamp == "" {
		t.Errorf("metadata source_updated_at must be stamped")
	}
	if len(queue.snapshot()) != 1 {
		t.Errorf("enqueued %d jobs, want 1", len(queue.snapshot()))
	}
	if len(writer.deleted) != 0 || len(purger.purged) != 0 {
		t.Errorf("backfill must not delete or purge anything")
	}
}

// A blank description with no backing memory is a no-op ("skip if empty").
func TestProjectDescriptionPhase_BlankNoop(t *testing.T) {
	proj := testProject("velocity", "   ")
	writer, queue, _ := runDescPhase(t, proj, nil)
	if len(writer.created) != 0 || len(writer.deleted) != 0 || len(queue.snapshot()) != 0 {
		t.Errorf("blank description must be a no-op: created=%d deleted=%d enqueued=%d",
			len(writer.created), len(writer.deleted), len(queue.snapshot()))
	}
}

// Reserved tiers (global, about_me) carry nram boilerplate and are skipped.
func TestProjectDescriptionPhase_ReservedSkipped(t *testing.T) {
	proj := testProject(model.ReservedProjectSlugGlobal, "world-knowledge")
	writer, queue, _ := runDescPhase(t, proj, nil)
	if len(writer.created) != 0 || len(queue.snapshot()) != 0 {
		t.Errorf("reserved project must be skipped: created=%d enqueued=%d",
			len(writer.created), len(queue.snapshot()))
	}
}

// An unchanged description (matching source stamp + content) is idempotent: no
// writes on the steady-state cycle.
func TestProjectDescriptionPhase_UnchangedIdempotent(t *testing.T) {
	proj := testProject("ranshaw", "C++17 elliptic-curve library")
	stamp := proj.UpdatedAt.UTC().Format(time.RFC3339Nano)
	existing := []model.Memory{descBackingMemory(proj.NamespaceID, proj.ID, proj.Description, stamp)}

	writer, queue, purger := runDescPhase(t, proj, existing)
	if len(writer.created) != 0 || len(writer.deleted) != 0 || len(queue.snapshot()) != 0 || len(purger.purged) != 0 {
		t.Errorf("unchanged description must not write: created=%d deleted=%d enqueued=%d purged=%d",
			len(writer.created), len(writer.deleted), len(queue.snapshot()), len(purger.purged))
	}
}

// Editing the description replaces the backing memory: soft-delete old (with
// vector purge) + create new + re-enqueue.
func TestProjectDescriptionPhase_EditReplaces(t *testing.T) {
	proj := testProject("ranshaw", "C++17 elliptic-curve library, CMake, gcc")
	// Existing row reflects an older description and an older stamp.
	old := descBackingMemory(proj.NamespaceID, proj.ID, "old description", "2026-01-01T00:00:00Z")
	writer, queue, purger := runDescPhase(t, proj, []model.Memory{old})

	if len(writer.deleted) != 1 || writer.deleted[0] != old.ID {
		t.Errorf("edit must soft-delete the old backing memory, deleted=%v", writer.deleted)
	}
	if len(purger.purged) != 1 || purger.purged[0] != old.ID {
		t.Errorf("edit must purge the old vector, purged=%v", purger.purged)
	}
	if len(writer.created) != 1 || writer.created[0].Content != proj.Description {
		t.Errorf("edit must create a fresh backing memory with the new content")
	}
	if len(queue.snapshot()) != 1 {
		t.Errorf("edit must re-enqueue enrichment, enqueued=%d", len(queue.snapshot()))
	}
}

// Clearing the description removes the backing memory and does not recreate it.
func TestProjectDescriptionPhase_ClearedRemoves(t *testing.T) {
	proj := testProject("ranshaw", "")
	old := descBackingMemory(proj.NamespaceID, proj.ID, "C++17 elliptic-curve library", "2026-01-01T00:00:00Z")
	writer, queue, purger := runDescPhase(t, proj, []model.Memory{old})

	if len(writer.deleted) != 1 || writer.deleted[0] != old.ID {
		t.Errorf("clear must soft-delete the backing memory, deleted=%v", writer.deleted)
	}
	if len(purger.purged) != 1 {
		t.Errorf("clear must purge the vector, purged=%v", purger.purged)
	}
	if len(writer.created) != 0 || len(queue.snapshot()) != 0 {
		t.Errorf("clear must not create or enqueue: created=%d enqueued=%d",
			len(writer.created), len(queue.snapshot()))
	}
}

// isProjectDescription recognises only marker-bearing memories.
func TestIsProjectDescription(t *testing.T) {
	marked := descBackingMemory(uuid.New(), uuid.New(), "x", "2026-01-01T00:00:00Z")
	if !isProjectDescription(&marked) {
		t.Errorf("marker-bearing memory must be recognised")
	}
	plain := model.Memory{ID: uuid.New(), Content: "ordinary", Origin: model.OriginUser}
	if isProjectDescription(&plain) {
		t.Errorf("ordinary memory must not be recognised")
	}
	if isProjectDescription(nil) {
		t.Errorf("nil must be false")
	}
}

// Shield: the pruning phase must never prune a project-description backing
// memory, even one that would otherwise be killed (zero confidence, stale).
func TestPruningShieldsProjectDescription(t *testing.T) {
	p := NewPruningPhase(nil, nil, nil, nil, nil)
	now := time.Now().UTC()
	old := now.Add(-30 * 24 * time.Hour)

	desc := descBackingMemory(uuid.New(), uuid.New(), "C++17 library", "2026-01-01T00:00:00Z")
	desc.Confidence = 0 // would trip the effectively-zero prune branch
	desc.UpdatedAt = old
	if prune, _ := p.shouldPrune(&desc, now, 0.01); prune {
		t.Errorf("project-description memory must be shielded from pruning")
	}

	// Control: an identical non-marked memory IS pruned, proving the guard is
	// the marker and not some other property.
	plain := model.Memory{ID: uuid.New(), Confidence: 0, UpdatedAt: old}
	if prune, _ := p.shouldPrune(&plain, now, 0.01); !prune {
		t.Errorf("control: a zero-confidence stale memory should prune")
	}
}
