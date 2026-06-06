package service

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// memProceduralRepo is an in-memory ProceduralRepository for service tests. Its
// Create rejects a duplicate global id so the import collision-avoidance is
// actually exercised: procedural_entries.id is a global primary key.
type memProceduralRepo struct {
	rows map[uuid.UUID]*model.ProceduralEntry
}

func newMemProceduralRepo() *memProceduralRepo {
	return &memProceduralRepo{rows: map[uuid.UUID]*model.ProceduralEntry{}}
}

func (m *memProceduralRepo) Create(_ context.Context, e *model.ProceduralEntry) error {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if _, exists := m.rows[e.ID]; exists {
		return fmt.Errorf("duplicate primary key %s", e.ID)
	}
	cp := *e
	m.rows[e.ID] = &cp
	return nil
}

func (m *memProceduralRepo) GetByID(_ context.Context, id uuid.UUID) (*model.ProceduralEntry, error) {
	e, ok := m.rows[id]
	if !ok {
		return nil, sql.ErrNoRows
	}
	cp := *e
	return &cp, nil
}

func (m *memProceduralRepo) ListByNamespace(_ context.Context, ns uuid.UUID) ([]model.ProceduralEntry, error) {
	out := []model.ProceduralEntry{}
	for _, e := range m.rows {
		if e.NamespaceID == ns {
			out = append(out, *e)
		}
	}
	return out, nil
}

func (m *memProceduralRepo) Update(_ context.Context, e *model.ProceduralEntry) error {
	existing, ok := m.rows[e.ID]
	if !ok || existing.NamespaceID != e.NamespaceID {
		return sql.ErrNoRows
	}
	cp := *e
	m.rows[e.ID] = &cp
	return nil
}

func (m *memProceduralRepo) Delete(_ context.Context, id, ns uuid.UUID) error {
	e, ok := m.rows[id]
	if !ok || e.NamespaceID != ns {
		return sql.ErrNoRows
	}
	delete(m.rows, id)
	return nil
}

func TestProceduralImport_UpdatesOwnEntryInPlace(t *testing.T) {
	repo := newMemProceduralRepo()
	svc := NewProceduralService(repo)
	ctx := context.Background()
	ns := uuid.New()

	created, err := svc.Create(ctx, &model.ProceduralEntry{
		NamespaceID: ns, Content: "original", Title: "t", Priority: 1, Enabled: true,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := svc.Import(ctx, ns, []ProceduralExportEntry{
		{ID: created.ID, Content: "updated", Title: "t2", Priority: 5, Enabled: false},
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.Updated != 1 || res.Imported != 0 || res.Skipped != 0 {
		t.Fatalf("counts: imported=%d updated=%d skipped=%d", res.Imported, res.Updated, res.Skipped)
	}

	all, _ := svc.List(ctx, ns)
	if len(all) != 1 {
		t.Fatalf("expected 1 row, got %d", len(all))
	}
	if all[0].Content != "updated" || all[0].Priority != 5 || all[0].Enabled {
		t.Fatalf("row not updated in place: %+v", all[0])
	}
	if all[0].ID != created.ID {
		t.Fatalf("id changed on in-place update: %s -> %s", created.ID, all[0].ID)
	}
}

func TestProceduralImport_ForeignIDCreatesNewRowWithNewID(t *testing.T) {
	repo := newMemProceduralRepo()
	svc := NewProceduralService(repo)
	ctx := context.Background()
	ns := uuid.New()

	foreignID := uuid.New() // an id that does not belong to this namespace
	res, err := svc.Import(ctx, ns, []ProceduralExportEntry{
		{ID: foreignID, Content: "from another brain", Title: "shared", Enabled: true},
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.Imported != 1 || res.Updated != 0 {
		t.Fatalf("counts: imported=%d updated=%d", res.Imported, res.Updated)
	}

	all, _ := svc.List(ctx, ns)
	if len(all) != 1 {
		t.Fatalf("expected 1 row, got %d", len(all))
	}
	if all[0].ID == foreignID {
		t.Fatalf("reused foreign id instead of generating a new one")
	}
	if all[0].Origin != string(model.OriginImport) {
		t.Fatalf("expected origin %q, got %q", model.OriginImport, all[0].Origin)
	}
}

func TestProceduralImport_EmptyContentSkippedWithoutAborting(t *testing.T) {
	repo := newMemProceduralRepo()
	svc := NewProceduralService(repo)
	ctx := context.Background()
	ns := uuid.New()

	res, err := svc.Import(ctx, ns, []ProceduralExportEntry{
		{Content: "  ", Title: "blank"},
		{Content: "good", Title: "kept"},
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.Skipped != 1 || res.Imported != 1 {
		t.Fatalf("counts: imported=%d skipped=%d errors=%d", res.Imported, res.Skipped, len(res.Errors))
	}
	if len(res.Errors) != 1 || res.Errors[0].Index != 0 {
		t.Fatalf("expected one error at index 0, got %+v", res.Errors)
	}
}

func TestProceduralExportImport_RoundTripIntoOtherNamespaceNoCollision(t *testing.T) {
	repo := newMemProceduralRepo()
	svc := NewProceduralService(repo)
	ctx := context.Background()
	nsA := uuid.New()
	nsB := uuid.New()

	for i := range 3 {
		if _, err := svc.Create(ctx, &model.ProceduralEntry{
			NamespaceID: nsA, Content: fmt.Sprintf("rule %d", i), Priority: i, Enabled: true,
		}); err != nil {
			t.Fatalf("seed create: %v", err)
		}
	}

	export, err := svc.Export(ctx, nsA)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if export.Stats.Count != 3 || len(export.Entries) != 3 {
		t.Fatalf("export count: %d / %d", export.Stats.Count, len(export.Entries))
	}

	// Import nsA's export (carrying nsA's ids) into nsB. Every entry must land as
	// a new row with a new id; reusing nsA's ids would collide on the global PK.
	res, err := svc.Import(ctx, nsB, export.Entries)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.Imported != 3 || res.Updated != 0 || res.Skipped != 0 {
		t.Fatalf("counts: imported=%d updated=%d skipped=%d", res.Imported, res.Updated, res.Skipped)
	}

	aRows, _ := svc.List(ctx, nsA)
	bRows, _ := svc.List(ctx, nsB)
	if len(aRows) != 3 || len(bRows) != 3 {
		t.Fatalf("rows A=%d B=%d", len(aRows), len(bRows))
	}
	aIDs := map[uuid.UUID]bool{}
	for _, e := range aRows {
		aIDs[e.ID] = true
	}
	for _, e := range bRows {
		if aIDs[e.ID] {
			t.Fatalf("namespace B reused a namespace A id: %s", e.ID)
		}
	}
}
