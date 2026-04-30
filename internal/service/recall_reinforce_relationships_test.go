package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
)

// recordingRelReinforcer records each Reinforce call so tests can assert
// per-id throttling, the propagated delta, and the namespace match. Mirrors
// recordingReinforcer (memory side) but records per-id rather than batches.
type recordingRelReinforcer struct {
	mu          sync.Mutex
	calls       []relReinforceCall
	returnError error
}

type relReinforceCall struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Delta       float64
}

func (r *recordingRelReinforcer) Reinforce(_ context.Context, id, namespaceID uuid.UUID, delta float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, relReinforceCall{ID: id, NamespaceID: namespaceID, Delta: delta})
	if r.returnError != nil {
		return r.returnError
	}
	return nil
}

func (r *recordingRelReinforcer) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingRelReinforcer) calledIDs() []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uuid.UUID, 0, len(r.calls))
	for _, c := range r.calls {
		out = append(out, c.ID)
	}
	return out
}

// staticRelSettings is a settings stub that knows the relationship-side
// keys. We keep it separate from staticSettings so the recall_reinforce
// memory tests aren't perturbed by a wider key map.
type staticRelSettings struct {
	mode  string
	delta float64
	cap   int
}

func (s *staticRelSettings) Resolve(_ context.Context, key, _ string) (string, error) {
	if key == SettingReconsolidationMode {
		return s.mode, nil
	}
	return "", nil
}

func (s *staticRelSettings) ResolveFloat(_ context.Context, key, _ string) (float64, error) {
	if key == SettingDreamingWeightRecallReinforceDelta {
		if s.delta == 0 {
			return 0, errors.New("no value")
		}
		return s.delta, nil
	}
	return 0, errors.New("no value")
}

func (s *staticRelSettings) ResolveInt(_ context.Context, key, _ string) (int, error) {
	if key == SettingReinforcementEventRelationshipCap {
		if s.cap == 0 {
			return 0, errors.New("no value")
		}
		return s.cap, nil
	}
	if key == SettingReinforcementEventMemoryCap {
		// Memory side is not the focus here; return the registered default
		// shape so the test does not depend on key absence.
		return 20, nil
	}
	return 0, errors.New("no value")
}

func makeRefs(n int) []RelationshipRef {
	refs := make([]RelationshipRef, n)
	ns := uuid.New()
	for i := range refs {
		refs[i] = RelationshipRef{ID: uuid.New(), NamespaceID: ns}
	}
	return refs
}

// --- reinforceRels() unit tests (direct, no goroutine) ---

func TestReinforceRels_OffMode_NoWriteNoEvent(t *testing.T) {
	writer := &recordingRelReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings:  &staticRelSettings{mode: ReconsolidationModeOff, delta: 0.05},
		Bus:       bus,
	})

	svc.reinforceRels(context.Background(), makeRefs(3))

	if writer.callCount() != 0 {
		t.Errorf("off mode must not write; got %d calls", writer.callCount())
	}
	if n := len(bus.publishedByType(events.RelationshipReinforced)); n != 0 {
		t.Errorf("off mode must not emit; got %d events", n)
	}
}

func TestReinforceRels_ShadowMode_EventOnlyNoWrite(t *testing.T) {
	writer := &recordingRelReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings:  &staticRelSettings{mode: ReconsolidationModeShadow, delta: 0.05},
		Bus:       bus,
	})

	refs := makeRefs(3)
	svc.reinforceRels(context.Background(), refs)

	if writer.callCount() != 0 {
		t.Errorf("shadow mode must not write; got %d calls", writer.callCount())
	}
	evts := bus.publishedByType(events.RelationshipReinforced)
	if len(evts) != 1 {
		t.Fatalf("shadow mode must emit exactly 1 event; got %d", len(evts))
	}

	var payload relReinforcementEvent
	if err := json.Unmarshal(evts[0].Data, &payload); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if payload.Mode != ReconsolidationModeShadow {
		t.Errorf("event mode: want shadow, got %q", payload.Mode)
	}
	if payload.Count != len(refs) {
		t.Errorf("event count: want %d, got %d", len(refs), payload.Count)
	}
	if payload.Persisted != 0 {
		t.Errorf("shadow must have persisted=0; got %d", payload.Persisted)
	}
	if payload.Delta != 0.05 {
		t.Errorf("event delta: want 0.05, got %v", payload.Delta)
	}
}

func TestReinforceRels_PersistMode_WritesAndEmits(t *testing.T) {
	writer := &recordingRelReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings:  &staticRelSettings{mode: ReconsolidationModePersist, delta: 0.05},
		Bus:       bus,
	})

	refs := makeRefs(3)
	svc.reinforceRels(context.Background(), refs)

	// One call per ref — the per-relationship throttle is enforced upstream
	// (seenRels in Recall and handleMemoryGraph), so reinforceRels itself
	// writes every ref it receives.
	if got := writer.callCount(); got != len(refs) {
		t.Fatalf("persist mode must write once per ref; want %d, got %d", len(refs), got)
	}

	for i, ref := range refs {
		if writer.calls[i].ID != ref.ID {
			t.Errorf("call[%d].ID: want %s, got %s", i, ref.ID, writer.calls[i].ID)
		}
		if writer.calls[i].NamespaceID != ref.NamespaceID {
			t.Errorf("call[%d].NamespaceID: want %s, got %s", i, ref.NamespaceID, writer.calls[i].NamespaceID)
		}
		if writer.calls[i].Delta != 0.05 {
			t.Errorf("call[%d].Delta: want 0.05, got %v", i, writer.calls[i].Delta)
		}
	}

	evts := bus.publishedByType(events.RelationshipReinforced)
	if len(evts) != 1 {
		t.Fatalf("persist mode must emit 1 event; got %d", len(evts))
	}
	var payload relReinforcementEvent
	_ = json.Unmarshal(evts[0].Data, &payload)
	if payload.Persisted != int64(len(refs)) {
		t.Errorf("persisted: want %d, got %d", len(refs), payload.Persisted)
	}
}

func TestReinforceRels_PersistMode_RowMissingDuringWrite(t *testing.T) {
	// sql.ErrNoRows on a single ref must not block the others or suppress
	// the event. Mirrors the race where a relationship is expired between
	// the recall traversal and the reinforcement goroutine.
	writer := &recordingRelReinforcer{returnError: sql.ErrNoRows}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings:  &staticRelSettings{mode: ReconsolidationModePersist, delta: 0.05},
		Bus:       bus,
	})

	refs := makeRefs(3)
	svc.reinforceRels(context.Background(), refs)

	if got := writer.callCount(); got != len(refs) {
		t.Fatalf("writer attempted: want %d, got %d", len(refs), got)
	}
	evts := bus.publishedByType(events.RelationshipReinforced)
	if len(evts) != 1 {
		t.Fatalf("event must still fire; got %d", len(evts))
	}
	var payload relReinforcementEvent
	_ = json.Unmarshal(evts[0].Data, &payload)
	if payload.Persisted != 0 {
		t.Errorf("ErrNoRows should not count as persisted; got %d", payload.Persisted)
	}
}

func TestReinforceRels_PersistMode_GenericErrorDoesNotSuppressEvent(t *testing.T) {
	writer := &recordingRelReinforcer{returnError: errors.New("db down")}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings:  &staticRelSettings{mode: ReconsolidationModePersist, delta: 0.05},
		Bus:       bus,
	})

	svc.reinforceRels(context.Background(), makeRefs(2))
	if n := len(bus.publishedByType(events.RelationshipReinforced)); n != 1 {
		t.Errorf("event must still fire on generic write failure; got %d", n)
	}
}

func TestReinforceRels_EmptyRefs_NoOp(t *testing.T) {
	writer := &recordingRelReinforcer{}
	bus := &collectingBus{}

	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings:  &staticRelSettings{mode: ReconsolidationModePersist, delta: 0.05},
		Bus:       bus,
	})

	svc.reinforceRels(context.Background(), nil)

	if writer.callCount() != 0 {
		t.Errorf("empty refs must not write; got %d", writer.callCount())
	}
	if n := len(bus.publishedByType(events.RelationshipReinforced)); n != 0 {
		t.Errorf("empty refs must not emit; got %d", n)
	}
}

func TestReinforceRels_DisabledWhenNotWired(t *testing.T) {
	svc := &RecallService{}
	// No SetReinforcement call — must not panic, must not touch anything.
	svc.reinforceRels(context.Background(), makeRefs(2))
}

func TestReinforceRels_DisabledWhenNoRelWriter(t *testing.T) {
	bus := &collectingBus{}
	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		// Writer for memory side present, RelWriter intentionally nil.
		Writer:   &recordingReinforcer{},
		Settings: &staticRelSettings{mode: ReconsolidationModePersist, delta: 0.05},
		Bus:      bus,
	})

	svc.reinforceRels(context.Background(), makeRefs(2))

	// No writer to receive calls; the event still emits in persist mode
	// because the function reached the emit path with a non-nil bus.
	// What we are protecting here is: no panic when RelWriter is nil.
	if n := len(bus.publishedByType(events.RelationshipReinforced)); n != 1 {
		t.Errorf("event should still emit even when RelWriter is nil; got %d", n)
	}
}

func TestReinforceRels_EventCapTruncatesPayload(t *testing.T) {
	writer := &recordingRelReinforcer{}
	bus := &collectingBus{}
	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		Settings: &staticRelSettings{
			mode:  ReconsolidationModePersist,
			delta: 0.05,
			cap:   3,
		},
		Bus: bus,
	})

	refs := makeRefs(10)
	svc.reinforceRels(context.Background(), refs)

	// All 10 refs must still be written...
	if got := writer.callCount(); got != 10 {
		t.Fatalf("writer must see every ref; want 10, got %d", got)
	}
	// ...but the event payload must carry only the first 3 IDs.
	evts := bus.publishedByType(events.RelationshipReinforced)
	if len(evts) != 1 {
		t.Fatalf("expected 1 event; got %d", len(evts))
	}
	var payload relReinforcementEvent
	_ = json.Unmarshal(evts[0].Data, &payload)
	if payload.Count != 10 {
		t.Errorf("Count must reflect total refs; want 10, got %d", payload.Count)
	}
	if len(payload.RelationshipIDs) != 3 {
		t.Errorf("payload IDs must be capped at 3; got %d", len(payload.RelationshipIDs))
	}
}

func TestReinforceRels_FallsBackToRegisteredDelta(t *testing.T) {
	// When the settings reader has no value for the delta key, the function
	// should fall through to settingDefaults (0.05). Pinning this guards
	// against drift between the runtime fallback and the registered default
	// — the same drift that motivated the explicit fallback in the memory
	// side's reinforce().
	writer := &recordingRelReinforcer{}
	bus := &collectingBus{}
	svc := &RecallService{}
	svc.SetReinforcement(&ReinforcementDeps{
		RelWriter: writer,
		// delta=0 → ResolveFloat returns "no value"; we want the fallback.
		Settings: &staticRelSettings{mode: ReconsolidationModePersist},
		Bus:      bus,
	})

	svc.reinforceRels(context.Background(), makeRefs(1))

	if writer.callCount() != 1 {
		t.Fatalf("expected one write; got %d", writer.callCount())
	}
	want, _ := strconv.ParseFloat(settingDefaults[SettingDreamingWeightRecallReinforceDelta], 64)
	if writer.calls[0].Delta != want {
		t.Errorf("delta fallback: want %v, got %v", want, writer.calls[0].Delta)
	}
}

// --- Recall() integration: relationship hook fires when graph is requested ---

func TestRecall_FiresRelationshipReinforcementHook(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memID := uuid.New()
	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(memID, nsID, "alice works at acme", []string{"t1"}, 0.5, 0, time.Now()),
		},
		memories: map[uuid.UUID]*model.Memory{},
	}

	// Provide an entity reader and traverser that surface a single
	// relationship for the recall query.
	entityID := uuid.New()
	relID := uuid.New()
	er := &mockEntityReader{
		entities: []model.Entity{{ID: entityID, NamespaceID: nsID, Name: "alice", EntityType: "person"}},
	}
	tr := &mockRelTraverser{
		rels: []model.Relationship{{
			ID: relID, NamespaceID: nsID,
			SourceID: entityID, TargetID: uuid.New(),
			Relation: "works_at", Weight: 0.7,
		}},
	}

	svc, _ := newRecallService(memReader, projects, namespaces, nil, er, tr, nil)

	// Memory side: no-op writer (we are testing the rel side here).
	memWriter := &recordingReinforcer{}
	relWriter := &recordingRelReinforcer{}
	bus := &collectingBus{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer:    memWriter,
		RelWriter: relWriter,
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist},
			floats: map[string]float64{
				SettingReconsolidationFactor:              0.02,
				SettingDreamingWeightRecallReinforceDelta: 0.05,
			},
		},
		Bus: bus,
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:    projectID,
		Query:        "alice",
		Limit:        5,
		IncludeGraph: true,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(resp.Graph.Relationships) == 0 {
		t.Fatal("recall surfaced no graph relationships; cannot exercise hook")
	}

	waitForCalls(t, relWriter.callCount, 1)

	// Throttle: only one Reinforce per relationship even if multiple
	// foundEntities traverse the same edge.
	called := relWriter.calledIDs()
	seen := map[uuid.UUID]struct{}{}
	for _, id := range called {
		if _, dup := seen[id]; dup {
			t.Errorf("relationship %s reinforced more than once in a single recall (throttle violated)", id)
		}
		seen[id] = struct{}{}
	}
}

// --- Panic isolation: relationship-side panic must not block the response ---

type panicRelWriter struct {
	attempted *int32
}

func (p panicRelWriter) Reinforce(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ float64) error {
	atomic.StoreInt32(p.attempted, 1)
	panic("simulated relationship reinforcement crash")
}

func TestRecall_RelReinforcementPanic_DoesNotAffectCaller(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	memID := uuid.New()
	memReader := &mockMemoryReader{
		nsList: []model.Memory{
			*makeTestMemory(memID, nsID, "hello", nil, 0.5, 0, time.Now()),
		},
		memories: map[uuid.UUID]*model.Memory{},
	}
	entityID := uuid.New()
	er := &mockEntityReader{
		entities: []model.Entity{{ID: entityID, NamespaceID: nsID, Name: "hello", EntityType: "tag"}},
	}
	tr := &mockRelTraverser{
		rels: []model.Relationship{{
			ID: uuid.New(), NamespaceID: nsID,
			SourceID: entityID, TargetID: uuid.New(),
			Relation: "rel", Weight: 0.5,
		}},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, nil, er, tr, nil)

	var attempted int32
	svc.SetReinforcement(&ReinforcementDeps{
		Writer:    &recordingReinforcer{},
		RelWriter: panicRelWriter{attempted: &attempted},
		Settings: &staticSettings{
			values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist},
			floats: map[string]float64{
				SettingReconsolidationFactor:              0.02,
				SettingDreamingWeightRecallReinforceDelta: 0.05,
			},
		},
	})

	resp, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:    projectID,
		Query:        "hello",
		Limit:        5,
		IncludeGraph: true,
	})
	if err != nil {
		t.Fatalf("recall failed because of relationship reinforcement panic: %v", err)
	}
	if resp == nil {
		t.Fatal("recall returned nil response despite panic")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&attempted) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&attempted) != 1 {
		t.Error("panicRelWriter was never invoked; goroutine did not run")
	}
}
