package dreaming

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// fakeMentionRecomputer records the namespace it was asked to recompute and
// serves a scripted rows-updated count / error.
type fakeMentionRecomputer struct {
	calls   []uuid.UUID
	updated int64
	err     error
}

func (f *fakeMentionRecomputer) RecomputeMentionCountsByNamespace(_ context.Context, ns uuid.UUID) (int64, error) {
	f.calls = append(f.calls, ns)
	return f.updated, f.err
}

func TestMentionRecomputePhase_RecomputesCycleNamespace(t *testing.T) {
	ns := uuid.New()
	ent := &fakeMentionRecomputer{updated: 7}
	phase := NewMentionRecomputePhase(ent)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Errorf("mention recompute never reports residual")
	}
	// Recompute must be scoped to exactly the cycle's namespace, never global.
	if len(ent.calls) != 1 || ent.calls[0] != ns {
		t.Fatalf("expected one recompute for namespace %s, got %v", ns, ent.calls)
	}
	if phase.Name() != model.DreamPhaseMentionRecompute {
		t.Fatalf("Name() = %q, want %q", phase.Name(), model.DreamPhaseMentionRecompute)
	}
}

// A recompute error is swallowed (logged + recorded in the phase summary) so a
// transient failure never fails the whole dream cycle in the runner.
func TestMentionRecomputePhase_RecomputeError_IsNonFatal(t *testing.T) {
	ns := uuid.New()
	ent := &fakeMentionRecomputer{err: errors.New("boom")}
	phase := NewMentionRecomputePhase(ent)
	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(10000, 2048), logger)
	if err != nil {
		t.Fatalf("Execute must swallow the recompute error, got %v", err)
	}
	if result.HasResidual {
		t.Errorf("no residual on error")
	}
	if len(ent.calls) != 1 || ent.calls[0] != ns {
		t.Fatalf("expected one recompute attempt for namespace %s, got %v", ns, ent.calls)
	}
}
