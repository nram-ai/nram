package dreaming

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestShouldPrune_EffectivelyZeroConfidenceCohort exercises the A2 plan: the
// prune branch must catch memories whose confidence sat at or below the
// effectively-zero threshold for longer than the 7d idle gate. Before the
// change the branch tested `Confidence == 0` exactly, which never fired
// because contradiction haircuts are multiplicative and never reach exact
// zero outside of float underflow.
func TestShouldPrune_EffectivelyZeroConfidenceCohort(t *testing.T) {
	now := time.Now().UTC()

	src := "dream"
	mem := &model.Memory{
		ID:           uuid.New(),
		NamespaceID:  uuid.New(),
		Confidence:   0.0005,                       // contradiction-haircut residual
		UpdatedAt:    now.Add(-10 * 24 * time.Hour), // older than the 7d idle gate
		CreatedAt:    now.Add(-10 * 24 * time.Hour),
		SupersededBy: nil,
		Source:       &src,
	}

	p := &PruningPhase{} // shouldPrune is pure, no deps required.
	prune, reason := p.shouldPrune(mem, now)
	if !prune {
		t.Fatalf("shouldPrune = false, want true for conf=0.0005 idle 10d")
	}
	if reason != pruneReasonZeroConfidence {
		t.Errorf("reason = %q, want %q", reason, pruneReasonZeroConfidence)
	}
}

// TestShouldPrune_FloorPinnedMemoryNotPruned verifies the threshold leaves
// memories sitting at the decay floor (default 0.05) alone — they are
// load-bearing for the eventual recall-reinforcement rollout.
func TestShouldPrune_FloorPinnedMemoryNotPruned(t *testing.T) {
	now := time.Now().UTC()

	src := "user"
	mem := &model.Memory{
		ID:           uuid.New(),
		NamespaceID:  uuid.New(),
		Confidence:   defaultConfidenceFloor, // 0.05
		UpdatedAt:    now.Add(-30 * 24 * time.Hour),
		CreatedAt:    now.Add(-30 * 24 * time.Hour),
		SupersededBy: nil,
		Source:       &src,
	}

	p := &PruningPhase{}
	prune, reason := p.shouldPrune(mem, now)
	if prune {
		t.Errorf("shouldPrune = true (reason=%q), want false for floor-pinned memory", reason)
	}
}

// TestShouldPrune_RecentlyTouchedNotPruned guards the 7d idle gate: a memory
// at the effectively-zero confidence but updated yesterday must not be
// pruned, because future recall reinforcement may rescue it.
func TestShouldPrune_RecentlyTouchedNotPruned(t *testing.T) {
	now := time.Now().UTC()

	src := "dream"
	mem := &model.Memory{
		ID:           uuid.New(),
		NamespaceID:  uuid.New(),
		Confidence:   0.0005,
		UpdatedAt:    now.Add(-1 * 24 * time.Hour), // inside the 7d window
		CreatedAt:    now.Add(-10 * 24 * time.Hour),
		SupersededBy: nil,
		Source:       &src,
	}

	p := &PruningPhase{}
	prune, _ := p.shouldPrune(mem, now)
	if prune {
		t.Error("shouldPrune = true, want false: memory was updated within 7d idle gate")
	}
}
