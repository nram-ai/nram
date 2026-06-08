package api

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// fakeGraphMemStore models MemoryRepo.GetBatch: it returns only the memories it
// is told about, and (like the real GetBatch) callers simulate a soft-deleted
// source by simply not including it in mems.
type fakeGraphMemStore struct {
	mems []model.Memory
	err  error
}

func (f *fakeGraphMemStore) GetBatch(_ context.Context, ids []uuid.UUID, _ []uuid.UUID) ([]model.Memory, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []model.Memory
	for _, id := range ids {
		for i := range f.mems {
			if f.mems[i].ID == id {
				out = append(out, f.mems[i])
			}
		}
	}
	return out, nil
}

func relWithSource(mem *uuid.UUID) model.Relationship {
	return model.Relationship{ID: uuid.New(), SourceID: uuid.New(), TargetID: uuid.New(), SourceMemory: mem}
}

func TestResolveLiveProvenance_NilStoreDisablesFilter(t *testing.T) {
	m := uuid.New()
	got := resolveLiveProvenance(context.Background(), nil, uuid.Nil, []model.Relationship{relWithSource(&m)})
	if got != nil {
		t.Fatalf("nil store must yield nil (filter disabled), got %v", got)
	}
}

func TestResolveLiveProvenance_ErrorFailsOpen(t *testing.T) {
	m := uuid.New()
	store := &fakeGraphMemStore{err: errors.New("boom")}
	got := resolveLiveProvenance(context.Background(), store, uuid.Nil, []model.Relationship{relWithSource(&m)})
	if got != nil {
		t.Fatalf("lookup error must fail open (nil map), got %v", got)
	}
}

func TestResolveLiveProvenance_OnlyNullSources(t *testing.T) {
	store := &fakeGraphMemStore{}
	got := resolveLiveProvenance(context.Background(), store, uuid.Nil,
		[]model.Relationship{relWithSource(nil), relWithSource(nil)})
	if got == nil {
		t.Fatalf("expected a non-nil (empty) map when filtering is active")
	}
	if len(got) != 0 {
		t.Fatalf("expected no live memories, got %v", got)
	}
}

func TestResolveLiveProvenance_LiveSupersededDeleted(t *testing.T) {
	live := uuid.New()
	superseder := uuid.New()
	superseded := uuid.New()
	deleted := uuid.New() // not returned by GetBatch → simulates soft-deleted

	store := &fakeGraphMemStore{mems: []model.Memory{
		{ID: live},
		{ID: superseded, SupersededBy: &superseder},
	}}

	rels := []model.Relationship{
		relWithSource(&live),
		relWithSource(&superseded),
		relWithSource(&deleted),
		relWithSource(nil),
	}
	got := resolveLiveProvenance(context.Background(), store, uuid.Nil, rels)

	if !got[live] {
		t.Fatalf("live memory should be present in the live set")
	}
	if got[superseded] {
		t.Fatalf("superseded memory must be excluded from the live set")
	}
	if got[deleted] {
		t.Fatalf("soft-deleted (absent) memory must be excluded from the live set")
	}
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 live memory, got %d: %v", len(got), got)
	}
}
