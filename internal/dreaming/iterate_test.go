package dreaming

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// stubReader implements MemoryReader for iterate tests. ListByNamespace
// returns pages slicing into a fixed memory list; other methods are
// minimal stubs (this fake never participates in by-id lookup).
type stubReader struct {
	memories []model.Memory
	calls    []stubReaderCall
	errAt    int // 1-indexed call number to fail at; 0 = never
}

type stubReaderCall struct {
	limit  int
	offset int
}

func (s *stubReader) ListByNamespace(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
	s.calls = append(s.calls, stubReaderCall{limit: limit, offset: offset})
	if s.errAt != 0 && len(s.calls) == s.errAt {
		return nil, errors.New("simulated reader error")
	}
	if offset >= len(s.memories) {
		return []model.Memory{}, nil
	}
	end := offset + limit
	if end > len(s.memories) {
		end = len(s.memories)
	}
	out := make([]model.Memory, end-offset)
	copy(out, s.memories[offset:end])
	return out, nil
}

func (s *stubReader) ListByNamespaceStale(_ context.Context, _ uuid.UUID, _ string, _ int) ([]model.Memory, error) {
	return s.memories, nil
}

func (s *stubReader) GetByID(_ context.Context, _ uuid.UUID) (*model.Memory, error) {
	return nil, errors.New("not implemented")
}

func (s *stubReader) GetBatch(_ context.Context, _ []uuid.UUID) ([]model.Memory, error) {
	return nil, errors.New("not implemented")
}

func (s *stubReader) CountByNamespace(_ context.Context, _ uuid.UUID) (int, error) {
	return len(s.memories), nil
}

func iterateTestMemories(n int) []model.Memory {
	out := make([]model.Memory, n)
	for i := range out {
		out[i].ID = uuid.New()
	}
	return out
}

func TestIterateMemoriesByNamespace_EmptyNamespace(t *testing.T) {
	reader := &stubReader{memories: nil}
	called := 0
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 100, func(_ []model.Memory) error {
		called++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called != 0 {
		t.Fatalf("expected callback never invoked on empty namespace, got %d calls", called)
	}
	if len(reader.calls) != 1 {
		t.Fatalf("expected exactly one ListByNamespace call (probe), got %d", len(reader.calls))
	}
}

func TestIterateMemoriesByNamespace_SingleBatch(t *testing.T) {
	reader := &stubReader{memories: iterateTestMemories(50)}
	got := 0
	calls := 0
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 100, func(batch []model.Memory) error {
		calls++
		got += len(batch)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 50 {
		t.Fatalf("expected 50 rows visited, got %d", got)
	}
	if calls != 1 {
		t.Fatalf("expected one callback invocation (short batch terminates), got %d", calls)
	}
}

func TestIterateMemoriesByNamespace_MultiBatch(t *testing.T) {
	// 2050 rows with batchSize=1000 → 3 batches of 1000, 1000, 50.
	reader := &stubReader{memories: iterateTestMemories(2050)}
	got := 0
	calls := 0
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 1000, func(batch []model.Memory) error {
		calls++
		got += len(batch)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 2050 {
		t.Fatalf("expected 2050 rows visited, got %d", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 callback invocations, got %d", calls)
	}
	// Verify offset advances correctly.
	wantOffsets := []int{0, 1000, 2000}
	if len(reader.calls) != 3 {
		t.Fatalf("expected 3 reader calls, got %d", len(reader.calls))
	}
	for i, want := range wantOffsets {
		if reader.calls[i].offset != want {
			t.Errorf("call %d: expected offset %d, got %d", i, want, reader.calls[i].offset)
		}
		if reader.calls[i].limit != 1000 {
			t.Errorf("call %d: expected limit 1000, got %d", i, reader.calls[i].limit)
		}
	}
}

func TestIterateMemoriesByNamespace_ExactlyBatchSizeMultiple(t *testing.T) {
	// Exact-fit case: 2000 rows with batchSize=1000 produces two full
	// batches; a third probe call should land on offset=2000 and return
	// empty, terminating cleanly.
	reader := &stubReader{memories: iterateTestMemories(2000)}
	calls := 0
	got := 0
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 1000, func(batch []model.Memory) error {
		calls++
		got += len(batch)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 2000 {
		t.Fatalf("expected 2000 rows visited, got %d", got)
	}
	if calls != 2 {
		t.Fatalf("expected 2 callback invocations, got %d", calls)
	}
	// Three reader calls total: 0, 1000, 2000 (the last returns empty
	// and the loop bails).
	if len(reader.calls) != 3 {
		t.Fatalf("expected 3 reader calls, got %d", len(reader.calls))
	}
	if reader.calls[2].offset != 2000 {
		t.Errorf("expected probe call at offset 2000, got %d", reader.calls[2].offset)
	}
}

func TestIterateMemoriesByNamespace_CallbackError(t *testing.T) {
	reader := &stubReader{memories: iterateTestMemories(2050)}
	calls := 0
	wantErr := errors.New("callback boom")
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 1000, func(_ []model.Memory) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 callback invocations before abort, got %d", calls)
	}
}

func TestIterateMemoriesByNamespace_ReaderError(t *testing.T) {
	reader := &stubReader{memories: iterateTestMemories(2050), errAt: 2}
	calls := 0
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 1000, func(_ []model.Memory) error {
		calls++
		return nil
	})
	if err == nil {
		t.Fatal("expected reader error to propagate")
	}
	if calls != 1 {
		t.Fatalf("expected 1 successful callback before reader error, got %d", calls)
	}
}

func TestIterateMemoriesByNamespace_ContextCanceled(t *testing.T) {
	reader := &stubReader{memories: iterateTestMemories(5000)}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := iterateMemoriesByNamespace(ctx, reader, uuid.New(), 1000, func(_ []model.Memory) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestIterateMemoriesByNamespace_DefaultBatchSize(t *testing.T) {
	// Zero batchSize falls back to 1000.
	reader := &stubReader{memories: iterateTestMemories(500)}
	err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 0, func(_ []model.Memory) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reader.calls) == 0 || reader.calls[0].limit != 1000 {
		t.Fatalf("expected default batchSize=1000, got %d (calls=%d)", reader.calls[0].limit, len(reader.calls))
	}
}

func TestIterateMemoriesByNamespace_GuardsNilArguments(t *testing.T) {
	if err := iterateMemoriesByNamespace(context.Background(), nil, uuid.New(), 100, func(_ []model.Memory) error { return nil }); err == nil {
		t.Fatal("expected nil-reader error")
	}
	reader := &stubReader{}
	if err := iterateMemoriesByNamespace(context.Background(), reader, uuid.New(), 100, nil); err == nil {
		t.Fatal("expected nil-callback error")
	}
}

