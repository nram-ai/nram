package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMemoryRepo_BumpReinforcement_BumpsAllThreeFields(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		mem.Confidence = 0.5
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		initialAccess := mem.AccessCount
		initialConfidence := mem.Confidence

		now := time.Now().UTC().Truncate(time.Second)
		rows, err := repo.BumpReinforcement(ctx, []uuid.UUID{mem.ID}, now, 0.02)
		if err != nil {
			t.Fatalf("bump reinforcement: %v", err)
		}
		if rows != 1 {
			t.Fatalf("expected 1 row affected, got %d", rows)
		}

		got, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}

		if got.AccessCount != initialAccess+1 {
			t.Errorf("access_count: want %d, got %d", initialAccess+1, got.AccessCount)
		}
		if got.LastAccessed == nil {
			t.Fatal("last_accessed was not set")
		}
		if !got.LastAccessed.Equal(now) {
			t.Errorf("last_accessed: want %v, got %v", now, got.LastAccessed)
		}
		wantConfidence := initialConfidence * 1.02
		if math.Abs(got.Confidence-wantConfidence) > 1e-9 {
			t.Errorf("confidence: want %v, got %v", wantConfidence, got.Confidence)
		}
	})
}

func TestMemoryRepo_BumpReinforcement_CapsConfidenceAtOne(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		mem.Confidence = 0.99
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		if _, err := repo.BumpReinforcement(ctx, []uuid.UUID{mem.ID}, time.Now(), 0.5); err != nil {
			t.Fatalf("bump reinforcement: %v", err)
		}

		got, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if got.Confidence != 1.0 {
			t.Errorf("confidence should be capped at 1.0, got %v", got.Confidence)
		}
	})
}

func TestMemoryRepo_BumpReinforcement_SkipsSoftDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := repo.SoftDelete(ctx, mem.ID, nsID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}

		rows, err := repo.BumpReinforcement(ctx, []uuid.UUID{mem.ID}, time.Now(), 0.1)
		if err != nil {
			t.Fatalf("bump reinforcement: %v", err)
		}
		if rows != 0 {
			t.Errorf("soft-deleted row should be skipped; got %d rows affected", rows)
		}
	})
}

func TestMemoryRepo_BumpReinforcement_IgnoresUnknownIDs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		real := newTestMemory(nsID)
		if err := repo.Create(ctx, real); err != nil {
			t.Fatalf("create: %v", err)
		}

		ids := []uuid.UUID{real.ID, uuid.New(), uuid.New()}
		rows, err := repo.BumpReinforcement(ctx, ids, time.Now(), 0.05)
		if err != nil {
			t.Fatalf("bump reinforcement: %v", err)
		}
		if rows != 1 {
			t.Errorf("want 1 row (only the real one), got %d", rows)
		}
	})
}

func TestMemoryRepo_BumpReinforcement_EmptyList(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		rows, err := repo.BumpReinforcement(ctx, nil, time.Now(), 0.02)
		if err != nil {
			t.Fatalf("empty list should not error: %v", err)
		}
		if rows != 0 {
			t.Errorf("empty list should affect 0 rows, got %d", rows)
		}
	})
}

func TestMemoryRepo_BumpReinforcement_Batch(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		var ids []uuid.UUID
		for i := 0; i < 5; i++ {
			mem := newTestMemory(nsID)
			mem.Confidence = 0.5
			if err := repo.Create(ctx, mem); err != nil {
				t.Fatalf("create: %v", err)
			}
			ids = append(ids, mem.ID)
		}

		rows, err := repo.BumpReinforcement(ctx, ids, time.Now(), 0.1)
		if err != nil {
			t.Fatalf("bump: %v", err)
		}
		if rows != 5 {
			t.Errorf("want 5 rows affected, got %d", rows)
		}

		for _, id := range ids {
			got, err := repo.GetByID(ctx, id)
			if err != nil {
				t.Fatalf("reload %s: %v", id, err)
			}
			wantConfidence := 0.5 * 1.1
			if math.Abs(got.Confidence-wantConfidence) > 1e-9 {
				t.Errorf("memory %s confidence: want %v, got %v", id, wantConfidence, got.Confidence)
			}
			if got.AccessCount != 1 {
				t.Errorf("memory %s access_count: want 1, got %d", id, got.AccessCount)
			}
		}
	})
}

func TestMemoryRepo_DecayConfidence_Multiplies(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		mem.Confidence = 0.8
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		rows, err := repo.DecayConfidence(ctx, []uuid.UUID{mem.ID}, 0.9, 0.05)
		if err != nil {
			t.Fatalf("decay: %v", err)
		}
		if rows != 1 {
			t.Errorf("want 1 row, got %d", rows)
		}

		got, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		wantConfidence := 0.8 * 0.9
		if math.Abs(got.Confidence-wantConfidence) > 1e-9 {
			t.Errorf("confidence: want %v, got %v", wantConfidence, got.Confidence)
		}
	})
}

func TestMemoryRepo_DecayConfidence_RespectsFloor(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		mem.Confidence = 0.06
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}

		// confidence * 0.5 = 0.03, floor is 0.05, so clamp to floor.
		if _, err := repo.DecayConfidence(ctx, []uuid.UUID{mem.ID}, 0.5, 0.05); err != nil {
			t.Fatalf("decay: %v", err)
		}

		got, err := repo.GetByID(ctx, mem.ID)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if math.Abs(got.Confidence-0.05) > 1e-9 {
			t.Errorf("confidence should be clamped to floor 0.05, got %v", got.Confidence)
		}
	})
}

func TestMemoryRepo_DecayConfidence_SkipsSoftDeleted(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		nsID := createTestMemoryNamespace(t, ctx, db)

		mem := newTestMemory(nsID)
		if err := repo.Create(ctx, mem); err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := repo.SoftDelete(ctx, mem.ID, nsID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}

		rows, err := repo.DecayConfidence(ctx, []uuid.UUID{mem.ID}, 0.9, 0.05)
		if err != nil {
			t.Fatalf("decay: %v", err)
		}
		if rows != 0 {
			t.Errorf("soft-deleted row should be skipped; got %d", rows)
		}
	})
}

func TestMemoryRepo_DecayConfidence_EmptyList(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)

		rows, err := repo.DecayConfidence(ctx, nil, 0.9, 0.05)
		if err != nil {
			t.Fatalf("empty list: %v", err)
		}
		if rows != 0 {
			t.Errorf("want 0 rows, got %d", rows)
		}
	})
}
