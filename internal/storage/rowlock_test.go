package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TestWithMemoryLock_SerializesSameID asserts that two concurrent
// WithMemoryLock holders on the same memory id observe sequential
// execution: while one body is in flight, the other must wait. The
// test exercises both backends — on Postgres the serialization is
// pg_advisory_xact_lock; on SQlite it is the in-process sync.Mutex
// map on the sqliteDB receiver.
func TestWithMemoryLock_SerializesSameID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		id := uuid.New()

		var inFlight atomic.Int32
		var maxInFlight atomic.Int32

		bumpMax := func() {
			cur := inFlight.Add(1)
			for {
				peak := maxInFlight.Load()
				if cur <= peak || maxInFlight.CompareAndSwap(peak, cur) {
					break
				}
			}
		}

		var wg sync.WaitGroup
		const goroutines = 8
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				err := db.WithMemoryLock(ctx, id, func(_ context.Context, _ *sql.Tx) error {
					bumpMax()
					// Hold the lock long enough for any concurrent goroutine
					// to observe overlap if the helper isn't actually
					// serializing.
					time.Sleep(20 * time.Millisecond)
					inFlight.Add(-1)
					return nil
				})
				if err != nil {
					t.Errorf("WithMemoryLock: %v", err)
				}
			}()
		}
		wg.Wait()

		if peak := maxInFlight.Load(); peak != 1 {
			t.Fatalf("expected at most 1 body in flight at any time (lock holds), got peak %d", peak)
		}
	})
}

// TestWithMemoryLock_DistinctIDsRunParallel asserts that locks on
// different memory ids do NOT serialize against each other — a row
// lock on memory A must not block work on memory B.
//
// Skipped on SQLite because the write pool has MaxOpenConns=1, so any
// two BeginTx calls serialize at the connection level regardless of
// memory id. The parallelism guarantee here is meaningful only on
// Postgres, where pg_advisory_xact_lock holds keyed locks on distinct
// rows without blocking each other.
func TestWithMemoryLock_DistinctIDsRunParallel(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		if db.Backend() == BackendSQLite {
			t.Skip("SQLite write pool is single-conn; cross-id parallelism is a Postgres-only guarantee")
		}
		ctx := context.Background()
		idA := uuid.New()
		idB := uuid.New()

		started := make(chan struct{}, 2)
		release := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(2)

		runLocked := func(id uuid.UUID) {
			defer wg.Done()
			err := db.WithMemoryLock(ctx, id, func(_ context.Context, _ *sql.Tx) error {
				started <- struct{}{}
				<-release
				return nil
			})
			if err != nil {
				t.Errorf("WithMemoryLock: %v", err)
			}
		}

		go runLocked(idA)
		go runLocked(idB)

		// Both bodies must signal "started" before either releases. If the
		// helper accidentally serialized across distinct ids, only one would
		// signal and the test would hang at this read.
		ctxT, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for range 2 {
			select {
			case <-started:
			case <-ctxT.Done():
				t.Fatal("timed out waiting for both locks to enter their bodies; distinct ids must not serialize")
			}
		}
		close(release)
		wg.Wait()
	})
}

// TestWithMemoryLock_BodyErrorRollsBack asserts that a body returning
// a non-nil error rolls back the transaction and surfaces the error
// verbatim, leaving no committed effect.
func TestWithMemoryLock_BodyErrorRollsBack(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		id := uuid.New()

		// Seed a row to mutate; we'll write inside the body and confirm
		// the rollback drops it.
		namespaceID := uuid.New()
		seedRowForLockTest(t, db, namespaceID, id, "seed")

		sentinel := errors.New("sentinel body error")
		err := db.WithMemoryLock(ctx, id, func(ctx context.Context, tx *sql.Tx) error {
			query := `UPDATE memories SET content = ? WHERE id = ?`
			if db.Backend() == BackendPostgres {
				query = `UPDATE memories SET content = $1 WHERE id = $2`
			}
			if _, exErr := tx.ExecContext(ctx, query, "mutated-but-rolled-back", id.String()); exErr != nil {
				return fmt.Errorf("write inside body: %w", exErr)
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected sentinel error to surface verbatim, got %v", err)
		}

		// Content should still be the seed value (rollback dropped the in-body UPDATE).
		var content string
		query := `SELECT content FROM memories WHERE id = ?`
		if db.Backend() == BackendPostgres {
			query = `SELECT content FROM memories WHERE id = $1`
		}
		if scanErr := db.QueryRow(ctx, query, id.String()).Scan(&content); scanErr != nil {
			t.Fatalf("read after rollback: %v", scanErr)
		}
		if content != "seed" {
			t.Fatalf("expected rolled-back content 'seed', got %q", content)
		}
	})
}

// TestMutateInLock_NoLostUpdate is the load-bearing race-reproduction
// test for the helper: N goroutines each append a distinct tag to the
// same memory row via MemoryRepo.MutateInLock. Without the lock, two
// goroutines would read the same baseline, compute their unions
// independently, and the second writer would silently clobber the first's
// tag. With the lock, the final tag set must contain every input tag.
func TestMutateInLock_NoLostUpdate(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		namespaceID := uuid.New()
		id := uuid.New()
		seedRowForLockTest(t, db, namespaceID, id, "race-reproduction-seed")

		const goroutines = 16
		want := make([]string, goroutines)
		for i := range goroutines {
			want[i] = fmt.Sprintf("tag-%02d", i)
		}

		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := range goroutines {
			tag := want[i]
			go func() {
				defer wg.Done()
				_, err := repo.MutateInLock(ctx, id, func(mem *model.Memory) (bool, error) {
					mem.Tags = append(mem.Tags, tag)
					return true, nil
				})
				if err != nil {
					t.Errorf("MutateInLock(%q): %v", tag, err)
				}
			}()
		}
		wg.Wait()

		final, err := repo.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		got := append([]string(nil), final.Tags...)
		sort.Strings(got)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("expected %d tags after %d concurrent merges (no lost updates), got %d: %v",
				len(want), goroutines, len(got), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("tag mismatch at index %d: got %q, want %q (full got=%v)",
					i, got[i], want[i], got)
			}
		}
	})
}

// TestMutateInLock_SkipWriteReturnsFresh asserts that returning (false, nil)
// from the mutator commits the surrounding transaction without performing
// the row UPDATE, and that the returned memory reflects the freshly-read
// state. Use case: callers compute a delta inside the lock, find no change
// is needed, and want to avoid bumping updated_at for nothing.
func TestMutateInLock_SkipWriteReturnsFresh(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewMemoryRepo(db)
		namespaceID := uuid.New()
		id := uuid.New()
		seedRowForLockTest(t, db, namespaceID, id, "no-change-test")

		before, err := repo.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("GetByID before: %v", err)
		}

		// SQLite's stored RFC3339 string strips sub-second precision, so
		// sleep long enough that a write would bump updated_at into a
		// strictly later second. Skipping the write means updated_at is
		// unchanged across the call.
		time.Sleep(1100 * time.Millisecond)

		result, err := repo.MutateInLock(ctx, id, func(mem *model.Memory) (bool, error) {
			if mem.ID != id {
				t.Errorf("mutator saw wrong id: %s vs %s", mem.ID, id)
			}
			return false, nil
		})
		if err != nil {
			t.Fatalf("MutateInLock: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil fresh memory when skipping write")
		}

		after, err := repo.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("GetByID after: %v", err)
		}
		if !after.UpdatedAt.Equal(before.UpdatedAt) {
			t.Errorf("skipped-write call should NOT bump updated_at; before=%s after=%s",
				before.UpdatedAt.Format(time.RFC3339Nano), after.UpdatedAt.Format(time.RFC3339Nano))
		}
	})
}

// seedRowForLockTest creates a minimal memory row for the lock tests via
// the existing NamespaceRepo and MemoryRepo so the test stays insulated
// from schema drift on the seed path.
func seedRowForLockTest(t *testing.T, db DB, namespaceID, memoryID uuid.UUID, content string) {
	t.Helper()
	ctx := context.Background()

	slug := "lock-test-" + namespaceID.String()[:8]
	ns := &model.Namespace{
		ID:       namespaceID,
		Name:     "Lock Test " + namespaceID.String()[:8],
		Slug:     slug,
		Kind:     "org",
		ParentID: &rootID,
		Path:     namespaceID.String(),
		Depth:    1,
	}
	if err := NewNamespaceRepo(db).Create(ctx, ns); err != nil {
		t.Fatalf("seed namespace: %v", err)
	}

	mem := &model.Memory{
		ID:          memoryID,
		NamespaceID: namespaceID,
		Content:     content,
		Confidence:  1.0,
		Importance:  1.0,
		Tags:        []string{},
	}
	if err := NewMemoryRepo(db).Create(ctx, mem); err != nil {
		t.Fatalf("seed memory: %v", err)
	}
}
