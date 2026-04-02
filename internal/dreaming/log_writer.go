package dreaming

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// DreamLogWriter provides a convenient API for dream phases to record
// mutations with before/after snapshots for rollback support.
type DreamLogWriter struct {
	repo      *storage.DreamLogRepo
	cycleID   uuid.UUID
	projectID uuid.UUID
	opCount   int
}

// NewDreamLogWriter creates a new DreamLogWriter for the given cycle.
func NewDreamLogWriter(repo *storage.DreamLogRepo, cycleID, projectID uuid.UUID) *DreamLogWriter {
	return &DreamLogWriter{
		repo:      repo,
		cycleID:   cycleID,
		projectID: projectID,
	}
}

// LogOperation records a single mutation performed during a dream phase.
// before and after are marshaled to JSON for the snapshot fields.
func (w *DreamLogWriter) LogOperation(
	ctx context.Context,
	phase, operation, targetType string,
	targetID uuid.UUID,
	before, after interface{},
) error {
	beforeJSON, err := marshalState(before)
	if err != nil {
		return fmt.Errorf("dream log marshal before state: %w", err)
	}

	afterJSON, err := marshalState(after)
	if err != nil {
		return fmt.Errorf("dream log marshal after state: %w", err)
	}

	entry := &model.DreamLog{
		ID:          uuid.New(),
		CycleID:     w.cycleID,
		ProjectID:   w.projectID,
		Phase:       phase,
		Operation:   operation,
		TargetType:  targetType,
		TargetID:    targetID,
		BeforeState: beforeJSON,
		AfterState:  afterJSON,
	}

	if err := w.repo.Create(ctx, entry); err != nil {
		return err
	}
	w.opCount++
	return nil
}

// OpCount returns the number of operations logged so far.
func (w *DreamLogWriter) OpCount() int {
	return w.opCount
}

// ResetOpCount resets the operation counter (used for per-phase tracking).
func (w *DreamLogWriter) ResetOpCount() {
	w.opCount = 0
}

func marshalState(v interface{}) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage(`{}`), nil
	}
	// If already raw JSON, use as-is.
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}
