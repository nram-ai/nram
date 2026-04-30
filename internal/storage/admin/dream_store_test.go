package admin

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

func TestDecorate_RunningOlderThanThresholdIsAbandonable(t *testing.T) {
	now := time.Now().UTC()
	c := &model.DreamCycle{
		ID:        uuid.New(),
		Status:    model.DreamStatusRunning,
		UpdatedAt: now.Add(-31 * time.Minute),
	}
	decorate(c, thresholds{stuck: 30 * time.Minute, heartbeat: 2 * time.Minute}, now)
	if !c.IsAbandonable {
		t.Fatalf("expected IsAbandonable=true for 31-minute stale running cycle")
	}
}

func TestDecorate_RunningWithinThresholdIsNotAbandonable(t *testing.T) {
	now := time.Now().UTC()
	c := &model.DreamCycle{
		Status:    model.DreamStatusRunning,
		UpdatedAt: now.Add(-29 * time.Minute),
	}
	decorate(c, thresholds{stuck: 30 * time.Minute, heartbeat: 2 * time.Minute}, now)
	if c.IsAbandonable {
		t.Fatalf("expected IsAbandonable=false for 29-minute stale running cycle")
	}
}

func TestDecorate_NonRunningStatusIsNeverFlagged(t *testing.T) {
	now := time.Now().UTC()
	stale := time.Now().Add(-1 * time.Hour)
	for _, status := range []string{
		model.DreamStatusPending,
		model.DreamStatusCompleted,
		model.DreamStatusFailed,
		model.DreamStatusRolledBack,
	} {
		c := &model.DreamCycle{
			Status:      status,
			UpdatedAt:   stale,
			HeartbeatAt: &stale,
		}
		decorate(c, thresholds{stuck: time.Minute, heartbeat: time.Minute}, now)
		if c.IsAbandonable {
			t.Fatalf("status=%q should not be IsAbandonable", status)
		}
		if c.IsStaleDiagnostic {
			t.Fatalf("status=%q should not be IsStaleDiagnostic", status)
		}
	}
}

func TestDecorate_HeartbeatStaleSetsDiagnosticFlag(t *testing.T) {
	now := time.Now().UTC()
	hb := now.Add(-3 * time.Minute)
	c := &model.DreamCycle{
		Status:      model.DreamStatusRunning,
		UpdatedAt:   now.Add(-1 * time.Minute), // not yet abandonable
		HeartbeatAt: &hb,
	}
	decorate(c, thresholds{stuck: 30 * time.Minute, heartbeat: 2 * time.Minute}, now)
	if c.IsAbandonable {
		t.Fatalf("expected IsAbandonable=false (updated_at still fresh)")
	}
	if !c.IsStaleDiagnostic {
		t.Fatalf("expected IsStaleDiagnostic=true (heartbeat 3 min old)")
	}
}

func TestDecorate_NilHeartbeatNoDiagnosticFlag(t *testing.T) {
	now := time.Now().UTC()
	c := &model.DreamCycle{
		Status:    model.DreamStatusRunning,
		UpdatedAt: now,
	}
	decorate(c, thresholds{stuck: 30 * time.Minute, heartbeat: 2 * time.Minute}, now)
	if c.IsStaleDiagnostic {
		t.Fatalf("expected IsStaleDiagnostic=false when heartbeat_at is nil")
	}
}
