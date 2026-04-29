package service

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
)

// MemoryReinforcer is the narrow write-capability RecallService needs to
// reinforce memories after a successful recall. It is deliberately separate
// from MemoryReader because recall is a read operation from the caller's
// perspective — the reinforcement write is a side effect, and mixing the
// interfaces would force every mock reader in the codebase to grow a
// method it does not use.
type MemoryReinforcer interface {
	BumpReinforcement(ctx context.Context, ids []uuid.UUID, now time.Time, factor float64) (int64, error)
}

// SettingsReader resolves setting values. Narrower than SettingsService so
// tests can stub it cheaply.
type SettingsReader interface {
	Resolve(ctx context.Context, key string, scope string) (string, error)
	ResolveFloat(ctx context.Context, key string, scope string) (float64, error)
	ResolveInt(ctx context.Context, key string, scope string) (int, error)
}

// ReinforcementDeps carries the optional dependencies that activate the
// reconsolidation hook. When any is nil the hook is effectively off.
type ReinforcementDeps struct {
	Writer   MemoryReinforcer
	Settings SettingsReader
	Bus      events.EventBus
	// Scope is the settings scope for reading reconsolidation keys.
	// Defaults to "global" when empty.
	Scope string
}

// reinforcementEvent is the data payload for events.MemoryReinforced.
// Only the first 20 memory IDs are included to bound event size; the count
// field carries the true total.
type reinforcementEvent struct {
	Mode       string      `json:"mode"`
	Count      int         `json:"count"`
	Factor     float64     `json:"factor"`
	MemoryIDs  []uuid.UUID `json:"memory_ids,omitempty"`
	ElapsedMs  int64       `json:"elapsed_ms"`
	Persisted  int64       `json:"persisted,omitempty"` // non-zero only in persist mode
}

// reinforce applies reconsolidation to the given memory IDs. The three
// possible outcomes:
//
//   - mode=off       → do nothing.
//   - mode=shadow    → emit the event with the would-be deltas but do not
//                       write to the database.
//   - mode=persist   → write to the database and emit the event.
//
// reinforce is called from a goroutine spawned by Recall. It is safe to call
// with nil dependencies; it short-circuits harmlessly when reinforcement is
// not wired up.
func (s *RecallService) reinforce(ctx context.Context, ids []uuid.UUID) {
	if len(ids) == 0 {
		return
	}
	if s.reinforcement == nil || s.reinforcement.Settings == nil {
		return
	}

	scope := s.reinforcement.Scope
	if scope == "" {
		scope = "global"
	}

	mode, _ := s.reinforcement.Settings.Resolve(ctx, SettingReconsolidationMode, scope)
	if mode == "" {
		mode = ReconsolidationModeShadow
	}
	if mode == ReconsolidationModeOff {
		return
	}

	factor, err := s.reinforcement.Settings.ResolveFloat(ctx, SettingReconsolidationFactor, scope)
	if err != nil || factor <= 0 {
		factor = 0.02
	}

	start := time.Now()
	var persisted int64
	if mode == ReconsolidationModePersist && s.reinforcement.Writer != nil {
		persisted, err = s.reinforcement.Writer.BumpReinforcement(ctx, ids, time.Now().UTC(), factor)
		if err != nil {
			slog.Warn("recall: reinforcement write failed", "err", err, "count", len(ids))
			// Still emit the event so observers can see that an attempt was made.
		}
	}

	cap, cerr := s.reinforcement.Settings.ResolveInt(ctx, SettingReinforcementEventMemoryCap, scope)
	if cerr != nil || cap < 1 {
		// Fall through to the registered default to avoid drift between code
		// and schema; see settingDefaults[SettingReinforcementEventMemoryCap].
		if def, ok := settingDefaults[SettingReinforcementEventMemoryCap]; ok {
			if v, perr := strconv.Atoi(def); perr == nil && v >= 1 {
				cap = v
			}
		}
	}
	idsForEvent := ids
	if len(idsForEvent) > cap {
		idsForEvent = idsForEvent[:cap]
	}

	payload := reinforcementEvent{
		Mode:      mode,
		Count:     len(ids),
		Factor:    factor,
		MemoryIDs: idsForEvent,
		ElapsedMs: time.Since(start).Milliseconds(),
		Persisted: persisted,
	}
	events.Emit(ctx, s.reinforcement.Bus, events.MemoryReinforced, "global", payload)
}

// SetReinforcement wires the optional reconsolidation hook. Passing a zero
// ReinforcementDeps disables reinforcement (it is off by default).
func (s *RecallService) SetReinforcement(deps *ReinforcementDeps) {
	s.reinforcement = deps
}
