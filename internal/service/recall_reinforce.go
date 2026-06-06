package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
)

// MemoryReinforcer is the narrow write-capability RecallService needs to
// reinforce memories after a successful recall. It is deliberately separate
// from MemoryReader because recall is a read operation from the caller's
// perspective: the reinforcement write is a side effect, and mixing the
// interfaces would force every mock reader in the codebase to grow a
// method it does not use.
type MemoryReinforcer interface {
	BumpReinforcement(ctx context.Context, ids []uuid.UUID, now time.Time, factor float64) (int64, error)
}

// RelationshipReinforcer is the narrow write-capability RecallService needs to
// reinforce graph edges after a recall surfaces them. The dream-side
// weight_adjustment phase computes a multi-memory support multiplier; this
// hook is its complement: it raises weight when the LLM actively touches an
// edge, so a heavily-used relationship cannot silently atrophy under decay.
//
// Refs may span namespaces (primary plus cross-namespace shares and
// global), so the caller groups by namespace and issues one
// BatchReinforce per group. The clamp at 2.0 lives at the SQL layer in
// RelationshipRepo so the cap cannot drift between the recall write and
// the dream-phase read.
type RelationshipReinforcer interface {
	Reinforce(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, delta float64) error
	BatchReinforce(ctx context.Context, namespaceID uuid.UUID, items []model.ReinforceItem) (int64, error)
}

// RelationshipRef pairs a relationship's id with its namespace so the recall
// path can reinforce edges that span the primary namespace plus its
// dependents (cross-namespace shares, global) in a single goroutine without
// re-resolving namespace at write time.
type RelationshipRef struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
}

// SettingsReader resolves setting values. Narrower than SettingsService so
// tests can stub it cheaply.
type SettingsReader interface {
	Resolve(ctx context.Context, key string, scope string) (string, error)
	ResolveFloat(ctx context.Context, key string, scope string) (float64, error)
	ResolveInt(ctx context.Context, key string, scope string) (int, error)
	ResolveFloatWithDefault(ctx context.Context, key, scope string) float64
	ResolveIntWithDefault(ctx context.Context, key, scope string) int
}

// ReinforcementDeps carries the optional dependencies that activate the
// reconsolidation hook. When any is nil the hook is effectively off.
//
// RelWriter is optional alongside Writer: wiring only the memory side keeps
// the historical behavior; wiring only the relationship side enables graph
// reinforcement without touching memory confidence. Both null = both off.
type ReinforcementDeps struct {
	Writer    MemoryReinforcer
	RelWriter RelationshipReinforcer
	Settings  SettingsReader
	Bus       events.EventBus
	// Scope is the settings scope for reading reconsolidation keys.
	// Defaults to "global" when empty.
	Scope string
}

// reinforcementEvent is the data payload for events.MemoryReinforced.
// Only the first 20 memory IDs are included to bound event size; the count
// field carries the true total.
type reinforcementEvent struct {
	Mode      string      `json:"mode"`
	Count     int         `json:"count"`
	Factor    float64     `json:"factor"`
	MemoryIDs []uuid.UUID `json:"memory_ids,omitempty"`
	ElapsedMs int64       `json:"elapsed_ms"`
	Persisted int64       `json:"persisted,omitempty"` // non-zero only in persist mode
}

// relReinforcementEvent is the data payload for
// events.RelationshipReinforced. Mirrors reinforcementEvent's shape so a
// downstream observer subscribing to both can use the same parser. Delta
// stands in for Factor because relationship reinforcement is additive
// (delta added to weight, capped at 2.0) rather than multiplicative.
type relReinforcementEvent struct {
	Mode            string      `json:"mode"`
	Count           int         `json:"count"`
	Delta           float64     `json:"delta"`
	RelationshipIDs []uuid.UUID `json:"relationship_ids,omitempty"`
	ElapsedMs       int64       `json:"elapsed_ms"`
	Persisted       int64       `json:"persisted,omitempty"` // non-zero only in persist mode
}

// reinforce applies reconsolidation to the given memory IDs. The three
// possible outcomes:
//
//   - mode=off       → do nothing.
//   - mode=shadow    → emit the event with the would-be deltas but do not
//     write to the database.
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

	factor := s.reinforcement.Settings.ResolveFloatWithDefault(ctx, SettingReconsolidationFactor, scope)
	if factor <= 0 {
		factor = GetDefaultFloat(SettingReconsolidationFactor)
	}

	start := time.Now()
	var persisted int64
	if mode == ReconsolidationModePersist && s.reinforcement.Writer != nil {
		var err error
		persisted, err = s.reinforcement.Writer.BumpReinforcement(ctx, ids, time.Now().UTC(), factor)
		if err != nil {
			slog.Warn("recall: reinforcement write failed", "err", err, "count", len(ids))
			// Still emit the event so observers can see that an attempt was made.
		}
	}

	cap := s.reinforcement.Settings.ResolveIntWithDefault(ctx, SettingReinforcementEventMemoryCap, scope)
	if cap < 1 {
		cap = GetDefaultInt(SettingReinforcementEventMemoryCap)
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

// reinforceRels applies graph-edge reinforcement to the given relationships.
// Recall side is additive (delta), dream side is multiplicative
// (support_gain), independent signals composing at the SQL-layer 2.0 cap.
// Gated by SettingReconsolidationMode so the whole reconsolidation pathway
// has one kill switch.
func (s *RecallService) reinforceRels(ctx context.Context, refs []RelationshipRef) {
	if len(refs) == 0 {
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

	delta := s.reinforcement.Settings.ResolveFloatWithDefault(ctx, SettingDreamingWeightRecallReinforceDelta, scope)
	if delta <= 0 {
		delta = GetDefaultFloat(SettingDreamingWeightRecallReinforceDelta)
	}

	start := time.Now()
	var persisted int64
	if mode == ReconsolidationModePersist && s.reinforcement.RelWriter != nil {
		// Group refs by namespace so each BatchReinforce call can run
		// inside a single tx. The traverser may surface edges across the
		// primary namespace plus cross-namespace shares and global, so
		// the bucket count is typically small but never assumed to be 1.
		byNamespace := make(map[uuid.UUID][]model.ReinforceItem, 4)
		for _, ref := range refs {
			byNamespace[ref.NamespaceID] = append(byNamespace[ref.NamespaceID],
				model.ReinforceItem{ID: ref.ID, Delta: delta})
		}
		for nsID, items := range byNamespace {
			// BatchReinforce returns RowsAffected; missing-id races
			// surface as a smaller affected count, not as an error.
			// Any error returned here is a genuine driver/SQL failure.
			n, werr := s.reinforcement.RelWriter.BatchReinforce(ctx, nsID, items)
			if werr != nil {
				slog.Warn("recall: relationship batch reinforcement write failed", "err", werr, "ns", nsID, "count", len(items))
				continue
			}
			persisted += n
		}
	}

	cap := s.reinforcement.Settings.ResolveIntWithDefault(ctx, SettingReinforcementEventRelationshipCap, scope)
	if cap < 1 {
		cap = GetDefaultInt(SettingReinforcementEventRelationshipCap)
	}
	idsForEvent := make([]uuid.UUID, 0, len(refs))
	for _, ref := range refs {
		idsForEvent = append(idsForEvent, ref.ID)
	}
	if len(idsForEvent) > cap {
		idsForEvent = idsForEvent[:cap]
	}

	payload := relReinforcementEvent{
		Mode:            mode,
		Count:           len(refs),
		Delta:           delta,
		RelationshipIDs: idsForEvent,
		ElapsedMs:       time.Since(start).Milliseconds(),
		Persisted:       persisted,
	}
	events.Emit(ctx, s.reinforcement.Bus, events.RelationshipReinforced, "global", payload)
}

// ReinforceGraphEdgesAsync fires the relationship-reinforcement hook for
// the given refs in a goroutine. Public so MCP handlers that surface graph
// edges outside Recall (memory_graph) write back through the same gate and
// event as Recall itself.
func (s *RecallService) ReinforceGraphEdgesAsync(refs []RelationshipRef) {
	if s.reinforcement == nil || len(refs) == 0 {
		return
	}
	copied := make([]RelationshipRef, len(refs))
	copy(copied, refs)
	go func(refs []RelationshipRef) {
		defer func() { _ = recover() }()
		s.reinforceRels(context.Background(), refs)
	}(copied)
}

// SetReinforcement wires the optional reconsolidation hook. Passing a zero
// ReinforcementDeps disables reinforcement (it is off by default).
func (s *RecallService) SetReinforcement(deps *ReinforcementDeps) {
	s.reinforcement = deps
}
