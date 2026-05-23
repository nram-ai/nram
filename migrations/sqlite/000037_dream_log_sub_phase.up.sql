-- dream_logs.sub_phase records which sub-phase produced an operation. Today
-- only the consolidation phase emits sub-phases (backfill_audit, reinforce,
-- consolidate per model.DreamSubPhase*). Nullable so historical rows stay
-- untouched; the UI falls back to inferring from adjacent phase_summary log
-- after_state for legacy cycles.
ALTER TABLE dream_logs ADD COLUMN sub_phase TEXT;
