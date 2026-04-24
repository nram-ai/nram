-- Track when a memory was superseded so the prune clock at
-- phase_pruning.go shouldPrune is immune to unrelated row touches that bump
-- updated_at. Backfill existing supersede rows with updated_at as a safe
-- approximation; new supersessions write the column directly.
ALTER TABLE memories ADD COLUMN superseded_at TEXT;
UPDATE memories SET superseded_at = updated_at
  WHERE superseded_by IS NOT NULL AND superseded_at IS NULL;
