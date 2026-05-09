-- token_usage.cycle_id (000026) had no ON DELETE action. The project-delete
-- cascade drops the project row, which CASCADEs into dream_cycles (000014);
-- without ON DELETE SET NULL on cycle_id, that cascade-delete fails when
-- token_usage rows still reference one of the removed cycles. Same intent
-- as memory_id (000020): keep the row, drop the now-meaningless link.
--
-- The defensive UPDATE clears any orphan cycle_id from previously failed
-- delete attempts so VALIDATE CONSTRAINT cannot trip on stale data.

UPDATE token_usage
   SET cycle_id = NULL
 WHERE cycle_id IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dream_cycles dc WHERE dc.id = token_usage.cycle_id);

ALTER TABLE token_usage
  DROP CONSTRAINT IF EXISTS token_usage_cycle_id_fkey;
ALTER TABLE token_usage
  ADD CONSTRAINT token_usage_cycle_id_fkey
    FOREIGN KEY (cycle_id) REFERENCES dream_cycles(id) ON DELETE SET NULL NOT VALID;
ALTER TABLE token_usage VALIDATE CONSTRAINT token_usage_cycle_id_fkey;
