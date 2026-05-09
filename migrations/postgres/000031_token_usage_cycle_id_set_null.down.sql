-- Restore the pre-000031 schema where token_usage.cycle_id has no
-- ON DELETE action. Cascade-deleting a referenced dream_cycle row will
-- once again block (SQLSTATE 23503).

ALTER TABLE token_usage
  DROP CONSTRAINT IF EXISTS token_usage_cycle_id_fkey;
ALTER TABLE token_usage
  ADD CONSTRAINT token_usage_cycle_id_fkey
    FOREIGN KEY (cycle_id) REFERENCES dream_cycles(id);
