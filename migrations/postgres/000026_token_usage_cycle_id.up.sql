-- Add a cycle_id column so dream-time token_usage rows can be attributed
-- to the dream cycle that incurred them. dream_cycles.tokens_used is now
-- derived live from SUM(tokens_input + tokens_output) FROM token_usage
-- WHERE cycle_id = dream_cycles.id, replacing the previous in-memory
-- TokenBudget tally that only six of eight phases ever incremented and
-- that froze at the boundary value of the last completed phase when a
-- long phase was Abandoned mid-flight.
--
-- Non-dream callers leave cycle_id NULL; the partial index makes the
-- correlated SUM cheap regardless of how large token_usage grows.
ALTER TABLE token_usage ADD COLUMN cycle_id UUID REFERENCES dream_cycles(id);

CREATE INDEX IF NOT EXISTS idx_token_usage_cycle_id
  ON token_usage (cycle_id) WHERE cycle_id IS NOT NULL;
