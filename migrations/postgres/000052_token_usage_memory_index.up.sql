-- Per-phase enrichment metrics read path. EnrichmentAdminStore.attachPhaseMetrics
-- hydrates the enrichment-queue page on every poll/refresh via
-- TokenUsageRepo.ListByMemoryIDs: WHERE memory_id IN (...) AND operation IN (...)
-- ORDER BY created_at DESC. token_usage had no memory_id index, so each refresh
-- scanned the whole table (one row per LLM request). This composite serves the
-- memory_id seek and the per-memory created_at ordering. Additive read-path
-- speedup; does not change query results.
CREATE INDEX IF NOT EXISTS idx_token_usage_memory_time
  ON token_usage (memory_id, created_at);
