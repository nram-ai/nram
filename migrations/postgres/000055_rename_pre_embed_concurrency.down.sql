-- Reverse the rename back to the legacy key.
-- Idempotent: re-running matches no new-key rows.
UPDATE settings SET key = 'enrichment.worker.pre_embed_concurrency'
 WHERE key = 'enrichment.worker.llm_concurrency';
