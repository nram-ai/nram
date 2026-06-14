-- Rename the enrichment worker LLM fan-out setting from the old
-- pipeline-jargon key (enrichment.worker.pre_embed_concurrency) to the
-- clearer enrichment.worker.llm_concurrency. The Go const
-- SettingEnrichmentWorkerLLMConcurrency and the admin schema-registry entry
-- move in the same change; this carries any operator-set live value across the
-- rename so a non-default value is preserved.
--
-- Idempotent: re-running matches no old-key rows.
UPDATE settings SET key = 'enrichment.worker.llm_concurrency'
 WHERE key = 'enrichment.worker.pre_embed_concurrency';
