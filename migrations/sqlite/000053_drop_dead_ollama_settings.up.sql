-- Drop settings rows for the Ollama-extension knobs that nram sent in the
-- OpenAI-compatibility request body (/v1/chat/completions, /v1/embeddings).
-- Ollama's OpenAI-compat layer does not parse keep_alive, num_ctx, or the
-- repeat_penalty / top_k / min_p sampling extensions from the body, so these
-- were write-only dead data: the code that read them and attached them to the
-- request was removed. Residency is controlled server-side via
-- OLLAMA_KEEP_ALIVE; context and sampling via an Ollama Modelfile. See
-- docs/models.md.
--
-- Idempotent: re-running finds no matching rows.
DELETE FROM settings WHERE key IN (
  'provider.ollama.keep_alive',
  'provider.ollama.num_ctx',
  'enrichment.fact_extraction.repeat_penalty',
  'enrichment.entity_extraction.repeat_penalty',
  'enrichment.fact_extraction.top_k',
  'enrichment.entity_extraction.top_k',
  'enrichment.fact_extraction.min_p',
  'enrichment.entity_extraction.min_p'
);
