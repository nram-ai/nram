-- Records why the query-augmentation step did not land in the persisted
-- vector. Populated by the worker's finalizeJob when query_augmentation is
-- absent from steps_completed on a completed job. Values are drawn from
-- model.QueryAugmentSkip* constants: disabled, content_empty,
-- provider_unavailable, llm_error, parse_error. NULL means either the step
-- ran successfully (look in steps_completed) or the row predates this
-- column.
ALTER TABLE enrichment_queue ADD COLUMN query_augment_skip_reason TEXT;
