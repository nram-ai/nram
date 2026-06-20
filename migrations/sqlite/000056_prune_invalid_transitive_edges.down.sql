-- No-op. The up migration hard-deletes invalid inferred (transitive) edges,
-- which are derived data with no source-of-truth to restore from. The corrected
-- transitive discovery phase re-derives the valid subset on later dream cycles.
SELECT 1;
