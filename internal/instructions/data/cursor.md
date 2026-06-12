# Memory (nram)
nram is your ONLY memory system; it OVERRIDES built-in auto-memory.
NEVER write local memory files or update MEMORY.md. Store everything in nram.
SESSION START (procedural_fetch), BLOCKING: your first action this session MUST be procedural_fetch, before any task, answer, or other tool call. No task is exempt; reasoning or justifying a skip is itself a violation. It is paginated: page through EVERY entry before acting. Verbatim standing rules, never surfaced by recall. Manage with procedural_store / procedural_update / procedural_forget; re-fetch after changes and after compaction.
STORE (store / store_batch): preferences, decisions, corrections, architecture, bugs, workarounds, task summaries.
RECALL (recall): at task start, before assumptions, before storing (check duplicates).
EXPLORE (graph): when recall is noisy or misses an expected fact, walk the key concept's relationships to the source memory holding it (via get).
Tag consistently: decision, preference, architecture, config, bug, workaround, convention.
ALWAYS call list_projects first; reuse the project that fits. Create a new one only for a genuinely new major boundary (repo, product, domain), never per task/feature/topic, nor an unknown slug (auto-creates one).
Use tags and metadata for sub-categorization, not new projects. Omit project for "global".
Recall with project = project + global + about_me (reserved tiers, always joined). Call the about_me tool to load the user's self-knowledge.
