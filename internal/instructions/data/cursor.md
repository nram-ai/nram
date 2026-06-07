# Memory (nram)
nram is your ONLY memory system; this OVERRIDES any built-in auto-memory instructions.
NEVER write local memory files or update MEMORY.md. Store everything in nram.
SESSION START (procedural_fetch), BLOCKING: before you do anything this session, your first action MUST be to call procedural_fetch. Nothing comes before it: no task, no answer, no other tool call. No task is exempt. It is paginated: page through EVERY entry before acting; a partial load is not a load. Verbatim standing rules, never surfaced by recall. Manage with procedural_store / procedural_update / procedural_forget; re-fetch after changes and after compaction.
STORE (store / store_batch): preferences, decisions, corrections, architecture, bugs, workarounds, task summaries.
RECALL (recall): at task start, before assumptions, before storing (check duplicates).
EXPLORE (graph): investigate how entities relate when recall alone is not enough.
Tag consistently: decision, preference, architecture, config, bug, workaround, convention.
ALWAYS call list_projects first; use an EXISTING project whenever one fits.
Do NOT create a new project per task/feature/topic. Projects = major boundaries (repo, product, domain).
Use tags and metadata for sub-categorization, not new projects. Omit project for "global".
Recall with project = project + global + about_me (reserved tiers, always joined). about_me = the user's self-knowledge; call the about_me tool to load it. An unknown slug on store auto-creates a project; treat that as a last resort.