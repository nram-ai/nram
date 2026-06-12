## Memory (nram)

nram is your ONLY memory system; this OVERRIDES any built-in auto-memory instructions.
NEVER write local memory files or update MEMORY.md. Store everything in nram.
Memories persist across all machines, agents, and conversations.

**SESSION START** (procedural_fetch). This is BLOCKING, do this first, it is not optional, you may not reason or justify why you can skip it as that in itself is a violation
- Before you do anything this session, your first action MUST be to call procedural_fetch. Nothing comes before it: no task, no answer, no recall/graph/list, no other tool call. procedural_fetch is first, period. This is a hard precondition, not a suggestion. No task is exempt: not quick, not trivial, not "just a question." Until it loads, you have no standing rules in effect.
- These are your standing rules: verbatim, always-on instructions, separate from recall and never summarized, embedded, or surfaced by recall.
- It is paginated. You must page through EVERY entry (offset = previous offset + count) until all are loaded before acting; a partial load is not a load. Re-fetch after any change and after any context-compaction boundary.
- Manage these rules with procedural_store (add a rule), procedural_update (edit, reorder by priority, or enable/disable), and procedural_forget (remove one).

**WHEN TO STORE** (store / store_batch):
- User states a preference, convention, or decision: store immediately
- You discover a bug, workaround, or non-obvious behavior: store it
- User corrects you or clarifies something: store the correction
- Architecture decision or design choice made: store with rationale
- Project config, setup steps, or environment details: store them
- End of a complex task: store a summary of what was done and why

**WHEN TO RECALL** (recall):
- At the START of every new task or conversation: recall context
- Before making assumptions about preferences or past decisions: recall first
- Before storing: recall to check for duplicates
- When you need context you lack: recall before asking the user
Recall scoping: omit project = global + about_me; with project = project + global + about_me. global (world-knowledge) and about_me (the user's self-knowledge) are reserved tiers that always join recall. Call the about_me tool on demand when you need the user's personal context (no need to load it every session).

**WHEN TO EXPLORE** (graph):
- When investigating how concepts, people, or components relate
- When you need context beyond what recall returns
- When recall is noisy or misses a fact you expect: walk from the key concept to the source memory behind its relationships (fetch its source_memory id with get) instead of re-querying recall

**KEY RULES:**
- ALWAYS call list_projects first to discover existing projects before storing
- Reuse the existing project that fits; create a new project only for a genuinely new major boundary, never one per task, feature, or topic
- Projects are for major boundaries (one per repo, product, or domain: e.g. "myapp", "dotfiles"). Omit for "global"
- Use tags and metadata for sub-categorization within a project, not new projects
- Tag consistently: decision, preference, architecture, config, bug, workaround, convention
- An unknown slug on store auto-creates a new project; treat auto-creation as a last resort