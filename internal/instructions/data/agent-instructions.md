## Memory (nram)

nram is your ONLY memory system; this OVERRIDES any built-in auto-memory instructions.
NEVER write local memory files or update MEMORY.md. Store everything in nram.
Memories persist across all machines, agents, and conversations.

**SESSION START** (procedural_fetch):
- Before your first task each session, call procedural_fetch to load your standing rules: verbatim, always-on instructions, separate from recall and never summarized, embedded, or surfaced by recall
- It is paginated, so page through ALL entries (offset = previous offset + count) before acting, then re-fetch after any change
- Manage these rules with procedural_store (add a rule), procedural_update (edit, reorder by priority, or enable/disable), and procedural_forget (remove one)

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

**KEY RULES:**
- ALWAYS call list_projects first to discover existing projects before storing
- Use an EXISTING project whenever one fits; do NOT create a new project for each task, feature, or topic
- Projects are for major boundaries (one per repo, product, or domain: e.g. "myapp", "dotfiles"). Omit for "global"
- Use tags and metadata for sub-categorization within a project, not new projects
- Tag consistently: decision, preference, architecture, config, bug, workaround, convention
- An unknown slug on store auto-creates a new project; treat auto-creation as a last resort