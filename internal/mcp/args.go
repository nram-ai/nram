package mcp

// includeSupersededArg is the optional bool argument that controls whether
// MCP read tools surface rows that paraphrase or contradiction dedup has
// marked as losers.
const includeSupersededArg = "include_superseded"

// includeSupersededDesc is the standard tool-arg description used by every
// MCP read tool that hides superseded rows by default. memory_graph keeps
// its own description because the filter there acts on relationships, not
// memory rows.
const includeSupersededDesc = "Include rows that were superseded by paraphrase or contradiction dedup. Default false hides them."

// includeAuditArg is the optional bool argument that controls whether
// memory_get surfaces the per-memory novelty/bookkeeping audit-stamp keys
// that the projector strips by default.
const includeAuditArg = "include_audit"

// includeAuditDesc is the standard tool-arg description for include_audit on
// memory_get.
const includeAuditDesc = "Include the per-memory novelty/bookkeeping audit-stamp keys (novelty_audited_at, novelty_audit_reason, contradictions_checked_at, paraphrase_checked_at, low_novelty, low_novelty_reason) in the response metadata. Default false strips them."

// argBool extracts a boolean tool argument by key, returning defaultVal
// when the key is absent or not a bool.
func argBool(args map[string]interface{}, key string, defaultVal bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return defaultVal
}
