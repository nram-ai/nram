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

// argBool extracts a boolean tool argument by key, returning defaultVal
// when the key is absent or not a bool.
func argBool(args map[string]interface{}, key string, defaultVal bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return defaultVal
}
