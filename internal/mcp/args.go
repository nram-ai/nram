package mcp

import (
	"context"
	"strconv"

	"github.com/nram-ai/nram/internal/service"
)

// includeSupersededArg is the optional bool argument that controls whether
// the export MCP tool surfaces rows that paraphrase or contradiction dedup
// has marked as losers. The other read-side MCP tools have this flag
// stripped (the diagnostic surface is REST/admin-only); export retains it
// because exports are an intentional backup/migration surface where seeing
// the full lineage often matters.
const includeSupersededArg = "include_superseded"

// includeSupersededDesc is the standard tool-arg description for export's
// include_superseded flag.
const includeSupersededDesc = "Include rows that were superseded by paraphrase or contradiction dedup. Default false hides them."

// argBool extracts a boolean tool argument by key, returning defaultVal
// when the key is absent or not a bool.
func argBool(args map[string]interface{}, key string, defaultVal bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return defaultVal
}

// resolvePositiveCapInt resolves an integer cap setting and returns a value
// guaranteed to be > 0. If the resolved value is ≤0 — which the admin
// settings schema rejects at write time but a stale row or hand-edited
// override can still produce — it falls back to the registered default in
// settingDefaults rather than silently disabling the cap. Callers use this
// for MCP-side limits (recall.max_limit, recall.graph.max_depth) where a
// zero is never the intended "no cap" knob.
func resolvePositiveCapInt(ctx context.Context, settings *service.SettingsService, key string) int {
	if v := settings.ResolveIntWithDefault(ctx, key, "global"); v > 0 {
		return v
	}
	def, ok := service.GetDefault(key)
	if !ok {
		return 1
	}
	parsed, err := strconv.Atoi(def)
	if err != nil || parsed <= 0 {
		return 1
	}
	return parsed
}
