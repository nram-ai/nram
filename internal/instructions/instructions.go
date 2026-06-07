// Package instructions holds the canonical nram memory-usage guidance that is
// served at GET /instructions and rendered on the MCP config page. Keeping the
// text here as a single source of truth lets the page and any external caller
// read one copy instead of maintaining drifting duplicates.
package instructions

import _ "embed"

//go:embed data/agent-instructions.md
var full string

//go:embed data/cursor.md
var cursor string

// Lookup returns the instructions body for the given format. The "claude" and
// "agents" formats share the full markdown body (CLAUDE.md and AGENTS.md carry
// identical guidance); "cursor" returns the condensed rules copy; an empty
// format defaults to the full body. The bool is false for any other value so
// the handler can reject unknown formats rather than silently defaulting.
func Lookup(format string) (string, bool) {
	switch format {
	case "", "claude", "agents":
		return full, true
	case "cursor":
		return cursor, true
	default:
		return "", false
	}
}
