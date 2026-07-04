// Package instructions holds the canonical nram memory-usage guidance that is
// served at GET /instructions and rendered on the MCP config page. Keeping the
// text here as a single source of truth lets the page and any external caller
// read one copy instead of maintaining drifting duplicates.
package instructions

import _ "embed"

//go:embed data/agent-instructions.md
var full string

//go:embed data/condensed.md
var condensed string

// Lookup returns the instructions body for the given format. The "claude" and
// "agents" formats share the full markdown body (CLAUDE.md and AGENTS.md carry
// identical guidance), which is also the empty-format default. The "condensed"
// format returns the length-limited copy for surfaces that hard-cap the field
// length, such as ChatGPT's 1500-character Custom instructions. "cursor" is a
// deprecated alias of "condensed" retained so existing callers keep working
// while they migrate; Cursor itself now consumes the full canonical body. The
// bool is false for any other value so the handler can reject unknown formats
// rather than silently defaulting.
func Lookup(format string) (string, bool) {
	switch format {
	case "", "claude", "agents":
		return full, true
	case "condensed", "cursor":
		return condensed, true
	default:
		return "", false
	}
}
