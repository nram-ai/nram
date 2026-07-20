// Package instructions holds all three tiers of nram's agent memory-usage
// guidance in one directory so they can be read side by side and diffed.
//
// Two tiers are served over HTTP by Lookup: the full markdown body behind GET
// /instructions and the MCP config page, and the length-limited condensed copy
// for surfaces that hard-cap the field. The third is Handshake, the base text
// of the MCP initialize instructions; it is NOT reachable through Lookup and is
// never web-served. internal/mcp composes it into the final handshake by
// substituting the provider-conditional retrieval and enrichment blocks, which
// vary per connection and therefore cannot live in a static file.
//
// The tiers are length-ordered (full, handshake, condensed) and two are capped,
// so they carry different amounts of detail by design. What must not vary is
// the core guidance; TestTiersAgree pins the facts that all three must state.
package instructions

import (
	_ "embed"
	"strings"
)

//go:embed data/agent-instructions.md
var full string

//go:embed data/condensed.md
var condensed string

//go:embed data/mcp-handshake.md
var handshake string

// The handshake body carries two placeholders that the caller fills with text
// that varies per connection and so cannot be static. The markers are unexported
// because the file format is this package's business: callers supply the two
// blocks and never learn how the body is cut.
const (
	retrievalMarker  = "{{retrieval}}\n"
	enrichmentMarker = "{{enrichment}}\n"
)

// The handshake split once, at package init. Splitting here rather than per call
// means a malformed data file panics at process start, which is the honest time
// to fail for content fixed at compile time; deferring it to the first splice
// would boot a green server that dies on its first client connection.
var handshakeHead, handshakeMid, handshakeFoot = splitHandshake()

func splitHandshake() (head, mid, foot string) {
	// The trailing newline goes so the composed handshake ends on the last rule.
	head, rest, ok := strings.Cut(strings.TrimSuffix(handshake, "\n"), retrievalMarker)
	if !ok {
		panic("instructions: data/mcp-handshake.md is missing the " + retrievalMarker + " marker")
	}
	mid, foot, ok = strings.Cut(rest, enrichmentMarker)
	if !ok {
		panic("instructions: data/mcp-handshake.md is missing the " + enrichmentMarker + " marker")
	}
	return head, mid, foot
}

// ComposeHandshake returns the MCP initialize instructions, splicing the two
// caller-supplied blocks into the embedded body. Both blocks are
// provider-conditional, which is why they are composed rather than stored.
//
// The result is deliberately unreachable through Lookup: it is a wire payload
// for MCP clients, not a document for humans to paste, and serving it over
// /instructions would give operators a fourth flavor to keep in step.
func ComposeHandshake(retrieval, enrichment string) string {
	var b strings.Builder
	b.Grow(len(handshakeHead) + len(retrieval) + len(handshakeMid) + len(enrichment) + len(handshakeFoot))
	b.WriteString(handshakeHead)
	b.WriteString(retrieval)
	b.WriteString(handshakeMid)
	b.WriteString(enrichment)
	b.WriteString(handshakeFoot)
	return b.String()
}

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
