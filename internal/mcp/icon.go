package mcp

import (
	"github.com/mark3labs/mcp-go/mcp"
)

// iconURL is the hosted brand mark. Referenced by URL rather than inlined as a
// base64 data URI so the icon costs ~30 bytes per registration site instead of
// ~48 KB; the MCP icon spec permits an HTTPS URL or a data URI for Icon.Src.
const iconURL = "https://nram.ai/favicon.ico"

// iconAnnotation returns the brand icon for attaching to tools, resources,
// and resource templates. Same asset advertised at initialize time via
// ServerInfo.Icons; kept here so every registration site emits an identical
// mcp.Icon value.
func iconAnnotation() mcp.Icon {
	return mcp.Icon{
		Src:      iconURL,
		MIMEType: "image/x-icon",
	}
}
