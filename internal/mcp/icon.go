package mcp

import (
	"encoding/base64"
	_ "embed"

	"github.com/mark3labs/mcp-go/mcp"
)

//go:embed assets/icon.png
var iconPNG []byte

// iconDataURIValue is the embedded brand mark encoded once at program init.
// Held as a base64 data URL so the MCP initialize hook can hand it directly
// to mcp.Icon.Src without re-encoding per request.
var iconDataURIValue = "data:image/png;base64," + base64.StdEncoding.EncodeToString(iconPNG)

// iconAnnotation returns the brand icon for attaching to tools, resources,
// and resource templates. Same asset advertised at initialize time via
// ServerInfo.Icons; kept here so every registration site emits an identical
// mcp.Icon value.
func iconAnnotation() mcp.Icon {
	return mcp.Icon{
		Src:      iconDataURIValue,
		MIMEType: "image/png",
		Sizes:    []string{"200x200"},
	}
}
