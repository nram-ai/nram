package mcp

import (
	"encoding/base64"
	_ "embed"
)

//go:embed assets/icon.png
var iconPNG []byte

// iconDataURIValue is the embedded brand mark encoded once at program init.
// Held as a base64 data URL so the MCP initialize hook can hand it directly
// to mcp.Icon.Src without re-encoding per request.
var iconDataURIValue = "data:image/png;base64," + base64.StdEncoding.EncodeToString(iconPNG)
