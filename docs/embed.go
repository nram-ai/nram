// Package docs embeds the hand-maintained OpenAPI specification so the
// server can serve it from the binary at GET /openapi.yaml. The canonical
// source remains docs/openapi.yaml; a conformance test
// (internal/server/openapi_conformance_test.go) keeps it in sync with the
// router.
package docs

import _ "embed"

// OpenAPISpec is the raw bytes of the OpenAPI 3.1 specification.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
