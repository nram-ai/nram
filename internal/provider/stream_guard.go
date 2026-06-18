package provider

import (
	"bytes"
	"fmt"
	"strings"
)

// errIfStreamedResponse reports an actionable error when an upstream returns a
// server-sent event (SSE) stream for a request that asked for a single JSON
// body. Every inference call in this package is single-shot and sends
// stream=false (or, for Gemini, targets the non-streaming generateContent
// endpoint), so a streamed body is always a misconfiguration: typically an
// Anthropic/OpenAI-compatible relay, gateway, or proxy that forces SSE and
// ignores the flag. Reading the whole body and handing it to json.Unmarshal
// would otherwise fail with a generic "failed to unmarshal response", which
// hides the real cause. Detecting it here turns that into a diagnosis.
//
// Detection is twofold because relays are inconsistent: the Content-Type may be
// text/event-stream, or it may be mislabeled while the body still carries the
// SSE "event:"/"data:" framing. Either signal is conclusive. This is detection
// only; the SSE body is intentionally not parsed.
func errIfStreamedResponse(providerName, contentType string, body []byte) error {
	streamed := strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/event-stream")
	if !streamed {
		trimmed := bytes.TrimLeft(body, " \t\r\n")
		if bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("data:")) {
			streamed = true
		}
	}
	if !streamed {
		return nil
	}
	return fmt.Errorf("%s: upstream returned a streaming (SSE) response despite stream=false; "+
		"point the provider base_url at an endpoint or relay that honors non-streaming requests", providerName)
}
