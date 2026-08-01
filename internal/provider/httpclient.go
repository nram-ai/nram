package provider

import (
	"net/http"
	"time"

	"github.com/nram-ai/nram/internal/netutil"
)

// newHTTPClient builds the HTTP client every provider adapter uses for egress.
// It applies the cloud-metadata egress guard (netutil.IsCloudMetadata): provider
// base URLs legitimately point at loopback and RFC 1918 hosts (self-hosted models
// such as Ollama on 127.0.0.1 or SGLang on a LAN address), so those ranges stay
// reachable, but a configured URL that resolves to a cloud instance-metadata
// endpoint is refused at dial time. Redirects are not followed. A timeout of 0
// leaves the client timeout unset for callers that bound each request by context.
func newHTTPClient(timeout time.Duration) *http.Client {
	return netutil.SafeHTTPClient(timeout, netutil.IsCloudMetadata)
}
