package server

import (
	"net/http"
	"strings"
)

// defaultMaxBodyBytes bounds request bodies on the general API surface. It is
// generous for any normal JSON request (a single memory store is kilobytes)
// while capping the many handlers that decode r.Body without their own limit,
// closing the memory-amplification / DoS path where an authenticated caller
// streams an unbounded body.
const defaultMaxBodyBytes int64 = 8 << 20 // 8 MiB

// bulkMaxBodyBytes is the larger cap for the endpoints that legitimately ingest
// bulk data: memory import and the MCP transport. It matches the batch-store
// handler's own limit so the three bulk write paths agree.
const bulkMaxBodyBytes int64 = 64 << 20 // 64 MiB

// isBulkBodyPath reports whether a request path is one of the bulk-ingest
// endpoints that manage their own (larger) body cap and must be exempt from the
// default limiter. The memory batch/import routes carry a stable suffix under
// the {project_id} route; the MCP transport is mounted at /mcp and /mcp/*.
func isBulkBodyPath(path string) bool {
	if path == "/mcp" || strings.HasPrefix(path, "/mcp/") {
		return true
	}
	return strings.HasSuffix(path, "/memories/batch") ||
		strings.HasSuffix(path, "/memories/import")
}

// BodyLimitMiddleware wraps request bodies in http.MaxBytesReader with the
// default cap so no handler decodes an unbounded body. Bulk-ingest endpoints
// are exempt here and apply their own larger cap (see MaxBytesHandler and the
// batch/import handlers). A capped read past the limit surfaces as a
// *http.MaxBytesError, which the decoding handlers already turn into a 4xx.
func BodyLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && !isBulkBodyPath(r.URL.Path) {
			r.Body = http.MaxBytesReader(w, r.Body, defaultMaxBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// MaxBytesHandler wraps a handler so its request body is capped at n bytes. Used
// to give a bulk endpoint (the MCP transport) an explicit larger cap than the
// default limiter, which it is exempt from.
func MaxBytesHandler(h http.Handler, n int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, n)
		}
		h.ServeHTTP(w, r)
	})
}
