package api

import (
	"net/http"
	"strings"

	"github.com/nram-ai/nram/internal/auth"
)

// RejectShareTokenMiddleware blocks any request that authenticated via a
// share token (ShareTokenID != nil on AuthContext) from reaching REST or
// userinfo endpoints. Share-token credentials are scoped strictly to the
// MCP tool surface — admin/owner REST endpoints expose tenant management,
// audit views, and other operations the recipient must never reach.
//
// The middleware checks the path so /mcp and /mcp/* pass through, and so
// does the OAuth `/userinfo` endpoint when it appears under the same auth
// group (callers identifying themselves via share creds still need
// /userinfo to bootstrap the MCP client's profile claims). All other
// authenticated routes return 403 for share-bearer requests.
//
// IMPORTANT: this middleware assumes AuthMiddleware has already run and
// populated the auth context. Mount it AFTER AuthMiddleware on any route
// group that should reject share-bearer access.
func RejectShareTokenMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil || ac.ShareTokenID == nil {
			next.ServeHTTP(w, r)
			return
		}
		path := r.URL.Path
		if path == "/mcp" || strings.HasPrefix(path, "/mcp/") || path == "/userinfo" {
			next.ServeHTTP(w, r)
			return
		}
		WriteError(w, ErrForbidden("share-token credentials are restricted to the MCP tool surface"))
	})
}
