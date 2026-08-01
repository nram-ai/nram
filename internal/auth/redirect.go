package auth

import (
	"net/url"
	"strings"
)

// ctrlStripper mirrors the WHATWG URL parser, which removes ASCII tab, LF, and
// CR from a URL before resolving it. A browser handed "Location: /\t/evil"
// navigates to "//evil" (protocol-relative, off-origin), so the guard must
// strip these before its prefix checks or the stripped bytes smuggle a value
// past them.
var ctrlStripper = strings.NewReplacer("\t", "", "\n", "", "\r", "")

// safeInternalRedirect returns a same-origin root-relative path safe to use as
// an HTTP redirect Location, or "/" otherwise. It is the server-side
// counterpart to the SPA guard in ui/src/lib/safeRedirect.ts (sameOriginPath):
// after stripping tab/newline/CR (as the browser will), the value must begin
// with a single "/", must not be protocol-relative ("//host") or a backslash
// variant ("/\\host") that browsers normalize to "//", and must not carry a
// scheme or host once parsed. This blocks the open-redirect vector where a
// crafted ?redirect= sends an authenticated user off-origin. It returns the
// cleaned value so the emitted header carries no control bytes, and is
// idempotent, so it is safe to apply at both capture and use.
func safeInternalRedirect(raw string) string {
	cleaned := ctrlStripper.Replace(raw)
	if cleaned == "" || cleaned[0] != '/' {
		return "/"
	}
	if strings.HasPrefix(cleaned, "//") || strings.HasPrefix(cleaned, "/\\") {
		return "/"
	}
	if u, err := url.Parse(cleaned); err != nil || u.Scheme != "" || u.Host != "" {
		return "/"
	}
	return cleaned
}
