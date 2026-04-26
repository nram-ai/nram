package api

import "net/http"

// includeSupersededParam is the query-string key that opts into surfacing
// rows superseded by paraphrase or contradiction dedup.
const includeSupersededParam = "include_superseded"

// queryParamBool reports whether the named query param equals the literal
// string "true". Anything else (missing, empty, "false", or any other
// value) is treated as false.
func queryParamBool(r *http.Request, key string) bool {
	return r.URL.Query().Get(key) == "true"
}
