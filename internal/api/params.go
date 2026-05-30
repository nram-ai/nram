package api

import "net/http"

// includeSupersededParam is the query-string key that opts into surfacing
// rows superseded by paraphrase or contradiction dedup.
const includeSupersededParam = "include_superseded"

// groupByParentParam is the query-string key that switches the memory list
// endpoint into parent-anchored mode: pagination is over non-enrichment
// parents, and each row carries its enrichment-derived children inline so
// a family is never split across pages.
const groupByParentParam = "group_by_parent"

// queryParamBool reports whether the named query param equals the literal
// string "true". Anything else (missing, empty, "false", or any other
// value) is treated as false.
func queryParamBool(r *http.Request, key string) bool {
	return r.URL.Query().Get(key) == "true"
}

// parseTriState maps a query-param value to a tri-state boolean filter:
// "true" → *true, "false" → *false, anything else (missing/empty/other) →
// nil meaning "no filter". Shared by the enriched and augmented filters.
func parseTriState(v string) *bool {
	switch v {
	case "true":
		t := true
		return &t
	case "false":
		f := false
		return &f
	}
	return nil
}
