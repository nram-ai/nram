package server

import (
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/nram-ai/nram/docs"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"gopkg.in/yaml.v3"
)

// This test keeps docs/openapi.yaml in sync with the router. It runs in two
// directions:
//
//   - Reverse (spec → router): every documented (method, path) must resolve to
//     a registered chi route. This is the strong direction and catches removed
//     or renamed endpoints. It also covers the sub-paths dispatched inside
//     wildcard HandleFunc handlers, because the live router matches them.
//
//   - Forward (router → spec): every concrete (single-method) chi route must be
//     documented; every all-method HandleFunc route (including the "/*"
//     wildcard handlers) is verified only at the prefix level, because chi
//     cannot see the concrete methods and sub-paths those handlers dispatch
//     internally — that surface is covered by the reverse check plus review.
//
// Non-JSON transports are intentionally undocumented and excluded here: the
// MCP endpoint, the SSE stream at /v1/events, Prometheus /metrics, and the two
// HTML consent/landing shells (GET /authorize, GET /share/accept).

// concreteExclusions are single-method router routes deliberately not in the
// OpenAPI spec: the SSE stream and the two HTML consent/landing shells.
var concreteExclusions = map[string]bool{
	"GET /v1/events":    true, // SSE stream
	"GET /authorize":    true, // OAuth consent HTML shell (POST /authorize is documented)
	"GET /share/accept": true, // share-accept HTML shell (GET /v1/share/accept is documented)
}

// prefixExclusions are all-method (HandleFunc/Handle) transport prefixes
// deliberately not in the OpenAPI spec: MCP JSON-RPC and Prometheus.
var prefixExclusions = []string{"/mcp", "/metrics"}

var httpMethods = map[string]bool{
	"get": true, "post": true, "put": true, "patch": true,
	"delete": true, "head": true, "options": true,
}

// routeEntry is one router endpoint discovered by traversing the chi tree.
type routeEntry struct {
	pattern   string
	methods   map[string]bool // upper-case HTTP methods registered
	allMethod bool            // registered for every method (HandleFunc/Handle)
}

// collectRoutes walks the chi route tree faithfully, mirroring chi.Walk's path
// concatenation but preserving the "*" catch-all so we can tell an all-method
// HandleFunc route from a single-method one.
func collectRoutes(r chi.Routes, parent string, out *[]routeEntry) {
	for _, rt := range r.Routes() {
		if rt.SubRoutes != nil {
			collectRoutes(rt.SubRoutes, parent+rt.Pattern, out)
			continue
		}
		full := strings.ReplaceAll(parent+rt.Pattern, "/*/", "/")
		e := routeEntry{pattern: full, methods: map[string]bool{}}
		for method := range rt.Handlers {
			if method == "*" {
				e.allMethod = true
				continue
			}
			e.methods[strings.ToUpper(method)] = true
		}
		*out = append(*out, e)
	}
}

func normalizePath(p string) string {
	if p != "/" {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}

// specPaths parses docs/openapi.yaml into a path -> set(METHOD) map.
func specPaths(t *testing.T) map[string]map[string]bool {
	t.Helper()
	var doc struct {
		Paths map[string]map[string]yaml.Node `yaml:"paths"`
	}
	if err := yaml.Unmarshal(docs.OpenAPISpec, &doc); err != nil {
		t.Fatalf("parse openapi.yaml: %v", err)
	}
	out := map[string]map[string]bool{}
	for p, ops := range doc.Paths {
		methods := map[string]bool{}
		for k := range ops {
			if httpMethods[strings.ToLower(k)] {
				methods[strings.ToUpper(k)] = true
			}
		}
		out[normalizePath(p)] = methods
	}
	return out
}

// buildConformanceRouter constructs the production router. It registers the
// optional non-JSON transports (MCP, Prometheus) so their routes are present
// and the transport exclusions above are exercised rather than dead; this also
// closes the gap where a documented route gated behind one of those config
// fields would otherwise be invisible to the forward check. Other handlers are
// left nil (the router substitutes a 501 stub) — only the registered route set
// matters here.
func buildConformanceRouter() *chi.Mux {
	noop := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return NewRouter(
		RouterConfig{Metrics: metrics.New()},
		Handlers{MCP: noop},
	)
}

// concretePath fills each chi/OpenAPI {param} with a value chi will match.
func concretePath(p string) string {
	var b strings.Builder
	for {
		i := strings.IndexByte(p, '{')
		if i < 0 {
			b.WriteString(p)
			break
		}
		j := strings.IndexByte(p[i:], '}')
		if j < 0 {
			b.WriteString(p)
			break
		}
		b.WriteString(p[:i])
		b.WriteString("00000000-0000-0000-0000-000000000000")
		p = p[i+j+1:]
	}
	return b.String()
}

// TestOpenAPIReverse_SpecPathsResolveInRouter asserts every documented endpoint
// is reachable in the router.
func TestOpenAPIReverse_SpecPathsResolveInRouter(t *testing.T) {
	router := buildConformanceRouter()
	spec := specPaths(t)

	var failures []string
	for path, methods := range spec {
		concrete := concretePath(path)
		for method := range methods {
			rctx := chi.NewRouteContext()
			if !router.Match(rctx, method, concrete) {
				failures = append(failures, method+" "+path+" (documented but not routed)")
			}
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf("openapi.yaml documents endpoints the router does not serve:\n  %s",
			strings.Join(failures, "\n  "))
	}
}

// TestOpenAPIForward_RouterRoutesAreDocumented asserts every concrete router
// route is documented, and every all-method route has prefix coverage.
func TestOpenAPIForward_RouterRoutesAreDocumented(t *testing.T) {
	router := buildConformanceRouter()
	spec := specPaths(t)

	var routes []routeEntry
	collectRoutes(router, "", &routes)

	// Sorted spec paths for prefix coverage checks.
	specPathList := make([]string, 0, len(spec))
	for p := range spec {
		specPathList = append(specPathList, p)
	}

	prefixCovered := func(prefix string) bool {
		prefix = normalizePath(prefix)
		for _, sp := range specPathList {
			if sp == prefix || strings.HasPrefix(sp, prefix+"/") {
				return true
			}
		}
		return false
	}
	excludedPrefix := func(prefix string) bool {
		for _, ex := range prefixExclusions {
			if prefix == ex || strings.HasPrefix(prefix, ex+"/") {
				return true
			}
		}
		return false
	}

	var failures []string
	prefixVerified := 0
	for _, e := range routes {
		if e.allMethod {
			prefix := normalizePath(strings.TrimSuffix(e.pattern, "/*"))
			if excludedPrefix(prefix) {
				continue
			}
			if !prefixCovered(prefix) {
				failures = append(failures, "ALL "+e.pattern+" (no documented path under prefix "+prefix+")")
				continue
			}
			prefixVerified++
			continue
		}

		path := normalizePath(e.pattern)
		documented := spec[path]
		for method := range e.methods {
			if concreteExclusions[method+" "+path] {
				continue
			}
			if !documented[method] {
				failures = append(failures, method+" "+path+" (router route not documented)")
			}
		}
	}

	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf("router serves endpoints openapi.yaml does not document:\n  %s",
			strings.Join(failures, "\n  "))
	}

	// Surface the limitation explicitly (no silent caps): these prefixes are
	// dispatch handlers whose concrete sub-paths/methods are verified by the
	// reverse check, not enumerated here.
	t.Logf("prefix-verified dispatch handlers (sub-paths checked via reverse test only): %d", prefixVerified)
}
