package server

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsBulkBodyPath(t *testing.T) {
	bulk := []string{
		"/mcp",
		"/mcp/",
		"/mcp/abc123",
		"/v1/projects/1111/memories/batch",
		"/v1/projects/2222/memories/import",
	}
	normal := []string{
		"/v1/projects/1111/memories",
		"/v1/auth/login",
		"/mcpfoo", // not the /mcp/ prefix, not exactly /mcp
		"/import", // suffix guard is /memories/import
		"/batch",  // suffix guard is /memories/batch
		"/v1/me/procedural/import",
	}
	for _, p := range bulk {
		if !isBulkBodyPath(p) {
			t.Errorf("isBulkBodyPath(%q) = false, want true", p)
		}
	}
	for _, p := range normal {
		if isBulkBodyPath(p) {
			t.Errorf("isBulkBodyPath(%q) = true, want false", p)
		}
	}
}

// readingHandler reads the whole body and reports 413 when the read hits the
// MaxBytesError that http.MaxBytesReader produces past its limit.
func readingHandler(w http.ResponseWriter, r *http.Request) {
	if _, err := io.ReadAll(r.Body); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func TestMaxBytesHandler_EnforcesCap(t *testing.T) {
	h := MaxBytesHandler(http.HandlerFunc(readingHandler), 16)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("small")))
	if rec.Code != http.StatusOK {
		t.Fatalf("under-cap body: got %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 64))))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over-cap body: got %d, want 413", rec.Code)
	}
}

func TestBodyLimitMiddleware_SmallBodiesPass(t *testing.T) {
	// Small bodies pass on both exempt (bulk) and non-exempt paths; the routing
	// of the exemption itself is covered by TestIsBulkBodyPath and the cap
	// mechanism by TestMaxBytesHandler_EnforcesCap.
	mw := BodyLimitMiddleware(http.HandlerFunc(readingHandler))
	for _, path := range []string{"/mcp", "/v1/projects/1/memories/batch", "/v1/auth/login"} {
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("hello")))
		if rec.Code != http.StatusOK {
			t.Errorf("path %q small body: got %d, want 200", path, rec.Code)
		}
	}
}
