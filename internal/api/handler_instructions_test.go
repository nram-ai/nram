package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/instructions"
)

func serveInstructions(t *testing.T, query string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	NewInstructionsHandler()(rec, httptest.NewRequest(http.MethodGet, "/instructions"+query, nil))
	return rec
}

func TestInstructionsHandler_DefaultIsFullBody(t *testing.T) {
	full, ok := instructions.Lookup("claude")
	if !ok {
		t.Fatal("claude format must resolve")
	}
	rec := serveInstructions(t, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
	if rec.Body.String() != full {
		t.Fatal("default body does not match the full (claude) body")
	}
	if !strings.Contains(rec.Body.String(), "you may not reason or justify") {
		t.Fatal("full body is missing the anti-rationalization session-start clause")
	}
}

func TestInstructionsHandler_ClaudeAndAgentsAreIdentical(t *testing.T) {
	claude := serveInstructions(t, "?format=claude")
	agents := serveInstructions(t, "?format=agents")
	if claude.Code != http.StatusOK || agents.Code != http.StatusOK {
		t.Fatalf("claude=%d agents=%d, want 200 each", claude.Code, agents.Code)
	}
	if claude.Body.String() != agents.Body.String() {
		t.Fatal("claude and agents bodies differ; they must be identical")
	}
}

func TestInstructionsHandler_Cursor(t *testing.T) {
	cursor, ok := instructions.Lookup("cursor")
	if !ok {
		t.Fatal("cursor format must resolve")
	}
	full, _ := instructions.Lookup("claude")
	rec := serveInstructions(t, "?format=cursor")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != cursor {
		t.Fatal("cursor body does not match the condensed body")
	}
	if rec.Body.String() == full {
		t.Fatal("cursor body should differ from the full body")
	}
	if !strings.Contains(rec.Body.String(), "reasoning or justifying a skip is itself a violation") {
		t.Fatal("cursor body is missing the anti-rationalization session-start clause")
	}
}

func TestInstructionsHandler_UnknownFormat(t *testing.T) {
	rec := serveInstructions(t, "?format=bogus")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
}
