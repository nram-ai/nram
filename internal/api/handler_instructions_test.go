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
	// This body is what users paste into CLAUDE.md, so it is the copy most
	// likely to be reflowed by a later edit. Its ask guidance must keep saying
	// that a confidence score is grounding strength and not correctness.
	for _, want := range []string{"grounding", "correctness"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("full body's ask guidance must distinguish grounding from correctness; missing %q", want)
		}
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

func TestInstructionsHandler_Condensed(t *testing.T) {
	condensed, ok := instructions.Lookup("condensed")
	if !ok {
		t.Fatal("condensed format must resolve")
	}
	full, _ := instructions.Lookup("claude")
	rec := serveInstructions(t, "?format=condensed")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != condensed {
		t.Fatal("condensed body does not match the condensed copy")
	}
	if rec.Body.String() == full {
		t.Fatal("condensed body should differ from the full body")
	}
	if !strings.Contains(rec.Body.String(), "reasoning or justifying a skip is itself a violation") {
		t.Fatal("condensed body is missing the anti-rationalization session-start clause")
	}
	// The condensed body is reused for ChatGPT's Custom instructions field, which
	// hard-caps at 1500 characters and rejects input past the cap. Keep it under
	// that limit so it stays pasteable there.
	if len(condensed) > 1500 {
		t.Fatalf("condensed body is %d chars, want <= 1500 to fit ChatGPT Custom instructions", len(condensed))
	}
}

// TestInstructionsHandler_CursorAlias pins the deprecated "cursor" format as a
// backward-compatible alias of "condensed": it must return the identical body
// so callers that have not migrated see no change in response.
func TestInstructionsHandler_CursorAlias(t *testing.T) {
	condensed := serveInstructions(t, "?format=condensed")
	cursor := serveInstructions(t, "?format=cursor")
	if cursor.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", cursor.Code)
	}
	if cursor.Body.String() != condensed.Body.String() {
		t.Fatal("cursor alias body differs from condensed; the alias must be identical")
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
