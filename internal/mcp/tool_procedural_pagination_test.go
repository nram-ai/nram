package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestProceduralFetch_Pagination verifies limit/offset paging returns
// priority-ordered pages, a correct total, and that paging through covers the
// full set exactly once.
func TestProceduralFetch_Pagination(t *testing.T) {
	userID, nsID := uuid.New(), uuid.New()
	srv, _ := newProceduralTestServer(userID, nsID)
	ctx := buildAuthCtx(userID)

	// Five enabled entries, descending priority so order is deterministic.
	for _, p := range []int{5, 4, 3, 2, 1} {
		res := callProcedural(t, handleProceduralStore, ctx, srv, "procedural_store", map[string]any{
			"content":  "rule-p" + string(rune('0'+p)),
			"priority": float64(p),
		})
		if res.IsError {
			t.Fatalf("store p%d: %s", p, extractText(res))
		}
	}

	fetchPage := func(limit, offset int) mcpProceduralFetchResponse {
		res := callProcedural(t, handleProceduralFetch, ctx, srv, "procedural_fetch", map[string]any{
			"limit":  float64(limit),
			"offset": float64(offset),
		})
		if res.IsError {
			t.Fatalf("fetch(limit=%d,offset=%d): %s", limit, offset, extractText(res))
		}
		var out mcpProceduralFetchResponse
		if err := json.Unmarshal([]byte(extractText(res)), &out); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return out
	}

	page1 := fetchPage(2, 0)
	if page1.Pagination.Total != 5 {
		t.Errorf("page1 total: want 5, got %d", page1.Pagination.Total)
	}
	if page1.Count != 2 || len(page1.Entries) != 2 {
		t.Fatalf("page1: want 2 entries, got %d", page1.Count)
	}
	if page1.Entries[0].Priority != 5 || page1.Entries[1].Priority != 4 {
		t.Errorf("page1 not priority-ordered: %d then %d", page1.Entries[0].Priority, page1.Entries[1].Priority)
	}

	// Page through the rest; assert each priority appears exactly once, descending.
	seen := []int{page1.Entries[0].Priority, page1.Entries[1].Priority}
	for offset := 2; offset < 5; offset += 2 {
		pg := fetchPage(2, offset)
		for _, e := range pg.Entries {
			seen = append(seen, e.Priority)
		}
	}
	if len(seen) != 5 {
		t.Fatalf("paging covered %d entries, want 5", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] >= seen[i-1] {
			t.Errorf("priorities not strictly descending across pages: %v", seen)
			break
		}
	}
}

// TestNewProceduralReducer halves whole entries and emits a _truncated marker
// with a next-offset hint — never a mid-content byte cut.
func TestNewProceduralReducer(t *testing.T) {
	orig := &mcpProceduralFetchResponse{
		Entries: []mcpProceduralEntry{
			{Content: "a"}, {Content: "b"}, {Content: "c"}, {Content: "d"},
		},
		Count: 4,
	}
	orig.Pagination.Total = 10
	orig.Pagination.Limit = 4
	orig.Pagination.Offset = 0

	reduce := newProceduralReducer(orig)

	out, more := reduce()
	r1, ok := out.(*mcpProceduralFetchResponse)
	if !ok {
		t.Fatalf("reducer returned %T, want *mcpProceduralFetchResponse", out)
	}
	if r1.Count != 2 || len(r1.Entries) != 2 {
		t.Errorf("first reduction: want 2 entries, got %d", r1.Count)
	}
	if r1.Truncated == nil {
		t.Fatal("first reduction must carry a _truncated marker")
	}
	if r1.Truncated.ReturnedCount != 2 || r1.Truncated.OriginalCount != 4 {
		t.Errorf("truncation counts: got returned=%d original=%d", r1.Truncated.ReturnedCount, r1.Truncated.OriginalCount)
	}
	if !strings.Contains(r1.Truncated.Hint, "offset=2") {
		t.Errorf("hint should point to the next offset, got %q", r1.Truncated.Hint)
	}
	if !more {
		t.Error("reducer should report more=true while it can still shrink")
	}

	out2, more2 := reduce()
	r2 := out2.(*mcpProceduralFetchResponse)
	if r2.Count != 1 {
		t.Errorf("second reduction: want 1 entry, got %d", r2.Count)
	}
	if more2 {
		t.Error("reducer at floor (1 entry) should report more=false")
	}
}
