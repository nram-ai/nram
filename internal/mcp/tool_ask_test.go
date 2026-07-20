package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/service"
)

// askEnabledSettings returns a settings service whose ask.enabled resolves to v.
func askEnabledSettings(v string) *service.SettingsService {
	return service.NewSettingsService(&mapSettingsRepo{values: map[string]string{
		service.SettingAskEnabled: v,
	}})
}

func TestAskVisible(t *testing.T) {
	ask := &service.AskService{}
	cases := []struct {
		name string
		deps Dependencies
		want bool
	}{
		{"no ask service", Dependencies{Settings: askEnabledSettings("true")}, false},
		{"flag off", Dependencies{Ask: ask, Settings: askEnabledSettings("false")}, false},
		{"flag unset (default off)", Dependencies{Ask: ask, Settings: askEnabledSettings("")}, false},
		{"flag on", Dependencies{Ask: ask, Settings: askEnabledSettings("true")}, true},
		{"nil settings", Dependencies{Ask: ask}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := askVisible(context.Background(), tc.deps); got != tc.want {
				t.Errorf("askVisible = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAskToolDescriptionCarriesGroundingCaveat pins the sentence in tools/list
// telling an agent that a high confidence is not a claim the answer is correct.
//
// Asserts against the registered tool, so it also catches RegisterAskTool being
// rewired to a different description. Asserts on the two load-bearing words
// rather than a phrasing, matching the sibling pins in
// TestAskSchemaDescribesConfidence and
// TestBuildInstructions_AskConfidenceIsGrounding: a meaning-preserving reword
// should not have to be chased through three tests.
func TestAskToolDescriptionCarriesGroundingCaveat(t *testing.T) {
	srv := newTestServer(Dependencies{Ask: &service.AskService{}})
	RegisterAskTool(srv)

	st, ok := srv.MCPServer().ListTools()["ask"]
	if !ok {
		t.Fatal("ask tool is not registered; cannot check its description")
	}

	for _, want := range []string{"grounding", "correctness"} {
		if !strings.Contains(st.Tool.Description, want) {
			t.Errorf("ask tool description must distinguish grounding from correctness; missing %q\ngot: %s", want, st.Tool.Description)
		}
	}
}

func TestFilterOutTool(t *testing.T) {
	tools := []mcp.Tool{{Name: "recall"}, {Name: "ask"}, {Name: "store"}}
	out := filterOutTool(tools, "ask")
	for _, tl := range out {
		if tl.Name == "ask" {
			t.Fatalf("ask was not filtered out: %+v", out)
		}
	}
	if len(out) != 2 {
		t.Errorf("expected 2 tools after filtering, got %d", len(out))
	}
	// Filtering a name not present is a no-op.
	if len(filterOutTool(tools, "nope")) != 3 {
		t.Error("filtering an absent name should not drop anything")
	}
}
