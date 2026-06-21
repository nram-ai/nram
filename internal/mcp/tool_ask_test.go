package mcp

import (
	"context"
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
