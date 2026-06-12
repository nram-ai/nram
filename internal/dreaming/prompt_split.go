package dreaming

import (
	"context"

	"github.com/nram-ai/nram/internal/service"
)

// The dreaming LLM phases each split their prompt into a tunable system message
// (role, rules, output schema) resolved from a *_system_prompt setting, and a
// hardcoded dynamic user message built from these wrappers (via fmt.Sprintf at
// the phase's data-join site). The data wrappers are code, not settings; only
// the system prompt is operator-tunable. The contradiction wrapper is shared
// with the enrichment conflict detector and lives in service.RenderContradictionUser.
const (
	synthesisUserWrapper = "<information>\n%s\n</information>"
	alignmentUserWrapper = "<synthesis>\n%s\n</synthesis>\n\n<evidence>\n%s\n</evidence>"
	noveltyUserWrapper   = "<synthesis>\n%s\n</synthesis>\n\n<sources>\n%s\n</sources>"
)

// resolvePromptOrDefault resolves a prompt-shaped setting through the dreaming
// SettingsResolver interface, treating an empty stored value as "use the
// registered default". Mirrors service.ResolveOrDefault, which is unavailable
// here because the dreaming phases hold the SettingsResolver interface rather
// than the concrete *service.SettingsService.
func resolvePromptOrDefault(ctx context.Context, s SettingsResolver, key string) string {
	if s != nil {
		if v, _ := s.Resolve(ctx, key, "global"); v != "" {
			return v
		}
	}
	def, _ := service.GetDefault(key)
	return def
}
