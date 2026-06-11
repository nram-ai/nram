package dreaming

import (
	"context"

	"github.com/nram-ai/nram/internal/service"
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
