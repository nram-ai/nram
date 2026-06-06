package model

// Reserved per-user project slugs. A reserved project is auto-provisioned for
// every user, carries canonical Name/Description that nram manages on the
// user's behalf (its identity fields are locked against edits), and cannot be
// deleted. The two tiers differ only by subject:
//
//   - global:   world-knowledge that spans projects (the cross-cutting tier).
//   - about_me: self-knowledge about the user (the persona tier).
//
// This file is the single source of truth for which slugs are reserved and the
// canonical copy each one carries. It lives in the bottom (model) layer so the
// storage, service, api, and mcp layers can all consult it without import
// cycles. Every reserved-slug decision elsewhere routes through
// IsReservedProjectSlug rather than comparing against a bare string literal.
const (
	ReservedProjectSlugGlobal  = "global"
	ReservedProjectSlugAboutMe = "about_me"
)

// ReservedProject describes a per-user reserved project: its slug and the
// canonical display name and description nram manages for it.
type ReservedProject struct {
	Slug        string
	Name        string
	Description string
}

// ReservedProjects is the ordered registry of per-user reserved projects. Order
// is the provisioning order (global first, so it exists as the deletion
// token-usage reassignment target before anything else is created).
var ReservedProjects = []ReservedProject{
	{
		Slug: ReservedProjectSlugGlobal,
		Name: ReservedProjectSlugGlobal,
		Description: "Reserved cross-cutting tier: world-knowledge that spans projects: " +
			"facts, tools, references, and context not specific to any single project. " +
			"Auto-created for every user, searched on every recall alongside the active " +
			"project, and cannot be deleted. World-knowledge, not self-knowledge; " +
			"biography and identity belong in about_me.",
	},
	{
		Slug: ReservedProjectSlugAboutMe,
		Name: ReservedProjectSlugAboutMe,
		Description: "Reserved persona tier: self-knowledge about the user you're assisting: " +
			"identity, background, preferences, relationships, and ongoing personal context. " +
			"Fully indexed (embedding, enrichment, dream synthesis) and surfaced on every " +
			"recall by association; load it directly with the about_me tool for a session-start " +
			"sense of who this person is. Auto-created for every user and cannot be deleted. " +
			"Self-knowledge, not world-knowledge; cross-cutting facts belong in global.",
	},
}

// IsReservedProjectSlug reports whether slug names a reserved per-user project.
func IsReservedProjectSlug(slug string) bool {
	return ReservedProjectBySlug(slug) != nil
}

// ReservedProjectBySlug returns the canonical reserved-project definition for
// slug, or nil if slug is not reserved.
func ReservedProjectBySlug(slug string) *ReservedProject {
	for i := range ReservedProjects {
		if ReservedProjects[i].Slug == slug {
			return &ReservedProjects[i]
		}
	}
	return nil
}
