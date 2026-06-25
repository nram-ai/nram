package main

import (
	"context"
	"log/slog"
	"strings"

	"github.com/nram-ai/nram/internal/service"
)

// oldCombinedEntitySystemPrompt is a byte snapshot of the pre-split
// enrichment.entity_system_prompt default (the version that asked for entities
// AND relationships in one response). It is frozen here so the boot migration
// can recognize a stored value that is still the old default and reset it to the
// new entity-only default, without clobbering an operator-customized prompt.
// Keep this exactly equal to the historical default; never "update" it.
const oldCombinedEntitySystemPrompt = `You are an entity and relationship extraction engine. Given a text, extract the named entities and the relationships between them as JSON.

Return a JSON object with two fields:
- "entities": array of objects with fields:
  - "name": the entity's proper name, as short as possible (string)
  - "type": one of EXACTLY these types (string): person, organization, location, product, event, role, date, concept, technology, software, code_symbol, file, data_store, system, configuration, command, vcs_ref, credential, identifier, metric, document, research_artifact, medication, medical_condition, biomarker. If none fit, use "other". Never invent a type outside this list.
  - "properties": optional key-value pairs (object)
- "relationships": array of objects with fields:
  - "source": source entity name (string)
  - "target": target entity name (string)
  - "relation": one of EXACTLY these relations (string). Map your verb to the closest one; do NOT invent verbs. Guide:
    - member of: employment/study/affiliation (worked at, studied at, joined, member of)
    - produces: creation/authorship (authored, founded, built, developed, created, wrote)
    - uses: consumes/operates/calls (uses, written in, calls, deployed, adopted)
    - depends on: needs/hosted by (requires, served by, runs on, relies on)
    - affects: manages/leads/changes (managed, led, oversaw, modifies, influences)
    - family of: kinship (married to, mother of, brother of, child of)
    - has property: traits/titles/credentials (has, held title, earned, characterized by)
    - located in: place (lives in, based in, near)
    - part of / has part / is a / references / implements / supports / compares to / interacts with: structural/semantic links
    If truly none fit, use "related to". Never output a relation outside this list.
  - "weight": confidence/strength 0.0 to 1.0 (number)
  - "temporal": "current", "as of <date>", "previously", or "no longer" (string, default "current")

Hard rules:
- An entity is a NAMED thing (a person, place, system, file, drug, etc.), not a statement. Do NOT extract whole sentences, claims, opinions, questions, or whole code/SQL/shell snippets or statements as entities; a single named code symbol (e.g. a function or type name, type code_symbol) or a file name/path (type file) is allowed. A name longer than a short phrase is almost always wrong.
- Do NOT repeat an entity or relationship you have already emitted, and do NOT loop. Each entity and relationship must be distinct.

Return ONLY valid JSON. Do not include markdown fences or explanation.

Return the JSON minified onto a single line: no spaces, newlines, or indentation between JSON tokens. Do not change whitespace inside string values.`

// migrateEntityPromptSplit resets a stored enrichment.entity_system_prompt that
// is still the old combined (entities+relationships) default to the new
// entity-only default, so deployments that seeded the old prompt pick up the
// split. Guarded by a marker so the check runs at most once. An
// operator-customized prompt is left untouched with a one-time warning to split
// it manually; the new relationship prompt seeds cleanly on its own. Idempotent
// and non-fatal: a failure logs and retries next boot.
func migrateEntityPromptSplit(ctx context.Context, settingsSvc *service.SettingsService) {
	const marker = "enrichment.entity_prompt_split_migrated"
	if settingsSvc.ResolveBool(ctx, marker, "global") {
		return
	}

	stored := service.ResolveOrDefault(ctx, settingsSvc, service.SettingEntitySystemPrompt, "global")
	newDefault := service.GetDefaultString(service.SettingEntitySystemPrompt)

	switch strings.TrimSpace(stored) {
	case strings.TrimSpace(oldCombinedEntitySystemPrompt):
		if err := settingsSvc.Set(ctx, service.SettingEntitySystemPrompt, newDefault, "global", nil); err != nil {
			slog.Warn("boot: failed to split stored entity_system_prompt to entity-only (will retry next boot)", "err", err)
			return
		}
		slog.Info("boot: migrated stored entity_system_prompt to the entity-only split default; relationships now extract in a separate pass")
	case strings.TrimSpace(newDefault):
		// Already the entity-only default (fresh seed or prior migration); nothing to do.
	default:
		slog.Warn("boot: stored entity_system_prompt is operator-customized and predates the entity/relationship split; relationships are now extracted by a separate pass. Split it manually in the Prompt Templates UI (Entity Extraction keeps the entity rules; move the relationship schema to the new Relationship Extraction prompt).")
	}

	if err := settingsSvc.Set(ctx, marker, "true", "global", nil); err != nil {
		slog.Warn("boot: failed to record entity-prompt split migration marker (will retry next boot)", "err", err)
	}
}
