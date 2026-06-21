package provider

// This file is the single source of truth for the set of provider slots. Every
// slot-set enumeration (registry build, admin status reporting, slot-update
// validation, the React provider page) derives from Slots below, so adding a
// slot is one SlotDef entry here plus wiring only where a feature consumes that
// provider. Do not re-list the slot set elsewhere.

// SlotKind distinguishes the embedding slot (a different provider type with
// dimension probing and a destructive model-switch cascade) from the uniform
// LLM slots.
type SlotKind int

const (
	KindEmbedding SlotKind = iota
	KindLLM
)

// Slot name constants. These are the wire/identity names used in the settings
// key ("provider.<name>"), the admin API, and the URL path.
const (
	SlotEmbedding         = "embedding"
	SlotFact              = "fact"
	SlotEntity            = "entity"
	SlotQueryAugment      = "query_augment"
	SlotIngestionDecision = "ingestion_decision"
	SlotAsk               = "ask"
)

// SlotDef is the canonical descriptor for a provider slot.
type SlotDef struct {
	Name        string   // identity, e.g. "query_augment"
	Label       string   // human label, served to the admin UI
	Description string   // help text, served to the admin UI
	Kind        SlotKind // embedding vs LLM
	Required    bool     // required for enrichment (embedding/fact/entity)
	FallbackTo  string   // "" or the slot name to fall back to when unconfigured
}

// SettingKey returns the settings-table key that stores this slot's config.
func (d SlotDef) SettingKey() string { return "provider." + d.Name }

// Slots is the canonical, ordered list of every provider slot. UI render order
// follows this slice.
var Slots = []SlotDef{
	{
		Name:        SlotEmbedding,
		Label:       "Embedding",
		Description: "Generates vector embeddings for semantic search",
		Kind:        KindEmbedding,
		Required:    true,
	},
	{
		Name:        SlotFact,
		Label:       "Fact Extraction",
		Description: "Extracts structured facts from stored memories",
		Kind:        KindLLM,
		Required:    true,
	},
	{
		Name:        SlotEntity,
		Label:       "Entity Extraction",
		Description: "Identifies entities and relationships in content",
		Kind:        KindLLM,
		Required:    true,
	},
	{
		Name:        SlotQueryAugment,
		Label:       "Query Augmentation",
		Description: "Generates paraphrased query forms per memory before embedding. Optional; falls back to the Fact Extraction provider when left unconfigured.",
		Kind:        KindLLM,
		FallbackTo:  SlotFact,
	},
	{
		Name:        SlotIngestionDecision,
		Label:       "Ingestion Decision",
		Description: "Judges ADD/UPDATE/DELETE/NONE on near-duplicate matches at write time. Optional; falls back to the Fact Extraction provider when left unconfigured.",
		Kind:        KindLLM,
		FallbackTo:  SlotFact,
	},
	{
		Name:        SlotAsk,
		Label:       "Ask Synthesis",
		Description: "Synthesizes answers over recalled memories for the ask tool. Required when the ask feature is enabled; no fallback, so ask traffic never lands on the enrichment providers.",
		Kind:        KindLLM,
		FallbackTo:  "",
	},
}

// SlotByName returns the SlotDef for name and whether it exists.
func SlotByName(name string) (SlotDef, bool) {
	for _, d := range Slots {
		if d.Name == name {
			return d, true
		}
	}
	return SlotDef{}, false
}

// IsValidSlot reports whether name identifies a known provider slot.
func IsValidSlot(name string) bool {
	_, ok := SlotByName(name)
	return ok
}
