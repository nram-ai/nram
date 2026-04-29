// Pure helpers for sparse settings payloads. Undefined fields are omitted
// so the cascade resolver picks them up from the system layer at runtime.
import type {
  ProjectRankingWeights,
  ProjectSettings,
} from "../api/client";

// TriState models a three-way boolean override: "inherit" delegates to the
// system layer, "on"/"off" set the field explicitly. Used by the
// enrichment_enabled <select> in both project and user editors.
export type TriState = "inherit" | "on" | "off";

export function triStateValue(b: boolean | undefined): TriState {
  if (b === undefined) return "inherit";
  return b ? "on" : "off";
}

export function fromTriState(v: string): boolean | undefined {
  if (v === "on") return true;
  if (v === "off") return false;
  return undefined;
}

export interface ProjectFormState {
  similarity: number | undefined;
  recency: number | undefined;
  importance: number | undefined;
  frequency: number | undefined;
  graph_relevance: number | undefined;
  confidence: number | undefined;
  dedup_threshold: number | undefined;
  enrichment_enabled: boolean | undefined;
}

export interface UserFormState {
  dedup_threshold: number | undefined;
  enrichment_enabled: boolean | undefined;
}

// Build the project.settings JSON the API expects. An entirely-unset
// ranking_weights object is dropped rather than sent as `{}`.
export function buildProjectSettingsPayload(state: ProjectFormState): ProjectSettings {
  const rankingOverride: ProjectRankingWeights = {};
  if (state.similarity !== undefined) rankingOverride.similarity = state.similarity;
  if (state.recency !== undefined) rankingOverride.recency = state.recency;
  if (state.importance !== undefined) rankingOverride.importance = state.importance;
  if (state.frequency !== undefined) rankingOverride.frequency = state.frequency;
  if (state.graph_relevance !== undefined) rankingOverride.graph_relevance = state.graph_relevance;
  if (state.confidence !== undefined) rankingOverride.confidence = state.confidence;

  const settings: ProjectSettings = {};
  if (state.dedup_threshold !== undefined) settings.dedup_threshold = state.dedup_threshold;
  if (state.enrichment_enabled !== undefined) settings.enrichment_enabled = state.enrichment_enabled;
  if (Object.keys(rankingOverride).length > 0) settings.ranking_weights = rankingOverride;
  return settings;
}

// Build the user.settings JSON the API expects. ranking_weights is never
// emitted at user scope — the server rejects it with a 400 because the
// cascade lands at project, not user.
export function buildUserSettingsPayload(state: UserFormState): Record<string, unknown> {
  const settings: Record<string, unknown> = {};
  if (state.dedup_threshold !== undefined) settings.dedup_threshold = state.dedup_threshold;
  if (state.enrichment_enabled !== undefined) settings.enrichment_enabled = state.enrichment_enabled;
  return settings;
}
