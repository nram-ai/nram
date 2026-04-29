/**
 * Pure helpers for building sparse settings payloads from form state.
 *
 * Extracted from ProjectManagement.tsx and UserManagement.tsx so the
 * sparse-omission logic can be unit-tested independently of the JSX.
 * Callers — both the page components and the tests — construct form state
 * with `number | undefined` (or `boolean | undefined` for tri-state booleans)
 * where `undefined` means "inherit system default." These helpers turn that
 * state into the JSON the server sees, omitting any field the operator did
 * not set. Empty objects are also omitted so the server-side parsers do not
 * have to special-case them.
 */
import type {
  ProjectRankingWeights,
  ProjectSettings,
} from "../api/client";

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

/**
 * Build the project.settings JSON the API expects, omitting any field the
 * operator did not set so the cascade resolver picks the inherited value
 * up at runtime. An entirely-unset ranking_weights object is dropped, not
 * sent as `{}`.
 */
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

/**
 * Build the user.settings JSON the API expects. ranking_weights is never
 * emitted at user scope — the server rejects it with a 400 because the
 * cascade lands at project, not user. Future refactors that add a field
 * to UserFormState should also extend the rejection pattern below.
 */
export function buildUserSettingsPayload(state: UserFormState): Record<string, unknown> {
  const settings: Record<string, unknown> = {};
  if (state.dedup_threshold !== undefined) settings.dedup_threshold = state.dedup_threshold;
  if (state.enrichment_enabled !== undefined) settings.enrichment_enabled = state.enrichment_enabled;
  return settings;
}
