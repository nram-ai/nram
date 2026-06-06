// Pure helpers for the admin Settings page navigation: grouping search and
// active-tab resolution. Kept free of React so they can be unit-tested in
// isolation (mirrors settingsPayload.ts).
import type { Setting, SettingSchema, SettingGroup } from "../api/client";

// SettingWithSchema pairs a schema definition with its live override (null when
// the setting is at its registered default).
export interface SettingWithSchema {
  schema: SettingSchema;
  setting: Setting | null;
}

// CategoryContext is the group/sub-section a category lives under, used to make
// search match group and section names as well as the setting itself.
export interface CategoryContext {
  groupLabel: string;
  subLabel?: string;
}

// Synthetic group id for categories the server taxonomy does not cover. Should
// never appear in practice (the backend test enforces total coverage); it is a
// client-side safety net so a setting can never silently vanish.
export const OTHER_GROUP_ID = "other";

// buildCategoryIndex maps each category to its group/sub-section labels.
export function buildCategoryIndex(
  groups: SettingGroup[],
): Map<string, CategoryContext> {
  const index = new Map<string, CategoryContext>();
  for (const group of groups) {
    for (const sub of group.subsections) {
      index.set(sub.category, {
        groupLabel: group.label,
        subLabel: sub.label,
      });
    }
  }
  return index;
}

// matchesQuery reports whether a setting matches a search query (case
// insensitive), checking the key, description, and the group/sub-section names
// it lives under. An empty or whitespace-only query never matches; callers
// treat an empty query as "not searching" and show the active tab instead.
export function matchesQuery(
  item: SettingWithSchema,
  ctx: CategoryContext | undefined,
  query: string,
): boolean {
  const q = query.trim().toLowerCase();
  if (q === "") return false;
  const haystacks = [
    item.schema.key,
    item.schema.description ?? "",
    ctx?.groupLabel ?? "",
    ctx?.subLabel ?? "",
  ];
  return haystacks.some((h) => h.toLowerCase().includes(q));
}

// resolveActiveGroup returns the group named by requestedId when it is
// currently visible, otherwise the first visible group (or null when none are
// visible). Keeps the active tab valid when gating hides the requested group
// (e.g. enrichment toggled off) or a stale URL points at one.
export function resolveActiveGroup(
  visibleGroups: SettingGroup[],
  requestedId: string | null | undefined,
): SettingGroup | null {
  return (
    visibleGroups.find((g) => g.id === requestedId) ?? visibleGroups[0] ?? null
  );
}

// buildFallbackGroup returns a synthetic "Other" group covering any category
// present in the settings list but not referenced by the server taxonomy, or
// null when every category is already mapped. Each orphan category becomes its
// own labelled sub-section so the operator can still see and edit it.
export function buildFallbackGroup(
  groups: SettingGroup[],
  categories: string[],
): SettingGroup | null {
  const referenced = new Set<string>();
  for (const group of groups) {
    for (const sub of group.subsections) referenced.add(sub.category);
  }
  const orphans = categories.filter((c) => !referenced.has(c)).sort();
  if (orphans.length === 0) return null;
  return {
    id: OTHER_GROUP_ID,
    label: "Other",
    description:
      "Settings not assigned to a group. If you see these, the server's group registry is missing a category.",
    subsections: orphans.map((category) => ({ category, label: category })),
  };
}
