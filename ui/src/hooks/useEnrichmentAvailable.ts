import { useProviderSlots } from "./useApi";
import type { ProviderSlot } from "../api/client";

export const REQUIRED_ENRICHMENT_SLOTS = ["embedding", "fact", "entity"] as const;

// Pure decision function so tests don't need a React renderer. Loading
// or errored states report the gate closed to prevent UI flashing.
export function isEnrichmentAvailable(
  slots: readonly ProviderSlot[] | undefined | null,
  isLoading: boolean,
  isError: boolean,
): boolean {
  if (isLoading || isError) return false;
  if (!Array.isArray(slots)) return false;
  return REQUIRED_ENRICHMENT_SLOTS.every(
    (name) => slots.find((s) => s.slot === name)?.configured === true,
  );
}

export function useEnrichmentAvailable(): {
  available: boolean;
  isLoading: boolean;
  isError: boolean;
} {
  const { data, isLoading, isError } = useProviderSlots();
  const slots = Array.isArray(data) ? data : [];
  return {
    available: isEnrichmentAvailable(slots, isLoading, isError),
    isLoading,
    isError,
  };
}
