import { useQuery } from "@tanstack/react-query";
import { meAPI, type MeCapabilities } from "../api/client";

// Pure decision function so tests don't need a React renderer. Loading or
// errored states report the gate closed to prevent UI flashing.
//
// The signal historically came from /admin/providers (was the user able to
// see provider slots and were all three configured?). After the
// 2026-04-30 admin-tier-split, that endpoint is administrator-only; non-
// admins got 403, the hook returned available=false, and the Enrichment
// Queue / Dreaming nav entries disappeared for project owners. The new
// /me/capabilities endpoint exposes the same enrichment_available signal
// (it mirrors the EnrichmentGate middleware) without leaking provider
// config, so all roles see the nav consistently.
export function isEnrichmentAvailable(
  caps: MeCapabilities | undefined | null,
  isLoading: boolean,
  isError: boolean,
): boolean {
  if (isLoading || isError) return false;
  if (!caps) return false;
  return caps.enrichment_available === true;
}

export function useMeCapabilities() {
  return useQuery({
    queryKey: ["me", "capabilities"],
    queryFn: meAPI.getCapabilities,
    // Capabilities only flip on provider-slot updates and the dreaming-
    // enabled toggle; useUpdateProviderSlot and useSetDreamingEnabled both
    // invalidate this key on success. 5 minutes is long enough to skip
    // refetch on routine page navigation, and explicit invalidation still
    // wins on real changes.
    staleTime: 5 * 60 * 1000,
  });
}

export function useEnrichmentAvailable(): {
  available: boolean;
  isLoading: boolean;
  isError: boolean;
} {
  const { data, isLoading, isError } = useMeCapabilities();
  return {
    available: isEnrichmentAvailable(data, isLoading, isError),
    isLoading,
    isError,
  };
}
