import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

/**
 * useSectionTabParam syncs an active-section id to a URL search param for a
 * SectionTabs bar. It reads the param, self-heals to the first id when the
 * param is missing or names an unknown id, and returns a setter that writes the
 * param with replace (so flipping tabs doesn't stack browser history).
 *
 * `ids` is the list of valid section ids in display order; it may be derived
 * from server data and change between renders.
 */
export function useSectionTabParam(
  paramName: string,
  ids: readonly string[],
): { active: string; select: (id: string) => void } {
  const [searchParams, setSearchParams] = useSearchParams();
  const requested = searchParams.get(paramName);
  const active = requested && ids.includes(requested) ? requested : ids[0] ?? "";
  const select = useCallback(
    (id: string) => {
      const next = new URLSearchParams(searchParams);
      next.set(paramName, id);
      setSearchParams(next, { replace: true });
    },
    [paramName, searchParams, setSearchParams],
  );
  return { active, select };
}
