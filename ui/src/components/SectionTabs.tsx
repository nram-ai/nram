import { useRef, useState, useEffect, useCallback } from "react";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faChevronLeft, faChevronRight } from "../lib/icons";

/** Optional tone that colors a tab's icon, e.g. provider slot health. */
export type SectionTabTone = "ok" | "error" | "muted";

export interface SectionTabItem {
  id: string;
  label: string;
  /** Leading Font Awesome icon for the section. */
  icon?: IconDefinition;
  /** When set, colors the icon (on the inactive tab) to signal status. */
  tone?: SectionTabTone;
}

const TONE_CLASS: Record<SectionTabTone, string> = {
  ok: "text-success",
  error: "text-destructive",
  muted: "text-muted-foreground/60",
};

/**
 * SectionTabs is a generic segmented tab bar: a single contained control that
 * shows one section at a time, used to break up otherwise-tall admin pages.
 *
 * It mirrors the look of TierTabs but is parameterized over arbitrary items
 * (TierTabs is hard-wired to AuthContext tiers). The tabs sit in a single
 * horizontally-scrollable row. When they overflow the container width, chevron
 * buttons appear on the overflowing side(s) so every tab stays reachable even
 * where the native scrollbar is hidden; the active tab is always scrolled into
 * view (e.g. when a far-right tab is deep-linked).
 *
 * Each tab can carry a Font Awesome icon. An optional tone colors that icon on
 * inactive tabs so a status (e.g. provider slot health) stays visible at a
 * glance; the active tab's icon inherits the selected text color for contrast.
 */
export default function SectionTabs({
  items,
  active,
  onChange,
  ariaLabel,
}: {
  items: SectionTabItem[];
  active: string;
  onChange: (id: string) => void;
  ariaLabel: string;
}) {
  const stripRef = useRef<HTMLDivElement>(null);
  const [canLeft, setCanLeft] = useState(false);
  const [canRight, setCanRight] = useState(false);

  const measure = useCallback(() => {
    const el = stripRef.current;
    if (!el) return;
    const { scrollLeft, scrollWidth, clientWidth } = el;
    setCanLeft(scrollLeft > 1);
    setCanRight(scrollLeft + clientWidth < scrollWidth - 1);
  }, []);

  // Attach the scroll listener and ResizeObserver once; `measure` is stable.
  // Item-count changes re-measure via the effect below (the strip's own box
  // size is unchanged by adding tabs, so the observer alone wouldn't catch it).
  useEffect(() => {
    const el = stripRef.current;
    if (!el) return;
    measure();
    el.addEventListener("scroll", measure, { passive: true });
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => {
      el.removeEventListener("scroll", measure);
      ro.disconnect();
    };
  }, [measure]);

  // Keep the active tab visible (incl. first paint with a deep-linked far-right
  // tab) and re-measure when the active tab or item set changes.
  useEffect(() => {
    const el = stripRef.current;
    if (!el) return;
    const activeBtn = el.querySelector<HTMLElement>('[aria-selected="true"]');
    activeBtn?.scrollIntoView({ block: "nearest", inline: "nearest" });
    measure();
  }, [active, items.length, measure]);

  const scrollByDir = (dir: 1 | -1) => {
    const el = stripRef.current;
    if (!el) return;
    el.scrollBy({ left: dir * Math.max(140, el.clientWidth * 0.6), behavior: "smooth" });
  };

  if (items.length <= 1) return null;

  return (
    <div className="flex max-w-full items-stretch gap-0.5 rounded-md border bg-card p-0.5">
      {canLeft && (
        <button
          type="button"
          aria-label="Scroll tabs left"
          onClick={() => scrollByDir(-1)}
          className="shrink-0 rounded px-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <FontAwesomeIcon icon={faChevronLeft} className="h-3 w-3" />
        </button>
      )}
      <div
        ref={stripRef}
        role="tablist"
        aria-label={ariaLabel}
        className="flex gap-0.5 overflow-x-auto scroll-smooth [&::-webkit-scrollbar]:hidden"
      >
        {items.map((item) => {
          const selected = item.id === active;
          const iconColor = selected ? "" : item.tone ? TONE_CLASS[item.tone] : "";
          return (
            <button
              key={item.id}
              type="button"
              role="tab"
              aria-selected={selected}
              onClick={() => onChange(item.id)}
              className={`inline-flex items-center gap-1.5 whitespace-nowrap rounded px-3 py-1.5 text-sm font-medium ${
                selected
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-muted"
              }`}
            >
              {item.icon && (
                <FontAwesomeIcon
                  icon={item.icon}
                  className={`h-3.5 w-3.5 shrink-0 ${iconColor}`}
                  aria-hidden="true"
                />
              )}
              {item.label}
            </button>
          );
        })}
      </div>
      {canRight && (
        <button
          type="button"
          aria-label="Scroll tabs right"
          onClick={() => scrollByDir(1)}
          className="shrink-0 rounded px-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <FontAwesomeIcon icon={faChevronRight} className="h-3 w-3" />
        </button>
      )}
    </div>
  );
}
