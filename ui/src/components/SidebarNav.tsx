// Desktop primary navigation: a slim icon rail of section icons with a flyout
// panel that lists one section's links at a time. Replaces the always-visible
// 240px labeled column on >= md screens; the mobile slide-out drawer (in
// App.tsx) is unchanged and owns navigation below md.
//
// Interaction model:
//   - Hover (mouse) or focus (keyboard) a rail icon -> its flyout panel opens.
//   - Click toggles the panel; single-item sections navigate directly instead.
//   - Pin docks the open panel so it stays beside the rail (pushing content);
//     the choice persists to localStorage (nram_nav_pinned), mirroring the
//     theme-preference pattern in ThemeContext.
//   - Escape / outside-click / navigation close an unpinned panel.
//
// Layout behavior is driven entirely by the section model (placement,
// alwaysPanel, showBuildInfo) rather than by branching on section names, so a
// new section only needs the right flags in App.tsx's SECTION_META.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { NavLink } from "react-router-dom";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import { faSun, faMoon, faRightFromBracket, faThumbtack } from "../lib/icons";
import { Logo } from "./Logo";
import { BuildInfo } from "./BuildInfo";
import type { HealthResponse, Theme } from "../api/client";

export interface NavItem {
  path: string;
  label: string;
  section: string;
  icon: IconDefinition;
  minRole?: string;
  writeOnly?: boolean;
  requiresEnrichment?: boolean;
  // requiresAsk hides the entry unless the ask feature flag (ask.enabled) is on.
  requiresAsk?: boolean;
  // External links to a server-served (non-SPA) route, opened in a new tab.
  external?: boolean;
}

// One rail entry: a section, its rail icon, the items it groups, and the
// layout metadata that drives placement and panel behavior.
export interface NavSection {
  section: string;
  icon: IconDefinition;
  items: NavItem[];
  // "middle" icons stack under the logo; "bottom" sit in the utility cluster
  // (beside theme/logout).
  placement: "middle" | "bottom";
  // Keep a flyout even for a single-item section (e.g. Account, which carries a
  // build-info footer). Without it, single-item sections navigate directly.
  alwaysPanel?: boolean;
  // Render the running-binary build identity in the panel footer.
  showBuildInfo?: boolean;
}

const PIN_KEY = "nram_nav_pinned";

function readPinned(): boolean {
  try {
    return localStorage.getItem(PIN_KEY) === "1";
  } catch {
    return false;
  }
}

function writePinned(v: boolean) {
  try {
    localStorage.setItem(PIN_KEY, v ? "1" : "0");
  } catch {
    // private-mode browsers throw; the in-memory state still applies.
  }
}

// A section earns a flyout panel when it has more than one item, or when it is
// explicitly flagged (a single link plus, e.g., the build-identity footer).
function hasPanel(s: NavSection): boolean {
  return s.items.length > 1 || !!s.alwaysPanel;
}

function isActivePath(itemPath: string, activePath: string): boolean {
  if (itemPath === "/") return activePath === "/";
  return activePath === itemPath || activePath.startsWith(itemPath + "/");
}

function sectionIsActive(s: NavSection, activePath: string): boolean {
  return s.items.some((it) => !it.external && isActivePath(it.path, activePath));
}

const railIconBase =
  "relative flex h-10 w-10 items-center justify-center rounded-md transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-ring";
// Shared active/idle treatment for both the rail icons and the panel links so
// the highlight stays in one place.
const navActive =
  "bg-accent text-accent-foreground shadow-[0_0_24px_-12px_hsl(var(--ring))] before:absolute before:left-0 before:top-1.5 before:bottom-1.5 before:w-0.5 before:rounded-r before:bg-primary";
const navIdle = "text-muted-foreground hover:bg-accent hover:text-accent-foreground";

function railIconClass(active: boolean): string {
  return `${railIconBase} ${active ? navActive : navIdle}`;
}

const panelLinkClass = ({ isActive }: { isActive: boolean }) =>
  `relative flex items-center gap-2.5 rounded-md px-2 py-2 text-sm transition-colors ${
    isActive ? `font-medium ${navActive}` : navIdle
  }`;

function panelClass(docked: boolean): string {
  return docked
    ? "surface-elevated flex h-full w-56 shrink-0 flex-col border-l border-border/60"
    : "surface-elevated absolute left-14 top-0 bottom-0 z-50 flex w-56 flex-col border-l border-border/60 shadow-xl motion-safe:transition-opacity";
}

function panelDomId(section: string): string {
  return `nav-panel-${section.toLowerCase()}`;
}

const railIcon = (icon: IconDefinition) => (
  <FontAwesomeIcon icon={icon} className="h-[1.05rem] w-[1.05rem]" />
);

interface Props {
  sections: NavSection[];
  activePath: string;
  theme: Theme;
  onToggleTheme: () => void;
  onLogout: () => void;
  health?: HealthResponse | null;
  buildCommit?: string | null;
}

export default function SidebarNav({
  sections,
  activePath,
  theme,
  onToggleTheme,
  onLogout,
  health,
  buildCommit,
}: Props) {
  const [openSection, setOpenSection] = useState<string | null>(null);
  const [pinned, setPinned] = useState<boolean>(readPinned);

  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const railBtnRefs = useRef<Record<string, HTMLButtonElement | null>>({});
  const openTimer = useRef<number | null>(null);
  const closeTimer = useRef<number | null>(null);

  const clearTimers = useCallback(() => {
    if (openTimer.current !== null) {
      window.clearTimeout(openTimer.current);
      openTimer.current = null;
    }
    if (closeTimer.current !== null) {
      window.clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
  }, []);

  useEffect(() => clearTimers, [clearTimers]);

  // Single pass over the sections: split by placement, index by name for O(1)
  // panel lookups, and find the active section.
  const { middleSections, bottomSections, byName, activeSection } = useMemo(() => {
    const middle: NavSection[] = [];
    const bottom: NavSection[] = [];
    const map = new Map<string, NavSection>();
    let active: string | null = null;
    for (const s of sections) {
      map.set(s.section, s);
      if (s.placement === "bottom") bottom.push(s);
      else middle.push(s);
      if (active === null && sectionIsActive(s, activePath)) active = s.section;
    }
    return { middleSections: middle, bottomSections: bottom, byName: map, activeSection: active };
  }, [sections, activePath]);

  // Which section's panel is shown, split by docked (pinned) vs overlay.
  const dockedName = pinned ? (openSection ?? activeSection) : null;
  const dockedSection = dockedName ? (byName.get(dockedName) ?? null) : null;
  const overlaySection = !pinned && openSection ? (byName.get(openSection) ?? null) : null;
  const openPanelName = dockedSection?.section ?? overlaySection?.section ?? null;

  // Close the panel on navigation (unless docked open).
  useEffect(() => {
    if (!pinned) setOpenSection(null);
  }, [activePath, pinned]);

  // Outside-click closes an unpinned panel.
  useEffect(() => {
    if (pinned) return;
    function onDown(e: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpenSection(null);
      }
    }
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [pinned]);

  function handleRailEnter(s: NavSection) {
    if (pinned) return;
    clearTimers();
    if (!hasPanel(s)) {
      setOpenSection(null);
      return;
    }
    openTimer.current = window.setTimeout(() => setOpenSection(s.section), 70);
  }

  function handleWrapperLeave() {
    if (pinned) return;
    clearTimers();
    closeTimer.current = window.setTimeout(() => setOpenSection(null), 140);
  }

  function handleRailFocus(s: NavSection) {
    clearTimers();
    setOpenSection(hasPanel(s) ? s.section : null);
  }

  function handleRailClick(s: NavSection) {
    clearTimers();
    setOpenSection((prev) => (prev === s.section && !pinned ? null : s.section));
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Escape" && openSection && !pinned) {
      const toFocus = openSection;
      setOpenSection(null);
      railBtnRefs.current[toFocus]?.focus();
    }
  }

  function togglePin() {
    setPinned((prev) => {
      const next = !prev;
      writePinned(next);
      if (next) setOpenSection((s) => s ?? activeSection);
      return next;
    });
  }

  function renderRailIcon(s: NavSection) {
    const active = sectionIsActive(s, activePath);
    // A single-item section with no forced panel navigates directly; no flyout.
    if (s.items.length === 1 && !s.alwaysPanel) {
      const item = s.items[0];
      if (item.external) {
        return (
          <a
            key={s.section}
            href={item.path}
            target="_blank"
            rel="noopener noreferrer"
            title={item.label}
            aria-label={item.label}
            className={railIconClass(false)}
            onMouseEnter={() => handleRailEnter(s)}
          >
            {railIcon(s.icon)}
          </a>
        );
      }
      return (
        <NavLink
          key={s.section}
          to={item.path}
          end={item.path === "/"}
          title={item.label}
          aria-label={item.label}
          className={() => railIconClass(active)}
          onMouseEnter={() => handleRailEnter(s)}
          onFocus={() => handleRailFocus(s)}
        >
          {railIcon(s.icon)}
        </NavLink>
      );
    }

    return (
      <button
        key={s.section}
        type="button"
        ref={(el) => {
          railBtnRefs.current[s.section] = el;
        }}
        title={s.section}
        aria-label={s.section}
        aria-haspopup="menu"
        aria-expanded={openPanelName === s.section}
        aria-controls={panelDomId(s.section)}
        className={railIconClass(active)}
        onMouseEnter={() => handleRailEnter(s)}
        onFocus={() => handleRailFocus(s)}
        onClick={() => handleRailClick(s)}
      >
        {railIcon(s.icon)}
      </button>
    );
  }

  function renderPanel(s: NavSection, docked: boolean) {
    const showPin = s.placement === "middle";
    return (
      <div
        id={panelDomId(s.section)}
        role="menu"
        aria-label={s.section}
        onMouseEnter={clearTimers}
        className={panelClass(docked)}
      >
        <div className="flex items-center justify-between px-3 py-4">
          <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            {s.section}
          </h2>
          {showPin && (
            <button
              type="button"
              onClick={togglePin}
              aria-pressed={docked}
              title={docked ? "Unpin panel" : "Pin panel open"}
              aria-label={docked ? "Unpin panel" : "Pin panel open"}
              className={`rounded p-1 transition-colors ${
                docked ? "text-primary" : "text-muted-foreground/60 hover:text-foreground"
              }`}
            >
              <FontAwesomeIcon icon={faThumbtack} className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
        <ul className="flex-1 space-y-0.5 overflow-y-auto px-2 pb-3">
          {s.items.map((item) => {
            const content = (
              <>
                <FontAwesomeIcon icon={item.icon} className="h-4 w-4 opacity-80" />
                <span>{item.label}</span>
              </>
            );
            return (
              <li key={item.path}>
                {item.external ? (
                  <a
                    href={item.path}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={`relative flex items-center gap-2.5 rounded-md px-2 py-2 text-sm transition-colors ${navIdle}`}
                  >
                    {content}
                  </a>
                ) : (
                  <NavLink
                    to={item.path}
                    end={item.path === "/"}
                    className={panelLinkClass}
                    onClick={() => {
                      if (!pinned) setOpenSection(null);
                    }}
                  >
                    {content}
                  </NavLink>
                )}
              </li>
            );
          })}
        </ul>
        {s.showBuildInfo && health && (
          <div className="border-t border-border/60 px-3 py-3">
            <BuildInfo health={health} commit={buildCommit} />
          </div>
        )}
      </div>
    );
  }

  return (
    <div
      ref={wrapperRef}
      className="relative z-50 hidden md:flex"
      onMouseLeave={handleWrapperLeave}
      onKeyDown={handleKeyDown}
    >
      <div className="surface-elevated flex h-full w-14 flex-col items-center py-3">
        <Logo size="sm" showWordmark={false} />
        <nav aria-label="Primary" className="mt-3 flex flex-1 flex-col items-center gap-1">
          {middleSections.map(renderRailIcon)}
        </nav>
        <div className="mt-2 flex w-full flex-col items-center gap-1 border-t border-border/60 pt-2">
          <button
            type="button"
            onClick={onToggleTheme}
            title={theme === "dark" ? "Light mode" : "Dark mode"}
            aria-label={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
            className={railIconClass(false)}
          >
            {railIcon(theme === "dark" ? faSun : faMoon)}
          </button>
          {bottomSections.map(renderRailIcon)}
          <button
            type="button"
            onClick={onLogout}
            title="Logout"
            aria-label="Logout"
            className={railIconClass(false)}
          >
            {railIcon(faRightFromBracket)}
          </button>
        </div>
      </div>

      {dockedSection && hasPanel(dockedSection) && renderPanel(dockedSection, true)}
      {overlaySection && hasPanel(overlaySection) && renderPanel(overlaySection, false)}
    </div>
  );
}
