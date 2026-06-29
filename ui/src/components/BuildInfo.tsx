import type { HealthResponse } from "../api/client";

// Renders the running binary's version + commit identity, with the build time
// and Go version in the title. Shared by the desktop Account flyout
// (SidebarNav) and the mobile drawer footer (App) so the format lives in one
// place.
export function BuildInfo({
  health,
  commit,
  className,
}: {
  health: HealthResponse;
  commit?: string | null;
  className?: string;
}) {
  return (
    <p
      className={`font-mono text-[11px] leading-tight text-muted-foreground/70${className ? ` ${className}` : ""}`}
      title={health.build.time ? `Built ${health.build.time} · ${health.build.go}` : health.build.go}
    >
      v{health.version}
      {commit && ` · ${commit}`}
    </p>
  );
}
