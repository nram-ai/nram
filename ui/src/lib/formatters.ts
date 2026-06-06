export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

// formatBytes renders a byte count as a human-readable size with up to
// one decimal place. Negative or null inputs surface as "-" so callers
// (table cells, summary lines) can pass `artifact_bytes` directly without
// pre-checking for absence. Uses base-1024 units per the storage / disk
// convention the rest of the app reads against.
export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null || bytes < 0) return "-";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

// truncateId renders a long identifier (UUID, memory id, project id) as a
// short prefix with an ellipsis. Used in table cells where the full id would
// blow out column width; the tooltip / title attribute still carries the
// full string for copy/paste.
export function truncateId(id: string): string {
  if (id.length <= 12) return id;
  return id.slice(0, 8) + "...";
}
