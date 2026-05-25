export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

// truncateId renders a long identifier (UUID, memory id, project id) as a
// short prefix with an ellipsis. Used in table cells where the full id would
// blow out column width — the tooltip / title attribute still carries the
// full string for copy/paste.
export function truncateId(id: string): string {
  if (id.length <= 12) return id;
  return id.slice(0, 8) + "...";
}
