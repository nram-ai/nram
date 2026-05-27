// Recharts color binding. The seven values map to --data-1..--data-7
// CSS variables defined in index.css; light/dark mode is honored by
// the variables, not by reading the theme here.
//
// Recharts color props accept full CSS color strings, so each entry is a
// concrete hsl() call computed from the runtime CSS-variable value.

function readDataColor(idx: number): string {
  if (typeof window === "undefined") return "hsl(201, 92%, 74%)";
  const raw = getComputedStyle(document.documentElement)
    .getPropertyValue(`--data-${idx}`)
    .trim();
  return raw ? `hsl(${raw})` : "hsl(201, 92%, 74%)";
}

export function getChartColors(): string[] {
  return [
    readDataColor(1),
    readDataColor(2),
    readDataColor(3),
    readDataColor(4),
    readDataColor(5),
    readDataColor(6),
    readDataColor(7),
  ];
}

// Convenience: a single color by 1-based index, wrapping past 7.
export function chartColor(i: number): string {
  const idx = ((i - 1) % 7) + 1;
  return readDataColor(idx);
}
