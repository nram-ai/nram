// Standalone entry for the public API reference served at /docs. It renders
// the OpenAPI spec the server already publishes at /openapi.yaml using Scalar,
// so the human-readable docs reuse the single source of truth with no
// duplication. This entry is separate from the console SPA (main.tsx), so
// Scalar and its Vue runtime never load in the main bundle.
import { createApiReference } from "@scalar/api-reference";
import "@scalar/api-reference/style.css";

// Themes Scalar to the Web Console palette/typography. Self-contained; touches
// no console or shared files.
import "./docs-theme.css";

// Self-hosted to match the console's typography and keep the embedded Go binary
// fully offline (Scalar's bundled default fonts are disabled below to avoid the
// external font request).
import "@fontsource-variable/geist/wght.css";
import "@fontsource/jetbrains-mono/latin-400.css";
import "@fontsource/jetbrains-mono/latin-500.css";

// The inline script in docs/index.html sets `.dark` on <html> from the stored
// theme before paint; mirror that into Scalar. theme: "none" ships no preset so
// the variable map in docs-theme.css is the only theme. forceDarkModeState +
// hideDarkModeToggle pin Scalar's mode to the console's choice (and override
// Scalar's own colorMode storage), so /docs can never drift from the console.
const dark = document.documentElement.classList.contains("dark");

createApiReference("#app", {
  url: "/openapi.yaml",
  theme: "none",
  darkMode: dark,
  forceDarkModeState: dark ? "dark" : "light",
  hideDarkModeToggle: true,
  withDefaultFonts: false,
});

// The console persists the theme to localStorage["nram_theme"] and the browser
// broadcasts a `storage` event to other same-origin tabs. Scalar resolves its
// mode only at mount, so the cleanest way to follow a theme change the user made
// in the console while /docs is open is to re-run this entry's init path —
// reload, and the inline script re-seeds the correct mode.
window.addEventListener("storage", (event) => {
  if (event.key === "nram_theme") {
    location.reload();
  }
});
