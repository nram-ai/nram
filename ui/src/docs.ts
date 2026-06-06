// Standalone entry for the public API reference served at /docs. It renders
// the OpenAPI spec the server already publishes at /openapi.yaml using Scalar,
// so the human-readable docs reuse the single source of truth with no
// duplication. This entry is separate from the console SPA (main.tsx), so
// Scalar and its Vue runtime never load in the main bundle.
import { createApiReference } from "@scalar/api-reference";
import "@scalar/api-reference/style.css";

// The inline script in docs/index.html sets `.dark` from the stored theme
// before paint; mirror that into Scalar's own dark-mode flag.
createApiReference("#app", {
  url: "/openapi.yaml",
  darkMode: document.documentElement.classList.contains("dark"),
});

// The console persists the theme to localStorage["nram_theme"] and the browser
// broadcasts a `storage` event to other same-origin tabs. Scalar resolves its
// dark-mode flag only at mount, so the cleanest way to follow a theme change
// the user made in the console while /docs is open is to re-run this entry's
// init path — reload, and the inline script re-seeds the correct mode.
window.addEventListener("storage", (event) => {
  if (event.key === "nram_theme") {
    location.reload();
  }
});
