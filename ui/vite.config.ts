import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/v1": {
        target: "http://localhost:8674",
        changeOrigin: true,
      },
      "/mcp": {
        target: "http://localhost:8674",
        changeOrigin: true,
      },
      // The /docs page fetches the OpenAPI spec from the backend.
      "/openapi.yaml": {
        target: "http://localhost:8674",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    rollupOptions: {
      // Two entries: the console SPA (index.html) and the standalone public
      // API reference (docs/index.html -> dist/docs/index.html, served at /docs).
      input: {
        main: path.resolve(__dirname, "index.html"),
        docs: path.resolve(__dirname, "docs/index.html"),
      },
      output: {
        manualChunks(id) {
          // Tiny low-level helpers shared between two otherwise independent
          // entry graphs: the console SPA and the standalone /docs Scalar page.
          // Without isolating them, Rollup glues each into a big SPA feature
          // chunk (commonjsHelpers -> vendor, clsx -> charts), which then makes
          // the public docs page statically import vendor/charts and transitively
          // preload react, recharts, d3 and three. Pulling them into their own
          // chunks keeps both entries lean. The commonjsHelpers check sits above
          // the node_modules guard because it is a Rollup-generated virtual module.
          if (id.includes("commonjsHelpers")) {
            return "cjs-helpers";
          }
          if (!id.includes("node_modules")) {
            return undefined;
          }
          if (/[\\/]node_modules[\\/]clsx[\\/]/.test(id)) {
            return "clsx";
          }
          // Shared d3 modules used by both the 3D graph stack and recharts.
          // Pulled into their own chunk so neither `three` nor `charts` ends
          // up with a circular dependency on the other.
          if (/[\\/]node_modules[\\/]d3-[^\\/]+[\\/]/.test(id) || /[\\/]node_modules[\\/]internmap[\\/]/.test(id)) {
            return "d3";
          }
          // Heavy 3D graph stack, only loaded by GraphVisualization.
          if (
            /[\\/]node_modules[\\/](three|react-force-graph-3d|three-forcegraph|three-render-objects)[\\/]/.test(
              id,
            )
          ) {
            return "three";
          }
          // Charting stack, only loaded by Analytics.
          if (
            /[\\/]node_modules[\\/](recharts|victory-vendor|decimal\.js-light|fast-equals|react-smooth|react-transition-group)[\\/]/.test(
              id,
            )
          ) {
            return "charts";
          }
          if (id.includes("@tanstack/react-query")) {
            return "query";
          }
          if (
            /[\\/]node_modules[\\/](react|react-dom|react-router|react-router-dom|scheduler)[\\/]/.test(
              id,
            )
          ) {
            return "vendor";
          }
          return undefined;
        },
      },
    },
  },
});
