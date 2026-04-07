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
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes("node_modules")) {
            return undefined;
          }
          // Shared d3 modules used by both the 3D graph stack and recharts.
          // Pulled into their own chunk so neither `three` nor `charts` ends
          // up with a circular dependency on the other.
          if (/[\\/]node_modules[\\/]d3-[^\\/]+[\\/]/.test(id) || /[\\/]node_modules[\\/]internmap[\\/]/.test(id)) {
            return "d3";
          }
          // Heavy 3D graph stack — only loaded by GraphVisualization.
          if (
            /[\\/]node_modules[\\/](three|react-force-graph-3d|three-forcegraph|three-render-objects)[\\/]/.test(
              id,
            )
          ) {
            return "three";
          }
          // Charting stack — only loaded by Analytics.
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
