import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import path from "node:path";
import { mockApiPlugin } from "./mock-api.mjs";

// Build outputs go to ../python/ematix_flow/web/ui_dist/ so the
// FastAPI server picks them up at runtime via `resources.files`
// (see python/ematix_flow/web/server.py:_resolve_ui_dir).
// The wheel-build hook (configured in pyproject.toml) calls
// `npm run build` before `pip wheel` so this directory is
// populated by the time the wheel is assembled.
// Dev-time API: the mock plugin returns realistic fixture JSON for
// /api/* so the SPA can be previewed without the FastAPI backend
// running. Set `EMAT_FLOW_API_PROXY=1` to disable the mock and
// forward /api calls to a real backend at 127.0.0.1:8080 instead.
// The mock is dev-only — it isn't bundled into the production build.
const useProxy = process.env.EMAT_FLOW_API_PROXY === "1";

export default defineConfig({
  plugins: [svelte(), ...(useProxy ? [] : [mockApiPlugin()])],
  build: {
    outDir: path.resolve(__dirname, "../python/ematix_flow/web/ui_dist"),
    emptyOutDir: true,
    target: "es2020",
    rollupOptions: {
      output: {
        // Keep bundle size predictable — one entry, no
        // chunk-splitting (the UI is small enough).
        manualChunks: undefined,
      },
    },
  },
  server: {
    proxy: useProxy
      ? {
          "/api": {
            target: "http://127.0.0.1:8080",
            changeOrigin: false,
          },
        }
      : undefined,
  },
});
