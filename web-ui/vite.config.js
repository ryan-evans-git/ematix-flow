import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import path from "node:path";

// Build outputs go to ../python/ematix_flow/web/ui_dist/ so the
// FastAPI server picks them up at runtime via `resources.files`
// (see python/ematix_flow/web/server.py:_resolve_ui_dir).
// The wheel-build hook (configured in pyproject.toml) calls
// `npm run build` before `pip wheel` so this directory is
// populated by the time the wheel is assembled.
export default defineConfig({
  plugins: [svelte()],
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
  // Dev-time proxy: when running `npm run dev`, forward /api
  // calls to the FastAPI server on its default port. The Web UI
  // and the API live at the same origin in production (served
  // by uvicorn from the embedded bundle), so no proxy is needed
  // there.
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: false,
      },
    },
  },
});
