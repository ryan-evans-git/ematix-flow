# ematix-flow Web UI

Single-page app (Vite + Svelte) that talks to the FastAPI server in
`python/ematix_flow/web/server.py`. Built bundle is emitted into
`../python/ematix_flow/web/ui_dist/` so the wheel picks it up via
the `[web]` extra.

## Pages

- `#/runs` — list of runs with pipeline + status filters
- `#/runs/:id` — single run detail with action buttons:
  - **Pause** (running) / **Resume** (paused)
  - **Restart from step** (failed batch) / **Resume from watermark** (failed streaming)
  - **Rerun from beginning** (finished / failed)
- `#/pipelines` — per-pipeline failure rate + median duration summary

## Build

```sh
cd web-ui
npm install
npm run build
```

The build outputs to `../python/ematix_flow/web/ui_dist/`. After
building, `pip install -e ../[web]` and `flow web --port 8080`
serves the SPA at `http://127.0.0.1:8080/`.

## Dev workflow

```sh
# Terminal 1 — Python API
flow web --port 8080

# Terminal 2 — Vite dev server with API proxy (port 5173)
cd web-ui && npm run dev
```

Vite's dev server proxies `/api/*` to `http://127.0.0.1:8080` per
`vite.config.js`, so changes in `src/` hot-reload while the
underlying RunLog data still comes from your real backend.

## Theme

CSS in `src/lib/styles.css` mirrors ematix.dev's Pip-Boy / CRT theme
(phosphor teal #33dccc, amber #ffb000, vault-black #060a08, VT323 +
IBM Plex Mono fonts, scanlines + flicker overlay).

## API contract

`src/lib/api.js` wraps the JSON endpoints documented in
`docs/PHASE_WEB_UI_DESIGN.md`. The Python server's action-gating
logic decides which buttons may be rendered for a given run; the
SPA just reads `run.actions.*` and shows / hides accordingly.
