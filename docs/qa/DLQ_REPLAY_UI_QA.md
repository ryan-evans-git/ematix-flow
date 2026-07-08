# Manual QA — DLQ / replay / rewind web UI (DLQ Phase 5)

Scope: the `#/streams/{name}/dlq` screen, the DLQ depth badge on
streaming job cards, the `replay` badge on the Runs tab, and the
Rewind control. Companion plan: `docs/plans/DLQ_REPLAY.md` (Phase 5).

## Setup A — mock API (no backend needed)

```bash
cd web-ui
npm install
npm run dev        # Vite + mock-api.mjs middleware
```

Open http://localhost:5173. The mock seeds one streaming pipeline
(`live_orders.stream_orders_to_olap`) with a 63-record DLQ.

## Setup B — real backend (for the server-side gates)

```bash
# In a module that registers a streaming pipeline via
# @ematix.streaming_pipeline(...), then:
flow web --token <secret>          # or run_server(history=..., bearer_token=...)
cd web-ui && npm run build         # serve the SPA from the FastAPI process
```

---

## 1. Jobs tab — DLQ depth badge

| # | Step | Expect |
|---|------|--------|
| 1.1 | Open `#/jobs` | The streaming card (`live_orders.stream_orders_to_olap`) shows a `DLQ 63` badge next to the Streaming pill, tinted red because depth > 0. Batch cards show no badge. |
| 1.2 | Hover the badge | Tooltip shows `59 pending / 4 parked`. |
| 1.3 | Click the badge | Navigates to `#/streams/live_orders.stream_orders_to_olap/dlq`. |

## 2. DLQ screen — summary row

| # | Step | Expect |
|---|------|--------|
| 2.1 | Land on the DLQ screen | Header `DLQ · live_orders.stream_orders_to_olap` + a "← back to jobs" link. |
| 2.2 | Depth card | `59 pending`, `4 parked`. |
| 2.3 | Arrivals sparkline | Four bars labelled `≤1m / 1–5m / 5–15m / 15–60m` with per-bucket counts under them; bar height grows with count. |
| 2.4 | Stage chips | `write`, `transform`, `late_data` chips with counts (38/13/12 in the mock), each tinted differently. |

## 3. Record table + payload drawer

| # | Step | Expect |
|---|------|--------|
| 3.1 | Table columns | Time, Stage, Error (truncated with full text on hover), Attempt, Offset (truncated base64, full on hover) + a checkbox column. |
| 3.2 | Status filter → `parked` | Table shows only the 4 parked records; page resets to 0. |
| 3.3 | Paging | With filter `(any)`, `next →` advances (25/page: pages 0,1,2 with 25/25/13); `← prev` disabled on page 0; `next →` disabled on a short page. |
| 3.4 | Click a row | A payload drawer panel opens: record id, `json · N bytes`, source id, event_ts, full error, pretty JSON preview. Clicking the row again (or `close`) closes it. |
| 3.5 | `download raw ↓` in the drawer | Downloads the exact payload bytes (octet-stream). |
| 3.6 | A record with a >4 KB payload (real backend) | Preview cuts at 4096 chars and the drawer says "(preview truncated at 4 KB)"; the download still returns the full bytes. |

## 4. Actions (confirm modals)

| # | Step | Expect |
|---|------|--------|
| 4.1 | `Replay all` | Confirm modal explains redrive semantics. Confirm → modal closes, a `last replay — taken: … succeeded: … re-dead-lettered: … parked: …` line appears above the table, stats + table reload. |
| 4.2 | Select 2 rows → `Replay selected (2)` | Modal → confirm → same report line. Buttons for selected-actions are disabled while nothing is selected. |
| 4.3 | `Replay first N…` | Modal contains an N input (default 10, min 1). |
| 4.4 | Select rows → `Park` | Modal explains parking; confirm → parked count rises in the depth card. |
| 4.5 | Select rows → `Purge` | Modal warns deletion is permanent. |
| 4.6 | `Purge all…` | Modal requires typing `purge` before Confirm enables. |
| 4.7 | Cancel any modal / click the backdrop | Modal closes, nothing happens. |

## 5. Rewind control

| # | Step | Expect |
|---|------|--------|
| 5.1 | Timestamp mode (default) | A `datetime-local` picker; empty/invalid date → inline "pick a valid date/time", no request. |
| 5.2 | Offset mode | Text input for base64 offset bytes; garbage input → inline base64 error, no request. |
| 5.3 | Rewind a stateful stream (mock always simulates this) | First submit returns the server's `confirm_state_reset` 400 → the UI switches to the typed-confirmation step ("type the stream name"). Submitting with the wrong text re-prompts; typing the exact stream name and submitting succeeds. |
| 5.4 | Success | Green line: `rewound N source(s) — state cleared — restart the stream to resume…`. |
| 5.5 | Real backend: stream running | Submit → error line shows the 409 detail ("stop it before rewinding"). |
| 5.6 | Real backend: stateless stream | No typed-confirmation step; rewind succeeds directly and reports `state_cleared` false (no "state cleared" suffix). |

## 6. Runs tab — replay badge

| # | Step | Expect |
|---|------|--------|
| 6.1 | Open `#/runs` | The `replay-live_orders-…` run row shows a blue `REPLAY` pill next to the pipeline name. |
| 6.2 | Click the pill | Navigates to that stream's DLQ screen (row-level run navigation must NOT fire). |
| 6.3 | Real backend | POST a replay from the DLQ screen, then refresh Runs: the new `replay-…` run appears with `kind=replay` and status `succeeded`. |

## 7. Errors + auth

| # | Step | Expect |
|---|------|--------|
| 7.1 | Visit `#/streams/no_such_stream/dlq` | Error panels render the 404 detail (`unknown stream …`); screen stays usable. |
| 7.2 | Real backend with `--token` | All DLQ/rewind calls ride the same-origin `/api/*` rules as the rest of the SPA — no separate auth plumbing (verify actions work when logged through the token-fronting proxy, 401 otherwise). |
| 7.3 | Build check | `cd web-ui && npm run build` exits 0 with no Svelte compile warnings for the new screen. |
