<script>
  // DLQ Phase 5: per-stream dead-letter queue screen.
  //
  //   #/streams/<name>/dlq
  //
  // Depth card + arrival sparkline + stage chips, a paged record
  // table with a payload drawer, the Replay / Park / Purge actions
  // behind confirm modals (same modal pattern as Pipelines' "Run
  // now"), and the stream Rewind control. Rewind's destructive
  // state-reset path is double-gated: the server hard-errors
  // without confirm_state_reset, and the UI only sends it after
  // the operator types the stream name.
  import { onMount } from "svelte";
  import {
    getStreamDlq,
    listStreamDlqRecords,
    replayStreamDlq,
    parkStreamDlq,
    purgeStreamDlq,
    rewindStream,
  } from "../lib/api.js";

  export let name;

  let stats = null;
  let statsError = null;
  let records = [];
  let recordsError = null;
  let loading = true;
  let statusFilter = "";
  let page = 0;
  const pageSize = 25;

  // Row selection (record ids) for Replay selected / Park / Purge.
  let selected = new Set();
  // Payload drawer: the record currently expanded, or null.
  let drawer = null;

  // Confirm modal state. `mode` picks the pending action:
  //   replay-all | replay-selected | replay-first | park |
  //   purge-selected | purge-all
  let confirm = null;
  let firstN = 10;
  let purgeTyped = "";
  let actionBusy = false;
  let actionError = null;
  let lastReport = null;

  // Rewind form state.
  let rewindMode = "timestamp"; // timestamp | offset
  let rewindDatetime = "";
  let rewindOffsetB64 = "";
  let rewindBusy = false;
  let rewindError = null;
  let rewindResult = null;
  // Set when the server demanded confirm_state_reset — the operator
  // must type the stream name to proceed.
  let rewindNeedsConfirm = false;
  let rewindTyped = "";

  async function loadStats() {
    statsError = null;
    try {
      stats = await getStreamDlq(name);
    } catch (e) {
      statsError = e.message;
    }
  }

  async function loadRecords() {
    recordsError = null;
    try {
      const body = await listStreamDlqRecords(name, {
        status: statusFilter || undefined,
        page,
        pageSize,
      });
      records = body.records;
    } catch (e) {
      recordsError = e.message;
    }
  }

  async function loadAll() {
    loading = true;
    await Promise.all([loadStats(), loadRecords()]);
    loading = false;
  }

  onMount(loadAll);

  function setStatusFilter(v) {
    statusFilter = v;
    page = 0;
    selected = new Set();
    loadRecords();
  }
  function nextPage() {
    if (records.length < pageSize) return;
    page += 1;
    loadRecords();
  }
  function prevPage() {
    if (page === 0) return;
    page -= 1;
    loadRecords();
  }

  function toggle(id) {
    if (selected.has(id)) selected.delete(id);
    else selected.add(id);
    selected = new Set(selected); // reassign for reactivity
  }
  function toggleAllVisible() {
    if (records.every((r) => selected.has(r.id))) {
      for (const r of records) selected.delete(r.id);
    } else {
      for (const r of records) selected.add(r.id);
    }
    selected = new Set(selected);
  }

  function openConfirm(mode) {
    confirm = mode;
    actionError = null;
    purgeTyped = "";
  }
  function closeConfirm() {
    confirm = null;
    actionError = null;
  }

  function selectionFor(mode) {
    switch (mode) {
      case "replay-all":
        return { kind: "all" };
      case "replay-first":
        return { kind: "first_n", n: Math.max(1, Number(firstN) || 1) };
      case "purge-all":
        return { kind: "all" };
      default:
        return { kind: "ids", ids: [...selected] };
    }
  }

  async function submitConfirm() {
    if (!confirm) return;
    actionBusy = true;
    actionError = null;
    try {
      const sel = selectionFor(confirm);
      if (confirm.startsWith("replay")) {
        const r = await replayStreamDlq(name, { selection: sel });
        lastReport = r.report;
      } else if (confirm === "park") {
        await parkStreamDlq(name, sel);
      } else {
        await purgeStreamDlq(name, sel);
      }
      selected = new Set();
      closeConfirm();
      await loadAll();
    } catch (e) {
      actionError = e.message;
    } finally {
      actionBusy = false;
    }
  }

  function rewindTarget() {
    if (rewindMode === "timestamp") {
      const ms = new Date(rewindDatetime).getTime();
      if (!rewindDatetime || Number.isNaN(ms)) return null;
      return { kind: "timestamp", ms };
    }
    try {
      const raw = atob(rewindOffsetB64.trim());
      return { kind: "offset", bytes: [...raw].map((c) => c.charCodeAt(0)) };
    } catch (_) {
      return null;
    }
  }

  async function submitRewind() {
    const to = rewindTarget();
    if (!to) {
      rewindError =
        rewindMode === "timestamp"
          ? "pick a valid date/time"
          : "offset must be valid base64 (copy it from a record row)";
      return;
    }
    const confirmReset = rewindNeedsConfirm && rewindTyped === name;
    if (rewindNeedsConfirm && !confirmReset) {
      rewindError = `type the stream name (${name}) to confirm the state reset`;
      return;
    }
    rewindBusy = true;
    rewindError = null;
    rewindResult = null;
    try {
      rewindResult = await rewindStream(name, {
        to,
        confirmStateReset: confirmReset,
      });
      rewindNeedsConfirm = false;
      rewindTyped = "";
    } catch (e) {
      if ((e.message || "").includes("confirm_state_reset")) {
        // Stateful pipeline — surface the typed-confirmation step.
        rewindNeedsConfirm = true;
        rewindError =
          "this stream holds windowed/session state; rewinding clears it. " +
          "Type the stream name below to confirm.";
      } else {
        rewindError = e.message;
      }
    } finally {
      rewindBusy = false;
    }
  }

  function fmtTime(ms) {
    if (ms == null) return "—";
    try {
      return new Date(ms).toISOString().replace("T", " ").slice(0, 19);
    } catch (_) {
      return String(ms);
    }
  }
  function shortError(s) {
    if (!s) return "—";
    return s.length > 90 ? `${s.slice(0, 90)}…` : s;
  }
  function shortOffset(b64) {
    if (!b64) return "—";
    return b64.length > 16 ? `${b64.slice(0, 16)}…` : b64;
  }

  // Arrival sparkline: cumulative trailing buckets → per-interval
  // bars (1m, 5m−1m, 15m−5m, 60m−15m), height ∝ sqrt(count).
  $: sparkBars = (() => {
    const a = stats?.arrivals || {};
    const bars = [
      { label: "≤1m", count: a.last_1m || 0 },
      { label: "1–5m", count: Math.max((a.last_5m || 0) - (a.last_1m || 0), 0) },
      { label: "5–15m", count: Math.max((a.last_15m || 0) - (a.last_5m || 0), 0) },
      { label: "15–60m", count: Math.max((a.last_60m || 0) - (a.last_15m || 0), 0) },
    ];
    const max = Math.max(...bars.map((b) => b.count), 1);
    return bars.map((b) => ({
      ...b,
      h: 6 + Math.round(26 * Math.sqrt(b.count / max)),
    }));
  })();

  $: stageChips = Object.entries(stats?.by_stage || {});
  $: allVisibleSelected =
    records.length > 0 && records.every((r) => selected.has(r.id));

  const confirmCopy = {
    "replay-all": {
      title: "Replay ALL pending records",
      body:
        "Every pending record is redriven through the stream's own " +
        "transform + targets. Records that fail again return to the " +
        "DLQ with attempt+1; poison records park at the attempt budget.",
    },
    "replay-selected": {
      title: "Replay selected records",
      body: "The selected records are redriven through the stream's own transform + targets.",
    },
    "replay-first": {
      title: "Replay the oldest N records",
      body: "The N oldest pending records are redriven (append order).",
    },
    park: {
      title: "Park selected records",
      body:
        "Parked records are excluded from replays until purged — the " +
        "terminal shelf for poison messages.",
    },
    "purge-selected": {
      title: "Purge selected records",
      body: "Purged records are DELETED from the dead-letter store. This cannot be undone.",
    },
    "purge-all": {
      title: "Purge the ENTIRE DLQ",
      body:
        "Every record — pending, leased, and parked — is DELETED from " +
        "the dead-letter store. This cannot be undone.",
    },
  };
</script>

<h1>
  DLQ · <span class="mono">{name}</span>
</h1>
<p class="dim" style="margin-top: -0.5rem;">
  Dead-lettered records for this stream.
  <a class="link" href="#/jobs">← back to jobs</a>
</p>

{#if loading}
  <div class="loading">▸ loading...</div>
{:else}
  {#if statsError}
    <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
      Error: {statsError}
    </div>
  {:else if stats}
    <div style="display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: stretch;">
      <div class="panel" style="min-width: 180px;">
        <div class="dim" style="font-size: 0.8em;">DEPTH</div>
        <div style="font-size: 1.8em;" class="mono">
          {stats.depth.pending}
          <span class="dim" style="font-size: 0.5em;">pending</span>
        </div>
        <div class="mono" style="color: var(--warning, #b8a040);">
          {stats.depth.parked} <span class="dim">parked</span>
        </div>
      </div>
      <div class="panel" style="min-width: 220px;">
        <div class="dim" style="font-size: 0.8em;">
          ARRIVALS {stats.truncated ? "(scan truncated)" : ""}
        </div>
        <div style="display: flex; gap: 0.8rem; align-items: flex-end; padding-top: 0.4rem;">
          {#each sparkBars as b}
            <div style="text-align: center;">
              <div
                class="spark-bar"
                style="height: {b.h}px;"
                title="{b.count} record(s)"
              ></div>
              <div class="dim" style="font-size: 0.7em;">{b.label}</div>
              <div class="mono" style="font-size: 0.75em;">{b.count}</div>
            </div>
          {/each}
        </div>
      </div>
      <div class="panel" style="flex: 1; min-width: 220px;">
        <div class="dim" style="font-size: 0.8em;">FAILED STAGE</div>
        <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; padding-top: 0.5rem;">
          {#if stageChips.length === 0}
            <span class="dim">no records</span>
          {/if}
          {#each stageChips as [stage, count]}
            <span class="stage-chip stage-chip--{stage}">
              {stage} <span class="mono">{count}</span>
            </span>
          {/each}
        </div>
      </div>
    </div>
  {/if}

  <div class="panel">
    <div style="display: flex; gap: 0.6rem; flex-wrap: wrap; align-items: center;">
      <label>
        Status:
        <select
          value={statusFilter}
          on:change={(e) => setStatusFilter(e.target.value)}
          style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);"
        >
          <option value="">(any)</option>
          <option value="pending">pending</option>
          <option value="leased">leased</option>
          <option value="parked">parked</option>
        </select>
      </label>
      <span style="flex: 1;"></span>
      <button class="action" on:click={() => openConfirm("replay-all")}>Replay all</button>
      <button
        class="action"
        disabled={selected.size === 0}
        on:click={() => openConfirm("replay-selected")}
      >
        Replay selected ({selected.size})
      </button>
      <button class="action" on:click={() => openConfirm("replay-first")}>Replay first N…</button>
      <button
        class="action"
        disabled={selected.size === 0}
        on:click={() => openConfirm("park")}
      >
        Park
      </button>
      <button
        class="action"
        disabled={selected.size === 0}
        on:click={() => openConfirm("purge-selected")}
      >
        Purge
      </button>
      <button class="action action--danger" on:click={() => openConfirm("purge-all")}>
        Purge all…
      </button>
      <button class="action" on:click={loadAll}>Refresh</button>
    </div>
    {#if lastReport}
      <div class="mono" style="margin-top: 0.5rem; font-size: 0.85em;">
        last replay — taken: {lastReport.taken}, succeeded: {lastReport.succeeded},
        re-dead-lettered: {lastReport.redeadlettered}, parked: {lastReport.parked}
      </div>
    {/if}
  </div>

  {#if recordsError}
    <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
      Error: {recordsError}
    </div>
  {:else if records.length === 0}
    <div class="empty">
      {page === 0 ? "the dead-letter queue is empty" : "no more records"}
    </div>
  {:else}
    <table class="runs">
      <thead>
        <tr>
          <th style="width: 2ch;">
            <input
              type="checkbox"
              checked={allVisibleSelected}
              on:change={toggleAllVisible}
              title="select all on this page"
            />
          </th>
          <th>Time</th>
          <th>Stage</th>
          <th>Error</th>
          <th>Attempt</th>
          <th>Offset</th>
        </tr>
      </thead>
      <tbody>
        {#each records as r (r.id)}
          <tr on:click={() => (drawer = drawer?.id === r.id ? null : r)}>
            <td on:click|stopPropagation>
              <input
                type="checkbox"
                checked={selected.has(r.id)}
                on:change={() => toggle(r.id)}
              />
            </td>
            <td class="mono">{fmtTime(r.failed_at)}</td>
            <td><span class="stage-chip stage-chip--{r.stage}">{r.stage}</span></td>
            <td title={r.error}>{shortError(r.error)}</td>
            <td class="mono">{r.attempt}</td>
            <td class="mono" title={r.offset_base64 || ""}>{shortOffset(r.offset_base64)}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    <div style="display: flex; gap: 0.5rem; align-items: center; margin-top: 0.5rem;">
      <button class="action" on:click={prevPage} disabled={page === 0}>← prev</button>
      <span class="mono">page {page}</span>
      <button class="action" on:click={nextPage} disabled={records.length < pageSize}>
        next →
      </button>
    </div>
  {/if}

  {#if drawer}
    <div class="panel" style="border-color: var(--color-phosphor-500);">
      <div style="display: flex; align-items: baseline; gap: 0.75rem;">
        <h3 style="margin: 0;">Payload · <span class="mono">{drawer.id}</span></h3>
        <span class="dim">
          {drawer.payload_format} · {drawer.payload_size} bytes
          {drawer.payload_truncated ? " (preview truncated at 4 KB)" : ""}
        </span>
        <span style="flex: 1;"></span>
        <a class="link" href={drawer.download} download>download raw ↓</a>
        <button class="action" on:click={() => (drawer = null)}>close</button>
      </div>
      <div class="dim" style="font-size: 0.85em; margin: 0.3rem 0;">
        source: <span class="mono">{drawer.source_id}</span>
        · event_ts: <span class="mono">{drawer.event_ts ?? "—"}</span>
      </div>
      <div class="dim" style="font-size: 0.85em; margin: 0.3rem 0;">
        error: {drawer.error}
      </div>
      <pre class="payload-preview">{drawer.payload_preview}</pre>
    </div>
  {/if}

  <div class="panel">
    <h3 style="margin-top: 0;">Rewind stream</h3>
    <p class="dim" style="font-size: 0.85em;">
      Move the stream's read position back to a timestamp (resolved
      per source) or to opaque offset bytes (single-source streams).
      The stream must be stopped first — the server refuses while it
      is running. Stateful (windowed/session) streams additionally
      require a typed confirmation: their accumulated state is
      cleared so re-consumed rows don't double-count.
    </p>
    <div style="display: flex; gap: 0.6rem; flex-wrap: wrap; align-items: center;">
      <label>
        <input type="radio" bind:group={rewindMode} value="timestamp" /> timestamp
      </label>
      <label>
        <input type="radio" bind:group={rewindMode} value="offset" /> offset bytes
      </label>
      {#if rewindMode === "timestamp"}
        <input
          type="datetime-local"
          bind:value={rewindDatetime}
          step="1"
          style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);"
        />
      {:else}
        <input
          type="text"
          bind:value={rewindOffsetB64}
          placeholder="base64 offset bytes (copy from a record row)"
          style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono); width: 34ch;"
        />
      {/if}
      <button class="action" on:click={submitRewind} disabled={rewindBusy}>
        {rewindBusy ? "..." : "Rewind"}
      </button>
    </div>
    {#if rewindNeedsConfirm}
      <div style="margin-top: 0.5rem; display: flex; gap: 0.5rem; align-items: center;">
        <span style="color: var(--color-alarm, #b85040);">
          state reset required — type <span class="mono">{name}</span> to confirm:
        </span>
        <input
          type="text"
          bind:value={rewindTyped}
          placeholder={name}
          style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-alarm, #b85040); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono); width: 28ch;"
        />
      </div>
    {/if}
    {#if rewindError}
      <div style="color: var(--color-alarm, #b85040); font-size: 0.85em; margin-top: 0.4rem;">
        {rewindError}
      </div>
    {/if}
    {#if rewindResult}
      <div class="mono" style="font-size: 0.85em; margin-top: 0.4rem; color: var(--color-phosphor-300, #8fd28f);">
        rewound {rewindResult.sources.length} source(s)
        {rewindResult.state_cleared ? "— state cleared" : ""}
        — restart the stream to resume from the new position
      </div>
    {/if}
  </div>
{/if}

{#if confirm}
  <div class="modal-bg" on:click={closeConfirm} role="presentation">
    <div class="modal" on:click|stopPropagation role="dialog" aria-label={confirmCopy[confirm].title}>
      <h3 class="modal-title">{confirmCopy[confirm].title}</h3>
      <p class="dim" style="font-size: 0.85em;">{confirmCopy[confirm].body}</p>
      {#if confirm === "replay-first"}
        <label>
          N:
          <input
            type="number"
            min="1"
            bind:value={firstN}
            style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono); width: 8ch;"
          />
        </label>
      {/if}
      {#if confirm === "purge-all"}
        <label style="display: block; margin-top: 0.4rem;">
          Type <span class="mono">purge</span> to confirm:
          <input
            type="text"
            bind:value={purgeTyped}
            placeholder="purge"
            style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-alarm, #b85040); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono); width: 12ch;"
          />
        </label>
      {/if}
      <div style="display: flex; gap: 0.5em; align-items: center; margin-top: 0.7em;">
        {#if actionError}
          <span style="color: var(--color-alarm, #b85040); font-size: 0.85em;">{actionError}</span>
        {/if}
        <span style="flex: 1;"></span>
        <button class="action" on:click={closeConfirm} disabled={actionBusy}>Cancel</button>
        <button
          class="action"
          on:click={submitConfirm}
          disabled={actionBusy || (confirm === "purge-all" && purgeTyped !== "purge")}
        >
          {actionBusy ? "..." : "Confirm"}
        </button>
      </div>
    </div>
  </div>
{/if}

<style>
  .modal-bg {
    position: fixed; inset: 0;
    background: rgba(0, 0, 0, 0.65);
    z-index: 50;
    display: flex; align-items: center; justify-content: center;
  }
  .modal {
    background: var(--color-vault-900, #0a1410);
    border: 1px solid var(--color-phosphor-500, #4eb84e);
    border-radius: 4px;
    padding: 1.2em 1.4em;
    min-width: 360px;
    max-width: 520px;
  }
  .modal-title { margin: 0 0 0.4em 0; }

  .spark-bar {
    width: 22px;
    margin: 0 auto;
    background: var(--accent, #4eb84e);
    opacity: 0.75;
    border-radius: 2px 2px 0 0;
  }

  .stage-chip {
    display: inline-block;
    padding: 1px 8px;
    border-radius: 999px;
    border: 1px solid var(--border, #444);
    font-size: 0.8em;
  }
  .stage-chip--transform { color: var(--warning, #b8a040); border-color: var(--warning, #b8a040); }
  .stage-chip--write     { color: var(--danger, #b85040);  border-color: var(--danger, #b85040); }
  .stage-chip--late_data { color: var(--info, #4090b8);    border-color: var(--info, #4090b8); }

  .action--danger {
    color: var(--danger, #b85040);
    border-color: var(--danger, #b85040);
  }

  .payload-preview {
    background: rgba(8, 16, 12, 0.7);
    border: 1px solid var(--border, #333);
    padding: 0.6rem;
    max-height: 320px;
    overflow: auto;
    white-space: pre-wrap;
    word-break: break-all;
    font-size: 0.82em;
  }
</style>
