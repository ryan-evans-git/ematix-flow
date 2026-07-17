<script>
  import { onMount } from "svelte";
  import { listRuns } from "../lib/api.js";

  let runs = [];
  let total = 0;
  let loading = true;
  let error = null;
  let pipelineFilter = "";
  let statusFilter = "";
  let sortKey = "started";  // pipeline | status | started | duration | attempt
  let sortDir = "desc";
  const PAGE_SIZE = 50;
  let offset = 0;
  let loadSeq = 0;

  // Reset to the first page whenever a filter changes, then load.
  function reload() {
    offset = 0;
    load();
  }

  async function load() {
    const seq = ++loadSeq;
    loading = true;
    error = null;
    try {
      const body = await listRuns({
        pipeline: pipelineFilter || undefined,
        status: statusFilter || undefined,
        limit: PAGE_SIZE,
        offset,
      });
      if (seq !== loadSeq) return; // superseded
      runs = body.runs;
      total = body.total;
    } catch (e) {
      if (seq !== loadSeq) return;
      error = e.message;
    } finally {
      if (seq === loadSeq) loading = false;
    }
  }

  function nextPage() {
    if (offset + PAGE_SIZE < total) { offset += PAGE_SIZE; load(); }
  }
  function prevPage() {
    if (offset > 0) { offset = Math.max(0, offset - PAGE_SIZE); load(); }
  }
  function openRun(runId) {
    window.location.hash = `#/runs/${encodeURIComponent(runId)}`;
  }
  $: pageStart = total === 0 ? 0 : offset + 1;
  $: pageEnd = Math.min(offset + runs.length, total);

  onMount(() => {
    // Honor the `?pipeline=` query string when arriving via "all jobs →".
    const hash = window.location.hash || "";
    const q = hash.split("?")[1];
    if (q) {
      const params = new URLSearchParams(q);
      const p = params.get("pipeline");
      if (p) pipelineFilter = p;
      const s = params.get("status");
      if (s) statusFilter = s;
    }
    load();
  });

  $: sorted = (() => {
    const dir = sortDir === "asc" ? 1 : -1;
    return runs.slice().sort((a, b) => {
      let av, bv;
      switch (sortKey) {
        case "pipeline": av = a.pipeline || ""; bv = b.pipeline || ""; break;
        case "status":   av = a.status || "";   bv = b.status || "";   break;
        case "duration": av = a.duration_ms ?? -1; bv = b.duration_ms ?? -1; break;
        case "attempt":  av = a.attempt ?? 0;   bv = b.attempt ?? 0;   break;
        case "started":
        default:         av = a.started_at || ""; bv = b.started_at || "";
      }
      if (av < bv) return -1 * dir;
      if (av > bv) return  1 * dir;
      return 0;
    });
  })();

  function setSort(key) {
    if (sortKey === key) {
      sortDir = sortDir === "asc" ? "desc" : "asc";
    } else {
      sortKey = key;
      sortDir = key === "started" ? "desc" : "asc";
    }
  }
  function arrow(key) {
    if (sortKey !== key) return "";
    return sortDir === "asc" ? " ▲" : " ▼";
  }

  function fmtDuration(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    try {
      return new Date(iso).toISOString().replace("T", " ").slice(0, 19);
    } catch (_) {
      return iso;
    }
  }
</script>

<h1>Runs</h1>

<div class="panel">
  <div style="display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;">
    <label>
      Pipeline:
      <input
        type="text"
        bind:value={pipelineFilter}
        on:change={reload}
        placeholder="(any)"
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);"
      />
    </label>
    <label>
      Status:
      <select
        bind:value={statusFilter}
        on:change={reload}
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);"
      >
        <option value="">(any)</option>
        <option value="running">running</option>
        <option value="paused">paused</option>
        <option value="requested">requested</option>
        <option value="succeeded">succeeded</option>
        <option value="failed">failed</option>
        <option value="cancelled">cancelled</option>
      </select>
    </label>
    <button class="action" on:click={load}>Refresh</button>
    <div class="pager mono" style="margin-left: auto;">
      <button class="action" on:click={prevPage} disabled={offset === 0}>‹ prev</button>
      <span>{pageStart}–{pageEnd} of {total}</span>
      <button class="action" on:click={nextPage} disabled={offset + runs.length >= total}>next ›</button>
    </div>
  </div>
</div>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if runs.length === 0}
  <div class="empty">no runs match the current filter</div>
{:else}
  <table class="runs">
    <thead>
      <tr>
        <th style="cursor: pointer" on:click={() => setSort("pipeline")}>Pipeline{arrow("pipeline")}</th>
        <th style="cursor: pointer" on:click={() => setSort("status")}>Status{arrow("status")}</th>
        <th style="cursor: pointer" on:click={() => setSort("started")}>Started{arrow("started")}</th>
        <th style="cursor: pointer" on:click={() => setSort("duration")}>Duration{arrow("duration")}</th>
        <th style="cursor: pointer" on:click={() => setSort("attempt")}>Attempt{arrow("attempt")}</th>
        <th>Run ID</th>
      </tr>
    </thead>
    <tbody>
      {#each sorted as r (r.run_id)}
        <tr
          class="run-row"
          tabindex="0"
          role="button"
          aria-label={`Open run ${r.run_id}`}
          on:click={() => openRun(r.run_id)}
          on:keydown={(e) => {
            if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openRun(r.run_id); }
          }}
        >
          <td>
            {r.pipeline}
            {#if r.kind === "replay"}
              <a
                class="replay-badge"
                href={`#/streams/${encodeURIComponent(r.pipeline)}/dlq`}
                on:click|stopPropagation
                title="DLQ replay run — open this stream's DLQ"
              >replay</a>
            {/if}
          </td>
          <td><span class="status status--{r.status}">{r.status}</span></td>
          <td>{fmtTime(r.started_at)}</td>
          <td>{fmtDuration(r.duration_ms)}</td>
          <td>{r.attempt}</td>
          <td class="mono">{r.run_id}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}


<style>
  .pager { display: inline-flex; gap: 0.5rem; align-items: center; }
  .run-row { cursor: pointer; }
  .run-row:focus-visible { outline: 2px solid var(--info, #4090b8); outline-offset: -2px; }
  .replay-badge {
    display: inline-block;
    margin-left: 6px;
    padding: 0 7px;
    border-radius: 999px;
    border: 1px solid var(--info, #4090b8);
    color: var(--info, #4090b8);
    font-size: 0.72em;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    text-decoration: none;
  }
</style>
