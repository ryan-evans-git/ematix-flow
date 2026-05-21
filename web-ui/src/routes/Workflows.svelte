<script>
  import { onMount } from "svelte";
  import { listWorkflows, listPipelines } from "../lib/api.js";
  import DagFlowchart from "../lib/DagFlowchart.svelte";

  let workflows = [];
  let jobMap = {};
  let loading = true;
  let error = null;

  let nameFilter = "";
  let kindFilter = "";
  let sortKey = "name";
  let sortDir = "asc";

  async function load() {
    loading = true;
    error = null;
    try {
      const [wfs, jobs] = await Promise.all([listWorkflows(), listPipelines()]);
      workflows = wfs.workflows || [];
      const m = {};
      for (const p of (jobs.pipelines || [])) m[p.name] = p;
      jobMap = m;
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  onMount(load);

  $: visible = (() => {
    const nf = nameFilter.trim().toLowerCase();
    let out = workflows.filter((w) => {
      if (nf) {
        const inName = w.name.toLowerCase().includes(nf);
        const inJobs = w.jobs.some((j) => j.toLowerCase().includes(nf));
        if (!inName && !inJobs) return false;
      }
      if (kindFilter && w.kind !== kindFilter) return false;
      return true;
    });
    const dir = sortDir === "asc" ? 1 : -1;
    out = out.slice().sort((a, b) => {
      let av, bv;
      switch (sortKey) {
        case "size": av = a.jobs.length; bv = b.jobs.length; break;
        case "kind": av = a.kind;         bv = b.kind;        break;
        case "name":
        default:     av = a.name;         bv = b.name;
      }
      if (av < bv) return -1 * dir;
      if (av > bv) return  1 * dir;
      return 0;
    });
    return out;
  })();

  function setSort(k) {
    if (sortKey === k) {
      sortDir = sortDir === "asc" ? "desc" : "asc";
    } else { sortKey = k; sortDir = "asc"; }
  }
  function arrow(k) {
    if (sortKey !== k) return " ";
    return sortDir === "asc" ? "▲" : "▼";
  }

  function workflowSummary(w) {
    let succeeded = 0, failed = 0, running = 0;
    for (const j of w.jobs) {
      const s = jobMap[j]?.latest_run?.status;
      if (s === "succeeded") succeeded++;
      else if (s === "failed") failed++;
      else if (s === "running") running++;
    }
    return { succeeded, failed, running };
  }

  // Materialize {name} stubs for the flowchart from a workflow's jobs[].
  function nodesForWorkflow(w) {
    return w.jobs.map((j) => ({
      name: j,
      schedule: jobMap[j]?.schedule || null,
    }));
  }
</script>

<h1>Workflows</h1>

<div class="panel">
  <div style="display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;">
    <label>
      Name / job:
      <input
        type="text"
        bind:value={nameFilter}
        placeholder="(any)"
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono); width: 16ch;"
      />
    </label>
    <label>
      Kind:
      <select bind:value={kindFilter}
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);">
        <option value="">(any)</option>
        <option value="declared">declared</option>
        <option value="single">single-job</option>
      </select>
    </label>
    <span style="margin-left: auto; display: flex; gap: 0.4rem; align-items: center;">
      <span class="dim">sort:</span>
      <button class="action" on:click={() => setSort("name")}>name {arrow("name")}</button>
      <button class="action" on:click={() => setSort("size")}>jobs {arrow("size")}</button>
      <button class="action" on:click={() => setSort("kind")}>kind {arrow("kind")}</button>
    </span>
    <button class="action" on:click={load}>Refresh</button>
    <span class="mono" style="margin-left: 0.5rem;">total: {visible.length}/{workflows.length}</span>
  </div>
</div>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if visible.length === 0}
  <div class="empty">no workflows match the current filter</div>
{:else}
  {#each visible as w}
    {@const summary = workflowSummary(w)}
    <div class="panel wf-card">
      <div class="wf-header">
        <h3 class="wf-name">
          <a class="link" href={`#/dag/${encodeURIComponent(w.jobs[0])}`}>{w.name}</a>
          {#if w.kind === "streaming"}
            <span class="wf-pill wf-pill--streaming">▶ LIVE STREAMING</span>
          {:else if w.kind === "single"}
            <span class="wf-pill wf-pill--single">single</span>
          {:else}
            <span class="wf-pill">declared</span>
          {/if}
        </h3>
        <span class="wf-meta mono">
          {#if w.kind === "streaming"}
            {@const sj = jobMap[w.jobs[0]]}
            {@const s1 = sj?.streaming_stats?.stats_1m}
            {#if s1?.rows_consumed_per_sec != null}
              <strong>Throughput:</strong>
              <span class="mono">{s1.rows_consumed_per_sec >= 10 ? s1.rows_consumed_per_sec.toFixed(0) : s1.rows_consumed_per_sec.toFixed(2)} rps</span>
              <span class="dim">in (1m)</span>
              <span class="sep">·</span>
              <strong>Batch cycle:</strong>
              <span class="mono">{s1.avg_batch_cycle_ms != null ? (s1.avg_batch_cycle_ms < 1000 ? s1.avg_batch_cycle_ms.toFixed(0) + ' ms' : (s1.avg_batch_cycle_ms / 1000).toFixed(2) + ' s') : '—'}</span>
              <span class="dim">avg (1m)</span>
            {:else}
              {w.jobs.length} job{w.jobs.length === 1 ? "" : "s"} · streaming
            {/if}
          {:else}
            {w.jobs.length} job{w.jobs.length === 1 ? "" : "s"}
            · {w.edges.length} edge{w.edges.length === 1 ? "" : "s"}
            {#if summary.running > 0}· <span class="status status--running">{summary.running} running</span>{/if}
            {#if summary.failed > 0}· <span class="status status--failed">{summary.failed} failed</span>{/if}
            {#if summary.succeeded > 0}· <span class="status status--succeeded">{summary.succeeded} ok</span>{/if}
          {/if}
        </span>
      </div>

      <DagFlowchart
        nodes={nodesForWorkflow(w)}
        edges={w.edges}
        jobMap={jobMap}
        compact={true}
      />

      <div class="wf-footer">
        <a class="link" href={`#/dag/${encodeURIComponent(w.jobs[0])}`}>full DAG →</a>
        <a class="link" href={`#/runs`} style="margin-left: 1rem;">all runs →</a>
      </div>
    </div>
  {/each}
{/if}

<style>
  .wf-card { padding-bottom: 0.5rem; }
  .wf-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.75rem;
    flex-wrap: wrap;
    margin-bottom: 0.6rem;
  }
  .wf-name { margin: 0; font-size: 1.05rem; }
  .wf-pill {
    display: inline-block;
    font-size: 0.65em;
    padding: 0.1em 0.45em;
    border: 1px solid var(--color-phosphor-700, #2e7d32);
    border-radius: 3px;
    color: var(--color-phosphor-300, #8fd28f);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-left: 0.5rem;
    vertical-align: middle;
  }
  .wf-pill--single { color: var(--color-fg-muted, #888); border-color: var(--color-border, #444); }
  .wf-pill--streaming {
    color: var(--color-amber-glow, #ffb000);
    border-color: var(--color-amber-glow, #ffb000);
    animation: wf-pulse 2s ease-in-out infinite;
  }
  @keyframes wf-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.55; }
  }
  .wf-meta { font-size: 0.78rem; color: var(--color-fg-muted, #888); }
  .wf-footer {
    margin-top: 0.4rem;
    padding-top: 0.4rem;
    border-top: 1px solid var(--color-border, #444);
    font-size: 0.85em;
  }
</style>
