<script>
  import { onMount } from "svelte";
  import { modal } from "../lib/a11y.js";
  import { listWorkflows, listPipelines, runWorkflowNow } from "../lib/api.js";
  import DagFlowchart from "../lib/DagFlowchart.svelte";
  import TriggerExpr from "../lib/TriggerExpr.svelte";

  let workflows = [];
  let jobMap = {};
  let loading = true;
  let error = null;

  // Run-now modal state
  let runNowFor = null;           // workflow object currently being run, or null
  let runNowSelected = new Set(); // selected job names
  let runNowBusy = false;
  let runNowError = null;
  let runNowOk = null;

  function openRunNow(w) {
    runNowFor = w;
    runNowSelected = new Set(w.jobs);  // default: all
    runNowError = null;
    runNowOk = null;
  }
  function closeRunNow() {
    runNowFor = null;
    runNowError = null;
    runNowOk = null;
  }
  function toggleJob(j) {
    if (runNowSelected.has(j)) runNowSelected.delete(j);
    else runNowSelected.add(j);
    runNowSelected = new Set(runNowSelected);  // trigger reactivity
  }
  async function submitRunNow() {
    if (!runNowFor || runNowSelected.size === 0) return;
    runNowBusy = true;
    runNowError = null;
    try {
      const r = await runWorkflowNow(runNowFor.name, {
        jobs: [...runNowSelected],
      });
      runNowOk = r.new_run_id || "queued";
      // Auto-close after a moment.
      setTimeout(closeRunNow, 1200);
    } catch (e) {
      runNowError = e.message;
    } finally {
      runNowBusy = false;
    }
  }

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
            <span class="wf-pill wf-pill--streaming">Streaming</span>
          {:else if w.kind === "single"}
            <span class="wf-pill wf-pill--single">Single</span>
          {:else}
            <span class="wf-pill">Declared</span>
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

      {#if w.kind !== "streaming" && w.triggers && w.triggers.length > 0}
        <div class="wf-trigger">
          <span class="wf-trigger-label">Trigger:</span>
          {#each w.triggers as t, ti}
            {#if ti > 0}<span class="wf-and">·&nbsp;AND&nbsp;·</span>{/if}
            {#if t.kind === "schedule"}
              <span class="wf-trig wf-trig--{t.state}">
                <span class="wf-trig-dot"></span>
                <strong>Schedule:</strong>
                <span class="mono">{t.label}</span>
              </span>
            {:else if t.kind === "on_message"}
              <span class="wf-trig wf-trig--{t.state}">
                <span class="wf-trig-dot"></span>
                <strong>On message:</strong>
                <span class="mono">{t.label}</span>
              </span>
            {:else if t.kind === "after_expr"}
              <TriggerExpr node={t.expr} topLevel={true} />
            {/if}
          {/each}
          <button class="action wf-run-now"
                  on:click|stopPropagation={() => openRunNow(w)}
                  disabled={w.jobs.length === 0}>
            Run now
          </button>
        </div>
      {:else if w.kind !== "streaming"}
        <div class="wf-trigger wf-trigger--empty">
          <span class="dim">No trigger declared</span>
          <button class="action wf-run-now"
                  on:click|stopPropagation={() => openRunNow(w)}
                  disabled={w.jobs.length === 0}>
            Run now
          </button>
        </div>
      {/if}

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

{#if runNowFor}
  <div class="modal-bg" on:click={closeRunNow} role="presentation">
    <div class="modal" on:click|stopPropagation role="dialog" aria-modal="true" aria-label="Run workflow now" use:modal={{ onClose: closeRunNow }}>
      <h3 class="modal-title">Run <span class="mono">{runNowFor.name}</span> now</h3>
      <p class="dim" style="font-size: 0.85em;">
        Pick the jobs to include. All trigger gates are bypassed.
      </p>
      <div class="modal-jobs">
        {#each runNowFor.jobs as j}
          <label class="modal-job">
            <input type="checkbox"
                   checked={runNowSelected.has(j)}
                   on:change={() => toggleJob(j)} />
            <span class="mono">{j}</span>
          </label>
        {/each}
      </div>
      <div class="modal-actions">
        {#if runNowError}
          <span style="color: var(--color-alarm, #b85040); font-size: 0.85em;">{runNowError}</span>
        {/if}
        {#if runNowOk}
          <span style="color: var(--color-phosphor-300, #8fd28f); font-size: 0.85em;">queued: {runNowOk}</span>
        {/if}
        <span style="flex: 1;"></span>
        <button class="action" on:click={closeRunNow} disabled={runNowBusy}>Cancel</button>
        <button class="action"
                on:click={submitRunNow}
                disabled={runNowBusy || runNowSelected.size === 0}>
          {runNowBusy ? "..." : `Run ${runNowSelected.size} job${runNowSelected.size === 1 ? "" : "s"}`}
        </button>
      </div>
    </div>
  </div>
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
    display: inline-flex;
    align-items: center;
    gap: 0.35em;
    font-size: 0.72em;
    font-weight: 500;
    padding: 0.1em 0.55em;
    border: 1px solid var(--border-strong, #2e2e38);
    border-radius: 999px;
    color: var(--fg-muted, #a1a1aa);
    background: var(--surface-2, #16161a);
    margin-left: 0.5rem;
    vertical-align: middle;
  }
  .wf-pill--single {
    color: var(--fg-muted, #a1a1aa);
    border-color: var(--border-strong, #2e2e38);
  }
  /* Streaming = active / live data. Uses the global info palette to
     read as "ongoing process" without the Pip-Boy amber pulse. */
  .wf-pill--streaming {
    color: var(--info, #60a5fa);
    border-color: var(--info, #60a5fa);
    background: var(--info-soft, rgba(96, 165, 250, 0.10));
  }
  .wf-pill--streaming::before {
    content: "";
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: currentColor;
    animation: wf-pulse 1.8s ease-in-out infinite;
  }
  @keyframes wf-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.45; }
  }
  .wf-meta { font-size: 0.78rem; color: var(--color-fg-muted, #888); }
  .wf-trigger {
    font-size: 0.78rem;
    color: var(--fg-muted, #a1a1aa);
    margin: 0.3em 0 0.5em 0;
    padding: 0.5em 0.75em;
    background: var(--surface-2, #16161a);
    border-left: 2px solid var(--accent, #5eead4);
    border-radius: var(--radius, 6px);
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.6em;
  }
  .wf-trigger-label {
    color: var(--fg-subtle, #71717a);
    font-size: 0.85em;
    font-weight: 500;
  }
  .wf-trig {
    display: inline-flex;
    align-items: center;
    gap: 0.4em;
    padding: 0.15em 0.5em;
    border-radius: var(--radius, 6px);
    background: var(--surface-1, #111114);
    color: var(--fg, #e4e4e7);
  }
  .wf-trig-dot {
    width: 0.5em;
    height: 0.5em;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }
  .wf-trig--ready .wf-trig-dot   { background: var(--success, #4ade80); }
  .wf-trig--pending .wf-trig-dot { background: var(--warning, #facc15); }
  .wf-trig--failed .wf-trig-dot  { background: var(--danger, #f87171); }
  .wf-trigger--empty { border-left-color: var(--border-strong, #2e2e38); }
  .wf-run-now {
    margin-left: auto;
    font-size: 0.9em;
  }
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
  .modal-jobs {
    display: flex; flex-direction: column; gap: 0.3em;
    margin: 0.6em 0;
    padding: 0.4em 0;
    border-top: 1px dashed var(--color-border, #444);
    border-bottom: 1px dashed var(--color-border, #444);
  }
  .modal-job { display: flex; align-items: center; gap: 0.5em; cursor: pointer; }
  .modal-actions { display: flex; align-items: center; gap: 0.5em; margin-top: 0.7em; }
  .wf-footer {
    margin-top: 0.4rem;
    padding-top: 0.4rem;
    border-top: 1px solid var(--color-border, #444);
    font-size: 0.85em;
  }
</style>
