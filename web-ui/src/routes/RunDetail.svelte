<script>
  import { onMount } from "svelte";
  import { getRun, restartRun, rerunRun, pauseRun, resumeRun } from "../lib/api.js";

  export let runId;

  let run = null;
  let loading = true;
  let error = null;
  let toast = null;
  let toastError = false;

  async function load() {
    loading = true;
    error = null;
    try {
      run = await getRun(runId);
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  $: if (runId) load();

  function showToast(msg, isError = false) {
    toast = msg;
    toastError = isError;
    setTimeout(() => { toast = null; }, 3500);
  }

  async function doRestart() {
    if (!run?.actions?.restart_from_step?.length) return;
    const step = run.actions.restart_from_step[0];
    if (!confirm(`Restart run ${runId} from step "${step}"?`)) return;
    try {
      const r = await restartRun(runId, step);
      showToast(`Enqueued — new run ${r.new_run_id}`);
      window.location.hash = `#/runs/${encodeURIComponent(r.new_run_id)}`;
    } catch (e) {
      showToast(`Restart failed: ${e.message}`, true);
    }
  }

  async function doResumeFromWatermark() {
    if (!confirm(`Resume run ${runId} from last committed watermark?`)) return;
    try {
      const r = await restartRun(runId, null);
      showToast(`Enqueued — new run ${r.new_run_id}`);
      window.location.hash = `#/runs/${encodeURIComponent(r.new_run_id)}`;
    } catch (e) {
      showToast(`Resume failed: ${e.message}`, true);
    }
  }

  async function doRerun() {
    if (!confirm(`Rerun ${runId} from the beginning?`)) return;
    try {
      const r = await rerunRun(runId);
      showToast(`Enqueued — new run ${r.new_run_id}`);
      window.location.hash = `#/runs/${encodeURIComponent(r.new_run_id)}`;
    } catch (e) {
      showToast(`Rerun failed: ${e.message}`, true);
    }
  }

  async function doPause() {
    try {
      await pauseRun(runId);
      showToast(`Pause requested — will hold at next boundary`);
      load();
    } catch (e) {
      showToast(`Pause failed: ${e.message}`, true);
    }
  }

  async function doResume() {
    try {
      await resumeRun(runId);
      showToast(`Resume requested`);
      load();
    } catch (e) {
      showToast(`Resume failed: ${e.message}`, true);
    }
  }

  // ---- DAG layout ---------------------------------------------------
  //
  // Lay steps out in layers by longest-path rank so a node sits to
  // the right of every dependency. Sibling steps in the same layer
  // share an x and stack vertically — that's where parallelism is
  // visible.
  const NODE_W = 168;
  const NODE_H = 44;
  const COL_GAP = 64;
  const ROW_GAP = 18;
  const PAD_X = 12;
  const PAD_Y = 12;

  function layoutDag(steps) {
    if (!steps || steps.length === 0) return null;
    const byName = new Map(steps.map((s) => [s.name, s]));
    const rank = new Map();
    function rankOf(name, seen = new Set()) {
      if (rank.has(name)) return rank.get(name);
      if (seen.has(name)) return 0; // cycle guard
      seen.add(name);
      const s = byName.get(name);
      const deps = (s?.depends_on || []).filter((d) => byName.has(d));
      const r = deps.length === 0 ? 0 : 1 + Math.max(...deps.map((d) => rankOf(d, seen)));
      rank.set(name, r);
      return r;
    }
    for (const s of steps) rankOf(s.name);

    // Group by rank → sorted columns of nodes.
    const cols = new Map();
    for (const s of steps) {
      const r = rank.get(s.name) || 0;
      if (!cols.has(r)) cols.set(r, []);
      cols.get(r).push(s);
    }
    const ranks = [...cols.keys()].sort((a, b) => a - b);

    // Position nodes: x by rank, y by index within rank (centered).
    const maxRows = Math.max(...ranks.map((r) => cols.get(r).length));
    const nodes = [];
    for (const r of ranks) {
      const col = cols.get(r);
      const colH = col.length * NODE_H + (col.length - 1) * ROW_GAP;
      const fullH = maxRows * NODE_H + (maxRows - 1) * ROW_GAP;
      const yOffset = (fullH - colH) / 2;
      col.forEach((s, i) => {
        nodes.push({
          ...s,
          x: PAD_X + r * (NODE_W + COL_GAP),
          y: PAD_Y + yOffset + i * (NODE_H + ROW_GAP),
        });
      });
    }

    const edges = [];
    const nodeByName = new Map(nodes.map((n) => [n.name, n]));
    for (const n of nodes) {
      for (const d of n.depends_on || []) {
        const src = nodeByName.get(d);
        if (!src) continue;
        edges.push({
          x1: src.x + NODE_W,
          y1: src.y + NODE_H / 2,
          x2: n.x,
          y2: n.y + NODE_H / 2,
        });
      }
    }

    const width = PAD_X * 2 + ranks.length * NODE_W + (ranks.length - 1) * COL_GAP;
    const height = PAD_Y * 2 + maxRows * NODE_H + (maxRows - 1) * ROW_GAP;
    return { nodes, edges, width, height };
  }

  function edgePath(e) {
    // Smooth horizontal bezier between two columns.
    const mx = (e.x1 + e.x2) / 2;
    return `M ${e.x1} ${e.y1} C ${mx} ${e.y1}, ${mx} ${e.y2}, ${e.x2} ${e.y2}`;
  }

  $: dag = layoutDag(run?.steps);
</script>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if !run}
  <div class="empty">run not found</div>
{:else}
  <h1>{run.pipeline}</h1>
  <p>
    <span class="status status--{run.status}">{run.status}</span>
    &middot; <span class="mono">{run.run_id}</span>
    &middot; attempt {run.attempt}
  </p>

  <div class="actions">
    {#if run.actions.pause}
      <button class="action" on:click={doPause}>Pause</button>
    {/if}
    {#if run.actions.resume}
      <button class="action" on:click={doResume}>Resume</button>
    {/if}
    {#if run.actions.restart_from_step?.length}
      <button class="action" on:click={doRestart}>
        Restart from step "{run.actions.restart_from_step[0]}"
      </button>
    {/if}
    {#if run.actions.resume_from_watermark}
      <button class="action" on:click={doResumeFromWatermark}>
        Resume from watermark
      </button>
    {/if}
    {#if run.actions.rerun_full}
      <button class="action action--danger" on:click={doRerun}>
        Rerun from beginning
      </button>
    {/if}
  </div>

  <div class="divider">▸ Timeline</div>
  <div class="panel">
    <p><strong>Started:</strong> <span class="mono">{run.started_at || "—"}</span></p>
    <p><strong>Finished:</strong> <span class="mono">{run.finished_at || "(still running)"}</span></p>
    {#if run.failed_step}
      <p><strong>Failed step:</strong> <span class="mono">{run.failed_step}</span></p>
    {/if}
    {#if run.failed_watermark}
      <p><strong>Last watermark:</strong> <span class="mono">{run.failed_watermark}</span></p>
    {/if}
    {#if run.error_summary}
      <p><strong>Error:</strong> <span style="color: var(--color-alarm);">{run.error_summary}</span></p>
    {/if}
  </div>

  {#if run.attempts?.length}
    <div class="divider">▸ Attempts</div>
    {#each run.attempts as att}
      <div class="panel">
        <p>
          <strong>Attempt {att.attempt}</strong>
          &middot; <span class="status status--{att.status}">{att.status}</span>
          &middot; <span class="mono">{att.started_at || "—"}</span>
        </p>
        {#if att.error_summary}
          <pre style="color: var(--color-alarm); white-space: pre-wrap;">{att.error_summary}</pre>
        {/if}
      </div>
    {/each}
  {/if}

  {#if dag}
    <div class="divider">▸ Task DAG</div>
    <div class="panel" style="overflow-x: auto;">
      <svg
        class="dag"
        viewBox="0 0 {dag.width} {dag.height}"
        width={dag.width}
        height={dag.height}
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5"
                  markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" />
          </marker>
        </defs>
        {#each dag.edges as e}
          <path d={edgePath(e)} class="dag-edge" marker-end="url(#arrow)" />
        {/each}
        {#each dag.nodes as n}
          <g class="dag-node dag-node--{n.status}" transform="translate({n.x},{n.y})">
            <rect width={NODE_W} height={NODE_H} rx="4" />
            <text x={NODE_W / 2} y={NODE_H / 2 - 4} text-anchor="middle" class="dag-name">
              {n.name}
            </text>
            <text x={NODE_W / 2} y={NODE_H / 2 + 12} text-anchor="middle" class="dag-status">
              {n.status}{n.duration_ms != null ? ` · ${(n.duration_ms / 1000).toFixed(1)}s` : ""}
            </text>
          </g>
        {/each}
      </svg>
      <div class="dag-legend">
        <span class="legend--succeeded">▣ done</span>
        <span class="legend--running">▣ running</span>
        <span class="legend--pending">▣ pending</span>
        <span class="legend--failed">▣ failed</span>
      </div>
    </div>
  {/if}

  {#if run.steps?.length}
    <div class="divider">▸ Steps</div>
    <table class="runs">
      <thead>
        <tr><th>Step</th><th>Status</th><th>Duration</th></tr>
      </thead>
      <tbody>
        {#each run.steps as s}
          <tr>
            <td>{s.name}</td>
            <td><span class="status status--{s.status}">{s.status}</span></td>
            <td>{s.duration_ms != null ? `${(s.duration_ms / 1000).toFixed(2)}s` : "—"}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
{/if}

{#if toast}
  <div class="toast {toastError ? 'toast--error' : ''}">{toast}</div>
{/if}
