<script>
  import { onMount } from "svelte";
  import { listRuns } from "../lib/api.js";

  let runs = [];
  let total = 0;
  let loading = true;
  let error = null;
  let pipelineFilter = "";
  let statusFilter = "";

  async function load() {
    loading = true;
    error = null;
    try {
      const body = await listRuns({
        pipeline: pipelineFilter || undefined,
        status: statusFilter || undefined,
      });
      runs = body.runs;
      total = body.total;
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  onMount(load);

  function fmtDuration(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    // Render as local-time, brief.
    try {
      return new Date(iso).toISOString().replace("T", " ").slice(0, 19);
    } catch (_) {
      return iso;
    }
  }
</script>

<h1>Jobs</h1>

<div class="panel">
  <div style="display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;">
    <label>
      Pipeline:
      <input
        type="text"
        bind:value={pipelineFilter}
        on:change={load}
        placeholder="(any)"
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);"
      />
    </label>
    <label>
      Status:
      <select
        bind:value={statusFilter}
        on:change={load}
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
    <span class="mono" style="margin-left: auto;">total: {total}</span>
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
        <th>Pipeline</th>
        <th>Status</th>
        <th>Started</th>
        <th>Duration</th>
        <th>Attempt</th>
        <th>Run ID</th>
      </tr>
    </thead>
    <tbody>
      {#each runs as r (r.run_id)}
        <tr on:click={() => (window.location.hash = `#/runs/${encodeURIComponent(r.run_id)}`)}>
          <td>{r.pipeline}</td>
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
