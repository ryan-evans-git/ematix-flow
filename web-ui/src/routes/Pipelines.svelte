<script>
  import { onMount } from "svelte";
  import { listPipelines } from "../lib/api.js";

  let pipelines = [];
  let loading = true;
  let error = null;

  async function load() {
    loading = true;
    error = null;
    try {
      const body = await listPipelines();
      pipelines = body.pipelines;
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

  function fmtPct(x) {
    if (x == null) return "—";
    return `${(x * 100).toFixed(1)}%`;
  }
</script>

<h1>Pipeline summary</h1>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if pipelines.length === 0}
  <div class="empty">no pipelines registered yet</div>
{:else}
  {#each pipelines as p}
    <div
      class="panel panel--hover"
      on:click={() => (window.location.hash = `#/runs?pipeline=${encodeURIComponent(p.name)}`)}
      on:keydown={(e) => e.key === "Enter" && (window.location.hash = `#/runs?pipeline=${encodeURIComponent(p.name)}`)}
      role="button"
      tabindex="0"
    >
      <h3>{p.name}</h3>
      <p>
        <strong>Kind:</strong> <span class="mono">{p.kind}</span>
      </p>
      {#if p.latest_run}
        <p>
          <strong>Latest run:</strong>
          <span class="status status--{p.latest_run.status}">{p.latest_run.status}</span>
          &middot; <span class="mono">{p.latest_run.started_at}</span>
        </p>
      {/if}
      <p>
        <strong>Failure rate (7d):</strong> {fmtPct(p.failure_rate_7d)}
        &middot;
        <strong>Median duration:</strong> {fmtDuration(p.median_duration_ms)}
      </p>
    </div>
  {/each}
{/if}
