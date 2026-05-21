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

  function fmtNextRun(iso) {
    if (!iso) return "—";
    try {
      return new Date(iso).toISOString().replace("T", " ").slice(0, 16) + " UTC";
    } catch (_) {
      return iso;
    }
  }

  function gotoRun(runId) {
    window.location.hash = `#/runs/${encodeURIComponent(runId)}`;
  }

  // Pad recent_runs with placeholder "pending" cells so every strip
  // is exactly 10 squares wide — keeps the visual grid aligned.
  function padRecent(recent) {
    const out = [...(recent || [])];
    while (out.length < 10) {
      out.unshift({ status: "pending", run_id: null });
    }
    return out;
  }
</script>

<h1>Pipelines</h1>

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
    <div class="panel pipeline-card">
      <div class="pipeline-header">
        <h3 class="pipeline-name">{p.name}</h3>
        <span class="pipeline-meta">
          {#if p.kind === "streaming"}
            <span class="streaming-pill">▶ LIVE STREAMING</span>
          {:else if p.next_run_at}
            <span class="next-run">Next: <span class="mono">{fmtNextRun(p.next_run_at)}</span></span>
          {:else}
            <span class="next-run next-run--muted">Next: —</span>
          {/if}
        </span>
      </div>

      <div class="last-10">
        <div class="last-10-label">Last 10 executions</div>
        <div class="last-10-strip">
          {#each padRecent(p.recent_runs) as r}
            {#if r.run_id}
              <button
                type="button"
                class="exec-cell exec-cell--{r.status}"
                title={`${r.status} · ${r.started_at || ""} · ${fmtDuration(r.duration_ms)}`}
                on:click={() => gotoRun(r.run_id)}
              ></button>
            {:else}
              <span class="exec-cell exec-cell--empty" title="no run yet"></span>
            {/if}
          {/each}
        </div>
      </div>

      <div class="pipeline-footer">
        <span><strong>Median duration:</strong> {fmtDuration(p.median_duration_ms)}</span>
        <span><a class="link" href="#/runs?pipeline={encodeURIComponent(p.name)}">all jobs →</a></span>
      </div>
    </div>
  {/each}
{/if}
