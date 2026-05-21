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

  // v0.5.0: streaming pipelines surface throughput + batch cycle
  // instead of a meaningless median over the single "still running"
  // record. Snapshots come from the daemon every ~30s via the
  // streaming-stats recorder.
  function fmtRate(rps) {
    if (rps == null) return "—";
    if (rps >= 1000) return `${(rps / 1000).toFixed(1)}k rps`;
    if (rps >= 10) return `${rps.toFixed(0)} rps`;
    return `${rps.toFixed(2)} rps`;
  }
  function fmtCycle(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms.toFixed(0)} ms`;
    return `${(ms / 1000).toFixed(2)} s`;
  }
  function fmtSnapshotAge(iso) {
    if (!iso) return "no snapshot yet";
    const age = (Date.now() - new Date(iso).getTime()) / 1000;
    if (age < 0) return "just now";
    if (age < 90) return `${Math.round(age)}s ago`;
    return `${Math.round(age / 60)}m ago`;
  }

  function fmtNextRun(iso, tz) {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      if (tz) {
        // Pipeline-local rendering via Intl. Format "YYYY-MM-DD HH:MM TZ"
        // so the column stays aligned with the UTC fallback. Tag with the
        // short zone name ("EDT" / "PST") so the user sees the offset.
        const dt = new Intl.DateTimeFormat("en-CA", {
          timeZone: tz,
          year: "numeric", month: "2-digit", day: "2-digit",
          hour: "2-digit", minute: "2-digit", hour12: false,
        }).format(d).replace(",", "");
        const zone = new Intl.DateTimeFormat("en-US", {
          timeZone: tz, timeZoneName: "short",
        }).formatToParts(d).find(p => p.type === "timeZoneName")?.value || tz;
        return `${dt} ${zone}`;
      }
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
            <span class="next-run">Next: <span class="mono">{fmtNextRun(p.next_run_at, p.timezone)}</span></span>
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
        {#if p.kind === "streaming"}
          {@const s1 = p.streaming_stats?.stats_1m}
          {@const s5 = p.streaming_stats?.stats_5m}
          <span class="streaming-stats">
            <strong>Throughput:</strong>
            <span class="mono">{fmtRate(s1?.rows_consumed_per_sec)}</span>
            <span class="dim">in (1m)</span>
            <span class="sep">/</span>
            <span class="mono">{fmtRate(s5?.rows_consumed_per_sec)}</span>
            <span class="dim">in (5m)</span>
            <span class="sep">·</span>
            <strong>Batch cycle:</strong>
            <span class="mono">{fmtCycle(s1?.avg_batch_cycle_ms)}</span>
            <span class="dim">avg (1m)</span>
            <span class="sep">·</span>
            <span class="dim" title={p.streaming_stats?.snapshot_at || ""}>
              {fmtSnapshotAge(p.streaming_stats?.snapshot_at)}
            </span>
          </span>
        {:else}
          <span><strong>Median duration:</strong> {fmtDuration(p.median_duration_ms)}</span>
        {/if}
        <span><a class="link" href="#/runs?pipeline={encodeURIComponent(p.name)}">all jobs →</a></span>
      </div>
    </div>
  {/each}
{/if}
