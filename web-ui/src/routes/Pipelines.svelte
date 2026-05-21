<script>
  import { onMount } from "svelte";
  import { listPipelines } from "../lib/api.js";

  let pipelines = [];
  let loading = true;
  let error = null;

  // --- filter + sort state ---
  let nameFilter = "";
  let kindFilter = "";            // "" | "batch" | "streaming"
  let statusFilter = "";          // "" | "running" | "succeeded" | "failed" | ...
  let sortKey = "name";           // name | kind | status | next | median
  let sortDir = "asc";            // asc | desc

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

  // --- visible = filtered + sorted ---
  $: visible = (() => {
    const nf = nameFilter.trim().toLowerCase();
    let out = pipelines.filter((p) => {
      if (nf && !p.name.toLowerCase().includes(nf)) return false;
      if (kindFilter && p.kind !== kindFilter) return false;
      if (statusFilter) {
        const s = p.latest_run?.status || "";
        if (s !== statusFilter) return false;
      }
      return true;
    });
    const dir = sortDir === "asc" ? 1 : -1;
    out = out.slice().sort((a, b) => {
      let av, bv;
      switch (sortKey) {
        case "kind":   av = a.kind || ""; bv = b.kind || ""; break;
        case "status": av = a.latest_run?.status || ""; bv = b.latest_run?.status || ""; break;
        case "next":   av = a.next_run_at || ""; bv = b.next_run_at || ""; break;
        case "median": av = a.median_duration_ms ?? -1; bv = b.median_duration_ms ?? -1; break;
        case "name":
        default:       av = a.name; bv = b.name;
      }
      if (av < bv) return -1 * dir;
      if (av > bv) return  1 * dir;
      return 0;
    });
    return out;
  })();

  function setSort(key) {
    if (sortKey === key) {
      sortDir = sortDir === "asc" ? "desc" : "asc";
    } else {
      sortKey = key;
      sortDir = "asc";
    }
  }
  function arrow(key) {
    if (sortKey !== key) return " ";
    return sortDir === "asc" ? "▲" : "▼";
  }

  function fmtDuration(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  }

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
  function gotoDag(name) {
    window.location.hash = `#/dag/${encodeURIComponent(name)}`;
  }

  function padRecent(recent) {
    const out = [...(recent || [])];
    while (out.length < 10) {
      out.unshift({ status: "pending", run_id: null });
    }
    return out;
  }

  const _MIN_H = 8;
  const _MAX_H = 28;
  function cellHeight(durationMs, maxDurationMs) {
    if (durationMs == null || maxDurationMs == null || maxDurationMs <= 0) {
      return _MIN_H;
    }
    const r = Math.sqrt(Math.max(durationMs, 0) / maxDurationMs);
    return Math.round(_MIN_H + (_MAX_H - _MIN_H) * Math.min(r, 1));
  }
  function stripMaxDuration(recent) {
    let m = 0;
    for (const r of recent || []) {
      if (typeof r.duration_ms === "number" && r.duration_ms > m) {
        m = r.duration_ms;
      }
    }
    return m > 0 ? m : null;
  }
</script>

<h1>Jobs</h1>
<p class="dim" style="margin-top: -0.5rem;">
  Individual jobs across all workflows. For named workflow groupings,
  see <a class="link" href="#/workflows">Workflows</a>.
</p>

<div class="panel">
  <div style="display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;">
    <label>
      Name:
      <input
        type="text"
        bind:value={nameFilter}
        placeholder="(any)"
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono); width: 14ch;"
      />
    </label>
    <label>
      Kind:
      <select bind:value={kindFilter}
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);">
        <option value="">(any)</option>
        <option value="batch">batch</option>
        <option value="streaming">streaming</option>
      </select>
    </label>
    <label>
      Latest status:
      <select bind:value={statusFilter}
        style="background: rgba(8,16,12,0.7); border: 1px solid var(--color-phosphor-700); color: var(--color-phosphor-200); padding: 0.25rem 0.5rem; font-family: var(--font-mono);">
        <option value="">(any)</option>
        <option value="running">running</option>
        <option value="succeeded">succeeded</option>
        <option value="failed">failed</option>
        <option value="paused">paused</option>
        <option value="cancelled">cancelled</option>
      </select>
    </label>
    <span style="margin-left: auto; display: flex; gap: 0.4rem; align-items: center;">
      <span class="dim">sort:</span>
      <button class="action" on:click={() => setSort("name")}>name {arrow("name")}</button>
      <button class="action" on:click={() => setSort("kind")}>kind {arrow("kind")}</button>
      <button class="action" on:click={() => setSort("status")}>status {arrow("status")}</button>
      <button class="action" on:click={() => setSort("next")}>next {arrow("next")}</button>
      <button class="action" on:click={() => setSort("median")}>duration {arrow("median")}</button>
    </span>
    <button class="action" on:click={load}>Refresh</button>
    <span class="mono" style="margin-left: 0.5rem;">total: {visible.length}/{pipelines.length}</span>
  </div>
</div>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if visible.length === 0}
  <div class="empty">no pipelines match the current filter</div>
{:else}
  {#each visible as p}
    {@const _maxDur = stripMaxDuration(p.recent_runs)}
    <div class="panel pipeline-card">
      <div class="pipeline-header">
        <h3 class="pipeline-name">
          <a class="link" href={`#/dag/${encodeURIComponent(p.name)}`}>{p.name}</a>
        </h3>
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
        <div class="last-10-label">Last 10 executions <span class="dim">(bar height ∝ duration)</span></div>
        <div class="last-10-strip">
          {#each padRecent(p.recent_runs) as r}
            {#if r.run_id}
              <button
                type="button"
                class="exec-cell exec-cell--{r.status}"
                style="height: {cellHeight(r.duration_ms, _maxDur)}px"
                title={`${r.status} · ${r.started_at || ""} · ${fmtDuration(r.duration_ms)}`}
                on:click={() => gotoRun(r.run_id)}
              ></button>
            {:else}
              <span
                class="exec-cell exec-cell--empty"
                style="height: {_MIN_H}px"
                title="no run yet"
              ></span>
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
        <span>
          <a class="link" href={`#/dag/${encodeURIComponent(p.name)}`}>dag →</a>
          &nbsp;
          <a class="link" href="#/runs?pipeline={encodeURIComponent(p.name)}">all jobs →</a>
        </span>
      </div>
    </div>
  {/each}
{/if}
