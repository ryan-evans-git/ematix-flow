<script>
  import { onMount } from "svelte";
  import { listQuality, listFreshness } from "../lib/api.js";

  let runs = [];
  let freshness = [];
  let loading = true;
  let error = null;
  let expanded = null; // id of the expanded quality row

  async function load() {
    loading = true;
    error = null;
    try {
      const [q, f] = await Promise.all([listQuality({}), listFreshness({})]);
      runs = q.quality_runs;
      freshness = f.freshness;
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  onMount(load);

  function fmtLag(secs) {
    if (secs == null) return "never";
    if (secs < 90) return `${secs}s`;
    if (secs < 5400) return `${Math.round(secs / 60)}m`;
    if (secs < 172800) return `${(secs / 3600).toFixed(1)}h`;
    return `${(secs / 86400).toFixed(1)}d`;
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    return iso.replace("T", " ").replace(/\.\d+.*$/, "").replace("+00:00", "");
  }

  function toggle(id) {
    expanded = expanded === id ? null : id;
  }
</script>

<div class="wrap">
  <h1>Data Quality</h1>
  <p class="lede">
    Expectations run as a post-write stage on each pipeline; freshness SLOs are
    evaluated on a schedule so a stalled pipeline is caught even when it isn't
    firing.
  </p>

  {#if error}
    <div class="err">{error}</div>
  {:else if loading}
    <div class="muted">Loading…</div>
  {:else}
    <!-- Freshness SLOs -->
    <section>
      <h2>Freshness SLOs</h2>
      {#if freshness.length === 0}
        <div class="muted">No freshness SLOs registered.</div>
      {:else}
        <div class="fresh-grid">
          {#each freshness as f}
            <div class="fresh-card">
              <div class="fresh-top">
                <span class="pill state-{f.state}">{f.state}</span>
                <span class="pipeline">{f.pipeline}</span>
              </div>
              <div class="fresh-meta">
                lag <strong>{fmtLag(f.lag_seconds)}</strong>
                · SLA {fmtLag(f.sla_seconds)}
              </div>
              <div class="fresh-sub">
                last success {fmtTime(f.last_success)}
              </div>
            </div>
          {/each}
        </div>
      {/if}
    </section>

    <!-- Expectation runs -->
    <section>
      <h2>Recent expectation runs</h2>
      {#if runs.length === 0}
        <div class="muted">
          No expectation runs recorded yet. Declare
          <code>expectations=</code> on a pipeline to get started.
        </div>
      {:else}
        <table>
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>Table</th>
              <th>Verdict</th>
              <th>Checks</th>
              <th>Checked</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each runs as r}
              <tr class="row" on:click={() => toggle(r.id)}>
                <td class="mono">{r.pipeline}</td>
                <td class="mono">{r.schema ? `${r.schema}.` : ""}{r.table}</td>
                <td><span class="pill verdict-{r.verdict}">{r.verdict}</span></td>
                <td>
                  {#if r.checks_failed > 0}
                    <span class="fail-count">{r.checks_failed}</span> / {r.checks_total} failed
                  {:else}
                    {r.checks_total} passed
                  {/if}
                </td>
                <td class="mono subtle">{fmtTime(r.finished_at)}</td>
                <td class="chev">{expanded === r.id ? "▾" : "▸"}</td>
              </tr>
              {#if expanded === r.id}
                <tr class="detail-row">
                  <td colspan="6">
                    <div class="assertions">
                      {#each r.assertions as a}
                        <div class="assertion">
                          <span class="pill verdict-{a.verdict} sm">{a.verdict}</span>
                          <span class="mono">{a.name}</span>
                          {#if a.message}<span class="amsg">{a.message}</span>{/if}
                        </div>
                      {/each}
                    </div>
                  </td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      {/if}
    </section>
  {/if}
</div>

<style>
  .wrap { padding: 20px 24px; max-width: 1100px; }
  h1 { font-size: 20px; margin: 0 0 4px; }
  .lede { color: var(--fg-muted); font-size: 13px; margin: 0 0 20px; max-width: 68ch; }
  h2 { font-size: 14px; color: var(--fg-muted); font-weight: 600; margin: 24px 0 10px; }
  .muted { color: var(--fg-subtle); font-size: 13px; }
  .err { color: var(--danger); background: var(--danger-soft); padding: 10px 14px; border-radius: 8px; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12.5px; }
  .subtle { color: var(--fg-subtle); }

  .pill {
    display: inline-block; text-transform: uppercase; letter-spacing: 0.04em;
    font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 999px;
    border: 1px solid var(--border-strong);
  }
  .pill.sm { font-size: 9px; padding: 1px 6px; }
  .verdict-pass, .state-healthy { color: var(--success); background: var(--success-soft); border-color: var(--success); }
  .verdict-warn, .state-warning { color: var(--warning); background: var(--warning-soft); border-color: var(--warning); }
  .verdict-fail, .state-breached { color: var(--danger); background: var(--danger-soft); border-color: var(--danger); }
  .verdict-error { color: var(--fg-muted); }
  .state-unknown { color: var(--fg-subtle); }

  .fresh-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
  .fresh-card { border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; background: var(--surface-1); }
  .fresh-top { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
  .fresh-top .pipeline { font-family: ui-monospace, monospace; font-size: 12.5px; }
  .fresh-meta { font-size: 13px; }
  .fresh-sub { color: var(--fg-subtle); font-size: 11.5px; margin-top: 4px; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; color: var(--fg-subtle); font-weight: 600; font-size: 11px;
       text-transform: uppercase; letter-spacing: 0.04em; padding: 6px 10px; border-bottom: 1px solid var(--border); }
  td { padding: 8px 10px; border-bottom: 1px solid var(--border); }
  tr.row { cursor: pointer; }
  tr.row:hover td { background: var(--surface-2); }
  .fail-count { color: var(--danger); font-weight: 700; }
  .chev { color: var(--fg-subtle); width: 1em; }
  .detail-row td { background: var(--surface-2); }
  .assertions { display: flex; flex-direction: column; gap: 6px; padding: 4px 2px; }
  .assertion { display: flex; align-items: center; gap: 10px; }
  .amsg { color: var(--fg-muted); font-size: 12px; }
</style>
