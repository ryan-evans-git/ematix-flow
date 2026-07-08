<script>
  import { onMount, onDestroy } from "svelte";
  import DashboardGrid from "../lib/DashboardGrid.svelte";
  import ChartView from "../lib/ChartView.svelte";
  import { me, can } from "../lib/session.js";
  import {
    listDashboards,
    getDashboard,
    createDashboard,
    updateDashboard,
    deleteDashboard,
    queryDashboard,
    listCharts,
  } from "../lib/api.js";

  let dashboards = [];
  let charts = [];
  let currentId = null;
  let currentName = "";
  let tiles = [];
  let tileData = {}; // chart_id -> {name, viz_type, encoding, columns, rows} | {error}
  let editing = false;
  let loading = false;
  let error = "";

  let filters = []; // [{column, values:[...]}]
  let addCol = "";
  let addVal = "";

  let refreshMs = 0;
  let refreshTimer = null;

  onMount(async () => {
    try {
      charts = (await listCharts()).charts || [];
      dashboards = (await listDashboards()).dashboards || [];
      if (dashboards.length) await select(dashboards[0]);
    } catch (e) {
      error = e.message;
    }
  });

  onDestroy(() => clearTimer());

  function clearTimer() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = null;
  }

  $: {
    clearTimer();
    if (refreshMs > 0 && currentId) {
      refreshTimer = setInterval(fetchData, refreshMs);
    }
  }

  async function select(dash) {
    currentId = dash.id;
    currentName = dash.name;
    editing = false;
    filters = [];
    try {
      const full = await getDashboard(dash.id);
      tiles = (full.layout && full.layout.tiles) || [];
      await fetchData();
    } catch (e) {
      error = e.message;
    }
  }

  async function fetchData() {
    if (!currentId) return;
    loading = true;
    try {
      tileData = (await queryDashboard(currentId, filters)).results || {};
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  // Drill-down: clicking a chart point opens a modal with the
  // underlying rows for that category, and offers to cross-filter.
  let drill = null; // {column, value, columns, rows}

  function openDrill(tile, detail) {
    const d = tileData[tile.chart_id];
    if (!d || !d.columns) return;
    const idx = d.columns.findIndex((c) => c.name === detail.column);
    const rows = idx < 0 ? [] : d.rows.filter((r) => String(r[idx]) === String(detail.value));
    drill = { column: detail.column, value: detail.value, columns: d.columns, rows };
  }

  function drillFilter() {
    if (drill) toggleFilter(drill.column, drill.value);
    drill = null;
  }

  // Cross-filter: a click on a chart toggles a {column: value} filter,
  // re-querying every tile.
  function toggleFilter(column, value) {
    const val = String(value);
    const existing = filters.find((f) => f.column === column);
    if (existing) {
      const has = existing.values.map(String).includes(val);
      const values = has ? existing.values.filter((v) => String(v) !== val) : [...existing.values, value];
      filters = values.length
        ? filters.map((f) => (f.column === column ? { ...f, values } : f))
        : filters.filter((f) => f.column !== column);
    } else {
      filters = [...filters, { column, values: [value] }];
    }
    fetchData();
  }

  function addManualFilter() {
    if (!addCol || addVal === "") return;
    toggleFilter(addCol, addVal);
    addVal = "";
  }

  function clearFilters() {
    if (!filters.length) return;
    filters = [];
    fetchData();
  }

  // Columns available to filter on = union of loaded tiles' columns.
  $: filterableColumns = [
    ...new Set(
      Object.values(tileData)
        .flatMap((d) => (d && d.columns ? d.columns.map((c) => c.name) : [])),
    ),
  ];

  async function saveLayout() {
    if (!currentId) return;
    try {
      await updateDashboard(currentId, { layout: { tiles } });
    } catch (e) {
      error = e.message;
    }
  }

  function onGridChange(e) {
    tiles = e.detail;
    saveLayout();
  }

  async function addChart(chart) {
    if (!chart) return;
    const nextY = tiles.reduce((m, t) => Math.max(m, t.y + t.h), 0);
    tiles = [...tiles, { chart_id: chart.id, x: 0, y: nextY, w: 6, h: 6 }];
    await saveLayout();
    await fetchData();
  }

  async function newDashboard() {
    const name = window.prompt("Dashboard name:");
    if (!name) return;
    try {
      const d = await createDashboard({ name });
      dashboards = [d, ...dashboards];
      await select(d);
      editing = true;
    } catch (e) {
      error = e.message;
    }
  }

  async function removeDashboard(dash, ev) {
    ev.stopPropagation();
    if (!window.confirm(`Delete dashboard "${dash.name}"?`)) return;
    try {
      await deleteDashboard(dash.id);
      dashboards = dashboards.filter((d) => d.id !== dash.id);
      if (currentId === dash.id) {
        currentId = null;
        tiles = [];
        tileData = {};
      }
    } catch (e) {
      error = e.message;
    }
  }

  $: usedIds = new Set(tiles.map((t) => t.chart_id));
  $: addableCharts = charts.filter((c) => !usedIds.has(c.id));
</script>

<div class="dash">
  <aside class="sidebar">
    {#if can($me, "write")}
      <div class="side-section">
        <button class="secondary full" on:click={newDashboard}>+ New dashboard</button>
      </div>
    {/if}
    <div class="side-section grow">
      <div class="side-label">Dashboards</div>
      {#if !dashboards.length}
        <div class="muted small">none yet</div>
      {:else}
        <ul class="list">
          {#each dashboards as d}
            <li>
              <button class="row" class:active={d.id === currentId} on:click={() => select(d)}>
                <span class="name">{d.name}</span>
                {#if can($me, "write")}
                  <button class="del" on:click={(e) => removeDashboard(d, e)} title="Delete">×</button>
                {/if}
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  </aside>

  <section class="main">
    {#if !currentId}
      <div class="muted empty">Create a dashboard, then add saved charts to it.</div>
    {:else}
      <div class="toolbar">
        <span class="title">{currentName}</span>
        {#if can($me, "write")}
          <button class="secondary" class:on={editing} on:click={() => (editing = !editing)}>
            {editing ? "Done" : "Edit"}
          </button>
        {/if}
        {#if editing}
          <select class="add" on:change={(e) => { const c = charts.find((x) => x.id === e.target.value); addChart(c); e.target.value = ""; }}>
            <option value="">+ Add chart…</option>
            {#each addableCharts as c}<option value={c.id}>{c.name}</option>{/each}
          </select>
        {/if}
        <span class="spacer"></span>
        <button class="ghost" on:click={fetchData} title="Refresh">⟳</button>
        <select class="refresh" bind:value={refreshMs} title="Auto-refresh">
          <option value={0}>manual</option>
          <option value={10000}>10s</option>
          <option value={30000}>30s</option>
          <option value={60000}>60s</option>
        </select>
      </div>

      <div class="filter-bar">
        <span class="fb-label">Filters</span>
        {#each filters as f}
          {#each f.values as v}
            <button class="fchip" on:click={() => toggleFilter(f.column, v)} title="Remove">
              <span class="fcol">{f.column}</span>=<span class="fval">{v}</span> <span class="fx">×</span>
            </button>
          {/each}
        {/each}
        {#if !filters.length}<span class="muted small">none — click a chart bar/slice to cross-filter</span>{/if}
        <span class="spacer"></span>
        {#if filterableColumns.length}
          <select class="fadd" bind:value={addCol}>
            <option value="">column…</option>
            {#each filterableColumns as c}<option value={c}>{c}</option>{/each}
          </select>
          <input class="fval-in" placeholder="value" bind:value={addVal} on:keydown={(e) => e.key === 'Enter' && addManualFilter()} />
          <button class="ghost" on:click={addManualFilter} disabled={!addCol || addVal === ''}>+ Add</button>
        {/if}
        {#if filters.length}<button class="ghost" on:click={clearFilters}>Clear</button>{/if}
      </div>

      {#if error}<div class="error">⚠ {error}</div>{/if}

      {#if !tiles.length}
        <div class="muted empty">Empty dashboard. Click <b>Edit</b> → <b>Add chart</b> to place saved charts.</div>
      {:else}
        <div class="grid-wrap">
          <DashboardGrid {tiles} editable={editing} on:change={onGridChange}>
            <span slot="title" let:tile>{tileData[tile.chart_id]?.name || (charts.find((c) => c.id === tile.chart_id) || {}).name || "chart"}</span>
            <svelte:fragment let:tile>
              {@const d = tileData[tile.chart_id]}
              <ChartView
                {loading}
                error={d?.error || ""}
                result={d && d.columns ? { columns: d.columns, rows: d.rows } : null}
                vizType={d?.viz_type || "table"}
                encoding={d?.encoding || {}}
                on:filter={(e) => openDrill(tile, e.detail)}
              />
            </svelte:fragment>
          </DashboardGrid>
        </div>
      {/if}
    {/if}
  </section>

  {#if drill}
    <div class="drill-bg" on:click={() => (drill = null)} role="presentation">
      <div class="drill" on:click|stopPropagation role="dialog" aria-label="Drill-down">
        <div class="drill-head">
          <span><b>{drill.column}</b> = <span class="mono">{drill.value}</span> · {drill.rows.length} row{drill.rows.length === 1 ? "" : "s"}</span>
          <button class="drill-x" on:click={() => (drill = null)}>×</button>
        </div>
        <div class="drill-body">
          <table class="grid">
            <thead><tr>{#each drill.columns as c}<th>{c.name}</th>{/each}</tr></thead>
            <tbody>
              {#each drill.rows as row}
                <tr>{#each row as cell}<td class:null={cell === null}>{cell === null ? "NULL" : cell}</td>{/each}</tr>
              {/each}
            </tbody>
          </table>
        </div>
        <div class="drill-foot">
          <button class="secondary" on:click={drillFilter}>Filter dashboard by this</button>
          <button class="ghost" on:click={() => (drill = null)}>Close</button>
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .drill-bg { position: fixed; inset: 0; background: rgba(0,0,0,.5); display: flex; align-items: center; justify-content: center; z-index: 50; }
  .drill { background: var(--surface-1); border: 1px solid var(--border-strong, var(--border)); border-radius: var(--radius-lg, 10px); width: min(720px, 92vw); max-height: 80vh; display: flex; flex-direction: column; overflow: hidden; }
  .drill-head { display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; border-bottom: 1px solid var(--border); font-size: 14px; }
  .drill-head .mono { font-family: var(--font-mono); color: var(--accent); }
  .drill-x { background: none; border: none; color: var(--fg-muted); font-size: 18px; cursor: pointer; }
  .drill-body { overflow: auto; padding: 8px; }
  .drill-foot { display: flex; gap: 8px; justify-content: flex-end; padding: 12px 16px; border-top: 1px solid var(--border); }
  .mono { font-family: var(--font-mono); }
  .drill table.grid { border-collapse: collapse; width: 100%; font-size: 13px; }
  .drill .grid th, .drill .grid td { border-bottom: 1px solid var(--border); border-right: 1px solid var(--border); padding: 5px 10px; text-align: left; white-space: nowrap; }
  .drill .grid thead th { position: sticky; top: 0; background: var(--surface-2); }
  .drill .grid td { font-family: var(--font-mono); }
  .drill .grid td.null { color: var(--fg-faint); font-style: italic; }
  .dash {
    display: grid;
    grid-template-columns: 220px 1fr;
    gap: 12px;
    height: calc(100vh - 60px);
    padding: 12px;
    box-sizing: border-box;
  }
  .sidebar {
    display: flex; flex-direction: column; gap: 14px; min-height: 0;
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: var(--radius, 8px); padding: 12px; overflow: hidden;
  }
  .side-section { display: flex; flex-direction: column; gap: 6px; min-height: 0; }
  .side-section.grow { flex: 1; overflow: auto; }
  .side-label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--fg-muted); }
  .muted { color: var(--fg-muted); }
  .small { font-size: 12px; }

  .list { list-style: none; margin: 0; padding: 0; }
  .row {
    width: 100%; display: flex; align-items: center; gap: 6px;
    background: none; border: none; color: var(--fg); padding: 5px 6px;
    font-size: 13px; cursor: pointer; border-radius: 4px; text-align: left;
  }
  .row:hover { background: var(--hover); }
  .row.active { background: var(--accent-soft); }
  .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .del { margin-left: auto; background: none; border: none; color: var(--fg-muted); cursor: pointer; padding: 0 4px; }
  .del:hover { color: var(--danger); }

  .main { display: flex; flex-direction: column; gap: 10px; min-height: 0; }
  .toolbar { display: flex; align-items: center; gap: 8px; }
  .title { font-weight: 600; font-size: 15px; }
  .spacer { flex: 1; }
  button.secondary, select.add, select.refresh {
    background: var(--surface-2); color: var(--fg);
    border: 1px solid var(--border); border-radius: var(--radius-sm, 5px);
    padding: 6px 12px; cursor: pointer; font-size: 13px;
  }
  button.secondary.full { width: 100%; }
  button.secondary.on { color: #fff; background: var(--accent); border-color: var(--accent); }
  button.ghost { background: none; border: 1px solid var(--border); color: var(--fg-muted); border-radius: var(--radius-sm, 5px); padding: 6px 10px; cursor: pointer; }
  button.ghost:hover { color: var(--fg); }

  .filter-bar {
    display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
    padding: 6px 8px; background: var(--surface-1);
    border: 1px solid var(--border); border-radius: var(--radius-sm, 6px);
  }
  .fb-label { font-size: 11px; text-transform: uppercase; letter-spacing: .05em; color: var(--fg-muted); }
  .fchip {
    display: inline-flex; align-items: center; gap: 3px;
    background: var(--accent-soft); color: var(--fg);
    border: 1px solid var(--accent); border-radius: 999px;
    padding: 2px 9px; font-size: 12px; cursor: pointer; font-family: var(--font-mono);
  }
  .fchip .fcol { color: var(--fg-muted); }
  .fchip .fx { color: var(--fg-muted); }
  .fchip:hover .fx { color: var(--danger); }
  .fadd, .fval-in {
    background: var(--surface-2); color: var(--fg);
    border: 1px solid var(--border); border-radius: var(--radius-sm, 4px);
    padding: 4px 8px; font-size: 12px;
  }
  .fval-in { width: 110px; }

  .grid-wrap { flex: 1; min-height: 0; overflow: auto; padding-right: 4px; }
  .empty { padding: 40px; text-align: center; margin: auto; }
  .error {
    color: var(--danger); background: var(--danger-soft);
    border: 1px solid var(--danger); border-radius: var(--radius-sm, 5px);
    padding: 8px 12px; font-family: var(--font-mono); font-size: 13px;
  }
</style>
