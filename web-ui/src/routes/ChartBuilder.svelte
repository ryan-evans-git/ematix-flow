<script>
  import { onMount } from "svelte";
  import SqlEditor from "../lib/SqlEditor.svelte";
  import EChart from "../lib/EChart.svelte";
  import VisualQueryBuilder from "../lib/VisualQueryBuilder.svelte";
  import { me, can } from "../lib/session.js";
  import {
    VIZ_TYPES,
    isNumericType,
    defaultEncoding,
    bigNumberValue,
    buildEChartsOption,
  } from "../lib/charts.js";
  import {
    listDatasources,
    runQuery,
    listCharts,
    createChart,
    updateChart,
    deleteChart,
  } from "../lib/api.js";

  let datasources = [];
  let datasourceId = "";
  let editor;
  let sql = "SELECT region, SUM(amount) AS total\nFROM sales\nGROUP BY region\nORDER BY total DESC";
  let maxRows = 1000;

  let running = false;
  let result = null;
  let error = "";

  let vizType = "bar";
  let encoding = {};
  let mode = "sql"; // "sql" | "build" (no-SQL query builder)

  function onGenerated(e) {
    if (editor) editor.setDoc(e.detail);
    sql = e.detail;
    mode = "sql";
    run();
  }

  let charts = [];
  let currentId = null;
  let currentName = "";

  onMount(async () => {
    try {
      const ds = (await listDatasources()).datasources || [];
      datasources = ds;
      if (ds.length) datasourceId = ds[0].id;
    } catch (e) {
      error = e.message;
    }
    await refreshCharts();
  });

  $: columns = result ? result.columns : [];
  $: numericCols = columns.filter((c) => isNumericType(c.type)).map((c) => c.name);
  $: allCols = columns.map((c) => c.name);
  $: option =
    result && !["table", "number"].includes(vizType)
      ? buildEChartsOption(vizType, result.columns, result.rows, encoding)
      : null;
  $: numberVal = result && vizType === "number" ? bigNumberValue(result.columns, result.rows, encoding) : null;

  async function run() {
    error = "";
    running = true;
    const text = editor ? editor.getDoc() : sql;
    try {
      result = await runQuery({ datasourceId, sql: text, maxRows: Number(maxRows) || undefined });
      // Seed a sensible encoding on first result / when columns change.
      encoding = defaultEncoding(vizType, result.columns);
    } catch (e) {
      error = e.message;
      result = null;
    } finally {
      running = false;
    }
  }

  function pickViz(v) {
    vizType = v;
    if (result) encoding = defaultEncoding(v, result.columns);
  }

  function toggleY(name) {
    const y = new Set(encoding.y || []);
    y.has(name) ? y.delete(name) : y.add(name);
    encoding = { ...encoding, y: [...y] };
  }

  async function refreshCharts() {
    try {
      charts = (await listCharts()).charts || [];
    } catch (_) {
      /* non-fatal */
    }
  }

  async function save() {
    const text = editor ? editor.getDoc() : sql;
    // Prompt for a name only if the name field is empty.
    if (!currentName.trim()) {
      const name = window.prompt("Name this chart:", "");
      if (!name) return;
      currentName = name;
    }
    try {
      if (currentId) {
        await updateChart(currentId, { name: currentName, datasourceId, sql: text, vizType, encoding });
      } else {
        const c = await createChart({ name: currentName, datasourceId, sql: text, vizType, encoding });
        currentId = c.id;
      }
      await refreshCharts();
    } catch (e) {
      error = e.message;
    }
  }

  function newChart() {
    currentId = null;
    currentName = "";
  }

  async function loadChart(c) {
    currentId = c.id;
    currentName = c.name;
    if (c.datasource_id && datasources.some((d) => d.id === c.datasource_id)) {
      datasourceId = c.datasource_id;
    }
    vizType = c.viz_type;
    if (editor) editor.setDoc(c.sql);
    sql = c.sql;
    await run();
    // run() reseeds encoding from columns; restore the saved encoding.
    encoding = c.encoding || encoding;
  }

  async function removeChart(c, ev) {
    ev.stopPropagation();
    if (!window.confirm(`Delete chart "${c.name}"?`)) return;
    try {
      await deleteChart(c.id);
      if (currentId === c.id) newChart();
      await refreshCharts();
    } catch (e) {
      error = e.message;
    }
  }
</script>

<div class="cb">
  <aside class="sidebar">
    <div class="side-section">
      <label class="side-label" for="cds">Data source</label>
      <select id="cds" bind:value={datasourceId}>
        {#each datasources as d}
          <option value={d.id}>{d.id} · {d.dialect}</option>
        {/each}
      </select>
    </div>
    <div class="side-section">
      <button class="secondary full" on:click={newChart}>+ New chart</button>
    </div>
    <div class="side-section grow">
      <div class="side-label">Saved charts</div>
      {#if !charts.length}
        <div class="muted small">none yet</div>
      {:else}
        <ul class="saved">
          {#each charts as c}
            <li>
              <button class="saved-row" class:active={c.id === currentId} on:click={() => loadChart(c)}>
                <span class="viz-dot">{(VIZ_TYPES.find((v) => v.id === c.viz_type) || {}).icon || "▦"}</span>
                <span class="saved-name">{c.name}</span>
                <button class="del" on:click={(e) => removeChart(c, e)} title="Delete">×</button>
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  </aside>

  <section class="main">
    <div class="toolbar">
      <button class="run" on:click={run} disabled={running || !datasourceId || !can($me, "query")} title={can($me, "query") ? "" : "Requires the query permission"}>
        {running ? "Running…" : "▶ Run"} <kbd>⌘⏎</kbd>
      </button>
      <div class="mode">
        <button class:on={mode === "sql"} on:click={() => (mode = "sql")}>SQL</button>
        <button class:on={mode === "build"} on:click={() => (mode = "build")}>Build</button>
      </div>
      <input class="chart-name" placeholder="Untitled chart" bind:value={currentName} />
      <button class="secondary" on:click={save} disabled={!result || !can($me, "write")} title={can($me, "write") ? "" : "Requires the write permission"}>{currentId ? "Update" : "Save"}</button>
      <span class="spacer"></span>
      <label class="rows">Limit <input type="number" min="1" max="100000" bind:value={maxRows} /></label>
    </div>

    {#if mode === "build"}
      <VisualQueryBuilder {datasourceId} on:generate={onGenerated} />
    {/if}
    <div class="editor-wrap">
      <SqlEditor bind:this={editor} bind:value={sql} onRun={run} />
    </div>

    {#if error}
      <div class="error">⚠ {error}</div>
    {/if}

    <div class="viz-bar">
      {#each VIZ_TYPES as v}
        <button class="viz-tab" class:active={vizType === v.id} on:click={() => pickViz(v.id)} title={v.label}>
          <span class="ico">{v.icon}</span>{v.label}
        </button>
      {/each}
    </div>

    {#if result}
      <div class="encoding">
        {#if vizType === "bar" || vizType === "line" || vizType === "area"}
          <div class="enc-field">
            <label>Category (X)</label>
            <select bind:value={encoding.x}>
              {#each allCols as c}<option value={c}>{c}</option>{/each}
            </select>
          </div>
          <div class="enc-field grow">
            <label>Metrics (Y)</label>
            <div class="chips">
              {#each numericCols as c}
                <button class="chip" class:on={(encoding.y || []).includes(c)} on:click={() => toggleY(c)}>{c}</button>
              {/each}
              {#if !numericCols.length}<span class="muted small">no numeric columns</span>{/if}
            </div>
          </div>
        {:else if vizType === "pie"}
          <div class="enc-field">
            <label>Label</label>
            <select bind:value={encoding.name}>{#each allCols as c}<option value={c}>{c}</option>{/each}</select>
          </div>
          <div class="enc-field">
            <label>Value</label>
            <select bind:value={encoding.value}>{#each numericCols as c}<option value={c}>{c}</option>{/each}</select>
          </div>
        {:else if vizType === "scatter"}
          <div class="enc-field">
            <label>X</label>
            <select bind:value={encoding.x}>{#each numericCols as c}<option value={c}>{c}</option>{/each}</select>
          </div>
          <div class="enc-field">
            <label>Y</label>
            <select value={(encoding.y || [])[0]} on:change={(e) => (encoding = { ...encoding, y: [e.target.value] })}>
              {#each numericCols as c}<option value={c}>{c}</option>{/each}
            </select>
          </div>
        {:else if vizType === "number"}
          <div class="enc-field">
            <label>Value (summed)</label>
            <select bind:value={encoding.value}>{#each numericCols as c}<option value={c}>{c}</option>{/each}</select>
          </div>
        {:else}
          <span class="muted small">Table view — no encoding needed.</span>
        {/if}
      </div>
    {/if}

    <div class="preview">
      {#if !result}
        <div class="muted empty">Run a query, then pick a chart type.</div>
      {:else if vizType === "table"}
        <div class="grid-scroll">
          <table class="grid">
            <thead><tr>{#each result.columns as c}<th>{c.name}</th>{/each}</tr></thead>
            <tbody>
              {#each result.rows as row}
                <tr>{#each row as cell}<td class:null={cell === null}>{cell === null ? "NULL" : cell}</td>{/each}</tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else if vizType === "number"}
        <div class="bignum">
          <div class="bignum-val">{numberVal === null ? "—" : numberVal.toLocaleString?.() ?? numberVal}</div>
          <div class="bignum-label">{encoding.value || ""}</div>
        </div>
      {:else if option}
        {#key vizType + JSON.stringify(encoding)}
          <EChart {option} />
        {/key}
      {:else}
        <div class="muted empty">Choose columns above to render the {vizType} chart.</div>
      {/if}
    </div>
  </section>
</div>

<style>
  .cb {
    display: grid;
    grid-template-columns: 240px 1fr;
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
  select, input[type="number"], .chart-name {
    background: var(--surface-2); color: var(--fg);
    border: 1px solid var(--border); border-radius: var(--radius-sm, 4px);
    padding: 5px 8px; font-size: 13px;
  }
  .muted { color: var(--fg-muted); }
  .small { font-size: 12px; }

  .saved { list-style: none; margin: 0; padding: 0; }
  .saved-row {
    width: 100%; display: flex; align-items: center; gap: 6px;
    background: none; border: none; color: var(--fg); padding: 4px 6px;
    font-size: 13px; cursor: pointer; border-radius: 4px; text-align: left;
  }
  .saved-row:hover { background: var(--hover); }
  .saved-row.active { background: var(--accent-soft); }
  .viz-dot { color: var(--accent); width: 14px; text-align: center; }
  .saved-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .del { margin-left: auto; background: none; border: none; color: var(--fg-muted); cursor: pointer; padding: 0 4px; }
  .del:hover { color: var(--danger); }

  .main { display: flex; flex-direction: column; gap: 10px; min-height: 0; }
  .toolbar { display: flex; align-items: center; gap: 8px; }
  .spacer { flex: 1; }
  .chart-name { width: 200px; }
  button.run { background: var(--accent); color: #fff; border: none; border-radius: var(--radius-sm, 5px); padding: 7px 14px; font-weight: 600; cursor: pointer; }
  button.run:disabled { opacity: .5; cursor: default; }
  button.secondary { background: var(--surface-2); color: var(--fg); border: 1px solid var(--border); border-radius: var(--radius-sm, 5px); padding: 7px 12px; cursor: pointer; }
  button.secondary.full { width: 100%; }
  button.secondary:disabled { opacity: .5; cursor: default; }
  kbd { font-family: var(--font-mono); font-size: 11px; background: rgba(255,255,255,.08); border-radius: 3px; padding: 1px 4px; }
  .rows { font-size: 12px; color: var(--fg-muted); display: flex; align-items: center; gap: 6px; }
  .rows input { width: 72px; }

  .mode { display: inline-flex; border: 1px solid var(--border); border-radius: var(--radius-sm, 5px); overflow: hidden; }
  .mode button { background: var(--surface-2); color: var(--fg-muted); border: none; padding: 6px 12px; font-size: 12px; cursor: pointer; }
  .mode button.on { background: var(--accent-soft); color: var(--fg); }
  .editor-wrap { height: 26%; min-height: 110px; }

  .viz-bar { display: flex; gap: 4px; flex-wrap: wrap; }
  .viz-tab {
    display: flex; align-items: center; gap: 5px;
    background: var(--surface-2); color: var(--fg-muted);
    border: 1px solid var(--border); border-radius: var(--radius-sm, 5px);
    padding: 5px 10px; font-size: 12px; cursor: pointer;
  }
  .viz-tab.active { color: var(--fg); border-color: var(--accent); background: var(--accent-soft); }
  .viz-tab .ico { font-size: 13px; }

  .encoding { display: flex; gap: 14px; align-items: flex-start; flex-wrap: wrap; }
  .enc-field { display: flex; flex-direction: column; gap: 4px; }
  .enc-field.grow { flex: 1; }
  .enc-field label { font-size: 11px; color: var(--fg-muted); text-transform: uppercase; letter-spacing: .05em; }
  .chips { display: flex; gap: 5px; flex-wrap: wrap; }
  .chip {
    background: var(--surface-2); color: var(--fg-muted);
    border: 1px solid var(--border); border-radius: 999px;
    padding: 3px 10px; font-size: 12px; cursor: pointer; font-family: var(--font-mono);
  }
  .chip.on { color: #fff; background: var(--accent); border-color: var(--accent); }

  .preview {
    flex: 1; min-height: 0;
    border: 1px solid var(--border); border-radius: var(--radius, 8px);
    background: var(--surface-1); padding: 8px; overflow: hidden;
    display: flex; flex-direction: column;
  }
  .empty { padding: 24px; text-align: center; margin: auto; }
  .bignum { margin: auto; text-align: center; }
  .bignum-val { font-size: 56px; font-weight: 700; color: var(--accent); font-family: var(--font-display, inherit); }
  .bignum-label { font-size: 13px; color: var(--fg-muted); font-family: var(--font-mono); }

  .grid-scroll { flex: 1; min-height: 0; overflow: auto; }
  table.grid { border-collapse: collapse; width: 100%; font-size: 13px; }
  .grid th, .grid td { border-bottom: 1px solid var(--border); border-right: 1px solid var(--border); padding: 5px 10px; text-align: left; white-space: nowrap; }
  .grid thead th { position: sticky; top: 0; background: var(--surface-2); }
  .grid td { font-family: var(--font-mono); }
  .grid td.null { color: var(--fg-faint); font-style: italic; }

  .error {
    color: var(--danger); background: var(--danger-soft);
    border: 1px solid var(--danger); border-radius: var(--radius-sm, 5px);
    padding: 8px 12px; font-family: var(--font-mono); font-size: 13px; white-space: pre-wrap;
  }
</style>
