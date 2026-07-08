<script>
  import { onMount } from "svelte";
  import SqlEditor from "../lib/SqlEditor.svelte";
  import { me, can } from "../lib/session.js";
  import {
    listDatasources,
    listSchemas,
    listTables,
    listColumns,
    runQuery,
    listSavedQueries,
    createSavedQuery,
    deleteSavedQuery,
  } from "../lib/api.js";

  let datasources = [];
  let datasourceId = "";
  let schema = "";
  let tables = [];
  let columnsByTable = {}; // name -> [{name,type,nullable}]
  let expanded = {}; // table -> bool
  let schemaMap = {}; // table -> [colName] for autocomplete

  let editor;
  let sql = "SELECT 1";
  let maxRows = 1000;

  let running = false;
  let result = null; // {columns, rows, stats}
  let error = "";

  let savedQueries = [];
  let catalogError = "";

  $: canQuery = can($me, "query");
  $: canWrite = can($me, "write");

  onMount(async () => {
    try {
      const ds = (await listDatasources()).datasources || [];
      datasources = ds;
      if (ds.length) {
        datasourceId = ds[0].id;
        await onDatasourceChange();
      }
      await refreshSaved();
    } catch (e) {
      error = e.message;
    }
  });

  async function onDatasourceChange() {
    tables = [];
    columnsByTable = {};
    expanded = {};
    schemaMap = {};
    schema = "";
    catalogError = "";
    if (!datasourceId) return;
    try {
      const schemas = (await listSchemas(datasourceId)).schemas || [];
      schema = schemas[0] || "";
      if (schema) {
        tables = (await listTables(datasourceId, schema)).tables || [];
        // Seed autocomplete with table names (columns fill in on expand).
        schemaMap = Object.fromEntries(tables.map((t) => [t.name, []]));
      }
    } catch (e) {
      catalogError = e.message;
    }
  }

  async function toggleTable(name) {
    expanded = { ...expanded, [name]: !expanded[name] };
    if (expanded[name] && !columnsByTable[name]) {
      try {
        const cols = (await listColumns(datasourceId, schema, name)).columns || [];
        columnsByTable = { ...columnsByTable, [name]: cols };
        schemaMap = { ...schemaMap, [name]: cols.map((c) => c.name) };
      } catch (e) {
        columnsByTable = { ...columnsByTable, [name]: [] };
      }
    }
  }

  function insertText(t) {
    if (editor) editor.insert(t);
  }

  async function run() {
    error = "";
    running = true;
    const text = editor ? editor.getDoc() : sql;
    try {
      result = await runQuery({ datasourceId, sql: text, maxRows: Number(maxRows) || undefined });
    } catch (e) {
      error = e.message;
      result = null;
    } finally {
      running = false;
    }
  }

  async function refreshSaved() {
    try {
      savedQueries = (await listSavedQueries()).saved_queries || [];
    } catch (_) {
      /* non-fatal */
    }
  }

  async function save() {
    const text = editor ? editor.getDoc() : sql;
    const name = window.prompt("Name this query:");
    if (!name) return;
    try {
      await createSavedQuery({ name, datasourceId, sql: text });
      await refreshSaved();
    } catch (e) {
      error = e.message;
    }
  }

  function loadSaved(q) {
    if (!q) return;
    if (q.datasource_id && datasources.some((d) => d.id === q.datasource_id)) {
      if (q.datasource_id !== datasourceId) {
        datasourceId = q.datasource_id;
        onDatasourceChange();
      }
    }
    if (editor) editor.setDoc(q.sql);
    sql = q.sql;
  }

  async function removeSaved(q, ev) {
    ev.stopPropagation();
    if (!window.confirm(`Delete saved query "${q.name}"?`)) return;
    try {
      await deleteSavedQuery(q.id);
      await refreshSaved();
    } catch (e) {
      error = e.message;
    }
  }
</script>

<div class="lab">
  <aside class="sidebar">
    <div class="side-section">
      <label class="side-label" for="ds">Data source</label>
      <select id="ds" bind:value={datasourceId} on:change={onDatasourceChange}>
        {#each datasources as d}
          <option value={d.id}>{d.id} · {d.dialect}</option>
        {/each}
      </select>
    </div>

    <div class="side-section grow">
      <div class="side-label">Schema{schema ? ` · ${schema}` : ""}</div>
      {#if catalogError}
        <div class="muted small">⚠ {catalogError}</div>
      {:else if !tables.length}
        <div class="muted small">no tables</div>
      {:else}
        <ul class="tree">
          {#each tables as t}
            <li>
              <button class="tree-row" on:click={() => toggleTable(t.name)}>
                <span class="caret">{expanded[t.name] ? "▾" : "▸"}</span>
                <span class="tbl" class:view={t.kind === "view"}>{t.name}</span>
                {#if t.kind === "view"}<span class="badge">view</span>{/if}
                <button class="ins" title="Insert name" on:click|stopPropagation={() => insertText(t.name)}>+</button>
              </button>
              {#if expanded[t.name]}
                <ul class="cols">
                  {#each columnsByTable[t.name] || [] as c}
                    <li>
                      <button class="col-row" on:click={() => insertText(c.name)} title={c.type}>
                        <span class="col-name">{c.name}</span>
                        <span class="col-type">{c.type}{c.nullable ? "" : " ·nn"}</span>
                      </button>
                    </li>
                  {/each}
                </ul>
              {/if}
            </li>
          {/each}
        </ul>
      {/if}
    </div>

    <div class="side-section">
      <div class="side-label">Saved</div>
      {#if !savedQueries.length}
        <div class="muted small">none yet</div>
      {:else}
        <ul class="saved">
          {#each savedQueries as q}
            <li>
              <button class="saved-row" on:click={() => loadSaved(q)} title={q.sql}>
                <span class="saved-name">{q.name}</span>
                <button class="del" on:click={(e) => removeSaved(q, e)} title="Delete">×</button>
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  </aside>

  <section class="main">
    <div class="toolbar">
      <button class="run" on:click={run} disabled={running || !datasourceId || !canQuery} title={canQuery ? "" : "Requires the query permission"}>
        {running ? "Running…" : "▶ Run"} <kbd>⌘⏎</kbd>
      </button>
      <button class="secondary" on:click={save} disabled={!datasourceId || !canWrite} title={canWrite ? "" : "Requires the write permission"}>Save</button>
      <span class="spacer"></span>
      <label class="rows">
        Limit
        <input type="number" min="1" max="100000" bind:value={maxRows} />
      </label>
    </div>

    <div class="editor-wrap">
      <SqlEditor bind:this={editor} bind:value={sql} schema={schemaMap} onRun={run} />
    </div>

    <div class="results">
      {#if error}
        <div class="error">⚠ {error}</div>
      {:else if result}
        <div class="stats">
          {result.stats.row_count} row{result.stats.row_count === 1 ? "" : "s"}
          · {result.stats.elapsed_ms} ms
          {#if result.stats.truncated}<span class="trunc">· truncated to {maxRows}</span>{/if}
        </div>
        <div class="grid-scroll">
          <table class="grid">
            <thead>
              <tr>
                <th class="rownum">#</th>
                {#each result.columns as c}
                  <th><span class="cn">{c.name}</span><span class="ct">{c.type}</span></th>
                {/each}
              </tr>
            </thead>
            <tbody>
              {#each result.rows as row, i}
                <tr>
                  <td class="rownum">{i + 1}</td>
                  {#each row as cell}
                    <td class:null={cell === null}>{cell === null ? "NULL" : cell}</td>
                  {/each}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <div class="muted empty">Run a query to see results. <kbd>⌘⏎</kbd></div>
      {/if}
    </div>
  </section>
</div>

<style>
  .lab {
    display: grid;
    grid-template-columns: 260px 1fr;
    gap: 12px;
    height: calc(100vh - 60px);
    padding: 12px;
    box-sizing: border-box;
  }
  .sidebar {
    display: flex;
    flex-direction: column;
    gap: 14px;
    min-height: 0;
    background: var(--surface-1);
    border: 1px solid var(--border);
    border-radius: var(--radius, 8px);
    padding: 12px;
    overflow: hidden;
  }
  .side-section { display: flex; flex-direction: column; gap: 6px; min-height: 0; }
  .side-section.grow { flex: 1; overflow: auto; }
  .side-label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--fg-muted); }
  select, input[type="number"] {
    background: var(--surface-2);
    color: var(--fg);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm, 4px);
    padding: 5px 8px;
    font-size: 13px;
  }
  .muted { color: var(--fg-muted); }
  .small { font-size: 12px; }

  .tree, .cols, .saved { list-style: none; margin: 0; padding: 0; }
  .tree-row, .col-row, .saved-row {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 6px;
    background: none;
    border: none;
    color: var(--fg);
    padding: 3px 4px;
    font-size: 13px;
    cursor: pointer;
    border-radius: 4px;
    text-align: left;
  }
  .tree-row:hover, .col-row:hover, .saved-row:hover { background: var(--hover); }
  .caret { color: var(--fg-muted); width: 10px; font-size: 10px; }
  .tbl { font-family: var(--font-mono); }
  .tbl.view { font-style: italic; color: var(--fg-muted); }
  .badge { font-size: 10px; color: var(--fg-muted); border: 1px solid var(--border); border-radius: 3px; padding: 0 4px; }
  .ins, .del {
    margin-left: auto;
    background: none; border: none; color: var(--fg-muted);
    cursor: pointer; font-size: 13px; padding: 0 4px; border-radius: 3px;
  }
  .ins:hover, .del:hover { color: var(--accent); background: var(--accent-soft); }
  .cols { margin-left: 16px; border-left: 1px solid var(--border); }
  .col-row { justify-content: space-between; }
  .col-name { font-family: var(--font-mono); font-size: 12px; }
  .col-type { color: var(--fg-faint); font-size: 11px; }
  .saved-name { font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

  .main {
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-height: 0;
  }
  .toolbar { display: flex; align-items: center; gap: 8px; }
  .spacer { flex: 1; }
  button.run {
    background: var(--accent);
    color: #fff;
    border: none;
    border-radius: var(--radius-sm, 5px);
    padding: 7px 14px;
    font-weight: 600;
    cursor: pointer;
  }
  button.run:disabled { opacity: .5; cursor: default; }
  button.secondary {
    background: var(--surface-2);
    color: var(--fg);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm, 5px);
    padding: 7px 12px;
    cursor: pointer;
  }
  kbd {
    font-family: var(--font-mono); font-size: 11px;
    background: rgba(255,255,255,.08); border-radius: 3px; padding: 1px 4px;
  }
  .rows { font-size: 12px; color: var(--fg-muted); display: flex; align-items: center; gap: 6px; }
  .rows input { width: 76px; }

  .editor-wrap { height: 38%; min-height: 140px; }

  .results { flex: 1; min-height: 0; display: flex; flex-direction: column; gap: 6px; }
  .stats { font-size: 12px; color: var(--fg-muted); }
  .trunc { color: var(--warning); }
  .error {
    color: var(--danger);
    background: var(--danger-soft);
    border: 1px solid var(--danger);
    border-radius: var(--radius-sm, 5px);
    padding: 10px 12px;
    font-family: var(--font-mono);
    font-size: 13px;
    white-space: pre-wrap;
  }
  .empty { padding: 24px; text-align: center; }
  .grid-scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius, 8px);
  }
  table.grid { border-collapse: collapse; width: 100%; font-size: 13px; }
  .grid th, .grid td {
    border-bottom: 1px solid var(--border);
    border-right: 1px solid var(--border);
    padding: 5px 10px;
    text-align: left;
    white-space: nowrap;
  }
  .grid thead th {
    position: sticky; top: 0;
    background: var(--surface-2);
    z-index: 1;
  }
  .grid th .cn { display: block; font-weight: 600; }
  .grid th .ct { display: block; font-size: 10px; color: var(--fg-faint); font-weight: 400; }
  .grid td { font-family: var(--font-mono); }
  .grid td.null { color: var(--fg-faint); font-style: italic; }
  .grid .rownum { color: var(--fg-faint); text-align: right; }
  .grid tbody tr:hover td { background: var(--hover); }
</style>
