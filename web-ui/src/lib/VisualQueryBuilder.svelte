<script>
  // No-SQL query builder: pick a table, dimensions and metrics, and it
  // generates GROUP BY SQL. Emits `generate` with the SQL string; the
  // Chart Builder drops it into the editor and runs it.
  import { createEventDispatcher } from "svelte";
  import { listSchemas, listTables, listColumns } from "../lib/api.js";

  export let datasourceId = "";

  const dispatch = createEventDispatcher();
  const AGGS = ["SUM", "AVG", "COUNT", "MIN", "MAX"];

  let schema = "";
  let tables = [];
  let table = "";
  let columns = []; // [{name,type,nullable}]
  let dims = new Set();
  let metrics = [{ column: "", agg: "SUM" }];
  let error = "";

  $: if (datasourceId) loadTables();

  async function loadTables() {
    error = "";
    dims = new Set();
    table = "";
    columns = [];
    try {
      const schemas = (await listSchemas(datasourceId)).schemas || [];
      schema = schemas[0] || "";
      tables = schema ? (await listTables(datasourceId, schema)).tables || [] : [];
    } catch (e) {
      error = e.message;
    }
  }

  async function onTable() {
    dims = new Set();
    columns = [];
    if (!table) return;
    try {
      columns = (await listColumns(datasourceId, schema, table)).columns || [];
    } catch (e) {
      error = e.message;
    }
  }

  function toggleDim(name) {
    dims.has(name) ? dims.delete(name) : dims.add(name);
    dims = dims;
  }

  const qid = (n) => '"' + String(n).replace(/"/g, '""') + '"';

  function generate() {
    if (!table) return;
    const qtable = schema && schema !== "main" ? `${qid(schema)}.${qid(table)}` : qid(table);
    const dimList = [...dims];
    const metricList = metrics.filter((m) => m.column);
    const selects = [
      ...dimList.map(qid),
      ...metricList.map((m) => `${m.agg}(${qid(m.column)}) AS ${m.agg.toLowerCase()}_${m.column}`),
    ];
    if (!selects.length) {
      error = "pick at least one dimension or metric";
      return;
    }
    let sql = `SELECT ${selects.join(", ")}\nFROM ${qtable}`;
    if (dimList.length) sql += `\nGROUP BY ${dimList.map(qid).join(", ")}`;
    if (metricList.length && dimList.length)
      sql += `\nORDER BY ${metricList[0].agg}(${qid(metricList[0].column)}) DESC`;
    sql += `\nLIMIT 1000`;
    dispatch("generate", sql);
  }

  // Catalog columns carry raw DB types (INTEGER / REAL / "double
  // precision" / NUMERIC / BIGINT …), not the chart-result labels, so
  // match numeric families by keyword.
  const isNumericDbType = (t) => /int|real|double|float|numeric|decimal|number|money/i.test(String(t || ""));
  $: numericCols = columns.filter((c) => isNumericDbType(c.type));
</script>

<div class="vqb">
  <div class="row">
    <label>Table</label>
    <select bind:value={table} on:change={onTable}>
      <option value="">choose…</option>
      {#each tables as t}<option value={t.name}>{t.name}</option>{/each}
    </select>
  </div>

  {#if columns.length}
    <div class="row top">
      <label>Dimensions</label>
      <div class="chips">
        {#each columns as c}
          <button class="chip" class:on={dims.has(c.name)} on:click={() => toggleDim(c.name)}>{c.name}</button>
        {/each}
      </div>
    </div>

    <div class="row top">
      <label>Metrics</label>
      <div class="metrics">
        {#each metrics as m, i}
          <div class="metric">
            <select bind:value={m.agg}>{#each AGGS as a}<option value={a}>{a}</option>{/each}</select>
            <select bind:value={m.column}>
              <option value="">column…</option>
              {#each (m.agg === "COUNT" ? columns : numericCols) as c}<option value={c.name}>{c.name}</option>{/each}
            </select>
            <button class="mini" on:click={() => (metrics = metrics.filter((_, j) => j !== i))} disabled={metrics.length === 1}>−</button>
          </div>
        {/each}
        <button class="mini add" on:click={() => (metrics = [...metrics, { column: "", agg: "SUM" }])}>+ metric</button>
      </div>
    </div>

    <div class="row">
      <button class="gen" on:click={generate}>Generate SQL →</button>
    </div>
  {/if}

  {#if error}<div class="err">⚠ {error}</div>{/if}
</div>

<style>
  .vqb { display: flex; flex-direction: column; gap: 10px; padding: 10px; background: var(--surface-1); border: 1px solid var(--border); border-radius: var(--radius, 8px); }
  .row { display: flex; gap: 10px; align-items: center; }
  .row.top { align-items: flex-start; }
  label { width: 88px; font-size: 11px; text-transform: uppercase; letter-spacing: .05em; color: var(--fg-muted); padding-top: 5px; }
  select { background: var(--surface-2); color: var(--fg); border: 1px solid var(--border); border-radius: var(--radius-sm, 4px); padding: 5px 8px; font-size: 13px; }
  .chips { display: flex; gap: 5px; flex-wrap: wrap; }
  .chip { background: var(--surface-2); color: var(--fg-muted); border: 1px solid var(--border); border-radius: 999px; padding: 3px 10px; font-size: 12px; cursor: pointer; font-family: var(--font-mono); }
  .chip.on { color: #fff; background: var(--accent); border-color: var(--accent); }
  .metrics { display: flex; flex-direction: column; gap: 6px; }
  .metric { display: flex; gap: 6px; align-items: center; }
  .mini { background: var(--surface-2); border: 1px solid var(--border); color: var(--fg); border-radius: 4px; padding: 3px 8px; font-size: 12px; cursor: pointer; }
  .mini.add { color: var(--accent); align-self: flex-start; }
  .gen { background: var(--accent); color: #fff; border: none; border-radius: var(--radius-sm, 5px); padding: 7px 14px; font-weight: 600; cursor: pointer; }
  .err { color: var(--danger); font-size: 12px; font-family: var(--font-mono); }
</style>
