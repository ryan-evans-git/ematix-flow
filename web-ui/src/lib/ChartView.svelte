<script>
  // Renders a single chart from a query result + encoding. Shared by
  // the Chart Builder preview and dashboard tiles. Table / big-number
  // are rendered directly; everything else goes through ECharts.
  import { createEventDispatcher } from "svelte";
  import EChart from "./EChart.svelte";
  import { buildEChartsOption, bigNumberValue } from "./charts.js";

  export let vizType = "table";
  export let encoding = {};
  export let result = null; // {columns, rows}
  export let error = "";
  export let loading = false;

  const dispatch = createEventDispatcher();

  // Map a chart click to a {column, value} filter using the chart's
  // dimension column (X for cartesian, label for pie). Emitting this
  // powers dashboard cross-filtering.
  function onPointClick(e) {
    const dimCol =
      vizType === "pie" ? encoding.name : ["bar", "line", "area"].includes(vizType) ? encoding.x : null;
    if (dimCol && e.detail.name != null) {
      dispatch("filter", { column: dimCol, value: e.detail.name });
    }
  }

  $: option =
    result && !["table", "number"].includes(vizType)
      ? buildEChartsOption(vizType, result.columns, result.rows, encoding)
      : null;
  $: numberVal =
    result && vizType === "number"
      ? bigNumberValue(result.columns, result.rows, encoding)
      : null;
</script>

<div class="chart-view">
  {#if loading}
    <div class="cv-msg">loading…</div>
  {:else if error}
    <div class="cv-error">⚠ {error}</div>
  {:else if !result}
    <div class="cv-msg">no data</div>
  {:else if vizType === "table"}
    <div class="cv-grid-scroll">
      <table class="cv-grid">
        <thead><tr>{#each result.columns as c}<th>{c.name}</th>{/each}</tr></thead>
        <tbody>
          {#each result.rows as row}
            <tr>{#each row as cell}<td class:null={cell === null}>{cell === null ? "NULL" : cell}</td>{/each}</tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else if vizType === "number"}
    <div class="cv-bignum">
      <div class="cv-bignum-val">{numberVal === null ? "—" : (numberVal.toLocaleString?.() ?? numberVal)}</div>
      <div class="cv-bignum-label">{encoding.value || ""}</div>
    </div>
  {:else if option}
    {#key vizType + JSON.stringify(encoding)}
      <EChart {option} on:pointclick={onPointClick} />
    {/key}
  {:else}
    <div class="cv-msg">configure columns to render {vizType}</div>
  {/if}
</div>

<style>
  .chart-view { width: 100%; height: 100%; display: flex; flex-direction: column; min-height: 0; }
  .cv-msg { margin: auto; color: var(--fg-muted); font-size: 13px; padding: 16px; text-align: center; }
  .cv-error {
    margin: auto; color: var(--danger); background: var(--danger-soft);
    border: 1px solid var(--danger); border-radius: var(--radius-sm, 5px);
    padding: 8px 12px; font-family: var(--font-mono); font-size: 12px;
    max-width: 90%; white-space: pre-wrap;
  }
  .cv-bignum { margin: auto; text-align: center; }
  .cv-bignum-val { font-size: 44px; font-weight: 700; color: var(--accent); font-family: var(--font-display, inherit); }
  .cv-bignum-label { font-size: 12px; color: var(--fg-muted); font-family: var(--font-mono); }
  .cv-grid-scroll { flex: 1; min-height: 0; overflow: auto; }
  table.cv-grid { border-collapse: collapse; width: 100%; font-size: 12px; }
  .cv-grid th, .cv-grid td { border-bottom: 1px solid var(--border); border-right: 1px solid var(--border); padding: 4px 8px; text-align: left; white-space: nowrap; }
  .cv-grid thead th { position: sticky; top: 0; background: var(--surface-2); }
  .cv-grid td { font-family: var(--font-mono); }
  .cv-grid td.null { color: var(--fg-faint); font-style: italic; }
</style>
