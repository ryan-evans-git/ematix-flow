<script>
  import { onMount } from "svelte";
  import { pipelineDag } from "../lib/api.js";

  let data = { nodes: [], edges: [] };
  let loading = true;
  let error = null;

  async function load() {
    loading = true;
    error = null;
    try {
      data = await pipelineDag();
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  onMount(load);

  // Longest-path topological rank gives each node a left-to-right
  // column so upstreams always sit to the left of their downstreams.
  function rankNodes(nodes, edges) {
    const incoming = new Map();
    const outgoing = new Map();
    for (const n of nodes) {
      incoming.set(n.name, []);
      outgoing.set(n.name, []);
    }
    for (const e of edges) {
      if (incoming.has(e.to)) incoming.get(e.to).push(e.from);
      if (outgoing.has(e.from)) outgoing.get(e.from).push(e.to);
    }
    const ranks = new Map();
    function rankOf(name, seen = new Set()) {
      if (seen.has(name)) return 0;  // cycle guard
      if (ranks.has(name)) return ranks.get(name);
      seen.add(name);
      const ups = incoming.get(name) || [];
      const r = ups.length === 0
        ? 0
        : 1 + Math.max(...ups.map((u) => rankOf(u, seen)));
      ranks.set(name, r);
      return r;
    }
    for (const n of nodes) rankOf(n.name);
    return ranks;
  }

  // Group nodes into columns by their rank.
  $: ranks = rankNodes(data.nodes, data.edges);
  $: columns = (() => {
    const byRank = new Map();
    for (const n of data.nodes) {
      const r = ranks.get(n.name) ?? 0;
      if (!byRank.has(r)) byRank.set(r, []);
      byRank.get(r).push(n);
    }
    return [...byRank.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([rank, nodes]) => ({
        rank,
        nodes: nodes.slice().sort((a, b) => a.name.localeCompare(b.name)),
      }));
  })();

  // Down-edges-out per node so we can show fan-out at a glance.
  $: outDegree = (() => {
    const m = new Map();
    for (const e of data.edges) m.set(e.from, (m.get(e.from) || 0) + 1);
    return m;
  })();
</script>

<h1>Pipeline DAG</h1>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if data.nodes.length === 0}
  <div class="empty">
    no pipelines registered — declare a <code>@pipeline.register</code> or
    <code>@ematix.pipeline</code> / <code>@ematix.warehouse_pipeline</code>
    function in your module to populate this view
  </div>
{:else}
  <div class="dag-summary">
    {data.nodes.length} pipeline{data.nodes.length === 1 ? "" : "s"},
    {data.edges.length} dependency edge{data.edges.length === 1 ? "" : "s"}.
    Columns are topological rank — upstreams are always to the left.
  </div>
  <div class="dag-grid">
    {#each columns as col}
      <div class="dag-col">
        <div class="dag-col-label">rank {col.rank}</div>
        {#each col.nodes as n}
          <div class="dag-node" title={`schedule: ${n.schedule || "—"}\nfan-out: ${outDegree.get(n.name) || 0}`}>
            <div class="dag-node-name">{n.name}</div>
            <div class="dag-node-meta">
              {n.schedule || "(no schedule)"}
              {#if outDegree.get(n.name)}
                · fan-out {outDegree.get(n.name)}
              {/if}
            </div>
          </div>
        {/each}
      </div>
    {/each}
  </div>
{/if}

<style>
  .dag-summary {
    color: var(--color-fg-muted, #888);
    margin-bottom: 1.5em;
    font-size: 0.9em;
  }
  .dag-grid {
    display: grid;
    grid-auto-flow: column;
    grid-auto-columns: 200px;
    gap: 1.5em;
    overflow-x: auto;
    padding-bottom: 1em;
  }
  .dag-col {
    display: flex;
    flex-direction: column;
    gap: 0.75em;
  }
  .dag-col-label {
    text-transform: uppercase;
    font-size: 0.75em;
    letter-spacing: 0.1em;
    color: var(--color-fg-muted, #888);
    border-bottom: 1px solid var(--color-border, #444);
    padding-bottom: 0.25em;
  }
  .dag-node {
    border: 1px solid var(--color-border, #444);
    border-radius: 4px;
    padding: 0.6em 0.8em;
    background: var(--color-panel, #1a1a1a);
  }
  .dag-node-name {
    font-weight: 600;
    margin-bottom: 0.2em;
  }
  .dag-node-meta {
    font-size: 0.75em;
    color: var(--color-fg-muted, #888);
    font-family: var(--font-mono, monospace);
  }
</style>
