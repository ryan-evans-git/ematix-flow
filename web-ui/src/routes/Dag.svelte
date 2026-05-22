<script>
  import { onMount } from "svelte";
  import { pipelineDag, listPipelines } from "../lib/api.js";
  import DagFlowchart from "../lib/DagFlowchart.svelte";

  export let focus = null;

  let data = { nodes: [], edges: [] };
  let pipelineMap = {};
  let loading = true;
  let error = null;

  async function load() {
    loading = true;
    error = null;
    try {
      const [dag, plist] = await Promise.all([pipelineDag(), listPipelines()]);
      data = dag;
      const m = {};
      for (const p of (plist.pipelines || [])) m[p.name] = p;
      pipelineMap = m;
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  onMount(load);

  // Subgraph: ancestors + descendants of `focus`.
  function subgraph(nodes, edges, root) {
    if (!root) return { nodes, edges };
    const byName = new Map(nodes.map((n) => [n.name, n]));
    if (!byName.has(root)) return { nodes: [], edges: [] };
    const up = new Map(), down = new Map();
    for (const e of edges) {
      if (!up.has(e.to)) up.set(e.to, []);
      up.get(e.to).push(e.from);
      if (!down.has(e.from)) down.set(e.from, []);
      down.get(e.from).push(e.to);
    }
    const reach = new Set([root]);
    let stack = [root];
    while (stack.length) {
      const n = stack.pop();
      for (const p of (up.get(n) || []))   if (!reach.has(p)) { reach.add(p); stack.push(p); }
    }
    stack = [root];
    while (stack.length) {
      const n = stack.pop();
      for (const c of (down.get(n) || [])) if (!reach.has(c)) { reach.add(c); stack.push(c); }
    }
    return {
      nodes: nodes.filter((n) => reach.has(n.name)),
      edges: edges.filter((e) => reach.has(e.from) && reach.has(e.to)),
    };
  }

  $: view = subgraph(data.nodes, data.edges, focus);
</script>

<h1>
  {#if focus}
    DAG · <span class="mono">{focus}</span>
    <a class="link" style="margin-left: 0.75rem; font-size: 0.7em;" href="#/dag">show all</a>
  {:else}
    DAG
  {/if}
</h1>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if view.nodes.length === 0}
  <div class="empty">
    {#if focus}
      job <code>{focus}</code> isn't registered (or has no graph).
      <a class="link" href="#/dag">show all →</a>
    {:else}
      no jobs registered — declare an <code>@ematix.job</code> function in
      your module to populate this view
    {/if}
  </div>
{:else}
  <div class="dag-summary">
    {view.nodes.length} job{view.nodes.length === 1 ? "" : "s"},
    {view.edges.length} edge{view.edges.length === 1 ? "" : "s"}.
    Arrows point from upstream to downstream. Bar width = latest-run duration relative to the slowest node here. Click any node to refocus.
  </div>
  <DagFlowchart
    nodes={view.nodes}
    edges={view.edges}
    jobMap={pipelineMap}
    focusName={focus}
  />
{/if}

<style>
  .dag-summary {
    color: var(--color-fg-muted, #888);
    margin-bottom: 1em;
    font-size: 0.9em;
  }
</style>
