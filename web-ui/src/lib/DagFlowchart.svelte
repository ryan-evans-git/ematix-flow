<script>
  // Renders a DAG as an SVG flowchart with arrows.
  //
  // Inputs:
  //   nodes:    [{name, schedule?}]                                — DAG vertices
  //   edges:    [{from, to}]                                       — directed edges
  //   jobMap:   {name -> /api/pipelines record}                    — overlay status + duration
  //   focusName: string | null                                     — highlight one node
  //   compact:  boolean                                            — denser layout in cards
  //   onNavigate: (jobName) => void  — optional; defaults to hash nav to #/dag/<name>
  //
  // Layout: topological rank → columns left→right; within a column,
  // alphabetical order top→bottom. Edges drawn as cubic Béziers from
  // each upstream's right edge to each downstream's left edge.

  export let nodes = [];
  export let edges = [];
  export let jobMap = {};
  export let focusName = null;
  export let compact = false;
  export let onNavigate = null;

  $: NODE_W   = compact ? 160 : 200;
  $: NODE_H   = compact ?  56 :  76;
  $: COL_GAP  = compact ?  60 :  80;
  $: ROW_GAP  = compact ?  16 :  24;
  $: PAD      = compact ?  10 :  16;

  function rankOf(nodes, edges) {
    const incoming = new Map();
    for (const n of nodes) incoming.set(n.name, []);
    for (const e of edges) {
      if (incoming.has(e.to)) incoming.get(e.to).push(e.from);
    }
    const ranks = new Map();
    function rec(name, seen = new Set()) {
      if (seen.has(name)) return 0;
      if (ranks.has(name)) return ranks.get(name);
      seen.add(name);
      const ups = incoming.get(name) || [];
      const r = ups.length === 0
        ? 0
        : 1 + Math.max(...ups.map((u) => rec(u, seen)));
      ranks.set(name, r);
      return r;
    }
    for (const n of nodes) rec(n.name);
    return ranks;
  }

  $: ranks = rankOf(nodes, edges);
  $: columns = (() => {
    const byRank = new Map();
    for (const n of nodes) {
      const r = ranks.get(n.name) ?? 0;
      if (!byRank.has(r)) byRank.set(r, []);
      byRank.get(r).push(n);
    }
    return [...byRank.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([rank, ns]) => ({ rank, nodes: ns.slice().sort((a, b) => a.name.localeCompare(b.name)) }));
  })();

  // Compute (x, y) per node.
  $: positions = (() => {
    const map = new Map();
    for (const col of columns) {
      const x = PAD + col.rank * (NODE_W + COL_GAP);
      col.nodes.forEach((n, idx) => {
        const y = PAD + idx * (NODE_H + ROW_GAP);
        map.set(n.name, { x, y });
      });
    }
    return map;
  })();

  // Canvas dimensions.
  $: width = (() => {
    const maxRank = columns.length === 0 ? 0 : columns[columns.length - 1].rank;
    return PAD * 2 + (maxRank + 1) * NODE_W + maxRank * COL_GAP;
  })();
  $: height = (() => {
    let maxRows = 0;
    for (const col of columns) maxRows = Math.max(maxRows, col.nodes.length);
    return PAD * 2 + maxRows * NODE_H + Math.max(0, maxRows - 1) * ROW_GAP;
  })();

  function fmtDuration(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  }

  function nodeStatus(name) {
    return jobMap[name]?.latest_run?.status || "—";
  }
  function nodeDuration(name) {
    const p = jobMap[name];
    return p?.latest_run?.duration_ms ?? p?.median_duration_ms ?? null;
  }

  $: maxDur = (() => {
    let m = 0;
    for (const n of nodes) {
      const d = nodeDuration(n.name);
      if (d && d > m) m = d;
    }
    return m;
  })();
  function barWidth(name) {
    if (!maxDur) return 0;
    const d = nodeDuration(name);
    if (!d) return 0;
    return Math.round((NODE_W - 16) * Math.sqrt(d / maxDur));
  }

  // Edge path: cubic Bézier from (sx,sy) to (tx,ty), bowing horizontally.
  function edgePath(e) {
    const s = positions.get(e.from);
    const t = positions.get(e.to);
    if (!s || !t) return "";
    const sx = s.x + NODE_W;
    const sy = s.y + NODE_H / 2;
    const tx = t.x - 4;             // stop short of the node border for the arrowhead
    const ty = t.y + NODE_H / 2;
    const dx = Math.max(40, (tx - sx) * 0.5);
    return `M ${sx} ${sy} C ${sx + dx} ${sy}, ${tx - dx} ${ty}, ${tx} ${ty}`;
  }

  function navigate(name, ev) {
    ev?.preventDefault?.();
    if (onNavigate) onNavigate(name);
    else window.location.hash = `#/dag/${encodeURIComponent(name)}`;
  }

  // colour-by-status border class on the node rect
  function strokeFor(name) {
    if (focusName && name === focusName) return "var(--color-phosphor-400, #5fd75f)";
    const s = nodeStatus(name);
    if (s === "failed") return "var(--color-alarm, #b85040)";
    if (s === "running") return "var(--color-phosphor-400, #5fd75f)";
    return "var(--color-border, #444)";
  }
  function strokeWidthFor(name) {
    return (focusName && name === focusName) ? 2 : 1;
  }
</script>

{#if nodes.length === 0}
  <div class="dag-empty">no nodes</div>
{:else}
  <svg
    class="dag-svg"
    viewBox="0 0 {width} {height}"
    width={width}
    height={height}
    style="max-width: 100%; height: auto;"
    role="img"
    aria-label="DAG flowchart"
  >
    <defs>
      <marker id="dag-arrow" viewBox="0 0 10 10" refX="9" refY="5"
              markerWidth="7" markerHeight="7" orient="auto">
        <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--color-phosphor-500, #4eb84e)" />
      </marker>
    </defs>

    {#each edges as e}
      <path
        d={edgePath(e)}
        fill="none"
        stroke="var(--color-phosphor-500, #4eb84e)"
        stroke-width="1.5"
        opacity="0.7"
        marker-end="url(#dag-arrow)"
      />
    {/each}

    {#each nodes as n}
      {@const pos = positions.get(n.name) || { x: 0, y: 0 }}
      {@const st = nodeStatus(n.name)}
      <g transform={`translate(${pos.x}, ${pos.y})`} class="dag-node" on:click={(ev) => navigate(n.name, ev)} on:keydown={(ev) => ev.key === 'Enter' && navigate(n.name, ev)} role="link" tabindex="0">
        <title>{`${n.name}\nschedule: ${n.schedule || '—'}\nstatus: ${st}\nduration: ${fmtDuration(nodeDuration(n.name))}`}</title>
        <rect
          x="0" y="0"
          width={NODE_W} height={NODE_H}
          rx="5" ry="5"
          fill="rgba(8,16,12,0.85)"
          stroke={strokeFor(n.name)}
          stroke-width={strokeWidthFor(n.name)}
        />
        <text x="10" y="18" class="dag-name" font-family="var(--font-mono, monospace)" font-size="12" font-weight="600" fill="var(--color-phosphor-200, #c8e8c8)">{n.name}</text>
        <text x="10" y={compact ? 32 : 36} class="dag-meta" font-family="var(--font-mono, monospace)" font-size="10" fill="var(--color-fg-muted, #888)">{st} · {fmtDuration(nodeDuration(n.name))}</text>
        {#if !compact}
          <text x="10" y="52" class="dag-meta" font-family="var(--font-mono, monospace)" font-size="9" fill="var(--color-fg-muted, #666)">{n.schedule || '(no schedule)'}</text>
        {/if}
        <!-- duration bar -->
        <rect x="8" y={NODE_H - 8} width={NODE_W - 16} height="3" rx="1.5" fill="rgba(255,255,255,0.05)" />
        <rect x="8" y={NODE_H - 8} width={barWidth(n.name)} height="3" rx="1.5" fill="var(--color-phosphor-500, #4eb84e)" />
      </g>
    {/each}
  </svg>
{/if}

<style>
  .dag-svg {
    display: block;
    background: rgba(0, 0, 0, 0.15);
    border: 1px solid var(--color-border, #444);
    border-radius: 4px;
    padding: 4px;
  }
  .dag-empty {
    color: var(--color-fg-muted, #888);
    font-style: italic;
    padding: 1em;
  }
  .dag-node {
    cursor: pointer;
  }
  .dag-node:hover rect {
    stroke: var(--color-phosphor-400, #5fd75f);
    stroke-width: 2;
  }
  .dag-node:focus {
    outline: none;
  }
  .dag-node:focus rect {
    stroke: var(--color-phosphor-400, #5fd75f);
    stroke-width: 2;
  }
</style>
