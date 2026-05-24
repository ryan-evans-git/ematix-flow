<script>
  // Recursive renderer for a trigger expression tree.
  //
  // Every element — leaf AND composite — renders as its own state pill
  // with its own colored dot. Composites (AllOf/AnyOf) get an outer
  // pill labelled "ALL" or "ANY" carrying the aggregate state, with
  // child pills nested inside, joined by AND/OR text.
  //
  // Shape of `node`:
  //   { kind: "leaf", name: "...", state: "ready"|"pending"|"failed" }
  //   { kind: "all" | "any", members: [...], state: ... }
  import Self from "./TriggerExpr.svelte";
  export let node;
  export let topLevel = false;
</script>

{#if node.kind === "leaf"}
  <span class="wf-trig wf-trig--{node.state}">
    <span class="wf-trig-dot"></span>
    {#if topLevel}<strong>After:</strong>{/if}
    <span class="mono">{node.name}</span>
  </span>
{:else}
  {@const joiner = node.kind === "all" ? "AND" : "OR"}
  {@const label = node.kind === "all" ? "ALL" : "ANY"}
  <span class="wf-trig wf-trig--composite wf-trig--{node.state}">
    <span class="wf-trig-dot"></span>
    {#if topLevel}<strong>After:</strong>{/if}
    <strong class="wf-trig-op">{label}</strong>
    <span class="wf-paren">(</span>
    {#each node.members as child, i}
      {#if i > 0}<span class="wf-and">·&nbsp;{joiner}&nbsp;·</span>{/if}
      <Self node={child} topLevel={false} />
    {/each}
    <span class="wf-paren">)</span>
  </span>
{/if}

<style>
  .wf-trig {
    display: inline-flex;
    align-items: center;
    gap: 0.4em;
    padding: 0.15em 0.5em;
    border-radius: 999px;
    border: 1px solid var(--color-border, #444);
    background: var(--color-card, #161916);
    color: var(--color-fg, #e0e3e0);
    font-size: 0.9em;
    line-height: 1.2;
  }
  .wf-trig-dot {
    width: 0.6em;
    height: 0.6em;
    border-radius: 50%;
    background: var(--color-border, #888);
    display: inline-block;
    flex-shrink: 0;
  }
  .wf-trig--ready   { border-color: var(--border-strong, #2e2e38); }
  .wf-trig--pending { border-color: var(--border-strong, #2e2e38); }
  .wf-trig--failed  { border-color: var(--border-strong, #2e2e38); }
  .wf-trig--ready   .wf-trig-dot { background: var(--success, #4ade80); }
  .wf-trig--pending .wf-trig-dot { background: var(--warning, #facc15); }
  .wf-trig--failed  .wf-trig-dot { background: var(--danger, #f87171); }

  .wf-trig--composite {
    border-width: 1px;
    padding: 0.2em 0.55em;
    gap: 0.35em;
  }
  .wf-trig-op {
    font-family: var(--font-mono, monospace);
    font-size: 0.85em;
    opacity: 0.85;
  }
  .wf-paren {
    color: var(--color-fg-muted, #888);
    font-family: var(--font-mono, monospace);
    font-weight: 600;
    margin: 0 0.15em;
  }
  .wf-and {
    color: var(--color-fg-muted, #888);
    font-family: var(--font-mono, monospace);
    font-size: 0.85em;
    margin: 0 0.25em;
  }
  .mono {
    font-family: var(--font-mono, monospace);
  }
</style>
