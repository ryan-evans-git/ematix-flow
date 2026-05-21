<script>
  // Recursive renderer for a trigger expression tree.
  //
  // Shape of `node`:
  //   { kind: "leaf", name: "...", state: "ready"|"pending"|"failed" }
  //   { kind: "all" | "any", members: [...], state: ... }
  //
  // `topLevel` means the outermost call — we skip the wrapping parens
  // and the "After:" header is rendered by the caller. Nested calls
  // render their own parens.
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
  {#if !topLevel}<span class="wf-paren">(</span>{/if}
  {#if topLevel && node.kind === "all"}
    <strong class="wf-trig-prefix">After:</strong>
  {:else if topLevel && node.kind === "any"}
    <strong class="wf-trig-prefix">After any of:</strong>
  {/if}
  {#each node.members as child, i}
    {#if i > 0}<span class="wf-and">·&nbsp;{joiner}&nbsp;·</span>{/if}
    <Self node={child} topLevel={false} />
  {/each}
  {#if !topLevel}<span class="wf-paren">)</span>{/if}
{/if}

<style>
  .wf-paren {
    color: var(--color-fg-muted, #888);
    font-family: var(--font-mono, monospace);
    font-weight: 600;
  }
  .wf-trig-prefix { margin-right: 0.2em; }
</style>
