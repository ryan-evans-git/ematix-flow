<script>
  // Hand-rolled drag/resize dashboard grid. Tiles are placed on a
  // 12-column grid in {x, y, w, h} grid units. In `editable` mode the
  // tile header drags (move) and the bottom-right handle resizes, both
  // snapped to the grid. Emits `change` with the updated tiles on drop.
  //
  // Tile content is provided by the parent via the default slot with a
  // `let:tile` prop, so the grid stays agnostic about what a tile shows.
  import { createEventDispatcher } from "svelte";

  export let tiles = []; // [{chart_id, x, y, w, h}]
  export let editable = false;

  const COLS = 12;
  const ROW_H = 44;
  const GAP = 10;

  const dispatch = createEventDispatcher();

  let containerWidth = 0;
  $: cellStride = containerWidth > 0 ? (containerWidth - GAP * (COLS - 1)) / COLS + GAP : 0;
  $: rows = tiles.reduce((m, t) => Math.max(m, t.y + t.h), 4);
  $: gridHeight = rows * (ROW_H + GAP);

  // Derived pixel geometry. This MUST reference `cellStride` and
  // `tiles` directly so Svelte recomputes it when the container is
  // measured (cellStride starts at 0) or a tile moves/resizes — a
  // plain helper function would hide the cellStride dependency and
  // leave tiles stuck at their initial 0-width position.
  $: positions = tiles.map((tile) => ({
    tile,
    left: tile.x * cellStride,
    top: tile.y * (ROW_H + GAP),
    width: Math.max(1, tile.w) * cellStride - GAP,
    height: Math.max(1, tile.h) * (ROW_H + GAP) - GAP,
  }));

  let drag = null; // {tile, mode, startX, startY, orig}

  function begin(tile, mode, ev) {
    if (!editable) return;
    ev.preventDefault();
    drag = { tile, mode, startX: ev.clientX, startY: ev.clientY, orig: { ...tile } };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  }

  function onMove(ev) {
    if (!drag || cellStride === 0) return;
    const dx = ev.clientX - drag.startX;
    const dy = ev.clientY - drag.startY;
    const t = drag.tile;
    const o = drag.orig;
    if (drag.mode === "move") {
      t.x = Math.min(COLS - o.w, Math.max(0, Math.round((o.x * cellStride + dx) / cellStride)));
      t.y = Math.max(0, Math.round((o.y * (ROW_H + GAP) + dy) / (ROW_H + GAP)));
    } else {
      const wpx = o.w * cellStride - GAP + dx;
      const hpx = o.h * (ROW_H + GAP) - GAP + dy;
      t.w = Math.min(COLS - o.x, Math.max(2, Math.round((wpx + GAP) / cellStride)));
      t.h = Math.max(2, Math.round((hpx + GAP) / (ROW_H + GAP)));
    }
    tiles = tiles; // trigger reactivity
  }

  function onUp() {
    window.removeEventListener("pointermove", onMove);
    window.removeEventListener("pointerup", onUp);
    if (drag) {
      drag = null;
      dispatch("change", tiles);
    }
  }

  function remove(tile) {
    tiles = tiles.filter((t) => t !== tile);
    dispatch("change", tiles);
  }
</script>

<div class="grid" bind:clientWidth={containerWidth} style="height: {gridHeight}px" class:editable>
  {#each positions as p (p.tile.chart_id)}
    {@const tile = p.tile}
    <div
      class="tile"
      style="left:{p.left}px; top:{p.top}px; width:{p.width}px; height:{p.height}px"
    >
      <div class="tile-head" on:pointerdown={(e) => begin(tile, "move", e)}>
        <span class="tile-title"><slot name="title" {tile}>chart</slot></span>
        {#if editable}
          <button class="tile-x" title="Remove" on:pointerdown|stopPropagation on:click={() => remove(tile)}>×</button>
        {/if}
      </div>
      <div class="tile-body">
        <slot {tile} />
      </div>
      {#if editable}
        <div class="tile-resize" on:pointerdown={(e) => begin(tile, "resize", e)} title="Resize"></div>
      {/if}
    </div>
  {/each}
</div>

<style>
  .grid {
    position: relative;
    width: 100%;
    min-height: 200px;
  }
  .tile {
    position: absolute;
    background: var(--surface-1);
    border: 1px solid var(--border);
    border-radius: var(--radius, 8px);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    box-sizing: border-box;
    transition: box-shadow 0.12s;
  }
  .editable .tile { border-color: var(--border-strong, var(--border)); }
  .editable .tile:hover { box-shadow: 0 0 0 1px var(--accent-soft); }
  .tile-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 5px 10px;
    font-size: 12px;
    color: var(--fg-muted);
    border-bottom: 1px solid var(--border);
    background: var(--surface-2);
    user-select: none;
  }
  .editable .tile-head { cursor: grab; }
  .tile-title { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 600; color: var(--fg); }
  .tile-x { background: none; border: none; color: var(--fg-muted); cursor: pointer; font-size: 15px; padding: 0 4px; }
  .tile-x:hover { color: var(--danger); }
  .tile-body { flex: 1; min-height: 0; padding: 6px; }
  .tile-resize {
    position: absolute;
    right: 0;
    bottom: 0;
    width: 16px;
    height: 16px;
    cursor: nwse-resize;
    background: linear-gradient(135deg, transparent 50%, var(--fg-faint) 50%, var(--fg-faint) 60%, transparent 60%);
  }
</style>
