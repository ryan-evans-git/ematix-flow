<script>
  // Thin wrapper over Apache ECharts. The library (~1MB) is dynamically
  // imported so it lands in its own chunk — only loaded when a chart is
  // actually shown.
  import { onMount, onDestroy, createEventDispatcher } from "svelte";

  export let option = null;

  const dispatch = createEventDispatcher();

  let el;
  let chart = null;
  let ro = null;

  onMount(async () => {
    const echarts = await import("echarts");
    chart = echarts.init(el, "dark", { renderer: "canvas" });
    chart.on("click", (params) =>
      dispatch("pointclick", { name: params.name, seriesName: params.seriesName, value: params.value }),
    );
    if (option) chart.setOption(option, true);
    ro = new ResizeObserver(() => chart && chart.resize());
    ro.observe(el);
  });

  onDestroy(() => {
    if (ro) ro.disconnect();
    if (chart) chart.dispose();
  });

  $: if (chart && option) chart.setOption(option, true);
</script>

<div class="echart" bind:this={el}></div>

<style>
  .echart {
    width: 100%;
    height: 100%;
    min-height: 220px;
  }
</style>
