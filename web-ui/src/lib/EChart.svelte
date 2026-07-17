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
  let echarts = null;
  let appliedTheme = null;
  let themeObserver = null;

  // The app's light/dark toggle sets data-theme="light" on <html>
  // (dark is the default, no attribute). ECharts bakes the theme in at
  // init(), so we read the current theme and re-init when it flips —
  // otherwise charts stay dark-on-light in light mode.
  function currentTheme() {
    return document.documentElement.getAttribute("data-theme") === "light"
      ? "light"
      : "dark";
  }

  function initChart() {
    if (!echarts || !el) return;
    if (chart) chart.dispose();
    appliedTheme = currentTheme();
    chart = echarts.init(el, appliedTheme, { renderer: "canvas" });
    chart.on("click", (params) =>
      dispatch("pointclick", { name: params.name, seriesName: params.seriesName, value: params.value }),
    );
    if (option) chart.setOption(option, true);
    if (!ro) {
      ro = new ResizeObserver(() => chart && chart.resize());
      ro.observe(el);
    }
  }

  onMount(async () => {
    echarts = await import("echarts");
    initChart();
    themeObserver = new MutationObserver(() => {
      if (currentTheme() !== appliedTheme) initChart();
    });
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });
  });

  onDestroy(() => {
    if (themeObserver) themeObserver.disconnect();
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
