<script>
  import { onMount } from "svelte";
  import Runs from "./routes/Runs.svelte";
  import RunDetail from "./routes/RunDetail.svelte";
  import Pipelines from "./routes/Pipelines.svelte";

  // Hash-based router: avoids needing SPA-fallback config on the
  // server. Routes:
  //   #/runs           → list view (default)
  //   #/runs/<id>      → detail view
  //   #/pipelines      → pipeline summary
  let route = parseHash(window.location.hash);

  function parseHash(h) {
    const m = (h || "#/runs").replace(/^#/, "");
    if (m.startsWith("/runs/")) {
      return { name: "run_detail", runId: decodeURIComponent(m.slice("/runs/".length)) };
    }
    if (m === "/pipelines") return { name: "pipelines" };
    return { name: "runs" };
  }

  onMount(() => {
    window.addEventListener("hashchange", () => {
      route = parseHash(window.location.hash);
    });
  });

  function navTarget(name) {
    if (route.name === name) return "active";
    if (name === "runs" && route.name === "run_detail") return "active";
    return "";
  }
</script>

<div class="app">
  <nav class="topbar">
    <span class="brand">▸ ematix-flow</span>
    <a href="#/runs" class={navTarget("runs")}>Pipelines</a>
    <a href="#/pipelines" class={navTarget("pipelines")}>Summary</a>
    <span style="flex: 1"></span>
    <a href="/api/docs" target="_blank" rel="noopener">API Docs ↗</a>
  </nav>

  {#if route.name === "runs"}
    <Runs />
  {:else if route.name === "run_detail"}
    <RunDetail runId={route.runId} />
  {:else if route.name === "pipelines"}
    <Pipelines />
  {/if}
</div>
