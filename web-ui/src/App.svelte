<script>
  import { onMount } from "svelte";
  import Runs from "./routes/Runs.svelte";
  import RunDetail from "./routes/RunDetail.svelte";
  import Pipelines from "./routes/Pipelines.svelte";

  // Hash-based router: avoids needing SPA-fallback config on the
  // server. Routes:
  //   #/pipelines      → per-pipeline cards with last-10 strip (default)
  //   #/runs           → jobs table (all run records)
  //   #/runs/<id>      → run detail with task DAG
  let route = parseHash(window.location.hash);

  function parseHash(h) {
    const m = (h || "#/pipelines").replace(/^#/, "");
    if (m.startsWith("/runs/")) {
      return { name: "run_detail", runId: decodeURIComponent(m.slice("/runs/".length)) };
    }
    if (m === "/runs" || m.startsWith("/runs?")) return { name: "runs" };
    return { name: "pipelines" };
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
    <a href="#/pipelines" class={navTarget("pipelines")}>Pipelines</a>
    <a href="#/runs" class={navTarget("runs")}>Jobs</a>
    <span style="flex: 1"></span>
    <a href="/api/docs" target="_blank" rel="noopener">API Docs ↗</a>
  </nav>

  {#if route.name === "pipelines"}
    <Pipelines />
  {:else if route.name === "run_detail"}
    <RunDetail runId={route.runId} />
  {:else if route.name === "runs"}
    <Runs />
  {/if}
</div>
