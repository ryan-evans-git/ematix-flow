<script>
  import { onMount } from "svelte";
  import Runs from "./routes/Runs.svelte";
  import RunDetail from "./routes/RunDetail.svelte";
  import Jobs from "./routes/Pipelines.svelte";
  import Workflows from "./routes/Workflows.svelte";
  import Dag from "./routes/Dag.svelte";

  // Hash-based router. v0.5.1 nav model:
  //   #/workflows      → top-level grouping of jobs (default)
  //   #/jobs           → flat list of individual jobs (was the
  //                       "Pipelines" page; component file kept the
  //                       old name to minimise churn)
  //   #/runs           → run history (the actual execution records)
  //   #/runs/<id>      → run detail
  //   #/dag, #/dag/<n> → cross-job DAG (focused or full)
  // Legacy "#/pipelines" still routes to Jobs for any saved links.
  let route = parseHash(window.location.hash);

  function parseHash(h) {
    const m = (h || "#/workflows").replace(/^#/, "");
    if (m.startsWith("/runs/")) {
      return { name: "run_detail", runId: decodeURIComponent(m.slice("/runs/".length)) };
    }
    if (m === "/runs" || m.startsWith("/runs?")) return { name: "runs" };
    if (m.startsWith("/dag/")) {
      return { name: "dag", focus: decodeURIComponent(m.slice("/dag/".length)) };
    }
    if (m === "/dag") return { name: "dag", focus: null };
    if (m === "/jobs" || m === "/pipelines" || m.startsWith("/jobs?") || m.startsWith("/pipelines?")) {
      return { name: "jobs" };
    }
    return { name: "workflows" };
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
    <a href="#/workflows" class={navTarget("workflows")}>Workflows</a>
    <a href="#/jobs" class={navTarget("jobs")}>Jobs</a>
    <a href="#/runs" class={navTarget("runs")}>Runs</a>
    <a href="#/dag" class={navTarget("dag")}>DAG</a>
    <span style="flex: 1"></span>
    <a href="/api/docs" target="_blank" rel="noopener">API Docs ↗</a>
  </nav>

  {#if route.name === "workflows"}
    <Workflows />
  {:else if route.name === "jobs"}
    <Jobs />
  {:else if route.name === "run_detail"}
    <RunDetail runId={route.runId} />
  {:else if route.name === "runs"}
    <Runs />
  {:else if route.name === "dag"}
    <Dag focus={route.focus} />
  {/if}
</div>
