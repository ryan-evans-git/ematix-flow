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
  // Theme state mirrors the data-theme attribute set inline in
  // index.html (so we avoid a flash of unstyled content). Toggling
  // here updates both the attribute and localStorage.
  let theme = "dark";

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

  function toggleTheme() {
    const next = theme === "dark" ? "light" : "dark";
    theme = next;
    if (next === "light") {
      document.documentElement.setAttribute("data-theme", "light");
    } else {
      document.documentElement.removeAttribute("data-theme");
    }
    try { localStorage.setItem("ematix-theme", next); } catch (_) {}
  }

  onMount(() => {
    theme = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
    window.addEventListener("hashchange", () => {
      route = parseHash(window.location.hash);
    });
  });

  // Reactive derived flags. Svelte's template can't track function
  // calls (a navTarget(name) call doesn't re-evaluate on `route`
  // change), so we materialise the active state into top-level
  // reactive vars and use `class:active={...}` in the template.
  $: workflowsActive = route.name === "workflows";
  $: jobsActive = route.name === "jobs";
  $: runsActive = route.name === "runs" || route.name === "run_detail";
  $: dagActive = route.name === "dag";
</script>

<div class="app">
  <nav class="topbar">
    <span class="brand">ematix-flow</span>
    <a href="#/workflows" class:active={workflowsActive}>Workflows</a>
    <a href="#/jobs" class:active={jobsActive}>Jobs</a>
    <a href="#/runs" class:active={runsActive}>Runs</a>
    <a href="#/dag" class:active={dagActive}>DAG</a>
    <span style="flex: 1"></span>
    <a href="/api/docs" target="_blank" rel="noopener">API Docs ↗</a>
    <button
      class="theme-toggle"
      on:click={toggleTheme}
      aria-label="Toggle color theme"
      title={theme === "dark" ? "Switch to light theme" : "Switch to dark theme"}
    >
      {theme === "dark" ? "☾" : "☀"}
    </button>
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
