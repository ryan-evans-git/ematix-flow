<script>
  import { onMount } from "svelte";
  import Runs from "./routes/Runs.svelte";
  import RunDetail from "./routes/RunDetail.svelte";
  import Jobs from "./routes/Pipelines.svelte";
  import Workflows from "./routes/Workflows.svelte";
  import Dag from "./routes/Dag.svelte";
  import SqlLab from "./routes/SqlLab.svelte";
  import ChartBuilder from "./routes/ChartBuilder.svelte";
  import Dashboards from "./routes/Dashboards.svelte";
  import Quality from "./routes/Quality.svelte";
  import StreamDlq from "./routes/StreamDlq.svelte";
  import { me, loadMe } from "./lib/session.js";

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
    // DLQ Phase 5: per-stream dead-letter queue screen.
    const dlq = m.match(/^\/streams\/(.+)\/dlq$/);
    if (dlq) return { name: "stream_dlq", stream: decodeURIComponent(dlq[1]) };
    if (m === "/jobs" || m === "/pipelines" || m.startsWith("/jobs?") || m.startsWith("/pipelines?")) {
      return { name: "jobs" };
    }
    if (m === "/sql" || m.startsWith("/sql?")) return { name: "sql" };
    if (m === "/charts" || m.startsWith("/charts")) return { name: "charts" };
    if (m === "/dashboards" || m.startsWith("/dashboards")) return { name: "dashboards" };
    if (m === "/quality" || m.startsWith("/quality")) return { name: "quality" };
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
    loadMe();
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
  $: sqlActive = route.name === "sql";
  $: chartsActive = route.name === "charts";
  $: dashboardsActive = route.name === "dashboards";
  $: qualityActive = route.name === "quality";
</script>

<div class="app">
  <nav class="topbar">
    <span class="brand">ematix-flow</span>
    <a href="#/workflows" class:active={workflowsActive}>Workflows</a>
    <a href="#/jobs" class:active={jobsActive}>Jobs</a>
    <a href="#/runs" class:active={runsActive}>Runs</a>
    <a href="#/dag" class:active={dagActive}>DAG</a>
    <a href="#/sql" class:active={sqlActive}>SQL Lab</a>
    <a href="#/charts" class:active={chartsActive}>Charts</a>
    <a href="#/dashboards" class:active={dashboardsActive}>Dashboards</a>
    <a href="#/quality" class:active={qualityActive}>Quality</a>
    <span style="flex: 1"></span>
    {#if $me.rbac_enabled && $me.identity}
      <span class="user-badge" title="Signed in via SSO">
        {$me.identity}<span class="role role-{$me.role}">{$me.role}</span>
      </span>
    {/if}
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
  {:else if route.name === "sql"}
    <SqlLab />
  {:else if route.name === "charts"}
    <ChartBuilder />
  {:else if route.name === "dashboards"}
    <Dashboards />
  {:else if route.name === "quality"}
    <Quality />
  {:else if route.name === "stream_dlq"}
    <StreamDlq name={route.stream} />
  {/if}
</div>

<style>
  .user-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: var(--fg-muted);
    margin-right: 10px;
  }
  .role {
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-size: 10px;
    font-weight: 600;
    padding: 1px 6px;
    border-radius: 999px;
    border: 1px solid var(--border);
  }
  .role-admin { color: var(--danger); border-color: var(--danger); }
  .role-editor { color: var(--accent); border-color: var(--accent); }
  .role-viewer { color: var(--fg-muted); }
</style>
