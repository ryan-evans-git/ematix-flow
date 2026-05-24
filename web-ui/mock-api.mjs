// Dev-only mock API for `npm run dev`. Returns realistic fixture
// JSON for the same endpoints the FastAPI backend serves at /api/*,
// so the SPA can be visually previewed without the Python server
// running. Imported from vite.config.js as a custom middleware;
// the production build bundle does not include this file.

const now = Date.now();
const ago = (ms) => new Date(now - ms).toISOString();
const ahead = (ms) => new Date(now + ms).toISOString();
const s = (n) => n * 1_000;
const m = (n) => n * 60_000;
const h = (n) => n * m(60);

// ----- Fixture: pipelines -----

function recentRuns(pattern) {
  return pattern.map((status, i) => ({
    run_id: `r-${(900 - i).toString(16)}`,
    status,
    duration_ms:
      status === "succeeded" ? 8_000 + i * 1_700 + Math.round(Math.random() * 3_000)
      : status === "failed"  ? 22_000 + Math.round(Math.random() * 10_000)
      : status === "running" ? 14_000
      : status === "paused"  ? 0
      : 0,
    started_at: ago(h(i * 2 + 1)),
  }));
}

const PIPELINES = [
  {
    name: "warehouse_etl.ingest_orders",
    kind: "batch",
    workflow: "warehouse_etl",
    schedule: "0 */2 * * *",
    timezone: "America/New_York",
    next_run_at: ahead(m(26)),
    latest_run: { run_id: "r-9af3", status: "succeeded", duration_ms: 12_410, started_at: ago(m(34)) },
    median_duration_ms: 12_400,
    failure_rate_pct: 0.0,
    recent_runs: recentRuns(["succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded"]),
  },
  {
    name: "warehouse_etl.build_sales_mart",
    kind: "batch",
    workflow: "warehouse_etl",
    schedule: "0 */2 * * *",
    timezone: "America/New_York",
    next_run_at: ahead(m(26)),
    latest_run: { run_id: "r-9ad9", status: "succeeded", duration_ms: 48_911, started_at: ago(m(48)) },
    median_duration_ms: 48_900,
    failure_rate_pct: 10.0,
    recent_runs: recentRuns(["succeeded","succeeded","succeeded","failed","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded"]),
  },
  {
    name: "warehouse_etl.publish_metrics",
    kind: "batch",
    workflow: "warehouse_etl",
    schedule: "0 */2 * * *",
    timezone: "America/New_York",
    next_run_at: ahead(m(26)),
    latest_run: { run_id: "r-9ab0", status: "succeeded", duration_ms: 4_311, started_at: ago(m(34)) },
    median_duration_ms: 4_300,
    failure_rate_pct: 0.0,
    recent_runs: recentRuns(["succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded"]),
  },
  {
    name: "realtime_orders.stream_orders_to_olap",
    kind: "streaming",
    workflow: "realtime_orders",
    schedule: null,
    timezone: "UTC",
    next_run_at: null,
    latest_run: { run_id: "r-9ada", status: "running", duration_ms: null, started_at: ago(h(2)) },
    median_duration_ms: null,
    failure_rate_pct: 0.0,
    recent_runs: recentRuns(["running","succeeded","succeeded","succeeded","paused","succeeded","succeeded","succeeded","succeeded","succeeded"]),
    streaming_stats: {
      snapshot_at: ago(s(8)),
      stats_1m: { rows_consumed_per_sec: 4212, avg_batch_cycle_ms: 38 },
      stats_5m: { rows_consumed_per_sec: 4087, avg_batch_cycle_ms: 41 },
    },
  },
  {
    name: "nightly_reporting.materialize_dashboards",
    kind: "batch",
    workflow: "nightly_reporting",
    schedule: "0 5 * * *",
    timezone: "America/New_York",
    next_run_at: ahead(h(17)),
    latest_run: { run_id: "r-99fc", status: "failed", duration_ms: 96_701, started_at: ago(h(7)) },
    median_duration_ms: 96_700,
    failure_rate_pct: 30.0,
    recent_runs: recentRuns(["failed","succeeded","failed","succeeded","succeeded","failed","succeeded","succeeded","succeeded","succeeded"]),
  },
  {
    name: "nightly_reporting.send_briefs",
    kind: "batch",
    workflow: "nightly_reporting",
    schedule: "0 5 * * *",
    timezone: "America/New_York",
    next_run_at: ahead(h(17)),
    latest_run: { run_id: "r-995a", status: "succeeded", duration_ms: 31_409, started_at: ago(h(24)) },
    median_duration_ms: 31_000,
    failure_rate_pct: 5.0,
    recent_runs: recentRuns(["succeeded","succeeded","succeeded","succeeded","succeeded","failed","succeeded","succeeded","succeeded","succeeded"]),
  },
  {
    name: "feature_store_sync.pull_feature_views",
    kind: "batch",
    workflow: "feature_store_sync",
    schedule: "*/15 * * * *",
    timezone: "UTC",
    next_run_at: ahead(m(7)),
    latest_run: { run_id: "r-9921", status: "requested", duration_ms: null, started_at: null },
    median_duration_ms: 1_800,
    failure_rate_pct: 0.0,
    recent_runs: recentRuns(["succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded"]),
  },
  {
    name: "feature_store_sync.publish_to_redis",
    kind: "batch",
    workflow: "feature_store_sync",
    schedule: "*/15 * * * *",
    timezone: "UTC",
    next_run_at: ahead(m(7)),
    latest_run: { run_id: "r-9ac2", status: "succeeded", duration_ms: 2_098, started_at: ago(m(12)) },
    median_duration_ms: 2_100,
    failure_rate_pct: 0.0,
    recent_runs: recentRuns(["succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded","succeeded"]),
  },
];

// ----- Fixture: workflows -----

const WORKFLOWS = [
  {
    name: "warehouse_etl",
    kind: "declared",
    jobs: ["ingest_orders", "load_dim_customer", "build_sales_mart", "publish_metrics"],
    edges: [
      { from: "ingest_orders", to: "build_sales_mart" },
      { from: "load_dim_customer", to: "build_sales_mart" },
      { from: "build_sales_mart", to: "publish_metrics" },
    ],
    triggers: [
      { kind: "schedule", state: "active", label: "0 */2 * * * (EST)" },
    ],
  },
  {
    name: "realtime_orders",
    kind: "streaming",
    jobs: ["stream_orders_to_olap"],
    edges: [],
    triggers: [],
  },
  {
    name: "nightly_reporting",
    kind: "declared",
    jobs: ["materialize_dashboards", "send_briefs"],
    edges: [
      { from: "materialize_dashboards", to: "send_briefs" },
    ],
    triggers: [
      { kind: "schedule", state: "active", label: "0 5 * * * (EST)" },
    ],
  },
  {
    name: "feature_store_sync",
    kind: "declared",
    jobs: ["pull_feature_views", "publish_to_redis"],
    edges: [
      { from: "pull_feature_views", to: "publish_to_redis" },
    ],
    triggers: [
      { kind: "schedule", state: "active", label: "*/15 * * * * (UTC)" },
    ],
  },
];

// ----- Fixture: runs -----

const RUNS = [
  { run_id: "r-9af3", pipeline: "warehouse_etl.ingest_orders",                  status: "succeeded", started_at: ago(m(34)),   duration_ms: 12_410, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-9ad9", pipeline: "warehouse_etl.build_sales_mart",               status: "succeeded", started_at: ago(m(48)),   duration_ms: 48_911, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-9ada", pipeline: "realtime_orders.stream_orders_to_olap",        status: "running",   started_at: ago(h(2)),    duration_ms: null,   attempt: 1, triggered_by: "manual" },
  { run_id: "r-9ac2", pipeline: "feature_store_sync.publish_to_redis",          status: "succeeded", started_at: ago(m(12)),   duration_ms: 2_098,  attempt: 1, triggered_by: "schedule" },
  { run_id: "r-9ab0", pipeline: "warehouse_etl.publish_metrics",                status: "succeeded", started_at: ago(m(34)),   duration_ms: 4_311,  attempt: 1, triggered_by: "schedule" },
  { run_id: "r-99fc", pipeline: "nightly_reporting.materialize_dashboards",     status: "failed",    started_at: ago(h(7)),    duration_ms: 96_701, attempt: 2, triggered_by: "schedule" },
  { run_id: "r-99f8", pipeline: "warehouse_etl.ingest_orders",                  status: "succeeded", started_at: ago(h(2) + m(34)), duration_ms: 11_988, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-99e1", pipeline: "warehouse_etl.build_sales_mart",               status: "succeeded", started_at: ago(h(2) + m(48)), duration_ms: 49_200, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-99d7", pipeline: "realtime_orders.stream_orders_to_olap",        status: "paused",    started_at: ago(h(8)),    duration_ms: null,   attempt: 1, triggered_by: "manual" },
  { run_id: "r-99c4", pipeline: "feature_store_sync.publish_to_redis",          status: "succeeded", started_at: ago(m(27)),   duration_ms: 2_120,  attempt: 1, triggered_by: "schedule" },
  { run_id: "r-99b2", pipeline: "warehouse_etl.publish_metrics",                status: "succeeded", started_at: ago(h(2) + m(34)), duration_ms: 4_298, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-99a9", pipeline: "warehouse_etl.ingest_orders",                  status: "succeeded", started_at: ago(h(4) + m(34)), duration_ms: 12_511, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-998c", pipeline: "warehouse_etl.build_sales_mart",               status: "failed",    started_at: ago(h(4) + m(48)), duration_ms: 38_421, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-9971", pipeline: "warehouse_etl.publish_metrics",                status: "succeeded", started_at: ago(h(4) + m(34)), duration_ms: 4_201, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-995a", pipeline: "nightly_reporting.send_briefs",                status: "succeeded", started_at: ago(h(24) + m(35)), duration_ms: 31_409, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-993f", pipeline: "warehouse_etl.ingest_orders",                  status: "succeeded", started_at: ago(h(6) + m(34)), duration_ms: 12_345, attempt: 1, triggered_by: "schedule" },
  { run_id: "r-9921", pipeline: "feature_store_sync.pull_feature_views",        status: "requested", started_at: null,         duration_ms: null,   attempt: 1, triggered_by: "manual" },
  { run_id: "r-9908", pipeline: "warehouse_etl.build_sales_mart",               status: "cancelled", started_at: ago(h(12)),   duration_ms: 1_028,  attempt: 1, triggered_by: "manual" },
];

function runDetail(id) {
  const base = RUNS.find((r) => r.run_id === id) || RUNS[0];
  const steps = [
    { name: "discover_inputs",        status: "succeeded", started_at: base.started_at, duration_ms: 180 },
    { name: "fetch_source",           status: "succeeded", started_at: base.started_at, duration_ms: 4_200 },
    { name: "transform_partition_0",  status: "succeeded", started_at: base.started_at, duration_ms: 3_180 },
    { name: "transform_partition_1",  status: base.status === "failed" ? "failed" : "succeeded", started_at: base.started_at, duration_ms: 3_298 },
    { name: "merge_partitions",       status: base.status === "failed" ? "pending" : "succeeded", started_at: base.started_at, duration_ms: 412 },
    { name: "write_destination",      status: base.status === "failed" ? "pending" : (base.status === "running" ? "running" : "succeeded"), started_at: base.started_at, duration_ms: base.status === "succeeded" ? 1_290 : null },
    { name: "publish_metrics",        status: base.status === "succeeded" ? "succeeded" : "pending", started_at: base.started_at, duration_ms: base.status === "succeeded" ? 180 : null },
  ];
  const failedStep = steps.find((s) => s.status === "failed")?.name || null;
  return {
    ...base,
    attempt: 1,
    finished_at: base.duration_ms != null && base.started_at
      ? new Date(new Date(base.started_at).getTime() + base.duration_ms).toISOString()
      : null,
    failed_step: failedStep,
    failed_watermark: null,
    error_summary: base.status === "failed"
      ? "transform_partition_1: ConnectionResetError during shuffle"
      : null,
    steps,
    attempts: [
      {
        attempt: 1,
        status: base.status,
        started_at: base.started_at,
        finished_at: base.duration_ms != null && base.started_at
          ? new Date(new Date(base.started_at).getTime() + base.duration_ms).toISOString()
          : null,
      },
    ],
    // `actions` shape per RunDetail.svelte:
    //   actions.pause                bool
    //   actions.resume               bool
    //   actions.restart_from_step    array of step names (truthy if non-empty)
    //   actions.resume_from_watermark bool
    //   actions.rerun_full           bool
    actions: {
      pause: base.status === "running",
      resume: base.status === "paused",
      restart_from_step: failedStep ? [failedStep] : [],
      resume_from_watermark: base.status === "failed" && false, // streaming-only
      rerun_full: ["succeeded","failed","cancelled"].includes(base.status),
    },
  };
}

// ----- Fixture: cross-job DAG -----

const DAG = {
  nodes: PIPELINES.map((p) => ({
    name: p.name,
    short_name: p.name.split(".").slice(-1)[0],
    status: p.latest_run?.status || "pending",
    kind: p.kind,
  })),
  edges: [
    { from: "warehouse_etl.ingest_orders",                to: "warehouse_etl.build_sales_mart" },
    { from: "warehouse_etl.build_sales_mart",             to: "warehouse_etl.publish_metrics" },
    { from: "warehouse_etl.build_sales_mart",             to: "nightly_reporting.materialize_dashboards" },
    { from: "nightly_reporting.materialize_dashboards",   to: "nightly_reporting.send_briefs" },
    { from: "feature_store_sync.pull_feature_views",      to: "feature_store_sync.publish_to_redis" },
    { from: "warehouse_etl.ingest_orders",                to: "realtime_orders.stream_orders_to_olap" },
  ],
};

// ----- Dispatch -----

function json(res, payload, status = 200) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(payload));
}

export function mockApiPlugin() {
  return {
    name: "ematix-flow-mock-api",
    configureServer(server) {
      server.middlewares.use("/api", (req, res, next) => {
        const url = new URL(req.url, "http://x");
        const path = url.pathname;
        const method = req.method || "GET";

        if (method === "GET" && path === "/health") return json(res, { ok: true });
        if (method === "GET" && path === "/workflows") return json(res, { workflows: WORKFLOWS });
        if (method === "GET" && path === "/pipelines") return json(res, { pipelines: PIPELINES });
        if (method === "GET" && path === "/dag") return json(res, DAG);

        if (method === "GET" && path === "/runs") {
          const pipeline = url.searchParams.get("pipeline");
          const status = url.searchParams.get("status");
          let filtered = RUNS;
          if (pipeline) filtered = filtered.filter((r) => r.pipeline.includes(pipeline));
          if (status) filtered = filtered.filter((r) => r.status === status);
          const limit = parseInt(url.searchParams.get("limit") || "50", 10);
          const offset = parseInt(url.searchParams.get("offset") || "0", 10);
          return json(res, { runs: filtered.slice(offset, offset + limit), total: filtered.length });
        }

        const runDetailMatch = path.match(/^\/runs\/([^/]+)$/);
        if (method === "GET" && runDetailMatch) {
          return json(res, runDetail(decodeURIComponent(runDetailMatch[1])));
        }

        const actionMatch = path.match(/^\/runs\/([^/]+)\/(restart|rerun|pause|resume)$/);
        if (method === "POST" && actionMatch) {
          return json(res, { ok: true, run_id: actionMatch[1], action: actionMatch[2], new_run_id: "r-MOCK01" });
        }
        const workflowRunMatch = path.match(/^\/workflows\/([^/]+)\/run-now$/);
        if (method === "POST" && workflowRunMatch) {
          return json(res, { ok: true, queued_run_id: "r-MOCK01" });
        }
        const jobRunMatch = path.match(/^\/jobs\/([^/]+)\/run-now$/);
        if (method === "POST" && jobRunMatch) {
          return json(res, { ok: true, new_run_id: "r-MOCK02" });
        }

        // Anything else: pass through (Vite will 404).
        next();
      });
    },
  };
}
