// Thin fetch wrappers around the /api/* endpoints. Errors are
// surfaced via the optional toast prop; otherwise this module is
// a pure JSON-in / JSON-out shim.

const API = "/api";

async function _request(path, init = {}) {
  const r = await fetch(`${API}${path}`, {
    headers: { "Accept": "application/json", ...(init.headers || {}) },
    ...init,
  });
  if (!r.ok) {
    let detail = `HTTP ${r.status}`;
    try {
      const body = await r.json();
      detail = body.detail || detail;
    } catch (_) { /* ignore */ }
    throw new Error(detail);
  }
  return r.json();
}

export async function listRuns({ pipeline, status, limit = 50, offset = 0 } = {}) {
  const params = new URLSearchParams();
  if (pipeline) params.set("pipeline", pipeline);
  if (status) params.set("status", status);
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  return _request(`/runs?${params.toString()}`);
}

export async function getRun(runId) {
  return _request(`/runs/${encodeURIComponent(runId)}`);
}

export async function listPipelines() {
  return _request("/pipelines");
}

export async function listWorkflows() {
  return _request("/workflows");
}

export async function restartRun(runId, fromStep) {
  return _request(`/runs/${encodeURIComponent(runId)}/restart`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ from_step: fromStep || null }),
  });
}

export async function rerunRun(runId) {
  return _request(`/runs/${encodeURIComponent(runId)}/rerun`, { method: "POST" });
}

export async function pauseRun(runId) {
  return _request(`/runs/${encodeURIComponent(runId)}/pause`, { method: "POST" });
}

export async function resumeRun(runId) {
  return _request(`/runs/${encodeURIComponent(runId)}/resume`, { method: "POST" });
}

export async function health() {
  return _request("/health");
}

export async function pipelineDag() {
  return _request("/dag");
}

export async function runWorkflowNow(name, { jobs } = {}) {
  const body = {};
  if (Array.isArray(jobs)) body.jobs = jobs;
  return _request(`/workflows/${encodeURIComponent(name)}/run-now`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function runJobNow(name, { cascadeDownstream = false } = {}) {
  return _request(`/jobs/${encodeURIComponent(name)}/run-now`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cascade_downstream: !!cascadeDownstream }),
  });
}

// ---- SQL Lab / analytics ------------------------------------------

export async function listDatasources() {
  return _request("/datasources");
}

const _ds = (id) => `/datasources/${encodeURIComponent(id)}`;

export async function listSchemas(datasourceId) {
  return _request(`${_ds(datasourceId)}/schemas`);
}

export async function listTables(datasourceId, schema) {
  return _request(`${_ds(datasourceId)}/schemas/${encodeURIComponent(schema)}/tables`);
}

export async function listColumns(datasourceId, schema, table) {
  return _request(
    `${_ds(datasourceId)}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns`,
  );
}

export async function runQuery({ datasourceId, sql, maxRows } = {}) {
  const body = { datasource_id: datasourceId, sql };
  if (maxRows != null) body.max_rows = maxRows;
  return _request("/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function listSavedQueries() {
  return _request("/saved-queries");
}

export async function createSavedQuery({ name, datasourceId, sql } = {}) {
  return _request("/saved-queries", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, datasource_id: datasourceId, sql }),
  });
}

export async function updateSavedQuery(id, { name, datasourceId, sql } = {}) {
  const body = {};
  if (name != null) body.name = name;
  if (datasourceId != null) body.datasource_id = datasourceId;
  if (sql != null) body.sql = sql;
  return _request(`/saved-queries/${encodeURIComponent(id)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function deleteSavedQuery(id) {
  return _request(`/saved-queries/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export async function listCharts() {
  return _request("/charts");
}

export async function getChart(id) {
  return _request(`/charts/${encodeURIComponent(id)}`);
}

export async function createChart({ name, datasourceId, sql, vizType, encoding } = {}) {
  return _request("/charts", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name,
      datasource_id: datasourceId,
      sql,
      viz_type: vizType,
      encoding: encoding || {},
    }),
  });
}

export async function updateChart(id, { name, datasourceId, sql, vizType, encoding } = {}) {
  const body = {};
  if (name != null) body.name = name;
  if (datasourceId != null) body.datasource_id = datasourceId;
  if (sql != null) body.sql = sql;
  if (vizType != null) body.viz_type = vizType;
  if (encoding != null) body.encoding = encoding;
  return _request(`/charts/${encodeURIComponent(id)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function deleteChart(id) {
  return _request(`/charts/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export async function listDashboards() {
  return _request("/dashboards");
}

export async function getDashboard(id) {
  return _request(`/dashboards/${encodeURIComponent(id)}`);
}

export async function createDashboard({ name, layout } = {}) {
  return _request("/dashboards", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, layout: layout || { tiles: [] } }),
  });
}

export async function updateDashboard(id, { name, layout } = {}) {
  const body = {};
  if (name != null) body.name = name;
  if (layout != null) body.layout = layout;
  return _request(`/dashboards/${encodeURIComponent(id)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function deleteDashboard(id) {
  return _request(`/dashboards/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export async function queryDashboard(id, filters = []) {
  return _request(`/dashboards/${encodeURIComponent(id)}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ filters }),
  });
}
