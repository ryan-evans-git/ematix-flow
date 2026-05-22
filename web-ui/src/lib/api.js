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
