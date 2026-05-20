<script>
  import { onMount } from "svelte";
  import { getRun, restartRun, rerunRun, pauseRun, resumeRun } from "../lib/api.js";

  export let runId;

  let run = null;
  let loading = true;
  let error = null;
  let toast = null;
  let toastError = false;

  async function load() {
    loading = true;
    error = null;
    try {
      run = await getRun(runId);
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  $: if (runId) load();

  function showToast(msg, isError = false) {
    toast = msg;
    toastError = isError;
    setTimeout(() => { toast = null; }, 3500);
  }

  async function doRestart() {
    if (!run?.actions?.restart_from_step?.length) return;
    const step = run.actions.restart_from_step[0];
    if (!confirm(`Restart run ${runId} from step "${step}"?`)) return;
    try {
      const r = await restartRun(runId, step);
      showToast(`Enqueued — new run ${r.new_run_id}`);
      window.location.hash = `#/runs/${encodeURIComponent(r.new_run_id)}`;
    } catch (e) {
      showToast(`Restart failed: ${e.message}`, true);
    }
  }

  async function doResumeFromWatermark() {
    if (!confirm(`Resume run ${runId} from last committed watermark?`)) return;
    try {
      const r = await restartRun(runId, null);
      showToast(`Enqueued — new run ${r.new_run_id}`);
      window.location.hash = `#/runs/${encodeURIComponent(r.new_run_id)}`;
    } catch (e) {
      showToast(`Resume failed: ${e.message}`, true);
    }
  }

  async function doRerun() {
    if (!confirm(`Rerun ${runId} from the beginning?`)) return;
    try {
      const r = await rerunRun(runId);
      showToast(`Enqueued — new run ${r.new_run_id}`);
      window.location.hash = `#/runs/${encodeURIComponent(r.new_run_id)}`;
    } catch (e) {
      showToast(`Rerun failed: ${e.message}`, true);
    }
  }

  async function doPause() {
    try {
      await pauseRun(runId);
      showToast(`Pause requested — will hold at next boundary`);
      load();
    } catch (e) {
      showToast(`Pause failed: ${e.message}`, true);
    }
  }

  async function doResume() {
    try {
      await resumeRun(runId);
      showToast(`Resume requested`);
      load();
    } catch (e) {
      showToast(`Resume failed: ${e.message}`, true);
    }
  }
</script>

{#if error}
  <div class="panel" style="border-color: var(--color-alarm); color: var(--color-alarm);">
    Error: {error}
  </div>
{:else if loading}
  <div class="loading">▸ loading...</div>
{:else if !run}
  <div class="empty">run not found</div>
{:else}
  <h1>{run.pipeline}</h1>
  <p>
    <span class="status status--{run.status}">{run.status}</span>
    &middot; <span class="mono">{run.run_id}</span>
    &middot; attempt {run.attempt}
  </p>

  <div class="actions">
    {#if run.actions.pause}
      <button class="action" on:click={doPause}>Pause</button>
    {/if}
    {#if run.actions.resume}
      <button class="action" on:click={doResume}>Resume</button>
    {/if}
    {#if run.actions.restart_from_step?.length}
      <button class="action" on:click={doRestart}>
        Restart from step "{run.actions.restart_from_step[0]}"
      </button>
    {/if}
    {#if run.actions.resume_from_watermark}
      <button class="action" on:click={doResumeFromWatermark}>
        Resume from watermark
      </button>
    {/if}
    {#if run.actions.rerun_full}
      <button class="action action--danger" on:click={doRerun}>
        Rerun from beginning
      </button>
    {/if}
  </div>

  <div class="divider">▸ Timeline</div>
  <div class="panel">
    <p><strong>Started:</strong> <span class="mono">{run.started_at || "—"}</span></p>
    <p><strong>Finished:</strong> <span class="mono">{run.finished_at || "(still running)"}</span></p>
    {#if run.failed_step}
      <p><strong>Failed step:</strong> <span class="mono">{run.failed_step}</span></p>
    {/if}
    {#if run.failed_watermark}
      <p><strong>Last watermark:</strong> <span class="mono">{run.failed_watermark}</span></p>
    {/if}
    {#if run.error_summary}
      <p><strong>Error:</strong> <span style="color: var(--color-alarm);">{run.error_summary}</span></p>
    {/if}
  </div>

  {#if run.attempts?.length}
    <div class="divider">▸ Attempts</div>
    {#each run.attempts as att}
      <div class="panel">
        <p>
          <strong>Attempt {att.attempt}</strong>
          &middot; <span class="status status--{att.status}">{att.status}</span>
          &middot; <span class="mono">{att.started_at || "—"}</span>
        </p>
        {#if att.error_summary}
          <pre style="color: var(--color-alarm); white-space: pre-wrap;">{att.error_summary}</pre>
        {/if}
      </div>
    {/each}
  {/if}

  {#if run.steps?.length}
    <div class="divider">▸ Steps</div>
    <table class="runs">
      <thead>
        <tr><th>Step</th><th>Status</th><th>Duration</th></tr>
      </thead>
      <tbody>
        {#each run.steps as s}
          <tr>
            <td>{s.name}</td>
            <td><span class="status status--{s.status}">{s.status}</span></td>
            <td>{s.duration_ms != null ? `${(s.duration_ms / 1000).toFixed(2)}s` : "—"}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
{/if}

{#if toast}
  <div class="toast {toastError ? 'toast--error' : ''}">{toast}</div>
{/if}
