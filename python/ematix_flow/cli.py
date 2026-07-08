"""`flow` CLI entry point.

v0.1 subcommands:

  flow list --module my_pipelines
  flow run  --module my_pipelines <name>
  flow run-due --module my_pipelines [--now ISO8601] [--interval 60]

`flow run-due` is intended to be invoked once per `--interval` seconds
from an external scheduler (host cron, k8s CronJob, GitHub Actions
schedule). It executes every registered pipeline whose cron expression
fires inside the half-open window `(now - interval, now]`.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

from ematix_flow import config
from ematix_flow import pipeline as p
from ematix_flow._banner import print_banner


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _import_user_module(name: str) -> None:
    # Setuptools entry-point scripts don't get cwd on sys.path the way
    # `python script.py` does. Prepend it so users can `flow ... --module
    # pipelines` from the dir containing `pipelines.py` without manually
    # exporting PYTHONPATH.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    importlib.import_module(name)


def _cmd_list(args: argparse.Namespace) -> int:
    """Phase 27c Q4.2 β: merged listing of pipelines + transforms."""
    _import_user_module(args.module)
    entries = p.list_entries()
    if args.format == "json":
        print(
            json.dumps(
                [
                    {"name": e.name, "kind": e.kind, "schedule": e.schedule}
                    for e in entries
                ]
            )
        )
        return 0
    if not entries:
        return 0
    name_w = max((len(e.name) for e in entries), default=4)
    kind_w = max((len(e.kind) for e in entries), default=4)
    for e in entries:
        sched = e.schedule if e.schedule is not None else "(unscheduled)"
        print(f"{e.name:<{name_w}}  {e.kind:<{kind_w}}  {sched}")
    return 0


def _cmd_transform_list(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    entries = p.list_transforms()
    if args.format == "json":
        print(
            json.dumps(
                [
                    {"name": e.name, "kind": e.kind, "schedule": e.schedule}
                    for e in entries
                ]
            )
        )
        return 0
    for e in entries:
        sched = e.schedule if e.schedule is not None else "(unscheduled)"
        print(f"{e.name}\t{sched}")
    return 0


def _cmd_transform_run(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    try:
        result = p.run_transform(args.name)
    except KeyError:
        print(f"error: no transform named {args.name!r}", file=sys.stderr)
        return 2
    print(json.dumps(result, default=str))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    # Ω.W.3: worker-side claim semantics. When the scheduler spawns
    # us via SubprocessExecutor, --claim-token is set and we need to
    # heartbeat the lease while the pipeline runs, then release on
    # exit so the next scheduler tick can re-fire if the schedule
    # comes due again.
    claim_token = getattr(args, "claim_token", None)
    # Skip the banner when running as a scheduler-dispatched worker —
    # the scheduler already prints its own startup line and the worker's
    # banner repeats every tick across N pipelines, drowning out actual
    # output. Bare `flow run` (one-shot, no claim token) still prints it.
    if not claim_token:
        print_banner()
    _import_user_module(args.module)

    run_log = _open_run_log_or_none(args) if claim_token else None
    if claim_token and run_log is not None:
        # Restore _LAST_RUN + _ATTEMPT_STATE from the RunLog BEFORE the
        # pipeline runs so the worker can compute the correct
        # attempt_count on failure (prev.attempt_count + 1 instead of
        # always 1). Without this, a flaky pipeline never advances past
        # attempt_count=1 and gave_up never fires.
        try:
            run_log.restore_into_process()
        except Exception as e:
            print(
                f"warning: restore_into_process failed in worker: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )
    heartbeat = None
    if claim_token and run_log is not None:
        from ematix_flow.executors.heartbeat import HeartbeatThread

        heartbeat = HeartbeatThread(
            run_log,
            claim_token,
            lease_seconds=int(args.lease_seconds),
            interval_seconds=int(args.heartbeat_interval),
        )
        heartbeat.start()

    success = False
    err: BaseException | None = None
    try:
        try:
            result = p.run_pipeline(args.name)
            success = True
        except KeyError:
            print(f"error: no pipeline named {args.name!r}", file=sys.stderr)
            return 2
        except BaseException as e:
            err = e
            raise
        print(json.dumps(result, default=str))
        return 0
    finally:
        # Ω.W.6 follow-up: worker must record its own outcome to the
        # RunLog so the central scheduler's DAG-gating sees this run.
        # Without this, `depends_on=[...]` downstream pipelines stay
        # gated forever because `_LAST_RUN` is only ever populated in
        # the scheduler when it `restore_into_process()`s from the
        # RunLog — which only the worker can write.
        if claim_token and run_log is not None:
            try:
                completed_at = datetime.now(UTC)
                run_log.record_run(args.name, completed_at, success)
                if success:
                    run_log.clear_attempt_state(args.name)
                else:
                    # Record an attempt so retry-backoff fires next tick.
                    prev = p._ATTEMPT_STATE.get(args.name)
                    attempt_count = (prev.attempt_count if prev else 0) + 1
                    policy = p._RETRY_POLICY.get(args.name, p.RetryPolicy())
                    new_state = p.AttemptState(
                        attempt_count=attempt_count,
                        last_attempt_at=completed_at,
                        gave_up=attempt_count >= policy.max_attempts,
                    )
                    run_log.record_attempt(args.name, new_state)
            except Exception as e:
                print(
                    f"warning: failed to record run outcome for "
                    f"{args.name!r}: {type(e).__name__}: {e}",
                    file=sys.stderr,
                )

        if heartbeat is not None:
            heartbeat.stop()
        if claim_token and run_log is not None:
            try:
                run_log.release(claim_token)
            except Exception as e:
                # Releasing is best-effort — the lease expires
                # naturally and the scheduler reclaims.
                print(
                    f"warning: failed to release claim {claim_token}: "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )
        if run_log is not None:
            run_log.close()
        # If the pipeline raised, surface it now (after RunLog writes).
        if err is not None and not isinstance(err, SystemExit):
            return 1  # noqa: B012 — intentional: convert raise to exit code 1 after cleanup


def _cmd_consume(args: argparse.Namespace) -> int:
    """Π.3: load a streaming pipeline from a Python module by name
    and hand its rendered TOML to the Rust streaming runner.

    The user's module decorates pipelines via
    ``@ematix.streaming_pipeline(name="...", ...)``; the decorator
    registers them by name. This CLI imports the module (which fires
    every decorator), looks the named pipeline up, and renders the
    TOML the existing Rust ``flow consume <toml>`` runtime parses.
    """
    from ematix_flow import streaming

    print_banner()
    _import_user_module(args.module)
    try:
        toml = streaming.render_streaming_pipeline_toml(args.name)
    except KeyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    # v0.5.0: opt-in streaming-stats recording. When --run-log-url is
    # set (or $EMATIX_FLOW_RUN_LOG_URL), streaming pipelines emit live
    # throughput + batch-cycle snapshots that the Web UI surfaces.
    run_log_url = getattr(args, "run_log_url", None) or os.environ.get(
        "EMATIX_FLOW_RUN_LOG_URL"
    )
    result = streaming.run_pipeline(
        config_str=toml,
        metrics_port=args.metrics_port,
        pipeline_name=args.name,
        run_log_url=run_log_url,
    )
    print(json.dumps(result, default=str))
    return 0


def _cmd_consume_list(args: argparse.Namespace) -> int:
    """Π.3: list streaming pipelines registered by the imported module."""
    from ematix_flow import streaming

    _import_user_module(args.module)
    names = streaming.list_streaming_pipelines()
    if args.format == "json":
        print(json.dumps({"pipelines": names}))
        return 0
    for name in names:
        print(name)
    return 0


def _cmd_run_due(args: argparse.Namespace) -> int:
    print_banner()
    _import_user_module(args.module)
    now = _parse_iso(args.now) if args.now else datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)

    # Phase Ω.D1a: open the run-log (default ~/.ematix-flow/run_log.db,
    # opt-out via --no-run-log) and restore prior _LAST_RUN +
    # _ATTEMPT_STATE so freshness + retry-backoff gates see what
    # earlier cron ticks did.
    run_log = _open_run_log_or_none(args)
    if run_log is not None:
        run_log.restore_into_process()

    due: list[str] = [
        sp.name for sp in p.list_pipelines()
        if p.is_due(sp.schedule, now, args.interval)
    ]
    if not due:
        print(json.dumps({"ran": [], "failed": [], "skipped": []}, default=str))
        return 0

    # Phase Ω.D2: use the unified `run_due_with_dag_detailed` so
    # retry backoff + gave-up gating actually fire from the CLI.
    # Ω.D4: attach alerters + metrics sink resolved from --alerter
    # and --metrics URLs. Slice 2d (#136): wire OTEL traces too.
    alerters = _open_alerters(args)
    metrics_sink = _open_metrics(args)
    _configure_tracing_from_args(args)
    try:
        result = p.run_due_with_dag_detailed(
            due, now=now, run_log=run_log,
            alerters=alerters, metrics=metrics_sink,
        )
    finally:
        try:
            metrics_sink.close()
        except Exception:
            pass

    # Log per-pipeline progress to stderr for tail-the-log operators;
    # the structured JSON on stdout is what scripts parse.
    for ev in result.fired:
        print(f"firing: {ev.name}", file=sys.stderr)
    for ev in result.failed:
        print(f"failed: {ev.name}: {ev.error_message}", file=sys.stderr)
    for ev in result.skipped:
        print(f"skipping: {ev.name} ({ev.reason})", file=sys.stderr)

    ran_out: list[dict[str, Any]] = []
    for ev in result.fired:
        out = dict(ev.result) if isinstance(ev.result, dict) else {"result": ev.result}
        out["_pipeline"] = ev.name
        ran_out.append(out)
    failed_out = [
        {
            "pipeline": ev.name,
            "error": ev.error_message,
            "error_type": ev.error_type,
            "attempt_count": ev.attempt_count,
            "gave_up": ev.gave_up,
        }
        for ev in result.failed
    ]
    skipped_out = [
        {"pipeline": ev.name, "reason": ev.reason}
        for ev in result.skipped
    ]
    print(json.dumps(
        {"ran": ran_out, "failed": failed_out, "skipped": skipped_out},
        default=str,
    ))
    return 1 if failed_out else 0


def _open_run_log_or_none(args: argparse.Namespace) -> p.RunLog | None:
    """Resolve --run-log-url / --run-log-path / --no-run-log to a RunLog or None.

    Resolution order (highest priority first):
      1. `--no-run-log` → None
      2. `--run-log-url <url>` (any scheme; e.g. postgres://, s3://)
      3. `--run-log-path <path>` (legacy; back-compat alias for sqlite:///)
      4. `$EMATIX_FLOW_RUN_LOG_URL` env var
      5. `$EMATIX_FLOW_RUN_LOG_PATH` env var (legacy)
      6. Default: ~/.ematix-flow/run_log.db (SQLite)

    Graceful degradation: if the resolved backend can't be opened
    (read-only filesystem, network unreachable, bad credentials, etc.)
    we print a warning to stderr and return None — the CLI continues
    to fire pipelines but without persistence. Operator intent ("run
    my schedule") is still served.
    """
    import os

    from ematix_flow.run_log import from_url

    if getattr(args, "no_run_log", False):
        return None

    url = getattr(args, "run_log_url", None)
    if url is None:
        legacy_path = getattr(args, "run_log_path", None)
        if legacy_path:
            url = legacy_path  # bare path is a valid `from_url` argument
        else:
            url = os.environ.get("EMATIX_FLOW_RUN_LOG_URL")
    if url is None:
        env_path = os.environ.get("EMATIX_FLOW_RUN_LOG_PATH")
        if env_path:
            url = env_path
    if url is None:
        url = _default_run_log_path()

    try:
        return from_url(url)
    except (OSError, PermissionError) as e:
        print(
            f"warning: could not open run-log at {url!r}: {e}. "
            f"Continuing without durable run history. Pass --no-run-log "
            f"to silence, or --run-log-url <url> to point at a writable "
            f"location.",
            file=sys.stderr,
        )
        return None
    except Exception as e:
        print(
            f"warning: run-log backend {url!r} failed to open: "
            f"{type(e).__name__}: {e}. Continuing without durable run "
            f"history.",
            file=sys.stderr,
        )
        return None


def _default_run_log_path() -> str:
    """`~/.ematix-flow/run_log.db` unless EMATIX_FLOW_RUN_LOG_PATH overrides."""
    import os
    env = os.environ.get("EMATIX_FLOW_RUN_LOG_PATH")
    if env:
        return env
    return os.path.expanduser("~/.ematix-flow/run_log.db")


def _open_alerters(args: argparse.Namespace) -> list:
    """Resolve --alerter / $EMATIX_FLOW_ALERTERS into a list of Alerter
    instances. Flags win over env; comma-separated URLs allowed in env.
    A bad URL prints a warning to stderr and is skipped — the rest of
    the list still loads. Default: empty list."""
    import os

    from ematix_flow.alerters import from_url as alerter_from_url

    flag_list = getattr(args, "alerter", None)
    if flag_list:
        urls = list(flag_list)
    else:
        env = os.environ.get("EMATIX_FLOW_ALERTERS", "")
        urls = [u.strip() for u in env.split(",") if u.strip()]

    out = []
    for url in urls:
        try:
            out.append(alerter_from_url(url))
        except Exception as e:
            print(
                f"warning: skipping alerter {url!r}: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )
    return out


def _open_metrics(args: argparse.Namespace):
    """Resolve --metrics / $EMATIX_FLOW_METRICS into a MetricsSink.
    Flag wins over env; default is NullSink. A bad URL warns and
    falls back to NullSink."""
    import os

    from ematix_flow.metrics import NullSink
    from ematix_flow.metrics import from_url as metrics_from_url

    url = getattr(args, "metrics", None) or os.environ.get("EMATIX_FLOW_METRICS")
    if not url:
        return NullSink()
    try:
        return metrics_from_url(url)
    except Exception as e:
        print(
            f"warning: metrics URL {url!r} failed to construct: "
            f"{type(e).__name__}: {e}. Falling back to NullSink "
            f"(metrics will not be recorded).",
            file=sys.stderr,
        )
        return NullSink()


def _add_observability_args(parser: argparse.ArgumentParser) -> None:
    """Shared --alerter (repeatable) and --metrics flags for run-due.

    Alerter URL schemes: stdout://, slack://hooks.slack.com/...,
    https://hooks.slack.com/... (passthrough).

    Metrics URL schemes: null://, stdout://, memory://,
    prometheus://[:port], otlp://endpoint, otlp+grpc://endpoint,
    otlp+http://endpoint.

    Default footprint is zero — no alerters, NullSink for metrics —
    so observability is fully opt-in.
    """
    parser.add_argument(
        "--alerter",
        action="append",
        default=None,
        help="alerter URL; repeatable. Examples: stdout://, "
        "slack://hooks.slack.com/services/X/Y/Z. Falls back to "
        "$EMATIX_FLOW_ALERTERS (comma-separated list).",
    )
    parser.add_argument(
        "--metrics",
        default=None,
        help="metrics sink URL. Examples: prometheus://:9090, "
        "otlp://collector:4317, stdout://. Falls back to "
        "$EMATIX_FLOW_METRICS. Default: no metrics.",
    )
    parser.add_argument(
        "--traces",
        default=None,
        help="OpenTelemetry traces URL — emits one span per "
        "pipeline run. Examples: otel://stdout, "
        "otel+otlp+grpc://collector:4317, otel+otlp+http://collector:4318. "
        "Falls back to $EMATIX_FLOW_TRACES. Default: no traces.",
    )


def _add_run_log_args(parser: argparse.ArgumentParser) -> None:
    """Shared --run-log-path / --no-run-log flags for run-due + status.

    Persistence location resolution order:
      1. `--run-log-path PATH` on the CLI (highest priority)
      2. `EMATIX_FLOW_RUN_LOG_PATH` env var
      3. Default: `~/.ematix-flow/run_log.db`

    Any path the user can write to works — local disk, mounted volume,
    NFS share, tmpfs. Parent directories are created on first use.
    For ephemeral environments (CI, lambdas with read-only FS) pass
    `--no-run-log` to skip persistence entirely.
    """
    parser.add_argument(
        "--run-log-url",
        default=None,
        help="URL identifying the run-history backend. Supported schemes: "
        "sqlite:///path, memory://, postgres://..., postgresql://..., "
        "mysql://..., mariadb://..., duckdb:///path, duckdb://:memory:, "
        "s3://bucket/prefix, gs://bucket/prefix, "
        "azure://account/container/prefix. A bare path is treated as SQLite.",
    )
    parser.add_argument(
        "--run-log-path",
        default=None,
        help="(legacy) SQLite-only alias for --run-log-url; kept for back-compat. "
        "Falls back to $EMATIX_FLOW_RUN_LOG_URL, then $EMATIX_FLOW_RUN_LOG_PATH, "
        "then ~/.ematix-flow/run_log.db. Parent dirs are created on first use.",
    )
    parser.add_argument(
        "--no-run-log",
        action="store_true",
        help="don't persist run history. Useful for CI, ephemeral hosts, "
        "or testing.",
    )


def _upstream_ok(upstream: str, max_age_secs: int | None, now: datetime) -> bool:
    """Mirror of `pipeline._upstream_is_fresh` for one upstream — used to
    enumerate which specific upstream(s) are stale when logging skips."""
    rec = p._LAST_RUN.get(upstream)
    if rec is None:
        return False
    ts, ok = rec
    if not ok:
        return False
    if max_age_secs is None:
        return True
    return (now - ts).total_seconds() <= max_age_secs


def _cmd_scheduler(args: argparse.Namespace) -> int:
    """Ω.W.6: run the long-running scheduler loop.

    Imports the user's module, opens the configured RunLog, builds
    the Executor from the URL, and hands off to
    `ematix_flow.scheduler.run_scheduler`. Returns when the loop
    exits (only on --max-iterations or SIGTERM).
    """
    import logging as _logging

    from ematix_flow.scheduler import executor_from_url, run_scheduler

    # Surface scheduler INFO-level events to stderr so operators can
    # watch sweep / leader-acquire / dispatch / release in real time.
    # Idempotent — `basicConfig` is a no-op if a handler is already
    # configured (e.g. by a host process embedding flow as a library).
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    print_banner()
    run_log = _open_run_log_or_none(args)
    if run_log is None:
        print(
            "error: scheduler requires a durable RunLog. Pass "
            "--run-log-url or remove --no-run-log.",
            file=sys.stderr,
        )
        return 2

    # Resolve the URL the scheduler hands to workers. The
    # `_open_run_log_or_none` helper already resolved the URL it
    # opened; surface it for workers. If the URL came from
    # default-path resolution, reconstruct the sqlite:// form.
    run_log_url = _resolved_run_log_url(args)

    try:
        executor = executor_from_url(args.executor)
    except (ValueError, ImportError) as e:
        print(f"error: --executor {args.executor!r}: {e}", file=sys.stderr)
        return 2

    alerters = _open_alerters(args)
    metrics_sink = _open_metrics(args)
    alerter_urls = _resolved_alerter_urls(args)
    metrics_url = _resolved_metrics_url(args)
    _configure_tracing_from_args(args)

    try:
        run_scheduler(
            module=args.module,
            run_log=run_log,
            run_log_url=run_log_url,
            executor=executor,
            alerter_urls=alerter_urls,
            metrics_url=metrics_url,
            alerters=alerters,
            metrics=metrics_sink,
            poll_interval_seconds=args.poll_interval,
            lease_seconds=args.lease_seconds,
            interval_seconds=args.interval,
            worker_id=args.worker_id,
            max_iterations=args.max_iterations,
        )
    finally:
        try:
            metrics_sink.close()
        except Exception:
            pass
        try:
            run_log.close()
        except Exception:
            pass
    return 0


def _resolved_run_log_url(args: argparse.Namespace) -> str:
    """The URL workers should use to write their outcome back —
    same resolution chain as `_open_run_log_or_none` but returning
    the string form, not the opened instance."""
    import os

    url = getattr(args, "run_log_url", None)
    if url:
        return url
    legacy = getattr(args, "run_log_path", None)
    if legacy:
        return legacy if "://" in legacy else f"sqlite://{legacy}"
    env_url = os.environ.get("EMATIX_FLOW_RUN_LOG_URL")
    if env_url:
        return env_url
    env_path = os.environ.get("EMATIX_FLOW_RUN_LOG_PATH")
    if env_path:
        return env_path if "://" in env_path else f"sqlite://{env_path}"
    return f"sqlite://{_default_run_log_path()}"


def _resolved_alerter_urls(args: argparse.Namespace) -> list[str]:
    """Mirror `_open_alerters` resolution but return the URL strings
    workers should use, not the live Alerter instances."""
    import os

    flag_list = getattr(args, "alerter", None)
    if flag_list:
        return list(flag_list)
    env = os.environ.get("EMATIX_FLOW_ALERTERS", "")
    return [u.strip() for u in env.split(",") if u.strip()]


def _resolved_metrics_url(args: argparse.Namespace) -> str | None:
    import os

    return getattr(args, "metrics", None) or os.environ.get(
        "EMATIX_FLOW_METRICS_URL"
    )


def _configure_tracing_from_args(args: argparse.Namespace) -> None:
    """Resolve --traces (or $EMATIX_FLOW_TRACES) into a global OTEL tracer.

    Best-effort: if the user supplied no URL, leave the tracer at its
    default no-op state. If they did supply a URL but the OTel SDK
    isn't installed, surface the actionable hint and exit non-zero so
    the misconfiguration doesn't silently degrade to no traces.
    """
    import os

    url = getattr(args, "traces", None) or os.environ.get("EMATIX_FLOW_TRACES")
    if not url:
        return
    from ematix_flow.tracing import configure_tracer_from_url

    configure_tracer_from_url(url)


def _cmd_status(args: argparse.Namespace) -> int:
    """Phase Ω.3: operator view of pipeline state.

    Imports the user's module (which registers pipelines) and prints
    the in-process state from `_LAST_RUN` + `_ATTEMPT_STATE`. With
    `--format json`, dumps the snapshot directly; default is the
    fixed-width text table from `render_status_table`.
    """
    _import_user_module(args.module)
    # Phase Ω.D1a: pull prior state off disk before snapshotting so
    # the table reflects what last night's cron actually did.
    run_log = _open_run_log_or_none(args)
    if run_log is not None:
        run_log.restore_into_process()
    snapshot = p.status_snapshot()
    if args.format == "json":
        def _ser(o: Any) -> Any:
            import datetime as _dt
            if isinstance(o, _dt.datetime):
                return o.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            if isinstance(o, tuple):
                return list(o)
            raise TypeError(f"unserialisable: {type(o).__name__}")
        print(json.dumps(snapshot, default=_ser, indent=2))
        return 0
    print(p.render_status_table(snapshot))
    return 0


def _cmd_connections_list(args: argparse.Namespace) -> int:
    entries = config.list_connections()
    if not entries:
        print("no connections configured")
        return 0
    width = max(len(name) for name in entries)
    for name in sorted(entries):
        print(f"{name:<{width}}  {entries[name]}")
    return 0


def _cmd_connections_check(args: argparse.Namespace) -> int:
    ok, message = config.check_connection(args.name)
    if ok:
        print(f"{args.name}: ok ({message})")
        return 0
    print(f"{args.name}: unreachable — {message}", file=sys.stderr)
    return 2


def _cmd_doctor(args: argparse.Namespace) -> int:
    """``flow doctor [--module M]`` — probe every registered typed
    connection and print a green/red liveness report.

    Loads the user's pipeline module (if given) so connections
    declared in it land in the registry before we probe.
    """
    from ematix_flow import doctor

    if getattr(args, "module", None):
        _import_user_module(args.module)
    reports = doctor.run_doctor()
    print(doctor.format_doctor_report(reports))
    # Exit 1 if any probe failed — useful as a pre-deploy gate.
    return 1 if any(r.is_fail for r in reports) else 0


def _cmd_logs(args: argparse.Namespace) -> int:
    """``flow logs <run_id>`` — print captured stdout/stderr for a
    past run. Reads from ``$EMATIX_FLOW_LOGS_DIR`` (or
    ``~/.ematix-flow/logs/``). Logs are only written when the
    pipeline executed under ``EMATIX_FLOW_CAPTURE_LOGS=1`` — see
    USER_GUIDE for the capture toggle."""
    from ematix_flow.logs import logs_dir, read_run_logs

    text = read_run_logs(args.run_id)
    if text is None:
        print(
            f"flow logs: no log file for run_id={args.run_id!r} under "
            f"{logs_dir()}. Logs are only captured when "
            f"EMATIX_FLOW_CAPTURE_LOGS=1 was set during the run.",
            file=sys.stderr,
        )
        return 1
    if args.tail is not None:
        lines = text.splitlines()
        text = "\n".join(lines[-args.tail:])
        if text and not text.endswith("\n"):
            text += "\n"
    print(text, end="")
    return 0


def _cmd_init(args: argparse.Namespace) -> int:
    """``flow init <path> [--force]`` — scaffold a new ematix-flow
    project: pipelines.py + connections.toml + Dockerfile + flow.service
    + .gitignore + README.md. Maven-archetype-style: enough to be
    runnable immediately."""
    from pathlib import Path

    from ematix_flow.init_scaffold import scaffold_project

    target = Path(args.path).resolve()
    try:
        written = scaffold_project(target, force=args.force)
    except FileExistsError as exc:
        print(f"flow init: {exc}", file=sys.stderr)
        return 1
    print(f"flow init: scaffolded {len(written)} files in {target}")
    for path in written:
        print(f"  + {path.relative_to(target)}")
    print(
        "\nNext: `cd " + str(target) + " && flow doctor --module pipelines`"
    )
    return 0


def _cmd_secrets_test(args: argparse.Namespace) -> int:
    """``flow secrets test '${vault:foo#bar}'`` — resolve a single
    ``${...}`` reference via the registered resolver chain and print
    the result. Redacted by default (first/last 2 chars + length);
    pass ``--show`` to print verbatim."""
    from ematix_flow.secrets import MissingSecretError, expand

    ref = args.reference
    # Accept both `${vault:foo}` and bare `vault:foo` — wrap the
    # bare form so ``expand`` sees a recognisable reference.
    if not (ref.startswith("${") and ref.endswith("}")):
        ref = "${" + ref + "}"
    try:
        resolved = expand(ref)
    except MissingSecretError as exc:
        print(f"{args.reference}: {exc}", file=sys.stderr)
        return 1
    if resolved is None or resolved == "":
        print(f"{args.reference}: <empty>")
        return 0
    if args.show:
        print(resolved)
    else:
        # Redact: first 2 + last 2 chars + length. Mirrors the way
        # SSH / k8s show fingerprints without leaking the value.
        if len(resolved) <= 4:
            display = "*" * len(resolved)
        else:
            display = f"{resolved[:2]}...{resolved[-2:]}"
        print(f"{display}  ({len(resolved)} chars; pass --show to reveal)")
    return 0


def _cmd_preview(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    try:
        result = p.preview(args.name)
    except KeyError:
        print(f"error: no pipeline named {args.name!r}", file=sys.stderr)
        return 2
    except TypeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(result.to_json())
        return 0
    from ematix_flow.preview import render_text

    use_color = not args.no_color
    print(render_text(result, verbose=args.verbose, use_color=use_color))
    return 0


def _cmd_dry_run(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    try:
        result = p.dry_run(args.name)
    except KeyError:
        print(f"error: no pipeline named {args.name!r}", file=sys.stderr)
        return 2
    except TypeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except NotImplementedError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(result.to_json())
        return 0
    from ematix_flow.preview import render_text

    use_color = not args.no_color
    print(render_text(result, verbose=args.verbose, use_color=use_color))
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    try:
        result = p.validate(args.name)
    except KeyError:
        print(f"error: no pipeline named {args.name!r}", file=sys.stderr)
        return 2
    except TypeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(
            json.dumps(
                {
                    "pipeline_name": result.pipeline_name,
                    "ok": result.ok,
                    "source_sql": result.source_sql,
                    "errors": result.errors,
                    "target_connection_name": result.target_connection_name,
                }
            )
        )
        return 0 if result.ok else 1
    if result.ok:
        print(f"{result.pipeline_name}: ok")
        return 0
    print(f"{result.pipeline_name}: failed", file=sys.stderr)
    for err in result.errors:
        print(f"  - {err}", file=sys.stderr)
    return 1


def _cmd_connections_set(args: argparse.Namespace) -> int:
    if "=" not in args.assignment:
        print(
            f"error: expected NAME=DSN, got {args.assignment!r}",
            file=sys.stderr,
        )
        return 2
    name, _, dsn = args.assignment.partition("=")
    if not name or not dsn:
        print(
            f"error: NAME and DSN must both be non-empty in {args.assignment!r}",
            file=sys.stderr,
        )
        return 2
    path = config.set_connection(name, dsn)
    print(f"wrote connection {name!r} to {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flow", description="ematix-flow CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    list_p = sub.add_parser(
        "list", help="list registered pipelines and transforms"
    )
    list_p.add_argument(
        "--module", required=True, help="dotted module path that registers pipelines"
    )
    list_p.add_argument("--format", choices=["text", "json"], default="text")
    list_p.set_defaults(func=_cmd_list)

    run_p = sub.add_parser("run", help="run a pipeline by name")
    run_p.add_argument("--module", required=True)
    run_p.add_argument("name", help="pipeline name (matches @register(name=...))")
    # Ω.W.3 worker-side flags. None of these are required when
    # invoked by a human — they're populated by the scheduler's
    # Executor when it dispatches a per-fire worker.
    run_p.add_argument(
        "--claim-token",
        default=None,
        help="RunLog claim token issued by the scheduler. When set, "
        "the worker heartbeats the lease for the duration of the run "
        "and releases the claim on exit.",
    )
    run_p.add_argument(
        "--lease-seconds",
        type=int,
        default=300,
        help="Lease duration the scheduler granted (default 300). "
        "The worker's heartbeat keeps it pushed out by this many "
        "seconds each interval.",
    )
    run_p.add_argument(
        "--heartbeat-interval",
        type=int,
        default=100,
        help="Seconds between heartbeat calls (default 100). Should "
        "be a fraction of --lease-seconds so the scheduler sees two "
        "missed windows before declaring the worker dead.",
    )
    _add_run_log_args(run_p)
    _add_observability_args(run_p)
    run_p.set_defaults(func=_cmd_run)

    # Π.3: streaming-pipeline subcommands. `consume` runs a single
    # pipeline by name; `consume-list` lists what the imported module
    # registers.
    consume_p = sub.add_parser(
        "consume",
        help="run a streaming pipeline declared via "
        "@ematix.streaming_pipeline by name",
    )
    consume_p.add_argument(
        "--module",
        required=True,
        help="dotted module path that registers streaming pipelines",
    )
    consume_p.add_argument(
        "name",
        help="streaming pipeline name "
        "(matches @ematix.streaming_pipeline(name=...))",
    )
    consume_p.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="if set, expose Prometheus metrics on 127.0.0.1:<PORT>/metrics",
    )
    consume_p.add_argument(
        "--run-log-url",
        default=None,
        help="if set (with --metrics-port), write a streaming RunRecord + "
        "periodically (~30s) snapshot throughput/batch-cycle into its extras. "
        "The Web UI surfaces these as live stats. Falls back to "
        "$EMATIX_FLOW_RUN_LOG_URL. Supported schemes match `flow run-due`.",
    )
    consume_p.set_defaults(func=_cmd_consume)

    consume_list_p = sub.add_parser(
        "consume-list",
        help="list streaming pipelines registered by the imported module",
    )
    consume_list_p.add_argument("--module", required=True)
    consume_list_p.add_argument(
        "--format", choices=["text", "json"], default="text"
    )
    consume_list_p.set_defaults(func=_cmd_consume_list)

    status_p = sub.add_parser(
        "status",
        help="operator view of pipeline state (last run, retry status, depends_on)",
    )
    status_p.add_argument("--module", required=True)
    status_p.add_argument("--format", choices=["text", "json"], default="text")
    _add_run_log_args(status_p)
    status_p.set_defaults(func=_cmd_status)

    # Ω.W.6: long-running scheduler daemon. Claims pipelines through
    # the RunLog lease layer + dispatches them via an Executor URL
    # (subprocess / k8s / lambda).
    sched_p = sub.add_parser(
        "scheduler",
        help="long-running scheduler: claims pipelines and dispatches "
        "them to the configured Executor (Ω.W)",
    )
    sched_p.add_argument("--module", required=True)
    sched_p.add_argument(
        "--executor",
        required=True,
        help="Executor URL: subprocess://, subprocess+python://, "
        "k8s://<namespace>?image=<image>[&service-account=<sa>], "
        "lambda://<function-name>[?qualifier=<alias>]",
    )
    sched_p.add_argument(
        "--poll-interval",
        type=int,
        default=10,
        help="seconds between scheduler ticks (default 10)",
    )
    sched_p.add_argument(
        "--lease-seconds",
        type=int,
        default=300,
        help="per-pipeline claim lease (default 300). Worker "
        "heartbeats keep it pushed out.",
    )
    sched_p.add_argument(
        "--interval",
        type=int,
        default=60,
        help="cron-match window in seconds (default 60). Mirrors the "
        "flow run-due --interval semantics.",
    )
    sched_p.add_argument(
        "--worker-id",
        default=None,
        help="identifier this scheduler reports. Default: "
        "scheduler-<hostname>-<pid>",
    )
    sched_p.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="(testing) stop after N loop ticks; default loops forever",
    )
    _add_run_log_args(sched_p)
    _add_observability_args(sched_p)
    sched_p.set_defaults(func=_cmd_scheduler)

    due_p = sub.add_parser(
        "run-due", help="run pipelines whose schedule fires in (now-interval, now]"
    )
    due_p.add_argument("--module", required=True)
    due_p.add_argument(
        "--now",
        default=None,
        help="ISO8601 datetime (default: current UTC time). Useful for testing.",
    )
    due_p.add_argument(
        "--interval",
        type=int,
        default=60,
        help="window size in seconds (default 60); should match how often "
        "you invoke `flow run-due`",
    )
    _add_run_log_args(due_p)
    _add_observability_args(due_p)
    due_p.set_defaults(func=_cmd_run_due)

    # `flow preview / dry-run` subcommands (Phase 25).
    preview_p = sub.add_parser(
        "preview",
        help="show what a pipeline would do without committing",
    )
    preview_p.add_argument("name", help="pipeline name (matches @ematix.pipeline name=)")
    preview_p.add_argument("--module", required=True)
    preview_p.add_argument("-v", "--verbose", action="store_true")
    preview_p.add_argument("--format", choices=["text", "json"], default="text")
    preview_p.add_argument("--no-color", action="store_true")
    preview_p.set_defaults(func=_cmd_preview)

    dry_p = sub.add_parser(
        "dry-run",
        help="execute the pipeline inside a transaction and ROLLBACK at end",
    )
    dry_p.add_argument("name", help="pipeline name (matches @ematix.pipeline name=)")
    dry_p.add_argument("--module", required=True)
    dry_p.add_argument("-v", "--verbose", action="store_true")
    dry_p.add_argument("--format", choices=["text", "json"], default="text")
    dry_p.add_argument("--no-color", action="store_true")
    dry_p.set_defaults(func=_cmd_dry_run)

    # `flow validate` (Phase 26): EXPLAIN the synthesized source SQL.
    val_p = sub.add_parser(
        "validate",
        help="EXPLAIN the synthesized source SQL against the target connection",
    )
    val_p.add_argument("name", help="pipeline name (matches @ematix.pipeline name=)")
    val_p.add_argument("--module", required=True)
    val_p.add_argument("--format", choices=["text", "json"], default="text")
    val_p.set_defaults(func=_cmd_validate)

    # `flow transform {list,run}` (Phase 27d).
    xfm_p = sub.add_parser(
        "transform", help="manage @ematix.transform-decorated callables"
    )
    xfm_sub = xfm_p.add_subparsers(dest="xfm_cmd", required=True)

    xfm_list = xfm_sub.add_parser("list", help="list registered transforms")
    xfm_list.add_argument("--module", required=True)
    xfm_list.add_argument("--format", choices=["text", "json"], default="text")
    xfm_list.set_defaults(func=_cmd_transform_list)

    xfm_run = xfm_sub.add_parser("run", help="run a transform standalone")
    xfm_run.add_argument("--module", required=True)
    xfm_run.add_argument("name")
    xfm_run.set_defaults(func=_cmd_transform_run)

    # `flow connections {list,check,set}` subcommands (Phase 21).
    conn_p = sub.add_parser("connections", help="manage named DB connections")
    conn_sub = conn_p.add_subparsers(dest="conn_cmd", required=True)

    conn_list = conn_sub.add_parser("list", help="list configured connections")
    conn_list.set_defaults(func=_cmd_connections_list)

    conn_check = conn_sub.add_parser(
        "check", help="verify a configured connection is reachable"
    )
    conn_check.add_argument("name")
    conn_check.set_defaults(func=_cmd_connections_check)

    conn_set = conn_sub.add_parser(
        "set",
        help="persist a connection to ~/.ematix-flow/connections.toml",
    )
    conn_set.add_argument(
        "assignment",
        metavar="NAME=DSN",
        help="e.g., warehouse=postgres://user:pass@host/db",
    )
    conn_set.set_defaults(func=_cmd_connections_set)

    # ``flow logs <run_id>`` — print captured stdout/stderr from a past run.
    logs_p = sub.add_parser(
        "logs",
        help="print captured stdout/stderr from a past pipeline run",
    )
    logs_p.add_argument("run_id", help="run id (from the run-log API or scheduler)")
    logs_p.add_argument(
        "--tail",
        type=int,
        default=None,
        help="show only the last N lines (default: full log)",
    )
    logs_p.set_defaults(func=_cmd_logs)

    # ``flow init <path>`` — scaffold a new project (Maven-archetype-style).
    init_p = sub.add_parser(
        "init",
        help="scaffold a new ematix-flow project (pipelines.py + "
        "connections.toml + Dockerfile + systemd unit)",
    )
    init_p.add_argument(
        "path",
        help="target directory for the new project (created if missing)",
    )
    init_p.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing files in the target directory",
    )
    init_p.set_defaults(func=_cmd_init)

    # ``flow doctor`` — health-check every registered typed connection.
    # Exit code is 1 if any probe failed; useful as a pre-deploy gate.
    doctor_p = sub.add_parser(
        "doctor",
        help="probe every registered connection and report green/red",
    )
    doctor_p.add_argument(
        "--module",
        help="user pipeline module to import first (so connections "
        "declared in it land in the registry before probing)",
    )
    doctor_p.set_defaults(func=_cmd_doctor)

    # ``flow secrets test`` — resolve a single ${...} reference.
    secrets_p = sub.add_parser(
        "secrets",
        help="debug secret-resolver setup (e.g. Vault / AWS / GCP)",
    )
    secrets_sub = secrets_p.add_subparsers(dest="secrets_cmd", required=True)
    secrets_test = secrets_sub.add_parser(
        "test",
        help="resolve a single ${vault:...} / ${aws:...} / ${gcp:...} "
        "reference and print the result (redacted unless --show)",
    )
    secrets_test.add_argument(
        "reference",
        metavar="REF",
        help="the reference, e.g. '${vault:secret/myapp#db_password}' "
        "or bare 'vault:secret/myapp#db_password'",
    )
    secrets_test.add_argument(
        "--show",
        action="store_true",
        help="print the resolved value verbatim (default: redacted "
        "to first/last 2 chars + length)",
    )
    secrets_test.set_defaults(func=_cmd_secrets_test)

    # Phase 4 of "What's not shipped": web UI for run history +
    # restart / rerun / pause actions.
    web_p = sub.add_parser(
        "web",
        help="launch the ematix-flow web UI on http://127.0.0.1:8080 by default",
    )
    web_p.add_argument(
        "--bind",
        default="127.0.0.1",
        help="address to bind (default 127.0.0.1; binding to a non-loopback "
        "address logs a warning since Phase 4a ships without auth)",
    )
    web_p.add_argument(
        "--port",
        type=int,
        default=8080,
        help="port to listen on (default 8080)",
    )
    web_p.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="uvicorn log level (default info)",
    )
    web_p.add_argument(
        "--token",
        default=None,
        help="bearer token required on every /api/* call (except /api/health). "
        "Set this before binding to a non-loopback address. Falls back to "
        "$EMATIX_FLOW_WEB_TOKEN if not passed on the CLI.",
    )
    web_p.add_argument(
        "--run-log-url",
        default=None,
        help="if set, open this RunLog backend as the rich-history source so "
        "the UI shows real run records (incl. live streaming throughput). "
        "Falls back to $EMATIX_FLOW_RUN_LOG_URL. Without this flag, the UI "
        "still works but pipeline / job lists fall back to stub data.",
    )
    web_p.add_argument(
        "--module",
        action="append",
        default=None,
        help="import a pipeline module (e.g. `--module pipelines`) so the UI "
        "can render schedule, next_run_at, and the cross-pipeline DAG. "
        "Repeatable. Without this flag the UI still works but next-run + DAG "
        "fall back to history-only data.",
    )
    web_p.add_argument(
        "--datasource",
        action="append",
        default=None,
        metavar="NAME=URL",
        help="register a queryable data source for the SQL editor / charts / "
        "dashboards as NAME=CONNECTION_URL (e.g. "
        "--datasource warehouse=postgres://user:pass@host/db or "
        "--datasource local=duckdb:///data.db). Repeatable. Supported schemes: "
        "postgres:// , mysql:// , sqlite:///<path> , duckdb:///<path>.",
    )
    web_p.add_argument(
        "--analytics-db",
        default=None,
        metavar="PATH",
        help="SQLite file to persist saved queries / charts / dashboards. "
        "Falls back to $EMATIX_FLOW_ANALYTICS_DB. Without it, those persist "
        "only in memory and are lost on restart.",
    )
    web_p.set_defaults(func=_cmd_web)

    args = parser.parse_args(argv)
    return args.func(args)


def _parse_datasource_specs(specs) -> dict[str, str]:
    """Parse repeated ``--datasource NAME=URL`` values into a dict,
    warning on (and skipping) malformed entries."""
    out: dict[str, str] = {}
    for spec in specs or []:
        name, sep, url = spec.partition("=")
        name, url = name.strip(), url.strip()
        if not sep or not name or not url:
            print(
                f"warning: --datasource {spec!r} is not NAME=URL; ignoring",
                file=sys.stderr,
            )
            continue
        out[name] = url
    return out


def _cmd_web(args) -> int:
    """Launch the FastAPI server. Lazy-imports the web module so users
    who never run the UI don't pay the fastapi / uvicorn import cost.
    """
    try:
        from ematix_flow.web import run_server
    except ImportError as exc:
        print(
            f"error: {exc}\n"
            "Install the web extra: pip install ematix-flow[web]",
            file=sys.stderr,
        )
        return 1
    # Import pipeline modules so the in-process registry is populated;
    # the /api/pipelines and /api/dag endpoints look up schedule +
    # depends_on metadata via _registry_lookup, which is empty until
    # the @ematix.pipeline / @ematix.streaming_pipeline decorators fire.
    for mod_name in (getattr(args, "module", None) or []):
        try:
            importlib.import_module(mod_name)
        except Exception as exc:
            print(
                f"warning: --module {mod_name!r} failed to import "
                f"({exc!r}); UI schedule + DAG will be empty for its "
                "pipelines",
                file=sys.stderr,
            )
    # Bearer-token auth — set via --token, or fall back to the
    # EMATIX_FLOW_WEB_TOKEN env var so secrets stay out of shell history.
    token = args.token or os.environ.get("EMATIX_FLOW_WEB_TOKEN")
    # v0.5.0: opt-in rich-history wiring. Same RunLog URL the streaming
    # consumer + scheduler use; the SqliteRunLog now satisfies both
    # protocols against the same backing file.
    run_log_url = getattr(args, "run_log_url", None) or os.environ.get(
        "EMATIX_FLOW_RUN_LOG_URL"
    )
    history = None
    if run_log_url:
        try:
            from ematix_flow.run_log import from_url

            store = from_url(run_log_url)
        except Exception as exc:
            print(
                f"warning: --run-log-url {run_log_url!r} could not be opened "
                f"({exc!r}); web UI will fall back to stub data",
                file=sys.stderr,
            )
            store = None
        if store is not None and hasattr(store, "record_run_record"):
            history = store
        elif store is not None:
            print(
                f"warning: RunLog backend {type(store).__name__} doesn't "
                "implement the rich-history protocol; web UI will fall back "
                "to stub data. SqliteRunLog has the in-tree implementation.",
                file=sys.stderr,
            )
    # SQL editor / analytics data sources: --datasource NAME=URL (repeatable).
    datasources = _parse_datasource_specs(getattr(args, "datasource", None))

    # Persistence for saved queries / charts / dashboards.
    analytics_store = None
    analytics_db = getattr(args, "analytics_db", None) or os.environ.get(
        "EMATIX_FLOW_ANALYTICS_DB"
    )
    if analytics_db:
        try:
            from ematix_flow.web.analytics_store import AnalyticsStore

            analytics_store = AnalyticsStore(analytics_db)
        except Exception as exc:
            print(
                f"warning: --analytics-db {analytics_db!r} could not be opened "
                f"({exc!r}); saved queries / charts / dashboards will be "
                "in-memory only",
                file=sys.stderr,
            )

    run_server(
        host=args.bind, port=args.port, log_level=args.log_level,
        bearer_token=token,
        history=history,
        datasources=datasources or None,
        analytics_store=analytics_store,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
