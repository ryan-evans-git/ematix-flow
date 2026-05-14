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
import sys
from datetime import UTC, datetime
from typing import Any

from ematix_flow import config
from ematix_flow import pipeline as p
from ematix_flow._banner import print_banner


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _import_user_module(name: str) -> None:
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
    print_banner()
    _import_user_module(args.module)
    try:
        result = p.run_pipeline(args.name)
    except KeyError:
        print(f"error: no pipeline named {args.name!r}", file=sys.stderr)
        return 2
    print(json.dumps(result, default=str))
    return 0


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
    result = streaming.run_pipeline(
        config_str=toml, metrics_port=args.metrics_port
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
    # and --metrics URLs.
    alerters = _open_alerters(args)
    metrics_sink = _open_metrics(args)
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

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
