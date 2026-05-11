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
    results: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for sp in p.list_pipelines():
        if not p.is_due(sp.schedule, now, args.interval):
            continue
        print(f"firing: {sp.name}", file=sys.stderr)
        try:
            r = sp.fn()
        except Exception as e:
            print(f"failed: {sp.name}: {e}", file=sys.stderr)
            failed.append({"pipeline": sp.name, "error": str(e)})
            continue
        out = dict(r) if isinstance(r, dict) else {"result": r}
        out["_pipeline"] = sp.name
        results.append(out)
    print(json.dumps({"ran": results, "failed": failed}, default=str))
    return 1 if failed else 0


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
