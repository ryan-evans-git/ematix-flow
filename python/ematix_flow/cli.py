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
from datetime import datetime, timezone
from typing import Any

from ematix_flow import pipeline as p


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _import_user_module(name: str) -> None:
    importlib.import_module(name)


def _cmd_list(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    for sp in p.list_pipelines():
        print(f"{sp.name}\t{sp.schedule}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    try:
        result = p.run_pipeline(args.name)
    except KeyError:
        print(f"error: no pipeline named {args.name!r}", file=sys.stderr)
        return 2
    print(json.dumps(result, default=str))
    return 0


def _cmd_run_due(args: argparse.Namespace) -> int:
    _import_user_module(args.module)
    now = _parse_iso(args.now) if args.now else datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    results: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for sp in p.list_pipelines():
        if not p.is_due(sp.schedule, now, args.interval):
            continue
        print(f"firing: {sp.name}", file=sys.stderr)
        try:
            r = sp.fn()
        except Exception as e:  # noqa: BLE001 — surface and continue
            print(f"failed: {sp.name}: {e}", file=sys.stderr)
            failed.append({"pipeline": sp.name, "error": str(e)})
            continue
        out = dict(r) if isinstance(r, dict) else {"result": r}
        out["_pipeline"] = sp.name
        results.append(out)
    print(json.dumps({"ran": results, "failed": failed}, default=str))
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flow", description="ematix-flow CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    list_p = sub.add_parser("list", help="list registered pipelines")
    list_p.add_argument("--module", required=True, help="dotted module path that registers pipelines")
    list_p.set_defaults(func=_cmd_list)

    run_p = sub.add_parser("run", help="run a pipeline by name")
    run_p.add_argument("--module", required=True)
    run_p.add_argument("name", help="pipeline name (matches @register(name=...))")
    run_p.set_defaults(func=_cmd_run)

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

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
