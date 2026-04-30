"""Phase 25: pipeline preview + dry-run output rendering.

`PreviewResult` is the structured plan produced by `pipeline.preview(name)`
and the `.preview()` method on `@ematix.pipeline`-decorated functions.
It carries everything a user needs to answer "what would this pipeline
do?" — connection info, augmented target spec, resolved keys with the
reason they were picked, and the synthesized SQL plan per target.

Rendering uses `rich` for color in TTYs and falls back to plain text
when piped to a file or `NO_COLOR` is set.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class TargetPlan:
    """Per-target details for a preview / dry-run."""

    schema_qualified_name: str
    mode: str
    path: str  # "same_db" or "cross_db"
    path_reason: str

    augmented_columns: list[dict[str, Any]] = field(default_factory=list)
    declared_columns: list[str] = field(default_factory=list)  # non-augmented cols

    merge_keys: list[str] = field(default_factory=list)
    merge_keys_reason: str = ""

    compare_columns: list[str] = field(default_factory=list)
    compare_columns_reason: str = ""

    target_connection_name: str | None = None
    target_connection_info: dict[str, Any] | None = None

    plan_sql: list[str] = field(default_factory=list)
    plan_sql_label: str = ""  # e.g., "INSERT...SELECT" or "3-statement SCD2"

    # Dry-run only.
    dry_run_rows_affected: dict[str, int] = field(default_factory=dict)
    dry_run_error: str | None = None


@dataclass
class PreviewResult:
    """Structured output of `pipeline.preview(name)`."""

    pipeline_name: str
    schedule: str | None
    mode: str | None  # None for multi-target pipelines

    source_connection_name: str | None
    source_connection_info: dict[str, Any] | None

    source_sql: str

    targets: list[TargetPlan] = field(default_factory=list)

    is_dry_run: bool = False
    notes: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def render_text(result: PreviewResult, *, verbose: bool = False, use_color: bool = True) -> str:
    """Render a PreviewResult as text, optionally with rich colors."""
    from rich.console import Console
    from rich.syntax import Syntax

    console = Console(
        record=True,
        force_terminal=use_color and not _is_no_color_set(),
        no_color=not use_color or _is_no_color_set(),
        width=120,
    )

    label = "DRY RUN" if result.is_dry_run else "PREVIEW"
    color = "yellow" if result.is_dry_run else "cyan"
    console.print(
        f"[bold {color}]{label}[/]  "
        f"[bold]{result.pipeline_name}[/]  "
        f"[dim]schedule:[/] {result.schedule or '—'}",
    )

    # Header row.
    if result.mode:
        console.print(f"[dim]mode:[/] {result.mode}")
    if result.source_connection_name:
        info = result.source_connection_info or {}
        host = info.get("host", "?")
        db = info.get("dbname", "?")
        console.print(
            f"[dim]source connection:[/] [bold]{result.source_connection_name}[/] "
            f"[dim]({host}/{db})[/]"
        )
    console.print()

    # Multi-target compact mode.
    if not verbose and len(result.targets) > 1:
        console.print(f"[bold]targets ({len(result.targets)}):[/]")
        for t in result.targets:
            status = ""
            if t.dry_run_error:
                status = f" [red]✗ {t.dry_run_error[:60]}[/]"
            elif t.dry_run_rows_affected:
                rows = ", ".join(f"{k}={v}" for k, v in t.dry_run_rows_affected.items())
                status = f" [green]✓[/] [dim]({rows})[/]"
            keys = ", ".join(t.merge_keys) if t.merge_keys else "—"
            path_color = "green" if t.path == "same_db" else "yellow"
            console.print(
                f"  • {t.schema_qualified_name:<35}  "
                f"[dim]mode=[/]{t.mode:<10}  "
                f"[dim]keys=[/]({keys})  "
                f"[dim]path=[/][{path_color}]{t.path}[/]"
                f"{status}"
            )
        console.print()
        console.print(f"[dim]--verbose for SQL plans per target[/]")
    else:
        # Verbose / single-target.
        for i, t in enumerate(result.targets):
            if len(result.targets) > 1:
                console.print(f"[bold][{i + 1}/{len(result.targets)}] {t.schema_qualified_name}[/]")
            else:
                console.print(f"[bold]Target:[/] {t.schema_qualified_name}")
            path_color = "green" if t.path == "same_db" else "yellow"
            console.print(
                f"  [dim]Path:[/] [{path_color}]{t.path}[/]  "
                f"[italic dim]{t.path_reason}[/]"
            )
            if t.augmented_columns:
                col_lines = []
                for col in t.augmented_columns:
                    is_aug = col["name"] not in t.declared_columns
                    if is_aug:
                        col_lines.append(
                            f"    [dim italic]{col['name']}[/]"
                            " [italic dim](auto)[/]"
                        )
                    else:
                        col_lines.append(f"    {col['name']}")
                console.print(f"  [dim]Columns:[/]")
                for line in col_lines:
                    console.print(line)
            if t.merge_keys:
                reason = (
                    f" [italic dim][{t.merge_keys_reason}][/]"
                    if t.merge_keys_reason
                    else ""
                )
                console.print(
                    f"  [dim]Merge keys:[/] ({', '.join(t.merge_keys)}){reason}"
                )
            if t.compare_columns:
                reason = (
                    f" [italic dim][{t.compare_columns_reason}][/]"
                    if t.compare_columns_reason
                    else ""
                )
                console.print(
                    f"  [dim]Compare columns:[/] ({', '.join(t.compare_columns)}){reason}"
                )
            if t.dry_run_rows_affected:
                console.print(
                    "  [dim]Dry-run rows affected:[/] "
                    + ", ".join(f"{k}={v}" for k, v in t.dry_run_rows_affected.items())
                )
            if t.dry_run_error:
                console.print(f"  [red]Dry-run error:[/] {t.dry_run_error}")
            if t.plan_sql:
                console.print(f"  [dim]SQL plan ({len(t.plan_sql)} statement(s)):[/]")
                for j, stmt in enumerate(t.plan_sql, 1):
                    console.print(f"    [dim][{j}/{len(t.plan_sql)}][/]")
                    syntax = Syntax(stmt, "sql", theme="ansi_dark", word_wrap=True)
                    console.print(syntax)
            console.print()

    # Notes.
    if result.notes:
        console.print("[bold yellow]Notes:[/]")
        for note in result.notes:
            console.print(f"  • {note}")

    return console.export_text()


def _is_no_color_set() -> bool:
    import os

    return bool(os.environ.get("NO_COLOR"))


__all__ = ["PreviewResult", "TargetPlan", "render_text"]
