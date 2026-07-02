"""Tests for strict_summarize.py — metadata header + isolated-layout parsing.

Run: python3 -m pytest scripts/bench/tests/ -q
"""

import json
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent.parent
REPO = BENCH_DIR.parent.parent


def run_summarize(tmp_path, run_contents, extra_args=()):
    runs = []
    for i, content in enumerate(run_contents, start=1):
        p = tmp_path / f"run-{i}.md"
        p.write_text(content)
        runs.append(str(p))
    out = tmp_path / "summary.md"
    cmd = [
        sys.executable,
        str(BENCH_DIR / "strict_summarize.py"),
        "--runs", *runs,
        "--out", str(out),
        *extra_args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc, out


ROW = "| Q{q:02d}  | {med:.2f} ± {sig:.2f} | 999.99 ± 1.00 | 999.99 ± 1.00 |\n"


def mk_run(medians: dict[int, float], sigma: float = 1.0) -> str:
    header = "| Query | ematix-flow | DuckDB | Polars |\n|---|---|---|---|\n"
    return header + "".join(
        ROW.format(q=q, med=m, sig=sigma) for q, m in medians.items()
    )


def test_basic_aggregation_unchanged(tmp_path):
    """Existing behavior: median-of-medians across runs."""
    proc, out = run_summarize(
        tmp_path,
        [mk_run({1: 100.0, 6: 10.0}), mk_run({1: 110.0, 6: 12.0}),
         mk_run({1: 105.0, 6: 11.0})],
    )
    assert proc.returncode == 0, proc.stderr
    text = out.read_text()
    assert "| Q01 | 105.00 |" in text
    assert "| Q06 | 11.00 |" in text


def test_env_json_metadata_header(tmp_path):
    """--env-json embeds machine/flag metadata in the summary header so
    no result table is machine-ambiguous."""
    env = {
        "chip": "Apple M4 Max",
        "perf_cores": 10,
        "efficiency_cores": 4,
        "ram_gb": 36,
        "macos": "25.5.0",
        "power_source": "AC Power",
        "git_sha": "abc1234",
        "git_dirty": False,
        "plan_cache": "off",
        "cache_policy": "warm",
        "emat_flags": {"EMAT_PLAN_CACHE": "0"},
        "engine_versions": {"duckdb": "1.2.0", "polars": "0.46"},
        "sf": 10,
    }
    env_path = tmp_path / "env.json"
    env_path.write_text(json.dumps(env))
    proc, out = run_summarize(
        tmp_path,
        [mk_run({1: 100.0}), mk_run({1: 101.0}), mk_run({1: 102.0})],
        extra_args=("--env-json", str(env_path)),
    )
    assert proc.returncode == 0, proc.stderr
    text = out.read_text()
    assert "Apple M4 Max" in text
    assert "abc1234" in text
    assert "plan_cache" in text or "plan cache" in text.lower()
    assert "warm" in text
    assert "SF=10" in text or '"sf": 10' in text or "sf: 10" in text


def test_env_json_missing_file_fails_loudly(tmp_path):
    proc, _ = run_summarize(
        tmp_path,
        [mk_run({1: 100.0}), mk_run({1: 101.0})],
        extra_args=("--env-json", str(tmp_path / "nope.json")),
    )
    assert proc.returncode != 0


def test_isolated_layout_concatenated_rows_parse(tmp_path):
    """Per-query isolation concatenates single-query tables into one
    run file; the parser must aggregate all rows found."""
    run = (
        mk_run({1: 100.0})
        + "\n"
        + mk_run({6: 10.0})
        + "\n"
        + mk_run({22: 5.0})
    )
    run2 = mk_run({1: 102.0}) + mk_run({6: 11.0}) + mk_run({22: 6.0})
    proc, out = run_summarize(tmp_path, [run, run2])
    assert proc.returncode == 0, proc.stderr
    text = out.read_text()
    for q in ("Q01", "Q06", "Q22"):
        assert f"| {q} |" in text
