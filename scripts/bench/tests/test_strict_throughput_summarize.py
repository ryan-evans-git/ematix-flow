"""Tests for strict_throughput_summarize.py — TPC-H-style concurrent-stream
throughput aggregation.

Layout produced by strict_throughput.sh:

    $OUT/<engine>/s<N>/batch-<b>/stream-<s>.md   one triangulation table per stream
    $OUT/<engine>/s<N>/batch-<b>/batch.json      {"makespan_ms": ..., ...}

Run: python3 -m pytest scripts/bench/tests/ -q
"""

import json
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent.parent


def table(engine_col_ms: dict[int, float], engine: str) -> str:
    """Render a triangulation-style table with the given engine's column
    populated and the others em-dashed, matching write_benchmarks_md."""
    cols = {"ematix": 0, "duckdb": 1, "polars": 2}
    idx = cols[engine]
    lines = [
        "| Query | ematix-flow | DuckDB | Polars | Best |",
        "|------:|------------:|-------:|-------:|:-----|",
    ]
    for q, ms in engine_col_ms.items():
        cells = ["—", "—", "—"]
        cells[idx] = f"{ms:.2f} ± 0.10"
        lines.append(f"| Q{q:02d}  | {cells[0]} | {cells[1]} | {cells[2]} | x |")
    return "\n".join(lines) + "\n"


def mk_layout(tmp_path, engine, n_streams, batches, base_ms=100.0, makespan_ms=5000.0):
    root = tmp_path / engine / f"s{n_streams}"
    for b in range(1, batches + 1):
        bdir = root / f"batch-{b}"
        bdir.mkdir(parents=True)
        for s in range(1, n_streams + 1):
            qs = {q: base_ms + q + s + b for q in range(1, 23)}
            (bdir / f"stream-{s}.md").write_text(table(qs, engine))
        (bdir / "batch.json").write_text(json.dumps({
            "engine": engine, "streams": n_streams, "batch": b,
            "makespan_ms": makespan_ms + b * 10,
        }))
    return tmp_path


def run_summarize(out_root, extra=()):
    out = out_root / "throughput-summary.md"
    cmd = [sys.executable, str(BENCH_DIR / "strict_throughput_summarize.py"),
           "--run-dir", str(out_root), "--out", str(out), *extra]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc, out


def test_qph_and_makespan(tmp_path):
    mk_layout(tmp_path, "ematix", n_streams=2, batches=3, makespan_ms=10_000.0)
    proc, out = run_summarize(tmp_path)
    assert proc.returncode == 0, proc.stderr
    text = out.read_text()
    # First batch discarded; median makespan of batches 2,3 = 10_025 ms.
    assert "10025" in text.replace(",", "")
    # QPH = 2 streams * 22 queries / (10.025 s / 3600) ≈ 15800.5
    assert "15800" in text.replace(",", "")


def test_duckdb_column_parsed(tmp_path):
    """Parsing must be column-aware: a DuckDB-only run has '—' in the
    ematix column and numbers in the DuckDB column."""
    mk_layout(tmp_path, "duckdb", n_streams=1, batches=2)
    proc, out = run_summarize(tmp_path)
    assert proc.returncode == 0, proc.stderr
    text = out.read_text()
    assert "duckdb" in text.lower()
    assert "p95" in text.lower()


def test_percentiles_across_streams_and_batches(tmp_path):
    mk_layout(tmp_path, "ematix", n_streams=4, batches=3)
    proc, out = run_summarize(tmp_path)
    assert proc.returncode == 0, proc.stderr
    text = out.read_text()
    assert "p50" in text and "p95" in text and "p99" in text


def test_first_batch_discarded(tmp_path):
    """Batch 1 is cold-start; summary must state it was discarded."""
    mk_layout(tmp_path, "ematix", n_streams=1, batches=3)
    proc, out = run_summarize(tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert "discard" in out.read_text().lower()


def test_single_batch_fails_loudly(tmp_path):
    """One batch cannot survive discard-first; must error, not report."""
    mk_layout(tmp_path, "ematix", n_streams=1, batches=1)
    proc, _ = run_summarize(tmp_path)
    assert proc.returncode != 0
