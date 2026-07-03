"""Tests for strict_diff.py — neutral engine labels for cross-engine diffs."""

import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent.parent


def summary(medians: dict[int, tuple[float, float]]) -> str:
    """Render a strict-22q-summary-style table: | Qnn | median | mean | σ |."""
    lines = [
        "| Query | median ms | mean ms | σ ms | CV% | per-run medians |",
        "|------:|----------:|--------:|-----:|----:|:----------------|",
    ]
    for q, (med, sig) in medians.items():
        lines.append(f"| Q{q:02d} | {med:.2f} | {med:.2f} | {sig:.2f} | 1.00% | x |")
    return "\n".join(lines) + "\n"


def run_diff(tmp_path, a, b, extra=()):
    pa, pb = tmp_path / "a.md", tmp_path / "b.md"
    pa.write_text(summary(a))
    pb.write_text(summary(b))
    out = tmp_path / "diff.md"
    proc = subprocess.run(
        [sys.executable, str(BENCH_DIR / "strict_diff.py"),
         "--a", str(pa), "--b", str(pb), "--out", str(out), *extra],
        capture_output=True, text=True)
    return proc, out


def test_default_labels_backward_compatible(tmp_path):
    proc, out = run_diff(
        tmp_path,
        {1: (100.0, 1.0)},         # A
        {1: (80.0, 1.0)},          # B faster by 20 > 2σ
    )
    assert proc.returncode in (0, None) or proc.returncode == 0, proc.stderr
    text = out.read_text()
    assert "WIN" in text  # legacy verdict wording without labels


def test_engine_labels(tmp_path):
    proc, out = run_diff(
        tmp_path,
        {1: (100.0, 1.0), 6: (50.0, 1.0), 9: (70.0, 1.0)},   # A = ematix
        {1: (80.0, 1.0), 6: (60.0, 1.0), 9: (70.5, 1.0)},    # B = duckdb
        extra=("--label-a", "ematix", "--label-b", "duckdb"),
    )
    text = out.read_text()
    assert "duckdb faster" in text    # Q01: B beat A beyond the bar
    assert "ematix faster" in text    # Q06: A beat B beyond the bar
    assert "noise" in text            # Q09: inside 2σ
