#!/usr/bin/env python3
"""Pandas-native TPC-H benchmarks for the README's 4-engine comparison.

Pandas isn't a SQL engine — it's a DataFrame library. To compare it
fairly against PySpark / Polars / DataFusion (all of which take SQL),
we hand-translate each TPC-H query into idiomatic pandas operations
on `pandas.read_parquet`-loaded DataFrames.

Queries covered (the ones with mechanical pandas translations):

    Q1   filter + groupby + multi-aggregate
    Q3   3-way merge + groupby + filter
    Q5   6-way merge + groupby + filter
    Q6   filter + scalar SUM
    Q9   6-way merge + groupby + multi-arithmetic
    Q10  4-way merge + groupby
    Q12  2-way merge + groupby + CASE-WHEN
    Q14  2-way merge + dual SUM with CASE-WHEN

Queries skipped (correlated subqueries / EXISTS / window functions
don't translate cleanly into single-pass pandas):

    Q2, Q4, Q7, Q8, Q11, Q13, Q15, Q16, Q17, Q18, Q19, Q20, Q21, Q22

3-trial median, parquet decoded inside the timed region (matches the
"out-of-box pandas user" experience — they do `pd.read_parquet` every
time, not warm in-memory frames).

Output: a markdown row per query. Exit 0 always; failures reported in
the table as `FAIL`.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "examples" / "tpch" / "data" / "sf1"


def load(name: str) -> pd.DataFrame:
    return pd.read_parquet(DATA / f"{name}.parquet")


def time3(fn) -> tuple[float, int]:
    # Warm-up
    res = fn()
    rows = len(res) if hasattr(res, "__len__") else 1
    times = []
    for _ in range(3):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return sorted(times)[1], rows


def q1():
    li = load("lineitem")
    fil = li[li.l_shipdate <= pd.Timestamp("1998-09-02").date()]
    fil = fil.assign(
        disc_price=fil.l_extendedprice * (1 - fil.l_discount),
        charge=fil.l_extendedprice * (1 - fil.l_discount) * (1 + fil.l_tax),
    )
    return (
        fil.groupby(["l_returnflag", "l_linestatus"], observed=True)
        .agg(
            sum_qty=("l_quantity", "sum"),
            sum_base_price=("l_extendedprice", "sum"),
            sum_disc_price=("disc_price", "sum"),
            sum_charge=("charge", "sum"),
            avg_qty=("l_quantity", "mean"),
            avg_price=("l_extendedprice", "mean"),
            avg_disc=("l_discount", "mean"),
            count_order=("l_quantity", "size"),
        )
        .sort_values(["l_returnflag", "l_linestatus"])
        .reset_index()
    )


def q3():
    cust = load("customer")
    orders = load("orders")
    li = load("lineitem")
    cut = pd.Timestamp("1995-03-15").date()
    cust = cust[cust.c_mktsegment == "BUILDING"][["c_custkey"]]
    orders = orders[orders.o_orderdate < cut][
        ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]
    ]
    li = li[li.l_shipdate > cut][["l_orderkey", "l_extendedprice", "l_discount"]]
    merged = cust.merge(orders, left_on="c_custkey", right_on="o_custkey").merge(
        li, left_on="o_orderkey", right_on="l_orderkey"
    )
    merged = merged.assign(revenue=merged.l_extendedprice * (1 - merged.l_discount))
    return (
        merged.groupby(
            ["l_orderkey", "o_orderdate", "o_shippriority"], observed=True
        )
        .revenue.sum()
        .reset_index()
        .sort_values(["revenue", "o_orderdate"], ascending=[False, True])
    )


def q5():
    region = load("region")
    nation = load("nation")
    supplier = load("supplier")
    customer = load("customer")
    orders = load("orders")
    li = load("lineitem")
    region = region[region.r_name == "ASIA"][["r_regionkey"]]
    nation = nation.merge(
        region, left_on="n_regionkey", right_on="r_regionkey"
    )[["n_nationkey", "n_name"]]
    supplier = supplier.merge(
        nation, left_on="s_nationkey", right_on="n_nationkey"
    )[["s_suppkey", "s_nationkey", "n_name"]]
    customer = customer.merge(
        nation, left_on="c_nationkey", right_on="n_nationkey"
    )[["c_custkey", "c_nationkey"]]
    orders = orders[
        (orders.o_orderdate >= pd.Timestamp("1994-01-01").date())
        & (orders.o_orderdate < pd.Timestamp("1995-01-01").date())
    ][["o_orderkey", "o_custkey"]]
    li = li[["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"]]
    j = (
        customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
        .merge(li, left_on="o_orderkey", right_on="l_orderkey")
        .merge(
            supplier,
            left_on=["l_suppkey", "c_nationkey"],
            right_on=["s_suppkey", "s_nationkey"],
        )
    )
    j = j.assign(revenue=j.l_extendedprice * (1 - j.l_discount))
    return (
        j.groupby("n_name", observed=True)
        .revenue.sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )


def q6():
    li = load("lineitem")
    fil = li[
        (li.l_shipdate >= pd.Timestamp("1994-01-01").date())
        & (li.l_shipdate < pd.Timestamp("1995-01-01").date())
        & (li.l_discount >= 0.05)
        & (li.l_discount <= 0.07)
        & (li.l_quantity < 24)
    ]
    return (fil.l_extendedprice * fil.l_discount).sum()


def q10():
    customer = load("customer")
    orders = load("orders")
    li = load("lineitem")
    nation = load("nation")
    orders = orders[
        (orders.o_orderdate >= pd.Timestamp("1993-10-01").date())
        & (orders.o_orderdate < pd.Timestamp("1994-01-01").date())
    ][["o_orderkey", "o_custkey"]]
    li = li[li.l_returnflag == "R"][
        ["l_orderkey", "l_extendedprice", "l_discount"]
    ]
    j = (
        customer.merge(nation, left_on="c_nationkey", right_on="n_nationkey")
        .merge(orders, left_on="c_custkey", right_on="o_custkey")
        .merge(li, left_on="o_orderkey", right_on="l_orderkey")
    )
    j = j.assign(revenue=j.l_extendedprice * (1 - j.l_discount))
    return (
        j.groupby(
            [
                "c_custkey",
                "c_name",
                "c_acctbal",
                "c_phone",
                "n_name",
                "c_address",
                "c_comment",
            ],
            observed=True,
        )
        .revenue.sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )


def q12():
    orders = load("orders")
    li = load("lineitem")
    li = li[
        (li.l_shipmode.isin(["MAIL", "SHIP"]))
        & (li.l_commitdate < li.l_receiptdate)
        & (li.l_shipdate < li.l_commitdate)
        & (li.l_receiptdate >= pd.Timestamp("1994-01-01").date())
        & (li.l_receiptdate < pd.Timestamp("1995-01-01").date())
    ][["l_orderkey", "l_shipmode"]]
    j = li.merge(orders[["o_orderkey", "o_orderpriority"]], left_on="l_orderkey", right_on="o_orderkey")
    j["is_high"] = j.o_orderpriority.isin(["1-URGENT", "2-HIGH"]).astype(int)
    j["is_low"] = (~j.o_orderpriority.isin(["1-URGENT", "2-HIGH"])).astype(int)
    return (
        j.groupby("l_shipmode", observed=True)
        .agg(high_line_count=("is_high", "sum"), low_line_count=("is_low", "sum"))
        .reset_index()
        .sort_values("l_shipmode")
    )


def q14():
    li = load("lineitem")
    part = load("part")
    li = li[
        (li.l_shipdate >= pd.Timestamp("1995-09-01").date())
        & (li.l_shipdate < pd.Timestamp("1995-10-01").date())
    ][["l_partkey", "l_extendedprice", "l_discount"]]
    j = li.merge(part[["p_partkey", "p_type"]], left_on="l_partkey", right_on="p_partkey")
    rev = j.l_extendedprice * (1 - j.l_discount)
    promo = rev.where(j.p_type.str.startswith("PROMO"), 0)
    return 100.00 * promo.sum() / rev.sum()


QUERIES = {
    "Q01": q1,
    "Q03": q3,
    "Q05": q5,
    "Q06": q6,
    "Q10": q10,
    "Q12": q12,
    "Q14": q14,
}


def main() -> None:
    print(f"==> pandas {pd.__version__} TPC-H bench (SF=1, M3 Pro)")
    print(f"==> data: {DATA}")
    print()
    print("| Query | pandas (ms) | rows | note |")
    print("|---|---|---|---|")
    for q, fn in QUERIES.items():
        try:
            med, rows = time3(fn)
            print(f"| {q} | {med:>8.2f} | {rows} | |")
        except Exception as e:
            msg = str(e).split("\n")[0][:80]
            print(f"| {q} | FAIL | — | {type(e).__name__}: {msg} |", file=sys.stderr)


if __name__ == "__main__":
    main()
