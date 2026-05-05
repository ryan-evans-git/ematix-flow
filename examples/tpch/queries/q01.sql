-- TPC-H Q1: Pricing Summary Report Query.
--
-- Spec: TPC-H § 2.4.1. Reports the amount of business that was
-- billed, shipped, and returned. Single-table aggregate over
-- lineitem with HAVING-style group-by; the canonical "scan + group"
-- workload. Used as Σ.A1 PR 2 representative bench query.
--
-- TPC-H validation parameters: DELTA=90 (yields shipdate ≤
-- 1998-09-02). SF=1 reference rows / values: see
-- `tpchgen::q_and_a::answers_sf1::Q1_ANSWER`.

SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity)                                       AS sum_qty,
    SUM(l_extendedprice)                                  AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)                                       AS avg_qty,
    AVG(l_extendedprice)                                  AS avg_price,
    AVG(l_discount)                                       AS avg_disc,
    COUNT(*)                                              AS count_order
FROM   lineitem
WHERE  l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
;
