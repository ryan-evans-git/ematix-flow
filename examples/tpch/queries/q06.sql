-- TPC-H Q6: Forecasting Revenue Change Query.
--
-- Spec: TPC-H § 2.4.6. Quantifies the revenue increase that would
-- have resulted from eliminating certain company-wide discounts in a
-- given year. The simplest aggregate-only TPC-H query — used as the
-- Σ.A1 smoke-test correctness gate.
--
-- SF=1 reference revenue: 123141078.23 (TPC-H official, bundled in
-- `tpchgen::q_and_a::answers_sf1::Q6_ANSWER`). The integration test
-- `crates/ematix-flow-core/tests/tpch_smoke.rs` asserts this value
-- within 0.01 absolute tolerance.

SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM   lineitem
WHERE  l_shipdate >= DATE '1994-01-01'
  AND  l_shipdate <  DATE '1995-01-01'
  AND  l_discount BETWEEN 0.05 AND 0.07
  AND  l_quantity < 24
;
