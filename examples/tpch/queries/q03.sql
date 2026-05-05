-- TPC-H Q3: Shipping Priority Query.
--
-- Spec: TPC-H § 2.4.3. Reports the 10 unshipped orders with the
-- highest revenue. Three-way join (customer ⋈ orders ⋈ lineitem)
-- with multi-column GROUP BY, ORDER BY DESC, LIMIT 10 — exercises
-- DataFusion's hash-join + top-N optimizer.
--
-- TPC-H validation parameters: SEGMENT='BUILDING', DATE='1995-03-15'.
-- SF=1 reference rows / values: see
-- `tpchgen::q_and_a::answers_sf1::Q3_ANSWER`.

SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM   customer
JOIN   orders   ON c_custkey = o_custkey
JOIN   lineitem ON l_orderkey = o_orderkey
WHERE  c_mktsegment = 'BUILDING'
  AND  o_orderdate < DATE '1995-03-15'
  AND  l_shipdate  > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
;
