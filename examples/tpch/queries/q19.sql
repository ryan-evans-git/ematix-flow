-- TPC-H Q19: Discounted Revenue Query.
--
-- Spec: TPC-H § 2.4.19. Sums revenue across three orthogonal sets of
-- (brand, container, quantity-range, size-range) constraints joined
-- against shared lineitem and shipping conditions. The complex
-- disjunctive WHERE often exposes optimizer-rewrite cliffs — kept in
-- the Σ.A1 representative bench set specifically because it stresses
-- the planner harder than Q1/Q3/Q6 do.
--
-- TPC-H validation parameters:
--   BRAND1 = 'Brand#12', QUANTITY1 = 1
--   BRAND2 = 'Brand#23', QUANTITY2 = 10
--   BRAND3 = 'Brand#34', QUANTITY3 = 20
-- SF=1 reference revenue: see
-- `tpchgen::q_and_a::answers_sf1::Q19_ANSWER`.

SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM   lineitem
JOIN   part ON p_partkey = l_partkey
WHERE
    (
        p_brand     = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity BETWEEN 1 AND 1 + 10
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_brand     = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity BETWEEN 10 AND 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_brand     = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity BETWEEN 20 AND 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
;
