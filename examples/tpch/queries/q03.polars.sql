-- TPC-H Q3, Polars-parser-compatible variant. Semantically identical
-- to q03.sql; only difference is the implicit `FROM a, b, c` cross-
-- product is rewritten as explicit `JOIN ... ON ...` clauses, because
-- Polars's SQL parser (1.40.x) rejects implicit-join FROM lists.
-- The join predicates (`c_custkey = o_custkey`, `l_orderkey = o_orderkey`)
-- have moved from WHERE into ON; the remaining filters are unchanged.
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer
	join orders on c_custkey = o_custkey
	join lineitem on l_orderkey = o_orderkey
where
	c_mktsegment = 'BUILDING'
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;
