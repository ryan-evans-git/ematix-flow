-- TPC-H Q5, Polars-parser-compatible variant. Semantically identical
-- to q05.sql; only difference is the implicit `FROM a, b, c, d, e, f`
-- cross-product is rewritten as explicit `JOIN ... ON ...` clauses.
-- Polars's SQL parser (1.40.x) rejects implicit-join FROM lists.
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	region
	join nation on n_regionkey = r_regionkey
	join supplier on s_nationkey = n_nationkey
	join customer on c_nationkey = s_nationkey
	join orders on c_custkey = o_custkey
	join lineitem on l_orderkey = o_orderkey and l_suppkey = s_suppkey
where
	r_name = 'ASIA'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1995-01-01'
group by
	n_name
order by
	revenue desc;
