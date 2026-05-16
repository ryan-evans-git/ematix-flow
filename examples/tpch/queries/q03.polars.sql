-- TPC-H Q3, Polars-parser-compatible variant. Semantically identical
-- to q03.sql. Two differences from canonical:
--   1. Implicit `FROM a, b, c` rewritten as explicit `JOIN ... ON ...`
--      (Polars's SQL parser rejects implicit-join FROM lists).
--   2. Join predicates use **qualified** column names (table.col), since
--      polars-sql's `process_join_on` requires both sides of the ON to
--      be `CompoundIdentifier` (see polars-sql/src/context.rs's
--      `collect_compound_identifiers`).
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer
	join orders on customer.c_custkey = orders.o_custkey
	join lineitem on lineitem.l_orderkey = orders.o_orderkey
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
