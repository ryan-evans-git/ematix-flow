-- TPC-H Q18, Polars-parser-compatible variant. Polars-sql resolves
-- HAVING against the SELECT-projected schema (post-aggregation), not
-- the underlying tables, so `having sum(l_quantity) > 300` errors with
-- "column l_quantity not found". Project the sum first, then filter on
-- the alias.
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum_qty as total_quantity
from
	(
		select
			l_orderkey,
			sum(l_quantity) as sum_qty
		from lineitem
		group by l_orderkey
	) as per_orderkey
	join orders on orders.o_orderkey = per_orderkey.l_orderkey
	join customer on customer.c_custkey = orders.o_custkey
where
	sum_qty > 300
order by
	o_totalprice desc,
	o_orderdate;
