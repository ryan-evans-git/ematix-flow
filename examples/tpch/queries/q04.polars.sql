-- TPC-H Q4, Polars-parser-compatible variant. Replace EXISTS with a
-- semi-join via DISTINCT + INNER JOIN: distinct-orderkey from lineitem
-- where the order's commit is late, then INNER JOIN to orders. Equiv
-- to `WHERE EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey
-- AND l_commitdate < l_receiptdate)`.
with late_orderkeys as (
	select distinct l_orderkey
	from lineitem
	where l_commitdate < l_receiptdate
)
select
	o_orderpriority,
	count(*) as order_count
from
	orders
	join late_orderkeys on late_orderkeys.l_orderkey = orders.o_orderkey
where
	o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-10-01'
group by
	o_orderpriority
order by
	o_orderpriority;
