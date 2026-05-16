-- TPC-H Q13, Polars-parser-compatible variant. The canonical query uses
-- `LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT
-- LIKE '...'` — but Polars's SQL parser requires JOIN ON predicates to
-- be **equi-joins on identifiers only**. The `NOT LIKE` part has to
-- move into a separate filter on `orders` before the join.
--
-- Semantically: pre-filter orders to drop rows whose comment matches
-- '%special%requests%', then LEFT JOIN customer to the filtered set.
-- Customers with no matching orders still appear (count = 0), which
-- is exactly what the original LEFT OUTER JOIN does.
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
			customer
			left join (
				select o_orderkey, o_custkey
				from orders
				where o_comment not like '%special%requests%'
			) as filtered_orders
				on filtered_orders.o_custkey = customer.c_custkey
		group by
			c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;
