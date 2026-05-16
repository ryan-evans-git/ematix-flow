-- TPC-H Q20, Polars-parser-compatible variant. The canonical query has
-- three levels of correlated subqueries; flatten to a CTE chain.
--
-- Inner-most: per-(partkey, suppkey), the sum of l_quantity in
--   1994 → use to compare ps_availqty > 0.5 * sum.
-- Next:    parts with name LIKE 'forest%'.
-- Outer:   (ps_partkey, ps_suppkey) where the part matches and
--          ps_availqty > threshold → distinct ps_suppkey.
-- Final:   supplier ∩ (nation = CANADA) ∩ the set above.
with forest_parts as (
	select p_partkey
	from part
	where p_name like 'forest%'
),
sumqty as (
	select
		l_partkey,
		l_suppkey,
		0.5 * sum(l_quantity) as threshold
	from
		lineitem
	where
		l_shipdate >= date '1994-01-01'
		and l_shipdate < date '1995-01-01'
	group by
		l_partkey,
		l_suppkey
),
candidate_suppliers as (
	select distinct partsupp.ps_suppkey
	from
		partsupp
		join forest_parts on forest_parts.p_partkey = partsupp.ps_partkey
		join sumqty on sumqty.l_partkey = partsupp.ps_partkey
			and sumqty.l_suppkey = partsupp.ps_suppkey
	where
		partsupp.ps_availqty > sumqty.threshold
)
select
	s_name,
	s_address
from
	supplier
	join nation on nation.n_nationkey = supplier.s_nationkey
	join candidate_suppliers on candidate_suppliers.ps_suppkey = supplier.s_suppkey
where
	n_name = 'CANADA'
order by
	s_name;
