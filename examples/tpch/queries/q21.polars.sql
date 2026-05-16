-- TPC-H Q21, Polars-parser-compatible variant. The canonical query
-- has implicit 4-table FROM plus EXISTS and NOT EXISTS correlated
-- subqueries on lineitem. Flatten to CTE chain.
--
-- exists_other_supplier: orderkeys that have ≥ 2 distinct suppliers.
-- not_exists_late_other: orderkeys where NO other supplier is late
--                        (l_receiptdate > l_commitdate) — equivalent
--                        to LEFT ANTI JOIN to "late other supplier"
--                        set.
-- Final: supplier x lineitem (where l1.l_receiptdate > l_commitdate)
--        x orders (status='F') x nation (CANADA-style filter →
--        'SAUDI ARABIA'), filtered by the two CTEs.
with other_suppliers as (
	-- orderkeys + suppkey pairs where some OTHER supplier also exists
	-- on this orderkey. Implements `exists ... where l2.l_orderkey =
	-- l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey` as a semi-join.
	select distinct l1.l_orderkey, l1.l_suppkey
	from
		lineitem as l1
		join lineitem as l2
			on l2.l_orderkey = l1.l_orderkey
	where
		l2.l_suppkey <> l1.l_suppkey
),
late_other_suppliers as (
	-- orderkeys + suppkey pairs where some OTHER supplier on the same
	-- orderkey was LATE. Used as the NOT-EXISTS anti-set.
	select distinct l1.l_orderkey, l1.l_suppkey
	from
		lineitem as l1
		join lineitem as l3
			on l3.l_orderkey = l1.l_orderkey
	where
		l3.l_suppkey <> l1.l_suppkey
		and l3.l_receiptdate > l3.l_commitdate
)
select
	s_name,
	count(*) as numwait
from
	supplier
	join lineitem l1 on l1.l_suppkey = supplier.s_suppkey
	join orders on orders.o_orderkey = l1.l_orderkey
	join nation on nation.n_nationkey = supplier.s_nationkey
	join other_suppliers
		on other_suppliers.l_orderkey = l1.l_orderkey
		and other_suppliers.l_suppkey = l1.l_suppkey
	left join late_other_suppliers
		on late_other_suppliers.l_orderkey = l1.l_orderkey
		and late_other_suppliers.l_suppkey = l1.l_suppkey
where
	o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and n_name = 'SAUDI ARABIA'
	and late_other_suppliers.l_orderkey is null
group by
	s_name
order by
	numwait desc,
	s_name;
