-- TPC-H Q11, Polars-parser-compatible variant. The canonical
-- `HAVING sum(...) > (SELECT sum(...) * 0.0001 ...)` uses a scalar
-- subquery comparison that Polars rejects. Pre-compute the global
-- threshold as a 1-row CTE and CROSS JOIN it into the outer query
-- so the comparison becomes a join-conditional filter.
with germany_total as (
	select
		sum(ps_supplycost * ps_availqty) * 0.0001 as threshold
	from
		partsupp
		join supplier on supplier.s_suppkey = partsupp.ps_suppkey
		join nation on nation.n_nationkey = supplier.s_nationkey
	where
		n_name = 'GERMANY'
),
per_part as (
	select
		ps_partkey,
		sum(ps_supplycost * ps_availqty) as value
	from
		partsupp
		join supplier on supplier.s_suppkey = partsupp.ps_suppkey
		join nation on nation.n_nationkey = supplier.s_nationkey
	where
		n_name = 'GERMANY'
	group by
		ps_partkey
)
select
	ps_partkey,
	value
from
	per_part
	cross join germany_total
where
	value > threshold
order by
	value desc;
