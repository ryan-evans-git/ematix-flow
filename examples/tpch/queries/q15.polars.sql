-- TPC-H Q15, Polars-parser-compatible variant. The canonical
-- `total_revenue = (SELECT max(total_revenue) FROM revenue_s)` uses
-- a scalar subquery comparison Polars rejects. Pre-compute the max
-- as a 1-row CTE and CROSS JOIN.
with revenue_s as (
	select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-04-01'
	group by
		l_suppkey
),
max_revenue as (
	select max(total_revenue) as max_total from revenue_s
)
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier
	join revenue_s on revenue_s.supplier_no = supplier.s_suppkey
	cross join max_revenue
where
	total_revenue = max_total
order by
	s_suppkey;
