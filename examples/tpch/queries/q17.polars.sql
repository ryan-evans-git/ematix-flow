-- TPC-H Q17, Polars-parser-compatible variant. Implicit `FROM lineitem,
-- part` rewritten as explicit JOIN. The correlated subquery
-- `(select 0.2 * avg(l_quantity) from lineitem where l_partkey =
-- p_partkey)` is restructured as a separate pre-aggregated CTE
-- because Polars-SQL doesn't support correlated scalar subqueries:
-- compute the per-part avg once, then join.
with part_avg as (
	select
		l_partkey,
		0.2 * avg(l_quantity) as threshold
	from
		lineitem
	group by
		l_partkey
)
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem
	join part on part.p_partkey = lineitem.l_partkey
	join part_avg on part_avg.l_partkey = lineitem.l_partkey
where
	p_brand = 'Brand#23'
	and p_container = 'MED BOX'
	and l_quantity < threshold;
