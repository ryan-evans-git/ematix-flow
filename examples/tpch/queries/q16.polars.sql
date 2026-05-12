-- TPC-H Q16, Polars-parser-compatible variant. Semantically identical
-- to q16.sql; implicit `FROM partsupp, part` rewritten as explicit
-- JOIN. The inner `ps_suppkey NOT IN (SELECT ... FROM supplier WHERE
-- s_comment LIKE ...)` subquery is kept as-is — Polars handles NOT IN
-- with a sub-SELECT once the outer FROM is explicit.
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp
	join part on p_partkey = ps_partkey
where
	p_brand <> 'Brand#45'
	and p_type not like 'MEDIUM POLISHED %'
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
