-- TPC-H Q14, Polars-parser-compatible variant. Semantically identical
-- to q14.sql; implicit `FROM lineitem, part` rewritten as explicit
-- JOIN.
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem
	join part on part.p_partkey = lineitem.l_partkey
where
	l_shipdate >= date '1995-09-01'
	and l_shipdate < date '1995-10-01';
