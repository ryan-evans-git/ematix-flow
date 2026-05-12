-- TPC-H Q9, Polars-parser-compatible variant. Semantically identical
-- to q09.sql; implicit `FROM` 6-table cross-product rewritten as
-- explicit JOIN chain. The 3-way relationship between part / supplier
-- / partsupp is preserved via the (ps_suppkey, ps_partkey) composite
-- join keys.
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part
			join lineitem on p_partkey = l_partkey
			join supplier on s_suppkey = l_suppkey
			join partsupp on ps_suppkey = l_suppkey and ps_partkey = l_partkey
			join orders on o_orderkey = l_orderkey
			join nation on s_nationkey = n_nationkey
		where
			p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
