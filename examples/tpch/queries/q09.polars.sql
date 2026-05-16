-- TPC-H Q9, Polars-parser-compatible variant. JOIN ON predicates use
-- qualified column refs.
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
			join lineitem on lineitem.l_partkey = part.p_partkey
			join supplier on supplier.s_suppkey = lineitem.l_suppkey
			join partsupp on partsupp.ps_suppkey = lineitem.l_suppkey
				and partsupp.ps_partkey = lineitem.l_partkey
			join orders on orders.o_orderkey = lineitem.l_orderkey
			join nation on nation.n_nationkey = supplier.s_nationkey
		where
			p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
