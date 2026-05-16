-- TPC-H Q8, Polars-parser-compatible variant. JOIN ON predicates use
-- qualified column refs. Nation is self-joined as n1 (customer side)
-- and n2 (supplier side).
select
	o_year,
	sum(case
		when nation = 'BRAZIL' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			region
			join nation n1 on n1.n_regionkey = region.r_regionkey
			join customer on customer.c_nationkey = n1.n_nationkey
			join orders on orders.o_custkey = customer.c_custkey
			join lineitem on lineitem.l_orderkey = orders.o_orderkey
			join part on part.p_partkey = lineitem.l_partkey
			join supplier on supplier.s_suppkey = lineitem.l_suppkey
			join nation n2 on n2.n_nationkey = supplier.s_nationkey
		where
			r_name = 'AMERICA'
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by
	o_year
order by
	o_year;
