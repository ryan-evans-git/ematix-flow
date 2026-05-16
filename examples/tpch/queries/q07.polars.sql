-- TPC-H Q7, Polars-parser-compatible variant. JOIN ON predicates use
-- qualified column refs. Nation is self-joined as n1/n2.
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier
			join lineitem on lineitem.l_suppkey = supplier.s_suppkey
			join orders on orders.o_orderkey = lineitem.l_orderkey
			join customer on customer.c_custkey = orders.o_custkey
			join nation n1 on n1.n_nationkey = supplier.s_nationkey
			join nation n2 on n2.n_nationkey = customer.c_nationkey
		where
			(
				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
