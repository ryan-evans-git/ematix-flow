-- TPC-H Q7, Polars-parser-compatible variant. Semantically identical
-- to q07.sql; implicit `FROM` cross-product rewritten as explicit JOIN.
-- Nation is self-joined twice (n1, n2) for the supplier and customer
-- sides — Polars handles aliased joins fine once the FROM clause is
-- explicit.
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
			join lineitem on s_suppkey = l_suppkey
			join orders on o_orderkey = l_orderkey
			join customer on c_custkey = o_custkey
			join nation n1 on s_nationkey = n1.n_nationkey
			join nation n2 on c_nationkey = n2.n_nationkey
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
