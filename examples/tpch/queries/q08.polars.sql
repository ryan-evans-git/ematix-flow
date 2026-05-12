-- TPC-H Q8, Polars-parser-compatible variant. Semantically identical
-- to q08.sql; implicit `FROM` 8-table cross-product rewritten as
-- explicit JOIN chain. Nation self-joined for the supplier-nation
-- (n2) and customer-nation (n1) sides.
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
			join nation n1 on n1.n_regionkey = r_regionkey
			join customer on c_nationkey = n1.n_nationkey
			join orders on o_custkey = c_custkey
			join lineitem on l_orderkey = o_orderkey
			join part on p_partkey = l_partkey
			join supplier on s_suppkey = l_suppkey
			join nation n2 on s_nationkey = n2.n_nationkey
		where
			r_name = 'AMERICA'
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by
	o_year
order by
	o_year;
