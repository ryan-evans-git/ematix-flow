-- TPC-H Q10, Polars-parser-compatible variant. Semantically identical
-- to q10.sql; implicit `FROM customer, orders, lineitem, nation`
-- rewritten as explicit JOIN chain.
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer
	join nation on c_nationkey = n_nationkey
	join orders on c_custkey = o_custkey
	join lineitem on l_orderkey = o_orderkey
where
	o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1994-01-01'
	and l_returnflag = 'R'
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc;
