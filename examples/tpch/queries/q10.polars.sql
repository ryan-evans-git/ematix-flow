-- TPC-H Q10, Polars-parser-compatible variant. JOIN ON predicates use
-- qualified column refs.
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
	join nation on nation.n_nationkey = customer.c_nationkey
	join orders on orders.o_custkey = customer.c_custkey
	join lineitem on lineitem.l_orderkey = orders.o_orderkey
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
