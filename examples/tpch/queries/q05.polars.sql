-- TPC-H Q5, Polars-parser-compatible variant. JOIN ON predicates use
-- qualified column refs because polars-sql requires both sides be
-- `CompoundIdentifier`.
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	region
	join nation on nation.n_regionkey = region.r_regionkey
	join supplier on supplier.s_nationkey = nation.n_nationkey
	join customer on customer.c_nationkey = supplier.s_nationkey
	join orders on orders.o_custkey = customer.c_custkey
	join lineitem on lineitem.l_orderkey = orders.o_orderkey
		and lineitem.l_suppkey = supplier.s_suppkey
where
	r_name = 'ASIA'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1995-01-01'
group by
	n_name
order by
	revenue desc;
