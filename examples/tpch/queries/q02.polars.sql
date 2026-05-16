-- TPC-H Q2, Polars-parser-compatible variant. Two rewrites:
--   1. Implicit FROM rewritten as explicit JOIN ON (qualified columns).
--   2. The correlated scalar subquery
--        `ps_supplycost = (select min(ps_supplycost) from ...)`
--      becomes a per-part `min_cost` CTE that we join against on
--      (part, supplycost). Polars's SQL surface rejects scalar-subquery
--      comparisons, but pre-aggregating to a derived table is equiv.
with min_cost as (
	select
		partsupp.ps_partkey,
		min(partsupp.ps_supplycost) as min_supplycost
	from
		partsupp
		join supplier on supplier.s_suppkey = partsupp.ps_suppkey
		join nation on nation.n_nationkey = supplier.s_nationkey
		join region on region.r_regionkey = nation.n_regionkey
	where
		region.r_name = 'EUROPE'
	group by
		partsupp.ps_partkey
)
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part
	join partsupp on partsupp.ps_partkey = part.p_partkey
	join supplier on supplier.s_suppkey = partsupp.ps_suppkey
	join nation on nation.n_nationkey = supplier.s_nationkey
	join region on region.r_regionkey = nation.n_regionkey
	join min_cost on min_cost.ps_partkey = part.p_partkey
		and min_cost.min_supplycost = partsupp.ps_supplycost
where
	p_size = 15
	and p_type like '%BRASS'
	and r_name = 'EUROPE'
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey;
