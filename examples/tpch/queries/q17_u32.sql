-- Σ.Q.L11 SPIKE — Q17 with manual ARROW_CAST(... AS 'UInt32') on
-- both sides of every l_partkey/p_partkey equi-key. If DataFusion's
-- HashJoinExec and AggregateExec benefit from u32 over i64 keys
-- (smaller hash buckets, less repartition bandwidth, fewer cache
-- misses on the 2 M-group AVG), this should be measurably faster
-- than vanilla Q17. If the gain is small/zero, the broader
-- CastDownExec plan-rule lever isn't worth building.
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	arrow_cast(p_partkey, 'UInt32') = arrow_cast(l_partkey, 'UInt32')
	and p_brand = 'Brand#23'
	and p_container = 'MED BOX'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			arrow_cast(l_partkey, 'UInt32') = arrow_cast(p_partkey, 'UInt32')
	);
