-- TPC-H Q19, Polars-parser-compatible variant. Semantically identical
-- to q19.sql; only difference is the implicit `FROM lineitem, part`
-- cross-product is rewritten as `lineitem JOIN part ON p_partkey =
-- l_partkey`. The `p_partkey = l_partkey` equality is the same join
-- condition repeated in each of the three OR branches of the original
-- WHERE; factoring it out into the ON clause is a standard equivalence
-- and leaves the residual disjunctive predicate intact.
select
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	lineitem
	join part on part.p_partkey = lineitem.l_partkey
where
	(
		p_brand = 'Brand#12'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 1 and l_quantity <= 1 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_brand = 'Brand#23'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 10 and l_quantity <= 10 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_brand = 'Brand#34'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 20 and l_quantity <= 20 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
