-- TPC-H Q22, Polars-parser-compatible variant. The canonical query
-- uses ANSI `substring(c_phone from 1 for 2)` which Polars's SQL parser
-- rejects; use the `substr(x, start, len)` form instead (1-indexed in
-- standard SQL). NOT EXISTS becomes LEFT JOIN + IS NULL anti-join.
with cust_cntry as (
	select
		c_custkey,
		c_acctbal,
		substr(c_phone, 1, 2) as cntrycode
	from customer
),
avg_acctbal as (
	select avg(c_acctbal) as avg_pos
	from cust_cntry
	where c_acctbal > 0.00
	  and cntrycode in ('13', '31', '23', '29', '30', '18', '17')
),
custs_in_scope as (
	select c.c_custkey, c.c_acctbal, c.cntrycode
	from cust_cntry c
	cross join avg_acctbal a
	where c.cntrycode in ('13', '31', '23', '29', '30', '18', '17')
	  and c.c_acctbal > a.avg_pos
),
custs_without_orders as (
	select s.c_custkey, s.c_acctbal, s.cntrycode
	from custs_in_scope s
	left join (select distinct o_custkey from orders) o
		on o.o_custkey = s.c_custkey
	where o.o_custkey is null
)
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	custs_without_orders
group by
	cntrycode
order by
	cntrycode;
