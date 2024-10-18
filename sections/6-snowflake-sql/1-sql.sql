-- =========================================
-- LIMIT/OFFSET (PostgreSQL) + OFFSET/FETCH (ANSI)

select TOP 2 $1
from (values (1), (2), (3), (4), (5))
order by 1;

select $1
from (values (1), (2), (3), (4), (5))
order by 1
LIMIT 2;
-- LIMIT 2 OFFSET 3;
-- LIMIT '' OFFSET 3;

select $1
from (values (1), (2), (3), (4), (5))
order by 1
FETCH 2;
-- OFFSET 3 FETCH 2;
-- FETCH FIRST 2 ROWS ONLY;
-- OFFSET 3 ROW FETCH NEXT 2 ROW ONLY;

-- =========================================
-- QUALIFY

-- this will fail
select name
from (values ('apples'), ('oranges'), ('nuts')) as fruits(name)
where row_number() over (order by name) >= 2
order by name;

-- fix with QUALIFY (instead of WHERE)
select name
from (values ('apples'), ('oranges'), ('nuts')) as fruits(name)
QUALIFY row_number() over (order by name) >= 2
order by name;

-- fix with subquery (in CTE)
with cte as (
    select name, row_number() over (order by name) as rn
    from (values ('apples'), ('oranges'), ('nuts')) as fruits(name)
    order by name)
select name
from cte
where rn >= 2;

-- =========================================
-- no WINDOW clause yet (as in PostgreSQL)

select $1 as id,
    row_number() over (order by id) row_number,
    rank() over (order by id) rank,
    dense_rank() over (order by id) dense_rank,
    round(percent_rank() over (order by id) * 100) || '%' percent_rank,
    ntile(2) over (order by id) bucket
from (values (3), (1), (1), (2), (3))
order by id;

select $1 as id,
    row_number() over w row_number,
    rank() over w rank,
    dense_rank() over w dense_rank,
    round(percent_rank() over w * 100) || '%' percent_rank,
    ntile(2) over w bucket
from (values (3), (1), (1), (2), (3))
order by id
WINDOW w as (order by id);

-- =========================================
-- EXCLUDE/ILIKE w/ RENAME/REPLACE

use snowflake_sample_data.tpch_sf1;

select *
from lineitem
limit 10;

-- EXCLUDE
select * exclude L_COMMENT
from lineitem
limit 10;

select *
    exclude (L_COMMENT, L_PARTKEY)
    rename L_ORDERKEY as k
from lineitem
limit 10;

-- ILIKE
select * ilike 'L_S%'
from lineitem
limit 10;

select *
    ilike 'L_S%'
    replace('ship-' || L_SUPPKEY as L_SHIPMODE)
from lineitem
limit 10;

select *
    ilike 'L_S%'
    replace('ship-' || L_SUPPKEY as L_SHIPMODE)
    rename L_SHIPMODE as newship
from lineitem
limit 10;

-- =========================================
-- COUNT_IF

use snowflake_sample_data.tpch_sf1;

-- COUNT w/ SUM
select c_nationkey,
    sum(case when c_acctbal >= 0 then 1 else 0 end) as good_paying,
    sum(case when c_acctbal < 0 then 1 else 0 end) as bad_paying
from customer c
group by c_nationkey
order by c_nationkey;

-- COUNT_IF
select c_nationkey,
    count_if(c_acctbal >= 0) as good_paying,
    count_if(c_acctbal < 0) as bad_paying
from customer c
group by c_nationkey
order by c_nationkey;

-- =========================================
-- dynamic PIVOT

select country,
    "'AUTOMOBILE'" as AUTOMOBILE, "'BUILDING'" as BUILDING
from (
    select n_name as country, c_mktsegment
    from customer
    join nation on c_nationkey = n_nationkey)
PIVOT(count(c_mktsegment) for c_mktsegment in ('AUTOMOBILE', 'BUILDING'))
order by 1;

-- dynamic pivot w/ different aggregate and value column
select *
from (
    select n_name as country, c_acctbal, c_mktsegment
    from customer
    join nation on c_nationkey = n_nationkey)
PIVOT(sum(c_acctbal) for c_mktsegment in (ANY ORDER BY c_mktsegment))
order by 1;

-- =========================================
-- higher-order functions

-- [1, 2, 3] --> (multiply each elem by 2) --> [2, 4, 6]
select ARRAY_AGG(e.value::int * 2) arr
from TABLE(FLATTEN([1, 2, 3])) e;

select TRANSFORM([1, 2, 3], a INT -> a * 2) arr;

-- [1, 2, 3] --> (exclude elem 2) --> (multiply each elem by 2) --> [2, 6]
select ARRAY_AGG(e.value::int * 2) arr
from TABLE(FLATTEN([1, 2, 3])) e
where e.value <> 2;

select TRANSFORM(
    FILTER([1, 2, 3], a INT -> a <> 2), a INT -> a * 2) arr;
