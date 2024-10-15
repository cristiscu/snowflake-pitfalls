-- LATERAL: https://docs.snowflake.com/en/sql-reference/constructs/join-lateral
use snowflake_sample_data.tpch_sf1;

select r_name, n_name
from region r
inner join nation on r_regionkey = n_regionkey
order by 1, 2;

select r_name, n_name
from region r
inner join (
    select n_name
    from nation
    where r.r_regionkey = n_regionkey)
order by 1, 2;

select r_name, n_name
from region r
inner join lateral (
    select n_name
    from nation
    where r.r_regionkey = n_regionkey)
order by 1, 2;

select r_name, n_name
from region r
outer join lateral (
    select n_name
    from nation
    where r.r_regionkey = n_regionkey)
order by 1, 2;

-- ====================================================================

SELECT elem.value
FROM (SELECT ARRAY_CONSTRUCT(1, 2, 3) arr),
    LATERAL FLATTEN(arr) elem;

SELECT elem.value
FROM (SELECT ARRAY_CONSTRUCT(1, 2, 3) arr)
    INNER JOIN LATERAL FLATTEN(arr) elem;

SELECT elem.value
FROM (SELECT ARRAY_CONSTRUCT(1, 2, 3) arr)
    OUTER JOIN LATERAL FLATTEN(arr) elem;

SELECT elem.value
FROM (SELECT ARRAY_CONSTRUCT(1, 2, 3) arr),
    LATERAL FLATTEN(input => arr, outer => TRUE) elem;

SELECT elem.value
FROM (SELECT ARRAY_CONSTRUCT(1, 2, 3) arr),
    TABLE(FLATTEN(input => arr, outer => TRUE)) elem;
