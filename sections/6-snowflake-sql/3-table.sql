-- ================================================
-- TABLE statement

use snowflake_sample_data.tpch_sf1;

select *
from lineitem
limit 10;

table lineitem
limit 10;

-- ================================================
-- TABLE function

select *
from table('lineitem')
limit 10;

select *
from identifier('lineitem')
limit 10;

select identifier('l_orderkey')
from identifier('lineitem')
limit 10;

-- ================================================
-- TABLE wrapper

select seq1()
from generator(rowcount => 10)
limit 10;

select seq1()
from table(generator(rowcount => 10))
limit 10;

select *
from table(result_scan(last_query_id()))
limit 10;

-- ================================================
-- TABLE vs LATERAL

select elem.value
from (select parse_json('[1, 2, 3]') j) arr,
    table(flatten(arr.j)) elem;

select elem.value
from (select parse_json('[1, 2, 3]') j) arr,
    lateral flatten(arr.j) elem;

select elem.value
from (select parse_json('[1, 2, 3]') j) arr
    inner join lateral flatten(arr.j) elem;
