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
from generator(rowcount => 10);

select seq1()
from table(generator(rowcount => 10));

select *
from result_scan(last_query_id());

select *
from table(result_scan(last_query_id(-2)));

-- ================================================
-- TABLE vs LATERAL

select elem.value
from (select parse_json('[1, 2, 3]') j) arr,
    table(flatten(arr.j)) elem;

select elem.value
from (select parse_json('[1, 2, 3]') j) arr,
    lateral flatten(arr.j) elem;
