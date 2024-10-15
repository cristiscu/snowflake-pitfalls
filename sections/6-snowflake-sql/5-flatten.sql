-- FLATTEN: https://docs.snowflake.com/en/sql-reference/functions/flatten

-- flattening arrays
select t.arr, elem.value, elem.index
from (select ['a', 'b', 'c'] arr) t,
    lateral flatten(t.arr) elem;

select t.arr, elem.value, elem.index
from (select array_construct('a', 'b', 'c') arr) t,
    lateral flatten(t.arr) elem;

-- flattening objects
select t.obj, kv.value, kv.key
from (select {'name':'John', 'age':32} obj) t,
    lateral flatten(t.obj) kv;

select t.obj, kv.value, kv.key
from (select object_construct('name', 'John', 'age', 32) obj) t,
    lateral flatten(t.obj) kv;

-- two-level (chained) array flattening
select query_id,
    t.value['objectName'] as tname,
    c.value['columnName'] as cname
from snowflake.account_usage.access_history ah,
    table(FLATTEN(base_objects_accessed)) t,
    table(FLATTEN(t.value['columns'])) c
where t.value['objectDomain'] = 'Table'
order by query_id, t.index, c.index;
