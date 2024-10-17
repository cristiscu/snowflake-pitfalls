-- A Simple Parameterized Query in Snowflake Scripting? Not So Easy :(
-- https://cristian-70480.medium.com/a-simple-parameterized-query-in-snowflake-scripting-not-so-easy-da95e0bf8ce7
use test.public;

select top 1 *
from snowflake_sample_data.tpch_sf1.lineitem;

select top 1 to_varchar($1) as c1
from snowflake_sample_data.tpch_sf1.lineitem;

select top 1 to_varchar($1) as c1
from snowflake_sample_data.tpch_sf1.lineitem
union all
select top 1 to_varchar($1) as c1
from snowflake_sample_data.tpch_sf1.lineitem;

create or replace function dup_lineitem()
    returns table(c1 varchar)
as 'select top 1 to_varchar($1) as c1
from snowflake_sample_data.tpch_sf1.lineitem
union all
select top 1 to_varchar($1) as c1
from snowflake_sample_data.tpch_sf1.lineitem';

select * from table(dup_lineitem());

-- =========================================================
-- w/ inline CURSOR

create or replace procedure dup_any(table_name varchar)
    returns table()
as
begin
    LET c1 CURSOR FOR
        select top 1 to_varchar($1) as c1 from table(?)
        union all
        select top 1 to_varchar($1) as c1 from table(?);
    OPEN c1 USING (:table_name, :table_name);
    RETURN TABLE(RESULTSET_FROM_CURSOR(c1));
end;

call dup_any('snowflake_sample_data.tpch_sf1.lineitem');
call dup_any('snowflake_sample_data.tpch_sf1.customer');

-- =========================================================
-- w/ declared CURSOR

create or replace procedure dup2_any(table_name varchar)
    returns table()
as
declare
    c1 CURSOR FOR
        select top 1 to_varchar($1) as c1 from table(?)
        union all
        select top 1 to_varchar($1) as c1 from table(?);
begin
    OPEN c1 USING (:table_name, :table_name);
    RETURN TABLE(RESULTSET_FROM_CURSOR(c1));
end;

call dup2_any('snowflake_sample_data.tpch_sf1.customer');

-- =========================================================
-- w/ inline RESULTSET

create or replace procedure dup3_any(table_name varchar)
    returns table()
as
begin
    LET r1 RESULTSET := (
        select top 1 to_varchar($1) as c1 from table(:table_name)
        union all
        select top 1 to_varchar($1) as c1 from table(:table_name));
    RETURN TABLE(r1);
end;

call dup3_any('snowflake_sample_data.tpch_sf1.customer');

-- =========================================================
-- w/ declared RESULTSET

create or replace procedure dup4_any(table_name varchar)
    returns table()
as
declare
    r1 RESULTSET DEFAULT (
        select top 1 to_varchar($1) as c1 from table(:table_name)
        union all
        select top 1 to_varchar($1) as c1 from table(:table_name));
begin
    RETURN TABLE(r1);
end;

call dup4_any('snowflake_sample_data.tpch_sf1.customer');

-- =========================================================
-- EXECUTE IMMEDIATE from SQL Worksheet

SET table_name = 'snowflake_sample_data.tpch_sf1.customer';
SET stmt = 'select top 1 to_varchar($1) as c1 from ' || $table_name
    || ' union all select top 1 to_varchar($1) as c1 from ' || $table_name;
EXECUTE IMMEDIATE $stmt;

-- =========================================================
-- w/ inline EXECUTE IMMEDIATE

create or replace procedure dup5_any(table_name varchar)
    returns table()
as
begin
    LET stmt := 'select top 1 to_varchar($1) as c1 from ' || table_name
        || ' union all select top 1 to_varchar($1) as c1 from ' || table_name;
    LET r1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
    RETURN TABLE(r1);
end;

call dup5_any('snowflake_sample_data.tpch_sf1.customer');

-- =========================================================
-- w/ declared EXECUTE IMMEDIATE

create or replace procedure dup6_any(table_name varchar)
    returns table()
as
declare
    stmt VARCHAR DEFAULT '';
    r1 RESULTSET;
begin
    stmt := 'select top 1 to_varchar($1) as c1 from ' || table_name
        || ' union all select top 1 to_varchar($1) as c1 from ' || table_name;
    r1 := (EXECUTE IMMEDIATE :stmt);
    RETURN TABLE(r1);
end;

call dup6_any('snowflake_sample_data.tpch_sf1.customer');
