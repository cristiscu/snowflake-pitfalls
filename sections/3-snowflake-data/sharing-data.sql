-- run from producer account
use schema test.public;

-- create test tables and data
create or replace table names(id int, name string)
as select 1, 'John Doe';

-- create objects you want to share (secure views/functions)
create or replace secure view names_view
as select name from names;
select * from names_view;

create or replace secure function get_name(id int) returns string
as $$ select name from names where id = id $$;
select get_name(1);

-- create outbound share + grant privileges to shared objects
create or replace share names_share;

grant usage on database test to share names_share;
grant usage on schema test.public to share names_share;
grant select on view names_view to share names_share;
grant usage on function get_name(int) to share names_share;

grant usage on database snowflake_sample_data to share names_share;

-- share w/ consumer accounts (replace w/ your own ORG.ACCT values)
alter share names_share add accounts = YI*****.RAA*****;

-- show all inbound and outbound shares in this account
show shares;

-- ======================================================
-- switch to & run from consumer account
-- Data > Private Sharing > Direct Shares --> Get as DB_NAMES_SHARE database
use db_names_share.public;

-- test shared objects
select * from names_view;
select get_name(1);
