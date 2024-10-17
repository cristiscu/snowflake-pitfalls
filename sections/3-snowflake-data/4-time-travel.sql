use test.public;

create or replace table tt(name string) as select 'John Doe';
select * from tt;
table tt;
SET qid = last_query_id();

-- =========================================
-- check data from the past

table tt before (statement => $qid);

table tt at (offset => -1000);

table tt at (timestamp => dateadd(hour, -2, current_timestamp()));

table tt before (timestamp => current_timestamp() - interval '2 hours');

table tt before (timestamp => current_timestamp() - interval '10 days');

-- =========================================
-- check/change retention interval

show tables like 'tt';

-- also for account/database/schema
show parameters like 'data_retention_time_in_days' for table tt;

alter table tt set data_retention_time_in_days = 3;

-- =========================================
-- drop/recover table

drop table tt;
table tt;

undrop table tt;
table tt;

-- =========================================
-- recover table with past data, with content
create or replace table tt2 as
table tt before (timestamp => current_timestamp() - interval '2 hours');
table tt2;

drop table tt;
alter table tt2 rename to tt;
table tt;

-- check changes, including for dropped & recovered tables
show tables HISTORY like 'tt%';
