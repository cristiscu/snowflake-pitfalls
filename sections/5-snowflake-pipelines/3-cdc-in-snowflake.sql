use test.public;

-- =============================================
-- (1) manual, w/ MERGE (in stored proc)

CREATE OR REPLACE TABLE cdc1_source(del BOOLEAN, id INT, name STRING);
CREATE OR REPLACE TABLE cdc1_target(id INT, name STRING);

merge into cdc1_target t using cdc1_source s on t.id = s.id
    when not matched and not del
        then insert (id, name) values (s.id, s.name)
    when matched and del
        then delete
    when matched and not del
        then update set t.name = s.name;

create or replace procedure cdc1_merge() returns int
as $$
merge into cdc1_target t using cdc1_source s on t.id = s.id
    when not matched and not del
        then insert (id, name) values (s.id, s.name)
    when matched and del
        then delete
    when matched and not del
        then update set t.name = s.name;
$$;

-- 3 x INSERT
INSERT INTO cdc1_source
    VALUES (False, 1, 'John'), (False, 2, 'Mary'), (False, 3, 'George');
CALL cdc1_merge();
SELECT * FROM cdc1_target;
TRUNCATE TABLE cdc1_source;

-- UPDATE + DELETE
INSERT INTO cdc1_source
    VALUES (False, 1, 'Mark'), (True, 2, NULL);
CALL cdc1_merge();
SELECT * FROM cdc1_target;
TRUNCATE TABLE cdc1_source;

-- =============================================
-- (2) w/ CHANGE_TRACKING + SELECT CHANGES(...)

CREATE OR REPLACE TABLE cdc2_source(id INT, name STRING);
ALTER TABLE cdc2_source SET CHANGE_TRACKING = TRUE;
CREATE OR REPLACE TABLE cdc3_target(id int, name string);

-- set initial point in time
SET ts1 = (SELECT CURRENT_TIMESTAMP());

-- 3 x INSERT
INSERT INTO cdc2_source
    VALUES (1, 'John'), (2, 'Mary'), (3, 'George');

-- UPDATE + DELETE
UPDATE cdc2_source SET name = 'Mark' WHERE id = 1;
DELETE FROM cdc2_source WHERE id = 2;

-- see all INSERTs
SELECT * FROM cdc2_source
CHANGES (INFORMATION => APPEND_ONLY) AT (TIMESTAMP => $ts1);

SELECT * FROM cdc2_source
CHANGES (INFORMATION => DEFAULT) AT (TIMESTAMP => $ts1);

-- create target with all changes
CREATE OR REPLACE TABLE cdc2_target AS
    SELECT id, name FROM cdc2_source
    CHANGES (INFORMATION => DEFAULT) AT (TIMESTAMP => $ts1);
SELECT * FROM cdc2_target;

-- =============================================
-- (3) w/ stream and task (w/ MERGE)

create or replace table cdc3_source(id int, name string);
create or replace table cdc3_target(id int, name string);

create stream cdc3_stream on table cdc3_source;

-- task on cust_stream data stream, w/ MERGE statement
create or replace task cdc3_task
  warehouse = compute_wh
  schedule = '1 minute'
  when system$stream_has_data('cdc3_stream')
as
  merge into cdc3_target t using cdc3_stream s on t.id = s.id
  when matched
    and metadata$action = 'DELETE'
    and metadata$isupdate = 'FALSE'
    then delete
  when matched
    and metadata$action = 'INSERT'
    and metadata$isupdate = 'TRUE'
    then update set t.name = s.name
  when not matched
    and metadata$action = 'INSERT'
    then insert (id, name) values (s.id, s.name);

-- insert 3 rows in the source table
select system$stream_has_data('cdc3_stream');
insert into cdc3_source values (1, 'John'), (2, 'Mary'), (3, 'George');
select system$stream_has_data('cdc3_stream');

-- could manually execute the task and look at its execution
alter task cdc3_task resume;
execute task cdc3_task;
select *
  from table(information_schema.task_history(task_name => 'cdc3_task'))
  order by run_id desc;

select * from cdc3_target;

-- update+delete existing source rows --> target should make in-place changes
update cdc3_source set name = 'Mark' where id = 1;
delete from cdc3_source where id = 2;
select system$stream_has_data('cdc3_stream');
select * from cdc3_target;

-- do not forget to suspend the task when done (or it will consume credits!)
alter task cdc3_task suspend;

-- =============================================
-- (4) w/ dynamic table

CREATE OR REPLACE TABLE cdc4_source(id INT, name STRING);
CREATE OR REPLACE DYNAMIC TABLE cdc4_target
  WAREHOUSE = compute_wh
  TARGET_LAG = '1 minute'
AS
  SELECT id, name FROM cdc4_source;

-- insert 3 rows in the source table
INSERT INTO cdc4_source
    VALUES (1, 'John'), (2, 'Mary'), (3, 'George');
SELECT * FROM cdc4_target;

-- update+delete existing source rows --> dynamic table should reflect in-place changes
UPDATE cdc4_source SET name = 'Mark' WHERE id = 1;
DELETE FROM cdc4_source WHERE id = 2;
SELECT * FROM cdc4_target;

-- do not forget to suspend the dynamic table when done (or it will consume credits!)
ALTER DYNAMIC TABLE cdc4_target SUSPEND;
