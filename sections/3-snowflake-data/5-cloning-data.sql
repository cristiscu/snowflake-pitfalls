use test.public;

-- (1) CREATE TABLE ... AS SELECT ... (CTAS, for metadata+data)
create or replace table src(name string) as select 'John Doe';
select * from src;
table src;

-- (2) CREATE TABLE ... LIKE ... (metadata-copy only)
create or replace table src_like like src;
table src_like;

insert overwrite into src_like table src;
table src_like;

-- (3) CREATE TABLE ... CLONE ... (zero-copy cloning, for metadata+data)
create or replace table src_cloned clone src;
table src_cloned;

-- change clone data (source unchanged)
insert into src_cloned select 'Mary Popins';
table src_cloned;

-- change source data (clone unchanged)
update src set name = 'Bill Maier' where name = 'John Doe';
table src;
table src_cloned;
