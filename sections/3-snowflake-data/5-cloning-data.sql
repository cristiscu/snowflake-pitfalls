use test.public;

create or replace table src(name string) as select 'John Doe';
table src;

-- (1) CTAS: CREATE TABLE ... AS SELECT ...
create or replace table src_ctas as table src;
table src_ctas;

-- (2) CREATE TABLE ... LIKE ... 
create or replace table src_like like src;
table src_like;
insert overwrite into src_like table src;
table src_like;

-- (3) zero-copy cloning: CREATE TABLE ... CLONE ...
create or replace table src_cloned clone src;
insert into src_cloned select 'Mary Popins';
table src_cloned;

update src set name = 'Bill Maier' where name = 'John Doe';
table src;
table src_cloned;
