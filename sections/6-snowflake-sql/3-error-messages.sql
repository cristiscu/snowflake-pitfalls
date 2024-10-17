-- 10 Wrong Error Messages in Snowflake Scripting
-- https://cristian-70480.medium.com/10-wrong-error-messages-in-snowflake-scripting-18c382848ccb
use test.public;

-- =======================================================
-- returns: syntax error ... unexpected 'select'
-- should rather be: a single SQL expression in a procedure or function should never end with a semicolon
create or replace function f1()
    returns int
as $$ select 0; $$;

-- possible fix
create or replace function f1()
    returns int
as $$ select 0 $$;

-- =======================================================
-- returns: syntax error ... unexpected 'select'
-- should rather be: a single SQL expression in a procedure or function should never end with a semicolon
create or replace function f2()
    returns int
as 'select 0;';

-- possible fix
create or replace function f2()
    returns int
as 'select 0';

-- =======================================================
-- returns: syntax error: unexpected '<EOF>'
-- should rather be: missing semicolon at the end of a statement
create or replace procedure p3()
    returns int
as
begin
    select 0
end;

-- possible fix
create or replace procedure p3()
    returns int
as
begin
    select 0;
end;

-- =======================================================
-- returns: syntax error ... unexpected 'select'
-- should rather be: Snowflake Scripting cannot be used to implement UDFs
create or replace function f4()
    returns int
as $$ begin select 0 end; $$;

-- possible fix
create or replace function f4()
    returns int
as $$ select 0 $$;

-- =======================================================
-- returns: syntax error: unexpected 'rs'. syntax error ... unexpected ':='
-- should rather be: required variable data type
create or replace procedure p6()
    returns table()
    language sql
as
begin
    let rs := (execute immediate 'select 0');
    return table(rs);
end;

-- possible fixes
create or replace procedure p6()
    returns table()
    language sql
as
begin
    let rs RESULTSET := (execute immediate 'select 0');
    return table(rs);
end;

create or replace procedure p6()
    returns table()
    language sql
as
declare
    rs RESULTSET;
begin
    rs := (execute immediate 'select 0');
    return table(rs);
end;

-- =======================================================
-- returns (at runtime): variable with name 'VAL' declared twice
-- should rather be: you initialized a variable already declared
create or replace procedure p7()
    returns string
as
declare
    val string;
begin
    let val := 'abc';
    return val;
end;

call p7();

-- possible fixes
create or replace procedure p7()
    returns string
as
declare
    val string;
begin
    val := 'abc';
    return val;
end;

call p7();

create or replace procedure p7()
    returns string
as
begin
    let val := 'abc';
    return val;
end;

call p7();

-- =======================================================
-- returns: syntax error ... unexpected ')'
-- should rather be: local var cannot be prefixed by “:” when not used in inline SQL code
create or replace procedure p9()
    returns table()
    language sql
as
begin
    let rs RESULTSET := (select 0);
    return table(:rs);
end;

-- possible fix
create or replace procedure p9()
    returns table()
    language sql
as
begin
    let rs RESULTSET := (select 0);
    return table(rs);
end;

-- =======================================================
-- returns: syntax error ... unexpected 'RESULTSET'
-- should rather be: assigned SQL statement of a RESULTSET must be surrounded by parentheses
create or replace procedure p10()
    returns table()
    language sql
as
begin
    let rs RESULTSET := select 0;
    return table(rs);
end;

-- possible fix
create or replace procedure p10()
    returns table()
    language sql
as
begin
    let rs RESULTSET := (select 0);
    return table(rs);
end;
