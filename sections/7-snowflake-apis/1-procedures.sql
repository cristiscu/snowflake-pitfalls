use test.public;

-- =================================================================
-- SQL or Snowflake Scripting

-- Snowflake Scripting stored procedure
create or replace procedure procSql(num float)
    returns string not null
    language sql
as
begin
    return '+' || to_varchar(num);
end;

call procSql(22.5);
select * from table(result_scan(last_query_id()));
select procSql(22.5);

create or replace procedure procSqlT(s string)
    returns table()
as
begin
    LET rs RESULTSET := (select :s union all select :s);
    RETURN TABLE(rs);
end;

call procSqlT('abc');
select * from table(procSqlT('abc'));

-- SQL UDF
create or replace function funcSql(num float) returns string
as 'select ''+'' || to_varchar(num)';

select funcSql(22.5);

-- SQL UDTF
create or replace function funcSqlT(s string)
    returns table(out varchar)
as $$
    select s
    union all
    select s
$$;

select * from table(funcSqlT('abc'));

-- =================================================================
-- JavaScript

-- JavaScript stored procedure
create or replace procedure procJs(num float)
    returns string not null
    language javascript
    strict
as $$
    return '+' + NUM.toString();
$$;

call procJs(22.5);

-- JavaScript UDF
create or replace function funcJs(num float)
    returns string not null
    language javascript
    strict
as 'return \'+\' + NUM.toString()';

select funcJs(22.5);

-- JavaScript UDTF
create or replace function funcJsT(s string)
    returns table(out varchar)
    language javascript
    strict
as $$
{
    processRow: function f(row, rowWriter, context)
    {
        rowWriter.writeRow({OUT: row.S});
        rowWriter.writeRow({OUT: row.S});
    }
}
$$;

select * from table(funcJsT('abc'));

-- =================================================================
-- Python

-- Python stored procedure (only w/ Snowpark!)
create or replace procedure procPython(num float)
    returns string
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'proc1'
as $$
import snowflake.snowpark as snowpark

def proc1(session: snowpark.Session, num: float):
    query = f"select '+' || to_char({num})"
    return session.sql(query).collect()[0][0]
$$;

call procPython(22.5);

-- Python UDF
create or replace function funcPython(num float)
    returns string
    language python
    runtime_version = '3.10'
    handler = 'proc1'
as $$
def proc1(num: float):
    return '+' + str(num)
$$;

select funcPython(22.5);

-- Python UDTF
create or replace function funcPythonT(s string)
    returns table(out varchar)
    language python
    runtime_version = '3.10'
    handler = 'MyClass'
as $$
class MyClass:
    def process(self, s: str):
        yield (s,)
        yield (s,)
$$;

select * from table(funcPythonT('abc'));

-- =================================================================
-- Java

-- Java stored procedure (only w/ Snowpark!)
create or replace procedure procJava(num float)
    returns string
    language java
    runtime_version = 11
    packages = ('com.snowflake:snowpark:latest')
    handler = 'MyClass.proc1'
as $$
    import com.snowflake.snowpark_java.*;

    class MyClass {
        public String proc1(Session session, float num) {
            String query = "select '+' || to_char(" + num + ")";
            return session.sql(query).collect()[0].getString(0);
        }
    }
$$;

call procJava(22.5);

-- Java UDF
create or replace function funcJava(num float)
    returns string
    language java
    runtime_version = 11
    handler = 'MyClass.fct1'
as $$
    class MyClass {
        public String fct1(float num) {
            return "+" + Float.toString(num);
        }
    }
$$;

select funcJava(22.5);

-- Java UDTF
create or replace function funcJavaT(s string)
    returns table(out varchar)
    language java
    runtime_version = 11
    handler = 'MyClass'
as $$
    import java.util.stream.Stream;

    class OutputRow {
        public String out;
        public OutputRow(String outVal) { this.out = outVal; }
    }
    class MyClass {
        public static Class getOutputClass() { return OutputRow.class; } 
        public Stream<OutputRow> process(String inVal)
        { return Stream.of(new OutputRow(inVal), new OutputRow(inVal)); }
    }
$$;

select * from table(funcJavaT('abc'));

-- =================================================================
-- Scala

-- Scala stored procedure (only w/ Snowpark!)
create or replace procedure procScala(num float)
    returns string
    language scala
    runtime_version = 2.12
    packages = ('com.snowflake:snowpark:latest')
    handler = 'MyClass.proc1'
as $$
    import com.snowflake.snowpark.Session;

    object MyClass {
        def proc1(session: Session, num: Float): String = {
            var query = "select '+' || to_char(" + num + ")"
            return session.sql(query).collect()(0).getString(0)
        }
    }
$$;

call procScala(22.5);

-- Scala UDF
create or replace function funcScala(num float)
    returns string
    language scala
    runtime_version = 2.12
    handler = 'MyClass.fct1'
as $$
    object MyClass {
        def fct1(num: Float): String = {
            return "+" + num.toString
        }
    }
$$;

select funcScala(22.5);
