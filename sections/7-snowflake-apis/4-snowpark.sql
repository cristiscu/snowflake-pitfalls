-- run this from an SQL Worksheet
use test.public;

create or replace procedure generate_fake_customers(rownum int)
    returns Table()
    language python
    runtime_version=3.10
    packages=('faker','snowflake-snowpark-python')
    handler='main'
as $$
import snowflake.snowpark as snowpark
from faker import Faker

def main(session: snowpark.Session, rownum: int):
    f = Faker()
    output = [[f.name(), f.address(), f.city(), f.state(), f.email()]
        for _ in range(rownum)]
    df = session.create_dataframe(output,
        schema=["name", "address", "city", "state", "email"])
    df.write.mode("overwrite").save_as_table("customers_from_sp")
    df.show()
    return df
$$;

call generate_fake_customers(1000);
