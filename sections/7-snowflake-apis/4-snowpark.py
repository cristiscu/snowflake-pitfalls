# copy and paste this into a Python Worksheet
# add faker to Packages + set context to test.public

import snowflake.snowpark as snowpark
from faker import Faker

def main(session: snowpark.Session):
    f = Faker()
    output = [[f.name(), f.address(), f.city(), f.state(), f.email()]
        for _ in range(1000)]
    df = session.create_dataframe(output,
        schema=["name", "address", "city", "state", "email"])
    df.write.mode("overwrite").save_as_table("customers_from_pw")
    df.show()
    return df
