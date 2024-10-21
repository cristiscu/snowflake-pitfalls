# replace this in a new Streamlit in Snowflake app
# add faker to Packages

import streamlit as st
from snowflake.snowpark.context import get_active_session
from faker import Faker

st.title("Generate Fake Rows")
rownum = st.number_input("Number of rows:", min_value=0, max_value=10000, value=0)
if rownum > 0:
    f = Faker()
    output = [[f.name(), f.address(), f.city(), f.state(), f.email()]
        for _ in range(rownum)]
    session = get_active_session()
    df = session.create_dataframe(output,
        schema=["name", "address", "city", "state", "email"])
    df.write.mode("overwrite").save_as_table("test.public.customers_from_streamlit")
    st.dataframe(df)
