# Open this file in a Snowflake Notebook.
# Add **faker**, **modin** and **pandas** to Packages.
# Set the **pandas** version to 2.2.1 (supported by modin).

# generate 100K fake rows in memory
from faker import Faker

f = Faker()
output = [{
    "name": f.name(),
    "address": f.address(),
    "city": f.city(),
    "state": f.state(),
    "email": f.email()
} for _ in range(100000)]

# Drop N/A w/ pandas
import pandas as pd

df = pd.DataFrame(output)
df.dropna(inplace=True)
print(df)

# Drop N/A w/ pandas on Snowflake
import snowflake.snowpark.modin.plugin
import modin.pandas as pd

df = pd.DataFrame(output)
df.dropna(inplace=True)
print(df)
