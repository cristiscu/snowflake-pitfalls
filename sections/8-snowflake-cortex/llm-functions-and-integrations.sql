USE SCHEMA SNOWFLAKE.CORTEX;

SET review = $$ I've been a customer for less than a year
and I have never had to visit a branch this much in my lifetime.
I've had my banking card locked THREE times for fraud.
I'm canceling both my debit and credit cards ASAP when I can access a branch. $$;

SELECT COMPLETE('mistral-large',
    'Is this bank customer cancelling his service? ' || $review) as completion;

SELECT EXTRACT_ANSWER($review,
    'Why is this customer not paying his bills?') as answer;

SELECT SENTIMENT($review) as mood;

SELECT SUMMARIZE($review) as summary;

SELECT TRANSLATE($review, 'en', 'fr') as translation;
SELECT SNOWFLAKE.CORTEX.TRANSLATE($review, 'en', 'fr') as translation;

-- ======================================================
-- EAI (External Access Integration) for ChatGPT (Open AI) REST API service
-- only in a paid account! (EAIs not yet supported in trial accounts)

CREATE OR REPLACE DATABASE openai_db;
USE openai_db.public;

CREATE OR REPLACE SECRET openai_key
    TYPE = GENERIC_STRING
    SECRET_STRING = '<paste your own OpenAI API Key>';

CREATE OR REPLACE NETWORK RULE openai_nr
   TYPE = HOST_PORT
   MODE = EGRESS
   VALUE_LIST = ('api.openai.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION openai_eai 
   ALLOWED_NETWORK_RULES = (openai_nr)
   ALLOWED_AUTHENTICATION_SECRETS = (openai_key) 
   ENABLED = TRUE;

CREATE OR REPLACE function openai(prompt text)
  RETURNS text
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (openai_eai)
  SECRETS = ('cred'=openai_key)
  HANDLER = 'handler'
AS $$
import snowflake.snowpark as snowpark
import _snowflake, requests, json

def handler(prompt):
    key = _snowflake.get_generic_secret_string('cred');
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={'Authorization': f'Bearer {key}'},
        json={"model": "gpt-4-turbo-preview",
              "messages": [{"role": "user", "content": prompt}],
              "temperature": 0.7}
    ).json()
    return r["choices"][0]["message"]["content"]
$$;

select 'Chile' as country,
    openai('President of ' || country) as answer;

select
   n_name as country,
   openai('Continent of ' || n_name || ' as one single word and nothing else') as continent
from snowflake_sample_data.tpch_sf1.nation
limit 5;
