use test.public;

CREATE OR REPLACE TABLE sales_ts(date TIMESTAMP_NTZ, sales FLOAT);
INSERT INTO sales_ts VALUES
  -- train data
  (to_timestamp_ntz('2020-01-01'), 2.0),
  (to_timestamp_ntz('2020-01-02'), 3.0),
  (to_timestamp_ntz('2020-01-03'), 5.0),
  (to_timestamp_ntz('2020-01-04'), 30.0),   -- labeled outlier!
  (to_timestamp_ntz('2020-01-05'), 8.0),
  (to_timestamp_ntz('2020-01-06'), 6.0),
  (to_timestamp_ntz('2020-01-07'), 4.6),
  (to_timestamp_ntz('2020-01-08'), 2.7),
  (to_timestamp_ntz('2020-01-09'), 8.6),
  (to_timestamp_ntz('2020-01-10'), 9.2),
  (to_timestamp_ntz('2020-01-11'), 4.6),
  (to_timestamp_ntz('2020-01-12'), 7.0),
  (to_timestamp_ntz('2020-01-13'), 3.6),
  (to_timestamp_ntz('2020-01-14'), 8.0),
  -- test data
  (to_timestamp_ntz('2020-01-15'), 6.0),
  (to_timestamp_ntz('2020-01-16'), 20.0);

SELECT date, sales,
  (sales > 10) as outlier,
  iff(date < '2020-01-15', 'train', 'test') as dataset
FROM sales_ts;

// ===================================================
// Forecasting (unsupervized)
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST fcast(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE(
    'SELECT * FROM sales_ts WHERE date < ''2020-01-15'''),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');
SHOW SNOWFLAKE.ML.FORECAST;

CALL fcast!FORECAST(FORECASTING_PERIODS => 3);

CALL fcast!FORECAST(
  FORECASTING_PERIODS => 3,
  CONFIG_OBJECT => {'prediction_interval': 0.8});

// ===================================================
-- Anomaly Detection, unlabeled (unsupervized)
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ad1(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE(
    'SELECT * FROM sales_ts WHERE date < ''2020-01-15'''),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => '');
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;

CALL ad1!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE(
    'SELECT * FROM sales_ts WHERE date >= ''2020-01-15'''),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

// ===================================================
-- Anomaly Detection, labeled (supervized)
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ad2(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE(
    'SELECT date, sales, (sales > 10) as outlier FROM sales_ts WHERE date < ''2020-01-15'''),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'outlier');

CALL ad2!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE(
    'SELECT date, sales, (sales > 10) as outlier FROM sales_ts WHERE date >= ''2020-01-15'''),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL ad2!EXPLAIN_FEATURE_IMPORTANCE();
