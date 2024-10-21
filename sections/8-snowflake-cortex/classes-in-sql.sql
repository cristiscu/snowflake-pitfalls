SHOW CLASSES IN DATABASE SNOWFLAKE;
SHOW CLASSES IN SCHEMA SNOWFLAKE.ML;

SHOW FUNCTIONS IN CLASS SNOWFLAKE.ML.CLASSIFICATION;
SHOW PROCEDURES IN CLASS SNOWFLAKE.ML.CLASSIFICATION;

SHOW SNOWFLAKE.ML.CLASSIFICATION;

-- ===============================================================

USE SCHEMA test.public;

CREATE OR REPLACE TABLE purchases AS (
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) as VARCHAR) as interest,
        UNIFORM(0, 3, RANDOM()) as rating,
        FALSE AS label
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(3, 7, RANDOM()) AS rating,
        FALSE AS label
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(7, 10, RANDOM()) AS rating,
        TRUE as label
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(0, 3, RANDOM()) AS rating,
        NULL as label
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(3, 7, RANDOM()) AS rating,
        NULL as label
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(7, 10, RANDOM()) AS rating,
        NULL as label
    FROM TABLE(GENERATOR(rowCount => 100))
);
select * from purchases;

-- ===============================================================

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION classifier(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE(
        'SELECT * FROM purchases WHERE label IS NOT NULL'),
    TARGET_COLNAME => 'label');
SHOW snowflake.ml.classification;

SELECT interest, rating, classifier!PREDICT(
    INPUT_DATA => object_construct(*)) as preds
FROM purchases
WHERE label IS NULL;

CALL classifier!SHOW_EVALUATION_METRICS();
CALL classifier!SHOW_GLOBAL_EVALUATION_METRICS();
CALL classifier!SHOW_THRESHOLD_METRICS();

CALL classifier!SHOW_CONFUSION_MATRIX();
CALL classifier!SHOW_FEATURE_IMPORTANCE();
CALL classifier!SHOW_TRAINING_LOGS();
