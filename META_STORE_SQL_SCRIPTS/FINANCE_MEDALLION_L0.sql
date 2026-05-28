-- =============================================================
-- FINANCE_MEDALLION_L0.sql
-- Sample Medallion project — Finance data
-- Uses free GitHub CSV datasets
-- =============================================================
CREATE CATALOG IF NOT EXISTS demo_catalog;
CREATE SCHEMA IF NOT EXISTS demo_catalog.admin;
-- ── Header ────────────────────────────────────────────────────
MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (
  SELECT
    'FINANCE_MEDALLION_L0'               AS DATA_FLOW_GROUP_ID,
    'FINANCE_MEDALLION_L0_JOB'           AS JOB_NAME,
    '/Workspace/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/notebooks/run_framework' AS NOTEBOOK_PATH,
    'BATCH'                              AS TRIGGER_TYPE,
    'L0'                                 AS ETL_LAYER,
    'S_R5'                               AS COMPUTE_CLASS_DEV,
    'S_R5'                               AS COMPUTE_CLASS,
    'Finance'                            AS BUSINESS_OBJECT_NAME,
    'FIN-001'                            AS COST_CENTER,
    'DATA_TEAM'                          AS DATA_SME,
    'FINANCE'                            AS BUSINESS_UNIT,
    'PROJECT_OWNER'                      AS PRODUCT_OWNER,
    'DB_INGEST'                          AS INGESTION_MODE,
    'github'                             AS INGESTION_BUCKET,
    NULL                                 AS SPARK_CONFIGS,
    '180'                                AS WARNING_THRESHOLD_MINS,
    'data_team'                          AS WARNING_DL_GROUP,
    '1.0'                                AS MIN_VERSION,
    '1.0'                                AS MAX_VERSION,
    'Y'                                  AS IS_ACTIVE,
    current_timestamp()                  AS CREATED_DATE,
    current_timestamp()                  AS UPDATED_DATE
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET
    tgt.NOTEBOOK_PATH = src.NOTEBOOK_PATH,
    tgt.ETL_LAYER     = src.ETL_LAYER,
    tgt.IS_ACTIVE     = src.IS_ACTIVE,
    tgt.UPDATED_DATE  = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

-- ── L0 Detail — Titanic (sample transactions) ─────────────────
MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
USING (
  SELECT
    'FINANCE_MEDALLION_L0'      AS DATA_FLOW_GROUP_ID,
    'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv' AS SOURCE_URL,
    'titanic'                   AS SOURCE_OBJ_SCHEMA,
    'titanic_raw'               AS SOURCE_OBJ_NAME,
    'titanic'                   AS LOB,
    'bronze'                    AS TARGET_SCHEMA,
    'titanic_bronze'            AS TARGET_TABLE,
    'csv'                       AS FILE_FORMAT,
    'csv'                       AS INPUT_FILE_FORMAT,
    'FULL'                      AS LOAD_TYPE,
    'GITHUB'                    AS STORAGE_TYPE,
    NULL                        AS DQ_LOGIC,
    ','                         AS DELIMETER,
    NULL                        AS CUSTOM_SCHEMA,
    NULL                        AS CDC_LOGIC,
    'N'                         AS PRESTAG_FLAG,
    'N'                         AS LS_FLAG,
    'Y'                         AS IS_ACTIVE,
    current_timestamp()         AS CREATED_DATE,
    current_timestamp()         AS UPDATED_DATE
) AS src
ON  tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
AND tgt.TARGET_TABLE       = src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
    tgt.SOURCE_URL   = src.SOURCE_URL,
    tgt.FILE_FORMAT  = src.FILE_FORMAT,
    tgt.LOAD_TYPE    = src.LOAD_TYPE,
    tgt.IS_ACTIVE    = src.IS_ACTIVE,
    tgt.UPDATED_DATE = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

-- ── L0 Detail — Weather data ───────────────────────────────────
MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
USING (
  SELECT
    'FINANCE_MEDALLION_L0'      AS DATA_FLOW_GROUP_ID,
    'https://raw.githubusercontent.com/Devasree03/ETL_JEN/refs/heads/main/data/weather.csv' AS SOURCE_URL,
    'weather'                   AS SOURCE_OBJ_SCHEMA,
    'weather_raw'               AS SOURCE_OBJ_NAME,
    'weather'                   AS LOB,
    'bronze'                    AS TARGET_SCHEMA,
    'weather_bronze'            AS TARGET_TABLE,
    'csv'                       AS FILE_FORMAT,
    'csv'                       AS INPUT_FILE_FORMAT,
    'FULL'                      AS LOAD_TYPE,
    'GITHUB'                    AS STORAGE_TYPE,
    NULL                        AS DQ_LOGIC,
    ','                         AS DELIMETER,
    NULL                        AS CUSTOM_SCHEMA,
    NULL                        AS CDC_LOGIC,
    'N'                         AS PRESTAG_FLAG,
    'N'                         AS LS_FLAG,
    'Y'                         AS IS_ACTIVE,
    current_timestamp()         AS CREATED_DATE,
    current_timestamp()         AS UPDATED_DATE
) AS src
ON  tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
AND tgt.TARGET_TABLE       = src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
    tgt.SOURCE_URL   = src.SOURCE_URL,
    tgt.FILE_FORMAT  = src.FILE_FORMAT,
    tgt.LOAD_TYPE    = src.LOAD_TYPE,
    tgt.IS_ACTIVE    = src.IS_ACTIVE,
    tgt.UPDATED_DATE = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

-- ── L1 Silver — Titanic cleaned ───────────────────────────────
MERGE INTO demo_catalog.admin.data_flow_l1_detail AS tgt
USING (
  SELECT
    'FINANCE_MEDALLION_L0'                    AS DATA_FLOW_GROUP_ID,
    'bronze'                                  AS SOURCE_OBJ_SCHEMA,
    'titanic_bronze'                          AS SOURCE_OBJ_NAME,
    'silver'                                  AS TARGET_SCHEMA,
    'titanic_silver'                          AS TARGET_TABLE,
    'MERGE'                                   AS LOAD_TYPE,
    'SELECT PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Fare, Embarked, _etl_load_ts AS bronze_ts, current_timestamp() AS silver_ts FROM bronze.titanic_bronze WHERE Age IS NOT NULL' AS TRANSFORMATION_QUERY,
    'PassengerId'                             AS MERGE_KEYS,
    'Pclass'                                  AS PARTITION_BY,
    NULL                                      AS DQ_LOGIC,
    'Y'                                       AS IS_ACTIVE,
    current_timestamp()                       AS CREATED_DATE,
    current_timestamp()                       AS UPDATED_DATE
) AS src
ON  tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
AND tgt.TARGET_TABLE       = src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
    tgt.TRANSFORMATION_QUERY = src.TRANSFORMATION_QUERY,
    tgt.IS_ACTIVE            = src.IS_ACTIVE,
    tgt.UPDATED_DATE         = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

-- ── L2 Gold — Survival summary ────────────────────────────────
MERGE INTO demo_catalog.admin.data_flow_l2_detail AS tgt
USING (
  SELECT
    'FINANCE_MEDALLION_L0'                    AS DATA_FLOW_GROUP_ID,
    'silver'                                  AS SOURCE_OBJ_SCHEMA,
    'titanic_silver'                          AS SOURCE_OBJ_NAME,
    'gold'                                    AS TARGET_SCHEMA,
    'titanic_survival_summary'                AS TARGET_TABLE,
    'FULL'                                    AS LOAD_TYPE,
    'SELECT Pclass, Sex, COUNT(*) AS total_passengers, SUM(Survived) AS total_survived, ROUND(AVG(Survived)*100,2) AS survival_rate_pct, ROUND(AVG(Age),1) AS avg_age, ROUND(AVG(Fare),2) AS avg_fare, current_timestamp() AS gold_ts FROM silver.titanic_silver GROUP BY Pclass, Sex ORDER BY Pclass, Sex' AS TRANSFORMATION_QUERY,
    NULL                                      AS MERGE_KEYS,
    'Pclass'                                  AS PARTITION_BY,
    'Y'                                       AS IS_ACTIVE,
    current_timestamp()                       AS CREATED_DATE,
    current_timestamp()                       AS UPDATED_DATE
) AS src
ON  tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
AND tgt.TARGET_TABLE       = src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
    tgt.TRANSFORMATION_QUERY = src.TRANSFORMATION_QUERY,
    tgt.IS_ACTIVE            = src.IS_ACTIVE,
    tgt.UPDATED_DATE         = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;
