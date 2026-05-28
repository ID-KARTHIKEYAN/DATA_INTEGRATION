```sql
-- =============================================================
-- FINANCE_MEDALLION_L0.sql
-- Medallion Architecture Demo Project
-- =============================================================

-- =============================================================
-- CREATE CATALOG & SCHEMA
-- =============================================================

CREATE CATALOG IF NOT EXISTS demo_catalog;

CREATE SCHEMA IF NOT EXISTS demo_catalog.admin;

-- =============================================================
-- CONTROL HEADER TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_control_header
(
    DATA_FLOW_GROUP_ID      STRING,
    JOB_NAME                STRING,
    NOTEBOOK_PATH           STRING,
    TRIGGER_TYPE            STRING,
    ETL_LAYER               STRING,
    COMPUTE_CLASS_DEV       STRING,
    COMPUTE_CLASS           STRING,
    BUSINESS_OBJECT_NAME    STRING,
    COST_CENTER             STRING,
    DATA_SME                STRING,
    BUSINESS_UNIT           STRING,
    PRODUCT_OWNER           STRING,
    INGESTION_MODE          STRING,
    INGESTION_BUCKET        STRING,
    SPARK_CONFIGS           STRING,
    WARNING_THRESHOLD_MINS  STRING,
    WARNING_DL_GROUP        STRING,
    MIN_VERSION             STRING,
    MAX_VERSION             STRING,
    IS_ACTIVE               STRING,
    CREATED_DATE            TIMESTAMP,
    UPDATED_DATE            TIMESTAMP
)
USING DELTA;

-- =============================================================
-- L0 DETAIL TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_l0_detail
(
    DATA_FLOW_GROUP_ID   STRING,
    SOURCE_URL           STRING,
    SOURCE_OBJ_SCHEMA    STRING,
    SOURCE_OBJ_NAME      STRING,
    LOB                  STRING,
    TARGET_SCHEMA        STRING,
    TARGET_TABLE         STRING,
    FILE_FORMAT          STRING,
    INPUT_FILE_FORMAT    STRING,
    LOAD_TYPE            STRING,
    STORAGE_TYPE         STRING,
    DQ_LOGIC             STRING,
    DELIMETER            STRING,
    CUSTOM_SCHEMA        STRING,
    CDC_LOGIC            STRING,
    PRESTAG_FLAG         STRING,
    LS_FLAG              STRING,
    IS_ACTIVE            STRING,
    CREATED_DATE         TIMESTAMP,
    UPDATED_DATE         TIMESTAMP
)
USING DELTA;

-- =============================================================
-- L1 DETAIL TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_l1_detail
(
    DATA_FLOW_GROUP_ID      STRING,
    SOURCE_OBJ_SCHEMA       STRING,
    SOURCE_OBJ_NAME         STRING,
    TARGET_SCHEMA           STRING,
    TARGET_TABLE            STRING,
    LOAD_TYPE               STRING,
    TRANSFORMATION_QUERY    STRING,
    MERGE_KEYS              STRING,
    PARTITION_BY            STRING,
    DQ_LOGIC                STRING,
    IS_ACTIVE               STRING,
    CREATED_DATE            TIMESTAMP,
    UPDATED_DATE            TIMESTAMP
)
USING DELTA;

-- =============================================================
-- L2 DETAIL TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_l2_detail
(
    DATA_FLOW_GROUP_ID      STRING,
    SOURCE_OBJ_SCHEMA       STRING,
    SOURCE_OBJ_NAME         STRING,
    TARGET_SCHEMA           STRING,
    TARGET_TABLE            STRING,
    LOAD_TYPE               STRING,
    TRANSFORMATION_QUERY    STRING,
    MERGE_KEYS              STRING,
    PARTITION_BY            STRING,
    IS_ACTIVE               STRING,
    CREATED_DATE            TIMESTAMP,
    UPDATED_DATE            TIMESTAMP
)
USING DELTA;

-- =============================================================
-- HEADER MERGE
-- =============================================================

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (
  SELECT
    'FINANCE_MEDALLION_L0' AS DATA_FLOW_GROUP_ID,
    'FINANCE_MEDALLION_L0_JOB' AS JOB_NAME,
    '/Workspace/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/notebooks/run_framework' AS NOTEBOOK_PATH,
    'BATCH' AS TRIGGER_TYPE,
    'L0' AS ETL_LAYER,
    'S_R5' AS COMPUTE_CLASS_DEV,
    'S_R5' AS COMPUTE_CLASS,
    'Finance' AS BUSINESS_OBJECT_NAME,
    'FIN-001' AS COST_CENTER,
    'DATA_TEAM' AS DATA_SME,
    'FINANCE' AS BUSINESS_UNIT,
    'PROJECT_OWNER' AS PRODUCT_OWNER,
    'DB_INGEST' AS INGESTION_MODE,
    'github' AS INGESTION_BUCKET,
    NULL AS SPARK_CONFIGS,
    '180' AS WARNING_THRESHOLD_MINS,
    'data_team' AS WARNING_DL_GROUP,
    '1.0' AS MIN_VERSION,
    '1.0' AS MAX_VERSION,
    'Y' AS IS_ACTIVE,
    current_timestamp() AS CREATED_DATE,
    current_timestamp() AS UPDATED_DATE
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID

WHEN MATCHED THEN
UPDATE SET
    tgt.NOTEBOOK_PATH = src.NOTEBOOK_PATH,
    tgt.ETL_LAYER = src.ETL_LAYER,
    tgt.IS_ACTIVE = src.IS_ACTIVE,
    tgt.UPDATED_DATE = src.UPDATED_DATE

WHEN NOT MATCHED THEN
INSERT *;

-- =============================================================
-- CONTINUE YOUR OTHER MERGE STATEMENTS BELOW
-- =============================================================
```
