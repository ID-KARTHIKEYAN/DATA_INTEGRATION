-- =====================================================================
-- metadata_setup_sample.sql
-- ---------------------------------------------------------------------
-- COMPLETE sample end-to-end setup using EXISTING framework tables
-- and their EXACT columns (source of truth).
--
-- Pipeline:
--   GROUP_ID = EMPLOYEE_MASTER_L0  → L0 (bronze.employee_master)
--   GROUP_ID = EMPLOYEE_MASTER_L1  → L1 (silver.employee_master_clean)
--   GROUP_ID = EMPLOYEE_MASTER_L2  → L2 (gold.dim_employee + gold.fact_hire_stats)
--
-- Run this in Databricks SQL or a SQL notebook against demo_catalog.
-- =====================================================================

USE CATALOG demo_catalog;

-- ---------------------------------------------------------------------
-- 0. Make sure admin schema exists
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS demo_catalog.admin;
CREATE SCHEMA IF NOT EXISTS demo_catalog.raw;
CREATE SCHEMA IF NOT EXISTS demo_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS demo_catalog.silver;
CREATE SCHEMA IF NOT EXISTS demo_catalog.gold;

-- ---------------------------------------------------------------------
-- 1. Audit log table (framework writes here, may already exist)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_catalog.admin.audit_log (
    DATA_FLOW_GROUP_ID STRING,
    TARGET_TABLE       STRING,
    STATUS             STRING,
    MESSAGE            STRING,
    CREATED_DATE       TIMESTAMP,
    ETL_LAYER          STRING,
    ROWS_PROCESSED     BIGINT,
    START_TIME         TIMESTAMP,
    END_TIME           TIMESTAMP,
    LOAD_TS            TIMESTAMP,
    DURATION_SECONDS   DOUBLE,
    LOB                STRING,
    ENVIRONMENT        STRING,
    EXTRA_JSON         STRING
) USING DELTA COMMENT 'Framework audit trail';

-- ================================================================
-- L0  :  EMPLOYEE_MASTER_L0   (bronze ingestion from GitHub CSV)
-- ================================================================

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L0'              AS DATA_FLOW_GROUP_ID,
        'JOB'                             AS TRIGGER_TYPE,
        'L0'                              AS ETL_LAYER,
        'serverless'                      AS COMPUTE_CLASS_DEV,
        'serverless'                      AS COMPUTE_CLASS,
        'Y'                               AS IS_ACTIVE,
        'admin'                           AS INSERTED_BY,
        'admin'                           AS UPDATED_BY,
        current_timestamp()               AS INSERTED_TS,
        current_timestamp()               AS UPDATED_TS,
        'Employee Master Data'            AS BUSINESS_OBJECT_NAME,
        'HR'                              AS COST_CENTER,
        'HR Data Team'                    AS DATA_SME,
        'Human Resources'                 AS BUSINESS_UNIT,
        'HR Manager'                      AS PRODUCT_OWNER,
        'github'                          AS INGESTION_MODE,
        'raw_data'                        AS INGESTION_BUCKET,
        NULL                              AS SPARK_CONFIGS,
        CAST(30 AS INT)                   AS WARNING_THRESHOLD_MINS,
        NULL                              AS WARNING_DL_GROUP,
        NULL                              AS MIN_VERSION,
        NULL                              AS MAX_VERSION,
        'demo_catalog'                    AS target_catalog
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET
    tgt.TRIGGER_TYPE          = src.TRIGGER_TYPE,
    tgt.ETL_LAYER             = src.ETL_LAYER,
    tgt.COMPUTE_CLASS_DEV     = src.COMPUTE_CLASS_DEV,
    tgt.COMPUTE_CLASS         = src.COMPUTE_CLASS,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp(),
    tgt.BUSINESS_OBJECT_NAME  = src.BUSINESS_OBJECT_NAME,
    tgt.COST_CENTER           = src.COST_CENTER,
    tgt.DATA_SME              = src.DATA_SME,
    tgt.BUSINESS_UNIT         = src.BUSINESS_UNIT,
    tgt.PRODUCT_OWNER         = src.PRODUCT_OWNER,
    tgt.INGESTION_MODE        = src.INGESTION_MODE,
    tgt.INGESTION_BUCKET      = src.INGESTION_BUCKET,
    tgt.WARNING_THRESHOLD_MINS = src.WARNING_THRESHOLD_MINS,
    tgt.target_catalog        = src.target_catalog
WHEN NOT MATCHED THEN INSERT *;


MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L0'                                                      AS DATA_FLOW_GROUP_ID,
        'https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/main/employee_master_data_messy_10000.csv' AS SOURCE,
        'bronze'                                                                  AS SOURCE_OBJ_SCHEMA,
        'employee_master_data_messy_10000.csv'                                   AS SOURCE_OBJ_NAME,
        'HR'                                                                      AS LOB,
        'FULL'                                                                    AS LOAD_TYPE,
        'csv'                                                                     AS INPUT_FILE_FORMAT,
        'https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/refs/heads/main' AS STORAGE_TYPE,
        NULL                                                                      AS DQ_LOGIC,
        ','                                                                       AS DELIMETER,
        NULL                                                                      AS CUSTOM_SCHEMA,
        NULL                                                                      AS CDC_LOGIC,
        NULL                                                                      AS TRANSFORM_QUERY,
        'N'                                                                       AS PRESTAG_FLAG,
        NULL                                                                      AS `PARTITION`,
        'N'                                                                       AS LS_FLAG,
        NULL                                                                      AS LS_DETAIL,
        'Y'                                                                       AS IS_ACTIVE,
        'admin'                                                                   AS INSERTED_BY,
        'admin'                                                                   AS UPDATED_BY,
        current_timestamp()                                                       AS INSERTED_TS,
        current_timestamp()                                                       AS UPDATED_TS,
        NULL                                                                      AS DEPLOYMENT_SOURCE_DFG
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID AND tgt.SOURCE_OBJ_NAME = src.SOURCE_OBJ_NAME
WHEN MATCHED THEN UPDATE SET
    tgt.SOURCE                = src.SOURCE,
    tgt.SOURCE_OBJ_SCHEMA     = src.SOURCE_OBJ_SCHEMA,
    tgt.LOB                   = src.LOB,
    tgt.LOAD_TYPE             = src.LOAD_TYPE,
    tgt.INPUT_FILE_FORMAT     = src.INPUT_FILE_FORMAT,
    tgt.STORAGE_TYPE          = src.STORAGE_TYPE,
    tgt.DELIMETER             = src.DELIMETER,
    tgt.PRESTAG_FLAG          = src.PRESTAG_FLAG,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;


-- ================================================================
-- L1  :  EMPLOYEE_MASTER_L1   (clean/silver layer)
-- ================================================================

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L1'              AS DATA_FLOW_GROUP_ID,
        'JOB'                             AS TRIGGER_TYPE,
        'L1'                              AS ETL_LAYER,
        'serverless'                      AS COMPUTE_CLASS_DEV,
        'serverless'                      AS COMPUTE_CLASS,
        'Y'                               AS IS_ACTIVE,
        'admin'                           AS INSERTED_BY,
        'admin'                           AS UPDATED_BY,
        current_timestamp()               AS INSERTED_TS,
        current_timestamp()               AS UPDATED_TS,
        'Employee Master Clean'           AS BUSINESS_OBJECT_NAME,
        'HR'                              AS COST_CENTER,
        'HR Data Team'                    AS DATA_SME,
        'Human Resources'                 AS BUSINESS_UNIT,
        'HR Manager'                      AS PRODUCT_OWNER,
        NULL                              AS INGESTION_MODE,
        NULL                              AS INGESTION_BUCKET,
        NULL                              AS SPARK_CONFIGS,
        CAST(45 AS INT)                   AS WARNING_THRESHOLD_MINS,
        NULL                              AS WARNING_DL_GROUP,
        NULL                              AS MIN_VERSION,
        NULL                              AS MAX_VERSION,
        'demo_catalog'                    AS target_catalog
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET
    tgt.TRIGGER_TYPE          = src.TRIGGER_TYPE,
    tgt.ETL_LAYER             = src.ETL_LAYER,
    tgt.COMPUTE_CLASS_DEV     = src.COMPUTE_CLASS_DEV,
    tgt.COMPUTE_CLASS         = src.COMPUTE_CLASS,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp(),
    tgt.BUSINESS_OBJECT_NAME  = src.BUSINESS_OBJECT_NAME,
    tgt.WARNING_THRESHOLD_MINS = src.WARNING_THRESHOLD_MINS,
    tgt.target_catalog        = src.target_catalog
WHEN NOT MATCHED THEN INSERT *;


MERGE INTO demo_catalog.admin.data_flow_pb_detail AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L1'                              AS DATA_FLOW_GROUP_ID,
        'HR'                                              AS LOB,
        'bronze'                                          AS SOURCE,
        'silver'                                          AS TARGET_OBJ_SCHEMA,
        'employee_master_clean'                           AS TARGET_OBJ_NAME,
        CAST(1 AS INT)                                    AS PRIORITY,
        'Table'                                           AS TARGET_OBJ_TYPE,
        -- Clean messy CSV: trim, cast, dedupe PKs, filter null PKs
        'SELECT
            CAST(employee_id    AS BIGINT)      AS employee_id,
            TRIM(first_name)                     AS first_name,
            TRIM(last_name)                      AS last_name,
            CONCAT_WS(" ", TRIM(first_name), TRIM(last_name)) AS full_name,
            LOWER(TRIM(email))                   AS email,
            TRIM(phone)                          AS phone,
            TRIM(department)                     AS department,
            TRIM(position_title)                 AS position_title,
            CAST(salary       AS DECIMAL(18,2))  AS salary,
            CAST(hire_date    AS DATE)           AS hire_date,
            CASE WHEN TRIM(UPPER(status)) IN ("ACTIVE","A","1") THEN "ACTIVE"
                 WHEN TRIM(UPPER(status)) IN ("INACTIVE","I","0","TERMINATED") THEN "INACTIVE"
                 ELSE "UNKNOWN" END             AS employment_status,
            TRIM(country)                        AS country,
            TRIM(city)                           AS city,
            current_timestamp()                  AS ingestion_timestamp
         FROM demo_catalog.bronze.employee_master_data_messy_10000
         WHERE employee_id IS NOT NULL
           AND TRIM(employee_id) <> ""
         QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(employee_id AS BIGINT)
                                    ORDER BY hire_date DESC NULLS LAST) = 1' AS TRANSFORM_QUERY,
        NULL                                              AS GENERIC_SCRIPTS,
        'employee_id'                                     AS SOURCE_PK,
        'employee_id'                                     AS TARGET_PK,
        'MERGE'                                           AS LOAD_TYPE,
        'Y'                                               AS IS_ACTIVE,
        'N'                                               AS LS_FLAG,
        NULL                                              AS LS_DETAIL,
        'department,country'                              AS PARTITION_OR_INDEX,
        'admin'                                           AS INSERTED_BY,
        'admin'                                           AS UPDATED_BY,
        current_timestamp()                               AS INSERTED_TS,
        current_timestamp()                               AS UPDATED_TS,
        NULL                                              AS CUSTOM_SCRIPT_PARAMS,
        'LIQUID_CLUSTER'                                  AS PARTITION_METHOD,
        NULL                                              AS RETENTION_DETAILS,
        NULL                                              AS DEPLOYMENT_SOURCE_DFG
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID AND tgt.TARGET_OBJ_NAME = src.TARGET_OBJ_NAME
WHEN MATCHED THEN UPDATE SET
    tgt.LOB                   = src.LOB,
    tgt.SOURCE                = src.SOURCE,
    tgt.TARGET_OBJ_SCHEMA     = src.TARGET_OBJ_SCHEMA,
    tgt.PRIORITY              = src.PRIORITY,
    tgt.TARGET_OBJ_TYPE       = src.TARGET_OBJ_TYPE,
    tgt.TRANSFORM_QUERY       = src.TRANSFORM_QUERY,
    tgt.SOURCE_PK             = src.SOURCE_PK,
    tgt.TARGET_PK             = src.TARGET_PK,
    tgt.LOAD_TYPE             = src.LOAD_TYPE,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.PARTITION_OR_INDEX    = src.PARTITION_OR_INDEX,
    tgt.PARTITION_METHOD      = src.PARTITION_METHOD,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;


-- ================================================================
-- L2  :  EMPLOYEE_MASTER_L2   (Gold aggregates + dimension)
-- ================================================================

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L2'              AS DATA_FLOW_GROUP_ID,
        'JOB'                             AS TRIGGER_TYPE,
        'L2'                              AS ETL_LAYER,
        'serverless'                      AS COMPUTE_CLASS_DEV,
        'serverless'                      AS COMPUTE_CLASS,
        'Y'                               AS IS_ACTIVE,
        'admin'                           AS INSERTED_BY,
        'admin'                           AS UPDATED_BY,
        current_timestamp()               AS INSERTED_TS,
        current_timestamp()               AS UPDATED_TS,
        'Employee Analytics Mart'         AS BUSINESS_OBJECT_NAME,
        'HR'                              AS COST_CENTER,
        'HR BI Team'                      AS DATA_SME,
        'Human Resources'                 AS BUSINESS_UNIT,
        'HR Director'                     AS PRODUCT_OWNER,
        NULL                              AS INGESTION_MODE,
        NULL                              AS INGESTION_BUCKET,
        NULL                              AS SPARK_CONFIGS,
        CAST(30 AS INT)                   AS WARNING_THRESHOLD_MINS,
        NULL                              AS WARNING_DL_GROUP,
        NULL                              AS MIN_VERSION,
        NULL                              AS MAX_VERSION,
        'demo_catalog'                    AS target_catalog
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET
    tgt.TRIGGER_TYPE          = src.TRIGGER_TYPE,
    tgt.ETL_LAYER             = src.ETL_LAYER,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp(),
    tgt.BUSINESS_OBJECT_NAME  = src.BUSINESS_OBJECT_NAME,
    tgt.target_catalog        = src.target_catalog
WHEN NOT MATCHED THEN INSERT *;


-- L2 Table 1: dim_employee (SCD2 dimension, priority=1)
MERGE INTO demo_catalog.admin.data_flow_pb_detail AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L2'                              AS DATA_FLOW_GROUP_ID,
        'HR'                                              AS LOB,
        'silver'                                          AS SOURCE,
        'gold'                                            AS TARGET_OBJ_SCHEMA,
        'dim_employee'                                    AS TARGET_OBJ_NAME,
        CAST(1 AS INT)                                    AS PRIORITY,
        'Table'                                           AS TARGET_OBJ_TYPE,
        'SELECT
            employee_id,
            first_name,
            last_name,
            full_name,
            email,
            phone,
            department,
            position_title,
            salary,
            hire_date,
            employment_status,
            country,
            city,
            ingestion_timestamp
         FROM demo_catalog.silver.employee_master_clean' AS TRANSFORM_QUERY,
        NULL                                              AS GENERIC_SCRIPTS,
        'employee_id'                                     AS SOURCE_PK,
        'employee_id'                                     AS TARGET_PK,
        'SCD'                                             AS LOAD_TYPE,
        'Y'                                               AS IS_ACTIVE,
        'N'                                               AS LS_FLAG,
        NULL                                              AS LS_DETAIL,
        'department,employment_status'                    AS PARTITION_OR_INDEX,
        'admin'                                           AS INSERTED_BY,
        'admin'                                           AS UPDATED_BY,
        current_timestamp()                               AS INSERTED_TS,
        current_timestamp()                               AS UPDATED_TS,
        NULL                                              AS CUSTOM_SCRIPT_PARAMS,
        'PARTITION'                                       AS PARTITION_METHOD,
        '3650 days'                                       AS RETENTION_DETAILS,
        NULL                                              AS DEPLOYMENT_SOURCE_DFG
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID AND tgt.TARGET_OBJ_NAME = src.TARGET_OBJ_NAME
WHEN MATCHED THEN UPDATE SET
    tgt.LOB                   = src.LOB,
    tgt.SOURCE                = src.SOURCE,
    tgt.TARGET_OBJ_SCHEMA     = src.TARGET_OBJ_SCHEMA,
    tgt.PRIORITY              = src.PRIORITY,
    tgt.TARGET_OBJ_TYPE       = src.TARGET_OBJ_TYPE,
    tgt.TRANSFORM_QUERY       = src.TRANSFORM_QUERY,
    tgt.SOURCE_PK             = src.SOURCE_PK,
    tgt.TARGET_PK             = src.TARGET_PK,
    tgt.LOAD_TYPE             = src.LOAD_TYPE,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.PARTITION_OR_INDEX    = src.PARTITION_OR_INDEX,
    tgt.PARTITION_METHOD      = src.PARTITION_METHOD,
    tgt.RETENTION_DETAILS     = src.RETENTION_DETAILS,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;


-- L2 Table 2: fact_hire_stats (aggregated fact table, priority=2 — depends on dim_employee)
MERGE INTO demo_catalog.admin.data_flow_pb_detail AS tgt
USING (
    SELECT
        'EMPLOYEE_MASTER_L2'                              AS DATA_FLOW_GROUP_ID,
        'HR'                                              AS LOB,
        'gold'                                            AS SOURCE,
        'gold'                                            AS TARGET_OBJ_SCHEMA,
        'fact_hire_stats'                                 AS TARGET_OBJ_NAME,
        CAST(2 AS INT)                                    AS PRIORITY,
        'Table'                                           AS TARGET_OBJ_TYPE,
        'SELECT
            country,
            department,
            employment_status,
            COUNT(*)                          AS total_employees,
            SUM(CAST(salary AS DECIMAL(18,2)))   AS total_salary,
            AVG(CAST(salary AS DECIMAL(18,2)))   AS avg_salary,
            MIN(hire_date)                     AS earliest_hire,
            MAX(hire_date)                     AS latest_hire,
            current_timestamp()                AS ingestion_timestamp
         FROM demo_catalog.gold.dim_employee
         WHERE is_current = true
         GROUP BY country, department, employment_status'     AS TRANSFORM_QUERY,
        NULL                                              AS GENERIC_SCRIPTS,
        'country,department,employment_status'            AS SOURCE_PK,
        'country,department,employment_status'            AS TARGET_PK,
        'FULL'                                            AS LOAD_TYPE,
        'Y'                                               AS IS_ACTIVE,
        'N'                                               AS LS_FLAG,
        NULL                                              AS LS_DETAIL,
        'country,department'                              AS PARTITION_OR_INDEX,
        'admin'                                           AS INSERTED_BY,
        'admin'                                           AS UPDATED_BY,
        current_timestamp()                               AS INSERTED_TS,
        current_timestamp()                               AS UPDATED_TS,
        NULL                                              AS CUSTOM_SCRIPT_PARAMS,
        'PARTITION'                                       AS PARTITION_METHOD,
        NULL                                              AS RETENTION_DETAILS,
        NULL                                              AS DEPLOYMENT_SOURCE_DFG
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID AND tgt.TARGET_OBJ_NAME = src.TARGET_OBJ_NAME
WHEN MATCHED THEN UPDATE SET
    tgt.LOB                   = src.LOB,
    tgt.SOURCE                = src.SOURCE,
    tgt.TARGET_OBJ_SCHEMA     = src.TARGET_OBJ_SCHEMA,
    tgt.PRIORITY              = src.PRIORITY,
    tgt.TARGET_OBJ_TYPE       = src.TARGET_OBJ_TYPE,
    tgt.TRANSFORM_QUERY       = src.TRANSFORM_QUERY,
    tgt.SOURCE_PK             = src.SOURCE_PK,
    tgt.TARGET_PK             = src.TARGET_PK,
    tgt.LOAD_TYPE             = src.LOAD_TYPE,
    tgt.IS_ACTIVE             = src.IS_ACTIVE,
    tgt.PARTITION_OR_INDEX    = src.PARTITION_OR_INDEX,
    tgt.PARTITION_METHOD      = src.PARTITION_METHOD,
    tgt.UPDATED_BY            = src.UPDATED_BY,
    tgt.UPDATED_TS            = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;


-- ================================================================
-- Validation queries to confirm metadata is correct
-- ================================================================

-- Show what we just created:
SELECT 'CONTROL_HEADER' AS src, DATA_FLOW_GROUP_ID, ETL_LAYER, TRIGGER_TYPE, IS_ACTIVE
FROM demo_catalog.admin.data_flow_control_header
WHERE DATA_FLOW_GROUP_ID LIKE 'EMPLOYEE_MASTER_L%'
ORDER BY DATA_FLOW_GROUP_ID;

SELECT 'L0_DETAIL' AS src, DATA_FLOW_GROUP_ID, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, LOAD_TYPE, INPUT_FILE_FORMAT, IS_ACTIVE
FROM demo_catalog.admin.data_flow_l0_detail
WHERE DATA_FLOW_GROUP_ID = 'EMPLOYEE_MASTER_L0';

SELECT 'PB_DETAIL' AS src, DATA_FLOW_GROUP_ID, PRIORITY, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, TARGET_OBJ_TYPE, LOAD_TYPE, TARGET_PK, IS_ACTIVE
FROM demo_catalog.admin.data_flow_pb_detail
WHERE DATA_FLOW_GROUP_ID LIKE 'EMPLOYEE_MASTER_L%'
ORDER BY DATA_FLOW_GROUP_ID, PRIORITY, TARGET_OBJ_NAME;
