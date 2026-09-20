-- =============================================================================
-- create_control_tables.sql
-- Run ONCE in a Databricks SQL editor or notebook per workspace.
-- Replace 'demo_catalog' with your catalog name if different.
-- =============================================================================
-- CHANGELOG:
--   v2: Fixed audit_log DDL — added LOB, DURATION_SECONDS, RUN_ID, ENVIRONMENT
--       columns that write_audit() already writes (were missing, caused INSERT errors).
--   v2: Added JOB_NAME + NOTEBOOK_PATH removal note (not in live table).
-- =============================================================================

-- ── Schema ────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS demo_catalog.admin;

-- ── data_flow_control_header ──────────────────────────────────────────────
-- One row per pipeline group. Governs ALL layers.
CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_control_header (
    DATA_FLOW_GROUP_ID      STRING    NOT NULL COMMENT 'Unique pipeline group ID, e.g. EMPLOYEE_MASTER_L0',
    TRIGGER_TYPE            STRING             COMMENT 'JOB only (DLT not supported on Free Edition)',
    ETL_LAYER               STRING             COMMENT 'L0, L1, or L2',
    COMPUTE_CLASS_DEV       STRING             COMMENT 'Compute for dev — must be serverless on Free Edition',
    COMPUTE_CLASS           STRING             COMMENT 'Compute for qa/prod — must be serverless on Free Edition',
    IS_ACTIVE               STRING   DEFAULT 'Y' COMMENT 'Y=active, N=disabled',
    BUSINESS_OBJECT_NAME    STRING             COMMENT 'Human label for the data product',
    COST_CENTER             STRING             COMMENT 'Org cost centre (informational)',
    DATA_SME                STRING             COMMENT 'SME contact for data issues',
    BUSINESS_UNIT           STRING             COMMENT 'Business unit name',
    PRODUCT_OWNER           STRING             COMMENT 'Data product owner',
    INGESTION_MODE          STRING             COMMENT 'github / s3 / api / volume (L0 only)',
    INGESTION_BUCKET        STRING             COMMENT 'Logical bucket/category label',
    SPARK_CONFIGS           STRING             COMMENT 'JSON string of Spark conf overrides, e.g. {"spark.sql.shuffle.partitions":"8"}',
    WARNING_THRESHOLD_MINS  INT                COMMENT 'SLA alert if pipeline exceeds this many minutes',
    WARNING_DL_GROUP        STRING             COMMENT 'Alert recipient email/DL group',
    MIN_VERSION             STRING             COMMENT 'Stable fallback wheel version',
    MAX_VERSION             STRING             COMMENT 'Preferred wheel version (falls back to MIN_VERSION)',
    target_catalog          STRING             COMMENT 'Target catalog name (note: stored in lowercase)',
    INSERTED_BY             STRING             COMMENT 'Who inserted this row',
    UPDATED_BY              STRING             COMMENT 'Who last updated this row',
    INSERTED_TS             TIMESTAMP          COMMENT 'Row insert timestamp',
    UPDATED_TS              TIMESTAMP          COMMENT 'Row update timestamp'
)
USING DELTA
COMMENT 'Master control — which pipeline groups are active and how they run';

-- ── data_flow_l0_detail ───────────────────────────────────────────────────
-- One row per source file/object ingested at L0 (bronze).
CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_l0_detail (
    DATA_FLOW_GROUP_ID      STRING    NOT NULL COMMENT 'FK → data_flow_control_header',
    SOURCE                  STRING             COMMENT 'Full source URL (HTTP/S3/Volumes) — NOT the schema name',
    SOURCE_OBJ_SCHEMA       STRING             COMMENT 'TARGET schema in Databricks (e.g. raw, bronze) — misleading name, this is the write destination',
    SOURCE_OBJ_NAME         STRING             COMMENT 'Target table name (may include file extension, e.g. file.csv — framework strips extension)',
    LOB                     STRING             COMMENT 'Line of Business',
    LOAD_TYPE               STRING   DEFAULT 'FULL' COMMENT 'FULL=overwrite, DELTA=CDC-based incremental',
    INPUT_FILE_FORMAT       STRING             COMMENT 'csv / tsv / json / parquet / delta / avro / orc / xlsx / xml',
    STORAGE_TYPE            STRING             COMMENT 'Base URL/bucket path (used when SOURCE is relative)',
    DQ_LOGIC                STRING             COMMENT 'SQL WHERE expression for data quality filtering, e.g. "employee_id IS NOT NULL"',
    DELIMETER               STRING             COMMENT 'CSV delimiter (,) or XML row tag. Note: column name has typo by design — do not rename.',
    CUSTOM_SCHEMA           STRING             COMMENT 'DDL schema string for TSV/fixed-width: "col1 STRING, col2 INT"',
    CDC_LOGIC               STRING             COMMENT 'Watermark filter expression for DELTA loads, e.g. "updated_ts > ''{watermark}''"',
    TRANSFORM_QUERY         MAP<STRING,STRING> COMMENT 'Per-column cast MAP: {col_name: cast_expr}. Different from pb_detail.TRANSFORM_QUERY.',
    PRESTAG_FLAG            VARCHAR(1)         COMMENT 'Y=streaming (NOT supported on Free Edition, treated as N). N=managed Delta table.',
    PARTITION               STRING             COMMENT 'Comma-separated partition columns',
    LS_FLAG                 STRING             COMMENT 'Y=use LS_DETAIL as alternate source. N=standard.',
    LS_DETAIL               STRING             COMMENT 'Alternate source path/config when LS_FLAG=Y',
    IS_ACTIVE               STRING   DEFAULT 'Y' COMMENT 'Y=active, N=skip',
    INSERTED_BY             STRING,
    UPDATED_BY              STRING,
    INSERTED_TS             TIMESTAMP,
    UPDATED_TS              TIMESTAMP,
    DEPLOYMENT_SOURCE_DFG   STRING             COMMENT 'Which DFG originally deployed this row'
)
USING DELTA
COMMENT 'L0 (bronze) ingestion config — one row per source file/object';

-- ── data_flow_pb_detail ───────────────────────────────────────────────────
-- One row per target table/MV at L1 (silver) or L2 (gold).
CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID      STRING    NOT NULL COMMENT 'FK → data_flow_control_header',
    LOB                     STRING             COMMENT 'Line of Business',
    SOURCE                  STRING             COMMENT 'Source SCHEMA name (not URL). E.g. bronze. Different from l0_detail.SOURCE.',
    TARGET_OBJ_SCHEMA       STRING             COMMENT 'Target schema (e.g. silver, gold)',
    TARGET_OBJ_NAME         STRING             COMMENT 'Target table or MV name (no extension)',
    PRIORITY                INT                COMMENT 'Same priority = parallel execution. Lower number = runs first. NULL treated as 999.',
    TARGET_OBJ_TYPE         STRING             COMMENT 'Table or MV. MV may be skipped on Free Edition Serverless Jobs.',
    TRANSFORM_QUERY         STRING             COMMENT 'Full SQL SELECT. Different from l0_detail.TRANSFORM_QUERY (which is MAP<>).',
    GENERIC_SCRIPTS         STRING             COMMENT 'Comma-separated script names to execute (without .py)',
    SOURCE_PK               STRING             COMMENT 'Comma-separated source PKs. Fallback merge key if TARGET_PK is null.',
    TARGET_PK               STRING             COMMENT 'Comma-separated target PKs. Required for LOAD_TYPE=DELTA (MERGE) and SCD.',
    LOAD_TYPE               STRING             COMMENT 'FULL / DELTA (=upsert/MERGE at L1/L2) / SCD. Not applicable for MV.',
    IS_ACTIVE               STRING   DEFAULT 'Y',
    LS_FLAG                 STRING,
    LS_DETAIL               STRING,
    PARTITION_OR_INDEX      STRING             COMMENT 'Comma-separated partition or liquid cluster columns',
    INSERTED_BY             STRING,
    UPDATED_BY              STRING,
    INSERTED_TS             TIMESTAMP,
    UPDATED_TS              TIMESTAMP,
    CUSTOM_SCRIPT_PARAMS    MAP<STRING,STRING> COMMENT 'Params injected into GENERIC_SCRIPTS execution context',
    PARTITION_METHOD        STRING             COMMENT 'PARTITION (classic partitionBy) or LIQUID_CLUSTER (ALTER TABLE CLUSTER BY). Optional.',
    RETENTION_DETAILS       STRING             COMMENT 'Days to retain data (numeric string). E.g. "90" deletes rows older than 90 days.',
    DEPLOYMENT_SOURCE_DFG   STRING
)
USING DELTA
COMMENT 'L1/L2 (silver/gold) publish config — one row per target table or MV';

-- ── audit_log ─────────────────────────────────────────────────────────────
-- Written by run_framework.ipynb — one row per object per execution.
-- FIXED v2: Added LOB, DURATION_SECONDS, RUN_ID, ENVIRONMENT (were missing,
--            caused INSERT errors from write_audit() at runtime).
CREATE TABLE IF NOT EXISTS demo_catalog.admin.audit_log (
    DATA_FLOW_GROUP_ID      STRING             COMMENT 'Pipeline group that generated this entry',
    TARGET_TABLE            STRING             COMMENT 'Table being loaded',
    STATUS                  STRING             COMMENT 'SUCCESS / FAILED / SKIPPED / RUNNING',
    MESSAGE                 STRING             COMMENT 'Result message or truncated error traceback (max 400 chars)',
    CREATED_DATE            TIMESTAMP          COMMENT 'Log entry wall-clock time',
    ETL_LAYER               STRING             COMMENT 'L0, L1, or L2',
    LOB                     STRING             COMMENT 'Line of Business (added v2)',
    ROWS_PROCESSED          BIGINT             COMMENT 'Rows written to target table',
    DURATION_SECONDS        DOUBLE             COMMENT 'Execution time in seconds (added v2)',
    START_TIME              TIMESTAMP          COMMENT 'Processing start',
    END_TIME                TIMESTAMP          COMMENT 'Processing end',
    LOAD_TS                 TIMESTAMP          COMMENT 'current_timestamp() at INSERT time',
    RUN_ID                  STRING             COMMENT 'Databricks job run_id for cross-system correlation (added v2)',
    ENVIRONMENT             STRING             COMMENT 'dev / qa / prod (added v2)'
)
USING DELTA
COMMENT 'Audit trail — one row per object per execution'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ── ALTER TABLE for existing workspaces (apply if audit_log already exists) ──
-- Run these only if upgrading an existing workspace from v1 DDL:
--
-- ALTER TABLE demo_catalog.admin.audit_log ADD COLUMN LOB           STRING  COMMENT 'Line of Business';
-- ALTER TABLE demo_catalog.admin.audit_log ADD COLUMN DURATION_SECONDS DOUBLE COMMENT 'Execution time in seconds';
-- ALTER TABLE demo_catalog.admin.audit_log ADD COLUMN RUN_ID        STRING  COMMENT 'Databricks job run_id';
-- ALTER TABLE demo_catalog.admin.audit_log ADD COLUMN ENVIRONMENT   STRING  COMMENT 'dev / qa / prod';

-- ── Verify ────────────────────────────────────────────────────────────────
SHOW TABLES IN demo_catalog.admin;

-- ── etl_pipeline_lookup ───────────────────────────────────────────────────
-- Live lookup table — one row per (GROUP_ID, LAYER, TARGET_FULL_NAME).
-- Auto-upserted by kiro_etl_engine after every object completes.
-- Use this table to answer:
--   "What pipelines exist?"         → DISTINCT DATA_FLOW_GROUP_ID, ETL_LAYER
--   "What tables does L1 produce?"  → WHERE ETL_LAYER = 'L1'
--   "When did dim_employee last run?" → WHERE TARGET_FULL_NAME LIKE '%dim_employee%'
--   "Which pipelines are failing?"   → WHERE LAST_STATUS = 'FAILED'
--   "What is the row count lineage?" → ORDER BY TARGET_FULL_NAME
-- Key: (DATA_FLOW_GROUP_ID, ETL_LAYER, TARGET_FULL_NAME) — MERGE on these 3 cols.
CREATE TABLE IF NOT EXISTS demo_catalog.admin.etl_pipeline_lookup (
    DATA_FLOW_GROUP_ID   STRING    COMMENT 'Pipeline group that produced this object',
    ETL_LAYER            STRING    COMMENT 'L0, L1, or L2',
    LOB                  STRING    COMMENT 'Line of Business',
    SOURCE_REFERENCE     STRING    COMMENT 'L0: source URL. L1/L2: source schema name',
    TARGET_FULL_NAME     STRING    COMMENT 'Fully-qualified target: catalog.schema.table',
    LOAD_TYPE            STRING    COMMENT 'FULL / APPEND / DELTA / MERGE / SCD',
    OBJECT_TYPE          STRING    COMMENT 'TABLE or MV',
    MERGE_KEYS           STRING    COMMENT 'Comma-separated merge/PK columns (blank for FULL loads)',
    PARTITION_COLS       STRING    COMMENT 'Comma-separated partition or liquid-cluster columns',
    LAST_STATUS          STRING    COMMENT 'SUCCESS or FAILED from the most recent run',
    LAST_ROW_COUNT       BIGINT    COMMENT 'Row count in target after the most recent write',
    LAST_DURATION_SECS   DOUBLE    COMMENT 'Seconds taken by the most recent run',
    LAST_RUN_TS          TIMESTAMP COMMENT 'Timestamp of the most recent run',
    RUN_ID               STRING    COMMENT 'Databricks job run_id of the most recent run',
    ENVIRONMENT          STRING    COMMENT 'dev / qa / prod'
)
USING DELTA
COMMENT 'Live pipeline object registry — auto-maintained by kiro_etl_engine'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ── Verify all tables ─────────────────────────────────────────────────────
SHOW TABLES IN demo_catalog.admin;
