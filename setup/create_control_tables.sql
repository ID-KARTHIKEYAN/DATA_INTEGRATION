-- =============================================================
-- create_control_tables.sql
-- Run this in a Databricks SQL editor or notebook (once per workspace)
-- Replace 'demo_catalog' with your catalog name if different
-- =============================================================

-- ── Schema ────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS demo_catalog.admin;

-- ── Header table (one row per pipeline group) ─────────────────
CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_control_header (
    DATA_FLOW_GROUP_ID  STRING    NOT NULL   COMMENT 'Unique pipeline group ID e.g. WEATHER_L0',
    JOB_NAME            STRING               COMMENT 'Databricks job name',
    NOTEBOOK_PATH       STRING               COMMENT 'Full path to run_framework notebook',
    IS_ACTIVE           STRING    DEFAULT 'Y' COMMENT 'Y = active, N = disabled',
    CREATED_DATE        TIMESTAMP,
    UPDATED_DATE        TIMESTAMP
)
USING DELTA
COMMENT 'Master control — which pipeline groups are active';

-- ── L0 detail table (one row per source-to-bronze mapping) ────
CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_l0_detail (
    DATA_FLOW_GROUP_ID  STRING    NOT NULL   COMMENT 'FK to data_flow_control_header',
    SOURCE_URL          STRING               COMMENT 'HTTP/S3/ADLS URL of the source file',
    TARGET_SCHEMA       STRING               COMMENT 'Target schema (e.g. bronze)',
    TARGET_TABLE        STRING               COMMENT 'Target Delta table name',
    FILE_FORMAT         STRING               COMMENT 'csv / json / parquet / delta',
    LOAD_TYPE           STRING    DEFAULT 'FULL' COMMENT 'FULL = overwrite, INCREMENTAL = append',
    IS_ACTIVE           STRING    DEFAULT 'Y',
    CREATED_DATE        TIMESTAMP,
    UPDATED_DATE        TIMESTAMP
)
USING DELTA
COMMENT 'L0 (bronze) ingestion config per pipeline group';

-- ── Run log table (auto-populated by run_framework) ───────────
CREATE TABLE IF NOT EXISTS demo_catalog.admin.data_flow_run_log (
    LOG_ID              BIGINT    GENERATED ALWAYS AS IDENTITY,
    DATA_FLOW_GROUP_ID  STRING,
    TARGET_TABLE        STRING,
    RUN_STATUS          STRING    COMMENT 'SUCCESS / FAILED / RUNNING',
    ROWS_LOADED         BIGINT,
    ERROR_MESSAGE       STRING,
    START_TIME          TIMESTAMP,
    END_TIME            TIMESTAMP
)
USING DELTA
COMMENT 'Audit trail for every ETL run';

-- ── Verify ────────────────────────────────────────────────────
SHOW TABLES IN demo_catalog.admin;