# Databricks notebook source
# =============================================================
# WEATHER_L0.py
# One-time setup: inserts config rows for the WEATHER pipeline.
# Run this ONCE per workspace after control tables are created.
# =============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## WEATHER_L0 — Pipeline Registration
# MAGIC Run this notebook **once** to register the Weather pipeline
# MAGIC in the control tables. After this, use Jenkins to trigger
# MAGIC `run_framework` with `GROUP_ID = WEATHER_L0`.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- ── Header: register the pipeline group ──────────────────────
# MAGIC MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         'WEATHER_L0'                                           AS DATA_FLOW_GROUP_ID,
# MAGIC         'WEATHER_L0_JOB'                                       AS JOB_NAME,
# MAGIC         '/Workspace/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/notebooks/run_framework'
# MAGIC                                                                AS NOTEBOOK_PATH,
# MAGIC         'Y'                                                    AS IS_ACTIVE,
# MAGIC         current_timestamp()                                    AS CREATED_DATE,
# MAGIC         current_timestamp()                                    AS UPDATED_DATE
# MAGIC ) AS src
# MAGIC ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         tgt.JOB_NAME      = src.JOB_NAME,
# MAGIC         tgt.NOTEBOOK_PATH = src.NOTEBOOK_PATH,
# MAGIC         tgt.IS_ACTIVE     = src.IS_ACTIVE,
# MAGIC         tgt.UPDATED_DATE  = src.UPDATED_DATE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (DATA_FLOW_GROUP_ID, JOB_NAME, NOTEBOOK_PATH,
# MAGIC             IS_ACTIVE, CREATED_DATE, UPDATED_DATE)
# MAGIC     VALUES (src.DATA_FLOW_GROUP_ID, src.JOB_NAME, src.NOTEBOOK_PATH,
# MAGIC             src.IS_ACTIVE, src.CREATED_DATE, src.UPDATED_DATE);

# COMMAND ----------
# MAGIC %sql
# MAGIC -- ── Detail: register the source-to-bronze mapping ────────────
# MAGIC MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         'WEATHER_L0'                                                          AS DATA_FLOW_GROUP_ID,
# MAGIC         'https://raw.githubusercontent.com/Devasree03/ETL_JEN/refs/heads/main/data/weather.csv'
# MAGIC                                                                               AS SOURCE_URL,
# MAGIC         'bronze'                                                              AS TARGET_SCHEMA,
# MAGIC         'Wea_data'                                                            AS TARGET_TABLE,
# MAGIC         'csv'                                                                 AS FILE_FORMAT,
# MAGIC         'FULL'                                                                AS LOAD_TYPE,
# MAGIC         'Y'                                                                   AS IS_ACTIVE,
# MAGIC         current_timestamp()                                                   AS CREATED_DATE,
# MAGIC         current_timestamp()                                                   AS UPDATED_DATE
# MAGIC ) AS src
# MAGIC ON  tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
# MAGIC AND tgt.TARGET_TABLE       = src.TARGET_TABLE
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         tgt.SOURCE_URL   = src.SOURCE_URL,
# MAGIC         tgt.FILE_FORMAT  = src.FILE_FORMAT,
# MAGIC         tgt.LOAD_TYPE    = src.LOAD_TYPE,
# MAGIC         tgt.IS_ACTIVE    = src.IS_ACTIVE,
# MAGIC         tgt.UPDATED_DATE = src.UPDATED_DATE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (DATA_FLOW_GROUP_ID, SOURCE_URL, TARGET_SCHEMA, TARGET_TABLE,
# MAGIC             FILE_FORMAT, LOAD_TYPE, IS_ACTIVE, CREATED_DATE, UPDATED_DATE)
# MAGIC     VALUES (src.DATA_FLOW_GROUP_ID, src.SOURCE_URL, src.TARGET_SCHEMA,
# MAGIC             src.TARGET_TABLE, src.FILE_FORMAT, src.LOAD_TYPE,
# MAGIC             src.IS_ACTIVE, src.CREATED_DATE, src.UPDATED_DATE);

# COMMAND ----------
# MAGIC %sql
# MAGIC -- ── Verify ────────────────────────────────────────────────────
# MAGIC SELECT 'HEADER' AS config_table, * FROM demo_catalog.admin.data_flow_control_header WHERE DATA_FLOW_GROUP_ID = 'WEATHER_L0'
# MAGIC UNION ALL
# MAGIC SELECT 'DETAIL' AS config_table, DATA_FLOW_GROUP_ID, SOURCE_URL, TARGET_SCHEMA,
# MAGIC        TARGET_TABLE, FILE_FORMAT, LOAD_TYPE, IS_ACTIVE, CREATED_DATE, UPDATED_DATE
# MAGIC FROM   demo_catalog.admin.data_flow_l0_detail  WHERE DATA_FLOW_GROUP_ID = 'WEATHER_L0';