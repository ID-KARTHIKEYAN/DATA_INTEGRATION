# Databricks notebook source
# =============================================================
# dlt_pipeline.py  — Delta Live Tables
# Medallion: Bronze (L0) → Silver (L1) → Gold (L2)
# Reads transformation logic from control tables
# GitHub: ID-KARTHIKEYAN/DATA_INTEGRATION
# =============================================================

# COMMAND ----------

import dlt
import requests
import pandas as pd
import io
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

CATALOG  = spark.conf.get("pipeline.catalog",  "demo_catalog")
GROUP_ID = spark.conf.get("pipeline.group_id", "FINANCE_MEDALLION_L0")

print(f"DLT Pipeline starting | GROUP_ID={GROUP_ID} | CATALOG={CATALOG}")

# COMMAND ----------
# ── Read control tables once ───────────────────────────────────

def get_l0_sources():
    return spark.sql(f"""
        SELECT SOURCE_URL, SOURCE_OBJ_NAME, TARGET_SCHEMA,
               TARGET_TABLE, INPUT_FILE_FORMAT, LOAD_TYPE, DELIMETER
        FROM {CATALOG}.admin.data_flow_l0_detail
        WHERE DATA_FLOW_GROUP_ID = '{GROUP_ID}'
        AND   IS_ACTIVE = 'Y'
    """).collect()

def get_l1_sources():
    return spark.sql(f"""
        SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, TARGET_TABLE,
               TRANSFORMATION_QUERY, MERGE_KEYS
        FROM {CATALOG}.admin.data_flow_l1_detail
        WHERE DATA_FLOW_GROUP_ID = '{GROUP_ID}'
        AND   IS_ACTIVE = 'Y'
    """).collect()

def get_l2_sources():
    return spark.sql(f"""
        SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, TARGET_TABLE,
               TRANSFORMATION_QUERY
        FROM {CATALOG}.admin.data_flow_l2_detail
        WHERE DATA_FLOW_GROUP_ID = '{GROUP_ID}'
        AND   IS_ACTIVE = 'Y'
    """).collect()

# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# BRONZE LAYER (L0) — Raw ingestion from HTTP sources
# ══════════════════════════════════════════════════════════════

def make_bronze_table(row):
    """
    Factory: returns a DLT table function for each L0 source row.
    DLT requires table functions defined at import time, so we
    generate them dynamically and register via dlt.table decorator.
    """
    source_url  = row['SOURCE_URL']
    table_name  = row['TARGET_TABLE']
    file_format = (row['INPUT_FILE_FORMAT'] or row['FILE_FORMAT'] or 'csv').lower()
    delimiter   = row['DELIMETER'] or ','
    group_id    = GROUP_ID

    @dlt.table(
        name    = table_name,
        comment = f"Bronze: {GROUP_ID} | source={source_url}",
        table_properties = {
            "quality"              : "bronze",
            "etl.group_id"        : group_id,
            "etl.layer"           : "L0",
            "delta.autoOptimize.optimizeWrite" : "true"
        }
    )
    def _bronze():
        resp = requests.get(source_url, timeout=60)
        resp.raise_for_status()

        if file_format == 'csv':
            pdf = pd.read_csv(io.BytesIO(resp.content))
        elif file_format == 'json':
            pdf = pd.read_json(io.BytesIO(resp.content))
        elif file_format in ('excel', 'xlsx'):
            pdf = pd.read_excel(io.BytesIO(resp.content))
        elif file_format == 'parquet':
            pdf = pd.read_parquet(io.BytesIO(resp.content))
        else:
            pdf = pd.read_csv(io.BytesIO(resp.content))

        return (
            spark.createDataFrame(pdf)
                 .withColumn("_etl_group_id", F.lit(group_id))
                 .withColumn("_etl_layer",    F.lit("L0"))
                 .withColumn("_etl_load_ts",  F.current_timestamp())
                 .withColumn("_dlt_run_id",   F.lit(spark.conf.get("pipelines.id", "unknown")))
        )

    return _bronze

# Register all Bronze tables from control table
for _row in get_l0_sources():
    make_bronze_table(_row)

# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# SILVER LAYER (L1) — Clean + conform
# Runs TRANSFORMATION_QUERY from data_flow_l1_detail
# ══════════════════════════════════════════════════════════════

def make_silver_table(row):
    src_schema  = row['SOURCE_OBJ_SCHEMA'] or 'LIVE'
    src_table   = row['SOURCE_OBJ_NAME']
    table_name  = row['TARGET_TABLE']
    trans_query = row['TRANSFORMATION_QUERY']
    merge_keys  = row['MERGE_KEYS']
    group_id    = GROUP_ID

    # Build expectations from merge keys (basic not-null DQ)
    expectations = {}
    if merge_keys:
        for k in merge_keys.split(','):
            k = k.strip()
            expectations[f"{k}_not_null"] = f"{k} IS NOT NULL"

    @dlt.table(
        name    = table_name,
        comment = f"Silver: {GROUP_ID} | src={src_table}",
        table_properties = {
            "quality"       : "silver",
            "etl.group_id" : group_id,
            "etl.layer"    : "L1"
        }
    )
    @dlt.expect_all_or_drop(expectations) if expectations else lambda f: f
    def _silver():
        if trans_query:
            # Replace table refs to use LIVE. prefix for DLT lineage
            final_query = trans_query.replace(
                f"{src_schema}.", "LIVE."
            ).replace(
                f"FROM {src_table}", f"FROM LIVE.{src_table}"
            )
            return (
                spark.sql(final_query)
                     .withColumn("_etl_group_id", F.lit(group_id))
                     .withColumn("_etl_layer",    F.lit("L1"))
                     .withColumn("_etl_load_ts",  F.current_timestamp())
            )
        else:
            return dlt.read(src_table) \
                      .withColumn("_etl_layer", F.lit("L1")) \
                      .withColumn("_etl_load_ts", F.current_timestamp())

    return _silver

for _row in get_l1_sources():
    make_silver_table(_row)

# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# GOLD LAYER (L2) — Aggregated / business-ready
# Runs TRANSFORMATION_QUERY from data_flow_l2_detail
# ══════════════════════════════════════════════════════════════

def make_gold_table(row):
    src_schema  = row['SOURCE_OBJ_SCHEMA'] or 'LIVE'
    src_table   = row['SOURCE_OBJ_NAME']
    table_name  = row['TARGET_TABLE']
    trans_query = row['TRANSFORMATION_QUERY']
    group_id    = GROUP_ID

    @dlt.table(
        name    = table_name,
        comment = f"Gold: {GROUP_ID} | src={src_table}",
        table_properties = {
            "quality"       : "gold",
            "etl.group_id" : group_id,
            "etl.layer"    : "L2"
        }
    )
    def _gold():
        if trans_query:
            final_query = trans_query.replace(
                f"{src_schema}.", "LIVE."
            ).replace(
                f"FROM {src_table}", f"FROM LIVE.{src_table}"
            )
            return (
                spark.sql(final_query)
                     .withColumn("_etl_group_id", F.lit(group_id))
                     .withColumn("_etl_layer",    F.lit("L2"))
                     .withColumn("_etl_load_ts",  F.current_timestamp())
            )
        else:
            return dlt.read(src_table) \
                      .withColumn("_etl_layer", F.lit("L2")) \
                      .withColumn("_etl_load_ts", F.current_timestamp())

    return _gold

for _row in get_l2_sources():
    make_gold_table(_row)
