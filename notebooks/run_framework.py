# Databricks notebook source
# =============================================================
# run_framework.py
# Generic L0 (Raw → Bronze) ETL Framework
# Reads config from control tables and loads data dynamically.
#
# Parameter : GROUP_ID  (e.g. "WEATHER_L0")
# =============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## DATA INTEGRATION — run_framework
# MAGIC Reads pipeline config from `admin.data_flow_control_header`
# MAGIC and `admin.data_flow_l0_detail`, then ingests each source
# MAGIC into the bronze Delta table.

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("run_framework")

# COMMAND ----------
# ── Read notebook parameter ───────────────────────────────────
dbutils.widgets.text("GROUP_ID", "", "Pipeline Group ID")
GROUP_ID = dbutils.widgets.get("GROUP_ID").strip().upper()

if not GROUP_ID:
    raise ValueError("❌  GROUP_ID widget is empty. Pass a valid GROUP_ID.")

log.info(f"▶  Starting pipeline for GROUP_ID: {GROUP_ID}")

# COMMAND ----------
# ── Step 1: Read control header ───────────────────────────────
header_df = spark.sql(f"""
    SELECT *
    FROM   demo_catalog.admin.data_flow_control_header
    WHERE  DATA_FLOW_GROUP_ID = '{GROUP_ID}'
    AND    IS_ACTIVE           = 'Y'
""")

if header_df.count() == 0:
    raise ValueError(
        f"❌  No active record found in data_flow_control_header "
        f"for GROUP_ID: {GROUP_ID}"
    )

header = header_df.first()
log.info(f"✅  Header found: JOB_NAME={header['JOB_NAME']}")

# COMMAND ----------
# ── Step 2: Read L0 detail rows ───────────────────────────────
detail_df = spark.sql(f"""
    SELECT *
    FROM   demo_catalog.admin.data_flow_l0_detail
    WHERE  DATA_FLOW_GROUP_ID = '{GROUP_ID}'
    AND    IS_ACTIVE           = 'Y'
""")

detail_rows = detail_df.collect()
if not detail_rows:
    raise ValueError(
        f"❌  No active L0 detail rows found for GROUP_ID: {GROUP_ID}"
    )

log.info(f"✅  {len(detail_rows)} source(s) to process")

# COMMAND ----------
# ── Step 3: Helper functions ──────────────────────────────────

def read_source(source_url: str, file_format: str):
    """
    Read a source file into a Spark DataFrame.
    Supports: csv, json, parquet, delta
    """
    fmt = file_format.lower().strip()
    log.info(f"   Reading {fmt} from: {source_url}")

    if fmt == "csv":
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(source_url)
        )
    elif fmt == "json":
        return (
            spark.read
            .option("multiLine", "true")
            .json(source_url)
        )
    elif fmt == "parquet":
        return spark.read.parquet(source_url)
    elif fmt == "delta":
        return spark.read.format("delta").load(source_url)
    else:
        raise ValueError(f"Unsupported FILE_FORMAT: {fmt}")


def add_audit_columns(df, group_id: str):
    """Add standard audit columns to every ingested table."""
    return (
        df
        .withColumn("_etl_group_id",    F.lit(group_id))
        .withColumn("_etl_load_ts",     F.current_timestamp())
        .withColumn("_etl_source_file", F.input_file_name())
    )


def write_to_bronze(df, catalog: str, target_schema: str,
                    target_table: str, load_type: str):
    """
    Write DataFrame to a Delta table.
    FULL  → overwriteSchema=true (full refresh)
    INCREMENTAL → append
    """
    full_table = f"{catalog}.{target_schema}.{target_table}"
    lt = load_type.upper().strip()
    log.info(f"   Writing to {full_table} | mode: {lt}")

    # Ensure schema/database exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")

    if lt == "FULL":
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table)
        )
    elif lt == "INCREMENTAL":
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(full_table)
        )
    else:
        raise ValueError(f"Unsupported LOAD_TYPE: {load_type}")

    row_count = spark.table(full_table).count()
    log.info(f"   ✅  {full_table} → {row_count:,} rows")
    return row_count


def log_run(group_id: str, target_table: str, status: str,
            rows: int, error_msg: str, start_ts, end_ts):
    """Write one row to the run log table."""
    try:
        spark.sql(f"""
            INSERT INTO demo_catalog.admin.data_flow_run_log
            (DATA_FLOW_GROUP_ID, TARGET_TABLE, RUN_STATUS,
             ROWS_LOADED, ERROR_MESSAGE, START_TIME, END_TIME)
            VALUES
            ('{group_id}', '{target_table}', '{status}',
             {rows}, '{error_msg}',
             CAST('{start_ts}' AS TIMESTAMP),
             CAST('{end_ts}' AS TIMESTAMP))
        """)
    except Exception as e:
        log.warning(f"⚠️  Could not write run log: {e}")


# COMMAND ----------
# ── Step 4: Process each L0 detail row ───────────────────────

CATALOG = "demo_catalog"
overall_success = True
summary = []

for row in detail_rows:
    source_url   = row["SOURCE_URL"]
    target_schema = row["TARGET_SCHEMA"]
    target_table  = row["TARGET_TABLE"]
    file_format   = row["FILE_FORMAT"]
    load_type     = row["LOAD_TYPE"]

    start_ts = datetime.now()
    rows_loaded = 0
    status = "FAILED"
    error_msg = ""

    log.info(f"\n{'─'*60}")
    log.info(f"Processing: {target_schema}.{target_table}")
    log.info(f"Source    : {source_url}")
    log.info(f"Format    : {file_format} | LoadType: {load_type}")

    try:
        # Read
        source_df = read_source(source_url, file_format)

        # Add audit columns
        source_df = add_audit_columns(source_df, GROUP_ID)

        # Write
        rows_loaded = write_to_bronze(
            source_df, CATALOG, target_schema, target_table, load_type
        )
        status = "SUCCESS"
        log.info(f"✅  {target_table} — SUCCESS ({rows_loaded:,} rows)")

    except Exception as e:
        overall_success = False
        error_msg = str(e)[:500].replace("'", "''")
        log.error(f"❌  {target_table} — FAILED: {e}")

    finally:
        end_ts = datetime.now()
        log_run(GROUP_ID, f"{target_schema}.{target_table}",
                status, rows_loaded, error_msg, start_ts, end_ts)
        summary.append({
            "table"  : f"{target_schema}.{target_table}",
            "status" : status,
            "rows"   : rows_loaded,
            "error"  : error_msg
        })

# COMMAND ----------
# ── Step 5: Print summary ─────────────────────────────────────

print(f"\n{'═'*60}")
print(f"  PIPELINE SUMMARY — GROUP_ID: {GROUP_ID}")
print(f"{'═'*60}")
for s in summary:
    icon = "✅" if s["status"] == "SUCCESS" else "❌"
    print(f"  {icon}  {s['table']:<35} | {s['status']:<8} | {s['rows']:>10,} rows")
    if s["error"]:
        print(f"       ↳  Error: {s['error'][:80]}")
print(f"{'═'*60}")

if not overall_success:
    raise Exception(
        f"❌  One or more tasks FAILED for GROUP_ID: {GROUP_ID}. "
        f"Check run log: demo_catalog.admin.data_flow_run_log"
    )

log.info(f"🎉  Pipeline complete for GROUP_ID: {GROUP_ID}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Quick audit query
# MAGIC Run the cell below to check recent runs.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATA_FLOW_GROUP_ID,
# MAGIC     TARGET_TABLE,
# MAGIC     RUN_STATUS,
# MAGIC     ROWS_LOADED,
# MAGIC     round((unix_timestamp(END_TIME) - unix_timestamp(START_TIME)), 1) AS duration_secs,
# MAGIC     START_TIME,
# MAGIC     ERROR_MESSAGE
# MAGIC FROM demo_catalog.admin.data_flow_run_log
# MAGIC ORDER BY START_TIME DESC
# MAGIC LIMIT 20;