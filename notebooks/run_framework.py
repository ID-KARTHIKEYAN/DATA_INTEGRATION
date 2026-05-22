# Databricks notebook source
# =============================================================
# run_framework.py
# Reads config from control tables, supports:
#   - File formats : csv, json, parquet, delta
#   - Load types   : FULL (overwrite), INCREMENTAL (append)
# =============================================================

# COMMAND ----------

dbutils.widgets.text("GROUP_ID", "")
group_id = dbutils.widgets.get("GROUP_ID").strip().upper()

if not group_id:
    raise Exception("GROUP_ID is empty. Pass a valid GROUP_ID.")

print(f"GROUP_ID : {group_id}")

# COMMAND ----------

import pandas as pd
import traceback
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------
# ── Audit log helper ──────────────────────────────────────────

def write_audit(group_id, table_name, status, message):
    """Write one row to audit_log. Never raises — audit must not kill the run."""
    try:
        safe_msg = message.replace("'", "''")[:500]
        spark.sql(f"""
            INSERT INTO demo_catalog.admin.audit_log
            VALUES (
                '{group_id}',
                '{table_name}',
                '{status}',
                '{safe_msg}',
                current_timestamp()
            )
        """)
    except Exception as audit_err:
        print(f"[AUDIT WRITE FAILED] {audit_err}")

# COMMAND ----------
# ── Read source helper ────────────────────────────────────────

def read_source(source_url, file_format):
    """
    Read any supported format into a Spark DataFrame.
    Raises a clear error for unsupported formats.
    """
    fmt = file_format.strip().lower()
    print(f"  Reading [{fmt}] from: {source_url}")

    if fmt == "csv":
        # Try Spark native first (handles large files)
        try:
            return (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .option("escape", '"')
                .csv(source_url)
            )
        except Exception:
            # Fall back to pandas for HTTP URLs
            pandas_df = pd.read_csv(source_url)
            return spark.createDataFrame(pandas_df)

    elif fmt == "json":
        try:
            return (
                spark.read
                .option("multiLine", "true")
                .json(source_url)
            )
        except Exception:
            pandas_df = pd.read_json(source_url)
            return spark.createDataFrame(pandas_df)

    elif fmt == "parquet":
        return spark.read.parquet(source_url)

    elif fmt == "delta":
        return spark.read.format("delta").load(source_url)

    elif fmt == "excel" or fmt == "xlsx":
        pandas_df = pd.read_excel(source_url)
        return spark.createDataFrame(pandas_df)

    else:
        raise ValueError(
            f"Unsupported FILE_FORMAT '{file_format}'. "
            f"Allowed: csv, json, parquet, delta, excel"
        )

# COMMAND ----------
# ── Write helper ──────────────────────────────────────────────

def write_table(df, full_table_name, load_type):
    """
    FULL        → overwrite entire table (recreate schema too)
    INCREMENTAL → append new rows only
    """
    lt = load_type.strip().upper()

    # Ensure schema/database exists
    parts = full_table_name.split(".")
    if len(parts) == 3:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {parts[0]}.{parts[1]}")

    if lt == "FULL":
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )

    elif lt == "INCREMENTAL":
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(full_table_name)
        )

    else:
        raise ValueError(
            f"Unsupported LOAD_TYPE '{load_type}'. "
            f"Allowed: FULL, INCREMENTAL"
        )

    return spark.table(full_table_name).count()

# COMMAND ----------
# ── Step 1 : Read control table ───────────────────────────────

control_df = spark.sql(f"""
    SELECT
        DATA_FLOW_GROUP_ID,
        SOURCE_URL,
        TARGET_SCHEMA,
        TARGET_TABLE,
        FILE_FORMAT,
        LOAD_TYPE,
        IS_ACTIVE
    FROM demo_catalog.admin.data_flow_l0_detail
    WHERE DATA_FLOW_GROUP_ID = '{group_id}'
    AND   IS_ACTIVE           = 'Y'
    ORDER BY TARGET_TABLE
""")

metadata_list = control_df.collect()

if not metadata_list:
    raise Exception(
        f"No active rows found in data_flow_l0_detail "
        f"for GROUP_ID = '{group_id}'. "
        f"Check IS_ACTIVE = 'Y' and GROUP_ID spelling."
    )

print(f"Found {len(metadata_list)} source(s) to process\n")

# COMMAND ----------
# ── Step 2 : Process each row ─────────────────────────────────

results  = []
all_ok   = True

for row in metadata_list:

    source_url    = row["SOURCE_URL"]
    target_schema = row["TARGET_SCHEMA"]
    target_table  = row["TARGET_TABLE"]
    file_format   = row["FILE_FORMAT"]
    load_type     = row["LOAD_TYPE"] or "FULL"
    full_name     = f"demo_catalog.{target_schema}.{target_table}"

    print(f"{'─'*55}")
    print(f"Table     : {full_name}")
    print(f"Source    : {source_url}")
    print(f"Format    : {file_format}  |  Load type: {load_type}")

    start_ts = datetime.now()
    status   = "FAILED"
    message  = ""
    rows     = 0

    try:
        # ── Read ──────────────────────────────────────────────
        df = read_source(source_url, file_format)

        # ── Add audit columns ─────────────────────────────────
        df = (
            df
            .withColumn("_etl_group_id",  F.lit(group_id))
            .withColumn("_etl_load_ts",   F.current_timestamp())
            .withColumn("_etl_load_type", F.lit(load_type))
        )

        # ── Write ─────────────────────────────────────────────
        rows    = write_table(df, full_name, load_type)
        status  = "SUCCESS"
        message = f"Loaded {rows:,} rows ({load_type})"
        print(f"✅ {message}")

    except Exception as e:
        all_ok  = False
        status  = "FAILED"
        # Full traceback so you can see exactly what went wrong
        message = traceback.format_exc()
        print(f"❌ FAILED — {target_table}")
        print(message)

    finally:
        end_ts   = datetime.now()
        duration = round((end_ts - start_ts).total_seconds(), 1)
        print(f"Duration  : {duration}s")
        write_audit(group_id, target_table, status, message)

    results.append({
        "table"    : full_name,
        "format"   : file_format,
        "load_type": load_type,
        "rows"     : rows,
        "status"   : status,
        "duration" : duration
    })

# COMMAND ----------
# ── Step 3 : Summary ──────────────────────────────────────────

print(f"\n{'═'*55}")
print(f"  SUMMARY — GROUP_ID: {group_id}")
print(f"{'═'*55}")
print(f"  {'TABLE':<30} {'TYPE':<13} {'ROWS':>8}  STATUS")
print(f"  {'─'*30} {'─'*13} {'─'*8}  {'─'*10}")

for r in results:
    icon = "✅" if r["status"] == "SUCCESS" else "❌"
    tbl  = r["table"].split(".")[-1][:29]
    lt   = r["load_type"][:12]
    print(f"  {icon} {tbl:<28} {lt:<13} {r['rows']:>8,}  {r['status']}")

print(f"{'═'*55}")
print(f"  Total: {len(results)} | "
      f"Success: {sum(1 for r in results if r['status']=='SUCCESS')} | "
      f"Failed: {sum(1 for r in results if r['status']=='FAILED')}")

# ── Fail the job if ANY table failed ─────────────────────────
if not all_ok:
    failed_tables = [r["table"] for r in results if r["status"] == "FAILED"]
    raise Exception(
        f"PIPELINE FAILED for GROUP_ID: {group_id}\n"
        f"Failed tables: {', '.join(failed_tables)}\n"
        f"Check audit_log: SELECT * FROM demo_catalog.admin.audit_log "
        f"WHERE DATA_FLOW_GROUP_ID = '{group_id}' ORDER BY LOAD_TS DESC"
    )

print(f"\n🎉 All tables loaded successfully for {group_id}")
