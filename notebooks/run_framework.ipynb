# Databricks notebook source
# =============================================================
# run_framework.py
# Supports: csv, json, parquet, delta, excel
# Load types: FULL (overwrite), INCREMENTAL (append)
# Source: https:// URLs → pandas | dbfs/s3/abfss → Spark native
# =============================================================

# COMMAND ----------

dbutils.widgets.text("GROUP_ID", "")
group_id = dbutils.widgets.get("GROUP_ID").strip().upper()

if not group_id:
    raise Exception("GROUP_ID is empty. Pass a valid GROUP_ID.")

print(f"GROUP_ID : {group_id}")

# COMMAND ----------

import pandas as pd
import requests
import io
import traceback
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------
# ── Detect if URL is HTTP ─────────────────────────────────────

def is_http(url):
    return url.strip().lower().startswith("http://") or \
           url.strip().lower().startswith("https://")

# COMMAND ----------
# ── Read source ───────────────────────────────────────────────

def read_source(source_url, file_format):
    fmt = file_format.strip().lower()
    url = source_url.strip()
    print(f"  Format : {fmt} | Source : {url}")

    if is_http(url):
        # ── HTTP/HTTPS → always download via requests then parse
        print("  Method : pandas (HTTP URL)")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()          # raises if 404 / 403 etc.
        raw  = resp.content

        if fmt == "csv":
            pandas_df = pd.read_csv(io.BytesIO(raw))

        elif fmt == "json":
            pandas_df = pd.read_json(io.BytesIO(raw))

        elif fmt in ("excel", "xlsx", "xls"):
            pandas_df = pd.read_excel(io.BytesIO(raw))

        elif fmt == "parquet":
            pandas_df = pd.read_parquet(io.BytesIO(raw))

        else:
            raise ValueError(
                f"Format '{fmt}' is not supported for HTTP URLs. "
                f"Allowed for HTTP: csv, json, excel, parquet"
            )

        return spark.createDataFrame(pandas_df)

    else:
        # ── DBFS / S3 / ABFSS → Spark native (fast, distributed)
        print("  Method : Spark native (cloud storage)")

        if fmt == "csv":
            return (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .option("escape", '"')
                .csv(url)
            )

        elif fmt == "json":
            return spark.read.option("multiLine", "true").json(url)

        elif fmt == "parquet":
            return spark.read.parquet(url)

        elif fmt == "delta":
            return spark.read.format("delta").load(url)

        elif fmt in ("excel", "xlsx", "xls"):
            # Excel needs pandas even from cloud — read via dbutils
            local = "/tmp/_fw_excel_tmp.xlsx"
            dbutils.fs.cp(url, "file:" + local)
            pandas_df = pd.read_excel(local)
            return spark.createDataFrame(pandas_df)

        else:
            raise ValueError(
                f"Format '{fmt}' not supported. "
                f"Allowed: csv, json, parquet, delta, excel"
            )

# COMMAND ----------
# ── Write table ───────────────────────────────────────────────

def write_table(df, full_table_name, load_type):
    lt    = load_type.strip().upper()
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
            f"LOAD_TYPE '{load_type}' not supported. "
            f"Allowed: FULL, INCREMENTAL"
        )

    return spark.table(full_table_name).count()

# COMMAND ----------
# ── Audit log ─────────────────────────────────────────────────

def write_audit(group_id, table_name, status, message):
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
    except Exception as e:
        print(f"[AUDIT WRITE FAILED] {e}")

# COMMAND ----------
# ── Step 1 : Read control table ───────────────────────────────

control_df = spark.sql(f"""
    SELECT
        DATA_FLOW_GROUP_ID,
        SOURCE_URL,
        TARGET_SCHEMA,
        TARGET_TABLE,
        FILE_FORMAT,
        LOAD_TYPE
    FROM demo_catalog.admin.data_flow_l0_detail
    WHERE DATA_FLOW_GROUP_ID = '{group_id}'
    AND   IS_ACTIVE           = 'Y'
    ORDER BY TARGET_TABLE
""")

metadata_list = control_df.collect()

if not metadata_list:
    raise Exception(
        f"No active rows found in data_flow_l0_detail "
        f"for GROUP_ID='{group_id}'. Check IS_ACTIVE='Y'."
    )

print(f"Found {len(metadata_list)} source(s)\n")

# COMMAND ----------
# ── Step 2 : Process each row ─────────────────────────────────

results = []
all_ok  = True

for row in metadata_list:

    source_url    = row["SOURCE_URL"]
    target_schema = row["TARGET_SCHEMA"]
    target_table  = row["TARGET_TABLE"]
    file_format   = row["FILE_FORMAT"]
    load_type     = row["LOAD_TYPE"] or "FULL"
    full_name     = f"demo_catalog.{target_schema}.{target_table}"

    print(f"{'─'*55}")
    print(f"Table     : {full_name}")
    print(f"Load type : {load_type}")

    start_ts = datetime.now()
    status   = "FAILED"
    message  = ""
    rows     = 0

    try:
        df = read_source(source_url, file_format)

        df = (
            df
            .withColumn("_etl_group_id",  F.lit(group_id))
            .withColumn("_etl_load_ts",   F.current_timestamp())
            .withColumn("_etl_load_type", F.lit(load_type))
        )

        rows    = write_table(df, full_name, load_type)
        status  = "SUCCESS"
        message = f"Loaded {rows:,} rows ({load_type})"
        print(f"✅ {message}")

    except Exception as e:
        all_ok  = False
        status  = "FAILED"
        message = traceback.format_exc()
        print(f"❌ FAILED — {target_table}\n{message}")

    finally:
        duration = round((datetime.now() - start_ts).total_seconds(), 1)
        print(f"Duration  : {duration}s")
        write_audit(group_id, target_table, status, message)

    results.append({
        "table"    : full_name,
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
print(f"  {'─'*30} {'─'*13} {'─'*8}  {'─'*8}")

for r in results:
    icon = "✅" if r["status"] == "SUCCESS" else "❌"
    tbl  = r["table"].split(".")[-1][:29]
    print(f"  {icon} {tbl:<28} {r['load_type']:<13} {r['rows']:>8,}  {r['status']}")

success_count = sum(1 for r in results if r["status"] == "SUCCESS")
failed_count  = sum(1 for r in results if r["status"] == "FAILED")

print(f"{'═'*55}")
print(f"  Total: {len(results)} | Success: {success_count} | Failed: {failed_count}")

if not all_ok:
    failed_tables = [r["table"] for r in results if r["status"] == "FAILED"]
    raise Exception(
        f"FAILED tables: {', '.join(failed_tables)}\n"
        f"Check: SELECT * FROM demo_catalog.admin.audit_log "
        f"WHERE DATA_FLOW_GROUP_ID = '{group_id}' ORDER BY LOAD_TS DESC"
    )

print(f"\n🎉 All tables loaded successfully for {group_id}")
