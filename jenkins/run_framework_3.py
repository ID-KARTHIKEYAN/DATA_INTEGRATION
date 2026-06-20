# Databricks notebook source
# =============================================================
# run_framework.py
# Medallion Architecture — L0 (Bronze) / L1 (Silver) / L2 (Gold)
# Features:
#   - Auto-detects layer from GROUP_ID suffix (_L0 / _L1 / _L2)
#   - Auto-detects catalog (demo_catalog → hive_metastore fallback)
#   - Reads TRANSFORMATION_QUERY from control tables for L1 / L2
#   - Supports TABLE and VIEW creation
#   - Rich error messages — shows exactly what failed and why
#   - Full audit log with rows, duration, start/end time
# =============================================================

# COMMAND ----------
# ── STEP 1 : Widgets ──────────────────────────────────────────

dbutils.widgets.text("GROUP_ID",  "")
dbutils.widgets.text("RUN_LAYER", "ALL")   # ALL / L0 / L1 / L2

group_id  = dbutils.widgets.get("GROUP_ID").strip().upper()
run_layer = dbutils.widgets.get("RUN_LAYER").strip().upper()

# COMMAND ----------
# ── STEP 2 : Validate ─────────────────────────────────────────

if not group_id:
    raise Exception(
        "❌  GROUP_ID widget is empty.\n"
        "    Fix : Pass GROUP_ID when triggering the job.\n"
        "    Example : GROUP_ID = FINANCE_MEDALLION_L0"
    )

# Auto-detect layer from GROUP_ID suffix
if run_layer == "ALL":
    if   group_id.endswith("_L0"): run_layer = "L0"
    elif group_id.endswith("_L1"): run_layer = "L1"
    elif group_id.endswith("_L2"): run_layer = "L2"

print(f"{'═'*55}")
print(f"  GROUP_ID  : {group_id}")
print(f"  RUN_LAYER : {run_layer}")
print(f"{'═'*55}")

# COMMAND ----------
# ── STEP 3 : Auto-detect catalog ──────────────────────────────

_PREFERRED = "demo_catalog"

try:
    available = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    if _PREFERRED in available:
        CATALOG = _PREFERRED
    elif "hive_metastore" in available:
        CATALOG = "hive_metastore"
        print(f"⚠️  '{_PREFERRED}' not found → using '{CATALOG}' (free edition default)")
    else:
        CATALOG = available[0] if available else _PREFERRED
        print(f"⚠️  '{_PREFERRED}' not found → using '{CATALOG}'")
except Exception:
    CATALOG = "hive_metastore"
    print(f"⚠️  Could not list catalogs → defaulting to '{CATALOG}'")

print(f"  CATALOG   : {CATALOG}\n")

# COMMAND ----------
# ── STEP 4 : Imports ──────────────────────────────────────────

import pandas as pd
import requests
import io
import traceback
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------
# ── STEP 5 : Helper — pretty error ───────────────────────────

def fmt_error(context, exc, query=None):
    """Return a clear, readable error string for logs and audit."""
    lines = [
        f"",
        f"  ┌─ ERROR in {context} {'─'*max(0,44-len(context))}",
        f"  │  Type    : {type(exc).__name__}",
        f"  │  Message : {str(exc)[:300]}",
    ]
    if query:
        lines.append(f"  │  Query   : {query[:200]}")
    lines.append(f"  └{'─'*50}")
    return "\n".join(lines)

# COMMAND ----------
# ── STEP 6 : Helper — read source ─────────────────────────────

def read_source(source_url, file_format, delimiter=","):
    fmt = (file_format or "csv").strip().lower()
    url = (source_url or "").strip()

    if not url:
        raise ValueError(
            "SOURCE_URL is empty.\n"
            "    Fix : Add SOURCE_URL in data_flow_l0_detail for this GROUP_ID."
        )

    print(f"   Reading  : {url}")
    print(f"   Format   : {fmt}")

    is_http = url.lower().startswith("http")

    if is_http:
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ValueError(
                f"HTTP {resp.status_code} when downloading source file.\n"
                f"    URL    : {url}\n"
                f"    Detail : {e}\n"
                f"    Fix    : Check the URL is public and accessible."
            )
        except requests.exceptions.ConnectionError:
            raise ValueError(
                f"Cannot reach URL: {url}\n"
                f"    Fix : Check network access from Databricks to the internet."
            )

        raw = resp.content

        if fmt == "csv":
            sep = delimiter if delimiter else ","
            return spark.createDataFrame(pd.read_csv(io.BytesIO(raw), sep=sep))
        elif fmt == "json":
            return spark.createDataFrame(pd.read_json(io.BytesIO(raw)))
        elif fmt == "parquet":
            return spark.createDataFrame(pd.read_parquet(io.BytesIO(raw)))
        elif fmt in ("excel", "xlsx", "xls"):
            return spark.createDataFrame(pd.read_excel(io.BytesIO(raw)))
        else:
            raise ValueError(
                f"Unsupported file format '{fmt}' for HTTP source.\n"
                f"    Allowed : csv, json, parquet, excel\n"
                f"    Fix     : Update FILE_FORMAT in data_flow_l0_detail."
            )
    else:
        # Cloud storage path (dbfs / s3 / abfss)
        if fmt == "csv":
            return (spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("sep", delimiter or ",")
                    .csv(url))
        elif fmt == "json":
            return spark.read.option("multiLine", "true").json(url)
        elif fmt == "parquet":
            return spark.read.parquet(url)
        elif fmt == "delta":
            return spark.read.format("delta").load(url)
        else:
            raise ValueError(
                f"Unsupported file format '{fmt}' for cloud path.\n"
                f"    Allowed : csv, json, parquet, delta\n"
                f"    Fix     : Update FILE_FORMAT in data_flow_l0_detail."
            )

# COMMAND ----------
# ── STEP 7 : Helper — write table ─────────────────────────────

def ensure_schema(schema):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    except Exception as e:
        raise RuntimeError(
            f"Cannot create schema '{CATALOG}.{schema}'.\n"
            f"    Detail : {e}\n"
            f"    Fix    : Check you have CREATE privilege on catalog '{CATALOG}'."
        )

def write_full(df, full_table):
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table))

def write_append(df, full_table):
    (df.write.format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(full_table))

def write_merge(df, full_table, merge_keys_str):
    if not merge_keys_str:
        raise ValueError(
            f"LOAD_TYPE=MERGE but MERGE_KEYS is empty for {full_table}.\n"
            f"    Fix : Add comma-separated key columns in MERGE_KEYS column."
        )
    keys       = [k.strip() for k in merge_keys_str.split(",")]
    match_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in keys])
    tmp_view   = f"_tmp_{full_table.replace('.','_').replace('-','_')}"
    df.createOrReplaceTempView(tmp_view)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table} "
        f"USING DELTA AS SELECT * FROM {tmp_view} WHERE 1=0"
    )

    all_cols = df.columns
    upd_cols = [c for c in all_cols if c not in keys]
    upd_set  = ", ".join([f"tgt.{c} = src.{c}" for c in upd_cols])
    ins_cols = ", ".join(all_cols)
    ins_vals = ", ".join([f"src.{c}" for c in all_cols])

    spark.sql(f"""
        MERGE INTO {full_table} AS tgt
        USING {tmp_view} AS src
        ON {match_cond}
        WHEN MATCHED     THEN UPDATE SET {upd_set}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
    """)

def create_view(sql_query, full_view_name):
    """Create or replace a SQL view from TRANSFORMATION_QUERY."""
    try:
        schema_part = ".".join(full_view_name.split(".")[:2])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_part}")
        spark.sql(f"CREATE OR REPLACE VIEW {full_view_name} AS {sql_query}")
        print(f"   ✅ View created/replaced : {full_view_name}")
        return 0   # views have no row count
    except Exception as e:
        raise RuntimeError(
            f"View creation failed for {full_view_name}.\n"
            f"    Query  : {sql_query[:200]}\n"
            f"    Detail : {e}\n"
            f"    Fix    : Check the SQL in TRANSFORMATION_QUERY is valid."
        )

def write_table(df, schema, table, load_type, merge_keys=None):
    ensure_schema(schema)
    full = f"{CATALOG}.{schema}.{table}"
    lt   = (load_type or "FULL").strip().upper()

    if   lt == "FULL":        write_full(df, full)
    elif lt in ("INCREMENTAL", "APPEND"): write_append(df, full)
    elif lt == "MERGE":       write_merge(df, full, merge_keys)
    else:
        print(f"   ⚠️  Unknown LOAD_TYPE '{lt}' — defaulting to FULL")
        write_full(df, full)

    return spark.table(full).count()

# COMMAND ----------
# ── STEP 8 : Helper — audit log ───────────────────────────────

def write_audit(table_name, layer, status, rows, message, start_time, end_time):
    try:
        safe_msg = str(message).replace("'", "''")[:500]
        spark.sql(f"""
            INSERT INTO {CATALOG}.admin.audit_log
            VALUES (
                '{group_id}',
                '{table_name}',
                '{layer}',
                '{status}',
                {rows},
                '{safe_msg}',
                '{start_time.strftime('%Y-%m-%d %H:%M:%S')}',
                '{end_time.strftime('%Y-%m-%d %H:%M:%S')}',
                current_timestamp()
            )
        """)
    except Exception as e:
        print(f"   ⚠️  Audit write failed (non-fatal) : {e}")

# COMMAND ----------
# ── STEP 9 : Helper — resolve query catalog refs ──────────────

def resolve_query(query, src_schema, src_table):
    """Prepend catalog to bare schema.table references in query."""
    q = query.strip()
    # Replace bare schema. refs with catalog.schema.
    if f"{src_schema}." in q and f"{CATALOG}.{src_schema}." not in q:
        q = q.replace(f"{src_schema}.", f"{CATALOG}.{src_schema}.")
    # Replace bare table name after FROM / JOIN
    for kw in ("FROM ", "JOIN "):
        bare = f"{kw}{src_table}"
        full = f"{kw}{CATALOG}.{src_schema}.{src_table}"
        if bare in q and full not in q:
            q = q.replace(bare, full)
    return q

# COMMAND ----------
# ── STEP 10 : Validate header ─────────────────────────────────

try:
    header_df = spark.sql(f"""
        SELECT * FROM {CATALOG}.admin.data_flow_control_header
        WHERE DATA_FLOW_GROUP_ID = '{group_id}' AND IS_ACTIVE = 'Y'
    """)
except Exception as e:
    raise RuntimeError(
        f"❌  Cannot read control_header table.\n"
        f"    Catalog : {CATALOG}\n"
        f"    Detail  : {e}\n"
        f"    Fix     : Run the deployment pipeline first to create control tables."
    )

if header_df.count() == 0:
    raise Exception(
        f"❌  GROUP_ID '{group_id}' not found in data_flow_control_header.\n"
        f"    Fix A : Check spelling — GROUP_ID is case-sensitive.\n"
        f"    Fix B : Run the deployment pipeline to insert control table entries.\n"
        f"    Fix C : Check IS_ACTIVE = 'Y' for this GROUP_ID.\n"
        f"    Query : SELECT * FROM {CATALOG}.admin.data_flow_control_header "
        f"WHERE DATA_FLOW_GROUP_ID = '{group_id}'"
    )

print(f"✅  Header found for {group_id}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# L0 — BRONZE  (raw ingestion from source files)
# ══════════════════════════════════════════════════════════

def run_l0():
    print(f"\n{'═'*55}")
    print(f"  L0 — BRONZE INGESTION")
    print(f"{'═'*55}")

    try:
        rows = spark.sql(f"""
            SELECT SOURCE_URL, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME,
                   TARGET_SCHEMA, TARGET_TABLE, FILE_FORMAT,
                   INPUT_FILE_FORMAT, LOAD_TYPE, DELIMETER
            FROM {CATALOG}.admin.data_flow_l0_detail
            WHERE DATA_FLOW_GROUP_ID = '{group_id}' AND IS_ACTIVE = 'Y'
        """).collect()
    except Exception as e:
        raise RuntimeError(
            f"❌  Cannot read l0_detail table.\n"
            f"    Detail : {e}\n"
            f"    Fix    : Run the deployment pipeline to create l0_detail table."
        )

    if not rows:
        raise Exception(
            f"❌  No active rows in data_flow_l0_detail for '{group_id}'.\n"
            f"    Fix A : Insert source config rows for this GROUP_ID.\n"
            f"    Fix B : Check IS_ACTIVE = 'Y'.\n"
            f"    Query : SELECT * FROM {CATALOG}.admin.data_flow_l0_detail "
            f"WHERE DATA_FLOW_GROUP_ID = '{group_id}'"
        )

    print(f"  Sources to ingest : {len(rows)}")
    all_ok = True

    for row in rows:
        source_url    = row["SOURCE_URL"]    or ""
        fmt           = row["INPUT_FILE_FORMAT"] or row["FILE_FORMAT"] or "csv"
        target_schema = row["TARGET_SCHEMA"] or "bronze"
        target_table  = row["TARGET_TABLE"]  or ""
        load_type     = row["LOAD_TYPE"]     or "FULL"
        delimiter     = row["DELIMETER"]     or ","
        full_name     = f"{CATALOG}.{target_schema}.{target_table}"

        if not target_table:
            print(f"\n   ⚠️  Skipping row — TARGET_TABLE is empty. Fix: update l0_detail.")
            continue

        print(f"\n  ▶  {full_name}")
        print(f"     Format    : {fmt}  |  Load : {load_type}")
        print(f"     Source    : {source_url[:80]}{'...' if len(source_url)>80 else ''}")

        start   = datetime.now()
        status  = "FAILED"
        rows_ct = 0
        msg     = ""

        try:
            df = read_source(source_url, fmt, delimiter)

            col_count = len(df.columns)
            df = (df
                  .withColumn("_etl_group_id", F.lit(group_id))
                  .withColumn("_etl_layer",    F.lit("L0"))
                  .withColumn("_etl_load_ts",  F.current_timestamp()))

            rows_ct = write_table(df, target_schema, target_table, load_type)
            status  = "SUCCESS"
            msg     = f"{rows_ct:,} rows loaded | {col_count} columns"
            print(f"   ✅ {msg} → {full_name}")

        except Exception as e:
            all_ok = False
            msg    = fmt_error(f"L0/{target_table}", e)
            print(msg)

        finally:
            end = datetime.now()
            print(f"   ⏱  Duration : {round((end-start).total_seconds(),1)}s")
            write_audit(target_table, "L0", status, rows_ct, msg, start, end)

    if not all_ok:
        raise Exception(
            f"❌  L0 FAILED for GROUP_ID='{group_id}'.\n"
            f"    Check audit : SELECT * FROM {CATALOG}.admin.audit_log\n"
            f"                  WHERE DATA_FLOW_GROUP_ID='{group_id}' AND ETL_LAYER='L0'\n"
            f"                  ORDER BY LOAD_TS DESC"
        )

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# L1 — SILVER  (clean, conform, transform)
# Supports LOAD_TYPE: FULL / MERGE / APPEND
# Supports TARGET_TABLE type: TABLE or VIEW
# TRANSFORMATION_QUERY is the SQL SELECT applied to bronze
# ══════════════════════════════════════════════════════════

def run_l1():
    print(f"\n{'═'*55}")
    print(f"  L1 — SILVER TRANSFORMATION")
    print(f"{'═'*55}")

    try:
        rows = spark.sql(f"""
            SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME,
                   TARGET_SCHEMA, TARGET_TABLE, LOAD_TYPE,
                   TRANSFORMATION_QUERY, MERGE_KEYS, PARTITION_BY
            FROM {CATALOG}.admin.data_flow_l1_detail
            WHERE DATA_FLOW_GROUP_ID = '{group_id}' AND IS_ACTIVE = 'Y'
        """).collect()
    except Exception as e:
        raise RuntimeError(
            f"❌  Cannot read l1_detail table.\n"
            f"    Detail : {e}\n"
            f"    Fix    : Run the deployment pipeline to create l1_detail table."
        )

    if not rows:
        print(f"  ℹ️  No L1 config for '{group_id}' — skipping Silver layer.")
        return

    print(f"  Transformations : {len(rows)}")
    all_ok = True

    for row in rows:
        src_schema  = row["SOURCE_OBJ_SCHEMA"] or "bronze"
        src_table   = row["SOURCE_OBJ_NAME"]   or ""
        tgt_schema  = row["TARGET_SCHEMA"]      or "silver"
        tgt_table   = row["TARGET_TABLE"]       or ""
        load_type   = (row["LOAD_TYPE"]         or "MERGE").upper()
        trans_query = row["TRANSFORMATION_QUERY"] or ""
        merge_keys  = row["MERGE_KEYS"]         or ""
        full_src    = f"{CATALOG}.{src_schema}.{src_table}"
        full_tgt    = f"{CATALOG}.{tgt_schema}.{tgt_table}"

        if not tgt_table:
            print(f"\n   ⚠️  Skipping — TARGET_TABLE is empty.")
            continue

        is_view = load_type == "VIEW"

        print(f"\n  ▶  {full_src} → {full_tgt}")
        print(f"     Type : {'VIEW' if is_view else f'TABLE ({load_type})'}")
        if trans_query:
            print(f"     Query: {trans_query[:80]}{'...' if len(trans_query)>80 else ''}")

        start   = datetime.now()
        status  = "FAILED"
        rows_ct = 0
        msg     = ""

        try:
            if is_view:
                if not trans_query:
                    raise ValueError(
                        f"LOAD_TYPE=VIEW but TRANSFORMATION_QUERY is empty for {full_tgt}.\n"
                        f"    Fix : Add a SELECT statement in TRANSFORMATION_QUERY."
                    )
                q = resolve_query(trans_query, src_schema, src_table)
                rows_ct = create_view(q, full_tgt)
                status  = "SUCCESS"
                msg     = f"View created/replaced : {full_tgt}"

            else:
                if trans_query:
                    q  = resolve_query(trans_query, src_schema, src_table)
                    df = spark.sql(q)
                else:
                    df = spark.table(full_src)

                df = (df
                      .withColumn("_etl_group_id", F.lit(group_id))
                      .withColumn("_etl_layer",    F.lit("L1"))
                      .withColumn("_etl_load_ts",  F.current_timestamp()))

                rows_ct = write_table(df, tgt_schema, tgt_table, load_type, merge_keys)
                status  = "SUCCESS"
                msg     = f"{rows_ct:,} rows → {full_tgt}"
                print(f"   ✅ {msg}")

        except Exception as e:
            all_ok = False
            msg    = fmt_error(f"L1/{tgt_table}", e, trans_query)
            print(msg)

        finally:
            end = datetime.now()
            print(f"   ⏱  Duration : {round((end-start).total_seconds(),1)}s")
            write_audit(tgt_table, "L1", status, rows_ct, msg, start, end)

    if not all_ok:
        raise Exception(
            f"❌  L1 FAILED for GROUP_ID='{group_id}'.\n"
            f"    Check audit : SELECT * FROM {CATALOG}.admin.audit_log\n"
            f"                  WHERE DATA_FLOW_GROUP_ID='{group_id}' AND ETL_LAYER='L1'\n"
            f"                  ORDER BY LOAD_TS DESC"
        )

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# L2 — GOLD  (aggregate, summarise, business metrics)
# Supports LOAD_TYPE: FULL / MERGE / APPEND / VIEW
# TRANSFORMATION_QUERY aggregates from silver
# ══════════════════════════════════════════════════════════

def run_l2():
    print(f"\n{'═'*55}")
    print(f"  L2 — GOLD AGGREGATION")
    print(f"{'═'*55}")

    try:
        rows = spark.sql(f"""
            SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME,
                   TARGET_SCHEMA, TARGET_TABLE, LOAD_TYPE,
                   TRANSFORMATION_QUERY, MERGE_KEYS, PARTITION_BY
            FROM {CATALOG}.admin.data_flow_l2_detail
            WHERE DATA_FLOW_GROUP_ID = '{group_id}' AND IS_ACTIVE = 'Y'
        """).collect()
    except Exception as e:
        raise RuntimeError(
            f"❌  Cannot read l2_detail table.\n"
            f"    Detail : {e}\n"
            f"    Fix    : Run the deployment pipeline to create l2_detail table."
        )

    if not rows:
        print(f"  ℹ️  No L2 config for '{group_id}' — skipping Gold layer.")
        return

    print(f"  Aggregations : {len(rows)}")
    all_ok = True

    for row in rows:
        src_schema  = row["SOURCE_OBJ_SCHEMA"] or "silver"
        src_table   = row["SOURCE_OBJ_NAME"]   or ""
        tgt_schema  = row["TARGET_SCHEMA"]      or "gold"
        tgt_table   = row["TARGET_TABLE"]       or ""
        load_type   = (row["LOAD_TYPE"]         or "FULL").upper()
        trans_query = row["TRANSFORMATION_QUERY"] or ""
        merge_keys  = row["MERGE_KEYS"]         or ""
        full_src    = f"{CATALOG}.{src_schema}.{src_table}"
        full_tgt    = f"{CATALOG}.{tgt_schema}.{tgt_table}"

        if not tgt_table:
            print(f"\n   ⚠️  Skipping — TARGET_TABLE is empty.")
            continue

        is_view = load_type == "VIEW"

        print(f"\n  ▶  {full_src} → {full_tgt}")
        print(f"     Type : {'VIEW' if is_view else f'TABLE ({load_type})'}")
        if trans_query:
            print(f"     Query: {trans_query[:80]}{'...' if len(trans_query)>80 else ''}")

        start   = datetime.now()
        status  = "FAILED"
        rows_ct = 0
        msg     = ""

        try:
            if is_view:
                if not trans_query:
                    raise ValueError(
                        f"LOAD_TYPE=VIEW but TRANSFORMATION_QUERY is empty for {full_tgt}.\n"
                        f"    Fix : Add a SELECT statement in TRANSFORMATION_QUERY."
                    )
                q = resolve_query(trans_query, src_schema, src_table)
                rows_ct = create_view(q, full_tgt)
                status  = "SUCCESS"
                msg     = f"View created/replaced : {full_tgt}"

            else:
                if trans_query:
                    q  = resolve_query(trans_query, src_schema, src_table)
                    df = spark.sql(q)
                else:
                    df = spark.table(full_src)

                df = (df
                      .withColumn("_etl_group_id", F.lit(group_id))
                      .withColumn("_etl_layer",    F.lit("L2"))
                      .withColumn("_etl_load_ts",  F.current_timestamp()))

                rows_ct = write_table(df, tgt_schema, tgt_table, load_type, merge_keys)
                status  = "SUCCESS"
                msg     = f"{rows_ct:,} rows → {full_tgt}"
                print(f"   ✅ {msg}")

        except Exception as e:
            all_ok = False
            msg    = fmt_error(f"L2/{tgt_table}", e, trans_query)
            print(msg)

        finally:
            end = datetime.now()
            print(f"   ⏱  Duration : {round((end-start).total_seconds(),1)}s")
            write_audit(tgt_table, "L2", status, rows_ct, msg, start, end)

    if not all_ok:
        raise Exception(
            f"❌  L2 FAILED for GROUP_ID='{group_id}'.\n"
            f"    Check audit : SELECT * FROM {CATALOG}.admin.audit_log\n"
            f"                  WHERE DATA_FLOW_GROUP_ID='{group_id}' AND ETL_LAYER='L2'\n"
            f"                  ORDER BY LOAD_TS DESC"
        )

# COMMAND ----------
# ── STEP 11 : Execute ─────────────────────────────────────────

pipeline_start = datetime.now()

if   run_layer == "L0": run_l0()
elif run_layer == "L1": run_l1()
elif run_layer == "L2": run_l2()
else:
    run_l0()
    run_l1()
    run_l2()

# COMMAND ----------
# ── STEP 12 : Final summary ───────────────────────────────────

total_secs = round((datetime.now() - pipeline_start).total_seconds(), 1)

print(f"\n{'═'*55}")
print(f"  ✅  PIPELINE COMPLETE")
print(f"{'═'*55}")
print(f"  GROUP_ID   : {group_id}")
print(f"  LAYER      : {run_layer}")
print(f"  CATALOG    : {CATALOG}")
print(f"  TOTAL TIME : {total_secs}s")
print(f"{'─'*55}")
print(f"  Audit query:")
print(f"  SELECT * FROM {CATALOG}.admin.audit_log")
print(f"  WHERE DATA_FLOW_GROUP_ID = '{group_id}'")
print(f"  ORDER BY LOAD_TS DESC")
print(f"{'═'*55}")
