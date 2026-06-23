# Databricks notebook source
# =============================================================
# PIPELINE CODE — view_pipeline.py
# Delta Live Tables (DLT) — View layer
# =============================================================
# STRUCTURE:
#   - No dummy tables, no run_log, no anchor tables.
#   - Every LOAD_TYPE='VIEW' row from control table becomes
#     one @dlt.table (DLT materialized view).
#   - DLT requirement of >=1 table is satisfied naturally
#     by your own business tables from the control table.
#   - Each table lands in the TARGET_SCHEMA from control table.
# =============================================================

import dlt
import re

# COMMAND ----------
# ── Step 1: Read DLT pipeline configuration ───────────────────

def get_conf(key, default=""):
    """Read parameter from DLT pipeline Configuration tab."""
    for k in (f"pipelines.{key}", key):
        try:
            v = spark.conf.get(k, "")
            if v and v.strip():
                return v.strip().upper()
        except Exception:
            pass
    return default.upper() if default else ""

GROUP_ID  = get_conf("GROUP_ID")
RUN_LAYER = get_conf("RUN_LAYER", "ALL")

if not GROUP_ID:
    raise Exception(
        "GROUP_ID is not set.\n"
        "Go to: DLT Pipeline → Settings → Configuration\n"
        "Add  : GROUP_ID = FINANCE_MEDALLION_L2"
    )

if RUN_LAYER == "ALL":
    if   GROUP_ID.endswith("_L1"): RUN_LAYER = "L1"
    elif GROUP_ID.endswith("_L2"): RUN_LAYER = "L2"

print(f"GROUP_ID  : {GROUP_ID}")
print(f"RUN_LAYER : {RUN_LAYER}")

# COMMAND ----------
# ── Step 2: Detect catalog ────────────────────────────────────

try:
    cats = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    CATALOG = "demo_catalog" if "demo_catalog" in cats else (
              "hive_metastore" if "hive_metastore" in cats else cats[0])
except Exception:
    CATALOG = "hive_metastore"

print(f"CATALOG   : {CATALOG}")

# COMMAND ----------
# ── Step 3: Helper — clean the SELECT from TRANSFORMATION_QUERY

def clean_select(query, name):
    """
    Accepts two formats:
      A) Pure SELECT ...
      B) CREATE OR REPLACE VIEW x AS SELECT ...
    Returns only the SELECT part.
    """
    q = query.strip()
    q = re.sub(
        r'^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[\w.`]+\s+AS\s*',
        '', q, flags=re.IGNORECASE | re.DOTALL
    ).strip()

    if not q.upper().startswith("SELECT"):
        raise ValueError(
            f"TRANSFORMATION_QUERY for '{name}' must be a SELECT statement.\n"
            f"Found : {q[:150]}\n"
            f"Fix   : Update TRANSFORMATION_QUERY in control table."
        )
    return q


def add_catalog(query, src_schema, src_table):
    """Add catalog prefix to unqualified schema/table references."""
    q = query
    # Fix schema references:  silver.x  →  demo_catalog.silver.x
    if src_schema:
        bare_schema = f"{src_schema}."
        full_schema = f"{CATALOG}.{src_schema}."
        if bare_schema in q and full_schema not in q:
            q = q.replace(bare_schema, full_schema)
    # Fix FROM/JOIN without schema:  FROM titanic_silver  →  FROM demo_catalog.silver.titanic_silver
    if src_table:
        for kw in ("FROM ", "JOIN "):
            if f"{kw}{src_table}" in q and f"{kw}{CATALOG}." not in q:
                q = q.replace(
                    f"{kw}{src_table}",
                    f"{kw}{CATALOG}.{src_schema}.{src_table}"
                )
    return q

# COMMAND ----------
# ── Step 4: Load VIEW rows from control tables ────────────────

def fetch_view_rows(detail_table, layer):
    """Read all LOAD_TYPE='VIEW' rows for this GROUP_ID."""
    try:
        rows = spark.sql(f"""
            SELECT
                SOURCE_OBJ_SCHEMA,
                SOURCE_OBJ_NAME,
                TARGET_SCHEMA,
                TARGET_TABLE,
                TRANSFORMATION_QUERY
            FROM  {CATALOG}.admin.{detail_table}
            WHERE DATA_FLOW_GROUP_ID = '{GROUP_ID}'
            AND   IS_ACTIVE          = 'Y'
            AND   UPPER(LOAD_TYPE)   = 'VIEW'
            ORDER BY TARGET_TABLE
        """).collect()
        print(f"  {layer} ({detail_table}) → {len(rows)} VIEW row(s)")
        return rows
    except Exception as e:
        print(f"  ⚠️  Could not read {detail_table} : {e}")
        return []

l1_rows = fetch_view_rows("data_flow_l1_detail", "L1") if RUN_LAYER in ("ALL","L1") else []
l2_rows = fetch_view_rows("data_flow_l2_detail", "L2") if RUN_LAYER in ("ALL","L2") else []
all_rows = l1_rows + l2_rows

print(f"\n  Total VIEW rows to register : {len(all_rows)}")

if not all_rows:
    raise Exception(
        f"No LOAD_TYPE='VIEW' rows found for GROUP_ID='{GROUP_ID}'.\n"
        f"Fix : Insert rows into data_flow_l1_detail or data_flow_l2_detail\n"
        f"      with LOAD_TYPE='VIEW' and IS_ACTIVE='Y'."
    )

# COMMAND ----------
# ── Step 5: Register one @dlt.table per VIEW row ─────────────
#
# @dlt.table creates a Materialized View in DLT — this is the
# correct DLT primitive for "run a SELECT and store the result".
# Each row from the control table becomes its own DLT table.
# DLT's requirement of >=1 table is met by your own business
# tables — no dummy or log table of any kind.
#
# The table name = TARGET_TABLE from control table.
# The schema    = TARGET_SCHEMA from control table (gold/silver).

def _make_dlt_table(row):
    """
    Factory function — creates a DLT table definition for one row.
    Uses a factory to correctly capture loop variables in closure.
    """
    src_schema  = (row["SOURCE_OBJ_SCHEMA"] or "").strip()
    src_table   = (row["SOURCE_OBJ_NAME"]   or "").strip()
    tgt_schema  = (row["TARGET_SCHEMA"]      or "").strip()
    tgt_table   = (row["TARGET_TABLE"]       or "").strip()
    raw_query   = (row["TRANSFORMATION_QUERY"] or "").strip()

    if not tgt_table:
        print(f"  ⚠️  Skipping row — TARGET_TABLE is empty")
        return

    if not raw_query:
        print(f"  ⚠️  Skipping '{tgt_table}' — TRANSFORMATION_QUERY is empty")
        return

    # Prepare the SELECT SQL
    select_sql = add_catalog(
        clean_select(raw_query, tgt_table),
        src_schema,
        src_table
    )

    # DLT table name — must be unique within the pipeline
    dlt_name = tgt_table

    print(f"  Registering @dlt.table : {dlt_name}")
    print(f"    Source : {CATALOG}.{src_schema}.{src_table}")
    print(f"    Target : {CATALOG}.{tgt_schema}.{dlt_name}")

    # Capture for closure
    _sql      = select_sql
    _name     = dlt_name
    _schema   = tgt_schema
    _src_full = f"{CATALOG}.{src_schema}.{src_table}"
    _tgt_full = f"{CATALOG}.{tgt_schema}.{dlt_name}"

    @dlt.table(
        name             = _name,
        comment          = f"{GROUP_ID} | {_src_full} → {_tgt_full}",
        table_properties = {"quality": _schema}
    )
    def _dlt_fn():
        return spark.sql(_sql)

    return _dlt_fn


# Register every row
for row in all_rows:
    _make_dlt_table(row)

print(f"\n  ✅ {len(all_rows)} DLT table(s) registered for pipeline execution")
