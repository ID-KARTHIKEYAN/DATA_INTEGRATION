# Databricks notebook source
# =============================================================
# PIPELINE CODE — view_pipeline.py
# Delta Live Tables (DLT) pipeline notebook.
# =============================================================
# FIXES in this version:
#   [FIX 1] Anchor table goes to demo_catalog.admin schema
#            (was going to gold — caused wrong location)
#   [FIX 2] Anchor uses @dlt.table with target_schema=admin
#            to avoid "already managed by pipeline" conflict
#   [FIX 3] Business views use CREATE OR REPLACE VIEW via
#            spark.sql() INSIDE the @dlt.table function
#            so they land in the correct target schema (gold/silver)
#            DLT @dlt.view puts output in pipeline target schema
#            which is wrong — spark.sql() gives us full control
# =============================================================

import dlt
import re
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------
# ── Parameters (DLT reads from pipeline Configuration tab) ────

def get_conf(key, default=""):
    for prefix in (f"pipelines.{key}", key):
        try:
            val = spark.conf.get(prefix, "")
            if val and val.strip():
                return val.strip().upper()
        except Exception:
            pass
    return default.upper() if default else ""

GROUP_ID  = get_conf("GROUP_ID",  "")
RUN_LAYER = get_conf("RUN_LAYER", "ALL")

if not GROUP_ID:
    raise Exception(
        "GROUP_ID is empty.\n"
        "Fix: DLT pipeline → Settings → Configuration tab\n"
        "     Key=GROUP_ID   Value=FINANCE_MEDALLION_L2"
    )

if RUN_LAYER == "ALL":
    if   GROUP_ID.endswith("_L1"): RUN_LAYER = "L1"
    elif GROUP_ID.endswith("_L2"): RUN_LAYER = "L2"

print(f"GROUP_ID  : {GROUP_ID}")
print(f"RUN_LAYER : {RUN_LAYER}")

# COMMAND ----------
# ── Auto-detect catalog ───────────────────────────────────────

_PREFERRED = "demo_catalog"
try:
    available = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    CATALOG = _PREFERRED if _PREFERRED in available else (
        "hive_metastore" if "hive_metastore" in available else
        (available[0] if available else _PREFERRED)
    )
except Exception:
    CATALOG = "hive_metastore"

print(f"CATALOG   : {CATALOG}")

# COMMAND ----------
# ── Helpers ───────────────────────────────────────────────────

def extract_select(query, view_name):
    """Strip CREATE OR REPLACE VIEW ... AS prefix if present."""
    q = query.strip()
    pattern = re.compile(
        r'^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[\w.`]+\s+AS\s*',
        re.IGNORECASE | re.DOTALL
    )
    q = pattern.sub('', q).strip()
    if not q.upper().startswith("SELECT"):
        raise ValueError(
            f"TRANSFORMATION_QUERY for '{view_name}' must start with SELECT.\n"
            f"Got: {q[:150]}"
        )
    return q


def resolve_refs(query, src_schema, src_table):
    """Prefix catalog to bare schema.table references."""
    q = query
    if src_schema and f"{src_schema}." in q and f"{CATALOG}.{src_schema}." not in q:
        q = q.replace(f"{src_schema}.", f"{CATALOG}.{src_schema}.")
    if src_table:
        for kw in ("FROM ", "JOIN "):
            bare = f"{kw}{src_table}"
            full = f"{kw}{CATALOG}.{src_schema}.{src_table}"
            if bare in q and full not in q:
                q = q.replace(bare, full)
    return q


def load_view_rows(detail_table, layer):
    """Return VIEW rows for this GROUP_ID from a detail table."""
    try:
        rows = spark.sql(f"""
            SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME,
                   TARGET_SCHEMA, TARGET_TABLE,
                   TRANSFORMATION_QUERY
            FROM   {CATALOG}.admin.{detail_table}
            WHERE  DATA_FLOW_GROUP_ID = '{GROUP_ID}'
            AND    IS_ACTIVE           = 'Y'
            AND    UPPER(LOAD_TYPE)    = 'VIEW'
        """).collect()
        print(f"  {layer} VIEW rows found : {len(rows)}")
        return rows
    except Exception as e:
        print(f"  ⚠️  Cannot read {detail_table}: {e}")
        return []


# Load VIEW rows at module level (DLT needs them at definition time)
l1_rows = load_view_rows("data_flow_l1_detail", "L1") if RUN_LAYER in ("ALL","L1") else []
l2_rows = load_view_rows("data_flow_l2_detail", "L2") if RUN_LAYER in ("ALL","L2") else []
all_view_rows = l1_rows + l2_rows

print(f"Total VIEW rows : {len(all_view_rows)}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# ANCHOR TABLE — satisfies DLT's ≥1 table requirement
# [FIX 1] Stored in admin schema, NOT gold/silver
# [FIX 2] pipeline_id in name makes it unique per pipeline
#          → avoids "already managed by pipeline" error
# ══════════════════════════════════════════════════════════

# Get pipeline_id from spark.conf to make anchor name unique
try:
    _pipeline_id = spark.conf.get("pipelines.id", "default").replace("-","")[:8]
except Exception:
    _pipeline_id = "default"

_anchor_name = f"dlt_anchor_{GROUP_ID.lower()}_{_pipeline_id}"

@dlt.table(
    name             = _anchor_name,
    comment          = f"DLT anchor for {GROUP_ID} — required by DLT (min 1 table). Stored in admin.",
    schema           = f"{CATALOG}.admin",
    table_properties = {"quality": "bronze", "pipelines.autoOptimize.managed": "false"}
)
def _dlt_anchor():
    """
    Minimal anchor table stored in admin schema.
    Also creates all business views via spark.sql() inside this function
    so views land in the correct target schema (gold / silver).
    spark.sql() gives full control over schema — @dlt.view does not.
    """
    created_views = []
    failed_views  = []

    for row in all_view_rows:
        src_schema  = row["SOURCE_OBJ_SCHEMA"] or ""
        src_table   = row["SOURCE_OBJ_NAME"]   or ""
        tgt_schema  = row["TARGET_SCHEMA"]      or "gold"
        tgt_table   = row["TARGET_TABLE"]       or ""
        trans_query = row["TRANSFORMATION_QUERY"] or ""

        if not tgt_table or not trans_query:
            print(f"  ⚠️  Skipping — TARGET_TABLE or TRANSFORMATION_QUERY empty")
            continue

        full_view = f"{CATALOG}.{tgt_schema}.{tgt_table}"

        try:
            # Extract pure SELECT
            select_sql = extract_select(trans_query, tgt_table)
            # Resolve catalog references
            select_sql = resolve_refs(select_sql, src_schema, src_table)

            # Ensure target schema exists
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{tgt_schema}")

            # CREATE OR REPLACE VIEW in the correct target schema
            spark.sql(f"CREATE OR REPLACE VIEW {full_view} AS {select_sql}")

            # Validate view is accessible
            spark.sql(f"SELECT 1 FROM {full_view} LIMIT 1")

            created_views.append(full_view)
            print(f"  ✅ View created : {full_view}")
            print(f"     Source      : {CATALOG}.{src_schema}.{src_table}")

        except Exception as e:
            failed_views.append(tgt_table)
            print(f"  ❌ Failed to create view '{full_view}': {e}")

    # Report
    print(f"\n  Views created : {len(created_views)}")
    for v in created_views:
        print(f"    ✅ {v}")
    if failed_views:
        raise Exception(
            f"View creation failed for: {failed_views}\n"
            f"Check TRANSFORMATION_QUERY in control tables."
        )

    # Return anchor table content (1 row per run)
    return spark.createDataFrame(
        [(GROUP_ID, RUN_LAYER, len(created_views), ",".join(created_views), str(datetime.now()))],
        ["GROUP_ID", "RUN_LAYER", "VIEWS_CREATED", "VIEW_NAMES", "RUN_TS"]
    )

