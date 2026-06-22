# Databricks notebook source
# =============================================================
# PIPELINE CODE — view_pipeline.py
# Delta Live Tables (DLT) pipeline notebook.
# =============================================================
# DESIGN:
#   - This notebook runs as a DLT PIPELINE (not a Job).
#   - Named: DBX_{GROUP_ID}_DLT
#   - DLT requires at least one @dlt.table — we add one
#     minimal anchor table: view_pipeline_run_log
#     (records each pipeline execution — useful audit trail).
#   - For every LOAD_TYPE='VIEW' row in l1_detail / l2_detail
#     we dynamically register a @dlt.view using the
#     TRANSFORMATION_QUERY stored in the control table.
#   - run_framework.py NEVER creates views.
#     This is the ONLY place views are created.
# =============================================================

import dlt
import re
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------
# ── Parameters ────────────────────────────────────────────────
# DLT pipelines pass config via spark.conf (pipeline Configuration tab)
# Set these in the DLT pipeline configuration:
#   GROUP_ID  = FINANCE_MEDALLION_L2
#   RUN_LAYER = L2

def get_conf(key, default=""):
    """Read from DLT pipeline configuration (spark.conf)."""
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
        "Fix: open the DLT pipeline → Settings → Configuration tab\n"
        "     and add key=GROUP_ID  value=FINANCE_MEDALLION_L2"
    )

# Auto-detect layer from GROUP_ID suffix
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
# ── Helper: resolve catalog refs in query ─────────────────────

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
            f"Got: {q[:100]}"
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

# COMMAND ----------
# ── Read VIEW rows from control tables ────────────────────────

def load_view_rows(detail_table, layer):
    """Return list of VIEW rows for this GROUP_ID from a detail table."""
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

# Collect all VIEW rows upfront (DLT decorators run at definition time)
l1_rows = load_view_rows("data_flow_l1_detail", "L1") if RUN_LAYER in ("ALL","L1") else []
l2_rows = load_view_rows("data_flow_l2_detail", "L2") if RUN_LAYER in ("ALL","L2") else []
all_view_rows = l1_rows + l2_rows

print(f"Total VIEW rows to register : {len(all_view_rows)}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# ANCHOR TABLE — required by DLT (must have ≥1 @dlt.table)
# This is a lightweight run-log table that records each
# DLT pipeline execution. Useful audit trail, zero overhead.
# ══════════════════════════════════════════════════════════

@dlt.table(
    name    = "view_pipeline_run_log",
    comment = f"DLT anchor table — records each view_pipeline execution for {GROUP_ID}",
    table_properties = {"quality": "bronze"}
)
def view_pipeline_run_log():
    """
    Anchor table required by DLT (pipelines need ≥1 @dlt.table).
    Records GROUP_ID, layer, view count, and run timestamp.
    """
    return spark.createDataFrame(
        [(
            GROUP_ID,
            RUN_LAYER,
            len(all_view_rows),
            str(datetime.now())
        )],
        ["GROUP_ID", "RUN_LAYER", "VIEW_COUNT", "RUN_TS"]
    )

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# DYNAMIC VIEW REGISTRATION
# For each LOAD_TYPE='VIEW' row, register a @dlt.view
# DLT will create/refresh these as part of the pipeline.
# ══════════════════════════════════════════════════════════

def register_dlt_view(row, layer):
    """Register a single @dlt.view for one control table row."""

    src_schema  = row["SOURCE_OBJ_SCHEMA"] or ""
    src_table   = row["SOURCE_OBJ_NAME"]   or ""
    tgt_schema  = row["TARGET_SCHEMA"]      or ("silver" if layer=="L1" else "gold")
    tgt_table   = row["TARGET_TABLE"]       or ""
    trans_query = row["TRANSFORMATION_QUERY"] or ""

    if not tgt_table or not trans_query:
        print(f"  ⚠️  Skipping {layer} row — TARGET_TABLE or TRANSFORMATION_QUERY empty")
        return

    # DLT view name must be unique within the pipeline
    view_name = f"{tgt_table}"

    try:
        select_sql = extract_select(trans_query, tgt_table)
        select_sql = resolve_refs(select_sql, src_schema, src_table)

        # Capture variables in closure for DLT decorator
        _sql = select_sql
        _name = view_name
        _layer = layer
        _src = f"{CATALOG}.{src_schema}.{src_table}"
        _tgt = f"{CATALOG}.{tgt_schema}.{tgt_table}"

        @dlt.view(
            name    = _name,
            comment = f"{_layer} VIEW | {GROUP_ID} | {_src} → {_tgt}"
        )
        def _view_fn():
            return spark.sql(_sql)

        print(f"  ✅ Registered DLT view : {_name}  ({_layer})")
        print(f"     Source : {_src}")
        print(f"     Target : {_tgt}")

    except Exception as e:
        print(f"  ❌ Failed to register view '{tgt_table}': {e}")
        raise


# Register all L1 VIEW rows
for row in l1_rows:
    register_dlt_view(row, "L1")

# Register all L2 VIEW rows
for row in l2_rows:
    register_dlt_view(row, "L2")

print(f"\n{'═'*55}")
print(f"  DLT pipeline definition complete")
print(f"  Anchor table : view_pipeline_run_log")
print(f"  DLT views    : {len(all_view_rows)}")
print(f"  GROUP_ID     : {GROUP_ID}")
print(f"  CATALOG      : {CATALOG}")
print(f"{'═'*55}")
