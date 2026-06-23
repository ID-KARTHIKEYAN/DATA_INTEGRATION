# Databricks notebook source
# =============================================================
# PIPELINE CODE — view_pipeline.py
# Delta Live Tables (DLT) pipeline notebook.
# =============================================================
# DESIGN:
#   - No anchor table, no run_log table whatsoever.
#   - DLT requires >=1 table → we use CREATE MATERIALIZED VIEW
#     for the FIRST view row (satisfies DLT requirement).
#   - Remaining view rows → CREATE OR REPLACE VIEW via spark.sql()
#     so they land in the exact TARGET_SCHEMA from control table.
#   - No view_pipeline_run_log anywhere.
# =============================================================

import dlt, re
from pyspark.sql import functions as F

# COMMAND ----------
# ── Parameters ────────────────────────────────────────────────

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
        "     Key = GROUP_ID   Value = FINANCE_MEDALLION_L2"
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
    """Strip CREATE OR REPLACE VIEW ... AS prefix if present, return pure SELECT."""
    q = query.strip()
    pattern = re.compile(
        r'^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[\w.`]+\s+AS\s*',
        re.IGNORECASE | re.DOTALL
    )
    q = pattern.sub('', q).strip()
    if not q.upper().startswith("SELECT"):
        raise ValueError(
            f"TRANSFORMATION_QUERY for '{view_name}' must be a SELECT.\n"
            f"Got: {q[:150]}"
        )
    return q


def resolve_refs(query, src_schema, src_table):
    """Prefix catalog to bare schema.table references in query."""
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
    try:
        rows = spark.sql(f"""
            SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME,
                   TARGET_SCHEMA, TARGET_TABLE,
                   TRANSFORMATION_QUERY
            FROM   {CATALOG}.admin.{detail_table}
            WHERE  DATA_FLOW_GROUP_ID = '{GROUP_ID}'
            AND    IS_ACTIVE           = 'Y'
            AND    UPPER(LOAD_TYPE)    = 'VIEW'
            ORDER BY TARGET_TABLE
        """).collect()
        print(f"  {layer} VIEW rows : {len(rows)}")
        return rows
    except Exception as e:
        print(f"  ⚠️  Cannot read {detail_table}: {e}")
        return []


# Load all VIEW rows at definition time
l1_rows = load_view_rows("data_flow_l1_detail", "L1") if RUN_LAYER in ("ALL","L1") else []
l2_rows = load_view_rows("data_flow_l2_detail", "L2") if RUN_LAYER in ("ALL","L2") else []
all_rows = l1_rows + l2_rows

if not all_rows:
    raise Exception(
        f"No VIEW rows found for GROUP_ID='{GROUP_ID}'.\n"
        f"Check data_flow_l1_detail / data_flow_l2_detail:\n"
        f"  IS_ACTIVE = 'Y' AND LOAD_TYPE = 'VIEW'"
    )

print(f"Total VIEW rows : {len(all_rows)}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════
# DLT requires >=1 @dlt.table or MATERIALIZED VIEW.
#
# Strategy:
#   Row[0]  → registered as @dlt.table (MATERIALIZED VIEW)
#              This satisfies DLT's table requirement.
#              It also physically creates the view content
#              as a materialized table in the target schema.
#   Row[1+] → created via spark.sql(CREATE OR REPLACE VIEW)
#              inside Row[0]'s function, so they all land
#              in the correct TARGET_SCHEMA from control table.
#
# No run_log, no anchor, no dummy table of any kind.
# ══════════════════════════════════════════════════════════

_first       = all_rows[0]
_remaining   = all_rows[1:]

_first_src_schema  = _first["SOURCE_OBJ_SCHEMA"] or ""
_first_src_table   = _first["SOURCE_OBJ_NAME"]   or ""
_first_tgt_schema  = _first["TARGET_SCHEMA"]      or "gold"
_first_tgt_table   = _first["TARGET_TABLE"]
_first_query       = _first["TRANSFORMATION_QUERY"] or ""

_first_select = resolve_refs(
    extract_select(_first_query, _first_tgt_table),
    _first_src_schema,
    _first_tgt_schema
)

print(f"  First row  → @dlt.table (MATERIALIZED VIEW) : {CATALOG}.{_first_tgt_schema}.{_first_tgt_table}")
for r in _remaining:
    print(f"  Other rows → CREATE OR REPLACE VIEW         : {CATALOG}.{r['TARGET_SCHEMA']}.{r['TARGET_TABLE']}")


@dlt.table(
    name    = _first_tgt_table,
    comment = (
        f"Materialized view for {GROUP_ID} | "
        f"Source: {CATALOG}.{_first_src_schema}.{_first_src_table}"
    ),
    table_properties = {"quality": "gold"}
)
def _first_view():
    """
    Registered as @dlt.table to satisfy DLT's table requirement.
    Also creates all remaining views via spark.sql() so they
    land in their correct TARGET_SCHEMA from the control table.
    """

    # ── Create remaining views via spark.sql ──────────────────
    for row in _remaining:
        src_schema  = row["SOURCE_OBJ_SCHEMA"] or ""
        src_table   = row["SOURCE_OBJ_NAME"]   or ""
        tgt_schema  = row["TARGET_SCHEMA"]      or "gold"
        tgt_table   = row["TARGET_TABLE"]       or ""
        trans_query = row["TRANSFORMATION_QUERY"] or ""

        if not tgt_table or not trans_query:
            print(f"  ⚠️  Skipping — empty TARGET_TABLE or TRANSFORMATION_QUERY")
            continue

        full_view = f"{CATALOG}.{tgt_schema}.{tgt_table}"
        try:
            select_sql = resolve_refs(
                extract_select(trans_query, tgt_table),
                src_schema, src_table
            )
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{tgt_schema}")
            spark.sql(f"CREATE OR REPLACE VIEW {full_view} AS {select_sql}")
            spark.sql(f"SELECT 1 FROM {full_view} LIMIT 1")
            print(f"  ✅ View created : {full_view}")
        except Exception as e:
            raise Exception(f"Failed to create view '{full_view}': {e}")

    # ── Return first view's data (becomes the DLT table) ──────
    return spark.sql(_first_select)
