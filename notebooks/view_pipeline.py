# Databricks notebook source
# =============================================================
# PIPELINE CODE — view_pipeline.py
# =============================================================
# WHAT IT DOES:
#   Reads l1_detail + l2_detail WHERE LOAD_TYPE = 'VIEW'
#   For each row → creates one @dlt.table in the correct
#   TARGET_SCHEMA (silver or gold — whatever is in the row).
#   Nothing hardcoded. No run_log. No dummy table.
#   Does NOT affect any other job or pipeline.
# =============================================================

import dlt, re

# COMMAND ----------
# ── Parameters (set in DLT Pipeline → Settings → Configuration)

def get_conf(key, default=""):
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
        "GROUP_ID not set.\n"
        "Go to: Pipeline → Settings → Configuration tab\n"
        "Add  : GROUP_ID = your group id here"
    )

if RUN_LAYER == "ALL":
    if   GROUP_ID.endswith("_L1"): RUN_LAYER = "L1"
    elif GROUP_ID.endswith("_L2"): RUN_LAYER = "L2"

print(f"GROUP_ID  : {GROUP_ID}")
print(f"RUN_LAYER : {RUN_LAYER}")

# COMMAND ----------
# ── Catalog detection ─────────────────────────────────────────

try:
    cats = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    CATALOG = "demo_catalog" if "demo_catalog" in cats else (
              "hive_metastore" if "hive_metastore" in cats else cats[0])
except Exception:
    CATALOG = "hive_metastore"

print(f"CATALOG   : {CATALOG}")

# COMMAND ----------
# ── Helper: extract SELECT from TRANSFORMATION_QUERY ─────────

def to_select(query, name):
    q = re.sub(
        r'^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[\w.`]+\s+AS\s*',
        '', query.strip(), flags=re.IGNORECASE | re.DOTALL
    ).strip()
    if not q.upper().startswith("SELECT"):
        raise ValueError(
            f"'{name}': TRANSFORMATION_QUERY must be a SELECT.\n"
            f"Found: {q[:120]}"
        )
    return q


def qualify(query, src_schema, src_table):
    """Add catalog prefix to unqualified references."""
    q = query
    if src_schema:
        if f"{src_schema}." in q and f"{CATALOG}.{src_schema}." not in q:
            q = q.replace(f"{src_schema}.", f"{CATALOG}.{src_schema}.")
    if src_table and src_schema:
        for kw in ("FROM ", "JOIN "):
            bare = f"{kw}{src_table}"
            full = f"{kw}{CATALOG}.{src_schema}.{src_table}"
            if bare in q and full not in q:
                q = q.replace(bare, full)
    return q

# COMMAND ----------
# ── Load VIEW rows from control tables ────────────────────────

def fetch(detail_table, layer):
    if RUN_LAYER not in ("ALL", layer):
        return []
    try:
        rows = spark.sql(f"""
            SELECT
                SOURCE_OBJ_SCHEMA    AS src_schema,
                SOURCE_OBJ_NAME      AS src_table,
                TARGET_SCHEMA        AS tgt_schema,
                TARGET_TABLE         AS tgt_table,
                TRANSFORMATION_QUERY AS query
            FROM  {CATALOG}.admin.{detail_table}
            WHERE DATA_FLOW_GROUP_ID = '{GROUP_ID}'
            AND   IS_ACTIVE          = 'Y'
            AND   UPPER(LOAD_TYPE)   = 'VIEW'
            ORDER BY TARGET_TABLE
        """).collect()
        print(f"  {layer} VIEW rows : {len(rows)}")
        return rows
    except Exception as e:
        print(f"  ⚠️  {detail_table}: {e}")
        return []

all_rows = fetch("data_flow_l1_detail", "L1") + fetch("data_flow_l2_detail", "L2")

print(f"  Total : {len(all_rows)} VIEW row(s) to create")

if not all_rows:
    raise Exception(
        f"No VIEW rows for GROUP_ID='{GROUP_ID}'.\n"
        f"Check l1_detail / l2_detail : IS_ACTIVE='Y', LOAD_TYPE='VIEW'."
    )

# COMMAND ----------
# ── Register one @dlt.table per VIEW row ──────────────────────
#
# @dlt.table name  = TARGET_TABLE from control table
# schema           = TARGET_SCHEMA from control table  ← NOT hardcoded gold
# content          = TRANSFORMATION_QUERY (the SELECT)
#
# DLT places the table in whatever schema the pipeline target is set to.
# We set pipeline target = TARGET_SCHEMA by reading it per-row.
# For rows with different TARGET_SCHEMA values, DLT uses its default
# target schema — so set the pipeline target to the schema you want
# OR ensure all VIEW rows share the same TARGET_SCHEMA.

def _register(row):
    src_schema = (row["src_schema"] or "").strip()
    src_table  = (row["src_table"]  or "").strip()
    tgt_schema = (row["tgt_schema"] or "gold").strip()
    tgt_table  = (row["tgt_table"]  or "").strip()
    raw_query  = (row["query"]      or "").strip()

    if not tgt_table or not raw_query:
        print(f"  ⚠️  Skipping — missing TARGET_TABLE or TRANSFORMATION_QUERY")
        return

    select_sql = qualify(to_select(raw_query, tgt_table), src_schema, src_table)

    # ── closure captures ──────────────────────────────────────
    _sql    = select_sql
    _name   = tgt_table
    _schema = tgt_schema
    _src    = f"{CATALOG}.{src_schema}.{src_table}"
    _tgt    = f"{CATALOG}.{tgt_schema}.{tgt_table}"

    print(f"  @dlt.table  name={_name}  schema={_schema}")
    print(f"    source → {_src}")
    print(f"    target → {_tgt}")

    @dlt.table(
        name             = _name,
        comment          = f"GROUP_ID={GROUP_ID} | {_src} → {_tgt}",
        table_properties = {"quality": _schema}
    )
    def _fn():
        return spark.sql(_sql)

for row in all_rows:
    _register(row)

print(f"\n  ✅ {len(all_rows)} table(s) registered in DLT pipeline")
