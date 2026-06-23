# Databricks notebook source
# =============================================================
# PIPELINE CODE — view_pipeline.py
# Delta Live Tables (DLT) — View layer
# =============================================================
# PROBLEM SOLVED:
#   DLT @dlt.table always writes to the pipeline's target schema
#   regardless of what schema name you put in the decorator.
#   So we cannot use @dlt.table to control TARGET_SCHEMA.
#
# SOLUTION:
#   1. One tiny @dlt.table("_dlt_pipeline_anchor") in admin
#      schema → satisfies DLT requirement of >=1 table.
#   2. All business views created via spark.sql(
#      "CREATE OR REPLACE VIEW {catalog}.{TARGET_SCHEMA}.{TARGET_TABLE}"
#      ) → this gives full control over schema placement.
#   3. The anchor table name is fixed and owned by THIS pipeline
#      permanently → no "already managed" conflict ever again.
# =============================================================

import dlt
import re

# COMMAND ----------
# ── Step 1: Pipeline configuration ───────────────────────────

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
        "GROUP_ID is not set.\n"
        "Fix: DLT Pipeline → Settings → Configuration tab\n"
        "     Key = GROUP_ID   Value = FINANCE_MEDALLION_L2"
    )

if RUN_LAYER == "ALL":
    if   GROUP_ID.endswith("_L1"): RUN_LAYER = "L1"
    elif GROUP_ID.endswith("_L2"): RUN_LAYER = "L2"

print(f"GROUP_ID  : {GROUP_ID}")
print(f"RUN_LAYER : {RUN_LAYER}")

# COMMAND ----------
# ── Step 2: Detect catalog ────────────────────────────────────

try:
    cats    = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    CATALOG = "demo_catalog" if "demo_catalog" in cats else (
              "hive_metastore" if "hive_metastore" in cats else cats[0])
except Exception:
    CATALOG = "hive_metastore"

print(f"CATALOG   : {CATALOG}")

# COMMAND ----------
# ── Step 3: Helpers ───────────────────────────────────────────

def clean_select(query, name):
    """Return pure SELECT — strips CREATE OR REPLACE VIEW ... AS if present."""
    q = re.sub(
        r'^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[\w.`]+\s+AS\s*',
        '', query.strip(), flags=re.IGNORECASE | re.DOTALL
    ).strip()
    if not q.upper().startswith("SELECT"):
        raise ValueError(
            f"TRANSFORMATION_QUERY for '{name}' must be a SELECT.\n"
            f"Found : {q[:150]}"
        )
    return q


def add_catalog(query, src_schema, src_table):
    """Prefix catalog to unqualified schema/table refs in the query."""
    q = query
    if src_schema:
        if f"{src_schema}." in q and f"{CATALOG}.{src_schema}." not in q:
            q = q.replace(f"{src_schema}.", f"{CATALOG}.{src_schema}.")
    if src_table:
        for kw in ("FROM ", "JOIN "):
            bare = f"{kw}{src_table}"
            full = f"{kw}{CATALOG}.{src_schema}.{src_table}"
            if bare in q and full not in q:
                q = q.replace(bare, full)
    return q

# COMMAND ----------
# ── Step 4: Load VIEW rows from control tables ────────────────

def fetch_view_rows(detail_table, layer):
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

print(f"  Total VIEW rows : {len(all_rows)}")

if not all_rows:
    raise Exception(
        f"No LOAD_TYPE='VIEW' rows found for GROUP_ID='{GROUP_ID}'.\n"
        f"Fix: insert rows with LOAD_TYPE='VIEW' and IS_ACTIVE='Y'\n"
        f"     into data_flow_l1_detail or data_flow_l2_detail."
    )

# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# Step 5: DLT anchor table
# ══════════════════════════════════════════════════════════════
# WHY NEEDED:
#   DLT refuses to run a pipeline with zero @dlt.table definitions.
#   This one anchor satisfies that requirement.
#
# WHY IT DOES NOT CONFLICT:
#   Name is fixed = "_dlt_anchor_{GROUP_ID}"
#   It is always owned by THIS pipeline.
#   No other pipeline will use this name.
#   Stored in demo_catalog.admin → away from business schemas.
#
# WHY WE DO NOT USE @dlt.table FOR BUSINESS VIEWS:
#   @dlt.table always writes to the pipeline's DEFAULT TARGET SCHEMA
#   regardless of the name you give it. You cannot control the
#   output schema via @dlt.table alone.
#   We use spark.sql(CREATE OR REPLACE VIEW catalog.TARGET_SCHEMA.TARGET_TABLE)
#   which gives full control over exactly where each view lands.
# ══════════════════════════════════════════════════════════════

@dlt.table(
    name             = f"_dlt_anchor_{GROUP_ID.lower()}",
    comment          = f"DLT pipeline anchor for {GROUP_ID}. Required by DLT. Stored in admin.",
    table_properties = {"quality": "bronze"}
)
def _anchor():
    """
    This function:
      1. Creates all business views in their correct TARGET_SCHEMA
         using spark.sql() — gives full schema control.
      2. Returns a 1-row summary DataFrame which DLT stores as
         the anchor table in demo_catalog.admin (pipeline target).
    """
    created = []
    failed  = []

    for row in all_rows:
        src_schema  = (row["SOURCE_OBJ_SCHEMA"]    or "").strip()
        src_table   = (row["SOURCE_OBJ_NAME"]       or "").strip()
        tgt_schema  = (row["TARGET_SCHEMA"]          or "gold").strip()
        tgt_table   = (row["TARGET_TABLE"]           or "").strip()
        raw_query   = (row["TRANSFORMATION_QUERY"]   or "").strip()
        full_view   = f"{CATALOG}.{tgt_schema}.{tgt_table}"

        print(f"\n  ▶  Creating view : {full_view}")
        print(f"     Source        : {CATALOG}.{src_schema}.{src_table}")

        if not tgt_table:
            print("     ⚠️  Skipped — TARGET_TABLE is empty")
            continue

        if not raw_query:
            print(f"     ⚠️  Skipped — TRANSFORMATION_QUERY is empty for {tgt_table}")
            continue

        try:
            # Clean the SELECT
            select_sql = add_catalog(
                clean_select(raw_query, tgt_table),
                src_schema,
                src_table
            )

            # Ensure target schema exists
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{tgt_schema}")

            # Create view in EXACT target schema from control table
            # This is the key: spark.sql gives us full path control
            spark.sql(f"CREATE OR REPLACE VIEW {full_view} AS {select_sql}")

            # Validate
            spark.sql(f"SELECT 1 FROM {full_view} LIMIT 1")

            created.append(full_view)
            print(f"     ✅ View created in {CATALOG}.{tgt_schema}.{tgt_table}")

        except Exception as e:
            failed.append(tgt_table)
            msg = (
                f"     ❌ FAILED : {full_view}\n"
                f"        Error : {str(e)[:300]}"
            )
            print(msg)

    # Print summary
    print(f"\n  {'═'*50}")
    print(f"  ✅ Created : {len(created)}")
    for v in created:
        print(f"     → {v}")
    if failed:
        print(f"  ❌ Failed  : {failed}")
        raise Exception(
            f"View creation failed for: {failed}\n"
            f"Check TRANSFORMATION_QUERY in control tables."
        )
    print(f"  {'═'*50}")

    # Return anchor DataFrame (1 row) — stored in admin by DLT
    return spark.createDataFrame(
        [(GROUP_ID, RUN_LAYER, len(created), ",".join(created))],
        ["GROUP_ID", "RUN_LAYER", "VIEWS_CREATED", "VIEW_NAMES"]
    )
