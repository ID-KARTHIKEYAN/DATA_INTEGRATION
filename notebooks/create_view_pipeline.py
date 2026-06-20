# =============================================================
# create_view_pipeline.py
# Standalone Databricks job task — handles ONLY LOAD_TYPE = 'VIEW'
# Reads TRANSFORMATION_QUERY from l1_detail / l2_detail control
# tables and (re)creates the SQL view. Run as a separate task/job
# so view-refresh logic is decoupled from TABLE write logic.
# =============================================================

# ── Widgets ────────────────────────────────────────────────────
dbutils.widgets.text("GROUP_ID", "")
dbutils.widgets.text("RUN_LAYER", "L1")   # L1 or L2 — which control table to read

group_id  = dbutils.widgets.get("GROUP_ID").strip().upper()
run_layer = dbutils.widgets.get("RUN_LAYER").strip().upper()

if not group_id:
    raise Exception("❌ GROUP_ID widget is empty. Pass GROUP_ID when triggering this task.")

# ── Catalog detect (same logic as main framework) ───────────────
_PREFERRED = "demo_catalog"
available = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
CATALOG = _PREFERRED if _PREFERRED in available else (
    "hive_metastore" if "hive_metastore" in available else (available[0] if available else _PREFERRED)
)
print(f"CATALOG: {CATALOG} | GROUP_ID: {group_id} | LAYER: {run_layer}")

detail_table = f"{CATALOG}.admin.data_flow_{run_layer.lower()}_detail"

# ── Pull only VIEW rows ──────────────────────────────────────────
rows = spark.sql(f"""
    SELECT SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, TARGET_SCHEMA, TARGET_TABLE,
           LOAD_TYPE, TRANSFORMATION_QUERY
    FROM {detail_table}
    WHERE DATA_FLOW_GROUP_ID = '{group_id}'
      AND IS_ACTIVE = 'Y'
      AND UPPER(LOAD_TYPE) = 'VIEW'
""").collect()

if not rows:
    print(f"ℹ️  No VIEW-type rows found for GROUP_ID='{group_id}' in {detail_table}. Nothing to do.")
    dbutils.notebook.exit("NO_VIEW_ROWS")

def resolve_query(query, src_schema, src_table):
    q = query.strip()
    if f"{src_schema}." in q and f"{CATALOG}.{src_schema}." not in q:
        q = q.replace(f"{src_schema}.", f"{CATALOG}.{src_schema}.")
    for kw in ("FROM ", "JOIN "):
        bare = f"{kw}{src_table}"
        full = f"{kw}{CATALOG}.{src_schema}.{src_table}"
        if bare in q and full not in q:
            q = q.replace(bare, full)
    return q

failures = []

for row in rows:
    src_schema  = row["SOURCE_OBJ_SCHEMA"] or "bronze"
    src_table   = row["SOURCE_OBJ_NAME"]   or ""
    tgt_schema  = row["TARGET_SCHEMA"]     or "silver"
    tgt_table   = row["TARGET_TABLE"]      or ""
    trans_query = row["TRANSFORMATION_QUERY"] or ""
    full_view   = f"{CATALOG}.{tgt_schema}.{tgt_table}"

    if not trans_query:
        failures.append(f"{full_view}: TRANSFORMATION_QUERY is empty")
        continue

    try:
        q = resolve_query(trans_query, src_schema, src_table)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{tgt_schema}")
        spark.sql(f"CREATE OR REPLACE VIEW {full_view} AS {q}")
        print(f"✅ View created/refreshed: {full_view}")
    except Exception as e:
        failures.append(f"{full_view}: {e}")
        print(f"❌ Failed: {full_view} -> {e}")

if failures:
    raise Exception("❌ One or more views failed:\n" + "\n".join(failures))

dbutils.notebook.exit("VIEW_PIPELINE_SUCCESS")
