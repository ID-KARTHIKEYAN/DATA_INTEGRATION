# Databricks notebook source
# =============================================================
# job_creation_automation.py
#
# Reads the SOURCE-OF-TRUTH metadata tables and creates one
# Databricks Multi-Task Job per DATA_FLOW_GROUP_ID.
#
# PRIORITY in data_flow_pb_detail (L1/L2) drives task DAGs:
#   * All tasks in priority P1 run in parallel
#   * Tasks in priority P2 depend ON ALL P1 task_keys
#   * Tasks in priority P3 depend ON ALL P2 task_keys, etc.
#
# Works WITHOUT any TOKEN parameter — reads both WORKSPACE_URL
# and API TOKEN from the Databricks notebook context.
#
# COLUMNS USED (exhaustive, from existing schema):
#   data_flow_control_header:
#     DATA_FLOW_GROUP_ID, TRIGGER_TYPE, ETL_LAYER,
#     COMPUTE_CLASS_DEV, COMPUTE_CLASS, IS_ACTIVE,
#     SPARK_CONFIGS, MIN_VERSION, MAX_VERSION, target_catalog
#   data_flow_l0_detail:
#     DATA_FLOW_GROUP_ID, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME,
#     LOAD_TYPE, IS_ACTIVE, PRIORITY (if added later – default 999)
#   data_flow_pb_detail:
#     DATA_FLOW_GROUP_ID, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME,
#     TARGET_OBJ_TYPE, PRIORITY, LOAD_TYPE, IS_ACTIVE
# =============================================================

# COMMAND ----------

import requests
import json
from datetime import datetime

# COMMAND ----------
# ── Widgets ───────────────────────────────────────────────────
dbutils.widgets.text("GROUP_ID", "", "Group ID (blank = ALL active)")

GROUP_ID = dbutils.widgets.get("GROUP_ID").strip().upper()

# ── Read workspace URL and token FROM CONTEXT (no param leak) ──
ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = "https://" + ctx.browserHostName().get()
TOKEN         = ctx.apiToken().get()

CATALOG        = "demo_catalog"
CONTROL_SCHEMA = "admin"
NOTEBOOK_PATH_FROM_REPO_ROOT = "notebooks/run_framework"

print(f"Workspace : {WORKSPACE_URL}")
print(f"Group ID  : {GROUP_ID if GROUP_ID else '(ALL active groups)'}")
print(f"Started   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------
# ── HTTP helpers ──────────────────────────────────────────────

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json"
}

def api_get(endpoint, params=None):
    url  = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if not resp.ok:
        raise Exception(f"GET {endpoint} failed [{resp.status_code}]: {resp.text[:500]}")
    return resp.json()

def api_post(endpoint, payload):
    url  = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
    if not resp.ok:
        raise Exception(f"POST {endpoint} failed [{resp.status_code}]: {resp.text[:500]}")
    return resp.json()

def api_delete_job(job_id):
    payload = {"job_id": job_id}
    return api_post("/api/2.1/jobs/delete", payload)

# COMMAND ----------
# ── Locate current notebook path so we know where the runner lives ──

try:
    raw_json = ctx.toJson()
    CURRENT_NB_PATH = json.loads(raw_json).get("extraContext", {}).get("notebook_path", "")
except Exception:
    CURRENT_NB_PATH = ""

REPO_ROOT = CURRENT_NB_PATH.rsplit("/", 2)[0] if CURRENT_NB_PATH else ""
RUNNER_NOTEBOOK_PATH = f"{REPO_ROOT}/{NOTEBOOK_PATH_FROM_REPO_ROOT}" if REPO_ROOT else NOTEBOOK_PATH_FROM_REPO_ROOT

print(f"Current NB : {CURRENT_NB_PATH}")
print(f"Repo root  : {REPO_ROOT}")
print(f"Runner NB  : {RUNNER_NOTEBOOK_PATH}")

# COMMAND ----------
# ── Step 1 : fetch list of active groups to process ──────────

filter_clause = f"AND DATA_FLOW_GROUP_ID = '{GROUP_ID}'" if GROUP_ID else ""

groups_df = spark.sql(f"""
    SELECT
        h.DATA_FLOW_GROUP_ID,
        h.ETL_LAYER,
        h.TRIGGER_TYPE,
        h.IS_ACTIVE,
        h.COMPUTE_CLASS_DEV,
        h.COMPUTE_CLASS,
        h.target_catalog
    FROM {CATALOG}.{CONTROL_SCHEMA}.data_flow_control_header h
    WHERE h.IS_ACTIVE = 'Y'
    {filter_clause}
    ORDER BY h.DATA_FLOW_GROUP_ID
""")

group_rows = groups_df.collect()

if not group_rows:
    target = f"GROUP_ID = '{GROUP_ID}'" if GROUP_ID else "any active group"
    raise Exception(f"No active header records found for {target}. "
                    f"Verify {CATALOG}.{CONTROL_SCHEMA}.data_flow_control_header IS_ACTIVE='Y'.")

print(f"\nActive groups to process: {len(group_rows)}")
for r in group_rows:
    print(f"  - {r.DATA_FLOW_GROUP_ID}  layer={r.ETL_LAYER}  trigger={r.TRIGGER_TYPE}")

# COMMAND ----------
# ── Step 2: Fetch existing jobs to detect if we need recreate ─

def get_all_jobs():
    existing = {}
    params   = {"limit": 100, "offset": 0}
    while True:
        data = api_get("/api/2.1/jobs/list", params=params)
        for job in data.get("jobs", []):
            name = job.get("settings", {}).get("name", "")
            existing[name] = {"job_id": job["job_id"], "tasks": job.get("settings", {}).get("tasks", [])}
        if not data.get("has_more", False):
            break
        params["offset"] += len(data.get("jobs", []))
    return existing

print("\nFetching existing jobs (paginated)...")
existing_jobs = get_all_jobs()
print(f"Found {len(existing_jobs)} existing job(s).")

# COMMAND ----------
# ── Step 3 : Build tasks list for a given group ──────────────

def safe_id(name: str) -> str:
    s = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    if s and s[0].isdigit():
        s = "_" + s
    return s[:80]

def fetch_targets_for_group(data_flow_group_id: str, etl_layer: str) -> list:
    """
    Returns list of dicts:
      {schema, name, priority, obj_type, load_type}
    L0 reads data_flow_l0_detail, L1/L2 read data_flow_pb_detail.
    """
    targets = []
    if etl_layer in ("L0", "ALL"):
        rows_l0 = spark.sql(f"""
            SELECT
                SOURCE_OBJ_SCHEMA AS schema_,
                SOURCE_OBJ_NAME   AS name_,
                CAST(999 AS INT)  AS priority_,
                'TABLE'           AS obj_type_,
                LOAD_TYPE         AS load_type_
            FROM {CATALOG}.{CONTROL_SCHEMA}.data_flow_l0_detail
            WHERE DATA_FLOW_GROUP_ID = '{data_flow_group_id}'
              AND IS_ACTIVE = 'Y'
            ORDER BY SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME
        """).collect()
        for r in rows_l0:
            clean_name = r.name_.rsplit(".", 1)[0] if r.name_ and "." in r.name_ and not r.name_.lower().endswith((".csv",".tsv",".json")) else r.name_
            targets.append({
                "schema": r.schema_,
                "name":   clean_name,
                "priority": r.priority_,
                "obj_type": r.obj_type_,
                "load_type": r.load_type_,
            })

    if etl_layer in ("L1", "L2", "ALL"):
        rows_pb = spark.sql(f"""
            SELECT
                TARGET_OBJ_SCHEMA   AS schema_,
                TARGET_OBJ_NAME     AS name_,
                COALESCE(PRIORITY, 999) AS priority_,
                COALESCE(TARGET_OBJ_TYPE, 'TABLE') AS obj_type_,
                LOAD_TYPE           AS load_type_
            FROM {CATALOG}.{CONTROL_SCHEMA}.data_flow_pb_detail
            WHERE DATA_FLOW_GROUP_ID = '{data_flow_group_id}'
              AND IS_ACTIVE = 'Y'
            ORDER BY COALESCE(PRIORITY, 999), TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME
        """).collect()
        for r in rows_pb:
            targets.append({
                "schema": r.schema_,
                "name":   r.name_,
                "priority": int(r.priority_) if r.priority_ is not None else 999,
                "obj_type": r.obj_type_ or "TABLE",
                "load_type": r.load_type_,
            })
    return targets

def build_tasks(data_flow_group_id: str, etl_layer: str, targets: list,
                notebook_path: str) -> list:
    if not targets:
        # Fallback: one task with TARGET_LOAD_TABLE=ALL
        return [{
            "task_key": safe_id(f"{data_flow_group_id}_ALL"),
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "GROUP_ID":          data_flow_group_id,
                    "TARGET_LOAD_TABLE": "",
                    "ENVIRONMENT":       "dev",
                    "RUN_LAYER":         etl_layer if etl_layer in ("L0","L1","L2") else "AUTO",
                },
                "source": "WORKSPACE",
            },
        }]

    # Group by priority
    buckets = {}
    for t in targets:
        p = t.get("priority") or 999
        buckets.setdefault(p, []).append(t)
    sorted_priorities = sorted(buckets.keys())

    prev_task_keys = []
    tasks = []
    for p in sorted_priorities:
        tgts = buckets[p]
        cur_keys = []
        for t in tgts:
            tkey = safe_id(f"{t['schema']}_{t['name']}")
            if any(tt["task_key"] == tkey for tt in tasks):
                tkey = safe_id(tkey + f"_p{p}")
            task = {
                "task_key": tkey,
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "GROUP_ID":          data_flow_group_id,
                        "TARGET_LOAD_TABLE": t["name"],
                        "ENVIRONMENT":       "dev",
                        "RUN_LAYER":         etl_layer if etl_layer in ("L0","L1","L2") else "AUTO",
                    },
                    "source": "WORKSPACE",
                },
            }
            if prev_task_keys:
                task["depends_on"] = [{"task_key": k} for k in prev_task_keys]
            tasks.append(task)
            cur_keys.append(tkey)
        prev_task_keys = cur_keys
    return tasks

# COMMAND ----------
# ── Step 4 : Create / Recreate jobs per group ────────────────

def build_job_payload(group_id: str, etl_layer: str, tasks: list,
                      compute_class_dev: str, compute_class: str,
                      runner_notebook: str):
    payload = {
        "name": f"DBX_{group_id}_JOB",
        "description": (
            f"Auto-created by job_creation_automation.py | "
            f"GROUP={group_id} | LAYER={etl_layer} | {datetime.now().isoformat(timespec='seconds')}"
        ),
        "max_concurrent_runs": 1,
        "timeout_seconds": 43200,
        "parameters": [
            {"name": "GROUP_ID",         "default": group_id},
            {"name": "TARGET_LOAD_TABLE","default": ""},
            {"name": "ENVIRONMENT",      "default": "dev"},
            {"name": "LOB",              "default": ""},
            {"name": "RUN_LAYER",        "default": etl_layer if etl_layer in ("L0","L1","L2") else "AUTO"},
        ],
        "tasks": tasks,
    }
    return payload

results = []
created = updated = skipped = failed = 0

print(f"\n{'─'*70}")

for g in group_rows:
    gid            = g.DATA_FLOW_GROUP_ID
    etl_layer      = (g.ETL_LAYER or "").strip().upper() or "AUTO"
    compute_dev    = g.COMPUTE_CLASS_DEV or ""
    compute_prod   = g.COMPUTE_CLASS or ""
    status = ""
    job_id = None
    action = ""

    print(f"\n▶  {gid}  (ETL_LAYER={etl_layer})")

    try:
        targets = fetch_targets_for_group(gid, etl_layer)
        print(f"   targets from metadata: {len(targets)}")
        for t in targets[:10]:
            print(f"     - p{t['priority']:<3} {t['schema']}.{t['name']}  [{t['obj_type']}] load={t['load_type']}")
        if len(targets) > 10:
            print(f"     (+ {len(targets) - 10} more)")

        tasks = build_tasks(gid, etl_layer, targets, RUNNER_NOTEBOOK_PATH)
        payload = build_job_payload(gid, etl_layer, tasks, compute_dev, compute_prod, RUNNER_NOTEBOOK_PATH)

        job_name = payload["name"]
        existing = existing_jobs.get(job_name)

        if existing:
            existing_task_count = len(existing["tasks"]) or 1
            if existing_task_count == len(tasks) == 1:
                # Still re-create to refresh parameters & cluster spec
                print(f"   ↳ Recreating (spec refresh) existing job_id={existing['job_id']}")
                try:
                    api_delete_job(existing["job_id"])
                except Exception as del_err:
                    print(f"     ⚠ delete failed (continuing): {del_err}")
                existing = None
                action = "RECREATED"
            else:
                print(f"   ↳ Recreating (tasks mismatch {existing_task_count}→{len(tasks)}) job_id={existing['job_id']}")
                try:
                    api_delete_job(existing["job_id"])
                except Exception as del_err:
                    print(f"     ⚠ delete failed (continuing): {del_err}")
                existing = None
                action = "RECREATED"

        if existing:
            job_id = existing["job_id"]
            status = "SKIPPED (already exists, matching task count)"
            skipped += 1
            print(f"   ✅ {status}")
        else:
            print(f"   ↳ Creating job with {len(tasks)} task(s)...")
            result = api_post("/api/2.1/jobs/create", payload)
            job_id = result.get("job_id")
            if not job_id:
                raise Exception(f"No job_id returned: {result}")
            status = action or "CREATED"
            if action == "RECREATED":
                updated += 1
            else:
                created += 1
            print(f"   ✅ Created: job_id={job_id}")
            for t in tasks:
                deps = ", ".join(d["task_key"] for d in t.get("depends_on", [])) or "none"
                print(f"       task: {t['task_key']:<40} → TARGET_LOAD_TABLE={t['notebook_task']['base_parameters'].get('TARGET_LOAD_TABLE') or '(ALL)'}  deps=[{deps}]")

    except Exception as e:
        status = f"FAILED: {type(e).__name__}: {str(e)[:200]}"
        failed += 1
        print(f"   ❌ {status}")

    results.append({
        "group_id": gid,
        "etl_layer": etl_layer,
        "job_id":   job_id,
        "status":   status,
    })

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────

print(f"\n{'═'*70}")
print(f"  {'GROUP ID':<32} {'LAYER':<6} {'JOB ID':<12} STATUS")
print(f"  {'─'*32} {'─'*6} {'─'*12} {'─'*20}")
for r in results:
    icon   = "✅" if "FAILED" not in r["status"] else "❌"
    jid    = str(r["job_id"]) if r["job_id"] else "—"
    print(f"  {icon} {r['group_id']:<30} {r['etl_layer']:<6} {jid:<12} {r['status']}")
print(f"{'═'*70}")
print(f"  Created: {created}  |  Updated/Recreated: {updated}  |  Skipped: {skipped}  |  Failed: {failed}")
print(f"\n  To run a job manually:")
print(f"    Jobs → DBX_<GROUP_ID>_JOB → Run Now → pass TARGET_LOAD_TABLE (blank=all)")

if failed > 0:
    raise Exception(f"{failed} job(s) failed. See output above.")

print("\n🎉 Done — jobs are aligned with metadata.")
