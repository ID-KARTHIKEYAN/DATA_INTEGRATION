# Databricks notebook source
# =============================================================================
# job_creation_automation.py
# Creates Databricks jobs from data_flow_control_header metadata.
#
# FIXES applied (v2):
#   - Removed SELECT of JOB_NAME / NOTEBOOK_PATH — these columns do NOT exist
#     in the live data_flow_control_header table (they are in the old DDL only).
#   - Job name now derived as: DBX_{GROUP_ID}_JOB
#   - Notebook path resolved from notebook context (git repo path convention).
#   - Token and workspace URL read from notebook context — never passed as params.
# =============================================================================

# COMMAND ----------

import requests
import json
from datetime import datetime

# COMMAND ----------
# ── Widgets ───────────────────────────────────────────────────────────────
dbutils.widgets.text("GROUP_ID", "", "Group ID (blank = ALL active)")

GROUP_ID = dbutils.widgets.get("GROUP_ID").strip().upper()

# ── Read workspace URL and token from context (no secrets as params) ──────
ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = "https://" + ctx.browserHostName().get()
TOKEN         = ctx.apiToken().get()

# ── Derive notebook base path from current notebook's path ───────────────
# Convention: this notebook lives at .../DATA_INTEGRATION/notebooks/job_creation_automation
# kiro_etl_engine lives at the same .../notebooks/kiro_etl_engine
raw_path      = ctx.notebookPath().get()           # e.g. /Workspace/Repos/user@x.com/DATA_INTEGRATION/notebooks/job_creation_automation
REPO_BASE     = raw_path.rsplit("/notebooks/", 1)[0]  # → /Workspace/Repos/user@x.com/DATA_INTEGRATION
NOTEBOOK_PATH = f"{REPO_BASE}/notebooks/kiro_etl_engine"

print(f"Workspace     : {WORKSPACE_URL}")
print(f"Repo base     : {REPO_BASE}")
print(f"Notebook path : {NOTEBOOK_PATH}")
print(f"Group ID      : {GROUP_ID if GROUP_ID else '(ALL active groups)'}")
print(f"Started       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------
# ── HTTP helpers ──────────────────────────────────────────────────────────

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json"
}

def api_get(endpoint, params=None):
    url  = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"GET {endpoint} failed [{resp.status_code}]: {resp.text[:300]}")
    return resp.json()

def api_post(endpoint, payload):
    url  = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    if not resp.ok:
        raise RuntimeError(f"POST {endpoint} failed [{resp.status_code}]: {resp.text[:300]}")
    return resp.json()

# COMMAND ----------
# ── Step 1: Read control table ────────────────────────────────────────────
# FIX: Removed JOB_NAME and NOTEBOOK_PATH — these columns do NOT exist in
#      the live data_flow_control_header table. Job name is derived;
#      notebook path is resolved from context above.

filter_clause = f"AND DATA_FLOW_GROUP_ID = '{GROUP_ID}'" if GROUP_ID else ""

metadata_df = spark.sql(f"""
    SELECT DATA_FLOW_GROUP_ID, ETL_LAYER, IS_ACTIVE, target_catalog
    FROM   demo_catalog.admin.data_flow_control_header
    WHERE  IS_ACTIVE = 'Y'
    {filter_clause}
    ORDER BY DATA_FLOW_GROUP_ID
""")

metadata_list = metadata_df.collect()

if not metadata_list:
    target = f"GROUP_ID = '{GROUP_ID}'" if GROUP_ID else "any active group"
    raise RuntimeError(
        f"[ConfigurationError] No active records found for {target}. "
        f"Check IS_ACTIVE = 'Y' in demo_catalog.admin.data_flow_control_header."
    )

print(f"\nFound {len(metadata_list)} active group(s):")
for row in metadata_list:
    job_name = f"DBX_{row['DATA_FLOW_GROUP_ID']}_JOB"
    print(f"  - {row['DATA_FLOW_GROUP_ID']} (layer={row['ETL_LAYER']}) → job: {job_name}")

# COMMAND ----------
# ── Step 2: Get all existing jobs (paginated) ─────────────────────────────

def get_all_jobs():
    existing = {}
    params   = {"limit": 100, "offset": 0}
    while True:
        data = api_get("/api/2.1/jobs/list", params=params)
        for job in data.get("jobs", []):
            name = job.get("settings", {}).get("name", "")
            existing[name] = job["job_id"]
        if not data.get("has_more", False):
            break
        params["offset"] += len(data.get("jobs", []))
    return existing

print("\nFetching existing jobs...")
existing_jobs = get_all_jobs()
print(f"Found {len(existing_jobs)} existing job(s).")

# COMMAND ----------
# ── Step 3: Create or verify jobs ────────────────────────────────────────

def build_payload(grp_id, notebook_path):
    """
    Builds a Databricks Job API payload for a Serverless job.
    Free Edition: uses environment_key='default_env' (serverless).
    """
    return {
        "name": f"DBX_{grp_id}_JOB",
        "description": f"ETL pipeline for {grp_id}. Auto-created by job_creation_automation.",
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": f"{grp_id}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "GROUP_ID":          grp_id,
                        "TARGET_LOAD_TABLE": "",
                        "RUN_LAYER":         "",
                        "ENVIRONMENT":       "dev",
                        "LOB":               ""
                    },
                    "source": "WORKSPACE"
                },
                "environment_key": "default_env"
            }
        ],
        "environments": [
            {
                "environment_key": "default_env",
                "spec": {"environment_version": "3"}
            }
        ],
        "parameters": [
            {"name": "GROUP_ID",          "default": grp_id},
            {"name": "TARGET_LOAD_TABLE", "default": ""},
            {"name": "RUN_LAYER",         "default": ""},
            {"name": "ENVIRONMENT",       "default": "dev"},
            {"name": "LOB",               "default": ""}
        ]
    }

results  = []
created  = skipped = failed = 0

print(f"\n{'─'*60}")
for row in metadata_list:
    grp_id   = row["DATA_FLOW_GROUP_ID"]
    job_name = f"DBX_{grp_id}_JOB"
    status   = ""
    job_id   = None

    print(f"\n▶  {grp_id} → {job_name}")

    try:
        if job_name in existing_jobs:
            job_id  = existing_jobs[job_name]
            status  = "SKIPPED (already exists)"
            skipped += 1
            print(f"   ↳ Already exists (job_id={job_id})")
        else:
            print(f"   ↳ Creating...")
            result = api_post("/api/2.1/jobs/create",
                              build_payload(grp_id, NOTEBOOK_PATH))
            job_id = result.get("job_id")
            if not job_id:
                raise RuntimeError(f"No job_id returned: {result}")
            status  = "CREATED"
            created += 1
            print(f"   ✅ Created (job_id={job_id})")

    except Exception as e:
        status = f"FAILED: {str(e)[:150]}"
        failed += 1
        print(f"   ❌ {status}")

    results.append({"group_id": grp_id, "job_name": job_name,
                    "job_id": job_id, "status": status})

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────────────────

print(f"\n{'═'*60}")
print(f"  {'GROUP ID':<30} {'JOB ID':<12} STATUS")
print(f"  {'─'*30} {'─'*12} {'─'*20}")
for r in results:
    icon   = "✅" if "FAILED" not in r["status"] else "❌"
    job_id = str(r["job_id"]) if r["job_id"] else "—"
    print(f"  {icon} {r['group_id']:<28} {job_id:<12} {r['status']}")
print(f"{'═'*60}")
print(f"  Created: {created}  |  Skipped: {skipped}  |  Failed: {failed}")

if failed > 0:
    raise RuntimeError(
        f"{failed} job creation(s) failed. "
        f"See output above for details."
    )

print("\n✅ Done — all jobs are ready.")
print(f"\nNotebook path used: {NOTEBOOK_PATH}")
print(f"Run a job manually via: {WORKSPACE_URL}/#job/<job_id>/run")
