# Databricks notebook source
# =============================================================
# job_creation_automation.py
# FIX: Removed TOKEN + WORKSPACE_URL widgets.
#      Now reads both from the notebook context automatically
#      so no secret is ever passed as a parameter.
# =============================================================

# COMMAND ----------

import requests
import json
from datetime import datetime

# COMMAND ----------
# ── Widgets — only GROUP_ID needed now ───────────────────────
dbutils.widgets.text("GROUP_ID", "", "Group ID (blank = ALL active)")

GROUP_ID = dbutils.widgets.get("GROUP_ID").strip().upper()

# ── Read workspace URL and token FROM CONTEXT (no param needed)
ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = "https://" + ctx.browserHostName().get()
TOKEN         = ctx.apiToken().get()

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
        raise Exception(f"GET {endpoint} failed [{resp.status_code}]: {resp.text}")
    return resp.json()

def api_post(endpoint, payload):
    url  = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    if not resp.ok:
        raise Exception(f"POST {endpoint} failed [{resp.status_code}]: {resp.text}")
    return resp.json()

# COMMAND ----------
# ── Step 1: Read control table ────────────────────────────────

filter_clause = f"AND DATA_FLOW_GROUP_ID = '{GROUP_ID}'" if GROUP_ID else ""

metadata_df = spark.sql(f"""
    SELECT DATA_FLOW_GROUP_ID, JOB_NAME, NOTEBOOK_PATH, IS_ACTIVE
    FROM   demo_catalog.admin.data_flow_control_header
    WHERE  IS_ACTIVE = 'Y'
    {filter_clause}
    ORDER BY DATA_FLOW_GROUP_ID
""")

metadata_list = metadata_df.collect()

if not metadata_list:
    target = f"GROUP_ID = '{GROUP_ID}'" if GROUP_ID else "any active group"
    raise Exception(f"No active records found for {target}. Check IS_ACTIVE = 'Y'.")

print(f"\nFound {len(metadata_list)} group(s):")
for row in metadata_list:
    print(f"  - {row['DATA_FLOW_GROUP_ID']} → {row['NOTEBOOK_PATH']}")

# COMMAND ----------
# ── Step 2: Get all existing jobs (paginated) ─────────────────

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
# ── Step 3: Create missing jobs ───────────────────────────────

def build_payload(grp_id, notebook_path):
    return {
        "name": grp_id,
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "GROUP_ID", "default": grp_id}
        ],
        "tasks": [
            {
                "task_key": f"{grp_id}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {"GROUP_ID": grp_id},
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
        ]
    }

results = []
created = skipped = failed = 0

print(f"\n{'─'*55}")

for row in metadata_list:
    grp_id        = row["DATA_FLOW_GROUP_ID"]
    notebook_path = row["NOTEBOOK_PATH"]
    status        = ""
    job_id        = None

    print(f"\n▶  {grp_id}")

    try:
        if grp_id in existing_jobs:
            job_id  = existing_jobs[grp_id]
            status  = "SKIPPED (already exists)"
            skipped += 1
            print(f"   ↳ Already exists (job_id={job_id})")
        else:
            print(f"   ↳ Creating...")
            result = api_post("/api/2.1/jobs/create", build_payload(grp_id, notebook_path))
            job_id = result.get("job_id")
            if not job_id:
                raise Exception(f"No job_id returned: {result}")
            status  = "CREATED"
            created += 1
            print(f"   ✅ Created (job_id={job_id})")

    except Exception as e:
        status = f"FAILED: {str(e)[:120]}"
        failed += 1
        print(f"   ❌ {status}")

    results.append({"group_id": grp_id, "job_id": job_id, "status": status})

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────

print(f"\n{'═'*55}")
print(f"  {'GROUP ID':<28} {'JOB ID':<15} STATUS")
print(f"  {'─'*28} {'─'*15} {'─'*20}")
for r in results:
    icon   = "✅" if "FAILED" not in r["status"] else "❌"
    job_id = str(r["job_id"]) if r["job_id"] else "—"
    print(f"  {icon} {r['group_id']:<26} {job_id:<15} {r['status']}")
print(f"{'═'*55}")
print(f"  Created: {created}  |  Skipped: {skipped}  |  Failed: {failed}")

if failed > 0:
    raise Exception(f"{failed} job(s) failed. See output above.")

print("\n🎉 Done — all jobs are ready.")
