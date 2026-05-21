# Databricks notebook source
# =============================================================
# job_creation_automation.py
# Automatically creates Databricks jobs for every active
# pipeline group found in data_flow_control_header.
#
# Run this notebook once per workspace whenever a new
# GROUP_ID is added to the control table.
#
# Parameters:
#   WORKSPACE_URL  — e.g. https://dbc-xxxx.cloud.databricks.com
#   TOKEN          — Databricks personal access token (dapi...)
#   GROUP_ID       — specific group to create, or leave blank for ALL
# =============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## Job Creation Automation
# MAGIC Reads active pipeline groups from the control table and
# MAGIC creates a Databricks job for each one that doesn't exist yet.

# COMMAND ----------

import requests
import json
import time
from datetime import datetime

# COMMAND ----------
# ── Widgets ───────────────────────────────────────────────────
dbutils.widgets.text("WORKSPACE_URL", "", "Workspace URL")
dbutils.widgets.text("TOKEN",         "", "Databricks Token")
dbutils.widgets.text("GROUP_ID",      "", "Group ID (blank = ALL active)")

WORKSPACE_URL = dbutils.widgets.get("WORKSPACE_URL").rstrip("/")
TOKEN         = dbutils.widgets.get("TOKEN").strip()
GROUP_ID      = dbutils.widgets.get("GROUP_ID").strip().upper()

# ── Validate required inputs ──────────────────────────────────
if not WORKSPACE_URL:
    raise ValueError("WORKSPACE_URL widget is empty. Please provide the workspace URL.")
if not TOKEN:
    raise ValueError("TOKEN widget is empty. Please provide a Databricks access token.")

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
    """GET request to Databricks REST API with error handling."""
    url = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if not resp.ok:
        raise Exception(f"GET {endpoint} failed [{resp.status_code}]: {resp.text}")
    return resp.json()

def api_post(endpoint, payload):
    """POST request to Databricks REST API with error handling."""
    url = f"{WORKSPACE_URL}{endpoint}"
    resp = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    if not resp.ok:
        raise Exception(f"POST {endpoint} failed [{resp.status_code}]: {resp.text}")
    return resp.json()

# COMMAND ----------
# ── Step 1: Read control table ────────────────────────────────

filter_clause = f"AND DATA_FLOW_GROUP_ID = '{GROUP_ID}'" if GROUP_ID else ""

metadata_df = spark.sql(f"""
    SELECT
        DATA_FLOW_GROUP_ID,
        JOB_NAME,
        NOTEBOOK_PATH,
        IS_ACTIVE
    FROM demo_catalog.admin.data_flow_control_header
    WHERE IS_ACTIVE = 'Y'
    {filter_clause}
    ORDER BY DATA_FLOW_GROUP_ID
""")

metadata_list = metadata_df.collect()

if not metadata_list:
    target = f"GROUP_ID = '{GROUP_ID}'" if GROUP_ID else "any active group"
    raise Exception(
        f"No active records found in data_flow_control_header for {target}. "
        f"Check that IS_ACTIVE = 'Y' and the GROUP_ID is correct."
    )

print(f"\nFound {len(metadata_list)} group(s) to process:")
for row in metadata_list:
    print(f"  - {row['DATA_FLOW_GROUP_ID']} → {row['NOTEBOOK_PATH']}")

# COMMAND ----------
# ── Step 2: Fetch all existing jobs (handles pagination) ──────

def get_all_jobs():
    """Return a dict of {job_name: job_id} for all jobs in the workspace."""
    existing = {}
    params = {"limit": 100, "offset": 0}

    while True:
        data     = api_get("/api/2.1/jobs/list", params=params)
        jobs     = data.get("jobs", [])
        for job in jobs:
            name = job.get("settings", {}).get("name", "")
            existing[name] = job["job_id"]
        # Stop if no more pages
        if not data.get("has_more", False):
            break
        params["offset"] += len(jobs)

    return existing

print("\nFetching existing jobs from workspace...")
existing_jobs = get_all_jobs()
print(f"Found {len(existing_jobs)} existing job(s) in workspace.")

# COMMAND ----------
# ── Step 3: Create jobs for groups that don't have one yet ────

def build_job_payload(group_id, notebook_path):
    """Build the job creation payload for a given group."""
    return {
        "name": group_id,
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "GROUP_ID", "default": group_id}
        ],
        "tasks": [
            {
                "task_key": f"{group_id}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "GROUP_ID": group_id
                    },
                    "source": "WORKSPACE"
                },
                "environment_key": "default_env"
            }
        ],
        "environments": [
            {
                "environment_key": "default_env",
                "spec": {
                    "environment_version": "3"
                }
            }
        ]
    }

# COMMAND ----------
# ── Step 4: Process each group ────────────────────────────────

results  = []
created  = 0
skipped  = 0
failed   = 0

print(f"\n{'─'*55}")
print(f"  Processing {len(metadata_list)} group(s)")
print(f"{'─'*55}")

for row in metadata_list:
    grp_id        = row["DATA_FLOW_GROUP_ID"]
    notebook_path = row["NOTEBOOK_PATH"]
    status        = ""
    job_id        = None

    print(f"\n▶  {grp_id}")
    print(f"   Notebook: {notebook_path}")

    try:
        if grp_id in existing_jobs:
            # Job already exists — skip
            job_id = existing_jobs[grp_id]
            status = "SKIPPED (already exists)"
            skipped += 1
            print(f"   ↳ Job already exists (job_id={job_id}) — skipping")

        else:
            # Create new job
            print(f"   ↳ Job not found — creating...")
            payload  = build_job_payload(grp_id, notebook_path)
            result   = api_post("/api/2.1/jobs/create", payload)
            job_id   = result.get("job_id")

            if not job_id:
                raise Exception(f"API returned no job_id. Response: {result}")

            status  = "CREATED"
            created += 1
            print(f"   ✅ Job created successfully (job_id={job_id})")

    except Exception as e:
        status = f"FAILED: {str(e)[:100]}"
        failed += 1
        print(f"   ❌ {status}")

    results.append({
        "group_id":      grp_id,
        "notebook_path": notebook_path,
        "job_id":        job_id,
        "status":        status
    })

# COMMAND ----------
# ── Step 5: Print summary ─────────────────────────────────────

print(f"\n{'═'*55}")
print(f"  JOB CREATION SUMMARY")
print(f"{'═'*55}")
print(f"  {'GROUP ID':<30} {'JOB ID':<15} STATUS")
print(f"  {'─'*30} {'─'*15} {'─'*20}")

for r in results:
    icon   = "✅" if r["status"] in ("CREATED", "SKIPPED (already exists)") else "❌"
    job_id = str(r["job_id"]) if r["job_id"] else "—"
    print(f"  {icon} {r['group_id']:<28} {job_id:<15} {r['status']}")

print(f"{'═'*55}")
print(f"  Created : {created}")
print(f"  Skipped : {skipped}  (jobs already existed)")
print(f"  Failed  : {failed}")
print(f"{'═'*55}")

if failed > 0:
    raise Exception(
        f"{failed} job(s) failed to create. "
        f"Check the output above for details."
    )

print(f"\n🎉 Done! All jobs are ready.")
print(f"   Next: trigger via Jenkins with the GROUP_ID parameter.")
