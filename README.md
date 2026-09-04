# DATA_INTEGRATION — Multi-Workspace ETL Framework

A Jenkins-driven, config-table-based ETL framework that runs the same
pipeline code across **multiple individual Databricks workspaces** from
a single GitHub repo.

---

## Architecture

```
GitHub (this repo)
      │
      │  auto-sync on every Jenkins run (SYNC_GIT=true)
      ▼
┌─────────────────────────────────────────────────────────┐
│                    Jenkins Pipeline                      │
│  Parameters: WORKSPACE (ALL / specific), GROUP_ID       │
│                                                          │
│  For each active workspace (in parallel):               │
│    1. Pull latest code from GitHub → Databricks Repos   │
│    2. Trigger Databricks Job via API                    │
│    3. Poll until SUCCESS / FAILURE                      │
└──────────┬──────────────┬──────────────┬────────────────┘
           │              │              │
    Workspace A     Workspace B    Workspace C
    (Karthik)       (User2)        (User3)
           │              │              │
    run_framework   run_framework   run_framework
           │
    Reads control tables
    → Loads source → bronze Delta table
    → Writes run log
```

---

## Repository Structure

```
DATA_INTEGRATION/
├── workspaces.json              ← Registry of all workspaces
├── jenkins/
│   └── Jenkinsfile              ← Multi-workspace pipeline
├── notebooks/
│   ├── run_framework.py         ← Generic ETL engine (PySpark)
│   └── WEATHER_L0.py            ← One-time config insert for weather
└── setup/
    ├── bootstrap_workspace.sh   ← One-time setup for a NEW workspace
    └── create_control_tables.sql ← DDL for admin control tables
```

---

## Control Tables

| Table | Purpose |
|---|---|
| `admin.data_flow_control_header` | One row per pipeline group (GROUP_ID) |
| `admin.data_flow_l0_detail` | Source→target mapping per group |
| `admin.data_flow_run_log` | Auto-written audit log per run |

---

## Onboarding a NEW Individual Workspace (One-Time)

```bash
# 1. Run the bootstrap script
chmod +x setup/bootstrap_workspace.sh
./setup/bootstrap_workspace.sh \
    --workspace-url  https://dbc-XXXX.cloud.databricks.com \
    --token          dapiXXXXXXXXXXXXXXXX \
    --owner-email    newuser@gmail.com \
    --catalog        demo_catalog

# 2. Copy the printed Job ID

# 3. Add a new entry to workspaces.json:
#    {
#      "name": "newuser_workspace",
#      "owner": "New User",
#      "workspace_url": "https://dbc-XXXX.cloud.databricks.com",
#      "job_id": "<PRINTED JOB ID>",
#      "credential_id": "DATABRICKS_TOKEN_NEWUSER",  ← Jenkins credential name
#      "catalog": "demo_catalog",
#      "github_repo_path": "/Workspace/Repos/newuser@gmail.com/DATA_INTEGRATION",
#      "is_active": true
#    }

# 4. Add a Jenkins credential:
#    Manage Jenkins → Credentials → Secret Text
#    ID   = DATABRICKS_TOKEN_NEWUSER
#    Value= dapiXXXXXXXXXXXXXXXX

# 5. Add workspace name to WORKSPACE choices in Jenkinsfile

# 6. In the new Databricks workspace, run WEATHER_L0.py once
#    to insert control table rows
```

---

## Jenkins Setup

### Required Credentials (add once per workspace)
| Credential ID | Type | Value |
|---|---|---|
| `GITHUB_CREDENTIALS` | Username+Password | GitHub PAT |
| `DATABRICKS_TOKEN_KARTHIK` | Secret Text | `dapi...` token |
| `DATABRICKS_TOKEN_USER2` | Secret Text | `dapi...` token |
| `DATABRICKS_TOKEN_USER3` | Secret Text | `dapi...` token |

### Required Jenkins Plugins
- `Pipeline`
- `Git`
- `Credentials Binding`
- `Pipeline Utility Steps` (for `readJSON`)

### Create the Jenkins Job
1. New Item → Pipeline
2. Pipeline → Pipeline script from SCM
3. SCM: Git → URL: `https://github.com/ID-KARTHIKEYAN/DATA_INTEGRATION.git`
4. Script Path: `jenkins/Jenkinsfile`
5. Save

---

## Triggering a Pipeline Run

### Via Jenkins UI
1. Open the Jenkins job
2. **Build with Parameters**
3. Set `GROUP_ID = WEATHER_L0`
4. Set `WORKSPACE = ALL` (or pick a specific one)
5. Build

### Via curl (CI trigger)
```bash
curl -X POST 'http://your-jenkins-url/job/DATA_INTEGRATION/buildWithParameters' \
  -u 'user:api_token' \
  --data-urlencode 'GROUP_ID=WEATHER_L0' \
  --data-urlencode 'WORKSPACE=ALL'
```

---

## Adding a New Pipeline (e.g. SALES_L0)

1. Create a new notebook `notebooks/SALES_L0.py` inserting rows into
   `data_flow_control_header` and `data_flow_l0_detail`
2. Run that notebook once in each workspace
3. Trigger Jenkins with `GROUP_ID=SALES_L0`

`run_framework.py` handles everything else automatically — no code changes needed.

---

## Code Changes (GitHub → Databricks)

When code is pushed to the `main` branch:
- Jenkins auto-syncs via the **Sync Git Repos** stage
  (`SYNC_GIT=true` by default)
- Each workspace's Git Folder is pulled before the job runs
- All workspaces always run the latest code