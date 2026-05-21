#!/bin/bash
# =============================================================
# bootstrap_workspace.sh
# One-time setup for a NEW Databricks workspace.
# Run this ONCE per person when onboarding a new workspace.
#
# Usage:
#   chmod +x bootstrap_workspace.sh
#   ./bootstrap_workspace.sh \
#       --workspace-url  https://dbc-XXXX.cloud.databricks.com \
#       --token          dapiXXXXXXXXXXXXXXXX \
#       --owner-email    user@gmail.com \
#       --catalog        demo_catalog \
#       --github-repo    https://github.com/ID-KARTHIKEYAN/DATA_INTEGRATION.git \
#       --branch         main
# =============================================================

set -euo pipefail

# ── Parse arguments ───────────────────────────────────────────
WORKSPACE_URL=""
TOKEN=""
OWNER_EMAIL=""
CATALOG="demo_catalog"
GITHUB_REPO="https://github.com/ID-KARTHIKEYAN/DATA_INTEGRATION.git"
BRANCH="main"

while [[ $# -gt 0 ]]; do
    case $1 in
        --workspace-url)  WORKSPACE_URL="$2"; shift 2 ;;
        --token)          TOKEN="$2";         shift 2 ;;
        --owner-email)    OWNER_EMAIL="$2";   shift 2 ;;
        --catalog)        CATALOG="$2";       shift 2 ;;
        --github-repo)    GITHUB_REPO="$2";   shift 2 ;;
        --branch)         BRANCH="$2";        shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Validate ──────────────────────────────────────────────────
if [[ -z "$WORKSPACE_URL" || -z "$TOKEN" || -z "$OWNER_EMAIL" ]]; then
    echo "❌  Missing required arguments: --workspace-url, --token, --owner-email"
    exit 1
fi

AUTH_HEADER="Authorization: Bearer $TOKEN"
BASE_URL="$WORKSPACE_URL/api"
REPO_PATH="/Workspace/Repos/${OWNER_EMAIL}/DATA_INTEGRATION"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Databricks Workspace Bootstrap            ║"
echo "╠══════════════════════════════════════════════╣"
echo "║ URL   : $WORKSPACE_URL"
echo "║ Owner : $OWNER_EMAIL"
echo "║ Catalog: $CATALOG"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── Helper ────────────────────────────────────────────────────
api_post() {
    local endpoint="$1"
    local data="$2"
    curl -s -f -X POST "${BASE_URL}${endpoint}" \
         -H "$AUTH_HEADER" \
         -H "Content-Type: application/json" \
         -d "$data"
}

api_get() {
    local endpoint="$1"
    curl -s -f -X GET "${BASE_URL}${endpoint}" \
         -H "$AUTH_HEADER"
}

# ── Step 1 : Create Git Repo (Databricks Repos) ───────────────
echo "📁 Step 1: Setting up Git Repo in Databricks workspace..."

REPO_RESPONSE=$(api_post "/2.0/repos" "{
    \"url\": \"${GITHUB_REPO}\",
    \"provider\": \"gitHub\",
    \"path\": \"${REPO_PATH}\"
}" 2>&1) || true

echo "   Repo response: $REPO_RESPONSE"

# Pull latest branch
echo "   Pulling branch: $BRANCH..."
api_post "/2.0/repos/update" "{
    \"path\": \"${REPO_PATH}\",
    \"branch\": \"${BRANCH}\"
}" > /dev/null 2>&1 || echo "   ⚠️  Branch pull returned non-200 (repo may already exist, continuing)"

echo "✅ Step 1 complete — Git Repo configured"

# ── Step 2 : Create Control Tables via SQL ────────────────────
echo ""
echo "🗄️  Step 2: Creating control tables in catalog: $CATALOG ..."

DDL_SQL=$(cat <<'ENDSQL'
CREATE SCHEMA IF NOT EXISTS ${CATALOG}.admin;

CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.data_flow_control_header (
    DATA_FLOW_GROUP_ID  STRING       NOT NULL,
    JOB_NAME            STRING,
    NOTEBOOK_PATH       STRING,
    IS_ACTIVE           STRING       DEFAULT 'Y',
    CREATED_DATE        TIMESTAMP,
    UPDATED_DATE        TIMESTAMP
)
USING DELTA
COMMENT 'Controls which ETL group is active and which notebook to execute';

CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.data_flow_l0_detail (
    DATA_FLOW_GROUP_ID  STRING       NOT NULL,
    SOURCE_URL          STRING,
    TARGET_SCHEMA       STRING,
    TARGET_TABLE        STRING,
    FILE_FORMAT         STRING,
    LOAD_TYPE           STRING       DEFAULT 'FULL',
    IS_ACTIVE           STRING       DEFAULT 'Y',
    CREATED_DATE        TIMESTAMP,
    UPDATED_DATE        TIMESTAMP
)
USING DELTA
COMMENT 'L0 (bronze) ingestion detail per group';

CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.data_flow_run_log (
    LOG_ID              BIGINT       GENERATED ALWAYS AS IDENTITY,
    DATA_FLOW_GROUP_ID  STRING,
    TARGET_TABLE        STRING,
    RUN_STATUS          STRING,
    ROWS_LOADED         BIGINT,
    ERROR_MESSAGE       STRING,
    START_TIME          TIMESTAMP,
    END_TIME            TIMESTAMP
)
USING DELTA
COMMENT 'Run history log for all ETL executions';
ENDSQL
)

# Replace placeholder
DDL_SQL="${DDL_SQL//\$\{CATALOG\}/$CATALOG}"

STATEMENT_RESPONSE=$(api_post "/2.0/sql/statements" "{
    \"statement\": \"${DDL_SQL}\",
    \"wait_timeout\": \"30s\"
}" 2>&1) || echo "⚠️  SQL statements API not available — run create_control_tables.sql manually in a notebook"

echo "✅ Step 2 complete — Control tables created"

# ── Step 3 : Create the Databricks Job ───────────────────────
echo ""
echo "⚙️  Step 3: Creating Databricks Job 'WEATHER_ETL_JOB'..."

JOB_JSON=$(cat <<ENDJOB
{
    "name": "DATA_INTEGRATION_JOB",
    "max_concurrent_runs": 3,
    "parameters": [
        {"name": "GROUP_ID", "default": ""}
    ],
    "tasks": [
        {
            "task_key": "run_framework",
            "notebook_task": {
                "notebook_path": "${REPO_PATH}/notebooks/run_framework",
                "base_parameters": {
                    "GROUP_ID": "{{job.parameters.GROUP_ID}}"
                },
                "source": "WORKSPACE"
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 1,
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true"
                }
            }
        }
    ]
}
ENDJOB
)

JOB_CREATE_RESP=$(api_post "/2.1/jobs/create" "$JOB_JSON")
JOB_ID=$(echo "$JOB_CREATE_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null || echo "UNKNOWN")

echo "✅ Step 3 complete — Job created. Job ID: $JOB_ID"

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   BOOTSTRAP COMPLETE ✅                             ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║ Workspace : $WORKSPACE_URL"
echo "║ Repo Path : $REPO_PATH"
echo "║ Job ID    : $JOB_ID"
echo "╠══════════════════════════════════════════════════════╣"
echo "║ NEXT STEPS:                                         ║"
echo "║  1. Add Job ID to workspaces.json                   ║"
echo "║  2. Add Databricks token to Jenkins Credentials     ║"
echo "║  3. Add workspace entry in workspaces.json          ║"
echo "║  4. Add workspace name to Jenkins WORKSPACE choices ║"
echo "╚══════════════════════════════════════════════════════╝"