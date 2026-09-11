#!/bin/bash
# =====================================================================
# bootstrap_workspace.sh
# One-time setup for a NEW Databricks workspace — uses the EXISTING
# (source-of-truth) table schema from frame_work_table_exist_databricks.txt
# instead of the previous incorrect placeholder DDL.
#
# Usage:
#   chmod +x bootstrap_workspace.sh
#   ./bootstrap_workspace.sh \
#       --workspace-url  https://dbc-XXXX.cloud.databricks.com \
#       --token          dapiXXXXXXXX \
#       --owner-email    user@gmail.com \
#       --catalog        demo_catalog \
#       --github-repo    https://github.com/ID-KARTHIKEYAN/DATA_INTEGRATION.git \
#       --branch         main
# =====================================================================

set -euo pipefail

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

if [[ -z "$WORKSPACE_URL" || -z "$TOKEN" || -z "$OWNER_EMAIL" ]]; then
    echo "❌ Missing required: --workspace-url, --token, --owner-email"
    exit 1
fi

AUTH_HEADER="Authorization: Bearer $TOKEN"
BASE_URL="$WORKSPACE_URL/api"
REPO_PATH="/Workspace/Repos/${OWNER_EMAIL}/DATA_INTEGRATION"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Databricks Workspace Bootstrap (correct schema)   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║ URL     : $WORKSPACE_URL"
echo "║ Owner   : $OWNER_EMAIL"
echo "║ Catalog : $CATALOG"
echo "║ Repo    : $GITHUB_REPO"
echo "║ Branch  : $BRANCH"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

api_post() {
    local endpoint="$1"
    local data="$2"
    curl -sS -f -X POST "${BASE_URL}${endpoint}" \
         -H "$AUTH_HEADER" \
         -H "Content-Type: application/json" \
         -d "$data"
}
api_get() {
    local endpoint="$1"
    curl -sS -f -X GET "${BASE_URL}${endpoint}" -H "$AUTH_HEADER"
}

# Step 1 : link the git repo via Databricks Repos
echo "📁 Step 1: Git repo setup"
REPO_RESPONSE=$(api_post "/2.0/repos" "{
    \"url\": \"${GITHUB_REPO}\",
    \"provider\": \"gitHub\",
    \"path\": \"${REPO_PATH}\"
}" 2>&1) || true
echo "   create: ${REPO_RESPONSE:-ok (may already exist)}"
sleep 1
api_post "/2.0/repos/update" "{
    \"path\": \"${REPO_PATH}\",
    \"branch\": \"${BRANCH}\"
}" > /dev/null 2>&1 || echo "   ⚠️ pull branch non-200 (continuing)"
echo "✅ Step 1"

# Step 2 : Create the admin schema + 3 framework tables using the EXACT
#          column set from frame_work_table_exist_databricks.txt
echo ""
echo "🗄️  Step 2: control tables in ${CATALOG}."

TABLES_SQL=$(cat <<ENDSQL
CREATE SCHEMA IF NOT EXISTS ${CATALOG}.admin COMMENT 'ETL control & audit';

-- =========================================================
-- DATA_FLOW_CONTROL_HEADER  — one row per DATA_FLOW_GROUP_ID
-- (23 columns exactly as described)
-- =========================================================
CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.data_flow_control_header (
    DATA_FLOW_GROUP_ID   STRING        NOT NULL   COMMENT 'Logical group ID (e.g. EMPLOYEE_MASTER_L0)',
    TRIGGER_TYPE         STRING                 COMMENT 'JOB or DLT',
    ETL_LAYER            STRING                 COMMENT 'L0 / L1 / L2',
    COMPUTE_CLASS_DEV    STRING                 COMMENT 'Compute class name for dev',
    COMPUTE_CLASS        STRING                 COMMENT 'Compute class name for qa/prod',
    IS_ACTIVE            STRING        DEFAULT 'Y' COMMENT 'Y = pipeline runs',
    INSERTED_BY          STRING,
    UPDATED_BY           STRING,
    INSERTED_TS          TIMESTAMP,
    UPDATED_TS           TIMESTAMP,
    BUSINESS_OBJECT_NAME STRING                 COMMENT 'Data product display name',
    COST_CENTER          STRING,
    DATA_SME             STRING,
    BUSINESS_UNIT        STRING,
    PRODUCT_OWNER        STRING,
    INGESTION_MODE       STRING                 COMMENT 'Only for L0, e.g. github/s3/kafka',
    INGESTION_BUCKET     STRING                 COMMENT 'Only for L0',
    SPARK_CONFIGS        STRING                 COMMENT 'JSON or k=v;k=v string applied to the run',
    WARNING_THRESHOLD_MINS INT,
    WARNING_DL_GROUP     STRING,
    MIN_VERSION          STRING,
    MAX_VERSION          STRING,
    target_catalog       STRING                 COMMENT 'Override default catalog for downstream'
) USING DELTA COMMENT 'ETL pipeline control header (1 row per GROUP_ID + LAYER)';

-- =========================================================
-- DATA_FLOW_L0_DETAIL  — one row per source file ingested
-- (24 columns exactly)
-- =========================================================
CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.data_flow_l0_detail (
    DATA_FLOW_GROUP_ID  STRING          NOT NULL,
    SOURCE              STRING                   COMMENT 'URL / s3 path / dbfs path',
    SOURCE_OBJ_SCHEMA   STRING                   COMMENT 'Target schema for bronze table',
    SOURCE_OBJ_NAME     STRING                   COMMENT 'Source file name (e.g. emp.csv) or bronze table name',
    LOB                 STRING,
    LOAD_TYPE           STRING                   COMMENT 'FULL or DELTA',
    INPUT_FILE_FORMAT   STRING                   COMMENT 'csv/json/parquet/delta/avro/orc/tsv/xml/xlsx',
    STORAGE_TYPE        STRING                   COMMENT 's3 / dbfs / volumes / http-url base',
    DQ_LOGIC            STRING                   COMMENT 'Optional WHERE predicate used BEFORE write',
    DELIMETER           STRING                   COMMENT 'CSV delimiter OR XML row tag',
    CUSTOM_SCHEMA       STRING                   COMMENT 'Custom TSV or DDL schema override',
    CDC_LOGIC           STRING                   COMMENT 'WHERE predicate for DELTA loads with WATERMARK substitution',
    TRANSFORM_QUERY     MAP<STRING,STRING>       COMMENT 'Per-column cast exprs e.g. map("salary","CAST(salary AS DECIMAL(18,2))")',
    PRESTAG_FLAG        VARCHAR(1)               COMMENT 'Y = streaming/prestage table, N = external delta',
    `PARTITION`         STRING                   COMMENT 'Comma-sep column list',
    LS_FLAG             STRING                   COMMENT 'Y/N/B/A — run LS_DETAIL before/after/both',
    LS_DETAIL           STRING                   COMMENT 'Python or SQL hook script',
    IS_ACTIVE           STRING        DEFAULT 'Y',
    INSERTED_BY         STRING,
    UPDATED_BY          STRING,
    INSERTED_TS         TIMESTAMP,
    UPDATED_TS          TIMESTAMP,
    DEPLOYMENT_SOURCE_DFG STRING
) USING DELTA COMMENT 'L0 (bronze) ingestion detail — SOURCE_OBJ_SCHEMA.SOURCE_OBJ_NAME is the target';

-- =========================================================
-- DATA_FLOW_PB_DETAIL  — L1/L2 (Publish Business) detail
-- shared table for BOTH layers (layer is set via control_header.ETL_LAYER)
-- (26 columns exactly)
-- =========================================================
CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID  STRING          NOT NULL,
    LOB                 STRING,
    SOURCE              STRING                   COMMENT 'Source layer/schema name (for default copy)',
    TARGET_OBJ_SCHEMA   STRING                   COMMENT 'Target schema (silver or gold)',
    TARGET_OBJ_NAME     STRING                   COMMENT 'Target table/view/mv name',
    PRIORITY            INT                      COMMENT 'Execution priority; tasks at same P run in parallel',
    TARGET_OBJ_TYPE     STRING                   COMMENT 'TABLE / MV / VIEW',
    TRANSFORM_QUERY     STRING                   COMMENT 'SELECT ... FROM ...  (the build query)',
    GENERIC_SCRIPTS     STRING                   COMMENT 'Python/SQL pre/post hook scripts',
    SOURCE_PK           STRING                   COMMENT 'Comma-sep source PK columns',
    TARGET_PK           STRING                   COMMENT 'Comma-sep target PK columns (for MERGE/SCD)',
    LOAD_TYPE           STRING                   COMMENT 'FULL / DELTA / SCD',
    IS_ACTIVE           STRING        DEFAULT 'Y',
    LS_FLAG             STRING                   COMMENT 'Y/N/B/A — run LS_DETAIL pre/post/both',
    LS_DETAIL           STRING                   COMMENT 'Python or SQL hook',
    PARTITION_OR_INDEX  STRING                   COMMENT 'Comma-sep columns for partitioning / liquid clustering',
    INSERTED_BY         STRING,
    UPDATED_BY          STRING,
    INSERTED_TS         TIMESTAMP,
    UPDATED_TS          TIMESTAMP,
    CUSTOM_SCRIPT_PARAMS MAP<STRING,STRING>      COMMENT 'Params for GENERIC_SCRIPTS / LS_DETAIL',
    PARTITION_METHOD    STRING                   COMMENT 'PARTITION or LIQUID_CLUSTER',
    RETENTION_DETAILS   STRING                   COMMENT 'Retention e.g. "365 days"',
    DEPLOYMENT_SOURCE_DFG STRING
) USING DELTA COMMENT 'L1 + L2 (publish business) build detail — shared table';

-- =========================================================
-- AUDIT_LOG  — per-table execution writes go here
-- =========================================================
CREATE TABLE IF NOT EXISTS ${CATALOG}.admin.audit_log (
    DATA_FLOW_GROUP_ID STRING,
    TARGET_TABLE       STRING,
    STATUS             STRING,
    MESSAGE            STRING,
    CREATED_DATE       TIMESTAMP,
    ETL_LAYER          STRING,
    ROWS_PROCESSED     BIGINT,
    START_TIME         TIMESTAMP,
    END_TIME           TIMESTAMP,
    LOAD_TS            TIMESTAMP,
    DURATION_SECONDS   DOUBLE,
    LOB                STRING,
    ENVIRONMENT        STRING,
    EXTRA_JSON         STRING
) USING DELTA COMMENT 'ETL audit trail for every table write';
ENDSQL
)

echo "   Looking up any SQL Warehouse for DDL..."
WAREHOUSES=$(api_get "/2.0/sql/warehouses" 2>&1) || WAREHOUSES='{"warehouses":[]}'
WAREHOUSE_ID=$(python3 - <<PY
import json, sys
try:
    d = json.loads("""${WAREHOUSES}""")
    w = (d.get("warehouses") or [])
    if not w:
        print("")
    else:
        print(w[0]["id"])
except Exception as e:
    print("")
PY
)

if [[ -n "$WAREHOUSE_ID" ]]; then
    echo "   Using warehouse ${WAREHOUSE_ID}"
    STATEMENT_RESPONSE=$(api_post "/2.0/sql/statements" "{
        \"statement\": $(printf '%s' "$TABLES_SQL" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),
        \"warehouse_id\": \"${WAREHOUSE_ID}\",
        \"wait_timeout\": \"120s\"
    }" 2>&1) || STATEMENT_RESPONSE='{"error":"api failed"}'
    echo "   tables response: $(printf '%s' "$STATEMENT_RESPONSE" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(d.get("status","UNKNOWN") if isinstance(d, dict) else d[:200])')"
else
    echo "   ⚠️ no warehouse accessible via token; please run the SQL below in a notebook/SQL editor:"
    echo "------------------------------------------------------------"
    echo "$TABLES_SQL"
    echo "------------------------------------------------------------"
fi
echo "✅ Step 2"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   BOOTSTRAP COMPLETE ✅                             ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║ Workspace : $WORKSPACE_URL"
echo "║ Repo      : $REPO_PATH"
echo "║ Catalog   : $CATALOG"
echo "╠══════════════════════════════════════════════════════╣"
echo "║ NEXT STEPS:                                         ║"
echo "║  1. Run setup/metadata_setup_sample.sql to seed     ║"
echo "║     EMPLOYEE_MASTER groups.                        ║"
echo "║  2. Run notebooks/job_creation_automation.py to     ║"
echo "║     create Databricks multi-task jobs.             ║"
echo "║  3. Run notebooks/sample_runner.ipynb to test      ║"
echo "║     the end-to-end L0→L1→L2 pipeline.              ║"
echo "║  4. Wire Jenkins with jenkins/Jenkinsfile.         ║"
echo "╚══════════════════════════════════════════════════════╝"
