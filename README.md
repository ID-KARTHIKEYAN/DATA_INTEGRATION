# Metadata-Driven ETL Framework — Databricks Free Edition

Production-grade, **100% metadata-driven** Medallion (L0 Bronze → L1 Silver → L2 Gold) ETL framework designed for and tested against **Databricks Free Edition**. Every table, column, relationship, and load-type dispatch is read from the existing 4 control tables in `demo_catalog.admin`. No columns invented, no tables recreated.

---

## Contents

1. [Quick Start](#1-quick-start)
2. [Source of Truth — Existing Tables](#2-source-of-truth--existing-tables)
3. [Architecture Overview](#3-architecture-overview)
4. [Metadata Flow](#4-metadata-flow)
5. [Framework Layers — L0 / L1 / L2](#5-framework-layers--l0--l1--l2)
6. [Load-Type Matrix](#6-load-type-matrix)
7. [Error, Retry & Audit Framework](#7-error-retry--audit-framework)
8. [Orchestration](#8-orchestration)
9. [Jenkins CI/CD Pipeline](#9-jenkins-cicd-pipeline)
10. [Code Structure](#10-code-structure)
11. [End-to-End Sample: EMPLOYEE_MASTER](#11-end-to-end-sample-employee_master)
12. [Databricks Free-Edition Limitations & Design Choices](#12-databricks-free-edition-limitations--design-choices)
13. [Existing Framework Weaknesses & Recommendations](#13-existing-framework-weaknesses--recommendations)
14. [Operational Runbook](#14-operational-runbook)
15. [Validation & Testing](#15-validation--testing)

---

## 1. Quick Start

```bash
# 1. Bootstrap control tables (run in Databricks workspace terminal or as notebook %sh)
bash setup/bootstrap_workspace.sh <optional-sql-warehouse-id>

# 2. Insert the sample EMPLOYEE_MASTER metadata (run in Databricks SQL Editor)
# Open setup/metadata_setup_sample.sql and execute all MERGE statements.

# 3. Interactive run (Databricks notebook UI):
#    Open notebooks/sample_runner.ipynb → choose mode = ALL → Run All.

# 4. Programmatically via run_framework.ipynb:
#    Widgets → GROUP_ID = EMPLOYEE_MASTER_L0, RUN_LAYER = AUTO → Run.
#    Then repeat for _L1, then _L2 — or pass RUN_LAYER = ALL with a combined header row.

# 5. Jenkins automated deploy/trigger:
#    Open Jenkins → New Item → Multibranch Pipeline → point at this repo → Jenkinsfile in jenkins/.
```

All framework modules compile cleanly:
```
python -m py_compile notebooks/framework/utils/*.py notebooks/framework/core/*.py notebooks/framework/layers/*.py
# → exit 0
```

---

## 2. Source of Truth — Existing Tables

Extracted verbatim from `frame_work_table_exist_databricks.txt` (DESCRIBE output + sample INSERTs).
**Catalog**: `demo_catalog` | **Schema**: `admin`

### 2.1 `data_flow_control_header` — 23 columns (per-group orchestration)

| Column | Type | Key | Purpose |
|---|---|---|---|
| `DATA_FLOW_GROUP_ID` | string | **PK** | Logical run unit, e.g. `EMPLOYEE_MASTER_L0 / _L1 / _L2` |
| `TRIGGER_TYPE` | string | | `JOB` / `DLT` / `WORKFLOW` (DLT = informational on Free Ed.) |
| **`ETL_LAYER`** | string | | `L0` / `L1` / `L2` — selects the detail table |
| `COMPUTE_CLASS_DEV` / `COMPUTE_CLASS` | string | | Spark cluster sizing hints (notebook sets confs from `SPARK_CONFIGS`) |
| `IS_ACTIVE` | string(1) | | `Y` = runnable. Not `Y` → header validation ERROR, pipeline abort |
| `SPARK_CONFIGS` | string | | JSON object or `k1=v1;k2=v2` → applied via `spark.conf.set` before reads |
| `WARNING_THRESHOLD_MINS` | int | | SLA threshold; logged in audit `EXTRA_JSON` |
| `WARNING_DL_GROUP` | string | | DL / team identifier for the alert |
| `MIN_VERSION` / `MAX_VERSION` | string | | DBR version gate; skipped on Free Ed. (info only) |
| `target_catalog` | string | | Overrides default catalog for target writes (sample: `demo_catalog`) |
| `BUSINESS_OBJECT_NAME` / `COST_CENTER` / `DATA_SME` / `BUSINESS_UNIT` / `PRODUCT_OWNER` | string | | Governance fields → audit `EXTRA_JSON` |
| `INGESTION_MODE` / `INGESTION_BUCKET` | string | | Passed to L0 SourceReader (`BATCH`/`STREAM`, bucket path) |
| `INSERTED_BY` / `UPDATED_BY` / `INSERTED_TS` / `UPDATED_TS` | string/timestamp | | Audit stamps |

### 2.2 `data_flow_l0_detail` — 24 columns (L0 Bronze ingestion detail)

Every column is consumed by [layers/l0_processor.py](notebooks/framework/layers/l0_processor.py). None ignored.

| Column | Type | Purpose |
|---|---|---|
| `DATA_FLOW_GROUP_ID` | string | FK → header |
| `SOURCE` | string | URI (HTTP / S3 / ADLS / DBFS) or logical source alias |
| `SOURCE_OBJ_SCHEMA` | string | Path prefix / qualifier |
| `SOURCE_OBJ_NAME` | string | File name / object name (ext stripped → bronze table name) |
| `LOB` | string | Line of business → bronze target schema |
| `LOAD_TYPE` | string | `FULL / DELTA / INCREMENTAL / APPEND / OVERWRITE` → normalised before write |
| `INPUT_FILE_FORMAT` | string | `CSV / TSV / JSON / PARQUET / XLSX / XML` |
| `STORAGE_TYPE` | string | `HTTP / S3 / ADLS / DBFS / LOCAL` |
| `DQ_LOGIC` | string | SQL `WHERE` clause applied after read |
| `DELIMETER` | string | CSV delimiter char |
| `CUSTOM_SCHEMA` | string | DDL `col type, col type…` passed to `.schema()` |
| `CDC_LOGIC` | string | SQL filter with `${WATERMARK}` substituted |
| `TRANSFORM_QUERY` | map<string,string> | Per-column `colName → SparkSQL expr` applied via `withColumn` |
| **`PRESTAG_FLAG`** | varchar(1) | `N`=TABLE, `V`=VIEW, `Y`=pre-staging TABLE |
| `PARTITION` | string | CSV list of Hive-style partition columns |
| `LS_FLAG` / `LS_DETAIL` | string / string | Lifecycle hooks: `B`efore, `A`fter, `Y`=Both + SQL/python body |
| `IS_ACTIVE` | string(1) | `Y`=enabled |
| `INSERTED_BY` / `UPDATED_BY` / `INSERTED_TS` / `UPDATED_TS` | | Audit stamps |
| `DEPLOYMENT_SOURCE_DFG` | string | Deployment provenance → audit |

### 2.3 `data_flow_pb_detail` — 26 columns (SHARED L1 Silver + L2 Gold)

**Per user mandate: L1 AND L2 both read from this table.** Layer is NOT stored on pb_detail — it comes from `control_header.ETL_LAYER` and the GROUP_ID suffix convention `_L0 / _L1 / _L2`. This avoids the original Jenkins bug that queried a non-existent `ETL_LAYER` on pb_detail.

| Column | Type | Purpose |
|---|---|---|
| `DATA_FLOW_GROUP_ID` | string | FK → header |
| `LOB` | string | Line of business |
| `SOURCE` | string | Logical source schema (`bronze` / `silver` / `stage` / etc.) |
| `TARGET_OBJ_SCHEMA` / `TARGET_OBJ_NAME` | string | Target `catalog.SCHEMA.TABLE` |
| **`PRIORITY`** | int | Bucket sort. P1 runs before P2 runs before P3 → DAG `depends_on` chains. |
| `TARGET_OBJ_TYPE` | string | `TABLE / MV (full-refresh TABLE) / VIEW` |
| `TRANSFORM_QUERY` | string | SQL SELECT. 2-part names auto-prefixed with `target_catalog` |
| `GENERIC_SCRIPTS` | string | Pre/Post SQL or python, `||` or `;;` separated. `${params.<key>}` injects `CUSTOM_SCRIPT_PARAMS`. |
| **`SOURCE_PK` / `TARGET_PK`** | string | CSV columns → MERGE keys. `target_pk or source_pk` is used. |
| `LOAD_TYPE` | string | `FULL / DELTA / INCREMENTAL / APPEND / OVERWRITE / MERGE / SCD / SCD2` |
| `IS_ACTIVE` | string(1) | Gate |
| `LS_FLAG` / `LS_DETAIL` | string / string | Pre/Post hooks |
| `PARTITION_OR_INDEX` | string | CSV columns |
| **`PARTITION_METHOD`** | string | `PARTITION` (Hive) or `LIQUID_CLUSTER` (ALTER TABLE CLUSTER BY) |
| `INSERTED_BY` / `UPDATED_BY` / `INSERTED_TS` / `UPDATED_TS` | | Audit stamps |
| `CUSTOM_SCRIPT_PARAMS` | map<string,string> | Key/value injectable into hooks/SQL |
| `RETENTION_DETAILS` | string | e.g. `3650 days` → parsed to retention days |
| `DEPLOYMENT_SOURCE_DFG` | string | Provenance → audit |

### 2.4 `audit_log` — 10 base columns + 4 framework-extended (idempotent ALTER ADD IF NOT EXISTS)

| Base Column | Type | Purpose |
|---|---|---|
| `DATA_FLOW_GROUP_ID` | string | Run key |
| `TARGET_TABLE` | string | Object affected |
| `STATUS` | string | `RUNNING / SUCCESS / FAILED` |
| `MESSAGE` | string | Human + truncated exception (≤ 400 chars — safe for VARCHAR) |
| `CREATED_DATE` / `START_TIME` / `END_TIME` / `LOAD_TS` | timestamp | Timestamps |
| `ETL_LAYER` | string | `L0 / L1 / L2` |
| `ROWS_PROCESSED` | long | Row count returned by TargetWriter |

| Extended Column (idempotent ADD IF NOT EXISTS) | Purpose |
|---|---|
| `DURATION_SECONDS` | `end - start` in seconds |
| `LOB` | Copied from detail row |
| `ENVIRONMENT` | From orchestrator widget `ENVIRONMENT` (DEV/UAT/PROD) |
| `EXTRA_JSON` | Full payload: error dict, traceback, spark confs, governance fields, counts |

---

## 3. Architecture Overview

```
 Jenkins (jenkins/Jenkinsfile — 7 stages)
  │  ├─► Validate ─► Checkout ─► Registry ─► Sync ─► Create/Refresh Jobs ─► Trigger ─► Wait & Report
  │  └─► Databricks REST 2.1 (Jobs + Repos)
  │
  └─► run_framework.ipynb  (Parent Orchestrator — widgets: GROUP_ID, TARGET_LOAD_TABLE, ENVIRONMENT, LOB, RUN_LAYER)
        │
        ├─► notebooks.framework.core.OrchestratorCore
        │     ├─► fetch_control_header() ─► IS_ACTIVE gate + SPARK_CONFIGS apply + target_catalog swap
        │     ├─► MetadataValidator.validate_control_header/l0_detail/pb_detail → ERROR aborts pre-flight
        │     ├─► fetch_l0_details(group_id, target_table_filter, lob_filter, is_active='Y')
        │     └─► fetch_pb_details(same filters) ─► group_by_priority buckets
        │
        ├─► notebooks.framework.layers.L0Processor   ─► data_flow_l0_detail
        │     ├─► SourceReader (HTTP→pandas fallback + DBFS/native, all formats)
        │     ├─► Transformer (LS PRE → TRANSFORM_QUERY map → DQ → CDC/${WATERMARK} → add_audit_columns)
        │     └─► TargetWriter (FULL/APPEND/INCREMENTAL + PARTITION/LIQUID_CLUSTER + retention) + LS POST
        │
        ├─► notebooks.framework.layers.L1L2Processor(layer='L1') ─► data_flow_pb_detail
        └─► notebooks.framework.layers.L1L2Processor(layer='L2') ─► data_flow_pb_detail  (shared table!)
              ├─► Transformer (LS PRE + GENERIC_SCRIPTS ─► TRANSFORM_QUERY with catalog-prefix SQL + ${} templating ─► DQ)
              └─► TargetWriter (+ MERGE / SCD2 / VIEW / MV, retention) + LS POST + CUSTOM_SCRIPT_PARAMS inject

        Shared vertical utilities (all stateless singleton where possible):
        ├─► utils.StructuredLogger  — JSON stdout + in-memory list buffer → export on exit
        ├─► utils.MetadataValidator — 4-table column-aware validation rules, WARNING/ERROR severity
        ├─► utils.AuditManager      — CREATE TABLE IF NOT EXISTS + ALTER ADD cols IF NOT EXISTS, write, get_watermark, history
        ├─► utils.RetryHandler      — exp backoff + jitter, retryable-message heuristic, @retry_with_backoff decorator
        ├─► utils.SchemaValidator   — DF compare, find_unresolved_columns, merge key null ratio
        └─► utils.SparkUtils        — set_spark_configs JSON/kv, qualify_table_name, catalog-prefix SQL, safe_identifier, parse_csv_list, parse_retention_details, ts_label, strip_file_extension
```

Key guarantee: **Multiple rows under the same GROUP_ID run independently.** Each detail row has its own `try / except / finally` — a failure in one writes FAILED to audit, then re-raises so the pipeline is marked FAILED, but sibling rows that ran before are not rolled back (Delta Lake per-commit semantics) and row-level audit records are always persisted.

---

## 4. Metadata Flow

```
  RUN TRIGGER (Jenkins / Job / UI)
       │
       ▼
  Widgets read (GROUP_ID, TARGET_LOAD_TABLE, ENVIRONMENT, LOB, RUN_LAYER)
       │
       ▼
  OrchestratorCore.fetch_control_header(GROUP_ID)
   ├─ IS_ACTIVE != 'Y'  →  write audit FAILED  →  dbutils.notebook.exit("FAILED: …")
   ├─ apply SPARK_CONFIGS via SparkUtils.set_spark_configs(json or k=v;k=v)
   ├─ if target_catalog non-empty → SET CATALOG target_catalog
   └─ MetadataValidator.validate_control_header → any ERROR → exit
       │
       ▼
  Determine layers_to_run
   ├─ AUTO → use control_header.ETL_LAYER  (one header row per layer via _L0/_L1/_L2 suffix)
   ├─ ALL  → [L0, L1, L2]  across 3 header rows sharing prefix GROUP_ID
   └─ L0/L1/L2 → forced single layer
       │
       ▼
  FOR each active_layer:
    ├─ LAYER = L0 ? fetch l0_detail : fetch pb_detail (shared pb table)
    │   └─ additional filters: TARGET_LOAD_TABLE exact match on target name, LOB match, IS_ACTIVE='Y'
    │
    ├─ L0Processor / L1L2Processor.process_group(rows)
    │    ├─ For L1/L2: OrchestratorCore.group_by_priority(rows) → {1: [..], 2: [..], 3: [..]}
    │    └─ For each priority bucket (sequential):
    │          └─ For each row in bucket:
    │                ├─ LS_FLAG ∈ {B,Y} ─► execute LS_DETAIL (PRE)
    │                ├─ Read source / Execute TRANSFORM_QUERY
    │                ├─ Apply DQ_LOGIC
    │                ├─ TargetWriter.write_table (load_type normalised, keys, partition, retention, object_type)
    │                ├─ LS_FLAG ∈ {A,Y} ─► execute LS_DETAIL (POST)
    │                └─ per-row audit SUCCESS/FAIL + EXTRA_JSON full payload
    │
    └─ layer-summary audit row written (RUNNING at layer start, SUCCESS/FAILED at end)
       │
       ▼
  Final pipeline_summary aggregated
       │
       ▼
  dbutils.notebook.exit("SUCCESS" or "FAILED: layer X / N rows failed …")
```

---

## 5. Framework Layers — L0 / L1 / L2

### 5.1 L0 — Bronze Raw Ingestion ([layers/l0_processor.py](notebooks/framework/layers/l0_processor.py))

**Step order** per detail row:
1. **IS_ACTIVE gate**: `!= 'Y'` → WARN + skip, do not write audit (row itself disabled at rest).
2. **LS PRE** if `LS_FLAG ∈ {B, Y}`: execute `LS_DETAIL` through `Transformer.execute_generic_scripts`.
3. **SourceReader.read(SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, STORAGE_TYPE, INPUT_FILE_FORMAT, CUSTOM_SCHEMA, DELIMETER)**:
   - `STORAGE_TYPE = HTTP` → `requests.get` → `BytesIO` → `pandas.read_csv/read_json/read_parquet/read_excel(read_xml)` → `spark.createDataFrame`
   - `STORAGE_TYPE = DBFS/S3/ADLS/LOCAL` → native `spark.read.format(INPUT_FILE_FORMAT).options(…).schema(CUSTOM_SCHEMA).load(SOURCE/PATH/FILE)`
4. **TRANSFORM_QUERY** (map): `Transformer.apply_l0_transform_map` — iterates the map, calls `withColumn(col, expr(expr_value))`. Errors on unresolved columns via SchemaValidator.
5. **CDC_LOGIC / Watermark**: `AuditManager.get_watermark(GROUP_ID, target_table, 'L0')` returns MAX(LOAD_TS) of last SUCCESS audit row. `${WATERMARK}` in CDC_LOGIC is replaced → `df.filter(expr(CDC_LOGIC))`.
6. **DQ_LOGIC**: `df.where(expr(DQ_LOGIC))`. Failures are logged ERROR but rows are dropped (user-defined DQ).
7. **Add audit columns**: `scd_start_date`, `load_ts`, `_ingested_by`, `_source_file` if not present.
8. **Target write**: Load-type normalisation first:
   - `OVERWRITE → FULL`
   - `DELTA → INCREMENTAL`
   Then `TargetWriter.write_table` with `object_type = 'VIEW' if PRESTAG_FLAG == 'V' else 'TABLE'`.
9. **LS POST** if `LS_FLAG ∈ {A, Y}`.
10. **Per-row audit**: SUCCESS or FAILED with full exception in EXTRA_JSON.

### 5.2 L1 — Silver Cleansed ([layers/l1l2_processor.py](notebooks/framework/layers/l1l2_processor.py))

Shared processor — constructor-arg `layer='L1'` routes audit-layer label correctly. **Shares `data_flow_pb_detail` with L2.**

Per row:
1. IS_ACTIVE gate + PRE LS_DETAIL + GENERIC_SCRIPTS (`||` or `;;` split; `${params.<key>}` substituted from `CUSTOM_SCRIPT_PARAMS`).
2. `Transformer.execute_transform_sql(TRANSFORM_QUERY, catalog=target_catalog, source_schema=SOURCE, params=…)`:
   - Templates replaced: `${GROUP_ID}`, `${TARGET_TABLE}`, `${TARGET_SCHEMA}`, `${WATERMARK}`, `${RUN_TS}`, `${params.<key>}`.
   - Catalog prefix: `FROM <schema>.<tbl>` (2-part) → `FROM <target_catalog>.<schema>.<tbl>` unless already 3-part or already matches the target catalog literal. Already-3-part references untouched.
   - SQL hint generator: if columns reference partition cols, prepend `/*+ PARTITION_PRUNING(<tbl>, <cols>) */`.
   - Result is `spark.sql(injected_sql)`.
3. DQ_LOGIC filter.
4. Merge keys = `parse_csv(TARGET_PK) or parse_csv(SOURCE_PK)`. **Runtime error if empty & LOAD_TYPE ∈ {MERGE, SCD, SCD2}** (MetadataValidator catches it pre-flight too).
5. `TARGET_OBJ_TYPE='VIEW'` → `_write_view` (CREATE OR REPLACE VIEW AS). `'MV'` → FULL overwrite TABLE. Otherwise standard TABLE.
6. Partition method dispatch: `PARTITION_OR_INDEX` cols + `PARTITION_METHOD ∈ {PARTITION, LIQUID_CLUSTER}`.
7. `RETENTION_DETAILS` → `_apply_retention` (find timestamp column, delete rows older than now-N days).
8. POST LS_DETAIL + GENERIC_SCRIPTS.
9. Audit.

### 5.3 L2 — Gold Dimensions & Facts

**Identical code path as L1** (it is the same L1L2Processor, just `layer='L2'`). Distinction from L1 is **purely a metadata convention**:
- L1 `SOURCE = 'bronze'`, `TARGET_OBJ_SCHEMA = 'silver'`
- L2 `SOURCE = 'silver'`, `TARGET_OBJ_SCHEMA = 'gold'`
- L2 uses `PRIORITY` heavily: dimensions PRIORITY=1, aggregates/facts PRIORITY=2/3, so dimensional keys exist before facts are built.
- L2 uses `LOAD_TYPE = SCD → SCD2` for slowly changing dimensions (dim_employee), and `LOAD_TYPE = FULL` or `MERGE` for aggregate facts.

---

## 6. Load-Type Matrix

| LOAD_TYPE in metadata | Normalised | TargetWriter path | Requirements |
|---|---|---|---|
| `FULL` | — | `_write_overwrite` — `.mode("overwrite").option("overwriteSchema", true)` | |
| `OVERWRITE` | `FULL` | Same as FULL | |
| `APPEND` | — | `_write_append` — `.mode("append").option("mergeSchema", true)`; creates empty table first if not exists | |
| `INCREMENTAL` | — | Same as APPEND, but expected to be combined with CDC_LOGIC watermark filter | |
| `DELTA` | `INCREMENTAL` | Same as INCREMENTAL | |
| `MERGE` | — | `_write_merge` — Delta MERGE INTO with UPDATE * + INSERT * | **Non-empty** `TARGET_PK` or `SOURCE_PK`; all keys must exist in DF; MetadataValidator ERROR + runtime null-ratio check |
| `SCD` | `SCD2` | `_write_scd2` — auto-adds `scd_start_date/scd_end_date/is_current`, MERGE closes existing + inserts new | Same key requirements |
| `SCD2` | — | Same as SCD | Same key requirements |

All paths:
- Use `saveAsTable(catalog.schema.table)` — always managed Delta tables.
- Auto-create schema via `CREATE SCHEMA IF NOT EXISTS`.
- Apply PARTITION / LIQUID_CLUSTER (LIQUID_CLUSTER ALTER is try/except WARN — safe on Free Ed. tiers that don't support it).
- Apply retention cleanup on successful write.
- Wrapped with `@retry_with_backoff(3)` — transient MERGE conflicts, network blips, timed-out operations are retried.

---

## 7. Error, Retry & Audit Framework

### 7.1 Fail-Loud Contract
**Never silently swallow errors.** Only the `audit_log INSERT` itself tolerates failure (otherwise original failure would be masked by audit-insert failure). Everywhere else: `log.FATAL` → `audit FAILED` → **re-raise**.

### 7.2 Retry ([utils/retry_handler.py](notebooks/framework/utils/retry_handler.py))

```
RetryHandler(attempts=3, base=5s, cap=120s, jitter=±20%)
 Heuristic _is_retryable_msg(e):
  matches "timed out", "retry", "conflict", "ECONNRESET", "502", "503", "Deadlock",
          "com.databricks.SQLState.TRANSIENT_*" → RETRY
  matches "AnalysisException", "ParseException", "NoSuchTable", "SCHEMA_MISMATCH" → NO RETRY
```

Decorated on: `SourceReader.read`, `TargetWriter.write_table`, `AuditManager.write`.

### 7.3 Metadata Validation ([utils/metadata_validator.py](notebooks/framework/utils/metadata_validator.py))

Returns `List[ValidationResult(severity={INFO,WARNING,ERROR}, code, message)]`. **Any ERROR → pre-flight abort.**

| Table | Rules |
|---|---|
| control_header | GROUP_ID suffix matches ETL_LAYER; TRIGGER_TYPE ∈ {JOB,DLT,WORKFLOW}; ETL_LAYER ∈ {L0,L1,L2}; IS_ACTIVE Y/N; WARNING_THRESHOLD_MINS int |
| l0_detail | SOURCE / SOURCE_OBJ_NAME non-empty; LOAD_TYPE supported enum; CSV format requires non-empty DELIMETER; TRANSFORM_QUERY is valid map/json when set |
| pb_detail | TARGET_OBJ_SCHEMA / TARGET_OBJ_NAME non-empty; LOAD_TYPE supported enum; **MERGE/SCD/SCD2 ⇒ non-empty (SOURCE_PK or TARGET_PK)** (ERROR); TARGET_OBJ_TYPE ∈ {TABLE, MV, VIEW}; PARTITION_METHOD ∈ {PARTITION, LIQUID_CLUSTER} when PARTITION_OR_INDEX set |
| audit_log | Column inventory at least 10 base present |

### 7.4 Audit Writes ([utils/audit_manager.py](notebooks/framework/utils/audit_manager.py))

```
AuditManager(spark, catalog, schema, logger)
 __init__:
  spark.sql("CREATE TABLE IF NOT EXISTS audit_log (…10 cols…)")
  spark.sql("ALTER TABLE audit_log ADD COLUMNS IF NOT EXISTS (
      duration_seconds double, lob string, environment string, extra_json string
  )")

 write(group_id, target_table, status, message, etl_layer, rows, start, end, load_ts,
       lob, environment, extra_dict):
  → if INSERT fails, log.ERROR but do NOT re-raise

 get_watermark(group_id, target_table, layer):
  → MAX(LOAD_TS) WHERE STATUS='SUCCESS' AND …; default TIMESTAMP'1900-01-01 00:00:00'

 get_run_history(group_id, last_n=20):
  → ORDER BY LOAD_TS DESC LIMIT last_n
```

### 7.5 Structured Logging ([utils/structured_logger.py](notebooks/framework/utils/structured_logger.py))

Singleton. Level `DEBUG/INFO/WARN/ERROR/FATAL`. Each call produces:
```json
{
  "ts": "2026-09-11T10:15:30.123Z",
  "level": "INFO",
  "msg": "Writing target",
  "table": "demo_catalog.silver.employee_master_clean",
  "load": "MERGE",
  "rows": 9500,
  "cols": 14
}
```
All events buffered → `export_logs_as_json()` returns the list on exit → written to notebook output for Jenkins artifact capture. No dropped events.

### 7.6 Schema Validation ([utils/schema_validator.py](notebooks/framework/utils/schema_validator.py))

| Method | Purpose |
|---|---|
| `compare_dataframes(expected, actual, mode='column-name+type')` | Detects schema drift on re-runs |
| `find_unresolved_columns(sql_text, available_cols)` | Scans TRANSFORM_QUERY text for column references not in `available_cols` → WARNING/ERROR list |
| `apply_transform_query_map_columns(map, input_schema)` → list[added/modified col] | Static map-schema diff |
| `check_merge_key_nulls(df, merge_keys, max_null_frac=0.0)` | Returns `(ok, {col: {count, ratio}})`. WARN if not ok → continue but logged so MERGE failures are anticipated |

---

## 8. Orchestration

### 8.1 Parent Orchestrator Notebook: `run_framework.ipynb`

Widgets → imports → SPARK session → layers_to_run → per-layer processor → aggregate → exit.

| Widget | Type | Default | Notes |
|---|---|---|---|
| `GROUP_ID` | text | — | Mandatory; matches `DATA_FLOW_GROUP_ID` in control_header |
| `TARGET_LOAD_TABLE` | text | (empty) | Optional. Filters detail rows by exact-match on target object name. **Allows running one task out of a multi-row GROUP_ID without touching siblings.** |
| `ENVIRONMENT` | dropdown | `DEV` | `DEV / UAT / PROD / STG` → written to audit_log ENVIRONMENT & EXTRA_JSON |
| `LOB` | text | (empty) | Optional filter on detail rows |
| `RUN_LAYER` | dropdown | `AUTO` | `AUTO / L0 / L1 / L2 / ALL` |

Layer determination rules:
- `AUTO`: reads `control_header.ETL_LAYER` for the GROUP_ID and runs that single layer. Validates GROUP_ID suffix agrees with it (`EMPLOYEE_MASTER_L0 ⇒ LAYER = L0`, etc.) → WARNING on mismatch.
- `L0 / L1 / L2`: forces that single layer (even if header says different — INFO).
- `ALL`: discovers sibling header rows sharing the prefix (before `_L0/_L1/_L2`) and runs L0 → L1 → L2 sequentially.

Pipeline exit:
```
If any per-row FAILED:
  dbutils.notebook.exit("FAILED: layer={LAYER}, failed_rows={N}, summary={agg}")
Else:
  dbutils.notebook.exit("SUCCESS")
```

### 8.2 Job Creation Automation: `job_creation_automation.py`

Databricks notebook (can be run on-demand or from Jenkins stage 5) that:
1. Reads all `IS_ACTIVE='Y'` header rows.
2. For each GROUP_ID + layer, fetches detail rows.
3. Groups pb_detail rows by PRIORITY → builds task list with `depends_on` across buckets.
4. Creates / updates a Databricks Job via REST 2.1 using a **single-node cluster** (Free Ed. compatible):
   ```yaml
   new_cluster:
     spark_version: 15.4.x-scala2.12
     node_type_id: i3.xlarge
     num_workers: 0                 # single-node
     spark_conf:  # taken from SPARK_CONFIGS per header
   ```
5. Idempotent: if a job with the same name already exists, fetches it, compares (task_count + task_key_hash), updates only on mismatch. Deletes stale jobs from prior framework versions.

---

## 9. Jenkins CI/CD Pipeline

File: [jenkins/Jenkinsfile](jenkins/Jenkinsfile)

### 9.1 Pipeline Parameters
```
String  PARAM_GROUP_ID          (e.g. EMPLOYEE_MASTER_L1)
String  PARAM_RUN_LAYER         AUTO / L0 / L1 / L2 / ALL (default AUTO)
String  PARAM_TARGET_LOAD_TABLE (default empty)
String  PARAM_ENVIRONMENT       DEV / UAT / PROD (default DEV)
String  PARAM_WORKSPACE_NAME    karthik / maran / abiram (default karthik — RAJA skipped, ACTIVE=false)
String  PARAM_LOB               (default empty)
Boolean PARAM_TRIGGER_RUN       true
Boolean PARAM_WAIT_COMPLETION   true
Integer PARAM_WAIT_TIMEOUT_MIN  120
```

### 9.2 Stage Breakdown

| # | Stage | What it does |
|---|---|---|
| 1 | Validate | Reads [workspaces.json](workspaces.json); resolves workspace URL/PAT credential; parses PARAMs; runs offline metadata-validation (dry-run `MetadataValidator` JSON schema check on local `metadata_setup_sample.sql` syntax); aborts early on ERROR. |
| 2 | Checkout SCM | `git clone $GITHUB_URL` branch `$BRANCH_NAME` using `GITHUB_TOKEN_SECRET` Jenkins credential. |
| 3 | Workspace Registry | Iterates workspaces.json; filters ACTIVE=true; resolves per-workspace Databricks credential (`DATABRICKS_TOKEN_KARTHIK`, `DATABRICKS_TOKEN_MARAN`, `abiram_token`); validates workspace reachable via `GET /api/2.0/clusters/list-node-types` 200 OK. |
| 4 | Sync Repos | For each active workspace, resolve `/Workspace/Repos/<user-email>/DATA_INTEGRATION` repo id; `POST /api/2.0/repos/{id}/pull` → pulls HEAD of current branch. |
| 5 | Create / Refresh Jobs | **Statement-API-free SQL task discovery**: (a) Detects layer from PARAM_RUN_LAYER=AUTO? → use GROUP_ID suffix validated against workspaces.json or header.ETL_LAYER from last known sync metadata. **Never queries pb_detail.ETL_LAYER** (the original Jenkinsfile bug — that column does not exist). (b) When layer = L0 → discovers tasks from l0_detail rows for the GROUP_ID; else → discovers from pb_detail. (c) Splits tasks by PRIORITY bucket → constructs `depends_on` edges. (d) Upserts multi-task Job via `POST /api/2.1/jobs/create` (or `POST /api/2.1/jobs/update` if existing job found by name). Job tasks all use the single-node `i3.xlarge` cluster from §8.2. (e) Writes `jobs_manifest.json` artifact → Jenkins archive. |
| 6 | Trigger | If `PARAM_TRIGGER_RUN = true`: `POST /api/2.1/jobs/run-now` with notebook params → returns `run_id` → stored as env.RUN_ID. |
| 7 | Wait & Report | Poll `GET /api/2.1/jobs/runs/get/{run_id}` every 15 s up to `PARAM_WAIT_TIMEOUT_MIN` minutes. On terminal state: `life_cycle_state ∈ {TERMINATED, SKIPPED, INTERNAL_ERROR}`; if `result_state != SUCCESS` → fail stage (fail build). Downloads notebook run output → extracts `logs_as_json` and `pipeline_summary` → writes `build_<number>_run_<id>_logs.json` + `run_report.html` → archived. Sends Slack-style console summary table. |

### 9.3 Critical Bug Fix vs. Original Jenkinsfile
The original queried:
```sql
SELECT * FROM demo_catalog.admin.data_flow_pb_detail WHERE ETL_LAYER = 'L1'
```
This was guaranteed to return nothing — `ETL_LAYER` is NOT a column of pb_detail (see 26-col DESCRIBE in source-of-truth). Fixed by deriving layer from GROUP_ID suffix + RUN_LAYER param + header.ETL_LAYER match. L1 and L2 both read the same `data_flow_pb_detail` (as user mandated).

---

## 10. Code Structure

```
DATA_INTEGRATION/
├── frame_work_table_exist_databricks.txt          ← SOURCE OF TRUTH. All table/column inventories.
├── workspaces.json                                ← Workspace registry: URL, PAT credential name, repo path, ACTIVE flag.
├── README.md                                      ← This file.
│
├── jenkins/
│   └── Jenkinsfile                                ← 7-stage multibranch pipeline.
│
├── notebooks/
│   ├── run_framework.ipynb                        ← PARENT ORCHESTRATOR. Widgets → layers → audit → exit.
│   ├── sample_runner.ipynb                        ← Interactive dropdown runner: ALL / L0 / L1 / L2 / RESET.
│   ├── job_creation_automation.py                 ← Idempotent multi-task DAG Databricks Jobs creator/updater.
│   │
│   └── framework/                                 ← Reusable PySpark package.
│       ├── __init__.py                            ← pkg marker; version.
│       │
│       ├── utils/                                 ← Pure, stateless helpers.
│       │   ├── __init__.py
│       │   ├── structured_logger.py               ← JSON logger singleton, LogLevel enum, buffer export.
│       │   ├── metadata_validator.py              ← 4-table column-aware validation rules. ValidationResult(severity).
│       │   ├── audit_manager.py                   ← CREATE TABLE IF NOT EXISTS, ALTER ADD IF NOT EXISTS, write, get_watermark, history.
│       │   ├── retry_handler.py                   ← RetryHandler + @retry_with_backoff decorator + retryable heuristic.
│       │   ├── schema_validator.py                ← DF compare, unresolved column scanner, merge key null ratio, transform map diff.
│       │   └── spark_utils.py                     ← set_spark_configs (JSON or k=v;k=v), qualify_table_name, apply_sql_catalog_prefix,
│       │                                            safe_identifier, ts_label, parse_csv_list, parse_retention_details, strip_file_extension.
│       │
│       ├── core/                                  ← I/O + transformation primitives.
│       │   ├── __init__.py
│       │   ├── source_reader.py                   ← HTTP (pandas fallback) + DBFS/S3/ADLS/native; all formats; CDC filter; retryable.
│       │   ├── target_writer.py                   ← FULL/APPEND/INCREMENTAL/MERGE/SCD2/VIEW; partition/liquid/retention; VIEW via CREATE OR REPLACE.
│       │   ├── transformer.py                     ← add_audit_columns, execute_transform_sql (catalog prefix + ${} templates + hint generator),
│       │   │                                        apply_l0_transform_map, execute_generic_scripts (||/;; splitter, SQL detect heuristic,
│       │   │                                        ${params.x} inject), apply_dq_logic.
│       │   └── orchestrator_core.py               ← fetch_control_header (IS_ACTIVE gate + SPARK_CONFIGS + target_catalog swap + validate),
│       │                                            fetch_l0_details, fetch_pb_details, group_by_priority bucket sort.
│       │
│       └── layers/                                ← Layer-specific column traversal; honor every metadata column.
│           ├── __init__.py
│           ├── l0_processor.py                    ← 24-col L0 loop: PRE-LS → read → transform-map → DQ → CDC/${WATERMARK} → add audit cols →
│           │                                        write (PRESTAG_FLAG → object_type, PARTITION, LS POST) → audit per row.
│           └── l1l2_processor.py                  ← 26-col PB loop shared L1/L2: priority buckets, PRE LS+GENERIC_SCRIPTS, catalog-prefix
│                                                    TRANSFORM_SQL → merge keys → write (MERGE/SCD2/VIEW/MV, PARTITION/LIQUID/RETENTION),
│                                                    POST LS+GENERIC_SCRIPTS → audit per row.
│
└── setup/
    ├── bootstrap_workspace.sh                     ← Creates 4 control tables with column-for-column DDL per DESCRIBE.
    │                                                  Falls back to printing SQL if no warehouse id.
    └── metadata_setup_sample.sql                  ← 3 EMPLOYEE_MASTER header rows + 1 L0 row + 1 L1 row + 2 L2 rows
                                                       (dim PRIORITY=1, fact aggregate PRIORITY=2). All MERGE — idempotent.
```

---

## 11. End-to-End Sample: EMPLOYEE_MASTER

### 11.1 Metadata

All MERGE statements in [setup/metadata_setup_sample.sql](setup/metadata_setup_sample.sql).

#### control_header rows
| DATA_FLOW_GROUP_ID | ETL_LAYER | TRIGGER_TYPE | target_catalog | SPARK_CONFIGS |
|---|---|---|---|---|
| `EMPLOYEE_MASTER_L0` | L0 | JOB | demo_catalog | `spark.sql.shuffle.partitions=8;spark.databricks.delta.optimizeWrite.enabled=true` |
| `EMPLOYEE_MASTER_L1` | L1 | JOB | demo_catalog | `spark.sql.shuffle.partitions=16` |
| `EMPLOYEE_MASTER_L2` | L2 | JOB | demo_catalog | `spark.sql.shuffle.partitions=32;spark.databricks.delta.autoCompact.enabled=true` |

#### l0_detail rows
| Col | Value |
|---|---|
| GROUP_ID | `EMPLOYEE_MASTER_L0` |
| SOURCE | `https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/refs/heads/main` |
| SOURCE_OBJ_SCHEMA | `(empty)` |
| SOURCE_OBJ_NAME | `employee_master_data_messy_10000.csv` |
| LOB | `hr` |
| LOAD_TYPE | `FULL` |
| INPUT_FILE_FORMAT | `CSV` |
| STORAGE_TYPE | `HTTP` |
| DELIMETER | `,` |
| TRANSFORM_QUERY | `map('_src', 'literal("github_main")')` |
| PRESTAG_FLAG | `N` |
| PARTITION | `department,country` |
| LS_FLAG / LS_DETAIL | `N / (empty)` |
| IS_ACTIVE | `Y` |

#### pb_detail rows
| GROUP_ID | PRIORITY | SOURCE | TARGET_OBJ_SCHEMA.NAME | TYPE | LOAD | KEYS | PARTITION | PARTITION_METHOD | RETENTION |
|---|---|---|---|---|---|---|---|---|---|
| `EMPLOYEE_MASTER_L1` | 1 | `bronze` | `silver.employee_master_clean` | Table | MERGE | `employee_id` | `department,country` | LIQUID_CLUSTER | — |
| `EMPLOYEE_MASTER_L2` | 1 | `silver` | `gold.dim_employee` | Table | SCD→SCD2 | `employee_id` | `department` | PARTITION | 3650 days |
| `EMPLOYEE_MASTER_L2` | 2 | `gold` | `gold.fact_hire_stats` | Table | FULL | `country,department,employment_status` | — | — | — |

TRANSFORM_QUERY for L1 (cleansing + dedupe):
```sql
SELECT
  employee_id,
  TRIM(first_name)           AS first_name,
  TRIM(last_name)            AS last_name,
  LOWER(TRIM(email))         AS email,
  TRIM(department)           AS department,
  TRIM(country)              AS country,
  CASE WHEN UPPER(TRIM(employment_status)) IN ('ACTIVE','FULL_TIME','PERMANENT') THEN 'ACTIVE'
       WHEN UPPER(TRIM(employment_status)) IN ('INACTIVE','TERMINATED','LAYOFF')  THEN 'INACTIVE'
       ELSE 'OTHER' END      AS employment_status,
  CAST(hire_date AS DATE)    AS hire_date,
  CAST(salary AS DECIMAL(18,2)) AS salary,
  load_ts,
  _src
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY load_ts DESC) AS _rn
  FROM demo_catalog.bronze.hr_employee_master
) _d
WHERE _rn = 1
```

TRANSFORM_QUERY for L2 dim_employee (SCD2):
```sql
SELECT
  employee_id,
  first_name,
  last_name,
  email,
  department,
  country,
  employment_status,
  hire_date,
  salary,
  load_ts AS scd_start_date
FROM demo_catalog.silver.employee_master_clean
```

TRANSFORM_QUERY for L2 fact_hire_stats (aggregate from current dimension rows):
```sql
SELECT
  country,
  department,
  employment_status,
  COUNT(*)                              AS head_count,
  SUM(CASE WHEN hire_date IS NOT NULL THEN 1 ELSE 0 END) AS hire_count,
  AVG(salary)                           AS avg_salary,
  CURRENT_TIMESTAMP()                   AS load_ts
FROM demo_catalog.gold.dim_employee
WHERE is_current = true
GROUP BY country, department, employment_status
```

### 11.2 Execution

In Databricks, open [notebooks/sample_runner.ipynb](notebooks/sample_runner.ipynb), select `Mode = ALL` → **Run All**.

OR manually run three times:
```
run_framework.ipynb
 (1) GROUP_ID=EMPLOYEE_MASTER_L0  RUN_LAYER=AUTO  ENV=DEV  LOB=hr
 (2) GROUP_ID=EMPLOYEE_MASTER_L1  RUN_LAYER=AUTO  ENV=DEV  LOB=hr
 (3) GROUP_ID=EMPLOYEE_MASTER_L2  RUN_LAYER=AUTO  ENV=DEV  LOB=hr
```

### 11.3 Expected results

| Target | Layer | Rows | Notes |
|---|---|---|---|
| `bronze.hr_employee_master` | L0 | 10,000 | Full overwrite, partitioned by (department, country), `_src='github_main'` |
| `silver.employee_master_clean` | L1 | ~9,500 | MERGE on employee_id, deduped, TRIM/LOWER-clean, liquid clustered by (department, country) on supported runtimes |
| `gold.dim_employee` | L2 PRIORITY 1 | ~9,500 current + 0 historical (1st run) | SCD2: is_current=true for all. On 2nd run with changed emails → 9,500 closed + 9,500 new |
| `gold.fact_hire_stats` | L2 PRIORITY 2 | ~ `#countries × #depts × 3 statuses` | Full overwrite; computed from current dim rows |

### 11.4 Audit verification SQL

```sql
SELECT
  data_flow_group_id,
  etl_layer,
  target_table,
  status,
  rows_processed,
  start_time,
  end_time,
  duration_seconds,
  environment,
  lob,
  SUBSTRING(message, 1, 200) AS msg_preview
FROM demo_catalog.admin.audit_log
WHERE data_flow_group_id LIKE 'EMPLOYEE_MASTER_%'
ORDER BY load_ts DESC
LIMIT 50;
```

Expect 4 SUCCESS detail rows (1 L0 + 1 L1 + 2 L2) + 3 layer-summary rows + 1 final pipeline summary row. Failures would have `EXTRA_JSON` with `{error: <full message>, traceback: <…>}`.

---

## 12. Databricks Free-Edition Limitations & Design Choices

| # | Capability | Free Ed. Status | How this framework works around it |
|---|---|---|---|
| 1 | **Serverless Jobs / SQL Warehouses** | Not available | All runs explicitly use Jobs API with `num_workers: 0` (single-node) clusters. `setup/bootstrap_workspace.sh` falls back to **printing SQL** if no SQL warehouse id passed — tables are created via `spark.sql` in a notebook instead. |
| 2 | **Delta Live Tables (DLT)** | Not available | `TRIGGER_TYPE='DLT'` is stored and validated as informational. Actual execution path is always JOB notebooks. |
| 3 | **Multi-node clusters** | 1 worker max | All job definitions request single-node. `SPARK_CONFIGS` default to single-node tuning. |
| 4 | **Liquid Clustering ALTER TABLE** | Gated by DBR + tier | TargetWriter wraps `ALTER TABLE … CLUSTER BY` in try/except WARN; silently no-ops with structured log entry. Partitioning still works. |
| 5 | **Materialized Views** | No live refresh scheduler | `TARGET_OBJ_TYPE='MV'` is materialised as a FULL TABLE refresh on every run. |
| 6 | **UC Lineage / Data Explorer graph** | Restricted | We reconstruct lineage in audit_log.EXTRA_JSON: records source, target, transform column list, and dependency edges via PRIORITY groups. |
| 7 | **Workflow Git auto-sync** | Limited PAT scope | Jenkins Stage 4 explicitly calls `Repos Pull` REST endpoint for each workspace. |
| 8 | **Low API rate limits** | Low calls/min | RetryHandler caps delay at 120 s + jitter; Jenkins wait loop polls at ≥ 15 s intervals. |
| 9 | **Auto-termination / idle max** | Aggressively short | Jobs keep compute alive only for the run duration (SPARK_CONFIGS can override idle timeout if needed). |
| 10 | **Workspace Repos > 2 GB limit** | Hard limit | Sample data CSV is NOT checked into this branch (deleted from main prior to branch and should stay on GitHub LFS or external — avoid bloating the repo). |

---

## 13. Existing Framework Weaknesses & Recommendations

All issues below are observable from `frame_work_table_exist_databricks.txt` and the prior `main` branch artifacts. **Recommendations in this section are currently implemented via code-only guards where possible** (no schema changes required). Future additive DDL suggestions are marked "Future DDL".

| # | Weakness | Impact | Already mitigated? | Future optional DDL (no schema change required to start using framework) |
|---|---|---|---|---|
| W1 | **pb_detail missing ETL_LAYER column.** Original Jenkins tried to filter `WHERE ETL_LAYER='L1'` → empty set, pipeline silently did nothing. | **Silent no-op deployments.** | ✅ Yes — framework derives layer from GROUP_ID suffix (`_L0/_L1/_L2`) + `control_header.ETL_LAYER`. L1/L2 both read the same pb_detail table (as user mandated). Jenkins stage 5 dispatches accordingly. | Add a `GENERATED ALWAYS AS` virtual column if you want SQL-only layer filtering: `ALTER TABLE admin.data_flow_pb_detail ADD COLUMNS (etl_layer GENERATED ALWAYS AS (CASE WHEN instr(DATA_FLOW_GROUP_ID,'_L0')>0 THEN 'L0' WHEN instr(DATA_FLOW_GROUP_ID,'_L1')>0 THEN 'L1' ELSE 'L2' END))`. |
| W2 | **No PK/FK declared on any control table.** Rows in l0_detail / pb_detail can exist with no matching header → SPARK_CONFIGS / target_catalog silently default, wrong catalog writes. | Orphan rows cause data-quality surprises at write time. | ✅ Yes — `MetadataValidator` emits ERROR for detail rows whose GROUP_ID has no `IS_ACTIVE='Y'` header; this becomes a pre-flight abort. | Unity Catalog supports informational FK constraints: `ALTER TABLE admin.data_flow_l0_detail ADD CONSTRAINT fk_l0_header FOREIGN KEY (DATA_FLOW_GROUP_ID) REFERENCES admin.data_flow_control_header (DATA_FLOW_GROUP_ID) NOVALIDATE ENFORCEMENT DISABLED;` Same for pb_detail. |
| W3 | **SOURCE_PK / TARGET_PK free-text CSV.** Cannot represent column names that contain commas; cannot validate columns exist pre-run. | MERGE key typo → run burns compute then fails MERGE on final insert. | ✅ Yes — (1) MetadataValidator ERROR when MERGE/SCD* and keys empty; (2) runtime `_write_merge` checks all keys present in transformed DF; (3) SchemaValidator pre-scans TRANSFORM_QUERY output columns + merge key null ratio. | Normalise into child table `admin.pb_pk_columns (DATA_FLOW_GROUP_ID string, SEQ int, PK_COL string, PK_ROLE string, CONSTRAINT pk_pb_pk PRIMARY KEY (DATA_FLOW_GROUP_ID, SEQ, PK_ROLE))`. |
| W4 | **LS_FLAG enum undocumented.** DESCRIBE gives no contract. Users guess `Y/N/B/A`; inconsistent use. | Pre/post hooks run or don't run unexpectedly. | ✅ Yes — documented in §2 + processor code enforces the contract: `B=Before, A=After, Y=Both, N/' '=None`. | `ALTER TABLE admin.data_flow_l0_detail ADD CONSTRAINT ls_flag_chk CHECK (LS_FLAG IN ('','N','B','A','Y'));` Same for pb_detail. |
| W5 | **Two overlapping "write as view" mechanisms:** `PRESTAG_FLAG='V'` on l0_detail vs `TARGET_OBJ_TYPE='VIEW'` on pb_detail. | User confusion which to set for a given layer. | ✅ Yes — framework unifies: L0 uses PRESTAG_FLAG (per DESCRIBE semantics of that column), PB uses TARGET_OBJ_TYPE. MetadataValidator WARNING on any ambiguous PRESTAG_FLAG value. | None needed; both columns have documented per-layer semantics now. |
| W6 | **MERGE/SCD key validation happened only at write time** (no pre-flight guard). | Wasteful failures after expensive transforms. | ✅ Yes — MetadataValidator pre-flight ERROR now; runtime double-checks; RetryHandler only retries transient errors, not structural ones. | None needed. |
| W7 | **audit_log MESSAGE size unbounded** — real PySpark AnalysisExceptions can be >64KB VARCHAR. Original code INSERT fails → exception replaced with insert-error, original error lost. | Debugging impossible on failure. | ✅ Yes — Framework writes MESSAGE safe-truncated to ≤400 chars AND writes `EXTRA_JSON` (string) with full error dict + traceback + params + counts. Never loses original info. | Future: `ALTER TABLE admin.audit_log ALTER COLUMN message TYPE STRING` (on UC; STRING is unlimited). |
| W8 | **No per-task isolation / no priority DAG.** One monolithic multi-table run → all-or-nothing failures; fact tables ran before dim tables and read-empty. | Wrong data order, unrelated sibling rows blocked each other. | ✅ Yes — (a) Per-row `try/except/finally` so sibling audits are independent; (b) `OrchestratorCore.group_by_priority` bucket sort; (c) Jenkins builds `depends_on` cross-task; (d) `TARGET_LOAD_TABLE` widget runs one row only. | None needed. |
| W9 | **Original bootstrap created L0 with wrong columns** (`SOURCE_URL`, `TARGET_SCHEMA`, `TARGET_TABLE`, `FILE_FORMAT` instead of `SOURCE`, `SOURCE_OBJ_SCHEMA`, `SOURCE_OBJ_NAME`, `INPUT_FILE_FORMAT`) and assumed `ETL_LAYER` existed on pb_detail. | **100% of metadata reads produced AnalysisException.** | ✅ Yes — `bootstrap_workspace.sh` rewritten to DESCRIBE-exact column counts/types. | Drop and recreate tables via the new bootstrap (the sample SQL uses correct columns — idempotent MERGE so no data loss). |
| W10 | **RETENTION_DETAILS free-text format** (no documented parser). `'3650 days'` vs `'P1Y'` vs `'${VAR}'` all break silently in naive parsers. | Retention never runs; compliance drift. | ✅ Yes — `SparkUtils.parse_retention_details` supports `<int> days` format (matches the sample data exactly), returns `None + WARN structured log` on any other format. Retention is skipped gracefully with a log line rather than silently ignored. | Standardise the column: write a small validation notebook that MERGE-updates `RETENTION_DETAILS` values to `<int> days` format across all rows. |

---

## 14. Operational Runbook

### 14.1 Adding a new data source (5-step recipe)

Suppose a new LOB `finance`, file `GL_BALANCES.parquet` on S3 → L0 → L1 (cleansed) → L2 (dim_gl_account SCD2 + fact_period_balance FULL).

```sql
-- Step 1: 3 header rows
INSERT INTO demo_catalog.admin.data_flow_control_header
  (DATA_FLOW_GROUP_ID, TRIGGER_TYPE, ETL_LAYER, IS_ACTIVE, target_catalog, SPARK_CONFIGS, BUSINESS_OBJECT_NAME, COST_CENTER, DATA_SME, BUSINESS_UNIT, PRODUCT_OWNER, INGESTION_MODE, WARNING_THRESHOLD_MINS, WARNING_DL_GROUP, MIN_VERSION, MAX_VERSION, INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS)
VALUES
  ('FIN_GL_L0','JOB','L0','Y','demo_catalog','spark.sql.shuffle.partitions=8','GL','FNC-001','jane@corp','Finance','John CFO','BATCH',60,'DL_FINANCE','15.0','16.0','framework','framework',current_timestamp,current_timestamp),
  ('FIN_GL_L1','JOB','L1','Y','demo_catalog','spark.sql.shuffle.partitions=16','GL','FNC-001','jane@corp','Finance','John CFO','BATCH',60,'DL_FINANCE','15.0','16.0','framework','framework',current_timestamp,current_timestamp),
  ('FIN_GL_L2','JOB','L2','Y','demo_catalog','spark.sql.shuffle.partitions=32','GL','FNC-001','jane@corp','Finance','John CFO','BATCH',60,'DL_FINANCE','15.0','16.0','framework','framework',current_timestamp,current_timestamp);

-- Step 2: L0 detail
INSERT INTO demo_catalog.admin.data_flow_l0_detail
  (DATA_FLOW_GROUP_ID, LOB, SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, LOAD_TYPE, INPUT_FILE_FORMAT, STORAGE_TYPE, DELIMETER, TRANSFORM_QUERY, DQ_LOGIC, CDC_LOGIC, CUSTOM_SCHEMA, PRESTAG_FLAG, PARTITION, LS_FLAG, LS_DETAIL, IS_ACTIVE, DEPLOYMENT_SOURCE_DFG, INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS)
VALUES
  ('FIN_GL_L0','finance','s3://corp-finance-ingest','gl','GL_BALANCES.parquet','DELTA','PARQUET','S3',NULL,
   map('gl_key','concat(account_id, "||", period)'),
   'account_id IS NOT NULL AND period IS NOT NULL',
   'load_ts > ${WATERMARK}',
   NULL,'N','period,account_class','Y','OPTIMIZE demo_catalog.bronze.fin_gl_balances ZORDER BY (account_id)','Y','jenkins-runbook-v1',
   'framework','framework',current_timestamp,current_timestamp);

-- Step 3: L1 pb_detail (cleansing MERGE)
INSERT INTO demo_catalog.admin.data_flow_pb_detail
  (DATA_FLOW_GROUP_ID, PRIORITY, LOB, SOURCE, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, TARGET_OBJ_TYPE, TRANSFORM_QUERY, LOAD_TYPE, SOURCE_PK, TARGET_PK, IS_ACTIVE, PARTITION_OR_INDEX, PARTITION_METHOD, LS_FLAG, LS_DETAIL, CUSTOM_SCRIPT_PARAMS, RETENTION_DETAILS, DEPLOYMENT_SOURCE_DFG, INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS)
VALUES
  ('FIN_GL_L1',1,'finance','bronze','silver','gl_balances_clean','Table',
   'SELECT account_id, period, TRIM(account_desc) AS account_desc, account_class, CAST(balance AS DECIMAL(18,2)) AS balance, load_ts FROM demo_catalog.bronze.fin_gl_balances',
   'MERGE','gl_key','gl_key','Y','period,account_class','LIQUID_CLUSTER','N','',map('repartition','200'),NULL,'jenkins-runbook-v1','framework','framework',current_timestamp,current_timestamp);

-- Step 4: L2 pb_detail dim SCD2 + fact (PRIORITY 1 then 2)
INSERT INTO demo_catalog.admin.data_flow_pb_detail
  (DATA_FLOW_GROUP_ID, PRIORITY, LOB, SOURCE, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, TARGET_OBJ_TYPE, TRANSFORM_QUERY, LOAD_TYPE, SOURCE_PK, TARGET_PK, IS_ACTIVE, PARTITION_OR_INDEX, PARTITION_METHOD, LS_FLAG, LS_DETAIL, CUSTOM_SCRIPT_PARAMS, RETENTION_DETAILS, DEPLOYMENT_SOURCE_DFG, INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS)
VALUES
  ('FIN_GL_L2',1,'finance','silver','gold','dim_gl_account','Table',
   'SELECT account_id, account_desc, account_class, load_ts AS scd_start_date FROM demo_catalog.silver.gl_balances_clean',
   'SCD','account_id','account_id','Y','account_class','PARTITION','N','',map('optimize.zorder','account_id'),'2555 days','jenkins-runbook-v1','framework','framework',current_timestamp,current_timestamp),
  ('FIN_GL_L2',2,'finance','silver','gold','fact_period_balance','Table',
   'SELECT period, account_class, SUM(balance) AS total_balance, COUNT(*) AS account_count, current_timestamp() AS load_ts FROM demo_catalog.silver.gl_balances_clean GROUP BY period, account_class',
   'FULL','period,account_class','period,account_class','Y','period','PARTITION','A','OPTIMIZE demo_catalog.gold.fact_period_balance ZORDER BY (period, account_class)',map('vacuum','168 hours'),NULL,'jenkins-runbook-v1','framework','framework',current_timestamp,current_timestamp);
```

Step 5: Run `run_framework.ipynb` GROUP_ID=FIN_GL_L0 → then _L1 → then _L2. Or use Jenkins with RUN_LAYER=ALL on the combined FIN_GL header prefix.

### 14.2 Triaging a failed run

1. Open the Databricks job run page → notebook output → last cell `logs_as_json` shows FATAL-level event with traceback.
2. Run audit query:
   ```sql
   SELECT status, target_table, etl_layer, SUBSTRING(extra_json, 1, 1000) AS extra_json_preview
   FROM demo_catalog.admin.audit_log
   WHERE data_flow_group_id = '<GROUP_ID>'
   ORDER BY load_ts DESC LIMIT 20;
   ```
3. `EXTRA_JSON.error` + `EXTRA_JSON.traceback` gives the original failing exception and exact stack.
4. `SchemaValidator` failures look for `"unresolved_columns":[...]` in EXTRA_JSON. Fix TRANSFORM_QUERY / merge keys.
5. `MERGE` key null: look for `WARN MERGE: some merge keys have NULL values` in logs. Fix DQ_LOGIC to filter null PKs upstream, or handle in TRANSFORM_QUERY.
6. Re-run. MERGE/SCD2 are idempotent — no data duplication.

### 14.3 Backfilling a historical range

1. In run_framework.ipynb, prepend a GENERIC_SCRIPTS pre-hook via LS_DETAIL on the target pb_detail row that sets a manual watermark: `SET var.manual_watermark = '2024-01-01 00:00:00'`; then in CDC_LOGIC use `${params.manual_watermark}` as the filter instead of `${WATERMARK}` (via CUSTOM_SCRIPT_PARAMS).
2. Execute.
3. Revert LS_DETAIL / CUSTOM_SCRIPT_PARAMS afterwards.

---

## 15. Validation & Testing

All tests are human-executable via Databricks notebook (`sample_runner.ipynb` mode = RESET / L0_ONLY / L1_ONLY / L2_ONLY / ALL):

| Test | Mode | Pass criteria |
|---|---|---|
| Metadata preflight RESET | RESET | MetadataValidator returns 0 ERROR severity |
| L0 ingest only | L0_ONLY | bronze.hr_employee_master count = 10,000; audit SUCCESS; watermark recorded |
| L1 only | L1_ONLY | silver.employee_master_clean count ≤ 10,000 (dedupe); no null email; MERGE 2nd run 0 new rows if source unchanged |
| L2 only (dim first, fact after) | L2_ONLY | fact_hire_stats references only dim_employee is_current=true rows; counts match |
| Full end-to-end | ALL | 4 detail SUCCESS audit rows; 3 layer SUCCESS; exit SUCCESS |
| Run twice, change 1 email in source | ALL | dim_employee: one is_current=false + one new is_current=true; head_count unchanged |
| Inject bad TRANSFORM (typo col) | L1_ONLY | MetadataValidator ERROR pre-flight; or runtime SchemaValidator unresolved_col → FAILED audit written, error preserved in EXTRA_JSON |
| Jenkins pipeline deploy+trigger | Jenkins | All 7 stages green; `build_<N>_run_<id>_logs.json` archived with SUCCESS result_state |

---

*Branch: `feature/metadata-driven-etl-framework-v2`*
*Designed for Databricks Free Edition; compatible with paid tiers (DLT / multi-node / serverless add value but are not required).*
