# Metadata-driven ETL framework

This design uses the existing Databricks tables in `demo_catalog.admin` as the contract. It does not recreate or rename them. The implementation is in `framework/`; the old mixed-schema notebook was deleted and replaced by `notebooks/run_framework.py`.

## 1. Existing table analysis

### `data_flow_control_header`

One active header is the control-plane record for a `DATA_FLOW_GROUP_ID`. `TRIGGER_TYPE` selects DLT or JOB; `ETL_LAYER` is the requested layer; `COMPUTE_CLASS_DEV` and `COMPUTE_CLASS` describe compute; `IS_ACTIVE` gates deployment/execution. `BUSINESS_OBJECT_NAME`, `COST_CENTER`, `DATA_SME`, `BUSINESS_UNIT`, and `PRODUCT_OWNER` are ownership and lineage metadata. `INGESTION_MODE` and `INGESTION_BUCKET` describe L0 ingestion. `SPARK_CONFIGS` is a string of Spark settings. `WARNING_THRESHOLD_MINS` and `WARNING_DL_GROUP` are alert settings. `MIN_VERSION` and `MAX_VERSION` bound the framework package version. `target_catalog` is the runtime target catalog.

`DATA_FLOW_GROUP_ID` is the logical parent key. `INSERTED_BY`, `UPDATED_BY`, `INSERTED_TS`, and `UPDATED_TS` are control-row audit fields. The attached schema has no explicit primary-key constraint; the framework therefore validates exactly one active header per group.

### `data_flow_l0_detail`

One row is one L0 source task under the group. `SOURCE` is the source location; `SOURCE_OBJ_SCHEMA` and `SOURCE_OBJ_NAME` identify the source object and become the L0 target identity in the current contract. `LOB` is business ownership. `LOAD_TYPE` controls load behavior. `INPUT_FILE_FORMAT`, `STORAGE_TYPE`, `DELIMETER`, and `CUSTOM_SCHEMA` control parsing. `DQ_LOGIC` is a source-row quality predicate. `CDC_LOGIC` is an incremental predicate. `TRANSFORM_QUERY` is a map of per-column cast expressions. `PRESTAG_FLAG` distinguishes streaming-table intent from external stage intent. `PARTITION` is physical partition metadata. `LS_FLAG` and `LS_DETAIL` are lift-and-shift metadata. `IS_ACTIVE` gates the task. The remaining by/timestamp/deployment columns are audit and deployment lineage.

There is no L0 `PRIORITY`, target schema, target table, or dedicated watermark column.

### `data_flow_pb_detail`

One row is one published-business task. `SOURCE` is the source layer/schema label. `TARGET_OBJ_SCHEMA` and `TARGET_OBJ_NAME` identify the output. `PRIORITY` provides dependency ordering and parallelism. `TARGET_OBJ_TYPE` is documented as `Table` or `MV`. `TRANSFORM_QUERY` is the executable select. `GENERIC_SCRIPTS` names optional scripts. `SOURCE_PK` and `TARGET_PK` define merge identity. `LOAD_TYPE` documents `FULL`, `DELTA`, or `SCD`. `IS_ACTIVE` gates execution. `LS_FLAG` and `LS_DETAIL` carry lift-and-shift metadata. `PARTITION_OR_INDEX`, `PARTITION_METHOD`, and `RETENTION_DETAILS` describe physical/retention intent. `CUSTOM_SCRIPT_PARAMS` is a `map<string,string>`, not JSON text. The remaining by/timestamp/deployment columns are audit and deployment lineage.

The attached PB schema has no `ETL_LAYER`, source object schema/name, watermark column, or target catalog column. Therefore the active header selects the layer for the group, and PB rows cannot independently distinguish L1 from L2 when both are stored under the same group. This is a contract limitation, not something the runner guesses around.

### `audit_log`

The sample insert establishes the operational log contract: `DATA_FLOW_GROUP_ID`, `TARGET_TABLE`, `STATUS`, `MESSAGE`, `CREATED_DATE`, `ETL_LAYER`, `ROWS_PROCESSED`, `START_TIME`, `END_TIME`, and `LOAD_TS`. The logger writes success/failure records and rethrows every failure.

## 2. Column mapping and relationships

`control_header.DATA_FLOW_GROUP_ID` joins to both detail tables' `DATA_FLOW_GROUP_ID`. Header `ETL_LAYER='L0'` selects L0 detail; header `ETL_LAYER='L1'` or `L2` selects PB detail. Header `target_catalog` plus L0 `SOURCE_OBJ_SCHEMA/SOURCE_OBJ_NAME` or PB `TARGET_OBJ_SCHEMA/TARGET_OBJ_NAME` forms the fully qualified output. PB `PRIORITY` orders tasks; it is never read from L0. PB keys select merge columns; they are validated against the transformation result. All `IS_ACTIVE` values must be `Y`.

## 3. Architecture

`run_framework.py` is the thin entry point. `MetadataRepository` reads each detail table with its own columns. `Orchestrator` validates the one active header, selects only the requested layer and optional task identity, and executes tasks in priority order. `engine.py` reads, filters, transforms, writes Delta, and validates keys. `AuditLogger` writes the existing `audit_log`. `identifiers.py` rejects unsafe dynamic catalog/schema/object names.

No table-specific notebook code is used. Runtime values are passed as `GROUP_ID`, `RUN_LAYER`, `TASK_ID`, and `CATALOG`.

## 4. Metadata flow

1. Read one active header for the requested group.
2. Require header `ETL_LAYER` to equal the requested `RUN_LAYER`.
3. Read only `data_flow_l0_detail` for L0, or only `data_flow_pb_detail` for L1/L2.
4. Filter active rows by the exact group.
5. Build a task identity: `group|L0|source_schema|source_name`, or `group|layer|target_schema|target_name`.
6. If `TASK_ID` is supplied, require exactly that active task; no other row runs.
7. Validate required fields before reading data.
8. Write one audit row per task and propagate failures.

## 5. L0/L1/L2 framework

L0 reads `SOURCE` using `INPUT_FILE_FORMAT`, `DELIMETER`, and optional `CUSTOM_SCHEMA`. It applies `DQ_LOGIC` and `CDC_LOGIC`, then writes Delta to the metadata-derived target. `TRANSFORM_QUERY` map expressions should be applied in a future extension with explicit column existence checks; the current runner never invents columns.

L1 and L2 execute only PB `TRANSFORM_QUERY`. The result must expose all configured merge keys. `FULL` and `OVERWRITE` use Delta overwrite; `APPEND` appends; attached `DELTA` is normalized to merge and attached `SCD` is normalized to merge pending a richer SCD contract. `MERGE` validates keys, creates the target on first load, and otherwise uses Delta `MERGE` with matched updates and not-matched inserts.

`MV` is rejected by the Free Edition runner because the attached contract says MV but does not provide refresh/dependency semantics. Use `TARGET_OBJ_TYPE='Table'` for executable tasks.

## 6. Error, DQ, and audit framework

Metadata errors include missing required fields, duplicate active headers, invalid identifiers, unsupported load types, missing merge keys, layer mismatch, and missing transformation columns. DQ uses `DQ_LOGIC` as a Spark filter and fails if the transformation produces no columns. CDC uses `CDC_LOGIC`; `{last_load_ts}` is substituted from the latest successful `audit_log.LOAD_TS` for that task. With no previous success it substitutes `NULL`, so metadata must explicitly handle the first run.

Every task writes `SUCCESS` or `FAILED` to `audit_log`. Audit-write errors become framework errors. Exceptions are never swallowed, and the orchestrator stops so Databricks/Jenkins receives a failed task. Retry is delegated to the Databricks Job task retry policy; reruns are idempotent for overwrite and key-based merge modes. Append is intentionally not idempotent without a source key or deduplication rule.

## 7. Orchestrator

Parent input is `GROUP_ID`, `RUN_LAYER`, and optional `TASK_ID`. A group with multiple rows runs all and only its active rows. A scheduler may create one Databricks task per PB target and connect priority N to priority N+1; the runner still receives `TASK_ID`, preventing a task from processing its siblings.

## 8. Jenkins deployment

Jenkins should sync the Repos checkout, validate the header/detail rows through a SQL warehouse, create or reconcile one job per group, and create one task per metadata identity. L0 task discovery must use `SOURCE_OBJ_SCHEMA/SOURCE_OBJ_NAME` and no `PRIORITY`. L1/L2 discovery must use `TARGET_OBJ_SCHEMA/TARGET_OBJ_NAME/PRIORITY`. Job parameters are `GROUP_ID`, `RUN_LAYER`, `TASK_ID`, and `CATALOG`; the task path is `notebooks/run_framework.py` or a Databricks notebook wrapper that imports `framework`.

Jenkins must fail on non-2xx API responses, missing warehouses, missing active metadata, duplicate task identities, or unsuccessful runs. It must not use `LOAD_TYPE='VIEW'` as a substitute for `TARGET_OBJ_TYPE`, and must not trigger a nonexistent `view_pipeline`.

## 9. Complete code structure

```text
DATA_INTEGRATION/
  framework/
    __init__.py
    audit.py
    engine.py
    errors.py
    identifiers.py
    metadata.py
    orchestrator.py
  notebooks/
    run_framework.py
    job_creation_automation.py
  tests/
    test_identifiers.py
  jenkins/
    Jenkinsfile
  setup/
    bootstrap_workspace.sh
  META_STORE_SQL_SCRIPTS/
  METADATA_ETL_FRAMEWORK.md
```

## 10. End-to-end example

The supplied metadata contains `EMPLOYEE_MASTER_L0` in the header and L0 detail, with `SOURCE` set to the GitHub CSV, `SOURCE_OBJ_SCHEMA='raw'`, `SOURCE_OBJ_NAME='employee_master_data_messy_10000.csv'`, `INPUT_FILE_FORMAT='csv'`, `LOAD_TYPE='FULL'`, and `target_catalog='demo_catalog'`. Running `GROUP_ID=EMPLOYEE_MASTER_L0`, `RUN_LAYER=L0` validates that one header and one active L0 row exist, reads the CSV with comma delimiter, applies configured filters, writes `demo_catalog.raw.employee_master_data_messy_10000.csv` as Delta, counts rows, and appends `SUCCESS` to `demo_catalog.admin.audit_log`.

For a PB row, `GROUP_ID=STU_ACTIVITY_L1`, `RUN_LAYER=L1`, and `TASK_ID=STU_ACTIVITY_L1|L1|silver|dim_group_silver` execute only that row's `TRANSFORM_QUERY`, validate `SOURCE_PK/TARGET_PK` if merge is requested, write `demo_catalog.silver.dim_group_silver`, and append the corresponding L1 audit record. A later L2 run requires a header whose `ETL_LAYER` is `L2`; the attached PB schema cannot safely separate L1 and L2 rows under one header group.

## 11. Databricks Free Edition limitations

Free Edition generally provides serverless compute and SQL warehouses but not the full production feature set. Do not depend on classic clusters, instance pools, cluster policies, private networking, secrets-backed external locations, Unity Catalog administration APIs, DLT/Lakeflow advanced features, continuous streaming, or enterprise alert integrations unless the tenant visibly supports them. Use serverless Jobs/SQL, Delta tables, workspace Git folders, and explicit job parameters. GitHub raw URLs are suitable for a demo but are not a production ingestion boundary.

Free Edition also does not solve missing metadata semantics. The existing tables lack a dedicated watermark, PB layer discriminator, retry columns, run ID, and explicit DQ rule type. The implementation uses audit `LOAD_TS` plus `CDC_LOGIC` as a constrained watermark convention and rejects ambiguous metadata.

## 12. Production upgrade path

Add, without changing the meaning of existing columns, a metadata migration for a stable task key, PB `ETL_LAYER`, source watermark column/type, retry policy, expected schema, DQ rule type/severity, and SCD effective dates. Add a run header/detail audit model with run ID and attempt number. Replace raw URL ingestion with governed external locations/volumes, secrets, schema evolution policy, quarantine, expectations, lineage, alerts, and managed orchestration. Enable a supported DLT/Lakeflow path only after the metadata has explicit streaming and refresh semantics. Reconcile jobs rather than delete/recreate them, and version the framework wheel using the existing `MIN_VERSION`/`MAX_VERSION` fields.
