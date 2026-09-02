# CONFIGURATION_GUIDE.md
## Metadata Table Configuration Reference

**Last Updated**: 2026-09-02 | **Version**: 1.0

This guide details all columns in the Databricks metadata tables and how to configure them.

---

## 📋 Table of Contents

1. [data_flow_control_header](#data_flow_control_header)
2. [data_flow_l0_detail](#data_flow_l0_detail)
3. [data_flow_pb_detail](#data_flow_pb_detail)
4. [audit_log](#audit_log)
5. [Configuration Examples](#configuration-examples)
6. [Common Patterns](#common-patterns)

---

## data_flow_control_header

**Purpose**: Master configuration for each pipeline group  
**Catalog**: `demo_catalog.admin`  
**Table**: `data_flow_control_header`  
**Rows**: One per pipeline group (e.g., one for EMPLOYEE_MASTER_L0)

### Column Reference

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `DATA_FLOW_GROUP_ID` | STRING | ✗ No | Unique pipeline identifier. Format: `{AREA}_{TYPE}_{LAYER}`. Examples: `EMPLOYEE_MASTER_L0`, `SALES_DAILY_L1`, `FINANCE_QUARTERLY_L2` |
| `TRIGGER_TYPE` | STRING | ✓ Yes | How pipeline is triggered: `DLT` (Delta Live Tables) or `JOB` (Databricks Jobs). Default: `JOB` |
| `ETL_LAYER` | STRING | ✓ Yes | Layer: `L0` (landing), `L1` (transformation), `L2` (aggregation) |
| `COMPUTE_CLASS_DEV` | STRING | ✓ Yes | Compute for dev environment: `serverless`, `all_purpose_cluster`, `job_cluster`. Free tier: `serverless` |
| `COMPUTE_CLASS` | STRING | ✓ Yes | Compute for qa/prod: `serverless`, `all_purpose_cluster`, `job_cluster`. Free tier: `serverless` |
| `IS_ACTIVE` | STRING | ✓ Yes | Enable/disable pipeline: `Y` (active) or `N` (disabled). Default: `Y` |
| `INSERTED_BY` | STRING | ✓ Yes | User who inserted record. Example: `admin`, `john.doe@company.com` |
| `UPDATED_BY` | STRING | ✓ Yes | User who last updated record |
| `INSERTED_TS` | TIMESTAMP | ✓ Yes | When record was inserted. Use: `CURRENT_TIMESTAMP` |
| `UPDATED_TS` | TIMESTAMP | ✓ Yes | When record was last updated. Use: `CURRENT_TIMESTAMP` |
| `BUSINESS_OBJECT_NAME` | STRING | ✓ Yes | Human-readable data product name. Example: `Employee Master Data`, `Daily Sales Transactions` |
| `COST_CENTER` | STRING | ✓ Yes | Cost center for billing. Example: `HR`, `SALES`, `FINANCE` |
| `DATA_SME` | STRING | ✓ Yes | Subject Matter Expert name. Example: `John Smith`, `Analytics Team` |
| `BUSINESS_UNIT` | STRING | ✓ Yes | Business unit. Example: `Human Resources`, `Sales Operations` |
| `PRODUCT_OWNER` | STRING | ✓ Yes | Data product owner. Example: `Jane Wilson` |
| `INGESTION_MODE` | STRING | ✓ Yes | How data is ingested: `github`, `s3`, `adls`, `http`, `sftp`. For L0 only |
| `INGESTION_BUCKET` | STRING | ✓ Yes | Ingestion bucket category: `raw_data`, `staging`, `shared`. For L0 only |
| `SPARK_CONFIGS` | STRING | ✓ Yes | Spark configuration JSON. Example: `{"spark.sql.shuffle.partitions": "200"}` |
| `WARNING_THRESHOLD_MINS` | INT | ✓ Yes | Alert threshold in minutes. Example: `30` = alert if > 30 min runtime |
| `WARNING_DL_GROUP` | STRING | ✓ Yes | Distribution list for alerts. Example: `data-team@company.com` |
| `MIN_VERSION` | STRING | ✓ Yes | Minimum code version. Example: `1.0.0` |
| `MAX_VERSION` | STRING | ✓ Yes | Maximum code version. Example: `2.0.0` |
| `target_catalog` | STRING | ✓ Yes | Target catalog for output. Default: `demo_catalog`. Can be overridden per config |

### Example Insertion

```sql
INSERT INTO demo_catalog.admin.data_flow_control_header (
  DATA_FLOW_GROUP_ID,
  TRIGGER_TYPE,
  ETL_LAYER,
  COMPUTE_CLASS_DEV,
  COMPUTE_CLASS,
  IS_ACTIVE,
  BUSINESS_OBJECT_NAME,
  COST_CENTER,
  DATA_SME,
  BUSINESS_UNIT,
  PRODUCT_OWNER,
  INGESTION_MODE,
  INGESTION_BUCKET,
  INSERTED_BY,
  UPDATED_BY,
  INSERTED_TS,
  UPDATED_TS,
  target_catalog
) VALUES (
  'EMPLOYEE_MASTER_L0',
  'JOB',
  'L0',
  'serverless',
  'serverless',
  'Y',
  'Employee Master Data',
  'HR',
  'John Smith (HR Data Team)',
  'Human Resources',
  'Jane Wilson',
  'github',
  'raw_data',
  'admin',
  'admin',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'demo_catalog'
);
```

---

## data_flow_l0_detail

**Purpose**: Configuration for L0 ingestion (source → bronze)  
**Catalog**: `demo_catalog.admin`  
**Table**: `data_flow_l0_detail`  
**Rows**: One per source/table to ingest. Multiple rows can share same `DATA_FLOW_GROUP_ID`

### Column Reference

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `DATA_FLOW_GROUP_ID` | STRING | ✗ No | FK to `data_flow_control_header`. Example: `EMPLOYEE_MASTER_L0` |
| `SOURCE` | STRING | ✓ Yes | Source URL or path: HTTP/HTTPS, S3, ADLS, DBFS, UC Volumes. Example: `https://raw.github.com/.../data.csv` or `s3://my-bucket/raw/employees.parquet` |
| `SOURCE_OBJ_SCHEMA` | STRING | ✓ Yes | Source schema/folder name. Example: `raw`, `landing`, `staging`. Not used for HTTP, informational only |
| `SOURCE_OBJ_NAME` | STRING | ✓ Yes | Source object name. Example: `employee_master_data.csv`, `sales_transactions.json`. Used in target table naming |
| `LOB` | STRING | ✓ Yes | Line of Business filter. Example: `HR`, `SALES`, `FINANCE`. Used to filter configs |
| `LOAD_TYPE` | STRING | ✓ Yes | Load strategy: `FULL` (overwrite, default) or `DELTA` (append/merge). Default: `FULL` |
| `INPUT_FILE_FORMAT` | STRING | ✓ Yes | Source file format: `csv`, `json`, `parquet`, `excel`, `avro`, `orc`, `delta`, `xml`. Default: `csv` |
| `STORAGE_TYPE` | STRING | ✓ Yes | Storage location type: `https`, `s3`, `adls`, `dbfs`, `volumes`. Auto-detected from SOURCE URL |
| `DQ_LOGIC` | STRING | ✓ Yes | Data quality rules as JSON. Example: `{"null_check": ["col1", "col2"], "unique_check": ["id"]}` |
| `DELIMETER` | STRING | ✓ Yes | CSV field delimiter. Default: `,`. Other: `;`, `\|`, `\t` (tab) |
| `CUSTOM_SCHEMA` | STRING | ✓ Yes | Custom schema override for specific formats. JSON with column types. Example: `{"id": "STRING", "amount": "DECIMAL(10,2)"}` |
| `CDC_LOGIC` | STRING | ✓ Yes | Change Data Capture logic for DELTA loads. JSON with CDC rules. Example: `{"key_column": "id", "timestamp_column": "updated_at"}` |
| `TRANSFORM_QUERY` | STRING | ✓ Yes | Pre-ingestion transformation SQL. Applied before writing to bronze. Example: `SELECT * EXCEPT(internal_col) FROM {source}` |
| `PRESTAG_FLAG` | STRING | ✓ Yes | Pre-staging flag: `Y` (create streaming table) or `N` (external/standard). Default: `N` |
| `PARTITION` | STRING | ✓ Yes | Partition columns (comma-separated). Example: `year,month,day` or `customer_id,date`. Improves performance for large tables |
| `LS_FLAG` | STRING | ✓ Yes | Lift & Shift flag: `Y` (legacy system migration) or `N` (standard). Default: `N` |
| `LS_DETAIL` | STRING | ✓ Yes | Lift & Shift details. Example: `Migrating from Legacy DB System` |
| `IS_ACTIVE` | STRING | ✓ Yes | Enable/disable: `Y` (process) or `N` (skip). Default: `Y`. Use `N` instead of deleting |
| `INSERTED_BY` | STRING | ✓ Yes | User who inserted record |
| `UPDATED_BY` | STRING | ✓ Yes | User who last updated record |
| `INSERTED_TS` | TIMESTAMP | ✓ Yes | When record was inserted |
| `UPDATED_TS` | TIMESTAMP | ✓ Yes | When record was last updated |
| `DEPLOYMENT_SOURCE_DFG` | STRING | ✓ Yes | Reference to deployment source group (for tracking lineage) |

### Common Column Combinations

**HTTP CSV Source**:
```json
{
  "SOURCE": "https://example.com/data.csv",
  "INPUT_FILE_FORMAT": "csv",
  "DELIMETER": ",",
  "LOAD_TYPE": "FULL"
}
```

**S3 Parquet Source**:
```json
{
  "SOURCE": "s3://my-bucket/parquet-files/data.parquet",
  "INPUT_FILE_FORMAT": "parquet",
  "LOAD_TYPE": "DELTA",
  "PARTITION": "year,month"
}
```

**With DQ Checks**:
```json
{
  "DQ_LOGIC": "{\"null_check\": [\"id\", \"name\"], \"unique_check\": [\"email\"]}",
  "TRANSFORM_QUERY": "SELECT * WHERE is_valid = 'Y'"
}
```

### Example Insertion

```sql
INSERT INTO demo_catalog.admin.data_flow_l0_detail (
  DATA_FLOW_GROUP_ID,
  SOURCE,
  SOURCE_OBJ_SCHEMA,
  SOURCE_OBJ_NAME,
  LOB,
  LOAD_TYPE,
  INPUT_FILE_FORMAT,
  STORAGE_TYPE,
  DQ_LOGIC,
  DELIMETER,
  CUSTOM_SCHEMA,
  CDC_LOGIC,
  TRANSFORM_QUERY,
  PRESTAG_FLAG,
  PARTITION,
  LS_FLAG,
  LS_DETAIL,
  IS_ACTIVE,
  INSERTED_BY,
  UPDATED_BY,
  INSERTED_TS,
  UPDATED_TS,
  DEPLOYMENT_SOURCE_DFG
) VALUES (
  'EMPLOYEE_MASTER_L0',
  'https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/main/employee_master_data.csv',
  'raw',
  'employee_master_data.csv',
  'HR',
  'FULL',
  'csv',
  'https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/main',
  '{\"null_check\": [\"employee_id\", \"employee_name\"]}',
  ',',
  NULL,
  NULL,
  NULL,
  'N',
  NULL,
  'N',
  NULL,
  'Y',
  'admin',
  'admin',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  NULL
);
```

---

## data_flow_pb_detail

**Purpose**: Configuration for L1 transformation (bronze → silver)  
**Catalog**: `demo_catalog.admin`  
**Table**: `data_flow_pb_detail`  
**Rows**: One per transformation/output table

### Column Reference

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `DATA_FLOW_GROUP_ID` | STRING | ✗ No | FK to header. Example: `EMPLOYEE_MASTER_L1` |
| `LOB` | STRING | ✓ Yes | Line of Business. Example: `HR`, `SALES` |
| `SOURCE` | STRING | ✓ Yes | Source schema. Example: `bronze`, `silver`. Must exist before transformation |
| `TARGET_OBJ_SCHEMA` | STRING | ✓ Yes | Target schema for output. Example: `silver`, `gold`. Default: `silver` |
| `TARGET_OBJ_NAME` | STRING | ✓ Yes | Target table name. Example: `dim_employee_silver`, `fact_sales`. Naming: `dim_*` for dimensions, `fact_*` for facts |
| `PRIORITY` | INT | ✓ Yes | Execution order (1 = first, 2 = second, etc.). Same priority runs parallel. Default: `1` |
| `TARGET_OBJ_TYPE` | STRING | ✓ Yes | Object type: `Table` (default) or `MV` (Materialized View, not in free tier). Default: `Table` |
| `TRANSFORM_QUERY` | STRING | ✗ No | SQL SELECT query for transformation. Must reference tables in `SOURCE` schema. Example: `SELECT * FROM demo_catalog.bronze.{table} WHERE ...` |
| `GENERIC_SCRIPTS` | STRING | ✓ Yes | Name of PySpark scripts if using custom Python. Comma-separated. Example: `script1.py,script2.py` |
| `SOURCE_PK` | STRING | ✓ Yes | Source primary key columns (comma-separated). Example: `employee_id` or `order_id,line_item` |
| `TARGET_PK` | STRING | ✓ Yes | Target primary key columns. Example: `employee_id`. Used for SCD and uniqueness |
| `LOAD_TYPE` | STRING | ✓ Yes | Load strategy: `FULL` (overwrite, default), `DELTA` (append), `SCD` (Slowly Changing Dimension). Default: `FULL` |
| `IS_ACTIVE` | STRING | ✓ Yes | Enable/disable: `Y` (process) or `N` (skip). Default: `Y` |
| `LS_FLAG` | STRING | ✓ Yes | Lift & Shift flag: `Y` or `N`. Default: `N` |
| `LS_DETAIL` | STRING | ✓ Yes | Lift & Shift details |
| `PARTITION_OR_INDEX` | STRING | ✓ Yes | Partition or liquid cluster columns (comma-separated). Example: `year,month,day` or `customer_id`. Improves query performance |
| `INSERTED_BY` | STRING | ✓ Yes | User who inserted record |
| `UPDATED_BY` | STRING | ✓ Yes | User who last updated record |
| `INSERTED_TS` | TIMESTAMP | ✓ Yes | When record was inserted |
| `UPDATED_TS` | TIMESTAMP | ✓ Yes | When record was last updated |
| `CUSTOM_SCRIPT_PARAMS` | MAP<STRING,STRING> | ✓ Yes | Parameters for custom scripts. Example: `MAP('threshold', '100000', 'days', '90')` |
| `PARTITION_METHOD` | STRING | ✓ Yes | Partitioning method: `PARTITION` (traditional) or `LIQUID_CLUSTER` (UC only). Default: `PARTITION` |
| `RETENTION_DETAILS` | STRING | ✓ Yes | Table retention policy. Example: `RETENTION 365 DAYS`. Databricks auto-cleans old snapshots |
| `DEPLOYMENT_SOURCE_DFG` | STRING | ✓ Yes | Reference to deployment source group |

### Example: Basic Transformation

```sql
INSERT INTO demo_catalog.admin.data_flow_pb_detail (
  DATA_FLOW_GROUP_ID,
  LOB,
  SOURCE,
  TARGET_OBJ_SCHEMA,
  TARGET_OBJ_NAME,
  PRIORITY,
  TARGET_OBJ_TYPE,
  TRANSFORM_QUERY,
  SOURCE_PK,
  TARGET_PK,
  LOAD_TYPE,
  IS_ACTIVE,
  INSERTED_BY,
  UPDATED_BY,
  INSERTED_TS,
  UPDATED_TS,
  PARTITION_METHOD
) VALUES (
  'EMPLOYEE_MASTER_L1',
  'HR',
  'bronze',
  'silver',
  'dim_employee_silver',
  1,
  'Table',
  'SELECT
    CAST(employee_id AS STRING) AS employee_id,
    TRIM(employee_name) AS employee_name,
    UPPER(department) AS department,
    CAST(salary AS DECIMAL(10,2)) AS salary,
    current_timestamp() AS transformation_timestamp
   FROM demo_catalog.bronze.employee_master_data
   WHERE employee_id IS NOT NULL
     AND employee_name IS NOT NULL',
  'employee_id',
  'employee_id',
  'FULL',
  'Y',
  'admin',
  'admin',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'PARTITION'
);
```

### Example: Multi-Priority Pipeline

```sql
-- Priority 1: Load dimension tables
INSERT INTO demo_catalog.admin.data_flow_pb_detail (...) VALUES (
  'SALES_ANALYSIS_L1', 'SALES', 'bronze', 'silver', 'dim_customer_silver', 1, 'Table',
  'SELECT ... FROM demo_catalog.bronze.customer', 'customer_id', 'customer_id', 'FULL', 'Y',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'PARTITION'
);

INSERT INTO demo_catalog.admin.data_flow_pb_detail (...) VALUES (
  'SALES_ANALYSIS_L1', 'SALES', 'bronze', 'silver', 'dim_product_silver', 1, 'Table',
  'SELECT ... FROM demo_catalog.bronze.product', 'product_id', 'product_id', 'FULL', 'Y',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'PARTITION'
);

-- Priority 2: Load fact table (depends on dimensions)
INSERT INTO demo_catalog.admin.data_flow_pb_detail (...) VALUES (
  'SALES_ANALYSIS_L1', 'SALES', 'bronze', 'silver', 'fact_sales_silver', 2, 'Table',
  'SELECT
    s.sale_id,
    c.customer_id,
    p.product_id,
    s.amount,
    s.sale_date
   FROM demo_catalog.bronze.sales s
   LEFT JOIN demo_catalog.silver.dim_customer_silver c ON s.customer_id = c.customer_id
   LEFT JOIN demo_catalog.silver.dim_product_silver p ON s.product_id = p.product_id',
  'sale_id', 'sale_id', 'FULL', 'Y',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'PARTITION'
);
```

---

## audit_log

**Purpose**: Execution audit trail for all L0 and L1 runs  
**Catalog**: `demo_catalog.admin`  
**Table**: `audit_log`  
**Rows**: Auto-populated by L0 and L1 notebooks. One per table load/transformation

### Column Reference

| Column | Type | Description |
|--------|------|-------------|
| `DATA_FLOW_GROUP_ID` | STRING | Pipeline group that ran. Example: `EMPLOYEE_MASTER_L0` |
| `TARGET_TABLE` | STRING | Target table name that was loaded/transformed |
| `STATUS` | STRING | Execution result: `SUCCESS`, `FAILED`, `RUNNING` |
| `MESSAGE` | STRING | Error message (if failed) or execution summary |
| `CREATED_DATE` | TIMESTAMP | When this audit entry was created |
| `ETL_LAYER` | STRING | Layer that ran: `L0` or `L1` |
| `ROWS_PROCESSED` | BIGINT | Number of rows loaded or transformed |
| `START_TIME` | TIMESTAMP | When execution started |
| `END_TIME` | TIMESTAMP | When execution completed |
| `LOAD_TS` | TIMESTAMP | Timestamp metadata column |

### Querying Audit Log

**Recent failures**:
```sql
SELECT * FROM demo_catalog.admin.audit_log
WHERE STATUS = 'FAILED'
  AND DATE(CREATED_DATE) >= CURRENT_DATE - INTERVAL 7 DAY
ORDER BY CREATED_DATE DESC;
```

**Daily execution summary**:
```sql
SELECT 
  DATA_FLOW_GROUP_ID,
  ETL_LAYER,
  STATUS,
  COUNT(*) AS run_count,
  SUM(ROWS_PROCESSED) AS total_rows,
  MIN(START_TIME) AS earliest_start,
  MAX(END_TIME) AS latest_end
FROM demo_catalog.admin.audit_log
WHERE DATE(CREATED_DATE) = CURRENT_DATE
GROUP BY DATA_FLOW_GROUP_ID, ETL_LAYER, STATUS
ORDER BY DATA_FLOW_GROUP_ID;
```

**Performance report**:
```sql
SELECT 
  DATA_FLOW_GROUP_ID,
  TARGET_TABLE,
  COUNT(*) AS runs,
  AVG(DATEDIFF(SECOND, START_TIME, END_TIME)) AS avg_duration_secs,
  AVG(ROWS_PROCESSED) AS avg_rows,
  MAX(CREATED_DATE) AS last_run
FROM demo_catalog.admin.audit_log
WHERE CREATED_DATE >= CURRENT_TIMESTAMP - INTERVAL 30 DAY
GROUP BY DATA_FLOW_GROUP_ID, TARGET_TABLE
ORDER BY avg_duration_secs DESC;
```

---

## Configuration Examples

### Example 1: Simple CSV Ingestion + Transformation

**Scenario**: Load employee CSV from GitHub, clean it in silver layer

**L0 Config** (Insert into `data_flow_l0_detail`):
```sql
INSERT INTO demo_catalog.admin.data_flow_l0_detail VALUES (
  'EMPLOYEE_MASTER_L0',
  'https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/main/employee_master.csv',
  'raw',
  'employee_master.csv',
  'HR',
  'FULL',
  'csv',
  'https://raw.github.com',
  '{"null_check": ["employee_id", "employee_name"]}',
  ',',
  NULL, NULL, NULL, 'N', NULL, 'N', NULL, 'Y',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL
);
```

**L1 Config** (Insert into `data_flow_pb_detail`):
```sql
INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'EMPLOYEE_MASTER_L1', 'HR', 'bronze', 'silver', 'dim_employee_silver', 1, 'Table',
  'SELECT
    CAST(employee_id AS STRING) AS employee_id,
    TRIM(employee_name) AS employee_name,
    UPPER(department) AS department,
    CAST(salary AS DECIMAL(10,2)) AS salary,
    current_timestamp() AS loaded_at
   FROM demo_catalog.bronze.employee_master
   WHERE employee_id IS NOT NULL',
  NULL, 'employee_id', 'FULL', 'Y', 'N', NULL, NULL,
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
  NULL, NULL, NULL, NULL
);
```

### Example 2: Multi-Source, Multi-Priority Pipeline

**Scenario**: Load customer, product, and sales data with dependencies

**L0 Configs** (3 sources):
```sql
-- Customer data
INSERT INTO demo_catalog.admin.data_flow_l0_detail VALUES (
  'SALES_DAILY_L0', 's3://data-bucket/customer.parquet', 'raw', 'customer.parquet',
  'SALES', 'FULL', 'parquet', 's3://data-bucket', NULL, NULL, NULL, NULL, NULL, 'N',
  'customer_id,date', 'N', NULL, 'Y', 'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL
);

-- Product data
INSERT INTO demo_catalog.admin.data_flow_l0_detail VALUES (
  'SALES_DAILY_L0', 's3://data-bucket/product.parquet', 'raw', 'product.parquet',
  'SALES', 'FULL', 'parquet', 's3://data-bucket', NULL, NULL, NULL, NULL, NULL, 'N',
  'product_id', 'N', NULL, 'Y', 'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL
);

-- Sales transactions
INSERT INTO demo_catalog.admin.data_flow_l0_detail VALUES (
  'SALES_DAILY_L0', 's3://data-bucket/sales.parquet', 'raw', 'sales.parquet',
  'SALES', 'DELTA', 'parquet', 's3://data-bucket', NULL, NULL, NULL, NULL, NULL, 'N',
  'date', 'N', NULL, 'Y', 'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL
);
```

**L1 Configs** (with priorities):
```sql
-- Priority 1: Dimensions
INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'SALES_DAILY_L1', 'SALES', 'bronze', 'silver', 'dim_customer_silver', 1, 'Table',
  'SELECT * FROM demo_catalog.bronze.customer', NULL, 'customer_id', 'FULL', 'Y', 'N', NULL, 'customer_id',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'PARTITION', NULL, NULL
);

INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'SALES_DAILY_L1', 'SALES', 'bronze', 'silver', 'dim_product_silver', 1, 'Table',
  'SELECT * FROM demo_catalog.bronze.product', NULL, 'product_id', 'FULL', 'Y', 'N', NULL, 'product_id',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'PARTITION', NULL, NULL
);

-- Priority 2: Facts (depends on dimensions)
INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'SALES_DAILY_L1', 'SALES', 'bronze', 'silver', 'fact_sales_silver', 2, 'Table',
  'SELECT
    s.sale_id,
    s.customer_id,
    s.product_id,
    s.amount,
    s.sale_date
   FROM demo_catalog.bronze.sales s
   INNER JOIN demo_catalog.silver.dim_customer_silver c ON s.customer_id = c.customer_id
   INNER JOIN demo_catalog.silver.dim_product_silver p ON s.product_id = p.product_id',
  'sale_id', 'sale_id', 'DELTA', 'Y', 'N', NULL, 'sale_date',
  'admin', 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'PARTITION', NULL, NULL
);
```

---

## Common Patterns

### Pattern 1: Incremental Load (DELTA)

**Use when**: Data is appended daily, never updated

**L0 Config**:
```json
{
  "LOAD_TYPE": "DELTA",
  "PARTITION": "date"
}
```

**L1 Config**:
```json
{
  "LOAD_TYPE": "DELTA",
  "PARTITION_OR_INDEX": "date"
}
```

### Pattern 2: Full Refresh (FULL)

**Use when**: Data is completely rebuilt each run

**L0 Config**:
```json
{
  "LOAD_TYPE": "FULL"
}
```

**L1 Config**:
```json
{
  "LOAD_TYPE": "FULL"
}
```

### Pattern 3: Slowly Changing Dimension (SCD Type 2)

**Use when**: You need to track historical changes (rare, complex)

**L1 Config**:
```json
{
  "LOAD_TYPE": "SCD",
  "TARGET_PK": "customer_id",
  "SOURCE_PK": "customer_id",
  "TRANSFORM_QUERY": "SELECT ... WITH change tracking ..."
}
```

### Pattern 4: Data Quality Checks

**L0 Config**:
```json
{
  "DQ_LOGIC": "{\"null_check\": [\"id\", \"name\"], \"unique_check\": [\"email\"]}"
}
```

### Pattern 5: Partitioned Tables (Large Datasets)

**L0 Config**:
```json
{
  "PARTITION": "year,month,day"
}
```

**L1 Config**:
```json
{
  "PARTITION_OR_INDEX": "year,month,day",
  "PARTITION_METHOD": "PARTITION"
}
```

### Pattern 6: Multiple Configurations per Group

**Scenario**: Load 3 different customer datasets in one pipeline group

```sql
INSERT INTO ... VALUES ('CUSTOMER_MASTER_L0', 'https://source1.com/csv', ...);
INSERT INTO ... VALUES ('CUSTOMER_MASTER_L0', 'https://source2.com/csv', ...);
INSERT INTO ... VALUES ('CUSTOMER_MASTER_L0', 's3://source3/csv', ...);

-- When you run with DATA_FLOW_GROUP_ID='CUSTOMER_MASTER_L0',
-- all 3 sources are loaded into bronze tables
```

---

## Tips & Tricks

### ✓ DO

- Use `CURRENT_TIMESTAMP` for timestamp columns
- Always specify `IS_ACTIVE = 'Y'` (or configs won't run!)
- Use meaningful table names (`dim_` for dimensions, `fact_` for facts)
- Document complex `TRANSFORM_QUERY` with comments
- Version your configurations in Git/GitHub
- Test queries in SQL editor before adding to config

### ✗ DON'T

- Use SELECT * (be explicit with column names)
- Hardcode dates (use CURRENT_DATE, functions instead)
- Mix L0 and L1 logic (keep layers separate)
- Leave `DELIMETER` null for CSV files
- Use  `IS_ACTIVE = 'N'` permanently (delete record instead if truly unwanted)
- Make changes without testing in dev first

---

**Configuration complete! Ready to ingest and transform data! 🚀**
