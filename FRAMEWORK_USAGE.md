# Framework Usage Guide - Columns & Processing

## 📋 Overview

This framework uses **metadata tables** to drive ETL pipelines. Every configuration is stored in Databricks tables, not in code. Jenkins reads these tables and dynamically creates Databricks Jobs or DLT Pipelines.

---

## 🎯 How It Works (Processing Flow)

```
Jenkins triggers with GROUP_ID
     ↓
Stage 1: Validate GROUP_ID format
     ↓
Stage 2: Checkout GitHub repo
     ↓
Stage 3: Load workspace registry from workspaces.json
     ↓
Stage 4: Sync Git to Databricks Repos
     ↓
Stage 5a: Read TRIGGER_TYPE from control_header table
     ├─ Unique key: DATA_FLOW_GROUP_ID
     └─ Returns: 'JOB' or 'DLT'
     ↓
Stage 5b: Read configuration from L0/L1 detail tables
     ├─ L0: data_flow_l0_detail (if layer=L0)
     └─ L1: data_flow_pb_detail (if layer=L1)
     ↓
Stage 5c: Create Job OR DLT Pipeline
     ├─ If TRIGGER_TYPE='JOB' → Create Databricks Job
     └─ If TRIGGER_TYPE='DLT' → Create DLT Pipeline
     ↓
Stage 6: Trigger execution
     ├─ Job: Run with job_id
     └─ DLT: Start update with pipeline_id
     ↓
Stage 7: Wait for completion
     ├─ Poll job runs OR
     └─ Poll DLT updates
     ↓
Stage 8: Report results
```

---

## 📊 Metadata Tables

### **1. data_flow_control_header** (MASTER CONFIG)

**Purpose**: Master configuration per DATA_FLOW_GROUP_ID. One row per pipeline group.

**Unique Key**: `DATA_FLOW_GROUP_ID`

| Column | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `DATA_FLOW_GROUP_ID` | STRING | ✅ YES | Pipeline identifier (unique) | `SALES_L0`, `INVENTORY_L1` |
| `TRIGGER_TYPE` | STRING | ✅ YES | 'JOB' or 'DLT' | `JOB` |
| `ETL_LAYER` | STRING | ✅ YES | L0, L1, or L2 | `L0` |
| `IS_ACTIVE` | STRING | ✅ YES | 'Y' or 'N' to enable/disable | `Y` |
| `INSERTED_BY` | STRING | Optional | Who created this row | `admin` |
| `UPDATED_TS` | TIMESTAMP | Auto | Last update time | `CURRENT_TIMESTAMP()` |

**Example - Job-Based Pipeline:**
```sql
INSERT INTO demo_catalog.admin.data_flow_control_header VALUES (
  'SALES_L0',      -- DATA_FLOW_GROUP_ID (unique identifier)
  'JOB',           -- TRIGGER_TYPE = JOB (will create Databricks Job)
  'L0',            -- ETL_LAYER = L0
  'Y',             -- IS_ACTIVE = Y (process this)
  'admin',
  CURRENT_TIMESTAMP()
);
```

**Example - DLT-Based Pipeline:**
```sql
INSERT INTO demo_catalog.admin.data_flow_control_header VALUES (
  'INVENTORY_L1',  -- DATA_FLOW_GROUP_ID (unique identifier)
  'DLT',           -- TRIGGER_TYPE = DLT (will create DLT Pipeline)
  'L1',            -- ETL_LAYER = L1
  'Y',             -- IS_ACTIVE = Y (process this)
  'admin',
  CURRENT_TIMESTAMP()
);
```

---

### **2. data_flow_l0_detail** (L0 INGESTION CONFIG)

**Purpose**: Data source configuration for L0 layer. Multiple rows per GROUP_ID allowed.

**Unique Key**: `DATA_FLOW_GROUP_ID` + `SOURCE_OBJ_NAME` (composite)

| Column | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `DATA_FLOW_GROUP_ID` | STRING | ✅ YES | Link to control_header | `SALES_L0` |
| `SOURCE` | STRING | ✅ YES | Data source URL | `https://github.com/data.csv` or `s3://bucket/file.parquet` |
| `SOURCE_OBJ_NAME` | STRING | ✅ YES | File name | `sales.csv` |
| `INPUT_FILE_FORMAT` | STRING | ✅ YES | Format type | `csv`, `json`, `parquet`, `excel` |
| `LOAD_TYPE` | STRING | ✅ YES | 'FULL' or 'DELTA' | `FULL` |
| `DELIMITER` | STRING | Optional | For CSV only | `,` |
| `DQ_LOGIC` | STRING | Optional | Data quality rules (JSON) | `{"field":"id","check":"NOT NULL"}` |
| `PARTITION` | STRING | Optional | Partition columns | `load_date` |
| `IS_ACTIVE` | STRING | ✅ YES | 'Y' or 'N' | `Y` |
| `PRIORITY` | INTEGER | Optional | Execution order | `1` |

**Example:**
```sql
INSERT INTO demo_catalog.admin.data_flow_l0_detail VALUES (
  'SALES_L0',              -- DATA_FLOW_GROUP_ID
  'https://github.com/.../sales.csv',  -- SOURCE URL
  'sales.csv',             -- SOURCE_OBJ_NAME
  'csv',                   -- INPUT_FILE_FORMAT
  'FULL',                  -- LOAD_TYPE (overwrite)
  ',',                     -- DELIMITER (for CSV)
  NULL,                    -- DQ_LOGIC (optional)
  'load_date',             -- PARTITION column
  'Y',                     -- IS_ACTIVE
  1,                       -- PRIORITY
  'admin', 'admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

**Processing in L0 notebook:**
1. Reads all rows where `DATA_FLOW_GROUP_ID = 'SALES_L0'` AND `IS_ACTIVE = 'Y'`
2. For each row, downloads from SOURCE URL
3. Detects schema (or uses custom schema)
4. Applies DQ checks (validates NOT NULL fields)
5. Writes to Bronze table (demo_catalog.bronze.sales)
6. Logs execution to audit_log table

---

### **3. data_flow_pb_detail** (L1 TRANSFORMATION CONFIG)

**Purpose**: Transformation logic for L1 layer. Multiple rows per GROUP_ID allowed.

**Unique Key**: `DATA_FLOW_GROUP_ID` + `TARGET_OBJ_NAME` (composite)

| Column | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `DATA_FLOW_GROUP_ID` | STRING | ✅ YES | Link to control_header | `SALES_L1` |
| `TARGET_OBJ_NAME` | STRING | ✅ YES | Output table name | `dim_sales_silver` |
| `TARGET_OBJ_SCHEMA` | STRING | Optional | Schema name | `silver` |
| `TRANSFORM_QUERY` | STRING | ✅ YES | SQL SELECT statement | `SELECT CAST(id AS STRING) AS id...` |
| `LOAD_TYPE` | STRING | ✅ YES | 'FULL', 'DELTA', or 'SCD' | `FULL` |
| `SOURCE_PK` | STRING | Optional | Primary key in source | `id` |
| `TARGET_PK` | STRING | Optional | Primary key in target | `id` |
| `PRIORITY` | INTEGER | Optional | Execution order | `1` |
| `IS_ACTIVE` | STRING | ✅ YES | 'Y' or 'N' | `Y` |
| `TARGET_OBJ_TYPE` | STRING | Optional | 'TABLE' or 'VIEW' | `TABLE` |

**Example - FULL Load (Overwrite):**
```sql
INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'SALES_L1',              -- DATA_FLOW_GROUP_ID
  'dim_sales_silver',      -- TARGET_OBJ_NAME (output table)
  'silver',                -- TARGET_OBJ_SCHEMA
  'SELECT 
     CAST(id AS STRING) AS id,
     TRIM(name) AS name,
     UPPER(region) AS region,
     CAST(amount AS DECIMAL(10,2)) AS amount
   FROM demo_catalog.bronze.sales
   WHERE id IS NOT NULL',  -- TRANSFORM_QUERY
  'FULL',                  -- LOAD_TYPE (overwrite each time)
  'id',                    -- SOURCE_PK
  'id',                    -- TARGET_PK
  1,                       -- PRIORITY
  'Y',                     -- IS_ACTIVE
  'TABLE',                 -- TARGET_OBJ_TYPE
  'admin', 'admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

**Example - DELTA Load (Incremental):**
```sql
INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'SALES_L1',              -- DATA_FLOW_GROUP_ID
  'fact_sales_silver',     -- TARGET_OBJ_NAME
  'silver',
  'SELECT 
     CAST(id AS STRING) AS id,
     CAST(sales_date AS DATE) AS sales_date,
     CAST(amount AS DECIMAL(10,2)) AS amount
   FROM demo_catalog.bronze.sales
   WHERE id IS NOT NULL',
  'DELTA',                 -- LOAD_TYPE (append new rows)
  'id',
  'id',
  2,                       -- PRIORITY 2 (runs after priority 1)
  'Y',
  'TABLE',
  'admin', 'admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

**Processing in L1 notebook:**
1. Reads all rows where `DATA_FLOW_GROUP_ID = 'SALES_L1'` AND `IS_ACTIVE = 'Y'`
2. Orders by PRIORITY (1, 2, 3...)
3. For each row:
   - Executes TRANSFORM_QUERY
   - If LOAD_TYPE='FULL': Overwrites target table
   - If LOAD_TYPE='DELTA': Appends to target table
   - If LOAD_TYPE='SCD': Updates with Type 2 history
4. Logs execution to audit_log table

---

### **4. audit_log** (AUTO-POPULATED TRACKING)

**Purpose**: Tracks every L0 and L1 execution. Auto-populated by notebooks.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `DATA_FLOW_GROUP_ID` | STRING | Which pipeline group ran | `SALES_L0` |
| `STATUS` | STRING | 'SUCCESS' or 'FAILED' | `SUCCESS` |
| `SOURCE_COUNT` | INTEGER | Rows read from source | `1000` |
| `TARGET_COUNT` | INTEGER | Rows written to target | `950` |
| `ERROR_MESSAGE` | STRING | Error details if failed | `NULL` |
| `RUN_START_TS` | TIMESTAMP | When execution started | `2026-09-02 10:00:00` |
| `RUN_END_TS` | TIMESTAMP | When execution ended | `2026-09-02 10:05:00` |

**No manual inserts needed** - L0 and L1 notebooks auto-populate this.

---

## 🔄 Execution Examples

### **Scenario 1: Run Job-Based Pipeline**

```bash
Jenkins Call:
  GROUP_ID = SALES_L0
  TRIGGER_TYPE (from control_header) = JOB
```

**Jenkins will:**
1. Query data_flow_control_header: `SELECT TRIGGER_TYPE WHERE DATA_FLOW_GROUP_ID='SALES_L0'`
2. Get result: `TRIGGER_TYPE = 'JOB'`
3. Create Databricks Job named `DBX_SALES_L0_JOB`
4. Set notebook path to `L0_DATA_INGESTION` (auto-selected from layer)
5. Trigger the job with `GROUP_ID = SALES_L0`
6. L0_DATA_INGESTION notebook queries `data_flow_l0_detail` for all rows with `DATA_FLOW_GROUP_ID='SALES_L0'` and `IS_ACTIVE='Y'`
7. Ingests data, logs to audit_log

---

### **Scenario 2: Run DLT-Based Pipeline**

```bash
Jenkins Call:
  GROUP_ID = INVENTORY_L1
  TRIGGER_TYPE (from control_header) = DLT
```

**Jenkins will:**
1. Query data_flow_control_header: `SELECT TRIGGER_TYPE WHERE DATA_FLOW_GROUP_ID='INVENTORY_L1'`
2. Get result: `TRIGGER_TYPE = 'DLT'`
3. Create DLT Pipeline named `DBX_INVENTORY_L1_DLT_PIPELINE`
4. Set notebook library to `L1_TRANSFORMATION`
5. Enable continuous mode (auto-triggers on source changes)
6. Trigger pipeline update
7. DLT queries `data_flow_pb_detail` for all rows with `DATA_FLOW_GROUP_ID='INVENTORY_L1'` and `IS_ACTIVE='Y'`
8. Executes transformations, logs to audit_log

---

## ✅ Implementation Checklist

### Step 1: Insert Control Header
```sql
INSERT INTO demo_catalog.admin.data_flow_control_header VALUES (
  'MY_PIPELINE_L0',  -- Unique identifier
  'JOB',             -- Or 'DLT'
  'L0',              -- Layer
  'Y',               -- Active
  'admin',
  CURRENT_TIMESTAMP()
);
```

### Step 2: Insert L0 Configuration (if L0 pipeline)
```sql
INSERT INTO demo_catalog.admin.data_flow_l0_detail VALUES (
  'MY_PIPELINE_L0',
  'https://example.com/data.csv',  -- SOURCE
  'data.csv',
  'csv',
  'FULL',            -- Load type
  ',',               -- CSV delimiter
  NULL,              -- DQ logic
  NULL,              -- Partition
  'Y',               -- Active
  1,                 -- Priority
  'admin', 'admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

### Step 3: Insert L1 Configuration (if L1 pipeline)
```sql
INSERT INTO demo_catalog.admin.data_flow_pb_detail VALUES (
  'MY_PIPELINE_L1',
  'my_table_silver',  -- TARGET
  'silver',
  'SELECT * FROM demo_catalog.bronze.my_table WHERE id IS NOT NULL',
  'FULL',             -- Load type
  'id',
  'id',
  1,                  -- Priority
  'Y',                -- Active
  'TABLE',
  'admin', 'admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

### Step 4: Trigger Jenkins
```
Jenkins Job: DATA_INTEGRATION
Parameter: GROUP_ID = MY_PIPELINE_L0
Click: BUILD
```

### Step 5: Monitor
```sql
SELECT * FROM demo_catalog.admin.audit_log
WHERE DATA_FLOW_GROUP_ID = 'MY_PIPELINE_L0'
ORDER BY RUN_START_TS DESC
LIMIT 10;
```

---

## 🐛 How Framework Uses Each Column

| Column | Used By | Purpose |
|--------|---------|---------|
| `DATA_FLOW_GROUP_ID` | Jenkins + Notebooks | **Unique key** to link all tables |
| `TRIGGER_TYPE` | Jenkins Stage 5a | Decide: Create JOB or DLT |
| `ETL_LAYER` | Jenkins Stage 1 | Detect L0/L1/L2 from GROUP_ID suffix |
| `IS_ACTIVE` | L0/L1 Notebook | Skip if 'N', process if 'Y' |
| `SOURCE` / `TARGET_OBJ_NAME` | L0/L1 Notebook | Read/write source/target |
| `TRANSFORM_QUERY` | L1 Notebook | Execute SQL transformation |
| `LOAD_TYPE` | L0/L1 Notebook | FULL=overwrite, DELTA=append, SCD=history |
| `PRIORITY` | Jenkins + Notebook | Execution order (1, 2, 3...) |
| `PARTITION` | L0 Notebook | Partition table for performance |
| `DQ_LOGIC` | L0 Notebook | Validate data quality |

---

## 📈 Key Benefits

✅ **No Code Changes** - Add new pipelines by inserting rows into tables  
✅ **Dynamic** - Change TRIGGER_TYPE from JOB to DLT without code changes  
✅ **Scalable** - Support multiple pipelines, workspaces, environments  
✅ **Auditable** - Every execution logged in audit_log  
✅ **Maintainable** - All config centralized in tables  

---

**Version**: 1.0.0  
**Last Updated**: 2026-09-02  
**Framework**: DATA_INTEGRATION ETL
