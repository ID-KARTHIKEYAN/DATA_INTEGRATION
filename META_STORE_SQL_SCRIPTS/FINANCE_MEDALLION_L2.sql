-- ============================================================
-- Workspace : MARAN_workspace
-- GROUP_ID  : FINANCE_MEDALLION_L2
-- Layers    : L0(0) L1(0) L2(1)
-- Generated : 2026-07-10 05:01:10
-- Bundle    : [v1.0][untagged] 2026-07-10 05:00 | L2 | main
-- ============================================================

-- control_header
MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (SELECT 'FINANCE_MEDALLION_L2' AS DATA_FLOW_GROUP_ID,'FINANCE_MEDALLION_L2_JOB' AS JOB_NAME,'/Workspace/Repos/ma3488473.com/DATA_INTEGRATION/notebooks/run_framework' AS NOTEBOOK_PATH,'Y' AS IS_ACTIVE,current_timestamp() AS CREATED_DATE,current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID=src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET tgt.NOTEBOOK_PATH=src.NOTEBOOK_PATH,tgt.IS_ACTIVE=src.IS_ACTIVE,tgt.UPDATED_DATE=src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

-- L2 Gold (1 table/s)
MERGE INTO demo_catalog.admin.data_flow_l2_detail AS tgt
USING (SELECT 'FINANCE_MEDALLION_L2' AS DATA_FLOW_GROUP_ID,'silver' AS SOURCE_OBJ_SCHEMA,'titanic_silver' AS SOURCE_OBJ_NAME,'gold' AS TARGET_SCHEMA,'titanic_gold' AS TARGET_TABLE,'FULL' AS LOAD_TYPE,'SELECT PassengerId, Name, sex FROM silver.titanic_silver WHERE sex = "female"' AS TRANSFORMATION_QUERY,'' AS MERGE_KEYS,'sex' AS PARTITION_BY,'Y' AS IS_ACTIVE,current_timestamp() AS CREATED_DATE,current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID=src.DATA_FLOW_GROUP_ID AND tgt.TARGET_TABLE=src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET tgt.TRANSFORMATION_QUERY=src.TRANSFORMATION_QUERY,tgt.LOAD_TYPE=src.LOAD_TYPE,tgt.MERGE_KEYS=src.MERGE_KEYS,tgt.IS_ACTIVE=src.IS_ACTIVE,tgt.UPDATED_DATE=src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;
