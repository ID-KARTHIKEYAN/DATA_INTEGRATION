-- ============================================================
-- Workspace : karthik_workspace
-- GROUP_ID  : FINANCE_MEDALLION_L1
-- Layers    : L0(0) L1(1) L2(0)
-- Generated : 2026-07-08 04:59:38
-- Bundle    : [v1.0][untagged] 2026-07-08 04:58 | L1 | main
-- ============================================================

-- control_header
MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (SELECT 'FINANCE_MEDALLION_L1' AS DATA_FLOW_GROUP_ID,'FINANCE_MEDALLION_L1_JOB' AS JOB_NAME,'/Workspace/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/notebooks/run_framework' AS NOTEBOOK_PATH,'Y' AS IS_ACTIVE,current_timestamp() AS CREATED_DATE,current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID=src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET tgt.NOTEBOOK_PATH=src.NOTEBOOK_PATH,tgt.IS_ACTIVE=src.IS_ACTIVE,tgt.UPDATED_DATE=src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

-- L1 Silver (1 table/s)
MERGE INTO demo_catalog.admin.data_flow_l1_detail AS tgt
USING (SELECT 'FINANCE_MEDALLION_L1' AS DATA_FLOW_GROUP_ID,'bronze' AS SOURCE_OBJ_SCHEMA,'titanic_bronze' AS SOURCE_OBJ_NAME,'silver' AS TARGET_SCHEMA,'titanic_silver' AS TARGET_TABLE,'FULL' AS LOAD_TYPE,'SELECT PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Fare, Embarked, _etl_load_ts AS bronze_ts, current_timestamp() AS silver_ts FROM bronze.titanic_bronze WHERE Age IS NOT NULL' AS TRANSFORMATION_QUERY,'PassengerId' AS MERGE_KEYS,'Pclass' AS PARTITION_BY,'Y' AS IS_ACTIVE,current_timestamp() AS CREATED_DATE,current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID=src.DATA_FLOW_GROUP_ID AND tgt.TARGET_TABLE=src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET tgt.TRANSFORMATION_QUERY=src.TRANSFORMATION_QUERY,tgt.LOAD_TYPE=src.LOAD_TYPE,tgt.MERGE_KEYS=src.MERGE_KEYS,tgt.IS_ACTIVE=src.IS_ACTIVE,tgt.UPDATED_DATE=src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;
