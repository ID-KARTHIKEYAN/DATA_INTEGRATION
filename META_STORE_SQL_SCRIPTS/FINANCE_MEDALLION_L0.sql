-- ==============================================
-- Workspace : karthik_workspace
-- Group     : FINANCE_MEDALLION_L0
-- Bundle    : [v1.0] [untagged] deployed: 2026-06-20 11:11 | branch: main | layer: L0
-- ==============================================

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (SELECT 'FINANCE_MEDALLION_L0' AS DATA_FLOW_GROUP_ID, 'FINANCE_MEDALLION_L0_JOB' AS JOB_NAME, '/Workspace/Repos/svkarthick0@gmail.com/DATA_INTEGRATION/notebooks/run_framework' AS NOTEBOOK_PATH, 'Y' AS IS_ACTIVE, current_timestamp() AS CREATED_DATE, current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET tgt.NOTEBOOK_PATH = src.NOTEBOOK_PATH, tgt.IS_ACTIVE = src.IS_ACTIVE, tgt.UPDATED_DATE = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
USING (SELECT 'FINANCE_MEDALLION_L0' AS DATA_FLOW_GROUP_ID, 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv' AS SOURCE_URL, 'bronze' AS TARGET_SCHEMA, 'titanic_bronze' AS TARGET_TABLE, 'csv' AS FILE_FORMAT, 'FULL' AS LOAD_TYPE, 'Y' AS IS_ACTIVE, current_timestamp() AS CREATED_DATE, current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID AND tgt.TARGET_TABLE = src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET tgt.SOURCE_URL = src.SOURCE_URL, tgt.FILE_FORMAT = src.FILE_FORMAT, tgt.LOAD_TYPE = src.LOAD_TYPE, tgt.IS_ACTIVE = src.IS_ACTIVE, tgt.UPDATED_DATE = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;
