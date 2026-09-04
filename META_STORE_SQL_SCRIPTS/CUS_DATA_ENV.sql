-- karthik_workspace | CUS_DATA_ENV | [v1.0][untagged] 2026-08-22 09:22 | ALL | main

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (SELECT 'CUS_DATA_ENV' AS DATA_FLOW_GROUP_ID,'CUS_DATA_ENV_JOB' AS JOB_NAME,'/Workspace/Users/svkarthick0@gmail.com/run_framework' AS NOTEBOOK_PATH,'Y' AS IS_ACTIVE,current_timestamp() AS CREATED_DATE,current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID=src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET tgt.NOTEBOOK_PATH=src.NOTEBOOK_PATH,tgt.IS_ACTIVE=src.IS_ACTIVE,tgt.UPDATED_DATE=src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
USING (SELECT 'CUS_DATA_ENV' AS DATA_FLOW_GROUP_ID,'https://drive.usercontent.google.com/download?id=1_zMCoEEcEm0Mp_so3vXSwruhZJj0QIQi&export=download&authuser=0' AS SOURCE_URL,'bronze' AS TARGET_SCHEMA,'cus_sys' AS TARGET_TABLE,'csv' AS FILE_FORMAT,'FULL' AS LOAD_TYPE,'Y' AS IS_ACTIVE,current_timestamp() AS CREATED_DATE,current_timestamp() AS UPDATED_DATE) AS src
ON tgt.DATA_FLOW_GROUP_ID=src.DATA_FLOW_GROUP_ID AND tgt.TARGET_TABLE=src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET tgt.SOURCE_URL=src.SOURCE_URL,tgt.FILE_FORMAT=src.FILE_FORMAT,tgt.LOAD_TYPE=src.LOAD_TYPE,tgt.IS_ACTIVE=src.IS_ACTIVE,tgt.UPDATED_DATE=src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;
