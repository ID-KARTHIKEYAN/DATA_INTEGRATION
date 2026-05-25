-- ==============================================
-- Workspace : karthik_workspace
-- Group     : DATAKAMIT_TEAM_MEMBER_L0
-- Generated : 2026-05-25 06:10:02
-- ==============================================

MERGE INTO demo_catalog.admin.data_flow_control_header AS tgt
USING (
  SELECT
    'DATAKAMIT_TEAM_MEMBER_L0'           AS DATA_FLOW_GROUP_ID,
    'DATAKAMIT_TEAM_MEMBER_L0_JOB'       AS JOB_NAME,
    '/Workspace/Users/svkarthick0@gmail.com/run_framework'      AS NOTEBOOK_PATH,
    'Y'                    AS IS_ACTIVE,
    current_timestamp()    AS CREATED_DATE,
    current_timestamp()    AS UPDATED_DATE
) AS src
ON tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
WHEN MATCHED THEN UPDATE SET
    tgt.NOTEBOOK_PATH = src.NOTEBOOK_PATH,
    tgt.IS_ACTIVE     = src.IS_ACTIVE,
    tgt.UPDATED_DATE  = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO demo_catalog.admin.data_flow_l0_detail AS tgt
USING (
  SELECT
    'DATAKAMIT_TEAM_MEMBER_L0'  AS DATA_FLOW_GROUP_ID,
    'https://raw.githubusercontent.com/ID-KARTHIKEYAN/DATA_INTEGRATION/refs/heads/main/Meta_data.csv'     AS SOURCE_URL,
    'comp_meta_data'     AS TARGET_SCHEMA,
    'team_mem_data'     AS TARGET_TABLE,
    'csv'     AS FILE_FORMAT,
    'FULL'     AS LOAD_TYPE,
    'Y'           AS IS_ACTIVE,
    current_timestamp() AS CREATED_DATE,
    current_timestamp() AS UPDATED_DATE
) AS src
ON  tgt.DATA_FLOW_GROUP_ID = src.DATA_FLOW_GROUP_ID
AND tgt.TARGET_TABLE       = src.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
    tgt.SOURCE_URL   = src.SOURCE_URL,
    tgt.FILE_FORMAT  = src.FILE_FORMAT,
    tgt.LOAD_TYPE    = src.LOAD_TYPE,
    tgt.IS_ACTIVE    = src.IS_ACTIVE,
    tgt.UPDATED_DATE = src.UPDATED_DATE
WHEN NOT MATCHED THEN INSERT *;
