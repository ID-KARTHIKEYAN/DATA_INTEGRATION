"""Constants bound to existing Unity Catalog object names. Do not invent tables."""

from __future__ import annotations

CONTROL_SCHEMA = "admin"
HEADER_TABLE = "data_flow_control_header"
L0_DETAIL_TABLE = "data_flow_l0_detail"
PB_DETAIL_TABLE = "data_flow_pb_detail"
AUDIT_TABLE = "audit_log"

DEFAULT_CATALOG = "demo_catalog"

# Header.ETL_LAYER
LAYERS_L0 = "L0"
LAYERS_L1 = "L1"
LAYERS_L2 = "L2"
VALID_LAYERS = (LAYERS_L0, LAYERS_L1, LAYERS_L2)

# Header.TRIGGER_TYPE — sample data uses JOB for L0; describe text says DLT for L0.
TRIGGER_JOB = "JOB"
TRIGGER_DLT = "DLT"

# L0 LOAD_TYPE (data_flow_l0_detail)
L0_LOAD_FULL = "FULL"
L0_LOAD_DELTA = "DELTA"

# PB LOAD_TYPE (data_flow_pb_detail) — not applicable when TARGET_OBJ_TYPE is MV
PB_LOAD_FULL = "FULL"
PB_LOAD_DELTA = "DELTA"
PB_LOAD_SCD = "SCD"

# Aliases accepted at runtime without adding columns. Mapped onto existing LOAD_TYPE.
LOAD_ALIASES = {
    "OVERWRITE": "FULL",
    "APPEND": "DELTA",
    "INCREMENTAL": "DELTA",
    "MERGE": "DELTA",
    "SCD2": "SCD",
}

# TARGET_OBJ_TYPE
OBJ_TABLE = "TABLE"
OBJ_MV = "MV"

# PARTITION_METHOD
PART_PARTITION = "PARTITION"
PART_LIQUID = "LIQUID_CLUSTER"

ACTIVE_Y = "Y"
ACTIVE_N = "N"

# audit_log.STATUS values used by this framework (column exists; values are runtime)
STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_RETRY = "RETRY"

# audit_log columns from the INSERT in frame_work_table_exist_databricks.txt
# There is no DESCRIBE for audit_log in that file. Only these names are used.
AUDIT_COLUMNS = (
    "DATA_FLOW_GROUP_ID",
    "TARGET_TABLE",
    "STATUS",
    "MESSAGE",
    "CREATED_DATE",
    "ETL_LAYER",
    "ROWS_PROCESSED",
    "START_TIME",
    "END_TIME",
    "LOAD_TS",
)
