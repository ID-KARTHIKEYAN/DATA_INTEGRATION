"""Validate existing metadata columns. Does not add or rename fields."""

from __future__ import annotations

from etl_framework.config import (
    ACTIVE_Y,
    L0_LOAD_DELTA,
    L0_LOAD_FULL,
    LAYERS_L0,
    LAYERS_L1,
    LAYERS_L2,
    LOAD_ALIASES,
    OBJ_MV,
    OBJ_TABLE,
    PART_LIQUID,
    PART_PARTITION,
    PB_LOAD_DELTA,
    PB_LOAD_FULL,
    PB_LOAD_SCD,
    TRIGGER_DLT,
    TRIGGER_JOB,
    VALID_LAYERS,
)
from etl_framework.exceptions import MetadataValidationError, UnsupportedOnFreeEditionError
from etl_framework.metadata import DetailRow, HeaderRow


def _upper(value: object | None) -> str:
    return str(value or "").strip().upper()


def normalize_load_type(raw: object | None, *, allowed: set[str], field: str) -> str:
    value = _upper(raw)
    value = LOAD_ALIASES.get(value, value)
    if value not in allowed:
        raise MetadataValidationError(
            f"{field}={raw!r} is not allowed. Expected one of {sorted(allowed)} "
            f"(aliases {sorted(LOAD_ALIASES)})"
        )
    return value


def apply_spark_configs(spark, spark_configs: object | None) -> None:
    text = str(spark_configs or "").strip()
    if not text:
        return
    pairs: list[tuple[str, str]] = []
    if text.startswith("{") or text.startswith("MAP"):
        raise MetadataValidationError(
            "SPARK_CONFIGS must be semicolon-separated key=value pairs on Free Edition parser"
        )
    for part in text.split(";"):
        if not part.strip():
            continue
        if "=" not in part:
            raise MetadataValidationError(f"Invalid SPARK_CONFIGS fragment: {part!r}")
        key, val = part.split("=", 1)
        pairs.append((key.strip(), val.strip()))
    for key, val in pairs:
        spark.conf.set(key, val)


def validate_header(header: HeaderRow, *, requested_layer: str, free_edition: bool = True) -> str:
    layer = _upper(header["ETL_LAYER"])
    if layer not in VALID_LAYERS:
        raise MetadataValidationError(f"ETL_LAYER={header['ETL_LAYER']!r} must be L0, L1, or L2")
    if requested_layer and requested_layer != layer:
        raise MetadataValidationError(
            f"RUN_LAYER={requested_layer} does not match header ETL_LAYER={layer} "
            f"for DATA_FLOW_GROUP_ID={header['DATA_FLOW_GROUP_ID']}"
        )
    if _upper(header["IS_ACTIVE"]) != ACTIVE_Y:
        raise MetadataValidationError("Header IS_ACTIVE is not Y")
    trigger = _upper(header["TRIGGER_TYPE"])
    if trigger not in {TRIGGER_JOB, TRIGGER_DLT}:
        raise MetadataValidationError(f"TRIGGER_TYPE={header['TRIGGER_TYPE']!r} must be JOB or DLT")
    if free_edition and trigger == TRIGGER_DLT:
        raise UnsupportedOnFreeEditionError(
            "TRIGGER_TYPE=DLT is not supported on Databricks Free Edition. "
            "Set TRIGGER_TYPE='JOB' (as in the EMPLOYEE_MASTER_L0 sample)."
        )
    catalog = header["target_catalog"]
    if not catalog:
        raise MetadataValidationError("target_catalog is required on data_flow_control_header")
    return layer


def validate_l0_row(row: DetailRow, *, free_edition: bool = True) -> None:
    raw = row.raw
    if _upper(raw.get("IS_ACTIVE")) != ACTIVE_Y:
        raise MetadataValidationError("L0 IS_ACTIVE is not Y")
    if not raw.get("SOURCE_OBJ_NAME"):
        raise MetadataValidationError("SOURCE_OBJ_NAME is required")
    if not raw.get("SOURCE_OBJ_SCHEMA"):
        raise MetadataValidationError("SOURCE_OBJ_SCHEMA is required")
    if not raw.get("SOURCE"):
        raise MetadataValidationError("SOURCE is required")
    if not raw.get("INPUT_FILE_FORMAT"):
        raise MetadataValidationError("INPUT_FILE_FORMAT is required")
    normalize_load_type(raw.get("LOAD_TYPE"), allowed={L0_LOAD_FULL, L0_LOAD_DELTA}, field="L0 LOAD_TYPE")
    prestag = _upper(raw.get("PRESTAG_FLAG"))
    if prestag not in {"", "Y", "N"}:
        raise MetadataValidationError("PRESTAG_FLAG must be Y or N")
    if free_edition and prestag == "Y":
        raise UnsupportedOnFreeEditionError(
            "PRESTAG_FLAG='Y' (streaming table) is not available on Databricks Free Edition. "
            "Set PRESTAG_FLAG='N'."
        )
    ls = _upper(raw.get("LS_FLAG"))
    if ls not in {"", "Y", "N"}:
        raise MetadataValidationError("LS_FLAG must be Y or N")
    tq = raw.get("TRANSFORM_QUERY")
    if tq is not None and not isinstance(tq, dict):
        raise MetadataValidationError("L0 TRANSFORM_QUERY must be map<string,string> or NULL")
    if _upper(raw.get("LOAD_TYPE")) in {L0_LOAD_DELTA, "MERGE"} and not raw.get("CDC_LOGIC"):
        # DELTA without CDC_LOGIC is allowed as append-all; MERGE-style needs logic or will append
        pass


def validate_pb_row(row: DetailRow, *, layer: str, free_edition: bool = True) -> None:
    if layer not in {LAYERS_L1, LAYERS_L2}:
        raise MetadataValidationError("PB detail is only valid for L1/L2")
    raw = row.raw
    if _upper(raw.get("IS_ACTIVE")) != ACTIVE_Y:
        raise MetadataValidationError("PB IS_ACTIVE is not Y")
    if not raw.get("TARGET_OBJ_NAME"):
        raise MetadataValidationError("TARGET_OBJ_NAME is required")
    if not raw.get("TARGET_OBJ_SCHEMA"):
        raise MetadataValidationError("TARGET_OBJ_SCHEMA is required")
    obj_type = _upper(raw.get("TARGET_OBJ_TYPE"))
    if obj_type not in {OBJ_TABLE, OBJ_MV, "TABLE"}:
        # describe: Table or MV
        if obj_type not in {"TABLE", "MV"}:
            raise MetadataValidationError("TARGET_OBJ_TYPE must be Table or MV")
    if obj_type == OBJ_MV and free_edition:
        raise UnsupportedOnFreeEditionError(
            "TARGET_OBJ_TYPE='MV' requires Lakeflow/DLT, which is not on Free Edition. "
            "Use TARGET_OBJ_TYPE='Table'."
        )
    if obj_type == OBJ_MV:
        return
    load = normalize_load_type(
        raw.get("LOAD_TYPE"),
        allowed={PB_LOAD_FULL, PB_LOAD_DELTA, PB_LOAD_SCD},
        field="PB LOAD_TYPE",
    )
    if load in {PB_LOAD_DELTA, PB_LOAD_SCD} and not (raw.get("TARGET_PK") or raw.get("SOURCE_PK")):
        if load == PB_LOAD_SCD:
            raise MetadataValidationError("SCD requires TARGET_PK (comma-separated)")
        # DELTA without PK is append
    tq = raw.get("TRANSFORM_QUERY")
    ls = _upper(raw.get("LS_FLAG"))
    if ls not in {"", "Y", "N"}:
        raise MetadataValidationError("LS_FLAG must be Y or N")
    if ls != "Y" and not tq and not raw.get("GENERIC_SCRIPTS"):
        raise MetadataValidationError("TRANSFORM_QUERY or GENERIC_SCRIPTS is required when LS_FLAG<>Y")
    method = _upper(raw.get("PARTITION_METHOD"))
    if method and method not in {PART_PARTITION, PART_LIQUID}:
        raise MetadataValidationError("PARTITION_METHOD must be PARTITION or LIQUID_CLUSTER")
    if free_edition and method == PART_LIQUID:
        raise UnsupportedOnFreeEditionError(
            "PARTITION_METHOD='LIQUID_CLUSTER' is not reliable on Free Edition. "
            "Use PARTITION_METHOD='PARTITION' or leave it NULL."
        )


def assert_single_task(rows: list[DetailRow], *, group_id: str, target: str) -> DetailRow:
    if not rows:
        raise MetadataValidationError(f"No tasks selected for {group_id}")
    if target and len(rows) != 1:
        keys = [r.task_key for r in rows]
        raise MetadataValidationError(
            f"TARGET_LOAD_TABLE={target!r} matched multiple tasks {keys}. "
            "Qualify as schema.object."
        )
    return rows[0] if target else rows[0]
