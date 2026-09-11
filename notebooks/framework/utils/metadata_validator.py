from typing import Any, Dict, List, Optional, Tuple
from enum import Enum


class ValidationSeverity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class ValidationResult:
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def add_error(self, msg: str):
        self.errors.append(msg)

    def add_warning(self, msg: str):
        self.warnings.append(msg)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def raise_if_invalid(self, prefix: str = "Metadata validation failed"):
        if not self.is_valid:
            joined = "\n  - ".join(self.errors)
            raise ValueError(f"{prefix}:\n  - {joined}")

    def summary(self) -> str:
        parts = []
        if self.errors:
            parts.append(f"ERRORS ({len(self.errors)}):\n  - " + "\n  - ".join(self.errors))
        if self.warnings:
            parts.append(f"WARNINGS ({len(self.warnings)}):\n  - " + "\n  - ".join(self.warnings))
        return "\n".join(parts) if parts else "VALID"


class MetadataValidator:
    VALID_LOAD_TYPES_L0 = {"FULL", "DELTA", "INCREMENTAL", "APPEND", "OVERWRITE"}
    VALID_LOAD_TYPES_PB = {"FULL", "DELTA", "INCREMENTAL", "APPEND", "OVERWRITE", "MERGE", "SCD", "SCD2"}
    VALID_OBJ_TYPES = {"TABLE", "MV", "VIEW"}
    VALID_PARTITION_METHODS = {"PARTITION", "LIQUID_CLUSTER", ""}
    VALID_YN = {"Y", "N", "B", "A"}
    VALID_TRIGGER_TYPES = {"JOB", "DLT", "WORKFLOW"}
    VALID_LAYERS = {"L0", "L1", "L2"}
    VALID_FILE_FORMATS = {"CSV", "JSON", "PARQUET", "DELTA", "AVRO", "ORC", "TSV", "XML", "XLSX", "XLS", "EXCEL"}

    @classmethod
    def validate_control_header(cls, row: Dict[str, Any], existing_catalogs: Optional[List[str]] = None) -> ValidationResult:
        result = ValidationResult()
        gid = (row.get("DATA_FLOW_GROUP_ID") or "").strip()
        if not gid:
            result.add_error("DATA_FLOW_GROUP_ID is required and cannot be empty")
        else:
            if gid.endswith("_L0"):
                expected_layer = "L0"
            elif gid.endswith("_L1"):
                expected_layer = "L1"
            elif gid.endswith("_L2"):
                expected_layer = "L2"
            else:
                expected_layer = None
            actual_layer = (row.get("ETL_LAYER") or "").strip().upper()
            if expected_layer and actual_layer and expected_layer != actual_layer:
                result.add_warning(
                    f"DATA_FLOW_GROUP_ID '{gid}' implies layer {expected_layer} "
                    f"but ETL_LAYER='{actual_layer}' in control_header"
                )

        trigger = (row.get("TRIGGER_TYPE") or "").strip().upper()
        if trigger and trigger not in cls.VALID_TRIGGER_TYPES:
            result.add_error(f"TRIGGER_TYPE='{trigger}' not in {cls.VALID_TRIGGER_TYPES}")

        layer = (row.get("ETL_LAYER") or "").strip().upper()
        if layer and layer not in cls.VALID_LAYERS:
            result.add_error(f"ETL_LAYER='{layer}' not in {cls.VALID_LAYERS}")

        is_active = (row.get("IS_ACTIVE") or "").strip().upper()
        if is_active not in ("Y", "N"):
            result.add_error(f"IS_ACTIVE='{is_active}' must be 'Y' or 'N'")

        if row.get("target_catalog"):
            tc = row["target_catalog"].strip()
            if existing_catalogs and tc not in existing_catalogs:
                result.add_warning(f"target_catalog='{tc}' not found in existing catalogs list")

        warn_mins = row.get("WARNING_THRESHOLD_MINS")
        if warn_mins is not None and not isinstance(warn_mins, int):
            try:
                int(warn_mins)
            except (ValueError, TypeError):
                result.add_error(f"WARNING_THRESHOLD_MINS='{warn_mins}' must be integer")

        return result

    @classmethod
    def validate_l0_detail(cls, row: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        gid = (row.get("DATA_FLOW_GROUP_ID") or "").strip()
        if not gid:
            result.add_error("L0: DATA_FLOW_GROUP_ID is required")

        source = (row.get("SOURCE") or "").strip()
        if not source:
            result.add_error("L0: SOURCE is required (file path or URL)")

        src_obj_schema = (row.get("SOURCE_OBJ_SCHEMA") or "").strip()
        if not src_obj_schema:
            result.add_error("L0: SOURCE_OBJ_SCHEMA is required (target schema for bronze)")

        src_obj_name = (row.get("SOURCE_OBJ_NAME") or "").strip()
        if not src_obj_name:
            result.add_error("L0: SOURCE_OBJ_NAME is required (target table/file name)")

        load_type = (row.get("LOAD_TYPE") or "").strip().upper()
        if load_type and load_type not in cls.VALID_LOAD_TYPES_L0:
            result.add_error(
                f"L0: LOAD_TYPE='{load_type}' not valid. Expected one of {cls.VALID_LOAD_TYPES_L0}"
            )

        file_fmt = (row.get("INPUT_FILE_FORMAT") or "").strip().upper()
        if file_fmt and file_fmt not in cls.VALID_FILE_FORMATS:
            result.add_warning(f"L0: INPUT_FILE_FORMAT='{file_fmt}' is not a known format")

        if file_fmt == "CSV":
            delim = (row.get("DELIMETER") or "").strip()
            if not delim:
                result.add_warning("L0: DELIMETER empty but INPUT_FILE_FORMAT=CSV (will use ',')")

        storage = (row.get("STORAGE_TYPE") or "").strip()
        if not storage:
            result.add_warning("L0: STORAGE_TYPE is empty")

        prestag = (row.get("PRESTAG_FLAG") or "").strip().upper()
        if prestag and prestag not in ("Y", "N"):
            result.add_error(f"L0: PRESTAG_FLAG='{prestag}' must be 'Y' or 'N'")

        ls_flag = (row.get("LS_FLAG") or "").strip().upper()
        if ls_flag and ls_flag not in cls.VALID_YN:
            result.add_warning(f"L0: LS_FLAG='{ls_flag}' expected Y/N/B/A")

        is_active = (row.get("IS_ACTIVE") or "").strip().upper()
        if is_active not in ("Y", "N"):
            result.add_error(f"L0: IS_ACTIVE='{is_active}' must be 'Y' or 'N'")

        tq = row.get("TRANSFORM_QUERY")
        if tq and not isinstance(tq, dict) and not (isinstance(tq, str) and tq.strip() == ""):
            try:
                import json
                if isinstance(tq, str):
                    json.loads(tq)
            except Exception:
                result.add_warning("L0: TRANSFORM_QUERY is not a valid map<string,string>")

        return result

    @classmethod
    def validate_pb_detail(cls, row: Dict[str, Any], layer: str) -> ValidationResult:
        result = ValidationResult()
        gid = (row.get("DATA_FLOW_GROUP_ID") or "").strip()
        if not gid:
            result.add_error(f"{layer}: DATA_FLOW_GROUP_ID is required")

        tgt_schema = (row.get("TARGET_OBJ_SCHEMA") or "").strip()
        if not tgt_schema:
            result.add_error(f"{layer}: TARGET_OBJ_SCHEMA is required")

        tgt_name = (row.get("TARGET_OBJ_NAME") or "").strip()
        if not tgt_name:
            result.add_error(f"{layer}: TARGET_OBJ_NAME is required")

        obj_type = (row.get("TARGET_OBJ_TYPE") or "TABLE").strip().upper()
        if obj_type not in cls.VALID_OBJ_TYPES:
            result.add_error(f"{layer}: TARGET_OBJ_TYPE='{obj_type}' not in {cls.VALID_OBJ_TYPES}")

        load_type = (row.get("LOAD_TYPE") or "").strip().upper()
        if load_type:
            if obj_type == "MV":
                pass
            elif load_type not in cls.VALID_LOAD_TYPES_PB:
                result.add_error(
                    f"{layer}: LOAD_TYPE='{load_type}' not valid. Expected one of {cls.VALID_LOAD_TYPES_PB}"
                )

        if obj_type not in ("MV", "VIEW"):
            if load_type in ("MERGE", "SCD", "SCD2"):
                pk = (row.get("TARGET_PK") or row.get("SOURCE_PK") or "").strip()
                if not pk:
                    result.add_error(
                        f"{layer}: LOAD_TYPE='{load_type}' requires TARGET_PK or SOURCE_PK "
                        f"for table '{tgt_schema}.{tgt_name}'"
                    )

        transform = (row.get("TRANSFORM_QUERY") or "").strip()
        if not transform:
            result.add_warning(
                f"{layer}: TRANSFORM_QUERY is empty for '{tgt_schema}.{tgt_name}'"
            )

        p_method = (row.get("PARTITION_METHOD") or "").strip().upper()
        if p_method and p_method not in {"PARTITION", "LIQUID_CLUSTER"}:
            result.add_warning(
                f"{layer}: PARTITION_METHOD='{p_method}' expected PARTITION or LIQUID_CLUSTER"
            )

        ls_flag = (row.get("LS_FLAG") or "").strip().upper()
        if ls_flag and ls_flag not in cls.VALID_YN:
            result.add_warning(f"{layer}: LS_FLAG='{ls_flag}' expected Y/N/B/A")

        is_active = (row.get("IS_ACTIVE") or "").strip().upper()
        if is_active not in ("Y", "N"):
            result.add_error(f"{layer}: IS_ACTIVE='{is_active}' must be 'Y' or 'N'")

        return result

    @classmethod
    def validate_audit_log_row(cls, row: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        gid = (row.get("DATA_FLOW_GROUP_ID") or "").strip()
        status = (row.get("STATUS") or "").strip().upper()
        if not gid:
            result.add_warning("audit_log: DATA_FLOW_GROUP_ID empty")
        if status not in ("SUCCESS", "FAILED", "RUNNING", "SKIPPED"):
            result.add_warning(f"audit_log: STATUS='{status}' is not a standard value")
        return result
