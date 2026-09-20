# =============================================================================
# framework/metadata_validator.py
# Pre-execution validation for every metadata row before any Spark action runs.
# Raises specific, actionable errors — never silently swallows problems.
# =============================================================================

from __future__ import annotations
from typing import Any, Dict, List, Optional


class ValidationError(ValueError):
    """Raised when a metadata row fails pre-execution validation."""
    pass


class ConfigurationError(ValueError):
    """Raised when control_header row is misconfigured."""
    pass


class MetadataValidator:
    """
    Validates a row from data_flow_l0_detail or data_flow_pb_detail
    before the framework executes any Spark operation.

    All methods raise ValidationError with the exact column name, table name,
    and a remediation hint.
    """

    VALID_LOAD_TYPES_L0    = {"FULL", "DELTA"}
    VALID_LOAD_TYPES_PB    = {"FULL", "DELTA", "SCD", "APPEND", "OVERWRITE"}
    VALID_OBJ_TYPES        = {"TABLE", "MV"}
    VALID_FILE_FORMATS     = {"csv", "tsv", "json", "parquet", "delta",
                               "avro", "orc", "xlsx", "xls", "excel", "xml"}
    VALID_PARTITION_METHODS = {"PARTITION", "LIQUID_CLUSTER", ""}

    # ── L0 validators ─────────────────────────────────────────────────────

    @staticmethod
    def validate_l0_row(row: Dict[str, Any]) -> None:
        """
        Full pre-flight validation for one data_flow_l0_detail row.
        Raises ValidationError on the first problem found.
        """
        tbl = row.get("SOURCE_OBJ_NAME") or "(unknown)"

        # Required fields
        if not (row.get("SOURCE") or "").strip():
            raise ValidationError(
                f"[L0:{tbl}] SOURCE is null/empty. "
                f"Set SOURCE to the full URL or path of the source file."
            )
        if not (row.get("SOURCE_OBJ_SCHEMA") or "").strip():
            raise ValidationError(
                f"[L0:{tbl}] SOURCE_OBJ_SCHEMA is null/empty. "
                f"Set it to the target Databricks schema (e.g. 'raw' or 'bronze')."
            )
        if not (row.get("SOURCE_OBJ_NAME") or "").strip():
            raise ValidationError(
                f"[L0:?] SOURCE_OBJ_NAME is null/empty. "
                f"Set it to the source file name (e.g. 'employee.csv')."
            )

        # LOAD_TYPE
        load_type = (row.get("LOAD_TYPE") or "FULL").upper()
        if load_type not in MetadataValidator.VALID_LOAD_TYPES_L0:
            raise ValidationError(
                f"[L0:{tbl}] LOAD_TYPE='{load_type}' is invalid. "
                f"Must be one of: {MetadataValidator.VALID_LOAD_TYPES_L0}"
            )

        # INPUT_FILE_FORMAT
        fmt = (row.get("INPUT_FILE_FORMAT") or "csv").lower()
        if fmt not in MetadataValidator.VALID_FILE_FORMATS:
            raise ValidationError(
                f"[L0:{tbl}] INPUT_FILE_FORMAT='{fmt}' is not supported. "
                f"Supported: {sorted(MetadataValidator.VALID_FILE_FORMATS)}"
            )

        # PRESTAG_FLAG = 'Y' → warn only (not a hard error, just unsupported)
        prestag = (row.get("PRESTAG_FLAG") or "N").upper()
        if prestag == "Y":
            print(
                f"  ⚠ [L0:{tbl}] PRESTAG_FLAG='Y' (streaming table) is NOT supported "
                f"on Databricks Free Edition. The table will be created as a managed "
                f"Delta table instead. Update PRESTAG_FLAG to 'N' to suppress this warning."
            )

        # CDC_LOGIC required for DELTA
        if load_type == "DELTA":
            cdc = (row.get("CDC_LOGIC") or "").strip()
            if not cdc:
                raise ValidationError(
                    f"[L0:{tbl}] LOAD_TYPE='DELTA' requires CDC_LOGIC to be set. "
                    f"Example: \"updated_ts > '{{watermark}}'\"  "
                    f"(use {{watermark}} as the placeholder for the last run timestamp)"
                )

    # ── L1/L2 validators ──────────────────────────────────────────────────

    @staticmethod
    def validate_pb_row(row: Dict[str, Any]) -> None:
        """
        Full pre-flight validation for one data_flow_pb_detail row.
        """
        tbl = row.get("TARGET_OBJ_NAME") or "(unknown)"

        # Required fields
        if not (row.get("TARGET_OBJ_SCHEMA") or "").strip():
            raise ValidationError(
                f"[PB:{tbl}] TARGET_OBJ_SCHEMA is null/empty. "
                f"Set it to the target Databricks schema (e.g. 'silver')."
            )
        if not (row.get("TARGET_OBJ_NAME") or "").strip():
            raise ValidationError(
                f"[PB:?] TARGET_OBJ_NAME is null/empty. "
                f"Set it to the target table or MV name."
            )

        # TARGET_OBJ_TYPE
        obj_type = (row.get("TARGET_OBJ_TYPE") or "Table").upper()
        if obj_type not in MetadataValidator.VALID_OBJ_TYPES:
            raise ValidationError(
                f"[PB:{tbl}] TARGET_OBJ_TYPE='{obj_type}' is invalid. "
                f"Must be 'Table' or 'MV'."
            )

        # TRANSFORM_QUERY required for all types
        if not (row.get("TRANSFORM_QUERY") or "").strip():
            raise ValidationError(
                f"[PB:{tbl}] TRANSFORM_QUERY is null/empty. "
                f"Provide a full SQL SELECT statement."
            )

        # LOAD_TYPE validation (only meaningful for Table, not MV)
        if obj_type == "TABLE":
            load_type = (row.get("LOAD_TYPE") or "FULL").upper()
            if load_type not in MetadataValidator.VALID_LOAD_TYPES_PB:
                raise ValidationError(
                    f"[PB:{tbl}] LOAD_TYPE='{load_type}' is invalid. "
                    f"Must be one of: {MetadataValidator.VALID_LOAD_TYPES_PB}"
                )

            # PK required for DELTA (=MERGE) and SCD
            target_pk = (row.get("TARGET_PK") or "").strip()
            source_pk = (row.get("SOURCE_PK") or "").strip()
            merge_keys = target_pk or source_pk

            if load_type == "DELTA" and not merge_keys:
                raise ValidationError(
                    f"[PB:{tbl}] LOAD_TYPE='DELTA' (merge/upsert) requires TARGET_PK "
                    f"or SOURCE_PK to be set. "
                    f"Example: TARGET_PK = 'employee_id'  or  'employee_id,department_id'"
                )
            if load_type == "SCD" and not merge_keys:
                raise ValidationError(
                    f"[PB:{tbl}] LOAD_TYPE='SCD' requires TARGET_PK to be set. "
                    f"Example: TARGET_PK = 'employee_id'"
                )

        # PARTITION_METHOD
        pm = (row.get("PARTITION_METHOD") or "").upper()
        if pm not in MetadataValidator.VALID_PARTITION_METHODS:
            raise ValidationError(
                f"[PB:{tbl}] PARTITION_METHOD='{pm}' is invalid. "
                f"Must be 'PARTITION', 'LIQUID_CLUSTER', or empty."
            )

        # PARTITION_OR_INDEX must be non-empty if PARTITION_METHOD is set
        if pm and not (row.get("PARTITION_OR_INDEX") or "").strip():
            raise ValidationError(
                f"[PB:{tbl}] PARTITION_METHOD='{pm}' is set but "
                f"PARTITION_OR_INDEX is empty. Provide comma-separated column names."
            )

    # ── DataFrame-level validators (run after read, before write) ─────────

    @staticmethod
    def validate_df_columns(df, required_cols: List[str], table_name: str) -> None:
        """
        Checks that all *required_cols* are present in the DataFrame.
        Used to validate merge keys and partition columns before MERGE/write.
        """
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValidationError(
                f"[{table_name}] Required column(s) not found in DataFrame: {missing}. "
                f"Available columns: {df.columns}"
            )

    @staticmethod
    def validate_transform_query(spark, query: str, table_name: str) -> None:
        """
        Dry-runs *query* with EXPLAIN to catch unresolved references before execution.
        Raises ValidationError with the full AnalysisException message.
        """
        from pyspark.sql.utils import AnalysisException
        try:
            spark.sql(f"EXPLAIN {query}")
        except AnalysisException as e:
            raise ValidationError(
                f"[{table_name}] TRANSFORM_QUERY failed dry-run validation.\n"
                f"  Query (first 300 chars): {query[:300]}\n"
                f"  AnalysisException: {str(e)[:400]}\n"
                f"  Hint: Check that all source tables/schemas exist in the target catalog "
                f"and that all referenced columns are spelled correctly."
            )
