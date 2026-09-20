# =============================================================================
# framework/metadata_reader.py
# Reads all three control tables and returns validated Python dicts/lists.
# Source of truth: demo_catalog.admin.data_flow_control_header
#                  demo_catalog.admin.data_flow_l0_detail
#                  demo_catalog.admin.data_flow_pb_detail
# =============================================================================

from __future__ import annotations
from typing import Any, Dict, List, Optional


class MetadataReader:
    """
    Centralised reader for all ETL control tables.
    All public methods return plain Python dicts or lists of dicts —
    never Spark Row objects — so downstream code stays testable.
    """

    DEFAULT_CATALOG = "demo_catalog"
    CONTROL_SCHEMA  = "admin"

    def __init__(self, spark, catalog: str = DEFAULT_CATALOG):
        self.spark   = spark
        self.catalog = catalog
        self._ctrl   = f"{catalog}.{self.CONTROL_SCHEMA}"

    # ── control_header ────────────────────────────────────────────────────

    def read_header(self, group_id: str) -> Dict[str, Any]:
        """
        Reads the control_header row for *group_id*.
        Raises ConfigurationError if not found or IS_ACTIVE != 'Y'.
        """
        rows = self.spark.sql(f"""
            SELECT
                DATA_FLOW_GROUP_ID,
                TRIGGER_TYPE,
                ETL_LAYER,
                COMPUTE_CLASS_DEV,
                COMPUTE_CLASS,
                IS_ACTIVE,
                BUSINESS_OBJECT_NAME,
                COST_CENTER,
                DATA_SME,
                BUSINESS_UNIT,
                PRODUCT_OWNER,
                INGESTION_MODE,
                INGESTION_BUCKET,
                SPARK_CONFIGS,
                WARNING_THRESHOLD_MINS,
                WARNING_DL_GROUP,
                MIN_VERSION,
                MAX_VERSION,
                target_catalog,
                INSERTED_BY,
                UPDATED_BY,
                INSERTED_TS,
                UPDATED_TS
            FROM {self._ctrl}.data_flow_control_header
            WHERE DATA_FLOW_GROUP_ID = '{group_id}'
        """).collect()

        if not rows:
            raise ValueError(
                f"[ConfigurationError] GROUP_ID='{group_id}' not found in "
                f"{self._ctrl}.data_flow_control_header"
            )

        hdr = rows[0].asDict()

        if (hdr.get("IS_ACTIVE") or "N").upper() != "Y":
            raise ValueError(
                f"[ConfigurationError] GROUP_ID='{group_id}' has IS_ACTIVE='N'. "
                f"Set IS_ACTIVE='Y' to enable execution."
            )

        trigger = (hdr.get("TRIGGER_TYPE") or "JOB").upper()
        if trigger == "DLT":
            raise ValueError(
                f"[ConfigurationError] GROUP_ID='{group_id}' uses TRIGGER_TYPE='DLT'. "
                f"Delta Live Tables are NOT supported on Databricks Free Edition. "
                f"Change TRIGGER_TYPE to 'JOB'."
            )

        # Normalise target_catalog (column is lowercase in live table)
        resolved_catalog = (
            hdr.get("target_catalog") or
            hdr.get("TARGET_CATALOG") or
            self.catalog
        )
        hdr["_resolved_catalog"] = resolved_catalog
        return hdr

    # ── data_flow_l0_detail ───────────────────────────────────────────────

    def read_l0_detail(
        self,
        group_id: str,
        target_table: Optional[str] = None,
        lob_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Returns list of active L0 rows ordered by SOURCE_OBJ_NAME.
        *target_table* filters to a single object (empty/ALL = all).
        *lob_filter*   filters by LOB (empty/ALL = all).
        """
        obj_clause = (
            f"AND SOURCE_OBJ_NAME = '{target_table}'"
            if target_table and target_table.upper() != "ALL"
            else ""
        )
        lob_clause = (
            f"AND LOB = '{lob_filter}'"
            if lob_filter and lob_filter.upper() != "ALL"
            else ""
        )

        rows = self.spark.sql(f"""
            SELECT
                DATA_FLOW_GROUP_ID,
                SOURCE,
                SOURCE_OBJ_SCHEMA,
                SOURCE_OBJ_NAME,
                LOB,
                LOAD_TYPE,
                INPUT_FILE_FORMAT,
                STORAGE_TYPE,
                DQ_LOGIC,
                DELIMETER,
                CUSTOM_SCHEMA,
                CDC_LOGIC,
                TRANSFORM_QUERY,
                PRESTAG_FLAG,
                `PARTITION`,
                LS_FLAG,
                LS_DETAIL,
                IS_ACTIVE,
                INSERTED_BY,
                UPDATED_BY,
                INSERTED_TS,
                UPDATED_TS,
                DEPLOYMENT_SOURCE_DFG
            FROM {self._ctrl}.data_flow_l0_detail
            WHERE DATA_FLOW_GROUP_ID = '{group_id}'
              AND IS_ACTIVE = 'Y'
              {obj_clause}
              {lob_clause}
            ORDER BY SOURCE_OBJ_NAME
        """).collect()

        return [r.asDict() for r in rows]

    # ── data_flow_pb_detail ───────────────────────────────────────────────

    def read_pb_detail(
        self,
        group_id: str,
        target_table: Optional[str] = None,
        lob_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Returns list of active L1/L2 rows ordered by PRIORITY then TARGET_OBJ_NAME.
        """
        obj_clause = (
            f"AND TARGET_OBJ_NAME = '{target_table}'"
            if target_table and target_table.upper() != "ALL"
            else ""
        )
        lob_clause = (
            f"AND LOB = '{lob_filter}'"
            if lob_filter and lob_filter.upper() != "ALL"
            else ""
        )

        rows = self.spark.sql(f"""
            SELECT
                DATA_FLOW_GROUP_ID,
                LOB,
                SOURCE,
                TARGET_OBJ_SCHEMA,
                TARGET_OBJ_NAME,
                PRIORITY,
                TARGET_OBJ_TYPE,
                TRANSFORM_QUERY,
                GENERIC_SCRIPTS,
                SOURCE_PK,
                TARGET_PK,
                LOAD_TYPE,
                IS_ACTIVE,
                LS_FLAG,
                LS_DETAIL,
                PARTITION_OR_INDEX,
                PARTITION_METHOD,
                RETENTION_DETAILS,
                CUSTOM_SCRIPT_PARAMS,
                INSERTED_BY,
                UPDATED_BY,
                INSERTED_TS,
                UPDATED_TS,
                DEPLOYMENT_SOURCE_DFG
            FROM {self._ctrl}.data_flow_pb_detail
            WHERE DATA_FLOW_GROUP_ID = '{group_id}'
              AND IS_ACTIVE = 'Y'
              {obj_clause}
              {lob_clause}
            ORDER BY COALESCE(PRIORITY, 999), TARGET_OBJ_NAME
        """).collect()

        return [r.asDict() for r in rows]

    # ── audit_log helpers ─────────────────────────────────────────────────

    def get_last_watermark(
        self, group_id: str, table_name: str, layer: str
    ) -> str:
        """
        Returns last successful END_TIME as string (YYYY-MM-DD HH:MM:SS).
        Returns '1970-01-01 00:00:00' if no prior success → forces full read.
        """
        try:
            result = self.spark.sql(f"""
                SELECT MAX(END_TIME)
                FROM   {self._ctrl}.audit_log
                WHERE  DATA_FLOW_GROUP_ID = '{group_id}'
                  AND  TARGET_TABLE       = '{table_name}'
                  AND  ETL_LAYER          = '{layer}'
                  AND  STATUS             = 'SUCCESS'
            """).collect()
            val = result[0][0] if result and result[0][0] else None
            return val.strftime("%Y-%m-%d %H:%M:%S") if val else "1970-01-01 00:00:00"
        except Exception as e:
            print(f"  ⚠ Watermark lookup failed: {e}. Using epoch.")
            return "1970-01-01 00:00:00"
