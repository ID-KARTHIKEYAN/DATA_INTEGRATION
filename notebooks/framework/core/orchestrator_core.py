from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..utils.structured_logger import StructuredLogger
from ..utils.metadata_validator import MetadataValidator, ValidationResult
from ..utils.audit_manager import AuditManager
from ..utils.spark_utils import SparkUtils
from ..utils.retry_handler import RetryHandler


class OrchestratorCore:
    def __init__(self, spark, dbutils=None, catalog: str = "demo_catalog",
                 control_schema: str = "admin",
                 logger: Optional[StructuredLogger] = None,
                 audit_manager: Optional[AuditManager] = None):
        self.spark = spark
        self.dbutils = dbutils
        self.catalog = catalog
        self.control_schema = control_schema
        self.logger = logger or StructuredLogger(catalog=catalog, control_schema=control_schema)
        self.audit = audit_manager or AuditManager(spark, catalog, control_schema)
        self.retry = RetryHandler(max_attempts=3, base_delay_sec=2.0, max_delay_sec=30.0)

    def set_context(self, group_id: Optional[str] = None, layer: Optional[str] = None):
        if group_id:
            self.logger.group_id = group_id
        if layer:
            self.logger.layer = layer
        self.audit.group_id = group_id or self.audit.__dict__.get("group_id")

    def fetch_control_header(self, group_id: str,
                             validate: bool = True) -> Optional[Dict[str, Any]]:
        gid = (group_id or "").strip().upper()
        if not gid:
            raise ValueError("GROUP_ID cannot be empty")
        self.logger.debug("Fetching control_header", group_id=gid)
        table = f"{self.catalog}.{self.control_schema}.data_flow_control_header"
        rows = self.spark.sql(f"""
            SELECT
                DATA_FLOW_GROUP_ID,
                TRIGGER_TYPE,
                ETL_LAYER,
                COMPUTE_CLASS_DEV,
                COMPUTE_CLASS,
                IS_ACTIVE,
                INSERTED_BY,
                UPDATED_BY,
                INSERTED_TS,
                UPDATED_TS,
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
                target_catalog
            FROM {table}
            WHERE DATA_FLOW_GROUP_ID = '{gid.replace("'", "''")}'
        """).collect()
        if not rows:
            raise ValueError(f"No DATA_FLOW_CONTROL_HEADER record found for GROUP_ID='{gid}'")
        record = rows[0].asDict()
        if record.get("IS_ACTIVE", "").strip().upper() != "Y":
            raise ValueError(
                f"Control header GROUP_ID='{gid}' has IS_ACTIVE='{record.get('IS_ACTIVE')}'. "
                f"Set IS_ACTIVE='Y' to enable."
            )
        if validate:
            vr = MetadataValidator.validate_control_header(record)
            if not vr.is_valid:
                raise ValueError(f"Control header invalid for {gid}: {vr.summary()}")
            if vr.warnings:
                for w in vr.warnings:
                    self.logger.warn(f"[control_header] {w}")
        if record.get("SPARK_CONFIGS"):
            utils = SparkUtils(self.spark, self.dbutils)
            result = utils.set_spark_configs(record["SPARK_CONFIGS"], self.logger)
            self.logger.debug("Spark configs applied", result=result)
        if record.get("target_catalog") and str(record["target_catalog"]).strip():
            self.logger.info(f"Overriding catalog from control_header.target_catalog",
                           from_catalog=self.catalog, to_catalog=record["target_catalog"])
            self.catalog = str(record["target_catalog"]).strip()
        return record

    def fetch_l0_details(self, group_id: str, target_obj: Optional[str] = None,
                         lob_filter: Optional[str] = None,
                         validate: bool = True,
                         validate_warn_threshold: int = 0) -> List[Dict[str, Any]]:
        gid = (group_id or "").strip().upper()
        if not gid:
            raise ValueError("GROUP_ID cannot be empty")
        table = f"{self.catalog}.{self.control_schema}.data_flow_l0_detail"
        filters = [f"DATA_FLOW_GROUP_ID = '{gid.replace("'", "''")}'"]
        filters.append("IS_ACTIVE = 'Y'")
        if target_obj and target_obj.strip().upper() not in ("", "ALL"):
            to = target_obj.strip()
            filters.append(f"(SOURCE_OBJ_NAME = '{to.replace("'", "''")}' "
                           f"OR SOURCE_OBJ_NAME LIKE '{to.replace("'", "''")}.%')")
        if lob_filter and lob_filter.strip().upper() not in ("", "ALL"):
            filters.append(f"LOB = '{lob_filter.strip().upper().replace("'", "''")}'")
        where = " AND ".join(filters)
        sql = f"""
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
            FROM {table}
            WHERE {where}
            ORDER BY SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME
        """
        self.logger.debug("Fetching L0 detail rows", group_id=gid, target=target_obj or "ALL")
        rows = self.spark.sql(sql).collect()
        records = [r.asDict() for r in rows]
        if validate:
            self._validate_l0(records, validate_warn_threshold)
        self.logger.info(f"L0: fetched {len(records)} active row(s)", group_id=gid)
        return records

    def _validate_l0(self, records: List[Dict[str, Any]], warn_threshold: int):
        for r in records:
            vr = MetadataValidator.validate_l0_detail(r)
            gid = r.get("DATA_FLOW_GROUP_ID", "?")
            obj = f"{r.get('SOURCE_OBJ_SCHEMA')}.{r.get('SOURCE_OBJ_NAME')}"
            if not vr.is_valid:
                raise ValueError(
                    f"L0 metadata invalid for {gid}/{obj}:\n{vr.summary()}"
                )
            for w in vr.warnings:
                self.logger.warn(f"[l0_detail:{obj}] {w}")

    def fetch_pb_details(self, group_id: str, target_obj: Optional[str] = None,
                         lob_filter: Optional[str] = None,
                         layer: Optional[str] = None,
                         validate: bool = True) -> List[Dict[str, Any]]:
        gid = (group_id or "").strip().upper()
        if not gid:
            raise ValueError("GROUP_ID cannot be empty")
        table = f"{self.catalog}.{self.control_schema}.data_flow_pb_detail"
        filters = [f"DATA_FLOW_GROUP_ID = '{gid.replace("'", "''")}'"]
        filters.append("IS_ACTIVE = 'Y'")
        if target_obj and target_obj.strip().upper() not in ("", "ALL"):
            to = target_obj.strip()
            filters.append(f"TARGET_OBJ_NAME = '{to.replace("'", "''")}'")
        if lob_filter and lob_filter.strip().upper() not in ("", "ALL"):
            filters.append(f"LOB = '{lob_filter.strip().upper().replace("'", "''")}'")
        where = " AND ".join(filters)
        sql = f"""
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
                INSERTED_BY,
                UPDATED_BY,
                INSERTED_TS,
                UPDATED_TS,
                CUSTOM_SCRIPT_PARAMS,
                PARTITION_METHOD,
                RETENTION_DETAILS,
                DEPLOYMENT_SOURCE_DFG
            FROM {table}
            WHERE {where}
            ORDER BY COALESCE(PRIORITY, 999), TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME
        """
        effective_layer = layer or "PB"
        self.logger.debug("Fetching PB detail rows", group_id=gid, target=target_obj or "ALL")
        rows = self.spark.sql(sql).collect()
        records = [r.asDict() for r in rows]
        if validate:
            self._validate_pb(records, effective_layer)
        self.logger.info(f"{effective_layer}: fetched {len(records)} active row(s)", group_id=gid)
        return records

    def _validate_pb(self, records: List[Dict[str, Any]], layer: str):
        for r in records:
            vr = MetadataValidator.validate_pb_detail(r, layer)
            gid = r.get("DATA_FLOW_GROUP_ID", "?")
            obj = f"{r.get('TARGET_OBJ_SCHEMA')}.{r.get('TARGET_OBJ_NAME')}"
            if not vr.is_valid:
                raise ValueError(
                    f"{layer} metadata invalid for {gid}/{obj}:\n{vr.summary()}"
                )
            for w in vr.warnings:
                self.logger.warn(f"[pb_detail:{obj}] {w}")

    @staticmethod
    def group_by_priority(records: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        buckets: Dict[int, List[Dict[str, Any]]] = {}
        for r in records:
            p_raw = r.get("PRIORITY")
            try:
                p = int(p_raw)
            except (TypeError, ValueError):
                p = 999
            buckets.setdefault(p, []).append(r)
        return dict(sorted(buckets.items()))
