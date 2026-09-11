import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..utils.structured_logger import StructuredLogger
from ..utils.audit_manager import AuditManager
from ..utils.spark_utils import SparkUtils
from ..utils.retry_handler import RetryHandler
from ..core.source_reader import SourceReader
from ..core.target_writer import TargetWriter
from ..core.transformer import Transformer
from ..core.orchestrator_core import OrchestratorCore


class L0Processor:
    LAYER = "L0"

    def __init__(self, spark, dbutils=None, catalog: str = "demo_catalog",
                 control_schema: str = "admin",
                 logger: Optional[StructuredLogger] = None,
                 audit_manager: Optional[AuditManager] = None,
                 environment: Optional[str] = None):
        self.spark = spark
        self.dbutils = dbutils
        self.catalog = catalog
        self.control_schema = control_schema
        self.env = environment or "dev"
        self.logger = logger or StructuredLogger(catalog=catalog, control_schema=control_schema)
        self.logger.layer = self.LAYER
        self.audit = audit_manager or AuditManager(spark, catalog, control_schema)
        self.reader = SourceReader(spark, dbutils, self.logger)
        self.writer = TargetWriter(spark, self.logger)
        self.transformer = Transformer(spark, dbutils, self.logger, self.audit)
        self.orch = OrchestratorCore(spark, dbutils, catalog, control_schema, self.logger, self.audit)
        self.results: List[Dict[str, Any]] = []

    def process_group(self, group_id: str, target_table: Optional[str] = None,
                      lob_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        gid = (group_id or "").strip().upper()
        self.logger.group_id = gid
        start_all = datetime.now()
        self.logger.info(f"=== L0 processing START for group '{gid}' ===", target=target_table or "ALL")
        records = self.orch.fetch_l0_details(gid, target_table, lob_filter)
        if not records:
            self.logger.warn(f"No L0 records returned for group '{gid}'")
            return []
        self.results = []
        failed = []
        for rec in records:
            res = self._process_one(gid, rec)
            self.results.append(res)
            if res["status"] != "SUCCESS":
                failed.append(res)
        duration = (datetime.now() - start_all).total_seconds()
        success_count = sum(1 for r in self.results if r["status"] == "SUCCESS")
        total = len(self.results)
        self.logger.info(
            f"=== L0 processing END for group '{gid}': {success_count}/{total} ok, {len(failed)} failed, duration={duration:.1f}s ==="
        )
        if failed:
            msgs = [f"{f['target_table']}: {f['message'][:150]}" for f in failed]
            raise RuntimeError(f"L0: {len(failed)} table(s) failed. Details:\n  - " + "\n  - ".join(msgs))
        return self.results

    def _process_one(self, group_id: str, rec: Dict[str, Any]) -> Dict[str, Any]:
        t0 = datetime.now()
        status = "RUNNING"
        message = ""
        rows = 0
        target_table_name = ""
        target_schema = ""
        extra: Dict[str, Any] = {}
        try:
            source = (rec.get("SOURCE") or "").strip()
            source_obj_schema = (rec.get("SOURCE_OBJ_SCHEMA") or "").strip()
            source_obj_name = (rec.get("SOURCE_OBJ_NAME") or "").strip()
            lob = (rec.get("LOB") or "").strip() or None
            load_type_raw = (rec.get("LOAD_TYPE") or "FULL").strip().upper()
            file_format = (rec.get("INPUT_FILE_FORMAT") or "csv").strip()
            storage_type = (rec.get("STORAGE_TYPE") or "").strip()
            dq_logic = (rec.get("DQ_LOGIC") or "").strip() or None
            delimeter = (rec.get("DELIMETER") or "").strip()
            custom_schema = (rec.get("CUSTOM_SCHEMA") or "").strip() or None
            cdc_logic = (rec.get("CDC_LOGIC") or "").strip() or None
            transform_query = rec.get("TRANSFORM_QUERY")
            prestag_flag = (rec.get("PRESTAG_FLAG") or "N").strip().upper()
            partition = (rec.get("PARTITION") or "").strip()
            ls_flag = (rec.get("LS_FLAG") or "N").strip().upper()
            ls_detail = (rec.get("LS_DETAIL") or "").strip() or None
            deployment_dfg = (rec.get("DEPLOYMENT_SOURCE_DFG") or "").strip() or None
            if load_type_raw == "DELTA":
                effective_load_type = "INCREMENTAL"
            elif load_type_raw == "OVERWRITE":
                effective_load_type = "FULL"
            else:
                effective_load_type = load_type_raw
            target_obj_no_ext = SparkUtils.strip_file_extension(source_obj_name)
            target_table_name = target_obj_no_ext
            target_schema = source_obj_schema
            target_full = SparkUtils.qualify_table_name(self.catalog, source_obj_schema, target_obj_no_ext)
            self.logger.info(
                f"L0: ingesting → {target_full}",
                source=source[:80], format=file_format, load=load_type_raw
            )
            extra.update({
                "source_preview": source[:200],
                "file_format": file_format,
                "storage_type": storage_type,
                "requested_load_type": load_type_raw,
                "effective_load_type": effective_load_type,
                "prestag_flag": prestag_flag,
                "ls_flag": ls_flag,
                "ls_detail": ls_detail[:200] if ls_detail else None,
                "deployment_source_dfg": deployment_dfg,
            })
            if ls_flag in ("B", "Y", "A"):
                if ls_detail and ls_flag in ("B", "Y"):
                    extra["ls_executed"] = "PRE"
                    self.logger.debug("Running PRE LS_DETAIL generic script")
                    try:
                        self.transformer.execute_generic_scripts(
                            ls_detail,
                            custom_script_params=rec.get("CUSTOM_SCRIPT_PARAMS"),
                            template_params={
                                "GROUP_ID": group_id,
                                "TARGET_SCHEMA": source_obj_schema,
                                "TARGET_TABLE": target_obj_no_ext,
                                "LOB": lob or "",
                                "ENV": self.env,
                                "CATALOG": self.catalog,
                            }
                        )
                    except Exception as e:
                        raise RuntimeError(f"PRE LS_DETAIL script failed: {e}") from e
            df = self.reader.read(
                source=source,
                file_format=file_format,
                delimiter=delimeter,
                custom_schema=custom_schema,
                custom_params=None,
                storage_type=storage_type,
            )
            extra["rows_after_read"] = int(df.count())
            extra["columns_after_read"] = list(df.columns)
            df, tq_info = self.transformer.apply_l0_transform_map(df, transform_query)
            if tq_info.get("changes_applied"):
                extra["transform_map_applied"] = True
            if tq_info.get("warnings"):
                extra["transform_map_warnings"] = tq_info["warnings"]
            watermark_ts = self.audit.get_watermark(group_id, target_obj_no_ext, self.LAYER)
            if watermark_ts:
                extra["watermark_ts"] = watermark_ts.isoformat()
            df, cdc_info = self.reader.apply_cdc_filter(df, cdc_logic, watermark_ts)
            extra["cdc"] = cdc_info
            df, dq_info = self.transformer.apply_dq_logic(df, dq_logic)
            extra["dq"] = dq_info
            df = self.transformer.add_audit_columns(
                df, group_id=group_id, layer=self.LAYER, lob=lob, environment=self.env,
                extra={
                    "source_name": source_obj_name,
                    "source_format": file_format,
                    "prestag": prestag_flag,
                    "deployment_dfg": deployment_dfg or "",
                }
            )
            partition_cols = SparkUtils.parse_csv_list(partition)
            extra["partition_cols"] = partition_cols
            rows = self.writer.write_table(
                df=df,
                catalog=self.catalog,
                schema=source_obj_schema,
                table=target_obj_no_ext,
                load_type=effective_load_type,
                merge_keys=None,
                partition_cols=partition_cols,
                partition_method=None,
                retention_days=SparkUtils.parse_retention_details(rec.get("RETENTION_DETAILS")),
                object_type="TABLE" if prestag_flag != "V" else "VIEW",
            )
            extra["rows_written"] = rows
            if ls_flag in ("A", "Y"):
                if ls_detail and ls_flag in ("A",):
                    extra["ls_executed"] = "POST"
                    self.logger.debug("Running POST LS_DETAIL generic script")
                    try:
                        self.transformer.execute_generic_scripts(
                            ls_detail,
                            custom_script_params=rec.get("CUSTOM_SCRIPT_PARAMS"),
                            template_params={
                                "GROUP_ID": group_id,
                                "TARGET_SCHEMA": source_obj_schema,
                                "TARGET_TABLE": target_obj_no_ext,
                                "LOB": lob or "",
                                "ENV": self.env,
                                "CATALOG": self.catalog,
                                "ROWS_WRITTEN": rows,
                            }
                        )
                    except Exception as e:
                        raise RuntimeError(f"POST LS_DETAIL script failed: {e}") from e
            status = "SUCCESS"
            message = f"L0 OK: {rows:,} rows written → {target_full}"
            self.logger.info(message, target=target_full, rows=rows)
        except Exception as e:
            status = "FAILED"
            error_short = f"{type(e).__name__}: {str(e)[:400]}"
            message = f"L0 FAIL: {error_short}"
            self.logger.error("L0 table ingestion failed", exception=e,
                            target=target_table_name, source_preview=extra.get("source_preview"))
        finally:
            t1 = datetime.now()
            self.audit.write(
                group_id=group_id,
                target_table=target_table_name or target_full,
                layer=self.LAYER,
                status=status,
                message=message,
                rows_processed=rows,
                start_time=t0,
                end_time=t1,
                lob=(rec.get("LOB") or "").strip() or None,
                environment=self.env,
                extra=extra,
            )
        return {
            "group_id": group_id,
            "target_schema": target_schema,
            "target_table": target_table_name,
            "status": status,
            "message": message,
            "rows_processed": rows,
            "start_time": t0,
            "end_time": datetime.now(),
            "extra": extra,
        }
