from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..utils.structured_logger import StructuredLogger
from ..utils.audit_manager import AuditManager
from ..utils.spark_utils import SparkUtils
from ..utils.retry_handler import RetryHandler
from ..utils.schema_validator import SchemaValidator
from ..core.source_reader import SourceReader
from ..core.target_writer import TargetWriter
from ..core.transformer import Transformer
from ..core.orchestrator_core import OrchestratorCore


class L1L2Processor:
    def __init__(self, layer: str, spark, dbutils=None,
                 catalog: str = "demo_catalog",
                 control_schema: str = "admin",
                 logger: Optional[StructuredLogger] = None,
                 audit_manager: Optional[AuditManager] = None,
                 environment: Optional[str] = None):
        if layer not in ("L1", "L2"):
            raise ValueError(f"L1L2Processor layer must be 'L1' or 'L2', got '{layer}'")
        self.LAYER = layer
        self.spark = spark
        self.dbutils = dbutils
        self.catalog = catalog
        self.control_schema = control_schema
        self.env = environment or "dev"
        self.logger = logger or StructuredLogger(catalog=catalog, control_schema=control_schema)
        self.logger.layer = self.LAYER
        self.audit = audit_manager or AuditManager(spark, catalog, control_schema)
        self.writer = TargetWriter(spark, self.logger)
        self.transformer = Transformer(spark, dbutils, self.logger, self.audit)
        self.orch = OrchestratorCore(spark, dbutils, catalog, control_schema, self.logger, self.audit)
        self.results: List[Dict[str, Any]] = []

    def process_group(self, group_id: str, target_table: Optional[str] = None,
                      lob_filter: Optional[str] = None,
                      run_by_priority: bool = True) -> List[Dict[str, Any]]:
        gid = (group_id or "").strip().upper()
        self.logger.group_id = gid
        start_all = datetime.now()
        self.logger.info(
            f"=== {self.LAYER} processing START for group '{gid}' ===",
            target=target_table or "ALL"
        )
        records = self.orch.fetch_pb_details(gid, target_table, lob_filter, self.LAYER)
        if not records:
            self.logger.warn(f"No {self.LAYER} records returned for group '{gid}'")
            return []
        self.results = []
        failed = []
        if run_by_priority:
            buckets = OrchestratorCore.group_by_priority(records)
            for prio, recs in buckets.items():
                self.logger.info(f"{self.LAYER}: processing priority={prio} ({len(recs)} table(s) parallel)")
                for rec in recs:
                    res = self._process_one(gid, rec)
                    self.results.append(res)
                    if res["status"] != "SUCCESS":
                        failed.append(res)
        else:
            for rec in records:
                res = self._process_one(gid, rec)
                self.results.append(res)
                if res["status"] != "SUCCESS":
                    failed.append(res)
        duration = (datetime.now() - start_all).total_seconds()
        success_count = sum(1 for r in self.results if r["status"] == "SUCCESS")
        total = len(self.results)
        self.logger.info(
            f"=== {self.LAYER} processing END for group '{gid}': {success_count}/{total} ok, "
            f"{len(failed)} failed, duration={duration:.1f}s ==="
        )
        if failed:
            msgs = [f"{f['target_table']}: {f['message'][:150]}" for f in failed]
            raise RuntimeError(
                f"{self.LAYER}: {len(failed)} table(s) failed. Details:\n  - " + "\n  - ".join(msgs)
            )
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
            lob = (rec.get("LOB") or "").strip() or None
            source = (rec.get("SOURCE") or "").strip()
            target_obj_schema = (rec.get("TARGET_OBJ_SCHEMA") or "").strip()
            target_obj_name = (rec.get("TARGET_OBJ_NAME") or "").strip()
            priority_raw = rec.get("PRIORITY")
            target_obj_type = (rec.get("TARGET_OBJ_TYPE") or "TABLE").strip().upper()
            transform_query = (rec.get("TRANSFORM_QUERY") or "").strip()
            generic_scripts = (rec.get("GENERIC_SCRIPTS") or "").strip()
            source_pk = (rec.get("SOURCE_PK") or "").strip()
            target_pk = (rec.get("TARGET_PK") or "").strip()
            load_type_raw = (rec.get("LOAD_TYPE") or "FULL").strip().upper()
            ls_flag = (rec.get("LS_FLAG") or "N").strip().upper()
            ls_detail = (rec.get("LS_DETAIL") or "").strip() or None
            partition_or_index = (rec.get("PARTITION_OR_INDEX") or "").strip()
            custom_script_params = rec.get("CUSTOM_SCRIPT_PARAMS")
            partition_method = (rec.get("PARTITION_METHOD") or "").strip().upper() or None
            retention_details = rec.get("RETENTION_DETAILS")
            deployment_dfg = (rec.get("DEPLOYMENT_SOURCE_DFG") or "").strip() or None
            if target_obj_type == "MV":
                effective_obj_type = "TABLE"
            else:
                effective_obj_type = target_obj_type
            if target_obj_type == "MV":
                effective_load_type = "FULL"
            elif target_obj_type == "VIEW":
                effective_load_type = "FULL"
            elif load_type_raw == "DELTA":
                effective_load_type = "INCREMENTAL"
            elif load_type_raw == "OVERWRITE":
                effective_load_type = "FULL"
            elif load_type_raw == "SCD":
                effective_load_type = "SCD2"
            else:
                effective_load_type = load_type_raw
            target_schema = target_obj_schema
            target_table_name = target_obj_name
            target_full = SparkUtils.qualify_table_name(self.catalog, target_obj_schema, target_obj_name)
            merge_keys_raw = target_pk or source_pk or ""
            merge_keys = SparkUtils.parse_csv_list(merge_keys_raw) if effective_load_type in ("MERGE", "SCD2") else None
            extra.update({
                "priority": priority_raw,
                "source_schema_ref": source,
                "target_object_type": target_obj_type,
                "requested_load_type": load_type_raw,
                "effective_load_type": effective_load_type,
                "source_pk": SparkUtils.parse_csv_list(source_pk),
                "target_pk": SparkUtils.parse_csv_list(target_pk),
                "merge_keys": merge_keys,
                "partition_cols": SparkUtils.parse_csv_list(partition_or_index),
                "partition_method": partition_method,
                "ls_flag": ls_flag,
                "ls_detail": ls_detail[:200] if ls_detail else None,
                "deployment_source_dfg": deployment_dfg,
                "retention_details": retention_details,
            })
            self.logger.info(
                f"{self.LAYER}: building → {target_full}",
                type=target_obj_type, load=load_type_raw, prio=priority_raw, merge_keys=merge_keys
            )
            if ls_flag in ("B", "Y"):
                if generic_scripts and ls_flag in ("B",):
                    extra["generic_scripts_run"] = "PRE"
                    self.logger.debug("Running PRE GENERIC_SCRIPTS")
                    try:
                        self.transformer.execute_generic_scripts(
                            generic_scripts,
                            custom_script_params=custom_script_params,
                            template_params={
                                "GROUP_ID": group_id,
                                "TARGET_SCHEMA": target_obj_schema,
                                "TARGET_TABLE": target_obj_name,
                                "SOURCE": source or "",
                                "LOB": lob or "",
                                "ENV": self.env,
                                "CATALOG": self.catalog,
                                "LOAD_TYPE": load_type_raw,
                            }
                        )
                    except Exception as e:
                        raise RuntimeError(f"PRE GENERIC_SCRIPTS failed: {e}") from e
                if ls_detail and ls_flag in ("B", "Y"):
                    extra["ls_executed"] = "PRE"
                    try:
                        self.transformer.execute_generic_scripts(
                            ls_detail,
                            custom_script_params=custom_script_params,
                            template_params={
                                "GROUP_ID": group_id,
                                "TARGET_SCHEMA": target_obj_schema,
                                "TARGET_TABLE": target_obj_name,
                                "LOB": lob or "",
                                "ENV": self.env,
                                "CATALOG": self.catalog,
                            }
                        )
                    except Exception as e:
                        raise RuntimeError(f"PRE LS_DETAIL script failed: {e}") from e
            if not transform_query and not generic_scripts:
                if not source:
                    raise ValueError(
                        f"{self.LAYER}: TRANSFORM_QUERY empty and GENERIC_SCRIPTS empty and SOURCE empty. "
                        f"Cannot build {target_full}. Provide TRANSFORM_QUERY SELECT * FROM ..."
                    )
                transform_query = (
                    f"SELECT *, current_timestamp() AS ingestion_timestamp "
                    f"FROM {self.catalog}.{source}.{target_obj_name}"
                )
                self.logger.warn(f"TRANSFORM_QUERY empty; using default copy query from SOURCE schema",
                               default_preview=transform_query[:120])
            if transform_query:
                template_params = {
                    "GROUP_ID": group_id,
                    "TARGET_SCHEMA": target_obj_schema,
                    "TARGET_TABLE": target_obj_name,
                    "SOURCE": source or "",
                    "LOB": lob or "",
                    "ENV": self.env,
                    "CATALOG": self.catalog,
                    "LOAD_TYPE": load_type_raw,
                    "RUN_TS": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                try:
                    df, tf_info = self.transformer.execute_transform_sql(
                        transform_query=transform_query,
                        catalog=self.catalog,
                        default_schema=source or target_obj_schema,
                        template_params=template_params,
                        source_df=None,
                    )
                except Exception as e:
                    raise RuntimeError(f"TRANSFORM_QUERY execution failed: {e}") from e
                if tf_info:
                    extra.update({
                        f"tf_{k}": v for k, v in tf_info.items() if k not in ("original_query",)
                    })
            else:
                df = self.spark.sql(f"SELECT current_timestamp() AS ingestion_timestamp WHERE 1=0")
                extra["transform_empty"] = True
            extra["rows_after_transform"] = int(df.count())
            extra["columns_after_transform"] = list(df.columns)
            if effective_load_type in ("MERGE", "SCD2") and merge_keys:
                merge_keys_exist = [k for k in merge_keys if k in df.columns]
                missing = [k for k in merge_keys if k not in df.columns]
                if missing:
                    raise ValueError(
                        f"{self.LAYER}: MERGE/SCD2 keys not present in output: {missing}. "
                        f"Available: {list(df.columns)[:30]}"
                    )
                extra["merge_keys_present"] = merge_keys_exist
            df = self.transformer.add_audit_columns(
                df, group_id=group_id, layer=self.LAYER, lob=lob, environment=self.env,
                extra={
                    "priority": str(priority_raw),
                    "target_obj_type": target_obj_type,
                    "source_ref": source or "",
                }
            )
            retention_days = SparkUtils.parse_retention_details(retention_details)
            part_cols = SparkUtils.parse_csv_list(partition_or_index)
            rows = self.writer.write_table(
                df=df,
                catalog=self.catalog,
                schema=target_obj_schema,
                table=target_obj_name,
                load_type=effective_load_type,
                merge_keys=merge_keys,
                partition_cols=part_cols,
                partition_method=partition_method,
                retention_days=retention_days,
                object_type=effective_obj_type,
            )
            extra["rows_written"] = rows
            if ls_flag in ("A", "Y"):
                if generic_scripts and ls_flag in ("A",):
                    extra["generic_scripts_run"] = "POST"
                    self.logger.debug("Running POST GENERIC_SCRIPTS")
                    try:
                        self.transformer.execute_generic_scripts(
                            generic_scripts,
                            custom_script_params=custom_script_params,
                            template_params={
                                "GROUP_ID": group_id,
                                "TARGET_SCHEMA": target_obj_schema,
                                "TARGET_TABLE": target_obj_name,
                                "SOURCE": source or "",
                                "LOB": lob or "",
                                "ENV": self.env,
                                "CATALOG": self.catalog,
                                "LOAD_TYPE": load_type_raw,
                                "ROWS_WRITTEN": rows,
                            }
                        )
                    except Exception as e:
                        raise RuntimeError(f"POST GENERIC_SCRIPTS failed: {e}") from e
                if ls_detail and ls_flag in ("A",):
                    extra["ls_executed"] = "POST"
                    try:
                        self.transformer.execute_generic_scripts(
                            ls_detail,
                            custom_script_params=custom_script_params,
                            template_params={
                                "GROUP_ID": group_id,
                                "TARGET_SCHEMA": target_obj_schema,
                                "TARGET_TABLE": target_obj_name,
                                "LOB": lob or "",
                                "ENV": self.env,
                                "CATALOG": self.catalog,
                                "ROWS_WRITTEN": rows,
                            }
                        )
                    except Exception as e:
                        raise RuntimeError(f"POST LS_DETAIL script failed: {e}") from e
            status = "SUCCESS"
            message = f"{self.LAYER} OK: {rows:,} rows → {target_full}"
            self.logger.info(message, target=target_full, rows=rows)
        except Exception as e:
            status = "FAILED"
            error_short = f"{type(e).__name__}: {str(e)[:400]}"
            message = f"{self.LAYER} FAIL: {error_short}"
            self.logger.error(f"{self.LAYER} table build failed", exception=e,
                            target=target_table_name, transform_preview=extra.get("tf_output_columns"))
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
            "priority": rec.get("PRIORITY"),
            "extra": extra,
        }
