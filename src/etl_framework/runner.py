from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from etl_framework import config
from etl_framework.audit import last_success_load_ts, write_audit
from etl_framework.exceptions import FrameworkError, LoadExecutionError
from etl_framework.generic_scripts import run_generic_scripts
from etl_framework.identifiers import fqn, sanitize_object_name
from etl_framework.io_source import read_l0_source
from etl_framework.logging_utils import log_event
from etl_framework.metadata import DetailRow, HeaderRow, MetadataStore
from etl_framework.spark_session import get_spark
from etl_framework.transforms import (
    add_load_ts,
    apply_cdc_filter,
    apply_dq,
    apply_l0_transform_map,
    parse_column_list,
)
from etl_framework.validation import apply_spark_configs, normalize_load_type, validate_header, validate_l0_row, validate_pb_row
from etl_framework.writers import apply_retention, pk_list, write_frame


def _catalog(header: HeaderRow, fallback: str) -> str:
    return str(header["target_catalog"] or fallback)


def _maybe_watermark_cdc(cdc: object | None, watermark) -> str | None:
    text = str(cdc or "").strip()
    if not text:
        return None
    if "{{LAST_SUCCESS_LOAD_TS}}" in text:
        ts = watermark.strftime("%Y-%m-%d %H:%M:%S") if watermark is not None else "1900-01-01 00:00:00"
        text = text.replace("{{LAST_SUCCESS_LOAD_TS}}", ts)
    return text


class LayerRunner:
    def __init__(
        self,
        *,
        control_catalog: str,
        scripts_root: Path,
        free_edition: bool = True,
    ) -> None:
        self.spark = get_spark()
        self.control_catalog = control_catalog
        self.store = MetadataStore(self.spark, control_catalog)
        self.scripts_root = scripts_root
        self.free_edition = free_edition

    def run_group(
        self,
        *,
        group_id: str,
        layer: str,
        target_object: str = "",
        environment: str = "dev",
    ) -> list[str]:
        header = self.store.get_header(group_id, layer)
        resolved_layer = validate_header(header, requested_layer=layer, free_edition=self.free_edition)
        apply_spark_configs(self.spark, header["SPARK_CONFIGS"])
        catalog = _catalog(header, self.control_catalog)
        log_event(
            "run_group_start",
            group_id=group_id,
            layer=resolved_layer,
            trigger=header["TRIGGER_TYPE"],
            target_catalog=catalog,
            compute=header["COMPUTE_CLASS_DEV"] if environment.lower() == "dev" else header["COMPUTE_CLASS"],
            target_object=target_object or "ALL",
        )
        if resolved_layer == config.LAYERS_L0:
            rows = self.store.list_l0(group_id, target_object)
            for row in rows:
                self._run_l0(header, row, catalog)
        else:
            rows = self.store.list_pb(group_id, target_object)
            for row in rows:
                self._run_pb(header, row, catalog, resolved_layer)
        if not rows:
            raise LoadExecutionError(
                f"No active detail rows for DATA_FLOW_GROUP_ID={group_id} ETL_LAYER={resolved_layer}"
            )
        return [r.task_key for r in rows]

    def _run_l0(self, header: HeaderRow, row: DetailRow, catalog: str) -> None:
        validate_l0_row(row, free_edition=self.free_edition)
        table = sanitize_object_name(str(row.get("SOURCE_OBJ_NAME")))
        schema = str(row.get("SOURCE_OBJ_SCHEMA"))
        target = fqn(catalog, schema, table)
        self._guarded(header, row, target, config.LAYERS_L0, lambda: self._l0_body(header, row, catalog, schema, table))

    def _l0_body(self, header: HeaderRow, row: DetailRow, catalog: str, schema: str, table: str) -> int:
        df = read_l0_source(self.spark, row.raw)
        load_type = normalize_load_type(
            row.get("LOAD_TYPE"), allowed={"FULL", "DELTA"}, field="L0 LOAD_TYPE"
        )
        wm = last_success_load_ts(
            self.spark, catalog=self.control_catalog, group_id=str(row.get("DATA_FLOW_GROUP_ID")), target_table=table
        )
        cdc = _maybe_watermark_cdc(row.get("CDC_LOGIC"), wm)
        df = apply_cdc_filter(df, cdc, load_type)
        df = apply_l0_transform_map(df, row.get("TRANSFORM_QUERY"), row.get("LS_FLAG"))
        df = apply_dq(df, row.get("DQ_LOGIC"), table)
        df = add_load_ts(df)
        keys = parse_column_list(None)
        # L0 has no PK columns; DELTA without CDC keys appends. If CDC_LOGIC references merge, still append unless
        # TRANSFORM_QUERY/CDC implies keys — do not invent PKs.
        partition_cols = parse_column_list(row.get("PARTITION"))
        return write_frame(
            self.spark,
            df,
            catalog=catalog,
            schema=schema,
            table=table,
            load_type="FULL" if load_type == "FULL" else "DELTA",
            keys=keys,
            partition_cols=partition_cols,
            partition_method=None,
        )

    def _run_pb(self, header: HeaderRow, row: DetailRow, catalog: str, layer: str) -> None:
        validate_pb_row(row, layer=layer, free_edition=self.free_edition)
        schema = str(row.get("TARGET_OBJ_SCHEMA"))
        table = str(row.get("TARGET_OBJ_NAME"))
        target = fqn(catalog, schema, table)
        self._guarded(header, row, target, layer, lambda: self._pb_body(header, row, catalog, schema, table, layer))

    def _pb_body(self, header: HeaderRow, row: DetailRow, catalog: str, schema: str, table: str, layer: str) -> int:
        obj_type = str(row.get("TARGET_OBJ_TYPE") or "Table").strip().upper()
        tq = str(row.get("TRANSFORM_QUERY") or "").strip()
        if obj_type == "MV":
            raise LoadExecutionError("MV should have been rejected in validation")
        if obj_type == "VIEW":
            # Not in describe allowed values; refuse rather than CREATE TABLE.
            raise LoadExecutionError("TARGET_OBJ_TYPE=VIEW is not in metadata describe (Table or MV)")
        context = {
            "header": header.raw,
            "row": row.raw,
            "catalog": catalog,
            "layer": layer,
        }
        run_generic_scripts(
            scripts_field=row.get("GENERIC_SCRIPTS"),
            params=row.get("CUSTOM_SCRIPT_PARAMS"),
            spark=self.spark,
            context=context,
            scripts_root=self.scripts_root,
        )
        if str(row.get("LS_FLAG") or "").upper() == "Y":
            source_schema = str(row.get("SOURCE") or "").strip()
            if not source_schema:
                raise LoadExecutionError("LS_FLAG=Y requires SOURCE schema name on data_flow_pb_detail")
            # Lift-shift: read source object named identically unless LS_DETAIL overrides table name
            src_table = str(row.get("LS_DETAIL") or table).strip() or table
            df = self.spark.table(fqn(catalog, source_schema, src_table))
        else:
            if not tq:
                return 0
            try:
                df = self.spark.sql(tq)
            except Exception as exc:  # noqa: BLE001
                raise LoadExecutionError(
                    f"TRANSFORM_QUERY failed for {schema}.{table}. "
                    f"Unresolved columns or invalid SQL: {exc}"
                ) from exc
        df = add_load_ts(df)
        load_type = normalize_load_type(
            row.get("LOAD_TYPE"), allowed={"FULL", "DELTA", "SCD"}, field="PB LOAD_TYPE"
        )
        keys = pk_list(row.get("SOURCE_PK"), row.get("TARGET_PK"))
        part_cols = parse_column_list(row.get("PARTITION_OR_INDEX"))
        method = str(row.get("PARTITION_METHOD") or "").strip().upper() or None
        rows = write_frame(
            self.spark,
            df,
            catalog=catalog,
            schema=schema,
            table=table,
            load_type=load_type,
            keys=keys,
            partition_cols=part_cols,
            partition_method=method,
            scd=load_type == "SCD",
        )
        apply_retention(self.spark, fqn(catalog, schema, table), row.get("RETENTION_DETAILS"))
        return rows

    def _guarded(self, header: HeaderRow, row: DetailRow, target: str, layer: str, fn) -> None:
        group_id = str(row.get("DATA_FLOW_GROUP_ID") or header["DATA_FLOW_GROUP_ID"])
        start = datetime.now(timezone.utc).replace(tzinfo=None)
        write_audit(
            self.spark,
            catalog=self.control_catalog,
            group_id=group_id,
            target_table=target,
            status=config.STATUS_STARTED,
            message=f"{layer} started task={row.task_key}",
            etl_layer=layer,
            rows_processed=None,
            start_time=start,
            end_time=None,
        )
        try:
            count = fn()
            end = datetime.now(timezone.utc).replace(tzinfo=None)
            duration_min = (end - start).total_seconds() / 60.0
            warn = header["WARNING_THRESHOLD_MINS"]
            msg = f"{layer} success task={row.task_key}"
            if warn is not None and duration_min > float(warn):
                msg = (
                    f"WARNING threshold {warn} min exceeded (ran {duration_min:.1f} min); "
                    f"WARNING_DL_GROUP={header['WARNING_DL_GROUP']}; {msg}"
                )
            write_audit(
                self.spark,
                catalog=self.control_catalog,
                group_id=group_id,
                target_table=target,
                status=config.STATUS_SUCCESS,
                message=msg,
                etl_layer=layer,
                rows_processed=count,
                start_time=start,
                end_time=end,
            )
        except Exception as exc:
            end = datetime.now(timezone.utc).replace(tzinfo=None)
            write_audit(
                self.spark,
                catalog=self.control_catalog,
                group_id=group_id,
                target_table=target,
                status=config.STATUS_FAILED,
                message=f"{layer}: {type(exc).__name__}: {exc}",
                etl_layer=layer,
                rows_processed=None,
                start_time=start,
                end_time=end,
            )
            if not isinstance(exc, FrameworkError):
                raise LoadExecutionError(str(exc)) from exc
            raise
