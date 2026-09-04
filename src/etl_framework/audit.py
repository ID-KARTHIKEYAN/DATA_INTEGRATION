"""audit_log writer using only columns present in the attached INSERT statement."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from etl_framework.config import AUDIT_TABLE, CONTROL_SCHEMA
from etl_framework.exceptions import FrameworkError
from etl_framework.identifiers import require_ident
from etl_framework.logging_utils import log_event


def _esc(value: str | None) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "''")


def write_audit(
    spark: SparkSession,
    *,
    catalog: str,
    group_id: str,
    target_table: str,
    status: str,
    message: str,
    etl_layer: str | None,
    rows_processed: int | None,
    start_time: datetime | None,
    end_time: datetime | None,
) -> None:
    """
    Insert one audit_log row.
    If the audit insert itself fails, raise FrameworkError — do not hide the original failure.
    Message is truncated to 4000 chars to stay within typical STRING cell limits.
    """
    cat = require_ident(catalog, "catalog")
    table = f"{cat}.{CONTROL_SCHEMA}.{AUDIT_TABLE}"
    msg = (message or "")[:4000]
    rows_sql = "NULL" if rows_processed is None else str(int(rows_processed))
    layer_sql = "NULL" if not etl_layer else f"'{_esc(etl_layer)}'"
    start_sql = "NULL" if start_time is None else f"TIMESTAMP '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'"
    end_sql = "NULL" if end_time is None else f"TIMESTAMP '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
    sql = f"""
        INSERT INTO {table} (
            DATA_FLOW_GROUP_ID, TARGET_TABLE, STATUS, MESSAGE, CREATED_DATE,
            ETL_LAYER, ROWS_PROCESSED, START_TIME, END_TIME, LOAD_TS
        )
        VALUES (
            '{_esc(group_id)}',
            '{_esc(target_table)}',
            '{_esc(status)}',
            '{_esc(msg)}',
            current_timestamp(),
            {layer_sql},
            {rows_sql},
            {start_sql},
            {end_sql},
            current_timestamp()
        )
    """
    try:
        spark.sql(sql)
        log_event(
            "audit_written",
            group_id=group_id,
            target_table=target_table,
            status=status,
            etl_layer=etl_layer,
            rows_processed=rows_processed,
        )
    except Exception as exc:  # noqa: BLE001
        log_event("audit_write_failed", error=str(exc), group_id=group_id, status=status)
        raise FrameworkError(f"audit_log insert failed: {exc}") from exc


def last_success_load_ts(
    spark: SparkSession,
    *,
    catalog: str,
    group_id: str,
    target_table: str,
) -> datetime | None:
    """Watermark helper using existing audit_log.LOAD_TS (SUCCESS rows only)."""
    cat = require_ident(catalog, "catalog")
    table = f"{cat}.{CONTROL_SCHEMA}.{AUDIT_TABLE}"
    row = spark.sql(
        f"""
        SELECT max(LOAD_TS) AS wm
        FROM {table}
        WHERE DATA_FLOW_GROUP_ID = '{_esc(group_id)}'
          AND TARGET_TABLE = '{_esc(target_table)}'
          AND STATUS = 'SUCCESS'
        """
    ).collect()
    if not row or row[0]["wm"] is None:
        return None
    return row[0]["wm"]
