# =============================================================================
# framework/audit_logger.py
# DataFrame-based audit writer for demo_catalog.admin.audit_log.
# Never raises — audit failure must not kill the ETL job.
# =============================================================================

from __future__ import annotations
from datetime import datetime
from typing import Optional


class AuditLogger:
    """
    Writes structured audit rows to demo_catalog.admin.audit_log.

    Uses the DataFrame API (not spark.sql string interpolation) to avoid
    SQL injection from multi-line error tracebacks and special characters
    in the MESSAGE field.

    Usage:
        logger = AuditLogger(spark, catalog="demo_catalog", run_id="12345", env="dev")
        logger.write(
            group_id   = "EMPLOYEE_MASTER_L0",
            table_name = "employee_master",
            layer      = "L0",
            status     = "SUCCESS",
            message    = "10000 rows loaded",
            rows       = 10000,
            start_time = t0,
            end_time   = t1,
            lob        = "HR",
        )
    """

    MAX_MSG_LEN    = 400
    CONTROL_SCHEMA = "admin"
    AUDIT_TABLE    = "audit_log"

    def __init__(self, spark,
                 catalog: str = "demo_catalog",
                 run_id: str  = "",
                 env: str     = "dev"):
        self.spark   = spark
        self.catalog = catalog
        self.run_id  = run_id or ""
        self.env     = env or "dev"
        self._tbl    = f"{catalog}.{self.CONTROL_SCHEMA}.{self.AUDIT_TABLE}"

    # ── Schema (must match create_control_tables.sql audit_log DDL) ───────

    @staticmethod
    def _get_schema():
        from pyspark.sql.types import (
            StructType, StructField,
            StringType, TimestampType, LongType, DoubleType
        )
        return StructType([
            StructField("DATA_FLOW_GROUP_ID", StringType(),   True),
            StructField("TARGET_TABLE",       StringType(),   True),
            StructField("STATUS",             StringType(),   True),
            StructField("MESSAGE",            StringType(),   True),
            StructField("CREATED_DATE",       TimestampType(), True),
            StructField("ETL_LAYER",          StringType(),   True),
            StructField("LOB",                StringType(),   True),
            StructField("ROWS_PROCESSED",     LongType(),     True),
            StructField("DURATION_SECONDS",   DoubleType(),   True),
            StructField("START_TIME",         TimestampType(), True),
            StructField("END_TIME",           TimestampType(), True),
            StructField("LOAD_TS",            TimestampType(), True),
            StructField("RUN_ID",             StringType(),   True),
            StructField("ENVIRONMENT",        StringType(),   True),
        ])

    # ── Public write method ───────────────────────────────────────────────

    def write(
        self,
        group_id:   str,
        table_name: str,
        layer:      str,
        status:     str,
        message:    str,
        rows:       int,
        start_time: datetime,
        end_time:   datetime,
        lob:        str = "UNKNOWN",
    ) -> None:
        """
        Appends one row to audit_log. Never raises.
        All exceptions are caught and printed (non-fatal).
        """
        try:
            from pyspark.sql import Row

            safe_msg  = str(message or "")[:self.MAX_MSG_LEN]
            duration  = (end_time - start_time).total_seconds()
            now       = datetime.now()

            row = Row(
                DATA_FLOW_GROUP_ID = str(group_id   or ""),
                TARGET_TABLE       = str(table_name or ""),
                STATUS             = str(status     or ""),
                MESSAGE            = safe_msg,
                CREATED_DATE       = now,
                ETL_LAYER          = str(layer or ""),
                LOB                = str(lob   or "UNKNOWN"),
                ROWS_PROCESSED     = int(rows  or 0),
                DURATION_SECONDS   = float(duration),
                START_TIME         = start_time,
                END_TIME           = end_time,
                LOAD_TS            = now,
                RUN_ID             = str(self.run_id or ""),
                ENVIRONMENT        = str(self.env    or ""),
            )

            (self.spark
             .createDataFrame([row], schema=self._get_schema())
             .write
             .format("delta")
             .mode("append")
             .saveAsTable(self._tbl))

        except Exception as e:
            # Audit failure MUST NOT kill the ETL job.
            print(
                f"  ⚠ AUDIT WRITE FAILED (non-fatal) for [{group_id}/{table_name}]: "
                f"{type(e).__name__}: {str(e)[:200]}"
            )

    # ── Convenience: write RUNNING status at job start ────────────────────

    def write_running(self, group_id: str, layer: str) -> datetime:
        """
        Writes a RUNNING row at pipeline start. Returns start_time.
        Useful for SLA monitoring queries against audit_log.
        """
        t0 = datetime.now()
        self.write(
            group_id   = group_id,
            table_name = "PIPELINE",
            layer      = layer,
            status     = "RUNNING",
            message    = f"Pipeline started",
            rows       = 0,
            start_time = t0,
            end_time   = t0,
        )
        return t0
