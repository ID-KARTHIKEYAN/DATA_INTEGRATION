from datetime import datetime, timezone

from .errors import FrameworkError


class AuditLogger:
    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.table = f"{catalog}.admin.audit_log"

    def _write(self, task, target, status, message, rows, started):
        ended = datetime.now(timezone.utc)
        data = [(task.group_id, target, status, message, task.layer, rows, started, ended, ended)]
        columns = ["DATA_FLOW_GROUP_ID", "TARGET_TABLE", "STATUS", "MESSAGE", "ETL_LAYER", "ROWS_PROCESSED", "START_TIME", "END_TIME", "LOAD_TS"]
        try:
            self.spark.createDataFrame(data, columns).write.mode("append").saveAsTable(self.table)
        except Exception as exc:
            raise FrameworkError(f"Unable to write audit_log for {task.task_id}: {exc}") from exc

    def last_success(self, task):
        rows = self.spark.sql(
            f"SELECT MAX(LOAD_TS) AS last_load_ts FROM {self.table} "
            "WHERE DATA_FLOW_GROUP_ID = '{}' AND TARGET_TABLE = '{}' "
            "AND ETL_LAYER = '{}' AND STATUS = 'SUCCESS'".format(
                task.group_id.replace("'", "''"),
                task.row.get("SOURCE_OBJ_NAME", task.row.get("TARGET_OBJ_NAME", "")).replace("'", "''"),
                task.layer,
            )
        ).collect()
        return rows[0]["last_load_ts"] if rows else None

    def success(self, task, target, rows, started):
        self._write(task, target, "SUCCESS", "Completed", rows, started)

    def failure(self, task, target, message, started):
        self._write(task, target, "FAILED", message[:16000], 0, started)