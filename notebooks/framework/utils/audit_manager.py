import json
from datetime import datetime
from typing import Any, Dict, Optional


class AuditManager:
    def __init__(self, spark, catalog: str = "demo_catalog", control_schema: str = "admin"):
        self.spark = spark
        self.catalog = catalog
        self.control_schema = control_schema
        self.audit_table = f"{catalog}.{control_schema}.audit_log"

    def _ensure_table_exists(self):
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.audit_table} (
                    DATA_FLOW_GROUP_ID STRING,
                    TARGET_TABLE STRING,
                    STATUS STRING,
                    MESSAGE STRING,
                    CREATED_DATE TIMESTAMP,
                    ETL_LAYER STRING,
                    ROWS_PROCESSED BIGINT,
                    START_TIME TIMESTAMP,
                    END_TIME TIMESTAMP,
                    LOAD_TS TIMESTAMP,
                    DURATION_SECONDS DOUBLE,
                    LOB STRING,
                    ENVIRONMENT STRING,
                    EXTRA_JSON STRING
                )
                USING DELTA
            """)
        except Exception as e:
            print(f"[AuditManager] WARN: Could not ensure audit table exists: {e}")

    def write(
        self,
        group_id: str,
        target_table: str,
        layer: str,
        status: str,
        message: str,
        rows_processed: int,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        lob: Optional[str] = None,
        environment: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
        propagate_error: bool = False
    ):
        try:
            self._ensure_table_exists()
            end = end_time or datetime.now()
            duration = (end - start_time).total_seconds()
            safe_msg = str(message).replace("'", "''")[:2000]
            safe_gid = str(group_id).replace("'", "''")[:200]
            safe_tbl = str(target_table).replace("'", "''")[:200]
            safe_layer = str(layer).replace("'", "''")[:10]
            safe_status = str(status).replace("'", "''")[:50]
            safe_lob = (str(lob).replace("'", "''")[:100]) if lob else None
            safe_env = (str(environment).replace("'", "''")[:50]) if environment else None
            extra_json = None
            if extra:
                try:
                    extra_json = json.dumps(extra, default=str).replace("'", "''")[:4000]
                except Exception:
                    extra_json = None

            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            end_str = end.strftime("%Y-%m-%d %H:%M:%S.%f")
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            lob_col = f"'{safe_lob}'" if safe_lob else "NULL"
            env_col = f"'{safe_env}'" if safe_env else "NULL"
            extra_col = f"'{extra_json}'" if extra_json else "NULL"
            rows_val = int(rows_processed) if rows_processed is not None else 0

            sql = f"""
                INSERT INTO {self.audit_table} (
                    DATA_FLOW_GROUP_ID,
                    TARGET_TABLE,
                    STATUS,
                    MESSAGE,
                    CREATED_DATE,
                    ETL_LAYER,
                    ROWS_PROCESSED,
                    START_TIME,
                    END_TIME,
                    LOAD_TS,
                    DURATION_SECONDS,
                    LOB,
                    ENVIRONMENT,
                    EXTRA_JSON
                ) VALUES (
                    '{safe_gid}',
                    '{safe_tbl}',
                    '{safe_status}',
                    '{safe_msg}',
                    '{now_str}',
                    '{safe_layer}',
                    {rows_val},
                    '{start_str}',
                    '{end_str}',
                    '{now_str}',
                    {duration},
                    {lob_col},
                    {env_col},
                    {extra_col}
                )
            """
            self.spark.sql(sql)
            return True
        except Exception as e:
            err_msg = f"[AuditManager] FAILED to write audit log: {type(e).__name__}: {str(e)[:200]}"
            print(err_msg)
            if propagate_error:
                raise RuntimeError(err_msg) from e
            return False

    def get_last_success(self, group_id: str, target_table: str, layer: str) -> Optional[Dict]:
        try:
            rows = self.spark.sql(f"""
                SELECT DATA_FLOW_GROUP_ID, TARGET_TABLE, STATUS, ROWS_PROCESSED,
                       START_TIME, END_TIME, LOAD_TS, DURATION_SECONDS, MESSAGE, ETL_LAYER
                FROM {self.audit_table}
                WHERE DATA_FLOW_GROUP_ID = '{group_id.replace("'", "''")}'
                  AND TARGET_TABLE = '{target_table.replace("'", "''")}'
                  AND STATUS = 'SUCCESS'
                  AND ETL_LAYER = '{layer.replace("'", "''")}'
                ORDER BY LOAD_TS DESC
                LIMIT 1
            """).collect()
            if rows:
                return rows[0].asDict()
            return None
        except Exception as e:
            print(f"[AuditManager] WARN: get_last_success failed: {e}")
            return None

    def get_watermark(self, group_id: str, target_table: str, layer: str) -> Optional[datetime]:
        last = self.get_last_success(group_id, target_table, layer)
        if last and last.get("LOAD_TS"):
            return last["LOAD_TS"]
        return None

    def get_run_history(self, group_id: str, layer: Optional[str] = None, limit: int = 50):
        try:
            where_clauses = [f"DATA_FLOW_GROUP_ID = '{group_id.replace("'", "''")}'"]
            if layer:
                where_clauses.append(f"ETL_LAYER = '{layer.replace("'", "''")}'")
            where_sql = " AND ".join(where_clauses)
            return self.spark.sql(f"""
                SELECT * FROM {self.audit_table}
                WHERE {where_sql}
                ORDER BY LOAD_TS DESC
                LIMIT {limit}
            """)
        except Exception as e:
            print(f"[AuditManager] WARN: get_run_history failed: {e}")
            return None
