import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional


class SparkUtils:
    def __init__(self, spark=None, dbutils=None):
        self.spark = spark
        self.dbutils = dbutils

    def set_spark_configs(self, config_str: Optional[str], logger=None) -> Dict[str, Any]:
        applied = {}
        failures = {}
        if not self.spark:
            return applied
        if not config_str or not str(config_str).strip():
            return applied
        config_items: Dict[str, str] = {}
        try:
            config_items = json.loads(config_str)
            if not isinstance(config_items, dict):
                config_items = {}
                raise ValueError("not a dict")
        except Exception:
            raw = str(config_str).strip()
            for line in raw.split(";"):
                line = line.strip()
                if not line or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                config_items[k.strip()] = v.strip()
        for key, value in config_items.items():
            try:
                self.spark.conf.set(key, str(value))
                applied[key] = value
                if logger:
                    logger.debug(f"Spark config set: {key}={value}")
            except Exception as e:
                failures[key] = str(e)
                if logger:
                    logger.warn(f"Failed to set spark config {key}={value}: {e}")
        return {"applied": applied, "failed": failures}

    @staticmethod
    def strip_file_extension(filename: str) -> str:
        if not filename:
            return ""
        name = str(filename).strip()
        for ext in (".csv", ".tsv", ".json", ".parquet", ".delta", ".avro",
                     ".orc", ".xml", ".xlsx", ".xls", ".gz", ".bz2", ".zip"):
            if name.lower().endswith(ext):
                name = name[:-len(ext)]
        return name

    @staticmethod
    def parse_csv_list(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(x).strip() for x in value if str(x).strip()]
        s = str(value).strip()
        if not s:
            return []
        return [x.strip() for x in s.split(",") if x.strip()]

    @staticmethod
    def parse_map_param(value: Any) -> Dict[str, str]:
        if not value:
            return {}
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items()}
        s = str(value).strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items()}
        except Exception:
            pass
        result = {}
        for item in s.split(";"):
            item = item.strip()
            if not item or "=" not in item:
                continue
            k, v = item.split("=", 1)
            result[k.strip()] = v.strip()
        return result

    @staticmethod
    def qualify_table_name(catalog: str, schema: str, table: str) -> str:
        parts = [p for p in [catalog, schema, table] if p]
        return ".".join(parts)

    @staticmethod
    def parse_retention_details(retention_str: Any) -> Optional[int]:
        if retention_str is None:
            return None
        s = str(retention_str).strip()
        if not s:
            return None
        m = re.search(r"(\d+)", s)
        if m:
            try:
                days = int(m.group(1))
                return days if days > 0 else None
            except Exception:
                return None
        return None

    @staticmethod
    def apply_sql_catalog_prefix(sql: str, catalog: str, schema: Optional[str] = None) -> str:
        if not sql or not catalog:
            return sql or ""
        if not schema:
            return sql
        patterns = [
            (r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
             lambda m: f"FROM {catalog}.{m.group(1)}.{m.group(2)}"
             if not m.group(1).lower() in (catalog.lower(),) else m.group(0)),
            (r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
             lambda m: f"JOIN {catalog}.{m.group(1)}.{m.group(2)}"
             if not m.group(1).lower() in (catalog.lower(),) else m.group(0)),
            (r"\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
             lambda m: f"UPDATE {catalog}.{m.group(1)}.{m.group(2)}"
             if not m.group(1).lower() in (catalog.lower(),) else m.group(0)),
            (r"\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
             lambda m: f"DELETE FROM {catalog}.{m.group(1)}.{m.group(2)}"
             if not m.group(1).lower() in (catalog.lower(),) else m.group(0)),
            (r"\bMERGE\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
             lambda m: f"MERGE INTO {catalog}.{m.group(1)}.{m.group(2)}"
             if not m.group(1).lower() in (catalog.lower(),) else m.group(0)),
            (r"\bUSING\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
             lambda m: f"USING {catalog}.{m.group(1)}.{m.group(2)}"
             if not m.group(1).lower() in (catalog.lower(),) else m.group(0)),
        ]
        result = sql
        for pattern, repl in patterns:
            try:
                result = re.sub(pattern, repl, result, flags=re.IGNORECASE)
            except Exception:
                continue
        return result

    @staticmethod
    def inject_template_params(text: str, params: Dict[str, Any]) -> str:
        if not text or not params:
            return text or ""
        result = str(text)
        for key, val in params.items():
            for placeholder in [f"${{{key}}}", f"$${key}$$", f"{{{key}}}", f"%{key}%"]:
                if placeholder in result:
                    result = result.replace(placeholder, str(val))
        return result

    def get_notebook_context(self):
        if not self.dbutils:
            return {}
        try:
            ctx = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            return {
                "browser_host_name": ctx.browserHostName().get() if hasattr(ctx, 'browserHostName') else None,
                "api_token": ctx.apiToken().get() if hasattr(ctx, 'apiToken') else None,
                "job_id": ctx.jobId().get() if hasattr(ctx, 'jobId') else None,
                "run_id": ctx.runId().get() if hasattr(ctx, 'runId') else None,
                "notebook_path": ctx.notebookPath().get() if hasattr(ctx, 'notebookPath') else None,
            }
        except Exception:
            return {}

    @staticmethod
    def ts_label(ts: Optional[datetime] = None) -> str:
        t = ts or datetime.now()
        return t.strftime("%Y%m%d_%H%M%S_%f")[:22]

    @staticmethod
    def safe_identifier(name: str) -> str:
        s = str(name or "").strip()
        s = re.sub(r"[^A-Za-z0-9_]", "_", s)
        if re.match(r"^\d", s):
            s = "_" + s
        return s[:128]
