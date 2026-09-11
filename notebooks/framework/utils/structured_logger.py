import json
import traceback
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StructuredLogger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, group_id: Optional[str] = None, layer: Optional[str] = None,
                 catalog: Optional[str] = None, control_schema: Optional[str] = None):
        if self._initialized:
            if group_id:
                self.group_id = group_id
            if layer:
                self.layer = layer
            return
        self.group_id = group_id or "UNKNOWN"
        self.layer = layer or "UNKNOWN"
        self.catalog = catalog or "demo_catalog"
        self.control_schema = control_schema or "admin"
        self.logs: list = []
        self._initialized = True

    def _emit(self, level: LogLevel, message: str, **context):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level.value,
            "group_id": self.group_id,
            "layer": self.layer,
            "message": message
        }
        if context:
            sanitized = {}
            for k, v in context.items():
                if isinstance(v, (str, int, float, bool, type(None))):
                    sanitized[k] = v
                else:
                    try:
                        sanitized[k] = json.dumps(v, default=str)
                    except Exception:
                        sanitized[k] = str(v)[:500]
            entry.update(sanitized)
        self.logs.append(entry)
        prefix = f"[{level.value}]"
        ctx_str = ""
        if context:
            items = [f"{k}={v}" for k, v in list(context.items())[:5]]
            ctx_str = " | " + ", ".join(items)
        print(f"{prefix} {message}{ctx_str}")

    def debug(self, message: str, **context):
        self._emit(LogLevel.DEBUG, message, **context)

    def info(self, message: str, **context):
        self._emit(LogLevel.INFO, message, **context)

    def warn(self, message: str, **context):
        self._emit(LogLevel.WARN, message, **context)

    def error(self, message: str, exception: Optional[BaseException] = None, **context):
        if exception:
            context["error_type"] = type(exception).__name__
            context["error_message"] = str(exception)[:500]
            tb = traceback.format_exc(limit=10)
            context["traceback"] = tb[:2000]
        self._emit(LogLevel.ERROR, message, **context)

    def critical(self, message: str, exception: Optional[BaseException] = None, **context):
        if exception:
            context["error_type"] = type(exception).__name__
            context["error_message"] = str(exception)[:500]
            context["traceback"] = traceback.format_exc(limit=15)[:2000]
        self._emit(LogLevel.CRITICAL, message, **context)

    def export_logs_as_json(self) -> str:
        return json.dumps(self.logs, indent=2, default=str)

    def get_error_count(self) -> int:
        return sum(1 for e in self.logs if e["level"] in ("ERROR", "CRITICAL"))

    def reset(self):
        self.logs = []
