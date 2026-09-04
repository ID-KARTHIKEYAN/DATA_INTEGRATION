from dataclasses import dataclass
from typing import Any

from .errors import MetadataError

HEADER = "data_flow_control_header"
L0_DETAIL = "data_flow_l0_detail"
PB_DETAIL = "data_flow_pb_detail"


@dataclass(frozen=True)
class Task:
    group_id: str
    layer: str
    row: dict[str, Any]

    @property
    def task_id(self) -> str:
        if self.layer == "L0":
            return f"{self.group_id}|L0|{self.row['SOURCE_OBJ_SCHEMA']}|{self.row['SOURCE_OBJ_NAME']}"
        return f"{self.group_id}|{self.layer}|{self.row['TARGET_OBJ_SCHEMA']}|{self.row['TARGET_OBJ_NAME']}"


class MetadataRepository:
    """Reads each existing table with its own documented column contract."""

    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def _table(self, name: str):
        return self.spark.table(f"{self.catalog}.admin.{name}")

    def header(self, group_id: str) -> dict[str, Any]:
        escaped = group_id.replace("'", "''")
        rows = [r.asDict(recursive=True) for r in self._table(HEADER).where(
            f"IS_ACTIVE = 'Y' AND DATA_FLOW_GROUP_ID = '{escaped}'"
        ).collect()]
        if len(rows) != 1:
            raise MetadataError(f"Expected exactly one active header for {group_id}; found {len(rows)}")
        return rows[0]

    def tasks(self, group_id: str, layer: str, task_id: str | None = None) -> list[Task]:
        layer = layer.upper()
        detail = L0_DETAIL if layer == "L0" else PB_DETAIL if layer in {"L1", "L2"} else None
        if detail is None:
            raise MetadataError(f"Unsupported layer: {layer}")
        escaped = group_id.replace("'", "''")
        rows = [r.asDict(recursive=True) for r in self._table(detail).where(
            f"IS_ACTIVE = 'Y' AND DATA_FLOW_GROUP_ID = '{escaped}'"
        ).collect()]
        tasks = [Task(group_id, layer, row) for row in rows]
        if task_id:
            tasks = [task for task in tasks if task.task_id == task_id]
            if not tasks:
                raise MetadataError(f"Task {task_id!r} is not active in {group_id}/{layer}")
        if not tasks:
            raise MetadataError(f"No active metadata tasks for {group_id}/{layer}")
        return tasks

    def validate_group(self, group_id: str, layer: str) -> dict[str, Any]:
        header = self.header(group_id)
        declared = str(header.get("ETL_LAYER") or "").upper()
        if declared != layer.upper():
            raise MetadataError(f"Header ETL_LAYER={declared!r} does not match requested {layer!r}")
        return header