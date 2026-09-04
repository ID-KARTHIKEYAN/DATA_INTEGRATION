"""Read existing control tables. Column lists are exact DESCRIBE names from the dump."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Row, SparkSession

from etl_framework.config import (
    CONTROL_SCHEMA,
    HEADER_TABLE,
    L0_DETAIL_TABLE,
    PB_DETAIL_TABLE,
)
from etl_framework.exceptions import MetadataNotFoundError, TaskSelectionError
from etl_framework.identifiers import require_ident, split_qualified_object

# Exact column lists from describe demo_catalog.admin.*
HEADER_COLUMNS = [
    "DATA_FLOW_GROUP_ID",
    "TRIGGER_TYPE",
    "ETL_LAYER",
    "COMPUTE_CLASS_DEV",
    "COMPUTE_CLASS",
    "IS_ACTIVE",
    "INSERTED_BY",
    "UPDATED_BY",
    "INSERTED_TS",
    "UPDATED_TS",
    "BUSINESS_OBJECT_NAME",
    "COST_CENTER",
    "DATA_SME",
    "BUSINESS_UNIT",
    "PRODUCT_OWNER",
    "INGESTION_MODE",
    "INGESTION_BUCKET",
    "SPARK_CONFIGS",
    "WARNING_THRESHOLD_MINS",
    "WARNING_DL_GROUP",
    "MIN_VERSION",
    "MAX_VERSION",
    "target_catalog",
]

L0_COLUMNS = [
    "DATA_FLOW_GROUP_ID",
    "SOURCE",
    "SOURCE_OBJ_SCHEMA",
    "SOURCE_OBJ_NAME",
    "LOB",
    "LOAD_TYPE",
    "INPUT_FILE_FORMAT",
    "STORAGE_TYPE",
    "DQ_LOGIC",
    "DELIMETER",
    "CUSTOM_SCHEMA",
    "CDC_LOGIC",
    "TRANSFORM_QUERY",
    "PRESTAG_FLAG",
    "PARTITION",
    "LS_FLAG",
    "LS_DETAIL",
    "IS_ACTIVE",
    "INSERTED_BY",
    "UPDATED_BY",
    "INSERTED_TS",
    "UPDATED_TS",
    "DEPLOYMENT_SOURCE_DFG",
]

PB_COLUMNS = [
    "DATA_FLOW_GROUP_ID",
    "LOB",
    "SOURCE",
    "TARGET_OBJ_SCHEMA",
    "TARGET_OBJ_NAME",
    "PRIORITY",
    "TARGET_OBJ_TYPE",
    "TRANSFORM_QUERY",
    "GENERIC_SCRIPTS",
    "SOURCE_PK",
    "TARGET_PK",
    "LOAD_TYPE",
    "IS_ACTIVE",
    "LS_FLAG",
    "LS_DETAIL",
    "PARTITION_OR_INDEX",
    "INSERTED_BY",
    "UPDATED_BY",
    "INSERTED_TS",
    "UPDATED_TS",
    "CUSTOM_SCRIPT_PARAMS",
    "PARTITION_METHOD",
    "RETENTION_DETAILS",
    "DEPLOYMENT_SOURCE_DFG",
]


def _esc(value: str) -> str:
    return value.replace("'", "''")


def _select(columns: list[str]) -> str:
    quoted = []
    for col in columns:
        if col == "target_catalog":
            quoted.append("`target_catalog`")
        elif col in {"SOURCE", "PARTITION"}:
            quoted.append(f"`{col}`")
        else:
            quoted.append(col)
    return ", ".join(quoted)


@dataclass
class HeaderRow:
    raw: dict[str, Any]

    def __getitem__(self, key: str) -> Any:
        return self.raw.get(key)


@dataclass
class DetailRow:
    layer: str
    raw: dict[str, Any]

    def get(self, key: str, default: Any = None) -> Any:
        return self.raw.get(key, default)

    @property
    def task_key(self) -> str:
        if self.layer == "L0":
            schema = self.raw.get("SOURCE_OBJ_SCHEMA") or ""
            name = self.raw.get("SOURCE_OBJ_NAME") or ""
            return f"{schema}.{name}" if schema else str(name)
        schema = self.raw.get("TARGET_OBJ_SCHEMA") or ""
        name = self.raw.get("TARGET_OBJ_NAME") or ""
        return f"{schema}.{name}"


def row_to_dict(row: Row) -> dict[str, Any]:
    return row.asDict(recursive=True)


class MetadataStore:
    def __init__(self, spark: SparkSession, catalog: str) -> None:
        self.spark = spark
        self.catalog = require_ident(catalog, "catalog")

    def _table(self, name: str) -> str:
        return f"{self.catalog}.{CONTROL_SCHEMA}.{name}"

    def get_header(self, group_id: str, layer: str | None = None) -> HeaderRow:
        filters = [
            f"DATA_FLOW_GROUP_ID = '{_esc(group_id)}'",
            "IS_ACTIVE = 'Y'",
        ]
        if layer:
            filters.append(f"upper(ETL_LAYER) = '{_esc(layer.upper())}'")
        sql = f"""
            SELECT {_select(HEADER_COLUMNS)}
            FROM {self._table(HEADER_TABLE)}
            WHERE {' AND '.join(filters)}
        """
        rows = [row_to_dict(r) for r in self.spark.sql(sql).collect()]
        if not rows:
            raise MetadataNotFoundError(
                f"No active data_flow_control_header row for DATA_FLOW_GROUP_ID={group_id!r}"
                + (f" ETL_LAYER={layer!r}" if layer else "")
            )
        if len(rows) > 1 and not layer:
            layers = sorted({str(r.get("ETL_LAYER")) for r in rows})
            raise TaskSelectionError(
                f"Multiple header rows for DATA_FLOW_GROUP_ID={group_id!r} "
                f"layers={layers}. Pass RUN_LAYER."
            )
        if len(rows) > 1:
            raise TaskSelectionError(
                f"Duplicate header rows for DATA_FLOW_GROUP_ID={group_id!r} ETL_LAYER={layer!r}"
            )
        return HeaderRow(rows[0])

    def list_l0(self, group_id: str, target_object: str = "") -> list[DetailRow]:
        filters = [
            f"DATA_FLOW_GROUP_ID = '{_esc(group_id)}'",
            "IS_ACTIVE = 'Y'",
        ]
        schema_f, name_f = split_qualified_object(target_object) if target_object else (None, "")
        if name_f:
            filters.append(f"SOURCE_OBJ_NAME = '{_esc(name_f)}'")
        if schema_f:
            filters.append(f"SOURCE_OBJ_SCHEMA = '{_esc(schema_f)}'")
        sql = f"""
            SELECT {_select(L0_COLUMNS)}
            FROM {self._table(L0_DETAIL_TABLE)}
            WHERE {' AND '.join(filters)}
            ORDER BY SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME
        """
        rows = [DetailRow("L0", row_to_dict(r)) for r in self.spark.sql(sql).collect()]
        if target_object and not rows:
            raise TaskSelectionError(
                f"No active L0 task DATA_FLOW_GROUP_ID={group_id!r} SOURCE_OBJ_NAME={target_object!r}"
            )
        return rows

    def list_pb(self, group_id: str, target_object: str = "") -> list[DetailRow]:
        filters = [
            f"DATA_FLOW_GROUP_ID = '{_esc(group_id)}'",
            "IS_ACTIVE = 'Y'",
        ]
        schema_f, name_f = split_qualified_object(target_object) if target_object else (None, "")
        if name_f:
            filters.append(f"TARGET_OBJ_NAME = '{_esc(name_f)}'")
        if schema_f:
            filters.append(f"TARGET_OBJ_SCHEMA = '{_esc(schema_f)}'")
        sql = f"""
            SELECT {_select(PB_COLUMNS)}
            FROM {self._table(PB_DETAIL_TABLE)}
            WHERE {' AND '.join(filters)}
            ORDER BY COALESCE(PRIORITY, 999), TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME
        """
        rows = [DetailRow("PB", row_to_dict(r)) for r in self.spark.sql(sql).collect()]
        if target_object and not rows:
            raise TaskSelectionError(
                f"No active PB task DATA_FLOW_GROUP_ID={group_id!r} TARGET_OBJ_NAME={target_object!r}"
            )
        return rows
