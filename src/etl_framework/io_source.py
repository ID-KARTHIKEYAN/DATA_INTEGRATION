"""Source readers driven by L0 columns: SOURCE, STORAGE_TYPE, INPUT_FILE_FORMAT, DELIMETER, CUSTOM_SCHEMA."""

from __future__ import annotations

import io

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from etl_framework.exceptions import LoadExecutionError, SchemaValidationError
from etl_framework.logging_utils import log_event
from etl_framework.retry import retry_call


def resolve_source_path(row: dict) -> str:
    source = str(row.get("SOURCE") or "").strip()
    storage = str(row.get("STORAGE_TYPE") or "").strip()
    obj = str(row.get("SOURCE_OBJ_NAME") or "").strip()
    if source.startswith("http://") or source.startswith("https://"):
        return source
    if storage.startswith("http://") or storage.startswith("https://"):
        return storage.rstrip("/") + "/" + obj.lstrip("/")
    if storage and obj and not source:
        return storage.rstrip("/") + "/" + obj.lstrip("/")
    if source:
        return source
    raise LoadExecutionError("Cannot resolve source path from SOURCE / STORAGE_TYPE / SOURCE_OBJ_NAME")


def _parse_custom_schema(custom_schema: object | None) -> StructType | None:
    if custom_schema is None:
        return None
    text = str(custom_schema).strip()
    if not text:
        return None
    try:
        return StructType.fromJson(__import__("json").loads(text))
    except Exception as exc:  # noqa: BLE001
        raise SchemaValidationError(f"CUSTOM_SCHEMA is not valid Spark JSON schema: {exc}") from exc


def read_l0_source(spark: SparkSession, row: dict) -> DataFrame:
    fmt = str(row.get("INPUT_FILE_FORMAT") or "csv").strip().lower()
    delimiter = str(row.get("DELIMETER") or ",").strip() or ","
    url = resolve_source_path(row)
    schema = _parse_custom_schema(row.get("CUSTOM_SCHEMA"))
    log_event("read_source", format=fmt, url=url[:200], delimiter=delimiter)

    def _read() -> DataFrame:
        if url.startswith("http://") or url.startswith("https://"):
            return _read_http(spark, url, fmt, delimiter, schema)
        reader = spark.read.format(fmt)
        if schema is not None:
            reader = reader.schema(schema)
        if fmt == "csv":
            reader = reader.option("header", "true").option("delimiter", delimiter)
            if schema is None:
                reader = reader.option("inferSchema", "true")
        elif fmt == "json":
            reader = reader.option("multiLine", "true")
        elif fmt in {"parquet", "orc", "delta", "avro"}:
            pass
        else:
            raise LoadExecutionError(f"Unsupported INPUT_FILE_FORMAT={fmt}")
        return reader.load(url)

    try:
        return retry_call(_read, context=f"read {url[:80]}")
    except Exception as exc:  # noqa: BLE001
        raise LoadExecutionError(f"Failed reading SOURCE={url!r} format={fmt}: {exc}") from exc


def _read_http(
    spark: SparkSession,
    url: str,
    fmt: str,
    delimiter: str,
    schema: StructType | None,
) -> DataFrame:
    import pandas as pd
    import requests

    response = requests.get(url, timeout=120)
    response.raise_for_status()
    bio = io.BytesIO(response.content)
    if fmt == "csv":
        pdf = pd.read_csv(bio, sep=delimiter)
    elif fmt == "json":
        pdf = pd.read_json(bio)
    elif fmt == "parquet":
        pdf = pd.read_parquet(bio)
    elif fmt in {"xlsx", "xls", "excel"}:
        pdf = pd.read_excel(bio)
    else:
        raise LoadExecutionError(f"HTTP INPUT_FILE_FORMAT not supported: {fmt}")
    df = spark.createDataFrame(pdf)
    if schema is not None:
        return _apply_schema(df, schema)
    return df


def _apply_schema(df: DataFrame, schema: StructType) -> DataFrame:
    from pyspark.sql import functions as F

    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise SchemaValidationError(f"Source missing columns from CUSTOM_SCHEMA: {missing}")
    selects = []
    for field in schema.fields:
        selects.append(F.col(field.name).cast(field.dataType).alias(field.name))
    return df.select(*selects)
