"""Delta writers. View vs table is explicit. MERGE never uses INSERT * blindly without column checks."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from etl_framework.exceptions import MergeExecutionError, SchemaValidationError
from etl_framework.identifiers import fqn, require_ident
from etl_framework.logging_utils import log_event
from etl_framework.transforms import assert_columns_exist, parse_column_list, validate_against_target


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {require_ident(catalog, 'catalog')}.{require_ident(schema, 'schema')}")


def write_frame(
    spark: SparkSession,
    df: DataFrame,
    *,
    catalog: str,
    schema: str,
    table: str,
    load_type: str,
    keys: list[str],
    partition_cols: list[str],
    partition_method: str | None,
    scd: bool = False,
) -> int:
    ensure_schema(spark, catalog, schema)
    full = fqn(catalog, schema, table)
    if partition_cols:
        assert_columns_exist(df, partition_cols, context=f"PARTITION {full}")
    validate_against_target(spark, df, full, load_type="FULL" if load_type == "FULL" else load_type)

    count = df.count()
    log_event("write_start", table=full, load_type=load_type, rows=count, scd=scd)

    if load_type == "FULL":
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(full)
        return count

    if load_type == "DELTA" and not keys:
        writer = df.write.format("delta").mode("append")
        if partition_cols and not spark.catalog.tableExists(full):
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(full)
        return count

    if load_type in {"DELTA", "SCD"} or scd:
        if not keys:
            raise MergeExecutionError(f"MERGE/SCD requires TARGET_PK or SOURCE_PK for {full}")
        assert_columns_exist(df, keys, context=f"merge keys {full}")
        _ensure_table(df, spark, full, partition_cols)
        if scd or load_type == "SCD":
            _merge_scd(spark, df, full, keys)
        else:
            _merge_upsert(spark, df, full, keys)
        return count

    raise MergeExecutionError(f"Unsupported load_type={load_type} for {full}")


def create_view(spark: SparkSession, *, catalog: str, schema: str, name: str, select_sql: str) -> None:
    ensure_schema(spark, catalog, schema)
    full = fqn(catalog, schema, name)
    # Explicit VIEW, never CREATE TABLE
    spark.sql(f"CREATE OR REPLACE VIEW {full} AS {select_sql}")
    log_event("view_created", view=full)


def apply_retention(spark: SparkSession, full_name: str, retention_details: str | None) -> None:
    """RETENTION_DETAILS is a free-form STRING. Supported: integer days using _framework_load_ts."""
    text = str(retention_details or "").strip()
    if not text:
        return
    if not text.isdigit():
        raise SchemaValidationError(
            f"RETENTION_DETAILS={retention_details!r} is not an integer day count. "
            "Do not put SQL here; store day count only (no schema change)."
        )
    days = int(text)
    cols = spark.table(full_name).columns
    ts_col = "_framework_load_ts" if "_framework_load_ts" in cols else None
    if ts_col is None:
        raise SchemaValidationError(
            f"RETENTION_DETAILS set for {full_name} but _framework_load_ts is missing. "
            "Framework adds this column on table loads."
        )
    spark.sql(f"DELETE FROM {full_name} WHERE {ts_col} < current_timestamp() - INTERVAL {days} DAYS")
    log_event("retention_applied", table=full_name, days=days)


def _ensure_table(df: DataFrame, spark: SparkSession, full: str, partition_cols: list[str]) -> None:
    if spark.catalog.tableExists(full):
        return
    writer = df.limit(0).write.format("delta").mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(full)


def _merge_upsert(spark: SparkSession, df: DataFrame, full: str, keys: list[str]) -> None:
    view = "_fw_src_" + full.replace(".", "_")
    df.createOrReplaceTempView(view)
    target_cols = spark.table(full).columns
    source_cols = df.columns
    for k in keys:
        if k not in target_cols:
            raise MergeExecutionError(f"MERGE key {k} not in target {full} columns {target_cols}")
    overlap = [c for c in source_cols if c in target_cols]
    if set(keys) - set(overlap):
        raise MergeExecutionError("MERGE keys missing from overlapping columns")
    cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in keys])
    update_cols = [c for c in overlap if c not in keys]
    if not update_cols:
        raise MergeExecutionError(f"No updatable columns for MERGE into {full}")
    set_clause = ", ".join([f"target.`{c}` = source.`{c}`" for c in update_cols])
    insert_cols = ", ".join([f"`{c}`" for c in overlap])
    insert_vals = ", ".join([f"source.`{c}`" for c in overlap])
    sql = f"""
        MERGE INTO {full} AS target
        USING {view} AS source
        ON {cond}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    try:
        spark.sql(sql)
    except Exception as exc:  # noqa: BLE001
        raise MergeExecutionError(f"MERGE into {full} failed: {exc}") from exc


def _merge_scd(spark: SparkSession, df: DataFrame, full: str, keys: list[str]) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, TimestampType

    work = df
    if "is_current" not in work.columns:
        work = work.withColumn("is_current", F.lit(True).cast(BooleanType()))
    if "scd_start_date" not in work.columns:
        work = work.withColumn("scd_start_date", F.current_timestamp())
    if "scd_end_date" not in work.columns:
        work = work.withColumn("scd_end_date", F.lit(None).cast(TimestampType()))
    view = "_fw_scd_" + full.replace(".", "_")
    work.createOrReplaceTempView(view)
    _ensure_table(work, spark, full, [])
    cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in keys])
    sql = f"""
        MERGE INTO {full} AS target
        USING {view} AS source
        ON {cond} AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.scd_end_date = current_timestamp(),
            target.is_current = false
    """
    insert_cols = ", ".join([f"`{c}`" for c in work.columns])
    try:
        spark.sql(sql)
        spark.sql(f"INSERT INTO {full} ({insert_cols}) SELECT {insert_cols} FROM {view}")
    except Exception as exc:  # noqa: BLE001
        raise MergeExecutionError(f"SCD MERGE into {full} failed: {exc}") from exc


def pk_list(source_pk: object | None, target_pk: object | None) -> list[str]:
    keys = parse_column_list(target_pk) or parse_column_list(source_pk)
    return keys
