import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from ..utils.retry_handler import RetryHandler, retry_with_backoff
from ..utils.schema_validator import SchemaValidator
from ..utils.structured_logger import StructuredLogger
from ..utils.spark_utils import SparkUtils


class TargetWriter:
    LOAD_TYPES = {"FULL", "DELTA", "INCREMENTAL", "APPEND", "OVERWRITE", "MERGE", "SCD", "SCD2"}

    def __init__(self, spark, logger: Optional[StructuredLogger] = None):
        self.spark = spark
        self.logger = logger or StructuredLogger()
        self.retry = RetryHandler(max_attempts=3, base_delay_sec=5.0, max_delay_sec=120.0)
        self.schema_validator = SchemaValidator()

    def _create_schema(self, catalog: str, schema: str):
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        except Exception as e:
            self.logger.warn(f"Could not create schema {catalog}.{schema}", error=str(e)[:100])

    @retry_with_backoff(max_attempts=3, base_delay_sec=5.0)
    def write_table(self, df: DataFrame, catalog: str, schema: str, table: str,
                load_type: str = "FULL", merge_keys: Optional[List[str]] = None,
                partition_cols: Optional[List[str]] = None,
                partition_method: Optional[str] = None,
                retention_days: Optional[int] = None,
                object_type: str = "TABLE",
                table_properties: Optional[Dict[str, str]] = None) -> int:
        if df is None:
            raise ValueError("DataFrame cannot be None")
        self._create_schema(catalog, schema)
        full_name = SparkUtils.qualify_table_name(catalog, schema, table)
        load = (load_type or "FULL").strip().upper()
        obj_type = (object_type or "TABLE").strip().upper()
        self.logger.info("Writing target", table=full_name, load=load, type=obj_type,
                       rows=int(df.count()), cols=len(df.columns))
        if obj_type == "VIEW":
            return self._write_view(df, catalog, schema, table, load, full_name)
        part_cols = [c.strip() for c in (partition_cols or []) if c and c.strip()]
        p_method = (partition_method or "PARTITION").strip().upper() if partition_cols else None
        if load in ("FULL", "OVERWRITE"):
            count = self._write_overwrite(df, full_name, part_cols, p_method)
        elif load in ("APPEND", "INCREMENTAL", "DELTA"):
            count = self._write_append(df, full_name, part_cols, p_method)
        elif load in ("MERGE",):
            if not merge_keys:
                raise ValueError(f"MERGE load requires non-empty merge_keys")
            count = self._write_merge(df, full_name, merge_keys, part_cols, p_method)
        elif load in ("SCD", "SCD2"):
            if not merge_keys:
                raise ValueError(f"{load} requires non-empty merge_keys (TARGET_PK or SOURCE_PK)")
            count = self._write_scd2(df, full_name, merge_keys, part_cols, p_method)
        else:
            raise ValueError(f"Unsupported LOAD_TYPE '{load}'. Expected one of {self.LOAD_TYPES}")
        if retention_days and retention_days > 0:
            try:
                self._apply_retention(full_name, retention_days)
            except Exception as e:
                self.logger.warn("Retention policy failed", error=str(e)[:150], table=full_name)
        return count

    def _write_view(self, df: DataFrame, catalog: str, schema: str, table: str,
                   load_type: str, full_name: str) -> int:
        count = int(df.count())
        tmp_view = f"_vw_src_{SparkUtils.safe_identifier(table)}_{SparkUtils.ts_label()}"
        df.createOrReplaceTempView(tmp_view)
        self.logger.info("Creating or replacing VIEW", view=full_name, rows=count)
        self.spark.sql(f"CREATE OR REPLACE VIEW {full_name} AS SELECT * FROM {tmp_view}")
        return count

    def _create_table_ddl(self, df: DataFrame, full_name: str, part_cols: List[str],
                         p_method: Optional[str]) -> str:
        return ""

    def _write_overwrite(self, df: DataFrame, full_name: str, part_cols: List[str],
                         p_method: Optional[str]) -> int:
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if part_cols and (not p_method or p_method == "PARTITION"):
            writer = writer.partitionBy(*part_cols)
        writer.saveAsTable(full_name)
        if part_cols and p_method == "LIQUID_CLUSTER":
            try:
                cols_csv = ", ".join([f"`{c}`" for c in part_cols])
                self.spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols_csv})")
            except Exception as e:
                self.logger.warn("Liquid clustering alter failed", table=full_name,
                               error=str(e)[:100])
        return int(df.count())

    def _write_append(self, df: DataFrame, full_name: str, part_cols: List[str],
                       p_method: Optional[str]) -> int:
        count = int(df.count())
        table_exists = self._table_exists(full_name)
        if not table_exists:
            writer = df.limit(0).write.format("delta").mode("append")
            if part_cols and (not p_method or p_method == "PARTITION"):
                writer = writer.partitionBy(*part_cols)
            writer.saveAsTable(full_name)
            if part_cols and p_method == "LIQUID_CLUSTER":
                try:
                    cols_csv = ", ".join([f"`{c}`" for c in part_cols])
                    self.spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols_csv})")
                except Exception as e:
                    self.logger.warn("Liquid clustering alter failed", table=full_name,
                                   error=str(e)[:100])
        append_writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
        if part_cols and (not p_method or p_method == "PARTITION"):
            append_writer = append_writer.partitionBy(*part_cols)
        append_writer.saveAsTable(full_name)
        return count

    def _write_merge(self, df: DataFrame, full_name: str, merge_keys: List[str],
                     part_cols: List[str], p_method: Optional[str]) -> int:
        if not self._table_exists(full_name):
            writer = df.limit(0).write.format("delta").mode("append")
            if part_cols and (not p_method or p_method == "PARTITION"):
                writer = writer.partitionBy(*part_cols)
            writer.saveAsTable(full_name)
            if part_cols and p_method == "LIQUID_CLUSTER":
                try:
                    cols_csv = ", ".join([f"`{c}`" for c in part_cols])
                    self.spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols_csv})")
                except Exception:
                    pass
        for k in merge_keys:
            if k not in df.columns:
                raise ValueError(f"Merge key '{k}' not in DataFrame columns: {df.columns}")
        null_ok, key_stats = self.schema_validator.check_merge_key_nulls(df, merge_keys, 0.0)
        if not null_ok:
            self.logger.warn("MERGE: some merge keys have NULL values", stats=key_stats)
        tmp_view = f"_mg_{SparkUtils.safe_identifier(full_name.split('.')[-1])}_{SparkUtils.ts_label()}"
        df.createOrReplaceTempView(tmp_view)
        merge_cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in merge_keys])
        non_keys = [c for c in df.columns if c not in merge_keys]
        if non_keys:
            update_set = ", ".join([f"target.`{c}` = source.`{c}`" for c in non_keys])
        else:
            update_set = "target.`{}` = source.`{}`" .format(merge_keys[0], merge_keys[0])
        col_list = ", ".join([f"`{c}`" for c in df.columns])
        val_list = ", ".join([f"source.`{c}`" for c in df.columns])
        merge_sql = f"""
            MERGE INTO {full_name} AS target
            USING {tmp_view} AS source
            ON {merge_cond}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
        """
        self.logger.debug("Executing MERGE", condition=merge_cond[:200], table=full_name)
        self.spark.sql(merge_sql)
        return int(df.count())

    def _write_scd2(self, df: DataFrame, full_name: str, merge_keys: List[str],
                    part_cols: List[str], p_method: Optional[str]) -> int:
        df_scd = df
        if "scd_start_date" not in df_scd.columns:
            df_scd = df_scd.withColumn("scd_start_date", F.current_timestamp())
        else:
            df_scd = df_scd.withColumn("scd_start_date",
                                        F.coalesce(F.col("scd_start_date"), F.current_timestamp()))
        if "scd_end_date" not in df_scd.columns:
            df_scd = df_scd.withColumn("scd_end_date", F.lit(None).cast(TimestampType()))
        if "is_current" not in df_scd.columns:
            df_scd = df_scd.withColumn("is_current", F.lit(True))
        count_new_only = df_scd
        if not self._table_exists(full_name):
            writer = count_new_only.limit(0).write.format("delta").mode("append")
            if part_cols and (not p_method or p_method == "PARTITION"):
                writer = writer.partitionBy(*part_cols)
            writer.saveAsTable(full_name)
            if part_cols and p_method == "LIQUID_CLUSTER":
                try:
                    cols_csv = ", ".join([f"`{c}`" for c in part_cols])
                    self.spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols_csv})")
                except Exception:
                    pass
        tmp_view = f"_scd_{SparkUtils.safe_identifier(full_name.split('.')[-1])}_{SparkUtils.ts_label()}"
        count_new_only.createOrReplaceTempView(tmp_view)
        merge_cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in merge_keys])
        col_list = ", ".join([f"`{c}`" for c in count_new_only.columns])
        val_list = ", ".join([f"source.`{c}`" for c in count_new_only.columns])
        merge_sql = f"""
            MERGE INTO {full_name} AS target
            USING {tmp_view} AS source
            ON {merge_cond} AND target.is_current = true
            WHEN MATCHED THEN
                UPDATE SET
                    target.scd_end_date = current_timestamp(),
                    target.is_current = false
            WHEN NOT MATCHED THEN
                INSERT ({col_list}) VALUES ({val_list})
        """
        self.logger.debug("Executing SCD2 MERGE", condition=merge_cond[:200], table=full_name)
        self.spark.sql(merge_sql)
        return int(count_new_only.count())

    def _table_exists(self, full_name: str) -> bool:
        try:
            parts = full_name.split(".")
            if len(parts) == 3:
                c, s, t = parts
                rows = self.spark.sql(f"SHOW TABLES IN {c}.{s} LIKE '{t}'").collect()
                return any(r.asDict().get("tableName", "").lower() == t.lower() for r in rows)
            return False
        except Exception:
            return False

    def _apply_retention(self, full_name: str, retention_days: int):
        try:
            sample = self.spark.sql(f"SELECT * FROM {full_name} LIMIT 1")
            cols_lower = {c.lower(): c for c in sample.columns}
            ts_candidates = ["load_ts", "_etl_load_ts", "inserted_ts", "created_ts",
                              "ingestion_timestamp", "load_time", "scd_start_date"]
            actual_col = None
            for candidate in ts_candidates:
                if candidate in cols_lower:
                    actual_col = cols_lower[candidate]
                    break
            if not actual_col:
                self.logger.warn("No timestamp column for retention", table=full_name)
                return
            cutoff = datetime.now() - timedelta(days=retention_days)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
            self.spark.sql(f"DELETE FROM {full_name} WHERE `{actual_col}` < '{cutoff_str}'")
            self.logger.info("Retention applied", table=full_name, days=retention_days,
                           cutoff=cutoff_str)
        except Exception as e:
            raise RuntimeError(f"Retention failed on {full_name}: {e}")
