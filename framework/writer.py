# =============================================================================
# framework/writer.py
# Handles all Delta write patterns:
#   FULL     → overwrite + overwriteSchema
#   APPEND   → append + mergeSchema
#   MERGE    → Delta MERGE (used for LOAD_TYPE=DELTA at L1/L2)
#   SCD      → SCD Type 2 (close old, insert new version)
# Also handles:
#   PARTITION          → classic partitionBy
#   LIQUID_CLUSTER     → ALTER TABLE ... CLUSTER BY (post-write)
#   RETENTION_DETAILS  → DELETE rows older than N days
# =============================================================================

from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional


class WriteError(RuntimeError):
    """Raised when a Delta write operation fails after retries."""
    pass


class DeltaWriter:
    """
    All write operations for the ETL framework.

    Usage:
        writer = DeltaWriter(spark, catalog="demo_catalog")
        count  = writer.write(df, schema="silver", table="dim_employee",
                              load_type="MERGE", merge_keys="employee_id",
                              partition_cols="department", partition_method="PARTITION")
    """

    def __init__(self, spark, catalog: str = "demo_catalog"):
        self.spark   = spark
        self.catalog = catalog

    # ── Main entry point ──────────────────────────────────────────────────

    def write(
        self,
        df,
        schema: str,
        table: str,
        load_type: str        = "FULL",
        merge_keys: str       = "",
        partition_cols: str   = "",
        partition_method: str = "",
        retention_days: Optional[int] = None,
    ) -> int:
        """
        Writes *df* to {catalog}.{schema}.{table} using the specified strategy.

        Args:
            df:               Source DataFrame (already transformed + audit cols added).
            schema:           Target Databricks schema.
            table:            Target Delta table name.
            load_type:        FULL | APPEND | MERGE | SCD | OVERWRITE
            merge_keys:       Comma-separated key columns for MERGE/SCD.
            partition_cols:   Comma-separated partition columns.
            partition_method: PARTITION | LIQUID_CLUSTER | empty
            retention_days:   Days to retain; older rows deleted after write.

        Returns:
            Row count of the target table after the write.

        Raises:
            WriteError on any non-retryable failure.
            ValidationError for bad parameters.
        """
        from framework.metadata_validator import ValidationError

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}")

        full_name       = f"{self.catalog}.{schema}.{table}"
        load_type       = (load_type or "FULL").strip().upper()
        part_cols       = [c.strip() for c in (partition_cols or "").split(",") if c.strip()]
        part_method     = (partition_method or "").strip().upper()

        # Validate partition columns exist in df before writing
        if part_cols:
            missing = [c for c in part_cols if c not in df.columns]
            if missing:
                raise ValidationError(
                    f"[{table}] Partition column(s) not found in DataFrame: {missing}. "
                    f"Available columns: {list(df.columns)}"
                )

        print(f"  Writing → {full_name}  [LOAD_TYPE={load_type}]")

        # ── Dispatch ─────────────────────────────────────────────────────
        if load_type in ("FULL", "OVERWRITE"):
            self._write_full(df, full_name, part_cols)

        elif load_type == "APPEND":
            self._write_append(df, full_name, part_cols)

        elif load_type == "MERGE":
            keys = [k.strip() for k in (merge_keys or "").split(",") if k.strip()]
            if not keys:
                from framework.metadata_validator import ValidationError
                raise ValidationError(
                    f"[{table}] LOAD_TYPE=MERGE requires merge_keys (TARGET_PK or SOURCE_PK). "
                    f"Both are empty."
                )
            # Validate merge key columns exist in df
            missing_keys = [k for k in keys if k not in df.columns]
            if missing_keys:
                from framework.metadata_validator import ValidationError
                raise ValidationError(
                    f"[{table}] Merge key column(s) not in DataFrame: {missing_keys}. "
                    f"Available: {list(df.columns)}"
                )
            self._write_merge(df, full_name, keys, part_cols)

        elif load_type == "SCD":
            keys = [k.strip() for k in (merge_keys or "").split(",") if k.strip()]
            if not keys:
                from framework.metadata_validator import ValidationError
                raise ValidationError(
                    f"[{table}] LOAD_TYPE=SCD requires merge_keys (TARGET_PK). "
                    f"Set TARGET_PK in data_flow_pb_detail."
                )
            self._write_scd2(df, full_name, keys, part_cols)

        else:
            raise WriteError(
                f"[{table}] Unsupported LOAD_TYPE='{load_type}'. "
                f"Must be FULL, APPEND, MERGE (DELTA), or SCD."
            )

        # ── Post-write: Liquid Clustering ─────────────────────────────────
        if part_method == "LIQUID_CLUSTER" and part_cols:
            self._apply_liquid_clustering(full_name, part_cols)

        # ── Post-write: Retention ─────────────────────────────────────────
        if retention_days and int(retention_days) > 0:
            self._apply_retention(full_name, int(retention_days))

        # Return row count
        count = self.spark.table(full_name).count()
        print(f"  Row count after write: {count:,}")
        return count

    # ── Write strategies ──────────────────────────────────────────────────

    def _write_full(self, df, full_name: str, part_cols: List[str]) -> None:
        writer = (df.write.format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true"))
        if part_cols:
            writer = writer.partitionBy(*part_cols)
        writer.saveAsTable(full_name)

    def _write_append(self, df, full_name: str, part_cols: List[str]) -> None:
        writer = (df.write.format("delta")
                  .mode("append")
                  .option("mergeSchema", "true"))
        if part_cols:
            writer = writer.partitionBy(*part_cols)
        writer.saveAsTable(full_name)

    def _write_merge(self, df, full_name: str,
                     keys: List[str], part_cols: List[str]) -> None:
        """Delta MERGE — upsert. Creates target table if it doesn't exist."""
        ts   = datetime.now().strftime("%Y%m%d%H%M%S%f")[:16]
        # Sanitise table name for temp view (strip catalog.schema prefix)
        base = full_name.split(".")[-1][:30]
        view = f"_mrg_{base}_{ts}"

        df.createOrReplaceTempView(view)

        # Create target table if absent
        try:
            self.spark.sql(f"SELECT 1 FROM {full_name} LIMIT 1")
        except Exception:
            init = df.limit(0).write.format("delta").mode("append")
            if part_cols:
                init = init.partitionBy(*part_cols)
            init.saveAsTable(full_name)
            print(f"  Created empty target table: {full_name}")

        merge_cond  = " AND ".join([f"tgt.`{k}` = src.`{k}`" for k in keys])
        update_cols = [c for c in df.columns if c not in keys]

        if not update_cols:
            # Edge case: all columns are keys — only INSERT matters
            spark_merge_sql = f"""
                MERGE INTO {full_name} AS tgt
                USING {view} AS src
                ON {merge_cond}
                WHEN NOT MATCHED THEN INSERT *
            """
        else:
            update_set  = ", ".join([f"tgt.`{c}` = src.`{c}`" for c in update_cols])
            spark_merge_sql = f"""
                MERGE INTO {full_name} AS tgt
                USING {view} AS src
                ON {merge_cond}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT *
            """

        try:
            self.spark.sql(spark_merge_sql)
        except Exception as e:
            raise WriteError(
                f"[MERGE] Failed on {full_name}: {type(e).__name__}: {str(e)[:300]}\n"
                f"  Keys: {keys}\n"
                f"  Hint: Check that the target schema has not drifted from the source. "
                f"If schema changed, run a FULL load once to reset the table."
            )
        finally:
            try:
                self.spark.catalog.dropTempView(view)
            except Exception:
                pass

    def _write_scd2(self, df, full_name: str,
                    keys: List[str], part_cols: List[str]) -> None:
        """
        SCD Type 2 — closes the current record and inserts a new version.
        Adds columns: scd_start_date, scd_end_date, is_current.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType, BooleanType

        # Add SCD2 tracking columns if not already present
        if "scd_start_date" not in df.columns:
            df = df.withColumn("scd_start_date", F.current_timestamp())
        if "scd_end_date" not in df.columns:
            df = df.withColumn("scd_end_date", F.lit(None).cast(TimestampType()))
        if "is_current" not in df.columns:
            df = df.withColumn("is_current", F.lit(True))

        ts   = datetime.now().strftime("%Y%m%d%H%M%S%f")[:16]
        base = full_name.split(".")[-1][:30]
        view = f"_scd_{base}_{ts}"
        df.createOrReplaceTempView(view)

        # Create target if absent
        try:
            self.spark.sql(f"SELECT 1 FROM {full_name} LIMIT 1")
        except Exception:
            init = df.limit(0).write.format("delta").mode("append")
            if part_cols:
                init = init.partitionBy(*part_cols)
            init.saveAsTable(full_name)
            print(f"  Created empty SCD2 target: {full_name}")

        merge_cond = " AND ".join([f"tgt.`{k}` = src.`{k}`" for k in keys])

        try:
            self.spark.sql(f"""
                MERGE INTO {full_name} AS tgt
                USING {view} AS src
                ON {merge_cond} AND tgt.is_current = true
                WHEN MATCHED THEN UPDATE SET
                    tgt.scd_end_date = current_timestamp(),
                    tgt.is_current   = false
                WHEN NOT MATCHED THEN INSERT *
            """)
        except Exception as e:
            raise WriteError(
                f"[SCD2] Merge failed on {full_name}: {type(e).__name__}: {str(e)[:300]}"
            )
        finally:
            try:
                self.spark.catalog.dropTempView(view)
            except Exception:
                pass

    # ── Post-write helpers ────────────────────────────────────────────────

    def _apply_liquid_clustering(self, full_name: str, part_cols: List[str]) -> None:
        """
        Applies Delta liquid clustering via ALTER TABLE.
        Best-effort — non-fatal if cluster version doesn't support it.
        """
        cols_sql = ", ".join(part_cols)
        try:
            self.spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cols_sql})")
            print(f"  Liquid clustering applied: CLUSTER BY ({cols_sql})")
        except Exception as e:
            print(
                f"  ⚠ Liquid clustering failed (non-fatal): {type(e).__name__}: {str(e)[:150]}. "
                f"Requires DBR 13.3+ and Delta 3.x."
            )

    def _apply_retention(self, full_name: str, retention_days: int) -> None:
        """
        Deletes rows older than *retention_days*.
        Searches for a known ETL timestamp column; logs warning if none found.
        Best-effort — never raises.
        """
        try:
            cols          = self.spark.table(full_name).columns
            ts_candidates = ["_etl_load_ts", "load_ts", "LOAD_TS",
                             "inserted_ts", "INSERTED_TS", "created_ts"]
            ts_col        = next((c for c in ts_candidates if c in cols), None)

            if not ts_col:
                print(
                    f"  ⚠ Retention: no timestamp column found in {full_name}. "
                    f"Checked: {ts_candidates}. Skipping."
                )
                return

            cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
            self.spark.sql(
                f"DELETE FROM {full_name} WHERE `{ts_col}` < '{cutoff}'"
            )
            print(f"  Retention: deleted rows with {ts_col} < {cutoff} ({retention_days} days)")

        except Exception as e:
            print(f"  ⚠ Retention failed (non-fatal): {type(e).__name__}: {str(e)[:150]}")

    # ── MV handler ────────────────────────────────────────────────────────

    def create_or_replace_mv(self, catalog: str, schema: str,
                              table: str, transform_query: str) -> str:
        """
        Creates or replaces a Materialized View.
        On Free Edition Serverless Jobs, MV creation may not be supported.
        Returns 'SUCCESS' or 'SKIPPED' with explanation.
        """
        from pyspark.sql.utils import AnalysisException

        full_name = f"{catalog}.{schema}.{table}"
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        try:
            self.spark.sql(f"""
                CREATE OR REPLACE MATERIALIZED VIEW {full_name}
                AS {transform_query}
            """)
            print(f"  MV created/refreshed: {full_name}")
            return "SUCCESS"
        except AnalysisException as e:
            err_str = str(e).lower()
            if any(kw in err_str for kw in
                   ["materialized view", "not supported", "dlt", "delta live"]):
                msg = (
                    f"Materialized View '{full_name}' cannot be created in a Serverless Job "
                    f"context (Databricks Free Edition). "
                    f"Consider changing TARGET_OBJ_TYPE to 'Table' with LOAD_TYPE='FULL', "
                    f"or use a VIEW instead. Error: {str(e)[:200]}"
                )
                print(f"  ⚠ SKIPPED: {msg}")
                return f"SKIPPED: {msg}"
            raise WriteError(
                f"[MV] CREATE MATERIALIZED VIEW failed for {full_name}: {str(e)[:300]}"
            )
