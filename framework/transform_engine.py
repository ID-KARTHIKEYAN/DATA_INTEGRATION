# =============================================================================
# framework/transform_engine.py
# Handles both TRANSFORM_QUERY flavours:
#   L0: MAP<STRING,STRING>  → per-column cast expressions
#   L1/L2: STRING           → full SQL SELECT query
# Also applies DQ_LOGIC filters and CDC watermark substitution.
# =============================================================================

from __future__ import annotations
from typing import Any, Dict, Optional


class TransformError(ValueError):
    """Raised when a transformation step fails validation or execution."""
    pass


class TransformEngine:
    """
    Stateless transformation utilities.
    All methods are classmethods — no instantiation needed.
    """

    # ── L0: column-level cast MAP ─────────────────────────────────────────

    @staticmethod
    def apply_l0_cast_map(df, transform_map: Optional[Dict[str, str]], table_name: str):
        """
        Applies per-column cast expressions from data_flow_l0_detail.TRANSFORM_QUERY
        (Spark MAP<STRING,STRING> stored as a Python dict after .asDict()).

        Example metadata value:
            MAP('salary', 'CAST(salary AS DOUBLE)',
                'hire_date', 'TO_DATE(hire_date, "yyyy-MM-dd")')

        Args:
            df:            Source DataFrame.
            transform_map: Python dict {col_name: cast_expr} or None.
            table_name:    Used in error messages.

        Returns:
            DataFrame with casts applied.
        """
        if not transform_map:
            return df

        from pyspark.sql import functions as F

        applied   = 0
        skipped   = []
        errors    = []

        for col_name, cast_expr in transform_map.items():
            if col_name not in df.columns:
                skipped.append(col_name)
                continue
            try:
                df = df.withColumn(col_name, F.expr(cast_expr))
                applied += 1
            except Exception as e:
                errors.append(
                    f"col='{col_name}' expr='{cast_expr[:60]}' err={str(e)[:80]}"
                )

        if skipped:
            print(
                f"  ⚠ [{table_name}] TRANSFORM_QUERY: {len(skipped)} column(s) not in "
                f"DataFrame — skipped: {skipped}"
            )
        if errors:
            raise TransformError(
                f"[{table_name}] TRANSFORM_QUERY cast failed for {len(errors)} column(s):\n"
                + "\n".join(f"    {e}" for e in errors)
            )

        print(f"  TRANSFORM_QUERY: applied {applied} cast(s) on '{table_name}'")
        return df

    # ── DQ_LOGIC filter ───────────────────────────────────────────────────

    @staticmethod
    def apply_dq_filter(df, dq_logic: Optional[str], table_name: str):
        """
        Applies DQ_LOGIC as a DataFrame filter.
        DQ_LOGIC is a SQL WHERE expression, e.g.:
            "employee_id IS NOT NULL AND salary > 0"

        Rows failing the filter are dropped (not sent to a quarantine table —
        count delta is logged for auditing).

        Returns:
            (filtered_df, dropped_count)
        """
        if not dq_logic or not dq_logic.strip():
            return df, 0

        from pyspark.sql import functions as F

        try:
            before = df.count()
            df     = df.filter(F.expr(dq_logic))
            after  = df.count()
            dropped = before - after
            if dropped:
                print(f"  DQ_LOGIC: {dropped} row(s) dropped on '{table_name}'")
            else:
                print(f"  DQ_LOGIC: 0 rows dropped on '{table_name}'")
            return df, dropped
        except Exception as e:
            raise TransformError(
                f"[{table_name}] DQ_LOGIC expression failed: '{dq_logic[:100]}'\n"
                f"  Error: {type(e).__name__}: {str(e)[:200]}\n"
                f"  Hint: DQ_LOGIC must be a valid Spark SQL WHERE expression."
            )

    # ── CDC watermark substitution ────────────────────────────────────────

    @staticmethod
    def apply_cdc_filter(df, cdc_logic: Optional[str],
                         watermark: str, table_name: str):
        """
        Substitutes {watermark} in CDC_LOGIC with the resolved watermark
        timestamp string, then applies as a DataFrame filter.

        CDC_LOGIC example:
            "updated_ts > '{watermark}'"
        Resolved to:
            "updated_ts > '2026-08-29 08:05:56'"

        Args:
            watermark: String timestamp from MetadataReader.get_last_watermark().
                       '1970-01-01 00:00:00' on first run → reads all rows.

        Returns:
            (filtered_df, applied_expression_str)
        """
        if not cdc_logic or not cdc_logic.strip():
            return df, ""

        from pyspark.sql import functions as F

        resolved = cdc_logic.replace("{watermark}", watermark)
        print(f"  CDC_LOGIC: applying '{resolved[:120]}'")

        try:
            filtered = df.filter(F.expr(resolved))
            return filtered, resolved
        except Exception as e:
            raise TransformError(
                f"[{table_name}] CDC_LOGIC expression failed: '{resolved[:120]}'\n"
                f"  Error: {type(e).__name__}: {str(e)[:200]}\n"
                f"  Hint: Check that the column referenced in CDC_LOGIC exists "
                f"in the source file and that the timestamp format matches."
            )

    # ── L1/L2: full SQL transform ─────────────────────────────────────────

    @staticmethod
    def execute_transform_query(spark, query: str, table_name: str):
        """
        Executes a full SQL SELECT query (data_flow_pb_detail.TRANSFORM_QUERY).

        Runs EXPLAIN first to catch unresolved column/table errors before
        executing the full query.

        Returns:
            Spark DataFrame
        """
        from pyspark.sql.utils import AnalysisException

        if not query or not query.strip():
            raise TransformError(
                f"[{table_name}] TRANSFORM_QUERY is empty. "
                f"Provide a full SQL SELECT statement."
            )

        # Dry-run
        print(f"  TRANSFORM_QUERY: running EXPLAIN on '{table_name}'...")
        try:
            spark.sql(f"EXPLAIN {query}")
        except AnalysisException as e:
            raise TransformError(
                f"[{table_name}] TRANSFORM_QUERY failed EXPLAIN (unresolved references).\n"
                f"  Query (first 400 chars): {query[:400]}\n"
                f"  AnalysisException: {str(e)[:400]}\n"
                f"  Hint: Ensure all source tables/schemas exist and use "
                f"fully-qualified 3-part names: catalog.schema.table"
            )

        # Execute
        try:
            df = spark.sql(query)
            print(f"  TRANSFORM_QUERY: executed successfully on '{table_name}'")
            return df
        except Exception as e:
            raise TransformError(
                f"[{table_name}] TRANSFORM_QUERY execution failed.\n"
                f"  {type(e).__name__}: {str(e)[:400]}"
            )
