# =============================================================================
# framework/source_reader.py
# Reads data from any supported source type into a Spark DataFrame.
# Handles: HTTP/GitHub, S3, DBFS, Databricks Volumes, local paths.
# Formats:  csv, tsv, json, parquet, delta, avro, orc, xlsx, xml
# =============================================================================

from __future__ import annotations
import io
import re
from typing import Optional


class SourceReadError(IOError):
    """Raised when source data cannot be read."""
    pass


class SourceReader:
    """
    Multi-format source reader.

    Usage:
        reader = SourceReader(spark)
        df = reader.read(
            source_url    = "https://raw.githubusercontent.com/.../file.csv",
            fmt           = "csv",
            delimiter     = ",",
            custom_schema = None,
        )
    """

    SUPPORTED_HTTP_FORMATS  = {"csv", "tsv", "json", "parquet", "xlsx", "xls", "excel"}
    SUPPORTED_SPARK_FORMATS = {"csv", "tsv", "json", "parquet", "delta",
                                "avro", "orc", "xml"}

    def __init__(self, spark):
        self.spark = spark

    # ── Public entry point ────────────────────────────────────────────────

    def read(
        self,
        source_url: str,
        fmt: str,
        delimiter: str = ",",
        custom_schema_ddl: Optional[str] = None,
    ):
        """
        Reads *source_url* in format *fmt* and returns a Spark DataFrame.

        Args:
            source_url:        Full URL or Databricks path.
            fmt:               File format (case-insensitive).
            delimiter:         CSV/TSV delimiter, or XML row tag if fmt='xml'.
            custom_schema_ddl: DDL-format schema string for TSV/fixed-width files.
                               If provided, inferSchema is disabled for CSV/TSV.

        Raises:
            SourceReadError: On HTTP errors, unsupported format, or read failure.
        """
        fmt = (fmt or "csv").strip().lower()
        if not source_url or not source_url.strip():
            raise SourceReadError(
                "[SourceReadError] SOURCE url/path is empty — cannot read. "
                "Set SOURCE in data_flow_l0_detail."
            )

        url = source_url.strip()
        print(f"  Reading [{fmt}] from: {url[:100]}{'...' if len(url) > 100 else ''}")

        if url.startswith("http"):
            return self._read_http(url, fmt, delimiter, custom_schema_ddl)
        else:
            return self._read_spark_native(url, fmt, delimiter, custom_schema_ddl)

    # ── HTTP / GitHub reader ──────────────────────────────────────────────

    def _read_http(self, url: str, fmt: str, delimiter: str,
                   custom_schema_ddl: Optional[str]):
        try:
            import requests
        except ImportError:
            raise SourceReadError(
                "[SourceReadError] 'requests' package not available. "
                "It is pre-installed in Databricks Runtime. "
                "If running locally, install with: pip install requests"
            )
        try:
            import pandas as pd
        except ImportError:
            raise SourceReadError(
                "[SourceReadError] 'pandas' package not available. "
                "It is pre-installed in Databricks Runtime."
            )

        try:
            resp = requests.get(url, timeout=120)
        except requests.exceptions.ConnectionError as e:
            raise SourceReadError(
                f"[SourceReadError] Could not connect to {url[:80]}: {e}"
            )

        if not resp.ok:
            raise SourceReadError(
                f"[SourceReadError] HTTP {resp.status_code} reading {url[:80]}. "
                f"Check the SOURCE URL and ensure it is publicly accessible."
            )

        content = resp.content

        if fmt in ("csv",):
            sep = delimiter or ","
            try:
                pdf = pd.read_csv(io.BytesIO(content), sep=sep,
                                  low_memory=False, on_bad_lines="skip")
            except Exception as e:
                raise SourceReadError(
                    f"[SourceReadError] pandas.read_csv failed for {url[:80]}: {e}"
                )
            return self.spark.createDataFrame(pdf)

        elif fmt == "tsv":
            try:
                pdf = pd.read_csv(io.BytesIO(content), sep="\t",
                                  low_memory=False, on_bad_lines="skip")
            except Exception as e:
                raise SourceReadError(
                    f"[SourceReadError] TSV read failed for {url[:80]}: {e}"
                )
            return self.spark.createDataFrame(pdf)

        elif fmt in ("xlsx", "xls", "excel"):
            try:
                pdf = pd.read_excel(io.BytesIO(content))
            except Exception as e:
                raise SourceReadError(
                    f"[SourceReadError] Excel read failed for {url[:80]}: {e}"
                )
            return self.spark.createDataFrame(pdf)

        elif fmt == "json":
            try:
                pdf = pd.read_json(io.BytesIO(content))
            except Exception as e:
                raise SourceReadError(
                    f"[SourceReadError] JSON read failed for {url[:80]}: {e}"
                )
            return self.spark.createDataFrame(pdf)

        elif fmt == "parquet":
            try:
                pdf = pd.read_parquet(io.BytesIO(content))
            except Exception as e:
                raise SourceReadError(
                    f"[SourceReadError] Parquet read failed for {url[:80]}: {e}"
                )
            return self.spark.createDataFrame(pdf)

        else:
            raise SourceReadError(
                f"[SourceReadError] Format '{fmt}' is not supported for HTTP sources. "
                f"Supported HTTP formats: {sorted(self.SUPPORTED_HTTP_FORMATS)}"
            )

    # ── Spark-native reader (S3, DBFS, Volumes, Delta) ───────────────────

    def _read_spark_native(self, path: str, fmt: str, delimiter: str,
                           custom_schema_ddl: Optional[str]):
        reader = self.spark.read

        try:
            if fmt in ("csv", "tsv"):
                sep = "\t" if fmt == "tsv" else (delimiter or ",")
                reader = reader.option("header", "true").option("delimiter", sep)
                if custom_schema_ddl and custom_schema_ddl.strip():
                    # Apply user-provided DDL schema — disables inferSchema
                    reader = reader.schema(custom_schema_ddl)
                    print(f"  Using CUSTOM_SCHEMA (no inferSchema)")
                else:
                    reader = (reader
                              .option("inferSchema", "true")
                              .option("multiLine", "true")
                              .option("escape", '"'))
                return reader.csv(path)

            elif fmt == "json":
                return reader.option("multiLine", "true").json(path)

            elif fmt in ("parquet", "avro", "orc", "delta"):
                return reader.format(fmt).load(path)

            elif fmt == "xml":
                # DELIMETER column repurposed as XML row tag for xml format
                row_tag = delimiter or "row"
                try:
                    return (reader
                            .format("xml")
                            .option("rowTag", row_tag)
                            .load(path))
                except Exception as e:
                    raise SourceReadError(
                        f"[SourceReadError] XML read failed for {path[:80]}. "
                        f"Ensure the 'spark-xml' package is available on the cluster. "
                        f"DELIMETER column should contain the XML row tag (e.g. 'record'). "
                        f"Error: {e}"
                    )

            else:
                raise SourceReadError(
                    f"[SourceReadError] Unsupported INPUT_FILE_FORMAT='{fmt}'. "
                    f"Supported: {sorted(self.SUPPORTED_SPARK_FORMATS | self.SUPPORTED_HTTP_FORMATS)}"
                )

        except SourceReadError:
            raise
        except Exception as e:
            raise SourceReadError(
                f"[SourceReadError] Failed to read [{fmt}] from {path[:80]}: "
                f"{type(e).__name__}: {str(e)[:300]}"
            )

    # ── URL resolver ──────────────────────────────────────────────────────

    @staticmethod
    def resolve_url(source: str, storage_type: str,
                    ls_flag: str, ls_detail: str) -> str:
        """
        Resolves the effective source URL from L0 metadata columns.

        Priority:
          1. LS_FLAG='Y' → use LS_DETAIL
          2. SOURCE starts with http/s3/dbfs//Volumes → use as-is
          3. SOURCE is relative → join with STORAGE_TYPE as base
        """
        ls_flag    = (ls_flag or "N").upper()
        ls_detail  = (ls_detail or "").strip()
        source     = (source or "").strip()
        storage    = (storage_type or "").strip().rstrip("/")

        if ls_flag == "Y" and ls_detail:
            print(f"  LS_FLAG=Y: source overridden with LS_DETAIL")
            return ls_detail

        absolute_prefixes = ("http://", "https://", "s3://", "s3a://",
                             "abfss://", "dbfs:/", "/Volumes/", "/mnt/")
        if any(source.startswith(p) for p in absolute_prefixes):
            return source

        # Relative path — join with STORAGE_TYPE base
        if storage:
            return f"{storage}/{source.lstrip('/')}"

        return source
